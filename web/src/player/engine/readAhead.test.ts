// @vitest-environment node
import { UrlSource } from "mediabunny";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ReadAhead, readAheadWindowBytes, type ReadAheadLimits } from "./readAhead";

const MiB = 1024 * 1024;
const BODY_CHUNK = 256 * 1024;

/** Small enough for a test file, with the same shape as the real limits. */
const LIMITS: ReadAheadLimits = {
  windowSeconds: 45,
  minWindowBytes: 8 * MiB,
  maxWindowBytes: 64 * MiB,
  stepBytes: 4 * MiB,
  warmBytes: 4 * MiB,
  backBytes: 8 * MiB,
};

interface ServedRequest {
  range: string;
  start: number;
  /** Exclusive end of what the request asked for. */
  end: number;
  delivered: number;
  /** Cancelled before its last byte was delivered. */
  aborted: boolean;
}

/**
 * A file served with byte ranges, each body handed out only as it is read,
 * and with `slow`, a timer turn after it is read. Byte i of the file is i & 0xff.
 */
function rangeServer(size: number, { hideContentRange = false, slow = false } = {}) {
  const requests: ServedRequest[] = [];
  const fetchFn = async (_input: RequestInfo | URL, init?: RequestInit) => {
    const range = (init?.headers as Record<string, string>).Range!;
    const [, from, to] = /^bytes=(\d+)-(\d*)$/.exec(range)!;
    const start = Number(from);
    const end = to ? Math.min(size, Number(to) + 1) : size;
    const served: ServedRequest = { range, start, end, delivered: 0, aborted: false };
    requests.push(served);
    let position = start;
    const markAborted = () => {
      if (position < end) served.aborted = true;
    };
    init?.signal?.addEventListener("abort", markAborted);
    const body = new ReadableStream<Uint8Array>(
      {
        async pull(controller) {
          if (slow) await new Promise((resolve) => setTimeout(resolve, 0));
          if (position >= end) {
            controller.close();
            return;
          }
          const bytes = new Uint8Array(Math.min(BODY_CHUNK, end - position));
          for (let i = 0; i < bytes.length; i++) bytes[i] = (position + i) & 0xff;
          position += bytes.length;
          served.delivered += bytes.length;
          controller.enqueue(bytes);
        },
        cancel: markAborted,
      },
      { highWaterMark: 0 },
    );
    const headers = new Headers({ "Content-Length": String(end - start) });
    if (!hideContentRange) headers.set("Content-Range", `bytes ${start}-${end - 1}/${size}`);
    return new Response(body, { status: 206, headers });
  };
  return { fetchFn: fetchFn as typeof fetch, requests };
}

interface Internals {
  _orchestrator: { fileSize: number | null; workers: Array<{ running: boolean }> };
  _read(
    start: number,
    end: number,
    min: number,
    max: number,
  ): Promise<{ bytes: Uint8Array; offset: number } | null>;
}

function openSource(server: ReturnType<typeof rangeServer>) {
  const readAhead = new ReadAhead(server.fetchFn, LIMITS);
  const source = new UrlSource("https://media.test/stream", {
    fetchFn: readAhead.fetch,
    maxCacheSize: 16 * MiB,
  });
  expect(readAhead.attach(source)).toBe(true);
  const internals = source as unknown as Internals;
  /** Reads like the demuxer, as soon as the bytes arrive, and checks them. */
  const readNow = async (start: number, end: number) => {
    const result = (await internals._read(start, end, 0, Infinity))!;
    const at = start - result.offset;
    expect(result.bytes[at]).toBe(start & 0xff);
    expect(result.bytes[at + end - start - 1]).toBe((end - 1) & 0xff);
  };
  /** Reads like the demuxer at playback pace: each read then waits for every fetch to finish. */
  const read = async (start: number, end: number) => {
    await readNow(start, end);
    for (let turn = 0; internals._orchestrator.workers.some((worker) => worker.running); turn++) {
      if (turn > 1000) throw new Error("the read workers never went idle");
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
  };
  /** How far past `from` the server has delivered, among requests that start in a region. */
  const fetchedPast = (from: number, region: { start: number; end: number }) =>
    Math.max(
      from,
      ...server.requests
        .filter((each) => each.start >= region.start - MiB && each.start < region.end)
        .map((each) => each.start + each.delivered),
    ) - from;
  return { readAhead, internals, read, readNow, fetchedPast };
}

describe("ReadAhead", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("holds the window ahead of an in-order read with bounded requests that all complete", async () => {
    // 45 s of a 128 MiB, 180 s file is a 32 MiB window.
    const size = 128 * MiB;
    const server = rangeServer(size);
    const { readAhead, read } = openSource(server);

    await read(0, 64 * 1024);
    readAhead.setDuration(180);
    expect(readAhead.windowBytes()).toBe(32 * MiB);

    const ahead: number[] = [];
    for (let position = 0; position < 80 * MiB; position += 256 * 1024) {
      // Playback is slower than the network: every read waits for the fetch to catch up.
      await read(position, position + 256 * 1024);
      if (position >= 48 * MiB) {
        const fetched = Math.max(...server.requests.map((each) => each.start + each.delivered));
        ahead.push(fetched - (position + 256 * 1024));
      }
    }

    expect(Math.min(...ahead)).toBeGreaterThanOrEqual(32 * MiB - LIMITS.stepBytes);
    expect(Math.max(...ahead)).toBeLessThanOrEqual(32 * MiB);
    for (const request of server.requests) {
      expect(request.range).toMatch(/^bytes=\d+-\d+$/);
      expect(request.end - request.start).toBeLessThanOrEqual(LIMITS.stepBytes);
      expect(request.aborted).toBe(false);
    }
    // Once the window is full, each request refills a whole step.
    const steady = server.requests.filter((request) => request.start >= 48 * MiB);
    expect(steady.length).toBeGreaterThan(0);
    for (const request of steady) expect(request.end - request.start).toBe(LIMITS.stepBytes);
  });

  it("reads the right bytes while targets grow under requests in flight, and after a seek", async () => {
    // The demuxer reads as soon as bytes arrive, so a target grows while its
    // bounded request is still arriving; that request ends short of the new
    // target, and UrlSource must ask again rather than take it as the file's end.
    const size = 256 * MiB;
    const server = rangeServer(size, { slow: true });
    const { readAhead, internals, readNow } = openSource(server);

    await readNow(0, 64 * 1024);
    readAhead.setDuration(360);
    for (let position = 0; position < 40 * MiB; position += 256 * 1024) {
      await readNow(position, position + 256 * 1024);
    }
    const beforeSeek = server.requests.length;
    for (let position = 200 * MiB; position < 224 * MiB; position += 256 * 1024) {
      await readNow(position, position + 256 * 1024);
    }

    expect(internals._orchestrator.fileSize).toBe(size);
    for (const request of server.requests) {
      expect(request.range).toMatch(/^bytes=\d+-\d+$/);
      expect(request.end - request.start).toBeLessThanOrEqual(LIMITS.stepBytes);
      expect(request.aborted).toBe(false);
    }
    // The first read after the seek asks for no more than it needs to start.
    const firstAfterSeek = server.requests
      .slice(beforeSeek)
      .find((request) => request.start >= 200 * MiB - MiB)!;
    expect(firstAfterSeek.end - firstAfterSeek.start).toBeLessThanOrEqual(MiB);
  });

  it("gives the whole window to a worker started by a seek back", async () => {
    // The old worker covered the stretch the demuxer now reads, but is not
    // reading there, so it takes no share of the window.
    const size = 256 * MiB;
    const server = rangeServer(size);
    const { readAhead, read } = openSource(server);
    let now = 0;
    vi.spyOn(performance, "now").mockImplementation(() => now);

    await read(0, 64 * 1024);
    readAhead.setDuration(360);
    for (let position = 0; position < 96 * MiB; position += 256 * 1024) {
      await read(position, position + 256 * 1024);
    }
    // The viewer seeks back a while after the old worker's last read.
    now += 60_000;
    const beforeSeek = server.requests.length;
    for (let position = 16 * MiB; position < 48 * MiB; position += 256 * 1024) {
      await read(position, position + 256 * 1024);
    }

    const fetched = Math.max(
      ...server.requests.slice(beforeSeek).map((request) => request.start + request.delivered),
    );
    expect(fetched - 48 * MiB).toBeGreaterThanOrEqual(32 * MiB - LIMITS.stepBytes);
  });

  it("shares the window between workers reading tracks stored apart", async () => {
    // A file whose video and audio are stored apart is read at two places at
    // once. Each with a full window would need twice the cache.
    const size = 256 * MiB;
    const server = rangeServer(size);
    const { readAhead, read, fetchedPast } = openSource(server);
    const audioStart = 160 * MiB;

    await read(0, 64 * 1024);
    readAhead.setDuration(360);
    expect(readAhead.windowBytes()).toBe(32 * MiB);

    for (let offset = 0; offset < 48 * MiB; offset += 256 * 1024) {
      await read(offset, offset + 256 * 1024);
      await read(audioStart + offset, audioStart + offset + 256 * 1024);
    }

    const readTo = 48 * MiB;
    // Reads start up to 64 KiB early, so the audio region's first request is just before it.
    const videoAhead = fetchedPast(readTo, { start: 0, end: audioStart - MiB });
    const audioAhead = fetchedPast(audioStart + readTo, { start: audioStart, end: size });
    expect(videoAhead).toBeGreaterThan(0);
    expect(audioAhead).toBeGreaterThan(0);
    expect(videoAhead + audioAhead).toBeLessThanOrEqual(32 * MiB);
  });

  it("asks again without an end when a bounded first response hides the file size", async () => {
    // A cross-origin read without Content-Range exposed: the Content-Length of
    // a bounded range would otherwise be taken as the size of the whole file.
    const size = 64 * MiB;
    const server = rangeServer(size, { hideContentRange: true });
    const { internals } = openSource(server);

    await internals._read(0, 64 * 1024, 0, Infinity);

    expect(internals._orchestrator.fileSize).toBe(size);
    expect(server.requests.map((each) => each.range)).toEqual(["bytes=0-524287", "bytes=0-"]);
  });

  it("sizes the window from the average bitrate within its bounds", () => {
    const cases: Array<[number | null, number | null, number]> = [
      [null, 180, LIMITS.minWindowBytes],
      [128 * MiB, null, LIMITS.minWindowBytes],
      [16 * MiB, 180, LIMITS.minWindowBytes],
      [128 * MiB, 180, 32 * MiB],
      [1024 * MiB, 180, LIMITS.maxWindowBytes],
    ];
    for (const [fileSize, duration, window] of cases) {
      expect(readAheadWindowBytes(fileSize, duration, LIMITS)).toBe(window);
    }
  });
});
