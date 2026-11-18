/**
 * How far ahead of the demuxer the engine reads the file, and in what pieces.
 *
 * Mediabunny's UrlSource reads with open-ended `Range: bytes=N-` requests and
 * aborts each one once its read worker reaches its target. Its network
 * prefetch keeps that target only 8–16 MiB ahead: a few seconds of a
 * high-bitrate file, so one slow read stalls playback. Every abort also lets
 * the server and each proxy on the way push far more than the browser keeps.
 *
 * ReadAhead keeps UrlSource's cache and request machinery and changes two
 * things:
 *
 * - A read worker that has read a few MiB in order keeps its target up to a
 *   window ahead of the demuxer, sized from the file's average bitrate and
 *   capped in bytes. The target grows a whole step at a time, once a step
 *   fits in the window and while the worker has less than a quarter of a step
 *   still to fetch, so a seek leaves at most about a step of stale prefetch.
 * - Each request asks only for the bytes up to its worker's target, and at
 *   most one step, so it ends on its own instead of being aborted. When the
 *   target grows meanwhile, UrlSource asks again from where the response
 *   ended.
 *
 * Both reach into UrlSource internals, which is why package.json pins
 * mediabunny to an exact version. When the internals are not where this
 * expects, attach() leaves the source as Mediabunny built it.
 */
import type { UrlSource } from "mediabunny";

const MiB = 1024 * 1024;
/** A worker read near its target within this long is reading, and takes a share of the window. */
const ACTIVE_READ_MS = 5_000;

export interface ReadAheadLimits {
  /** Media time the window aims to hold ahead of the demuxer. */
  windowSeconds: number;
  /** Smallest window, also used until the file's size and duration are known. */
  minWindowBytes: number;
  /** Largest window, whatever the bitrate. */
  maxWindowBytes: number;
  /** Most bytes one request asks for, and how far one extension moves a target. */
  stepBytes: number;
  /** How far a worker must have read in order before its target runs ahead. */
  warmBytes: number;
  /** Cache kept beside the window and the step, for data behind the demuxer. */
  backBytes: number;
}

export const READ_AHEAD_LIMITS: ReadAheadLimits = {
  windowSeconds: 45,
  minWindowBytes: 32 * MiB,
  maxWindowBytes: 128 * MiB,
  stepBytes: 16 * MiB,
  warmBytes: 4 * MiB,
  backBytes: 48 * MiB,
};

/** The cache a window needs: the window, a step on top of it, and data behind. */
export function readAheadCacheBytes(
  windowBytes: number,
  limits: ReadAheadLimits = READ_AHEAD_LIMITS,
): number {
  return windowBytes + limits.stepBytes + limits.backBytes;
}

/** Bytes of a file the window holds: `windowSeconds` at its average bitrate, within bounds. */
export function readAheadWindowBytes(
  fileSize: number | null,
  durationSeconds: number | null,
  limits: ReadAheadLimits = READ_AHEAD_LIMITS,
): number {
  if (!fileSize || !durationSeconds || !(durationSeconds > 0)) return limits.minWindowBytes;
  const bytes = Math.round((fileSize / durationSeconds) * limits.windowSeconds);
  return Math.min(limits.maxWindowBytes, Math.max(limits.minWindowBytes, bytes));
}

/** The parts of Mediabunny's ReadWorker this module reads. */
interface ReadWorker {
  startPos: number;
  currentPos: number;
  targetPos: number;
  /** Whether ending before the target is an error, rather than the end of the file. */
  strictTarget: boolean;
  running: boolean;
}

type PrefetchProfile = (
  start: number,
  end: number,
  workers: ReadWorker[],
) => { start: number; end: number };

/** The parts of Mediabunny's ReadOrchestrator this module reads and replaces. */
interface ReadOrchestrator {
  fileSize: number | null;
  workers: ReadWorker[];
  options: { maxCacheSize: number; prefetchProfile: PrefetchProfile };
}

interface SourceInternals {
  orchestrator: ReadOrchestrator;
  /** The abort controller UrlSource gives each worker's current request. */
  controllers: Map<ReadWorker, AbortController>;
}

function internalsOf(source: UrlSource): SourceInternals | null {
  const { _orchestrator: orchestrator, _abortControllers: controllers } = source as unknown as {
    _orchestrator?: Partial<ReadOrchestrator>;
    _abortControllers?: unknown;
  };
  if (
    !orchestrator ||
    !("fileSize" in orchestrator) ||
    !Array.isArray(orchestrator.workers) ||
    typeof orchestrator.options?.prefetchProfile !== "function" ||
    typeof orchestrator.options.maxCacheSize !== "number" ||
    !(controllers instanceof Map)
  ) {
    return null;
  }
  return {
    orchestrator: orchestrator as ReadOrchestrator,
    controllers: controllers as Map<ReadWorker, AbortController>,
  };
}

/** Whether a 206 states the whole file's size where UrlSource reads it. */
function statesFileSize(response: Response): boolean {
  return (
    !response.headers.has("Content-Encoding") &&
    /\/\d+\s*$/.test(response.headers.get("Content-Range") ?? "")
  );
}

export class ReadAhead {
  private internals: SourceInternals | null = null;
  private durationSeconds: number | null = null;
  /** Cleared once a bounded 206 failed to state the size, until UrlSource learns it some other way. */
  private boundBeforeSize = true;

  constructor(
    private readonly fetchFn: typeof fetch,
    private readonly limits: ReadAheadLimits = READ_AHEAD_LIMITS,
  ) {}

  /** The fetch function to give the UrlSource. */
  readonly fetch = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> =>
    this.boundedFetch(input, init);

  /**
   * Takes over the source's prefetch and request sizes. Returns false, and
   * leaves the source as it is, when its internals are not where expected.
   */
  attach(source: UrlSource): boolean {
    const internals = internalsOf(source);
    if (!internals) return false;
    this.internals = internals;
    const { options } = internals.orchestrator;
    options.prefetchProfile = this.deepen(options.prefetchProfile);
    return true;
  }

  /** Sizes the window, and the cache to hold it, from the file's duration. */
  setDuration(seconds: number): void {
    this.durationSeconds = seconds;
    const orchestrator = this.internals?.orchestrator;
    if (orchestrator) {
      orchestrator.options.maxCacheSize = readAheadCacheBytes(this.windowBytes(), this.limits);
    }
  }

  windowBytes(): number {
    return readAheadWindowBytes(
      this.internals?.orchestrator.fileSize ?? null,
      this.durationSeconds,
      this.limits,
    );
  }

  /**
   * Wraps Mediabunny's profile: a warm worker the demuxer reads near the
   * target of also keeps the window ahead. Its target moves a whole step at a
   * time, and only once a whole step fits in the window, so each request asks
   * for a full step rather than for what the demuxer read since the last one.
   *
   * Interleaved files are read by one worker. A file that stores its tracks
   * apart is read by one worker per track, and those share the window, so
   * together they stay within the cache. A worker counts as reading only near
   * its target: after a seek back, the demuxer reads inside the stretch the
   * old worker once covered without that worker reading anything.
   */
  private deepen(original: PrefetchProfile): PrefetchProfile {
    const { stepBytes, warmBytes } = this.limits;
    const lastRead = new WeakMap<ReadWorker, number>();
    return (start, end, workers) => {
      const range = original(start, end, workers);
      const now = performance.now();
      const fullWindow = this.windowBytes();
      const warm = (worker: ReadWorker) => worker.targetPos - worker.startPos >= warmBytes;
      const atHead = (worker: ReadWorker) =>
        start <= worker.targetPos && end >= worker.targetPos - fullWindow - stepBytes;
      for (const worker of workers) if (atHead(worker)) lastRead.set(worker, now);
      const active = workers.filter(
        (worker) => warm(worker) && now - (lastRead.get(worker) ?? -Infinity) < ACTIVE_READ_MS,
      ).length;
      const window = fullWindow / Math.max(1, active);
      for (const worker of workers) {
        const pending = worker.targetPos - worker.currentPos;
        const room = end + window - worker.targetPos;
        if (!warm(worker) || !atHead(worker) || pending >= stepBytes / 4 || room < stepBytes) {
          continue;
        }
        range.end = Math.max(range.end, worker.targetPos + stepBytes);
      }
      return range;
    };
  }

  private async boundedFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    const bounded = this.bound(init);
    if (!bounded) return this.fetchFn(input, init);
    const response = await this.fetchFn(input, bounded.init);
    // Until a response has stated the file's size, UrlSource takes a body that
    // ends before the target as the end of the file. A bounded 206 must state
    // the size itself (Content-Range hidden from a cross-origin read does not),
    // or it is asked for again without an end, as UrlSource would.
    if (bounded.sizeKnown || response.status !== 206 || statesFileSize(response)) return response;
    this.boundBeforeSize = false;
    void response.body?.cancel().catch(() => {});
    return this.fetchFn(input, init);
  }

  /** The request with its open-ended range ended at the worker's target, or null to send it as is. */
  private bound(init?: RequestInit): { init: RequestInit; sizeKnown: boolean } | null {
    const internals = this.internals;
    const headers = init?.headers;
    if (!internals || !init?.signal || !headers || headers instanceof Headers) return null;
    if (Array.isArray(headers)) return null;
    const key = Object.keys(headers).find((name) => name.toLowerCase() === "range");
    const from = key ? /^bytes=(\d+)-$/.exec(headers[key] ?? "") : null;
    if (!key || !from) return null;
    const start = Number(from[1]);
    // UrlSource sends each request with its worker's abort signal.
    const worker = internals.orchestrator.workers.find(
      (each) => internals.controllers.get(each)?.signal === init.signal,
    );
    if (!worker || worker.currentPos !== start) return null;
    const { fileSize } = internals.orchestrator;
    if (fileSize === null) {
      if (!this.boundBeforeSize) return null;
    } else if (!worker.strictTarget) {
      // UrlSource makes its live workers strict when it learns the size, but
      // not a worker it starts later from a read queued before then. A target
      // within the file is never reached by the file ending, so the worker is
      // made strict here the same way.
      if (worker.targetPos > fileSize) return null;
      worker.strictTarget = true;
    }
    const end = Math.min(worker.targetPos, start + this.limits.stepBytes);
    if (end <= start) return null;
    return {
      init: { ...init, headers: { ...headers, [key]: `bytes=${start}-${end - 1}` } },
      sizeKnown: fileSize !== null,
    };
  }
}
