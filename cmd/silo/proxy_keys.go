package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Silo-Server/silo-server/internal/cache"
	"github.com/Silo-Server/silo-server/internal/nodepool"
	"github.com/Silo-Server/silo-server/internal/secret"
	"github.com/Silo-Server/silo-server/internal/streamtelemetry"
)

func proxyNodeIdentity(listen, mode string) (string, string) {
	nodeURL := os.Getenv("NODE_URL")
	nodeName := os.Getenv("NODE_NAME")
	if nodeURL == "" {
		nodeURL = "http://localhost" + listen
		// The guess is this process's whole identity: it keys session
		// bookkeeping *and* selects the stream_nodes row whose acceleration
		// overrides this node adopts. Two nodes listening on the same port
		// guess the same URL and would share both.
		slog.Warn("NODE_URL not set, using listen address — session keys may collide across nodes, and this node adopts the acceleration overrides of whichever stream_nodes row carries that URL",
			"node_url", nodeURL)
	}
	if nodeName == "" {
		nodeName = mode
	}

	return nodeURL, nodeName
}

// startProxyNodeKeys starts the proxy's live signing registry. A proxy cannot
// serve safely without its initial credentials, so startup fails closed.
func startProxyNodeKeys(ctx context.Context, pool *pgxpool.Pool, dataCipher *secret.Cipher, eventBus cache.EventBus) *nodepool.KeyRegistry {
	nodeKeys := nodepool.NewKeyRegistry(nodepool.NewRepository(pool), dataCipher)
	if err := nodeKeys.Refresh(ctx); err != nil {
		slog.ErrorContext(ctx, "load stream node credentials failed", "error", err)
		os.Exit(1)
	}
	nodeKeys.Start(ctx)
	if err := eventBus.Subscribe(ctx, cache.ChannelAdmin, func(event cache.Event) {
		if event.Type == cache.EventNodePoolChanged {
			nodeKeys.Invalidate()
		}
	}); err != nil {
		slog.WarnContext(ctx, "node credential reload events unavailable; reloading every minute instead", "error", err)
	}
	return nodeKeys
}

func stopProxyTelemetry(registry *streamtelemetry.Registry) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := registry.Stop(ctx); err != nil {
		slog.ErrorContext(ctx, "stream telemetry shutdown error", "error", err)
	}
}
