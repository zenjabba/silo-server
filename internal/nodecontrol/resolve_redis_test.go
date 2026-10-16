package nodecontrol_test

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/Silo-Server/silo-server/internal/nodecontrol"
	"github.com/Silo-Server/silo-server/internal/nodecontrol/nodecontroltest"
	"github.com/Silo-Server/silo-server/internal/noderecipe"
	"github.com/Silo-Server/silo-server/internal/playback"
)

// A force reload revokes the node's recipes in the real recipe store before
// it calls the node (internal/api/handlers/admin_node_reload.go). Resolve
// reads that store, so after either revocation a node can neither start a
// progressive remux nor rebuild a lost HLS transcode it was serving.
func TestResolveRefusesAfterAForceReloadRevokesTheNode(t *testing.T) {
	f := newReloadResolveFixture(t)
	// Check each revoked binding independently, including a token minted
	// for a node that no longer owns the transport.
	t.Run("node id", f.checkNodeIDRevocation)
	t.Run("moved transport", f.checkMovedTransportRevocation)
	t.Run("node URL", f.checkNodeURLRevocation)
}

type reloadResolveFixture struct {
	nodeID  int
	nodeURL string
	unique  string
	store   *noderecipe.Store
}

func newReloadResolveFixture(t *testing.T) reloadResolveFixture {
	t.Helper()
	rawURL := os.Getenv("SILO_TEST_REDIS_URL")
	if rawURL == "" {
		t.Skip("SILO_TEST_REDIS_URL not set")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		t.Fatalf("parse SILO_TEST_REDIS_URL: %v", err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })

	unique := uuid.NewString()
	parsed, err := strconv.ParseInt(unique[:7], 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	nodeID := int(parsed) + 1
	nodeURL := "http://node-" + unique
	store := noderecipe.NewStore(client, time.Minute)

	return reloadResolveFixture{nodeID: nodeID, nodeURL: nodeURL, unique: unique, store: store}
}

func (f reloadResolveFixture) core(t *testing.T) *nodecontrol.Client {
	t.Helper()
	return nodecontroltest.Resolver(t, f.nodeID, nodecontrol.Options{
		Nodes:        nodeURLs{urls: map[int]string{f.nodeID: f.nodeURL}},
		StreamSecret: func() string { return resolveSecret },
		Recipes:      f.store,
	})
}

func resolveStatus(t *testing.T, client *nodecontrol.Client, purpose, transportID, token string) (int, string) {
	t.Helper()
	_, err := client.Resolve(t.Context(), nodecontrol.ResolveRequest{TransportID: transportID, Purpose: purpose, StreamToken: token})
	if err == nil {
		return http.StatusOK, ""
	}
	if nodecontrol.Status(err) == 0 {
		t.Fatalf("resolve: %v", err)
	}
	return nodecontrol.Status(err), nodecontrol.Reason(err)
}

func (f reloadResolveFixture) put(t *testing.T, transportID string, card playback.RecipeCard) {
	t.Helper()
	if err := f.store.Put(context.Background(), transportID, card); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.store.Delete(context.Background(), transportID) })
}

func (f reloadResolveFixture) checkNodeIDRevocation(t *testing.T) {
	t.Helper()
	client := f.core(t)
	remux := remuxClaims(f.nodeID, "/media/movie.mkv")
	remux.TranscodeNode = f.nodeURL
	remux.TranscodeTransportID = "remux-" + f.unique
	f.put(t, remux.TranscodeTransportID, playback.RecipeCardFromClaims(&remux))
	remuxToken := signClaims(t, remux, resolveSecret)
	hls := nativeCard("viewer-id-"+f.unique, "hls-id-"+f.unique)
	hls.TranscodeNodeURL = f.nodeURL
	hls.RoutingExecutionNodeID = f.nodeID
	f.put(t, hls.TranscodeTransportID, hls)
	hlsToken := signClaims(t, hls.ToClaims(), resolveSecret)
	// API clocks can run ahead of Redis. A signed token with a future
	// iat still predates the revocation that follows in this test.
	ahead := hls.ToClaims()
	ahead.IssuedAt = jwt.NewNumericDate(time.Now().Add(time.Minute))
	ahead.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Hour))
	aheadToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, ahead).SignedString([]byte(resolveSecret))
	if err != nil {
		t.Fatal(err)
	}

	if status, reason := resolveStatus(t, client, nodecontrol.ResolvePurposeRemux, remux.TranscodeTransportID, remuxToken); status != http.StatusOK {
		t.Fatalf("remux before the revocation = %d %q", status, reason)
	}
	for _, token := range []string{"", hlsToken, aheadToken} {
		if status, reason := resolveStatus(t, client, nodecontrol.ResolvePurposeHLS, hls.TranscodeTransportID, token); status != http.StatusOK {
			t.Fatalf("HLS before the revocation = %d %q", status, reason)
		}
	}
	if err := f.store.RevokeNodeID(context.Background(), f.nodeID); err != nil {
		t.Fatal(err)
	}
	if status, reason := resolveStatus(t, client, nodecontrol.ResolvePurposeRemux, remux.TranscodeTransportID, remuxToken); status != http.StatusGone || reason != nodecontrol.ResolveStopped {
		t.Fatalf("remux after the revocation = %d %q, want 410 %q", status, reason, nodecontrol.ResolveStopped)
	}
	for _, token := range []string{"", hlsToken, aheadToken} {
		if status, reason := resolveStatus(t, client, nodecontrol.ResolvePurposeHLS, hls.TranscodeTransportID, token); status != http.StatusNotFound || reason != nodecontrol.ResolveNotFound {
			t.Fatalf("HLS after the revocation = %d %q, want 404 %q", status, reason, nodecontrol.ResolveNotFound)
		}
	}
	// Revocation must still hold after the recipe itself disappears.
	if err := f.store.Delete(t.Context(), hls.TranscodeTransportID); err != nil {
		t.Fatal(err)
	}
	if status, reason := resolveStatus(t, client, nodecontrol.ResolvePurposeHLS, hls.TranscodeTransportID, hlsToken); status != http.StatusNotFound {
		t.Fatalf("HLS after recipe deletion = %d %q, want 404", status, reason)
	}
	// A new durable start stamps the current generation and can rebuild,
	// even with a token minted in the same second as the reload.
	f.put(t, hls.TranscodeTransportID, hls)
	if status, reason := resolveStatus(t, client, nodecontrol.ResolvePurposeHLS, hls.TranscodeTransportID, hlsToken); status != http.StatusOK {
		t.Fatalf("new HLS after reload = %d %q, want 200", status, reason)
	}
}

func (f reloadResolveFixture) checkMovedTransportRevocation(t *testing.T) {
	t.Helper()
	client := f.core(t)
	minted := nativeCard("viewer-moved-"+f.unique, "hls-moved-"+f.unique)
	minted.RoutingExecutionNodeID = f.nodeID + 1
	token := signClaims(t, minted.ToClaims(), resolveSecret)
	moved := minted
	moved.TranscodeNodeURL = f.nodeURL
	moved.RoutingExecutionNodeID = f.nodeID
	f.put(t, moved.TranscodeTransportID, moved)

	if status, reason := resolveStatus(t, client, nodecontrol.ResolvePurposeHLS, moved.TranscodeTransportID, token); status != http.StatusOK {
		t.Fatalf("HLS before the revocation = %d %q", status, reason)
	}
	if err := f.store.RevokeNodeID(context.Background(), f.nodeID); err != nil {
		t.Fatal(err)
	}
	if status, reason := resolveStatus(t, client, nodecontrol.ResolvePurposeHLS, moved.TranscodeTransportID, token); status != http.StatusForbidden || reason != nodecontrol.ResolveWrongRoute {
		t.Fatalf("HLS after the revocation = %d %q, want 403 %q", status, reason, nodecontrol.ResolveWrongRoute)
	}
}

func (f reloadResolveFixture) checkNodeURLRevocation(t *testing.T) {
	t.Helper()
	client := f.core(t)
	hls := nativeCard("viewer-url-"+f.unique, "hls-url-"+f.unique)
	hls.TranscodeNodeURL = f.nodeURL
	f.put(t, hls.TranscodeTransportID, hls)
	hlsToken := signClaims(t, hls.ToClaims(), resolveSecret)

	for _, token := range []string{"", hlsToken} {
		if status, reason := resolveStatus(t, client, nodecontrol.ResolvePurposeHLS, hls.TranscodeTransportID, token); status != http.StatusOK {
			t.Fatalf("HLS before the revocation = %d %q", status, reason)
		}
	}
	if err := f.store.RevokeNode(context.Background(), f.nodeURL); err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"", hlsToken} {
		if status, reason := resolveStatus(t, client, nodecontrol.ResolvePurposeHLS, hls.TranscodeTransportID, token); status != http.StatusNotFound || reason != nodecontrol.ResolveNotFound {
			t.Fatalf("HLS after the revocation = %d %q, want 404 %q", status, reason, nodecontrol.ResolveNotFound)
		}
	}
	// Revocation must still hold after the recipe itself disappears.
	if err := f.store.Delete(t.Context(), hls.TranscodeTransportID); err != nil {
		t.Fatal(err)
	}
	if status, reason := resolveStatus(t, client, nodecontrol.ResolvePurposeHLS, hls.TranscodeTransportID, hlsToken); status != http.StatusNotFound {
		t.Fatalf("HLS after recipe deletion = %d %q, want 404", status, reason)
	}
	// A new durable start stamps the current generation and can rebuild,
	// even with a token minted in the same second as the reload.
	f.put(t, hls.TranscodeTransportID, hls)
	if status, reason := resolveStatus(t, client, nodecontrol.ResolvePurposeHLS, hls.TranscodeTransportID, hlsToken); status != http.StatusOK {
		t.Fatalf("new HLS after reload = %d %q, want 200", status, reason)
	}
}
