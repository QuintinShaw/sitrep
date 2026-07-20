package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/config"
	"github.com/QuintinShaw/sitrep/daemon/internal/health"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/outbox"
)

// recordingServer captures the path/method of each request and replies with
// a fixed JSON body, so a control-plane CLI command can be driven end to end
// without a real server.
type recordingServer struct {
	mu     sync.Mutex
	path   string
	method string
	body   string
}

func newRecordingServer(reply string) (*httptest.Server, *recordingServer) {
	rec := &recordingServer{body: reply}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.path, rec.method = r.URL.Path, r.Method
		rec.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(rec.body))
	}))
	return srv, rec
}

func (r *recordingServer) hit() (string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.method, r.path
}

// TestSpaceCreateHitsV1PersistsDeviceID drives `sitrep space create` against
// a mock and pins P0-1: it POSTs /v1/spaces and persists the owner device_id
// (as well as token/space) the v1 response now returns — device_seq is scoped
// to (device_id, space), so the owner Mac must know its device_id to uplink.
func TestSpaceCreateHitsV1PersistsDeviceID(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	srv, rec := newRecordingServer(`{"space_id":"k7m3qzx2vt","device_id":"dev_own_8f2a1c","owner_token":"sr1_k7m3qzx2vt_0a47bf1ebf9b6cef76cef2f33d48189f29ea1a552bceaf51"}`)
	defer srv.Close()

	cmdSpaceCreate([]string{"--server", srv.URL})

	if m, p := rec.hit(); m != http.MethodPost || p != "/v1/spaces" {
		t.Fatalf("space create hit %s %s, want POST /v1/spaces", m, p)
	}
	cfg := config.Load()
	if cfg.Space != "k7m3qzx2vt" || cfg.Token == "" {
		t.Fatalf("config not persisted from /v1/spaces response: %+v", cfg)
	}
	if cfg.DeviceID != "dev_own_8f2a1c" {
		t.Fatalf("space create did not persist the owner device_id: %+v", cfg)
	}
	// "owner can send": the uplink config the CLI builds carries that device_id,
	// so the owner's /v1/events reliable-event bodies are valid.
	if uc := uplinkConfig("t1"); uc.DeviceID != "dev_own_8f2a1c" || uc.Space != "k7m3qzx2vt" {
		t.Fatalf("uplinkConfig missing owner device_id/space, owner can't uplink: %+v", uc)
	}
}

// TestJoinHitsV1AndSavesDeviceID drives `sitrep join` and pins that it POSTs
// /v1/join and now persists the device_id the v1 response returns (needed to
// populate /v1/events reliable-event bodies).
func TestJoinHitsV1AndSavesDeviceID(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	srv, rec := newRecordingServer(`{"token":"sr1_k7m3qzx2vt_05b971d953ce184904e159f6886091b6216155fc8b35b6e7","device_id":"dev_9c4e2b7a1f","role":"source","space_id":"k7m3qzx2vt"}`)
	defer srv.Close()

	cmdJoin([]string{"--server", srv.URL, "--space", "k7m3qzx2vt", "--code", "xq7t9m2k4n8p1r6wz"})

	if m, p := rec.hit(); m != http.MethodPost || p != "/v1/join" {
		t.Fatalf("join hit %s %s, want POST /v1/join", m, p)
	}
	cfg := config.Load()
	if cfg.DeviceID != "dev_9c4e2b7a1f" {
		t.Fatalf("join did not persist device_id from the v1 response: %+v", cfg)
	}
	if cfg.Space != "k7m3qzx2vt" || cfg.Token == "" {
		t.Fatalf("join config not persisted: %+v", cfg)
	}
}

// TestInviteHitsV1 drives `sitrep invite` against a mock (with a pre-seeded
// config so api.FromConfig resolves) and pins POST /v1/invites.
func TestInviteHitsV1(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	srv, rec := newRecordingServer(`{"code":"XQ7T9M2K4N8P1R6WZ","expires_in":600,"space_id":"k7m3qzx2vt"}`)
	defer srv.Close()

	if err := config.Save(config.Config{Server: srv.URL, Token: "sr1_k7m3qzx2vt_deadbeef", Space: "k7m3qzx2vt"}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	cmdInvite([]string{"--role", "source"})

	if m, p := rec.hit(); m != http.MethodPost || p != "/v1/invites" {
		t.Fatalf("invite hit %s %s, want POST /v1/invites", m, p)
	}
}

// TestJoinRetriesOnKVLag pins 5b: `sitrep join` must not declare a code
// invalid on a single 404 — the INVITE_DIR KV routing cache is eventually
// consistent, so a code minted moments ago can 404 transiently. Join retries
// across a brief grace window and succeeds once the invite resolves.
func TestJoinRetriesOnKVLag(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	// Fast retry schedule so the test doesn't wait the production grace window.
	old := joinRetrySchedule
	joinRetrySchedule = []time.Duration{5 * time.Millisecond, 5 * time.Millisecond, 5 * time.Millisecond}
	defer func() { joinRetrySchedule = old }()

	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		if n < 3 {
			// KV negative-cache / cross-region lag: the invite isn't visible yet.
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"invite invalid or expired"}`))
			return
		}
		_, _ = w.Write([]byte(`{"token":"sr1_k7m3qzx2vt_05b971d953ce184904e159f6886091b6216155fc8b35b6e7","device_id":"dev_9c4e2b7a1f","role":"source","space_id":"k7m3qzx2vt"}`))
	}))
	defer srv.Close()

	cmdJoin([]string{"--server", srv.URL, "--space", "k7m3qzx2vt", "--code", "xq7t9m2k4n8p1r6wz"})

	if n := atomic.LoadInt32(&calls); n != 3 {
		t.Fatalf("join made %d attempts, want 3 (retried the transient 404s then succeeded)", n)
	}
	if cfg := config.Load(); cfg.DeviceID != "dev_9c4e2b7a1f" {
		t.Fatalf("join did not succeed after KV lag cleared: %+v", cfg)
	}
}

// TestSeqAllocatorFailureSetsHealth pins N1: when the device_seq store
// becomes unwritable, the allocator keeps the safe degraded mode (returns an
// error → the uplink skips the event rather than sending a colliding seq) AND
// surfaces an explicit, persistent ok:false health state the menubar reads —
// not just a log line.
func TestSeqAllocatorFailureSetsHealth(t *testing.T) {
	health.SetDirForTest(filepath.Join(t.TempDir(), "health.d"))

	store, err := outbox.Open(filepath.Join(t.TempDir(), "seq.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	alloc := seqAllocator(store, "sp")

	// Works while the store is open; health stays ok.
	if _, err := alloc(); err != nil {
		t.Fatalf("first alloc: %v", err)
	}
	if !health.Load().OK {
		t.Fatal("health should be ok after a successful allocation")
	}

	// Close the store so AllocSeq fails (unwritable) → degraded health.
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := alloc(); err == nil {
		t.Fatal("expected AllocSeq to fail against a closed store")
	}
	s := health.Load()
	if s.OK {
		t.Fatal("AllocSeq failure must set health ok:false (N1)")
	}
	if s.Reason == "" {
		t.Fatal("degraded health must carry a reason the menubar can show")
	}
}

// TestOutboxOpenFailureSetsHealth pins the pre-launch health.d fix
// (v1-architecture.md §14): the local outbox/device_seq store failing to
// OPEN AT ALL — previously only ever logged to stderr, with no health signal
// — now also reports to health.d/outbox_open.json, so the menubar can
// surface it.
func TestOutboxOpenFailureSetsHealth(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	// health's dirFn is package-level test-only state that persists across
	// tests in this binary — point it explicitly at THIS test's HOME rather
	// than relying on the (possibly stale, from an earlier test) default.
	health.SetDirForTest(filepath.Join(home, ".config", "sitrep", "health.d"))

	// Force outbox.OpenWithMaxRows to fail: make the target DB path a
	// DIRECTORY instead of a file, so sqlite's open fails.
	outboxPath := config.RealtimeOutboxPath()
	if err := os.MkdirAll(outboxPath, 0o700); err != nil {
		t.Fatalf("seed a directory at the outbox path: %v", err)
	}

	if _, err := openSeqStore(config.Config{DeviceID: "dev-1"}); err == nil {
		t.Fatal("openSeqStore should fail against a path that is a directory")
	}

	s := health.Load()
	if s.OK {
		t.Fatal("outbox open failure must set health ok:false")
	}
	if s.Reason == "" {
		t.Fatal("degraded health must carry a reason the menubar can show")
	}
	if _, err := os.Stat(filepath.Join(health.Dir(), "outbox_open.json")); err != nil {
		t.Fatalf("expected health.d/outbox_open.json to exist: %v", err)
	}
}
