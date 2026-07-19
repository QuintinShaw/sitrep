package client

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/outbox"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/rttest"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

const testDeviceID = "test-device-01"
const testSpace = "space-test"

func newTestOutbox(t *testing.T) *outbox.Store {
	t.Helper()
	store, err := outbox.Open(filepath.Join(t.TempDir(), "outbox.db"))
	if err != nil {
		t.Fatalf("outbox.Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func newTestClient(t *testing.T, url string, store *outbox.Store, mutate func(*Config)) *Client {
	t.Helper()
	cfg := Config{
		URL:                     url,
		Token:                   "test-token",
		DeviceID:                testDeviceID,
		Space:                   testSpace,
		Outbox:                  store,
		BackoffBase:             5 * time.Millisecond,
		BackoffMax:              20 * time.Millisecond,
		BackoffJitter:           0,
		ResendInterval:          20 * time.Millisecond,
		MetricFlushInterval:     5 * time.Millisecond,
		MetricThrottledInterval: 300 * time.Millisecond,
		HelloTimeout:            2 * time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c := New(cfg)
	t.Cleanup(c.Close)
	return c
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %v", timeout)
	}
}

func TestClientHelloAndTaskEventAcked(t *testing.T) {
	srv := rttest.New(func(conn *rttest.Conn) {
		offer, err := conn.HelloAccept("sess-1", 1000)
		if err != nil {
			t.Errorf("HelloAccept: %v", err)
			return
		}
		if offer.Role != "source" {
			t.Errorf("offer.Role = %q, want source", offer.Role)
		}
		if offer.DeviceID != testDeviceID {
			t.Errorf("offer.DeviceID = %q, want %q", offer.DeviceID, testDeviceID)
		}
		for {
			env, err := conn.ReadEnvelope()
			if err == rttest.ErrPing {
				conn.WritePong()
				continue
			}
			if err != nil {
				return
			}
			if env.Type == wire.TypeTaskEvent {
				body, _ := wire.DecodeBody(env)
				tb := body.(wire.TaskEventBody)
				if err := conn.Ack(tb.DeviceID, tb.DeviceSeq); err != nil {
					return
				}
			}
		}
	})
	defer srv.Close()

	store := newTestOutbox(t)
	c := newTestClient(t, srv.URL(), store, nil)

	if err := c.SendTaskEvent(TaskEvent{
		TaskID:     "run-1",
		Kind:       "started",
		OccurredAt: time.Now(),
		Title:      "Nightly backup",
	}); err != nil {
		t.Fatalf("SendTaskEvent: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		pending, err := store.Pending(context.Background(), testSpace)
		return err == nil && len(pending) == 0
	})
}

// TestClientReconnectReplaysUnacked drops the first connection before
// acking a task.event and asserts the client resends the SAME device_seq
// (in a fresh envelope) on the next connection, per SPEC.md's Agent
// reconnect replay flow (section 5.4).
func TestClientReconnectReplaysUnacked(t *testing.T) {
	var connNum int32
	var firstEnvID, secondEnvID string
	var firstSeq, secondSeq int64

	srv := rttest.New(func(conn *rttest.Conn) {
		n := atomic.AddInt32(&connNum, 1)
		if _, err := conn.HelloAccept("sess", 1000); err != nil {
			t.Errorf("HelloAccept: %v", err)
			return
		}
		switch n {
		case 1:
			env, err := conn.ReadEnvelope()
			if err != nil {
				return
			}
			if env.Type != wire.TypeTaskEvent {
				t.Errorf("connection 1: expected task.event, got %s", env.Type)
				return
			}
			body, _ := wire.DecodeBody(env)
			tb := body.(wire.TaskEventBody)
			firstEnvID = env.ID
			firstSeq = tb.DeviceSeq
			// Deliberately do NOT ack; close so the client must reconnect.
			conn.Close("simulated drop")
		case 2:
			for {
				env, err := conn.ReadEnvelope()
				if err == rttest.ErrPing {
					conn.WritePong()
					continue
				}
				if err != nil {
					return
				}
				if env.Type == wire.TypeTaskEvent {
					body, _ := wire.DecodeBody(env)
					tb := body.(wire.TaskEventBody)
					secondEnvID = env.ID
					secondSeq = tb.DeviceSeq
					conn.Ack(tb.DeviceID, tb.DeviceSeq)
				}
			}
		}
	})
	defer srv.Close()

	store := newTestOutbox(t)
	c := newTestClient(t, srv.URL(), store, nil)

	if err := c.SendTaskEvent(TaskEvent{TaskID: "run-1", Kind: "started", OccurredAt: time.Now()}); err != nil {
		t.Fatalf("SendTaskEvent: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		pending, err := store.Pending(context.Background(), testSpace)
		return err == nil && len(pending) == 0
	})

	if firstSeq == 0 || secondSeq == 0 {
		t.Fatalf("did not observe both connections' task.event (first=%d second=%d)", firstSeq, secondSeq)
	}
	if firstSeq != secondSeq {
		t.Fatalf("device_seq changed across resend: first=%d second=%d, want identical (SPEC.md 5.4)", firstSeq, secondSeq)
	}
	if firstEnvID == secondEnvID {
		t.Fatalf("envelope id must be freshly generated on resend, got the same id %q both times", firstEnvID)
	}
}

// TestClientCommandThrottleAndResumeRate asserts a server-issued
// command{throttle} switches the metric batcher into its throttled cadence
// and command{resume_rate} switches it back (SPEC.md section 7). The exact
// merge/rate-limit *math* of the batcher (N samples -> 1 frame, the 500ms
// per-metric and overall-interval gates) is covered deterministically,
// without any wall-clock sleeping, by TestMetricBatcher* in metrics_test.go;
// this test's job is only to prove the command actually reaches and flips
// the batcher's state end-to-end over the wire, so it asserts on that state
// transition rather than timing frame arrivals (which would make the test
// flaky under scheduler jitter).
func TestClientCommandThrottleAndResumeRate(t *testing.T) {
	connReady := make(chan *rttest.Conn, 1)
	srv := rttest.New(func(conn *rttest.Conn) {
		if _, err := conn.HelloAccept("sess", 1000); err != nil {
			t.Errorf("HelloAccept: %v", err)
			return
		}
		connReady <- conn
		for {
			_, err := conn.ReadEnvelope()
			if err != nil && err != rttest.ErrPing && err != rttest.ErrPong {
				return
			}
			if err == rttest.ErrPing {
				conn.WritePong()
			}
		}
	})
	defer srv.Close()

	store := newTestOutbox(t)
	c := newTestClient(t, srv.URL(), store, nil)

	var conn *rttest.Conn
	select {
	case conn = <-connReady:
	case <-time.After(3 * time.Second):
		t.Fatal("server never observed a connection")
	}

	if c.MetricsThrottled() {
		t.Fatal("expected the client to start un-throttled")
	}

	if err := conn.SendCommand(wire.CommandBody{
		CommandID: "cmd-throttle-1", Origin: "server", Action: "throttle", TTLMs: 3600000,
	}); err != nil {
		t.Fatalf("SendCommand(throttle): %v", err)
	}
	waitFor(t, time.Second, c.MetricsThrottled)

	if err := conn.SendCommand(wire.CommandBody{
		CommandID: "cmd-resume-1", Origin: "server", Action: "resume_rate", TTLMs: 3600000,
	}); err != nil {
		t.Fatalf("SendCommand(resume_rate): %v", err)
	}
	waitFor(t, time.Second, func() bool { return !c.MetricsThrottled() })
}

// TestClientCommandTTLRejectsExpired asserts a command whose TTL window has
// already elapsed (even allowing the +/-30s clock-skew grace, SPEC.md
// section 8) is dropped without being executed.
func TestClientCommandTTLRejectsExpired(t *testing.T) {
	connReady := make(chan *rttest.Conn, 1)
	srv := rttest.New(func(conn *rttest.Conn) {
		if _, err := conn.HelloAccept("sess", 1000); err != nil {
			t.Errorf("HelloAccept: %v", err)
			return
		}
		connReady <- conn
		for {
			if _, err := conn.ReadEnvelope(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	var executed int32
	store := newTestOutbox(t)
	newTestClient(t, srv.URL(), store, func(cfg *Config) {
		cfg.OnCommand = func(CommandAction) { atomic.AddInt32(&executed, 1) }
	})

	var conn *rttest.Conn
	select {
	case conn = <-connReady:
	case <-time.After(3 * time.Second):
		t.Fatal("server never observed a connection")
	}

	// ts is far enough in the past that even the 30s skew allowance and a
	// tiny ttl_ms cannot make "now" fall inside the actionable window.
	expiredTS := rttest.NowMS() - 10*60*1000 // 10 minutes ago
	env, err := wire.NewEnvelope(wire.TypeCommand, "cmd-env-1", expiredTS, wire.CommandBody{
		CommandID: "cmd-pause-expired", Origin: "viewer", IssuedByDeviceID: "viewer-1",
		Action: "pause", TaskID: "run-1", TTLMs: 1000,
	})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if err := conn.WriteEnvelope(env); err != nil {
		t.Fatalf("WriteEnvelope: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if n := atomic.LoadInt32(&executed); n != 0 {
		t.Fatalf("OnCommand invoked %d times for an expired command, want 0", n)
	}
}

// TestClientCommandDedupeByID asserts the same command_id is never executed
// twice (SPEC.md section 8).
func TestClientCommandDedupeByID(t *testing.T) {
	connReady := make(chan *rttest.Conn, 1)
	srv := rttest.New(func(conn *rttest.Conn) {
		if _, err := conn.HelloAccept("sess", 1000); err != nil {
			t.Errorf("HelloAccept: %v", err)
			return
		}
		connReady <- conn
		for {
			if _, err := conn.ReadEnvelope(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	var executed int32
	store := newTestOutbox(t)
	newTestClient(t, srv.URL(), store, func(cfg *Config) {
		cfg.OnCommand = func(CommandAction) { atomic.AddInt32(&executed, 1) }
	})

	var conn *rttest.Conn
	select {
	case conn = <-connReady:
	case <-time.After(3 * time.Second):
		t.Fatal("server never observed a connection")
	}

	send := func() {
		body := wire.CommandBody{
			CommandID: "cmd-dupe-1", Origin: "viewer", IssuedByDeviceID: "viewer-1",
			Action: "pause", TaskID: "run-1", TTLMs: 3600000,
		}
		env, err := wire.NewEnvelope(wire.TypeCommand, rttestEnvID(), rttest.NowMS(), body)
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		if err := conn.WriteEnvelope(env); err != nil {
			t.Fatalf("WriteEnvelope: %v", err)
		}
	}
	send()
	send()
	time.Sleep(100 * time.Millisecond)

	if n := atomic.LoadInt32(&executed); n != 1 {
		t.Fatalf("OnCommand invoked %d times for a duplicated command_id, want exactly 1", n)
	}
}

var envCounter int64

func rttestEnvID() string {
	return fmt.Sprintf("env-%d", atomic.AddInt64(&envCounter, 1))
}

// TestClientStopsAfterFatalNonRetryableHello pins the give-up rule: a
// handshake rejection that is fatal AND not retryable (here
// version_unsupported, SPEC.md section 13) must stop the reconnect loop
// entirely instead of hammering the server forever.
func TestClientStopsAfterFatalNonRetryableHello(t *testing.T) {
	var connCount int32
	srv := rttest.New(func(conn *rttest.Conn) {
		atomic.AddInt32(&connCount, 1)
		if _, err := conn.ReadEnvelope(); err != nil {
			return // expected the hello offer
		}
		conn.SendError(wire.ErrorBody{
			Code:      wire.ErrVersionUnsupported,
			Message:   "no shared protocol version",
			Retryable: boolPtr(false),
			Fatal:     boolPtr(true),
		})
	})
	defer srv.Close()

	store := newTestOutbox(t)
	newTestClient(t, srv.URL(), store, nil)

	// Backoff base is 5ms/cap 20ms: if the client kept reconnecting, this
	// window would see dozens of connections.
	time.Sleep(300 * time.Millisecond)
	if n := atomic.LoadInt32(&connCount); n != 1 {
		t.Fatalf("server saw %d connections after a fatal non-retryable hello rejection, want exactly 1 (no reconnect loop)", n)
	}
}

// TestClientStopsWhenAcceptNamesUnofferedVersion pins the accept-side
// version check: SPEC.md section 9.2 requires the server to select from
// the intersection with the offer, so an accept naming a version the
// client never offered is a failed negotiation — treated like
// version_unsupported, and the reconnect loop stops.
func TestClientStopsWhenAcceptNamesUnofferedVersion(t *testing.T) {
	var connCount int32
	srv := rttest.New(func(conn *rttest.Conn) {
		atomic.AddInt32(&connCount, 1)
		if _, err := conn.ReadEnvelope(); err != nil {
			return // expected the hello offer
		}
		accept := wire.HelloAccept{
			Stage:               "accept",
			ProtocolVersion:     99, // never offered (client offers [1])
			SessionID:           "sess-bogus",
			HeartbeatIntervalMS: 1000,
		}
		env, err := wire.NewEnvelope(wire.TypeHello, rttestEnvID(), rttest.NowMS(), wire.HelloBody{Accept: &accept})
		if err != nil {
			t.Errorf("NewEnvelope: %v", err)
			return
		}
		_ = conn.WriteEnvelope(env)
		// Keep the connection open; the client must abandon it (and not
		// come back) on its own.
		for {
			if _, err := conn.ReadEnvelope(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	store := newTestOutbox(t)
	newTestClient(t, srv.URL(), store, nil)

	time.Sleep(300 * time.Millisecond)
	if n := atomic.LoadInt32(&connCount); n != 1 {
		t.Fatalf("server saw %d connections after an accept naming an unoffered version, want exactly 1 (no reconnect loop)", n)
	}
}
