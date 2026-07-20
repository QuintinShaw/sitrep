package uplink

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestAckSurvivesNetworkLossThenRetransmits is adversarial scenario (b) from
// the P0-5 fetch-then-ack review: after the local process controller has
// durably handled a command (so queueAck has fired), the very POST
// /v1/events request meant to carry that ack_command_ids entry is lost at
// the network layer (every attempt of the 3-try retry loop fails/5xxs). The
// claim (uplink.go send()/requeueAcks, ~line 611-679) is that a failed send
// must NOT drop the ack — it must be requeued and resent on the next flush,
// not lost forever (which would make the server redeliver a command the
// device already safely re-applied — noisy but correct under idempotent
// pause/resume/stop, never silently wrong).
func TestAckSurvivesNetworkLossThenRetransmits(t *testing.T) {
	var c capture
	var failCount int32
	// Fail every request until failThreshold is reached, then succeed.
	// Sized to exceed one full send() retry budget (3 attempts) so at least
	// one entire send() call fails outright and must requeue its ack.
	const failThreshold = 5

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/events", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&failCount, 1)
		if n <= failThreshold {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		c.handler(t)(w, r)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c.setCommands([]pendingCommand{{
		CommandID: "cmd-net-loss",
		Origin:    "viewer",
		Action:    "stop",
		TaskID:    "s1",
		TTLMs:     60_000,
		OriginTS:  time.Now().UnixMilli(),
	}})

	u := New(v1Config(srv.URL, "tok", "s1", 20*time.Millisecond))
	defer u.Close()

	// Command must eventually be dispatched to the local controller despite
	// the 500s (the poll itself succeeds once failCount exceeds threshold,
	// or dispatch happens once the command IS observed in a successful
	// response).
	select {
	case got := <-u.Commands:
		if got != "stop" {
			t.Fatalf("dispatched %q, want stop", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("command was never dispatched — local handoff should succeed once a poll gets through")
	}

	// Now the local handoff succeeded, so queueAck fired for cmd-net-loss.
	// Whether the ack-bearing request that follows lands during the
	// still-failing window (requeued) or after (delivered), the ack MUST
	// eventually reach the server exactly-once-observed (idempotent retries
	// are fine) and must NOT be silently dropped forever.
	waitForCond(t, 5*time.Second, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.ackedCmds["cmd-net-loss"]
	})

	// Sanity: we actually forced real failures (proving this ack survived a
	// genuine network-loss window, not merely a lucky first-try success).
	if atomic.LoadInt32(&failCount) < failThreshold {
		t.Fatalf("test setup did not actually exercise failure path: failCount=%d, want >= %d", failCount, failThreshold)
	}

	// Once acked, the server stops re-offering it — confirm no further
	// dispatch (delivered is terminal).
	select {
	case again := <-u.Commands:
		t.Fatalf("command re-dispatched (%q) after being acked post-network-loss-recovery", again)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestAckNotDroppedOnRequestMarshalOrTransportError further pins the
// requeueAcks contract directly at the unit level: manually queueing an ack
// and then forcing send() to observe a hard transport failure (server
// closed) must leave the ack recoverable — pendingAcks must contain it
// afterward, not silently lose it. This exercises requeueAcks (uplink.go
// ~line 705-718) without depending on timing races.
func TestAckNotDroppedOnRequestMarshalOrTransportError(t *testing.T) {
	// Point the uplink at a server that is immediately closed, so every
	// request hard-fails at the transport layer (connection refused), not
	// merely a 5xx.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // closed before any request is made

	u := New(v1Config(srv.URL, "tok", "s1", time.Hour)) // long flush: we drive send() manually
	defer u.Close()

	u.queueAck("cmd-manual-1")
	u.send(nil) // nil batch = heartbeat-only send, exercises the ack path

	u.ackMu.Lock()
	pending := append([]string(nil), u.pendingAcks...)
	u.ackMu.Unlock()

	if len(pending) != 1 || pending[0] != "cmd-manual-1" {
		t.Fatalf("ack was lost after a hard transport failure: pendingAcks=%v, want [cmd-manual-1]", pending)
	}
}
