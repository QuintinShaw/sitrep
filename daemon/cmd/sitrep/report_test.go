package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/QuintinShaw/sitrep/daemon/internal/config"
	rtclient "github.com/QuintinShaw/sitrep/daemon/internal/realtime/client"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/outbox"
)

// TestReportPostsTaskEventToV1Events pins 5a's daemon half: `sitrep report`
// (the entry point the Claude Code hook now calls instead of raw HTTP) emits
// one task event through the unified uplink to POST /v1/events — with a real
// persistent device_seq allocated by the outbox, not a hand-rolled shell
// counter.
func TestReportPostsTaskEventToV1Events(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	var mu sync.Mutex
	var paths []string
	var taskEventSeen bool
	var gotTaskID, gotKind string
	var gotSeq int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		if r.URL.Path == "/v1/events" {
			var req struct {
				Events []struct {
					Type string          `json:"type"`
					Body json.RawMessage `json:"body"`
				} `json:"events"`
			}
			_ = json.Unmarshal(body, &req)
			for _, e := range req.Events {
				if e.Type == "task.event" {
					var b struct {
						TaskID    string `json:"task_id"`
						Kind      string `json:"kind"`
						DeviceSeq int64  `json:"device_seq"`
					}
					_ = json.Unmarshal(e.Body, &b)
					mu.Lock()
					taskEventSeen, gotTaskID, gotKind, gotSeq = true, b.TaskID, b.Kind, b.DeviceSeq
					mu.Unlock()
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"space_revision":1,"acked":[],"results":[]}`))
	}))
	defer srv.Close()

	if err := config.Save(config.Config{Server: srv.URL, Token: "tok", DeviceID: "device-1", Space: "sp"}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	cmdReport([]string{"--task", "cc-abc123", "--kind", "started", "--title", "Claude Code · repo"})

	mu.Lock()
	defer mu.Unlock()
	if !taskEventSeen {
		t.Fatalf("no task.event reached /v1/events; paths hit: %v", paths)
	}
	for _, p := range paths {
		if p != "/v1/events" {
			t.Fatalf("report hit path %q, want only /v1/events (no raw /v2)", p)
		}
	}
	if gotTaskID != "cc-abc123" || gotKind != "started" {
		t.Fatalf("task.event = {task_id:%q kind:%q}, want {cc-abc123 started}", gotTaskID, gotKind)
	}
	if gotSeq < 1 {
		t.Fatalf("device_seq = %d, want >= 1 (allocated by the persistent outbox, not reset)", gotSeq)
	}
}

// TestReportSurvivesNetworkBlip is the pre-launch repro (external review
// round 3): `sitrep report`'s HTTP delivery attempt fails outright (the
// server is unreachable — the "response is lost client-side" scenario, here
// reproduced as "the request never even reached the server") at the moment
// the hook reports. The event must NOT be silently dropped: it stays durable
// in the local outbox, and a LATER flush against a now-reachable server
// delivers it — proving durability does not depend on the reporting
// process's own network attempt succeeding.
func TestReportSurvivesNetworkBlip(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	// An address nothing is listening on: the POST /v1/events attempt inside
	// cmdReport's reportDurable -> rtclient.FlushOnce fails immediately
	// (connection refused), simulating a network blip / lost response.
	const unreachable = "http://127.0.0.1:1"
	if err := config.Save(config.Config{Server: unreachable, Token: "tok", DeviceID: "device-1", Space: "sp"}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	t.Logf("REPRO: reporting against unreachable server %s", unreachable)
	cmdReport([]string{"--task", "cc-blip", "--kind", "started", "--title", "Claude Code · repo"})

	// Durability check: open the SAME on-disk outbox directly and confirm
	// the event survived the failed delivery attempt — it must still be
	// sitting there, unacked, not silently dropped.
	store, err := outbox.Open(config.RealtimeOutboxPath())
	if err != nil {
		t.Fatalf("reopen outbox: %v", err)
	}
	defer store.Close()
	pending, err := store.Pending(context.Background(), "sp")
	if err != nil {
		t.Fatalf("read pending: %v", err)
	}
	t.Logf("REPRO: after the failed delivery attempt, outbox has %d pending item(s) for space \"sp\"", len(pending))
	if len(pending) != 1 {
		t.Fatalf("outbox has %d pending items after a network blip, want 1 (durably persisted despite the failed delivery attempt)", len(pending))
	}

	// --- Now the server comes back. A later flush (any process touching
	// this device's outbox) delivers the durably-stored event.
	var mu sync.Mutex
	var delivered bool
	var gotTaskID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Events []struct {
				Type string          `json:"type"`
				Body json.RawMessage `json:"body"`
			} `json:"events"`
		}
		_ = json.Unmarshal(body, &req)
		var acked []struct {
			DeviceID  string `json:"device_id"`
			DeviceSeq int64  `json:"device_seq"`
		}
		for _, e := range req.Events {
			if e.Type == "task.event" {
				var b struct {
					TaskID    string `json:"task_id"`
					DeviceID  string `json:"device_id"`
					DeviceSeq int64  `json:"device_seq"`
				}
				_ = json.Unmarshal(e.Body, &b)
				mu.Lock()
				delivered, gotTaskID = true, b.TaskID
				mu.Unlock()
				acked = append(acked, struct {
					DeviceID  string `json:"device_id"`
					DeviceSeq int64  `json:"device_seq"`
				}{b.DeviceID, b.DeviceSeq})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		resp, _ := json.Marshal(map[string]any{"space_revision": 1, "acked": acked, "results": []any{}})
		_, _ = w.Write(resp)
	}))
	defer srv.Close()

	t.Logf("REPRO: server now reachable at %s; flushing the durably-stored event", srv.URL)
	rtclient.FlushOnce(context.Background(), rtclient.Config{
		EventsURL: srv.URL + "/v1/events",
		Token:     "tok",
		DeviceID:  "device-1",
		Space:     "sp",
		Outbox:    store,
	})

	mu.Lock()
	gotDelivered, gotID := delivered, gotTaskID
	mu.Unlock()
	if !gotDelivered || gotID != "cc-blip" {
		t.Fatalf("REPRO FAILED: event never delivered after the server recovered (delivered=%v taskID=%q)", gotDelivered, gotID)
	}
	t.Log("REPRO: event delivered after recovery — durability held across the network blip")

	pendingAfter, err := store.Pending(context.Background(), "sp")
	if err != nil {
		t.Fatalf("read pending after flush: %v", err)
	}
	if len(pendingAfter) != 0 {
		t.Fatalf("outbox still has %d pending item(s) after a successful, acked flush", len(pendingAfter))
	}
}
