package client

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/outbox"
)

// TestE2ESourceLifecycle drives a real *daemon* client against a real,
// locally running server (see docs/design/realtime-integration.md for how
// to start one with `wrangler dev`). It is opt-in: it does nothing unless
// SITREP_E2E_URL, SITREP_E2E_TOKEN, SITREP_E2E_DEVICE_ID and
// SITREP_E2E_SPACE are all set, so `go test ./...` in CI/regression never
// touches the network.
//
// Coverage: hello offer/accept over a real WebSocket -> task.event send ->
// server ack (observed as the outbox item retiring) -> disconnect -> a
// second Client sharing the same outbox/device/space reconnects and
// replays the still-pending event -> ack again. This is the cross-line
// interoperability smoke test for the daemon "source" role against the
// real server implementation (as opposed to the in-process rttest fake
// server the rest of this package's tests use).
func TestE2ESourceLifecycle(t *testing.T) {
	url := os.Getenv("SITREP_E2E_URL")
	token := os.Getenv("SITREP_E2E_TOKEN")
	deviceID := os.Getenv("SITREP_E2E_DEVICE_ID")
	space := os.Getenv("SITREP_E2E_SPACE")
	if url == "" || token == "" || deviceID == "" || space == "" {
		t.Skip("set SITREP_E2E_URL, SITREP_E2E_TOKEN, SITREP_E2E_DEVICE_ID, SITREP_E2E_SPACE to run the real-server E2E smoke test")
	}

	dir := t.TempDir()
	store, err := outbox.Open(filepath.Join(dir, "outbox.db"))
	if err != nil {
		t.Fatalf("outbox.Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	mkClient := func() *Client {
		return New(Config{
			URL:            url,
			Token:          token,
			DeviceID:       deviceID,
			Space:          space,
			Outbox:         store,
			BackoffBase:    50 * time.Millisecond,
			BackoffMax:     500 * time.Millisecond,
			ResendInterval: 200 * time.Millisecond,
			HelloTimeout:   5 * time.Second,
			Logf:           t.Logf,
		})
	}

	c1 := mkClient()
	waitFor(t, 5*time.Second, c1.Connected)
	t.Log("e2e: hello offer/accept completed against real server (task 1)")

	taskID := "e2e-task-1"
	if err := c1.SendTaskEvent(TaskEvent{
		TaskID:     taskID,
		Kind:       "started",
		OccurredAt: time.Now(),
		Title:      "e2e smoke",
	}); err != nil {
		t.Fatalf("SendTaskEvent(started): %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		n, err := store.Count(context.Background())
		return err == nil && n == 0
	})
	if n, _ := store.Count(context.Background()); n != 0 {
		t.Fatalf("expected task.event started to be acked (outbox drained), %d still pending", n)
	}
	t.Log("e2e: task.event{started} acked by real server, outbox drained")

	c1.Close()

	// Enqueue a second event while fully disconnected: it must sit in the
	// outbox until the next connection's post-hello replay picks it up.
	percent := 50
	if err := c1.SendTaskEvent(TaskEvent{
		TaskID:     taskID,
		Kind:       "progress",
		OccurredAt: time.Now(),
		Percent:    &percent,
	}); err != nil {
		t.Fatalf("SendTaskEvent(progress) while offline: %v", err)
	}
	if n, _ := store.Count(context.Background()); n == 0 {
		t.Fatalf("expected the offline progress event to remain pending in the outbox")
	}

	c2 := mkClient()
	t.Cleanup(c2.Close)
	waitFor(t, 5*time.Second, c2.Connected)
	t.Log("e2e: reconnect hello offer/accept completed (task 1, replay)")

	waitFor(t, 5*time.Second, func() bool {
		n, err := store.Count(context.Background())
		return err == nil && n == 0
	})
	if n, _ := store.Count(context.Background()); n != 0 {
		t.Fatalf("expected replayed task.event{progress} to be acked after reconnect, %d still pending", n)
	}
	t.Log("e2e: task.event{progress} replayed after reconnect and acked, outbox drained")
}
