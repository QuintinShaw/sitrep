package uplink

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/protocol"
)

type capture struct {
	mu      sync.Mutex
	batches [][]Event
	auth    []string
}

func (c *capture) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var batch []Event
		if err := json.Unmarshal(body, &batch); err != nil {
			t.Errorf("bad batch JSON: %v", err)
		}
		c.mu.Lock()
		c.batches = append(c.batches, batch)
		c.auth = append(c.auth, r.Header.Get("Authorization"))
		c.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}
}

func (c *capture) all() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []Event
	for _, b := range c.batches {
		out = append(out, b...)
	}
	return out
}

func ev(kind protocol.Kind, mod func(*Event)) Event {
	e := Event{Event: protocol.Event{Kind: kind}, SourceID: "s1", TS: "2026-07-17T12:00:00Z"}
	if mod != nil {
		mod(&e)
	}
	return e
}

func TestCoalescingLastWinsAndDiscretePreserved(t *testing.T) {
	var c capture
	srv := httptest.NewServer(c.handler(t))
	defer srv.Close()

	u := New(Config{ServerURL: srv.URL, Token: "tok", FlushInterval: time.Hour})
	u.Offer(ev(protocol.TaskProgress, func(e *Event) { e.Percent = 10 }))
	u.Offer(ev(protocol.TaskProgress, func(e *Event) { e.Percent = 20 }))
	u.Offer(ev(protocol.TaskStep, func(e *Event) { e.Step = "later step" }))
	u.Offer(ev(protocol.MetricUpdate, func(e *Event) { e.Key = "k"; e.Value = "1" }))
	u.Offer(ev(protocol.MetricUpdate, func(e *Event) { e.Key = "k"; e.Value = "2" }))
	u.Offer(ev(protocol.MessageSend, func(e *Event) { e.Text = "boom"; e.Level = "info" }))
	u.Offer(ev(protocol.MessageSend, func(e *Event) { e.Text = "boom2"; e.Level = "info" }))
	u.Close()

	got := c.all()
	byKind := map[protocol.Kind][]Event{}
	for _, e := range got {
		byKind[e.Kind] = append(byKind[e.Kind], e)
	}
	// Both discrete events survive.
	if len(byKind[protocol.MessageSend]) != 2 {
		t.Fatalf("want 2 message.send, got %d", len(byKind[protocol.MessageSend]))
	}
	// Task updates coalesce to one, keeping latest percent + step.
	tasks := byKind[protocol.TaskProgress]
	if len(tasks) != 1 || tasks[0].Percent != 20 || tasks[0].Step != "later step" {
		t.Fatalf("bad coalesced task update: %+v", tasks)
	}
	// Metric coalesces to last value.
	metrics := byKind[protocol.MetricUpdate]
	if len(metrics) != 1 || metrics[0].Value != "2" {
		t.Fatalf("bad coalesced metric: %+v", metrics)
	}
	// Auth header present.
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, a := range c.auth {
		if a != "Bearer tok" {
			t.Fatalf("bad auth header %q", a)
		}
	}
}

func TestDiscreteEventTriggersImmediateFlush(t *testing.T) {
	var c capture
	srv := httptest.NewServer(c.handler(t))
	defer srv.Close()

	u := New(Config{ServerURL: srv.URL, FlushInterval: time.Hour})
	defer u.Close()
	u.Offer(ev(protocol.TaskStart, nil))

	deadline := time.After(2 * time.Second)
	for {
		if len(c.all()) == 1 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("task.start not flushed promptly")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestNilUplinkIsSafe(t *testing.T) {
	var u *Uplink
	u.Offer(ev(protocol.TaskStart, nil))
	u.Close()
}
