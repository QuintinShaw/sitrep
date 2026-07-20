package uplink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/protocol"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/outbox"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

// logPost is one observed POST /v1/tasks/:id/log request: the task id from
// the path and the {"lines":[...]} body (v1-architecture.md §1.2.2).
type logPost struct {
	taskID string
	lines  []string
}

// capturedEnvelope is one decoded event from a POST /v1/events request body,
// tagged by type with the relevant decoded body.
type capturedEnvelope struct {
	Type   string
	Task   *wire.TaskEventBody
	Msg    *wire.MessageEventBody
	Metric *wire.MetricFrameBody
}

// capture is a path-aware v1 mock: it serves POST /v1/events (recording the
// envelopes and returning an ACK with an optional piggybacked commands[])
// and POST /v1/tasks/:id/log (recording the {"lines":[...]} body). It
// deliberately understands no /v2 route — every request must be a /v1 one.
type capture struct {
	mu       sync.Mutex
	events   []capturedEnvelope
	logPosts []logPost
	auth     []string
	paths    []string
	forTasks []string // for_task_id sent on each /v1/events request
	revision int64

	// seenSeq tracks (device_id, device_seq) for reliable events; a repeat
	// pair sets dupSeen — modelling the server's never-pruned dedup, so a
	// test can prove two "processes" never collide on device_seq.
	seenSeq map[string]bool
	dupSeen bool

	// commandsToReturn is echoed on EVERY /v1/events response's commands[]
	// (not drained) so a test can assert the uplink's own command_id
	// idempotency: the server re-offering the same command_id must not make
	// the uplink dispatch it twice.
	commandsToReturn []pendingCommand

	// ackedCmds models CommandStore.pending_commands.delivered (P0-5,
	// fetch-then-ack): a command_id lands here only when a request's
	// ack_command_ids names it, and once here it is filtered out of every
	// future response's commands[] — exactly like the real server's
	// "delivered is terminal" rule (v1-architecture.md §1.4).
	ackedCmds map[string]bool
	// ackCalls records every ack_command_ids array this mock has seen, one
	// entry per /v1/events request (including empty ones), so a test can
	// assert exactly when (and how many times) a command_id was acked.
	ackCalls [][]string
}

func (c *capture) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		c.auth = append(c.auth, r.Header.Get("Authorization"))
		c.paths = append(c.paths, r.URL.Path)
		c.mu.Unlock()

		switch {
		case r.URL.Path == "/v1/events":
			c.serveEvents(t, w, body)
		case strings.HasPrefix(r.URL.Path, "/v1/tasks/") && strings.HasSuffix(r.URL.Path, "/log"):
			c.serveLog(t, w, r, body)
		default:
			t.Errorf("unexpected request path %q (no /v2 route should ever be hit)", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

func (c *capture) serveEvents(t *testing.T, w http.ResponseWriter, body []byte) {
	var req struct {
		Events []struct {
			Type string          `json:"type"`
			Body json.RawMessage `json:"body"`
		} `json:"events"`
		ForTaskID     string   `json:"for_task_id"`
		AckCommandIDs []string `json:"ack_command_ids"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		t.Errorf("bad /v1/events JSON: %v", err)
	}
	resp := v1EventsResponse{}
	c.mu.Lock()
	c.forTasks = append(c.forTasks, req.ForTaskID)
	c.ackCalls = append(c.ackCalls, req.AckCommandIDs)
	if c.ackedCmds == nil {
		c.ackedCmds = map[string]bool{}
	}
	// Acks are applied BEFORE this response's commands[] is computed
	// (v1-architecture.md §4.1) — mirrored below by filtering
	// commandsToReturn against ackedCmds after this loop.
	for _, id := range req.AckCommandIDs {
		c.ackedCmds[id] = true
	}
	if c.seenSeq == nil {
		c.seenSeq = map[string]bool{}
	}
	dedup := func(deviceID string, seq int64) {
		key := deviceID + ":" + strconvI(seq)
		if c.seenSeq[key] {
			c.dupSeen = true
		}
		c.seenSeq[key] = true
	}
	for _, e := range req.Events {
		ce := capturedEnvelope{Type: e.Type}
		switch e.Type {
		case wire.TypeTaskEvent:
			var b wire.TaskEventBody
			_ = json.Unmarshal(e.Body, &b)
			ce.Task = &b
			c.revision++
			dedup(b.DeviceID, b.DeviceSeq)
			resp.Acked = append(resp.Acked, v1AckedPair{DeviceID: b.DeviceID, DeviceSeq: b.DeviceSeq})
		case wire.TypeMessageEvent:
			var b wire.MessageEventBody
			_ = json.Unmarshal(e.Body, &b)
			ce.Msg = &b
			c.revision++
			dedup(b.DeviceID, b.DeviceSeq)
			resp.Acked = append(resp.Acked, v1AckedPair{DeviceID: b.DeviceID, DeviceSeq: b.DeviceSeq})
		case wire.TypeMetricFrame:
			var b wire.MetricFrameBody
			_ = json.Unmarshal(e.Body, &b)
			ce.Metric = &b
		default:
			t.Errorf("unexpected envelope type %q on /v1/events", e.Type)
		}
		c.events = append(c.events, ce)
	}
	resp.SpaceRevision = c.revision
	for _, cmd := range c.commandsToReturn {
		if !c.ackedCmds[cmd.CommandID] {
			resp.Commands = append(resp.Commands, cmd)
		}
	}
	c.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (c *capture) serveLog(t *testing.T, w http.ResponseWriter, r *http.Request, body []byte) {
	var parsed struct {
		Lines []string `json:"lines"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Errorf("bad task-log JSON: %v", err)
	}
	taskID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/tasks/"), "/log")
	c.mu.Lock()
	c.logPosts = append(c.logPosts, logPost{taskID: taskID, lines: parsed.Lines})
	c.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func (c *capture) allEvents() []capturedEnvelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]capturedEnvelope, len(c.events))
	copy(out, c.events)
	return out
}

// byType filters captured events to one envelope type.
func (c *capture) byType(typ string) []capturedEnvelope {
	var out []capturedEnvelope
	for _, e := range c.allEvents() {
		if e.Type == typ {
			out = append(out, e)
		}
	}
	return out
}

func (c *capture) logs() []logPost {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]logPost, len(c.logPosts))
	copy(out, c.logPosts)
	return out
}

func (c *capture) setCommands(cmds []pendingCommand) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commandsToReturn = cmds
}

func (c *capture) duplicateSeen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dupSeen
}

func (c *capture) forTaskIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.forTasks))
	copy(out, c.forTasks)
	return out
}

func strconvI(n int64) string {
	return strconv.FormatInt(n, 10)
}

// waitForCond polls cond until it returns true or the timeout elapses.
func waitForCond(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %v", timeout)
	}
}

func ev(kind protocol.Kind, mod func(*Event)) Event {
	e := Event{Event: protocol.Event{Kind: kind}, SourceID: "s1", TS: "2026-07-17T12:00:00Z"}
	if mod != nil {
		mod(&e)
	}
	return e
}

// v1Config builds a Config with a device_id/space so reliable events pass
// wire validation on the /v1/events path.
func v1Config(serverURL, token, commandSource string, flush time.Duration) Config {
	return Config{
		ServerURL:     serverURL,
		Token:         token,
		DeviceID:      "device-1",
		Space:         "space-1",
		CommandSource: commandSource,
		FlushInterval: flush,
	}
}

func TestCoalescingLastWinsAndDiscretePreserved(t *testing.T) {
	var c capture
	srv := httptest.NewServer(c.handler(t))
	defer srv.Close()

	u := New(v1Config(srv.URL, "tok", "", time.Hour))
	u.Offer(ev(protocol.TaskProgress, func(e *Event) { e.Percent = 10 }))
	u.Offer(ev(protocol.TaskProgress, func(e *Event) { e.Percent = 20 }))
	u.Offer(ev(protocol.TaskStep, func(e *Event) { e.Step = "later step" }))
	u.Offer(ev(protocol.MetricUpdate, func(e *Event) { e.Key = "k"; e.Value = "1" }))
	u.Offer(ev(protocol.MetricUpdate, func(e *Event) { e.Key = "k"; e.Value = "2" }))
	u.Offer(ev(protocol.MessageSend, func(e *Event) { e.Text = "boom"; e.Level = "info" }))
	u.Offer(ev(protocol.MessageSend, func(e *Event) { e.Text = "boom2"; e.Level = "info" }))
	u.Close()

	// Both discrete message events survive as message.event envelopes.
	if msgs := c.byType(wire.TypeMessageEvent); len(msgs) != 2 {
		t.Fatalf("want 2 message.event, got %d", len(msgs))
	}
	// Task progress/step coalesce to one task.event keeping latest percent + step.
	tasks := c.byType(wire.TypeTaskEvent)
	if len(tasks) != 1 || tasks[0].Task == nil || tasks[0].Task.Percent == nil || *tasks[0].Task.Percent != 20 || tasks[0].Task.Step != "later step" {
		t.Fatalf("bad coalesced task.event: %+v", tasks)
	}
	// Metric coalesces to last value inside one metric.frame.
	frames := c.byType(wire.TypeMetricFrame)
	if len(frames) != 1 || frames[0].Metric == nil || len(frames[0].Metric.Metrics) != 1 || frames[0].Metric.Metrics[0].Value != "2" {
		t.Fatalf("bad coalesced metric.frame: %+v", frames)
	}
	// device_seq is monotonic and unique across the reliable events.
	seen := map[int64]bool{}
	for _, e := range tasks {
		seen[e.Task.DeviceSeq] = true
	}
	for _, e := range c.byType(wire.TypeMessageEvent) {
		if seen[e.Msg.DeviceSeq] {
			t.Fatalf("device_seq %d reused across reliable events", e.Msg.DeviceSeq)
		}
		seen[e.Msg.DeviceSeq] = true
	}
	// Auth header present on every request.
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

	u := New(v1Config(srv.URL, "", "", time.Hour))
	defer u.Close()
	u.Offer(ev(protocol.TaskStart, nil))

	deadline := time.After(2 * time.Second)
	for {
		if tasks := c.byType(wire.TypeTaskEvent); len(tasks) == 1 && tasks[0].Task != nil && tasks[0].Task.Kind == "started" {
			return
		}
		select {
		case <-deadline:
			t.Fatal("task.start not flushed promptly to /v1/events")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestNilUplinkIsSafe(t *testing.T) {
	var u *Uplink
	u.Offer(ev(protocol.TaskStart, nil))
	u.Close()
}

// TestTelemetryPostsToV1Events pins that `sitrep run`'s non-realtime uplink
// posts its telemetry batch to POST /v1/events (never /v2/ingest) with the
// {"events":[...]} envelope shape, and that the request path is exactly
// /v1/events.
func TestTelemetryPostsToV1Events(t *testing.T) {
	var c capture
	srv := httptest.NewServer(c.handler(t))
	defer srv.Close()

	u := New(v1Config(srv.URL, "tok", "", 20*time.Millisecond))
	u.Offer(ev(protocol.TaskStart, func(e *Event) { e.Title = "t" }))
	waitForCond(t, 2*time.Second, func() bool { return len(c.byType(wire.TypeTaskEvent)) == 1 })
	u.Close()

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.paths) == 0 {
		t.Fatal("no request reached the server")
	}
	for _, p := range c.paths {
		if p != "/v1/events" {
			t.Fatalf("telemetry hit path %q, want /v1/events", p)
		}
	}
}

// TestTaskLogPostsToV1Route pins the frozen contract change: task.log
// passthrough lines are POSTed to POST /v1/tasks/:id/log with the
// {"lines":[...]} body (docs/api/v1/openapi.yaml; v1-architecture.md
// §1.2.2), NOT the telemetry event stream.
func TestTaskLogPostsToV1Route(t *testing.T) {
	var c capture
	srv := httptest.NewServer(c.handler(t))
	defer srv.Close()

	u := New(v1Config(srv.URL, "tok", "", 20*time.Millisecond))
	u.LogLine("build-release-3.2", "[12:00:01] linking libfoo.a")
	u.LogLine("build-release-3.2", "[12:00:02] linking libbar.a")

	waitForCond(t, 2*time.Second, func() bool { return len(c.logs()) > 0 })
	u.Close()

	posts := c.logs()
	var gotLines []string
	for _, p := range posts {
		if p.taskID != "build-release-3.2" {
			t.Fatalf("task.log POSTed to task id %q, want the source id %q as the path parameter", p.taskID, "build-release-3.2")
		}
		gotLines = append(gotLines, p.lines...)
	}
	if len(gotLines) != 2 || gotLines[0] != "[12:00:01] linking libfoo.a" || gotLines[1] != "[12:00:02] linking libbar.a" {
		t.Fatalf("log lines POSTed = %q, want the two lines in order as a {\"lines\":[...]} array", gotLines)
	}
	// task.log must NOT appear on the telemetry event stream at all.
	if evs := c.allEvents(); len(evs) != 0 {
		t.Fatalf("task.log leaked onto the /v1/events telemetry stream: %+v", evs)
	}
}

// TestReverseControlCommandsDispatchedWithIdempotency pins section 3's
// reverse-control channel: a command piggybacked on the POST /v1/events
// response is dispatched to Commands exactly once even when the server keeps
// re-offering the same command_id (idempotency), and only within its TTL
// window.
func TestReverseControlCommandsDispatchedWithIdempotency(t *testing.T) {
	var c capture
	srv := httptest.NewServer(c.handler(t))
	defer srv.Close()

	// A live, in-TTL pause command the server re-offers on every poll.
	c.setCommands([]pendingCommand{{
		CommandID: "cmd-pause-1",
		Origin:    "viewer",
		Action:    "pause",
		TaskID:    "s1",
		TTLMs:     60_000,
		OriginTS:  time.Now().UnixMilli(),
	}})

	// CommandSource set → reverse control on; short flush so idle heartbeats
	// poll quickly.
	u := New(v1Config(srv.URL, "tok", "s1", 20*time.Millisecond))
	defer u.Close()

	var got string
	select {
	case got = <-u.Commands:
	case <-time.After(3 * time.Second):
		t.Fatal("expected a pause command to be dispatched from the /v1/events response")
	}
	if got != "pause" {
		t.Fatalf("dispatched action = %q, want pause", got)
	}

	// The same command_id, re-offered every subsequent poll, must NOT be
	// dispatched again (command_id idempotency).
	select {
	case again := <-u.Commands:
		t.Fatalf("command re-dispatched (%q) despite same command_id — idempotency broken", again)
	case <-time.After(300 * time.Millisecond):
	}

	// It must also have been acked exactly once (fetch-then-ack, P0-5): the
	// mock's ackedCmds set only contains ids the uplink actually sent in
	// ack_command_ids.
	waitForCond(t, 2*time.Second, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.ackedCmds["cmd-pause-1"]
	})
}

// TestCommandNotAckedUntilLocalHandoffSucceeds is the P0-5 fetch-then-ack
// regression an external review live-reproduced against real workerd: the
// old code called markCommandSeen BEFORE attempting the u.Commands channel
// send, so a momentarily full/busy local channel silently and PERMANENTLY
// dropped the command — the server was never told, so it correctly kept
// resending it, but the daemon had already (wrongly) marked it seen and
// would never hand it off again.
//
// This test reproduces the exact scenario the review specified: (1) fill
// u.Commands so the local channel is full, (2) confirm the command is
// received on a poll but NOT included in that request's ack_command_ids,
// (3) confirm the server (this mock) — never acked — still returns the
// SAME command_id on the next poll, (4) drain the channel to free room,
// (5) confirm the command is then handed off successfully and acked, and
// (6) confirm it stops reappearing in subsequent polls once acked.
func TestCommandNotAckedUntilLocalHandoffSucceeds(t *testing.T) {
	var c capture
	srv := httptest.NewServer(c.handler(t))
	defer srv.Close()

	c.setCommands([]pendingCommand{{
		CommandID: "cmd-stop-full-chan",
		Origin:    "viewer",
		Action:    "stop",
		TaskID:    "s1",
		TTLMs:     60_000,
		OriginTS:  time.Now().UnixMilli(),
	}})

	u := New(v1Config(srv.URL, "tok", "s1", 20*time.Millisecond))
	defer u.Close()

	// Fill the local dispatch channel BEFORE any poll can drain it, so
	// dispatchCommands's send hits its `default:` (full/busy) branch —
	// simulating "the local process controller's handoff channel is
	// momentarily full."
	for i := 0; i < cap(u.Commands); i++ {
		u.Commands <- "filler"
	}

	// Give the poll loop several cycles to observe the command and hit the
	// full-channel branch. It must NOT be acked while the channel stays full.
	time.Sleep(150 * time.Millisecond)
	c.mu.Lock()
	acked := c.ackedCmds["cmd-stop-full-chan"]
	c.mu.Unlock()
	if acked {
		t.Fatal("command was acked despite the local channel being full — the handoff never actually happened")
	}
	// The server, having received no ack, must still be offering the same
	// command_id — i.e. it is still available for redelivery, not lost.
	c.mu.Lock()
	sawUnacked := false
	for _, calls := range c.ackCalls {
		for _, id := range calls {
			if id == "cmd-stop-full-chan" {
				sawUnacked = true
			}
		}
	}
	c.mu.Unlock()
	if sawUnacked {
		t.Fatal("ack_command_ids carried the command_id before the local handoff ever succeeded")
	}

	// Free up room in the channel (drain the fillers) so the next poll's
	// handoff can succeed.
	for i := 0; i < cap(u.Commands); i++ {
		<-u.Commands
	}

	// Now the command should be durably handed off...
	var got string
	select {
	case got = <-u.Commands:
	case <-time.After(3 * time.Second):
		t.Fatal("command was never dispatched after the local channel freed up — at-least-once redelivery did not recover it")
	}
	if got != "stop" {
		t.Fatalf("dispatched action = %q, want stop", got)
	}

	// ...and, having succeeded, acked on the next request.
	waitForCond(t, 2*time.Second, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.ackedCmds["cmd-stop-full-chan"]
	})

	// Once acked, the server (this mock, matching v1-architecture.md §1.4's
	// "delivered is terminal") must stop offering it — assert no further
	// dispatch happens even though the server keeps polling.
	select {
	case again := <-u.Commands:
		t.Fatalf("command re-dispatched (%q) after being acked — server should have stopped resending it", again)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestExpiredCommandNotDispatched pins that a command whose TTL window has
// elapsed (even allowing the ±30s skew) is never dispatched.
func TestExpiredCommandNotDispatched(t *testing.T) {
	var c capture
	srv := httptest.NewServer(c.handler(t))
	defer srv.Close()

	c.setCommands([]pendingCommand{{
		CommandID: "cmd-stale-1",
		Origin:    "viewer",
		Action:    "stop",
		TaskID:    "s1",
		TTLMs:     1_000,
		OriginTS:  time.Now().UnixMilli() - 10*60*1000, // 10 min ago
	}})

	u := New(v1Config(srv.URL, "tok", "s1", 20*time.Millisecond))
	defer u.Close()

	select {
	case got := <-u.Commands:
		t.Fatalf("dispatched an expired command %q, want none", got)
	case <-time.After(400 * time.Millisecond):
	}
}

// TestEmptyHeartbeatPullsPendingCommand pins that an idle source (nothing to
// report) still POSTs an empty {"events":[]} heartbeat and drains a pending
// command from the response — the v1 replacement for the legacy /v2/ingest
// reply-piggyback.
func TestEmptyHeartbeatPullsPendingCommand(t *testing.T) {
	var c capture
	srv := httptest.NewServer(c.handler(t))
	defer srv.Close()

	c.setCommands([]pendingCommand{{
		CommandID: "cmd-stop-hb",
		Origin:    "viewer",
		Action:    "stop",
		TaskID:    "s1",
		TTLMs:     60_000,
		OriginTS:  time.Now().UnixMilli(),
	}})

	// No events are ever Offer()ed: the only POSTs are the idle heartbeats
	// (every ~3 flush intervals) that exist purely to poll for commands.
	u := New(v1Config(srv.URL, "tok", "s1", 20*time.Millisecond))
	defer u.Close()

	select {
	case got := <-u.Commands:
		if got != "stop" {
			t.Fatalf("dispatched %q, want stop", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expected the idle heartbeat POST to pull the pending stop command")
	}

	// Every request must have been an empty {"events":[]} to /v1/events.
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.events) != 0 {
		t.Fatalf("heartbeat carried events, want none: %+v", c.events)
	}
	for _, p := range c.paths {
		if p != "/v1/events" {
			t.Fatalf("heartbeat hit path %q, want /v1/events", p)
		}
	}
}

// TestPersistentDeviceSeqAcrossProcesses is the BLOCKER-1 regression: two
// sequential `sitrep run` invocations from the same machine must produce
// strictly increasing, non-resetting device_seqs, so the server's never-
// pruned (device_id, device_seq) dedup never mistakes the second run's
// task.start for a duplicate and silently drops it. Both "processes" share
// the persisted seq state (one outbox file, reopened) exactly as two real
// processes on one machine would.
func TestPersistentDeviceSeqAcrossProcesses(t *testing.T) {
	var c capture
	srv := httptest.NewServer(c.handler(t))
	defer srv.Close()

	dbPath := filepath.Join(t.TempDir(), "seq.db")
	const space = "space-1"

	// Simulate one process: open the persistent seq store, run an uplink that
	// posts a task.start, then fully close everything (as a process exit
	// would).
	runProcess := func(taskID string) {
		store, err := outbox.Open(dbPath)
		if err != nil {
			t.Fatalf("open seq store: %v", err)
		}
		defer store.Close()
		u := New(Config{
			ServerURL:     srv.URL,
			Token:         "tok",
			DeviceID:      "device-1",
			Space:         space,
			FlushInterval: 15 * time.Millisecond,
			NextDeviceSeq: func() (int64, error) { return store.AllocSeq(context.Background(), space) },
		})
		u.Offer(ev(protocol.TaskStart, func(e *Event) { e.SourceID = taskID }))
		waitForCond(t, 2*time.Second, func() bool {
			for _, e := range c.byType(wire.TypeTaskEvent) {
				if e.Task != nil && e.Task.TaskID == taskID {
					return true
				}
			}
			return false
		})
		u.Close()
	}

	runProcess("run-1")
	runProcess("run-2")

	tasks := c.byType(wire.TypeTaskEvent)
	if len(tasks) != 2 {
		t.Fatalf("want 2 task.start on the server, got %d", len(tasks))
	}
	var seqs []int64
	for _, e := range tasks {
		seqs = append(seqs, e.Task.DeviceSeq)
	}
	if seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("device_seqs across two processes = %v, want [1 2] (strictly increasing, no reset)", seqs)
	}
	if c.duplicateSeen() {
		t.Fatal("server saw a duplicate (device_id, device_seq) — the second process reset its counter (BLOCKER 1)")
	}
}

// TestForTaskIDScopesReverseControl is the MAJOR-3 regression: a `sitrep
// run` sets for_task_id on its /v1/events POSTs, and the client-side
// backstop applies only its OWN task's pause/resume/stop — a command for a
// different concurrent task is never applied to this process's PID.
func TestForTaskIDScopesReverseControl(t *testing.T) {
	var c capture
	srv := httptest.NewServer(c.handler(t))
	defer srv.Close()

	// The server (a misbehaving/loose mock) returns commands for BOTH this
	// task and a different one; the backstop must drop the foreign one.
	now := time.Now().UnixMilli()
	c.setCommands([]pendingCommand{
		{CommandID: "cmd-foreign", Origin: "viewer", Action: "stop", TaskID: "task-OTHER", TTLMs: 60_000, OriginTS: now},
		{CommandID: "cmd-mine", Origin: "viewer", Action: "pause", TaskID: "task-A", TTLMs: 60_000, OriginTS: now},
	})

	u := New(Config{
		ServerURL:     srv.URL,
		Token:         "tok",
		DeviceID:      "device-1",
		Space:         "space-1",
		CommandSource: "task-A",
		ForTaskID:     "task-A",
		FlushInterval: 15 * time.Millisecond,
	})
	defer u.Close()

	// Only this task's command (pause) is dispatched.
	select {
	case got := <-u.Commands:
		if got != "pause" {
			t.Fatalf("dispatched %q, want pause (task-A's own command)", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expected task-A's pause command to be dispatched")
	}

	// The foreign task's stop must never be dispatched.
	select {
	case got := <-u.Commands:
		t.Fatalf("dispatched a foreign-task command %q — task scoping backstop failed (MAJOR 3)", got)
	case <-time.After(300 * time.Millisecond):
	}

	// Every /v1/events request carried for_task_id = this process's task.
	for _, ft := range c.forTaskIDs() {
		if ft != "task-A" {
			t.Fatalf("for_task_id on a request = %q, want task-A", ft)
		}
	}
}

// TestUplinkAuth401ReportsHealth pins the MINOR fix on the non-realtime
// (`sitrep run`) uplink: a persistent 401 on POST /v1/events (revoked/
// expired token) is surfaced through OnAuthState so the daemon routes it to
// the health file — a revoked device is visible, not silently failing. A
// later authenticated POST clears it.
func TestUplinkAuth401ReportsHealth(t *testing.T) {
	var unauthorized atomic.Bool
	unauthorized.Store(true)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if unauthorized.Load() {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"space_revision":1,"acked":[],"results":[]}`))
	}))
	defer srv.Close()

	var mu sync.Mutex
	var states []bool // OnAuthState ok values, in order
	cfg := v1Config(srv.URL, "tok", "", 15*time.Millisecond)
	cfg.OnAuthState = func(ok bool, reason string) {
		mu.Lock()
		states = append(states, ok)
		mu.Unlock()
		if !ok && reason == "" {
			t.Errorf("degraded OnAuthState must carry a reason")
		}
	}
	u := New(cfg)
	defer u.Close()

	u.Offer(ev(protocol.TaskStart, func(e *Event) { e.Title = "t" }))

	// Degraded reported.
	waitForCond(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(states) >= 1 && !states[0]
	})

	// Re-pair: the token authenticates again → recovery reported.
	unauthorized.Store(false)
	u.Offer(ev(protocol.TaskStart, func(e *Event) { e.Title = "t2" }))
	waitForCond(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(states) >= 2 && states[len(states)-1]
	})
}
