// Package uplink batches parsed protocol events and ships them to the server.
//
// Coalescing policy (proto/SPEC.md "Rate limits"): continuous updates
// (task.progress / task.step / metric.update) are merged per coalescing key
// within each flush interval — last value wins. Discrete events (task.start,
// task.done, task.fail, message.send) are never dropped and trigger an
// immediate flush so lifecycle changes reach devices promptly.
package uplink

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	neturl "net/url"
	"sync"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/protocol"
	rtclient "github.com/QuintinShaw/sitrep/daemon/internal/realtime/client"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

// Event is the wire form of a protocol event (schema/event.schema.json).
type Event struct {
	protocol.Event
	SourceID string `json:"source_id"`
	TS       string `json:"ts"`
}

// NewSourceID returns a random 12-hex-char source identifier.
func NewSourceID() string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

type Config struct {
	ServerURL     string        // e.g. https://sitrep.example.com; empty = offline
	Token         string        // bearer token; empty = no auth header
	FlushInterval time.Duration // default 1s
	HTTPTimeout   time.Duration // default 10s

	// DeviceID and Space identify this source on the POST /v1/events uplink:
	// every reliable event body carries device_id (the server verifies it
	// matches the authenticated identity, v1-architecture.md §4) and
	// device_seq is scoped to (device_id, space). Empty DeviceID means this
	// uplink cannot send reliable events over /v1/events (their bodies would
	// fail validation) — a source must be provisioned with a device_id (via
	// `sitrep join`, which persists it) to uplink telemetry in v1.
	DeviceID string
	Space    string

	// NextDeviceSeq allocates the next PERSISTENT, strictly-monotonic
	// (device, space)-scoped device_seq for a reliable event. It MUST be
	// backed by durable, cross-process state (the realtime outbox's
	// space_seq_counters via outbox.Store.AllocSeq) — never a per-process
	// in-memory counter — because the server's (device_id, device_seq)
	// dedup is never pruned: a fresh process that restarted its counter at 1
	// would collide with an earlier process's events and have its own
	// silently dropped as duplicates. When nil, this uplink falls back to an
	// in-memory counter that resets every process; that fallback is for
	// tests and offline/unprovisioned cases only and must not be relied on
	// where two processes share a device_id.
	NextDeviceSeq func() (int64, error)

	// ForTaskID, when set, is sent as POST /v1/events `for_task_id`: the
	// server then scopes the response `commands[]` to just this task (plus
	// broadcasts), so a `sitrep run` process only ever drains its OWN task's
	// reverse-control commands (v1-architecture.md §4.1). It also gates the
	// client-side backstop in dispatchCommands. Empty for a general source
	// (e.g. the resident agent) with no task partitioning.
	ForTaskID string

	// CommandSource turns on reverse control for this uplink: idle
	// heartbeats are POSTed every ~3 flush intervals purely to poll for
	// pending commands, and any `commands` in a POST /v1/events response are
	// dispatched on Commands (deduped by command_id, TTL-checked). The
	// string value is retained as a stable source label; only its
	// emptiness ("" = reverse control off) is significant in v1 — the source
	// is identified to the server by its bearer token, not a ?sources=
	// query, so the old query-parameter form is gone.
	CommandSource string

	// Realtime is the opt-in realtime uplink (see internal/realtime/client
	// and its "use realtime uplink at all" config flag): when non-nil,
	// every reliable event (task lifecycle + message.send) and every
	// metric.update this Uplink is Offer()ed is routed there instead of
	// this package's own HTTP batch — the two paths are never used for the
	// same event, so nothing is double-written. Unlike an earlier version
	// of this package, there is no further fallback once Realtime is set:
	// the realtime Client itself is responsible for reliable delivery
	// across every transport degradation it can encounter (WS down/
	// reconnecting, or WS_TRANSPORT_ENABLED off — see that package's doc
	// comment), all against the one SpaceHub the event belongs to. This
	// package never re-routes a reliable event to a second store. task.log
	// passthrough output is unaffected: it has no realtime-protocol
	// equivalent and always continues over this package's own HTTP batch.
	// Nil (the default) preserves this package's behavior from before this
	// feature existed. The caller owns Realtime's lifecycle (construction
	// and Close).
	Realtime *rtclient.Client

	// Logf is a pluggable logger for conditions worth surfacing but not
	// worth failing on (e.g. a reliable event dropped because it could
	// never validate). Nil discards.
	Logf func(format string, args ...any)

	// OnAuthState surfaces a persistent HTTP 401 on the /v1/events uplink
	// (e.g. this device's token was revoked) so the daemon can route it to
	// the health-status file. Called only on a transition: (false, reason)
	// the first time a 401 is seen, (true, "") once a POST authenticates
	// again. nil is a no-op.
	OnAuthState func(ok bool, reason string)
}

type Uplink struct {
	cfg    Config
	client *http.Client

	// Commands receives reverse-control actions ("pause"|"resume"|"stop")
	// when cfg.CommandSource is set. Never closed; buffered.
	Commands chan string

	mu      sync.Mutex
	pending map[string]Event    // coalesced continuous updates, by key
	logs    map[string][]string // passthrough output tails, per source
	queue   []Event             // discrete events, order preserved
	closed  bool
	kick    chan struct{}
	done    chan struct{}

	// residualMu guards residual, the last-resort in-memory queue for a
	// reliable event (task.event/message.event) that could not be durably
	// persisted at all — not even into the outbox's overflow table — after
	// outbox.Store.Enqueue exhausted its own bounded internal retries (see
	// that method's doc comment). This is NOT the local-backpressure path:
	// an outbox at its row cap durably overflows instead of failing (see
	// outbox.ErrOverflowed), so it never reaches here. residual exists only
	// for the rare case of a genuinely failing local disk (e.g. the outbox
	// file's filesystem is full or read-only) — see sendReliable.
	residualMu sync.Mutex
	residual   []func() error

	// deviceSeq is the in-memory FALLBACK sequence counter used only when
	// Config.NextDeviceSeq is nil (tests / unprovisioned). Production wires a
	// persistent allocator (outbox.Store.AllocSeq) because this counter
	// resets to 0 every process — see Config.NextDeviceSeq. Touched only from
	// the single loop goroutine (via send), so it needs no lock.
	deviceSeq int64

	// seenCmds / seenCmdsOrder is the bounded command_id idempotency set for
	// reverse-control commands drained from POST /v1/events responses: a
	// command_id already acted on is never dispatched again (the same row a
	// WS source would receive as a `command` frame — v1-architecture.md §4.1
	// guarantees once-per-command delivery per transport, and this closes
	// the cross-transport double-apply window). Touched only from the loop
	// goroutine via send → dispatchCommands.
	seenCmds      map[string]bool
	seenCmdsOrder []string

	// authDegraded tracks whether a persistent 401 has been reported to
	// OnAuthState, so the hook fires only on transitions. Touched only from
	// the single loop goroutine (via send).
	authDegraded bool

	ticksSinceSend int

	// ackMu guards pendingAcks: command_ids durably handed off to the local
	// process controller (dispatchCommands's successful u.Commands <- send)
	// since the last time they were included on a POST /v1/events request's
	// ack_command_ids. Fetch-then-ack (v1-architecture.md §1.4/§4.1, P0-5):
	// a command_id is queued here ONLY after the local handoff genuinely
	// succeeded — never merely because it was received — and is sent on the
	// NEXT request (the current request's body is already built by the time
	// its own response is parsed and dispatchCommands runs). A send that
	// never reaches the server (network failure, or 5xx exhausting the
	// retry loop) puts its acks back via requeueAcks so they are not lost —
	// an ack is safe to send late or redundantly (server-side, acking is a
	// no-op-safe idempotent operation), but never safe to drop, since
	// dropping one would let the server keep a genuinely-applied command
	// marked undelivered forever.
	ackMu       sync.Mutex
	pendingAcks []string
}

// New starts the background flusher. Returns nil when cfg.ServerURL is empty
// (offline mode); all methods are nil-safe.
func New(cfg Config) *Uplink {
	if cfg.ServerURL == "" {
		return nil
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 10 * time.Second
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	u := &Uplink{
		cfg:      cfg,
		client:   &http.Client{Timeout: cfg.HTTPTimeout},
		Commands: make(chan string, 8),
		pending:  make(map[string]Event),
		logs:     make(map[string][]string),
		kick:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	go u.loop()
	return u
}

// Offer enqueues an event. Never blocks on the network.
func (u *Uplink) Offer(ev Event) {
	if u == nil {
		return
	}
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		return
	}
	realtime := u.cfg.Realtime
	u.mu.Unlock()

	// Feature flag: when a realtime uplink is configured, every kind with a
	// realtime-protocol equivalent is routed there instead of the HTTP
	// batch below, so the same event is never sent both ways. task.log (no
	// realtime equivalent) falls through unconditionally; a realtime send
	// failure never falls back either — see routeToRealtime/sendReliable.
	if realtime != nil && u.routeToRealtime(realtime, ev) {
		return
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return
	}
	switch ev.Kind {
	case protocol.TaskProgress, protocol.TaskStep:
		// One task per source: progress/step merge into a single pending slot,
		// preserving the latest percent even when a bare task.step follows.
		if prev, ok := u.pending["task:"+ev.SourceID]; ok {
			if ev.Kind == protocol.TaskStep && prev.Kind == protocol.TaskProgress {
				prev.Step, prev.TS = ev.Step, ev.TS
				ev = prev
			}
		}
		u.pending["task:"+ev.SourceID] = ev
	case protocol.MetricUpdate:
		u.pending["metric:"+ev.Key] = ev
	default: // task.start / task.done / task.fail / message.send
		u.queue = append(u.queue, ev)
		select {
		case u.kick <- struct{}{}:
		default:
		}
	}
}

// routeToRealtime translates ev into the matching realtime-protocol
// message and hands it to the realtime client. It reports whether it
// handled ev (true) so the caller skips the HTTP batch entirely for that
// event, or leaves it (false) to fall through unchanged — only because the
// kind has no realtime equivalent (task.log).
//
// Reliable events (task.event/message.event) NEVER fall back to this
// package's own HTTP batch while the realtime flag is on, even when a
// single send attempt fails: this package's HTTP batch has no notion of
// SpaceHub's revisioned state at all, so diverting so much as one terminal
// task.done/task.fail/message.send would let a viewer resuming from
// SpaceHub permanently miss it. See sendReliable for what happens to a send
// failure instead — the realtime Client's own outbox and transport
// degradation (WS or HTTP POST /v1/events, both the same SpaceHub) are the
// only path a reliable event ever takes once this flag is on.
func (u *Uplink) routeToRealtime(realtime *rtclient.Client, ev Event) bool {
	switch ev.Kind {
	case protocol.TaskStart, protocol.TaskProgress, protocol.TaskStep, protocol.TaskDone, protocol.TaskFail:
		te := rtclient.TaskEvent{
			TaskID:     ev.SourceID,
			Kind:       taskEventKind(ev.Kind),
			OccurredAt: parseEventTime(ev.TS),
			Display:    displayHints(ev),
		}
		switch ev.Kind {
		case protocol.TaskStart:
			te.Title = ev.Title
		case protocol.TaskProgress:
			p := ev.Percent
			te.Percent = &p
			te.Step = ev.Step
		case protocol.TaskStep:
			te.Step = ev.Step
		case protocol.TaskDone, protocol.TaskFail:
			te.Message = ev.Text
		}
		return u.sendReliable(func() error { return realtime.SendTaskEvent(te) })
	case protocol.MessageSend:
		me := rtclient.MessageEvent{
			Level:      ev.Level,
			Text:       ev.Text,
			OccurredAt: parseEventTime(ev.TS),
		}
		return u.sendReliable(func() error { return realtime.SendMessageEvent(me) })
	case protocol.MetricUpdate:
		realtime.SendMetric(wire.MetricSample{
			MetricID:   ev.Key,
			Value:      ev.Value,
			Label:      ev.Label,
			TS:         parseEventTime(ev.TS).UnixMilli(),
			Display:    displayHints(ev),
			Target:     ev.Target,
			Min:        ev.Min,
			Max:        ev.Max,
			AlertAbove: ev.AlertAbove,
			AlertBelow: ev.AlertBelow,
		})
		return true // metric.frame is fire-and-forget; there is no failure to react to
	default:
		return false // e.g. task.log: no realtime-protocol equivalent
	}
}

// sendReliable always reports "handled" (true) for a reliable event — the
// caller (routeToRealtime) must never fall back to HTTP for these, so there
// is nothing left for it to do except decide whether to hold it for a
// residual retry.
//
// send() is outbox.Store.Enqueue underneath (via rtclient.Client.Send*),
// which now persists synchronously: it durably writes the event into either
// the outbox or its overflow table before returning, retrying transient
// SQLite failures internally, bounded (see that method's doc comment). So
// by the time send() returns here, one of three things is true:
//
//   - success: the event is durable and (if the outbox had room) already
//     handed to the realtime client to deliver over whichever transport is
//     currently active;
//   - rtclient.ErrInvalidBody: the event can never validate, at any seq, on
//     any retry — logged and dropped, permanently;
//   - anything else: Enqueue's own bounded retries were exhausted — a
//     persistent local-disk failure. The event was NOT durably written
//     anywhere. This is the one case this function still holds the event in
//     memory (residual) and keeps retrying every flush tick, so it is not
//     silently dropped while the process lives. This residual window exists
//     ONLY under that persistent local-disk failure; ordinary outbox-full
//     backpressure durably overflows inside Enqueue and never reaches here.
func (u *Uplink) sendReliable(send func() error) bool {
	if err := send(); err != nil {
		if errors.Is(err, rtclient.ErrInvalidBody) {
			u.cfg.Logf("uplink: dropping reliable event, validation failed: %v", err)
			return true
		}
		u.cfg.Logf("uplink: reliable event failed to persist (%v); holding in memory and retrying every flush tick", err)
		u.residualMu.Lock()
		u.residual = append(u.residual, send)
		u.residualMu.Unlock()
		u.wakeFlush()
	}
	return true
}

// drainResidual re-attempts every event sendReliable could not persist at
// all, oldest first, stopping at the first one that still fails so order
// among residual entries is preserved. Called once per flush tick from
// loop. In the overwhelming majority of runs residual is always empty, so
// this is a cheap no-op check.
func (u *Uplink) drainResidual() {
	u.residualMu.Lock()
	defer u.residualMu.Unlock()
	for len(u.residual) > 0 {
		send := u.residual[0]
		if err := send(); err != nil {
			if errors.Is(err, rtclient.ErrInvalidBody) {
				u.cfg.Logf("uplink: dropping reliable event, validation failed on retry: %v", err)
				u.residual = u.residual[1:]
				continue
			}
			return // still failing to persist; try again next tick
		}
		u.residual = u.residual[1:]
	}
}

// wakeFlush nudges the flush loop to run sooner than the next tick, e.g.
// after queuing a reliable event for retry so a freed outbox row is picked
// up promptly rather than waiting a full FlushInterval.
func (u *Uplink) wakeFlush() {
	select {
	case u.kick <- struct{}{}:
	default:
	}
}

func taskEventKind(k protocol.Kind) string {
	switch k {
	case protocol.TaskStart:
		return "started"
	case protocol.TaskProgress:
		return "progress"
	case protocol.TaskStep:
		return "step"
	case protocol.TaskDone:
		return "done"
	case protocol.TaskFail:
		return "failed"
	default:
		return ""
	}
}

func displayHints(ev Event) *wire.DisplayHints {
	if ev.Icon == "" && ev.Tint == "" && ev.Template == "" {
		return nil
	}
	return &wire.DisplayHints{Icon: ev.Icon, Tint: ev.Tint, Template: ev.Template}
}

// parseEventTime parses the RFC3339 timestamp uplink.Event carries
// (protocol.go / runner.MakeEmitter always stamp one); a malformed or empty
// value falls back to now rather than producing an invalid envelope.
func parseEventTime(ts string) time.Time {
	if t, err := time.Parse(time.RFC3339, ts); err == nil {
		return t
	}
	return time.Now()
}

// LogLine buffers one passthrough output line for the task detail view.
// Batched into a single task.log event per flush; capped per window.
func (u *Uplink) LogLine(sourceID, line string) {
	if u == nil || line == "" {
		return
	}
	if len(line) > 300 {
		line = line[:300]
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return
	}
	buf := u.logs[sourceID]
	if len(buf) >= 50 {
		buf = buf[1:] // window overflow: keep the newest lines
	}
	u.logs[sourceID] = append(buf, line)
}

// Close flushes remaining events and stops the flusher.
func (u *Uplink) Close() {
	if u == nil {
		return
	}
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		return
	}
	u.closed = true
	u.mu.Unlock()
	select {
	case u.kick <- struct{}{}:
	default:
	}
	<-u.done
}

func (u *Uplink) loop() {
	defer close(u.done)
	ticker := time.NewTicker(u.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-u.kick:
		}
		u.drainResidual()
		batch, logs, closed := u.drain()
		// task.log is a best-effort, non-reliable log-tail append: it goes
		// to its own v1 route (POST /v1/tasks/:id/log), never the reliable
		// event/telemetry batch, and never through the outbox/device_seq/ACK
		// machinery. It carries no reverse-control commands, so it does not
		// reset the command heartbeat counter below.
		for taskID, lines := range logs {
			u.postTaskLog(taskID, lines)
		}
		if len(batch) > 0 {
			u.send(batch)
			u.ticksSinceSend = 0
		} else if u.cfg.CommandSource != "" {
			// Heartbeat every ~3 intervals so command latency stays bounded
			// even while the script is silent.
			u.ticksSinceSend++
			if u.ticksSinceSend >= 3 {
				u.send(nil)
				u.ticksSinceSend = 0
			}
		}
		if closed {
			return
		}
	}
}

// drain returns the reliable/telemetry batch (task lifecycle, metric,
// message — bound for the HTTP batch when realtime is off) and, separately,
// the accumulated task.log tails per source (bound for POST
// /v1/tasks/:id/log). The two are kept distinct because task.log is a
// best-effort log-tail append with its own frozen v1 route, not a telemetry
// event on the batch path (see postTaskLog).
func (u *Uplink) drain() (batch []Event, logs map[string][]string, closed bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	batch = append(batch, u.queue...)
	u.queue = u.queue[:0]
	for _, ev := range u.pending {
		batch = append(batch, ev)
	}
	clear(u.pending)
	if len(u.logs) > 0 {
		logs = u.logs
		u.logs = make(map[string][]string)
	}
	return batch, logs, u.closed
}

// postTaskLog best-effort appends log lines to a task's server-side ring
// buffer via POST /v1/tasks/:id/log (docs/api/v1/openapi.yaml;
// docs/design/v1-architecture.md §1.2.2) — the v1 successor to the legacy
// /v2/ingest task.log passthrough. Unlike a reliable event this is pure
// fire-and-forget: no outbox, no device_seq, no ACK — the server appends to
// a per-task 100-line ring buffer, and a dropped post is simply superseded
// by the next flush's tail (task.log telemetry is lossy by design). The
// request body is {"lines": [...]} and the source sr1 token authenticates
// it (source-only route, §3).
func (u *Uplink) postTaskLog(taskID string, lines []string) {
	if taskID == "" || len(lines) == 0 {
		return
	}
	body, err := json.Marshal(struct {
		Lines []string `json:"lines"`
	}{Lines: lines})
	if err != nil {
		return
	}
	url := u.cfg.ServerURL + "/v1/tasks/" + neturl.PathEscape(taskID) + "/log"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if u.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+u.cfg.Token)
	}
	resp, err := u.client.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// v1 POST /v1/events request/response shapes (docs/api/v1/openapi.yaml;
// v1-architecture.md §4.1). The envelope is proto/realtime/envelope.schema.json
// verbatim. This is the non-realtime, best-effort telemetry uplink (the one
// `sitrep run` uses when the realtime opt-in is off) — distinct from the
// outbox-backed realtime Client, which owns durable reliable delivery.
type v1Envelope struct {
	Type string          `json:"type"`
	ID   string          `json:"id"`
	TS   int64           `json:"ts"`
	Body json.RawMessage `json:"body"`
}

type v1EventsRequest struct {
	Events        []v1Envelope `json:"events"`
	ForTaskID     string       `json:"for_task_id,omitempty"`
	AckCommandIDs []string     `json:"ack_command_ids,omitempty"`
}

type v1AckedPair struct {
	DeviceID  string `json:"device_id"`
	DeviceSeq int64  `json:"device_seq"`
}

// pendingCommand mirrors #/components/schemas/PendingCommand: a
// reverse-control command drained from CommandStore for this HTTP source,
// shaped like the WS `command` envelope body.
type pendingCommand struct {
	CommandID string `json:"command_id"`
	Origin    string `json:"origin"`
	Action    string `json:"action"` // pause|resume|stop
	TaskID    string `json:"task_id"`
	TTLMs     int64  `json:"ttl_ms"`
	OriginTS  int64  `json:"origin_ts"`
}

type v1EventsResponse struct {
	SpaceRevision int64            `json:"space_revision"`
	Acked         []v1AckedPair    `json:"acked"`
	Commands      []pendingCommand `json:"commands"`
}

// send POSTs one telemetry batch to /v1/events and drains any piggybacked
// reverse-control commands from the response. Reliable events
// (task.event/message.event) are assigned a device_seq here; the request
// body is built ONCE and reused across the small retry loop, so a resend
// after a 5xx carries the same device_seqs — idempotent via the server's
// (device_id, device_seq) dedup, exactly as the realtime HTTP path relies
// on. On final failure the batch is dropped (this path is best-effort
// telemetry by design — the next update supersedes); durable cross-restart
// retirement is the opt-in realtime Client's job, not this one's.
//
// ack_command_ids is different: it is drained from pendingAcks (fetch-then-
// ack, P0-5) and, unlike the telemetry batch, is NEVER dropped on a failed
// send — see requeueAcks below. Losing an ack would let the server keep
// re-sending a command this device already durably applied forever (still
// correct, just noisy) but worse, it risks the ack never landing at all if
// this process exits before a later successful send, which is the exact
// failure fetch-then-ack exists to close.
//
// A nil batch is an empty {events:[]} heartbeat: it applies nothing but
// still pulls pending commands (the v1 replacement for the legacy
// /v2/ingest reply-piggyback).
func (u *Uplink) send(batch []Event) {
	envs := u.buildEnvelopes(batch)
	acks := u.drainPendingAcks()
	body, err := json.Marshal(v1EventsRequest{Events: envs, ForTaskID: u.cfg.ForTaskID, AckCommandIDs: acks})
	if err != nil {
		u.requeueAcks(acks)
		return
	}
	url := u.cfg.ServerURL + "/v1/events"
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			u.requeueAcks(acks)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if u.cfg.Token != "" {
			req.Header.Set("Authorization", "Bearer "+u.cfg.Token)
		}
		resp, err := u.client.Do(req)
		if err != nil {
			continue
		}
		if resp.StatusCode == http.StatusUnauthorized {
			// Persistent 401 (revoked/expired token): surface it to the health
			// signal so a revoked device is visible, not silently failing. The
			// acks never reached the server in an applied request — requeue
			// them for a later, authenticated send.
			u.reportAuth(false, "telemetry credential rejected (401) — device may be revoked")
			resp.Body.Close()
			u.requeueAcks(acks)
			return // retrying a 401 won't help
		}
		if resp.StatusCode < 500 {
			u.reportAuth(true, "")
			u.handleEventsResponse(resp)
			resp.Body.Close()
			return // success, or a 4xx retrying won't fix — either way the server parsed this body's ack_command_ids
		}
		resp.Body.Close()
	}
	// Every attempt failed to reach the server (or kept 5xx-ing): the batch
	// is dropped (best-effort telemetry, as before), but the acks must
	// survive to the next send — see the doc comment above.
	u.requeueAcks(acks)
}

// drainPendingAcks returns every command_id queued for ack since the last
// call, clearing the queue. Safe to call with an empty queue (returns nil).
func (u *Uplink) drainPendingAcks() []string {
	u.ackMu.Lock()
	defer u.ackMu.Unlock()
	if len(u.pendingAcks) == 0 {
		return nil
	}
	acks := u.pendingAcks
	u.pendingAcks = nil
	return acks
}

// queueAck records that command_id was durably handed off to the local
// process controller and should be included in the NEXT POST /v1/events
// request's ack_command_ids (v1-architecture.md §1.4/§4.1, P0-5). Must only
// be called after that handoff genuinely succeeded — see dispatchCommands.
func (u *Uplink) queueAck(commandID string) {
	u.ackMu.Lock()
	u.pendingAcks = append(u.pendingAcks, commandID)
	u.ackMu.Unlock()
}

// requeueAcks puts acks (drained by an earlier drainPendingAcks call for a
// send that did not durably reach the server) back onto the pending queue,
// merged ahead of anything queued in the meantime — order has no
// correctness impact (acking is idempotent and order-independent) but
// putting the older batch first keeps behavior predictable. A no-op for an
// empty/nil acks.
func (u *Uplink) requeueAcks(acks []string) {
	if len(acks) == 0 {
		return
	}
	u.ackMu.Lock()
	u.pendingAcks = append(acks, u.pendingAcks...)
	u.ackMu.Unlock()
}

// reportAuth fires OnAuthState only on a transition in/out of the degraded
// (persistent 401) state.
func (u *Uplink) reportAuth(ok bool, reason string) {
	if ok == !u.authDegraded {
		return // no change
	}
	u.authDegraded = !ok
	if u.cfg.OnAuthState != nil {
		u.cfg.OnAuthState(ok, reason)
	}
}

// buildEnvelopes converts a coalesced telemetry batch into v1 event
// envelopes: task lifecycle/progress → task.event, message.send →
// message.event (both reliable, each assigned the next device_seq), and all
// metric.update samples merged into a single best-effort metric.frame (no
// device_seq). task.log is not here — it left this path for POST
// /v1/tasks/:id/log. An event whose body fails wire validation (e.g. no
// device_id configured) is skipped and logged rather than aborting the
// batch.
func (u *Uplink) buildEnvelopes(batch []Event) []v1Envelope {
	var envs []v1Envelope
	var metrics []wire.MetricSample
	for _, ev := range batch {
		switch ev.Kind {
		case protocol.TaskStart, protocol.TaskProgress, protocol.TaskStep, protocol.TaskDone, protocol.TaskFail:
			b := wire.TaskEventBody{
				DeviceID:   u.cfg.DeviceID,
				DeviceSeq:  u.nextDeviceSeq(),
				TaskID:     ev.SourceID,
				Kind:       taskEventKind(ev.Kind),
				OccurredAt: parseEventTime(ev.TS).UnixMilli(),
				Display:    displayHints(ev),
			}
			switch ev.Kind {
			case protocol.TaskStart:
				b.Title = ev.Title
			case protocol.TaskProgress:
				p := ev.Percent
				b.Percent = &p
				b.Step = ev.Step
			case protocol.TaskStep:
				b.Step = ev.Step
			case protocol.TaskDone, protocol.TaskFail:
				b.Message = ev.Text
			}
			if env, ok := u.envelopeFor(wire.TypeTaskEvent, b, b.Validate); ok {
				envs = append(envs, env)
			}
		case protocol.MessageSend:
			b := wire.MessageEventBody{
				DeviceID:   u.cfg.DeviceID,
				DeviceSeq:  u.nextDeviceSeq(),
				MessageID:  newMessageID(),
				Level:      ev.Level,
				Text:       ev.Text,
				OccurredAt: parseEventTime(ev.TS).UnixMilli(),
			}
			if env, ok := u.envelopeFor(wire.TypeMessageEvent, b, b.Validate); ok {
				envs = append(envs, env)
			}
		case protocol.MetricUpdate:
			metrics = append(metrics, wire.MetricSample{
				MetricID:   ev.Key,
				Value:      ev.Value,
				Label:      ev.Label,
				TS:         parseEventTime(ev.TS).UnixMilli(),
				Display:    displayHints(ev),
				Target:     ev.Target,
				Min:        ev.Min,
				Max:        ev.Max,
				AlertAbove: ev.AlertAbove,
				AlertBelow: ev.AlertBelow,
			})
		}
	}
	if len(metrics) > 0 {
		mb := wire.MetricFrameBody{DeviceID: u.cfg.DeviceID, Metrics: metrics}
		if env, ok := u.envelopeFor(wire.TypeMetricFrame, mb, mb.Validate); ok {
			envs = append(envs, env)
		}
	}
	return envs
}

// envelopeFor validates body, then wraps it in a v1 envelope. A validation
// failure is logged and reported as (_, false) so the caller skips it.
func (u *Uplink) envelopeFor(typ string, body any, validate func() error) (v1Envelope, bool) {
	if err := validate(); err != nil {
		u.cfg.Logf("uplink: dropping invalid %s for /v1/events: %v", typ, err)
		return v1Envelope{}, false
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return v1Envelope{}, false
	}
	return v1Envelope{Type: typ, ID: newEnvelopeID(), TS: time.Now().UnixMilli(), Body: raw}, true
}

// nextDeviceSeq returns the next reliable-event device_seq — from the
// persistent allocator when configured (the production path; monotonic
// across processes), else the in-memory fallback. On an allocator error it
// returns 0, which fails wire validation (device_seq must be >= 1) so the
// event is skipped rather than sent with a possibly-colliding number.
func (u *Uplink) nextDeviceSeq() int64 {
	if u.cfg.NextDeviceSeq != nil {
		seq, err := u.cfg.NextDeviceSeq()
		if err != nil {
			u.cfg.Logf("uplink: allocate persistent device_seq: %v", err)
			return 0
		}
		return seq
	}
	u.deviceSeq++
	return u.deviceSeq
}

// handleEventsResponse parses the /v1/events ACK and dispatches any
// piggybacked reverse-control commands. `acked`/`space_revision` are
// informational for this best-effort path (there is no persistent resend
// queue to retire from — server-side (device_id, device_seq) dedup provides
// the idempotency the in-call retry relies on).
func (u *Uplink) handleEventsResponse(resp *http.Response) {
	var parsed v1EventsResponse
	if json.NewDecoder(resp.Body).Decode(&parsed) != nil {
		return
	}
	u.dispatchCommands(parsed.Commands)
}

// dispatchCommands forwards each not-yet-seen, unexpired reverse-control
// command to u.Commands as an action string, applying command_id
// idempotency (never act on the same command_id twice, even if it also
// arrived over a brief WS connection) and the TTL window (measured from
// origin_ts, with the same ±30s clock-skew allowance the realtime command
// path uses). Only meaningful when reverse control is enabled
// (CommandSource set); the actual SIGSTOP/SIGCONT/SIGTERM dispatch happens
// in the Commands consumer (cmd/sitrep `run`).
//
// Fetch-then-ack (v1-architecture.md §1.4/§4.1, P0-5): inclusion in a
// response is NOT delivery. A command is marked seen (local idempotency)
// and queued for ack ONLY after it is durably handed off to u.Commands —
// i.e. the channel send below actually succeeds. If the local channel is
// momentarily full/busy (the consumer goroutine hasn't started yet, or is
// stalled), the command is neither marked seen nor acked: it is simply left
// for the next poll, where the server — which never saw an ack — includes
// it again, and this same handoff is retried. This closes the P0 data-loss
// bug an external review live-reproduced against real workerd: the old code
// called markCommandSeen BEFORE attempting the channel send, so a full
// channel silently and PERMANENTLY dropped the command — never retried,
// even though nothing ever actually ran it.
func (u *Uplink) dispatchCommands(commands []pendingCommand) {
	if u.cfg.CommandSource == "" || len(commands) == 0 {
		return
	}
	const skewMS = 30_000
	nowMS := time.Now().UnixMilli()
	for _, cmd := range commands {
		if cmd.CommandID == "" || u.commandSeen(cmd.CommandID) {
			continue
		}
		// Task-scope backstop (MAJOR fix): the server's for_task_id filter is
		// the primary guarantee, but never apply a command to this process's
		// PID unless its task_id matches the task this process owns. A command
		// for a different concurrent task is dropped, not misapplied.
		if u.cfg.ForTaskID != "" && cmd.TaskID != u.cfg.ForTaskID {
			continue
		}
		switch cmd.Action {
		case "pause", "resume", "stop":
		default:
			continue // unknown action; ignore
		}
		ttl := cmd.TTLMs
		if ttl <= 0 {
			ttl = 60_000
		}
		if nowMS < cmd.OriginTS-skewMS || nowMS > cmd.OriginTS+ttl+skewMS {
			continue // outside the actionable window
		}
		select {
		case u.Commands <- cmd.Action:
			// Durably handed off — now, and only now, is it safe to mark
			// seen and ack.
			u.markCommandSeen(cmd.CommandID)
			u.queueAck(cmd.CommandID)
		default:
			// Consumer stalled/not yet listening: do NOT mark seen, do NOT
			// ack. delivered stays 0 server-side, so this command_id is
			// re-included on the next poll and the handoff is retried then —
			// at-least-once, safe because pause/resume/stop are idempotent.
		}
	}
}

func (u *Uplink) commandSeen(id string) bool { return u.seenCmds[id] }

func (u *Uplink) markCommandSeen(id string) {
	const cap = 512
	if u.seenCmds == nil {
		u.seenCmds = make(map[string]bool)
	}
	u.seenCmds[id] = true
	u.seenCmdsOrder = append(u.seenCmdsOrder, id)
	if len(u.seenCmdsOrder) > cap {
		drop := u.seenCmdsOrder[0]
		u.seenCmdsOrder = u.seenCmdsOrder[1:]
		delete(u.seenCmds, drop)
	}
}

// newEnvelopeID / newMessageID mint opaque ids matching the envelope_id /
// message_id grammars for the /v1/events bodies this uplink builds.
func newEnvelopeID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("rt%d", time.Now().UnixNano())
	}
	return "rt" + hex.EncodeToString(b)
}

func newMessageID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("msg%d", time.Now().UnixNano())
	}
	return "msg_" + hex.EncodeToString(b)
}
