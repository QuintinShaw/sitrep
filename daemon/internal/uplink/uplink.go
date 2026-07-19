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
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
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

	// CommandSource enables reverse control for this source id: flushes
	// (including empty heartbeats every 3 ticks) carry ?sources= and any
	// commands in the response are delivered on Commands.
	CommandSource string

	// Realtime is an opt-in feature flag: when non-nil, every reliable
	// event (task lifecycle + message.send) and every metric.update this
	// Uplink is Offer()ed is routed to the realtime WebSocket uplink
	// (internal/realtime/client) instead of the HTTP /v2/ingest batch —
	// the two paths are never used for the same event, so nothing is
	// double-written. task.log passthrough output is unaffected: it has
	// no realtime-protocol equivalent and always continues over HTTP.
	// Nil (the default) preserves this package's exact prior behavior.
	// The caller owns Realtime's lifecycle (construction and Close).
	Realtime *rtclient.Client

	// Logf is a pluggable logger for conditions worth surfacing but not
	// worth failing on (e.g. a reliable event dropped because it could
	// never validate). Nil discards.
	Logf func(format string, args ...any)
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

	// legacyMu guards legacyMode: whether the server has rejected the
	// realtime dial (rtclient.ErrRealtimeDisabled) and this Uplink has
	// switched the whole session to routing reliable events over the
	// legacy /v2 HTTP path instead (see pollRealtimeMode).
	legacyMu   sync.Mutex
	legacyMode bool

	ticksSinceSend int
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

	// Feature flag: when a realtime uplink is configured AND the server
	// hasn't rejected it (see the truth table on pollRealtimeMode), every
	// kind with a realtime-protocol equivalent is routed there instead of
	// the HTTP batch below, so the same event is never sent both ways.
	// task.log (no realtime equivalent) and any realtime send failure fall
	// through.
	if realtime != nil && !u.inLegacyMode() && u.routeToRealtime(realtime, ev) {
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
// Reliable events (task.event/message.event) NEVER fall back to the legacy
// /v2 HTTP path while the realtime flag is on AND the server accepts
// realtime, even when a single send attempt fails: /v2 ingest writes
// UserStore, while /v3 viewers resume from SpaceHub, so diverting so much as
// one terminal task.done/task.fail/message.send would let a viewer
// permanently miss it. See sendReliable for what happens to a send failure
// instead. (The one deliberate exception to "never falls back" is the
// server-disabled whole-session legacy mode this Uplink switches into via
// pollRealtimeMode — see that function's truth table — which routeToRealtime
// is not even called under, per Offer's inLegacyMode guard.)
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
//     handed to the realtime connection to deliver;
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

// inLegacyMode reports whether this Uplink is currently routing reliable
// events over the legacy /v2 HTTP path because the server rejected the
// realtime dial (see pollRealtimeMode).
func (u *Uplink) inLegacyMode() bool {
	u.legacyMu.Lock()
	defer u.legacyMu.Unlock()
	return u.legacyMode
}

// pollRealtimeMode is called once per flush tick from loop. It is the
// daemon-side half of the server-capability truth table:
//
//	local SITREP_REALTIME | server /v3/realtime          | routing
//	-----------------------|----------------------------|--------------------
//	off                    | (never dialed)             | legacy /v2, always
//	on                     | accepts (normal)           | realtime
//	on                     | 403 realtime_disabled       | legacy /v2, whole
//	                       |                             | session, until the
//	                       |                             | client's own long-
//	                       |                             | interval re-probe
//	                       |                             | (see rtclient's
//	                       |                             | serverDisabledRecheckInterval)
//	                       |                             | succeeds again
//
// The local flag is enforced upstream: newAgentRealtimeUplink (cmd/sitrep)
// never even constructs an rtclient.Client, let alone dials, when
// SITREP_REALTIME is off, so cfg.Realtime is nil and this function is a
// no-op (Offer's nil check on cfg.Realtime already covers that row).
//
// The remaining two rows are what this function drives: rtclient.Client
// dials in the background on its own reconnect loop and exposes
// ServerDisabled() as the result of its last dial attempt. When that flips
// true, this Uplink switches its whole session to legacy routing (not just
// the one event that happened to be in flight) and drains every event
// already sitting durably in the outbox/overflow tables to /v2, in FIFO
// order, so a terminal task state that arrived before the flag flipped
// doesn't sit forever in a store no viewer reads. When it flips back to
// false (the client's periodic re-probe succeeded), new events resume
// routing to realtime immediately — there is no backlog to replay for
// those, since legacy mode drained everything as it went.
func (u *Uplink) pollRealtimeMode() {
	rt := u.cfg.Realtime
	if rt == nil {
		return
	}
	disabled := rt.ServerDisabled()

	u.legacyMu.Lock()
	wasLegacy := u.legacyMode
	u.legacyMode = disabled
	u.legacyMu.Unlock()

	if !disabled {
		if wasLegacy {
			u.cfg.Logf("uplink: realtime accepted again; resuming realtime routing for new events")
		}
		return
	}
	if !wasLegacy {
		u.cfg.Logf("uplink: server rejected realtime (403 realtime_disabled); switching this session to legacy /v2 routing until it re-enables")
	}
	u.drainOutboxToLegacy(rt)
}

// drainOutboxToLegacy walks every reliable event durably sitting in rt's
// outbox (real device_seq, ready to deliver) and then its overflow table (no
// device_seq yet), oldest first in each, translating each one back into the
// legacy uplink.Event shape and posting it to /v2/ingest. An item is only
// retired (Ack for outbox items, DeleteOverflow for overflow items) once its
// /v2 send actually succeeds; the first failure stops the whole drain for
// this tick; the next flush tick resumes exactly where it left off, because
// nothing was retired.
//
// Acking an outbox item may itself promote an overflow row into the outbox
// (outbox.Store.Ack's own trigger) — the outer loop re-reads Pending() every
// iteration rather than caching the initial list, so a promoted row is
// picked up and drained in the same pass instead of waiting for the
// overflow-scanning loop below.
func (u *Uplink) drainOutboxToLegacy(rt *rtclient.Client) {
	store := rt.Outbox()
	space := rt.Space()
	if store == nil {
		return
	}
	ctx := context.Background()

	for {
		pending, err := store.Pending(ctx, space)
		if err != nil {
			u.cfg.Logf("uplink: legacy drain: read outbox: %v", err)
			return
		}
		if len(pending) == 0 {
			break
		}
		item := pending[0]
		ev, ok := legacyEventFromOutboxItem(item.Kind, item.Body)
		if !ok {
			u.cfg.Logf("uplink: legacy drain: dropping outbox item with unrecognized kind %q", item.Kind)
			if err := store.Ack(ctx, space, item.DeviceSeq); err != nil {
				u.cfg.Logf("uplink: legacy drain: ack seq %d: %v", item.DeviceSeq, err)
				return
			}
			continue
		}
		if !u.sendLegacySync(ev) {
			return // /v2 unreachable right now; resume on the next tick
		}
		if err := store.Ack(ctx, space, item.DeviceSeq); err != nil {
			u.cfg.Logf("uplink: legacy drain: ack seq %d: %v", item.DeviceSeq, err)
			return
		}
	}

	for {
		overflow, err := store.OverflowPending(ctx, space)
		if err != nil {
			u.cfg.Logf("uplink: legacy drain: read overflow: %v", err)
			return
		}
		if len(overflow) == 0 {
			return
		}
		ov := overflow[0]
		ev, ok := legacyEventFromOutboxItem(ov.Kind, ov.Body)
		if !ok {
			u.cfg.Logf("uplink: legacy drain: dropping overflow item with unrecognized kind %q", ov.Kind)
			if err := store.DeleteOverflow(ctx, ov.ID); err != nil {
				u.cfg.Logf("uplink: legacy drain: delete overflow id %d: %v", ov.ID, err)
				return
			}
			continue
		}
		if !u.sendLegacySync(ev) {
			return
		}
		if err := store.DeleteOverflow(ctx, ov.ID); err != nil {
			u.cfg.Logf("uplink: legacy drain: delete overflow id %d: %v", ov.ID, err)
			return
		}
	}
}

// sendLegacySync posts a single event to /v2/ingest synchronously and
// reports whether the server accepted it (2xx). Unlike send (the normal
// best-effort batch flusher, which retries a few times and then drops on
// failure since telemetry is lossy by design), the legacy drain must know
// definitively whether to retire the durable item it just sent, so it needs
// a real success/failure signal rather than fire-and-forget.
func (u *Uplink) sendLegacySync(ev Event) bool {
	body, err := json.Marshal([]Event{ev})
	if err != nil {
		return false
	}
	req, err := http.NewRequest(http.MethodPost, u.cfg.ServerURL+"/v2/ingest", bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	if u.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+u.cfg.Token)
	}
	resp, err := u.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// legacyEventFromOutboxItem reverse-translates a durably-stored reliable
// event body (wire.TaskEventBody or wire.MessageEventBody JSON, as written
// by routeToRealtime's forward direction) back into the legacy uplink.Event
// shape /v2/ingest expects. ok is false for a kind this function does not
// recognize (defensive; the outbox/overflow tables only ever hold the two
// kinds routeToRealtime writes).
func legacyEventFromOutboxItem(kind string, body json.RawMessage) (Event, bool) {
	switch kind {
	case wire.TypeTaskEvent:
		var b wire.TaskEventBody
		if err := json.Unmarshal(body, &b); err != nil {
			return Event{}, false
		}
		k := legacyTaskKind(b.Kind)
		if k == "" {
			return Event{}, false
		}
		ev := Event{
			Event: protocol.Event{
				Kind:  k,
				Title: b.Title,
				Step:  b.Step,
				Text:  b.Message,
			},
			SourceID: b.TaskID,
			TS:       time.UnixMilli(b.OccurredAt).UTC().Format(time.RFC3339),
		}
		if b.Percent != nil {
			ev.Percent = *b.Percent
		}
		if b.Display != nil {
			ev.Icon, ev.Tint, ev.Template = b.Display.Icon, b.Display.Tint, b.Display.Template
		}
		return ev, true
	case wire.TypeMessageEvent:
		var b wire.MessageEventBody
		if err := json.Unmarshal(body, &b); err != nil {
			return Event{}, false
		}
		return Event{
			Event:    protocol.Event{Kind: protocol.MessageSend, Text: b.Text, Level: b.Level},
			SourceID: b.AutomationID,
			TS:       time.UnixMilli(b.OccurredAt).UTC().Format(time.RFC3339),
		}, true
	default:
		return Event{}, false
	}
}

// legacyTaskKind reverses taskEventKind (wire task.event "kind" string back
// to the protocol.Kind the legacy HTTP path expects). Empty return means
// unrecognized.
func legacyTaskKind(wireKind string) protocol.Kind {
	switch wireKind {
	case "started":
		return protocol.TaskStart
	case "progress":
		return protocol.TaskProgress
	case "step":
		return protocol.TaskStep
	case "done":
		return protocol.TaskDone
	case "failed":
		return protocol.TaskFail
	default:
		return ""
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
		u.pollRealtimeMode()
		batch, closed := u.drain()
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

func (u *Uplink) drain() (batch []Event, closed bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	batch = append(batch, u.queue...)
	u.queue = u.queue[:0]
	for _, ev := range u.pending {
		batch = append(batch, ev)
	}
	clear(u.pending)
	for source, lines := range u.logs {
		batch = append(batch, Event{
			Event:    protocol.Event{Kind: protocol.TaskLog, Text: strings.Join(lines, "\n")},
			SourceID: source,
			TS:       time.Now().UTC().Format(time.RFC3339),
		})
	}
	clear(u.logs)
	return batch, u.closed
}

// send posts with small retry; on final failure events are dropped (telemetry
// is lossy by design — the next update supersedes anyway). The response may
// carry reverse-control commands, forwarded to u.Commands.
func (u *Uplink) send(batch []Event) {
	if batch == nil {
		batch = []Event{}
	}
	body, err := json.Marshal(batch)
	if err != nil {
		return
	}
	url := u.cfg.ServerURL + "/v2/ingest"
	if u.cfg.CommandSource != "" {
		url += "?sources=" + u.cfg.CommandSource
	}
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
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
			continue
		}
		if resp.StatusCode < 500 {
			u.deliverCommands(resp)
			resp.Body.Close()
			return // success, or a 4xx retrying won't fix
		}
		resp.Body.Close()
	}
}

func (u *Uplink) deliverCommands(resp *http.Response) {
	if u.cfg.CommandSource == "" {
		return
	}
	var parsed struct {
		Commands map[string][]string `json:"commands"`
	}
	if json.NewDecoder(resp.Body).Decode(&parsed) != nil {
		return
	}
	for _, cmd := range parsed.Commands[u.cfg.CommandSource] {
		select {
		case u.Commands <- cmd:
		default: // consumer stalled; drop rather than block the flusher
		}
	}
}
