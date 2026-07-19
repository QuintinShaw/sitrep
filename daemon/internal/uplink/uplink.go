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

	// Feature flag: when a realtime uplink is configured, every kind with a
	// realtime-protocol equivalent is routed there instead of the HTTP
	// batch below, so the same event is never sent both ways. task.log (no
	// realtime equivalent) and any realtime send failure fall through.
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
// Reliable events (task.event/message.event) NEVER fall back to the legacy
// /v2 HTTP path while the realtime flag is on, even when the send fails:
// /v2 ingest writes UserStore, while /v3 viewers resume from SpaceHub, so
// diverting so much as one terminal task.done/task.fail/message.send would
// let a viewer permanently miss it. See sendReliable for what happens to a
// send failure instead.
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
