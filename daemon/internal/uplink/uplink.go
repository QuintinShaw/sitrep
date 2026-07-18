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
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/protocol"
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
