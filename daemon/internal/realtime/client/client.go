package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/outbox"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

const sourceRole = "source"

var errClientClosing = errors.New("realtime client: closing")

// errNoRetry marks a handshake failure that is fatal AND not retryable
// (version_unsupported, unauthenticated, or a server accept naming a
// protocol version we never offered): reconnecting cannot succeed without
// an out-of-band change (a software upgrade, a new credential), so the
// reconnect loop must stop instead of hammering the server forever. The
// process recovers by constructing a new Client (i.e. an explicit restart).
var errNoRetry = errors.New("realtime client: fatal, not retryable")

// ErrRealtimeDisabled marks a dial rejected with HTTP 403 before the
// WebSocket upgrade completed: the server's own REALTIME_ENABLED flag is
// off, so its /v3/realtime endpoint refuses every connection regardless of
// this device's credentials. This is neither the errNoRetry case (the flag
// can flip back on at any time, with no software upgrade or credential
// change required) nor an ordinary transient dial failure (hammering the
// normal reconnect backoff would be pointless load on a condition that
// cannot self-heal within seconds) — see run's handling and
// serverDisabledRecheckInterval.
var ErrRealtimeDisabled = errors.New("realtime client: server reports realtime disabled (403)")

// serverDisabledRecheckInterval is how long the client waits before
// re-probing an endpoint that rejected the last dial with
// ErrRealtimeDisabled. Deliberately long and fixed: this is a server
// operator's flag, not something expected to flip from moment to moment, so
// there is no user-facing config for it (see the work order) — only a
// same-package test seam (Config.testDisabledRecheckInterval) so tests
// don't have to wait 5 minutes.
const serverDisabledRecheckInterval = 5 * time.Minute

// ErrInvalidBody wraps a failure from a wire body's Validate(), returned by
// SendTaskEvent/SendMessageEvent when the caller-supplied fields can never
// produce a valid envelope (bad kind, out-of-range percent, oversized free
// text, ...). It is the one Enqueue-adjacent failure callers should treat as
// permanent: unlike outbox.ErrOutboxFull or any other Enqueue error (a
// transient BeginTx/COUNT/INSERT/Commit failure), retrying the exact same
// malformed body can never succeed. Use errors.Is to check for it.
var ErrInvalidBody = errors.New("realtime: event body failed validation")

// Client is a reconnecting realtime-protocol source connection. Construct
// with New; it starts connecting immediately in the background. Call
// SendTaskEvent/SendMessageEvent for reliable events and SendMetric for
// best-effort metric samples; call Close to shut down.
type Client struct {
	cfg Config

	mu        sync.Mutex
	conn      *websocket.Conn
	connected bool
	attempt   int

	// wake coalesces "check the outbox for new work" notifications into the
	// single connection goroutine that owns writing reliable events to the
	// wire (see connectAndServe's select loop and replayPending). It is
	// intentionally never written to directly by anything other than
	// wakeSender, and never read by anything other than connectAndServe:
	// that single-writer/single-reader discipline is what guarantees
	// device_seq order on the wire even when SendTaskEvent/SendMessageEvent
	// are called concurrently, including during an in-flight replay after a
	// reconnect (SPEC.md section 5.1).
	wake chan struct{}

	metrics *metricBatcher

	seenMu    sync.Mutex
	seenOrder []string
	seen      map[string]bool

	supersededPenalty atomic.Bool

	// serverDisabled mirrors the outcome of the most recent dial attempt:
	// true from the moment a dial is rejected with ErrRealtimeDisabled
	// until the next dial actually completes hello (see connectAndServe),
	// at which point it is cleared. Uplink polls this (ServerDisabled) to
	// decide whether to switch its whole session to legacy /v2 routing —
	// see internal/uplink.Uplink.pollRealtimeMode's truth table.
	serverDisabled atomic.Bool

	// legacyDrainMu serializes DrainToLegacy against itself and against
	// connectAndServe's post-hello replay, one outbox row at a time. This
	// Client is shared process-wide (cmd/sitrep/agent.go builds exactly one
	// per resident agent, and hands it to a fresh uplink.Uplink for every
	// concurrently-running automation, each with its own drain-driving
	// poll loop), so without a lock owned HERE — not per-Uplink, which
	// would do nothing to stop two different Uplinks from racing — two
	// callers could both read the same unacked/un-promoted row via
	// Pending()/OverflowPending() before either retired it, and both would
	// deliver it: a duplicate reliable event on the wire. See
	// DrainToLegacy's doc comment for the full invariant, including how
	// this same lock also rules out the drain racing this Client's own
	// realtime replay across a switch back from legacy mode.
	legacyDrainMu sync.Mutex

	closing chan struct{}
	closed  chan struct{}
	once    sync.Once
}

// New starts a Client connecting in the background.
func New(cfg Config) *Client {
	cfg.applyDefaults()
	c := &Client{
		cfg:     cfg,
		wake:    make(chan struct{}, 1),
		metrics: newMetricBatcher(cfg.MetricFlushInterval, cfg.MetricThrottledInterval),
		seen:    make(map[string]bool),
		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	go c.run()
	return c
}

// Close stops the client and waits for its background goroutine to exit.
func (c *Client) Close() {
	c.once.Do(func() { close(c.closing) })
	<-c.closed
}

// Connected reports whether a hello-completed connection is currently up.
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

// ServerDisabled reports whether the most recent dial attempt was rejected
// with ErrRealtimeDisabled (server-side REALTIME_ENABLED off) and no dial
// has completed hello since. See Config's package doc and
// internal/uplink.Uplink.pollRealtimeMode for what the caller does with
// this.
func (c *Client) ServerDisabled() bool { return c.serverDisabled.Load() }

// Outbox exposes the durable store backing this client, for a caller that
// needs to drain it through a path other than this client's own realtime
// delivery — specifically internal/uplink's legacy-mode fallback, which
// walks Outbox/Overflow directly once ServerDisabled reports the server has
// rejected realtime for this session.
func (c *Client) Outbox() *outbox.Store { return c.cfg.Outbox }

// Space reports the (device, space) scope this client's outbox and
// device_seq counter are bound to (SPEC.md section 5.1) — see Outbox.
func (c *Client) Space() string { return c.cfg.Space }

// LegacySink is called once per durable outbox/overflow row by
// DrainToLegacy, oldest first, and must translate + attempt delivery of
// that row over whatever non-realtime path the caller uses (e.g. legacy
// /v2 HTTP). It reports whether the row should be retired:
//
//   - true: either delivery succeeded, or the row is permanently
//     undeliverable (e.g. an unrecognized kind) and must never be retried —
//     either way DrainToLegacy acks/deletes it and moves on to the next row.
//   - false: delivery failed for a reason that might succeed later (e.g.
//     the legacy endpoint is unreachable right now); DrainToLegacy stops
//     immediately, retiring nothing further, so the next call resumes at
//     exactly this row.
type LegacySink func(kind string, body json.RawMessage) bool

// DrainToLegacy walks this Client's shared outbox — every durable reliable
// event with a real device_seq (Pending), then everything still waiting in
// overflow (OverflowPending) — oldest first in each, handing each row to
// sink and retiring it (Ack / DeleteOverflow) only once sink reports true.
//
// This is the single owning entry point for legacy-mode draining: this
// Client is constructed once per resident agent process
// (cmd/sitrep/agent.go) and shared by a fresh uplink.Uplink for every
// concurrently-running automation (see runAutomation), each running its own
// pollRealtimeMode poll loop against the same outbox. Without process-wide
// serialization here, two Uplinks could both read the same unretired row
// before either retired it and both deliver it — a duplicate reliable
// event reaching the server. DrainToLegacy rules that out: every row's
// read-sink-retire sequence runs under legacyDrainMu (see
// drainOneToLegacy), held for exactly that one row, so a second concurrent
// caller blocks until the first has fully retired its row and always sees a
// fresh Pending()/OverflowPending() read with that row already gone.
//
// The same lock also closes the narrower switch-back race the reviewer
// flagged: connectAndServe acquires legacyDrainMu around clearing
// serverDisabled and its initial post-hello replayPending call (see
// connectAndServe), so a drain in flight when the server re-accepts
// realtime either fully retires its current row before that replay reads
// the outbox (the row is already gone by then — no duplicate), or blocks
// until the replay (which reads every row still sitting in the outbox at
// that instant) has already committed to delivering them over realtime —
// drainOneToLegacy then observes ServerDisabled() flip false on its next
// iteration and stops without ever touching those rows itself. Either way a
// given row is delivered exactly once, by exactly one of the two paths.
//
// The lock is held per-row rather than across the whole call so a
// reconnecting Client is never blocked behind an entire backlog — only
// behind whichever single row happens to be mid-flight.
func (c *Client) DrainToLegacy(ctx context.Context, sink LegacySink) {
	if c.cfg.Outbox == nil {
		return
	}
	for {
		if done := c.drainOneToLegacy(ctx, sink); done {
			return
		}
	}
}

// drainOneToLegacy drains at most one row (see DrainToLegacy) under
// legacyDrainMu. It reports true when the caller should stop: nothing left
// to drain, sink asked to stop, a store error, or the server has
// re-accepted realtime since the last iteration.
func (c *Client) drainOneToLegacy(ctx context.Context, sink LegacySink) bool {
	c.legacyDrainMu.Lock()
	defer c.legacyDrainMu.Unlock()

	if !c.ServerDisabled() {
		// Realtime has resumed (or was never disabled to begin with, for a
		// stray call): every row still sitting in the outbox at this
		// instant is guaranteed to be delivered by connectAndServe's
		// post-hello replayPending instead, which holds this same lock
		// while doing so — see DrainToLegacy's doc comment. Stopping here
		// cannot drop or duplicate anything.
		return true
	}

	store := c.cfg.Outbox
	pending, err := store.Pending(ctx, c.cfg.Space)
	if err != nil {
		c.cfg.Logf("realtime: legacy drain: read outbox: %v", err)
		return true
	}
	if len(pending) > 0 {
		item := pending[0]
		if !sink(item.Kind, item.Body) {
			return true
		}
		if err := store.Ack(ctx, c.cfg.Space, item.DeviceSeq); err != nil {
			c.cfg.Logf("realtime: legacy drain: ack seq %d: %v", item.DeviceSeq, err)
			return true
		}
		return false
	}

	overflow, err := store.OverflowPending(ctx, c.cfg.Space)
	if err != nil {
		c.cfg.Logf("realtime: legacy drain: read overflow: %v", err)
		return true
	}
	if len(overflow) == 0 {
		return true
	}
	ov := overflow[0]
	if !sink(ov.Kind, ov.Body) {
		return true
	}
	if err := store.DeleteOverflow(ctx, ov.ID); err != nil {
		c.cfg.Logf("realtime: legacy drain: delete overflow id %d: %v", ov.ID, err)
		return true
	}
	return false
}

// MetricsThrottled reports whether the metric batcher is currently in its
// throttled cadence (SPEC.md section 7's command{throttle}/{resume_rate}).
func (c *Client) MetricsThrottled() bool { return c.metrics.Throttled() }

func newEnvelopeID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("rt%d", time.Now().UnixNano())
	}
	return "rt" + hex.EncodeToString(b)
}

func (c *Client) now() time.Time { return c.cfg.Clock.Now() }
func (c *Client) nowMS() int64   { return c.now().UnixMilli() }

// ---- public send API ----

// TaskEvent is the caller-facing shape for SendTaskEvent; see
// wire.TaskEventBody for field semantics.
type TaskEvent struct {
	TaskID     string
	Kind       string // started|progress|step|done|failed
	OccurredAt time.Time
	Title      string
	Percent    *int
	Step       string
	Message    string
	Display    *wire.DisplayHints
}

// SendTaskEvent durably enqueues a task lifecycle/progress event (assigning
// its device_seq) and attempts immediate delivery if connected. The event
// survives in the outbox until acknowledged, across any number of
// reconnects or process restarts.
func (c *Client) SendTaskEvent(ev TaskEvent) error {
	_, err := c.cfg.Outbox.Enqueue(context.Background(), c.cfg.Space, wire.TypeTaskEvent, func(seq int64) (json.RawMessage, error) {
		body := wire.TaskEventBody{
			DeviceID:   c.cfg.DeviceID,
			DeviceSeq:  seq,
			TaskID:     ev.TaskID,
			Kind:       ev.Kind,
			OccurredAt: ev.OccurredAt.UnixMilli(),
			Title:      ev.Title,
			Percent:    ev.Percent,
			Step:       ev.Step,
			Message:    ev.Message,
			Display:    ev.Display,
		}
		if err := body.Validate(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidBody, err)
		}
		return json.Marshal(body)
	})
	if err != nil {
		return err
	}
	c.wakeSender()
	return nil
}

// MessageEvent is the caller-facing shape for SendMessageEvent. If
// MessageID is empty it defaults to "<device_id>:<device_seq>".
type MessageEvent struct {
	MessageID    string
	Level        string // info|warn|error
	Text         string
	OccurredAt   time.Time
	AutomationID string
}

// SendMessageEvent durably enqueues a message event; see SendTaskEvent.
func (c *Client) SendMessageEvent(ev MessageEvent) error {
	_, err := c.cfg.Outbox.Enqueue(context.Background(), c.cfg.Space, wire.TypeMessageEvent, func(seq int64) (json.RawMessage, error) {
		id := ev.MessageID
		if id == "" {
			id = fmt.Sprintf("%s:%d", c.cfg.DeviceID, seq)
		}
		body := wire.MessageEventBody{
			DeviceID:     c.cfg.DeviceID,
			DeviceSeq:    seq,
			MessageID:    id,
			Level:        ev.Level,
			Text:         ev.Text,
			OccurredAt:   ev.OccurredAt.UnixMilli(),
			AutomationID: ev.AutomationID,
		}
		if err := body.Validate(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidBody, err)
		}
		return json.Marshal(body)
	})
	if err != nil {
		return err
	}
	c.wakeSender()
	return nil
}

// SendMetric offers one best-effort metric sample. It is merged with any
// other pending sample for the same MetricID and flushed on the client's
// own cadence (SPEC.md section 12); it is never persisted, never
// retransmitted, and silently superseded by the next sample if lost.
func (c *Client) SendMetric(s wire.MetricSample) {
	c.metrics.Offer(s)
}

// wakeSender notifies the active connection's single writer goroutine
// (connectAndServe's select loop) that new work may be waiting in the
// outbox. It never writes to the wire itself and is always safe to call,
// including when no connection is currently up (the signal is simply
// coalesced/dropped and the next connection's initial replayPending call
// will pick up the item from durable storage regardless).
//
// This indirection — rather than writing directly from the caller's
// goroutine, as a prior version of this code did — is what guarantees wire
// order matches device_seq order (SPEC.md section 5.1): every reliable
// event write, whether a fresh live send or a reconnect replay, now goes
// through replayPending called from exactly one goroutine per connection,
// so two writes for the same connection can never race each other onto the
// wire out of order.
func (c *Client) wakeSender() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// ---- connection lifecycle ----

func (c *Client) run() {
	defer close(c.closed)
	for {
		select {
		case <-c.closing:
			return
		default:
		}

		err := c.connectAndServe()
		if errors.Is(err, errClientClosing) {
			return
		}
		if errors.Is(err, errNoRetry) {
			// A retry can only fail the same way (wrong protocol version,
			// revoked credential, ...): surface it and stop the loop
			// entirely rather than reconnecting forever. Recovery is an
			// explicit restart (a new Client).
			c.cfg.Logf("realtime: giving up, not retryable: %v", err)
			return
		}
		if errors.Is(err, ErrRealtimeDisabled) {
			// The server itself has realtime turned off. This is not a
			// transient dial failure (the normal exponential backoff below
			// would hammer it every ~60s for no reason: an operator flag
			// does not flip back on within seconds) and not the
			// give-up-forever case either (it CAN flip back on at any time,
			// with no software upgrade or new credential needed) — so wait
			// a long, fixed interval and try again, without touching the
			// normal reconnect attempt counter at all.
			c.serverDisabled.Store(true)
			interval := serverDisabledRecheckInterval
			if c.cfg.testDisabledRecheckInterval > 0 {
				interval = c.cfg.testDisabledRecheckInterval
			}
			c.cfg.Logf("realtime: server reports realtime disabled; re-checking in %v", interval)
			select {
			case <-time.After(interval):
			case <-c.closing:
				return
			}
			continue
		}
		c.cfg.Logf("realtime: connection ended: %v", err)

		c.mu.Lock()
		attempt := c.attempt
		c.attempt++
		c.mu.Unlock()

		delay := Backoff(attempt, c.cfg.BackoffBase, c.cfg.BackoffMax, c.cfg.BackoffJitter, c.cfg.Rand)
		if c.supersededPenalty.Swap(false) {
			// Superseded means our own newer connection won, or our
			// credential is compromised — either way, reconnecting
			// aggressively is the wrong instinct; stretch the wait to the
			// backoff ceiling instead of a reconnect storm.
			delay = c.cfg.BackoffMax
		}
		select {
		case <-time.After(delay):
		case <-c.closing:
			return
		}
	}
}

func (c *Client) connectAndServe() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dialCtx, dialCancel := context.WithTimeout(ctx, c.cfg.HelloTimeout)
	header := http.Header{}
	if c.cfg.Token != "" {
		header.Set("Authorization", "Bearer "+c.cfg.Token)
	}
	conn, resp, err := websocket.Dial(dialCtx, c.cfg.URL, &websocket.DialOptions{HTTPHeader: header})
	dialCancel()
	if err != nil {
		// A rejection before the WebSocket upgrade completes surfaces here
		// as a non-nil err with resp still populated (coder/websocket keeps
		// the response, with a small snippet of its body, specifically for
		// this kind of diagnosis). Status 403 on this endpoint is
		// sufficient signal on its own that the server's REALTIME_ENABLED
		// is off — SERVER-side authority has moved to UserStore for this
		// session, exactly like the local SITREP_REALTIME flag off has
		// always meant "legacy only" on this side; see ErrRealtimeDisabled
		// and internal/uplink.Uplink.pollRealtimeMode for what happens next.
		if resp != nil && resp.StatusCode == http.StatusForbidden {
			return fmt.Errorf("dial: %w", ErrRealtimeDisabled)
		}
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()

	if err := c.sendHelloOffer(ctx, conn); err != nil {
		return fmt.Errorf("hello offer: %w", err)
	}
	accept, err := c.awaitHelloAccept(ctx, conn)
	if err != nil {
		return fmt.Errorf("hello accept: %w", err)
	}
	heartbeatInterval := time.Duration(accept.HeartbeatIntervalMS) * time.Millisecond

	c.mu.Lock()
	c.conn = conn
	c.connected = true
	c.attempt = 0
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.connected = false
		c.conn = nil
		c.mu.Unlock()
	}()

	// A completed hello is proof the server currently accepts realtime,
	// whether or not this connection had ever previously been rejected with
	// ErrRealtimeDisabled — clear it so pollRealtimeMode's caller resumes
	// realtime routing for new events immediately, and replay everything
	// still sitting in the outbox over this connection.
	//
	// Both happen under legacyDrainMu — the same lock DrainToLegacy holds
	// per row — so a legacy drain in flight when the server re-accepts
	// realtime cannot race this replay onto the same row: see
	// DrainToLegacy's doc comment for the full invariant.
	c.legacyDrainMu.Lock()
	c.serverDisabled.Store(false)
	c.replayPending(ctx, conn)
	c.legacyDrainMu.Unlock()

	frames := make(chan frameMsg, 8)
	pings := make(chan struct{}, 8)
	pongs := make(chan struct{}, 8)
	go c.readLoop(ctx, conn, frames, pings, pongs)

	resendTicker := time.NewTicker(c.cfg.ResendInterval)
	defer resendTicker.Stop()
	metricsTicker := time.NewTicker(c.cfg.MetricFlushInterval)
	defer metricsTicker.Stop()
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()

	lastPong := c.now()
	heartbeatTimeout := time.Duration(c.cfg.HeartbeatTimeoutFactor) * heartbeatInterval

	for {
		select {
		case fm := <-frames:
			if fm.err != nil {
				return fm.err
			}
			if err := c.handleFrame(ctx, conn, fm.env); err != nil {
				return err
			}
		case <-pings:
			_ = conn.Write(ctx, websocket.MessageText, []byte("pong"))
		case <-pongs:
			lastPong = c.now()
		case <-resendTicker.C:
			c.replayPending(ctx, conn)
		case <-c.wake:
			// A live SendTaskEvent/SendMessageEvent call enqueued a new
			// item. replayPending re-reads the full outbox (oldest seq
			// first) and writes from this same goroutine, so it can never
			// race a concurrent replay/resend for this connection — it
			// simply folds into the same single-writer stream.
			c.replayPending(ctx, conn)
		case <-metricsTicker.C:
			c.flushMetrics(ctx, conn)
		case <-heartbeatTicker.C:
			if err := conn.Write(ctx, websocket.MessageText, []byte("ping")); err != nil {
				return fmt.Errorf("heartbeat write: %w", err)
			}
			if c.now().Sub(lastPong) > heartbeatTimeout {
				return fmt.Errorf("heartbeat timeout: no pong within %v", heartbeatTimeout)
			}
		case <-c.closing:
			_ = conn.Close(websocket.StatusNormalClosure, "client closing")
			return errClientClosing
		}
	}
}

func (c *Client) sendHelloOffer(ctx context.Context, conn *websocket.Conn) error {
	offer := wire.HelloOffer{
		Stage:            "offer",
		DeviceID:         c.cfg.DeviceID,
		Role:             sourceRole,
		ProtocolVersions: c.cfg.ProtocolVersions,
	}
	env, err := wire.NewEnvelope(wire.TypeHello, newEnvelopeID(), c.nowMS(), wire.HelloBody{Offer: &offer})
	if err != nil {
		return err
	}
	data, err := env.Encode()
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func (c *Client) awaitHelloAccept(ctx context.Context, conn *websocket.Conn) (wire.HelloAccept, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.HelloTimeout)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		return wire.HelloAccept{}, err
	}
	env, err := wire.DecodeEnvelope(data)
	if err != nil {
		return wire.HelloAccept{}, err
	}
	if env.Type == wire.TypeError {
		body, derr := wire.DecodeBody(env)
		if derr == nil {
			eb := body.(wire.ErrorBody)
			// A fatal AND non-retryable rejection (version_unsupported,
			// unauthenticated, ...) can only repeat on the next attempt:
			// mark it errNoRetry so the reconnect loop stops (SPEC.md
			// section 13's retryable flag is authoritative).
			if eb.Fatal != nil && *eb.Fatal && eb.Retryable != nil && !*eb.Retryable {
				return wire.HelloAccept{}, fmt.Errorf("server rejected hello: %s: %s: %w", eb.Code, eb.Message, errNoRetry)
			}
			return wire.HelloAccept{}, fmt.Errorf("server rejected hello: %s: %s", eb.Code, eb.Message)
		}
		return wire.HelloAccept{}, fmt.Errorf("server rejected hello with malformed error body")
	}
	if env.Type != wire.TypeHello {
		return wire.HelloAccept{}, fmt.Errorf("expected hello, got %q", env.Type)
	}
	body, err := wire.DecodeBody(env)
	if err != nil {
		return wire.HelloAccept{}, err
	}
	hb := body.(wire.HelloBody)
	if hb.Accept == nil {
		return wire.HelloAccept{}, fmt.Errorf("expected hello accept, got offer")
	}
	// SPEC.md section 9.2: the server selects from the INTERSECTION of the
	// offer's protocol_versions with its own set, so a conformant accept
	// always names a version we offered. An accept naming any other version
	// is a failed negotiation — retrying with the same offer cannot end
	// differently, so treat it like version_unsupported and stop.
	offered := false
	for _, v := range c.cfg.ProtocolVersions {
		if v == hb.Accept.ProtocolVersion {
			offered = true
			break
		}
	}
	if !offered {
		return wire.HelloAccept{}, fmt.Errorf(
			"server accepted protocol version %d, which we never offered (%v): %w",
			hb.Accept.ProtocolVersion, c.cfg.ProtocolVersions, errNoRetry)
	}
	return *hb.Accept, nil
}

// frameMsg carries one decoded envelope, or the terminal read error that
// ended the connection, from readLoop to connectAndServe's select loop.
type frameMsg struct {
	env wire.Envelope
	err error
}

// readLoop pumps decoded envelopes (or the bare ping/pong heartbeat text)
// onto the given channels until the connection dies, then reports the
// terminal error on frames and returns.
func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn, frames chan<- frameMsg, pings, pongs chan<- struct{}) {
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			select {
			case frames <- frameMsg{err: err}:
			case <-ctx.Done():
			}
			return
		}
		if typ == websocket.MessageText {
			switch string(data) {
			case "ping":
				select {
				case pings <- struct{}{}:
				case <-ctx.Done():
					return
				}
				continue
			case "pong":
				select {
				case pongs <- struct{}{}:
				case <-ctx.Done():
					return
				}
				continue
			}
		}
		env, derr := wire.DecodeEnvelope(data)
		select {
		case frames <- frameMsg{env: env, err: derr}:
		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) replayPending(ctx context.Context, conn *websocket.Conn) {
	items, err := c.cfg.Outbox.Pending(ctx, c.cfg.Space)
	if err != nil {
		c.cfg.Logf("realtime: read pending outbox: %v", err)
		return
	}
	for _, item := range items {
		env, err := wire.NewEnvelope(item.Kind, newEnvelopeID(), c.nowMS(), json.RawMessage(item.Body))
		if err != nil {
			continue
		}
		data, err := env.Encode()
		if err != nil {
			continue
		}
		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		werr := conn.Write(writeCtx, websocket.MessageText, data)
		cancel()
		if werr != nil {
			return // connection is dying; the outer loop will notice and reconnect
		}
	}
}

func (c *Client) flushMetrics(ctx context.Context, conn *websocket.Conn) {
	samples, ok := c.metrics.Flush(c.now())
	if !ok {
		return
	}
	body := wire.MetricFrameBody{DeviceID: c.cfg.DeviceID, Metrics: samples}
	if err := body.Validate(); err != nil {
		c.cfg.Logf("realtime: dropping invalid metric.frame: %v", err)
		return
	}
	env, err := wire.NewEnvelope(wire.TypeMetricFrame, newEnvelopeID(), c.nowMS(), body)
	if err != nil {
		return
	}
	data, err := env.Encode()
	if err != nil {
		return
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = conn.Write(writeCtx, websocket.MessageText, data)
}

func (c *Client) handleFrame(ctx context.Context, conn *websocket.Conn, env wire.Envelope) error {
	if !wire.KnownType(env.Type) {
		return nil // SPEC.md section 15: ignore, not an error
	}
	body, err := wire.DecodeBody(env)
	if err != nil {
		c.cfg.Logf("realtime: dropping malformed %s frame: %v", env.Type, err)
		return nil
	}
	switch env.Type {
	case wire.TypeAck:
		ab := body.(wire.AckBody)
		// SPEC.md sections 5.3/13: every pair in one ack MUST carry the
		// device_id of this connection's authenticated device; a pair for
		// any other device makes the WHOLE envelope malformed. Drop the
		// ack without retiring anything — the events stay queued for a
		// later well-formed ack (resend makes this safe) — report the
		// protocol violation back as error{malformed}, and keep the
		// connection (malformed is retryable, non-fatal).
		for _, p := range ab.Acked {
			if p.DeviceID != c.cfg.DeviceID {
				c.cfg.Logf("realtime: protocol violation: ack %s carries a pair for foreign device %q; dropping whole ack", env.ID, p.DeviceID)
				c.sendError(ctx, conn, wire.ErrorBody{
					Code:      wire.ErrMalformed,
					Message:   "ack pair for a device other than this connection's",
					InReplyTo: env.ID,
					Retryable: boolPtr(true),
					Fatal:     boolPtr(false),
				})
				return nil
			}
		}
		for _, p := range ab.Acked {
			if err := c.cfg.Outbox.Ack(ctx, c.cfg.Space, p.DeviceSeq); err != nil {
				c.cfg.Logf("realtime: ack seq %d: %v", p.DeviceSeq, err)
			}
		}
		// An ack frees outbox capacity, and Outbox.Ack itself promotes at
		// most one waiting overflow row into that freed slot per call (see
		// its doc comment). Wake the sender so a freshly-promoted row (now
		// bearing a real device_seq) is written to this connection promptly
		// via replayPending, rather than waiting for the next resend tick.
		c.wakeSender()
		return nil
	case wire.TypeCommand:
		cb := body.(wire.CommandBody)
		c.handleCommand(ctx, conn, env, cb)
		return nil
	case wire.TypeError:
		eb := body.(wire.ErrorBody)
		return c.handleError(eb)
	default:
		// snapshot/delta/hello/resume/subscribe/... are not meaningful on a
		// source connection; ignore rather than kill the connection.
		return nil
	}
}

func (c *Client) handleError(eb wire.ErrorBody) error {
	if eb.Code == wire.ErrSuperseded {
		c.supersededPenalty.Store(true)
		c.cfg.Logf("realtime: connection superseded by a newer one (or credential in use elsewhere)")
		return fmt.Errorf("%s: %s", eb.Code, eb.Message)
	}
	if eb.Fatal != nil && *eb.Fatal {
		return fmt.Errorf("%s: %s", eb.Code, eb.Message)
	}
	// Advisory (rate_limited, sequence_gap, ...): log and keep the
	// connection.
	c.cfg.Logf("realtime: server error %s: %s", eb.Code, eb.Message)
	return nil
}

// handleCommand validates the command's TTL against the local clock
// (SPEC.md section 8: +/- 30s skew allowance), deduplicates by command_id,
// and either applies it (throttle/resume_rate) or forwards it to
// Config.OnCommand.
func (c *Client) handleCommand(ctx context.Context, conn *websocket.Conn, env wire.Envelope, cb wire.CommandBody) {
	const skew = 30 * time.Second
	now := c.now()
	windowStart := time.UnixMilli(env.TS).Add(-skew)
	windowEnd := time.UnixMilli(env.TS).Add(time.Duration(cb.TTLMs) * time.Millisecond).Add(skew)
	if now.Before(windowStart) || now.After(windowEnd) {
		c.cfg.Logf("realtime: dropping expired command %s (action %s)", cb.CommandID, cb.Action)
		c.sendError(ctx, conn, wire.ErrorBody{
			Code:      wire.ErrCommandExpired,
			Message:   "command ttl window elapsed",
			InReplyTo: env.ID,
			Retryable: boolPtr(false),
			Fatal:     boolPtr(false),
		})
		return
	}

	if c.alreadySeen(cb.CommandID) {
		return // SPEC.md section 8: MUST NOT execute the same command_id twice
	}

	switch cb.Action {
	case "throttle":
		c.metrics.SetThrottled(true)
	case "resume_rate":
		c.metrics.SetThrottled(false)
	default:
		if c.cfg.OnCommand != nil {
			c.cfg.OnCommand(CommandAction{
				CommandID:      cb.CommandID,
				Action:         cb.Action,
				TaskID:         cb.TaskID,
				AutomationID:   cb.AutomationID,
				TargetDeviceID: cb.TargetDeviceID,
			})
		}
	}
}

func boolPtr(b bool) *bool { return &b }

// sendError best-effort writes one error envelope on the current
// connection; failures are ignored (the connection's own health checks
// notice a dying socket independently).
func (c *Client) sendError(ctx context.Context, conn *websocket.Conn, body wire.ErrorBody) {
	env, err := wire.NewEnvelope(wire.TypeError, newEnvelopeID(), c.nowMS(), body)
	if err != nil {
		return
	}
	data, err := env.Encode()
	if err != nil {
		return
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = conn.Write(writeCtx, websocket.MessageText, data)
}

// alreadySeen reports whether commandID has been handled before, recording
// it if not. Bounded so a long-lived connection doesn't grow this set
// forever.
func (c *Client) alreadySeen(commandID string) bool {
	const cap = 512
	c.seenMu.Lock()
	defer c.seenMu.Unlock()
	if c.seen[commandID] {
		return true
	}
	c.seen[commandID] = true
	c.seenOrder = append(c.seenOrder, commandID)
	if len(c.seenOrder) > cap {
		drop := c.seenOrder[0]
		c.seenOrder = c.seenOrder[1:]
		delete(c.seen, drop)
	}
	return false
}
