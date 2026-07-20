package client

import (
	"bytes"
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

// ErrTransportUnavailable marks a WS dial rejected with HTTP 503
// {"error":"transport_unavailable"} before the WebSocket upgrade completed:
// the server's WS_TRANSPORT_ENABLED flag is off
// (docs/design/v1-architecture.md section 8.1). This is a pure TRANSPORT
// fact, never a reason to switch which store reliable events target — the
// same outbox keeps draining over HTTP POST /v1/events to the identical
// SpaceHub the whole time (see httpFallbackLoop, always active whenever
// !Connected(), 503 or not). It is also not the errNoRetry case (the flag
// can flip back on at any time, with no software upgrade or credential
// change required) nor an ordinary transient dial failure (hammering the
// normal reconnect backoff would be pointless load on a condition that
// cannot self-heal within seconds) — see run's handling and
// wsReprobeInterval.
var ErrTransportUnavailable = errors.New("realtime client: server reports WS transport unavailable (503)")

// wsReprobeInterval is how long the client waits before re-attempting a WS
// dial after the last one was rejected with ErrTransportUnavailable.
// Deliberately long and fixed: this is a server operator's flag, not
// something expected to flip from moment to moment, so there is no
// user-facing config for it — only a same-package test seam
// (Config.testWSReprobeInterval) so tests don't have to wait 5 minutes.
// Reliable delivery does not wait on this interval at all: httpFallbackLoop
// keeps uplinking over HTTP the entire time WS is down, 503 or not.
const wsReprobeInterval = 5 * time.Minute

// ErrInvalidBody wraps a failure from a wire body's Validate(), returned by
// SendTaskEvent/SendMessageEvent when the caller-supplied fields can never
// produce a valid envelope (bad kind, out-of-range percent, oversized free
// text, ...). It is the one Enqueue-adjacent failure callers should treat as
// permanent: unlike outbox.ErrOutboxFull or any other Enqueue error (a
// transient BeginTx/COUNT/INSERT/Commit failure), retrying the exact same
// malformed body can never succeed. Use errors.Is to check for it.
var ErrInvalidBody = errors.New("realtime: event body failed validation")

// promoteOverflowBatch bounds how many outbox_overflow rows a single
// proactive promotion attempt (made at the top of every replay/resend
// cycle, WS or HTTP) will move into the seq-bearing outbox. This is the
// independent, idempotent retry the round-3 review's promotion-stranding
// fix requires: outbox.Store.Ack already attempts one promotion per ack,
// best-effort, but if THAT attempt fails transiently and nothing else ever
// acks again soon (e.g. the outbox is otherwise empty), nothing would retry
// it before this loop's own next tick. Small and bounded — this is a
// backstop, not the primary path.
const promoteOverflowBatch = 32

// Client is a reconnecting realtime-protocol source connection. Construct
// with New; it starts connecting immediately in the background. Call
// SendTaskEvent/SendMessageEvent for reliable events and SendMetric for
// best-effort metric samples; call Close to shut down.
//
// Transport: exactly one SpaceHub is ever targeted (docs/design/
// v1-architecture.md section 0). Whenever a WS connection is up, reliable
// events and metric frames are sent over it (connectAndServe's single
// per-connection goroutine, via replayPending/flushMetrics). Whenever it is
// not — dialing, backing off after a transient failure, or waiting out
// wsReprobeInterval after a 503 — the SAME outbox is drained over HTTP POST
// /v1/events by httpFallbackLoop instead. flushMu is the single-writer
// discipline that generalizes across both: replayPending and flushHTTP each
// hold it for the full duration of one flush attempt, so the two transports
// can never be mid-flight at the same instant — device_seq order on the
// wire (SPEC.md section 5.1) is preserved not just within one transport but
// across a switch between them.
type Client struct {
	cfg Config

	httpClient *http.Client

	mu        sync.Mutex
	conn      *websocket.Conn
	connected bool
	attempt   int

	// wake and httpWake coalesce "check the outbox for new work"
	// notifications into whichever of the two transport loops is
	// currently responsible for sending: wake is watched by
	// connectAndServe's select loop (WS), httpWake by httpFallbackLoop
	// (HTTP). wakeSender signals both, non-blocking; whichever loop isn't
	// the active transport simply finds nothing to do (flushHTTP no-ops
	// when Connected()). Two channels rather than one shared channel avoid
	// a single wake being consumed by the "wrong" (currently inactive)
	// loop, which would just cost a little promptness, not correctness —
	// but a dedicated channel per loop keeps that from ever mattering.
	wake     chan struct{}
	httpWake chan struct{}

	// flushMu is the single-writer lock guarding every attempt to drain
	// the outbox to the wire, WS or HTTP (see replayPending/flushHTTP). At
	// most one flush is ever in flight at a time, on either transport.
	flushMu sync.Mutex

	metrics *metricBatcher

	seenMu    sync.Mutex
	seenOrder []string
	seen      map[string]bool

	supersededPenalty atomic.Bool

	// authDegraded tracks whether a persistent auth (401) failure has been
	// reported to OnAuthState, so the hook fires only on transitions.
	authDegraded atomic.Bool

	closing  chan struct{}
	closed   chan struct{} // run() (WS supervisor) has returned
	httpDone chan struct{} // httpFallbackLoop has returned
	once     sync.Once
}

// New starts a Client connecting in the background.
func New(cfg Config) *Client {
	cfg.applyDefaults()
	if cfg.Outbox != nil {
		cfg.Outbox.SetLogf(outbox.Logf(cfg.Logf))
	}
	c := &Client{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: cfg.HTTPTimeout},
		wake:       make(chan struct{}, 1),
		httpWake:   make(chan struct{}, 1),
		metrics:    newMetricBatcher(cfg.MetricFlushInterval, cfg.MetricThrottledInterval),
		seen:       make(map[string]bool),
		closing:    make(chan struct{}),
		closed:     make(chan struct{}),
		httpDone:   make(chan struct{}),
	}
	go c.run()
	go c.httpFallbackLoop()
	return c
}

// Close stops the client and waits for both background loops to exit.
func (c *Client) Close() {
	c.once.Do(func() { close(c.closing) })
	<-c.closed
	<-c.httpDone
}

// Connected reports whether a hello-completed WS connection is currently
// up. This is purely informational/observability — it is never used to
// decide whether a write is allowed to reach SpaceHub (see the package doc
// comment): flushHTTP consults it only to avoid redundant work, not to gate
// correctness.
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

// Space reports the (device, space) scope this client's outbox and
// device_seq counter are bound to (SPEC.md section 5.1).
func (c *Client) Space() string { return c.cfg.Space }

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

// newMessageID returns a stable, opaque message_id independent of any
// device_seq. Fixed bug (round-3 review): a message_id previously derived
// from the device_seq passed into Outbox.Enqueue's build callback, but an
// event that overflows (outbox.Store routes it to outbox_overflow when the
// outbox is at capacity) is built with a PLACEHOLDER seq of 1 — every
// overflowed message with an unspecified MessageID collided on the same
// id, and promotion only ever patches device_seq, never message_id, so the
// collision was permanent. Generating the id here, once, before Enqueue is
// even called, makes it independent of whichever seq (real or placeholder)
// the event is eventually assigned.
func newMessageID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("msg%d", time.Now().UnixNano())
	}
	return "msg_" + hex.EncodeToString(b)
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
// reconnects, transport switches, or process restarts.
//
// outbox.ErrOverflowed is deliberately not surfaced as an error: per its own
// doc comment it is informational, not a failure — the event is already
// durably persisted (into outbox_overflow instead of outbox) by the time
// Enqueue returns it, and "[c]allers must not retry or reroute on this
// error".
func (c *Client) SendTaskEvent(ev TaskEvent) error {
	// task.progress is a continuous, last-value-wins "metric-kind" update: if
	// it has to sit in overflow (outbox full), successive progress frames for
	// the same task coalesce onto one row rather than piling up, bounding the
	// durable footprint (5d). Lifecycle kinds (started/step/done/failed) are
	// discrete and never coalesce (empty key).
	coalesceKey := ""
	if ev.Kind == "progress" {
		coalesceKey = "task.progress:" + ev.TaskID
	}
	_, err := c.cfg.Outbox.EnqueueCoalesce(context.Background(), c.cfg.Space, wire.TypeTaskEvent, coalesceKey, func(seq int64) (json.RawMessage, error) {
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
	if err != nil && !errors.Is(err, outbox.ErrOverflowed) {
		return err
	}
	c.wakeSender()
	return nil
}

// MessageEvent is the caller-facing shape for SendMessageEvent. If
// MessageID is empty a stable random id is generated (see newMessageID).
type MessageEvent struct {
	MessageID    string
	Level        string // info|warn|error
	Text         string
	OccurredAt   time.Time
	AutomationID string
}

// SendMessageEvent durably enqueues a message event; see SendTaskEvent,
// including why outbox.ErrOverflowed is not surfaced as an error here.
func (c *Client) SendMessageEvent(ev MessageEvent) error {
	id := ev.MessageID
	if id == "" {
		// Generated ONCE, here, before Enqueue picks a (possibly
		// placeholder) seq — see newMessageID's doc comment for the bug
		// this fixes.
		id = newMessageID()
	}
	_, err := c.cfg.Outbox.Enqueue(context.Background(), c.cfg.Space, wire.TypeMessageEvent, func(seq int64) (json.RawMessage, error) {
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
	if err != nil && !errors.Is(err, outbox.ErrOverflowed) {
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

// wakeSender notifies both transport loops that new work may be waiting in
// the outbox. It never writes to the wire itself and is always safe to
// call. See the wake/httpWake field doc comment for why there are two
// channels.
func (c *Client) wakeSender() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
	select {
	case c.httpWake <- struct{}{}:
	default:
	}
}

// retireAck retires exactly one (device_id, device_seq) pair using the
// SAME outbox.Store.Ack call regardless of which transport observed it —
// the WS ack handler and the HTTP ACK-response handler (flushHTTP) both
// call this, which is what "no separate reconciliation" (docs/design/
// v1-architecture.md section 4.1) means concretely on the daemon side. A
// pair naming any device other than this client's own is ignored
// defensively (the server should never send one to a single-device
// connection/request).
func (c *Client) retireAck(ctx context.Context, deviceID string, seq int64) {
	if deviceID != c.cfg.DeviceID {
		return
	}
	if err := c.cfg.Outbox.Ack(ctx, c.cfg.Space, seq); err != nil {
		c.cfg.Logf("realtime: ack seq %d: %v", seq, err)
	}
}

// reportAuthDegraded records a persistent auth (401) failure and fires
// OnAuthState only on the first transition into the degraded state.
func (c *Client) reportAuthDegraded(reason string) {
	if c.authDegraded.CompareAndSwap(false, true) && c.cfg.OnAuthState != nil {
		c.cfg.OnAuthState(false, reason)
	}
}

// reportAuthOK clears a previously-reported auth failure, firing OnAuthState
// only on the transition back to healthy.
func (c *Client) reportAuthOK() {
	if c.authDegraded.CompareAndSwap(true, false) && c.cfg.OnAuthState != nil {
		c.cfg.OnAuthState(true, "")
	}
}

// promoteOverflowBestEffort proactively retries promoting overflowed rows
// into the seq-bearing outbox, logging (not swallowing) a failure. Called
// at the top of every replay/resend cycle on both transports — see
// promoteOverflowBatch's doc comment for why this exists independent of
// outbox.Store.Ack's own best-effort trigger.
func (c *Client) promoteOverflowBestEffort(ctx context.Context) {
	if _, err := c.cfg.Outbox.PromoteOverflow(ctx, promoteOverflowBatch); err != nil {
		c.cfg.Logf("realtime: promote overflow: %v", err)
	}
}

// ---- connection lifecycle (WS) ----

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
			// explicit restart (a new Client). httpFallbackLoop keeps
			// running independently (Close() stops it too), so anything
			// already enqueued still has a path to the server if EventsURL
			// is configured — but a credential this invalid would fail
			// there too; this case is expected to be terminal either way.
			c.cfg.Logf("realtime: giving up, not retryable: %v", err)
			return
		}
		if errors.Is(err, ErrTransportUnavailable) {
			// The server itself has WS transport turned off. Not a
			// transient dial failure (the normal exponential backoff below
			// would hammer it every ~60s for no reason: an operator flag
			// does not flip back on within seconds) and not the
			// give-up-forever case either (it CAN flip back on at any
			// time) — so wait a long, fixed interval and try again,
			// without touching the normal reconnect attempt counter.
			// httpFallbackLoop is uplinking reliably the entire time this
			// waits; nothing is stranded.
			interval := wsReprobeInterval
			if c.cfg.testWSReprobeInterval > 0 {
				interval = c.cfg.testWSReprobeInterval
			}
			c.cfg.Logf("realtime: WS transport unavailable (503); using HTTP transport, re-probing WS in %v", interval)
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
		// this kind of diagnosis). Status 503 on this endpoint is the
		// frozen signal that WS_TRANSPORT_ENABLED is off (docs/design/
		// v1-architecture.md section 8.1) — never 403 (that would mean
		// this credential/role isn't permitted, which is not the
		// condition here).
		if resp != nil && resp.StatusCode == http.StatusServiceUnavailable {
			return fmt.Errorf("dial: %w", ErrTransportUnavailable)
		}
		// A 401 at the WS upgrade means this device's credential no longer
		// resolves (revoked/expired token). It is NON-retryable — exactly like
		// a post-hello `unauthenticated` frame — so it must NOT be retried on
		// the ordinary transient backoff, which would hammer the server
		// forever. Stop the reconnect loop (errNoRetry) and surface it to the
		// health signal so a revoked device is visible in the menubar.
		if resp != nil && resp.StatusCode == http.StatusUnauthorized {
			c.reportAuthDegraded("realtime credential rejected (401) — device may be revoked")
			return fmt.Errorf("dial: unauthenticated (401): %w", errNoRetry)
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

	// A completed hello proves the credential authenticates, so clear any
	// previously-reported auth failure (e.g. after the device was re-paired).
	c.reportAuthOK()

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

	// A completed hello is proof the server currently accepts WS,
	// regardless of whether this connection had ever previously been
	// rejected with ErrTransportUnavailable. Replay everything still
	// sitting in the outbox over this connection — flushMu (inside
	// replayPending) ensures this can never overlap an in-flight HTTP
	// fallback flush.
	c.replayPending(ctx, conn)

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

// replayPending flushes every currently-pending outbox row over the WS
// connection, oldest device_seq first. Held under flushMu for its whole
// duration, which is what makes this safe to call both right after hello
// (the mandatory reconnect replay) and on every resend tick/wake without
// ever overlapping an in-flight HTTP fallback flush (flushHTTP holds the
// same lock).
func (c *Client) replayPending(ctx context.Context, conn *websocket.Conn) {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	c.promoteOverflowBestEffort(ctx)

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
			c.retireAck(ctx, p.DeviceID, p.DeviceSeq)
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

// ---- HTTP transport fallback (POST /v1/events) ----

// httpEventEnvelope / httpEventsRequest / httpAckedPair / httpEventResult /
// httpEventsResponse mirror docs/api/v1/openapi.yaml's POST /v1/events
// request/response shapes (docs/design/v1-architecture.md section 4.1) —
// the envelope shape is identical to proto/realtime/envelope.schema.json
// (wire.Envelope), duplicated here rather than reusing wire.Envelope only
// because wire.Envelope's DecodeEnvelope enforces WS-only strictness this
// package doesn't need for building an outgoing HTTP request.
type httpEventEnvelope struct {
	Type string          `json:"type"`
	ID   string          `json:"id"`
	TS   int64           `json:"ts"`
	Body json.RawMessage `json:"body"`
}

type httpEventsRequest struct {
	Events []httpEventEnvelope `json:"events"`
}

type httpAckedPair struct {
	DeviceID  string `json:"device_id"`
	DeviceSeq int64  `json:"device_seq"`
}

type httpEventResult struct {
	Index     int    `json:"index"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	DeviceSeq int64  `json:"device_seq"`
	Revision  int64  `json:"revision"`
}

type httpEventsResponse struct {
	SpaceRevision int64             `json:"space_revision"`
	Acked         []httpAckedPair   `json:"acked"`
	Results       []httpEventResult `json:"results"`
}

// maxEventsPerBatch mirrors openapi.yaml's EventsRequest.events cap
// (docs/design/v1-architecture.md section 4.1): a client with more queued
// events than this makes multiple requests, which is always safe per the
// retry contract there.
const maxEventsPerBatch = 500

// httpFallbackLoop is the HTTP transport's always-alive sender: it wakes on
// ResendInterval or httpWake and, every time, attempts a flush — flushHTTP
// itself no-ops the instant WS is connected or EventsURL is unset, so this
// loop imposes no behavior when the HTTP fallback isn't in use. It runs for
// the Client's entire lifetime rather than being started/stopped around
// each WS outage, which keeps the "is HTTP active right now" question
// purely a data check (Connected()) rather than a second piece of lifecycle
// state to keep in sync with the WS supervisor.
func (c *Client) httpFallbackLoop() {
	defer close(c.httpDone)
	ticker := time.NewTicker(c.cfg.ResendInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closing:
			return
		case <-ticker.C:
		case <-c.httpWake:
		}
		c.flushHTTP(context.Background())
	}
}

// flushHTTP is the HTTP transport's equivalent of replayPending: it drains
// the outbox (plus, opportunistically, any due metric batch) to POST
// /v1/events and retires acked rows via the identical retireAck path the WS
// ack handler uses (docs/design/v1-architecture.md section 4.1 — "the same
// field an implementation should map onto its existing resend queue logic
// unchanged"). Held under flushMu for its whole duration, same as
// replayPending, so the two transports never write concurrently.
func (c *Client) flushHTTP(ctx context.Context) {
	if c.cfg.EventsURL == "" {
		return // HTTP fallback not configured; WS is the only transport
	}

	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	if c.Connected() {
		return // WS is up and already responsible for delivery
	}

	c.promoteOverflowBestEffort(ctx)

	items, err := c.cfg.Outbox.Pending(ctx, c.cfg.Space)
	if err != nil {
		c.cfg.Logf("realtime: http transport: read pending outbox: %v", err)
		return
	}

	samples, haveMetrics := c.metrics.Flush(c.now())

	if len(items) == 0 && !haveMetrics {
		return
	}

	envs := make([]httpEventEnvelope, 0, len(items)+1)
	for _, item := range items {
		if len(envs) >= maxEventsPerBatch {
			break // the rest are picked up by the next tick; Pending() is re-read fresh each time
		}
		envs = append(envs, httpEventEnvelope{
			Type: item.Kind,
			ID:   newEnvelopeID(),
			TS:   c.nowMS(),
			Body: json.RawMessage(item.Body),
		})
	}
	if haveMetrics && len(envs) < maxEventsPerBatch {
		body := wire.MetricFrameBody{DeviceID: c.cfg.DeviceID, Metrics: samples}
		if verr := body.Validate(); verr != nil {
			c.cfg.Logf("realtime: http transport: dropping invalid metric.frame: %v", verr)
		} else if raw, merr := json.Marshal(body); merr == nil {
			envs = append(envs, httpEventEnvelope{Type: wire.TypeMetricFrame, ID: newEnvelopeID(), TS: c.nowMS(), Body: raw})
		}
	}
	if len(envs) == 0 {
		return
	}

	reqBody, err := json.Marshal(httpEventsRequest{Events: envs})
	if err != nil {
		c.cfg.Logf("realtime: http transport: marshal request: %v", err)
		return
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.EventsURL, bytes.NewReader(reqBody))
	if err != nil {
		c.cfg.Logf("realtime: http transport: build request: %v", err)
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.cfg.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		c.cfg.Logf("realtime: http transport: POST /v1/events: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		// Persistent 401 over HTTP too (revoked/expired token): surface it to
		// the health signal so the failure is visible, not silent.
		c.reportAuthDegraded("realtime credential rejected (401) — device may be revoked")
		c.cfg.Logf("realtime: http transport: POST /v1/events: unauthenticated (401)")
		return
	}
	if resp.StatusCode != http.StatusOK {
		c.cfg.Logf("realtime: http transport: POST /v1/events: unexpected status %d", resp.StatusCode)
		return
	}
	// Authenticated successfully — clear any prior auth failure.
	c.reportAuthOK()
	var parsed httpEventsResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		c.cfg.Logf("realtime: http transport: decode response: %v", err)
		return
	}
	for _, p := range parsed.Acked {
		c.retireAck(ctx, p.DeviceID, p.DeviceSeq)
	}
}

// FlushOnce drains and attempts to deliver whatever is currently sitting in
// cfg.Outbox for cfg.Space over ONE POST /v1/events request, then returns —
// no background goroutines, no WS dial, no lifecycle to manage. It exists
// for a short-lived, one-shot caller (`sitrep report`, the Claude Code
// hook's entry point — pre-launch fix, external review round 3) that wants a
// single prompt delivery attempt without paying for a full Client's
// reconnecting-WS machinery when the caller is about to exit either way.
// Durability does not depend on this function succeeding: by the time a
// caller reaches FlushOnce, cfg.Outbox.Enqueue has already durably persisted
// the event (see outbox.Store.Enqueue) — this is purely a best-effort
// prompt-delivery attempt layered on top of that already-durable state,
// functionally the same drain/POST/ack sequence flushHTTP performs for a
// long-lived Client, just invoked once instead of on a resend loop.
//
// Not finding cfg.EventsURL set, or the outbox empty for cfg.Space, is not
// an error — there is simply nothing to do. A delivery failure (network
// error, non-2xx status) is also not returned as an error: the event stays
// durable in cfg.Outbox regardless, so a failed attempt here only means
// delivery is deferred to the next process that flushes this device's
// outbox (another `sitrep report`, `sitrep run`, or a `sitrep agent` with
// the realtime opt-in on) — never silently lost. Failures are logged via
// cfg.Logf. Safe to call concurrently with a live *Client for the same
// store/space: SQLite serializes the store, so a race between this and a
// live Client's own flush just means one of the two sends the batch first;
// the other, if anything remains, harmlessly resends already-acked seqs
// that dedup on the server.
func FlushOnce(ctx context.Context, cfg Config) {
	cfg.applyDefaults()
	if cfg.Outbox == nil || cfg.EventsURL == "" {
		return
	}
	if _, err := cfg.Outbox.PromoteOverflow(ctx, promoteOverflowBatch); err != nil {
		cfg.Logf("realtime: flush once: promote overflow: %v", err)
	}
	items, err := cfg.Outbox.Pending(ctx, cfg.Space)
	if err != nil {
		cfg.Logf("realtime: flush once: read pending outbox: %v", err)
		return
	}
	if len(items) == 0 {
		return
	}
	envs := make([]httpEventEnvelope, 0, len(items))
	for _, item := range items {
		if len(envs) >= maxEventsPerBatch {
			break // the rest are left for the next flush (another caller, or this device's next FlushOnce/Client)
		}
		envs = append(envs, httpEventEnvelope{
			Type: item.Kind,
			ID:   newEnvelopeID(),
			TS:   cfg.Clock.Now().UnixMilli(),
			Body: json.RawMessage(item.Body),
		})
	}
	reqBody, err := json.Marshal(httpEventsRequest{Events: envs})
	if err != nil {
		cfg.Logf("realtime: flush once: marshal request: %v", err)
		return
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.EventsURL, bytes.NewReader(reqBody))
	if err != nil {
		cfg.Logf("realtime: flush once: build request: %v", err)
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if cfg.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+cfg.Token)
	}
	httpClient := &http.Client{Timeout: cfg.HTTPTimeout}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		cfg.Logf("realtime: flush once: POST /v1/events: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		if cfg.OnAuthState != nil {
			cfg.OnAuthState(false, "realtime credential rejected (401) — device may be revoked")
		}
		return
	}
	if resp.StatusCode != http.StatusOK {
		cfg.Logf("realtime: flush once: POST /v1/events: unexpected status %d", resp.StatusCode)
		return
	}
	if cfg.OnAuthState != nil {
		cfg.OnAuthState(true, "")
	}
	var parsed httpEventsResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		cfg.Logf("realtime: flush once: decode response: %v", err)
		return
	}
	for _, p := range parsed.Acked {
		if p.DeviceID != cfg.DeviceID {
			continue
		}
		if err := cfg.Outbox.Ack(ctx, cfg.Space, p.DeviceSeq); err != nil {
			cfg.Logf("realtime: flush once: ack seq %d: %v", p.DeviceSeq, err)
		}
	}
}
