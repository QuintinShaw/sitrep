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

	metrics *metricBatcher

	seenMu    sync.Mutex
	seenOrder []string
	seen      map[string]bool

	supersededPenalty atomic.Bool

	closing chan struct{}
	closed  chan struct{}
	once    sync.Once
}

// New starts a Client connecting in the background.
func New(cfg Config) *Client {
	cfg.applyDefaults()
	c := &Client{
		cfg:     cfg,
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
	item, err := c.cfg.Outbox.Enqueue(context.Background(), c.cfg.Space, wire.TypeTaskEvent, func(seq int64) (json.RawMessage, error) {
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
			return nil, err
		}
		return json.Marshal(body)
	})
	if err != nil {
		return err
	}
	c.trySendNow(item)
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
	item, err := c.cfg.Outbox.Enqueue(context.Background(), c.cfg.Space, wire.TypeMessageEvent, func(seq int64) (json.RawMessage, error) {
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
			return nil, err
		}
		return json.Marshal(body)
	})
	if err != nil {
		return err
	}
	c.trySendNow(item)
	return nil
}

// SendMetric offers one best-effort metric sample. It is merged with any
// other pending sample for the same MetricID and flushed on the client's
// own cadence (SPEC.md section 12); it is never persisted, never
// retransmitted, and silently superseded by the next sample if lost.
func (c *Client) SendMetric(s wire.MetricSample) {
	c.metrics.Offer(s)
}

// trySendNow best-effort writes one outbox item immediately if a
// connection is currently up. On any failure it does nothing further: the
// resend loop and the next reconnect's replay will pick it up.
func (c *Client) trySendNow(item outbox.Item) {
	c.mu.Lock()
	conn := c.conn
	connected := c.connected
	c.mu.Unlock()
	if !connected || conn == nil {
		return
	}
	env, err := wire.NewEnvelope(item.Kind, newEnvelopeID(), c.nowMS(), json.RawMessage(item.Body))
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data, err := env.Encode()
	if err != nil {
		return
	}
	_ = conn.Write(ctx, websocket.MessageText, data)
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
	conn, _, err := websocket.Dial(dialCtx, c.cfg.URL, &websocket.DialOptions{HTTPHeader: header})
	dialCancel()
	if err != nil {
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
		for _, p := range ab.Acked {
			if p.DeviceID != c.cfg.DeviceID {
				continue // malformed per SPEC.md 5.3; ignore rather than trust
			}
			if err := c.cfg.Outbox.Ack(ctx, c.cfg.Space, p.DeviceSeq); err != nil {
				c.cfg.Logf("realtime: ack seq %d: %v", p.DeviceSeq, err)
			}
		}
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
		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		errBody := wire.ErrorBody{
			Code:      wire.ErrCommandExpired,
			Message:   "command ttl window elapsed",
			InReplyTo: env.ID,
			Retryable: boolPtr(false),
			Fatal:     boolPtr(false),
		}
		if eenv, eerr := wire.NewEnvelope(wire.TypeError, newEnvelopeID(), c.nowMS(), errBody); eerr == nil {
			if data, derr := eenv.Encode(); derr == nil {
				_ = conn.Write(writeCtx, websocket.MessageText, data)
			}
		}
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
