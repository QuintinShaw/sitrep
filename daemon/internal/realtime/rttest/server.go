// Package rttest is a test-only, minimal WebSocket + HTTP server used to
// exercise the realtime client (internal/realtime/client) against scripted
// server behavior: hello accept, acking, withholding acks, dropping
// connections, sending commands, the ping/pong heartbeat, and — for the v1
// HTTP uplink transport — POST /v1/events with the frozen ACK shape
// (docs/design/v1-architecture.md section 4.1, docs/api/v1/openapi.yaml).
//
// This is NOT a reimplementation of the real Sitrep server: it does not
// persist state, fold events, track space_revision, manage interest
// leases, or implement anything resembling SpaceHub. It exists purely so
// daemon-side tests can drive the client through the connection sequences
// SPEC.md describes, and through the HTTP ACK contract, without a network
// dependency on a real server. It must never be described as, or grow
// into, a self-hosted server implementation.
package rttest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"

	"github.com/coder/websocket"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

// Conn wraps one accepted connection with envelope-level read/write helpers.
type Conn struct {
	ws  *websocket.Conn
	ctx context.Context

	// Request carries the *http.Request that established this connection,
	// e.g. for asserting on the Authorization header a real client sends
	// (SPEC.md section 10: the credential is presented once, out of band,
	// at connection establishment).
	Request *http.Request
}

// ReadEnvelope reads one frame and decodes it as an envelope. A literal
// "ping"/"pong" text frame is returned as a sentinel error so callers can
// distinguish it from a decode failure.
var ErrPing = fmt.Errorf("rttest: received ping")
var ErrPong = fmt.Errorf("rttest: received pong")

func (c *Conn) ReadEnvelope() (wire.Envelope, error) {
	typ, data, err := c.ws.Read(c.ctx)
	if err != nil {
		return wire.Envelope{}, err
	}
	if typ == websocket.MessageText {
		switch string(data) {
		case "ping":
			return wire.Envelope{}, ErrPing
		case "pong":
			return wire.Envelope{}, ErrPong
		}
	}
	return wire.DecodeEnvelope(data)
}

// WriteEnvelope encodes and sends one envelope.
func (c *Conn) WriteEnvelope(env wire.Envelope) error {
	data, err := env.Encode()
	if err != nil {
		return err
	}
	return c.ws.Write(c.ctx, websocket.MessageText, data)
}

// WritePing / WritePong send the bare heartbeat text frames (SPEC.md
// section 9.3) — deliberately not JSON envelopes.
func (c *Conn) WritePing() error { return c.ws.Write(c.ctx, websocket.MessageText, []byte("ping")) }
func (c *Conn) WritePong() error { return c.ws.Write(c.ctx, websocket.MessageText, []byte("pong")) }

// Close closes the connection with the given close code/reason.
func (c *Conn) Close(reason string) error {
	return c.ws.Close(websocket.StatusNormalClosure, reason)
}

// HelloAccept performs the mandatory first exchange (SPEC.md section 9):
// reads the client's hello{stage: offer}, validates it is well-formed, and
// replies hello{stage: accept} with the given session id and heartbeat
// interval. It returns the decoded offer for the caller's assertions.
func (c *Conn) HelloAccept(sessionID string, heartbeatMS int) (wire.HelloOffer, error) {
	env, err := c.ReadEnvelope()
	if err != nil {
		return wire.HelloOffer{}, fmt.Errorf("rttest: read hello offer: %w", err)
	}
	if env.Type != wire.TypeHello {
		return wire.HelloOffer{}, fmt.Errorf("rttest: expected hello, got %q", env.Type)
	}
	body, err := wire.DecodeBody(env)
	if err != nil {
		return wire.HelloOffer{}, fmt.Errorf("rttest: decode hello offer: %w", err)
	}
	hb := body.(wire.HelloBody)
	if hb.Offer == nil {
		return wire.HelloOffer{}, fmt.Errorf("rttest: expected hello offer, got accept")
	}
	accept := wire.HelloAccept{
		Stage:               "accept",
		ProtocolVersion:     1,
		SessionID:           sessionID,
		HeartbeatIntervalMS: heartbeatMS,
	}
	acceptEnv, err := wire.NewEnvelope(wire.TypeHello, newID(), NowMS(), wire.HelloBody{Accept: &accept})
	if err != nil {
		return wire.HelloOffer{}, err
	}
	if err := c.WriteEnvelope(acceptEnv); err != nil {
		return wire.HelloOffer{}, err
	}
	return *hb.Offer, nil
}

// Ack acknowledges one reliable event.
func (c *Conn) Ack(deviceID string, seq int64) error {
	body := wire.AckBody{Acked: []wire.AckedPair{{DeviceID: deviceID, DeviceSeq: seq}}}
	env, err := wire.NewEnvelope(wire.TypeAck, newID(), NowMS(), body)
	if err != nil {
		return err
	}
	return c.WriteEnvelope(env)
}

// SendCommand sends a server-originated command (e.g. throttle/resume_rate).
func (c *Conn) SendCommand(body wire.CommandBody) error {
	env, err := wire.NewEnvelope(wire.TypeCommand, newID(), NowMS(), body)
	if err != nil {
		return err
	}
	return c.WriteEnvelope(env)
}

// SendError sends an error envelope (e.g. superseded).
func (c *Conn) SendError(body wire.ErrorBody) error {
	env, err := wire.NewEnvelope(wire.TypeError, newID(), NowMS(), body)
	if err != nil {
		return err
	}
	return c.WriteEnvelope(env)
}

// Handler is invoked once per accepted connection, in its own goroutine.
// The handler owns the connection's lifetime: when it returns, the
// connection is closed.
type Handler func(*Conn)

// Server is a scriptable mock realtime endpoint bound to an httptest
// server. It serves the WebSocket upgrade at "/" (see Handler) and, when an
// events handler is installed (SetEventsHandler / NewWithEvents), HTTP POST
// /v1/events at a fixed path — the two are independent so a test can
// exercise either transport, or both against the same client instance, per
// docs/design/v1-architecture.md section 0 ("degradation switches
// transport only").
type Server struct {
	httpSrv *httptest.Server
	handler Handler

	mu    sync.Mutex
	conns []*Conn

	eventsMu      sync.Mutex
	eventsHandler http.HandlerFunc
}

// New starts a mock server that invokes handler for every accepted
// connection. Call URL() to get the ws:// address for a client to dial.
// POST /v1/events 404s until SetEventsHandler installs a handler.
func New(handler Handler) *Server {
	s := &Server{handler: handler}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.serve)
	mux.HandleFunc("/v1/events", s.serveEvents)
	s.httpSrv = httptest.NewServer(mux)
	return s
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	conn := &Conn{ws: ws, ctx: r.Context(), Request: r}
	s.mu.Lock()
	s.conns = append(s.conns, conn)
	s.mu.Unlock()
	s.handler(conn)
	ws.Close(websocket.StatusNormalClosure, "handler done")
}

func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request) {
	s.eventsMu.Lock()
	h := s.eventsHandler
	s.eventsMu.Unlock()
	if h == nil {
		http.NotFound(w, r)
		return
	}
	h(w, r)
}

// SetEventsHandler installs (or replaces) the handler for POST /v1/events.
// Pair with AckingEventsHandler for the common "accept and ack every
// envelope" case, or write a custom http.HandlerFunc using the
// EventsRequest/EventsResponse types below for scripted per-test behavior
// (partial acks, 503s, malformed responses, ...).
func (s *Server) SetEventsHandler(h http.HandlerFunc) {
	s.eventsMu.Lock()
	s.eventsHandler = h
	s.eventsMu.Unlock()
}

// EventsURL returns the http(s) URL for POST /v1/events on this mock.
func (s *Server) EventsURL() string { return s.httpSrv.URL + "/v1/events" }

// URL returns the ws:// URL clients should dial.
func (s *Server) URL() string {
	return "ws" + strings.TrimPrefix(s.httpSrv.URL, "http")
}

// Close shuts down the underlying HTTP server.
func (s *Server) Close() { s.httpSrv.Close() }
