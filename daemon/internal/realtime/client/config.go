// Package client is the daemon-side realtime protocol client
// (proto/realtime/SPEC.md). It connects to the server as a **source**
// device only — the viewer role (subscribe/resume/snapshot/delta) belongs
// to a different consumer (the phone/menu-bar apps) and is out of scope
// here; see the wire package's authorization matrix for the full role set.
//
// Responsibilities:
//   - Maintain one outbound WebSocket connection with the mandatory hello
//     offer/accept sequence, reconnecting with exponential backoff + jitter
//     on any drop.
//   - Persist every reliable event (task.event/message.event) to a local
//     outbox before attempting delivery, retire it on a matching ack, and
//     replay every still-unacked event oldest-first after each reconnect.
//   - Merge and rate-limit metric samples into metric.frame batches,
//     honoring server-issued throttle/resume_rate commands.
//   - Answer the ping/pong heartbeat and detect a silent peer.
package client

import (
	"math/rand"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/outbox"
)

// Clock abstracts time for components that need deterministic tests. The
// live Client uses time.Now()/time.Sleep directly (see run/connectAndServe);
// Clock exists for the pure, sleep-free pieces (Backoff, metricBatcher) that
// tests drive with an injected instant instead of waiting in real time.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Logf is the client's pluggable logger; nil means "discard".
type Logf func(format string, args ...any)

// CommandAction is a validated, deduplicated, not-yet-expired command this
// client is not itself responsible for executing (anything other than the
// throttle/resume_rate pair it handles internally) — pause/resume/stop/
// run_now. Forwarded to Config.OnCommand, if set, for the caller to wire
// into process control; this package does not dispatch these itself
// (that plumbing already exists on the HTTP uplink path, see
// internal/uplink.Uplink.Commands, and is out of scope for this package).
type CommandAction struct {
	CommandID      string
	Action         string // pause|resume|stop|run_now
	TaskID         string
	AutomationID   string
	TargetDeviceID string
}

// Config configures a Client. Zero-value fields with a documented default
// are filled in by New.
type Config struct {
	// URL is the ws:// or wss:// endpoint to dial.
	URL string
	// Token is presented once at connection establishment (SPEC.md section
	// 10), as an Authorization: Bearer header — never repeated in an
	// envelope.
	Token string
	// DeviceID and Space identify this connection's hello{device_id} and
	// the (device, space) scope its device_seq counter and outbox are
	// bound to (SPEC.md section 5.1). One Client serves exactly one space.
	DeviceID string
	Space    string

	// Outbox is the durable reliable-event queue. Required.
	Outbox *outbox.Store

	// ProtocolVersions defaults to []int{1}.
	ProtocolVersions []int

	// HeartbeatTimeoutFactor multiplies the server-advertised
	// heartbeat_interval_ms to decide when a silent peer is dead (SPEC.md
	// section 9.3 recommends 2x). Defaults to 2.
	HeartbeatTimeoutFactor int

	// Reconnect backoff (SPEC.md work order: "1s起, 2倍, 上限60s, ±20% jitter").
	BackoffBase   time.Duration // default 1s
	BackoffMax    time.Duration // default 60s
	BackoffJitter float64       // default 0.2

	// ResendInterval is how often, while connected, the client re-attempts
	// delivery of anything still sitting unacked in the outbox (belt and
	// suspenders on top of the mandatory post-reconnect replay).
	ResendInterval time.Duration // default 5s

	// MetricFlushInterval is the normal-cadence poll period for the metric
	// batcher; MetricThrottledInterval replaces it while under a server
	// throttle command (SPEC.md section 7: "an implementation-defined
	// lower rate"). The 2 Hz per-metric / 10 Hz per-connection protocol
	// ceilings (section 11/12) are enforced independently of this value.
	MetricFlushInterval     time.Duration // default 200ms
	MetricThrottledInterval time.Duration // default 30s

	// HelloTimeout bounds how long to wait for hello{accept} after sending
	// the offer (SPEC.md section 9.1 recommends 10s).
	HelloTimeout time.Duration // default 10s

	Clock Clock
	Rand  func() float64 // returns [0,1); defaults to a package-level RNG

	OnCommand func(CommandAction)
	Logf      Logf
}

func (c *Config) applyDefaults() {
	if len(c.ProtocolVersions) == 0 {
		c.ProtocolVersions = []int{1}
	}
	if c.HeartbeatTimeoutFactor <= 0 {
		c.HeartbeatTimeoutFactor = 2
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = time.Second
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 60 * time.Second
	}
	if c.BackoffJitter == 0 {
		c.BackoffJitter = 0.2
	}
	if c.ResendInterval <= 0 {
		c.ResendInterval = 5 * time.Second
	}
	if c.MetricFlushInterval <= 0 {
		c.MetricFlushInterval = 200 * time.Millisecond
	}
	if c.MetricThrottledInterval <= 0 {
		c.MetricThrottledInterval = 30 * time.Second
	}
	if c.HelloTimeout <= 0 {
		c.HelloTimeout = 10 * time.Second
	}
	if c.Clock == nil {
		c.Clock = realClock{}
	}
	if c.Rand == nil {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		c.Rand = rng.Float64
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
}
