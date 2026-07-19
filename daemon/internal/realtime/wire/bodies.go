package wire

import (
	"encoding/json"
	"fmt"
)

// DisplayHints mirrors common.schema.json#/$defs/display_hints.
type DisplayHints struct {
	Icon     string `json:"icon,omitempty"`
	Tint     string `json:"tint,omitempty"`
	Template string `json:"template,omitempty"`
}

// TaskState mirrors common.schema.json#/$defs/task_state.
type TaskState struct {
	TaskID    string        `json:"task_id"`
	DeviceID  string        `json:"device_id"`
	Title     string        `json:"title,omitempty"`
	State     string        `json:"state"` // running|done|failed
	Percent   *int          `json:"percent,omitempty"`
	Step      string        `json:"step,omitempty"`
	Message   string        `json:"message,omitempty"`
	UpdatedAt int64         `json:"updated_at"`
	Display   *DisplayHints `json:"display,omitempty"`
}

// MetricSample mirrors common.schema.json#/$defs/metric_sample.
type MetricSample struct {
	MetricID   string        `json:"metric_id"`
	Value      string        `json:"value"`
	Label      string        `json:"label,omitempty"`
	TS         int64         `json:"ts"`
	Display    *DisplayHints `json:"display,omitempty"`
	Target     string        `json:"target,omitempty"`
	Min        string        `json:"min,omitempty"`
	Max        string        `json:"max,omitempty"`
	AlertAbove string        `json:"alert_above,omitempty"`
	AlertBelow string        `json:"alert_below,omitempty"`
}

// Schedule mirrors the schedule object nested in automation_state.
type Schedule struct {
	Kind         string `json:"kind"` // always "interval" in v1
	EverySeconds int    `json:"every_seconds"`
}

// AutomationState mirrors common.schema.json#/$defs/automation_state.
type AutomationState struct {
	AutomationID string   `json:"automation_id"`
	Name         string   `json:"name"`
	ExecutorKind string   `json:"executor_kind"` // script|agent|hybrid
	Schedule     Schedule `json:"schedule"`
	State        string   `json:"state"` // active|paused
	LastRunAt    int64    `json:"last_run_at,omitempty"`
}

// MessageRecord mirrors common.schema.json#/$defs/message_record.
type MessageRecord struct {
	MessageID  string `json:"message_id"`
	DeviceID   string `json:"device_id"`
	Level      string `json:"level"`
	Text       string `json:"text"`
	OccurredAt int64  `json:"occurred_at"`
}

// ---- hello (messages/hello.schema.json) ----

// HelloOffer is sent by the connecting device, client -> server.
//
// Capabilities is a pointer so an explicitly-present-but-empty
// "capabilities": [] round-trips distinctly from an entirely absent field
// (both are valid on the wire; schema default is [] when omitted).
type HelloOffer struct {
	Stage            string    `json:"stage"` // const "offer"
	DeviceID         string    `json:"device_id"`
	Role             string    `json:"role"` // source|viewer
	ProtocolVersions []int     `json:"protocol_versions"`
	Capabilities     *[]string `json:"capabilities,omitempty"`
}

// HelloAccept is sent by the server once it has negotiated a version.
type HelloAccept struct {
	Stage               string    `json:"stage"` // const "accept"
	ProtocolVersion     int       `json:"protocol_version"`
	SessionID           string    `json:"session_id"`
	Capabilities        *[]string `json:"capabilities,omitempty"`
	HeartbeatIntervalMS int       `json:"heartbeat_interval_ms"`
}

// HelloBody is the oneOf{offer, accept} discriminated union keyed by `stage`.
// Exactly one of Offer/Accept is non-nil after decode.
type HelloBody struct {
	Offer  *HelloOffer
	Accept *HelloAccept
}

func (h HelloBody) MarshalJSON() ([]byte, error) {
	switch {
	case h.Offer != nil:
		return json.Marshal(h.Offer)
	case h.Accept != nil:
		return json.Marshal(h.Accept)
	default:
		return nil, fmt.Errorf("hello body: neither offer nor accept set")
	}
}

func (h *HelloBody) UnmarshalJSON(data []byte) error {
	var disc struct {
		Stage string `json:"stage"`
	}
	if err := json.Unmarshal(data, &disc); err != nil {
		return err
	}
	switch disc.Stage {
	case "offer":
		var o HelloOffer
		if err := json.Unmarshal(data, &o); err != nil {
			return err
		}
		h.Offer = &o
	case "accept":
		var a HelloAccept
		if err := json.Unmarshal(data, &a); err != nil {
			return err
		}
		h.Accept = &a
	default:
		return fmt.Errorf("hello body: unknown stage %q", disc.Stage)
	}
	return nil
}

// ---- resume (messages/resume.schema.json) ----

type ResumeBody struct {
	LastRevision int64 `json:"last_revision"`
}

// ---- snapshot (messages/snapshot.schema.json) ----

type SnapshotBody struct {
	Revision    int64             `json:"revision"`
	Part        int               `json:"part"`
	Final       bool              `json:"final"`
	Tasks       []TaskState       `json:"tasks"`
	Metrics     []MetricSample    `json:"metrics"`
	Messages    []MessageRecord   `json:"messages"`
	Automations []AutomationState `json:"automations"`
}

// ---- reliable events ----

// TaskEventBody mirrors messages/task.event.schema.json#/$defs/body.
type TaskEventBody struct {
	DeviceID   string        `json:"device_id"`
	DeviceSeq  int64         `json:"device_seq"`
	TaskID     string        `json:"task_id"`
	Kind       string        `json:"kind"` // started|progress|step|done|failed
	OccurredAt int64         `json:"occurred_at"`
	Title      string        `json:"title,omitempty"`
	Percent    *int          `json:"percent,omitempty"`
	Step       string        `json:"step,omitempty"`
	Message    string        `json:"message,omitempty"`
	Display    *DisplayHints `json:"display,omitempty"`
}

// MessageEventBody mirrors messages/message.event.schema.json#/$defs/body.
type MessageEventBody struct {
	DeviceID     string `json:"device_id"`
	DeviceSeq    int64  `json:"device_seq"`
	MessageID    string `json:"message_id"`
	Level        string `json:"level"` // info|warn|error
	Text         string `json:"text"`
	OccurredAt   int64  `json:"occurred_at"`
	AutomationID string `json:"automation_id,omitempty"`
}

// ConfigEventBody mirrors messages/config.event.schema.json#/$defs/body.
// Server-minted only: no device_id/device_seq (SPEC.md section 5.5).
type ConfigEventBody struct {
	Kind         string           `json:"kind"` // automation.upserted|automation.removed
	AutomationID string           `json:"automation_id"`
	Automation   *AutomationState `json:"automation,omitempty"`
	OccurredAt   int64            `json:"occurred_at"`
}

// ---- metric.frame (messages/metric.frame.schema.json) ----

type MetricFrameBody struct {
	DeviceID string         `json:"device_id"`
	Metrics  []MetricSample `json:"metrics"`
}

// ---- delta (messages/delta.schema.json) ----

// DeltaEventItem is one oneOf{task.event, message.event, config.event} entry
// of delta.body.events, tagged by event_type.
type DeltaEventItem struct {
	EventType    string
	TaskEvent    *TaskEventBody
	MessageEvent *MessageEventBody
	ConfigEvent  *ConfigEventBody
}

func (d DeltaEventItem) MarshalJSON() ([]byte, error) {
	var wrapper struct {
		EventType string `json:"event_type"`
		Event     any    `json:"event"`
	}
	wrapper.EventType = d.EventType
	switch d.EventType {
	case TypeTaskEvent:
		wrapper.Event = d.TaskEvent
	case TypeMessageEvent:
		wrapper.Event = d.MessageEvent
	case TypeConfigEvent:
		wrapper.Event = d.ConfigEvent
	default:
		return nil, fmt.Errorf("delta event: unknown event_type %q", d.EventType)
	}
	return json.Marshal(wrapper)
}

func (d *DeltaEventItem) UnmarshalJSON(data []byte) error {
	var wrapper struct {
		EventType string          `json:"event_type"`
		Event     json.RawMessage `json:"event"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return err
	}
	d.EventType = wrapper.EventType
	switch wrapper.EventType {
	case TypeTaskEvent:
		var b TaskEventBody
		if err := json.Unmarshal(wrapper.Event, &b); err != nil {
			return err
		}
		d.TaskEvent = &b
	case TypeMessageEvent:
		var b MessageEventBody
		if err := json.Unmarshal(wrapper.Event, &b); err != nil {
			return err
		}
		d.MessageEvent = &b
	case TypeConfigEvent:
		var b ConfigEventBody
		if err := json.Unmarshal(wrapper.Event, &b); err != nil {
			return err
		}
		d.ConfigEvent = &b
	default:
		return fmt.Errorf("delta event: unknown event_type %q", wrapper.EventType)
	}
	return nil
}

type DeltaBody struct {
	FromRevision int64            `json:"from_revision"`
	ToRevision   int64            `json:"to_revision"`
	Events       []DeltaEventItem `json:"events"`
}

// ---- ack (messages/ack.schema.json) ----

type AckedPair struct {
	DeviceID  string `json:"device_id"`
	DeviceSeq int64  `json:"device_seq"`
}

type Lease struct {
	ExpiresAt int64 `json:"expires_at"`
}

type AckBody struct {
	Acked     []AckedPair `json:"acked,omitempty"`
	InReplyTo string      `json:"in_reply_to,omitempty"`
	Lease     *Lease      `json:"lease,omitempty"`
}

// ---- subscribe / interest.renew (messages/subscribe.schema.json) ----

// SubscribeBody is shared verbatim by subscribe and interest.renew
// (interest.renew.schema.json $refs subscribe's body).
type SubscribeBody struct {
	Topics []string `json:"topics,omitempty"`
}

// UnsubscribeBody is intentionally empty (messages/unsubscribe.schema.json).
type UnsubscribeBody struct{}

// ---- command (messages/command.schema.json) ----

type CommandBody struct {
	CommandID        string `json:"command_id"`
	Origin           string `json:"origin"` // viewer|server
	IssuedByDeviceID string `json:"issued_by_device_id,omitempty"`
	Action           string `json:"action"`
	TaskID           string `json:"task_id,omitempty"`
	AutomationID     string `json:"automation_id,omitempty"`
	TargetDeviceID   string `json:"target_device_id,omitempty"`
	TTLMs            int64  `json:"ttl_ms"`
	// Params is a pointer for the same reason as HelloOffer.Capabilities:
	// an explicit "params": {} must round-trip distinctly from an absent
	// field, even though both mean "no extension keys" (protocol v1 defines
	// none) per messages/command.schema.json.
	Params *map[string]any `json:"params,omitempty"`
}

// ---- error (messages/error.schema.json) ----

type ErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	InReplyTo string `json:"in_reply_to,omitempty"`
	Retryable *bool  `json:"retryable"`
	Fatal     *bool  `json:"fatal"`
}

// Error codes, SPEC.md section 13.
const (
	ErrVersionUnsupported  = "version_unsupported"
	ErrHelloRequired       = "hello_required"
	ErrUnauthenticated     = "unauthenticated"
	ErrUnauthorized        = "unauthorized"
	ErrMalformed           = "malformed"
	ErrRateLimited         = "rate_limited"
	ErrFrameTooLarge       = "frame_too_large"
	ErrBatchTooLarge       = "batch_too_large"
	ErrRevisionUnavailable = "revision_unavailable"
	ErrCommandExpired      = "command_expired"
	ErrSequenceGap         = "sequence_gap"
	ErrSuperseded          = "superseded"
	ErrInternal            = "internal_error"
)
