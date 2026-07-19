package wire

import (
	"encoding/json"
	"fmt"
)

// Field length caps from common.schema.json.
const (
	freeTextMax    = 2048 // $defs/free_text
	labelTextMax   = 256  // $defs/label_text
	metricValueMax = 256  // metric_sample.value
)

// DecodeBody decodes env.Body into the concrete Go type for env.Type and
// validates it against the semantic rules this package mirrors from the
// JSON Schemas (required fields, enums, ranges, conditional requirements).
// It returns an error for any envelope whose type is not one of the 14
// registered types; per SPEC.md section 15 a caller that only needs
// forward-compatible tolerance should check KnownType(env.Type) before
// calling DecodeBody and ignore (not reject) an unrecognized type.
func DecodeBody(env Envelope) (any, error) {
	switch env.Type {
	case TypeHello:
		var b HelloBody
		if err := json.Unmarshal(env.Body, &b); err != nil {
			return nil, err
		}
		if err := b.Validate(); err != nil {
			return nil, err
		}
		return b, nil
	case TypeResume:
		var b ResumeBody
		if err := json.Unmarshal(env.Body, &b); err != nil {
			return nil, err
		}
		if err := b.Validate(); err != nil {
			return nil, err
		}
		return b, nil
	case TypeSnapshot:
		var b SnapshotBody
		if err := json.Unmarshal(env.Body, &b); err != nil {
			return nil, err
		}
		if err := b.Validate(); err != nil {
			return nil, err
		}
		return b, nil
	case TypeDelta:
		var b DeltaBody
		if err := json.Unmarshal(env.Body, &b); err != nil {
			return nil, err
		}
		if err := b.Validate(); err != nil {
			return nil, err
		}
		return b, nil
	case TypeAck:
		var b AckBody
		if err := json.Unmarshal(env.Body, &b); err != nil {
			return nil, err
		}
		if err := b.Validate(); err != nil {
			return nil, err
		}
		return b, nil
	case TypeTaskEvent:
		var b TaskEventBody
		if err := json.Unmarshal(env.Body, &b); err != nil {
			return nil, err
		}
		if err := b.Validate(); err != nil {
			return nil, err
		}
		return b, nil
	case TypeMessageEvent:
		var b MessageEventBody
		if err := json.Unmarshal(env.Body, &b); err != nil {
			return nil, err
		}
		if err := b.Validate(); err != nil {
			return nil, err
		}
		return b, nil
	case TypeMetricFrame:
		var b MetricFrameBody
		if err := json.Unmarshal(env.Body, &b); err != nil {
			return nil, err
		}
		if err := b.Validate(); err != nil {
			return nil, err
		}
		return b, nil
	case TypeConfigEvent:
		var b ConfigEventBody
		if err := json.Unmarshal(env.Body, &b); err != nil {
			return nil, err
		}
		if err := b.Validate(); err != nil {
			return nil, err
		}
		return b, nil
	case TypeSubscribe, TypeInterestRenew:
		var b SubscribeBody
		if err := json.Unmarshal(env.Body, &b); err != nil {
			return nil, err
		}
		if err := b.Validate(); err != nil {
			return nil, err
		}
		return b, nil
	case TypeUnsubscribe:
		var b UnsubscribeBody
		if err := json.Unmarshal(env.Body, &b); err != nil {
			return nil, err
		}
		return b, nil
	case TypeCommand:
		var b CommandBody
		if err := json.Unmarshal(env.Body, &b); err != nil {
			return nil, err
		}
		if err := b.Validate(); err != nil {
			return nil, err
		}
		return b, nil
	case TypeError:
		var b ErrorBody
		if err := json.Unmarshal(env.Body, &b); err != nil {
			return nil, err
		}
		if err := b.Validate(); err != nil {
			return nil, err
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unknown message type %q", env.Type)
	}
}

func (h HelloBody) Validate() error {
	switch {
	case h.Offer != nil:
		o := h.Offer
		if o.DeviceID == "" || !ValidDeviceID(o.DeviceID) {
			return fmt.Errorf("hello offer: invalid device_id %q", o.DeviceID)
		}
		if o.Role != "source" && o.Role != "viewer" {
			return fmt.Errorf("hello offer: invalid role %q", o.Role)
		}
		if len(o.ProtocolVersions) == 0 {
			return fmt.Errorf("hello offer: protocol_versions must not be empty")
		}
		for _, v := range o.ProtocolVersions {
			if v < 1 {
				return fmt.Errorf("hello offer: protocol_versions entries must be >= 1")
			}
		}
		return nil
	case h.Accept != nil:
		a := h.Accept
		if a.ProtocolVersion < 1 {
			return fmt.Errorf("hello accept: invalid protocol_version %d", a.ProtocolVersion)
		}
		if a.SessionID == "" || len(a.SessionID) > 64 {
			return fmt.Errorf("hello accept: invalid session_id")
		}
		if a.HeartbeatIntervalMS < 1000 || a.HeartbeatIntervalMS > 300000 {
			return fmt.Errorf("hello accept: heartbeat_interval_ms out of range")
		}
		return nil
	default:
		return fmt.Errorf("hello body: neither offer nor accept present")
	}
}

func (r ResumeBody) Validate() error {
	if r.LastRevision < 0 {
		return fmt.Errorf("resume: last_revision must be >= 0, got %d", r.LastRevision)
	}
	return nil
}

func (s SnapshotBody) Validate() error {
	if s.Revision < 0 {
		return fmt.Errorf("snapshot: revision must be >= 0")
	}
	if s.Part < 1 {
		return fmt.Errorf("snapshot: part must be >= 1")
	}
	for i, t := range s.Tasks {
		if err := t.Validate(); err != nil {
			return fmt.Errorf("snapshot: tasks[%d]: %w", i, err)
		}
	}
	for i, m := range s.Metrics {
		if err := m.Validate(); err != nil {
			return fmt.Errorf("snapshot: metrics[%d]: %w", i, err)
		}
	}
	return nil
}

func (t TaskState) Validate() error {
	if t.State != "running" && t.State != "done" && t.State != "failed" {
		return fmt.Errorf("task_state: invalid state %q", t.State)
	}
	if !ValidUnixMS(t.UpdatedAt) {
		return fmt.Errorf("task_state: updated_at is not a plausible unix-ms timestamp")
	}
	if len(t.Title) > freeTextMax || len(t.Step) > freeTextMax || len(t.Message) > freeTextMax {
		return fmt.Errorf("task_state: free-text field exceeds %d chars", freeTextMax)
	}
	return nil
}

func (m MetricSample) Validate() error {
	if len(m.Value) > metricValueMax {
		return fmt.Errorf("metric_sample: value exceeds %d chars", metricValueMax)
	}
	if len(m.Label) > labelTextMax {
		return fmt.Errorf("metric_sample: label exceeds %d chars", labelTextMax)
	}
	if !ValidUnixMS(m.TS) {
		return fmt.Errorf("metric_sample: ts is not a plausible unix-ms timestamp")
	}
	return nil
}

func (b DeltaBody) Validate() error {
	if b.FromRevision < 0 || b.ToRevision < 0 {
		return fmt.Errorf("delta: revisions must be >= 0")
	}
	if b.ToRevision-b.FromRevision != int64(len(b.Events)) {
		return fmt.Errorf("delta: to_revision - from_revision (%d) must equal events.length (%d)",
			b.ToRevision-b.FromRevision, len(b.Events))
	}
	for i, e := range b.Events {
		switch e.EventType {
		case TypeTaskEvent:
			if e.TaskEvent == nil {
				return fmt.Errorf("delta: events[%d]: missing task.event body", i)
			}
			if err := e.TaskEvent.Validate(); err != nil {
				return fmt.Errorf("delta: events[%d]: %w", i, err)
			}
		case TypeMessageEvent:
			if e.MessageEvent == nil {
				return fmt.Errorf("delta: events[%d]: missing message.event body", i)
			}
			if err := e.MessageEvent.Validate(); err != nil {
				return fmt.Errorf("delta: events[%d]: %w", i, err)
			}
		case TypeConfigEvent:
			if e.ConfigEvent == nil {
				return fmt.Errorf("delta: events[%d]: missing config.event body", i)
			}
			if err := e.ConfigEvent.Validate(); err != nil {
				return fmt.Errorf("delta: events[%d]: %w", i, err)
			}
		default:
			return fmt.Errorf("delta: events[%d]: unknown event_type %q", i, e.EventType)
		}
	}
	return nil
}

func (a AckBody) Validate() error {
	if len(a.Acked) == 0 && a.InReplyTo == "" {
		return fmt.Errorf("ack: at least one of acked or in_reply_to is required")
	}
	if a.InReplyTo != "" && !ValidEnvelopeID(a.InReplyTo) {
		return fmt.Errorf("ack: in_reply_to is not a valid envelope id")
	}
	for i, p := range a.Acked {
		if !ValidDeviceID(p.DeviceID) {
			return fmt.Errorf("ack: acked[%d]: invalid device_id", i)
		}
		if p.DeviceSeq < 1 {
			return fmt.Errorf("ack: acked[%d]: device_seq must be >= 1", i)
		}
	}
	if a.Lease != nil && !ValidUnixMS(a.Lease.ExpiresAt) {
		return fmt.Errorf("ack: lease.expires_at is not a plausible unix-ms timestamp")
	}
	return nil
}

func (t TaskEventBody) Validate() error {
	if !ValidDeviceID(t.DeviceID) {
		return fmt.Errorf("task.event: invalid device_id %q", t.DeviceID)
	}
	if t.DeviceSeq < 1 {
		return fmt.Errorf("task.event: device_seq must be >= 1 (missing or invalid)")
	}
	if t.TaskID == "" {
		return fmt.Errorf("task.event: missing task_id")
	}
	switch t.Kind {
	case "started", "progress", "step", "done", "failed":
	default:
		return fmt.Errorf("task.event: invalid kind %q", t.Kind)
	}
	if !ValidUnixMS(t.OccurredAt) {
		return fmt.Errorf("task.event: occurred_at is not a plausible unix-ms timestamp")
	}
	if t.Kind == "progress" && t.Percent == nil {
		return fmt.Errorf("task.event: percent is required when kind is progress")
	}
	if t.Percent != nil && (*t.Percent < 0 || *t.Percent > 100) {
		return fmt.Errorf("task.event: percent out of range")
	}
	if len(t.Title) > freeTextMax || len(t.Step) > freeTextMax || len(t.Message) > freeTextMax {
		return fmt.Errorf("task.event: free-text field exceeds %d chars", freeTextMax)
	}
	return nil
}

func (m MessageEventBody) Validate() error {
	if !ValidDeviceID(m.DeviceID) {
		return fmt.Errorf("message.event: invalid device_id %q", m.DeviceID)
	}
	if m.DeviceSeq < 1 {
		return fmt.Errorf("message.event: device_seq must be >= 1 (missing or invalid)")
	}
	if m.MessageID == "" {
		return fmt.Errorf("message.event: missing message_id")
	}
	switch m.Level {
	case "info", "warn", "error":
	default:
		return fmt.Errorf("message.event: invalid level %q", m.Level)
	}
	if len(m.Text) > freeTextMax {
		return fmt.Errorf("message.event: text exceeds %d chars", freeTextMax)
	}
	if !ValidUnixMS(m.OccurredAt) {
		return fmt.Errorf("message.event: occurred_at is not a plausible unix-ms timestamp")
	}
	return nil
}

func (c ConfigEventBody) Validate() error {
	switch c.Kind {
	case "automation.upserted", "automation.removed":
	default:
		return fmt.Errorf("config.event: invalid kind %q", c.Kind)
	}
	if c.AutomationID == "" {
		return fmt.Errorf("config.event: missing automation_id")
	}
	if c.Kind == "automation.upserted" && c.Automation == nil {
		return fmt.Errorf("config.event: automation is required when kind is automation.upserted")
	}
	if !ValidUnixMS(c.OccurredAt) {
		return fmt.Errorf("config.event: occurred_at is not a plausible unix-ms timestamp")
	}
	return nil
}

func (m MetricFrameBody) Validate() error {
	if !ValidDeviceID(m.DeviceID) {
		return fmt.Errorf("metric.frame: invalid device_id %q", m.DeviceID)
	}
	if len(m.Metrics) == 0 {
		return fmt.Errorf("metric.frame: metrics must not be empty")
	}
	if len(m.Metrics) > 64 {
		return fmt.Errorf("metric.frame: metrics exceeds 64-item cap")
	}
	seen := make(map[string]bool, len(m.Metrics))
	for i, s := range m.Metrics {
		if err := s.Validate(); err != nil {
			return fmt.Errorf("metric.frame: metrics[%d]: %w", i, err)
		}
		if seen[s.MetricID] {
			return fmt.Errorf("metric.frame: duplicate metric_id %q in one frame", s.MetricID)
		}
		seen[s.MetricID] = true
	}
	return nil
}

var validTopics = map[string]bool{"task": true, "metric": true, "message": true}

func (s SubscribeBody) Validate() error {
	seen := make(map[string]bool, len(s.Topics))
	for _, t := range s.Topics {
		if !validTopics[t] {
			return fmt.Errorf("subscribe: unknown topic %q", t)
		}
		if seen[t] {
			return fmt.Errorf("subscribe: duplicate topic %q", t)
		}
		seen[t] = true
	}
	return nil
}

func (c CommandBody) Validate() error {
	if c.CommandID == "" || len(c.CommandID) > 64 {
		return fmt.Errorf("command: invalid command_id")
	}
	if c.Origin != "viewer" && c.Origin != "server" {
		return fmt.Errorf("command: invalid origin %q", c.Origin)
	}
	if c.TTLMs < 1 || c.TTLMs > 86400000 {
		return fmt.Errorf("command: ttl_ms out of range")
	}
	switch c.Action {
	case "pause", "resume", "stop":
		if c.Origin != "viewer" {
			return fmt.Errorf("command: action %q requires origin viewer", c.Action)
		}
		if c.IssuedByDeviceID == "" {
			return fmt.Errorf("command: action %q requires issued_by_device_id", c.Action)
		}
		if c.TaskID == "" {
			return fmt.Errorf("command: action %q requires task_id", c.Action)
		}
		if c.AutomationID != "" {
			return fmt.Errorf("command: action %q must not carry automation_id", c.Action)
		}
	case "run_now":
		if c.Origin != "viewer" {
			return fmt.Errorf("command: action run_now requires origin viewer")
		}
		if c.IssuedByDeviceID == "" {
			return fmt.Errorf("command: action run_now requires issued_by_device_id")
		}
		if c.AutomationID == "" {
			return fmt.Errorf("command: action run_now requires automation_id")
		}
		if c.TaskID != "" {
			return fmt.Errorf("command: action run_now must not carry task_id")
		}
	case "throttle", "resume_rate":
		if c.Origin != "server" {
			return fmt.Errorf("command: action %q requires origin server", c.Action)
		}
		if c.TaskID != "" || c.AutomationID != "" {
			return fmt.Errorf("command: action %q must not carry task_id or automation_id", c.Action)
		}
	default:
		return fmt.Errorf("command: invalid action %q", c.Action)
	}
	return nil
}

func (e ErrorBody) Validate() error {
	if e.Code == "" {
		return fmt.Errorf("error: missing code")
	}
	switch e.Code {
	case ErrVersionUnsupported, ErrHelloRequired, ErrUnauthenticated, ErrUnauthorized,
		ErrMalformed, ErrRateLimited, ErrFrameTooLarge, ErrBatchTooLarge,
		ErrRevisionUnavailable, ErrCommandExpired, ErrSequenceGap, ErrSuperseded, ErrInternal:
	default:
		return fmt.Errorf("error: unknown code %q", e.Code)
	}
	if len(e.Message) > 500 {
		return fmt.Errorf("error: message exceeds 500 chars")
	}
	if e.Retryable == nil || e.Fatal == nil {
		return fmt.Errorf("error: retryable and fatal are both required")
	}
	return nil
}
