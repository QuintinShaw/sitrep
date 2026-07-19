// Package wire implements the Go representation of the frozen Sitrep
// realtime protocol (proto/realtime/SPEC.md, v1.0.0). It mirrors the JSON
// Schemas under proto/realtime/ field-for-field: this package MUST NOT
// diverge from that spec, which is the sole source of truth. When in doubt,
// re-read proto/realtime/SPEC.md and the relevant messages/*.schema.json
// before changing anything here.
//
// The envelope's top level is permanently strict (SPEC.md section 3): every
// frame carries exactly type/id/ts/body and nothing else. The tolerance for
// unrecognized fields described in section 15 applies only inside `body`,
// which is why body structs are decoded with the standard (lenient)
// encoding/json behavior while the envelope itself is decoded with
// DisallowUnknownFields.
package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
)

// MessageType enumerates the complete, closed set of 14 message types for
// protocol version 1 (SPEC.md section 4). No implementation may invent an
// additional type without a version negotiation both peers agree to.
const (
	TypeHello         = "hello"
	TypeResume        = "resume"
	TypeSnapshot      = "snapshot"
	TypeDelta         = "delta"
	TypeAck           = "ack"
	TypeTaskEvent     = "task.event"
	TypeMessageEvent  = "message.event"
	TypeMetricFrame   = "metric.frame"
	TypeConfigEvent   = "config.event"
	TypeSubscribe     = "subscribe"
	TypeUnsubscribe   = "unsubscribe"
	TypeInterestRenew = "interest.renew"
	TypeCommand       = "command"
	TypeError         = "error"
)

// knownTypes is used by callers that must decide, per SPEC.md section 15,
// whether an unrecognized type should be silently ignored (not an error)
// rather than rejected as malformed.
var knownTypes = map[string]bool{
	TypeHello: true, TypeResume: true, TypeSnapshot: true, TypeDelta: true,
	TypeAck: true, TypeTaskEvent: true, TypeMessageEvent: true,
	TypeMetricFrame: true, TypeConfigEvent: true, TypeSubscribe: true,
	TypeUnsubscribe: true, TypeInterestRenew: true, TypeCommand: true,
	TypeError: true,
}

// KnownType reports whether typ is one of the 14 registered message types.
func KnownType(typ string) bool { return knownTypes[typ] }

// Envelope is the generic frame shape shared by every message
// (envelope.schema.json). Body is kept as raw JSON so a receiver can dispatch
// on Type before committing to a concrete body shape.
type Envelope struct {
	Type string          `json:"type"`
	ID   string          `json:"id"`
	TS   int64           `json:"ts"`
	Body json.RawMessage `json:"body"`
}

// envelopeIDPattern / deviceIDPattern mirror common.schema.json's $defs.
var (
	envelopeIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	deviceIDPattern   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

// ValidEnvelopeID reports whether id satisfies common.schema.json#/$defs/envelope_id.
func ValidEnvelopeID(id string) bool { return envelopeIDPattern.MatchString(id) }

// ValidDeviceID reports whether id satisfies common.schema.json#/$defs/device_id.
func ValidDeviceID(id string) bool { return deviceIDPattern.MatchString(id) }

// MinUnixMS / MaxUnixMS mirror common.schema.json#/$defs/unix_ms_timestamp's
// bound: any value in this range cannot plausibly be Unix seconds (10 digits,
// under 1e10) and is therefore guaranteed to be milliseconds. This is a
// deliberate mechanical guard against the historical seconds/milliseconds
// confusion called out in SPEC.md section 3.1.
const (
	MinUnixMS int64 = 1_000_000_000_000
	MaxUnixMS int64 = 9_999_999_999_999
)

// ValidUnixMS reports whether ts is a plausible Unix-milliseconds timestamp.
func ValidUnixMS(ts int64) bool { return ts >= MinUnixMS && ts <= MaxUnixMS }

// DecodeEnvelope parses one frame's top level. It enforces the permanently
// strict envelope shape: exactly type/id/ts/body, no other top-level field
// (SPEC.md section 3) — an envelope carrying, e.g., a smuggled "token" field
// is rejected here with the same effect as error{code: malformed}.
func DecodeEnvelope(data []byte) (Envelope, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var env Envelope
	if err := dec.Decode(&env); err != nil {
		return Envelope{}, fmt.Errorf("malformed envelope: %w", err)
	}
	if env.Type == "" {
		return Envelope{}, fmt.Errorf("malformed envelope: missing type")
	}
	if env.ID == "" {
		return Envelope{}, fmt.Errorf("malformed envelope: missing id")
	}
	if !ValidEnvelopeID(env.ID) {
		return Envelope{}, fmt.Errorf("malformed envelope: id %q does not match envelope_id pattern", env.ID)
	}
	if !ValidUnixMS(env.TS) {
		return Envelope{}, fmt.Errorf("malformed envelope: ts %d is not a plausible unix-ms timestamp", env.TS)
	}
	if env.Body == nil {
		return Envelope{}, fmt.Errorf("malformed envelope: missing body")
	}
	return env, nil
}

// NewEnvelope marshals body and wraps it in an Envelope ready for encoding.
func NewEnvelope(typ, id string, ts int64, body any) (Envelope, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{Type: typ, ID: id, TS: ts, Body: raw}, nil
}

// Encode serializes the envelope to its wire form. The envelope struct's own
// field set is exactly {type,id,ts,body}, so plain json.Marshal already
// satisfies the strict top level.
func (e Envelope) Encode() ([]byte, error) { return json.Marshal(e) }
