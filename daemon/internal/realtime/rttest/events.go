package rttest

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

// EventEnvelope / EventsRequest / AckedPair / EventResult / EventsResponse
// mirror docs/api/v1/openapi.yaml's POST /v1/events request/response shapes
// (docs/design/v1-architecture.md section 4.1) and
// docs/api/v1/fixtures/events-post-ack.json — duplicated here (rather than
// imported from the client package) purely so daemon-side tests can script
// a mock HTTP responder without a network dependency on a real server; see
// this package's doc comment.
type EventEnvelope struct {
	Type string          `json:"type"`
	ID   string          `json:"id"`
	TS   int64           `json:"ts"`
	Body json.RawMessage `json:"body"`
}

type EventsRequest struct {
	Events []EventEnvelope `json:"events"`
}

type AckedPair struct {
	DeviceID  string `json:"device_id"`
	DeviceSeq int64  `json:"device_seq"`
}

type EventResult struct {
	Index     int    `json:"index"`
	Type      string `json:"type"`
	Status    string `json:"status"` // applied|duplicate|stale|rejected
	DeviceSeq int64  `json:"device_seq,omitempty"`
	Revision  int64  `json:"revision,omitempty"`
}

type EventsResponse struct {
	SpaceRevision int64         `json:"space_revision"`
	Acked         []AckedPair   `json:"acked"`
	Results       []EventResult `json:"results"`
}

// AckingEvents is a stateful, scriptable POST /v1/events responder that
// acks every task.event/message.event it receives (mirroring the WS mock's
// Conn.Ack) and bumps a shared space_revision counter per reliable event —
// enough fidelity to exercise the daemon's HTTP uplink transport
// (docs/design/v1-architecture.md section 4) without reimplementing
// SpaceHub. metric.frame envelopes are acknowledged as "applied" in
// results but never contribute to acked[] (SPEC.md section 12: metric
// frames are never acknowledged).
type AckingEvents struct {
	mu       sync.Mutex
	revision int64
	received []EventEnvelope
}

// Handler returns the http.HandlerFunc for SetEventsHandler.
func (a *AckingEvents) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req EventsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"malformed"}`))
			return
		}
		a.mu.Lock()
		defer a.mu.Unlock()

		resp := EventsResponse{}
		for i, env := range req.Events {
			a.received = append(a.received, env)
			result := EventResult{Index: i, Type: env.Type}
			switch env.Type {
			case wire.TypeTaskEvent:
				var b wire.TaskEventBody
				if err := json.Unmarshal(env.Body, &b); err != nil {
					result.Status = "rejected"
					break
				}
				a.revision++
				result.Status = "applied"
				result.DeviceSeq = b.DeviceSeq
				result.Revision = a.revision
				resp.Acked = append(resp.Acked, AckedPair{DeviceID: b.DeviceID, DeviceSeq: b.DeviceSeq})
			case wire.TypeMessageEvent:
				var b wire.MessageEventBody
				if err := json.Unmarshal(env.Body, &b); err != nil {
					result.Status = "rejected"
					break
				}
				a.revision++
				result.Status = "applied"
				result.DeviceSeq = b.DeviceSeq
				result.Revision = a.revision
				resp.Acked = append(resp.Acked, AckedPair{DeviceID: b.DeviceID, DeviceSeq: b.DeviceSeq})
			case wire.TypeMetricFrame:
				result.Status = "applied"
			default:
				result.Status = "rejected"
			}
			resp.Results = append(resp.Results, result)
		}
		resp.SpaceRevision = a.revision

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// Received returns every envelope this responder has decoded so far, in
// arrival order.
func (a *AckingEvents) Received() []EventEnvelope {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]EventEnvelope, len(a.received))
	copy(out, a.received)
	return out
}
