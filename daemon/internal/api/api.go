// Package api is the daemon's typed client for the Sitrep server.
package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/config"
)

type Client struct {
	Server string
	Token  string
	http   *http.Client
}

func FromConfig() (*Client, error) {
	cfg := config.Load()
	if cfg.Server == "" {
		return nil, fmt.Errorf("not connected — run `sitrep join` or set SITREP_SERVER")
	}
	return &Client{Server: cfg.Server, Token: cfg.Token, http: &http.Client{Timeout: 15 * time.Second}}, nil
}

func New(server, token string) *Client {
	return &Client{Server: server, Token: token, http: &http.Client{Timeout: 15 * time.Second}}
}

// APIError is returned by Do for any HTTP >= 400 response. It carries the
// status code so callers can react to specific outcomes (e.g. `sitrep join`
// treating a 404 as a possibly-transient KV-lag miss worth retrying). Its
// Error() string is unchanged from the previous plain-error form.
type APIError struct {
	Status  int
	Method  string
	Path    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s %s: %s", e.Method, e.Path, e.Message)
}

func (c *Client) Do(method, path string, body, out any) error {
	var reader *bytes.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, c.Server+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		if e.Error == "" {
			e.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
		}
		return &APIError{Status: resp.StatusCode, Method: method, Path: path, Message: e.Error}
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// Schedule mirrors the v1 AutomationState.schedule object.
type Schedule struct {
	Kind         string `json:"kind,omitempty"` // "interval"
	EverySeconds int    `json:"every_seconds"`
}

// Automation mirrors the frozen v1 AutomationState
// (docs/api/v1/openapi.yaml #/components/schemas/AutomationState): the
// shared control-plane view of an automation. Note it carries NO executor
// command — the command argv is machine-local in v1 (config.LocalAutomation;
// "code never leaves the machine"), joined by AutomationID at execution
// time. Fields renamed from the old /v2 shape: id→automation_id,
// enabled(bool)→state("active"|"paused"), every_s→schedule.every_seconds,
// last_run(RFC3339)→last_run_at(unix ms).
type Automation struct {
	AutomationID string   `json:"automation_id"`
	Name         string   `json:"name"`
	ExecutorKind string   `json:"executor_kind"`
	Schedule     Schedule `json:"schedule"`
	State        string   `json:"state"`                 // active|paused
	LastRunAt    int64    `json:"last_run_at,omitempty"` // unix ms
	// RunRequestID is a monotonic per-automation counter incremented by
	// POST /v1/automations/:id/run. The resident agent runs the automation
	// when this advances strictly beyond its last-consumed value — no
	// wall-clock, no stale-config comparison (v1-architecture.md §5.1). That
	// value is PERSISTED locally (config.LocalAutomation), not held only in
	// memory, and defaults to 0 the first time an automation is seen on this
	// device rather than being adopted from this field's current value — so
	// run-now is at-least-once (a crash mid-run, or an offline device,
	// causes a re-run/deferred-run rather than a silent skip), not
	// exactly-once (external review round 3, P0). See
	// cmd/sitrep/agent.go and config.LocalAutomation. 0 = never triggered.
	RunRequestID int64 `json:"run_request_id"`
	// RunRequestedAt (nullable unix ms) is DISPLAY-ONLY ("last requested Ns
	// ago"); the agent keys off RunRequestID, never this timestamp.
	RunRequestedAt *int64 `json:"run_requested_at,omitempty"`
}

// Active reports whether the automation's run state is "active" (the v1
// successor to the old boolean Enabled).
func (a Automation) Active() bool { return a.State == "active" }

// automationMintResult is the POST/PATCH/DELETE /v1/automations response
// (#/components/schemas/AutomationMintResult): the config.event revision plus
// the folded automation (null on delete).
type automationMintResult struct {
	Revision   int64       `json:"revision"`
	Automation *Automation `json:"automation"`
}

// Automations lists the space's automations (GET /v1/automations). For a
// SOURCE-role token this poll also IS the v1 agent presence heartbeat: the
// server stamps agent_last_seen when a source calls it, so the old
// ?agent=1 query flag is gone (v1-architecture.md §2.3).
func (c *Client) Automations() ([]Automation, error) {
	var out []Automation
	err := c.Do(http.MethodGet, "/v1/automations", nil, &out)
	return out, err
}

// AutomationsAsAgent is the resident scheduler's registry poll. In v1 it is
// identical to Automations — a source polling GET /v1/automations is itself
// the "your Mac is online" heartbeat (the server stamps agent_last_seen),
// so there is no separate ?agent=1 ping any more.
func (c *Client) AutomationsAsAgent() ([]Automation, error) {
	return c.Automations()
}

// AddAutomation creates an automation on the server (POST /v1/automations).
// Only the shared control-plane fields are sent; the executor command is
// stored machine-locally by the caller (config.SaveLocalAutomation), keyed
// by the returned AutomationID.
func (c *Client) AddAutomation(name, executorKind string, everyS int) (Automation, error) {
	var out automationMintResult
	err := c.Do(http.MethodPost, "/v1/automations", map[string]any{
		"name":          name,
		"executor_kind": executorKind,
		"schedule":      map[string]any{"every_seconds": everyS},
		"state":         "active",
	}, &out)
	if err != nil {
		return Automation{}, err
	}
	if out.Automation == nil {
		return Automation{}, fmt.Errorf("POST /v1/automations: server returned no automation")
	}
	return *out.Automation, nil
}

func (c *Client) DeleteAutomation(id string) error {
	return c.Do(http.MethodDelete, "/v1/automations/"+id, nil, nil)
}

// RunAutomation triggers an immediate run by incrementing the server's
// monotonic run_request_id (POST /v1/automations/:id/run, 200 with no body).
// This is NOT a reverse-control command: it mints no config.event and
// enqueues nothing in CommandStore — the resident agent reacts on its next
// GET /v1/automations poll when run_request_id is strictly greater than the
// last value it consumed in memory (v1-architecture.md §5.1). Naturally
// idempotent; no body, no Idempotency-Key.
func (c *Client) RunAutomation(id string) error {
	return c.Do(http.MethodPost, "/v1/automations/"+id+"/run", nil, nil)
}
