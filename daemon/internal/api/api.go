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
		return fmt.Errorf("%s %s: %s", method, path, e.Error)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// Automation is the private executor view returned only to source devices.
type Automation struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Command      []string `json:"command"`
	ExecutorKind string   `json:"executor_kind"`
	EveryS       int      `json:"every_s"`
	Enabled      bool     `json:"enabled"`
	UpdatedAt    string   `json:"updated_at"`
	LastRun      string   `json:"last_run,omitempty"`
	// RunRequestedAt: a viewer asked for an immediate run (docs: phones can
	// trigger execution but never define it).
	RunRequestedAt string `json:"run_requested_at,omitempty"`
}

func (c *Client) Automations() ([]Automation, error) {
	var out []Automation
	err := c.Do(http.MethodGet, "/v2/automations", nil, &out)
	return out, err
}

// AutomationsAsAgent is the scheduler's registry poll; ?agent=1 doubles as the
// heartbeat behind "your Mac is online" on the phone.
func (c *Client) AutomationsAsAgent() ([]Automation, error) {
	var out []Automation
	err := c.Do(http.MethodGet, "/v2/automations?agent=1", nil, &out)
	return out, err
}

func (c *Client) AddAutomation(name, executorKind string, command []string, everyS int) (Automation, error) {
	var out Automation
	err := c.Do(http.MethodPost, "/v2/automations", map[string]any{
		"name":     name,
		"executor": map[string]any{"kind": executorKind, "command": command},
		"schedule": map[string]any{"kind": "interval", "every_seconds": everyS},
		"state":    "active",
	}, &out)
	return out, err
}

func (c *Client) StampAutomationRun(id string, when time.Time) error {
	return c.Do(http.MethodPatch, "/v2/automations/"+id, map[string]any{
		"last_run_at": when.UTC().Format(time.RFC3339),
	}, nil)
}

func (c *Client) DeleteAutomation(id string) error {
	return c.Do(http.MethodDelete, "/v2/automations/"+id, nil, nil)
}
