// Package config resolves client credentials: SITREP_SERVER / SITREP_TOKEN
// env vars override ~/.config/sitrep/config.json (written by `sitrep login`,
// shared with the macOS menu bar app and the Claude Code hook).
package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Config struct {
	Server   string `json:"server"`
	Token    string `json:"token,omitempty"` // source role: daemon/CLI writes
	DeviceID string `json:"device_id,omitempty"`
	Space    string `json:"space,omitempty"`

	// RealtimeEnabled is the local opt-in for using the realtime uplink at
	// all (docs/design/v1-architecture.md; internal/realtime/client). When
	// on, every reliable event (task lifecycle + message.send) and every
	// metric.update this device offers is routed through the realtime
	// Client, which is itself responsible for delivering them to the
	// space's one SpaceHub across every transport degradation it can
	// encounter — live WebSocket when up, HTTP POST /v1/events the entire
	// time it is down or WS_TRANSPORT_ENABLED is off (v1-architecture.md
	// section 0/8.1). This flag does not choose between two stores; there
	// is only one. Defaults to false. Overridable with the SITREP_REALTIME
	// env var (1/true/yes).
	RealtimeEnabled bool `json:"realtime_enabled,omitempty"`
	// RealtimeURL overrides the wss://<server>/v1/realtime endpoint this
	// package derives from Server by default. Overridable with
	// SITREP_REALTIME_URL.
	RealtimeURL string `json:"realtime_url,omitempty"`
	// EventsURL overrides the https://<server>/v1/events endpoint this
	// package derives from Server by default (the HTTP transport fallback,
	// v1-architecture.md section 4). Overridable with SITREP_EVENTS_URL.
	EventsURL string `json:"events_url,omitempty"`

	// OutboxMaxRows bounds the local realtime outbox's row count (see
	// internal/realtime/outbox.DefaultMaxRows) — the point at which further
	// reliable events must wait for local backpressure rather than being
	// enqueued, because the outbox is the sole durable queue feeding the
	// realtime Client's delivery to SpaceHub (no second store to fall back
	// to; see internal/uplink.routeToRealtime). Zero/unset uses
	// outbox.DefaultMaxRows. Overridable with SITREP_OUTBOX_MAX_ROWS.
	OutboxMaxRows int `json:"outbox_max_rows,omitempty"`
}

func Path() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "sitrep", "config.json")
}

// Dir is the ~/.config/sitrep directory that holds config.json and the
// daemon's sibling state files (the realtime outbox, the local automations
// store, the health-status file). Empty only if the home dir is unresolvable.
func Dir() string {
	p := Path()
	if p == "" {
		return ""
	}
	return filepath.Dir(p)
}

// RealtimeOutboxPath is where the realtime uplink's local SQLite outbox
// (internal/realtime/outbox) lives, alongside the existing config file.
func RealtimeOutboxPath() string {
	dir := Dir()
	if dir == "" || dir == "." {
		return "realtime-outbox.db"
	}
	return filepath.Join(dir, "realtime-outbox.db")
}

func Load() Config {
	var cfg Config
	if data, err := os.ReadFile(Path()); err == nil {
		_ = json.Unmarshal(data, &cfg)
	}
	if v := os.Getenv("SITREP_SERVER"); v != "" {
		cfg.Server = v
		cfg.Token = os.Getenv("SITREP_TOKEN")
	} else if v := os.Getenv("SITREP_TOKEN"); v != "" {
		cfg.Token = v
	}
	if v := os.Getenv("SITREP_REALTIME"); v != "" {
		cfg.RealtimeEnabled = v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
	}
	if v := os.Getenv("SITREP_REALTIME_URL"); v != "" {
		cfg.RealtimeURL = v
	}
	if v := os.Getenv("SITREP_EVENTS_URL"); v != "" {
		cfg.EventsURL = v
	}
	if v := os.Getenv("SITREP_OUTBOX_MAX_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.OutboxMaxRows = n
		}
	}
	return cfg
}

// RealtimeURLFor derives the wss://.../v1/realtime endpoint from cfg.Server
// (http -> ws, https -> wss), unless cfg.RealtimeURL explicitly overrides
// it. GET /v1/realtime is the WebSocket upgrade carrying
// proto/realtime/SPEC.md verbatim (docs/design/v1-architecture.md section
// 2.1) — distinct from the HTTP POST /v1/events transport fallback
// (EventsURLFor), which the non-realtime telemetry uplink (internal/uplink)
// also targets.
func (cfg Config) RealtimeURLFor() string {
	if cfg.RealtimeURL != "" {
		return cfg.RealtimeURL
	}
	u := cfg.Server
	switch {
	case strings.HasPrefix(u, "https://"):
		u = "wss://" + strings.TrimPrefix(u, "https://")
	case strings.HasPrefix(u, "http://"):
		u = "ws://" + strings.TrimPrefix(u, "http://")
	}
	return strings.TrimRight(u, "/") + "/v1/realtime"
}

// EventsURLFor derives the https://.../v1/events endpoint from cfg.Server,
// unless cfg.EventsURL explicitly overrides it. POST /v1/events is the HTTP
// ingest for device-uplinked events — the SAME SpaceHub ingest function
// GET /v1/realtime's WebSocket path calls (docs/design/v1-architecture.md
// section 4) — used by the realtime Client as its transport fallback
// whenever the WebSocket is unavailable, never a second store.
func (cfg Config) EventsURLFor() string {
	if cfg.EventsURL != "" {
		return cfg.EventsURL
	}
	return strings.TrimRight(cfg.Server, "/") + "/v1/events"
}

func Save(cfg Config) error {
	path := Path()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}
