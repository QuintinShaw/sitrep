// Package config resolves client credentials: SITREP_SERVER / SITREP_TOKEN
// env vars override ~/.config/sitrep/config.json (written by `sitrep login`,
// shared with the macOS menu bar app and the Claude Code hook).
package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	Server   string `json:"server"`
	Token    string `json:"token,omitempty"` // source role: daemon/CLI writes
	DeviceID string `json:"device_id,omitempty"`
	Space    string `json:"space,omitempty"`
	// ViewerToken is a legacy field for read-side clients; owner tokens
	// cover both directions in the space model.
	ViewerToken string `json:"viewer_token,omitempty"`

	// RealtimeEnabled opts into the realtime WebSocket uplink
	// (proto/realtime/SPEC.md) for reliable task/message events and
	// metric frames, in place of the HTTP /v2/ingest batch for those
	// event kinds. Defaults to false: HTTP ingest remains the default
	// path until this is explicitly turned on. Overridable with the
	// SITREP_REALTIME env var (1/true/yes).
	RealtimeEnabled bool `json:"realtime_enabled,omitempty"`
	// RealtimeURL overrides the wss://<server>/v3/realtime endpoint this
	// package derives from Server by default. Overridable with
	// SITREP_REALTIME_URL.
	RealtimeURL string `json:"realtime_url,omitempty"`
}

func Path() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "sitrep", "config.json")
}

// RealtimeOutboxPath is where the realtime uplink's local SQLite outbox
// (internal/realtime/outbox) lives, alongside the existing config file.
func RealtimeOutboxPath() string {
	dir := filepath.Dir(Path())
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
	return cfg
}

// RealtimeURLFor derives the wss://.../v3/realtime endpoint from cfg.Server
// (http -> ws, https -> wss), unless cfg.RealtimeURL explicitly overrides
// it. The realtime protocol (proto/realtime/SPEC.md) is transport-agnostic
// and does not fix a URL path; "/v3/realtime" is the path agreed across
// implementation lines (the server exposes it there, and the Apple client
// targets the same), distinct from the existing "/v2/ingest" and
// "/v2/automations" HTTP routes.
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
	return strings.TrimRight(u, "/") + "/v3/realtime"
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
