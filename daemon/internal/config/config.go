// Package config resolves client credentials: SITREP_SERVER / SITREP_TOKEN
// env vars override ~/.config/sitrep/config.json (written by `sitrep login`,
// shared with the macOS menu bar app and the Claude Code hook).
package config

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type Config struct {
	Server   string `json:"server"`
	Token    string `json:"token,omitempty"` // source role: daemon/CLI writes
	DeviceID string `json:"device_id,omitempty"`
	Space string `json:"space,omitempty"`
	// ViewerToken is a legacy field for read-side clients; owner tokens
	// cover both directions in the space model.
	ViewerToken string `json:"viewer_token,omitempty"`
}

func Path() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "sitrep", "config.json")
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
	return cfg
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
