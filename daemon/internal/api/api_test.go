package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestAutomationsHitsV1NoAgentFlag pins that the resident-agent registry
// poll uses GET /v1/automations with NO ?agent=1 query — a source polling
// this route IS the v1 presence heartbeat (server stamps agent_last_seen),
// so the old query flag is gone (v1-architecture.md §2.3).
func TestAutomationsHitsV1NoAgentFlag(t *testing.T) {
	var gotPath, gotQuery, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery, gotMethod = r.URL.Path, r.URL.RawQuery, r.Method
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"automation_id":"a1","name":"nightly","executor_kind":"script","schedule":{"kind":"interval","every_seconds":30},"state":"active","last_run_at":1784476800000}]`))
	}))
	defer srv.Close()

	c := New(srv.URL, "tok")
	got, err := c.AutomationsAsAgent()
	if err != nil {
		t.Fatalf("AutomationsAsAgent: %v", err)
	}
	if gotMethod != http.MethodGet || gotPath != "/v1/automations" {
		t.Fatalf("hit %s %s, want GET /v1/automations", gotMethod, gotPath)
	}
	if gotQuery != "" {
		t.Fatalf("query = %q, want empty (no ?agent=1 in v1)", gotQuery)
	}
	if len(got) != 1 || got[0].AutomationID != "a1" || !got[0].Active() || got[0].Schedule.EverySeconds != 30 || got[0].LastRunAt != 1784476800000 {
		t.Fatalf("decoded automation mismatch: %+v", got)
	}
}

// TestAddAutomationHitsV1WithControlPlaneOnlyBody pins POST /v1/automations
// and the v1 request body: {name, executor_kind, schedule:{every_seconds},
// state} — and crucially NO command/executor (the executor command is
// machine-local in v1, not part of the server's shared automation state).
func TestAddAutomationHitsV1WithControlPlaneOnlyBody(t *testing.T) {
	var gotPath, gotMethod string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"revision":130,"automation":{"automation_id":"a1","name":"nightly","executor_kind":"script","schedule":{"kind":"interval","every_seconds":30},"state":"active"}}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "tok")
	a, err := c.AddAutomation("nightly", "script", 30)
	if err != nil {
		t.Fatalf("AddAutomation: %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/automations" {
		t.Fatalf("hit %s %s, want POST /v1/automations", gotMethod, gotPath)
	}
	if gotBody["name"] != "nightly" || gotBody["executor_kind"] != "script" || gotBody["state"] != "active" {
		t.Fatalf("request body missing/renamed control-plane fields: %+v", gotBody)
	}
	sched, ok := gotBody["schedule"].(map[string]any)
	if !ok || sched["every_seconds"].(float64) != 30 {
		t.Fatalf("request schedule = %+v, want {every_seconds:30}", gotBody["schedule"])
	}
	if _, has := gotBody["command"]; has {
		t.Fatalf("request body must NOT carry a command in v1 (command is machine-local): %+v", gotBody)
	}
	if _, has := gotBody["executor"]; has {
		t.Fatalf("request body must NOT carry the old executor object in v1: %+v", gotBody)
	}
	if a.AutomationID != "a1" || a.Schedule.EverySeconds != 30 || !a.Active() {
		t.Fatalf("returned automation mismatch: %+v", a)
	}
}

// TestDeleteAutomationHitsV1 pins DELETE /v1/automations/:id.
func TestDeleteAutomationHitsV1(t *testing.T) {
	var gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"revision":131,"automation":null}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "tok")
	if err := c.DeleteAutomation("a1"); err != nil {
		t.Fatalf("DeleteAutomation: %v", err)
	}
	if gotMethod != http.MethodDelete || gotPath != "/v1/automations/a1" {
		t.Fatalf("hit %s %s, want DELETE /v1/automations/a1", gotMethod, gotPath)
	}
}
