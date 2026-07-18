# skills/

Agent-facing integration surface. The thesis: **whatever an agent can script,
Sitrep can display**. The primary SDK is a skill that classifies user intent,
chooses a local executor, validates it, and registers ongoing work safely.

- `sitrep-skill/` — vendor-neutral Agent skill for tasks, metrics, messages,
  script/Agent/hybrid selection, automation lifecycle, and reusable producers.
- `claude-code-hook/` — adapter that maps Claude Code hook events
  (Notification / Stop / task progress) onto Sitrep tasks, zero-config.
- `examples/` — automation/task script templates (see `demo-task.sh`).
