---
name: sitrep
description: >
  Connect computer-side work to Sitrep on iPhone and Mac. Use when the user
  asks to show a long-running task's progress, monitor a changing metric,
  receive a notification when an event or condition occurs, create, inspect,
  replace, or remove a Sitrep automation, or manage its phone-side run, pause,
  and schedule controls.
  Choose and validate a local script, installed Agent, or explicit hybrid
  executor; register ongoing work through the Sitrep CLI without exposing
  pairing credentials.
---

# Sitrep

Turn a user's monitoring request into one or more of exactly three product
objects:

| Need | Emit | Primary surface |
|---|---|---|
| A bounded process with a beginning and end | `task.*` | Dynamic Island, lock screen, 现在 |
| A continuously changing value | `metric.update` | widgets, 指标, menu bar |
| A point-in-time event that already happened | `message.send` | notification, 消息 |

Treat an ongoing condition as an automation that produces a metric and/or a
message. Do not invent a fourth product object.

## Resolve Sitrep safely

1. Resolve the CLI with `command -v sitrep`.
2. On macOS, if it is not on `PATH`, use
   `/Applications/Sitrep Menu Bar.app/Contents/Resources/sitrep` when executable.
3. Run `sitrep automation list` to verify that the computer is paired and the
   service is reachable.
4. If neither CLI exists or pairing fails, stop and tell the user to install or
   connect Sitrep. Do not fabricate a local-only monitor.

Let the CLI load credentials. Never print, return, copy, or ask an Agent to
read `~/.config/sitrep/config.json`, `SITREP_TOKEN`, or Keychain values.

## Classify before implementing

Map the request by user intent, not by its data source:

- Use a **task** for a run the user waits to finish: build, training, deploy,
  download, or an interactive Agent session.
- Use a **metric** for a value the user wants to glance at repeatedly: CPU,
  price, odds, stars, queue depth, or latest version.
- Use a **message** only when something happened. A pending rule is not a
  message.
- Combine a metric and message when the user wants both visibility and a
  crossing/change notification.

Keep ownership explicit: thresholds and display settings belong to a metric;
schedules and executors belong to an automation. Never emit or create a global
parameter.

## Choose the executor at authoring time

Choose the cheapest reliable executor, then keep that choice stable:

1. **Script — default.** Use a local command, API, feed, or deterministic parser
   for system values and structured data.
2. **Hybrid — explicit optimization.** Fetch and diff with a script; invoke an
   installed Agent only when new material needs interpretation.
3. **Agent — use when necessary.** Use an Agent for login-gated pages,
   unstructured content, browser/app interaction, search, or judgment.

Do not silently fall back from a broken script to an Agent at runtime. A stale
metric is an honest failure signal; an invisible fallback hides breakage and
creates unpredictable cost.

For Agent executors:

- Discover what is installed and already logged in. Do not hardcode Claude,
  Codex, Chrome, or a particular computer-use implementation.
- Inspect the chosen Agent's supported non-interactive invocation before
  registering it. Do not guess flags.
- Confirm one unattended test run can access the required browser/app state and
  prints valid Sitrep lines without explanatory prose around them.
- Use conservative intervals and state the expected token/session cost.
- Limit the initial product to reading and reminding. Do not register purchase,
  posting, destructive, or open-ended computer-control actions.

## Implement persistent producers

For ongoing monitoring, write a small executable into a stable user-owned path,
for example:

```text
~/.local/share/sitrep/automations/<slug>/run.sh
```

Use a project-owned script only when the automation is intentionally tied to
that project. Never schedule a temporary file or a working-tree path likely to
move.

For semantic change detection, persist only the minimum comparison state under
`~/.local/state/sitrep/<slug>/`. On the first run, establish a baseline without
sending a historical "new" message unless the user explicitly wants one.

Keep secrets out of commands, stdout, automation names, and protocol lines.
Use the source application's existing credential store or a purpose-built
secret mechanism.

## Emit the protocol

Print protocol lines to stdout. Leave all other output as normal logs.

```text
::sitrep task.progress <0-100> [current step]
::sitrep task.step <current step>
::sitrep task.done [message]
::sitrep task.fail [message]
::sitrep metric.update [hints] <id> <value> [label]
::sitrep message.send [--level=info|warn|error] <text>
```

Use `sitrep run` for bounded work; it emits task start and final state
automatically:

```bash
sitrep run --title "train model" -- python train.py
```

Use `task.step` instead of inventing progress. Keep percentages monotonic and
honest. Scheduled automations ignore task lifecycle lines; they should emit
metrics and messages.

Apply hints directly after the verb:

- Presentation: `--icon`, `--tint`, `--template`.
- Metric geometry: `--target`, `--min`, `--max`.
- Metric thresholds: `--alert-above`, `--alert-below`.

Use `progress|timer|plain` for task templates and
`number|gauge|bar|spark` for metrics. Use stable metric IDs matching
`[a-z0-9_.-]{1,64}` so later samples update the same card. A target is not a
denominator; never present a value as `current/target` unless that ratio is the
actual metric.

## Validate before registering

Follow this sequence for every ongoing request:

1. Run the producer once locally.
2. Confirm it exits successfully and emits the expected metric/message lines.
3. Check that values are real, labels are clear, IDs are stable, and no secrets
   appear in output.
4. Run `sitrep automation list` and check for the same intent/name.
5. Do not create a duplicate. Reuse the existing automation when it already
   matches. Remove and recreate it only when the user asked to replace its
   executor or command.
6. Register the validated command:

```bash
sitrep automation add --name "<name>" --executor script --every 5m -- /absolute/path/to/run.sh
```

7. Allow the resident Agent up to its discovery interval, then run
   `sitrep automation list` again. Confirm the automation is active and `last
   run` is no longer `never`.
8. Verify the resulting value/message in Sitrep when the surface is available.
9. Report the automation ID, executor, interval, script location, latest real
   value, notification rule, and how to pause/delete it.

The iPhone may run now, pause, reschedule, or delete an automation. It must not
replace the computer-side command or Agent prompt.

## Standard macOS CPU monitor

Use `scripts/macos-cpu-usage.sh` instead of rewriting CPU parsing. Copy it to a
stable path, test it, then register it. Pass an optional numeric argument to
set an alert-above threshold:

```bash
mkdir -p "$HOME/.local/share/sitrep/automations/macos-cpu"
install -m 0755 scripts/macos-cpu-usage.sh \
  "$HOME/.local/share/sitrep/automations/macos-cpu/run.sh"

"$HOME/.local/share/sitrep/automations/macos-cpu/run.sh" 80

sitrep automation add --name "Mac CPU 使用率" --executor script --every 15s -- \
  "$HOME/.local/share/sitrep/automations/macos-cpu/run.sh" 80
```

This updates `system.cpu` with a real whole-machine percentage and sends a
message only when it crosses above 80%, re-arming after it drops below.

## Finish with an operational handoff

Tell the user what is running and where. Distinguish app polling from
Apple-controlled widget refresh latency. If verification fails, leave no
duplicate automation behind, report the exact failed boundary, and keep the
producer available for repair.
