# Sitrep protocol v1

Sitrep reads line-oriented events from processes started by `sitrep run` or a
registered automation. A protocol line starts with `::sitrep `; all other
output remains ordinary process output.

The protocol has exactly three product primitives:

- `task.*` — a bounded run with progress and an end.
- `metric.update` — the latest sample of a continuously changing value.
- `message.send` — a point-in-time event that has already happened.

Automation, scheduling and user-editable inputs are control-plane objects.
They are registered with the CLI and are not stdout events.

## Grammar

```text
::sitrep <verb> [--hint=value ...] [arguments ...]
```

Leading whitespace is tolerated. Unknown hints and malformed lines are
ignored. Text may use matching single or double quotes; the final text
argument may run to end of line.

## Tasks

One `sitrep run` invocation creates one run ID. The CLI emits start and final
state automatically, so a wrapped command does not need to print anything.

```text
::sitrep task.start [title]
::sitrep task.progress <0-100> [current step]
::sitrep task.step <current step>
::sitrep task.done [message]
::sitrep task.fail [message]
```

Presentation hints: `--icon`, `--tint`, and
`--template=progress|timer|plain`.

## Metrics

```text
::sitrep metric.update [hints] <id> <value> [label]
```

`id` matches `[a-z0-9_.-]{1,64}`. Values are strings; numeric values gain
history and charting.

Metric-owned hints:

- `--alert-above=N` and `--alert-below=N` define threshold rules. The server
  edge-detects crossings and creates messages.
- `--target=N`, `--min=N`, and `--max=N` describe chart/gauge geometry. A
  target is never rendered as a denominator of the current value.
- `--icon`, `--tint`, and `--template=number|gauge|bar|spark` control display.

There is no global parameter verb. A threshold belongs to its metric; an
automation input belongs to its automation and is declared when registering
that automation.

## Messages

```text
::sitrep message.send [--level=info|warn|error] <text>
```

Messages are appended to history and may trigger a push notification. They do
not have an armed or pending lifecycle. An automation that watches a complex
condition keeps its comparison state locally and sends a message only when the
condition becomes true.

## Transport

The daemon stamps every event with:

- `source_id`: run ID for a task, or `a<automation-id>` for scheduled work.
- `ts`: RFC 3339 timestamp.

It batches events to `POST /v2/ingest`. Progress and metric bursts are
coalesced to the latest value per source/metric within one second; messages
and lifecycle changes are never coalesced.
