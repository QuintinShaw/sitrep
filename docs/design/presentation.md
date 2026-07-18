# Design: Presentation Templates

Status: accepted 2026-07-18 · Design spec: [apple-design-skill.md](apple-design-skill.md)

## Principles (from the design spec, applied to glanceable surfaces)

- **Restraint**: one accent color per task, system materials, SF Symbols
  only. The island/widget is read in under a second; decoration that does
  not carry state is noise.
- **Feedback is state**: color and icon encode status (running/paused/
  done/failed) — never text alone.
- **Native motion over pushed motion**: the `timer` template uses the
  system's live timer text, which ticks every second on-device with zero
  pushes and zero budget.

## Extensibility within Apple's rules

Live Activities cannot load remote images (hard platform rule). The
extensible surface is therefore **semantic hints**, pushed as plain strings:

| Hint | Values | Applies to |
|---|---|---|
| `icon` | any SF Symbol name (`brain.head.profile`) or a single emoji | task, metric |
| `tint` | `blue` `purple` `green` `orange` `red` `pink` `teal` `indigo` `gray` or `#rrggbb` | task, metric |
| `template` | `progress` (default) · `timer` · `plain` | task |

Unknown symbol names fall back to the default icon; unknown tints fall back
to blue; unknown templates fall back to `progress` — hints can never break
rendering, so the protocol stays forward-compatible (a future `steps`
template degrades gracefully on old apps).

## Protocol form

Hints are `--key=value` flags immediately after the verb:

```
::sitrep task.start --icon=brain.head.profile --tint=purple --template=timer "train resnet"
::sitrep metric.update --icon=star.fill --tint=orange gh_stars 1284 "GitHub ★"
::sitrep message.send --level=error "loss=NaN"
```

Hints ride in Live Activity **attributes** (fixed at start, like the title);
content-state stays minimal (`percent`, `step`, `status`) to keep update
pushes tiny.

## Templates

- **progress** — icon · title · percent, tinted bar, current step below.
  For tasks that can estimate progress.
- **timer** — icon · title · live elapsed time (system timer text), step
  below. For tasks with unknown duration; feels alive with zero pushes.
- **plain** — icon · title · step. For simple jobs; the quietest.

Status still overrides tint at the ends of the lifecycle: done → green
check, failed → red cross, paused (step prefix `⏸`) → dimmed.
