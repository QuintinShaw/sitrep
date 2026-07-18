# Sitrep

[![CI](https://github.com/QuintinShaw/sitrep/actions/workflows/ci.yml/badge.svg)](https://github.com/QuintinShaw/sitrep/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**Your agents, reporting in.**

Sitrep streams the status of anything running on your computer — an AI agent's long task, a training run, a scraper, a deploy — to your phone's Dynamic Island, lock screen widgets, and your Mac's menu bar. Real time, read-only, open source, self-hostable.

The core idea: **if an agent can write a script, Sitrep can display it.** A script prints one line to stdout in a simple convention, and the state shows up on every device you own.

```
$ sitrep run -- python train.py
```

```python
print("::sitrep task.progress 45 downloading model weights")
print("::sitrep metric.update gh_stars 1284")
print("::sitrep message.send 'Sketch 101.2 released'")
```

## How it works

```
┌─────────────────────────┐      ┌──────────────┐      ┌─────────────────────┐
│  Your Mac               │      │  Server      │      │  Your devices       │
│  sitrepd (Go daemon)    │─────▶│  Hono/TS     │─────▶│  iOS: Live Activity │
│  runs agent-written     │  WS  │  CF Workers  │ APNs │  Dynamic Island     │
│  local automations,     │      │  or Docker   │  WS  │  widgets            │
│  parses ::sitrep lines  │      │  (self-host) │      │  macOS: menu bar    │
└─────────────────────────┘      └──────────────┘      └─────────────────────┘
```

Three primitives, one pipe:

| Primitive | Shape | Rendered as |
|---|---|---|
| **Task** | has a start, an end, and progress | Live Activity / Dynamic Island |
| **Metric** | a value that changes forever | widgets, menu bar |
| **Message** | happens once | push notification + history |

One automation may emit metrics and messages; one bounded run emits task progress.

## Repository layout

```
proto/    protocol spec + JSON Schemas (single source of truth)
daemon/   Go: sitrepd daemon + sitrep CLI
server/   TypeScript/Hono: runs on Cloudflare Workers or Docker
apple/    Swift: iOS app, Widget/Live Activity extensions, macOS menu bar, shared SitrepKit
skills/   agent skills, Claude Code hook adapter, automation templates
docs/     research & design docs
```

## Status

Working end-to-end today (built and verified on real devices):

- `sitrep run -- <cmd>` → task progress on the **Dynamic Island / lock
  screen** via push-to-start Live Activities, and in the **macOS menu bar**
- `sitrep automation add --executor script --every 5m -- <script>` → persistent automations feeding
  **home-screen widgets** and menu bar metrics
- Server on **Cloudflare Workers** (Durable Objects) or Node — one codebase,
  [self-host either](docs/self-hosting.md)
- [Agent skill](skills/sitrep-skill/SKILL.md) so Claude Code & friends can
  wire up monitoring from one sentence, and a
  [Claude Code hook adapter](skills/claude-code-hook/README.md) that mirrors
  sessions to your phone

Not yet: prebuilt binaries / App Store build (build from source), Android,
Windows. See [troubleshooting](docs/troubleshooting.md) for the Live
Activity platform quirks we've already mapped out.

## License

[MIT](LICENSE)
