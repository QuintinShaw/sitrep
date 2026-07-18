# Contributing to Sitrep

Thanks for your interest! Sitrep is early — the fastest way to help right now
is to open an issue describing your use case, or to try the protocol and tell
us where it fights you.

## Repository layout

This is a monorepo; each directory is independently buildable and CI only runs
the jobs your change touches:

| Path | Stack | Check locally with |
|---|---|---|
| `proto/` | spec + JSON Schema | — (changes here trigger all CI jobs) |
| `daemon/` | Go (latest stable) | `cd daemon && go test ./...` |
| `server/` | TypeScript + Hono | `cd server && npm ci && npm run typecheck` |
| `apple/SitrepKit` | Swift 6 | `cd apple/SitrepKit && swift test` |
| `skills/` | shell / markdown | run the example scripts |

## Ground rules

- **The protocol spec (`proto/SPEC.md`) is the source of truth.** A change to
  parsing/serialization behavior must update the spec, the Go parser, the
  server types, and SitrepKit models in the same PR.
- Keep components decoupled: `daemon` never imports server code; `server`
  talks to storage only through the `Store` interface; Swift targets depend
  only on `SitrepKit`.
- New dependencies need a reason in the PR description. Default to zero-dep.
- Match the surrounding code style; no drive-by reformatting.

## Pull requests

1. Fork, branch from `main`.
2. Make sure the CI job for your area passes locally.
3. Describe *why*, not just *what*, in the PR body.

By contributing you agree your contributions are licensed under the
[MIT License](LICENSE).
