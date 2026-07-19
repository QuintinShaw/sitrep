# Realtime protocol cross-line integration

This document records the result of integrating three independently
developed implementations of `proto/realtime/` onto `integration/realtime`:

- `server/` (Cloudflare Workers, TypeScript) — the authoritative "SpaceHub+
  v3" server.
- `daemon/` (Go) — the `source` role client (device daemon that pushes task
  events).
- `apple/SitrepKit` (Swift) — the `viewer` role client (subscribes to space
  state).

It is not a spec: `proto/realtime/` remains the source of truth. This
records what was verified, how, and how to re-run it.

## 1. Full regression (per line)

| Suite | Command | Result |
| --- | --- | --- |
| Protocol fixtures | `cd proto/realtime/tools && npm install && npm run validate` | 81/81 |
| Server | `cd server && npm install && npm run typecheck && npm test` | typecheck clean; 88 (node:test fixtures) + 33 (vitest workers pool) = 121 |
| Daemon | `cd daemon && go build ./... && go vet ./... && go test ./... -race -count=1` | build/vet clean; 38 top-level tests, 0 failures |
| SitrepKit | `cd apple/SitrepKit && swift test` | 30/30 |
| SitrepApp | `cd apple/SitrepApp && xcodebuild build -scheme Sitrep -destination 'generic/platform=iOS Simulator'` | BUILD SUCCEEDED |
| SitrepMenuBar | `cd apple/SitrepMenuBar && swift build` | Build complete |

No fixes were needed to get all six suites green as merged.

## 2. Cross-line contract audit

Six wire-level interface points were checked with file:line evidence from
all three lines. Full detail was produced by a dedicated audit pass; the
verdicts:

1. **Endpoint URL** (`/v3/realtime`, scheme swap, no query params) — MATCH.
   `daemon/internal/config/config.go:80-92` (`RealtimeURLFor`),
   `apple/SitrepKit/Sources/SitrepKit/Realtime/RealtimeEndpoint.swift:14-24`,
   `server/src/adapters/workers.ts:509`.
2. **Authorization header** (`Authorization: Bearer <token>`) — MATCH.
   `daemon/internal/realtime/client/client.go:266-268`,
   `apple/SitrepKit/Sources/SitrepKit/Realtime/RealtimeClient.swift:226-228`,
   `server/src/app.ts:175` (case-insensitive `Bearer` strip),
   token format `TOKEN_RE` at `server/src/app.ts:22`.
3. **Hello handshake** — MATCH. Both clients send
   `{stage, device_id, role, protocol_versions}`
   (`daemon/internal/realtime/client/client.go:346-353`,
   `apple/SitrepKit/Sources/SitrepKit/Realtime/RealtimeClient.swift:293`);
   server negotiates the version intersection and replies
   `{stage:"accept", protocol_version, session_id, heartbeat_interval_ms}`
   (`server/src/realtime/space-hub.ts:430-489`). One intentional race was
   confirmed both in code and live on the wire (§3 below): the server may
   push a `command` envelope to a `source` connection immediately after
   `hello{accept}`, before any application-level exchange
   (`server/src/realtime/space-hub.ts:476-488`). Both clients handle this
   correctly (daemon's `readLoop` just dispatches it as the next frame;
   apple never receives it, since the push is source-only, and defensively
   ignores a stray `command` if one ever arrived). This was reproduced live
   during the E2E run below (`scripts/e2e/source-drive.mjs` log shows two
   `recv command` frames arriving right after `hello{accept}`, before the
   script's own `task.event` was sent).
4. **Heartbeat** (`ping`/`pong` as literal, lower-case, bare-text frames,
   either side may initiate) — MATCH. `server/src/realtime/space-hub.ts:107`
   (`setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping",
   "pong"))`), `daemon/internal/realtime/client/client.go:325,333`,
   `apple/SitrepKit/Sources/SitrepKit/Realtime/RealtimeClient.swift:451-481`.
5. **Error codes** — MATCH. All 12 codes in
   `server/src/realtime/protocol.ts:72-86` (`ERROR_SEMANTICS`) are mirrored
   verbatim in `daemon/internal/realtime/wire/bodies.go:324-336` and
   `apple/SitrepKit/Sources/SitrepKit/Realtime/RealtimeCommon.swift:196-209`.
   Both clients react to the server-supplied `fatal`/`retryable` flags on
   the wire rather than a hardcoded local table, so neither can silently
   diverge from server semantics as codes evolve.
6. **Envelope id / device_seq** — MATCH. Schema:
   `proto/realtime/common.schema.json` — `envelope_id` pattern
   `^[A-Za-z0-9_-]{1,64}$`, `device_seq` minimum `1`. Server generates
   `crypto.randomUUID()` ids and validates the same regex
   (`server/src/realtime/protocol.ts:47-48`,
   `server/src/realtime/guards.ts:41,420`). Daemon generates `"rt" + hex(16
   random bytes)` ids (`daemon/internal/realtime/client/client.go:89-95`)
   and starts `device_seq` at 1 (`daemon/internal/realtime/outbox/outbox.go:168-183`).
   Apple generates `UUID().uuidString` ids
   (`apple/SitrepKit/Sources/SitrepKit/Realtime/RealtimeClient.swift:527`)
   and validates incoming `device_seq >= 1`
   (`apple/SitrepKit/Sources/SitrepKit/Realtime/RealtimeBodies.swift:304,406,473`).

No mismatches were found. No integration fixes were required for the
contract points above.

## 3. End-to-end smoke test (real cross-process, not a fake/in-process server)

Both halves ran against a real `wrangler dev` instance (local workerd, not
`--remote`), i.e. real separate OS processes talking real WebSockets over
`127.0.0.1` — not an in-process/simulated transport.

### Setup

```bash
cd server
npm install
# The environment used a proxy; wrangler dev only needs to bind locally, so
# no proxy bypass was actually required beyond the existing NO_PROXY setting
# (NO_PROXY=localhost,127.0.0.1,::1,.local was already present). If your
# shell's NO_PROXY doesn't cover 127.0.0.1, unset http_proxy/https_proxy or
# add it before running the command below.
npx wrangler dev --port 8787 --local
```

Bootstrapping a test space and three device tokens (source, viewer1,
viewer2) uses the server's existing `/v2/spaces` + `/v2/invites` + `/v2/join`
HTTP endpoints — no new test-only backdoor was added:

```bash
curl -s -X POST http://127.0.0.1:8787/v2/spaces \
  -H 'content-type: application/json' -d '{"platform":"macos","name":"e2e"}'
# => {"space_id":"...", "owner_token":"st2_..._..."}

curl -s -X POST http://127.0.0.1:8787/v2/invites \
  -H "Authorization: Bearer $OWNER_TOKEN" -H 'content-type: application/json' \
  -d '{"role":"source"}'          # or {"role":"viewer"}
# => {"code":"...", "expires_in":600, "space_id":"..."}

curl -s -X POST http://127.0.0.1:8787/v2/join \
  -H 'content-type: application/json' \
  -d '{"code":"...", "space":"...", "name":"...", "platform":"..."}'
# => {"token":"st2_..._...", "device_id":"...", "role":"source|viewer", "space_id":"..."}
```

### Source role (real Go client, real network)

`daemon/internal/realtime/client/e2e_test.go` (`TestE2ESourceLifecycle`) is a
new, opt-in test in the daemon's own `client` package. It is a no-op under
plain `go test ./...` (skips immediately) and only runs when four env vars
are set, so it never affects the regular regression run:

```bash
cd daemon
SITREP_E2E_URL="ws://127.0.0.1:8787/v3/realtime" \
SITREP_E2E_TOKEN="<source device token>" \
SITREP_E2E_DEVICE_ID="<source device id from /v2/join>" \
SITREP_E2E_SPACE="<space id>" \
go test ./internal/realtime/client/... -run TestE2ESourceLifecycle -v
```

It drives the actual `daemon/internal/realtime/client.Client` (the same
code the daemon binary uses) through: hello offer/accept over the real
socket -> `task.event{started}` -> waits for the outbox to drain (server
ack) -> disconnects -> enqueues a second event while offline -> reconnects
with a fresh `Client` sharing the same on-disk outbox -> confirms the
pending event replays and is acked. Observed run:

```
=== RUN   TestE2ESourceLifecycle
    e2e_test.go:60: e2e: hello offer/accept completed against real server (task 1)
    e2e_test.go:79: e2e: task.event{started} acked by real server, outbox drained
    e2e_test.go:101: e2e: reconnect hello offer/accept completed (task 1, replay)
    e2e_test.go:110: e2e: task.event{progress} replayed after reconnect and acked, outbox drained
--- PASS: TestE2ESourceLifecycle (10.10s)
```

### Viewer role (node `ws` script, real network)

`scripts/e2e/viewer-smoke.mjs` connects two independent viewer devices with
the `ws` npm package (credentials via env vars only) and drives the
mandatory viewer sequence: `hello` -> `subscribe` -> `resume`. A companion
driver, `scripts/e2e/source-drive.mjs`, connects as the source device and,
after a delay (letting the viewers finish subscribing), sends one
`task.event` and one `metric.frame`.

```bash
cd scripts/e2e
npm install
export SITREP_E2E_URL="ws://127.0.0.1:8787/v3/realtime"
export SITREP_E2E_SOURCE_TOKEN="..." SITREP_E2E_SOURCE_DEVICE_ID="..."
export SITREP_E2E_VIEWER1_TOKEN="..." SITREP_E2E_VIEWER2_TOKEN="..."
node source-drive.mjs &     # background: waits, then emits the events
node viewer-smoke.mjs       # foreground: asserts the full scenario
```

Observed run (trimmed to the load-bearing frames; full output is
reproducible verbatim by re-running the commands above):

```
[viewer1] send hello {"stage":"offer","device_id":"viewer1-e2e","role":"viewer"}
[viewer1] recv hello {"stage":"accept","protocol_version":1,"session_id":"0c66d634-..."}
[viewer1] send subscribe {"topics":["task","metric","message"]}
[viewer1] recv ack {"in_reply_to":"e2e-subscribe-2-...","lease":{"expires_at":...}}
[viewer1] send resume {"last_revision":0}
[viewer1] recv snapshot {"revision":0,"part":1,"final":true,"tasks":0,"metrics":0}
[viewer1] fresh-viewer resume(0) correctly produced a snapshot at revision 0
[viewer2] ... same hello/subscribe/ack sequence ...
[viewer2] send resume {"last_revision":0}
[viewer2] recv snapshot {"revision":0,"part":1,"final":true,...}
[viewer2] recv delta {"from_revision":0,"to_revision":1,"events":1}
[viewer1] recv delta {"from_revision":0,"to_revision":1,"events":1}
[main] PASS: live delta chained correctly (snapshot rev 0 -> delta 0->1)
[viewer2] recv metric.frame {"device_id":"a85ee0f8-...","metrics":[{"metric_id":"e2e.cpu.load","value":"0.42","ts":...}]}
[main] PASS: viewer2 received the metric.frame broadcast triggered by the source
[main] ALL VIEWER E2E CHECKS PASSED
```

Source-side log for the same run (note the server's post-accept `command`
push mentioned in §2 item 3, arriving before the driver's own
`task.event`):

```
[source] recv hello {"stage":"accept","protocol_version":1,"session_id":"...","heartbeat_interval_ms":30000}
[source] recv command
[source] recv command
[source] send task.event (should trigger a live delta to subscribed viewers)
[source] recv ack {"acked":[{"device_id":"a85ee0f8-...","device_seq":1}]}
[source] send metric.frame (should broadcast to metric-subscribed viewers)
```

### What this confirms end-to-end

- Real TCP/WebSocket connections across three separate Node/Go processes and
  one real (local) Workers runtime — not an in-process fake.
- `resume{last_revision:0}` from a fresh viewer correctly returns a
  `snapshot` (not a `delta`), matching the revision-gap-snapshot scenario in
  `proto/realtime/fixtures/scenarios/client-revision-gap-snapshot/`.
- A live `task.event` from the source is turned into a `delta` broadcast to
  every subscribed, lease-holding viewer, with revision continuity
  (`snapshot.revision == delta.from_revision`,
  `delta.to_revision == delta.from_revision + 1`).
- `metric.frame` from the source is broadcast to a viewer that declared
  `metric` interest via `subscribe`, independent of the viewer that received
  the live task delta — i.e. server fan-out works across N independent
  viewer connections, not just the one that happened to trigger a resume.
- The server's post-hello-accept `command` push to `source` connections
  (§2 item 3) is real and was observed live, not just inferred from code.

No `wrangler dev` startup failure was encountered in this environment
(`NO_PROXY` already covered `127.0.0.1`/`localhost`), so the
vitest-pool-workers fallback described in the work order was not needed.

### A bug found and fixed in the E2E scripts themselves (not a cross-line defect)

The first draft of `source-drive.mjs`/`viewer-smoke.mjs` built each
envelope's `id` by embedding the message `type` literally (e.g.
`e2e-src-task.event-1-...`). `task.event` and `metric.frame` contain a `.`,
which `envelope_id`'s pattern (`^[A-Za-z0-9_-]{1,64}$`) forbids — the server
correctly rejected both with `error{code:"malformed", message:"invalid
envelope id"}`. Fixed by stripping non-alphanumeric characters out of the
type before using it in the id. This was a bug in the test fixture, not in
any of the three integrated lines.

## 4. Integration fixes made in this worktree

None. No cross-line contract mismatch or regression-suite failure required
a code change in `server/`, `daemon/`, or `apple/`. The only files added on
top of the three merged lines are the new opt-in E2E test/scripts
(`daemon/internal/realtime/client/e2e_test.go`,
`scripts/e2e/source-drive.mjs`, `scripts/e2e/viewer-smoke.mjs`,
`scripts/e2e/package.json`) and this document.

## 5. Defects requiring a fix on the owning line

None found.

## 6. Residual risks

- The post-hello-accept `command` push to `source` connections
  (`server/src/realtime/space-hub.ts:476-488`) is documented in code as a
  "v1.1 protocol clarification candidate" but is not yet reflected in
  `proto/realtime/messages/hello.schema.json`'s prose. Both clients handle
  it safely today; a future client implementation that assumes "nothing
  arrives before my first subscribe/resume" would not be safe. Worth an
  explicit spec note in a future protocol revision (out of scope here since
  `proto/realtime/` is frozen for this integration pass).
- The E2E smoke test only exercises one source + two viewers in one space,
  for a few seconds; it does not cover the interest-lease-expiry, duplicate-
  connection-supersede, or duplicate-device-seq scenarios end-to-end over a
  real network (those are covered at the fixture/unit level in all three
  lines, and in the server's own vitest-pool-workers suite, but not by a
  live cross-process run in this pass).
- `wrangler dev --local` state is ephemeral (fresh DO storage per run); the
  space/device tokens embedded as examples above are from a throwaway local
  run and are not valid against any deployed environment.
