# Realtime server: SpaceHub cost model

This document is the server-side cost/capacity companion to
`proto/realtime/SPEC.md` section 14. It maps every message type onto the
concrete SQLite writes, `space_revision` accounting, and broadcast fan-out
`server/src/realtime/space-hub.ts` performs, and then analyzes that from a
Durable Object billing perspective (requests, duration, storage). It also
records exactly when this Durable Object can hibernate.

## Per-message-type cost table

Columns mirror SPEC.md section 14 plus a `SQL writes` column naming the
actual tables touched, since "persisted" alone doesn't say how many
statements or which tables.

| `type` | SQL writes | revision++ | outbox | broadcast |
|---|---|---|---|---|
| `hello` | none (attachment only, via `serializeAttachment`) | no | no | no |
| `resume` | none (reads `tasks`/`messages`/`automations`/`event_log`) | no | no | no (unicast reply, possibly chunked) |
| `snapshot` | none | no | no | no |
| `delta` | none (built from `event_log` rows, or from the just-applied event in memory) | no | no | yes, live single-event deltas only; catch-up deltas are unicast |
| `ack` | none | no | no | no |
| `task.event` | `event_log` insert, `dedup` insert, `tasks` upsert, `space_meta` update, `push_outbox` insert (only on the non-duplicate path) | yes (skipped on duplicate) | yes | re-emitted as `delta` to every delta-eligible, leased viewer |
| `message.event` | `event_log` insert, `dedup` insert, `messages` insert + bounded-window prune, `space_meta` update, `push_outbox` insert (non-duplicate path only) | yes (skipped on duplicate) | yes | re-emitted as `delta` |
| `metric.frame` | **none** — no table is ever written for this type | **no** | no | yes, filtered to leases whose `topics` include `metric` (or omit `topics`) |
| `config.event` | `event_log` insert, `automations` upsert/delete, `space_meta` update, `http_idempotency` insert (only when an `Idempotency-Key` was supplied) | yes | no | re-emitted as `delta` |
| `subscribe` | `leases` upsert | no | no | no (its ack is unicast; may trigger a `command` broadcast, see below) |
| `unsubscribe` | `leases` delete | no | no | no (same caveat) |
| `interest.renew` | `leases` upsert | no | no | no (same caveat) |
| `command` (viewer-issued) | `pending_commands` insert, only if no connected source could take live delivery | no | no (viewer command) / yes (targeted at one or all sources) | no (targeted, not fanned out to viewers) |
| `command` (server-issued: `throttle`/`resume_rate`) | none | no | no | yes, to every connected source, on the space's lease-count 1↔0 edge only |
| `error` | none | no | no | no |

Two things worth calling out because they are easy to get wrong and are
covered by dedicated tests (`server/test/workers/broadcast.workers.ts`):

- **`metric.frame` never touches SQLite and never advances `space_revision`.**
  It is a pure in-memory relay (`SpaceHub#metricsCache`, a `Map`) plus a
  websocket fan-out. This is the single biggest cost lever in the whole
  design: a source can push metrics at up to 10 Hz per connection
  (SPEC.md section 11) with zero storage cost, at the price of the
  `snapshot.metrics` cache being best-effort and empty after eviction
  (acceptable per spec, section 6.2).
- **Duplicate `device_seq` costs one read, not one write.** The dedup check
  (`SELECT revision FROM dedup WHERE device_id = ? AND device_seq = ?`) is
  the *only* SQL statement executed on the retry path; the ack the source
  needs is still sent. This bounds the cost of a source's at-least-once
  resend behavior (SPEC.md section 5.3) to a single indexed read per retry.

## Durable Object billing perspective

Cloudflare bills a Durable Object roughly on three axes: **requests**
(wall-clock-billed invocations — HTTP `fetch()` calls and, importantly,
each Hibernatable WebSocket message callback), **duration** (active CPU/wall
time per invocation), and **storage** (row/byte volume plus read/write unit
counts against the attached SQLite database).

- **Requests.** Every inbound WebSocket frame is one `webSocketMessage`
  invocation — this is unavoidable per-message overhead independent of the
  handler's cost, which is why the Worker-side gate (rejecting invalid
  tokens with a plain 401, `adapters/workers.ts`'s `/v3/realtime` route)
  matters: an invalid token or a reconnect storm must never turn into
  SpaceHub invocations at all, let alone SpaceHub *instances* (verified by
  `server/test/workers/token-gate.workers.ts`). The one exception to "one
  request per frame" is the literal `ping`/`pong` heartbeat text, which
  `ctx.setWebSocketAutoResponse()` answers entirely inside the runtime
  without invoking `webSocketMessage` at all (SPEC.md section 9.3) — this
  is a genuine request-count win for a connection that's alive but idle.
- **Duration.** The dominant cost per request is proportional to the
  number of `sql.exec` calls a handler issues, not to network I/O (there is
  none inside a handler — `ws.send()` is fire-and-forget and never
  `await`ed). `task.event`/`message.event` cost roughly 5–6 statements;
  `metric.frame` costs zero; `resume` costs a handful of `SELECT`s plus,
  for a multi-chunk snapshot, one `ws.send()` per chunk (still no `await`,
  so duration scales with payload size, not round trips). Snapshot chunking
  (`chunkSnapshot`/`chunkDeltaEvents` in `chunking.ts`) exists specifically
  to keep any single `sql.exec`+`JSON.stringify`+`send` unit bounded, not to
  reduce request count — one `resume` is still one request no matter how
  many chunks it produces, since all of them are sent from inside the same
  `webSocketMessage` invocation.
- **Storage.** Two tables grow without bound if left unchecked:
  `event_log` (pruned to the last `EVENT_LOG_RETENTION_REVISIONS` = 1000
  revisions — older ones force a `resume` onto the `snapshot` path instead
  of erroring, SPEC.md section 6.2) and `messages` (pruned to the last
  `MESSAGE_WINDOW` = 200, SPEC.md section 6.4's normative truncation).
  `dedup` is intentionally **not** pruned: SPEC.md section 5.2's
  deduplication guarantee must hold regardless of how old a retried
  `device_seq` is, so it trades unbounded (but tiny — one row per
  historical event, `device_id`+`device_seq`+`revision`, all fixed-width)
  storage growth for correctness. `http_idempotency` is bounded by lazy
  cleanup on its own write path (no alarm): every insert also deletes
  entries older than 24 hours and caps the table at the 500 most recent
  rows — a control-plane retry arriving after both windows simply
  re-executes as a fresh request, which is state-level safe because
  automation upsert/delete are idempotent operations even without the
  key. `push_outbox` and `pending_commands` are the two "queue" tables in
  this design; both need a consumer (a future Alarm/Queue-based push
  worker, explicitly out of scope for this phase) to actually shrink —
  today they only grow, which is an accepted, called-out gap (see the
  handoff's "known gaps" section).

## Observability config

`wrangler.jsonc`'s `observability` block is the other half of the cost
model above — `logSampler`'s ≤1% in-code sampling only bounds what
SpaceHub *chooses* to write to `console.log` (`logHotPath`); it does
nothing about the Workers platform's own per-invocation telemetry, which
fires once per `webSocketMessage` call regardless of what the handler logs.
Final configuration:

```jsonc
"observability": {
  "enabled": true,
  "logs": { "enabled": true, "head_sampling_rate": 1, "invocation_logs": false },
  "traces": { "enabled": true, "head_sampling_rate": 0.01 }
}
```

- **`logs.head_sampling_rate: 1` (i.e. no head sampling on structured
  logs).** This is the deliberate choice, not an oversight: `logAlways()`
  (superseded connections, protocol errors, unhandled exceptions — the
  security/error tier) is a correctness requirement that must reach 100%
  of the time, and Workers Logs head sampling is applied indiscriminately
  before the Worker code runs, so any rate below 1.0 here would silently
  drop `logAlways` events alongside the hot-path ones. Volume is bounded
  instead at the *source*, in-code, by `logSampler` (`≤1%` of
  `logHotPath` calls) — a rate this repository controls and tests directly
  (`space-hub.ts`'s injectable `logSampler`), rather than a platform knob
  that can't distinguish log tiers.
- **`logs.invocation_logs: false`.** This is what actually bounds request
  volume: Workers' automatic invocation log is emitted once per request
  (once per WebSocket message callback here) independent of anything the
  Worker code logs, and cannot be tier-aware the way `logSampler` is.
  Disabling it removes the one telemetry source neither `logAlways` nor
  `logSampler` was ever designed to control.
- **`traces.head_sampling_rate: 0.01`.** Traces have no `logAlways`
  equivalent (nothing security-relevant depends on a trace existing), so
  they're sampled at the platform level like any other diagnostic-only
  signal.

Revised cost estimate: per-space WebSocket message volume is bounded by
the protocol itself (10 `metric.frame`/s per connection, SPEC.md section
11, plus whatever rate reliable events arrive at) — call it on the order
of a few thousand `webSocketMessage` invocations/day for an actively-used
space. With `invocation_logs: false`, none of those turn into a platform
invocation-log write; with `logSampler` at ≤1%, `logHotPath` contributes
at most ~1 structured log line per 100 frames; `logAlways` contributes a
handful of lines per space (supersession, occasional protocol errors) —
negligible even at 100% sampling. Net: structured log volume scales with
*intentional* logging, not raw message count, which is what makes the
`head_sampling_rate: 1` choice on `logs` affordable.

## Hibernation

SpaceHub is written so that an idle connection costs nothing beyond the
in-memory footprint the runtime itself keeps for a parked WebSocket:

- Every handler uses `ws.deserializeAttachment()` fresh, every time,
  instead of any DO-instance-level `Map<WebSocket, ...>` for identity —
  this is what makes it safe for the DO's JS instance to be evicted and
  reconstructed between messages. `ConnAttachment` (`attachment.ts`) is
  deliberately small (five scalars plus an optional `sessionId`) to stay
  far under the platform's 2 KiB serialized-attachment cap.
- The only volatile, non-attachment, non-SQLite in-memory state is
  `metricsCache` (a best-effort cache the spec explicitly allows to reset
  to empty after a restart, section 6.2) and the per-connection rate
  limiter state (`rateLimiters`, a `WeakMap`, similarly allowed to reset —
  worst case a freshly-evicted-and-restarted connection gets one extra
  `metric.frame` or two before the rate limit re-establishes itself).
  Neither is required for correctness, only for cost control, so losing
  them on eviction is safe by design, not just by accident.
- Nothing in this Durable Object ever calls `setInterval`, `setTimeout`, or
  `ctx.storage.setAlarm()`. The interest lease (SPEC.md section 7)
  explicitly permits lazy expiry evaluation, which `reconcileLeaseEdge()`
  performs by re-checking `COUNT(*) FROM leases WHERE expires_at > ?` every
  time a lease is mutated or a reliable event is applied — never on a
  timer. This means a space with no active connections and no pending
  automation changes has literally nothing scheduled and can hibernate (or
  be evicted entirely) indefinitely.
- The heartbeat auto-response (`ctx.setWebSocketAutoResponse`) is itself a
  hibernation-compatible primitive: it answers `ping` without waking the DO
  instance at all, which is precisely why SPEC.md section 9.3 specifies a
  bare text frame rather than a JSON envelope in the first place.

Net effect: a space's ongoing cost while every connection is idle (no
task/message/metric traffic, no lease churn) is exactly zero DO
invocations — the state sits in SQLite and the connections sit hibernated
until either side has something to say.

## Intentional omissions

- **The 10 s handshake timeout (SPEC.md section 9.1, a SHOULD) is not
  implemented.** Enforcing it server-side would require a per-connection
  timer — exactly the `setTimeout`/alarm machinery this design excludes to
  stay hibernation-friendly. The exposure is bounded: a connection that
  never sends an offer holds no lease, receives no deltas, writes nothing,
  and costs only the runtime's parked-socket overhead; the client side of
  the recommendation (close and reconnect if no accept arrives within
  10 s) is where the liveness benefit actually lives, and belongs to the
  client implementations.
- **`sequence_gap` (SPEC.md section 5.1, a MAY) is not emitted.** The
  event that did arrive is applied either way; the advisory adds a
  per-event bookkeeping read ("what was this device's previous seq")
  purely to produce a non-actionable warning.
