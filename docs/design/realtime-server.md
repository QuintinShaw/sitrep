# Realtime server: SpaceHub cost model (v1)

This document is the server-side cost/capacity companion to
`proto/realtime/SPEC.md` section 14 and `docs/design/v1-architecture.md` /
`docs/design/v1-apns-outbox.md`. It maps every message type and every `/v1`
HTTP route onto the concrete SQLite writes, `space_revision` accounting, and
broadcast fan-out `server/src/realtime/space-hub.ts` performs, then analyzes
that from a Durable Object billing perspective (requests, duration,
storage). It also records exactly when this Durable Object can hibernate —
which, in v1, is no longer "always while idle": the `PushOutbox` Alarm is a
genuine, intentional exception to that rule (see Hibernation below).

This is an update of the pre-v1 realtime-integration cost model to account
for the unified SpaceHub: DeviceRegistry's control-plane tables, the
per-device (not per-connection) metric-frame rate limiter, and — the
biggest change — the `push_outbox` Alarm.

## Per-message-type cost table (WS + shared HTTP ingest)

`POST /v1/events` and the WS `task.event`/`message.event`/`metric.frame`
handlers share one apply path (`v1-architecture.md` §4), so the SQL-write
column below is identical regardless of which transport carried the frame.

| `type` | SQL writes | revision++ | outbox | broadcast |
|---|---|---|---|---|
| `hello` | none (attachment only, via `serializeAttachment`) | no | no | no |
| `resume` | none (reads `tasks`/`messages`/`automations`/`event_log`) | no | no | no (unicast reply, possibly chunked) |
| `snapshot` (WS) / `GET /v1/snapshot` (HTTP) | none (plus two `space_meta` reads for presence) | no | no | no |
| `delta` | none (built from `event_log` rows, or the just-applied event in memory) | no | no | yes, live single-event deltas to delta-eligible, leased viewers |
| `ack` | none | no | no | no |
| `task.event` | `event_log` insert, `dedup` insert, `tasks` upsert (+ `generation` column), `space_meta` update (`ingest_last_seen`), 0-N `push_outbox` inserts/updates (only on the non-duplicate path — see below) | yes (skipped on duplicate) | yes, kind-dependent | re-emitted as `delta` |
| `message.event` | `event_log` insert, `dedup` insert, `messages` insert + bounded-window prune, `space_meta` update, N `push_outbox` inserts (one per registered `alert_token` device) | yes (skipped on duplicate) | yes (`alert`) | re-emitted as `delta` |
| `metric.frame` | **none** for the sample itself; a **best-effort** threshold-crossing edge MAY insert one `push_outbox` row per alert-token device | **no** | conditional (`alert`, on edge only) | yes, filtered to leases whose `topics` include `metric` (or omit `topics`) |
| `config.event` (automations) | `event_log` insert, `automations` upsert/delete, `space_meta` update, `http_idempotency` insert (only when an `Idempotency-Key` was supplied) | yes | no (§9 note: config.event does not enqueue a push in this freeze — see the implementation report) | re-emitted as `delta` |
| `subscribe` / `unsubscribe` / `interest.renew` | `leases` upsert/delete | no | no | no (unicast ack; may trigger a `command` broadcast on a lease-count edge) |
| `command` (viewer-issued, incl. HTTP `POST /v1/tasks/:id/commands` and `.../automations/:id/run`) | `pending_commands` upsert (doubles as the idempotency ledger — see below) | no | no | no (targeted at one/all sources, not fanned to viewers) |
| `command` (server-issued `throttle`/`resume_rate`) | none | no | no | yes, to every connected source, on the space's lease-count 1↔0 edge only |

`task.event`'s outbox fan-out is the one genuinely variable cost in this
table: a `kind:"started"` for a new generation writes one `push_outbox` row
per device with a registered `push_to_start_token` (typically 0-2 in the
expected product shape — a couple of phones); `progress`/`step` coalesces
into at most one `activity_update` row (an `UPDATE`, not an `INSERT`, once
the first one for that `(device, task)` exists); `done`/`failed` writes one
`activity_end` row (if a Live Activity is registered) plus one `alert` row
per alert-token device. None of this is unbounded per event — it's bounded
by paired-device count, which `v1-apns-outbox.md` §1 argues is small enough
that a DO Alarm (not a Queue) is the right mechanism.

`pending_commands` reuse as an idempotency ledger (`v1-architecture.md`
§4.4/§6): a command/run-now write is one `INSERT ... ON CONFLICT DO
NOTHING` plus one lazy `DELETE ... WHERE origin_ts + ttl_ms < now` sweep —
same "cheap, no alarm" pattern `http_idempotency` already established.

## Durable Object billing perspective

- **Requests.** Unchanged from the pre-v1 design for the WS path: every
  inbound frame is one `webSocketMessage` invocation, `ping`/`pong` is
  answered by `ctx.setWebSocketAutoResponse` without waking the instance at
  all. New in v1: `POST /v1/events` is one Worker `fetch()` request that,
  internally, issues one **RPC call per envelope** into the same SpaceHub
  instance (`applyTaskEvent`/`applyMessageEvent`/`ingestMetricFrame`) — up
  to `EVENTS_BATCH_MAX` (500) per HTTP request. This is more DO-request-like
  work per HTTP call than a single WS frame, by design (the whole point of
  HTTP batching is amortizing N events over one HTTP round trip); the
  500-envelope cap exists specifically to keep that amortized cost bounded
  per request.
- **Duration.** Same shape as before: cost is proportional to `sql.exec`
  call count, not network I/O. The Alarm handler adds a new duration
  source — up to 100 rows read per wake, dispatched at a concurrency of 8
  `fetch()` calls to APNs (`v1-apns-outbox.md` §2.1) — but this work
  happens in a **separate invocation** (the alarm), never inside a
  `webSocketMessage`/HTTP-request's own duration budget.
- **Storage.** `event_log` (pruned to `EVENT_LOG_RETENTION_REVISIONS` =
  1000) and `messages` (pruned to `MESSAGE_WINDOW` = 200) are unchanged.
  `dedup` is still intentionally unpruned (correctness over storage). New
  in v1: `push_outbox` is now **self-bounding** — the pre-v1 doc flagged
  `push_outbox`/`pending_commands` as "only grow, an accepted gap" because
  no consumer existed; v1 closes that gap with the Alarm-driven drain plus
  explicit caps (2000 rows/space, 200/device, `v1-apns-outbox.md` §6) and a
  lazy retention sweep (`sent` rows after 1h, terminal-failure rows after
  24h) run at the end of every alarm invocation. `pending_commands` keeps
  its own lazy per-call TTL sweep (see above). `devices`/`token_hashes`/
  `invites`/`push_tokens`/`activity_tokens` (DeviceRegistry) are all
  small, one-row-per-entity tables with no unbounded-growth concern in the
  expected product shape (a handful of paired devices per space).

## Observability config

Unchanged from the pre-v1 design (`wrangler.jsonc`):

```jsonc
"observability": {
  "enabled": true,
  "logs": { "enabled": true, "head_sampling_rate": 1, "invocation_logs": false },
  "traces": { "enabled": true, "head_sampling_rate": 0.01 }
}
```

`logs.head_sampling_rate: 1` so `logAlways()` (superseded connections,
device revocation, protocol errors, unhandled exceptions, outbox anomaly
logging) never gets silently dropped by platform-level sampling; volume is
bounded instead at the source by `logSampler`'s in-code `<=1%` sampling of
`logHotPath` calls. `logs.invocation_logs: false` bounds the platform's own
per-request invocation log (fires once per `webSocketMessage`/`fetch`
regardless of what the handler logs) — now also relevant to the Alarm
handler's own invocations, which is why `alarm()`'s own logging follows the
same `logAlways`-for-anomalies-only discipline rather than logging every
dispatched row.

## Hibernation — the Alarm is a deliberate exception

The pre-v1 doc's claim was "nothing in this Durable Object ever calls
`setAlarm()` ... a space with no active connections and no pending
automation changes has literally nothing scheduled." **That invariant no
longer holds in v1, on purpose:**

- `PushOutbox` calls `ctx.storage.setAlarm()` (via `ensureAlarm()`)
  whenever a business event enqueues a `push_outbox` row. A space that just
  had a task finish or a message fire will have a scheduled alarm even with
  zero open WebSocket connections, until the outbox drains to empty.
- This is the correct, intended tradeoff: push delivery must happen even
  when no viewer/source socket is open (that's the entire point of
  push-to-start and alert notifications — reaching a device that isn't
  currently connected). A space that is otherwise fully idle but has a
  pending push wakes once (or a few times, on transient-retry backoff) to
  drain it, then returns to zero scheduled work once `push_outbox` empties
  and `ensureAlarm()` finds nothing pending.
- Everything else about the pre-v1 hibernation story is unchanged: the
  interest lease is still lazily evaluated (no timer), the heartbeat
  auto-response still avoids waking the instance, and `ConnAttachment`
  remains the only per-connection state, kept out of DO-instance memory via
  `serializeAttachment`.
- The per-device metric-frame rate limiter (`perDeviceMetricRate`, a
  `Map<device_id, number[]>`) replaces the pre-v1 per-connection `WeakMap<
  WebSocket, ...>`. Unlike the WeakMap, this Map is NOT automatically
  reclaimed when a connection closes — it is keyed by `device_id`, which is
  stable across reconnects (that's the whole point: the limiter must
  survive hibernation/reconnect to stay per-device rather than
  per-connection, `v1-architecture.md` §4.3). Its size is bounded by
  distinct paired-device count, not connection count, which is small in
  the expected product shape; it is still best-effort/in-memory-only and
  safe to lose on eviction (a freshly-restarted DO's limiter starts empty,
  worst case allowing one extra second of unthrottled frames per device).

Net effect: a space with only WS/HTTP state traffic and no push-worthy
events is still hibernation-friendly exactly as before; a space with
recent task/message activity carries a short tail of scheduled Alarm wakes
until its outbox drains, which is strictly less often than "one wake per
event" thanks to coalescing (`activity_update`) and batched draining (up to
100 rows/wake, §2.1).

## Intentional omissions (unchanged from pre-v1)

- **The 10s handshake timeout (SPEC.md §9.1, a SHOULD) is not
  implemented** — enforcing it server-side would require a per-connection
  timer, which this design still avoids for the WS handshake path (the
  Alarm exception above is scoped to `PushOutbox` only, not handshake
  liveness).
- **`sequence_gap` (SPEC.md §5.1, a MAY) is not emitted** — the event that
  did arrive is applied either way; the advisory adds bookkeeping for a
  non-actionable warning.
