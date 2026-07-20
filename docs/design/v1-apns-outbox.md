# Sitrep v1 APNs outbox — frozen design

Status: **frozen for implementation.** Companion to
`docs/design/v1-architecture.md` §1.3/§9 — this document is the full
specification of the `PushOutbox` module: the mechanism, the pipeline, the
per-push-kind idempotency/retry/priority rules, and the frozen
`push_outbox` schema. Nothing here changes `proto/realtime/` — APNs
delivery is a side effect of state changes SpaceHub already applies, never
a participant in `space_revision` or the WS/HTTP ingest contract.

## 1. Mechanism: DO Alarm, not Queue

`PushOutbox` is implemented with a Durable Object Alarm scoped to the
space's own SpaceHub instance — **not** a Cloudflare Queue. This is a
deliberate choice for v1, not an oversight:

- **Per-space fan-out is small.** A space has, in the expected product
  shape, a handful of paired devices (one or two computers, one or two
  phones). Even a burst of task/message events produces a small, bounded
  number of outbound APNs calls per space per wake — nowhere near the
  volume a Queue's own batching/backpressure machinery exists to manage.
- **The alarm shares the space's own SQLite consistency boundary.** The
  outbox row is written in the *same* durable object, often the *same*
  synchronous transaction, as the state change that produced it (§2). A
  Queue would mean a second write (enqueue) that itself needs a
  consistency story with the first (did the state write commit before the
  enqueue? what if the enqueue succeeds but the state write's transaction
  later needs to be considered together with it?) — machinery this design
  gets for free by keeping both in the one DO instead.
- **No extra Queue write/read/ack/lease cycle.** A Queue message needs to
  be produced, consumed, acked or nacked, and potentially DLQ'd — a second
  reliability protocol layered on top of the one `push_outbox`'s own
  `status`/`attempts`/`next_attempt_at` columns (§3) already provide. For
  the per-space volumes above, that second protocol buys nothing v1 needs.

### 1.1 Escalation criteria for a future v2 (Queue-backed)

This is explicitly **out of scope for v1**, recorded here so a future
redesign starts from the right trigger conditions instead of guessing:

- **Large per-space device count.** If a space's paired-device count grows
  well past the "a couple of phones and computers" shape this design
  assumes (say, tens of devices routinely needing a push from one event),
  the bounded-concurrency APNs dispatch inside one alarm invocation (§2)
  starts trading off against the DO's own request-duration budget.
- **APNs latency measurably blocking SpaceHub responsiveness.** If alarm
  invocations start taking long enough, or often enough, to compete with
  the DO's ability to promptly service `webSocketMessage`/`fetch` calls for
  the same space, that is the signal push dispatch needs to move off the
  state-serving critical path entirely.
- **Alarm time/failure thresholds.** Repeated `setAlarm()` failures, or an
  alarm handler regularly needing to re-arm itself many times to drain one
  backlog (rather than the common case of "drain everything pending in one
  wake"), are the concrete, measurable signal to act on rather than a
  hunch.
- **Need for a push worker that scales independently of SpaceHub.** If
  push volume needs its own scaling/observability/retry tuning separate
  from the DO serving realtime state, that is a Queue's actual value
  proposition, and at that point it is worth paying for.

**The future model, when any of the above is met, is one wake-task per
space, not one message per device.** A Queue consumer in that design still
reads a *batch* for a space (calling back into SpaceHub, or a decoupled
read replica of its outbox state, to fetch what's pending and dispatch it)
rather than the Queue itself holding one message per device per push — the
per-space grouping and the coalescing rules in §4 stay exactly as specified
below; only the trigger that wakes the dispatcher changes from "DO alarm"
to "Queue message consumed by a worker."

## 2. Pipeline

```
business-event tx (task.event / message.event / config.event mint)
  │  (same synchronous transaction, no `await` in between — see
  │   space-hub.ts's class-level concurrency note)
  ├─ update StateStore tables (tasks / messages / automations / event_log)
  ├─ write push_outbox row(s) for this event, status='pending'
  └─ return ACK to caller (WS `ack` / HTTP POST /v1/events response)
       │
       ▼ (fire-and-forget, does not block the ACK above)
  ensureAlarm()
       │
       ▼ (later, possibly a different DO instance wake)
  alarm() handler
    ├─ read a bounded batch of push_outbox rows ordered by next_attempt_at
    ├─ coalesce (per §4's per-kind rules — keep only the row(s) that should
    │            actually be dispatched from this batch)
    ├─ dispatch with bounded concurrency (§2.1) — NOT Promise.all(unbounded)
    ├─ write back sent / retry(pending, attempts+1) / permanent_failure /
    │  expired per row (§5's transition table)
    └─ if rows remain pending, re-arm: setAlarm(next earliest next_attempt_at)
```

`ensureAlarm()` is deliberately **not** part of the business-event's own
synchronous transaction — `ctx.storage.setAlarm()` is itself synchronous in
the Durable Object storage API (no `await` needed to call it), so it *can*
sit inside the same transaction as the outbox insert, and should: a
business event that writes a `push_outbox` row and then fails to also call
`setAlarm()` (even synchronously, in the same handler) risks a row that sits
`pending` forever with nothing scheduled to look at it. Two things make this
safe even in the rare case `setAlarm()` itself throws or is skipped by a
bug:

1. **The business write must not roll back or fail because of an outbox
   scheduling problem.** The state change (the task/message/config event)
   is durable and correct regardless of whether its push notification ever
   fires — push delivery is a side effect, never a correctness dependency
   for state sync (§0 of `v1-architecture.md`). If `setAlarm()` throws, the
   handler catches it, logs it, and still returns the normal ACK; it does
   **not** propagate the failure as if the business event itself failed.
2. **Every business-request entry point cheaply self-heals the anomaly.**
   Every time SpaceHub is woken by *any* business request (a WS frame, an
   HTTP call), before or after doing that request's own work, it performs
   one cheap check: does `push_outbox` have at least one `status='pending'`
   row (`SELECT 1 FROM push_outbox WHERE status = 'pending' LIMIT 1`) while
   `ctx.storage.getAlarm()` returns `null` (no alarm currently scheduled)?
   If so, call `ensureAlarm()` again. This one indexed existence check per
   invocation is what guarantees a `setAlarm()` failure is self-healing
   within, at worst, one request/frame's delay — not a silent permanent
   stall — without needing a second alarm-watching-the-alarm mechanism.

`ensureAlarm()` itself: read the current alarm (`ctx.storage.getAlarm()`);
if unset, or later than the earliest `pending` row's `next_attempt_at`, set
it to that earliest value. If a row is inserted whose `next_attempt_at` is
earlier than an already-scheduled alarm, `ensureAlarm()` moves the alarm
earlier (never leaves it later than the earliest pending work).

### 2.1 Bounded-concurrency dispatch

The alarm handler dispatches with a fixed concurrency limit — a small
worker-pool pattern, e.g. at most 8 in-flight `fetch()` calls to APNs at
once, pulling the next row as each slot frees up — never
`Promise.all(rows.map(dispatch))` unbounded. Illustrative shape (design
intent, not implementation):

```ts
const CONCURRENCY = 8;
async function drain(rows: PushOutboxRow[]) {
  let i = 0;
  const workers = Array.from({ length: Math.min(CONCURRENCY, rows.length) }, async () => {
    while (i < rows.length) {
      const row = rows[i++];
      await dispatchOne(row); // writes back status/attempts/next_attempt_at itself
    }
  });
  await Promise.all(workers);
}
```

Unbounded `Promise.all` over an entire backlog is exactly the anti-pattern
this guards against: a backlog built up during an APNs outage or a
reconnect storm must not turn into hundreds of simultaneous outbound
`fetch()` calls from one alarm invocation.

## 3. `push_outbox` schema (frozen)

```sql
CREATE TABLE push_outbox (
  push_id            TEXT PRIMARY KEY,   -- crypto.randomUUID()
  kind                TEXT NOT NULL,      -- see §4 enum
  device_id           TEXT NOT NULL,      -- target device (DeviceRegistry.devices.device_id)
  subject_id          TEXT NOT NULL,      -- task_id (push_to_start/activity_update/activity_end) or message_id/metric_id (alert)
  generation          INTEGER,            -- tasks.generation snapshot at enqueue time; NULL for kinds with no task generation (alert)
  revision            INTEGER NOT NULL,   -- space_revision (or metric ts, for metric-threshold alerts) at enqueue time; used for staleness/coalescing
  coalesce_key        TEXT NOT NULL,      -- activity_update: "${device_id}:${task_id}"; every non-coalescing kind: = push_id (self-unique). See §3.2 + §4.
  payload             TEXT NOT NULL,      -- JSON: the APNs aps payload this row will send
  status              TEXT NOT NULL DEFAULT 'pending', -- pending | sent | permanent_failure | expired
  attempts            INTEGER NOT NULL DEFAULT 0,
  next_attempt_at      INTEGER NOT NULL,   -- unix ms; alarm dispatch eligibility
  dispatch_started_at  INTEGER,            -- unix ms; set when an APNs call begins, cleared on a definitive outcome
  last_error           TEXT,               -- human-readable, last APNs status/error seen
  created_at           INTEGER NOT NULL,
  expires_at           INTEGER NOT NULL,    -- unix ms; row is dropped (status='expired') if not sent by this time
  terminal_at          INTEGER              -- unix ms; NULL while pending; set to the dispatch/decision time when the row moves to sent/permanent_failure/expired. The retention sweep (§6) keys on this.
);

CREATE INDEX idx_push_outbox_dispatch ON push_outbox(status, next_attempt_at);
CREATE INDEX idx_push_outbox_device ON push_outbox(device_id, status);

-- push-to-start: at most one row per (device, task, generation), regardless
-- of its current status — a duplicate enqueue attempt for the same logical
-- start is a no-op, not a second row (§4.1).
CREATE UNIQUE INDEX idx_push_outbox_start_dedup
  ON push_outbox(device_id, subject_id, generation)
  WHERE kind = 'push_to_start';

-- Live Activity update: at most one *unsent* row per (device, activity) at
-- a time — a fresh update coalesces into the existing pending row instead
-- of queuing a second one (§4.2).
CREATE UNIQUE INDEX idx_push_outbox_update_coalesce
  ON push_outbox(coalesce_key)
  WHERE kind = 'activity_update' AND status = 'pending';
```

**`coalesce_key` is `NOT NULL` for every row, so every kind must supply
one — including the kinds that do not coalesce.** The rule (owner ruling):

- **`activity_update`** — the only coalescing kind — uses
  `coalesce_key = "${device_id}:${task_id}"`, so a fresh update for an
  activity that already has a `pending` row collapses into it
  (`idx_push_outbox_update_coalesce`, §4.2).
- **`push_to_start`, `activity_end`, `alert`** — non-coalescing kinds — set
  `coalesce_key = push_id` (the row's own primary key, hence self-unique).
  This satisfies the `NOT NULL` column and the partial unique index (which
  is scoped `WHERE kind = 'activity_update'` and so never constrains these
  kinds) while making it structurally impossible for two non-coalescing
  rows to ever collide on `coalesce_key`. There is no "undefined
  coalesce_key" state.

### 3.1 Status enum and transitions

Four values: `pending`, `sent`, `permanent_failure`, `expired`.

Every transition **into** a terminal status (`sent`, `permanent_failure`,
`expired`) also sets `terminal_at = now` (the dispatch/decision time) in the
same write; the retention sweep (§6) keys on it. `terminal_at` stays `NULL`
for as long as the row is `pending`.

| from | to | trigger |
|---|---|---|
| *(insert)* | `pending` | business event enqueues the row; `attempts = 0`, `next_attempt_at = now` (or a computed future time, e.g. a throttled routine update), `terminal_at = NULL` |
| `pending` | `pending` (self-loop) | alarm dispatched, APNs returned a transient failure (429, 5xx, or a network error) and `attempts < MAX_TRANSIENT_ATTEMPTS` (8): `attempts += 1`, `last_error` set, `next_attempt_at = now + backoff(attempts)` (exponential, capped; `Retry-After` honored — see §5). `terminal_at` stays `NULL` |
| `pending` | `sent` | alarm dispatched, APNs returned 2xx; set `terminal_at = now` |
| `pending` | `permanent_failure` | either (a) APNs returned a permanent error (`BadDeviceToken`, `Unregistered`/410, `DeviceTokenNotForTopic`, or equivalent) — the associated token is also invalidated in `DeviceRegistry` (§5.4) — or (b) `attempts` reached `MAX_TRANSIENT_ATTEMPTS` while still only seeing transient errors (retry budget exhausted; `last_error` distinguishes this case textually, e.g. `"retry_budget_exhausted"`, from an APNs-classified permanent error, but both share the `permanent_failure` status value — a fourth status value for "gave up after retries" was considered and rejected as unnecessary granularity for what is, either way, "this row will never be sent"). Set `terminal_at = now` |
| `pending` | `permanent_failure` | **push-to-start only**: `dispatch_started_at` is set and more than `AMBIGUOUS_DISPATCH_GRACE_MS` (60 000 ms) has elapsed with the row still `pending` — treated as an ambiguous outcome and *not* retried, to preserve push-to-start's at-most-once bias (§4.1); `last_error = "ambiguous_dispatch_outcome_not_retried"`, `terminal_at = now` |
| `pending` | `permanent_failure` | **`activity_update`/`activity_end` only**: at dispatch, the row's snapshotted `generation` is less than the task's current `tasks.generation` — the task has already restarted, so this row targets a superseded run's Live Activity; **skip the APNs call** and mark `permanent_failure`, `last_error = "superseded_by_newer_generation"` (§4.2/§4.3), `terminal_at = now` |
| `pending` | `expired` | alarm considers dispatching the row, finds `next_attempt_at` (or `now`) would exceed `expires_at` — dropped without an APNs call; set `terminal_at = now` |
| `sent` / `permanent_failure` / `expired` | *(retention sweep)* | terminal; rows are deleted, not transitioned further — `sent` rows `terminal_at` + 1 h; `permanent_failure`/`expired` rows `terminal_at` + 24 h (§6) |

No transition ever moves a row backward out of a terminal status. A new
logical push for the same subject after a terminal outcome is always a
**new row** (a new `push_id`), subject to the same unique-index rules in
§3.

## 4. Per-kind idempotency, retry, and priority

`kind` enum: `push_to_start`, `activity_update`, `activity_end`, `alert`.

### 4.1 `push_to_start`

- **Unique key**: `(device_id, task_id, generation)` — enforced by
  `idx_push_outbox_start_dedup` (§3), scoped by `kind = 'push_to_start'` so
  it does not constrain other kinds. `subject_id` = `task_id`,
  `generation` = the task's `tasks.generation` value at the moment the
  triggering `task.event{kind: "started"}` was folded
  (`docs/design/v1-architecture.md` §1.2).
- **At most one start push per task-generation per device.** A second
  `task.event{kind: "started"}` for the same `task_id` while its
  `generation` hasn't changed (e.g. a script re-emitting `started` mid-run —
  the existing `UserStore.apply()` guard against this, `ev.kind !==
  "task.start" || prev === undefined`, carries the same intent forward) must
  not enqueue a second push-to-start row. The unique index makes the
  `INSERT` for a duplicate a no-op (`INSERT OR IGNORE`) rather than an
  error — the enqueue call site does not need its own pre-check.
- **At-most-once bias on ambiguous outcomes.** `dispatch_started_at` is
  stamped the instant the alarm begins the APNs call for this row (before
  awaiting the response), specifically so a DO eviction, crash, or
  unhandled exception between "request sent" and "response processed"
  leaves durable evidence that a start push **might** already be in
  flight. On the next wake, if this row is still `pending` with
  `dispatch_started_at` set and more than 60 s old, it moves to
  `permanent_failure` (§3.1) rather than being retried — **duplicate Live
  Activities on a device are a worse user-facing failure than one missed
  start push**, so this kind is deliberately biased toward under- rather
  than over-delivery in the ambiguous case. This 60 s grace window is
  chosen to comfortably exceed any realistic single APNs round trip
  (typically well under 1 s) while still resolving the ambiguity promptly.
- **Priority 10** (per Apple's guidance that push-to-start, being
  user-visible and time-critical to start the Live Activity promptly, uses
  the immediate-delivery priority — matching the existing
  `startActivity()` implementation in `server/src/apns.ts`).

### 4.2 Live Activity progress/update (`activity_update`)

- **Coalesce by `(device_id, activity_id)`** — `activity_id` here is the
  `task_id` (one Live Activity per task per device in this model, per the
  `activity_tokens` table's cardinality, `v1-architecture.md` §1.1).
  `coalesce_key = "${device_id}:${task_id}"`. Enforced by
  `idx_push_outbox_update_coalesce` (§3): at most one `pending`
  `activity_update` row per `coalesce_key` at any time.
- **Keep only the latest unsent revision.** Enqueuing a new update for a
  `coalesce_key` that already has a `pending` row does **not** insert a
  second row — it `UPDATE`s the existing row's `payload` and `revision`
  in place, but **only if the new `revision` is greater than the existing
  row's `revision`** (monotonic check, mirroring `metric.frame`'s own
  `ts`-monotonicity discard rule in `proto/realtime/SPEC.md` §12 — the same
  "a sender may merge multiple pending updates into one" principle applied
  to the outbox instead of the wire). A revision that is not strictly
  greater than what's already queued is dropped: it is already stale by
  the time the queued row would be dispatched.
- **`apns-priority: 5`** (matches the existing `updateActivity()` — routine
  updates use the non-immediate priority, consistent with Apple's Live
  Activity update budget guidance and the requirement that routine content
  updates should not compete with time-critical pushes for delivery
  priority).
- **Monotonic revision so a device ignores stale content.** The
  `content-state` payload carries the same monotonic `revision` this row
  was coalesced against, so even in the rare case two updates for the same
  activity are in flight on different connections/alarms (should not
  happen given the coalescing above, but a client-side defense costs
  nothing), the receiving device can discard an update whose revision is
  not newer than the last one it applied — the same defense-in-depth
  principle `proto/realtime/SPEC.md` §6.3 already applies to WS `delta`
  ordering.
- **Generation-staleness guard at dispatch (owner ruling).** A fast task
  restart — same `task_id`, incremented `tasks.generation` — can leave an
  `activity_update` row from the *previous* run still `pending`. Because
  `activity_tokens` is keyed by `task_id` (one row per task,
  `v1-architecture.md` §1.1), dispatching that stale row would push the old
  run's content-state onto the **new** run's Live Activity. So at dispatch
  time, if the row's snapshotted `generation` is less than the task's
  current `tasks.generation`, **skip the APNs call** and mark the row
  `permanent_failure` with `last_error = "superseded_by_newer_generation"`
  (§3.1). This is a dispatch-time check, distinct from the coalescing above
  (which only merges rows *within* one generation).
- **Not every metric sample gets a push.** `metric.frame` is best-effort,
  never persisted, never revisioned (`v1-architecture.md` §1.2) — it never
  reaches `PushOutbox` at all. APNs is reserved for Live Activity content
  (task state) and threshold-crossing events (`alert`, §4.3); high-frequency
  metric samples are a WS/HTTP-only concern with no push analogue, by
  design — pushing every metric tick would blow through both the device's
  update budget and this table's row caps (§6) for no user-visible benefit
  beyond what the Live Activity's own `content-state` already carries.

### 4.3 `activity_end` and `alert` (critical alert / normal notification)

- **`activity_end`** (`subject_id = task_id`): fires on `task.event{kind:
  "done"}` or `task.event{kind: "failed"}`. **Priority 10** — ending a
  Live Activity is a terminal, user-visible transition (matches the
  existing `endActivity()`). No coalescing (`coalesce_key = push_id`,
  self-unique per §3.2) — unlike a progress update, an end event is
  not superseded by a later one for the same generation, so there is
  nothing to coalesce against; the push-to-start unique index's
  `generation` scoping means a *new* run of the same `task_id` (a new
  generation) gets its own independent `push_to_start`/`activity_end`
  pair. **The same generation-staleness guard as `activity_update` applies
  (owner ruling):** if this end-row's snapshotted `generation` is less than
  the task's current `tasks.generation` at dispatch, skip the APNs call and
  mark `permanent_failure` / `superseded_by_newer_generation` — a stale end
  push must never terminate the Live Activity of a newer run sharing the
  same `task_id` row.
- **A task reaching `done`/`failed` fires ONLY `activity_end`, never an
  extra `alert` push (owner ruling).** Completing a task ends its Live
  Activity and nothing more — v1 does **not** auto-send a notification on
  task completion or failure. The *only* thing that produces a user-facing
  `alert` push is a script explicitly emitting a message (`event.fire`);
  auto-notifying on task failure is a product-semantics change deferred to
  v1.1. There is no `task.event{kind:"done"|"failed"}` → `alert` mapping in
  v1; task completion maps to `activity_end` alone.
- **`alert`** (`subject_id = message_id` or a metric's threshold-crossing
  identifier): fires on a script-emitted `message.event` (matches
  `sendAlert()`) and on a metric threshold edge-crossing (matches the
  existing `metricViolation()` edge-detection in `server/src/store.ts` —
  fires once on the false→true edge, re-arms on true→false, unchanged). It
  does **not** fire on task lifecycle events (previous bullet). **This
  edge-detection reads/writes live, in-memory `alert_state` on every
  accepted sample, unconditionally — it is never delayed by
  `metrics_current`'s routine-sample write-downsample window (P0-7,
  `v1-architecture.md` §1.2.0). An edge transition (armed→fired or
  fired→cleared) forces an immediate `metrics_current` persist in the same
  transaction as this `push_outbox` alert-row insert, so a DO rebuild
  immediately after a threshold-crossing alert can never observe a stale,
  un-persisted `armed` state and re-fire a duplicate alert — alert firing
  always reads/writes live state, never a debounced write.** **Priority
  is level-dependent**: `error`-level messages (and `error`-severity
  threshold crossings) use priority 10 (important, time-sensitive → the app
  surfaces these as Time-Sensitive); `info`/`warn`-level use priority 5.
  This is a deliberate change from the current implementation
  (`sendAlert()` today always sends priority 10 regardless of level) —
  bringing routine notifications down to priority 5 is what "prefer
  priority 5 for routine updates, 10 only for important moments" actually
  requires. A future per-device "frequent updates" preference (there is
  **no field in `DeviceRegistry`** for it today — it belongs with the
  deferred per-device preference work, `v1-architecture.md` §13.2) would
  adjust this further, e.g. letting a device opt into "route everything
  through priority 5." Until that preference exists, the level-based
  priority split above is the frozen v1 behavior.
- **Retry only on transient failure.** Both kinds follow the shared
  transition table (§3.1): retry on 429/5xx/network error up to
  `MAX_TRANSIENT_ATTEMPTS`; a permanent APNs error (see next bullet) stops
  retrying immediately, on the first occurrence, never counted against the
  transient retry budget.
- **Permanent-error classification and token cleanup.** `BadDeviceToken`,
  `Unregistered` (APNs HTTP 410), and `DeviceTokenNotForTopic` (and any
  APNs response in that same class — a device token that will never
  succeed regardless of retry) move the row straight to
  `permanent_failure` **and** trigger cleanup of the invalid token in
  `DeviceRegistry` (`push_tokens.alert_token`,
  `push_tokens.push_to_start_token`, or `activity_tokens.token`, whichever
  table's value matches the token that just failed) — mirroring the
  existing dedup-by-token-value cleanup already present in
  `registerAlertToken`/`registerDevice` today, applied on the failure path
  instead of only on the re-registration path. A token that has gone
  permanently invalid must not silently accumulate future
  `permanent_failure` rows forever; cleaning it up means the *next*
  business event for that device does not even enqueue a doomed row (no
  token to target).
- **Honor `Retry-After`.** When an APNs 429/503 response carries a
  `Retry-After` header, `next_attempt_at = max(now + Retry-After-seconds *
  1000, now + backoff(attempts))` — never sooner than what the server
  explicitly asked for, even if the in-code backoff schedule would have
  retried sooner.
- **ActivityKit push-to-start hourly budget.** Apple enforces a per-device,
  per-app hourly budget on push-to-start Live Activity starts. This design
  does not attempt to pre-emptively rate-limit against that budget
  server-side (no per-device counter exists for it) — a start push that
  APNs itself throttles for budget reasons surfaces as a transient (429)
  or permanent error from APNs like any other failure and is classified by
  the same rules above. Server-side budget tracking, if wanted, is future
  work and not part of this freeze.

## 5. Backoff schedule

`backoff(attempts)` for transient retries: exponential with a cap,
`min(2^attempts * 1000, 300_000)` ms (1 s, 2 s, 4 s, ... capped at 5 min),
plus up to ±20% jitter to avoid synchronized retry storms across many rows
hitting a transient failure at once. `MAX_TRANSIENT_ATTEMPTS = 8` (roughly
covers a multi-minute transient APNs blip before giving up).

## 6. Bounded growth: caps and retention

An extended APNs outage, or a misconfigured/permanently-broken credential,
must not let `push_outbox` grow without bound:

- **Total-row cap per space**: 2000 rows. An `INSERT` that would exceed
  this cap first evicts the oldest **eligible-to-evict** `pending` rows —
  eligible meaning `kind IN ('activity_update', 'alert')` (coalescable/
  best-effort-ish kinds) — oldest `created_at` first. `push_to_start` and
  `activity_end` rows are never evicted to make room; if the cap is still
  exceeded after evicting every eligible row, the new insert is rejected
  (logged, not retried) rather than exceeding the cap — the business
  transaction that produced it still succeeds; only the notification is
  dropped.
- **Per-device backlog cap**: 200 rows. Same eviction preference (oldest
  eligible-kind `pending` row first) scoped to one `device_id`, so one
  device with a stuck/invalid token cannot crowd out another device's
  pending pushes within the same space.
- **Retention/expiry** — the sweep keys on the explicit `terminal_at`
  column (§3), the time a row reached its terminal status, never on
  `next_attempt_at` (which has no meaning once a row is terminal):
  - `sent` rows are deleted 1 hour after `terminal_at` (kept briefly for
    debugging/observability, not indefinitely):
    `DELETE FROM push_outbox WHERE status = 'sent' AND terminal_at < now - 3600000`.
  - `permanent_failure` and `expired` rows are deleted 24 hours after
    `terminal_at` (enough window to investigate a delivery problem without
    the table growing unbounded):
    `DELETE FROM push_outbox WHERE status IN ('permanent_failure','expired') AND terminal_at < now - 86400000`.
  - `expires_at` on insert is kind-dependent: `push_to_start` and
    `activity_end` default to `created_at + 15 minutes` (a start/end push
    that's still not delivered after 15 minutes is no longer useful — the
    task has long since moved on); `activity_update` defaults to
    `created_at + 2 minutes` (a stale progress update is actively
    misleading, not just late); `alert` defaults to `created_at + 1 hour`
    (a delayed but still-relevant notification, e.g. "task finished," is
    more tolerant of lateness than a progress bar). Note `expires_at` (the
    *pre-dispatch* drop deadline) and `terminal_at` (the *post-terminal*
    retention anchor) are distinct columns with distinct roles.
  - Cleanup runs lazily on the write path (mirroring `http_idempotency`'s
    existing lazy-sweep pattern in `space-hub.ts` — no separate alarm or
    cron for retention): each alarm invocation, after processing its
    batch, also runs the two `terminal_at`-keyed `DELETE` sweeps above.

## 7. Anomaly self-check (cross-reference)

See `v1-architecture.md` §2's pipeline description and this document's §2:
every business-request entry point (WS frame handler, HTTP route handler)
performs the cheap "pending rows exist but no alarm is scheduled" check
before/after its own work and calls `ensureAlarm()` again if so. This is the
mechanism that makes a `setAlarm()` failure self-healing without a second
watchdog alarm — restated here because it is as much a `PushOutbox`
correctness property as it is a general SpaceHub pipeline note.
