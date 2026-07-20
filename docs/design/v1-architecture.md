# Sitrep v1 architecture — frozen contract

Status: **frozen for implementation.** This document, `docs/api/v1/openapi.yaml`,
`docs/api/v1/fixtures/`, and `docs/design/v1-apns-outbox.md` together are the
single source of truth for the unified `/v1` HTTP API and its storage
architecture. Three implementation lines (server, daemon, apple) build
against this contract without re-interpreting it. Any ambiguity found during
implementation is a bug in this contract, to be raised with the protocol
owner — not silently resolved differently by each line.

This document does **not** change `proto/realtime/` (the WS envelope
protocol, frozen at `1.0.0`). It defines the HTTP API, the storage
architecture behind it, and the APNs delivery layer that all sit *around*
that protocol. `POST /v1/events` carries the same event envelopes defined in
`proto/realtime/`; `GET /v1/realtime` is the WS upgrade for the same
protocol.

## 0. Core invariant

> **All business state — task, metric, message, automation, and the
> event log/revision that folds into them — is written and read through
> exactly ONE `SpaceHub` Durable Object per space.**
>
> Degradation switches **transport** only. It never switches **which
> store** a read or write resolves to. WebSocket, HTTP `POST /v1/events` /
> `GET /v1/snapshot`, and APNs delivery all resolve to the same SpaceHub
> instance for a given space, at every point along the availability
> spectrum — including when WebSocket is disabled and even during a
> partial APNs outage.

This is the rule the realtime rebuild violated: `/v2` wrote a `UserStore` DO,
`/v3` wrote a `SpaceHub` DO for the same space, and the two never
reconciled — producing double writes, rollback forks, and idempotency bugs
whenever a client crossed between them. The fix is not "make the two stores
agree more carefully"; it is **delete the second store**. There is one
authoritative store. Every route in this document, with no exception, reads
from or writes to that one store.

A corollary that implementers must not violate: **no code path may declare a
transport-disabled condition (`WS_TRANSPORT_ENABLED=false`, an APNs outage,
a dashboard override) as a reason to skip, gate, or fork a *write* to
SpaceHub.** Section 8 specifies exactly which two things a transport switch
is allowed to touch, and it is not "whether the automations HTTP routes may
write."

## 1. Single DO, internal module separation

There is **one `SpaceHub` Durable Object class**, one instance per space
(`env.SPACE_HUB.getByName(spaceId)`), backed by that instance's own SQLite
storage. There is:

- **No separate `SpaceRegistry` DO.** Device/token/invite state that today
  lives in `UserStore` moves into SpaceHub's own tables (§1.1
  `DeviceRegistry`). A space's registry and a space's business state are the
  same durability domain — there is no scenario in this product where they
  need independent consistency, and splitting them is exactly the shape of
  bug this freeze exists to close off.
- **No KV, period (P0-6).** `INVITE_DIR` is **removed** — v1 has zero KV
  dependency anywhere on the join path, not merely a de-prioritized one. The
  R1 draft had `INVITE_DIR` as a code→`space_id` routing cache (TTL 600 s)
  consulted whenever a bare code arrived with no space context; KV's
  eventual consistency (cross-region propagation lag) meant a scan could
  spuriously 404 immediately after code creation, and a retry-on-404 patch
  cannot fix that failure class — only removing the KV read from the
  correctness path can. The fix (§10.5): the connect code now **encodes
  `space_id` directly**, so `POST /v1/join` always routes via a client-
  supplied `space` field (`env.SPACE_HUB.getByName(space)`) with **zero**
  lookup of any kind — a QR/scan payload for pairing already carried a
  routable `space_id` (unchanged principle from
  `docs/design/pairing-and-control.md`), and now the code string itself
  does too, so there is no remaining "no space in the link" fallback case
  for KV to serve. Invite validation happens entirely inside the routed
  SpaceHub's own `DeviceRegistry.invites` table (§1.1) — the source of
  truth was always there, never in KV; KV was only ever a routing shortcut,
  and v1 deletes it rather than keep an unused binding around.

Inside the one SpaceHub DO, the code and its SQLite tables are organized
into four modules with disjoint table ownership. This is a source-file/table
convention, not a DO/RPC boundary — every module runs in the same durable
object, the same SQLite database, the same synchronous-transaction
discipline already established by `space-hub.ts` (see that file's
class-level comment on why a sequence of `sql.exec` calls with no `await`
between them is sufficient for atomicity). There is no cross-DO call
anywhere in this design; "consistency boundary" below means exactly "these
tables are read and written inside the same durable object, so a
correctness argument that spans them never has to reason about partial
failure between two different DOs."

### 1.1 `DeviceRegistry`

Owns device identity, credentials, and the invite/join lifecycle.

| table | owns |
|---|---|
| `devices` | `device_id` (PK), `name`, `role` (`owner`\|`viewer`\|`source`), `platform`, `created_at`, `last_seen` |
| `token_hashes` | `token_hash` (SHA-256 hex, PK) → `device_id`; the only place a credential's hash is stored |
| `invites` | `code` (PK), `role`, `created_at`; single-use, 10 min TTL, deleted on redemption |
| `push_tokens` | `device_id` (PK/FK → `devices`), `push_to_start_token`, `alert_token`, `updated_at` — the **device-level** APNs tokens (§1.3, §10) |
| `activity_tokens` | `task_id` (PK), `device_id`, `token`, `updated_at` — the **per-activity** Live Activity update token (§1.3) |

Carried over unchanged from the current `UserStore`/`SpaceRegistry`
semantics in `docs/design/pairing-and-control.md`: SHA-256-only credential
storage, revocation effective on next request (no cached "still valid"
window), single-use time-boxed invites.

**Known limitation carried forward, not fixed by this freeze**: exactly one
row per `task_id` in `activity_tokens` — if two viewer devices both open a
Live Activity for the same task, the second `PUT
/v1/tasks/:id/live-activity-token` silently replaces the first, and only the
most recent registrant receives Live Activity pushes for that task. This
matches the current `/v2/activities` behavior exactly (`latoken:<sourceId>`,
last-write-wins) and is **not** an in-scope fix for this freeze — see
§13.2 open questions.

### 1.2 `StateStore`

Owns the domain state the realtime protocol folds — the same table set
`space-hub.ts` already implements, carried over with these additions:
`tasks.generation` + `tasks.owning_device_id` (push-to-start idempotency and
task-directed command routing, §1.3/§1.4/`v1-apns-outbox.md`),
`metrics_current` (persistent last-value + alert edge-state, §1.2.0),
`metric_series` (persistent metric history, §1.2.1),
`automations.run_request_id` (idempotent run-now trigger, §5.1),
and `task_logs` (persistent task-log tail, §1.2.2).

| table | owns |
|---|---|
| `event_log` | append-only reliable-event log; `revision` PK; source of catch-up `delta` |
| `dedup` | `(device_id, device_seq)` → `revision`; at-least-once dedup, never pruned |
| `tasks` | folded `task_state`, one row per `task_id`; **adds `generation INTEGER NOT NULL DEFAULT 1`** and **`owning_device_id TEXT`** (§ below) |
| `messages` | append-only, bounded window (`MESSAGE_WINDOW` = 200) |
| `automations` | folded automation set; **adds `run_request_id INTEGER NOT NULL DEFAULT 0`** (§5.1) |
| `metrics_current` | **persistent** last folded metric value + alert edge-state; one row per `metric_id` (§1.2.0) |
| `metric_series` | **non-folded** persistent tiered metric history; one row per `(metric_id, tier)` (§1.2.1) |
| `task_logs` | **non-folded** persistent per-task 100-line log ring buffer; one row per `task_id` (§1.2.2) |
| `space_meta` | singleton key/value: `revision`, `lease_active_count`, `ingest_last_seen`, `agent_last_seen` |

`tasks.generation`: incremented each time a `task.event{kind: "started"}`
arrives for a `task_id` currently absent or in a terminal state
(`done`/`failed`); unchanged by every other event kind. This column does not
exist in the current `space-hub.ts` schema and is a genuinely new addition
this freeze requires — it exists so the push-to-start outbox can key its
idempotency unique constraint on `(device_id, task_id, generation)`
(`v1-apns-outbox.md` §3) without conflating "this task restarted" with "this
is a duplicate start push for the same run."

`tasks.owning_device_id` (P0-3): the `device_id` of the source device that
is actually running this task — set to the `body.device_id` of the **first**
`task.event{kind: "started"}` for the `task_id`, and **reset** to the new
starter's `device_id` on each new-generation start (the same edge that bumps
`generation`; a re-`started` within the same generation does not change it).
It is `NULL` only for a `task_id` the server has never seen a `started` for
(e.g. a command posted for an unknown task). This column is what lets a
reverse-control command be **directed to the one device running the task**
rather than broadcast to every source in the space (§1.4) — the fix for
commands being consumed and marked delivered by the wrong source.

`space_meta`'s `ingest_last_seen` and `agent_last_seen` are **non-folded
presence markers** (owner ruling): they record the most-recent Unix-ms
timestamp at which the space received any device uplink (`ingest_last_seen`,
stamped on every reliable-event or metric ingest, WS or HTTP) and the
most-recent agent heartbeat (`agent_last_seen`). They are written on the
same request that touches them but **do NOT increment `space_revision`** —
this preserves the "one increment per meaningful domain event" invariant
(`proto/realtime/SPEC.md` §6.1) that the whole revisioned-delta machinery
depends on; a presence marker changes on nearly every request and would
make `space_revision` a far noisier signal if it participated. They surface
in `GET /v1/snapshot`'s `presence` object (§7) — "is my computer online" is
the product's core status-pill UX (green/amber/red), so presence is a
first-class frozen feature, not a dropped one.

#### 1.2.0 `metrics_current` — persistent last-value + alert edge-state (P0-2)

The R0 draft held current metric state (last value, threshold arm/fired
edge-state) **only** in an in-memory `Map` (`metricsCache`), and had
`GET /v1/snapshot`'s `metrics` and `GET /v1/metrics/:id` read only that Map.
That conflates two different things: *metric transport* is best-effort (a
dropped `metric.frame` is fine, §4.3), but *the server's already-accepted
latest state* must not silently vanish. A DO eviction/rebuild would lose the
last value AND reset the per-threshold edge-state — re-arming a threshold
that had already fired, so the next sample past the line **re-fires a
duplicate alert**. This freeze fixes that with a persistent table:

```sql
CREATE TABLE metrics_current (
  metric_id   TEXT PRIMARY KEY,
  value       TEXT NOT NULL,            -- last folded value (string, proto metric value)
  fields      TEXT NOT NULL,            -- JSON: last folded label/display/target/min/max/alert_above/alert_below/ts
  alert_state TEXT NOT NULL,            -- JSON: per-threshold edge-state, e.g. {"above":"fired","below":"armed"}
  updated_at  INTEGER NOT NULL          -- unix ms
);
```

- **Edge-detection is immediate and unconditional; persistence is
  downsampled (P0-7).** On every accepted `metric.frame` sample (passes the
  staleness check, §4.3), the fold path **recomputes `alert_state` in the
  DO's in-memory hot cache immediately** — this governs `push_outbox` alert
  enqueue timing (§9, `v1-apns-outbox.md` §4.3) and is **never** delayed by
  anything below. Whether that recomputed row is *persisted* to
  `metrics_current` in this same transaction depends on which of two cases
  applies:
  - **Alert edge transition (armed→fired or fired→cleared): persist
    immediately, no debounce, same synchronous transaction as the
    `push_outbox` alert row (§9).** A DO rebuild immediately after a
    threshold-crossing alert must never re-arm a threshold that already
    fired — that correctness property depends on the persisted
    `alert_state` never lagging a fired/cleared transition, even by the
    downsample window below.
  - **Routine sample (no edge transition): downsample.** UPSERT
    `metrics_current`'s `value`/`fields`/`updated_at` (and the unchanged
    `alert_state`) is coalesced — last-value-wins — and flushed at most
    once per `METRICS_CURRENT_DOWNSAMPLE_MS` = **10000** (10 s; within the
    5–15 s band this freeze specifies) **per space**, not per metric_id: a
    per-space pending-flush timer (reusing the existing `ensureAlarm()`
    DO-alarm machinery, §9) batches every metric_id with a coalesced update
    into one write when it fires, rather than one timer per metric_id. This
    is the write-amplification fix — a metric reporting every few hundred
    ms no longer costs one SQLite write per sample. A metric_id with a
    single routine update and no further samples is still flushed within
    the window by this timer, never left unpersisted indefinitely for lack
    of a follow-up sample.
  - `metric_series` (§1.2.1) append is **unaffected** by this downsample —
    it remains a best-effort append on every accepted sample, unchanged.
    Only the `metrics_current` *row* write is coalesced; the *series* still
    records every accepted sample (subject to its own tiering/caps).
- **Existence-vs-freshness split, and the accepted eviction-loss bound
  (owner ruling).** The debounce buffer above (`pendingMetricsFlush`) is
  in-memory, per-DO state — a DO eviction that happens *inside* the 10 s
  window loses whatever is currently buffered in it. Two different
  guarantees follow from *which* samples can land in that buffer:
  - A metric_id's **first-ever** accepted sample (no existing
    `metrics_current` row for it, in the table or the hot cache) is **never**
    buffered — it is persisted synchronously, in the same transaction as its
    acceptance, exactly like an alert edge transition (same bullet class as
    above, not a separate code path). This is what keeps the existence
    invariant below absolute: `GET /v1/metrics/:id` `404` is authoritative
    iff the metric_id was genuinely never reported, with **no** eviction
    window in which a metric that was truly reported could still 404.
  - A **routine value update** to a metric_id that already has a persisted
    row *can* land in the debounce buffer and *can* be lost if the DO is
    evicted before the buffer's flush timer fires. This is a **documented,
    accepted tradeoff, not a bug**: the bound is at most one
    `METRICS_CURRENT_DOWNSAMPLE_MS` (10 s) window's tail of value updates for
    an already-persisted metric_id — after such an eviction, the persisted
    row still exists (it is not evicted, only stale) and simply serves a
    value up to ~10 s older than the last sample actually accepted, until
    the next sample re-persists it. Alert edge transitions are exempt from
    this loss window entirely (they always bypass the buffer, per the first
    bullet above), so this tradeoff never affects alert-firing correctness —
    only the freshness of a routine current-value read in the narrow window
    right after an eviction.
- **Cap: 256 distinct `metric_id`s per space, LRU eviction on overflow
  (P0-7).** An UPSERT that would introduce a **new** `metric_id` beyond the
  cap first evicts the least-recently-updated existing row (by
  `updated_at`). This reuses the exact same number as the in-memory
  `metricsCache`'s existing `METRIC_CACHE_MAX_METRICS` constant (§4.3-
  adjacent hot-cache cap already in `server/src/realtime/protocol.ts`) —
  **one cap number for both the hot cache and the persistent table**,
  rather than two independently-drifting limits. An eviction from
  `metrics_current` here is a **bounded-growth tradeoff, not a bug**: a
  space that never exceeds 256 distinct concurrently-tracked metric_ids
  (the expected product shape) never evicts anything and keeps the full
  P0-2 guarantee below; a space that does exceed it trades the evicted
  metric_id's history for a bounded per-space footprint.
- **The in-memory Map is now a hot cache only**, rehydrated from
  `metrics_current` on DO wake rather than being the source of truth. Losing
  it on eviction costs at most a lazy re-read, never data. It is
  independently capped at the same `METRIC_CACHE_MAX_METRICS` (LRU),
  unchanged from before this freeze.
- **Alert edge-detection reads/writes `alert_state`**, not volatile memory:
  a threshold that has `fired` stays `fired` across a rebuild until the value
  returns inside the line (`armed`), so a rebuilt DO does **not** re-fire an
  alert that already fired. This is the concrete duplicate-alert fix, and it
  holds regardless of the downsample above because edge transitions always
  bypass it (first bullet).
- **`GET /v1/snapshot`'s `metrics` and `GET /v1/metrics/:id` read
  `metrics_current`** (§7). A rebuilt DO still serves the last accepted
  value. Because `metrics_current` is now the authoritative existence-of-
  record, `GET /v1/metrics/:id` **`404` is authoritative for a space within
  the 256-metric_id cap** — the row is absent iff the metric was genuinely
  never reported (resolving the former "never-existed vs.
  evicted-from-cache" ambiguity; see §13.2). A space that has exceeded the
  cap is the one documented exception: `404` on a `metric_id` that was
  LRU-evicted to make room for a newer one is expected, not a bug — see the
  cap bullet above and §13.2.
- Still **not revisioned**: `metric.frame` remains outside `space_revision`
  accounting (§4.3). Persisting current value/edge-state is a durability
  fix, not a promotion to a reliable revisioned event — no `delta`, no
  revision bump. The downsample above is purely a write-timing detail of
  this non-revisioned persistence; it has no bearing on `space_revision`.

#### 1.2.1 `metric_series` — persistent tiered metric history (non-folded)

`GET /v1/metrics/:id/series` (§2.1) is a **read of persistent history**, not
of the best-effort in-memory `metricsCache` — so it needs a durable backing
table, which the R0 draft omitted (the route existed but had no table, and
was implemented against a lossy in-memory ring buffer that a DO eviction
would wipe). This freeze adds `metric_series`, mirroring the product's
existing three-tier retention (`server/src/store.ts`'s `appendSeries`/
`selectSeries`, unchanged semantics):

```sql
CREATE TABLE metric_series (
  metric_id  TEXT NOT NULL,
  tier       TEXT NOT NULL,            -- 'raw' | 'hour' | 'day'
  points     TEXT NOT NULL,            -- JSON array of {t: unix_ms, v: number}, oldest first
  updated_at INTEGER NOT NULL,         -- unix ms
  PRIMARY KEY (metric_id, tier)
);
```

- **Three tiers, per-bucket last-value, bounded caps** (the frozen constants,
  from the existing product implementation): `raw` keeps the last **720**
  points (recent, per-sample); `hour` keeps the last **768** hourly buckets
  (~32 days); `day` keeps the last **400** daily buckets (~13 months). Each
  bucket stores the **last** value seen in that bucket window; `raw` is
  per-sample. This is `SERIES_RAW_CAP` / `SERIES_HOUR_CAP` / `SERIES_DAY_CAP`
  in `store.ts`.
- **Folding is best-effort append on `metric.frame`.** When a `metric.frame`
  sample is accepted (passes the `metricsCache` staleness check, §7), the
  same fold path appends it to this metric's series via the pure
  `appendSeries(prev, ts, value)` and writes the affected tier rows back.
- **CRITICAL — does NOT participate in `space_revision`.** A series append is
  *derived history*, not a reliable domain event: `metric.frame` itself is
  already non-revisioned and non-persisted as live state (§1.2/§4.3 of
  `SPEC.md`), and its derived history must be too. Appending to
  `metric_series` MUST NOT increment `space_revision`, MUST NOT emit a
  `delta`, and MUST NOT sit in the reliable-event path — exactly the same
  non-folded discipline as the presence markers above. A viewer that missed
  series points has lost nothing revision-relevant; it re-reads the series
  on demand.
- **Read path.** `GET /v1/metrics/:id/series?range=` selects from the
  appropriate tier(s) via the pure `selectSeries(series, range)` — `raw` for
  short ranges, `hour`/`day` for longer ones — returning the point list the
  openapi `MetricSeriesPoint[]` shape defines (§7, `docs/api/v1/openapi.yaml`).

#### 1.2.2 `task_logs` — persistent per-task log tail (non-folded)

`GET /v1/tasks/:id/log` (§2.1) reads a per-task log tail, but the R0 draft
had **no v1 write path** for it — the daemon still posted log lines to the
legacy `POST /v2/ingest` as `task.log` passthrough events. This freeze adds a
first-class v1 ingest (`POST /v1/tasks/:id/log`, §2.1/§3) and its backing
table:

```sql
CREATE TABLE task_logs (
  task_id    TEXT PRIMARY KEY,
  lines      TEXT NOT NULL,            -- JSON array of strings, oldest first, capped at TASK_LOG_WINDOW
  updated_at INTEGER NOT NULL          -- unix ms
);
```

- **Bounded ring buffer**: the last `TASK_LOG_WINDOW` = **100** lines per
  task; appending past the cap drops the oldest lines (same shape as the
  existing `appendTaskLog` tail in `store.ts`).
- **`POST /v1/tasks/:id/log`** (role `source` only — it is a source uplink,
  like `POST /v1/events`): body `{lines: [string, ...]}`, appended
  best-effort to this task's ring buffer.
- **Non-folded — does NOT participate in `space_revision`.** A log tail is a
  best-effort diagnostic passthrough, not a reliable domain event: same
  rationale as `metric_series` and presence. Appending MUST NOT bump
  `space_revision`, emit a `delta`, or enter the reliable-event/dedup path.
  It is deliberately *not* on the `device_seq` at-least-once track — a
  dropped log line is acceptable; a duplicated or lost one has no
  revision-continuity consequence.

Interest leases (§7 of `proto/realtime/SPEC.md`) are unchanged and stay in
`StateStore`'s `leases` table (device-keyed, decoupled from connection
lifetime, lazy expiry — no new behavior in v1).

### 1.3 `PushOutbox`

Owns everything APNs. Fully specified in `docs/design/v1-apns-outbox.md`;
summarized here:

| table | owns |
|---|---|
| `push_outbox` | one row per pending/attempted/terminal push; frozen column set in `v1-apns-outbox.md` §3 |

Pipeline: a business-event transaction (task.event, message.event, a
config.event mint) writes its `push_outbox` row(s) in the **same**
synchronous transaction as the state write, then calls `ensureAlarm()`. A DO
Alarm — not a Queue — drains the outbox in bounded-concurrency batches. See
`v1-apns-outbox.md` for the full state machine, per-kind idempotency, and
the Queue-escalation criteria for a hypothetical v2.

### 1.4 `CommandStore`

Owns phone → source reverse-control commands that could not be delivered
live because no source connection was open.

| table | owns |
|---|---|
| `pending_commands` | `command_id` (PK), `target_device_id` (nullable = broadcast), `origin_ts`, `ttl_ms`, `payload`, `delivered`, `last_sent_at?`, `send_attempts?` |

`last_sent_at`/`send_attempts` are **optional observability-only columns**
(P0-5, see below) — a server MAY populate them for debugging/metrics, but
MUST NOT let them suppress redelivery or otherwise gate whether a row
appears in a poll response. `delivered` is the only column that controls
whether a row is still returned; see the fetch-then-ack rules below.

Owns **only task-scoped reverse control** — `pause`/`resume`/`stop`, each
naming the `task_id` (a task run) it acts on. `POST /v1/tasks/:id/commands`
(§3) is the HTTP entry point that produces the `command{origin: "viewer"}`
envelope `proto/realtime/SPEC.md` §8 specifies, relayed live if the owning
source is connected or persisted here if not.

`run_now` is **not** a command and does not live here — triggering an
automation is an automation-field poll (`run_request_id`, §5.1), not a
reverse-control command. CommandStore, the WS `command` frame, and the
`POST /v1/events` `commands[]` piggyback carry **only** task-scoped
`pause`/`resume`/`stop`; none of them carries `run_now` or an
`automation_id`.

**Enqueue is directed to the task's owning device, not broadcast (P0-3).**
When `POST /v1/tasks/:id/commands` enqueues a command, the server sets
`pending_commands.target_device_id = tasks.owning_device_id` for that
`task_id` (looked up at enqueue time, §1.2). The command is destined for the
**one** device actually running the task, never fanned out to every source
in the space. The R0 draft's "relay to every connected source and mark
delivered because *some* source is online" path is **removed**: it let a
command for an HTTP-polling task be consumed and marked delivered by an
unrelated resident agent's WS connection, after which the task's real owner
never received it. (If `tasks.owning_device_id` is `NULL` — a command for
a task no `started` has been seen for — the row is enqueued with a `NULL`
target and stays undelivered until, or is dropped when, the task's owner
appears; a server MAY instead reject such a command at
`POST /v1/tasks/:id/commands` with `404 {"error":"task not running"}` rather
than persisting a row no device will ever drain.)

**Delivery is fetch-then-ack, at-least-once (P0-5).** An external review
live-reproduced a data-loss bug against real workerd: the R1 draft flipped
`pending_commands.delivered = 1` the instant a command was *included* in a
`POST /v1/events` response's `commands[]` array — so if that HTTP response
was lost in transit (or the client's local dispatch channel was momentarily
full and silently dropped the frame), the command was gone forever, never
re-sent, even though the owning device never actually acted on it.
"Included in a response" and "durably handed off to the device's local
process controller" are **not the same event**, and only the second one may
ever set `delivered`. The fix is fetch-then-ack, not a smarter drain:

- **Inclusion never sets `delivered`.** A `POST /v1/events` response's
  `commands[]` includes **every** pending, non-expired, task-matching
  command addressed to the authenticated device on **every** poll — not
  just the first time. A device that polls twice without acting in between
  sees the identical command both times. The server MAY record
  `last_sent_at`/`send_attempts` on the row purely for observability (above
  table), but these values MUST NOT be consulted to decide whether the row
  is included in a response — that decision is `delivered = 0` AND
  `now ≤ origin_ts + ttl_ms`, full stop.
- **`ack_command_ids` is how a device retires a command.** `EventsRequest`
  (§4.1) gains an optional `ack_command_ids?: string[]` field. A device
  includes a `command_id` there **only after** it has durably handed the
  action off to its local process controller — e.g. successfully enqueued
  the pause/resume/stop for the owning `sitrep run` process, or applied it
  in-process if it *is* that process. Receiving the command over the wire is
  not enough to ack it; only a successful **local** handoff is.
- **Acks are processed before the response's `commands[]` is computed, in
  the same request.** The server applies every `ack_command_ids` entry
  (setting `delivered = 1` on matching, device-owned rows) **before**
  building the response's `commands[]` — so a single request that both acks
  an old command and polls for new ones never gets back the very command it
  just acked. An ack for a `command_id` this device does not own, has
  already expired, or does not exist, is silently ignored (a no-op, not an
  error) — acking is idempotent and safe to retry, exactly like everything
  else on this route (§4.2).
- **`delivered` is terminal and flips only on a matching ack from the owning
  device.** Once set, the row stops appearing in any future poll response
  (any transport) regardless of TTL. Until acked, it keeps appearing on
  every poll from the owning device, bounded only by the existing TTL — an
  unacked command still expires past `origin_ts + ttl_ms` and is dropped
  from responses / eligible for sweep, exactly as before (§1.4 above).
- **Idempotent actions are what makes at-least-once safe.** `pause`,
  `resume`, and `stop` are idempotent by design: re-applying any of them to
  a task already in that state is a safe no-op, never a double-effect. This
  is the property that lets the server redeliver an unacked command freely,
  without needing a persisted "already executed" log that survives a
  process restart — a fresh `sitrep run` process for the same `task_id` may
  legitimately see and safely re-apply an already-executed-but-unacked
  command (e.g. the process that executed it crashed before writing its
  ack). A non-idempotent action would make this design unsafe; none of the
  three v1 command actions are.

**Two drain transports, one store, one target device, one ack authority.**
The directed command is delivered to its owning device on whichever
transport that device next uses, but only the HTTP path can ever ack it:
- **WS drain** — a `command` frame to the owning device's connection, via
  `drainPendingCommands` on (re)connect and live relay
  (`proto/realtime/SPEC.md` §8). A source WS whose `device_id` is not the
  row's `target_device_id` is **not** a valid recipient and does not drain
  it. **WS delivery is a best-effort hint only — it never sets `delivered`,
  under any circumstance.** The socket that receives the WS frame (typically
  a long-lived resident agent connection) is frequently *not* the process
  that actually executes the task (an HTTP-only `sitrep run`), so the WS
  path has no authority to declare the command handled. This was already
  true before P0-5 and remains unchanged by it — restated here because it
  is easy to mis-generalize "fetch-then-ack" as "every transport needs an
  ack path," which is not what this section specifies.
- **HTTP drain** — piggybacked on the `POST /v1/events` ACK response's
  `commands` array (§4.1), and only for the owning device: the request must
  come from `target_device_id` **and** (for a multi-task device) carry a
  matching `for_task_id`. A short-lived `sitrep run` that never opens a WS is
  reachable this way — every uplink (even an empty `events:[]` heartbeat)
  drains the commands for the task(s) it owns. **This is the only transport
  that can ever set `delivered`**, and only via an explicit
  `ack_command_ids` entry from the owning device, never via mere inclusion
  in a response (above).

Because a command is now addressed to exactly one device, a device running
several concurrent `sitrep run` processes routes each task's command to the
process that owns it: the HTTP drain additionally filters by `for_task_id`
(§4.1) so process A (task A) and process B (task B) on the same device each
receive only their own task's commands, never each other's. `command_id`
keeps execution idempotent if the owning device briefly sees the same
command on both paths (e.g. a WS hint arrives moments before the HTTP poll
that actually acts on and acks it) — a device that has already locally
handed off a `command_id` must not hand it off twice, whether or not it has
acked yet. The daemon-side consumer additionally re-checks
`command.task_id` against the process's own task before acting (defense in
depth), but the server-side `target_device_id` + `for_task_id` addressing is
the **primary** routing guarantee — a command is never consumed by the
wrong device or process in the first place.

### 1.5 Idempotency table (shared, not owned by one module)

`http_idempotency` (`idempotency_key` PK, `fingerprint`, `revision`,
`created_at`) is the control-plane idempotency ledger used by the
automations write routes (§6). It is not "owned" by any one module above
because it exists purely to make `StateStore` writes (config.event minting)
safe to retry over HTTP — same table, same lazy-cleanup policy (drop
entries older than 24 h, cap at the 500 most recent rows) as the current
implementation.

## 2. `/v1` route surface

Every route below is served by the Worker in front of the SpaceHub DO. There
is no route in this list that reads or writes anything other than the one
space's SpaceHub instance — including the two unauthenticated control-plane
routes: `POST /v1/join` routes directly off its required `space` field
(§10.5), with no KV or other side-store involved (P0-6, §1).

### 2.1 State plane

| method | path | what it does |
|---|---|---|
| `GET` | `/v1/realtime` | WebSocket upgrade; carries `proto/realtime/SPEC.md` verbatim |
| `POST` | `/v1/events` | HTTP ingest for device-uplinked events (`task.event`, `message.event`, `metric.frame`) — same SpaceHub ingest function as the WS path (§4) |
| `GET` | `/v1/snapshot` | Full folded state + `space_revision` + transport capabilities (§7, §8) |
| `GET` | `/v1/metrics/:id` | Current folded state of one metric, read from the persistent `metrics_current` table (§1.2.0); `404` is authoritative (never-reported) |
| `GET` | `/v1/metrics/:id/series` | Historical series for one metric, `?range=` |
| `POST` | `/v1/tasks/:id/commands` | Viewer-issued reverse control (`pause`/`resume`/`stop`); relayed live or queued in `CommandStore` |
| `GET` | `/v1/tasks/:id/log` | Read a task's log tail (from `task_logs`, §1.2.2) |
| `POST` | `/v1/tasks/:id/log` | Source uplink of log lines (`{lines:[...]}`); best-effort append to `task_logs`, non-folded (§1.2.2) |
| `DELETE` | `/v1/messages/:id` | Delete one message from history |
| `DELETE` | `/v1/messages` | Clear **all** messages from history (no path id) |
| `GET` | `/v1/automations` | List automations (source's poll-for-definitions path, §5) |
| `POST` | `/v1/automations` | Create an automation (mints `config.event`, §5, §6) |
| `PATCH` | `/v1/automations/:id` | Edit schedule/state (mints `config.event`) |
| `DELETE` | `/v1/automations/:id` | Remove an automation (mints `config.event`) |
| `POST` | `/v1/automations/:id/run` | Trigger an automation now: increments its monotonic `run_request_id`; the resident agent runs it on the next `GET /v1/automations` poll when the id advances (§5.1) |

### 2.2 Control plane

| method | path | what it does |
|---|---|---|
| `POST` | `/v1/spaces` | Create a space + its owner device; returns `{space_id, device_id, owner_token}` (unauthenticated) |
| `POST` | `/v1/join` | Redeem an invite code, mint a device + token; `space` is now a required request field, routing directly to the target SpaceHub with zero KV lookup (§10.5, P0-6); returns `{space_id, device_id, role, token}` (unauthenticated) |
| `POST` | `/v1/invites` | Mint a single-use, 10 min invite code for `viewer` or `source` |
| `GET` | `/v1/devices` | List paired devices |
| `DELETE` | `/v1/devices/:id` | Revoke a device: its token stops resolving **and** every live WS for that device is force-closed in the same operation (§10.2) |
| `PUT` | `/v1/devices/self/push-tokens` | Register the **caller's own** device-level APNs tokens (push-to-start, alert) |
| `PUT` | `/v1/tasks/:id/live-activity-token` | Register the **caller's own** per-activity Live Activity update token for one task |

`GET /healthz` (unauthenticated, unversioned) is unchanged and out of scope
for this freeze.

**`POST /v1/spaces` returns `device_id` (P0-1).** Creating a space mints the
owner **device** server-side (persisted in `DeviceRegistry.devices`) along
with its `owner_token`, and the response now carries that device's `device_id`
alongside `space_id` and `owner_token`. This is not cosmetic: `device_seq`
(the at-least-once dedup key for every `task.event`/`message.event` this
device uplinks, §4.1) is scoped to `(device_id, space)`, so the creating Mac
**must** know its own `device_id` to report tasks/metrics/messages. Without
it in the response the owner device could not populate `body.device_id`
correctly, and its uplinks would be rejected (`body.device_id` must match the
authenticated identity, §4). `POST /v1/join` already returns `device_id`;
`POST /v1/spaces` now matches it.

**`POST /v1/spaces` is rate-limited per source IP.** Being unauthenticated
(there is no device/token yet at space-creation time), `POST /v1/spaces` is
otherwise open to unbounded automated space creation. It is bounded to
`SPACE_CREATION_RATE_LIMIT_PER_HOUR` (default **5**) creates per rolling
window per caller IP, keyed off the `cf-connecting-ip` request header — the
header Cloudflare's edge sets on every real request that reaches the Worker.
A request that arrives **without** `cf-connecting-ip` (local development
against the Worker directly, or a misconfigured proxy in front of it) is not
treated as unlimited: it falls back to a single shared `"unknown"` bucket,
so every IP-less caller collectively shares one rate-limit budget rather
than each bypassing the limit individually. This is a bounded,
fail-closed-leaning fallback, not a loophole — real Cloudflare edge traffic
always carries `cf-connecting-ip`, so the fallback bucket is expected to be
exercised only in local/dev or misconfigured-proxy scenarios, never in
normal production traffic.

### 2.3 API changes from the old `/v2`+`/v3` surface

These are the concrete wire-shape breaks implementers must apply, not just
a path-prefix rename:

- `POST /v2/messages/delete` (body `{ids: [...]}` or `{all: true}`) splits
  into two v1 routes: `DELETE /v1/messages/:id` (one id per call; path
  parameter, not a body array) and `DELETE /v1/messages` (no path id =
  clear all, the successor to the old `{all: true}` form). A client
  deleting N specific messages makes N per-id calls; a client clearing
  everything makes one `DELETE /v1/messages` call.
- `POST /v2/devices` (body `{device_id, push_to_start_token?,
  alert_token?}`) → `PUT /v1/devices/self/push-tokens` (body
  `{push_to_start_token?, alert_token?}`, **no `device_id` field** — the
  target device is the authenticated caller, resolved from the bearer
  token, never asserted in the body). This closes a latent hole in the old
  route: nothing previously stopped a device from registering push tokens
  under a `device_id` that was not its own.
- `POST /v2/activities` (body `{source_id, token}`) → `PUT
  /v1/tasks/:id/live-activity-token` (path carries the task id, body is
  `{token}`). Device-level tokens (push-to-start, alert — "does this device
  want to be pushed to at all") and the per-activity token ("this specific
  Live Activity instance's direct update address") are different in kind and
  different in cardinality (one row per device vs. one row per task), so v1
  splits them into two routes with two owning tables (`push_tokens` vs.
  `activity_tokens`, §1.1) instead of one `/activities` route that
  conflated both.
- `GET /v2/snapshot`'s top-level `realtime_enabled: boolean` field →
  `capabilities.ws_transport_enabled` inside a nested `capabilities` object
  (§8), alongside a new `capabilities.apns_delivery_enabled` field that had
  no v2 equivalent.
- `/v2/automations` (plain CRUD against `UserStore`, no revisioning) and
  `/v3/automations` (SpaceHub-backed, revisioned, `config.event`-minting)
  merge into one `/v1/automations` surface with the **v3 semantics**
  (revisioned, idempotency-keyed, folds into `space_revision`) — this is the
  whole point of the unification: there is no longer a non-revisioned
  automations write path to keep in sync with the revisioned one.
- Metric display **preference** (`PATCH /v2/metrics/:id`, backed by a
  `metric-pref:` KV-style key with no home in the SpaceHub schema) has
  **no v1 route** — **deferred to v1.1**, not dropped (§13.2): cross-viewer
  preference sync needs its own convergence channel (like automations), so
  v1 keeps display preferences client-local.
- Presence tracking is **preserved** (owner ruling): `GET /v1/snapshot`
  returns a `presence` object (`ingest_last_seen`, `agent_last_seen`) built
  from the non-folded `space_meta` markers (§1.2, §7). The old `?agent=1`
  query flag on `GET /v2/automations` that stamped agent presence is gone;
  agent presence is stamped instead on the agent's own uplink/heartbeat
  path (an implementation detail for R1, not a client-visible route).
- Task-log passthrough: the legacy `POST /v2/ingest` carried task-log lines
  as `task.log` domain events on the single ingest firehose. v1 gives them a
  dedicated best-effort route, `POST /v1/tasks/:id/log` (body `{lines:[...]}`,
  `task_logs` table, §1.2.2) — the daemon uplinks log lines there instead of
  smuggling them through the reliable-event ingest. `POST /v1/events` carries
  only the three protocol event types (`task.event`, `message.event`,
  `metric.frame`); it does **not** accept `task.log` (there is no such
  reliable event type — log lines are non-folded, §1.2.2).

## 3. Role/endpoint authorization matrix

**v1 has exactly three roles** (owner ruling): `owner`, `viewer`, `source`.
Every role is resolved through `DeviceRegistry` to a real `device_id`
(§1.1) — there is **no device-less `admin` role, no bare-secret
`AUTH_TOKEN` comparison path, and no legacy single-tenant credential** of
any kind. The old `admin` role and the `AUTH_TOKEN` env fallback are
**deleted**, not merely unreferenced (§10.3, implementer callout).

**`owner` is a strict capability superset of BOTH `source` and `viewer`
(P0-1).** An owner device may call **every** route a `source` may call
(`POST /v1/events`, `POST /v1/tasks/:id/log`) **and** every route a `viewer`
may call. This is not a widening for convenience — it is required for
correctness: the initial Mac creates the space with `POST /v1/spaces` and
holds the **owner** token, and it is *also* the machine that runs and
reports tasks/metrics/messages. Narrowing `owner` to a viewer-class role (as
the R0 draft did when it deleted `admin`) made that Mac unable to call
`POST /v1/events` — a live `POST /v1/spaces` 200 → `POST /v1/events` 403
dead end. `source` and `viewer` are unchanged; only `owner` gains the
source-only cells.

Roles here are the HTTP-layer roles resolved from the `sr1_` bearer token.
The realtime-protocol role (`source`/`viewer`) `proto/realtime/SPEC.md` §9.2
negotiates over `hello` is **declared by the client at connect**, no longer
a fixed function of the HTTP role (§4): a `source`-token may present only WS
role `source`, a `viewer`-token only `viewer`, and an **`owner`-token may
present either** (it reports as a `source` when running tasks, observes as a
`viewer` when watching).

Single-tenant self-hosting does **not** reintroduce an admin/bare-token
path: the menubar app silently creates a space on first launch
(`POST /v1/spaces`) and stores its own `sr1_` **owner device** token — which,
being a superset credential, both reports and observes; a phone joins as a
real `viewer` device via the ordinary invite/QR flow (`POST /v1/invites` →
`POST /v1/join`). There is one credential grammar and three device-backed
roles in every deployment shape.

| route | source | viewer | owner |
|---|---|---|---|
| `GET /v1/realtime` | yes (WS role `source`) | yes (WS role `viewer`) | yes (WS role `source` or `viewer`, client-declared) |
| `POST /v1/events` | yes | no | **yes** |
| `GET /v1/snapshot` | no | yes | yes |
| `GET /v1/metrics/:id` | no | yes | yes |
| `GET /v1/metrics/:id/series` | no | yes | yes |
| `POST /v1/tasks/:id/commands` | no | yes | yes |
| `GET /v1/tasks/:id/log` | no | yes | yes |
| `POST /v1/tasks/:id/log` | yes | no | **yes** |
| `DELETE /v1/messages/:id` | no | yes | yes |
| `DELETE /v1/messages` | no | yes | yes |
| `GET /v1/automations` | yes | no | yes |
| `POST /v1/automations` | no | no | yes |
| `PATCH /v1/automations/:id` | no | yes | yes |
| `DELETE /v1/automations/:id` | no | yes | yes |
| `POST /v1/automations/:id/run` | no | yes | yes |
| `POST /v1/spaces` | unauthenticated | | |
| `POST /v1/join` | unauthenticated | | |
| `POST /v1/invites` | no | yes | yes |
| `GET /v1/devices` | no | yes | yes |
| `DELETE /v1/devices/:id` | no | yes | yes |
| `PUT /v1/devices/self/push-tokens` | no | yes | yes |
| `PUT /v1/tasks/:id/live-activity-token` | no | yes | yes |

`owner` has `yes` in every cell where `source` OR `viewer` has `yes` — that
is exactly what "superset of both" means; the two source-only rows
(`POST /v1/events`, `POST /v1/tasks/:id/log`) are the cells the R0 draft got
wrong and are now **yes** for owner.

Rationale for the rows where `source` and `viewer` differ (owner is `yes`
throughout by the superset rule):

- `GET /v1/automations`: `source` needs this to poll which automations it
  should be running (`docs/design/pairing-and-control.md`: "source: emit
  events, poll automation definitions and receive commands"); `viewer`
  does not call this route directly — a viewer's automations view comes
  from the `automations` array embedded in `GET /v1/snapshot`.
- `POST /v1/automations`: creation is `owner` only (a device must be
  trusted at pairing time to introduce a new automation; `source` and
  `viewer` cannot create); `PATCH`/`DELETE`/`POST .../run` are additionally
  open to `viewer`, which is **intentional** — pausing/resuming/
  rescheduling/deleting/**running** an existing automation from a phone is a
  supported product flow (`pairing-and-control.md`: "The phone may also run,
  pause, reschedule or delete an existing automation. It cannot replace the
  automation's command or Agent prompt.").
- `POST /v1/events` / `POST /v1/tasks/:id/log` are source **uplinks** — a
  device reporting about its own work — so `viewer` cannot call them, but
  `source` and (as a source-superset) `owner` can. A viewer reads the log
  via `GET /v1/tasks/:id/log` but never writes it.

`unauthorized` (401) vs `forbidden` (403): 401 means the bearer token itself
did not resolve to any device/role (missing, malformed, revoked, or a hash
miss); 403 means the token resolved fine but the resolved role is not in the
table's allowed set for that route. `GET /v1/realtime` additionally has a
503 outcome that is neither (§8) — never confuse the transport-disabled case
with an auth failure.

## 4. WS vs HTTP shared ingest

`POST /v1/events` **must** call the exact same SpaceHub-internal ingest path
`GET /v1/realtime`'s `webSocketMessage` handler calls for `task.event` /
`message.event` / `metric.frame` — concretely, the same
`applyReliableEvent(eventType, deviceId, deviceSeq, body, occurredAt)` (for
the two reliable types) and the same best-effort metric-cache path (for
`metric.frame`) that `server/src/realtime/space-hub.ts` already implements.
**There is no second reducer.** The Worker-side HTTP handler for
`POST /v1/events` does exactly three things the WS handler's
`dispatchMessage` also does, in the same order, before calling that shared
function:

1. Parse each item in the request body as an envelope using the same
   `parseEnvelope` validation `guards.ts` already implements (line-for-line
   the same schemas as `proto/realtime/messages/*.schema.json`).
2. Authorize it with the same `authorizeClientEnvelope(role, envelope)` used
   on the WS path — for `POST /v1/events` this evaluates against
   `role: "source"` (the route is `source`-callable, and `owner` reaches it
   as a source-superset, §3), so in practice only `task.event`,
   `message.event`, and `metric.frame` ever pass; anything else (a client
   attempting `config.event`, `snapshot`, `delta`, or a viewer-only type) is
   rejected exactly as it would be over WS. A `viewer`-token never reaches
   this handler (403 at the route gate).
3. Verify `body.device_id` matches the authenticated identity (§10 of
   `proto/realtime/SPEC.md`), identical to the WS handler's check in
   `handleTaskEvent`/`handleMessageEvent`/`handleMetricFrame`.

Only after those three checks does the HTTP handler call into SpaceHub, and
it calls the **same method** the WS handler calls — there must be no
`applyReliableEventViaHttp` twin.

**WS role is client-declared, not derived from HTTP role (P0-1).** On
`GET /v1/realtime` the connecting device declares its intended
realtime-protocol role — `source` or `viewer` — in its `hello{stage:offer}`
(`proto/realtime/SPEC.md` §9.2). The server no longer maps HTTP role to WS
role by a fixed rule ("owner always presents `viewer`" is **removed**);
instead it **constrains** the declared role by the token:

- a `source`-token may present only WS role `source`;
- a `viewer`-token may present only WS role `viewer`;
- an `owner`-token may present **either** — `source` when it opens a
  connection to run/report a task, `viewer` when it opens one to observe.

A device presenting a role its token does not permit is rejected at the
upgrade (`unauthorized`). The `x-sitrep-role` header the Worker forwards to
the SpaceHub DO (`adapters/workers.ts`) therefore carries the *validated
declared* role, not a role mechanically inferred from the HTTP tier — an
owner opening a source connection forwards `source`, and its uplinks are
authorized as a source's are.

### 4.1 device_seq, dedup, and the ACK response shape

`device_seq` handling is unchanged from `proto/realtime/SPEC.md` §5:
scoped to `(device_id, space)`, shared between `task.event` and
`message.event`, deduplicated on `(device_id, device_seq)` in the `dedup`
table. A device that uplinks over HTTP and over WS (e.g. it lost its socket
mid-batch and retried the same events over `POST /v1/events`) uses **one**
counter — the dedup table has no notion of "which transport sent this,"
only `(device_id, device_seq)`. This is what makes it safe for a device to
freely switch transports mid-stream without any handshake: the server-side
identity the dedup and fold logic keys on never mentions transport.

Request body:

```json
{
  "events": [ { "type": "task.event", "id": "...", "ts": 1731999999000, "body": { "...": "..." } } ],
  "for_task_id": "build-release-3.2",
  "ack_command_ids": [ "9f3a…" ]
}
```

`events`: one or more envelopes, same shape
`proto/realtime/envelope.schema.json` + the relevant `messages/*.schema.json`
define. Cap: 500 envelopes per request (an HTTP-layer bound with no WS
equivalent — WS has no batching at all, one frame is one envelope — chosen
to keep one request's processing time bounded; a client with more than 500
queued events makes multiple requests, which is always safe per the retry
contract below).

`for_task_id` (optional): the `task_id` this uplink's process is responsible
for. A `sitrep run` process sets its own `task_id` here so the response's
`commands[]` is scoped to just that task (see below); a general source (the
resident agent, a CI uplink with no task partitioning) omits it. It does not
affect event application in any way — it only scopes command draining.

`ack_command_ids` (optional, P0-5): `command_id`s the device has **durably
handed off to its local process controller** since it last acked — e.g. a
`pause` it has already successfully enqueued/applied locally. This is the
fetch-then-ack retirement channel described in §1.4: a `command_id` MUST
NOT appear here until the local handoff actually succeeded; receiving the
command is not sufficient grounds to ack it. The server applies every
matching, device-owned entry (setting `pending_commands.delivered = 1`)
**before** computing this same response's `commands[]` (below), so a
request that both acks and polls in one call never gets back what it just
acked. An id that does not match a pending row owned by this device
(unknown, already acked, expired, or someone else's) is silently ignored —
acking is a no-op-safe, idempotent operation, safe to retry or to send
redundantly.

Response body — the HTTP-only ACK shape:

```json
{
  "space_revision": 129,
  "acked": [ { "device_id": "dev_abc", "device_seq": 11 } ],
  "results": [
    { "index": 0, "type": "task.event", "status": "applied", "device_seq": 11, "revision": 129 }
  ],
  "commands": [
    { "command_id": "9f3a…", "origin": "viewer", "action": "pause", "task_id": "build-release-3.2", "ttl_ms": 60000, "origin_ts": 1731999990000 }
  ]
}
```

The `9f3a…` command shown here is a **different**, still-unacked command
than the one this same request's `ack_command_ids` retired above — a single
request can legitimately ack an old command and be handed a new (or still-
pending) one in the same round trip.

- `space_revision`: the space's revision **after** applying this batch —
  the HTTP equivalent of "what would `to_revision` be if this had all
  arrived as one delta." A client bootstrapping purely over HTTP (no WS at
  all, e.g. a headless CI agent) can treat this exactly like a `resume`
  reply's revision (§7).
- `acked`: **exactly** the WS `ack.body.acked` shape (`proto/realtime`'s
  `messages/ack.schema.json`) — every `(device_id, device_seq)` pair that is
  now durably applied, whether this call is what applied it or it was
  already applied by an earlier call/connection (§4.2). This is the field
  an implementation should map onto its existing "resend queue" logic
  unchanged: a device retires a queued reliable event the moment its pair
  appears here, exactly as it would on receiving a WS `ack`.
- `results`: an HTTP-only enrichment with no WS analogue, one entry per
  input envelope in input order, so a caller that submitted a mixed batch
  (some reliable, some `metric.frame`) gets feedback on every item, not only
  the reliable ones. `status` is one of `applied` (new), `duplicate`
  (deduped, still counts toward `acked`), `stale` (a `metric.frame` sample
  whose `ts` was not newer than the cached one — silently discarded over
  WS, but HTTP tells you), `rejected` (failed validation/authorization —
  carries an `error: {code, message}` matching `proto/realtime`'s
  `ErrorCode` enum).
- `commands`: the **reverse-control channel for an HTTP-only source**. It
  carries the currently-pending, non-expired commands drained from
  `CommandStore` (§1.4) that are **addressed to this device** — i.e. rows
  whose `target_device_id` equals the authenticated `device_id` (the task's
  owning device, §1.4). These are the exact same `pending_commands` rows the
  owning device would receive as WS `command` frames via
  `drainPendingCommands`. This is what makes an HTTP-only source (the
  short-lived `sitrep run`, which may never open a WS) reachable by
  `pause`/`resume`/`stop`: every uplink — including an empty `events:[]`
  heartbeat POST — polls for and drains the commands for the task(s) it owns.
  Semantics are identical to the WS command path, because they are the same
  rows in the same store:
  - Each entry is shaped like the WS `command` envelope **body**
    (`proto/realtime` `messages/command.schema.json`), purely task-scoped:
    `command_id`, `origin` (`"viewer"`), `action` ∈ `pause`/`resume`/`stop`,
    `task_id`, `ttl_ms`, and `origin_ts` (the viewer's original send time,
    from which `ttl_ms` measures — the HTTP equivalent of the WS envelope
    `ts` the relay preserves). There is **no `run_now` action and no
    `automation_id`** — triggering an automation is a field poll (§5.1), not
    a command.
  - **Device- and task-scoped draining (P0-3).** The drain is first filtered
    to rows whose `target_device_id == the authenticated device_id` — a
    command directed to a *different* device (a different task's owner) is
    never returned here, even if this device is the one polling. Then, when
    the request carries `for_task_id`, the response returns **only** the
    matching rows whose `task_id == for_task_id` (plus any device-addressed
    row with no task scoping). Commands for a *different* task this same
    device owns are **not** drained and **not** marked delivered by this
    request — they wait for that task's process to poll with its own
    `for_task_id`. This is the fix for the multi-process defect: a device
    running several concurrent `sitrep run` processes has each POST with its
    own `for_task_id`, so task A's `pause` reaches task A's process and is
    never consumed by task B's. When `for_task_id` is **omitted** (a general
    source with no task partitioning — the resident agent), the drain returns
    all rows addressed to this device.
  - **Inclusion is NOT delivery (P0-5, fetch-then-ack).** A drained command
    is returned on **every** poll from its owning device as long as
    `delivered = 0` and it has not expired — inclusion in a response never
    by itself sets `delivered`. A device that polls repeatedly without
    acking sees the same command every time; this is intentional
    at-least-once redelivery, not a bug (§1.4). Only non-expired rows
    (`now ≤ origin_ts + ttl_ms`) are included; expired rows are dropped,
    not delivered late — the same 60 s default TTL the reverse-control path
    has always used.
  - `delivered` flips to `1` **only** when the owning device later sends this
    `command_id` in a request's `ack_command_ids` (above) — never merely
    because it was included in a response, and never because of WS activity
    (§1.4). Once `delivered = 1`, the row stops appearing in any future
    response, over any transport.
  - `command_id` is also the idempotency key for **local execution**,
    independent of the delivery/ack bookkeeping above: a source that already
    handed a `command_id` off to its local process controller (e.g. it saw
    the same command over a WS hint moments earlier, then also polled over
    HTTP) MUST NOT hand it off a second time, exactly as
    `proto/realtime/SPEC.md` §8 already requires — this is what makes
    at-least-once *delivery* safe to pair with an idempotent local action
    even before any ack lands. The daemon-side consumer additionally
    re-checks each `command.task_id` against its own process's task before
    acting (defense in depth) — but the server-side `target_device_id` +
    `for_task_id` addressing is the **primary** guarantee that the wrong
    device or process never receives the command in the first place.
  - The field is **omitted or `[]`** when the source has no pending
    commands for its scope — the common case for a heartbeat that finds
    nothing queued.
  - This is the HTTP-transport equivalent of the WS `command` frame: both
    read the **same** `CommandStore` rows, and a device may see a given
    command over either or both transports before acting on it — but only
    an explicit HTTP `ack_command_ids` entry ever sets `delivered` (§1.4),
    so there is exactly one ack authority even though there are two
    delivery-hint transports.

A malformed or unauthorized item in a batch does **not** abort the rest of
the batch: each envelope is independently validated, authorized, and
applied/rejected, matching "an HTTP request is many WS frames arriving
close together" rather than "an HTTP request is one atomic transaction." The
request-level HTTP status is 200 as long as the request body itself parsed
as `{"events": [...]}`; per-item outcomes live in `results`. A request body
that isn't valid JSON, or isn't an object with an `events` array, is `400
{"error": "malformed"}` — no `results` to report per-item because nothing
was parsed.

### 4.2 Idempotency and client retry expectations

`POST /v1/events` is **safe to retry unconditionally** — resending the exact
same batch (same envelopes, same `device_seq` values) after a timeout, a
5xx, or a dropped connection produces the same `acked` set and the same
final `space_revision`, because the dedup key is `(device_id, device_seq)`,
not anything about the HTTP request itself (no request-level
`Idempotency-Key` is needed or honored on this route — `device_seq` already
is the idempotency key, and adding a second one would be redundant and could
disagree with it). A client's retry policy is exactly the WS resend policy
`proto/realtime/SPEC.md` §5.3 already specifies: keep every sent reliable
event queued until its `device_seq` appears in an `acked` response (from
either transport), resend oldest-first with backoff.

The **automations control-plane writes** (`POST`/`PATCH`/`DELETE
/v1/automations[...]`) are different: they are single logical operations,
not a device's replayed event stream, so they use the existing
`Idempotency-Key` header + `http_idempotency` table mechanism unchanged from
the current `/v3/automations` implementation:

- An `Idempotency-Key` header is optional but recommended for any
  automations write a client might need to safely retry.
- The fingerprint bound to a key is the **canonical JSON** (recursively
  key-sorted) of the operation's client-provided content — `{kind,
  automation_id, automation}` — never a server-generated field, so a
  reconstructed retry always fingerprint-matches its own original request.
- Replaying the same key with the same fingerprint returns the original
  result (200, not a new `config.event`, not a new revision). Replaying the
  same key with a **different** fingerprint is `409 {"error": "idempotency
  key was already used for a different operation"}` and mints nothing.
- `POST /v1/automations` that omits `automation_id` derives one
  deterministically from the `Idempotency-Key` (SHA-256, hex, truncated to
  32 chars) **before** the fingerprint is computed, so an honest retry of a
  create (same key, still no explicit id) reconstructs the identical
  payload including the same derived id, rather than either minting a
  second automation or spuriously 409ing against its own retry. A create
  with no `Idempotency-Key` and no explicit `automation_id` gets a random
  id and has no retry safety — callers that need retry safety must supply
  one or the other.
- `http_idempotency` retention is unchanged: entries older than 24 h are
  swept on every write, and the table is capped at the 500 most recent
  rows. A retry arriving after both windows re-executes as a fresh
  operation — accepted, because automation upsert/delete are idempotent at
  the state level (an upsert-by-id or delete-by-id) even without the key.

### 4.3 `metric.frame` rate limit is per-device, not per-connection

`proto/realtime/SPEC.md` §11 caps `metric.frame` at 10/s "per connection."
v1 **refines this to per-device** (keyed on `device_id`), a single limiter
shared by the WS path and `POST /v1/events`. This is a necessary
clarification, not a behavior change for the normal case: HTTP has no
"connection" object to hang a limiter on, and a device that fans its metric
frames across both transports at once could otherwise evade a per-connection
cap entirely. For the ordinary one-connection-per-device case, per-device
and per-connection are behavior-equivalent, and per-device is the only
definition that works uniformly across transports.

Concretely, this **replaces** the current `WeakMap<WebSocket,
RateLimiterState>` limiter in `space-hub.ts` with per-device limiter state
held in the DO (a small in-memory `Map<device_id, timestamps>` recomputed
lazily, same best-effort reset-on-eviction contract the metric cache already
has). A side benefit: the old `WeakMap<ws>` limiter did not survive
hibernation (the `ws` key is a fresh object after a wake), so a
reconnecting connection could briefly exceed the cap; per-device state keyed
by a stable id removes that hazard. A device exceeding the cap gets the same
`error{code: rate_limited}` over WS, and a `results[i].status: "rejected"`
with `error.code: "rate_limited"` over HTTP.

### 4.4 (M5) HTTP-command field mapping

For `POST /v1/tasks/:id/commands`, the server constructs the `command`
envelope's body from **server-trusted** sources, never from the request
body: `CommandBody.task_id` comes from the **path parameter**, and
`CommandBody.issued_by_device_id` comes from the **authenticated identity**
resolved from the bearer token — exactly as the WS handler already binds
`issued_by_device_id` to the connection's authenticated device
(`proto/realtime/SPEC.md` §8/§10). The HTTP request body carries only the
`action` (`pause`/`resume`/`stop`) and the optional `ttl_ms`/`command_id`
(§6); it can never assert which task or which issuing device a command
belongs to. (`POST /v1/automations/:id/run` is **not** a command — it is a
field poll, §5.1 — so it is out of scope for this mapping.)

## 5. Automations: single write path

`GET/POST/PATCH/DELETE /v1/automations[/:id]` all read and write
`StateStore`'s `automations` table through the **same** `mintConfigEvent`
path `space-hub.ts` already implements for `/v3/automations` — there is no
separate non-revisioned automations store to keep in sync (§2.3). Every
successful write mints a `config.event`, advances `space_revision` by
exactly 1, and is folded into `automations` by the deterministic rule
`proto/realtime/SPEC.md` §6.4 already specifies (`automation.upserted`
replaces the whole object; `automation.removed` deletes it) — a viewer that
took a snapshot and a viewer that replayed the resulting `delta` converge on
identical automation state, because both paths are the same table.

`PATCH`/`DELETE /v1/automations/:id` against an id with no existing row is
`404`, checked **before** minting — deleting or editing a nonexistent
automation must not burn a revision on a no-op `config.event`.

### 5.1 `POST /v1/automations/:id/run` — trigger now (a monotonic-id field poll)

Triggering an automation to run immediately (the phone's "run now" button)
bumps a **monotonic counter field on the automation**, not a reverse-control
command. This is the fix for a concrete cross-line defect: the resident agent
is the only process that runs automations, it defaults to
`WS_TRANSPORT_ENABLED`-off and never opens a WebSocket, and it discovers work
solely by polling `GET /v1/automations` as its heartbeat. A WS-only `run_now`
command would therefore sit unclaimed in `CommandStore` until its TTL expired
and the button would do nothing. The proven mechanism is a poll field.

An earlier revision used a wall-clock `run_requested_at` timestamp and had
the agent compare it against a stale, 10-s-refreshed local config value while
polling every 2 s. That was **not** actually idempotent — the server rewrote
`Date.now()` on every retry, and a short automation could finish and re-fire
the same logical request before the next config refresh, double-running it.
This freeze replaces the timestamp comparison with a **monotonic
`run_request_id`**, which removes both the non-idempotency and the clock-skew
comparison entirely:

- **`AutomationState.run_request_id`** (a per-automation integer counter,
  `INTEGER NOT NULL DEFAULT 0`) is part of the automation's folded state
  (StateStore `automations` table, §1.2) and is returned by
  `GET /v1/automations` (and in the snapshot's `automations` array).
- **`POST /v1/automations/:id/run`** (roles `viewer` + `owner`) **increments**
  that automation's `run_request_id` by 1 and returns **`200` with no body**.
  It mints **no** `config.event`, does **not** advance `space_revision`, and
  enqueues **no** command anywhere. It `404`s before writing if the
  `automation_id` names no existing automation (same existence-check
  discipline as `PATCH`/`DELETE`).
  - **Optional `Idempotency-Key` header** dedups a network retry of the
    *same logical tap*: the server records the key→resulting-`run_request_id`
    binding (reusing the `http_idempotency` ledger, §1.5), so replaying the
    same key returns the same `run_request_id` **without** a second
    increment. A retry with no key increments again — which is safe anyway
    (next bullet), but the key makes a double-tap-vs-retry distinction exact.
- **The resident agent** tracks the **last-consumed `run_request_id`** per
  automation in memory (seeded from disk once per automation, then advanced
  in-process — never re-read from the stale local config it refreshes every
  10 s), and separately **persists** it to the machine-local
  `automations.json` store (`LocalAutomation.last_consumed_run_request_id`,
  `daemon/internal/config/automations.go`) after each run completes. On each
  `GET /v1/automations` poll the agent runs the automation iff the server's
  `run_request_id` is **strictly greater** than its last-consumed value, then
  immediately advances that value in memory. The in-memory comparison against
  fresh state is what avoids the poll-skew double-fire within a single
  process's lifetime (2-s poll vs. 10-s config-refresh skew cannot re-trip a
  completed short run); the **persisted** value is what lets a run-now tap
  that arrives while the agent is offline survive a restart instead of being
  silently adopted as "already handled."
  - A device's first-ever sighting of a given automation seeds its
    in-memory last-consumed value from the persisted field, **defaulting to
    0 when absent** — it is never initialized from the server's *current*
    `run_request_id`. Defaulting to 0 is deliberate: if the counter is
    already above 0 (an earlier run-now tap fired while this device had no
    local record — never provisioned yet, or a fresh reinstall), the next
    poll sees `run_request_id > 0 = last-consumed` and correctly runs it
    once. Adopting the server's current value on first sight was the exact
    bug this replaces: it treated any tap that happened before the agent's
    first poll as pre-consumed, silently swallowing it forever.
  - **Two-phase persisted claim/complete around execution.** Immediately
    before starting the executor for a run-requested run, the agent persists
    `claimed_run_request_id = run_request_id`
    (`config.MarkRunClaimed`). Only after the executor returns does it
    persist `completed_run_request_id` and advance
    `last_consumed_run_request_id` together, in one atomic write
    (`config.MarkRunCompleted`). If the process is killed or crashes between
    the two writes, `claimed_run_request_id > completed_run_request_id`
    survives on disk; on the next agent startup this is detected per
    automation and treated exactly like a fresh run-now request — the same
    `run_request_id` is re-executed, at the priority of a pending recovery,
    before schedule-driven runs are considered.
  - If either persisted write fails (e.g. the local disk is read-only), the
    agent logs the error to stderr and proceeds with the in-memory value
    only — it does not crash and does not block the automation from running.
    A **permanently** unwritable local disk therefore means the same
    `run_request_id` is re-claimed (and, per the crash-recovery rule above,
    re-executed) on every subsequent agent restart; this is accepted under
    the at-least-once/idempotency contract below, not treated as a
    correctness bug. Where a health signal exists for the underlying store
    (e.g. `outbox_open`, §14), a persistently unwritable local state
    directory is expected to surface there as well.
- **`run_requested_at`** (nullable Unix-ms timestamp) MAY still be returned
  alongside `run_request_id` for **display only** ("last requested 3s ago") —
  but the agent keys off the **id**, never the timestamp. Its presence is
  optional and carries no control semantics.

**Semantics are at-least-once, not exactly-once.** Because `run_request_id`
is a monotonic counter compared against agent state, there are **no clocks**
in the run-due decision (this dissolves the earlier clock-skew boundary
question) — but the agent state the counter is compared against is
persisted, crash-recoverable, local state, not a distributed transaction
with the server. Two situations are expected, accepted at-least-once
behavior, not bugs:
- A crash between claim and completion (above) re-executes that
  `run_request_id` on restart.
- A device whose local automation record has **no persisted cursor
  fields at all** — either it predates this cursor mechanism, or its
  `automations.json` was hand-restored/hand-written naming an
  `automation_id` that already exists on the server — treats the server's
  current `run_request_id` as unconsumed (see the "defaulting to 0" rule
  above) and runs it once on first sight. This is intentional: it is the
  same offline-tap-recovery path, not a special case. The operator-facing
  consequence is that hand-writing or restoring an `automations.json` that
  names a pre-existing server automation can trigger one immediate run as
  soon as the agent starts — automations must be idempotent and safe to
  re-run for exactly this reason (same standing assumption the
  pause/resume/stop reverse-control commands already rely on, §1.4). A
  **genuinely new** automation created by this device, in contrast, always
  starts at `run_request_id = 0` server-side, so a newly configured
  automation can never fire spuriously on creation.
- A retried `POST .../run` without an `Idempotency-Key` just advances the
  counter and the agent still runs at most once per distinct id it
  observes; with an `Idempotency-Key` even the counter is not
  double-advanced. Neither case changes the at-least-once nature of
  execution itself — the key only protects the counter increment, not the
  claim/complete cycle above.

## 6. HTTP retry & idempotency keys — summary table

| route family | idempotency key | safe to retry unconditionally? |
|---|---|---|
| `POST /v1/events` | `(device_id, device_seq)`, primary and only | yes — always. `ack_command_ids` (§1.4, §4.1) is separately idempotent by construction (acking an unknown/already-acked/expired/foreign id is a silent no-op) — retrying a request that included acks is exactly as safe as retrying one that didn't. |
| `POST/PATCH/DELETE /v1/automations[...]` | optional `Idempotency-Key` header + canonical-JSON fingerprint | yes, if `Idempotency-Key` supplied; otherwise a bare retry may create a second automation (POST) or is a no-op-safe re-apply (PATCH/DELETE are naturally idempotent at the state level even without a key, since they target an existing id) |
| `POST /v1/spaces` | none | no — each call mints a new space; a client must not blindly retry a timed-out call without checking whether the first attempt actually landed |
| `POST /v1/join` | invite code is single-use | no — a retried call after a successful join 404s (code already redeemed), which a client should treat as "check whether the first attempt's response was actually lost, not as a hard failure" |
| `PUT /v1/devices/self/push-tokens`, `PUT /v1/tasks/:id/live-activity-token` | none (idempotent by construction — a PUT of the same token twice has the same effect) | yes |
| `POST /v1/tasks/:id/commands` | optional `Idempotency-Key` header (or body `command_id`), mapped to the WS `command_id` (`proto/realtime/SPEC.md` §8) | yes when a key/`command_id` is supplied — the server dedups on it so a retried POST maps to the same `command_id` and does not enqueue a second command; a bare retry with no key is not exactly-once-safe (a lost response after a successful enqueue could enqueue twice), so callers that need retry safety supply one |
| `POST /v1/automations/:id/run` | optional `Idempotency-Key` (dedups the same tap to one `run_request_id`); the monotonic `run_request_id` itself makes the increment idempotent-per-tap, but agent execution is **at-least-once**, not exactly-once (§5.1) | yes — with a key a retry is not double-counted on the counter; without one it just advances the counter, and the agent's persisted claim/complete cursor still runs at most once per distinct id it observes in the common case (crash recovery or a cursor-less local record can re-run it, §5.1) |
| `DELETE /v1/messages/:id`, `DELETE /v1/messages` | none (idempotent by construction — deleting an already-gone id, or clearing an already-empty history, has the same effect) | yes |

## 7. Snapshot revision semantics

`GET /v1/snapshot` returns the full folded state of the space (`tasks`,
`metrics`, `messages`, `automations` — identical arrays to
`proto/realtime/SPEC.md` §6.2's `snapshot` envelope body) plus
`space_revision`, `capabilities` (§8), and `presence`:

```json
{
  "space_revision": 128,
  "generated_at": "2026-07-19T10:00:00.000Z",
  "capabilities": { "ws_transport_enabled": true, "apns_delivery_enabled": true, "protocol_versions": [1] },
  "presence": { "ingest_last_seen": 1784476800100, "agent_last_seen": 1784476790000, "sources_online": 1 },
  "tasks": [...],
  "metrics": [...],
  "messages": [...],
  "automations": [...]
}
```

### 7.1 `presence` (owner ruling — not folded, not revisioned)

`presence` is built from the non-folded `space_meta` markers (§1.2) plus the
DO's live-connection view, and is **outside `space_revision` accounting** —
it changes on nearly every request and must not perturb the "one increment
per meaningful domain event" invariant (§6.1 of `SPEC.md`) that the delta
machinery depends on. Fields:

- `ingest_last_seen`: Unix-ms of the most recent device uplink (reliable
  event or metric frame, WS or HTTP) into this space.
- `agent_last_seen`: Unix-ms of the most recent agent heartbeat.
- `sources_online`: count of source devices with a currently-open live WS
  (derived from `ctx.getWebSockets()` at snapshot time; 0 when
  `WS_TRANSPORT_ENABLED` is off, since no WS can be open — a client should
  fall back to `ingest_last_seen` recency to drive its status pill in that
  case).

This is the data behind the product's core status pill (green = source
online / recent ingest; amber = stale; red = long silent). Because it is
just reads of `space_meta` plus a live-socket count, computing it adds no
write and no revision to the snapshot path.

A client that bootstraps over HTTP and then wants to keep the connection
live over WS uses `space_revision` exactly as the WS protocol's `resume`
flow (`proto/realtime/SPEC.md` §6.3) uses a locally-tracked `C`: connect,
complete `hello → subscribe`, then send `resume{last_revision:
<the space_revision this GET returned>}`. The decision table in SPEC.md §6.3
applies unchanged — if the space has advanced past the HTTP snapshot's
revision by the time the WS `resume` lands, the server replies with a
catch-up `delta` covering exactly the gap (or, if the gap exceeds the
retention window, a fresh `snapshot` over WS — which the client discards its
HTTP-sourced state in favor of, same as any other snapshot-over-`resume`
case). This is what "bootstrap over HTTP then resume over WS without a gap"
means concretely: the HTTP snapshot's `space_revision` **is** a valid
`last_revision` value for the very next `resume` call, because both are
reads of the same SpaceHub instance's same `space_revision` counter — there
is no second counter to reconcile.

`snapshot.metrics` and `GET /v1/metrics/:id` read the **persistent**
`metrics_current` table (§1.2.0), not a volatile cache — so a rebuilt DO
still serves the last accepted value and its threshold edge-state (P0-2).
These reads remain **outside `space_revision` accounting** (a `metric.frame`
is still non-revisioned, §4.3): persisting the current value durably is a
loss-prevention fix, not a promotion to a revisioned event. The in-memory
`metricsCache` is now only a hot read cache in front of `metrics_current`,
rehydrated on wake; if it is empty the read falls through to the table, never
to "empty/stale." (This supersedes the R0 caveat that `snapshot.metrics`
could be empty after an eviction.) A metric_id's first-ever persisted value
can never be lost to eviction — that write always bypasses the debounce
buffer (§1.2.0). A **routine** value update to an already-persisted
metric_id can be lost if a DO eviction lands inside the 10 s debounce
window; the accepted bound is documented in §1.2.0 and is at most one
downsample window's staleness on the served value, never a reversion to
"empty" and never a loss of the row's existence.

### 7.2 Non-folded read routes (series and task log)

`GET /v1/metrics/:id/series` and `GET /v1/tasks/:id/log` read **persistent
but non-folded** state — `metric_series` (§1.2.1) and `task_logs` (§1.2.2)
respectively. Like `presence` and `metrics_current`, these tables are written
outside `space_revision` accounting: a series append or a log-line append
never increments the revision, never emits a `delta`, and never appears in
the reliable-event stream. This is deliberate — historical series points and
diagnostic log lines are *derived/best-effort* data, not reliable domain
events, so they must not perturb the "one increment per meaningful domain
event" invariant (§6.1 of `SPEC.md`) that delta continuity depends on. Like
`metrics_current`, they are **durable** — they survive DO eviction, which is
exactly why they exist as tables rather than as in-memory buffers (the R0 gap
this re-freeze closes).

## 8. Transport capability / kill switches

Two independent switches. Neither one is allowed to gate a **write** to
SpaceHub (§0). Both are Wrangler `vars`, both use the same strict boolean
parser (§8.3).

### 8.1 `WS_TRANSPORT_ENABLED` (renamed from `REALTIME_ENABLED`)

A pure **transport** switch. It controls exactly two things:

1. Whether `GET /v1/realtime` accepts the WebSocket upgrade at all.
2. Whether `GET /v1/snapshot`'s `capabilities.ws_transport_enabled` field
   reads `true` or `false` — this is how a client learns, without attempting
   and failing an upgrade, whether it should even try.

When `WS_TRANSPORT_ENABLED` is `false`:

- `GET /v1/realtime` returns **`503 {"error": "transport_unavailable"}`** —
  not 403. This is deliberate and implementers must not conflate the two:
  403 means "your role/token doesn't permit this," which is never true here
  (the token may be perfectly valid, an owner device, etc.) — the condition
  is "this deployment currently has WebSocket transport turned off," a
  capability/availability fact, not an authorization fact. A client seeing
  503 should fall back to polling `GET /v1/snapshot` / using `POST
  /v1/events`, not treat its credential as bad.
- `POST /v1/events`, `GET /v1/snapshot`, and every other `/v1/*` route
  continue to read and write the same SpaceHub instance with **no**
  behavior change whatsoever. This is the direct fix for the bug this
  freeze exists to close: the current `/v3/automations` routes gate on
  `realtimeDisabled()` and return 403 when the flag is off, on the theory
  that a "half-open control plane" (WS blocked, HTTP still mutating state
  behind connected viewers' backs) was itself the dual-authority hazard. In
  the unified v1 architecture that reasoning no longer applies: there is
  only one authority (SpaceHub), HTTP is a first-class peer of WS against
  that one authority, not a side-channel that needs to be kept in lockstep
  with a *different* store — so **v1 removes that gate entirely**. Every
  `/v1/automations` write, and `POST /v1/events`, work identically whether
  `WS_TRANSPORT_ENABLED` is true or false. Implementers building the server
  line must not port the old `realtimeDisabled()` check onto any `/v1`
  route other than `GET /v1/realtime` itself.

### 8.2 `APNS_DELIVERY_ENABLED`

Independent switch, orthogonal to `WS_TRANSPORT_ENABLED`. When `false`, the
`PushOutbox` alarm (`v1-apns-outbox.md`) does not dispatch — it still
**enqueues** rows (business events keep writing `push_outbox` rows
normally; state sync is never affected) but the alarm handler no-ops its
APNs calls and leaves rows `pending`, re-arming the alarm for a later check
rather than burning attempts against a switch that is expected to flip back
on. This lets an operator pause outbound push (e.g. during an APNs
credential rotation, or a suspected delivery-storm bug) without touching
WebSocket or HTTP state sync at all. Reflected in `GET
/v1/snapshot`'s `capabilities.apns_delivery_enabled`.

### 8.3 Strict parse semantics (shared by both switches)

Both switches are declared as boolean in `wrangler.jsonc` but — as the
current `parseRealtimeEnabledFlag` in `server/src/app.ts` already documents
and fixes — a Cloudflare dashboard variable override always arrives as a
**string** at runtime regardless of the declared type, and naive
`Boolean("false")` evaluates to `true`. This is not a hypothetical: it is
exactly the footgun a bare JS boolean coercion would reintroduce, and the
existing parser's fix carries forward unchanged (rename only):

```ts
export function parseTransportFlag(value: string | boolean | undefined): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value !== "string") return false;
  const normalized = value.trim().toLowerCase();
  return normalized === "true" || normalized === "1";
}
```

Only the literal boolean `true`, or the trimmed, case-insensitive strings
`"true"` / `"1"`, enable a switch. Every other value — `false`, `"false"`,
`"0"`, `""`, `undefined`, any other string — disables it. Both
`WS_TRANSPORT_ENABLED` and `APNS_DELIVERY_ENABLED` use this same function;
implementers must not write a second, subtly different parser for the
second switch.

## 9. APNs outbox — summary

Fully specified in `docs/design/v1-apns-outbox.md`. In one paragraph: a
business event that should notify a device writes a row to `push_outbox` in
the same synchronous transaction as its state write, then calls
`ensureAlarm()`. The push-worthy events are exactly the **four** kinds
`v1-apns-outbox.md` §4 defines — `push_to_start`, `activity_update`,
`activity_end` (all task-lifecycle Live Activity transitions) and `alert`
(a script-emitted message or a metric-threshold crossing). An automation
config change (`config.event`) is **not** among them — editing an automation
never enqueues a push (matching the existing product behavior). A DO Alarm — not a Cloudflare Queue — drains the outbox in
bounded-concurrency batches per space, because a single space's push fan-out
is small enough that the alarm mechanism's shared SQLite consistency
boundary is strictly simpler than adding a Queue's own write/ack/lease
lifecycle on top. Per-kind idempotency (push-to-start is at-most-once
biased; Live Activity updates coalesce to the latest revision; end/alert
pushes retry only on transient errors and clean up permanently-invalid
tokens) is the hard-requirement core of that document.

## 10. Token format v1: `sr1_<space>_<secret>`

The credential format bumps from `st2_<space>_<secret>` to
`sr1_<space>_<secret>`. **The `1` in `sr1` is a credential-format version,
unrelated to the `/v1` HTTP API version number** — they happen to both be
"1" right now by coincidence of timing, not by design; a future credential
format bump (`sr2`) does not imply or require an HTTP API version bump, and
vice versa. Do not conflate the two version numbers in code or comments.

### 10.1 Grammar

```
sr1_<space_id>_<secret>

sr1              literal prefix
space_id         [a-z0-9]{1,16}   non-secret space locator
secret           [a-f0-9]{48}     24 random bytes, hex-encoded
```

Regex: `/^sr1_([a-z0-9]{1,16})_[a-f0-9]{48}$/`. This is a straight
prefix-and-charset edit of the current `TOKEN_RE` in `server/src/app.ts`
(`st2` → `sr1`); the space-id charset, the 48-hex-char secret length, and
the two-underscore-delimited structure are unchanged.

### 10.2 Security model (unchanged)

- **Non-secret locator, embedded for stateless routing.** `space_id` inside
  the token is exactly the routing key used to address the space's
  SpaceHub DO (`env.SPACE_HUB.getByName(space_id)`) — no separate lookup is
  needed to find which space a token belongs to before validating it.
  Anyone who sees the token can read `space_id`; that alone grants nothing
  (it is not a capability, just an address).
- **24 random bytes of secret.** Generated with `crypto.getRandomValues`,
  same as today.
- **Server stores only the SHA-256 hash.** `DeviceRegistry.token_hashes`
  (§1.1) never stores or logs the raw secret; `resolveToken` hashes the
  presented bearer value and looks up the hash.
  `role`/`device_id` come from the server-side `devices` record the hash
  resolves to, never from anything the client asserts.
- **Revocation is effective immediately, on HTTP *and* on any live WS.**
  `DELETE /v1/devices/:id` deletes the `devices` row, so `resolveToken`
  fails on the device's very next HTTP request (hash resolves to a
  device_id whose record is gone) — no cache or session lifetime to
  invalidate. But an already-open WebSocket resolves the device's identity
  **once**, at the upgrade, and never re-checks the token per frame — so a
  purely "next request" revocation would let a revoked device keep
  streaming on its existing socket indefinitely. **RULING (protocol
  owner):** `DELETE /v1/devices/:id` MUST, in the same operation, force-close
  every live connection for that `device_id`: for each socket in
  `ctx.getWebSockets(device_id)`, send
  `error{code: "unauthenticated", retryable: false, fatal: true}` (the code
  `proto/realtime/SPEC.md` §13 reserves for exactly "the connection's
  credential became invalid mid-connection, e.g. revoked") and then close
  it. A revoked device is thereby cut off on both transports at once, not
  merely blocked from opening *new* connections.

  > **Implementer note (R1):** the revoke handler must reach into the same
  > SpaceHub instance and enumerate `ctx.getWebSockets(deviceId)` to send
  > the `unauthenticated` error + close, as part of the delete — do not
  > rely on the deleted `devices` row alone; nothing re-reads it on the
  > live-socket path.

### 10.3 Migration: no `st2` back-compat parsing

Because this is an unreleased product with no business-data migration
requirement (§11), v1 does **not** parse `st2_...` tokens at all — there is
exactly one accepted grammar, `sr1_...`, enforced by one regex. Everything
that currently encodes or matches `st2_` moves to `sr1_` in the same change:

- `proto/realtime/fixtures/**` — any fixture embedding an example token
  string.
- `TOKEN_RE` and `newToken()` in `server/src/app.ts` (or their v1
  successors).
- The Go daemon's token-handling/regex (if any pattern-matches the prefix
  rather than treating the token as an opaque bearer string).
- The Apple client's Keychain storage and any client-side format
  validation/regex.
- Every doc referencing the `st2_` shape (`docs/design/pairing-and-control.md`,
  this document, README-level examples).

No implementation line ships a parser that accepts both prefixes "to be
safe" — a token is either a valid `sr1_` credential or it is rejected `401`,
full stop. This keeps the authentication surface a single code path with a
single regex to audit, rather than two prefixes whose divergence would be
exactly the kind of dual-track hazard this whole freeze exists to eliminate.

### 10.4 No `admin` role, no `AUTH_TOKEN` — one credential, three device-backed roles

**RULING (protocol owner):** v1 has exactly one credential grammar (`sr1_`)
and exactly three roles (`owner`, `viewer`, `source`), each resolved through
`DeviceRegistry` to a real `device_id` (§1.1, §3). The legacy single-tenant
`AUTH_TOKEN` env secret, and the device-less `admin` role it resolved to,
are **deleted** — there is no bare-secret comparison path anywhere in v1,
and "the credential is either a valid `sr1_` device token or it is `401`" is
therefore literally true (§10.3 has no `AUTH_TOKEN` exception hiding behind
it). Self-host single-tenant is served entirely by ordinary device tokens:
the menubar app silently creates a space on first launch (`POST /v1/spaces`)
and holds its own `sr1_` **owner device** token; a phone joins as a real
`viewer` device through the normal invite/QR flow — no admin, no shared
secret, in any deployment shape.

> **Implementer note (R1):** deleting `admin`/`AUTH_TOKEN` means the
> `authToken` option and its resolution branch in `server/src/app.ts`'s
> `authenticate` middleware (the `if (admin && got === admin) { role:
> "admin", space: "default" }` path and the `!admin && !opts.registry`
> open-dev bypass), the `AUTH_TOKEN` field in `Secrets`
> (`adapters/workers.ts`), and the `/debug/tokens` route that authenticates
> against it, must all be **removed from the code**, not merely left
> unreferenced. A dormant bare-secret branch is exactly the kind of second
> authentication path this freeze exists to eliminate. `Role` narrows to
> `"owner" | "viewer" | "source"`; there is no `"admin"` member.

### 10.5 Connect-code encoding: self-routing, zero KV dependency (P0-6)

**RULING (protocol owner):** the visual "connect code" a phone scans or
types to join a space now **encodes `space_id` directly**, eliminating the
KV-lookup routing failure class at the protocol level rather than papering
over it with a retry. This section is the exact byte/char layout — the
daemon's minting implementation and Apple's decoding implementation must
independently produce/consume an identical format without further
coordination.

**Problem this replaces.** The R1 connect code (`apple/SitrepKit/Sources/
SitrepKit/ConnectCode.swift`) was 16 characters of pure random noise —
`X` + 14 random + `Z` — with no `space_id` embedded, so `POST /v1/join`
had to resolve the target space via `INVITE_DIR`, a KV code→`space_id`
lookup. KV is eventually consistent (cross-region propagation lag), so a
join attempted immediately after invite creation could scan a replica that
had not yet observed the write and spuriously 404. A retry-on-404 patch
treats the symptom; it does not change that the join path's correctness
still depended on an eventually-consistent store. Encoding the space
directly in the code removes that dependency instead of retrying around
it.

**Shared alphabet (unchanged from the current implementation).** v1 already
mints `space_id` from a 31-symbol, confusable-free alphabet — digits `2-9`
and lowercase letters `a-z` excluding `i`, `l`, `o` (`server/src/app.ts`'s
`newSpaceId()`: `"abcdefghjkmnpqrstuvwxyz23456789"`, 10 characters) — and
`ConnectCode.swift`'s existing 14-character noise payload already drew from
the exact same 31 symbols, case-folded (`A-HJ-KM-NP-Z2-9`, excluding `0`,
`1`, `O`, `I`, `L`). Because the two alphabets are identical sets, embedding
a `space_id` inside the connect code needs no re-encoding or translation
table — a `space_id` character and a connect-code character are the same
symbol, one lowercase and one uppercase.

```
Alphabet (31 symbols, case-insensitive; canonical/server-side form is
lowercase, matching the SpaceId token grammar's charset, §10.1):

  2 3 4 5 6 7 8 9 a b c d e f g h j k m n p q r s t u v w x y z

  (digits 2-9, then a-z excluding i, l, o)
```

**Layout — 21 characters, fixed-width, uppercase display form:**

```
Position   Length   Content
--------   ------   -------------------------------------------------
[0]        1        'X'  — literal start anchor (fixed)
[1..10]    10       space_id, uppercased 1:1 (this space's actual
                     space_id, same alphabet, no transformation beyond
                     case-fold)
[11..19]   9        secret — a freshly random one-time verification
                     secret, same alphabet, minted per invite
[20]       1        'Z'  — literal end anchor (fixed)

Total: 21 characters.
```

Display/scan regex: `/^X[2-9A-HJ-KM-NP-Z]{19}Z$/` (shape-only check, mirrors
the existing `X…Z` anchor pattern implementers already rely on for
instant live-scan verification against OCR transcripts — anchors are
**positional**, not exclusive: `X` and `Z` are themselves valid alphabet
symbols and may also appear inside positions `[1..19]`, exactly as in the
R1 code). Decode: `space_id = lowercase(code[1..10])`,
`secret = lowercase(code[11..19])`.

Entropy: the 9-character secret carries `9 × log2(31) ≈ 44.6` bits —
ample for a single-use, 10-minute-TTL, server-validated secret exchanged
between two devices already in scanning/typing proximity. The prior
16-character length is **not** preserved; per the reviewer's explicit
guidance, precision of the byte/char layout takes priority over matching
the old length.

**`POST /v1/join` now always requires `space` (unifies both join paths).**
`space` moves from optional to **required** in the request body
(`{code, space, name?, platform?}`, openapi.yaml). Both join paths now
populate it the same way, collapsing what used to be two special cases into
one call shape:
- **Connect-code path** (scan/paste, no explicit space in the payload
  before this freeze): the client decodes `code` locally per the layout
  above and sends the resulting `space_id` as `space`.
- **Self-host deep-link path** (`sitrep://join?server&space&code`): already
  carried `space` explicitly; unchanged, now simply the same required field
  every other join call also sends.

**Routing and validation, in order:**
1. The Worker resolves the target SpaceHub directly from the request's
   `space` field — `env.SPACE_HUB.getByName(space)` — **zero** lookup of
   any kind. This is the load-bearing fix: routing no longer depends on
   `code` being decoded correctly, or on any store besides the one
   SpaceHub the request is already headed to.
2. Inside that SpaceHub, the server decodes `code` itself (same layout) as
   a defense-in-depth structural check: malformed shape (wrong length,
   missing/wrong anchors, out-of-alphabet character) is `400
   {"error": "malformed code"}`.
3. The code's embedded `space_id` (from `code[1..10]`) MUST equal the
   `space` the request routed on; a mismatch is `400
   {"error": "code does not match space"}` — this catches a corrupted or
   cross-space-pasted code early, with a clear error, rather than a
   confusing downstream 404.
4. The extracted `secret` (`code[11..19]`, lowercased) is looked up in this
   SpaceHub's own `DeviceRegistry.invites` table (§1.1) — single-use,
   10-minute TTL, deleted on redemption, exactly as before. Not found /
   expired / already redeemed is `404 {"error": "invite invalid or
   expired"}`, unchanged from the current response shape.
5. On success: delete the invite row, mint the device + token, respond
   `{token, device_id, role, space_id}` — the response shape is unchanged.

**`DeviceRegistry.invites` schema updates its primary key.** Because
`space_id` is now implicit (the SpaceHub instance the row lives in already
*is* that space), the table's primary key moves from the old opaque `code`
to `secret`: `invites` = `secret` (PK), `role`, `created_at`. This is the
**only** SpaceHub table shape this section changes; every other invariant
in §1.1 (single-use, 10 min TTL, SHA-256-only for device *tokens* — the
invite secret itself is short-lived and low-stakes enough to store raw, as
the old opaque `code` already was) is unchanged.

**`POST /v1/invites` mints the full self-routing code.** The response's
`code` field is now the complete 21-character layout above — the SpaceHub
prefixes its own (already-known) `space_id` to a freshly generated 9-char
secret. `expires_in`/`space_id` in the response are otherwise unchanged.

**KV is fully removed from the join path (P0-6, §1).** There is no
remaining scenario in v1 where a code arrives with no space context — every
code is now self-describing, and the deep-link path always carried `space`
explicitly anyway. `INVITE_DIR` is deleted, not merely deprioritized to an
optional cache: see §1 for the corresponding removal from the "one
authoritative store" invariant, and §11.2 for the corresponding deployment
note (no KV namespace to carry over on a from-scratch service rebuild).

## 11. `UserStore` deletion / deployment plan

Main's `wrangler.jsonc` has **already declared and deployed**
`migrations: [{ tag: "v1", new_sqlite_classes: ["UserStore"] }]`. Cloudflare
Durable Object migrations are an **append-only, sequential ledger** scoped
to one Worker service: you cannot edit or remove an already-shipped
migration tag, and a class name that has ever been the target of a
`deleted_classes` entry can never be reused for a new SQLite-backed class of
the same name in that service again. Given that, "delete `UserStore`" is not
a code change alone — it requires an explicit new migration entry, and the
two concrete options below have different operational cost.

No business-data migration is required either way: this is an unreleased
product, so every row currently in `UserStore` (mock data, pre-launch
pairing state) is droppable. The plan below is about the **mechanics** of
retiring the class safely, not about preserving its contents.

### 11.1 Option A — keep the existing Worker/domain/secrets; tombstone `UserStore`

Two sequential deploys against the **same** `sitrep-server` Worker service,
because a `deleted_classes` migration and the removal of all code/bindings
referencing that class must land together, and a live space migrating from
`UserStore`-backed pairing state to `SpaceHub`-backed pairing state needs
`SpaceHub` to exist first.

**Deploy 1** (safe to ship any time, purely additive — this is what
`realtime-integration`'s `wrangler.jsonc` already does today):

```jsonc
"durable_objects": {
  "bindings": [
    { "name": "USER_STORE", "class_name": "UserStore" },
    { "name": "SPACE_HUB", "class_name": "SpaceHub" }
  ]
},
"migrations": [
  { "tag": "v1", "new_sqlite_classes": ["UserStore"] },
  { "tag": "v2", "new_sqlite_classes": ["SpaceHub"] }
]
```

**Deploy 2** (the actual cutover — ships once `/v1` routes exist and no
code path references `UserStore` anymore):

```jsonc
"durable_objects": {
  "bindings": [
    { "name": "SPACE_HUB", "class_name": "SpaceHub" }
  ]
},
"migrations": [
  { "tag": "v1", "new_sqlite_classes": ["UserStore"] },
  { "tag": "v2", "new_sqlite_classes": ["SpaceHub"] },
  { "tag": "v3", "deleted_classes": ["UserStore"] }
]
```

Requirements on this deploy, all of which must land in the **same** commit
that ships tag `v3` (Wrangler will refuse a deploy where the code still
imports/exports a class a migration in the same or an earlier tag deletes,
and a dangling binding to a deleted class is also an error):

- Every export of `UserStore` and every route/handler that touches it
  (`server/src/adapters/workers.ts`'s `UserStore` class, `stub()`/`doStore()`/
  `doRegistry()` helpers, the `/v2` and `/v3` Hono route registrations in
  `server/src/app.ts`) is deleted from the codebase, not merely unreferenced.
- The `USER_STORE` binding is removed from `wrangler.jsonc` in the same
  change.
- `tag: "v3"` is a **new**, never-before-seen tag string — `v1` and `v2`
  above are exactly what already shipped (or will have shipped in Deploy 1)
  and must not be edited.

**Rollback note**: `deleted_classes` is **not reversible**. Once tag `v3`
ships, `UserStore` can never again be declared as a `new_sqlite_classes`
target in this service — the name is permanently retired from this Worker's
migration history. If a production issue is discovered after this deploy,
the fix is to roll forward (a new tag, e.g. reintroducing equivalent state
under a **different** class name), never to attempt to "undo" tag `v3`.

### 11.2 Option B — rebuild the Worker service from scratch

Provision a new Worker service (a new `name` in `wrangler.jsonc`, e.g.
`sitrep-server-v1`) with a migrations array that never mentions `UserStore`
at all:

```jsonc
"name": "sitrep-server-v1",
"durable_objects": {
  "bindings": [{ "name": "SPACE_HUB", "class_name": "SpaceHub" }]
},
"migrations": [{ "tag": "v1", "new_sqlite_classes": ["SpaceHub"] }]
```

Then: re-provision secrets (`APNS_KEY_P8`, `APNS_KEY_ID`, `APNS_TEAM_ID`) on
the new service (no KV namespace to carry over — `INVITE_DIR` is removed in
v1, §1/§10.5, P0-6), cut the custom-domain route
(`sitrep.quintinshaw.com`) over from the old
service to the new one, and finally decommission the old `sitrep-server`
service (`wrangler delete` or leave it dormant with no route pointed at
it).

Tradeoff versus Option A: Option B has a zero-migration-baggage clean slate
(no tombstoned class name, no append-only ledger to reason about later) at
the cost of a domain cutover window, re-provisioning every secret by hand,
and carrying two live services during the transition. Option A stays on the
already-configured domain/secrets/KV and costs exactly one irreversible
tombstone entry in the migrations ledger. **This freeze recommends Option A**
given there is no business data to preserve either way and the domain/secret
provisioning cost of Option B buys nothing this product needs — but the
decision is the deploying engineer's to make at cutover time, and both are
documented here precisely because either is a legitimate, fully-specified
path.

## 12. Consistency checklist for implementers

Before shipping any `/v1` route, confirm:

- [ ] It resolves the space's SpaceHub instance the same way every other
      route does (`env.SPACE_HUB.getByName(spaceId)`), and touches no other
      Durable Object class.
- [ ] Any write to `task`/`message`/`automation`/event-log state goes
      through the same fold/ingest functions the WS path uses — never a
      parallel implementation, even a "temporarily simpler" one.
- [ ] `WS_TRANSPORT_ENABLED=false` is checked **only** by `GET
      /v1/realtime`. If you find yourself adding this check to any other
      route, stop — that is the bug this freeze exists to remove (§8.1).
- [ ] Every token literal, regex, or fixture you touch uses `sr1_`, never
      `st2_` (§10.3).
- [ ] Role checks match §3's table exactly. `owner` is a strict superset of
      both `source` and `viewer` — it is `yes` in every cell either of them
      is, including the source-only `POST /v1/events` and
      `POST /v1/tasks/:id/log` (P0-1). `GET /v1/automations` excludes viewer;
      `POST /v1/automations` excludes both source and viewer.
- [ ] `GET /v1/realtime` WS role is client-declared and token-constrained
      (source-token→source, viewer-token→viewer, owner-token→either), never
      a fixed HTTP-role→WS-role mapping (§4).
- [ ] A reverse-control command enqueues with `target_device_id =
      tasks.owning_device_id` — never a space-wide broadcast (§1.4, P0-3).
- [ ] A reverse-control command's `delivered` flag flips **only** on a
      matching `ack_command_ids` entry from the owning device — never on
      mere inclusion in a `POST /v1/events` response's `commands[]`, and
      never on a WS drain/relay (WS is always a best-effort hint). If you
      find yourself setting `delivered` anywhere except the
      `ack_command_ids` handler, stop — that is the P0-5 bug this freeze
      exists to remove (§1.4, §4.1).
- [ ] `POST /v1/join` routes on the request's `space` field
      (`env.SPACE_HUB.getByName(space)`) with **no** KV read anywhere in the
      routing path. `INVITE_DIR` does not exist in v1 (§1, §10.5, P0-6). If
      you find yourself adding a KV binding or lookup to the join path,
      stop — that is the bug this freeze exists to remove.
- [ ] `metrics_current` never grows past 256 distinct `metric_id`s per
      space (LRU eviction on overflow), and alert edge-state persistence is
      never gated by the routine-sample write-downsample — an edge
      transition writes immediately, every time (§1.2.0, P0-7).

## 13. Frozen decisions & open questions for the protocol owner

### 13.1 Resolved by owner ruling — now frozen in-scope

These were flagged as open in the R0 draft and have since been ruled on;
they are recorded here for traceability but are **no longer open**:

- **`admin` role and `AUTH_TOKEN` deleted** (§3, §10.4): one credential
  grammar, three device-backed roles, no bare-secret path. Self-host uses
  ordinary owner/viewer device tokens.
- **`owner` is a capability superset of `source` and `viewer` (P0-1)** (§3):
  an owner device may call every source-only and viewer-only route,
  including `POST /v1/events` — the fix for the space-creating Mac being
  unable to report tasks. WS role on `GET /v1/realtime` is client-declared
  and token-constrained (owner may be either), not a fixed HTTP→WS mapping
  (§4). `POST /v1/spaces` now returns `device_id` (§2.2).
- **Automation "run now" is a monotonic-id field poll (P0-4)**
  (`POST /v1/automations/:id/run`, §2.1, §5.1): roles viewer + owner. It
  increments the automation's `run_request_id` counter (returned by
  `GET /v1/automations`); the resident agent runs the automation when the id
  advances beyond its last-consumed, persisted cursor — no wall-clock
  comparison, and **at-least-once**, not exactly-once (a crash between
  claim and completion, or a local record with no persisted cursor, can
  re-run the same id; §5.1). An optional `Idempotency-Key` dedups a network
  retry of the same tap. It is **not** a WS command — the agent never opens
  a WS — so `run_now` is not in the command channel at all (§1.4).
- **Reverse-control commands are directed to the task's owning device
  (P0-3)** (§1.4, §4.1): `tasks.owning_device_id` (the first `started`
  reporter) is the enqueue `target_device_id` — never a space-wide
  broadcast.
- **Command delivery is fetch-then-ack, at-least-once (P0-5)** (§1.4, §4.1):
  a live-reproduced bug showed a command could be lost forever if the HTTP
  response that included it was dropped in transit, because inclusion alone
  set `delivered`. Fixed: `commands[]` in a `POST /v1/events` response
  includes every pending, non-expired, device-addressed command on **every**
  poll, regardless of prior inclusion. `EventsRequest` gains
  `ack_command_ids?: string[]`, processed **before** that same response's
  `commands[]` is computed; `delivered` flips to `1` **only** on a matching
  ack from the owning device, never on inclusion, and never via WS (WS
  delivery has always been, and remains, a best-effort hint that never sets
  `delivered`). Safe because `pause`/`resume`/`stop` are idempotent —
  at-least-once redelivery needs no persisted done-log across process
  restarts.
- **Self-routing connect code eliminates the KV join dependency (P0-6)**
  (§1, §10.5): the 21-character connect code now encodes `space_id`
  directly (positions `[1..10]`) alongside a 9-character one-time secret
  (`[11..19]`), reusing the existing confusable-free 31-symbol alphabet
  `newSpaceId()` already mints from. `POST /v1/join` requires `space` and
  routes to the target SpaceHub with zero KV lookup; invite validation
  happens entirely inside that SpaceHub's `DeviceRegistry.invites` (keyed
  by `secret`, not the old opaque `code`). `INVITE_DIR` is deleted, not
  merely bypassed — this closes the failure class (KV eventual consistency
  spuriously 404ing a fresh invite) at the protocol level rather than
  papering over it with a retry.
- **Metric current-value + alert edge-state are persistent (P0-2)** (§1.2.0):
  `metrics_current` table survives DO eviction, so `GET /v1/snapshot` /
  `GET /v1/metrics/:id` still serve the last value and a fired threshold
  stays fired (no duplicate alert on rebuild). This makes the metrics `404`
  authoritative for a space within the metric cap (resolving former §13.2
  item 3; see the P0-7 bullet immediately below for the cap's narrow,
  documented exception). **Existence and freshness are two different
  guarantees, not one (§1.2.0):** a metric_id's first-ever persisted value
  is always synchronous (never buffered, never lost to eviction), which is
  what makes the `404` invariant absolute; a routine value update to an
  already-persisted metric_id is debounced, and a DO eviction inside that
  10 s window can lose at most that window's tail of value updates — the
  row still exists and is served, just up to ~10 s stale. This staleness
  bound is a documented, accepted tradeoff, not a bug.
- **`metrics_current` is capped and write-downsampled (P0-7)** (§1.2.0): the
  persistent table has no equivalent of the in-memory `metricsCache`'s LRU
  cap, so an unbounded number of distinct `metric_id`s could grow the table
  without limit, and every single `metric.frame` sample UPSERTing
  unconditionally was write amplification (one SQLite write per sample).
  Fixed: capped at 256 distinct `metric_id`s per space (LRU eviction on
  overflow), reusing the same `METRIC_CACHE_MAX_METRICS` constant/value the
  in-memory cache already uses — one cap number, not two. Routine
  (non-edge-crossing) samples are persisted on a 10 s per-space debounce
  (last-value-wins); an alert edge transition (armed→fired or fired→cleared)
  always persists immediately, in the same transaction as its triggering
  sample, bypassing the debounce entirely — alert correctness is never
  delayed by the write-timing optimization.
- **Revocation force-closes live WS** (§10.2): `DELETE /v1/devices/:id`
  sends `error{code:"unauthenticated"}` + close on every open socket for
  the device, in the same operation.
- **Presence preserved** (§1.2, §7.1): non-folded `ingest_last_seen`/
  `agent_last_seen` in `space_meta`, surfaced as `GET /v1/snapshot`'s
  `presence` object; does not increment `space_revision`.
- **Task-command idempotency** (§6): `POST /v1/tasks/:id/commands` accepts an
  optional `Idempotency-Key` header (or body `command_id`), mapped to the WS
  `command_id` so a retried POST does not enqueue a second command.
- **Bulk message delete** (§2.1, §3): `DELETE /v1/messages` (no path id)
  clears all, distinct from `DELETE /v1/messages/:id`.
- **`metric.frame` rate limit is per-device** (§4.3), shared across WS and
  HTTP, replacing the per-`WebSocket` `WeakMap` limiter.
- **Daemon local health status moves to a `health.d` directory** (§14): a
  single shared `health.json` raced multiple independent short-lived
  processes' read-modify-writes, silently clobbering each other's
  component entries. Replaced with one file per component
  (`health.d/<component>.json`), atomically written; absence still means
  healthy at both the directory and per-file level; a reader-side staleness
  rule (mtime older than 5 minutes) resolves a crashed process's
  never-recovered failure without a permanent false alarm. Also closes a
  gap where an outbox-open failure at daemon startup was reported only to
  stderr, never to health status.

### 13.2 Still open / deferred

1. **Metric display preference (`PATCH /v2/metrics/:id`) is deferred to
   v1.1**, not dropped. Per-viewer metric label/threshold overrides need
   their own cross-viewer convergence channel — the same way automations
   converge, i.e. a new folded reliable event type in a `v1.1` protocol
   bump — because a preference one viewer sets must reach the others
   deterministically. Adding that is out of scope for this freeze; **v1
   keeps display preferences client-local** (each device stores its own,
   no server round-trip), and `GET /v1/metrics/:id` returns only the raw
   folded metric with no preference overlay. Revisit when v1.1 opens the
   protocol for a new event type.
2. **Single-viewer-per-task Live Activity token (§1.1) is preserved
   as-is** — accepted for v1, revisit in v1.1. `activity_tokens` keeps one
   row per `task_id`, last-write-wins across devices. A product decision is
   needed on whether v1.1 should key this `(device_id, task_id)` instead,
   to let multiple viewer devices each run their own Live Activity for the
   same task.
3. **`GET /v1/metrics/:id` 404 ambiguity — RESOLVED by P0-2 for spaces
   within the metric cap, no longer open.** With the persistent
   `metrics_current` table (§1.2.0) the metric's existence-of-record is
   durable, not a best-effort evictable cache: for a space that has never
   exceeded 256 distinct `metric_id`s, the row is present iff the metric was
   ever reported, so a `404` unambiguously means "never reported." The
   former in-memory-only ambiguity is gone. **Narrow, documented exception
   added by P0-7 (item 5 below):** a space that *has* exceeded the 256-cap
   can 404 on a metric_id that was genuinely reported but LRU-evicted to
   make room for a newer one — a deliberate bounded-growth tradeoff, not a
   regression of this resolution. (Kept here for traceability; not an open
   item.)
4. **Recommended `UserStore` retirement path is Option A, not Option B**
   (§11) — flagged in case the deploying engineer has an operational reason
   (e.g. wanting to shed the `st2`-era KV namespace entirely rather than
   reuse its id) to prefer the from-scratch rebuild instead. Not a blocker;
   both paths are fully specified.
5. **`metrics_current` cap is a hard per-space budget of 256 distinct
   metric_ids (P0-7, §1.2.0)** — not an open question, but flagged here
   because it is a product-facing constraint: a space (an unusual shape —
   the expected product has a handful of metrics) that reports more than
   256 distinct metric_ids concurrently will silently lose the
   least-recently-updated one's current-value/series-continuity-of-record
   past the cap. If a future product need requires more, raising the cap is
   a config change (one shared constant, §1.2.0), not a design change.

## 14. Daemon local health status: `health.d` directory (multi-process-safe)

**Status: this section is scoped to the daemon/apple lines' local-file
convention, not the `/v1` HTTP wire contract** — nothing here has an HTTP
representation, so unlike §1–§13 it does not touch `openapi.yaml`,
`docs/api/v1/fixtures/`, or `server/src/v1/contract/types.ts`; those three
artifacts are the HTTP wire-shape contract and this is not a wire shape.
It is documented here, in the narrative architecture doc, because it is
still a cross-line (daemon + apple) convention that needs one unambiguous
specification rather than two independently-drifting implementations —
exactly the same rationale that puts §10.3's daemon token-handling and
Keychain-storage notes in this document.

**Problem this replaces.** A single shared file, `~/.config/sitrep/
health.json`, is read-modify-written by multiple independent, short-lived
daemon processes (`sitrep run`, `sitrep report`, `sitrep agent`) that each
hold their own in-memory view of "which components currently have an
issue." Because each process's view is independent, one process's write —
which recomputes and overwrites the *entire* file from only what it
locally knows — can silently clobber another process's currently-reported
issue for a *different* component, since neither process has visibility
into the other's in-flight state. Separately, an outbox-open failure at
daemon startup is today reported only to stderr, never to health status at
all, so the menubar app has no way to surface it.

**RULING (protocol owner): replace the single file with a directory, one
file per component.**

```
~/.config/sitrep/health.d/<component>.json

{
  "ok": boolean,
  "reason": string    // present/non-empty only when ok == false
}
```

- **One file per health component** (e.g. `outbox.json`, `device_seq.json`,
  `outbox_open.json` — see below; the set is extensible, any component name
  is a valid file). Concurrent processes touching **different** components
  never race with each other at all, because they touch different files.
  Concurrent processes touching the **same** component only need simple
  last-write-wins semantics on that one small file — there is no
  shared-in-memory map to synchronize across process boundaries, which is
  what made the single-file design race in the first place.
- **Written atomically**: temp file in the same directory, then rename over
  the target path — the same temp-file-plus-rename pattern the current
  single-file implementation already uses (`daemon/internal/health`'s
  `atomicWrite`), applied per-component-file instead of to one whole-file
  rewrite. A reader never observes a partially-written file.
- **Absence means healthy**, preserved from the single-file design at both
  levels: the entire `health.d/` directory being absent, or one specific
  component's file being absent, both mean that component (or, for a
  missing directory, every component) is healthy. A component is never
  "unknown/error" by default — only an explicit `{"ok": false, ...}` file
  signals a problem.
- **Staleness resolves a component without requiring an active recovery
  write.** A reader (the menubar app) treats a component file as
  contributing to the combined warning surface only if it is **both**
  `ok: false` **and not stale**. A file is stale when
  `now - mtime > HEALTH_STALE_AFTER_MS` (**300000**, 5 minutes). A stale
  `ok: false` file is treated as resolved/healthy for aggregation purposes,
  even though its on-disk content still says otherwise. This is what lets a
  short-lived process's failure self-heal without a guaranteed follow-up
  write: a `sitrep run` invocation that fails to open its outbox, reports
  that failure, and then exits (crashes, or simply finishes) never gets a
  chance to write a recovery file — but its failure report ages out of the
  aggregate warning after five minutes with no fresh re-report, rather than
  becoming a permanent false alarm. A long-running process (`sitrep agent`)
  MAY still proactively write `{"ok": true}` on a later successful
  recovery, for faster UI resolution than waiting out the staleness window,
  but this is an optimization, not a requirement — correctness does not
  depend on any process ever writing a recovery file.
- **Reader aggregation**: scan `health.d/`, parse every `*.json` file found,
  discard stale ones (above), and combine the remaining non-stale
  `ok: false` files' `reason`s into the existing single warning-banner UI
  surface (the union of currently-live, non-stale failures — an
  implementation detail of presentation left to the apple line; the data
  model handed to it is this filtered set).

**Components defined by this freeze:**

| file | reported by | condition |
|---|---|---|
| `outbox.json` | any process opening the local outbox/seq store | existing backpressure/capacity check (unchanged from the single-file design) |
| `device_seq.json` | any process allocating a device_seq | existing check: the device_seq allocator DB is unwritable (unchanged) |
| `outbox_open.json` | `sitrep run`, `sitrep report`, `sitrep agent` — any process, at startup | **NEW (closes the stderr-only gap):** the outbox/seq store failed to open at all. Previously logged to stderr only, with no health signal; this freeze requires it to ALSO write `health.d/outbox_open.json = {"ok": false, "reason": "<short error text>"}`, atomically, in addition to (not instead of) the existing stderr log, before the process exits or continues in a degraded mode. |

**Caveat: `sitrep report`'s durability guarantee is conditional on the
outbox being openable.** `sitrep report` (the Claude Code hook's reporting
entry point) routes a hook-reported event through the same durable local
outbox and persistent `device_seq` allocator as `sitrep run`'s realtime
uplink and `sitrep agent` — the event is durable on disk the instant
`outbox.Store.Enqueue` returns, before the process ever touches the network,
so a network blip or the process being killed mid-flush cannot silently
drop it. That guarantee holds only while the local outbox can actually be
opened. When it cannot (a filesystem fault — read-only disk, missing
directory permissions, etc.), `sitrep report` degrades to the legacy
best-effort HTTP path instead: a bounded number of retries with no durable
fallback, which may still drop the event on final failure, exactly like the
pre-durable-outbox behavior. This degradation is never silent — the outbox
open failure that triggered it is reported via the `outbox_open` health.d
component (row above) before the fallback path is taken, so the
menubar/health surface reflects the reduced durability even on a call whose
HTTP send itself appears to succeed.

The set of component file names is **extensible** — any name is a valid
`health.d/<name>.json` file, and a future component (e.g. an `auth.json`
for a not-yet-implemented auth health check) needs no schema change, only
a new file name, which is exactly the point of moving to a directory.

**Migration**: `~/.config/sitrep/health.json` (the single file) is
**deleted**, replaced entirely by `health.d/`. No dual-write, no
back-compat reader for the old path — this is an unreleased product (§11's
precedent already established that no migration story is owed here).
