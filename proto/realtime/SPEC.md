# Sitrep realtime protocol v1

Status: frozen for implementation. Version `1.0.0` — see `CHANGELOG.md`.

## 0. Scope and relationship to other protocols

This document specifies the **realtime synchronization protocol** between a
Sitrep source device (an Agent that executes and reports on tasks), a Sitrep
viewer device (a phone, tablet, or menu bar app watching a space), and a
server that relays and persists state for that space.

This is a *different layer* from `proto/SPEC.md`, which describes the
line-oriented protocol a task process writes to stdout for consumption by the
local Agent. That layer is unaffected by this document. By the time a task
transition or metric sample reaches this protocol, the Agent has already
parsed it into a discrete domain event; this protocol is only concerned with
getting that event (or a metric sample, or a control instruction) from one
device to another, reliably or best-effort as appropriate, and keeping a
space's state convergent across every connected device.

This protocol is **transport-independent**. It defines a message format and a
set of behavioral rules; it does not assume WebSockets, a particular cloud
provider, or any specific network topology. An implementation MAY carry this
protocol over a persistent bidirectional byte stream that supports discrete
text frames — a WebSocket, a raw TCP connection with framing, a WebRTC
DataChannel, or an equivalent. The rest of this document says "connection"
to mean whatever such channel the transport provides, and "frame" to mean one
discrete unit of data sent over it.

## 1. Conformance language

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**,
**SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **MAY**, and **OPTIONAL** in this
document are to be interpreted as described in RFC 2119. Every implementation
(TS server, Go Agent, Swift app, and any future one) MUST satisfy every MUST
in this document; deviating from a SHOULD requires understanding and
accepting the consequences described here.

## 2. Directory contents

```
proto/realtime/
├── SPEC.md                    this document
├── common.schema.json         shared $defs (timestamps, ids, task/metric/message shapes)
├── envelope.schema.json       the generic envelope shape, common to every message type
├── messages/                  one *.schema.json per message type, named after its `type`
├── fixtures/
│   ├── valid/                 one or more valid fixtures per message type
│   ├── invalid/               one fixture per constraint this spec calls out as rejectable
│   └── scenarios/             numbered multi-message flows with a README each
├── tools/                     self-contained ajv-based fixture validator (see §16)
└── CHANGELOG.md
```

`common.schema.json` and `envelope.schema.json` are not sent on the wire by
themselves; every frame validates against exactly one file under `messages/`,
which itself composes `envelope.schema.json` via `$ref`.

## 3. Envelope

Every frame except the literal heartbeat text (§9.3) is a single UTF-8 JSON
object — an **envelope** — with exactly these top-level fields:

| field | type | required | meaning |
|---|---|---|---|
| `type` | string | yes | one of the 14 message type names in §4 |
| `id` | string | yes | unique identifier for this transmission, assigned by the sender (RECOMMENDED: UUIDv4 or ULID) |
| `ts` | integer | yes | Unix epoch **milliseconds** at which the sender constructed/sent this envelope |
| `body` | object | yes | type-specific payload, see `messages/<type>.schema.json` |

No other top-level field is permitted, and this strictness is enforced at
runtime: a receiver MUST reject an envelope carrying any unknown top-level
field with `error{code: malformed}`. This rule is permanent — it MUST NOT
be relaxed by any future minor version (the runtime tolerance for unknown
fields in §15 applies **only inside `body`**). The strict top level is what
guarantees that **an envelope never carries an authentication credential**
(§10) — there is no `token`, `secret`, or similar field, at this level or
inside `body`, in any message type, and any attempt to smuggle one in at
the top level is mechanically rejected (see
`fixtures/invalid/envelope-carries-credential-field.json`).

`id` identifies one transmission, not one logical event. When a reliable
event (§5) is retransmitted after a disconnect, it is sent in a **new**
envelope with a **new** `id`; the identity that matters for deduplication is
`(device_id, device_seq)` inside `body`, never the envelope `id`. `id` exists
for logging, and for correlating a control-plane request with its `ack`/
`error` response via `body.in_reply_to`.

`ts` is the send-time of the envelope itself. It is **not** the same as any
domain timestamp carried inside `body` (e.g. `task.event.body.occurred_at`):
a resent `task.event` keeps its original `occurred_at` but gets a fresh `ts`
each time it is put on the wire.

### 3.1 Timestamps are always milliseconds

**Every** timestamp field anywhere in this protocol — `envelope.ts`, every
`occurred_at`, every metric `ts`, every lease `expires_at` — is an integer
count of milliseconds since the Unix epoch. There is no field anywhere in
this protocol expressed in seconds, and none MAY be added in a
backward-compatible way (introducing a seconds-based timestamp field would be
a breaking change requiring a major version bump, precisely because this
codebase has previously suffered a seconds/milliseconds confusion bug).

To make this mechanically checkable, every timestamp field's schema
(`common.schema.json#/$defs/unix_ms_timestamp`) sets `minimum:
1000000000000` (13 digits — 2001-09-09T01:46:40.000Z in milliseconds). A
plausible Unix seconds value is at most 10 digits (until the year 2286), so
any value that is actually in seconds fails this bound and is rejected by
schema validation rather than silently misinterpreted. See
`fixtures/invalid/message-event-timestamp-in-seconds.json`.

## 4. Message types (complete set)

| `type` | direction | reliable? (§5) | schema |
|---|---|---|---|
| `hello` | offer: client → server; accept: server → client | n/a (connection setup) | `messages/hello.schema.json` |
| `resume` | viewer → server | n/a | `messages/resume.schema.json` |
| `snapshot` | server → viewer (only as reply to `resume`) | n/a | `messages/snapshot.schema.json` |
| `delta` | server → viewer | n/a (carries reliable events, itself not acked) | `messages/delta.schema.json` |
| `ack` | server → source (event acks); server → viewer (control-plane acks); source → server (optional command-delivery acks) | n/a (is itself the ack) | `messages/ack.schema.json` |
| `task.event` | source → server (uplink only; reaches viewers wrapped in `delta`) | yes | `messages/task.event.schema.json` |
| `message.event` | source → server (uplink only; reaches viewers wrapped in `delta`) | yes | `messages/message.event.schema.json` |
| `metric.frame` | source → server → viewers | no (best-effort) | `messages/metric.frame.schema.json` |
| `config.event` | server → viewer only (server-minted; in v1 always carried inside `delta`, never standalone) | yes (server-minted, §5.5) | `messages/config.event.schema.json` |
| `subscribe` | viewer → server | n/a | `messages/subscribe.schema.json` |
| `unsubscribe` | viewer → server | n/a | `messages/unsubscribe.schema.json` |
| `interest.renew` | viewer → server | n/a | `messages/interest.renew.schema.json` |
| `command` | viewer → server → source, or server → source | delivery best-effort, effect idempotent (§8) | `messages/command.schema.json` |
| `error` | any → any | n/a | `messages/error.schema.json` |

This is the complete set of 14 message types for protocol version 1. No
implementation may invent an additional type without a version negotiation
(§9) that both peers agree to; see §15 for how an unrecognized type is
handled in the meantime.

**Viewer-facing reliable events travel only inside `delta`.** A viewer
never receives a bare `task.event`, `message.event`, or `config.event`
envelope: each time the server applies one reliable event it emits a
single-event `delta` (§6.2) to every viewer holding an active interest
lease. This is what gives every viewer-visible reliable event an attached
revision, so a viewer can always tell exactly where it stands.

## 5. Reliable events

The reliable events are `task.event` and `message.event` (device-uplinked,
§5.1–5.4) and `config.event` (server-minted, §5.5). Device-uplinked
reliable events work at-least-once: a source device MUST keep retrying
delivery until it is acknowledged, and the server MUST deduplicate so that
retries never apply twice.

### 5.1 `device_seq`

`device_seq` is scoped to one **(device, space)** pair: every source device
maintains **one** monotonically increasing counter per space it reports
into, starting at 1, **shared across both** `task.event` and
`message.event` (they are not counted separately; `config.event` does not
participate at all, §5.5). Within one (device, space) scope, a device:

- MUST include `device_seq` in every `task.event` and `message.event` it
  sends.
- MUST NOT reuse a `device_seq` value once used for a different event.
- MUST NOT send `device_seq` values out of increasing order.
- MAY skip values (e.g. 5 then 7) if it locally determines an event is
  unrecoverably lost (e.g. it crashed mid-event and cannot reconstruct it);
  the server MAY surface this to the source as an advisory
  `error{code: sequence_gap}` (§13), but MUST still accept and apply the
  event that arrived.

### 5.2 Deduplication

The server deduplicates reliable events on the pair `(device_id,
device_seq)`. Receiving the same pair twice (see
`fixtures/scenarios/duplicate-device-seq/`):

- MUST NOT apply the event's effect a second time.
- MUST NOT increment `space_revision` (§6) a second time.
- MUST still respond with an `ack` covering that `device_seq`, so the
  sender can retire it from its resend queue.

This is why `id` (envelope-level) is deliberately independent of
`device_seq`: repeated transmissions of the same logical event are expected
and harmless, precisely because the identity the server keys on lives in
`body`, not in the transport-level envelope wrapper.

### 5.3 Acknowledgement and resend

- The server MUST send `ack{body.acked: [{device_id, device_seq}, ...]}`
  once it has durably applied one or more reliable events. `acked` MAY
  batch multiple `device_seq` values in a single `ack` envelope, but every
  pair in one ack MUST carry the `device_id` of the connection's
  authenticated device — an ack is always addressed to exactly one device
  (and routed to that device's most recent connection, §9.4); a receiver
  that finds a pair for any other device MUST treat the envelope as
  `malformed`.
- `acked` is an **exact enumeration**, not cumulative coverage:
  acknowledging `device_seq` 7 confirms event 7 only and says nothing about
  events 1–6. A sender retires from its resend queue exactly the
  `device_seq` values literally listed, no more.
- A source device MUST keep every sent `task.event`/`message.event` in a
  local resend queue until it receives an `ack` that covers its
  `device_seq`.
- A source device SHOULD use an increasing backoff between resend attempts
  of the same unacked event while a connection is up, and MUST resend every
  still-unacked event (oldest first) immediately after `hello`/`hello accept`
  completes on a new connection (§5.4, "Agent reconnect replay flow").
  It does not send a `resume` for its own queue — `resume` is a viewer-only
  message (§6.3).

### 5.4 Agent reconnect replay flow (step by step)

See `fixtures/scenarios/agent-reconnect-replay/` for the executable version
of this flow.

1. Source device is connected, sends `task.event` for `device_seq` 10 and
   11.
2. Server acks `device_seq` 10; the ack for 11 never reaches the source
   (connection drops, or the ack itself is lost in flight — the source
   cannot distinguish these, and does not need to).
3. Connection drops. The source's local resend queue now holds one entry:
   `device_seq` 11.
4. Source reconnects: sends `hello{stage: offer}`, receives
   `hello{stage: accept}`.
5. Source immediately resends `device_seq` 11 in a fresh envelope (new
   `id`, new `ts`, identical `body.occurred_at` and domain content).
6. Server applies it if it had not already done so, or recognizes the
   duplicate if it had (§5.2) — either way it now sends `ack` for
   `device_seq` 11.
7. Source's resend queue is now empty; steady-state resumes.

The source does **not** need to know whether the server actually received
event 11 before the drop. Idempotent apply-by-`(device_id, device_seq)`
makes resending always safe.

### 5.5 Server-minted reliable events: `config.event`

Configuration changes (automation created/edited/removed) are initiated
through the HTTP control plane, where the server is the sole writer. The
server records each such change as a **`config.event`** — a reliable event
with special provenance rules:

- Its body carries **no `device_id` and no `device_seq`**: since the server
  mints it, there is no uplink to deduplicate. Its identity IS the
  `space_revision` slot it occupies.
- Minting a `config.event` and incrementing `space_revision` MUST be a
  **single durable transaction**: the event exists if and only if its
  revision slot does. This gives `config.event` a duplicate-resistance
  guarantee equivalent to the `(device_id, device_seq)` deduplication of
  uplinked events — in particular, retries of the HTTP control-plane
  operation that triggered the change MUST NOT produce duplicate
  `config.event`s (the control plane deduplicates its own retries before
  minting).
- It is never acked by anyone and never sits in a resend queue.
- Like every reliable event, it is persisted and increments
  `space_revision` by exactly 1 (§6.1), and reaches viewers inside `delta`
  (§6.2) — so a delta-following viewer and a snapshot-taking viewer
  converge on the same automation state (`snapshot.body.automations`
  carries the folded result).
- No client ever sends `config.event`; a client-sent one MUST be rejected
  with `error{code: unauthorized}` (§10.1). In protocol version 1 the
  server also never sends it as a standalone envelope — it appears only as
  an entry in `delta.body.events` — but the type is registered and
  schema'd (`messages/config.event.schema.json`) so the body shape is
  pinned and the name is reserved.

## 6. `space_revision` and state synchronization

### 6.1 Definition

Every space has one monotonically increasing integer counter,
`space_revision`, starting at 0. The server increments it by **exactly 1**
each time it durably applies one reliable event: a `task.event` or
`message.event` from a source device (§5.1–5.4), or a server-minted
`config.event` (§5.5).

`metric.frame` and every control-plane message type (`hello`, `resume`,
`snapshot`, `delta`, `ack`, `subscribe`, `unsubscribe`, `interest.renew`,
`command`, `error`) never change `space_revision`.

### 6.2 `delta` (catch-up and live) and `snapshot`

`delta` is the **only** carrier of reliable events toward a viewer. It is
sent in two situations:

- **Live**: each time the server applies one reliable event, taking the
  space from revision `R-1` to `R`, it MUST send a single-event
  `delta{from_revision: R-1, to_revision: R, events: [e]}` to every viewer
  **connection that is delta-eligible**: it holds an active interest lease
  (§7) AND has completed the full `hello → subscribe → resume` sequence,
  including having been sent a **`snapshot` or `delta` reply** to its most
  recent `resume` (§6.3). An `error` reply (e.g. `revision_unavailable`)
  does NOT make the connection delta-eligible; eligibility begins only
  with a successful snapshot-or-delta reply. The server MUST NOT send any
  live delta on a connection before that reply.
- **Catch-up**: as the reply to `resume` (§6.3), covering the range
  `(from_revision, to_revision]`.

Rules, in both situations:

- `to_revision - from_revision` MUST equal `events.length`, since each
  event advances the revision by exactly 1. `events` MAY be empty only in
  the explicit you-are-current reply (`from_revision == to_revision`,
  §6.3).
- A server MUST send a catch-up `delta` only when it has retained every
  reliable event in the range. If it cannot — because the requested
  `last_revision` is older than its retention window, or because it has no
  history at all for the space — it MUST send `snapshot` instead. This is
  not an error condition.
- When a catch-up range would exceed the frame size limit (§11), the
  server MUST split it **on event boundaries** into several consecutive
  `delta`s whose ranges chain exactly: `d1.to_revision ==
  d2.from_revision`, and so on. Each chained delta is individually valid,
  and the viewer applies them one by one under the §6.3 rules — no special
  chunk-reassembly logic is needed for deltas.

`snapshot` carries the full folded state (`tasks`, `metrics`, `messages`,
`automations`) of a space at one revision, and is sent **only** as a reply
to `resume` — nothing else triggers it. Because a full snapshot routinely
exceeds the frame limit, it is chunkable:

- Every chunk carries the SAME `revision`; `part` numbers the chunks
  consecutively from 1; `final: true` marks the last chunk. A single-chunk
  snapshot is `part: 1, final: true`.
- Chunks of one snapshot MUST be sent consecutively, in order, on the same
  connection. **While a chunked snapshot is in flight, the server MUST
  defer every other outbound envelope on that connection — including
  control-plane `ack`s — until after the `final` chunk**; only the
  `ping`/`pong` heartbeat text frames (§9.3), which are not envelopes, may
  interleave. A viewer that receives any non-`ping`/`pong` envelope
  between chunks MUST treat it as a malformed sequence and MAY close the
  connection and reconnect.
- The receiver concatenates the four arrays across chunks and MUST NOT
  apply anything until the `final` chunk has arrived. If the connection
  drops mid-snapshot, the partial chunks are discarded and the viewer
  resumes afresh on its next connection.
- A snapshot MAY be empty (all four arrays empty) — e.g. the reply to
  `resume{last_revision: 0}` on a brand-new space.
- `snapshot.body.metrics` is a **best-effort convenience cache**: metric
  samples are not persisted and are outside `space_revision` accounting
  (§12), so this array MAY be empty or stale — for instance after a server
  restart — even while metrics are actively flowing. A viewer MUST treat
  it as a hint and rely on subsequent `metric.frame` envelopes for truth.

### 6.3 Client resume flow (step by step)

The mandatory viewer connection sequence is **`hello` → `subscribe` →
`resume`**, and `resume` is **required on every connection**, not
optional: a connection — including a new connection that supersedes an
old one (§9.4) — becomes **delta-eligible** only once it has completed
all three steps in order and the server has sent a snapshot-or-delta
reply to its `resume` (§6.2; an `error` reply does not confer
eligibility). A
viewer MUST NOT send `resume` before it has sent `subscribe` on the same
connection; a server receiving them out of order MUST reject the early
`resume` with `error{code: malformed, retryable: true, fatal: false}`.
Within one connection the transport delivers messages in order (§11),
which this flow relies on.

The gate has a server side and a defensive viewer side:

- **Server gate**: the server MUST NOT send any live delta on a
  connection until it has sent that connection's resume reply. The resume
  reply is therefore always the **first** delta-family envelope (`delta`
  or `snapshot`) the server sends on a connection.
- **Viewer defense**: a viewer MUST discard any `delta` received on a
  connection before that connection's resume reply. This is provably
  safe, not merely heuristic: frames on one connection are delivered in
  order (§11), so a delta that arrives before the reply was necessarily
  *sent* before the server computed the reply — the reply's
  revision therefore already covers that delta's content. (This rule is
  pure defense-in-depth against a non-conformant server; a conformant
  server never triggers it.)

Together these remove any window in which a viewer would have to evaluate
a delta against an uninitialized local revision: `C` (below) is defined
by the resume reply, and no delta is ever evaluated before it exists.

`resume` MUST produce **exactly one reply** — never silence. The complete
decision table, for `resume{last_revision: N}` against the space's current
revision `R`:

| condition | server reply |
|---|---|
| `N == 0` | `snapshot{revision: R, ...}` (chunked if needed; possibly empty) |
| `0 < N == R` | empty `delta{from_revision: N, to_revision: N, events: []}` — the explicit "you are already current" signal |
| `0 < N < R`, full range retained | `delta{from_revision: N, to_revision: R, events: [...]}` (split into chained deltas if needed, §6.2) |
| `0 < N < R`, range not fully retained | `snapshot{revision: R, ...}` (chunked if needed) |
| `N > R` | `error{code: revision_unavailable, retryable: true, fatal: false}`; the viewer MUST retry with `last_revision: 0` |

The chunked-snapshot and chained-delta replies each count as the one
reply.

After the reply, the viewer maintains a local current revision `C`
(initialized to the reply's `revision`/`to_revision`) and applies every
subsequently received `delta` by these rules:

- `from_revision < C`: **discard silently.** The delta's content is
  already covered by the resume reply or an earlier applied delta (the
  same in-order argument as the pre-reply discard rule above: anything
  the server sent with an older `from_revision` reflects state the viewer
  has already absorbed).
- `from_revision == C`: **apply**, then set `C = to_revision`.
- `from_revision > C`: **revision gap** — one or more deltas were lost
  (which a transport with in-order lossless frames should not produce, but
  e.g. a server-side drop under pressure might). The viewer MUST send a
  fresh `resume{last_revision: C}` and process the new reply under the
  same table above.

Worked example (executable versions:
`fixtures/scenarios/client-resume-delta/` and
`fixtures/scenarios/client-revision-gap-snapshot/`):

1. Viewer connects: `hello{stage: offer, role: viewer}` /
   `hello{stage: accept}`.
2. Viewer sends `subscribe`; server replies `ack{lease: {expires_at}}`.
   The lease is now active, but this connection is **not yet
   delta-eligible**: the server sends no live delta on it yet, even if
   reliable events are being applied to the space right now.
3. Viewer sends `resume{last_revision: 126}`.
4. Server has revisions 127–128 retained → replies
   `delta{from_revision: 126, to_revision: 128, events: [2 events]}` —
   the first delta-family envelope on this connection. Viewer applies it,
   `C = 128`; the connection is now delta-eligible. (Had the retention
   window not reached back to 126, the reply would instead have been
   `snapshot{revision: 128, part: 1, final: true, ...}` and the viewer
   would discard its stale local state; had the viewer sent
   `last_revision: 128`, the reply would have been the empty delta. Any
   reliable events applied between steps 2 and 4 are simply included in
   the reply itself — the reply is computed against the space's current
   revision at reply time.)
5. A source reports progress; the server applies it (revision 129) and
   sends `delta{from_revision: 128, to_revision: 129, events: [1 event]}`
   to every delta-eligible lease-holding connection. `from_revision == C`,
   so the viewer applies it, `C = 129`.
6. Any duplicate or late delta with `from_revision < 129` is discarded;
   any future delta with `from_revision > C` triggers a fresh
   `resume{last_revision: C}` (and while that re-resume is outstanding,
   the same pre-reply discard rule applies to any delta that races the
   new reply).

### 6.4 Deterministic event folding

Two viewers — one that replayed deltas, one that took a snapshot — MUST
converge on identical task and automation state. To guarantee that, the
folding of reliable events into state is fixed, field by field. Servers
MUST produce `snapshot` contents by these exact rules, and viewers MUST
apply delta-carried events by these same rules.

**`task.event` sequence → `task_state`** (events applied in revision
order):

| field | folding rule |
|---|---|
| `task_id`, `device_id` | from the event; constant across a run |
| `state` | `started`/`progress`/`step` → `running`; `done` → `done`; `failed` → `failed` |
| `title` | latest **non-empty** `title` seen; an event without `title` (or with an empty one) leaves it unchanged |
| `percent` | latest `percent` seen while running; on `done` → set to `100`; on `failed` → keep last value |
| `step` | latest `step` seen while running; on `done` or `failed` → **cleared** (absent) |
| `message` | from the `done`/`failed` event only; absent while running |
| `updated_at` | the `occurred_at` of the latest applied event — **never** the server's receive time |
| `display` | latest event that carried a `display` object wins wholesale (no per-key merge) |

**`config.event` sequence → automation set**: `automation.upserted`
replaces the automation's entire state with the event's `automation`
object (create-or-replace, no per-field merge); `automation.removed`
deletes it. `snapshot.body.automations` is exactly the surviving set.

**`message.event` sequence → message history**: append-only, ordered by
revision. The server keeps a bounded window: `snapshot.body.messages`
carries only the most recent `N` messages (implementations SHOULD document
their `N`; 200 is RECOMMENDED). **Truncation to this window is normative
behavior**, and the resulting asymmetry — a delta-replaying viewer may
hold older messages than a snapshot-taking viewer ever sees — is an
**accepted, expected deviation**: message history is a log, not folded
state, and the convergence guarantee above covers task and automation
state, not the depth of message history.

## 7. Interest lease

A source device MAY reduce the frequency and richness of what it publishes
when it knows nobody is watching. The interest lease is how a viewer tells
the server (and transitively, the server tells the source) that someone is
watching.

Leases are held **per device**, not per connection, and their lifetime is
**decoupled from connection lifetime**: a lease ends in exactly two ways —
its `expires_at` passes without renewal, or the device sends an explicit
`unsubscribe`. A connection drop does NOT end the device's lease; a device
that reconnects within its lease window still counts as interested the
whole time. Because leases are persisted (§14), this also gives a
restarted server the basis to rebuild the space's interest count instead
of spuriously firing a throttle notification.

- `subscribe` establishes (or wholly replaces) the sending device's lease.
  The server MUST choose a lease duration between 30000 ms and 60000 ms
  inclusive and communicate the absolute deadline as
  `ack.body.lease.expires_at` (an absolute timestamp, not a duration, so
  the viewer never needs to know or assume the exact constant the server
  chose). 45000 ms is RECOMMENDED as a default when an implementation has
  no other reason to pick a value.
- `interest.renew`, sent by the viewer before `expires_at`, extends the
  lease; the server responds the same way, with a freshly computed
  `expires_at`. `interest.renew` ALWAYS replaces the lease's `topics`
  wholesale with the request's `topics` (omitted/empty meaning all) — it
  never merges with or preserves the previous set. If the device holds no
  active lease when `interest.renew` arrives (already lapsed, or never
  established), the server MUST treat it identically to a fresh
  `subscribe` using the request's `topics` — this avoids a race between
  the viewer's local renewal timer and server-side expiry, and means
  `interest.renew` never needs to fail with "no such lease".
- `unsubscribe` ends the lease immediately, without waiting for
  `expires_at`, and is confirmed with `ack{in_reply_to}` like `subscribe`
  and `interest.renew` (with no `lease` object, since none remains).
- A lease that is not renewed lapses silently at `expires_at`; no envelope
  marks the lapse by itself. Expiry detection MAY be **lazy**: the server
  is not required to run a timer to the exact `expires_at` instant — it
  MAY evaluate lease expiry the next time it processes any state change or
  interest operation for the space, as long as an expired lease is never
  counted as active in that evaluation.
- `topics` scoping: in protocol version 1 only the `metric` topic has
  filtering effect (a lease without it receives no `metric.frame`
  forwarding). `task` and `message` are advisory: the server MUST always
  deliver every reliable single-event `delta` to every active lease
  holder regardless of topics, because withholding any reliable event
  would break that viewer's revision continuity (§6.3) and force it into
  a resume loop.

A lease's survival across disconnects and connection supersession (§9.4)
affects **only** this section's throttle-edge accounting: in particular,
a supersession MUST NOT cause a `throttle`/`resume_rate` emission, since
the device's lease — and therefore the space's count — is unchanged by
it. Lease survival does NOT make any connection eligible to receive
deltas: delta eligibility is strictly per connection and requires that
connection's own completed `hello → subscribe → resume` sequence (§6.3).

The server tracks **one lease-count per space**: the number of devices
currently holding an unexpired lease. A device with multiple simultaneous
connections (§9.4) contributes exactly once. On every transition of that
count:

- **1 → 0** (the last active lease in the space just ended, by expiry or
  `unsubscribe`): the server MUST send
  `command{origin: "server", action: "throttle"}` (§8) to every source
  device currently connected to the space.
- **0 → 1** (a `subscribe` or `interest.renew` just created the space's
  first active lease): the server MUST send
  `command{origin: "server", action: "resume_rate"}` to every source device
  currently connected to the space.

Both notifications omit `target_device_id` (broadcast to every source in
the space) unless an implementation has a reason to scope more narrowly.
`throttle` is advisory: a source SHOULD reduce its `metric.frame` cadence
while throttled (an implementation-defined lower rate; this document does
not fix one), but MUST continue sending `task.event` for lifecycle
transitions (`started`/`done`/`failed`) and every `message.event` at normal
priority regardless of throttle state — those are cheap, reliable, and
often user-facing (push notifications), so there is no cost benefit to
throttling them.

See `fixtures/scenarios/interest-lease-expiry/` for the executable version
of this flow, including the two-viewer resume_rate case.

## 8. `command`: reverse control

`command` carries an instruction toward one or more source devices. It has
two distinct origins that share one envelope type:

- `origin: "viewer"` — a human-initiated action, relayed by the server:
  `action` is one of `pause`, `resume`, `stop`, `run_now`. The sender MUST
  set `issued_by_device_id` to its own authenticated `device_id`; the
  server MUST reject a mismatch with `error{code: unauthorized}` (§10.1) and
  MUST NOT relay the command to any source in that case.
- `origin: "server"` — a lease-driven notification the server generates
  itself (§7): `action` is one of `throttle`, `resume_rate`. **The server
  MUST reject ANY client-sent command with `origin: "server"` with
  `error{code: unauthorized}`, regardless of action** (see
  `fixtures/invalid/role-viewer-command-origin-server.json`); additionally
  the schema rejects a viewer-origin envelope that carries `throttle`/
  `resume_rate` (see
  `fixtures/invalid/command-viewer-sends-throttle.json`).

Per-action required-field matrix — the command names the exact object it
operates on, aligning with the HTTP control surface
(`POST /v1/tasks/:id/commands`):

| `action` | `origin` | required object field | forbidden object field |
|---|---|---|---|
| `pause`, `resume`, `stop` | `viewer` | `task_id` (the task run to act on) | `automation_id` |
| `run_now` | `viewer` | `automation_id` (the automation to trigger) | `task_id` |
| `throttle`, `resume_rate` | `server` | none (device-level, not object-level) | both `task_id` and `automation_id` |

The forbidden column is enforced by schema (`not`/`required` patterns in
`messages/command.schema.json`): a command carrying an object field its
action does not use is `malformed`, never silently tolerated.

Other fields:

- `command_id` is an idempotency key chosen by the sender (the viewer for
  `origin: viewer`, the server for `origin: server`). A source device MUST
  track `command_id`s it has already acted on and MUST NOT execute the
  same `command_id` twice.
- `target_device_id`, when present, scopes the command to one source
  device; when absent, it applies to every source device connected to the
  space (used for `throttle`/`resume_rate`).
- `ttl_ms` bounds how long, from `envelope.ts`, the command remains
  actionable. A device that would first act on a command after `ts +
  ttl_ms` has passed MUST drop it without executing it, and SHOULD emit
  `error{code: command_expired, retryable: false}`.
- `params` is a reserved extension point; **protocol version 1 defines no
  keys in it**. Unlike every other object in this protocol, `params`
  explicitly permits additional properties, which a receiver that does not
  recognize them MUST ignore.

Relay rules (viewer → server → source):

- When relaying, the server preserves the **original** envelope `ts` and
  `command_id` unchanged; only the envelope `id` is freshly assigned (it
  identifies the new transmission, §3). TTL therefore measures from the
  viewer's send time, not the relay time.
- The server validates the TTL **at relay time** and MUST NOT relay an
  already-expired command (it SHOULD answer the viewer with
  `error{code: command_expired}` instead).
- The receiving source re-validates the TTL against its **own local
  clock**, allowing ±30 s of clock skew: the command is actionable while
  the local clock reads within `[ts − 30000, ts + ttl_ms + 30000]`. In
  particular a source MUST NOT reject a command merely because its `ts`
  appears to lie slightly in the local future.

The §10.1 authorization matrix governs only client-originated `command`
envelopes (i.e. `origin: "viewer"`); the matrix does not restrict what the
trusted server itself may send — the server is not a "role" in the
source/viewer sense, and messages it originates (like `throttle`) are not
subject to the client authorization checks in §10.1.

A source device receiving a `command` MAY send back
`ack{body.in_reply_to: <the command envelope's id>}` to confirm delivery,
but this is OPTIONAL: correctness does not depend on it, since
`command_id`-based idempotency (above) already makes re-delivery safe, and
the command's effect is independently observable through domain events
(next paragraph).

A `command`'s *effect* is observed by everyone through ordinary domain
events: e.g. a successful `pause` surfaces as a subsequent `task.event`
reporting the affected task's new state. This protocol intentionally has no
separate "command result" message type — reusing the existing reliable
event stream for effects keeps the type list closed and avoids a second
notion of "did it work" alongside `ack`/`error`.

## 9. `hello` and version negotiation

### 9.1 Ordering and handshake timeouts

`hello` MUST be the first envelope sent on every new connection, in **both**
directions: the client's `hello{stage: offer}` (client → server) and the
server's `hello{stage: accept}` (server → client).

The handshake is strictly sequential:

- A client may only ever send `hello{stage: "offer"}`; `stage: "accept"`
  belongs exclusively to the server. If the server receives a
  `hello{stage: "accept"}` from a client — at any point in the
  connection's life — it MUST treat it as a handshake violation: reply
  `error{code: hello_required, fatal: true}` and close the connection.
- After sending its offer, the client **MUST NOT send any other envelope
  until it has received and validated the server's accept.** There is no
  pipelining of `subscribe`/`resume`/events behind the offer.
- The server MUST answer any frame it receives before it has sent its
  accept — including a second `hello` — with `error{code: hello_required,
  fatal: true}` and then close the connection. (`hello_required` is only
  ever sent by the server; a client never polices this.)
- The server MUST NOT send any envelope other than its accept (or an
  `error`) before the handshake completes.

Recommended timeouts: the server SHOULD close a connection that has sent
no offer within **10 s** of transport establishment; the client SHOULD
close and reconnect if no accept has arrived within **10 s** of sending
its offer.

### 9.2 Offer / accept

- The connecting device sends `hello{body.stage: "offer", device_id, role,
  protocol_versions, capabilities?}`. `protocol_versions` lists every major
  protocol version that device can speak, ascending, at least one entry.
- The server computes the intersection of the offer's `protocol_versions`
  with its own supported set and selects the **maximum** version in that
  intersection.
  - If the intersection is non-empty: the server MUST reply
    `hello{body.stage: "accept", protocol_version, session_id,
    heartbeat_interval_ms, capabilities?}` using the selected version, and
    the connection proceeds on that version.
  - If the intersection is empty: the server MUST reply
    `error{code: version_unsupported, retryable: false, fatal: true}` and
    then close the connection. It MUST NOT send `hello{stage: accept}` in
    this case.
- `session_id` is an opaque, per-connection identifier for logging and
  observability. It is not a credential and is not required to resume
  state — resuming is entirely keyed by `space_revision` (§6), not by
  session continuity.
- `role` is the realtime-protocol role (`source` or `viewer`), which is
  coarser than and distinct from the HTTP-layer roles (`owner`, `admin`,
  `viewer`, `source`) used for pairing and REST access: an `owner` or
  `admin` device opening a realtime connection to observe state always
  presents `role: "viewer"` here. Only a device that executes and reports
  tasks/metrics/messages presents `role: "source"`.

### 9.3 Heartbeat

Once `hello` has completed, either side MAY send a bare text frame
containing exactly the four ASCII bytes `ping` — not a JSON envelope, no
`type`/`id`/`ts`/`body` wrapper — at the interval named in
`hello{stage: accept}.heartbeat_interval_ms`. This is deliberately plain
text rather than JSON specifically so that a transport-layer intermediary
that does byte-level keepalive (a reverse proxy, load balancer, or any
other pass-through component) MAY answer it directly without parsing JSON
or waking any protocol/business logic above it.

A recipient of a `ping` text frame MUST respond with a bare text frame
containing exactly the four ASCII bytes `pong`, as promptly as possible,
without forwarding it to application logic. A `pong` received without a
preceding `ping` MUST be ignored. This exchange MUST NOT reset or otherwise
interact with the interest lease (§7) or any resend timer (§5.3) — it is
a pure liveness probe, decoupled from every other piece of protocol state.
Either side MAY close the connection if it sends `ping` and observes no
`pong` within an implementation-defined timeout (2 × `heartbeat_interval_ms`
is RECOMMENDED).

### 9.4 Connection supersession

At most one connection per device is current. When a device completes
`hello` on a new connection while the server still holds an open connection
for the same authenticated `device_id`:

- The server MUST accept the **new** connection.
- The server MUST close the **old** connection, first sending
  `error{code: superseded, retryable: false, fatal: true}` on it.
  `retryable: false` refers to that dying connection; the device's newer
  connection is unaffected and simply continues.
- From that moment, ALL server-to-device traffic — `ack`s for the device's
  reliable events, targeted `command`s, live deltas — MUST route to the
  device's **most recent** connection. A resume reply is the exception: it
  is always sent on the connection that carried the `resume` request, so a
  reply still pending on a superseded connection dies with that connection
  and is never forwarded (the new connection obtains its own reply by
  running the full sequence below).
- Interest leases are unaffected: they are held per device (§7), so
  supersession neither ends a lease nor double-counts it — a device
  briefly holding two open connections contributes exactly one lease to
  the space's count, and the supersession itself MUST NOT trigger a
  `throttle`/`resume_rate` emission (§7).
- Lease survival does NOT carry delta eligibility across connections.
  The new connection MUST run the full `hello → subscribe → resume`
  sequence (§6.3) before it receives any delta — for a viewer, `resume`
  is required on every connection — and the viewer's local revision `C`
  is re-established from the **new** connection's resume reply. No
  cross-connection inheritance of `C` (or of any in-flight snapshot
  chunks or pending replies) is defined by this protocol.

A client that receives `superseded` after itself opening a new connection
treats it as confirmation the new connection won. A client that receives
`superseded` without having opened a new connection SHOULD surface the
situation (its credential may be in use elsewhere) rather than silently
reconnecting in a loop.

See `fixtures/scenarios/duplicate-connection-supersede/`.

## 10. Authentication boundary

The credential (a `sr1_<space>_<secret>` token, or an equivalent a future
transport might use) is presented **once, at transport/connection
establishment** — e.g. as a WebSocket upgrade header or query parameter, or
whatever the concrete transport's native mechanism for out-of-band
credentials is. It is never repeated inside any envelope afterward.

An envelope carries only `device_id` (in `hello` and event bodies) and,
where relevant, `issued_by_device_id`/`target_device_id` (in `command`) —
none of these are secrets; they identify a device, they do not
authenticate one. The server MUST bind `device_id` and `role` to the
identity it resolved from the connection-establishment credential and MUST
NOT trust a `device_id` or `role` value asserted inside a
`hello{stage: offer}` body if it conflicts with that resolved identity.

The same binding applies to every event a source uplinks: the server MUST
reject any `task.event`, `message.event`, or `metric.frame` whose
`body.device_id` does not match the connection's authenticated device
identity, with `error{code: unauthorized}`. A source can only ever report
as itself.

### 10.1 Authorization matrix

The server MUST validate **every** incoming envelope's `type` against the
sending connection's `role`, not only once at connection time. A message
sent by a role not listed below MUST be rejected with
`error{code: unauthorized}`.

| `type` | source may send | viewer may send |
|---|---|---|
| `hello` | yes (`stage: "offer"` only — a client-sent `stage: "accept"` is a handshake violation answered with `hello_required` + close, §9.1) | yes (`stage: "offer"` only, same rule) |
| `resume` | no | yes |
| `snapshot` | no (server-only) | no (server-only) |
| `delta` | no (server-only) | no (server-only) |
| `ack` | yes, OPTIONAL (may acknowledge receipt of a `command` it was sent; not required for correctness since `command_id` already makes execution idempotent, §8) | no (a viewer has no scenario in this protocol requiring it to send `ack`: the server always originates the acks a viewer receives for `subscribe`/`unsubscribe`/`interest.renew`, and a viewer never receives a reliable event it must acknowledge — catch-up after a gap uses `resume`/`delta`/`snapshot`, §6, not per-message acking) |
| `task.event` | yes (body.device_id must match the authenticated identity) | no |
| `message.event` | yes (body.device_id must match the authenticated identity) | no |
| `metric.frame` | yes (body.device_id must match the authenticated identity) | no |
| `config.event` | no (server-only) | no (server-only) |
| `subscribe` | no | yes |
| `unsubscribe` | no | yes |
| `interest.renew` | no | yes |
| `command` | no (only receives it) | yes, with `origin: "viewer"` only — a client-sent `origin: "server"` is always unauthorized regardless of action (§8) — and actions limited to `pause`/`resume`/`stop`/`run_now` |
| `error` | yes | yes |

`snapshot`, `delta`, and `config.event` are server-to-client only; a
client that sends any of them MUST be rejected with
`error{code: unauthorized}`. This matrix governs client-originated
envelopes only — see §8 for why server-originated `command` envelopes are
not restricted by it.

## 11. Transport framing limits

- One frame carries exactly one JSON object (or the literal `ping`/`pong`
  text, §9.3) — this protocol never batches multiple envelopes into one
  frame.
- The transport MUST deliver frames of one connection **in order**. The
  resume/delta interleaving rules (§6.3) and snapshot chunk reassembly
  (§6.2) rely on this.
- A frame MUST NOT exceed **64 KiB** of UTF-8-encoded JSON. A sender that
  would exceed this MUST split the content across multiple envelopes.
  Every payload that can grow has a defined split mechanism:
  - `snapshot` → chunks (`part`/`final`, same `revision`; §6.2),
  - catch-up `delta` → consecutive chained deltas split on event
    boundaries (`d1.to_revision == d2.from_revision`; §6.2),
  - `metric.frame` → multiple frames (each within the item cap below),
  - `ack.body.acked` → multiple `ack` envelopes.
  A receiver that gets an oversized frame MUST reject it with
  `error{code: frame_too_large, retryable: true}` and MUST NOT attempt to
  parse it.
- Every free-text field is capped by the shared `$defs` in
  `common.schema.json` (`free_text` ≤ 2048 characters for task title,
  step, completion message, and message text; `label_text` ≤ 256 for
  metric labels and automation names), and every id/value field has its
  own cap. These caps guarantee that any **single** record — a
  `task_state`, `message_record`, `metric_sample`, `automation_state`, or
  one delta event — serializes far below the 64 KiB frame limit. A
  single record can therefore never exceed a frame by itself, and the
  split mechanisms above (snapshot chunks, chained deltas) always have a
  valid record/event boundary to split on.
- Batch item caps: `metric.frame.body.metrics` MUST NOT exceed 64 entries;
  `delta.body.events` and `ack.body.acked` have no protocol-fixed cap but
  are bounded in practice by the 64 KiB frame limit. A receiver that gets a
  batch exceeding an enforced cap MUST reject it with
  `error{code: batch_too_large, retryable: true}`.
- A connection MUST NOT emit more than 10 `metric.frame` envelopes per
  second, regardless of how many distinct metrics it is reporting (see also
  the per-metric-id cadence rule in §12); excess frames MAY be rejected
  with `error{code: rate_limited, retryable: true}`.

## 12. `metric.frame` merge and ordering rules

- A sender MUST merge multiple pending updates to the same `metric_id`
  into a single sample (the latest one observed) before emitting a frame —
  it MUST NOT emit two samples for the same `metric_id` within one frame.
- A sender MUST NOT emit a frame containing a given `metric_id` more often
  than once per 500 ms (a 2 Hz ceiling per metric); it MAY combine several
  different metrics that changed within that window into one frame.
- A receiver MUST discard any incoming sample whose `ts` is less than or
  equal to the last-applied `ts` it has already accepted for that
  `metric_id`, regardless of the order in which frames arrive at the
  transport level. This makes stale/out-of-order frames harmless without
  requiring in-order delivery from the transport.
- `metric.frame` is never acknowledged and never retransmitted: a dropped
  frame is superseded by the sender's next one, and the protocol accepts
  the resulting gap in the metric's visible history.

## 13. Error codes

Every `error` envelope's `body.retryable` and `body.fatal` fields are
authoritative and self-describing — an implementation never needs this
table to decide how to react to a specific error it receives. The table
below documents the fixed values every code in this version always carries,
for the benefit of implementers writing the code that produces them.

For a code with `fatal: true` (the connection closes), `retryable: true`
means: **reconnect, then retry the logical operation on the new
connection** — never retry on the dying connection itself.

| code | sent by | retryable | closes connection | when |
|---|---|---|---|---|
| `version_unsupported` | server | false | yes | `hello{stage: offer}`'s `protocol_versions` shares no version with the server's supported set (§9.2) |
| `hello_required` | server (only — a client never emits it) | true (after reconnect) | yes | any frame other than the offer arrived before the server sent its accept (§9.1) |
| `unauthenticated` | server | false | yes | the connection's credential became invalid mid-connection (e.g. revoked) |
| `unauthorized` | server | false | no | sender's role does not permit this `type` (§10.1); a client-sent `command` with `origin: "server"` (§8); a `command.issued_by_device_id` mismatch (§8); or an uplink event whose `body.device_id` does not match the authenticated identity (§10) |
| `malformed` | any | true | no | an envelope fails schema validation (including an unknown top-level envelope field, §3), or violates a protocol sequence rule other than the pre-hello case (e.g. `resume` before `subscribe`, §6.3; an `ack` pair for a foreign device, §5.3) |
| `rate_limited` | server | true | no | a per-connection frequency cap was exceeded (§11, §12) |
| `frame_too_large` | server | true | no | a single frame exceeded 64 KiB (§11) |
| `batch_too_large` | server | true | no | an array field exceeded its item cap (§11) |
| `revision_unavailable` | server | true | no | `resume.body.last_revision` is greater than the space's current `space_revision` (§6.3); the viewer retries with `last_revision: 0` |
| `command_expired` | any | false | no | a `command` was received or first actionable after its TTL window (§8) |
| `sequence_gap` | server | false | no | a source's `device_seq` skipped one or more values (§5.1); advisory only, the event that did arrive is still applied |
| `superseded` | server (only) | false (on that connection — the device's newer connection continues) | yes | the same device completed `hello` on a newer connection; this older one is being closed (§9.4) |
| `internal_error` | server | true | no | an unexpected server-side fault unrelated to the request's validity; its `message` MUST NOT contain stack traces, internal file paths, or other implementation details |

Unknown fields anywhere in `body` MUST be ignored by every receiver in
every one of these cases and every other message type (§15) — `malformed`
is reserved for validation failures against fields this version of the
protocol *does* define, never for the mere presence of extra ones.

## 14. Cost table

This table is the basis for server-side cost/capacity review. "Persisted"
means the server durably stores the message or its effect beyond the
lifetime of the connection; "revision++" means it advances `space_revision`;
"outbox" means it may need delivery to a device that is not currently
connected (e.g. a push notification, or queued for a source that is
offline); "broadcast" means the server fans it out to every subscribed
viewer rather than replying only to the requester.

| `type` | persisted | revision++ | outbox | broadcast |
|---|---|---|---|---|
| `hello` | no | no | no | no |
| `resume` | no | no | no | no |
| `snapshot` | no (computed on demand from persisted state; its `metrics` array is a best-effort cache outside revision accounting, §6.2) | no | no | no (unicast reply) |
| `delta` | no (computed from persisted event log) | no | no | yes (live single-event deltas fan out to every active lease holder, §6.2; catch-up deltas are unicast) |
| `ack` | no | no | no | no |
| `task.event` | yes | yes | yes (push/Live Activity update) | no as itself — re-emitted as a broadcast single-event `delta` |
| `message.event` | yes | yes | yes (push notification) | no as itself — re-emitted as a broadcast single-event `delta` |
| `metric.frame` | no | no | no | yes (best-effort, filtered by the lease's `metric` topic) |
| `config.event` | yes | yes | no (never triggers push) | no as itself — re-emitted as a broadcast single-event `delta` |
| `subscribe` | yes (lease record; survives disconnects, §7) | no | no | no |
| `unsubscribe` | yes (lease record removed) | no | no | no |
| `interest.renew` | yes (lease record updated) | no | no | no |
| `command` | yes (until delivered or expired) | no | yes (source may be offline) | no (targeted) |
| `error` | no | no | no | no |

## 15. Compatibility policy

- **Minor/patch changes** (this protocol's version stays `1`): additive
  only. New optional **body** fields, new enum values in a field that is
  documented as open to extension, or new `capabilities` flags. A receiver
  MUST ignore fields **inside `body`** that it does not recognize — it
  MUST NOT reject a message solely because of their presence. This
  tolerance applies to `body` ONLY: the envelope's top level is
  permanently strict (§3) — an unknown top-level field is always
  `malformed`, in this and every future minor version, so no minor version
  may add an envelope-level field. The body-level tolerance is a
  **runtime requirement on implementations**; it is deliberately looser
  than what the JSON Schemas in this directory enforce (most body schemas
  here use `additionalProperties: false`). The schemas describe the
  exact, frozen v1.0.0 conformance surface for this repository's fixtures
  and validator (§16) — precision that is valuable for catching
  implementation bugs against a known-good baseline — while production
  parsers remain lenient toward body fields a future minor/patch version
  might add. These are not in tension: the schemas are a conformance test
  for *this* version, not a description of every byte a lenient parser
  must accept.
- **Breaking changes** (removing a field, changing a field's meaning or
  type, removing a message type, or tightening a previously-open
  constraint) require a **major version bump**, a new entry in
  `CHANGELOG.md`, and — because schema `$id`s are versioned by directory
  (`.../realtime/v1/...`) — a new `v2/` schema namespace published
  alongside `v1/` until every implementation has migrated. `hello`'s
  version negotiation (§9.2) is exactly the mechanism that lets `v1` and
  `v2` implementations coexist during that migration.
- **Unknown message types**: a receiver that gets an envelope whose `type`
  it does not recognize MUST ignore that envelope (log it if useful) and
  MUST NOT treat it as `malformed`. This lets a future protocol version add
  a wholly new message type as an additive change from the perspective of
  a peer that has not yet been upgraded, as long as that peer can safely
  make no progress on it.

## 16. Fixture validation

`tools/validate.js` is a self-contained Node script (its own
`package.json`/`package-lock.json`, pinned `ajv@8.20.0`, no dependency on or
from `server/`, `daemon/`, or `apple/`) that:

1. Loads `common.schema.json`, `envelope.schema.json`, and every
   `messages/*.schema.json` into one Ajv (draft 2020-12) instance.
2. Asserts every fixture under `fixtures/valid/` and
   `fixtures/scenarios/**/` validates against both the generic envelope
   schema and the specific schema selected by its own `type` field.
3. Asserts every fixture under `fixtures/invalid/` is rejected — by one of
   those two schemas, or by the client-authorization matrix (§10.1),
   which the script mirrors (including the client-may-only-send-offer and
   client-may-only-send-origin-viewer rules).
4. Checks semantic invariants JSON Schema cannot express: delta revision
   arithmetic (`to_revision − from_revision == events.length`) on every
   delta fixture, and — per scenario directory, in filename order —
   consecutive-delta chaining, snapshot chunk runs (same revision,
   consecutive parts from 1, nothing interleaved, `final: true` closing
   the run), and per-device `device_seq` monotonicity (equal values
   allowed, as retransmissions).
5. Exits non-zero if any fixture or invariant produces an unexpected
   result.

Fixtures come in two forms. A bare envelope object is schema-checked
only. A wrapper of the form
`{"sender_role": "source" | "viewer", "frame": {…}}` is schema-checked
AND checked against the §10.1 authorization matrix as if a client with
that role had sent the frame — this form exists for constraints that are
role-dependent rather than shape-dependent (e.g.
`fixtures/invalid/role-viewer-command-origin-server.json`, a perfectly
well-formed command that only the server may originate). The wrapper is a
test artifact, not a wire format.

Run it with:

```sh
cd proto/realtime/tools
npm install     # once, materializes node_modules/ from the committed lockfile
npm run validate
```
