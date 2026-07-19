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
| `type` | string | yes | one of the 13 message type names in §4 |
| `id` | string | yes | unique identifier for this transmission, assigned by the sender (RECOMMENDED: UUIDv4 or ULID) |
| `ts` | integer | yes | Unix epoch **milliseconds** at which the sender constructed/sent this envelope |
| `body` | object | yes | type-specific payload, see `messages/<type>.schema.json` |

No other top-level field is permitted. In particular, **an envelope never
carries an authentication credential** (§10) — there is no `token`, `secret`,
or similar field, at this level or inside `body`, in any message type.

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
| `hello` | both, symmetric | n/a (connection setup) | `messages/hello.schema.json` |
| `resume` | viewer → server | n/a | `messages/resume.schema.json` |
| `snapshot` | server → requester | n/a | `messages/snapshot.schema.json` |
| `delta` | server → requester | n/a (carries reliable events, itself not acked) | `messages/delta.schema.json` |
| `ack` | any → any | n/a (is itself the ack) | `messages/ack.schema.json` |
| `task.event` | source → server → viewers | yes | `messages/task.event.schema.json` |
| `message.event` | source → server → viewers | yes | `messages/message.event.schema.json` |
| `metric.frame` | source → server → viewers | no (best-effort) | `messages/metric.frame.schema.json` |
| `subscribe` | viewer → server | n/a | `messages/subscribe.schema.json` |
| `unsubscribe` | viewer → server | n/a | `messages/unsubscribe.schema.json` |
| `interest.renew` | viewer → server | n/a | `messages/interest.renew.schema.json` |
| `command` | viewer → server → source, or server → source | delivery best-effort, effect idempotent (§8) | `messages/command.schema.json` |
| `error` | any → any | n/a | `messages/error.schema.json` |

This is the complete set of message types for protocol version 1. No
implementation may invent an additional type without a version negotiation
(§9) that both peers agree to; see §15 for how an unrecognized type is
handled in the meantime.

## 5. Reliable events: `task.event` and `message.event`

`task.event` and `message.event` are **reliable**: a source device MUST keep
retrying delivery until it is acknowledged, and the server MUST deduplicate
so that retries never apply twice.

### 5.1 `device_seq`

Every source device maintains **one** monotonically increasing counter,
`device_seq`, starting at 1, **shared across both** `task.event` and
`message.event` (they are not counted separately). A device:

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
  batch multiple `(device_id, device_seq)` pairs, from one or more devices,
  in a single `ack` envelope.
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

## 6. `space_revision` and state synchronization

### 6.1 Definition

Every space has one monotonically increasing integer counter,
`space_revision`, starting at 0. The server increments it by **exactly 1**
each time it durably applies one reliable event (`task.event`,
`message.event`, or a config-plane change carried as one of those — e.g. an
automation's schedule edit is modeled as a `message.event`-shaped or
`task.event`-shaped reliable event in the domain layer above this protocol;
this protocol only requires that *whatever* is deemed a reliable event
follows the §5 rules and this §6 accounting).

`metric.frame` and every control-plane message type (`hello`, `resume`,
`snapshot`, `delta`, `ack`, `subscribe`, `unsubscribe`, `interest.renew`,
`command`, `error`) never change `space_revision`.

### 6.2 `snapshot` and `delta`

- `snapshot.body.revision` states the revision the enclosed full state
  reflects.
- `delta.body.from_revision` and `delta.body.to_revision` bound a
  contiguous, gapless range of reliable events; `to_revision -
  from_revision` MUST equal `events.length`, since each event advances the
  revision by exactly 1.
- A server MUST send `delta` only when it has retained every reliable event
  in `(from_revision, to_revision]`. If it cannot — because the requested
  `last_revision` (§6.3) is older than its retention window, or because it
  has no history at all for the space — it MUST send `snapshot` instead.
  This is not an error condition.

### 6.3 Client resume flow (step by step)

A viewer device sends `resume{body.last_revision}` after `hello` completes,
on a fresh connection or after a reconnect, to catch up.

See `fixtures/scenarios/client-resume-delta/` (the delta case) and
`fixtures/scenarios/client-revision-gap-snapshot/` (the snapshot fallback
case) for the executable versions of this flow.

1. Viewer connects: `hello{stage: offer, role: viewer}` /
   `hello{stage: accept}`.
2. Viewer sends `resume{last_revision: N}`, where `N` is the last revision
   it fully applied (0 if it holds no prior state at all).
3. Server compares `N` to its current `space_revision` for the space,
   call it `R`:
   - If `N == R`: the server MAY send nothing (the viewer is already
     current) or send an empty `delta{from_revision: N, to_revision: N,
     events: []}`; either is conformant, and a viewer MUST handle both.
   - If `N < R` and the server has retained every reliable event in
     `(N, R]`: it MUST send exactly one `delta{from_revision: N,
     to_revision: R, events: [...]}`.
   - If `N < R` but the server's retention does not cover the full range
     (the "gap too large" case): it MUST send exactly one
     `snapshot{revision: R, ...}` instead, and the viewer discards
     whatever partial local state it had.
   - If `N > R` (the viewer claims a revision the space never reached —
     e.g. the space was reset): the server MUST send
     `error{code: revision_unavailable, retryable: true, fatal: false}`
     and MUST NOT send `snapshot` or `delta` in the same response. A
     viewer that receives this MUST retry `resume` with `last_revision: 0`,
     which unconditionally yields a `snapshot`.
4. Once the viewer applies the `delta` or `snapshot`, its local
   `last_revision` becomes `R`, and it proceeds to `subscribe` (§7) to keep
   receiving live updates.

## 7. Interest lease

A source device MAY reduce the frequency and richness of what it publishes
when it knows nobody is watching. The interest lease is how a viewer tells
the server (and transitively, the server tells the source) that someone is
watching.

- `subscribe` establishes a lease. The server MUST choose a lease duration
  between 30000 ms and 60000 ms inclusive and communicate the absolute
  deadline as `ack.body.lease.expires_at` (an absolute timestamp, not a
  duration, so the viewer never needs to know or assume the exact constant
  the server chose). 45000 ms is RECOMMENDED as a default when an
  implementation has no other reason to pick a value.
- `interest.renew`, sent by the viewer before `expires_at`, extends the
  lease; the server responds the same way, with a freshly computed
  `expires_at`. If the connection holds no active lease when
  `interest.renew` arrives (already lapsed, or never established), the
  server MUST treat this identically to a fresh `subscribe` using the
  request's `topics` — this avoids a race between the viewer's local
  renewal timer and server-side expiry, and means `interest.renew` never
  needs to fail with "no such lease".
- `unsubscribe` ends the lease immediately, without waiting for
  `expires_at`.
- A lease that is not renewed lapses silently at `expires_at`; no envelope
  marks the lapse by itself.

The server tracks **one lease-count per space** (summed across every viewer
connection subscribed to it, not per individual viewer). On every
transition of that count:

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
  itself (§7): `action` is one of `throttle`, `resume_rate`. A viewer MUST
  NOT send either of these actions; the server MUST reject such an attempt
  with `error{code: unauthorized}` (see
  `fixtures/invalid/command-viewer-sends-throttle.json`).

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
- `params` is reserved for future action-specific data. Unlike every other
  object in this protocol, `params` explicitly permits additional
  properties, which a receiver that does not recognize them MUST ignore.

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

### 9.1 Ordering

`hello` MUST be the first envelope sent on every new connection, in **both**
directions. A device that sends any other message type before `hello` MUST
be rejected by the peer with `error{code: hello_required, fatal: true}` and
the connection MUST be closed. Symmetrically, a device MUST NOT act on any
envelope from its peer before that peer's `hello` has been received and
validated.

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

## 10. Authentication boundary

The credential (a `st2_<space>_<secret>` token, or an equivalent a future
transport might use) is presented **once, at transport/connection
establishment** — e.g. as a WebSocket upgrade header or query parameter, or
whatever the concrete transport's native mechanism for out-of-band
credentials is. It is never repeated inside any envelope afterward.

An envelope carries only `device_id` (in `hello`) and, where relevant,
`issued_by_device_id`/`target_device_id` (in `command`) — none of these are
secrets; they identify a device, they do not authenticate one. The server
MUST bind `device_id` and `role` to the identity it resolved from the
connection-establishment credential and MUST NOT trust a `device_id` or
`role` value asserted inside a `hello{stage: offer}` body if it conflicts
with that resolved identity.

### 10.1 Authorization matrix

The server MUST validate **every** incoming envelope's `type` against the
sending connection's `role`, not only once at connection time. A message
sent by a role not listed below MUST be rejected with
`error{code: unauthorized}`.

| `type` | source may send | viewer may send |
|---|---|---|
| `hello` | yes (as offer) | yes (as offer) |
| `resume` | no | yes |
| `snapshot` | no (server-only) | no (server-only) |
| `delta` | no (server-only) | no (server-only) |
| `ack` | yes, OPTIONAL (may acknowledge receipt of a `command` it was sent; not required for correctness since `command_id` already makes execution idempotent, §8) | no (a viewer has no scenario in this protocol requiring it to send `ack`: the server always originates the acks a viewer receives for `subscribe`/`unsubscribe`/`interest.renew`, and a viewer never receives a reliable event it must acknowledge — catch-up after a gap uses `resume`/`delta`/`snapshot`, §6, not per-message acking) |
| `task.event` | yes | no |
| `message.event` | yes | no |
| `metric.frame` | yes | no |
| `subscribe` | no | yes |
| `unsubscribe` | no | yes |
| `interest.renew` | no | yes |
| `command` | no (only receives it) | yes, with `origin: "viewer"` and actions limited to `pause`/`resume`/`stop`/`run_now` (§8) |
| `error` | yes | yes |

`snapshot` and `delta` are server-to-client only; a client that sends
either MUST be rejected with `error{code: unauthorized}`. This matrix
governs client-originated envelopes only — see §8 for why server-originated
`command` envelopes are not restricted by it.

## 11. Transport framing limits

- One frame carries exactly one JSON object (or the literal `ping`/`pong`
  text, §9.3) — this protocol never batches multiple envelopes into one
  frame.
- A frame MUST NOT exceed **64 KiB** of UTF-8-encoded JSON. A sender that
  would exceed this MUST split the content across multiple envelopes
  (`delta.body.events` and `metric.frame.body.metrics` are the only
  naturally batch-shaped payloads, and both already have their own item
  caps below). A receiver that gets an oversized frame MUST reject it with
  `error{code: frame_too_large, retryable: true}` and MUST NOT attempt to
  parse it.
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

| code | sent by | retryable | closes connection | when |
|---|---|---|---|---|
| `version_unsupported` | server | false | yes | `hello{stage: offer}`'s `protocol_versions` shares no version with the server's supported set (§9.2) |
| `hello_required` | server | true | yes | any envelope other than `hello` arrived before `hello` completed (§9.1) |
| `unauthenticated` | server | false | yes | the connection's credential became invalid mid-connection (e.g. revoked) |
| `unauthorized` | server | false | no | sender's role does not permit this `type` (§10.1), or a `command.issued_by_device_id` mismatch (§8) |
| `malformed` | any | true | no | an envelope fails schema validation for a reason other than the specific codes below (e.g. a required field is missing or the wrong type) |
| `rate_limited` | server | true | no | a per-connection frequency cap was exceeded (§11, §12) |
| `frame_too_large` | server | true | no | a single frame exceeded 64 KiB (§11) |
| `batch_too_large` | server | true | no | an array field exceeded its item cap (§11) |
| `revision_unavailable` | server | true | no | `resume.body.last_revision` is greater than the space's current `space_revision` (§6.3) |
| `command_expired` | any | false | no | a `command` was first actionable after its `ttl_ms` had elapsed (§8) |
| `sequence_gap` | server | false | no | a source's `device_seq` skipped one or more values (§5.1); advisory only, the event that did arrive is still applied |
| `internal_error` | server | true | no | an unexpected server-side fault unrelated to the request's validity |

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
| `snapshot` | no (computed on demand from persisted state) | no | no | no |
| `delta` | no (computed from persisted event log) | no | no | no |
| `ack` | no | no | no | no |
| `task.event` | yes | yes | yes (push/Live Activity update) | yes |
| `message.event` | yes | yes | yes (push notification) | yes |
| `metric.frame` | no | no | no | yes (best-effort) |
| `subscribe` | yes (lease record) | no | no | no |
| `unsubscribe` | yes (lease record removed) | no | no | no |
| `interest.renew` | yes (lease record updated) | no | no | no |
| `command` | yes (until delivered or expired) | no | yes (source may be offline) | no (targeted) |
| `error` | no | no | no | no |

## 15. Compatibility policy

- **Minor/patch changes** (this protocol's version stays `1`): additive
  only. New optional fields, new enum values in a field that is
  documented as open to extension, or new `capabilities` flags. A receiver
  MUST ignore fields inside `body` (or at the envelope level, beyond the 4
  fixed fields) that it does not recognize — it MUST NOT reject a message
  solely because of their presence. This tolerance is a **runtime
  requirement on implementations**; it is deliberately stricter than what
  the JSON Schemas in this directory enforce (most schemas here use
  `additionalProperties: false`). The schemas describe the exact,
  frozen v1.0.0 conformance surface for this repository's fixtures and
  validator (§16) — precision that is valuable for catching
  implementation bugs against a known-good baseline — while production
  parsers remain lenient toward fields a future minor/patch version might
  add. These are not in tension: the schemas are a conformance test for
  *this* version, not a description of every byte a lenient parser must
  accept.
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
3. Asserts every fixture under `fixtures/invalid/` is rejected by at least
   one of those two schemas.
4. Exits non-zero if any fixture produces an unexpected result.

Run it with:

```sh
cd proto/realtime/tools
npm install     # once, materializes node_modules/ from the committed lockfile
npm run validate
```
