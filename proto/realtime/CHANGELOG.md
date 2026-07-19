# Changelog

All notable changes to the Sitrep realtime protocol are documented here.
Versioning follows the compatibility policy in `SPEC.md` §15: the protocol's
own major/minor/patch version is independent of this repository's release
version.

## 1.0.0 — initial frozen specification

### Pre-freeze revisions (adversarial review, protocol owner rulings)

Applied before the 1.0.0 freeze was published; the version stays 1.0.0.

- Unified the viewer-facing reliable-event carrier as `delta`: `task.event`
  and `message.event` are now uplink-only (source → server), and the server
  re-emits every applied reliable event as a broadcast single-event
  `delta{from, to, events}`, so every viewer-visible reliable event carries
  its revision. Mandatory viewer connection sequence is now
  hello → subscribe → resume, with explicit client rules for
  `from_revision` <, ==, > the local revision.
- `resume` now always produces exactly one reply (never silence):
  `last_revision: 0` or an unservable gap → `snapshot` (possibly empty);
  `N == R > 0` → an explicit empty `delta{from: N, to: N, events: []}`.
- Added snapshot chunking (`part` from 1 + `final`, all chunks sharing one
  `revision`; receiver applies only after the final chunk) and chained
  catch-up deltas split on event boundaries; listed both in the framing
  section's split mechanisms.
- `command` gained optional `task_id`/`automation_id` with a per-action
  required-field matrix (pause/resume/stop → `task_id`; run_now →
  `automation_id`), aligning with `POST /v2/tasks/:id/commands`. `params`
  stays a reserved extension point with no v1 keys.
- Added the 14th message type `config.event`: a server-minted reliable
  event for control-plane changes with no device_id/device_seq (its
  identity is its revision slot), carried to viewers inside `delta`;
  `snapshot` gained an `automations` array; authorization matrix and cost
  table updated (persisted, revision++, no push).
- Envelope top level is now permanently strict: any unknown top-level
  field is `malformed` and this rule can never be relaxed; the runtime
  ignore-unknown-fields tolerance applies only inside `body`.
- Hardened the handshake: the client sends nothing between its offer and
  the server's accept; pre-accept frames are answered with
  `hello_required` and the connection closes; 10 s recommended offer and
  accept timeouts; `hello_required` documented as server-only, with
  retryable meaning retry-after-reconnect.
- Decoupled interest leases from connections: leases are per device, end
  only by expiry or explicit `unsubscribe`, survive disconnects, and
  support lazy expiry evaluation; `throttle`/`resume_rate` fire only on
  the space lease count's 1↔0 edges; `interest.renew` wholesale-replaces
  topics; `unsubscribe` is acked via `in_reply_to` like the other lease
  operations; in v1 only the `metric` topic filters (reliable deltas are
  always delivered to preserve revision continuity).
- Command relay rules: client-sent `origin: "server"` commands are always
  rejected as unauthorized; the server preserves the original envelope
  `ts` and `command_id` when relaying, validates TTL at relay time, and
  the source re-validates against its local clock with ±30 s skew
  allowance.
- The server now rejects any uplink event (`task.event`, `message.event`,
  `metric.frame`) whose `body.device_id` does not match the connection's
  authenticated identity.
- `snapshot.metrics` documented as a best-effort ephemeral cache (may be
  empty or stale, outside revision accounting); snapshot is sent only as
  a reply to `resume`.
- Added the deterministic event-folding section (task.event sequence →
  task_state field by field; config.event → automation set; message
  window truncation as normative behavior with the delta/snapshot history
  asymmetry declared an accepted deviation).
- `ack.acked` defined as an exact enumeration scoped to the receiving
  connection's device; `device_seq` scope pinned to (device, space).
- Added connection supersession: a device's new connection replaces its
  old one, which is closed with the new error code `superseded`; acks and
  targeted commands route to the newest connection; duplicate connections
  do not double-count leases.
- `metric_sample` gained optional gauge/threshold geometry
  (`target`/`min`/`max`/`alert_above`/`alert_below`); `internal_error`
  messages must not leak implementation details; `message.event` now
  composes the shared reliability `$def` like `task.event`.
- Fixtures: added config.event, empty delta, and `superseded` error
  fixtures; a role-based invalid fixture (viewer sending an
  `origin: "server"` command) checked by a new authorization layer in the
  validator; scenarios updated to the hello → subscribe → resume order
  with a live single-event delta; the revision-gap scenario now shows a
  two-chunk snapshot; new `duplicate-connection-supersede` scenario.

### Initial content

- Defines the complete v1 message set: `hello`, `resume`, `snapshot`,
  `delta`, `ack`, `task.event`, `message.event`, `metric.frame`,
  `config.event`, `subscribe`, `unsubscribe`, `interest.renew`, `command`,
  `error`.
- Defines the generic envelope (`type`/`id`/`ts`/`body`) and the shared
  `$defs` in `common.schema.json`.
- Defines reliable-event semantics: per-device `device_seq`,
  `(device_id, device_seq)` deduplication, `ack`-driven resend queues.
- Defines `space_revision` accounting and the two resume flows (`delta` for
  a small gap, `snapshot` for a large one or no prior state).
- Defines the interest lease (30-60 s, server-chosen duration) and the
  `command{origin: server}` `throttle`/`resume_rate` notifications on the
  space's 1↔0 active-lease transitions.
- Defines `command` for viewer-initiated `pause`/`resume`/`stop`/`run_now`,
  with idempotency key, TTL, and role/origin validation.
- Defines version negotiation via `hello` offer/accept, the authentication
  boundary (credential lives at the transport layer, never in an envelope),
  and the per-message-type/per-role authorization matrix.
- Fixes all timestamps to integer Unix milliseconds, with a schema-level
  minimum bound that rejects seconds-scale values.
- Defines the plain-text `ping`/`pong` heartbeat, decoupled from every other
  piece of protocol state.
- Enumerates all 13 error codes with fixed `retryable`/`fatal` semantics.
- Publishes the server cost table (persisted / revision++ / outbox /
  broadcast) per message type.
- Ships fixtures for every message type (valid and invalid) and five
  end-to-end scenario sequences, plus a self-contained ajv-based validator
  under `tools/`.
