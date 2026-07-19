# Changelog

All notable changes to the Sitrep realtime protocol are documented here.
Versioning follows the compatibility policy in `SPEC.md` §15: the protocol's
own major/minor/patch version is independent of this repository's release
version.

## 1.0.0 — initial frozen specification

- Defines the complete v1 message set: `hello`, `resume`, `snapshot`,
  `delta`, `ack`, `task.event`, `message.event`, `metric.frame`,
  `subscribe`, `unsubscribe`, `interest.renew`, `command`, `error`.
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
- Enumerates all 12 error codes with fixed `retryable`/`fatal` semantics.
- Publishes the server cost table (persisted / revision++ / outbox /
  broadcast) per message type.
- Ships fixtures for every message type (valid and invalid) and five
  end-to-end scenario sequences, plus a self-contained ajv-based validator
  under `tools/`.
