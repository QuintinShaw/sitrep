# Device pairing and remote control

Status: v1 — superseded route/field detail lives in the frozen contract,
`docs/design/v1-architecture.md` (§1, §1.4, §3, §4.1, §10.5); this file is a
short, product-level companion, not the authority for wire shapes.

A space is the security and coordination boundary. A computer normally owns
the space; phones and additional source computers join with a single-use,
10-minute invite code.

## Roles

- `owner`: a strict superset of `source` and `viewer` — read, emit events,
  manage devices, control task runs, and everything below. The
  space-creating computer holds this role so it can both run tasks and
  observe (v1-architecture.md §3, P0-1).
- `viewer`: read, register push tokens, manage devices and control task
  runs.
- `source`: emit events, poll automation definitions and receive commands.

Every device receives a scoped token (`sr1_<space_id>_<secret>`). The server
stores its SHA-256 hash and resolves the space from the token prefix.
Revocation takes effect on the next request, and force-closes any live
WebSocket for that device in the same operation (v1-architecture.md §10.2).

## Flow

1. The first computer calls `POST /v1/spaces` and stores the owner token
   (and its own `device_id` — required to report its own tasks/metrics).
2. A trusted device calls `POST /v1/invites` for a `viewer` or `source`
   code.
3. The new device calls `POST /v1/join`, sending both the code and the
   `space` it decoded from it. Codes are single-use and expire after ten
   minutes.
4. Viewers list or revoke devices through `/v1/devices`.

**Connect codes are self-routing (v1-architecture.md §10.5).** The code a
QR scan or manual entry carries encodes the target `space_id` directly — a
joining device decodes it locally and always sends `{code, space}` to
`POST /v1/join`, which routes straight to that space's SpaceHub with no
separate lookup of any kind. This never carries a durable device token —
only the one-time code and the space locator, exactly as before — but
unlike the pre-v1 design, the space locator no longer depends on a
side-store to resolve.

## Remote control

The phone sends `pause`, `resume`, or `stop` to
`POST /v1/tasks/:id/commands`. Delivery is fetch-then-ack and at-least-once
(v1-architecture.md §1.4, §4.1): the daemon reads pending commands in the
response to its regular `POST /v1/events` heartbeat (or a live WebSocket
hint), but a command is not consumed by merely being read — it keeps
reappearing on every poll until the daemon acks it (`ack_command_ids`),
which it does only after durably handing the action off locally. This is
safe because pause/resume/stop are idempotent: re-applying one to a task
already in that state is a no-op. Commands expire after 60 seconds by
default and cannot contain executable text or arguments.

The phone may also run, pause, reschedule or delete an existing automation.
It cannot replace the automation's command or Agent prompt. Purchase, posting,
destructive actions and general computer control require a future explicit
permission model.
