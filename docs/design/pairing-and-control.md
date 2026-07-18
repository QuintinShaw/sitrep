# Device pairing and remote control

Status: v2

A space is the security and coordination boundary. A computer normally owns
the space; phones and additional source computers join with a single-use,
10-minute invite code.

## Roles

- `owner`: read, emit, manage devices and control task runs.
- `viewer`: read, register push tokens, manage devices and control task runs.
- `source`: emit events, poll automation definitions and receive commands.

Every device receives a scoped token. The server stores its SHA-256 hash and
resolves the space from the token prefix. Revocation takes effect on the next
request.

## Flow

1. The first computer calls `POST /v2/spaces` and stores the owner token.
2. A trusted device calls `POST /v2/invites` for a `viewer` or `source` code.
3. The new device calls `POST /v2/join`. Codes are single-use and expire after
   ten minutes.
4. Viewers list or revoke devices through `/v2/devices`.

QR codes contain the invite code and space locator, never a durable device
token.

## Remote control

The phone sends `pause`, `resume`, or `stop` to
`POST /v2/tasks/:id/commands`. The daemon receives commands in the response to
its regular `POST /v2/ingest` heartbeat. Commands expire after 60 seconds and
cannot contain executable text or arguments.

The phone may also run, pause, reschedule or delete an existing automation.
It cannot replace the automation's command or Agent prompt. Purchase, posting,
destructive actions and general computer control require a future explicit
permission model.
