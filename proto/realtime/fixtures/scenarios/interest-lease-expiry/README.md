# Scenario: interest-lease-expiry

A viewer subscribes, its lease lapses without renewal while it is the space's
only interested viewer, and the server tells the source device(s) to
throttle. Later a second viewer subscribes and the server tells the source(s)
to resume full rate. This is the "服务端通知 source 降频" behavior required by
SPEC.md's interest-lease section (§7).

## Message sequence

| # | file | sender | note |
|---|------|--------|------|
| 1 | `01-viewer-subscribe.json` | viewer | first subscriber in the space (after its hello completes) |
| 2 | `02-server-ack-subscribe.json` | server | `lease.expires_at = ts + 45000` |
| — | *(45 seconds pass; the viewer sends no `interest.renew`)* | | no envelope marks the lapse itself; the server MAY detect it lazily, on the next state change it evaluates |
| 3 | `03-server-command-throttle.json` | server | `command{origin: server, action: throttle}`, sent to every connected source device in the space, because the space's lease count just went from 1 to 0 |
| 4 | `04-viewer2-subscribe.json` | a different viewer | |
| 5 | `05-server-ack-subscribe-viewer2.json` | server | fresh lease for the new viewer's device |
| 6 | `06-server-command-resume-rate.json` | server | `command{origin: server, action: resume_rate}`, sent because the lease count just went from 0 to 1 |

## Expected behavior

- Leases are held **per device**, not per connection, and are fully
  decoupled from connection lifetime: a lease ends ONLY when `expires_at`
  passes without renewal or when the device sends an explicit
  `unsubscribe`. A connection drop does not end the device's lease - if
  the first viewer had merely disconnected at second 10 and reconnected at
  second 20, its lease (and the space's count) would have been unaffected.
  This also lets a restarted server rebuild the count from persisted lease
  records.
- Expiry evaluation MAY be lazy: the server is not required to fire a timer
  at the exact `expires_at` instant; it may notice the lapse the next time
  it evaluates the space's interest state. The 1→0 `throttle` notification
  follows the (possibly lazy) detection.
- The server tracks one interest-lease-count per space (one entry per
  device holding an unexpired lease). The `throttle`/`resume_rate`
  notifications fire ONLY on the 1→0 and 0→1 transitions of that count,
  never on every subscribe/unsubscribe/expiry individually - two devices
  subscribed at once produce no command traffic when one of them
  unsubscribes, since the count only drops from 2 to 1. A device with two
  open connections still counts once (see `duplicate-connection-supersede/`).
- `throttle` and `resume_rate` commands always have `origin: "server"` and
  can never be sent by a client: the server MUST reject a client-sent
  command with origin "server" as unauthorized (see
  `fixtures/invalid/role-viewer-command-origin-server.json`) and the schema
  additionally rejects a viewer-origin envelope carrying these actions (see
  `fixtures/invalid/command-viewer-sends-throttle.json`).
- `target_device_id` is omitted on both command envelopes here: they
  broadcast to every source device currently connected to the space, not
  just one.
- `throttle` is advisory: a source SHOULD reduce its `metric.frame` cadence
  while throttled, but MUST continue sending `task.event` for lifecycle
  transitions (started/done/failed) and all `message.event`s at normal
  priority regardless of throttle state.
