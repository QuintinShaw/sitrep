# Scenario: interest-lease-expiry

A viewer subscribes, its lease lapses without renewal while it is the space's
only interested viewer, and the server tells the source device(s) to
throttle. Later a second viewer subscribes and the server tells the source(s)
to resume full rate. This is the "服务端通知 source 降频" behavior required by
SPEC.md's interest-lease section.

## Message sequence

| # | file | sender | note |
|---|------|--------|------|
| 1 | `01-viewer-subscribe.json` | viewer | first subscriber in the space |
| 2 | `02-server-ack-subscribe.json` | server | `lease.expires_at = ts + 45000` |
| — | *(45 seconds pass; the viewer sends no `interest.renew` and disconnects or goes idle)* | | no envelope marks the lapse itself - it is a server-local timer firing |
| 3 | `03-server-command-throttle.json` | server | `command{origin: server, action: throttle}`, sent to every connected source device in the space, because the lease count just went from 1 to 0 |
| 4 | `04-viewer2-subscribe.json` | a different viewer | |
| 5 | `05-server-ack-subscribe-viewer2.json` | server | fresh lease for the new viewer |
| 6 | `06-server-command-resume-rate.json` | server | `command{origin: server, action: resume_rate}`, sent because the lease count just went from 0 to 1 |

## Expected behavior

- The server tracks one interest-lease-count per space (across all viewer
  connections), not per viewer. The `throttle`/`resume_rate` notifications
  fire only on the 1-to-0 and 0-to-1 transitions of that count, never on
  every subscribe/unsubscribe/expiry individually - two viewers subscribed at
  once produces no additional command traffic when one of them
  unsubscribes, since the count only drops from 2 to 1.
- `throttle` and `resume_rate` commands always have `origin: "server"` and
  MUST NOT be sent by a viewer (see `fixtures/invalid/command-viewer-sends-throttle.json`).
- `target_device_id` is omitted on both command envelopes here: they
  broadcast to every source device currently connected to the space, not
  just one.
- `throttle` is advisory: a source SHOULD reduce its `metric.frame` cadence
  while throttled, but MUST continue sending `task.event` for lifecycle
  transitions (started/done/failed) and all `message.event`s at normal
  priority regardless of throttle state.
