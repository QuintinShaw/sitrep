# Scenario: agent-reconnect-replay

A source device (`mac-quintin-01`) loses its connection while it still has an
unacknowledged reliable event queued, reconnects, and replays it. This is the
"Agent 重连重放" flow required by SPEC.md's reliable-events section.

## Message sequence

| # | file | sender | note |
|---|------|--------|------|
| 1 | `01-source-hello-offer.json` | source | first envelope on the connection |
| 2 | `02-server-hello-accept.json` | server | negotiates protocol_version 1 |
| 3 | `03-source-task-event-seq10.json` | source | `device_seq: 10`, task started |
| 4 | `04-source-task-event-seq11.json` | source | `device_seq: 11`, progress 15% |
| 5 | `05-server-ack-seq10.json` | server | acks seq 10 only |
| — | *(connection drops here)* | | the ack for seq 11 never arrives at the source, whether because the server never sent it or because it was in flight when the socket died - the source cannot tell which, and does not need to |
| 6 | `06-source-hello-offer-reconnect.json` | source | fresh connection, hello is first again |
| 7 | `07-server-hello-accept-reconnect.json` | server | new `session_id`, same `protocol_version` |
| 8 | `08-source-task-event-seq11-resend.json` | source | **same `device_seq: 11`**, identical domain content, but a **new envelope `id`** (`arr-env-08`, not `arr-env-04`) |
| 9 | `09-server-ack-seq11.json` | server | acks seq 11 |

## Expected behavior

- The source's resend queue is keyed by its own unacked `device_seq` values,
  not by connection or envelope id. On reconnect it replays every reliable
  event at or after the oldest unacked `device_seq` (here, just 11) - it does
  NOT need a `resume` message of its own; `resume` is a viewer-side message.
- The server deduplicates on `(device_id, device_seq)`. If the server had, in
  fact, already durably applied `device_seq: 11` before the drop (i.e. only
  its ack was lost), reapplying it on step 8 is a no-op for `space_revision`
  (it is NOT incremented a second time) and step 9's ack is simply resent.
  A conformant server implementation cannot tell steps 3-4 apart from a
  first-time delivery vs. a replay by looking at the envelope alone, and it
  does not need to: idempotent apply-by-`device_seq` makes both cases safe.
- Envelope `id` is intentionally different between step 4 and step 8: it is a
  per-transmission identifier, not part of the reliable-event identity.
