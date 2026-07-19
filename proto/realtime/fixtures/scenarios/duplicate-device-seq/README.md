# Scenario: duplicate-device-seq

A source device sends a reliable event, its `ack` is lost in transit (not
absence of connectivity - just packet/frame loss), the source's own resend
timeout fires before it ever sees the ack, and it retransmits the identical
event. The server must treat this as a no-op duplicate, not a second event.

## Message sequence

| # | file | sender | note |
|---|------|--------|------|
| 1 | `01-source-task-event-seq20-first.json` | source | `device_seq: 20`, task done |
| 2 | `02-server-ack-seq20-first.json` | server | acks seq 20 - **this ack is lost in transit and never reaches the source** |
| 3 | `03-source-task-event-seq20-retry.json` | source | same `device_id`/`device_seq: 20`/domain content as step 1, but a new envelope `id` (`dds-env-03`) and a later `ts`, sent because the source's local resend timeout elapsed without seeing an ack |
| 4 | `04-server-ack-seq20-retry.json` | server | acks seq 20 again |

## Expected behavior

- The server's deduplication key is `(device_id, device_seq)` =
  `(mac-quintin-01, 20)`, identical between steps 1 and 3. The server MUST
  detect step 3 as an already-applied event and MUST NOT apply it again:
  `space_revision` is incremented exactly once for this event, not twice.
- The server MUST still send the ack in step 4 even though it performed no
  new work, so the source can stop retrying. Acking a duplicate is always
  safe (idempotent) and never rejected as an error.
- This is the normal, expected behavior for at-least-once delivery. It is not
  an error condition and produces no `error` envelope - contrast with the
  invalid fixtures under `fixtures/invalid/`, which are all cases a
  conformant server MUST reject.
- Envelope `id` and `ts` differ between steps 1 and 3, exactly as they do
  between the two transmissions in `agent-reconnect-replay/`; only
  `(device_id, device_seq)` inside `body` carries reliable-event identity.
