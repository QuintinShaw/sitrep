# Scenario: client-revision-gap-snapshot

A viewer device (`ipad-quintin-01`) reconnects after being offline long
enough that the server can no longer serve it an incremental catch-up, so
the server falls back to a full `snapshot` - delivered here as **two
chunks** to demonstrate snapshot chunking under the frame-size limit. This
is the fallback half of the "客户端 resume" flow of SPEC.md §6.3.

## Message sequence

| # | file | sender | note |
|---|------|--------|------|
| 1 | `01-viewer-hello-offer.json` | viewer | |
| 2 | `02-server-hello-accept.json` | server | |
| 3 | `03-viewer-subscribe.json` | viewer | mandatory order: subscribe before resume |
| 4 | `04-server-ack-subscribe.json` | server | lease granted |
| 5 | `05-viewer-resume.json` | viewer | `last_revision: 40` |
| 6 | `06-server-snapshot-part1.json` | server | `revision: 150, part: 1, final: false` - tasks + automations |
| 7 | `07-server-snapshot-part2-final.json` | server | `revision: 150, part: 2, final: true` - metrics + messages |

## Expected behavior

- The server MUST send `snapshot` instead of `delta` whenever it cannot
  produce the complete, gapless event range `(last_revision, current]` -
  here because its retained event history does not go back as far as
  revision 40. This is a normal outcome, not an error, and `resume` always
  produces exactly one reply (a snapshot counts as one reply even when
  chunked) - never silence.
- Chunking rules (SPEC.md §6.2): every chunk carries the SAME `revision`
  (150); `part` counts from 1; chunks arrive consecutively and in order on
  the same connection; the receiver concatenates the four arrays across
  chunks and applies nothing until the `final: true` chunk arrives. A
  single-chunk snapshot would be `part: 1, final: true`.
- `snapshot.metrics` is a best-effort cache and MAY be empty or stale
  (e.g. after a server restart); the viewer refreshes metrics from
  subsequent `metric.frame` envelopes.
- Contrast with `last_revision` *greater* than the server's current
  `space_revision` (the viewer claims a revision the space never reached,
  e.g. after the space was rebuilt): that case is answered with
  `error{code: revision_unavailable}`, and the viewer retries with
  `last_revision: 0`. This scenario does not exercise it.
- After applying the aggregated snapshot, the viewer's local revision
  becomes 150 regardless of what it was before; a live delta with
  `from_revision: 150` is then applicable.
