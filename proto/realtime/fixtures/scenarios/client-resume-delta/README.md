# Scenario: client-resume-delta

A viewer device (`iphone-quintin-01`) connects holding recent state, follows
the mandatory viewer connection sequence **hello -> subscribe -> resume**,
catches up with an incremental `delta`, and then receives a live
single-event `delta` - the only carrier of reliable events toward a viewer.
This is the "客户端 resume" flow of SPEC.md §6.3, in the case where the gap
is small enough for the server to serve from its retained event history.

## Message sequence

| # | file | sender | note |
|---|------|--------|------|
| 1 | `01-viewer-hello-offer.json` | viewer | role: viewer |
| 2 | `02-server-hello-accept.json` | server | viewer sends nothing else until this arrives |
| 3 | `03-viewer-subscribe.json` | viewer | establishes the device's interest lease BEFORE resume |
| 4 | `04-server-ack-subscribe.json` | server | lease `expires_at` |
| 5 | `05-viewer-resume.json` | viewer | `last_revision: 126` - the last revision this viewer fully applied |
| 6 | `06-server-delta-catchup.json` | server | `from_revision: 126, to_revision: 128`, the 2 reliable events applied in between; viewer's local revision C becomes 128 |
| 7 | `07-server-delta-live.json` | server | live single-event delta `128 -> 129`, sent because a source just reported progress; `from_revision (128) == C`, so the viewer applies it and sets C = 129 |

## Expected behavior

- subscribe precedes resume (mandatory order). Because the lease is active
  before the resume reply arrives, any live delta emitted in that window is
  also delivered; the viewer's `from_revision` rules make the interleaving
  safe (next point).
- Viewer delta application rules (SPEC.md §6.3), driving every step above:
  - `from_revision < C`: discard silently (already covered by the resume
    reply or an earlier delta).
  - `from_revision == C`: apply, set `C = to_revision`.
  - `from_revision > C`: revision gap - the viewer MUST re-send `resume`.
- `to_revision - from_revision` MUST equal `events.length` in every delta
  (2 in step 6, 1 in step 7): every reliable event advances `space_revision`
  by exactly 1, so the arithmetic is exact, never approximate.
- The server MUST only serve the step-6 catch-up delta when it has retained
  every reliable event in `(126, 128]`; compare with
  `client-revision-gap-snapshot/`, which shows the snapshot fallback.
- Live reliable events always arrive as single-event deltas like step 7 -
  a viewer never receives a bare `task.event`/`message.event`/`config.event`
  envelope.
