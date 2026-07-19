# Scenario: client-resume-delta

A viewer device (`iphone-quintin-01`) reconnects holding recent state and
catches up with an incremental `delta` instead of a full `snapshot`. This is
the "客户端 resume" flow required by SPEC.md's `space_revision` section, in
the case where the gap is small enough for the server to serve from its
retained event history.

## Message sequence

| # | file | sender | note |
|---|------|--------|------|
| 1 | `01-viewer-hello-offer.json` | viewer | role: viewer |
| 2 | `02-server-hello-accept.json` | server | |
| 3 | `03-viewer-resume.json` | viewer | `last_revision: 126` - the last revision this viewer fully applied before disconnecting |
| 4 | `04-server-delta.json` | server | `from_revision: 126, to_revision: 128`, carrying the 2 reliable events applied in between |

## Expected behavior

- The server MUST only answer with `delta` when it has retained every
  reliable event between `from_revision` (exclusive) and `to_revision`
  (inclusive) - here revisions 127 and 128, one event each.
- `to_revision - from_revision` MUST equal `events.length` (2 here): every
  reliable event advances `space_revision` by exactly 1, so the arithmetic is
  exact, never approximate.
- After applying `events` in order, the viewer's local `last_revision`
  becomes 128 and it is caught up; it does not need a `snapshot` at all.
- Compare with `client-revision-gap-snapshot/`, which shows the fallback path
  when the server cannot serve a delta for the requested `last_revision`.
