# Scenario: client-revision-gap-snapshot

A viewer device (`ipad-quintin-01`) reconnects after being offline long
enough that the server can no longer serve it an incremental catch-up, so the
server falls back to a full `snapshot`. This is the fallback half of the
"客户端 resume" flow required by SPEC.md's `space_revision` section.

## Message sequence

| # | file | sender | note |
|---|------|--------|------|
| 1 | `01-viewer-hello-offer.json` | viewer | |
| 2 | `02-server-hello-accept.json` | server | |
| 3 | `03-viewer-resume.json` | viewer | `last_revision: 40` |
| 4 | `04-server-snapshot.json` | server | `revision: 150` - the server does NOT attempt a `delta`; it answers with the full current state directly |

## Expected behavior

- The server MUST send `snapshot` instead of `delta` whenever it cannot
  produce the complete, gapless event range `(last_revision, current]` -
  here because its retained event history does not go back as far as
  revision 40 (a deployment-defined retention window, e.g. the last N
  revisions or the last N days, elapsed).
- This is a normal, expected outcome, not an error: no `error` envelope is
  sent, and the connection is not affected.
- Contrast with the case where `last_revision` is *greater* than the
  server's current `space_revision` (the viewer claims a revision the space
  never reached, e.g. after the space was rebuilt) - SPEC.md's error table
  covers that distinct case with `error{code: revision_unavailable}`,
  which this scenario does not exercise.
- After applying this snapshot, the viewer's local `last_revision` becomes
  150 regardless of what it was before.
