# Scenario: duplicate-connection-supersede

A device that already holds an open connection opens a second one (e.g. the
app relaunched before the old socket's death was detected, or the network
path changed). The server accepts the new connection and closes the old one
with `error{code: superseded}`. See SPEC.md §9.4.

## Message sequence

| # | file | sender | connection | note |
|---|------|--------|------------|------|
| 1 | `01-viewer-hello-offer-conn1.json` | viewer | C1 | device `iphone-quintin-01` |
| 2 | `02-server-hello-accept-conn1.json` | server | C1 | C1 is live |
| 3 | `03-viewer-hello-offer-conn2.json` | viewer | C2 | SAME device_id, new connection |
| 4 | `04-server-hello-accept-conn2.json` | server | C2 | server MUST accept the new connection |
| 5 | `05-server-error-superseded-conn1.json` | server | C1 | then closes C1 |

## Expected behavior

- On completing `hello` for a device that already has a live connection,
  the server MUST accept the NEW connection and close the OLD one, sending
  `error{code: superseded, retryable: false, fatal: true}` on the old
  connection first. `retryable: false` means: do not retry on that (dying)
  connection - the device's newer connection is unaffected and simply
  continues.
- From the moment C2's hello completes, ALL server-to-device traffic -
  `ack`s for the device's reliable events, targeted `command`s, resume
  replies, live deltas - routes to C2, the device's most recent connection,
  never to C1.
- Interest leases are held per DEVICE, not per connection (SPEC.md §7):
  the supersession neither ends nor double-counts the device's lease, and
  MUST NOT trigger a throttle/resume_rate emission (the count is
  unchanged). A viewer briefly holding two open connections still
  contributes exactly one lease to the space's count.
- Lease survival does NOT carry delta eligibility to C2: after step 4 the
  viewer MUST still send `subscribe` and then `resume` on C2 before the
  server sends it any delta, and its local revision C is re-established
  from C2's resume reply. Nothing (C, in-flight snapshot chunks, pending
  replies) is inherited from C1.
- A client that receives `superseded` on an old connection it still
  considered live SHOULD treat it as confirmation that its newer connection
  won; if it receives `superseded` unexpectedly (it did not itself open a
  new connection), its credential may be in use elsewhere and it SHOULD
  surface that to the user rather than silently reconnecting in a loop.
