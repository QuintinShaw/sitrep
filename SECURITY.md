# Security Policy

Sitrep runs agent-written scripts on your machine and relays their output to
your devices — security reports are taken seriously.

## Reporting a vulnerability

Please use [GitHub private vulnerability reporting](../../security/advisories/new)
instead of a public issue. You'll get an acknowledgment within 72 hours.

## Scope notes

- The daemon executes scripts the **user's own agent** wrote at the user's
  request; arbitrary script execution is the product, not a vulnerability.
  In scope: sandbox escapes of any future isolation features, protocol
  injection via crafted stdout from a *watched* (not run) process, server
  auth bypass, cross-tenant data leaks.
- v1 is deliberately read-only from mobile: no remote command execution
  surface exists by design.
