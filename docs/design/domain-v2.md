# Sitrep domain model v2

Sitrep exposes three primary product objects. Everything else must have an
explicit owner and must not become another top-level navigation concept.

## Primary objects

- **Task run** — one bounded execution. It has a stable run ID, lifecycle,
  progress and log. A task run may belong to an automation, but can also be
  started directly by `sitrep run` or an Agent hook.
- **Metric** — one continuously updated value. Display configuration,
  thresholds and metric-specific settings belong to the metric.
- **Message** — one event that already happened. It has severity, time and an
  optional originating automation. A pending condition is not a message.

## Supporting objects

- **Automation** owns an executor, schedule, safe user-editable inputs and run
  history. Its executor is `script`, `agent`, or an explicit `hybrid`; Sitrep
  does not blindly try a script before every Agent request.
- **Device** owns a credential, role, platform and push tokens.
- **Space** is the isolation and coordination boundary. One Durable Object is
  used per space.

## Ownership rules

1. A metric threshold is part of the metric, never a global parameter.
2. An automation input is part of the automation. If it controls a metric, the
   relationship uses the metric ID; names and labels are never parsed to infer
   ownership.
3. Presentation is stored against stable entity IDs, not task titles.
4. A source ID identifies a producer; a run ID identifies one execution. They
   are not interchangeable.
5. The phone may run, pause, schedule or delete an existing automation. It may
   not replace its command or prompt in v1 of remote control.

## Execution boundary

The local daemon schedules and executes work that needs local files, installed
Agents, browser login state or desktop applications. Cloudflare coordinates
spaces, state, commands and push delivery. Server alarms never attempt to run a
user's local Agent.

Read/remind is the first security tier. Purchase, posting, destructive actions
and general computer control require a later permission model and are outside
the initial product scope.

## Migration

`GET /v2/snapshot` is the only read model for new Apple clients. It adapts old
storage without exposing global parameters, armed alerts, commands or inferred
relationships. Existing pairing credentials and useful history remain valid;
unowned legacy parameters disappear instead of being guessed into the UI.
