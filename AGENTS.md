# Repository instructions

## Product language

- Describe Sitrep through its own product model: tasks, metrics, messages,
  automations, devices, and spaces.
- Keep public source and documentation focused on Sitrep. Do not name or frame
  the product through competing products unless the user explicitly requests a
  private research artifact.
- Remove obsolete concepts instead of preserving compatibility language that
  no longer matches the current architecture.

## Commit format

Use Conventional Commits for every commit:

```text
type(scope): imperative subject
```

Allowed types:

- `feat`: user-visible capability
- `fix`: bug fix
- `refactor`: behavior-preserving structural change
- `perf`: performance improvement
- `test`: test-only change
- `docs`: documentation-only change
- `build`: build system or dependency change
- `ci`: continuous-integration change
- `chore`: repository maintenance or release bookkeeping
- `revert`: revert of an earlier commit

Rules:

- Use a short, lowercase scope such as `apple`, `server`, `daemon`, `skill`,
  `protocol`, `docs`, `ci`, or `release`.
- Write the subject in English, imperative mood, without a trailing period.
- Keep the subject concise and describe the outcome, not the work session.
- Make one logical change per commit. Split unrelated product, refactor, test,
  and release work.
- Do not use vague subjects such as `update`, `final`, `batch`, `history`,
  `cleanup`, or `work in progress`.
- Do not create duplicate release commits. A release has one
  `chore(release): vX.Y.Z` commit after its required feature and fix commits.
- Use `BREAKING CHANGE:` in the commit body when a migration is required.
- Run relevant tests before committing and record any untested boundary in the
  handoff.

The initial repository snapshot is the sole exception and uses:

```text
chore(init): initialize Sitrep
```

## Pull request format

Use the same Conventional Commit form for the pull request title:

```text
type(scope): imperative subject
```

GitHub appends the pull request number to the squash commit, producing:

```text
type(scope): imperative subject (#123)
```

Keep each pull request focused on one outcome and use squash merge. The pull
request body must contain:

```markdown
## Summary
- What changed and why

## Validation
- Exact checks that passed

## Risk
- Migration, compatibility, deployment, or rollback notes
```

Use branch names in the form `type/short-description`, for example
`feat/live-activities` or `fix/pairing-refresh`.

## History safety

- Never rewrite shared history or force-push unless the user explicitly asks.
- When explicitly authorized, resolve the exact remote refs first and use
  `--force-with-lease` rather than an unconditional force push.
- Never commit credentials, generated signing material, local pairing state,
  build output, or user-specific automation data.
