# Ratatoskr Channel Digests Agent Instructions

## Mission and ownership

This repository owns explicitly consented MTProto session use for public Telegram channels,
owner-scoped subscriptions, immutable post revisions, digest windows/runs, manifests, and Knowledge
result linkage. It does not own LLM inference, Platform public sessions/APIs, Bot API interaction, or
Telegram notification policy.

Only public username-addressed channels are in scope. Never read private channels, groups, personal
dialogs, invite targets, or arbitrary account history, and never join or leave a channel as a side
effect.

## Development status

Ratatoskr is in development. Keep one API/contract version, edit the current `schema.sql` in place,
and add no migrations, compatibility shims, or later major versions. The product name is Ratatoskr.

## How a change starts

Every non-trivial change begins as an OpenSpec change rather than as an edit, and each assistant
starts one in its own syntax. Claude Code has the command: `/opsx:propose <what you want to build>`,
or `/opsx:explore` first when the shape is not clear yet. Codex has no project-level command and
triggers the same skill by name, `$openspec-propose`, or lets its description match it. OpenCode has
its own command, `/opsx-propose`. Whichever starts it, the result is `openspec/changes/<id>/` holding
a proposal, the spec deltas, a design and a task list, and you read that plan before any code is
written. `/opsx:apply`, `$openspec-apply-change` or `/opsx-apply` builds it, and `/opsx:archive`,
`$openspec-archive-change` or `/opsx-archive` folds the deltas into `openspec/specs/`.

`openspec/specs/` holds the behaviour that is true today. A spec here grows from a change that
needed it. Do NOT convert `docs/REQUIREMENTS.md`, `docs/INTERFACES.md` or
`docs/DATA_MODEL.md` into specs in bulk. Those documents stay where they are, as
material an exploration reads. A spec set produced by bulk conversion is large, stale on the day it
lands, and trusted by nobody.

Behaviour that more than one repository can see — the shape of a contract, the meaning of a field, the
order in which repositories must receive a change — belongs in the `ratatoskr-workspace` store, not
here. `openspec/config.yaml` references it, so `openspec instructions` in this repository lists the
store's specs with the exact command that fetches one. Cite that spec from a local proposal instead
of restating it.

### Tests come first

The task list carries one pair per behaviour. The first task adds a test that fails. The second makes
it pass. Never one task that does both.

- Run the new test before you write the implementation, and confirm it fails for the reason the task
  states — not for a compile error or a typo.
- A refactor task comes after the tests are green. It adds no test and changes no behaviour.
- A task that cannot start from a failing test says why in one line. Configuration, documentation and
  generated files are the usual reasons.
- Do not tick a task whose test has not been run.

Nothing can check the order in which the two were written. What CI does check is
`openspec validate --archived`, which fails when a change was archived with a task left unticked, and
the step in `fleet.yml` that fails when a repository holds a manifest and a `ci.yml` that never runs
a test. `ratatoskr-workspace/docs/QUALITY_GATES.md` states that limit rather than implying it is
covered.

## The Rust skill catalogue

`.agents/skills/` holds eighteen Rust skills, and `.claude/skills/` symlinks to them, so all three
assistants read one copy. Codex reads `.agents/skills/`, Claude Code reads `.claude/skills/`, and
OpenCode scans both, so the existing symlink already covers it and nothing belongs under
`.opencode/skills/`. Each is a reference sheet rather than a tutorial: the commands, flags,
thresholds and triage tables for one Rust concern. Your assistant reads the descriptions and opens a
skill only when the task matches one, so the set costs almost nothing until it is needed.

`rust-tdd` is the Rust form of the task pair above. `rust-lints` owns `clippy.toml`, which is where
this repository's size limits live. `rust-security` answers a `RUSTSEC` advisory.
`rust-async-internals` covers `tokio::select!` cancel safety and shutdown. `rust-database` covers
pool budgets and transaction ownership. `rust-compiler-errors` is the entry point when the build
fails and the cause is not obvious.

`rust-database` also carries a section on deploying migrations in compatible phases. The Development
status above overrides it: while that status holds, this product has no migrations at all. Read the
rest of that skill and skip that section.

The eighteen are identical in every Ratatoskr repository whose stack is Rust, and
`ratatoskr-workspace/.github/workflows/drift.yml` fails when one copy stops matching the others. Do
not edit a file under `.agents/skills/`. A correction belongs upstream in `po4yka/rust-skills` and
reaches this repository through `npx skills update`.

The catalogue holds forty-four skills and eighteen are vendored here.
`ratatoskr-workspace/docs/QUALITY_GATES.md` records which were left out and why. They are vendored
under BSD-3-Clause, (c) 2026 Nikita Pochaev, who also owns this repository; each `SKILL.md` keeps its
`license` field, and the full text is in that repository's `LICENSE`.

## Workflow

Every non-trivial change starts in `openspec/changes/`. Read the active proposal, design, specs, and
tasks before implementation. Add and run the named failing test before its implementation task, and
do not check a task until its evidence was observed. Archive only after the exact `DEVELOPMENT.md`
gate is green.

Run compiler-backed Rust commands through `build-gate --`. Keep Cargo jobs at four or below and
release/LTO builds at two jobs. Preserve unrelated work and use the repository's dedicated task
branch/worktree for delivery.

## Security boundaries

- Session ciphertext and its key use separate files and never enter PostgreSQL, events, logs,
  configuration output, fixtures, or another service.
- The API role has no provider credential or session access.
- Events contain references, digests, counts, and safe classes, never channel-post bodies.
- Every read and mutation is owner scoped; foreign and absent resources are indistinguishable.
- Open only exact pre-provisioned JetStream topology. Never create or widen fleet-owned consumers.
- Provider calls, response sizes, pagination, retries, concurrency, pools, and shutdown are finite.

## Completion

Relevant real-PostgreSQL, fake-provider, restart/replay, API authorization, privacy, OpenSpec, lint,
dependency, build, and documentation checks must pass. Synthetic/Compose evidence is not live
MTProto authorization or production deployment evidence.
