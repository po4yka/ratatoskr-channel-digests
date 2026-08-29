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
