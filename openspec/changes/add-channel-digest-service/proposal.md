## Why

Ratatoskr needs one bounded context that can use an explicitly consented MTProto account to acquire
public channel posts without moving provider credentials into Telegram, Platform, or Knowledge. The
workspace `channel-digest-system` contract is approved and Contracts plus the dormant Knowledge
consumer are published; this repository must now implement the source-owning producer boundary.

## What Changes

- Bootstrap a Rust workspace with separate loopback API and worker roles.
- Add one current PostgreSQL schema for provider status, subscriptions, immutable revisions, runs,
  manifests, results, inbox/outbox records, and leases.
- Implement a worker-only encrypted-session boundary and a narrow public-channel MTProto adapter.
- Build deterministic bounded acquisition, immutable manifests, Knowledge recap exchange, scheduled
  occurrence execution, retention, and restart-safe state machines.
- Expose authenticated owner-scoped subscription, manifest, result, and command routes on loopback
  port `8098`, with operator listeners on `9469` and `9470`.
- Keep LLM inference, public client authentication, schedule timing, Bot API delivery, private
  channels/dialogs, provider membership writes, and generic automation outside this repository.

## Capabilities

### New Capabilities

- `channel-digest-service`: Repository-local configuration, persistence, provider, acquisition,
  manifest, API, event, recovery, retention, and role-lifecycle behavior implementing the approved
  workspace channel-digest contracts.

## Impact

This is the first repository commit and imports the approved TG-012 plan before runtime source. It
depends on the published `ratatoskr-contracts` channel-digest crate and dormant Knowledge consumer.
Platform and Telegram remain downstream and the schedule stays disabled until their compatible
revisions and the workspace composed profile are green. No migration or second contract major is
introduced.
