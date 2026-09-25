# Architecture

## Processes

The `ratatoskr-channel-digests` workspace builds two binaries from one shared library crate (`src/`):

- `ratatoskr-channel-digests-api` (`services/api`) — a loopback, owner-scoped read process. It binds only `127.0.0.1:8098`, requires a service bearer secret and an explicit owner header on every `/v1` route, holds the dedicated Knowledge result-reader client, and has no provider credential or session access. Operator plane on `127.0.0.1:9469`.
- `ratatoskr-channel-digests-worker` (`services/worker`) — the event-driven acquisition and run-execution process. It is the only process that decrypts the MTProto session, calls the public-channel provider, consumes commands and events from JetStream, and drives runs through to a terminal outcome. Operator plane on `127.0.0.1:9470`.

Both processes load configuration through `Config::load` (`src/config.rs`) with role-specific validation, and both expose `/live` and `/ready` with `Cache-Control: no-store`. `src/runtime.rs` joins each role's listeners and background tasks and drains them on `SIGTERM` within a finite shutdown budget.

## Library modules (`src/`)

- `config.rs`, `database.rs` — strict configuration and the finite PostgreSQL pool shared by both roles.
- `session.rs` — the fail-closed encrypted MTProto session boundary; only constructed for the worker.
- `provider.rs` — the narrow public-channel provider capability (`PublicChannelProvider`) and its grammers-backed implementation; accepts only canonical usernames.
- `subscriptions.rs` — owner-scoped subscription persistence backed by `channel_digests.set_subscription`.
- `acquisition.rs`, `revisions.rs` — bounded, restart-safe channel paging and immutable content-digest revision persistence.
- `runs.rs`, `executor.rs` — deterministic run/window state and the durable provider-to-manifest execution path, including lease-based restart safety.
- `manifest.rs` — canonical, order-independent manifest construction and its SHA-256 identity.
- `coordinator.rs` — atomic manifest-to-Knowledge exchange: builds the typed recap request and settles completion/failure against durable evidence.
- `intake.rs` — typed transactional command intake (subscription set, run requested, schedule occurrence) with transport/semantic deduplication.
- `bus.rs` — the exact JetStream consumer/producer boundary; opens only pre-provisioned durable consumers.
- `result_reader.rs` — the bounded, non-retrying read-through client to Knowledge's result endpoint.
- `api.rs` — the loopback HTTP surface (subscriptions, manifests, results) built on the modules above.
- `maintenance.rs` — retention/telemetry-safety hooks referenced by the worker's periodic maintenance path.
- `runtime.rs` — wires configuration, database, session, provider, bus, and API into `run_api` and `run_worker`.

## Data flow

1. Platform issues a typed subscription-set, run-requested, or schedule-occurrence command through Contracts; the worker's `bus.rs` delivers it from a pre-provisioned durable consumer to `intake.rs`, which commits the domain effect and an outbox operation report atomically.
2. For an accepted run, `executor.rs` acquires eligible revisions through `provider.rs` and `acquisition.rs`, appending immutable rows via `revisions.rs`.
3. `manifest.rs` builds the canonical manifest from selected revisions; `coordinator.rs` commits it and publishes exactly one typed recap request to Knowledge from the transactional outbox.
4. Knowledge's completion or failure event is delivered back through `bus.rs`; `coordinator.rs` settles the run only when owner, run, manifest digest, and result identity match the durable evidence, producing a `digest_results` row.
5. An owner reads a subscription, manifest, or result through `api.rs`; for a completed or partial result, `result_reader.rs` re-verifies the Knowledge analysis identity and result digest before the API returns the recap, and never persists it locally.

`schema.sql` (see [docs/DATA_MODEL.md](DATA_MODEL.md)) is the single current definition backing all of the above; there are no migrations. The exact HTTP and JetStream contracts are in [docs/INTERFACES.md](INTERFACES.md).

## Ownership boundaries

This repository owns MTProto session use, owner subscriptions, immutable post revisions, digest runs/manifests, and the linkage to Knowledge results. It does not own LLM inference (Knowledge), the authenticated public facade or schedule authority (Platform), Bot API interaction or notification delivery (Telegram), or private channels, groups, dialogs, invite links, and join/leave operations, which are out of scope entirely. See [AGENTS.md](../AGENTS.md) for the full mission and security-boundary statement.

## Dependencies on other Ratatoskr repositories

- `ratatoskr-contracts` — pinned via Git revision in `Cargo.toml` for `ratatoskr-channel-digest-contracts`, `ratatoskr-event-envelope`, and `ratatoskr-identifiers`; these define the typed commands and events this service consumes and produces.
- Platform — issues subscription/run/schedule commands over Contracts and owns the public facade and schedule authority; this service never bypasses that command path.
- Knowledge — receives the typed recap request and is the sole owner of recap inference and narrative; this service only reads its result endpoint through the bounded client in `result_reader.rs`.
- Telegram — owns Bot API interaction and delivery of any user-facing notification; this service does not call the Bot API or notify users directly.

Operational procedures (session rotation, Knowledge reader credential rotation, recovery queries, rollout/rollback order) are in [docs/OPERATIONS.md](OPERATIONS.md). Threat boundaries are in [docs/THREAT_MODEL.md](THREAT_MODEL.md). Functional requirements are in [docs/REQUIREMENTS.md](REQUIREMENTS.md), and the normative behavior spec is `openspec/specs/channel-digest-service/spec.md`.
