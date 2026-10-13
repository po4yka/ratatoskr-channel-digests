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
- `manifest.rs` — builds the contract `ChannelDigestManifest` from the selected revisions, renders its canonical text, and takes the SHA-256 of that exact text; the stored text is what the API serves, and it returns the distinct `(channel, message)` pairs the manifest holds so that `coordinator.rs` can record them with the manifest.
- `reports.rs` — the typed `OperationReported` builder; every report is validated and queued in the transaction of the state it reports.
- `reaper.rs` — the run deadline reaper: fails runs that stayed non-terminal past the deadline and reports their on-demand operations in the same transaction.
- `registration.rs` — queues the daily-digest schedule registration command at worker start.
- `coordinator.rs` — atomic manifest-to-Knowledge exchange: builds the typed recap request and settles completion/failure against durable evidence.
- `intake.rs` — typed transactional command intake (subscription set, run requested, schedule occurrence) with transport/semantic deduplication.
- `bus.rs` — the exact JetStream consumer/producer boundary: connects with the nkey seed, verifies (never creates) the pre-provisioned durables, and wraps outbox rows into contract `CommandEnvelope` and `EventEnvelope` values at publish time.
- `result_reader.rs` — the bounded, non-retrying read-through client to Knowledge's result endpoint.
- `api.rs` — the loopback HTTP surface (`/ready`, subscriptions, manifests, result summaries and results) built on the modules above, serializing through the contract view types.
- `maintenance.rs`, `retention.rs` — body minimization and its timer: `RetentionSweeper` runs `Maintenance::minimize_before` every `RATATOSKR__RETENTION__SWEEP_INTERVAL_SECONDS` for bodies older than `RATATOSKR__RETENTION__BODY_DAYS`, on its own task beside the reaper, and, with `Maintenance::minimize_manifests_before`, nulls the text of manifests of settled runs whose run window is older than the same cutoff. It serves the counters `channel_digests_bodies_minimized_total` and `channel_digests_manifests_minimized_total` on `GET /metrics` of the worker operator plane.
- `runtime.rs` — wires configuration, database, session, provider, bus, and API into `run_api` and `run_worker`.

## Data flow

1. Platform issues a typed subscription-set, run-requested, or schedule-occurrence command through Contracts; the worker's `bus.rs` delivers it from a pre-provisioned durable consumer to `intake.rs` (or, for an occurrence, `coordinator.rs`), which commits the domain effect and a typed operation report atomically. Every on-demand operation ends in a terminal report on every path; scheduled runs are owned by no operation and report nothing. The report vocabulary is in [docs/INTERFACES.md](INTERFACES.md).
2. For an accepted run, scheduled or on-demand (each carries its `operation_id`), `executor.rs` acquires eligible revisions through `provider.rs` and `acquisition.rs`, appending immutable rows via `revisions.rs`.
3. `executor.rs` selects the newest `RATATOSKR__LIMITS__SOURCE_COUNT` revisions of the window (default and maximum 100: published time descending, then channel, message id descending, revision id descending) and hands them to the manifest oldest first. When the window held more eligible revisions than were selected the run is marked `acquisition_incomplete` exactly as an unread channel is, so the result is `partial` and an on-demand operation `partially_succeeded` with `channel_digest.acquisition_incomplete`; a WARN carries the selected and eligible counts and no content. `manifest.rs` builds the canonical manifest from selected revisions; `coordinator.rs` commits it and queues exactly one typed recap request for Knowledge in the transactional outbox. A run that selects nothing completes without a manifest; an acquisition failure or an unbuildable manifest fails the run and its operation instead of being retried forever.
   Posts deleted upstream are withdrawn by absence. After `acquisition.rs` stores a page, `revisions.rs` withdraws every stored, non-deleted message of that channel whose id lies in the range the page proves and that is not on the page: the range runs from the page's lowest id up to the cursor the page was requested with (no upper limit for the first page of a walk), a short last page also reaches the channel start, and a walk that stopped at the page limit or at a provider error proves only the pages it read. A post without text is still on the page and so is present, and a post the provider marks deleted is withdrawn wherever it lies. Withdrawing sets `deleted_at`, nulls the body and sets `minimized_at` on every revision of the message, and selection skips `deleted_at`, so later digests of the window no longer contain it. The same statement drops the whole text of every manifest that holds a withdrawn message (`digest_manifest_sources`), whatever the state of its run, and `api.rs` then answers `410` for that manifest; a recap Knowledge already stored, and the copy of the manifest Knowledge keeps, are not withdrawn: only later digests and later manifest reads change. A withdrawn revision is final, because the unique digest key makes a later identical observation converge on the withdrawn row.
4. Knowledge's completion or failure event is delivered back through `bus.rs`; `coordinator.rs` settles the run only when owner, run, manifest digest, and result identity match the durable evidence, producing a `digest_results` row.
5. An owner reads a subscription, manifest, or result through `api.rs`; for a completed or partial result, `result_reader.rs` re-verifies the Knowledge analysis identity and result digest before the API returns the recap, and never persists it locally.

`schema.sql` (see [docs/DATA_MODEL.md](DATA_MODEL.md)) is the single current definition backing all of the above; there are no migrations. The exact HTTP and JetStream contracts are in [docs/INTERFACES.md](INTERFACES.md).

## Ownership boundaries

This repository owns MTProto session use, owner subscriptions, immutable post revisions, digest runs/manifests, and the linkage to Knowledge results. It does not own LLM inference (Knowledge), the authenticated public facade or schedule authority (Platform), Bot API interaction or notification delivery (Telegram), or private channels, groups, dialogs, invite links, and join/leave operations, which are out of scope entirely. See [AGENTS.md](../AGENTS.md) for the full mission and security-boundary statement.

## Dependencies on other Ratatoskr repositories

- `ratatoskr-contracts` — pinned via Git revision in `Cargo.toml` for `ratatoskr-channel-digest-contracts`, `ratatoskr-error-contracts`, `ratatoskr-event-envelope`, `ratatoskr-identifiers`, and `ratatoskr-operation-contracts`; these define the typed commands and events this service consumes and produces.
- Platform — issues subscription/run/schedule commands over Contracts and owns the public facade and schedule authority; this service never bypasses that command path.
- Knowledge — receives the typed recap request and is the sole owner of recap inference and narrative; this service only reads its result endpoint through the bounded client in `result_reader.rs`.
- Telegram — owns Bot API interaction and delivery of any user-facing notification; this service does not call the Bot API or notify users directly.

Operational procedures (session rotation, Knowledge reader credential rotation, recovery queries, rollout/rollback order) are in [docs/OPERATIONS.md](OPERATIONS.md). Threat boundaries are in [docs/THREAT_MODEL.md](THREAT_MODEL.md). Functional requirements are in [docs/REQUIREMENTS.md](REQUIREMENTS.md), and the normative behavior spec is `openspec/specs/channel-digest-service/spec.md`.
