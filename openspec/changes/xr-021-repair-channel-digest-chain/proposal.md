## Why

The digest chain is dead end to end. Scheduled runs never execute because the executor joins the outbox on a report row that only on-demand runs have. The report written for an accepted subscription uses the status `completed`, which is not an `OperationStatus`, so Platform rejects it. The 21st active subscription raises SQLSTATE `P0001`, which is mapped to a storage error and redelivered forever. The worker connects to NATS without a credential. The bus hand-builds JSON envelopes and discards the correlation id. The manifest it serves is `{run_id, window, sources[...]}`, which the canonical manifest contract cannot decode. This change moves this repository onto the canonical contracts of changeset XR-021 (XR-021 CONTRACTS.md sections S01 to S04, S08 and S14).

## What Changes

- Pin every `ratatoskr-*` dependency to the XR-021 contracts commit `ad16855c4e7f3d52cd118274faa3b8f3ab4da576` (S00).
- **BREAKING**: build the manifest as the contract `ChannelDigestManifest`, store its exact canonical text in `digest_manifests.canonical_text` (replacing `canonical_json jsonb`), and serve those bytes verbatim (S08).
- **BREAKING**: `digest_runs.operation_id` is required, `create_digest_run` takes it, and the executor no longer joins the outbox; scheduled runs execute and reuse the occurrence operation id for correlation (S08).
- **BREAKING**: every operation report is a typed `OperationReported` with a terminal status on every path; the invalid status `completed` disappears, the subscription limit fails the operation and acknowledges the message, and an acquisition or manifest failure fails the run and reports atomically (S08 report points a to i).
- **BREAKING**: the bus wraps outbox payloads in contract `CommandEnvelope` and `EventEnvelope` values, derives the schedule occurrence operation from `correlation_id` and its owner from `tenant_id`, and authenticates to NATS with an nkey seed read from `RATATOSKR__BUS__NKEY_SEED_PATH` (S01, S02, S03).
- Register this service's daily digest schedule with Platform at worker start through `platform.schedule.registration_requested.v1`, configured by `RATATOSKR__SCHEDULE__{OWNER_USER_ID,CRON,ENABLED}` (S08).
- **BREAKING**: the API serves a bearer-only `/ready`, verifies the run-id and manifest-digest claim headers before answering a manifest read with 404 on any mismatch, adds `GET /v1/results`, and serializes subscriptions and results through the contract view types (S08).
- Carry the CHANNEL_DIGESTS identity fragment in `deploy/nats/identity.conf` and prove it against an authorization-enabled broker (S03, S04).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `channel-digest-service`: scheduled runs execute, operation reports are typed and terminal on every path, the manifest is the contract artifact served verbatim, the worker authenticates to the bus and registers its own schedule, and the API gains the readiness and results-list surfaces.

## Impact

- Affects `schema.sql` (edited in place, no migration), `src/manifest.rs`, `src/executor.rs`, `src/coordinator.rs`, `src/intake.rs`, `src/bus.rs`, `src/api.rs`, `src/config.rs`, `src/runtime.rs`, a new `src/reports.rs`, tests, `deploy/nats/identity.conf`, and the operations, interfaces, architecture and data-model documents.
- Adds `ratatoskr-operation-contracts` and `ratatoskr-error-contracts` from the already pinned contracts repository. No other new production dependency.
- Cross-repository behavior is defined in XR-021 CONTRACTS.md sections S01 to S04, S08 and S14, not restated here. Rollout order: contracts, then Platform and Knowledge together with this repository (all independent in wave 2), then the workspace pin. Recreate the development database from the edited `schema.sql`; there is no data conversion.
