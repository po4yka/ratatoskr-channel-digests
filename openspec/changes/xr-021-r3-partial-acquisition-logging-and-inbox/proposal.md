## Why

Round 3 of changeset XR-021 re-verified the digest chain against the pushed heads and found five defects in this service. The worker connects to NATS with the default `_INBOX` prefix, so its deployed ACL has to let it subscribe to the shared `_INBOX.>` namespace and a compromised neighbour could read its deliveries (XR-021 CONTRACTS.md section R3-01). An acquisition that failed on one channel or stopped half way through another is treated as complete, so a window can settle as an empty success or as a plain `completed` result that hides missing posts (R3-10 rules 1 to 3). An accepted on-demand run is reported `running` / `acquiring` although nothing acquires while the provider is down (R3-10 rule 4). Neither binary installs a tracing subscriber, so every documented warning is dropped (R3-10 rule 5). The PostgreSQL role the examples name is created by nothing and one example names another database than the rest of the fleet (R3-11).

## What Changes

- **BREAKING**: the worker connection uses the inbox prefix `_INBOX_channel_digests` and `deploy/nats/identity.conf` allows `subscribe` to exactly `_INBOX_channel_digests.>`. A worker built before this change cannot complete JetStream requests under the new ACL; reload NATS and deploy the services of one snapshot together (R3-01).
- Add `digest_runs.acquisition_incomplete boolean not null default false`, edited into `schema.sql` in place. Acquisition sets it when any subscribed channel ended unavailable or reported a partial page walk. A run with no selected source and an incomplete acquisition fails with `provider_unavailable` and the retryable report `channel_digest.provider_unavailable` instead of completing empty. A run with sources proceeds, and its result outcome is `partial` and its terminal report `partially_succeeded` with the warning `channel_digest.acquisition_incomplete` next to `channel_digest.context_omitted` when that also applies (R3-10 rules 1 to 3).
- **BREAKING**: the intake report of an accepted on-demand run is `queued` with stage `awaiting_acquisition`; the transition `accepted` to `acquiring` queues `running` / `acquiring` in its own transaction (R3-10 rule 4).
- Add `RATATOSKR__TELEMETRY__LOG_FORMAT` (`json` default, or `pretty`) and `RATATOSKR__TELEMETRY__LOG_FILTER` (an `EnvFilter` directive string) to both roles, and install the subscriber first thing in both binaries (R3-10 rule 5).
- Ship `deploy/postgres/role.sql`, correct the examples to the shared `ratatoskr` database, and state the start order and the shared service secrets in the example headers (R3-11, R3-18 rule 3).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `channel-digest-service`: the worker has its own inbox prefix, incomplete acquisition is reported as incomplete, a waiting run is queued, both binaries log, and the database role is provisioned by a checked-in file.

## Impact

- Affects `schema.sql` (one column, edited in place, no migration), `src/bus.rs`, `src/executor.rs`, `src/coordinator.rs`, `src/reports.rs`, `src/intake.rs`, `src/config.rs`, a new `src/telemetry.rs`, both `main.rs`, `Cargo.toml` (the `env-filter` feature of `tracing-subscriber`), `deploy/nats/identity.conf`, new `deploy/postgres/role.sql`, both systemd examples, tests, `docs/DATA_MODEL.md`, `docs/OPERATIONS.md`.
- No wire shape changes and the repository stays on contracts commit `ad16855c4e7f3d52cd118274faa3b8f3ab4da576`. Cross-repository behaviour is defined in XR-021 CONTRACTS.md sections R3-01, R3-10, R3-11 and R3-18, not restated here.
- Rollout: the NATS reload and the worker deployment go together (R3-01 rule 5). Rollback: revert the commits; a run already marked incomplete keeps its flag.
