## Why

Round 6 of changeset XR-021 found that the digest manifest keeps every selected post body for ever (XR-021 CONTRACTS.md R6-02). `digest_manifests.canonical_text` holds the exact canonical manifest text, which contains the body of every selected post. It is written once by `DigestCoordinator::commit_manifest` and never updated or deleted, and its only reader is `GET /v1/manifests/{manifest_id}`. Round 5 (R5-07) gave `post_revisions.body` a retention timer and withdrawal by absence, but neither reaches the manifest, so a post deleted upstream and a post older than `RATATOSKR__RETENTION__BODY_DAYS` stay readable in the manifest of every earlier run.

## What Changes

- `digest_manifests.canonical_text` becomes nullable and the table gains `minimized_at`; a CHECK requires exactly one of the two states. `sha256`, `source_count`, `channel_count`, `owner_id`, `run_id` and `created_at` are never changed. A new table `digest_manifest_sources` links a manifest to the `(channel_id, provider_message_id)` pairs it holds. The schema is edited in place, with no migration.
- `ManifestBuilder::build` returns the distinct message pairs on `CanonicalManifest`, and `commit_manifest` writes them in the same transaction as the manifest row.
- The retention sweep also minimizes manifests of terminal runs whose `window_start` is older than the same cutoff as post bodies, at most 5000 per pass, counted by `channel_digests_manifests_minimized_total` beside `channel_digests_bodies_minimized_total`. **BREAKING (internal):** `RetentionSweeper::sweep_once` returns a `SweepOutcome` carrying both counts instead of one number, and `RevisionRepository::withdraw_missing` returns a `Withdrawn` carrying the revisions and the manifests changed instead of one number; every call site is updated.
- Withdrawal by absence also minimizes every manifest that holds a withdrawn message, whatever the state of its run.
- `GET /v1/manifests/{manifest_id}` answers `410 Gone` with an empty body when the owner, run and digest claims match a minimized manifest. Every other case is unchanged. This is a service-documented status, not a contract change.
- `docs/DATA_MODEL.md`, `docs/OPERATIONS.md`, `docs/INTERFACES.md`, `docs/ARCHITECTURE.md`, `docs/THREAT_MODEL.md` and the `channel-digest-service` spec say so.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `channel-digest-service`: the retention requirement covers manifests, the manifest route gains the `410` case, and a new requirement states that a withdrawn message minimizes the manifests that hold it.

## Impact

- Affects `schema.sql`, `src/manifest.rs`, `src/coordinator.rs`, `src/maintenance.rs`, `src/retention.rs`, `src/revisions.rs`, `src/acquisition.rs`, `src/runtime.rs`, `src/api.rs`, the tests named in `tasks.md` and the documents above. No new configuration key, no wire change, no migration, and no contracts commit: every `ratatoskr-*` dependency stays at `d98967b7b7ee80442c93ef4657140068955c8f61`. The manifest path and the three header names are contract constants and do not change. Cross-repository behaviour is defined in XR-021 CONTRACTS.md section R6-02, not restated here.
- Knowledge's handling of the `410` is its own work order. Until it lands, Knowledge treats any non-success status as an unavailable manifest, which is the safe behaviour.
- Rollback: revert the commits. A manifest already minimized stays minimized; the text is not recoverable by design.
