## 0. Plan (R6-02)

- [x] 0.1 Write this change (proposal, design, spec delta). No failing test applies because this task only edits documentation.

## 1. Schema (R6-02 rule 1)

- [x] 1.1 RED: extend `tests/schema.rs` so `digest_manifests` has `minimized_at`, `canonical_text` is nullable, a row with both `canonical_text` and `minimized_at` and a row with neither are refused by the CHECK, and `digest_manifest_sources` exists with its primary key and the `(channel_id, provider_message_id)` index; run it and confirm it fails because the schema has none of them.
- [x] 1.2 GREEN: edit `schema.sql` in place (nullable `canonical_text`, `minimized_at`, the `digest_manifests_text_xor_minimized` CHECK, the link table and its index) and add the link table to every `truncate` list of the tests.

## 2. The builder carries the messages and the commit writes them (R6-02 rule 1)

- [ ] 2.1 RED: extend `tests/manifest.rs`: `ManifestBuilder::build` over three sources in two channels (two of them two revisions of one message) returns the distinct `(channel_id, message_id)` pairs; `commit_manifest` writes exactly those rows in its transaction; a replay writes no second set; a commit that fails leaves no link row. Add the `CanonicalManifest::messages` field first, empty, so the test fails on the assertion.
- [ ] 2.2 GREEN: `ManifestBuilder::build` fills the pairs and `commit_manifest` inserts them after the manifest row inside the transaction.

## 3. The timer (R6-02 rule 2)

- [ ] 3.1 RED: extend `tests/retention_worker.rs`: a manifest of a terminal run (`completed`, `partial`, `failed`) whose window is 40 days old is minimized by `sweep_once` with `sha256`, counts, `created_at`, `owner_id`, `run_id` and the joined result unchanged; a 10 day old window and a non-terminal (`waiting_recap`) old window are untouched; 5001 manifests drain over two sweeps; `channel_digests_manifests_minimized_total` counts them and is rendered by `metrics_router` beside the bodies counter. Add `Maintenance::minimize_manifests_before` as a signature-only stub returning `Ok(0)` first.
- [ ] 3.2 GREEN: implement `minimize_manifests_before`, run it from `RetentionSweeper::sweep_once` after `minimize_before`, return a `SweepOutcome` with both counts (every call site updated), add the second counter to `RetentionCounter` and `render`, and log both counts in `supervise_retention`.

## 4. Withdrawal (R6-02 rule 3)

- [ ] 4.1 RED: extend `tests/deleted_posts.rs`: of a manifest holding messages 7 and 8 and a manifest holding only message 9, a walk that proves message 7 gone minimizes the first and leaves the second intact; a walk that proves nothing gone minimizes nothing; a manifest whose run is `waiting_recap` is minimized too; the existing post-revision behaviour is unchanged. Make `Withdrawn` exist first, with the manifest count at zero.
- [ ] 4.2 GREEN: `withdraw_missing` minimizes the holding manifests in the same statement, returns a `Withdrawn { revisions, manifests }`, and the acquisition path adds the manifest count to the manifests counter.

## 5. The route (R6-02 rule 4)

- [ ] 5.1 RED: extend `services/api/tests/views.rs`: a minimized manifest read with the right owner, run and digest answers `410` with an empty body and no `Content-Type`; the same row with a wrong digest, a wrong run or another owner answers `404` with an empty body; a live manifest still answers `200` with the exact text; a database failure is still `503`.
- [ ] 5.2 GREEN: `get_manifest` answers `410` with an empty body for a matching row without text.

## 6. Documentation (R6-02 rule 6)

- [ ] 6.1 Update `docs/DATA_MODEL.md`, `docs/OPERATIONS.md`, `docs/INTERFACES.md`, `docs/ARCHITECTURE.md` and `docs/THREAT_MODEL.md`. No failing test applies because this task only edits documentation.

## 7. Gate

- [ ] 7.1 Run the `DEVELOPMENT.md` gate to green. No failing test applies because this task only runs checks.
