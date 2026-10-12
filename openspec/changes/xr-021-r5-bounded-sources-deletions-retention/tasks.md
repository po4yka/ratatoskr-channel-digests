## 1. Newest posts, honest partial (R5-07 rule 1)

- [x] 1.1 RED: add `tests/executor.rs` cases for 101 and 250 revisions (the manifest holds the newest 100 in ascending order, the run is marked incomplete, the operation ends `partially_succeeded` with `channel_digest.acquisition_incomplete`, the result outcome is `partial`), exactly 100 (complete and `succeeded`) and `with_source_count(10)` over 25 revisions, and a `tests/config.rs` case that `RATATOSKR__LIMITS__SOURCE_COUNT` reaches `limits.source_count`; run them and confirm they fail on the assertion (the oldest 100 are selected and the run is complete), not on a compile error (a signature-only `with_source_count` is added first).
- [x] 1.2 GREEN: select newest first with `count(*) over ()` returning the eligible total, keep `source_count`, order ascending, OR the truncation into `acquisition_incomplete` before `record_acquisition`, log the two counts, feed `with_source_count` from `config.limits.source_count` in `runtime.rs`, and reword the warning in `reports.rs`.

## 2. Deleted posts (R5-07 rule 2)

- [x] 2.1 RED: add `tests/deleted_posts.rs` (vanished id inside the proved range, outside it, a `max_pages` stop, a provider error mid-walk, a media-only post, `deleted: true`, and the first-page, middle-page and short-last-page range cases) and a `tests/revisions.rs` case that selection never returns a revision with `deleted_at`; run them and confirm they fail because nothing sets `deleted_at`.
- [x] 2.2 GREEN: add `RevisionRepository::withdraw_missing`, call it from `AcquisitionEngine::store_page` with the range of each page, withdraw `deleted: true` posts, and filter `deleted_at is null` in `select_sources`.

## 3. Retention timer (R5-07 rule 3)

- [x] 3.1 RED: add the `tests/config.rs` cases for the two retention keys, `tests/retention_worker.rs` (a supervisor with a short interval nulls old bodies in batches of at most 5000, keeps digest, provenance and `published_at`, leaves younger bodies, increments the counter, keeps running with no provider) and the `tests/deployment_profile.rs` pin of the two keys in `worker.conf.example`; run them and confirm they fail because no caller and no keys exist (signature-only stubs are added first).
- [x] 3.2 GREEN: add the config keys and validation, the `limit` argument of `Maintenance::minimize_before` (and `tests/privacy.rs`), `supervise_retention` joined at shutdown next to the reaper, the counter and its metrics route, and the example file.

## 4. Documentation (R5-07)

- [x] 4.1 Update `docs/ARCHITECTURE.md`, `docs/REQUIREMENTS.md`, `docs/OPERATIONS.md` and `README.md` for newest-first selection and the partial marking, withdrawal by absence and its range rule, and the retention keys. No failing test applies because this task only edits documentation.

## 5. Gate

- [x] 5.1 Run the `DEVELOPMENT.md` gate to green. No failing test applies because this task only runs checks.
