## Why

Round 5 of changeset XR-021 found three defects in what channel-digests puts into a digest and keeps afterwards (XR-021 CONTRACTS.md R5-07). `RunExecutor::select_sources` ends with `order by published_at, ... limit 100`, so a window with more than 100 posts keeps the oldest 100, drops the newest silently, and the run still settles `completed` and the operation `succeeded`; `config.limits.source_count` is parsed and never read. The real MTProto adapter marks every post `deleted: false` and nothing writes `post_revisions.deleted_at`, so a post deleted upstream is summarized again in every later digest of its window. `Maintenance::minimize_before` has no caller outside one test, so stored bodies are never minimized, against the retention requirement of the accepted spec.

## What Changes

- Selection takes the newest `limits.source_count` revisions (default and maximum 100), orders them ascending for the manifest, and sets `acquisition_incomplete` when more eligible revisions existed. The existing report path then yields result outcome `partial`, operation `partially_succeeded` and the warning `channel_digest.acquisition_incomplete`, with a new message text and the same code. A content-free WARN carries the two counts.
- A post that is absent from a re-read page inside the id range that page proves, or that a page marks `deleted: true`, is withdrawn: every revision of the message gets `deleted_at`, a null `body` and `minimized_at`, and selection excludes it. A provider error, a `max_pages` stop and a media-only post withdraw nothing they should not.
- The worker minimizes bodies older than `RATATOSKR__RETENTION__BODY_DAYS` (default 30, 8 to 3650) every `RATATOSKR__RETENTION__SWEEP_INTERVAL_SECONDS` (default 3600, 60 to 86400), at most 5000 rows per pass, counted by `channel_digests_bodies_minimized_total` and logged as a count only. **BREAKING (internal):** `Maintenance::minimize_before` gains a `limit` argument and `RunExecutor` gains `with_source_count`; every call site is updated.
- `docs/ARCHITECTURE.md`, `docs/REQUIREMENTS.md`, `docs/OPERATIONS.md`, `README.md` and `deploy/systemd/worker.conf.example` describe the new behaviour. The archive of `xr-021-r4-window-bounded-acquisition` is a separate housekeeping commit.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `channel-digest-service`: adds the requirements for newest-first bounded selection, withdrawal of deleted posts, and the retention timer with its two worker-only keys.

## Impact

- Affects `src/executor.rs`, `src/acquisition.rs`, `src/revisions.rs`, `src/maintenance.rs`, `src/config.rs`, `src/runtime.rs`, `src/reports.rs`, a new `src/retention.rs`, `deploy/systemd/worker.conf.example`, the tests named in `tasks.md` and the documents above. No schema change (`deleted_at` and `minimized_at` exist), no wire change, no migration. The repository moves to contracts commit `d98967b7b7ee80442c93ef4657140068955c8f61` in its own commit. Cross-repository behaviour is defined in XR-021 CONTRACTS.md section R5-07, not restated here.
- Rollback: revert the commits. Revisions already withdrawn keep `deleted_at`; bodies already minimized stay minimized.
