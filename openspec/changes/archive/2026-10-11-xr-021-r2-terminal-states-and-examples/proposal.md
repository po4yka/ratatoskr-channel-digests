## Why

Round 2 of changeset XR-021 re-verified the digest chain against the pushed heads and found five remaining defects. An on-demand digest operation can stay `running` until Platform's 24-hour stale reaper because nothing in this service looks at a run's age. A lower `RATATOSKR__LIMITS__PAGE_SIZE` turns a valid Platform list request into an upstream 400 and a public 502. Scheduled digests are always Russian because nothing sets the column default. A command that decodes far enough to name its operation and owner but fails validation is Termed without telling Platform. And the service ships no deploy example, so a flow whose enabling keys are not defaults is silently disabled. This change fixes them (XR-021 CONTRACTS.md sections R2-05 a, b, e and f, R2-10 and R2-01).

## What Changes

- Add a run deadline reaper: an on-demand run in `accepted`, `acquiring` or `waiting_recap` older than `RATATOSKR__LIMITS__RUN_DEADLINE_SECONDS` (default 1800, 60..=86400) fails with `safe_failure_class = 'deadline_exceeded'` and one retryable `failed` report `channel_digest.run_deadline_exceeded` in the same transaction. A scheduled run fails without a report. The reaper is its own worker task, every 30 seconds, independent of the provider connection (R2-05 a).
- **BREAKING**: a run or subscription command that is attributable (it names an operation and an owner the envelope tenant confirms) but fails validation or an invariant is no longer Termed silently; the inbox row is marked failed, one `failed` report `channel_digest.command_invalid` (not retryable) is queued and the message is acknowledged. Only an unattributable command is Termed (R2-05 b).
- **BREAKING**: `GET /v1/subscriptions` and `GET /v1/results` clamp `page_size` to the configured `RATATOSKR__LIMITS__PAGE_SIZE` instead of answering 400 for a larger valid request; zero and non-numeric values stay 400 (R2-05 e).
- Add `RATATOSKR__SCHEDULE__OUTPUT_LANGUAGE` (`ru` or `en`, default `ru`, worker only, strict) and create every scheduled run with it (R2-05 f).
- Ship `deploy/systemd/api.conf.example` and `deploy/systemd/worker.conf.example`, loaded by a test through the real configuration loader, and give the Knowledge origin as `http://127.0.0.1:8091` after the Knowledge listener split (R2-10, R2-01).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `channel-digest-service`: runs always reach a terminal state, attributable rejects are reported, list pages are clamped, scheduled runs take a configured language, and shipped examples are loadable.

## Impact

- Affects `schema.sql` (a reaper index only, edited in place, no migration), `src/config.rs`, a new `src/reaper.rs`, `src/runtime.rs`, `src/bus.rs`, `src/intake.rs`, `src/coordinator.rs`, `src/api.rs`, new files under `deploy/systemd/`, tests, `docs/OPERATIONS.md`, `docs/INTERFACES.md` and `README.md`.
- No new dependency and no change to a wire shape; the repository stays on contracts commit `ad16855c4e7f3d52cd118274faa3b8f3ab4da576`. Cross-repository behaviour is defined in XR-021 CONTRACTS.md sections R2-05, R2-10 and R2-01, not restated here.
- Rollout: independent of the other repositories; Platform keeps forwarding `page_size` up to 100 and now gets 200. Rollback: revert the commits; a run the reaper failed stays failed.
