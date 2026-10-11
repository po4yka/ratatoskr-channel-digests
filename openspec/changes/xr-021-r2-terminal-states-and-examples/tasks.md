## 1. Run deadline reaper (R2-05 a)

- [x] 1.1 RED: add `tests/config.rs::run_deadline_is_finite` and `tests/run_deadline.rs` covering an overdue run in each of `accepted`, `acquiring` and `waiting_recap`, a younger run, a scheduled run, a second reaper pass, an existing failed report, and a late Knowledge completion and failure; add a signature-only `Reaper` that reaps nothing; run them and confirm each fails on its stated state or count.
- [x] 1.2 GREEN: add `RATATOSKR__LIMITS__RUN_DEADLINE_SECONDS`, implement `Reaper::reap_once`, add the partial index `digest_runs_open_idx` to `schema.sql` and spawn the reaper from `run_worker` every 30 seconds.

## 2. Attributable rejects (R2-05 b)

- [x] 2.1 RED: add `tests/worker_messages.rs::a_run_command_that_fails_validation_after_decoding_reports_failed` and its subscription and unattributable companions; run them and confirm they fail because the message is Termed and no report exists.
- [x] 2.2 GREEN: classify invalid commands into attributable and unattributable in `src/bus.rs` and `src/intake.rs`, and report through `src/reports.rs`.

## 3. Page size clamp (R2-05 e)

- [x] 3.1 RED: add `services/api/tests/api.rs::a_page_size_above_the_configured_limit_is_clamped_not_refused`; run it and confirm it fails with 400 instead of 200.
- [x] 3.2 GREEN: clamp in `checked_page_size` and update `docs/INTERFACES.md`.

## 4. Scheduled output language (R2-05 f)

- [x] 4.1 RED: add `tests/config.rs::schedule_output_language_is_strict_and_worker_only` and `tests/schedule.rs::fan_out_uses_the_configured_language`; run them and confirm they fail on the unrecognized key and the default language.
- [x] 4.2 GREEN: add `ScheduleConfig.output_language` and pass it through the worker handler into `accept_occurrence`.

## 5. Operator examples and documentation (R2-10, R2-01)

- [x] 5.1 RED: add `tests/deployment_profile.rs::shipped_examples_load_through_the_config_loader`; run it and confirm it fails because the example files are absent.
- [x] 5.2 GREEN: ship the two example files. No failing behaviour test applies to the documentation edits to `docs/OPERATIONS.md`, `docs/INTERFACES.md` and `README.md`; the example test and `openspec validate` are the evidence.

## 6. Gate

- [ ] 6.1 Run the `DEVELOPMENT.md` gate to green. No failing test applies because this task only runs checks.
