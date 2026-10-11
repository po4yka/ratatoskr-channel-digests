## 1. Inbox prefix (R3-01)

- [ ] 1.1 RED: add `services/worker/tests/authorized_bus.rs::the_worker_completes_jetstream_requests_under_its_inbox_prefix` and change `deploy/nats/identity.conf` and `the_fragment_grants_exactly_the_contract` to `subscribe: { allow: ["_INBOX_channel_digests.>"] }`; run them and confirm the connection-level requests time out because the worker still uses the default inbox.
- [ ] 1.2 GREEN: add `const INBOX_PREFIX` to `src/bus.rs` and apply it with `custom_inbox_prefix` in `connect`.

## 2. Incomplete acquisition is reported as incomplete (R3-10 rules 1 to 3)

- [ ] 2.1 RED: add the executor tests `one_unavailable_channel_and_no_posts_fails_the_run_instead_of_completing_empty` and `a_channel_that_fails_mid_pagination_marks_the_run_incomplete`, and the report tests `an_incomplete_acquisition_with_sources_settles_partially_succeeded`, `a_complete_acquisition_still_settles_succeeded` and a scheduled-run variant; add a signature-only `acquisition_incomplete` column and run them to confirm each fails on its stated state, outcome or report.
- [ ] 2.2 GREEN: edit `schema.sql` in place, make `acquire` return `Complete`, `Incomplete`, `Deferred` or `Unavailable`, persist the flag before `commit`, fail an empty incomplete window, and take the flag into `settle_completion` and `reports::report_completion`. Update `docs/DATA_MODEL.md` and `docs/OPERATIONS.md`.

## 3. An accepted run is queued until acquisition starts (R3-10 rule 4)

- [ ] 3.1 RED: add `an_accepted_on_demand_run_is_reported_queued_awaiting_acquisition`, `starting_acquisition_reports_running_acquiring_once` and `the_deadline_reaper_still_fails_a_queued_run` to `tests/operation_reports.rs`; run them and confirm the first two fail on the report status and count.
- [ ] 3.2 GREEN: enqueue `queued` / `awaiting_acquisition` in `src/intake.rs` and move the guarded transition and the `running` / `acquiring` report into one transaction in `RunExecutor::begin_acquisition`.

## 4. Both binaries log (R3-10 rule 5)

- [ ] 4.1 RED: add `tests/config.rs::the_telemetry_keys_are_accepted_by_both_roles_and_validated`, `services/worker/tests/boot.rs::the_worker_logs_a_warning_from_library_code` and `services/api/tests/boot.rs::the_api_logs_its_startup`; run them and confirm the keys are unrecognized and both processes write nothing.
- [ ] 4.2 GREEN: add the two telemetry keys to `Config`, enable `env-filter` on `tracing-subscriber`, add `install_subscriber` and call it first in both `main` functions. Check `docs/OPERATIONS.md` against the output.

## 5. Provisioning (R3-11, R3-18 rule 3)

- [ ] 5.1 RED: add `tests/deployment_profile.rs::the_role_file_creates_the_role_the_examples_name`; run it and confirm it fails because `deploy/postgres/role.sql` is absent.
- [ ] 5.2 GREEN: ship `deploy/postgres/role.sql`, correct the examples and their header comments, and add the PostgreSQL paragraph to `docs/OPERATIONS.md`. No failing behaviour test applies to the comment and documentation edits; the role test and `openspec validate` are the evidence.

## 6. Gate

- [ ] 6.1 Run the `DEVELOPMENT.md` gate to green. No failing test applies because this task only runs checks.
