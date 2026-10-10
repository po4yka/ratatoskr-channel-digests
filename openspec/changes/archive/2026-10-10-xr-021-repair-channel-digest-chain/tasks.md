## 1. Contracts pin

- [x] 1.1 Move every `ratatoskr-*` dependency to the XR-021 contracts commit `ad16855c4e7f3d52cd118274faa3b8f3ab4da576` and refresh `Cargo.lock`. No failing behavior test applies because this is a dependency pin; the existing suite staying green is the evidence.

## 2. Contract manifest (CONTRACTS S08)

- [x] 2.1 RED: add `tests/manifest.rs::built_manifest_round_trips_through_the_contract_and_matches_the_stored_digest` with two revisions of one message and a second post, driven through the executor; run it and confirm it fails because the stored output is `{run_id, window, sources}` and decoding returns `ManifestEncoding`.
- [x] 2.2 GREEN: rework `src/manifest.rs` to build the contract type, mint `manifest_id` before building, replace `canonical_json jsonb` with `canonical_text text`, store and serve the exact text, complete zero-source runs without a manifest, and adapt the dependent tests.

## 3. Scheduled runs execute (CONTRACTS S08)

- [x] 3.1 RED: add `tests/executor.rs::scheduled_run_from_an_occurrence_executes_and_commits_a_manifest`; run it and confirm it fails because `execute_one` returns `Ok(false)` and the run stays `accepted`.
- [x] 3.2 GREEN: add `digest_runs.operation_id`, store it on both acceptance paths, select it in the executor without the outbox join and use it in the recap request.

## 4. Typed terminal operation reports (CONTRACTS S08 report points a to i)

- [x] 4.1 RED: add `tests/operation_reports.rs` covering `subscription_set_reports_succeeded_not_completed`, `twenty_first_active_subscription_fails_the_operation_and_acks`, `zero_source_run_reports_succeeded`, `completion_with_omitted_sources_reports_partially_succeeded_with_warning_and_result_ref`, `knowledge_failure_reports_failed_with_mapped_code`, `acquisition_failure_reports_failed_in_the_same_transaction_as_the_state_change`, `manifest_limit_fails_the_run_instead_of_retrying_forever` and `occurrence_reports_succeeded_on_its_own_operation_and_scheduled_runs_report_nothing`; run them and confirm each fails on its stated status, code or count.
- [x] 4.2 GREEN: add `src/reports.rs`, use it from intake, coordinator and executor, catch P0001 in `accept_subscription`, and update the worker boot test, the worker message tests and the architecture document.

## 5. Bus, configuration, occurrence operation and schedule registration (CONTRACTS S01 to S03, S08)

- [x] 5.1 RED: add `tests/config.rs` cases `worker_role_accepts_an_absolute_bus_nkey_seed_path`, `worker_role_rejects_a_relative_nkey_seed_path`, `api_role_rejects_the_bus_nkey_seed_path` and the `RATATOSKR__SCHEDULE__*` cases; run them and confirm they fail on the unrecognized keys.
- [x] 5.2 GREEN: add the bus and schedule configuration.
- [x] 5.3 RED: add `tests/worker_messages.rs::schedule_occurrence_accepts_a_platform_contract_envelope_and_terminates_its_operation` and the `src/bus.rs` unit test `outbox_rows_wrap_into_contract_envelopes`; run them and confirm they fail on the missing operation id, the missing report and the hand-built envelope.
- [x] 5.4 GREEN: build contract envelopes, classify commands by the explicit subject set, factor `connect(endpoint, Option<&Path>)`, and take the occurrence operation and owner from the envelope.
- [x] 5.5 RED: add `tests/registration.rs::registration_row_is_enqueued_once_per_distinct_configuration` and `registration_is_skipped_without_an_owner`; run them and confirm they fail because no registration is queued.
- [x] 5.6 GREEN: enqueue the registration at worker start with the SHA-256 semantic key, and update `docs/OPERATIONS.md` and `docs/INTERFACES.md`.

## 6. API (CONTRACTS S08)

- [x] 6.1 RED: add the `services/api/tests/api.rs` cases `ready_requires_bearer_only`, `manifest_serves_exact_stored_bytes_and_hashes_to_the_digest_header`, `manifest_with_wrong_run_id_or_wrong_digest_or_foreign_owner_is_404_with_empty_body`, `results_list_is_owner_scoped_newest_first_and_content_free` and `result_and_subscription_bodies_parse_as_contract_views`; run them and confirm each fails on its status or body.
- [x] 6.2 GREEN: serve `/ready`, enforce the claim headers, return the stored text, add `GET /v1/results` and serialize with the contract views; fix `docs/INTERFACES.md`.

## 7. Authorized broker and identity fragment (CONTRACTS S03, S04)

- [x] 7.1 RED: add `services/worker/tests/authorized_bus.rs` against an authorization-enabled broker built from `deploy/nats/identity.conf`; run it and confirm it fails because the fragment does not exist.
- [x] 7.2 GREEN: add the fragment, extract the outbox publish into a function testable without PostgreSQL, and document the seed path in `docs/OPERATIONS.md` and `DEVELOPMENT.md`.

## 8. Documentation and gate

- [x] 8.1 Update `docs/DATA_MODEL.md`, `docs/OPERATIONS.md`, `docs/INTERFACES.md` and `docs/ARCHITECTURE.md` for the keys, nkey, owner prerequisite and report vocabulary. No failing test applies because these files document behavior already exercised above; verify names against the implementation with targeted searches.
- [x] 8.2 Run the exact `DEVELOPMENT.md` gate and `openspec validate --all --strict` and `--archived`, and record the observed results.
