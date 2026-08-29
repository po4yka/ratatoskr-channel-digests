## 1. Foundation and roles

- [ ] 1.1 RED: add `tests/config.rs::configuration_is_strict_finite_and_role_scoped` for database/API/operator/provider/source/body/retry/shutdown bounds, ports `8098`/`9469`/`9470`, unknown-key refusal, API credential absence, and redacted invalid values; run it against the planning-only tree and confirm the target is absent.
- [ ] 1.2 GREEN: add the minimal Rust workspace, shared crate, API/worker binaries, exact vetted dependencies, strict lints/toolchain, and configuration loader; rerun the focused test through `build-gate`.
- [ ] 1.3 RED: add `tests/boot.rs::api_and_worker_roles_are_separate_ready_and_bounded` for `/live`, dependency-aware `/ready`, no-store, `check-config` no-bind, signal drain, and API provider-credential absence; run it and confirm role lifecycle is absent.
- [ ] 1.4 GREEN: implement joined role composition, operator listeners, readiness, telemetry, and bounded shutdown; rerun the boot test.
- [ ] 1.5 Add CI and an exact matching `DEVELOPMENT.md` gate list; no failing behavior test applies to workflow metadata, so validate YAML, command drift, locked metadata, and dependency policy.

## 2. Current schema and domain state

- [ ] 2.1 RED: add `tests/schema.rs::owned_schema_is_complete_idempotent_and_isolated` using real PostgreSQL for provider status, channels, subscriptions, revisions, runs/windows, manifests/results, inbox/outbox, and leases; run it and confirm relations are absent.
- [ ] 2.2 GREEN: add idempotent current `schema.sql`, finite SQLx pool, and disposable database harness with no migration tooling; rerun schema and second-apply tests.
- [ ] 2.3 RED: add `tests/subscriptions.rs::subscriptions_are_owner_scoped_idempotent_and_limited` for lowercase uniqueness, first activation, limit 20, enable/disable replay, foreign absence, and retained evidence; run it and confirm the repository is absent.
- [ ] 2.4 GREEN: implement typed subscription persistence/transitions and rerun the focused PostgreSQL tests.
- [ ] 2.5 RED: add `tests/revisions.rs::provider_edits_append_immutable_revisions_without_leakage` for timestamps/links, duplicates, edits, digests, and content-free telemetry/outbox; run it and confirm revision capture is absent.
- [ ] 2.6 GREEN: implement immutable revision persistence and safe observability; rerun revision tests.
- [ ] 2.7 RED: add `tests/runs.rs::windows_replay_leases_and_terminal_state_are_deterministic` for on-demand/scheduled/floor/cap windows, natural keys, empty/partial truth, terminal non-regression, leases, and restart; run it and confirm an invariant fails.
- [ ] 2.8 GREEN: implement deterministic windows, leases, and expected-state transitions; rerun the matrix.

## 3. MTProto boundary and acquisition

- [ ] 3.1 RED: add `tests/session.rs::session_files_are_separate_worker_only_and_fail_closed` for file separation, permission/absence/corruption/reauth refusal, API denial, and safe diagnostics; run it and confirm the boundary is absent.
- [ ] 3.2 GREEN: implement the worker-only session/key-file boundary and provider readiness; rerun security tests.
- [ ] 3.3 RED: add `tests/provider_policy.rs::only_public_usernames_can_reach_resolution` covering private/group/dialog/invite/message/numeric locators and the absence of join/leave operations; run it against the fake provider and confirm no adapter exists.
- [ ] 3.4 GREEN: implement the narrow provider trait and production MTProto adapter without leaking SDK types; rerun policy tests.
- [ ] 3.5 RED: add `tests/acquisition.rs::pagination_edits_partial_failures_and_restart_are_bounded` for pagination, edits, duplicates, deletion, partial channels, flood waits, timeout, reconnect, calls, checkpoints, and restart; run it and confirm recovery is absent.
- [ ] 3.6 GREEN: implement bounded acquisition and persisted retry/checkpoint handling; rerun the fake-provider matrix.

## 4. Manifest, API, and command boundary

- [ ] 4.1 RED: add `tests/manifest.rs::canonical_manifest_is_stable_bounded_and_integral` for order, 100/20 limits, closed-open windows, digests, links, omissions, and byte equality; run it and confirm no builder exists.
- [ ] 4.2 GREEN: implement canonical immutable manifest construction/storage; rerun focused tests.
- [ ] 4.3 RED: add `tests/api.rs::routes_require_service_and_owner_scope` for subscription/result/manifest reads, pagination/body limits, foreign absence, redirect-free links, and minimized DTOs; run it and confirm routes are absent.
- [ ] 4.4 GREEN: implement loopback authenticated routes over owned repositories; rerun authorization tests.
- [ ] 4.5 RED: add `tests/intake.rs::typed_commands_are_deduplicated_and_atomic` for Contracts validation, inbox dedupe, owner/idempotency, 20-subscription refusal, and operation outbox atomicity; run it and confirm duplicate effects occur.
- [ ] 4.6 GREEN: implement command consumption and transactional inbox/domain/outbox behavior; rerun replay tests.

## 5. Knowledge exchange, schedule, and privacy

- [ ] 5.1 RED: add `tests/recap_exchange.rs::non_empty_manifest_publishes_one_body_free_recap_request` including empty bypass and restart/redelivery; run it and confirm sequencing is absent.
- [ ] 5.2 GREEN: implement recap-request outbox and durable waiting state; rerun worker/replay tests.
- [ ] 5.3 RED: add `tests/recap_results.rs::only_consistent_terminal_facts_settle_a_run` for owner/run/manifest/count/citation validation, complete/partial/failed outcomes, ordering, duplicates, and unavailable Knowledge; run it and confirm an invalid fact settles.
- [ ] 5.4 GREEN: implement typed result verification, immutable linkage, safe failure, and monotonic completion; rerun the result matrix.
- [ ] 5.5 RED: add `tests/schedule.rs::occurrences_fan_out_once_through_the_run_engine` for prior occurrence, inactive/empty skip, provider wait/restart, and no delivery event; run it and confirm scheduling is nondeterministic.
- [ ] 5.6 GREEN: implement occurrence fan-out over active owners through the durable run engine; rerun schedule/recovery tests.
- [ ] 5.7 RED: add `tests/privacy.rs::retention_minimizes_payloads_without_losing_provenance` for owned body storage, session exclusion, expired payload minimization, and retained run/revision evidence; run it and confirm leakage or unsafe deletion.
- [ ] 5.8 GREEN: implement retention/minimization and content-free telemetry; rerun privacy tests.

## 6. Documentation and gate

- [ ] 6.1 Document interfaces, data model, threat model, provider authorization/reauthorization, flood-wait/restart recovery, stuck inbox/outbox/run inspection, schedule disable, and evidence boundaries; no failing unit test applies to prose, so dry-run every read-only command against fixtures.
- [ ] 6.2 Run the exact `DEVELOPMENT.md` gate through `build-gate` where compiler-backed, real-PostgreSQL and fake-provider suites, strict OpenSpec validation, source-size and secret/content audits, and `git diff --check`; record only observed results.
- [ ] 6.3 Sync and archive `add-channel-digest-service` only after every task is checked; validate main specs and archived changes before publication.
