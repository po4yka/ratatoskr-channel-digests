## 1. API-only result-reader authority

- [x] 1.1 RED: extend `tests/config.rs::knowledge_result_reader_is_api_only_redacted_and_bounded` to require the loopback Knowledge base URL, a non-empty at-most-4096-byte dedicated secret, finite connect/request/response limits, redacted diagnostics, and rejection of every reader key by the worker role; run the exact test through `build-gate` and confirm it fails because the API reader configuration is absent.
- [x] 1.2 GREEN: implement strict API-only result-reader configuration and accessors, rerun the exact config test through `build-gate`, and confirm the API can represent the bounded authority while the worker cannot.

## 2. Durable completion linkage

- [x] 2.1 RED: extend `tests/schema.rs::owned_schema_is_complete_idempotent_and_isolated` to assert the current `digest_results` shape requires canonical result-digest linkage for completed/partial rows, forbids it for failed rows, and contains no recap narrative column; run the exact test through `build-gate` and confirm it fails on the current schema.
- [x] 2.2 GREEN: edit `schema.sql` in place to add the constrained result-digest linkage without a migration, recreate/apply the test schema, rerun the exact schema test through `build-gate`, and confirm the constraints pass idempotently.
- [x] 2.3 RED: extend `tests/recap_results.rs::only_consistent_terminal_facts_settle_a_run` to assert the exact Knowledge analysis UUID and result SHA-256 are persisted atomically and that a replay with changed analysis, result digest, outcome, or citation count is rejected; run the exact test through `build-gate` and confirm it fails because the digest and full replay identity are not durable.
- [x] 2.4 GREEN: update completion settlement and replay equivalence to persist and compare the complete immutable linkage, rerun both terminal-fact tests through `build-gate`, and confirm valid replay is idempotent while contradictions do not settle or regress state.

## 3. Bounded Knowledge reader

- [x] 3.1 RED: add `tests/result_projection.rs::knowledge_reader_is_authenticated_bounded_and_integrity_checked` with a deterministic fake HTTP server covering the exact analysis path, dedicated bearer credential, disabled redirects, one finite attempt, request deadline, 64 KiB response cap, closed outer envelope, SHA-256 syntax, identity/digest mismatch, and secret/body-safe diagnostics; run the exact test through `build-gate` and confirm it fails because no reader exists.
- [x] 3.2 GREEN: add pinned `reqwest = 0.12.28` with default features disabled and `rustls-tls` plus `stream`, implement the pooled reader and finite error taxonomy, update the lockfile, rerun the exact reader test through `build-gate`, and run `cargo deny --locked check` through `build-gate` to verify licenses, advisories, bans, and sources.

## 4. Owner-scoped result projection

- [x] 4.1 RED: extend `services/api/tests/api.rs::routes_require_service_and_owner_scope` with seeded completed, partial, failed, missing, and foreign results plus a recording fake Knowledge server; assert local owner resolution precedes any upstream call, success returns the explicit linkage/digest/recap DTO with `Cache-Control: no-store`, failed results remain local and minimized, and recap text is absent from Channel Digests storage; run the exact API test through `build-gate` and confirm the current metadata-only route fails the success assertions.
- [x] 4.2 GREEN: compose the reader into `ApiState`, replace the metadata-only successful result response with the verified read-through projection, preserve failed/foreign behavior, rerun the exact API acceptance test through `build-gate`, and inspect SQL/query DTOs to confirm no recap field is written locally.
- [x] 4.3 RED: extend the same API acceptance test with upstream `401`/`403`/`404`, redirect, `5xx`, disconnect, timeout, oversized body, malformed JSON, unknown envelope fields, invalid digest, and identity/digest mismatch; assert the finite `502`/`503` matrix, uniform content-free failures, no recap fragments, and no leaked credential or upstream diagnostic, then run it through `build-gate` and confirm at least one unsupported class fails for the stated reason.
- [x] 4.4 GREEN: complete the route error mapping and safe finite telemetry classes, rerun the full API result matrix through `build-gate`, and confirm every unusable upstream response fails closed without partial success or sensitive metric labels.

## 5. Process composition and lifecycle

- [x] 5.1 RED: extend `services/api/tests/boot.rs::api_and_worker_roles_are_separate_ready_and_bounded` to start the real API binary with a fake Knowledge endpoint, read a seeded result across a restart, observe dependency unavailability as request-scoped `503` without making unrelated API readiness false, and terminate within the configured drain bound; run the exact test through `build-gate` and confirm the old process fixture fails because it cannot represent the now-required production reader authority.
- [x] 5.2 GREEN: build one reader during API startup, pass it into the result router, preserve listener/database readiness semantics and bounded drain, rerun the exact boot test through `build-gate`, and confirm `check-config` rejects missing/invalid reader authority before binding listeners.

## 6. Documentation, validation, and archive

- [x] 6.1 Update `README.md`, `docs/INTERFACES.md`, `docs/DATA_MODEL.md`, `docs/OPERATIONS.md`, and `docs/THREAT_MODEL.md` with the result DTO, secret direction and rotation probe, finite status matrix, schema ownership, rollout/rollback order, and privacy boundary; no failing behavior test applies because these files document behavior already exercised above, so verify every named environment key, route, status, and rollout step against the implementation with targeted `rg` inspection.
- [x] 6.2 Run the exact `DEVELOPMENT.md` full gate with disposable PostgreSQL and NATS through `build-gate`, including locked dependency/security checks, formatting, clippy, all tests, doctests, release build, source-size/privacy audits, and strict OpenSpec validation; record the observed command and result without treating fake Knowledge transport as live deployment proof.
- [x] 6.3 Sync the validated delta into `openspec/specs/channel-digest-service/spec.md`, archive `add-channel-digest-result-projection`, run `openspec validate --specs` and `openspec validate --archived --strict`, and confirm the archived checklist is fully checked before delivery.
