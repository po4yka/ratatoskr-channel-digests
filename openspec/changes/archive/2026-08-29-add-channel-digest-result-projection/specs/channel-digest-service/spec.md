## ADDED Requirements

### Requirement: Completed digest results project Knowledge-owned recaps without copying them

An owner-authorized result read SHALL resolve the local terminal result before contacting Knowledge.
Completed and partial results SHALL be returned only after the Knowledge analysis identity and exact
SHA-256 result digest match the immutable linkage accepted from the completion fact. The successful
projection SHALL contain an explicit local result envelope and the closed recap returned by
Knowledge, SHALL remain bounded and non-cacheable, and SHALL not cause recap narrative to be stored
by Channel Digests. Failed results SHALL return only their safe local failure projection and SHALL
not contact Knowledge. Missing or foreign local results SHALL remain indistinguishable. An absent,
unauthorized, malformed, oversized, unavailable, or integrity-inconsistent Knowledge response SHALL
fail closed with a stable content-free error and no partial recap.

#### Scenario: Completed result is projected through verified Knowledge linkage

- **WHEN** an authorized owner reads a completed or partial result whose Knowledge analysis identity and result digest match the durable completion fact
- **THEN** the service returns the explicit local result envelope and exact Knowledge-owned recap with `Cache-Control: no-store` without persisting recap narrative locally

#### Scenario: Foreign result is rejected before Knowledge access

- **WHEN** an authenticated service reads an existing result under another owner
- **THEN** it receives the same scoped `404` as a nonexistent result and Knowledge receives no request

#### Scenario: Failed result remains a safe local projection

- **WHEN** an authorized owner reads a terminal failed result
- **THEN** the service returns only the result identity, run identity, failed outcome, and safe failure class without contacting Knowledge or exposing recap fields

#### Scenario: Knowledge projection is unusable

- **WHEN** Knowledge is unavailable or returns an absent, unauthorized, malformed, oversized, foreign, or digest-inconsistent projection
- **THEN** the result read returns a stable `502` or `503` class with no recap bytes, upstream diagnostics, secret material, or partial success
