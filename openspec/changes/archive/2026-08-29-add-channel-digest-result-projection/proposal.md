## Why

The result route currently exposes only Channel Digests linkage metadata, so an authorized client cannot render a completed recap without either copying Knowledge-owned narrative into this service or calling a private Knowledge API directly. Knowledge now exposes a bounded authenticated result reader, which makes a read-through projection possible while preserving ownership and end-to-end result integrity.

## What Changes

- Persist the expected Knowledge result digest beside the existing analysis/result linkage when a completion fact settles a digest run; keep recap narrative out of the Channel Digests database.
- Add API-role-only configuration and a bounded authenticated client for the loopback Knowledge result-reader endpoint.
- **BREAKING**: enrich successful `GET /v1/results/{result_id}` responses with the exact typed recap returned by Knowledge after owner, linkage, and digest verification; failed results remain minimized safe-failure projections.
- Fail closed without partial recap content when Knowledge is unavailable or returns a missing, malformed, foreign, or integrity-inconsistent result.
- Document the service-secret boundary, limits, rollout order, rollback order, and observable failure classes.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `channel-digest-service`: require completed and partial result reads to project Knowledge-owned recap data through an authenticated, bounded, integrity-checked read-through path without persisting the recap locally.

## Impact

- Affects the current PostgreSQL schema, completion-fact coordinator, API configuration/runtime, result route, telemetry, fake-server tests, and operator documentation.
- Adds `reqwest` as a pinned production dependency with Rustls and streaming enabled and default features disabled, providing a maintained pooled HTTP client with redirects disabled, finite timeouts, and response-size enforcement. Its Apache-2.0/MIT licensing and TLS/HTTP maintenance surface must be accepted before implementation.
- Depends on the Knowledge result-reader API already released on Knowledge `main`; Platform remains the sole public API consumer and will consume the enriched Channel Digests projection in a later repository change.
- Rollout order is Knowledge, Channel Digests, then Platform. Rollback proceeds in reverse and does not require data conversion because development status permits editing the one current schema in place.
