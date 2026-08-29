## Context

See `proposal.md` for motivation. Channel Digests currently persists a terminal result with the
Knowledge `analysis_ref` UUID but drops the completion fact's result digest. Its owner-scoped result
route therefore returns linkage metadata only. Knowledge now offers
`GET /internal/channel-digest-results/{analysis_id}` on loopback with a dedicated bearer secret and
returns bounded `{analysis_id, result_digest, recap}` JSON after revalidating its stored digest and
closed recap type.

The API and worker are separate role configurations. Only the API serves result reads; the worker
must not be able to represent Knowledge result-reader authority. Development status requires an
in-place edit of the one current schema and forbids migrations or compatibility routes.

## Goals / Non-Goals

**Goals:**

- Preserve Knowledge as the sole owner of recap narrative while making one authorized result read
  sufficient for Platform.
- Bind every successful recap projection to the owner-scoped local result, Knowledge analysis UUID,
  and exact result SHA-256 accepted from the completion fact.
- Bound dependency latency, response bytes, redirects, parsing, diagnostics, and shutdown behavior.
- Keep local failures and upstream failures stable, private, and mechanically testable.

**Non-Goals:**

- Persisting, indexing, mutating, caching, or semantically revalidating Knowledge recap narrative.
- Exposing the Knowledge service directly to Platform or Telegram.
- Public API design, Bot delivery, notification preferences, digest schedule ownership, channel
  history outside the existing explicitly consented provider boundary, or LLM/model controls.
- A shared generic analysis reader or a compatibility form of the old metadata-only response.

## Decisions

### Authorize and resolve the local terminal result before any dependency call

The route continues to authenticate the calling service and owner, then performs one owner-scoped
lookup by local `result_id`. Foreign and missing rows return the existing uniform `404`; no Knowledge
request occurs. A failed local result returns its minimized local projection immediately. Only a
completed or partial row with both `recap_id` and `result_digest_hex` may enter read-through.

This ordering prevents the upstream route from becoming an identity oracle and avoids spending
dependency capacity on unauthorized requests. Calling Knowledge first or accepting an analysis UUID
from the caller was rejected because either would bypass Channel Digests ownership linkage.

### Persist the expected result digest, never the recap

`channel_digests.digest_results` gains a nullable, 64-hex-character `result_digest_hex`; its outcome
constraint requires `recap_id` and the digest for completed/partial rows and requires both to be null
for failed rows. Completion settlement stores `analysis_ref` and `result_digest.hex` atomically with
the terminal result. Replay equivalence includes owner, run, manifest digest, analysis UUID, result
digest, outcome, and citation count, so a contradictory fact fails instead of being called a replay.

The current `schema.sql` is edited in place and test databases are recreated. Storing the recap JSON,
adding a migration, or deriving integrity only from the mutable HTTP response were rejected: all
three would violate either ownership, development status, or durable linkage.

### Use one API-only bounded Knowledge reader

API configuration requires a loopback HTTP base URL and a dedicated redacted result-reader secret.
It also carries finite connect and end-to-end request timeouts and a response cap no greater than the
Knowledge route's 64 KiB contract. Unknown, empty, oversized, non-loopback, zero, and out-of-range
values fail startup without echoing their values. The worker rejects every reader-specific key and
its effective configuration has no reader field.

One pooled client is created during API composition. It disables redirects, sends only
`Authorization: Bearer <reader secret>` to the configured loopback origin, accepts JSON, streams the
body under the byte cap, and performs one request per result read. Handler-level retries are omitted:
the GET is idempotent, but automatic retry would extend user latency and amplify an unhealthy local
dependency; Platform can retry a `503` under its own operation budget.

The implementation adds pinned `reqwest = 0.12.28` with default features disabled and only
`rustls-tls` and `stream`. This reuses a maintained pooled HTTP/TLS stack already used in the
workspace. Its Apache-2.0/MIT license and transitive security surface are covered by the existing
locked dependency and `cargo deny` gates. A hand-written HTTP client was rejected because redirect,
framing, timeout, connection reuse, and TLS behavior would become bespoke security code.

### Decode a strict ownership envelope and preserve Knowledge's recap object

The reader accepts exactly `analysis_id`, a SHA-256 `result_digest`, and `recap`. The outer response
is closed to unknown fields; the digest algorithm must be `sha256`, the hex value must be canonical,
and `analysis_id` plus digest must match the local row. `recap` remains a JSON object owned and
validated by Knowledge. Channel Digests does not duplicate Knowledge's recap schema or recompute a
digest from reserialized JSON; Knowledge has already validated the closed type and canonical bytes,
while the completion digest binds this response to the terminal fact.

The successful Channel Digests DTO contains local `result_id`, `run_id`, `outcome`, `recap_id`,
`citation_count`, the exact result digest, and the nested recap. It excludes provider attempts,
manifest bodies, post content outside the recap, storage metadata, and raw upstream fields.

Publishing the recap type from Contracts was considered and rejected for this change: recap
narrative is Knowledge-owned and the cross-service integrity contract already consists of typed
completion linkage plus digest. A shared recap schema can be proposed separately if a future
consumer must construct or validate recap documents rather than display this projection.

### Fail closed with a finite status matrix

Local database unavailability, transport errors, timeouts, and upstream `5xx` map to `503 Service
Unavailable`. Upstream `401`/`403`/`404`, redirects, oversized bodies, non-JSON or non-closed
envelopes, invalid digest syntax, and identity/digest mismatch map to `502 Bad Gateway`. No upstream
body, URL, secret, analysis identifier, recap fragment, or parser error enters the client response or
ordinary logs. Every response remains `Cache-Control: no-store`.

Metrics and tracing use finite outcome classes such as `success`, `local_absent`, `local_failed`,
`upstream_unavailable`, and `upstream_invalid`; owner IDs, result IDs, analysis IDs, recap text, URLs,
and secrets are not metric labels. The API's readiness continues to describe its listener and
database. A transient read dependency failure is reported on the result request as `503` rather than
making unrelated subscription/manifest routes unready.

## Risks / Trade-offs

- [Knowledge availability now affects successful result reads] → keep one short bounded request,
  return retryable `503`, and do not duplicate or cache narrative.
- [A response remains syntactically valid but belongs to another completion] → compare both the
  analysis UUID and durable SHA-256 before emitting any recap bytes.
- [The completion digest was not persisted by older development databases] → recreate the
  development database from the edited current schema; no compatibility or backfill path is added.
- [Credential rotation creates a brief mismatch] → deploy Knowledge first with the new shared
  value, then Channel Digests, and use the documented scoped-absence probe before removing the old
  secret source.
- [Opaque recap JSON can drift semantically] → Knowledge remains the only decoder and rejects
  invalid stored recap data; Channel Digests verifies the immutable envelope and digest rather than
  establishing a second narrative contract.

## Migration Plan

1. Confirm the Knowledge result route is deployed on loopback and a random UUID returns scoped
   `404` with the dedicated credential while missing/wrong credentials return `401`.
2. Recreate the Channel Digests development database from the edited current schema and deploy the
   API with its new loopback base URL, secret, and finite limits. The worker receives none of these
   settings.
3. Exercise a completed, partial, failed, foreign, missing, and dependency-unavailable result through
   the Channel Digests loopback API before enabling the Platform consumer.
4. Deploy Platform consumption and Telegram rendering only after Channel Digests is healthy.
5. Roll back Platform first, then Channel Digests. Disable or rotate the Knowledge reader last. No
   recap bytes require cleanup because none are persisted outside Knowledge.
