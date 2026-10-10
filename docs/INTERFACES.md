# Interfaces

## Loopback HTTP

The API binds only to `127.0.0.1:8098`. Every `/v1` request requires
`Authorization: Bearer <service secret>` and `X-Ratatoskr-Owner-Id: <bare internal user UUID>`; a
missing or wrong bearer, or an owner header that is not a canonical lowercase UUID, returns `401`.
Foreign and missing resources both return `404`; responses use `Cache-Control: no-store`. The header
names and the manifest path are constants of `ratatoskr_channel_digest_contracts` (XR-021
CONTRACTS.md S08), and the subscription and result bodies are its view types.

Routes are:

- `GET /ready` requires the bearer only, with no owner header, and answers `200` while the database
  answers `select 1` and `503` otherwise. It sits outside `/v1` and is how Platform and Knowledge
  probe this service. `/live` stays `404` on this listener; liveness is on the operator plane;
- `GET /v1/subscriptions?page_size=<1..100>` (default 50), newest first by first activation;
- `GET /v1/results?page_size=<1..100>` (default 50), newest first, as content-free summaries
  (`result_id`, `run_id`, `outcome`, `safe_failure_class`, `created_at`);
- `GET /v1/results/{result_id}`;
- `GET /v1/manifests/{manifest_id}`.

`GET /v1/manifests/{manifest_id}` also requires the claims `X-Ratatoskr-Digest-Run-Id` (bare run
UUID) and `X-Ratatoskr-Manifest-Digest` (64 lowercase hex). It answers `200` with
`Content-Type: application/json` and a body that is exactly the stored canonical manifest text,
never re-serialized, so the SHA-256 of the body is the digest header. It answers `404` with an empty
body for an absent manifest, another owner's manifest, a missing or malformed claim, a run id that is
not the manifest's run, and a digest that is not the stored one; these cases are indistinguishable.

For a completed or partial result, `GET /v1/results/{result_id}` returns the local linkage and the
Knowledge-owned recap only after the upstream analysis UUID and SHA-256 match the stored completion
fact:

```json
{
  "result_id": "<uuid>",
  "run_id": "<uuid>",
  "outcome": "completed",
  "recap_id": "<knowledge-analysis-uuid>",
  "citation_count": 2,
  "result_digest": { "algorithm": "sha256", "hex": "<64 lowercase hex>" },
  "recap": { "title": "...", "summary": "...", "citations": [] }
}
```

The nested recap is an opaque Knowledge-owned JSON object. A failed result is local and minimized to
`result_id`, `run_id`, `outcome`, and `safe_failure_class`; it does not contact Knowledge. Missing or
foreign local results return `404` before any dependency request. Knowledge transport failures,
timeouts, and `5xx` return content-free `503`; `401`, `403`, `404`, redirects, oversized or malformed
JSON, unknown envelope fields, and identity/digest mismatch return content-free `502`. No result
response is cacheable.

The API calls exactly
`GET /internal/channel-digest-results/{analysis_id}` at the configured numeric loopback HTTP origin,
with its dedicated bearer secret. It follows no redirects, performs no automatic retry, and accepts
at most 65,536 response bytes within the configured connect and request deadlines.

Platform publishes subscription and run mutations through Contracts instead of bypassing the
durable command path. Knowledge retrieves an owner-bound immutable manifest through the same
service-authenticated boundary.

## JetStream

The worker opens but never creates these fleet-owned pull consumers:

| Stream | Durable | Exact filter |
| --- | --- | --- |
| `ratatoskr_commands` | `ratatoskr_channel_digest_subscriptions` | `cmd.channel_digest.subscription.set_requested.v1` |
| `ratatoskr_commands` | `ratatoskr_channel_digest_runs` | `cmd.channel_digest.run.requested.v1` |
| `ratatoskr_commands` | `ratatoskr_channel_digest_schedule_occurrences` | `cmd.channel_digest.schedule.occurrence_requested.v1` |
| `ratatoskr_events` | `ratatoskr_channel_digest_recap_completed` | `evt.knowledge.channel_digest_recap.completed.v1` |
| `ratatoskr_events` | `ratatoskr_channel_digest_recap_failed` | `evt.knowledge.channel_digest_recap.failed.v1` |

All are durable pull consumers with explicit acknowledgements, a 30 second acknowledgement wait and
deliver-all replay; the worker verifies filter, acknowledgement policy and wait at startup and stays
unready on any difference. The worker authenticates with the nkey seed file named by
`RATATOSKR__BUS__NKEY_SEED_PATH` (`/etc/ratatoskr/channel-digests.nkey`) when the broker requires
one; `deploy/nats/identity.conf` is this identity's stanza of the deployed ACL.

From its transactional outbox the worker publishes, as contract envelopes with `Nats-Msg-Id` equal to
the durable outbox identity:

- `cmd.knowledge.channel_digest_recap.requested.v1`, a `CommandEnvelope` correlated to the
  operation;
- `evt.platform.operation.reported.v1`, an `EventEnvelope` carrying an `OperationReported` with
  correlation `operation:<uuid>` and tenant `user:<owner>`;
- `cmd.platform.schedule.registration_requested.v1`, the one command that registers this service's
  `daily-digest` schedule with Platform. Its producer equals `payload.service_name`.

A publish the broker does not acknowledge leaves the row unpublished, counts the attempt and backs
the row off, so one refused row does not starve the others. A refused publish and a denied one look
the same to the client: check the NATS server log for a `Publish Violation`.

Bus payloads contain references, counts, and safe classes, never post or session bodies.

### Operation reports

Every report is a validated `OperationReported`, queued in the transaction that changes the state it
reports, once per operation and status (outbox semantic key `operation:<id>:<status>`).

| Point | Status | Stage | Notes |
| --- | --- | --- | --- |
| subscription applied | `succeeded` | `applied` | |
| 21st active subscription | `failed` | `rejected` | `channel_digest.subscription_limit_reached`, not retryable; the inbox row is failed and the message is acknowledged |
| on-demand run accepted | `running` | `acquiring` | |
| run selects no source | `succeeded` | `no_sources` | no results, same transaction as the run's `completed` state |
| Knowledge completion | `succeeded` or `partially_succeeded` | `completed` | one `channel_digest.result` reference; omitted sources add the warning `channel_digest.context_omitted` |
| Knowledge failure | `failed` | `recap` | `channel_digest.recap.<safe_failure_class>`, retryable for `provider_unavailable`, `provider_timeout`, `manifest_unavailable` |
| every channel unavailable | `failed` | `acquiring` | `channel_digest.provider_unavailable`, retryable, same transaction as the run's `failed` state |
| manifest bound or validity | `failed` | `manifest` | `channel_digest.manifest_invalid`, not retryable, same transaction as the run's `failed` state |
| schedule occurrence fanned out | `succeeded` | `fanned_out` | on the occurrence operation, which the fanned-out runs reuse for correlation |

Scheduled digest runs are owned by no Platform operation and report nothing. A schedule occurrence
takes its operation from the envelope `correlation_id` and its owner from the envelope `tenant_id`;
an occurrence missing either is terminated.

## Operator plane

Both roles expose `GET /live` and `GET /ready`. Worker readiness requires both exact JetStream
topology and an authorized provider session; the API requires its database and listener. A drain
turns readiness off before work is joined.
