# Interfaces

## Loopback HTTP

The API binds only to `127.0.0.1:8098`. Every `/v1` request requires
`Authorization: Bearer <service secret>` and `X-Ratatoskr-Owner-Id: <internal user UUID>`. Foreign
and missing resources both return `404`; responses use `Cache-Control: no-store`.

Owned routes are:

- `GET /v1/subscriptions?limit=<1..100>`;
- `GET /v1/manifests/{manifest_id}`;
- `GET /v1/results/{result_id}`.

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

All are durable pull consumers with explicit acknowledgements and deliver-all replay. The worker
publishes `cmd.knowledge.channel_digest_recap.requested.v1` and
`evt.platform.operation.reported.v1` from its transactional outbox with `Nats-Msg-Id` equal to the
durable outbox identity. Bus payloads contain references, counts, and safe classes, never post or
session bodies.

## Operator plane

Both roles expose `GET /live` and `GET /ready`. Worker readiness requires both exact JetStream
topology and an authorized provider session; the API requires its database and listener. A drain
turns readiness off before work is joined.
