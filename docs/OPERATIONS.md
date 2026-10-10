# Channel Digest Operations

## Authority and evidence boundary

The API serves only loopback service-authenticated, owner-scoped projections. The worker is the only
process that can read the separately mounted session ciphertext and 32-byte key. Local fake-provider
tests prove bounds and recovery logic; they do not prove live Telegram authorization, channel
visibility, production NATS topology, or deployment acceptance.

## Knowledge result-reader authority

Only the API role receives these settings:

- `RATATOSKR__KNOWLEDGE__BASE_URL` — numeric loopback HTTP origin with a nonzero port and no path;
- `RATATOSKR__KNOWLEDGE__RESULT_READER_SERVICE_SECRET` — dedicated non-empty secret, at most 4096
  bytes;
- `RATATOSKR__KNOWLEDGE__CONNECT_TIMEOUT_MS` — `1..10000`;
- `RATATOSKR__KNOWLEDGE__REQUEST_TIMEOUT_MS` — `1..30000` and not shorter than connect timeout;
- `RATATOSKR__KNOWLEDGE__MAX_RESPONSE_BYTES` — `1..65536`.

The worker rejects every one of these keys. `check-config` validates their shape without binding a
listener or contacting Knowledge, and diagnostics redact the secret. The API builds one pooled
client, disables proxies and redirects, and performs one bounded request with no handler retry.

Rotate the dedicated secret in this order: configure Knowledge to accept the new value, stage the
new API secret, run API `check-config`, restart only the API, then probe direct Knowledge scope. Use
an explicit nonexistent UUID; `404` proves the new credential reached the owner-scoped reader while
the same request without authorization must return `401`:

```sh
test -n "${CHANNEL_DIGEST_KNOWLEDGE_RESULT_READER_SECRET:?set rotated reader secret}"
test -n "${RATATOSKR__KNOWLEDGE__BASE_URL:?set validated Knowledge loopback origin}"
curl --silent --output /dev/null --write-out '%{http_code}\n' --max-time 2 \
  --header "Authorization: Bearer ${CHANNEL_DIGEST_KNOWLEDGE_RESULT_READER_SECRET}" \
  "${RATATOSKR__KNOWLEDGE__BASE_URL}/internal/channel-digest-results/00000000-0000-0000-0000-000000000000"
curl --silent --output /dev/null --write-out '%{http_code}\n' --max-time 2 \
  "${RATATOSKR__KNOWLEDGE__BASE_URL}/internal/channel-digest-results/00000000-0000-0000-0000-000000000000"
```

Do not use a real analysis UUID for this probe and do not pass the secret with shell tracing enabled.
Remove the old Knowledge credential only after an authorized Channel Digests result read succeeds.

## Bus credential

The broker authenticates this service by nkey. Generate the CHANNEL_DIGESTS seed as
`ratatoskr-platform/deploy/nats/README.md` describes, install it as the file
`/etc/ratatoskr/channel-digests.nkey` owned by the worker user with mode `0600`, and point the
worker at it with `RATATOSKR__BUS__NKEY_SEED_PATH` (absolute path, worker role only; the API role
rejects the key). The seed is read when the worker connects and is never logged. Leave the key unset
only for an unauthenticated local development broker.

Start order on a fresh broker: reload NATS with the ACL, start Edge so it provisions the fixed
durables, then start the worker; the worker verifies each durable and stays unready until all five
match.

## Daily schedule registration

At start the worker queues one `platform.schedule.registration_requested.v1` command for its
`daily-digest` schedule. The settings are worker-only and strict:

- `RATATOSKR__SCHEDULE__OWNER_USER_ID` - canonical lowercase UUID of the Platform user that owns the
  schedule. Without it nothing is registered and the worker logs the safe class
  `schedule_owner_absent`. This user MUST exist in `identity.users`: Platform has no system
  principal, so a registration for an unknown user is rejected;
- `RATATOSKR__SCHEDULE__CRON` - five-field UTC cron expression, default `0 6 * * *`;
- `RATATOSKR__SCHEDULE__ENABLED` - `true` or `false`, default `true` once an owner is set.

Setting the cron or enabled key without an owner is a configuration error. The outbox semantic key
is the SHA-256 of owner, cron and enabled, so an unchanged configuration is queued once, a changed one
registers again, and returning to an earlier configuration registers it again. Platform upserts on
`(service_name, name)` and the producer must be an allowlisted registrar
(`ALLOWED_REGISTRARS` includes `ratatoskr-channel-digests`).

## Session provisioning and reauthorization

Perform interactive first authorization only in a temporary owner-controlled environment using the
approved Telegram application identity. Export a canonical grammers session JSON containing
`home_dc`, `dc_options`, `peer_infos`, and
`updates_state`; encrypt it with XChaCha20-Poly1305 using the associated data
`ratatoskr-channel-digests-session-v1`. Store the 24-byte nonce before the ciphertext. Mount the
ciphertext and key as separate regular files owned by the service with mode `0600`. Run:

```sh
install -d -m 0700 /run/ratatoskr-channel-digests-rotation
install -m 0600 session.enc /run/ratatoskr-channel-digests-rotation/session.enc
install -m 0600 session.key /run/ratatoskr-channel-digests-rotation/session.key
systemctl show ratatoskr-channel-digests-worker -p User -p Group
sudo -u ratatoskr-channel-digests /opt/ratatoskr/bin/ratatoskr-channel-digests-worker check-config
```

The two `install` commands are staging examples: substitute only explicit operator-owned source
files and the deployment's secret-store destinations. Review `check-config` before atomically
repointing the worker environment. Remove the staging directory after successful rotation; never
copy either artifact into `/mnt/nvme/ratatoskr/channel-digests`, a repository, or a ticket.

On `provider reauthorization required`, disable schedule dispatch, stop the worker, replace both
files atomically under owner control, rerun `check-config`, then start the worker. Never paste a key,
session, phone code, or raw provider error into logs or tickets.

## Recovery and inspection

Read-only inspection of stuck runs and replay state:

```sh
psql "$CHANNEL_DIGEST_DATABASE_URL" -v ON_ERROR_STOP=1 -c \
  "select run_id,state,updated_at,safe_failure_class from channel_digests.digest_runs where state not in ('completed','partial','failed') order by updated_at limit 100"
psql "$CHANNEL_DIGEST_DATABASE_URL" -v ON_ERROR_STOP=1 -c \
  "select subject,state,received_at,completed_at,safe_failure_class from channel_digests.inbox_messages where state <> 'completed' order by received_at limit 100"
psql "$CHANNEL_DIGEST_DATABASE_URL" -v ON_ERROR_STOP=1 -c \
  "select subject,attempts,next_attempt_at,safe_failure_class from channel_digests.outbox_messages where published_at is null order by next_attempt_at limit 100"
psql "$CHANNEL_DIGEST_DATABASE_URL" -v ON_ERROR_STOP=1 -c \
  "select resource_kind,resource_id,expires_at,checkpoint from channel_digests.leases order by expires_at limit 100"
```

Restarting after a page boundary reuses immutable revision keys, the persisted cursor, the run
natural key, and outbox semantic identity. Do not delete inbox/outbox/run rows to force progress.
Disable the Platform-owned schedule first if repeated occurrences would amplify an outage.

## Health and shutdown

The API domain listener answers `GET /ready` behind the service bearer alone (`200` when the database
answers, `503` otherwise); this is the probe Platform and Knowledge use. The API operator listener is
`127.0.0.1:9469`; worker is `127.0.0.1:9470`. `/live` reports process
liveness and `/ready` reports dependency readiness; both send `Cache-Control: no-store`. `SIGTERM`
immediately removes readiness and drains listeners within the configured bound.

Dry-run service and schedule actions before writes:

```sh
systemctl show ratatoskr-channel-digests-api ratatoskr-channel-digests-worker \
  -p FragmentPath -p User -p Group -p ActiveState -p SubState
curl --fail --max-time 2 http://127.0.0.1:9469/live
curl --fail --max-time 2 http://127.0.0.1:9470/live
```

The Platform runbook owns the actual schedule disable write. During an outage, first inspect its
registered digest schedule and pending occurrence count, then disable it through that documented
Platform command before stopping either digest role. Re-enable only after both digest `/ready`
endpoints and Knowledge compatibility are healthy.

## Result-reader rollout and rollback

Roll out Knowledge first, recreate the development Channel Digests schema from `schema.sql`, then
deploy Channel Digests API with the five reader settings. Do not give them to the worker. Exercise
completed, partial, failed, foreign, missing, and unavailable result reads before enabling the
Platform consumer; deploy Platform last.

Roll back in reverse: disable or roll back Platform consumption, roll back Channel Digests API, and
only then remove or rotate away the Knowledge reader. A transient Knowledge failure is request-local:
the result route returns an empty `503`, while unrelated API `/ready` remains healthy. Invalid or
integrity-inconsistent upstream data returns an empty `502`. There is no recap cleanup or conversion
step because Channel Digests never stores recap narrative and development databases are recreated
from the one current schema.
