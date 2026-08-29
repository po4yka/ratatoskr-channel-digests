# Channel Digest Operations

## Authority and evidence boundary

The API serves only loopback service-authenticated, owner-scoped projections. The worker is the only
process that can read the separately mounted session ciphertext and 32-byte key. Local fake-provider
tests prove bounds and recovery logic; they do not prove live Telegram authorization, channel
visibility, production NATS topology, or deployment acceptance.

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

The API operator listener is `127.0.0.1:9469`; worker is `127.0.0.1:9470`. `/live` reports process
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
