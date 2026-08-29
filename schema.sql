create schema if not exists channel_digests;

create table if not exists channel_digests.provider_status (
    singleton boolean primary key default true check (singleton),
    state text not null check (state in ('ready', 'unavailable', 'reauth_required')),
    safe_reason text,
    retry_after timestamptz,
    updated_at timestamptz not null default now()
);

create table if not exists channel_digests.channels (
    channel_id uuid primary key,
    username text not null unique check (username = lower(username)),
    provider_peer_id bigint unique,
    display_name text,
    resolution_state text not null default 'pending'
        check (resolution_state in ('pending', 'resolved', 'unavailable')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists channel_digests.subscriptions (
    subscription_id uuid primary key,
    owner_id uuid not null,
    channel_id uuid not null references channel_digests.channels(channel_id),
    first_activated_at timestamptz not null,
    enabled boolean not null default true,
    updated_at timestamptz not null default now(),
    unique (owner_id, channel_id)
);
create index if not exists subscriptions_owner_active_idx
    on channel_digests.subscriptions (owner_id, enabled, first_activated_at);

create or replace function channel_digests.set_subscription(
    requested_subscription_id uuid,
    requested_channel_id uuid,
    requested_owner_id uuid,
    requested_username text,
    requested_enabled boolean,
    requested_at timestamptz
) returns table(subscription_id uuid, first_activated_at timestamptz, enabled boolean)
language plpgsql
as $$
declare
    canonical_username text := lower(requested_username);
    selected_channel_id uuid;
    existing_subscription channel_digests.subscriptions%rowtype;
begin
    perform pg_advisory_xact_lock(hashtextextended(requested_owner_id::text, 0));
    insert into channel_digests.channels (channel_id, username)
    values (requested_channel_id, canonical_username)
    on conflict (username) do nothing;
    select c.channel_id into strict selected_channel_id
    from channel_digests.channels c where c.username = canonical_username;
    select s.* into existing_subscription
    from channel_digests.subscriptions s
    where s.owner_id = requested_owner_id and s.channel_id = selected_channel_id;
    if found then
        if requested_enabled and not existing_subscription.enabled and (
            select count(*) from channel_digests.subscriptions s
            where s.owner_id = requested_owner_id and s.enabled
        ) >= 20 then
            raise exception using errcode = 'P0001', message = 'active subscription limit reached';
        end if;
        update channel_digests.subscriptions s
        set enabled = requested_enabled, updated_at = requested_at
        where s.subscription_id = existing_subscription.subscription_id;
        return query select existing_subscription.subscription_id,
            existing_subscription.first_activated_at, requested_enabled;
        return;
    end if;
    if requested_enabled and (
        select count(*) from channel_digests.subscriptions s
        where s.owner_id = requested_owner_id and s.enabled
    ) >= 20 then
        raise exception using errcode = 'P0001', message = 'active subscription limit reached';
    end if;
    insert into channel_digests.subscriptions (
        subscription_id, owner_id, channel_id, first_activated_at, enabled, updated_at
    ) values (
        requested_subscription_id, requested_owner_id, selected_channel_id,
        requested_at, requested_enabled, requested_at
    );
    return query select requested_subscription_id, requested_at, requested_enabled;
end;
$$;

create table if not exists channel_digests.post_revisions (
    revision_id uuid primary key,
    channel_id uuid not null references channel_digests.channels(channel_id),
    provider_message_id bigint not null,
    content_sha256 text not null check (length(content_sha256) = 64),
    body text,
    published_at timestamptz not null,
    observed_at timestamptz not null,
    canonical_link text not null,
    deleted_at timestamptz,
    minimized_at timestamptz,
    unique (channel_id, provider_message_id, content_sha256)
);
create index if not exists post_revisions_window_idx
    on channel_digests.post_revisions (channel_id, published_at, provider_message_id);

create or replace function channel_digests.append_revision(
    requested_revision_id uuid,
    requested_channel_id uuid,
    requested_message_id bigint,
    requested_digest text,
    requested_body text,
    requested_link text,
    requested_published_at timestamptz,
    requested_observed_at timestamptz
) returns uuid
language sql
as $$
    with inserted as (
        insert into channel_digests.post_revisions (
            revision_id, channel_id, provider_message_id, content_sha256, body,
            canonical_link, published_at, observed_at
        ) values (
            requested_revision_id, requested_channel_id, requested_message_id,
            requested_digest, requested_body, requested_link,
            requested_published_at, requested_observed_at
        )
        on conflict (channel_id, provider_message_id, content_sha256) do nothing
        returning revision_id
    )
    select revision_id from inserted
    union all
    select r.revision_id from channel_digests.post_revisions r
    where r.channel_id = requested_channel_id
      and r.provider_message_id = requested_message_id
      and r.content_sha256 = requested_digest
    limit 1;
$$;

create table if not exists channel_digests.digest_runs (
    run_id uuid primary key,
    owner_id uuid not null,
    trigger text not null check (trigger in ('on_demand', 'scheduled')),
    idempotency_key text not null,
    window_start timestamptz not null,
    window_end timestamptz not null,
    output_language text not null default 'ru' check (output_language in ('ru', 'en')),
    state text not null check (state in (
        'accepted', 'acquiring', 'waiting_recap', 'completed', 'partial', 'failed'
    )),
    safe_failure_class text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check (window_start < window_end),
    unique (owner_id, trigger, idempotency_key, window_start, window_end)
);
create index if not exists digest_runs_owner_created_idx
    on channel_digests.digest_runs (owner_id, created_at desc, run_id);

create or replace function channel_digests.normalized_window(
    scheduled boolean,
    activation_at timestamptz,
    prior_occurrence timestamptz,
    end_at timestamptz
) returns table(start_at timestamptz, end_at timestamptz)
language sql immutable
as $$
    select greatest(
        activation_at,
        case when scheduled
            then greatest(coalesce(prior_occurrence, end_at - interval '7 days'), end_at - interval '7 days')
            else end_at - interval '24 hours'
        end
    ), end_at;
$$;

create or replace function channel_digests.create_digest_run(
    requested_run_id uuid,
    requested_owner_id uuid,
    requested_trigger text,
    requested_idempotency_key text,
    requested_window_start timestamptz,
    requested_window_end timestamptz
) returns uuid
language sql
as $$
    with inserted as (
        insert into channel_digests.digest_runs (
            run_id, owner_id, trigger, idempotency_key, window_start, window_end, state
        ) values (
            requested_run_id, requested_owner_id, requested_trigger, requested_idempotency_key,
            requested_window_start, requested_window_end, 'accepted'
        )
        on conflict (owner_id, trigger, idempotency_key, window_start, window_end) do nothing
        returning run_id
    )
    select run_id from inserted
    union all
    select r.run_id from channel_digests.digest_runs r
    where r.owner_id = requested_owner_id
      and r.trigger = requested_trigger
      and r.idempotency_key = requested_idempotency_key
      and r.window_start = requested_window_start
      and r.window_end = requested_window_end
    limit 1;
$$;

create or replace function channel_digests.transition_run(
    requested_run_id uuid,
    expected_state text,
    target_state text,
    failure_class text
) returns boolean
language plpgsql
as $$
declare
    changed_count bigint;
begin
    update channel_digests.digest_runs r
    set state = target_state, safe_failure_class = failure_class, updated_at = now()
    where r.run_id = requested_run_id
      and r.state = expected_state
      and r.state not in ('completed', 'partial', 'failed');
    get diagnostics changed_count = row_count;
    return changed_count > 0;
end;
$$;

create table if not exists channel_digests.digest_manifests (
    manifest_id uuid primary key,
    run_id uuid not null unique references channel_digests.digest_runs(run_id),
    owner_id uuid not null,
    sha256 text not null check (length(sha256) = 64),
    source_count integer not null check (source_count between 0 and 100),
    channel_count integer not null check (channel_count between 0 and 20),
    canonical_json jsonb not null,
    created_at timestamptz not null default now(),
    unique (owner_id, manifest_id)
);

create table if not exists channel_digests.digest_results (
    result_id uuid primary key,
    run_id uuid not null unique references channel_digests.digest_runs(run_id),
    manifest_id uuid not null unique references channel_digests.digest_manifests(manifest_id),
    owner_id uuid not null,
    outcome text not null check (outcome in ('completed', 'partial', 'failed')),
    recap_id uuid,
    result_digest_hex text check (
        result_digest_hex is null or result_digest_hex ~ '^[0-9a-f]{64}$'
    ),
    citation_count integer not null default 0 check (citation_count >= 0),
    safe_failure_class text,
    created_at timestamptz not null default now(),
    check (
        (
            outcome in ('completed', 'partial')
            and recap_id is not null
            and result_digest_hex is not null
        )
        or (
            outcome = 'failed'
            and recap_id is null
            and result_digest_hex is null
        )
    ),
    unique (owner_id, result_id)
);

create table if not exists channel_digests.inbox_messages (
    message_id uuid primary key,
    subject text not null,
    semantic_key text not null,
    payload_sha256 text not null check (length(payload_sha256) = 64),
    state text not null check (state in ('processing', 'completed', 'failed')),
    received_at timestamptz not null default now(),
    completed_at timestamptz,
    safe_failure_class text,
    unique (subject, semantic_key)
);

create table if not exists channel_digests.outbox_messages (
    outbox_id uuid primary key,
    subject text not null,
    semantic_key text not null,
    owner_id uuid not null,
    operation_id uuid not null,
    payload jsonb not null,
    created_at timestamptz not null default now(),
    published_at timestamptz,
    attempts integer not null default 0 check (attempts >= 0),
    next_attempt_at timestamptz not null default now(),
    safe_failure_class text,
    unique (subject, semantic_key)
);
create index if not exists outbox_pending_idx
    on channel_digests.outbox_messages (next_attempt_at, created_at)
    where published_at is null;

create table if not exists channel_digests.leases (
    resource_kind text not null,
    resource_id uuid not null,
    holder_id uuid not null,
    acquired_at timestamptz not null,
    expires_at timestamptz not null,
    checkpoint jsonb not null default '{}'::jsonb,
    primary key (resource_kind, resource_id),
    check (acquired_at < expires_at)
);

create or replace function channel_digests.acquire_lease(
    requested_kind text,
    requested_resource_id uuid,
    requested_holder_id uuid,
    requested_at timestamptz,
    requested_expires_at timestamptz
) returns boolean
language plpgsql
as $$
declare
    changed_count bigint;
begin
    insert into channel_digests.leases (
        resource_kind, resource_id, holder_id, acquired_at, expires_at
    ) values (
        requested_kind, requested_resource_id, requested_holder_id,
        requested_at, requested_expires_at
    )
    on conflict (resource_kind, resource_id) do update
    set holder_id = excluded.holder_id,
        acquired_at = excluded.acquired_at,
        expires_at = excluded.expires_at
    where channel_digests.leases.expires_at <= requested_at
       or channel_digests.leases.holder_id = requested_holder_id;
    get diagnostics changed_count = row_count;
    return changed_count > 0;
end;
$$;
