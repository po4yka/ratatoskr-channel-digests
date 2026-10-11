-- Run as a PostgreSQL administrator after Platform's 01-database-and-roles.sql has created the
-- ratatoskr database and before the first start of the channel-digests API or worker. Set the
-- password separately with `\password ratatoskr_channel_digests`; no credential belongs in this
-- repository. The service applies its own channel_digests schema from schema.sql at start.

do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'ratatoskr_channel_digests') then
        create role ratatoskr_channel_digests login;
    end if;
end
$$;

alter role ratatoskr_channel_digests nosuperuser nocreatedb nocreaterole noreplication nobypassrls;
grant connect, create on database ratatoskr to ratatoskr_channel_digests;

\connect ratatoskr
revoke all on schema public from ratatoskr_channel_digests;
