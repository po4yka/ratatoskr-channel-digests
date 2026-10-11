//! Strict finite configuration and role-separation behavior.

use ratatoskr_channel_digest_contracts::OutputLanguage;
use ratatoskr_channel_digests::{Config, Role};

fn base() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "RATATOSKR__DATABASE__URL",
            "postgres://fixture.invalid/digests",
        ),
        ("RATATOSKR__AUTH__SERVICE_SECRET", "service-LEAKME"),
    ]
}

fn reader_entries() -> [(&'static str, &'static str); 5] {
    [
        ("RATATOSKR__KNOWLEDGE__BASE_URL", "http://127.0.0.1:8096"),
        (
            "RATATOSKR__KNOWLEDGE__RESULT_READER_SERVICE_SECRET",
            "knowledge-reader-LEAKME",
        ),
        ("RATATOSKR__KNOWLEDGE__CONNECT_TIMEOUT_MS", "1000"),
        ("RATATOSKR__KNOWLEDGE__REQUEST_TIMEOUT_MS", "3000"),
        ("RATATOSKR__KNOWLEDGE__MAX_RESPONSE_BYTES", "65536"),
    ]
}

#[test]
fn configuration_is_strict_finite_and_role_scoped() -> Result<(), Box<dyn std::error::Error>> {
    let api = Config::from_environment(Role::Api, base().into_iter().chain(reader_entries()))?;
    assert_eq!(api.api.listen_address.to_string(), "127.0.0.1:8098");
    assert_eq!(api.operator.listen_address.to_string(), "127.0.0.1:9469");
    assert_eq!(api.database.max_connections, 8);
    assert!((1..=1_048_576).contains(&api.limits.request_bytes));
    assert!((1..=100).contains(&api.limits.page_size));
    assert!((1..=130_000).contains(&api.limits.shutdown_timeout_ms));
    assert!(
        api.provider.is_none(),
        "API process must not represent provider settings"
    );
    let rendered = format!("{api:?}");
    assert!(!rendered.contains("service-LEAKME"));
    assert!(rendered.contains("[redacted]"));

    let mut worker_env = base();
    worker_env.extend([
        ("RATATOSKR__PROVIDER__API_ID", "12345"),
        ("RATATOSKR__PROVIDER__API_HASH", "api-hash-LEAKME"),
        (
            "RATATOSKR__PROVIDER__SESSION_FILE",
            "/run/credentials/session.enc",
        ),
        (
            "RATATOSKR__PROVIDER__SESSION_KEY_FILE",
            "/run/credentials/session.key",
        ),
        ("RATATOSKR__BUS__ENDPOINT", "nats://127.0.0.1:4222"),
    ]);
    let worker = Config::from_environment(Role::Worker, worker_env)?;
    assert_eq!(worker.operator.listen_address.to_string(), "127.0.0.1:9470");
    let provider = worker
        .provider
        .as_ref()
        .ok_or("worker provider is absent")?;
    assert_eq!(provider.max_concurrency, 2);
    assert!((1..=100).contains(&worker.limits.source_count));
    assert!((1..=20).contains(&worker.limits.channel_count));
    assert!((1..=16_384).contains(&worker.limits.source_bytes));
    assert!((1..=10).contains(&worker.limits.retry_attempts));
    let rendered = format!("{worker:?}");
    assert!(!rendered.contains("api-hash-LEAKME"));
    assert!(!rendered.contains("service-LEAKME"));

    let unknown = Config::from_environment(
        Role::Api,
        base()
            .into_iter()
            .chain([("RATATOSKR__PROVIDER__SURPRISE", "unknown-LEAKME")]),
    )
    .expect_err("unknown prefixed key must fail");
    let diagnostic = unknown.to_string();
    assert!(diagnostic.contains("RATATOSKR__PROVIDER__SURPRISE"));
    assert!(!diagnostic.contains("unknown-LEAKME"));

    let invalid = Config::from_environment(
        Role::Api,
        base()
            .into_iter()
            .chain([("RATATOSKR__LIMITS__REQUEST_BYTES", "invalid-LEAKME")]),
    )
    .expect_err("invalid bound must fail");
    assert!(!invalid.to_string().contains("invalid-LEAKME"));
    Ok(())
}

#[test]
fn knowledge_result_reader_is_api_only_redacted_and_bounded()
-> Result<(), Box<dyn std::error::Error>> {
    const BASE_URL: &str = "RATATOSKR__KNOWLEDGE__BASE_URL";
    const SERVICE_SECRET: &str = "RATATOSKR__KNOWLEDGE__RESULT_READER_SERVICE_SECRET";
    const CONNECT_TIMEOUT: &str = "RATATOSKR__KNOWLEDGE__CONNECT_TIMEOUT_MS";
    const REQUEST_TIMEOUT: &str = "RATATOSKR__KNOWLEDGE__REQUEST_TIMEOUT_MS";
    const MAX_RESPONSE_BYTES: &str = "RATATOSKR__KNOWLEDGE__MAX_RESPONSE_BYTES";
    const READER_SECRET: &str = "knowledge-reader-LEAKME";

    let reader_entries = reader_entries();

    Config::from_environment(Role::Api, base())
        .expect_err("API role must require explicit Knowledge result-reader authority");

    let api = Config::from_environment(Role::Api, base().into_iter().chain(reader_entries))?;
    let rendered = format!("{api:?}");
    assert!(!rendered.contains(READER_SECRET));
    assert!(rendered.contains("[redacted]"));

    for (key, value) in [
        (BASE_URL, "http://192.0.2.1:8096".to_owned()),
        (SERVICE_SECRET, String::new()),
        (SERVICE_SECRET, "LEAKME".repeat(683)),
        (CONNECT_TIMEOUT, "0".to_owned()),
        (CONNECT_TIMEOUT, u64::MAX.to_string()),
        (REQUEST_TIMEOUT, "0".to_owned()),
        (REQUEST_TIMEOUT, u64::MAX.to_string()),
        (MAX_RESPONSE_BYTES, "0".to_owned()),
        (MAX_RESPONSE_BYTES, "65537".to_owned()),
    ] {
        let environment = base()
            .into_iter()
            .chain(reader_entries)
            .filter(|(existing_key, _)| *existing_key != key)
            .map(|(existing_key, existing_value)| {
                (existing_key.to_owned(), existing_value.to_owned())
            })
            .chain([(key.to_owned(), value.clone())]);
        let error = Config::from_environment(Role::Api, environment)
            .expect_err("invalid Knowledge result-reader configuration must fail");
        let diagnostic = error.to_string();
        assert!(diagnostic.contains(key));
        if !value.is_empty() {
            assert!(!diagnostic.contains(value.as_str()));
        }
        assert!(!diagnostic.contains("LEAKME"));
    }

    let mut worker_entries = base();
    worker_entries.extend([
        ("RATATOSKR__PROVIDER__API_ID", "12345"),
        ("RATATOSKR__PROVIDER__API_HASH", "worker-hash-LEAKME"),
        (
            "RATATOSKR__PROVIDER__SESSION_FILE",
            "/run/credentials/session.enc",
        ),
        (
            "RATATOSKR__PROVIDER__SESSION_KEY_FILE",
            "/run/credentials/session.key",
        ),
        ("RATATOSKR__BUS__ENDPOINT", "nats://127.0.0.1:4222"),
    ]);
    for (key, value) in reader_entries {
        let error = Config::from_environment(
            Role::Worker,
            worker_entries.iter().copied().chain([(key, value)]),
        )
        .expect_err("worker role must reject every Knowledge result-reader key");
        let diagnostic = error.to_string();
        assert!(diagnostic.contains(key));
        assert!(!diagnostic.contains(value));
        assert!(!diagnostic.contains("LEAKME"));
    }

    Ok(())
}

fn worker_entries() -> Vec<(&'static str, &'static str)> {
    let mut entries = base();
    entries.extend([
        ("RATATOSKR__PROVIDER__API_ID", "12345"),
        ("RATATOSKR__PROVIDER__API_HASH", "worker-hash-LEAKME"),
        (
            "RATATOSKR__PROVIDER__SESSION_FILE",
            "/run/credentials/session.enc",
        ),
        (
            "RATATOSKR__PROVIDER__SESSION_KEY_FILE",
            "/run/credentials/session.key",
        ),
        ("RATATOSKR__BUS__ENDPOINT", "nats://127.0.0.1:4222"),
    ]);
    entries
}

const NKEY_SEED_PATH: &str = "RATATOSKR__BUS__NKEY_SEED_PATH";

#[test]
fn worker_role_accepts_an_absolute_bus_nkey_seed_path() -> Result<(), Box<dyn std::error::Error>> {
    let without = Config::from_environment(Role::Worker, worker_entries())?;
    assert_eq!(
        without
            .bus
            .as_ref()
            .ok_or("worker bus is absent")?
            .nkey_seed_path,
        None,
        "an unauthenticated development broker needs no seed"
    );

    let worker = Config::from_environment(
        Role::Worker,
        worker_entries()
            .into_iter()
            .chain([(NKEY_SEED_PATH, "/etc/ratatoskr/channel-digests.nkey")]),
    )?;
    assert_eq!(
        worker
            .bus
            .as_ref()
            .ok_or("worker bus is absent")?
            .nkey_seed_path,
        Some(std::path::PathBuf::from(
            "/etc/ratatoskr/channel-digests.nkey"
        ))
    );
    Ok(())
}

#[test]
fn worker_role_rejects_a_relative_nkey_seed_path() {
    let error = Config::from_environment(
        Role::Worker,
        worker_entries()
            .into_iter()
            .chain([(NKEY_SEED_PATH, "channel-digests-LEAKME.nkey")]),
    )
    .expect_err("a relative seed path must fail");
    let diagnostic = error.to_string();
    assert!(diagnostic.contains(NKEY_SEED_PATH));
    assert!(
        diagnostic.contains("must be an absolute path"),
        "{diagnostic}"
    );
    assert!(!diagnostic.contains("LEAKME"));
}

#[test]
fn api_role_rejects_the_bus_nkey_seed_path() -> Result<(), Box<dyn std::error::Error>> {
    // The same key is valid for the worker, so the refusal below is role scoping and not a typo.
    Config::from_environment(
        Role::Worker,
        worker_entries()
            .into_iter()
            .chain([(NKEY_SEED_PATH, "/etc/ratatoskr/channel-digests.nkey")]),
    )?;
    let error = Config::from_environment(
        Role::Api,
        base()
            .into_iter()
            .chain(reader_entries())
            .chain([(NKEY_SEED_PATH, "/etc/ratatoskr/channel-digests.nkey")]),
    )
    .expect_err("the API role holds no bus credential");
    let diagnostic = error.to_string();
    assert!(diagnostic.contains(NKEY_SEED_PATH));
    assert!(
        diagnostic.contains("is not recognized for this role"),
        "{diagnostic}"
    );
    Ok(())
}

#[test]
fn schedule_keys_are_worker_only_strict_and_default_when_an_owner_is_set()
-> Result<(), Box<dyn std::error::Error>> {
    const OWNER: &str = "RATATOSKR__SCHEDULE__OWNER_USER_ID";
    const CRON: &str = "RATATOSKR__SCHEDULE__CRON";
    const ENABLED: &str = "RATATOSKR__SCHEDULE__ENABLED";
    let owner = "018f0000-0000-7000-8000-000000000042";

    let absent = Config::from_environment(Role::Worker, worker_entries())?;
    assert_eq!(absent.schedule, None, "no owner registers nothing");

    let defaults = Config::from_environment(
        Role::Worker,
        worker_entries().into_iter().chain([(OWNER, owner)]),
    )?;
    let schedule = defaults
        .schedule
        .ok_or("an owner configures the schedule")?;
    assert_eq!(schedule.owner_user_id.to_string(), owner);
    assert_eq!(schedule.cron_expression, "0 6 * * *");
    assert!(schedule.enabled);

    let explicit = Config::from_environment(
        Role::Worker,
        worker_entries().into_iter().chain([
            (OWNER, owner),
            (CRON, "30 4 * * *"),
            (ENABLED, "false"),
        ]),
    )?;
    let schedule = explicit
        .schedule
        .ok_or("an owner configures the schedule")?;
    assert_eq!(schedule.cron_expression, "30 4 * * *");
    assert!(!schedule.enabled);

    for (key, value) in [
        (OWNER, "not-a-uuid"),
        (OWNER, "018F0000-0000-7000-8000-000000000042"),
        (CRON, "0 6 * *"),
        (ENABLED, "yes"),
    ] {
        let entries = worker_entries()
            .into_iter()
            .filter(|(existing, _)| *existing != key)
            .chain([(OWNER, owner), (key, value)]);
        let error = Config::from_environment(Role::Worker, entries)
            .expect_err("an invalid schedule value must fail");
        assert!(error.to_string().contains(key));
    }

    for (key, value) in [(CRON, "0 6 * * *"), (ENABLED, "true")] {
        let error = Config::from_environment(
            Role::Worker,
            worker_entries().into_iter().chain([(key, value)]),
        )
        .expect_err("a schedule detail without an owner must fail");
        assert!(error.to_string().contains(OWNER), "{error}");
    }

    let error = Config::from_environment(
        Role::Api,
        base()
            .into_iter()
            .chain(reader_entries())
            .chain([(OWNER, owner)]),
    )
    .expect_err("the API role registers no schedule");
    assert!(
        error
            .to_string()
            .contains("is not recognized for this role")
    );
    Ok(())
}

#[test]
fn run_deadline_is_finite() -> Result<(), Box<dyn std::error::Error>> {
    const DEADLINE: &str = "RATATOSKR__LIMITS__RUN_DEADLINE_SECONDS";

    let default = Config::from_environment(Role::Worker, worker_entries())?;
    assert_eq!(default.limits.run_deadline_seconds, 1_800);

    for (value, expected) in [("60", 60), ("3600", 3_600), ("86400", 86_400)] {
        let worker = Config::from_environment(
            Role::Worker,
            worker_entries().into_iter().chain([(DEADLINE, value)]),
        )?;
        assert_eq!(worker.limits.run_deadline_seconds, expected, "{value}");
    }

    for value in ["59", "86401", "0", "-1", "soon-LEAKME", ""] {
        let error = Config::from_environment(
            Role::Worker,
            worker_entries().into_iter().chain([(DEADLINE, value)]),
        )
        .expect_err("a deadline outside 60..=86400 must fail");
        let diagnostic = error.to_string();
        assert!(diagnostic.contains(DEADLINE), "{diagnostic}");
        assert!(
            diagnostic.contains("is outside the finite range"),
            "{diagnostic}"
        );
        assert!(!diagnostic.contains("LEAKME"));
    }
    Ok(())
}

#[test]
fn schedule_output_language_is_strict_and_worker_only() -> Result<(), Box<dyn std::error::Error>> {
    const OWNER: &str = "RATATOSKR__SCHEDULE__OWNER_USER_ID";
    const LANGUAGE: &str = "RATATOSKR__SCHEDULE__OUTPUT_LANGUAGE";
    let owner = "018f0000-0000-7000-8000-000000000042";
    let with_owner = || worker_entries().into_iter().chain([(OWNER, owner)]);

    let default = Config::from_environment(Role::Worker, with_owner())?;
    assert_eq!(
        default
            .schedule
            .ok_or("owner configures the schedule")?
            .output_language,
        OutputLanguage::Ru,
        "the default stays Russian"
    );
    for (value, expected) in [("ru", OutputLanguage::Ru), ("en", OutputLanguage::En)] {
        let worker =
            Config::from_environment(Role::Worker, with_owner().chain([(LANGUAGE, value)]))?;
        assert_eq!(
            worker
                .schedule
                .ok_or("owner configures the schedule")?
                .output_language,
            expected,
            "{value}"
        );
    }

    for value in ["de", "RU", "En", "", "ru-LEAKME"] {
        let error = Config::from_environment(Role::Worker, with_owner().chain([(LANGUAGE, value)]))
            .expect_err("only ru and en are languages");
        let diagnostic = error.to_string();
        assert!(diagnostic.contains(LANGUAGE), "{diagnostic}");
        assert!(!diagnostic.contains("LEAKME"));
    }

    let without_owner = Config::from_environment(
        Role::Worker,
        worker_entries().into_iter().chain([(LANGUAGE, "en")]),
    )
    .expect_err("a schedule detail without an owner must fail");
    assert!(without_owner.to_string().contains(OWNER), "{without_owner}");

    let api = Config::from_environment(
        Role::Api,
        base()
            .into_iter()
            .chain(reader_entries())
            .chain([(LANGUAGE, "en")]),
    )
    .expect_err("the API role registers no schedule");
    assert!(api.to_string().contains("is not recognized for this role"));
    Ok(())
}
