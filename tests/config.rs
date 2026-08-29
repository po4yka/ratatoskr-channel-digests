//! Strict finite configuration and role-separation behavior.

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
