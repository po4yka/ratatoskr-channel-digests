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

#[test]
fn configuration_is_strict_finite_and_role_scoped() -> Result<(), Box<dyn std::error::Error>> {
    let api = Config::from_environment(Role::Api, base())?;
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
