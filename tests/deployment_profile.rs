//! The shipped operator examples load through the real configuration loader (XR-021 CONTRACTS.md
//! R2-10): an example that names a key the loader does not know, or omits one a flow needs, fails
//! here instead of silently disabling the flow in production.

use std::path::PathBuf;

use ratatoskr_channel_digest_contracts::OutputLanguage;
use ratatoskr_channel_digests::{Config, Role};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Ports `docs/DEPLOYMENT_TARGET.md` allocates to this repository.
const ALLOCATED_PORTS: [u16; 3] = [8098, 9469, 9470];

/// The Knowledge domain API port of the same table, where `/internal/channel-digest-results/*` is
/// served after the Knowledge listener split (R2-01).
const KNOWLEDGE_API_PORT: u16 = 8091;

fn example(name: &str) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?)
        .join("deploy/systemd")
        .join(name);
    let text = std::fs::read_to_string(&path)?;
    let mut entries = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("{name}: a line without a key"))?;
        entries.push((key.to_owned(), value.to_owned()));
    }
    Ok(entries)
}

fn value<'a>(entries: &'a [(String, String)], key: &str) -> Option<&'a str> {
    entries
        .iter()
        .find(|(candidate, _)| candidate == key)
        .map(|(_, value)| value.as_str())
}

/// Every secret in an example is the placeholder, so a real credential cannot be committed.
fn assert_placeholders_only(name: &str, entries: &[(String, String)]) {
    for (key, value) in entries {
        if key.contains("SECRET") || key.contains("HASH") {
            assert_eq!(value, "CHANGE-ME", "{name}: {key} must be the placeholder");
        }
        if key.ends_with("DATABASE__URL") {
            assert!(value.contains(":CHANGE-ME@"), "{name}: {key}");
        }
    }
}

#[test]
fn shipped_examples_load_through_the_config_loader() -> TestResult {
    let api_entries = example("api.conf.example")?;
    assert_placeholders_only("api.conf.example", &api_entries);
    let api = Config::from_environment(Role::Api, api_entries.clone())?;
    assert_eq!(api.api.listen_address.to_string(), "127.0.0.1:8098");
    assert_eq!(api.operator.listen_address.to_string(), "127.0.0.1:9469");
    assert_eq!(api.service_secret(), "CHANGE-ME");
    let reader = api
        .knowledge_result_reader
        .as_ref()
        .ok_or("the API example configures the Knowledge result reader")?;
    assert_eq!(reader.base_url, "http://127.0.0.1:8091");
    assert_eq!(reader.service_secret(), "CHANGE-ME");
    assert_eq!(
        reader
            .base_url
            .rsplit(':')
            .next()
            .and_then(|port| port.parse::<u16>().ok()),
        Some(KNOWLEDGE_API_PORT)
    );
    assert!(api.provider.is_none() && api.bus.is_none() && api.schedule.is_none());
    assert_eq!(api.limits.page_size, 100);

    let worker_entries = example("worker.conf.example")?;
    assert_placeholders_only("worker.conf.example", &worker_entries);
    let worker = Config::from_environment(Role::Worker, worker_entries.clone())?;
    assert_eq!(worker.operator.listen_address.to_string(), "127.0.0.1:9470");
    let bus = worker
        .bus
        .as_ref()
        .ok_or("the worker example sets the bus")?;
    assert_eq!(bus.endpoint, "nats://127.0.0.1:4222");
    assert_eq!(
        bus.nkey_seed_path,
        Some(PathBuf::from("/etc/ratatoskr/channel-digests.nkey"))
    );
    let schedule = worker
        .schedule
        .as_ref()
        .ok_or("the worker example registers the daily schedule")?;
    assert_eq!(schedule.cron_expression, "0 6 * * *");
    assert!(schedule.enabled);
    assert_eq!(schedule.output_language, OutputLanguage::Ru);
    assert!(
        worker_entries
            .iter()
            .any(|(key, _)| key == "RATATOSKR__SCHEDULE__OWNER_USER_ID"),
        "the schedule owner is spelled out in the example"
    );
    let provider = worker
        .provider
        .as_ref()
        .ok_or("the worker example sets the provider")?;
    assert_eq!(provider.api_hash(), "CHANGE-ME");
    assert_eq!(
        provider.session_file,
        PathBuf::from("/etc/ratatoskr/secrets/channel-digests-session.enc")
    );
    assert_eq!(
        provider.session_key_file,
        PathBuf::from("/etc/ratatoskr/secrets/channel-digests-session.key")
    );
    assert_eq!(worker.limits.run_deadline_seconds, 1_800);
    assert_eq!(
        value(&worker_entries, "RATATOSKR__LIMITS__RUN_DEADLINE_SECONDS"),
        Some("1800"),
        "the deadline is spelled out so an operator sees it"
    );

    for config in [&api, &worker] {
        assert!(
            ALLOCATED_PORTS.contains(&config.operator.listen_address.port()),
            "operator bind {} is not in the DEPLOYMENT_TARGET port table",
            config.operator.listen_address
        );
    }
    assert!(ALLOCATED_PORTS.contains(&api.api.listen_address.port()));
    Ok(())
}

#[test]
fn the_worker_example_documents_the_schedule_owner_prerequisite() -> TestResult {
    let path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?)
        .join("deploy/systemd/worker.conf.example");
    let text = std::fs::read_to_string(path)?;
    assert!(
        text.contains("identity.users"),
        "the example must say the schedule owner has to exist in identity.users"
    );
    Ok(())
}
