//! The `CHANNEL_DIGESTS` identity fragment proven on an authorization-enabled broker
//! (XR-021 CONTRACTS.md S03 and S04).
//!
//! `deploy/nats/identity.conf` is rendered with a generated nkey into the `authorization` block of a
//! real `nats-server`. Edge's part is played by an admin identity that provisions both streams and the
//! durables the table of S04 gives this service. The worker's own connect helper, consumer
//! verification and outbox publish path then run as the channel-digests identity. A denied publish is
//! never reported to a client (the server log says `Publish Violation`), so every refusal is observed
//! as a missing acknowledgement or a request that is never answered, next to a control that the same
//! operation succeeds for the admin identity.

use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::{AckPolicy, pull};
use async_nats::jetstream::context::ContextBuilder;
use async_nats::jetstream::{self, stream};
use futures_util::StreamExt as _;
use ratatoskr_channel_digests::{
    OutboxRow, connect, publish_message, verify_consumers, wrap_outbox_row,
};
use ratatoskr_identifiers::WireTimestamp;
use serde_json::json;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const FRAGMENT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../deploy/nats/identity.conf"
);
const PLACEHOLDER: &str = "UREPLACE_ME_WITH_THE_PUBLIC_NKEY_OF_RATATOSKR_CHANNEL_DIGESTS_";
/// The image CI already starts for `JetStream`, used when no `nats-server` binary is installed.
const NATS_IMAGE: &str =
    "nats@sha256:d4ac35882ac65aff236cd65b9d3fa4d24332c681e1a85f94eedccd3cdd65b1da";

const COMMANDS: &str = "ratatoskr_commands";
const EVENTS: &str = "ratatoskr_events";

/// Every durable of S04 this identity owns: stream, name, filter.
const DURABLES: [(&str, &str, &str); 5] = [
    (
        COMMANDS,
        "ratatoskr_channel_digest_subscriptions",
        "cmd.channel_digest.subscription.set_requested.v1",
    ),
    (
        COMMANDS,
        "ratatoskr_channel_digest_runs",
        "cmd.channel_digest.run.requested.v1",
    ),
    (
        COMMANDS,
        "ratatoskr_channel_digest_schedule_occurrences",
        "cmd.channel_digest.schedule.occurrence_requested.v1",
    ),
    (
        EVENTS,
        "ratatoskr_channel_digest_recap_completed",
        "evt.knowledge.channel_digest_recap.completed.v1",
    ),
    (
        EVENTS,
        "ratatoskr_channel_digest_recap_failed",
        "evt.knowledge.channel_digest_recap.failed.v1",
    ),
];

/// Knowledge's durable: it exists on the broker but this identity may not even describe it.
const FOREIGN_DURABLE: (&str, &str, &str) = (
    COMMANDS,
    "ratatoskr_knowledge_channel_recap",
    "cmd.knowledge.channel_digest_recap.requested.v1",
);

/// The publish grants of S03, exactly and in order.
fn contract_publish_allow() -> Vec<String> {
    let mut allow = vec![
        "cmd.knowledge.channel_digest_recap.requested.v1".to_owned(),
        "evt.platform.operation.reported.v1".to_owned(),
        "cmd.platform.schedule.registration_requested.v1".to_owned(),
    ];
    for (stream, durable, _) in DURABLES {
        allow.push(format!("$JS.API.CONSUMER.INFO.{stream}.{durable}"));
        allow.push(format!("$JS.API.CONSUMER.MSG.NEXT.{stream}.{durable}"));
        allow.push(format!("$JS.ACK.{stream}.{durable}.>"));
    }
    allow
}

#[test]
fn the_fragment_grants_exactly_the_contract() -> TestResult {
    let fragment = std::fs::read_to_string(FRAGMENT)?;
    let code = without_comments(&fragment);
    assert_eq!(
        quoted_strings(section(&code, "publish")?),
        contract_publish_allow()
    );
    assert_eq!(quoted_strings(section(&code, "subscribe")?), ["_INBOX.>"]);
    assert!(
        !code.contains("deny"),
        "no stanza except EDGE has a deny block"
    );
    for forbidden in ["$JS.API.>", "evt.>", "cmd.>", "$JS.ACK.>"] {
        assert!(
            !quoted_strings(section(&code, "publish")?).contains(&forbidden.to_owned()),
            "{forbidden} belongs to EDGE alone"
        );
    }
    Ok(())
}

#[tokio::test]
async fn channel_digests_identity_gets_exactly_its_grants_on_an_authorized_broker() -> TestResult {
    let fragment = std::fs::read_to_string(FRAGMENT)?;
    let admin = nkeys::KeyPair::new_user();
    let digests = nkeys::KeyPair::new_user();
    let workdir = Workdir::new()?;
    let seed_path = workdir.path.join("channel-digests.nkey");
    std::fs::write(&seed_path, format!("{}\n", digests.seed()?))?;
    std::fs::set_permissions(&seed_path, std::fs::Permissions::from_mode(0o600))?;
    let identity = render_identity(&fragment, &digests.public_key())?;
    let broker = Broker::start(&workdir, &admin.public_key(), &identity)?;
    let url = format!("nats://127.0.0.1:{}", broker.port);

    // Edge's part: provision the topology as the admin identity.
    let admin_client = async_nats::ConnectOptions::with_nkey(admin.seed()?)
        .connect(&url)
        .await?;
    let admin_context = jetstream::new(admin_client);
    provision(&admin_context).await?;

    // The broker requires a credential at all.
    assert!(
        connect(&url, None).await.is_err(),
        "an unauthenticated worker must be refused"
    );

    // The worker's own connect helper, as the channel-digests identity.
    let client = connect(&url, Some(&seed_path)).await?;
    let context = jetstream::new(client.clone());
    verify_consumers(&context)
        .await
        .map_err(|error| format!("all five durables must verify: {error}"))?;

    // One command is pulled and acknowledged through the granted subjects.
    admin_context
        .publish("cmd.channel_digest.run.requested.v1", "{}".into())
        .await?
        .await?;
    let runs: jetstream::consumer::PullConsumer = context
        .get_consumer_from_stream("ratatoskr_channel_digest_runs", COMMANDS)
        .await?;
    let mut batch = runs
        .fetch()
        .max_messages(1)
        .expires(Duration::from_secs(5))
        .messages()
        .await?;
    let delivered = batch
        .next()
        .await
        .ok_or("no command was delivered")?
        .map_err(|error| error.to_string())?;
    delivered
        .double_ack()
        .await
        .map_err(|error| format!("the acknowledgement must be granted: {error}"))?;

    // The three subjects the outbox publishes, through the worker's own wrapping and publish path.
    for row in outbox_rows()? {
        let message = wrap_outbox_row(&row)?;
        publish_message(&context, &message)
            .await
            .map_err(|error| format!("{} must be granted: {error}", message.subject))?;
    }

    // Refusals, each next to a control that the admin identity may do the same.
    let quick = ContextBuilder::new()
        .timeout(Duration::from_millis(700))
        .build(client);
    let denied_fact = "evt.knowledge.channel_digest_recap.completed.v1";
    admin_context
        .publish(denied_fact, "{}".into())
        .await?
        .await
        .map_err(|error| format!("control: the stream accepts {denied_fact}: {error}"))?;
    let accepted = match quick.publish(denied_fact, "{}".into()).await {
        Ok(ack) => ack.await.is_ok(),
        Err(_) => false,
    };
    assert!(
        !accepted,
        "Knowledge's completion fact is not this identity's to publish"
    );

    let (stream, durable, _) = FOREIGN_DURABLE;
    let control: Result<jetstream::consumer::PullConsumer, _> = admin_context
        .get_consumer_from_stream(durable, stream)
        .await;
    control.map_err(|error| format!("control: the admin may describe {durable}: {error}"))?;
    let described: Result<jetstream::consumer::PullConsumer, _> =
        quick.get_consumer_from_stream(durable, stream).await;
    assert!(
        described.is_err(),
        "describing Knowledge's durable is not granted"
    );
    drop(broker);
    Ok(())
}

async fn provision(context: &jetstream::Context) -> TestResult {
    let commands = context
        .create_stream(stream::Config {
            name: COMMANDS.to_owned(),
            subjects: vec!["cmd.>".to_owned()],
            ..stream::Config::default()
        })
        .await?;
    let events = context
        .create_stream(stream::Config {
            name: EVENTS.to_owned(),
            subjects: vec!["evt.>".to_owned()],
            ..stream::Config::default()
        })
        .await?;
    for (stream, durable, filter) in DURABLES.into_iter().chain([FOREIGN_DURABLE]) {
        let target = if stream == COMMANDS {
            &commands
        } else {
            &events
        };
        target
            .create_consumer(pull::Config {
                durable_name: Some(durable.to_owned()),
                filter_subject: filter.to_owned(),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                ..pull::Config::default()
            })
            .await?;
    }
    Ok(())
}

fn outbox_rows() -> Result<Vec<OutboxRow>, Box<dyn std::error::Error>> {
    let owner = Uuid::now_v7();
    let operation = Uuid::now_v7();
    let created_at = WireTimestamp::parse("2026-08-29T10:00:00Z")?;
    let row = |subject: &str, operation_id, payload| OutboxRow {
        outbox_id: Uuid::now_v7(),
        subject: subject.to_owned(),
        owner_id: owner,
        operation_id,
        causation_ref: None,
        created_at,
        payload,
    };
    Ok(vec![
        row(
            "knowledge.channel_digest_recap.requested.v1",
            Some(operation),
            json!({
                "operation_id": operation,
                "owner": format!("user:{owner}"),
                "digest_run_id": Uuid::now_v7(),
                "window": {"start_at": "2026-08-28T10:00:00Z", "end_at": "2026-08-29T10:00:00Z"},
                "output_language": "ru",
                "source_count": 1,
                "channel_count": 1,
                "manifest_ref": format!("channel-digest-manifest:{}", Uuid::now_v7()),
                "manifest_digest": {"algorithm": "sha256", "hex": "11".repeat(32)},
                "analysis_family": "channel_digest_recap",
                "analysis_contract": "channel_digest_recap.v1"
            }),
        ),
        row(
            "platform.operation.reported.v1",
            Some(operation),
            json!({"operation_id": operation, "status": "succeeded", "stage": "applied"}),
        ),
        row(
            "platform.schedule.registration_requested.v1",
            None,
            json!({
                "service_name": "ratatoskr-channel-digests",
                "name": "daily-digest",
                "owner_user_id": owner,
                "cron_expression": "0 6 * * *",
                "command_type": "channel_digest.schedule.occurrence_requested.v1",
                "operation_kind": "channel_digest.schedule.occurrence",
                "payload": {},
                "enabled": true
            }),
        ),
    ])
}

/// The fragment with its placeholder nkey replaced by a generated public key.
fn render_identity(fragment: &str, public_key: &str) -> Result<String, Box<dyn std::error::Error>> {
    let (before, rest) = fragment
        .split_once(PLACEHOLDER)
        .ok_or("the fragment carries no CHANNEL_DIGESTS placeholder")?;
    let token_end = rest
        .find(char::is_whitespace)
        .ok_or("the placeholder is not followed by whitespace")?;
    let after = rest
        .get(token_end..)
        .ok_or("the placeholder is truncated")?;
    Ok(format!("{before}{public_key}{after}"))
}

fn without_comments(text: &str) -> String {
    text.lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The bracketed list that follows `<name>: {` ... `allow: [`.
fn section<'a>(code: &'a str, name: &str) -> Result<&'a str, Box<dyn std::error::Error>> {
    let needle = format!("{name}:");
    let (_, after_name) = code
        .split_once(needle.as_str())
        .ok_or_else(|| format!("no {name} section"))?;
    let (_, after_allow) = after_name
        .split_once("allow: [")
        .ok_or_else(|| format!("no {name} allow list"))?;
    let (list, _) = after_allow
        .split_once(']')
        .ok_or_else(|| format!("unterminated {name} allow list"))?;
    Ok(list)
}

fn quoted_strings(list: &str) -> Vec<String> {
    list.split('"')
        .enumerate()
        .filter(|(index, _)| index % 2 == 1)
        .map(|(_, value)| value.to_owned())
        .collect()
}

/// A private scratch directory, removed on drop.
struct Workdir {
    path: PathBuf,
}

impl Workdir {
    fn new() -> Result<Self, std::io::Error> {
        let path =
            std::env::temp_dir().join(format!("channel-digests-authorized-bus-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&path)?;
        Ok(Self { path })
    }
}

impl Drop for Workdir {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.path));
    }
}

/// A `nats-server` with authorization, native when the binary exists and in the CI image otherwise.
struct Broker {
    child: Child,
    port: u16,
    container: Option<String>,
}

impl Broker {
    fn start(
        workdir: &Workdir,
        admin_public_key: &str,
        identity: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let binary = std::env::var("CHANNEL_DIGEST_TEST_NATS_SERVER")
            .unwrap_or_else(|_| "nats-server".to_owned());
        let native = Command::new(&binary)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success());
        let port = free_port()?;
        let (listen, store) = if native {
            (
                format!("port: {port}\nhost: 127.0.0.1"),
                workdir.path.join("js").display().to_string(),
            )
        } else {
            ("port: 4222\nhost: 0.0.0.0".to_owned(), "/tmp/js".to_owned())
        };
        let configuration = format!(
            "{listen}\njetstream {{ store_dir: \"{store}\" }}\nauthorization {{\n  users: [\n    {{ nkey: {admin_public_key}, permissions: {{ publish: {{ allow: [\">\"] }}, subscribe: {{ allow: [\">\"] }} }} }},\n{identity}\n  ]\n}}\n"
        );
        let configuration_path = workdir.path.join("nats.conf");
        std::fs::write(&configuration_path, configuration)?;
        let (child, container) = if native {
            (
                Command::new(&binary)
                    .arg("-c")
                    .arg(&configuration_path)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()?,
                None,
            )
        } else {
            let name = format!("channel-digests-authorized-bus-{port}");
            (
                Command::new("docker")
                    .args(["run", "--rm", "--name", &name, "-p"])
                    .arg(format!("127.0.0.1:{port}:4222"))
                    .arg("-v")
                    .arg(format!("{}:/conf:ro", workdir.path.display()))
                    .args([NATS_IMAGE, "-c", "/conf/nats.conf"])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()?,
                Some(name),
            )
        };
        let mut broker = Self {
            child,
            port,
            container,
        };
        broker.wait_ready()?;
        Ok(broker)
    }

    fn wait_ready(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Err(format!("the broker exited before it was ready: {status}").into());
            }
            if TcpStream::connect_timeout(
                &(Ipv4Addr::LOCALHOST, self.port).into(),
                Duration::from_millis(200),
            )
            .is_ok()
            {
                // The listener can open before JetStream is ready, so give it a moment.
                std::thread::sleep(Duration::from_millis(500));
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("the broker did not become ready".into());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        if let Some(name) = &self.container {
            drop(Command::new("docker").args(["rm", "-f", name]).output());
        }
        drop(self.child.kill());
        drop(self.child.wait());
    }
}

/// A free loopback port, preferring the block reserved for private test brokers.
fn free_port() -> Result<u16, std::io::Error> {
    for port in 57_090..=57_099 {
        if TcpListener::bind((Ipv4Addr::LOCALHOST, port)).is_ok() {
            return Ok(port);
        }
    }
    Ok(TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?
        .local_addr()?
        .port())
}
