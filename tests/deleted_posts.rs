//! A post deleted upstream is withdrawn by absence from a page that proves its id range
//! (XR-021 CONTRACTS.md R5-07 rule 2).

use std::sync::Mutex;
use std::time::Duration;

use ratatoskr_channel_digest_contracts::sha256_hex;
use ratatoskr_channel_digests::{
    AcquisitionEngine, AcquisitionError, AcquisitionReport, AcquisitionRequest, CommandIntake,
    Database, DigestCoordinator, ManifestBuilder, ManifestSource, ObservedRevision, ProviderError,
    ProviderPage, ProviderPost, PublicChannelProvider, PublicChannelUsername, RetentionCounter,
    RevisionRepository, RunExecutor, SubscriptionRepository,
};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const WINDOW_START: &str = "2026-08-28T10:00:00Z";
const WINDOW_END: &str = "2026-08-29T10:00:00Z";
const BEFORE_THE_WINDOW: &str = "2026-08-27T10:00:00Z";
/// Page limit of an ordinary walk, as the executor sets it.
const FULL_WALK: usize = 20;
const USERNAME: &str = "withdrawn_posts";

#[tokio::test]
async fn a_message_missing_from_a_middle_page_is_withdrawn_with_all_its_revisions() -> TestResult {
    let channel = Channel::with_posts(1..=8).await?;
    channel.walk(FULL_WALK).await?;
    channel.append_edit(4, "edited text of message 4").await?;
    channel.provider.remove(4)?;

    channel.walk(FULL_WALK).await?;

    assert_eq!(channel.withdrawn_ids().await?, [4]);
    let rows: Vec<(Option<String>, bool, bool)> = sqlx::query_as(
        "select body, deleted_at is not null, minimized_at is not null
         from channel_digests.post_revisions where channel_id = $1 and provider_message_id = 4",
    )
    .bind(channel.id)
    .fetch_all(channel.database.pool())
    .await?;
    assert_eq!(rows.len(), 2, "both revisions of the message are withdrawn");
    assert!(
        rows.iter().all(|row| *row == (None, true, true)),
        "a withdrawn revision keeps no body and records both instants: {rows:?}"
    );
    Ok(())
}

#[tokio::test]
async fn an_id_above_the_first_page_is_withdrawn_because_the_first_range_has_no_upper_limit()
-> TestResult {
    let channel = Channel::with_posts(1..=9).await?;
    channel.walk(FULL_WALK).await?;
    channel.provider.remove(9)?;

    channel.walk(FULL_WALK).await?;

    assert_eq!(channel.withdrawn_ids().await?, [9]);
    Ok(())
}

#[tokio::test]
async fn an_id_below_a_short_last_page_is_withdrawn_because_the_page_reaches_the_channel_start()
-> TestResult {
    let channel = Channel::with_posts(1..=8).await?;
    channel.walk(FULL_WALK).await?;
    channel.provider.remove(1)?;

    channel.walk(FULL_WALK).await?;

    assert_eq!(
        channel.withdrawn_ids().await?,
        [1],
        "the last page [2] is short, so nothing exists below it"
    );
    Ok(())
}

#[tokio::test]
async fn a_vanished_id_below_the_last_page_of_a_full_walk_is_kept() -> TestResult {
    let channel = Channel::with_posts(3..=7).await?;
    channel
        .provider
        .push(post(2, "ancient two", BEFORE_THE_WINDOW))?;
    channel.walk(FULL_WALK).await?;
    channel.append_old(1).await?;

    let report = channel.walk(FULL_WALK).await?;

    assert!(!report.partial, "the walk ended at the window start");
    assert_eq!(
        channel.withdrawn_ids().await?,
        [] as [i64; 0],
        "message 1 lies below the lowest id of the page that ended the walk"
    );
    Ok(())
}

#[tokio::test]
async fn a_walk_that_stops_at_max_pages_withdraws_only_inside_the_pages_it_read() -> TestResult {
    let channel = Channel::with_posts(1..=9).await?;
    channel.walk(FULL_WALK).await?;
    channel.provider.remove(8)?;
    channel.provider.remove(2)?;

    let report = channel.walk(1).await?;

    assert!(report.partial);
    assert_eq!(
        channel.withdrawn_ids().await?,
        [8],
        "message 2 lies on a page the walk never read"
    );
    Ok(())
}

#[tokio::test]
async fn a_provider_error_mid_walk_withdraws_nothing_from_the_pages_it_did_not_read() -> TestResult
{
    let channel = Channel::with_posts(1..=9).await?;
    channel.walk(FULL_WALK).await?;
    channel.provider.remove(2)?;
    channel.provider.remove(5)?;
    channel.provider.fail_from_call(2)?;

    let report = channel.walk(FULL_WALK).await?;

    assert!(
        report.partial,
        "the failed second page leaves the walk partial"
    );
    assert_eq!(
        channel.withdrawn_ids().await?,
        [] as [i64; 0],
        "only the first page [9, 8, 7] was read and nothing on it vanished"
    );
    Ok(())
}

#[tokio::test]
async fn a_post_without_text_counts_as_present() -> TestResult {
    let channel = Channel::with_posts(1..=8).await?;
    channel.walk(FULL_WALK).await?;
    channel.provider.set_body(4, "")?;

    channel.walk(FULL_WALK).await?;

    assert_eq!(
        channel.withdrawn_ids().await?,
        [] as [i64; 0],
        "a media-only post is still on the page"
    );
    Ok(())
}

#[tokio::test]
async fn a_post_marked_deleted_by_the_provider_is_withdrawn() -> TestResult {
    let channel = Channel::with_posts(1..=8).await?;
    channel.walk(FULL_WALK).await?;
    channel.provider.mark_deleted(5)?;

    channel.walk(FULL_WALK).await?;

    assert_eq!(channel.withdrawn_ids().await?, [5]);
    Ok(())
}

#[tokio::test]
async fn a_withdrawn_message_minimizes_the_manifest_that_held_it_and_only_that_one() -> TestResult {
    let channel = Channel::with_posts(1..=9).await?;
    channel.walk(FULL_WALK).await?;
    let holding = channel.commit_manifest(&[7, 8]).await?;
    let unrelated = channel.commit_manifest(&[9]).await?;
    channel.provider.remove(7)?;

    channel.walk(FULL_WALK).await?;

    assert_eq!(
        channel.manifest_state(holding).await?,
        (true, true),
        "the whole text of the manifest that held message 7 is gone, though its run waits for a recap"
    );
    assert_eq!(
        channel.manifest_state(unrelated).await?,
        (false, false),
        "a manifest that did not hold the message is intact"
    );
    assert_eq!(
        channel.counter.manifests(),
        1,
        "the minimized manifest is counted"
    );
    let kept: (String, i32, i32) = sqlx::query_as(
        "select sha256, source_count, channel_count from channel_digests.digest_manifests
         where manifest_id = $1",
    )
    .bind(holding)
    .fetch_one(channel.database.pool())
    .await?;
    assert_eq!((kept.1, kept.2), (2, 1), "counts and digest stay: {kept:?}");
    Ok(())
}

#[tokio::test]
async fn a_walk_that_proves_nothing_gone_minimizes_no_manifest() -> TestResult {
    let channel = Channel::with_posts(1..=9).await?;
    channel.walk(FULL_WALK).await?;
    let manifest = channel.commit_manifest(&[7, 8]).await?;

    channel.walk(FULL_WALK).await?;

    assert_eq!(channel.manifest_state(manifest).await?, (false, false));
    assert_eq!(channel.counter.manifests(), 0);
    Ok(())
}

#[tokio::test]
async fn a_withdrawal_also_minimizes_the_manifest_of_a_settled_run() -> TestResult {
    let channel = Channel::with_posts(1..=9).await?;
    channel.walk(FULL_WALK).await?;
    let manifest = channel.commit_manifest(&[8]).await?;
    sqlx::query(
        "update channel_digests.digest_runs set state = 'completed'
         where run_id = (select run_id from channel_digests.digest_manifests where manifest_id = $1)",
    )
    .bind(manifest)
    .execute(channel.database.pool())
    .await?;
    channel.provider.mark_deleted(8)?;

    channel.walk(FULL_WALK).await?;

    assert_eq!(channel.manifest_state(manifest).await?, (true, true));
    Ok(())
}

#[tokio::test]
async fn the_run_executor_counts_the_manifests_its_acquisition_minimizes() -> TestResult {
    let channel = Channel::with_posts(5..=6).await?;
    let executor = RunExecutor::new(channel.database.pool().clone(), &channel.provider)
        .with_retention_counter(channel.counter.clone());
    channel.accept_run().await?;
    assert!(executor.execute_one().await?);
    let first: (Uuid,) = sqlx::query_as("select manifest_id from channel_digests.digest_manifests")
        .fetch_one(channel.database.pool())
        .await?;
    channel.provider.remove(5)?;
    channel.accept_run().await?;

    assert!(executor.execute_one().await?);

    assert_eq!(
        channel.manifest_state(first.0).await?,
        (true, true),
        "the next run's walk proved message 5 gone"
    );
    assert_eq!(channel.counter.manifests(), 1);
    Ok(())
}

/// A subscribed channel with a scripted provider and the database its revisions land in.
struct Channel {
    database: Database,
    id: Uuid,
    owner: Uuid,
    provider: ChannelPages,
    counter: RetentionCounter,
}

impl Channel {
    /// A channel whose posts are the given ids, each published a minute after the one below it.
    async fn with_posts(
        ids: std::ops::RangeInclusive<i64>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
        let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
        database.apply_schema().await?;
        sqlx::query(
            "truncate channel_digests.digest_results, channel_digests.digest_manifest_sources,
                  channel_digests.digest_manifests,
                      channel_digests.post_revisions, channel_digests.subscriptions,
                      channel_digests.channels, channel_digests.digest_runs,
                      channel_digests.inbox_messages, channel_digests.outbox_messages,
                      channel_digests.leases cascade",
        )
        .execute(database.pool())
        .await?;
        let owner = Uuid::now_v7();
        SubscriptionRepository::new(database.pool().clone())
            .set(owner, USERNAME, true, "2026-08-27T09:00:00Z")
            .await?;
        let (channel_id,): (Uuid,) =
            sqlx::query_as("select channel_id from channel_digests.channels where username = $1")
                .bind(USERNAME)
                .fetch_one(database.pool())
                .await?;
        let provider = ChannelPages::default();
        for id in ids {
            provider.push(post(id, &format!("post number {id}"), &in_window(id)))?;
        }
        Ok(Self {
            database,
            id: channel_id,
            owner,
            provider,
            counter: RetentionCounter::new(),
        })
    }

    /// Runs one acquisition walk under a fresh run identity, three posts per page.
    async fn walk(&self, max_pages: usize) -> Result<AcquisitionReport, AcquisitionError> {
        let engine = AcquisitionEngine::new(
            &self.provider,
            RevisionRepository::new(self.database.pool().clone()),
            Duration::from_secs(5),
        )
        .with_retention_counter(self.counter.clone());
        engine
            .execute(&AcquisitionRequest {
                run_id: Uuid::now_v7(),
                channel_id: self.id,
                username: USERNAME,
                window_start: WINDOW_START,
                window_end: WINDOW_END,
                max_pages,
                page_size: 3,
            })
            .await
    }

    /// Commits a manifest holding the given stored messages for a fresh run of the owner and
    /// returns its id; the run waits for its recap, as a committed run does.
    async fn commit_manifest(&self, ids: &[i64]) -> Result<Uuid, Box<dyn std::error::Error>> {
        let run_id = self.accept_run().await?;
        let sources: Vec<ManifestSource> = ids
            .iter()
            .map(|id| {
                let body = format!("post number {id}");
                ManifestSource {
                    revision_id: Uuid::now_v7(),
                    channel_id: self.id,
                    channel_username: USERNAME.to_owned(),
                    channel_display_name: None,
                    message_id: *id,
                    content_sha256: sha256_hex(body.as_bytes()),
                    published_at: in_window(*id),
                    canonical_link: format!("https://t.me/{USERNAME}/{id}"),
                    body,
                    revision_index: 1,
                }
            })
            .collect();
        let manifest_id = Uuid::now_v7();
        let manifest = ManifestBuilder::build(
            manifest_id,
            self.owner,
            run_id,
            WINDOW_START,
            WINDOW_END,
            &sources,
        )?;
        let request = serde_json::to_vec(&serde_json::json!({
            "operation_id": Uuid::now_v7(),
            "owner": format!("user:{}", self.owner),
            "digest_run_id": run_id,
            "window": {"start_at": WINDOW_START, "end_at": WINDOW_END},
            "output_language": "ru",
            "source_count": manifest.source_count,
            "channel_count": manifest.channel_count,
            "manifest_ref": format!("channel-digest-manifest:{manifest_id}"),
            "manifest_digest": {"algorithm": "sha256", "hex": manifest.sha256},
            "analysis_family": "channel_digest_recap",
            "analysis_contract": "channel_digest_recap.v1"
        }))?;
        DigestCoordinator::new(self.database.pool().clone())
            .commit_manifest(&manifest, &request)
            .await?;
        Ok(manifest_id)
    }

    /// Accepts a fresh on-demand run of the owner over the test window.
    async fn accept_run(&self) -> Result<Uuid, Box<dyn std::error::Error>> {
        let run_id = Uuid::now_v7();
        let command = serde_json::to_vec(&serde_json::json!({
            "operation_id": Uuid::now_v7(),
            "owner": format!("user:{}", self.owner),
            "digest_run_id": run_id,
            "idempotency_key": format!("withdrawn-{run_id}"),
            "window": {"start_at": WINDOW_START, "end_at": WINDOW_END},
            "output_language": "ru",
            "trigger": {"kind": "on_demand", "accepted_at": WINDOW_END}
        }))?;
        CommandIntake::new(self.database.pool().clone())
            .accept_run(Uuid::now_v7(), &command)
            .await?;
        Ok(run_id)
    }

    /// Whether the manifest text is gone and whether the minimization instant is recorded.
    async fn manifest_state(&self, manifest_id: Uuid) -> Result<(bool, bool), sqlx::Error> {
        sqlx::query_as(
            "select canonical_text is null, minimized_at is not null
             from channel_digests.digest_manifests where manifest_id = $1",
        )
        .bind(manifest_id)
        .fetch_one(self.database.pool())
        .await
    }

    /// Stores another body for a message already observed, so it holds two revisions.
    async fn append_edit(&self, message_id: i64, body: &str) -> TestResult {
        self.append(message_id, body, &in_window(message_id)).await
    }

    /// Stores a message published before the window, as an earlier run's window would have.
    async fn append_old(&self, message_id: i64) -> TestResult {
        self.append(message_id, "ancient one", BEFORE_THE_WINDOW)
            .await
    }

    async fn append(&self, message_id: i64, body: &str, published_at: &str) -> TestResult {
        RevisionRepository::new(self.database.pool().clone())
            .append(&ObservedRevision {
                channel_id: self.id,
                provider_message_id: message_id,
                body,
                canonical_link: &format!("https://t.me/{USERNAME}/{message_id}"),
                published_at,
                observed_at: WINDOW_END,
            })
            .await?;
        Ok(())
    }

    /// Message ids with at least one withdrawn revision, ascending.
    async fn withdrawn_ids(&self) -> Result<Vec<i64>, sqlx::Error> {
        let rows: Vec<(i64,)> = sqlx::query_as(
            "select distinct provider_message_id from channel_digests.post_revisions
             where channel_id = $1 and deleted_at is not null order by 1",
        )
        .bind(self.id)
        .fetch_all(self.database.pool())
        .await?;
        Ok(rows.into_iter().map(|row| row.0).collect())
    }
}

fn in_window(message_id: i64) -> String {
    format!(
        "2026-08-28T{:02}:{:02}:00Z",
        10 + message_id / 60,
        message_id % 60
    )
}

fn post(message_id: i64, body: &str, published_at: &str) -> ProviderPost {
    ProviderPost {
        message_id,
        body: body.to_owned(),
        published_at: published_at.to_owned(),
        deleted: false,
    }
}

/// One channel served newest first by cursor, like a real one, whose posts a test can change
/// between walks.
#[derive(Debug, Default)]
struct ChannelPages {
    state: Mutex<PagesState>,
}

#[derive(Debug, Default)]
struct PagesState {
    posts: Vec<ProviderPost>,
    calls: usize,
    fail_from_call: Option<usize>,
}

impl ChannelPages {
    fn with_state<T>(&self, change: impl FnOnce(&mut PagesState) -> T) -> Result<T, &'static str> {
        self.state
            .lock()
            .map(|mut state| change(&mut state))
            .map_err(|_| "the provider state is poisoned")
    }

    fn push(&self, post: ProviderPost) -> Result<(), &'static str> {
        self.with_state(|state| state.posts.push(post))
    }

    fn remove(&self, message_id: i64) -> Result<(), &'static str> {
        self.with_state(|state| state.posts.retain(|post| post.message_id != message_id))
    }

    fn set_body(&self, message_id: i64, body: &str) -> Result<(), &'static str> {
        self.with_state(|state| {
            for post in &mut state.posts {
                if post.message_id == message_id {
                    body.clone_into(&mut post.body);
                }
            }
        })
    }

    fn mark_deleted(&self, message_id: i64) -> Result<(), &'static str> {
        self.with_state(|state| {
            for post in &mut state.posts {
                if post.message_id == message_id {
                    post.deleted = true;
                }
            }
        })
    }

    /// Fails every page request from the `call`-th of the next walk on, counting from one.
    fn fail_from_call(&self, call: usize) -> Result<(), &'static str> {
        self.with_state(|state| {
            state.calls = 0;
            state.fail_from_call = Some(call);
        })
    }
}

impl PublicChannelProvider for ChannelPages {
    type Channel = String;

    async fn resolve_public_channel(
        &self,
        username: &PublicChannelUsername,
    ) -> Result<Self::Channel, ProviderError> {
        Ok(username.as_str().to_owned())
    }

    async fn fetch_public_posts(
        &self,
        _channel: &Self::Channel,
        before_message_id: Option<i64>,
        limit: usize,
    ) -> Result<ProviderPage, ProviderError> {
        let mut state = self.state.lock().map_err(|_| ProviderError::Unavailable)?;
        state.calls += 1;
        if state.fail_from_call.is_some_and(|call| state.calls >= call) {
            return Err(ProviderError::Unavailable);
        }
        let mut eligible: Vec<ProviderPost> = state
            .posts
            .iter()
            .filter(|post| before_message_id.is_none_or(|before| post.message_id < before))
            .cloned()
            .collect();
        eligible.sort_by_key(|post| std::cmp::Reverse(post.message_id));
        eligible.truncate(limit);
        let next_before_message_id = (eligible.len() == limit)
            .then(|| eligible.last().map(|post| post.message_id))
            .flatten();
        Ok(ProviderPage {
            posts: eligible,
            next_before_message_id,
        })
    }
}
