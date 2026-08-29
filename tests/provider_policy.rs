//! Public-username-only capability acceptance.

use std::sync::atomic::{AtomicUsize, Ordering};

use ratatoskr_channel_digests::{ProviderError, PublicChannelProvider, PublicChannelUsername};

#[tokio::test]
async fn only_public_usernames_can_reach_resolution() {
    let provider = FakeProvider::default();
    for invalid in [
        "https://t.me/+invite",
        "https://t.me/example/42",
        "t.me/example",
        "+invite_hash",
        "-100123456789",
        "123456789",
        "@private",
        "Example_Channel",
        "group chat",
    ] {
        let parsed = PublicChannelUsername::parse(invalid);
        assert!(parsed.is_err(), "accepted non-public locator {invalid:?}");
    }
    assert_eq!(provider.calls.load(Ordering::Relaxed), 0);

    let username = PublicChannelUsername::parse("example_channel").expect("public username");
    assert_eq!(
        provider.resolve_public_channel(&username).await,
        Ok("example_channel".to_owned())
    );
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
}

#[derive(Debug, Default)]
struct FakeProvider {
    calls: AtomicUsize,
}

impl PublicChannelProvider for FakeProvider {
    type Channel = String;

    async fn resolve_public_channel(
        &self,
        username: &PublicChannelUsername,
    ) -> Result<Self::Channel, ProviderError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(username.as_str().to_owned())
    }

    async fn fetch_public_posts(
        &self,
        _channel: &Self::Channel,
        _before_message_id: Option<i64>,
        _limit: usize,
    ) -> Result<ratatoskr_channel_digests::ProviderPage, ProviderError> {
        Ok(ratatoskr_channel_digests::ProviderPage {
            posts: Vec::new(),
            next_before_message_id: None,
        })
    }
}
