//! Canonical manifest determinism acceptance.

use ratatoskr_channel_digests::{ManifestBuilder, ManifestSource};
use uuid::Uuid;

#[test]
fn canonical_manifest_is_stable_bounded_and_integral() {
    let run_id = Uuid::now_v7();
    let first = source(1, "bravo", "11");
    let second = source(2, "alpha", "22");
    let a = ManifestBuilder::build(
        run_id,
        "2026-08-20T10:00:00Z",
        "2026-08-21T10:00:00Z",
        vec![first.clone(), second.clone()],
    )
    .expect("bounded manifest");
    let b = ManifestBuilder::build(
        run_id,
        "2026-08-20T10:00:00Z",
        "2026-08-21T10:00:00Z",
        vec![second, first],
    )
    .expect("same sources in another order");
    assert_eq!(a.bytes, b.bytes);
    assert_eq!(a.sha256, b.sha256);
    assert_eq!(a.source_count, 2);
    assert_eq!(a.channel_count, 2);
    assert!(
        String::from_utf8(a.bytes)
            .expect("JSON")
            .contains("https://t.me/alpha/2")
    );

    let too_many = (0..101).map(|id| source(id, "alpha", "33")).collect();
    assert!(
        ManifestBuilder::build(
            run_id,
            "2026-08-20T10:00:00Z",
            "2026-08-21T10:00:00Z",
            too_many,
        )
        .is_err()
    );
}

fn source(message_id: i64, username: &str, digest_pair: &str) -> ManifestSource {
    ManifestSource {
        revision_id: Uuid::now_v7(),
        channel_username: username.to_owned(),
        message_id,
        content_sha256: digest_pair.repeat(32),
        published_at: "2026-08-20T12:00:00Z".to_owned(),
        canonical_link: format!("https://t.me/{username}/{message_id}"),
        body: format!("body {message_id}"),
    }
}
