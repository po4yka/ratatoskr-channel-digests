# Channel Digests requirements

## Goals

1. Acquire posts from explicitly subscribed public Telegram channels through one operator-authorized MTProto account.
2. Preserve immutable, append-only post revisions and construct deterministic, byte-identical digest manifests.
3. Coordinate recap analysis with Knowledge and settle run outcomes only from matching durable evidence.
4. Serve owner-scoped, service-authenticated loopback reads of subscriptions, manifests, and results, including a verified read-through of Knowledge-owned recaps.
5. Consume typed subscription, run, and schedule-occurrence commands and Knowledge completion/failure events through exact pre-provisioned JetStream consumers.

## Non-goals

Owning LLM inference, Platform's public facade or schedule authority, Bot API interaction or Telegram notification delivery, private channels/groups/personal dialogs/invite links, join/leave side effects, continuous account history mirroring, or storing recap narrative, provider credentials, or session bytes outside the worker's encrypted session files.

## Requirements

- Configuration is strict, finite, and role-separated; the API role listens on `127.0.0.1:8098` with no provider credential, and unknown or invalid settings fail without exposing values.
- Subscriptions are owner-scoped and idempotent under redelivery: usernames normalize to lowercase, activation is preserved across repeated enable/disable, and at most 20 subscriptions stay active per owner.
- Provider session and channel policy fail closed: only the worker reads the separate session-ciphertext and key files, and the provider adapter accepts only public usernames with no dialog, invite, or join/leave capability.
- Acquisition is bounded and restart-safe: duplicate observations converge, changed normalized content appends an immutable revision, and partial, flood-wait, or timeout outcomes checkpoint durably and resume without erasing successful channels.
- Runs and manifests are deterministic: on-demand windows cover the trailing 24 hours, scheduled windows cover the previous occurrence through the current one capped at seven days, natural-key replay reuses one run, and canonical manifest bytes are identical regardless of input order.
- API and command intake enforce service and owner authority with finite body/page bounds; foreign and missing subscriptions, manifests, and results return the same absence response, and duplicate commands produce one domain effect.
- Knowledge exchange and schedule execution are replay-safe: a non-empty manifest causes exactly one typed recap request, completion or failure settles only when owner, run, manifest digest, and result identity match durable evidence, and scheduled occurrence intake commits one inbox decision per owner without creating extra runs.
- Completed and partial results are projected only after the owner-authorized read verifies the Knowledge analysis identity and result digest against the stored completion fact; failed results stay local and safe; recap narrative is never stored by Channel Digests.
- Retention, telemetry, and shutdown preserve privacy and recovery: session bytes and post bodies never enter logs, events, metrics, or diagnostics, and both processes drain and checkpoint within a finite shutdown budget.

Core flow: an owner-authorized subscription accepts a public channel, an on-demand or scheduled run acquires and manifests eligible revisions, Knowledge produces a recap from that manifest, and the owner reads the linked result through the loopback API only after linkage verification.
