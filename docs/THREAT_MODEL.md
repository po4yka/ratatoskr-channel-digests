# Threat Model

The highest-impact authority is the consented MTProto session. Only the worker can express its
ciphertext/key settings; files are separate, regular, owner-only (`0600`) artifacts. Decryption and
authorization fail closed, diagnostics contain stable classes rather than values or paths, and the
API process has no provider configuration.

Provider input is untrusted. The adapter admits only canonical public usernames and provides no
dialog enumeration, numeric peer, invite, private/group, join, or leave operation. Calls, pages,
post sizes, selected sources, channels, retries, concurrency, and shutdown are finite. Provider
partial failures remain partial and never become an empty success.

HTTP trusts only loopback plus a fixed service credential and explicit internal owner claim.
JetStream trusts only exact producer, subject, envelope, tenant, and typed payload combinations.
Inbox/outbox identities make redelivery safe. Dynamic bodies stay behind owner-authorized manifest
retrieval and never enter normal events, logs, metrics, or diagnostics.

Residual boundaries: synthetic fixtures do not prove live Telegram consent, provider flood behavior,
fleet NATS permissions, Linux systemd activation, or production deployment. Those require separate
operator evidence and must not be inferred from this repository's green gate.
