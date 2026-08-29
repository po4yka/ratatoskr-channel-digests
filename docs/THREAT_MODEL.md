# Threat Model

The highest-impact authority is the consented MTProto session. Only the worker can express its
ciphertext/key settings; files are separate, regular, owner-only (`0600`) artifacts. Decryption and
authorization fail closed, diagnostics contain stable classes rather than values or paths, and the
API process has no provider configuration.

Provider input is untrusted. The adapter admits only canonical public usernames and provides no
dialog enumeration, numeric peer, invite, private/group, join, or leave operation. Calls, pages,
post sizes, selected sources, channels, retries, concurrency, and shutdown are finite. Provider
partial failures remain partial and never become an empty success.

HTTP trusts only loopback plus a fixed service credential and explicit internal owner claim. The API
alone holds a second, dedicated bearer credential directed only to the numeric-loopback Knowledge
result-reader origin; the worker cannot represent it. The client ignores proxy settings, does not
redirect or retry, and bounds connection time, total request time, and response bytes. Local owner
resolution precedes dependency access, so foreign and absent result IDs cannot probe Knowledge.
JetStream trusts only exact producer, subject, envelope, tenant, and typed payload combinations.
Inbox/outbox identities make redelivery safe. Dynamic bodies stay behind owner-authorized manifest
retrieval and never enter normal events, logs, metrics, or diagnostics.

Completed and partial projections must match both the stored Knowledge analysis UUID and the exact
SHA-256 from the accepted completion fact. Any transport, envelope, syntax, identity, or digest
failure emits no recap bytes. Recap narrative remains transient response data and is neither stored
in Channel Digests nor included in ordinary diagnostics; reader errors use finite safe classes with
no owner, result, analysis, URL, secret, or upstream body labels.

Residual boundaries: synthetic fixtures do not prove live Telegram consent, provider flood behavior,
fleet NATS permissions, Linux systemd activation, or production deployment. Those require separate
operator evidence and must not be inferred from this repository's green gate.
