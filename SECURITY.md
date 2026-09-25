# Security Policy for Ratatoskr Channel Digests

Report vulnerabilities privately. Do not publish MTProto session ciphertext or its decryption key, provider API credentials, the loopback service-authentication secret, the Knowledge result-reader bearer secret, owner or channel identifiers tied to a person, raw post bodies or normalized content bytes, recap narrative, or production configuration values.

Security review is required for MTProto session provisioning and rotation, session ciphertext/key file handling, provider adapter scope (public usernames only), loopback service authentication and owner scoping, JetStream consumer topology, the Knowledge result-reader credential and response verification, and any change to telemetry, logs, or diagnostics.

Baseline: one operator-authorized MTProto session, readable only by the worker, with fail-closed decryption and no dialog/invite/join/leave capability; the API holds no provider credential; every read and mutation is owner scoped and foreign/absent resources are indistinguishable; only exact pre-provisioned JetStream consumers are opened, never created or widened; the Knowledge result reader is a dedicated bounded loopback client with no redirects or retries; session bytes, credentials, and post/recap bodies never enter PostgreSQL, events, logs, metrics, fixtures, or diagnostics.
