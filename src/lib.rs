#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Public-channel digest bounded-context library.

mod acquisition;
mod api;
mod bus;
mod config;
mod coordinator;
mod database;
mod envelopes;
mod executor;
mod intake;
mod maintenance;
mod manifest;
mod provider;
mod reaper;
mod registration;
mod reports;
mod result_reader;
mod revisions;
mod runs;
mod runtime;
mod session;
mod subscriptions;

pub use acquisition::{AcquisitionEngine, AcquisitionError, AcquisitionReport, AcquisitionRequest};
pub use bus::{
    BusError, ConsumerSet, DeliveryDisposition, WorkerMessageHandler, connect, publish_message,
    verify_consumers,
};
pub use config::{BusConfig, Config, ConfigError, Role, ScheduleConfig};
pub use coordinator::{CoordinatorError, DigestCoordinator, OccurrenceRequest};
pub use database::{Database, DatabaseError};
pub use envelopes::{OutboundMessage, OutboxRow, wrap_outbox_row};
pub use executor::{RunExecutionError, RunExecutor};
pub use intake::{CommandIntake, CommandKind, IntakeError, IntakeOutcome};
pub use maintenance::{Maintenance, MaintenanceError};
pub use manifest::{CanonicalManifest, ManifestBuilder, ManifestError, ManifestSource};
pub use provider::{
    MtProtoPublicChannelProvider, ProviderError, ProviderPage, ProviderPost, PublicChannelProvider,
    PublicChannelUsername, ResolvedPublicChannel,
};
pub use reaper::{Reaper, ReaperError};
pub use registration::{RegistrationError, RegistrationOutcome, enqueue_schedule_registration};
pub use result_reader::{
    KnowledgeResultProjection, KnowledgeResultReadError, KnowledgeResultReader,
};
pub use revisions::{ObservedRevision, RevisionError, RevisionRepository};
pub use runs::{DigestRun, RunError, RunRepository, RunState, RunTrigger};
pub use runtime::{RuntimeError, run_api, run_worker};
pub use session::{SessionError, SessionMaterial};
pub use subscriptions::{Subscription, SubscriptionError, SubscriptionRepository};
