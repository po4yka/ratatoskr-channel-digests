#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Public-channel digest bounded-context library.

mod acquisition;
mod api;
mod bus;
mod config;
mod coordinator;
mod database;
mod executor;
mod intake;
mod maintenance;
mod manifest;
mod provider;
mod revisions;
mod runs;
mod runtime;
mod session;
mod subscriptions;

pub use acquisition::{AcquisitionEngine, AcquisitionError, AcquisitionReport, AcquisitionRequest};
pub use bus::{DeliveryDisposition, WorkerMessageHandler};
pub use config::{Config, ConfigError, Role};
pub use coordinator::{CoordinatorError, DigestCoordinator};
pub use database::{Database, DatabaseError};
pub use executor::{RunExecutionError, RunExecutor};
pub use intake::{CommandIntake, IntakeError, IntakeOutcome};
pub use maintenance::{Maintenance, MaintenanceError};
pub use manifest::{CanonicalManifest, ManifestBuilder, ManifestError, ManifestSource};
pub use provider::{
    MtProtoPublicChannelProvider, ProviderError, ProviderPage, ProviderPost, PublicChannelProvider,
    PublicChannelUsername, ResolvedPublicChannel,
};
pub use revisions::{ObservedRevision, RevisionError, RevisionRepository};
pub use runs::{DigestRun, RunError, RunRepository, RunState, RunTrigger};
pub use runtime::{RuntimeError, run_api, run_worker};
pub use session::{SessionError, SessionMaterial};
pub use subscriptions::{Subscription, SubscriptionError, SubscriptionRepository};
