//! PURPOSE: The concrete PostgreSQL repositories, and the split between them.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: keep two flavours distinct and explicit —
//! scoped repositories that take a tenant on every call, and unit-of-work
//! repositories that only ever run inside a caller's transaction. The split is
//! what stops a repository from quietly borrowing a second connection.
//!
//! It must NOT:
//!   • re-export a store without its tenant-scoped variant;
//!   • let a `*_in` method open its own transaction;
//!   • hide a connection checkout behind a convenience wrapper.

//! Concrete PostgreSQL repositories.
//!
//! Two flavors:
//!
//! * **Scoped repositories** (`scoped`) accept an explicit
//!   [`crate::rls::RlsContext`] per call — app traffic, fail-closed by RLS.
//! * **Service repositories** ([`outbox`], [`audit`]) are written for the
//!   dedicated BYPASSRLS dispatcher/audit roles documented in the migration
//!   set; their APIs make the cross-tenant intent explicit instead of hiding
//!   it behind convenience.

pub mod agents;
pub mod analytics;
pub mod api_keys;
pub mod approvals;
pub mod audit;
pub mod broker;
pub mod connector_lifecycle;
pub mod connectors;
pub mod control_plane;
pub mod credentials;
pub mod data_hub;
pub mod delivery_receipts;
pub mod execution_steps;
pub mod executions;
pub mod inbound_webhooks;
pub mod notifications;
pub mod oauth_cleanup;
pub mod oauth_credentials;
pub mod oauth_materialization;
pub mod oauth_pkce;
pub mod oauth_refresh;
pub mod oauth_states;
pub mod outbox;
pub mod rollbacks;
pub mod schedules;
mod scoped;
pub mod search;
pub mod secret_references;
pub mod services;
pub mod tasks;
pub mod tenancy;
pub mod tools;
pub mod usage;
pub mod webhook_admin;
pub mod webhook_relay;
pub mod webhooks;
pub mod workflow_versions;
pub mod workflows;

pub use crate::durable::PgDurableExecution;
pub use analytics::PostgresAnalyticsStore;
pub use approvals::PostgresApprovalStore;
pub use audit::PostgresAuditLog;
pub use connector_lifecycle::PostgresConnectorLifecycle;
pub use connectors::PostgresConnectorStore;
pub use control_plane::{
    build_postgres_control_plane, ControlPlaneStores, PostgresBillingWebhookStore,
    PostgresCounterStore, PostgresEntitlementStore, PostgresOverrideStore, PostgresPlanResolver,
    PostgresPlanStore, PostgresPolicyStore, PostgresQuotaRegistry, PostgresReservationStore,
    PostgresSubscriptionStore, PostgresUsageLedger,
};
pub use credentials::PostgresConnectorCredentialLookup;
pub use data_hub::{PostgresDataSourceStore, PostgresDocumentStore};
pub use delivery_receipts::PostgresDeliveryReceiptStore;
pub use execution_steps::PostgresExecutionStepStore;
pub use inbound_webhooks::{
    PostgresInboundWebhookEndpointStore, PostgresInboundWebhookReceiptStore,
};
pub use notifications::{PostgresNotificationDeliveryStore, PostgresNotificationStore};
pub use oauth_cleanup::PostgresOAuthTokenCleanupStore;
pub use oauth_credentials::PostgresOAuthCredentialStore;
pub use oauth_materialization::PostgresOAuthMaterializationStore;
pub use oauth_pkce::PostgresOAuthPkceSecretStore;
pub use oauth_refresh::PostgresOAuthTokenRefreshStore;
pub use oauth_states::PostgresOAuthStateStore;
pub use outbox::PostgresOutboxStore;
pub use scoped::{DocumentTable, ScopedStore};
pub use search::PostgresSearchCatalog;
pub use secret_references::{PostgresRelaySecretReferenceLookup, PostgresSecretReferenceStore};
pub use tenancy::{MembershipStore, OrganizationStore, ProjectStore, TenantStore, UserStore};
pub use tools::PostgresToolStore;
pub use usage::PostgresUsageStore;
pub use webhook_admin::PostgresWebhookAdmin;
pub use webhook_relay::PostgresWebhookRelayStore;
pub use webhooks::PostgresWebhookStore;
