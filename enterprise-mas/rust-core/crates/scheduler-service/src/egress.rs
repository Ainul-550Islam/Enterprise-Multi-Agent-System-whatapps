// PURPOSE: Scheduler compatibility export for the canonical integration
// egress adapter.
//
// Owning layer: `mas-scheduler-service` process edge.
//
// Production responsibility: keep existing scheduler construction code bound
// to the single `mas-integrations` concrete transport instead of maintaining a
// second implementation with divergent DNS, TLS, redirect, or response-limit
// behavior.
//
// Security boundary: this module opens no sockets and adds no policy. All
// outbound behavior lives in `mas_integrations::reqwest_egress::ReqwestEgress`
// beneath `GuardedHttpClient`.
//
// It must NOT: reintroduce a local Reqwest client, relax SSRF checks, handle
// credentials, or make delivery decisions.

pub use mas_integrations::ReqwestEgress;
