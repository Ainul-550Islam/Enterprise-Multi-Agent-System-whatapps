//! `mas-cli` — the operator command-line client for the multi-agent system.
//!
//! One HTTP client ([`client::MasApiClient`]), one argument parser
//! ([`args::Cli`]), and one command executor ([`commands::run`]) that maps
//! each subcommand onto a public API route from `mas-api`:
//!
//! ```text
//!   status                        GET  /v1/health/live + /v1/health/ready
//!   execution list                GET  /v1/executions          (scoped)
//!   execution get <id>            GET  /v1/executions/{id}     (scoped)
//!   execution submit ...          POST /v1/projects/{project_id}/executions
//!                                 (idempotent; --wait polls to a terminal state)
//!   execution <t> <id>            POST /v1/executions/{id}/{t}  t ∈ start|pause|resume|cancel
//!   schedule <t> <id>             POST /v1/schedules/{id}/{t}   t ∈ pause|resume|disable
//!   schedule list | get <id>      GET  /v1/schedules[/{id}]
//!   agent|workflow list|get       GET  /v1/{resource}[/{id}]
//! ```
//!
//! Output and errors mirror the wire contract exactly: [`MasApiClient`]
//! decodes the `ApiEnvelope<T>` and surfaces `StableApiError` values;
//! [`commands::run`] renders `data` (pretty JSON, or a brief key/field form
//! with `--format brief`) and translates failures into process exit codes
//! ([`commands::ExitCode`]). Registration/mutation of agents, workflows, and
//! tenancy stay out of scope here by design — this CLI is for operators
//! driving the platform (submitting, watching, pausing, diagnosing), and the
//! richer builder flows belong to provisioning tooling.
//!
//! Scope/membership headers (`x-tenant-id`, `x-organization-id`), a bearer
//! token, and an optional correlation id are attached to every request;
//! `Idempotency-Key` is supplied automatically for submits unless `--idempotency-key`
//! overrides it.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod args;
pub mod client;
pub mod commands;
pub mod render;

pub use args::{Cli, Command, Format, GlobalOpts};
pub use client::{ClientConfig, MasApiClient};
pub use commands::{run, ExitCode};
pub use render::{render_data, render_envelope_error};
