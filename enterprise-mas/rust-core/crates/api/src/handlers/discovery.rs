//! PURPOSE: Deployment-surface discovery — the OpenAPI document, AI model
//! provider status, and messaging-platform provisioning status.
//!
//! Every answer here is computed from what the deployment actually composed
//! at boot (config presence, service wiring): these routes report state, they
//! never invent capability. `GET /v1/openapi.json` serves the same document
//! `docs/openapi.yaml` describes, as plain JSON (the OpenAPI document itself,
//! not an API envelope — that is what spec tooling expects at this URL).

use axum::extract::{Path, State};
use axum::response::IntoResponse as _;
use mas_common::error::AppError;
use serde::Serialize;

use crate::handlers::Ctx;
use crate::response::{ok, ApiResult};
use crate::state::AppState;

/// AI provider availability in this deployment, computed once at composition.
#[derive(Debug, Clone)]
pub struct ModelCatalog {
    providers: Vec<ModelProviderStatus>,
}

/// Status of one AI model provider as this deployment knows it.
#[derive(Debug, Clone, Serialize)]
pub struct ModelProviderStatus {
    /// Provider name (`openai`, `anthropic`, `gemini`, `azure_openai`).
    pub provider: String,
    /// Whether the deployment composes credentials/configuration for it.
    pub configured: bool,
    /// `configured` | `unconfigured`.
    pub status: String,
}

impl ModelCatalog {
    /// Reads provider credential presence from the config merger (env
    /// `MAS_AI_<PROVIDER>_API_KEY` or config `ai.<provider>.api_key`). Only
    /// the boolean is kept — never the credential.
    #[must_use]
    pub fn from_merger(merger: &mas_common::config::Merger<'_>) -> Self {
        let providers = [
            (
                "anthropic",
                "MAS_AI_ANTHROPIC_API_KEY",
                "ai.anthropic.api_key",
            ),
            (
                "azure_openai",
                "MAS_AI_AZURE_OPENAI_API_KEY",
                "ai.azure_openai.api_key",
            ),
            ("gemini", "MAS_AI_GEMINI_API_KEY", "ai.gemini.api_key"),
            ("openai", "MAS_AI_OPENAI_API_KEY", "ai.openai.api_key"),
        ]
        .into_iter()
        .map(|(name, env_key, path)| {
            let configured = !merger.string(env_key, path, "").trim().is_empty();
            ModelProviderStatus {
                provider: name.to_owned(),
                status: if configured {
                    "configured".to_owned()
                } else {
                    "unconfigured".to_owned()
                },
                configured,
            }
        })
        .collect();
        Self { providers }
    }

    /// All known providers, in stable order.
    #[must_use]
    pub fn providers(&self) -> &[ModelProviderStatus] {
        &self.providers
    }
}

/// Status of one messaging platform the deployment can compose.
#[derive(Debug, Clone, Serialize)]
pub struct PlatformStatus {
    /// Platform name (`whatsapp`).
    pub name: String,
    /// Whether the platform's service was composed at boot.
    pub provisioned: bool,
    /// `ok` | `unprovisioned`.
    pub status: String,
    /// Human-readable explanation of the status.
    pub detail: String,
}

/// `GET /v1/openapi.json` — the OpenAPI document for this deployment's API,
/// served as plain JSON so spec tooling can consume it directly. It is the
/// same contract `docs/openapi.yaml` documents; the parity test keeps the
/// router, the YAML, and this JSON aligned.
pub async fn openapi_json() -> axum::response::Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/json; charset=utf-8",
        )],
        crate::openapi_bundle::OPENAPI_JSON,
    )
        .into_response()
}

/// `GET /v1/models/status` — AI provider availability for this deployment.
pub async fn models_status(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<ModelProviderStatus>> {
    let catalog = state.model_catalog(&ctx)?;
    Ok(ok(&ctx, catalog.providers().to_vec()))
}

/// `GET /v1/models/status/{provider}` — one provider's availability.
pub async fn models_status_provider(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Ctx(ctx): Ctx,
) -> ApiResult<ModelProviderStatus> {
    let catalog = state.model_catalog(&ctx)?;
    catalog
        .providers()
        .iter()
        .find(|p| p.provider == provider)
        .cloned()
        .map(|p| ok(&ctx, p))
        .ok_or_else(|| {
            crate::response::ApiError::new(AppError::not_found("model_provider", provider), &ctx)
        })
}

/// `GET /v1/platforms` — messaging platforms and their provisioning state.
pub async fn platforms(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<PlatformStatus>> {
    Ok(ok(&ctx, vec![whatsapp_status(&state)]))
}

/// `GET /v1/platforms/whatsapp/health` — the WhatsApp plane's status. A
/// health *report* of the platform's composed state: the check itself always
/// succeeds and describes reality, whether the plane is provisioned or not.
pub async fn whatsapp_health(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
) -> ApiResult<PlatformStatus> {
    Ok(ok(&ctx, whatsapp_status(&state)))
}

fn whatsapp_status(state: &AppState) -> PlatformStatus {
    if state.whatsapp.is_some() {
        PlatformStatus {
            name: "whatsapp".to_owned(),
            provisioned: true,
            status: "ok".to_owned(),
            detail: "the WhatsApp messaging plane is composed in this deployment".to_owned(),
        }
    } else {
        PlatformStatus {
            name: "whatsapp".to_owned(),
            provisioned: false,
            status: "unprovisioned".to_owned(),
            detail: "this deployment has not composed the WhatsApp plane (MAS_WHATSAPP_* configuration absent)"
                .to_owned(),
        }
    }
}
