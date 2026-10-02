//! The typed HTTP client: builds requests, decodes [`ApiEnvelope<T>`],
//! surfaces `StableApiError` wholesale. Never prints; never knows about
//! exit codes (that's [`crate::commands`]).

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_contracts::api::ApiEnvelope;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Client configuration (from [`crate::GlobalOpts`]).
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// API base URL (`http://host:port`).
    pub base_url: String,
    /// Bearer token.
    pub token: String,
    /// `x-tenant-id`.
    pub tenant: Option<uuid::Uuid>,
    /// `x-organization-id`.
    pub organization: Option<uuid::Uuid>,
    /// `x-correlation-id`; auto-generated when absent.
    pub correlation_id: String,
    /// Per-request timeout.
    pub request_timeout: std::time::Duration,
}

impl ClientConfig {
    /// Sanity (base URL must be absolute http(s); correlation falls back).
    pub fn validated(mut self) -> Result<Self> {
        if !(self.base_url.starts_with("http://") || self.base_url.starts_with("https://")) {
            return Err(AppError::validation(
                "base_url must start with http:// or https://",
            ));
        }
        if self.correlation_id.trim().is_empty() {
            self.correlation_id = uuid::Uuid::now_v7().to_string();
        }
        Ok(self)
    }

    /// Builds a request builder with auth + scope headers applied.
    fn prepare(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut request = request
            .bearer_auth(&self.token)
            .header("x-correlation-id", &self.correlation_id)
            .header("accept", "application/json");
        if let Some(tenant) = &self.tenant {
            request = request.header("x-tenant-id", tenant.to_string());
        }
        if let Some(org) = &self.organization {
            request = request.header("x-organization-id", org.to_string());
        }
        request
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}/{}/{}",
            self.base_url.trim_end_matches('/'),
            "v1",
            path.trim_start_matches('/')
        )
    }
}

/// One decoded response: the data payload plus wire metadata the operator
/// may care about (idempotent replay).
#[derive(Debug)]
pub struct Response<T> {
    /// The decoded `data` field.
    pub data: T,
    /// Server-correlation id (echoing back the client's own unless rotated).
    pub correlation_id: String,
    /// `X-Idempotent-Replay: true` was present (mutating POST replays).
    pub idempotent_replay: bool,
}

/// An API-side failure (stable contract, no internals).
#[derive(Debug, Clone)]
pub struct ApiFailure {
    /// HTTP status.
    pub status: u16,
    /// Stable machine code (`VALIDATION`, `CONFLICT`, …).
    pub code: String,
    /// Safe message.
    pub message: String,
    /// Server request id (log correlation).
    pub request_id: Option<uuid::Uuid>,
    /// Optional details blob (validation issues…).
    pub details: Option<serde_json::Value>,
}

/// Client outcomes: typed success, typed API failure, or transport.
#[derive(Debug)]
pub enum Outcome<T> {
    /// Envelope decoded with data.
    Ok(Response<T>),
    /// Envelope decoded with a stable error (or a non-envelope non-2xx).
    Err(ApiFailure),
}

impl<T> Outcome<T> {
    /// Unwraps or converts to an [`AppError`] (transport stays `external_service`).
    pub fn into_result(self) -> Result<T> {
        match self {
            Outcome::Ok(resp) => Ok(resp.data),
            Outcome::Err(failure) => Err(AppError::external_service(
                "mas-api",
                format!("{} ({})", failure.code, failure.message),
            )),
        }
    }
}

/// The synchronous-in-shape, async-in-body API client.
#[derive(Debug, Clone)]
pub struct MasApiClient {
    config: ClientConfig,
    http: reqwest::Client,
}

impl MasApiClient {
    /// Builds a client (validates the config).
    pub fn new(config: ClientConfig) -> Result<Self> {
        let config = config.validated()?;
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|err| AppError::external_service("http-client", err.to_string()))?;
        Ok(Self { config, http })
    }

    /// The effective config (post-validation fallbacks applied).
    #[must_use]
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Full URL for a `/v1/…` path (tests + debugging).
    #[must_use]
    pub fn url(&self, path: &str) -> String {
        self.config.url(path)
    }

    async fn decode<T: DeserializeOwned>(&self, response: reqwest::Response) -> Outcome<T> {
        let status = response.status().as_u16();
        let idempotent_replay = response
            .headers()
            .get("x-idempotent-replay")
            .is_some_and(|v| v == "true");
        let correlation_id = response
            .headers()
            .get("x-correlation-id")
            .and_then(|v| v.to_str().ok())
            .unwrap_or(&self.config.correlation_id)
            .to_owned();
        let body = response.text().await.unwrap_or_default();
        // Decode the envelope around Value first — `ApiEnvelope<T>`'s
        // serde(default) fields carry an inherited `T: Default` derive bound
        // at generic decode time; two-phase decoding keeps `T` free of it.
        if let Ok(envelope) = serde_json::from_str::<ApiEnvelope<serde_json::Value>>(&body) {
            if let Some(error) = envelope.error {
                return Outcome::Err(ApiFailure {
                    status,
                    code: error.code,
                    message: error.message,
                    request_id: error.request_id,
                    details: error.details,
                });
            }
            if let Some(data) = envelope.data {
                if let Ok(typed) = serde_json::from_value::<T>(data) {
                    return Outcome::Ok(Response {
                        data: typed,
                        correlation_id,
                        idempotent_replay,
                    });
                }
            }
            // Neither data nor error — every v1 route returns one of them,
            // so this envelope is malformed (fall through to UNEXPECTED_BODY).
        }
        // Tolerant fallback only for 2xx (health probes and future
        // non-envelope dev routes): raw JSON, else plain text as a string.
        if (200..300).contains(&status) {
            if let Ok(data) = serde_json::from_str::<T>(&body) {
                return Outcome::Ok(Response {
                    data,
                    correlation_id,
                    idempotent_replay,
                });
            }
            if let Ok(data) =
                serde_json::from_value::<T>(serde_json::Value::String(body.trim().to_owned()))
            {
                return Outcome::Ok(Response {
                    data,
                    correlation_id,
                    idempotent_replay,
                });
            }
        }
        Outcome::Err(ApiFailure {
            status,
            code: "UNEXPECTED_BODY".to_owned(),
            message: format!("malformed response body ({} bytes)", body.len()),
            request_id: None,
            details: Some(serde_json::json!({ "snippet": &body[..body.len().min(256)] })),
        })
    }

    /// GET.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Outcome<T> {
        let request = self.http.get(self.url(path));
        let request = self.config.prepare(request);
        match request.send().await {
            Ok(resp) => self.decode(resp).await,
            Err(err) => transport(err),
        }
    }

    /// POST with a JSON body (+ optional `Idempotency-Key` header).
    pub async fn post<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
    ) -> Outcome<T> {
        let mut request = self.http.post(self.url(path)).json(body);
        if let Some(key) = idempotency_key {
            request = request.header("Idempotency-Key", key);
        }
        let request = self.config.prepare(request);
        match request.send().await {
            Ok(resp) => self.decode(resp).await,
            Err(err) => transport(err),
        }
    }

    /// POST without a body (transitions).
    pub async fn post_empty<T: DeserializeOwned>(&self, path: &str) -> Outcome<T> {
        self.post(path, &serde_json::json!({}), None).await
    }
}

fn transport<T>(err: reqwest::Error) -> Outcome<T> {
    Outcome::Err(ApiFailure {
        status: 0,
        code: "TRANSPORT".to_owned(),
        message: err.to_string(),
        request_id: None,
        details: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_validation() {
        let base = ClientConfig {
            base_url: "127.0.0.1:8080".to_owned(),
            token: "t".to_owned(),
            tenant: None,
            organization: None,
            correlation_id: String::new(),
            request_timeout: std::time::Duration::from_secs(10),
        };
        assert!(base.clone().validated().is_err(), "scheme required");
        let good = ClientConfig {
            base_url: "http://127.0.0.1:8080".to_owned(),
            ..base
        };
        let good = good.validated().expect("ok");
        assert!(
            !good.correlation_id.is_empty(),
            "correlation fallback minted"
        );
    }

    #[test]
    fn url_join_is_stable() {
        let config = ClientConfig {
            base_url: "http://a:1/".to_owned(),
            token: "t".to_owned(),
            tenant: None,
            organization: None,
            correlation_id: "c".to_owned(),
            request_timeout: std::time::Duration::from_secs(1),
        };
        assert_eq!(config.url("health/live"), "http://a:1/v1/health/live");
        assert_eq!(config.url("/v1/x"), "http://a:1/v1/v1/x"); // honest: caller passes v1-free paths
    }
}
