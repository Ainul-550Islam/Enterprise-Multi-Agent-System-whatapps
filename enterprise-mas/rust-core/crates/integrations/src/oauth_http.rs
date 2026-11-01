//! PURPOSE: Guarded HTTP OAuth token-endpoint adapter.
//!
//! Owning layer: `mas-integrations` infrastructure edge.
//!
//! Production role: execute OAuth 2.0 authorization-code and refresh-token
//! exchanges over the canonical guarded HTTP transport, parse only a bounded
//! standards-shaped JSON response, and return redacting `TokenBundle` values.
//!
//! Security boundary: token endpoint URLs were validated in `OAuth2Config` and
//! are rechecked by `GuardedHttpClient` immediately before connection; PKCE
//! verifier/refresh token values are form encoded only at the send point and
//! never appear in errors or logs. POST redirects are forbidden by the guard.
//!
//! Data ownership: the provider owns token responses; downstream credential
//! lifecycle ports decide how to write token material to an external manager.
//! This adapter stores nothing.
//!
//! Prohibited responsibilities: raw token persistence, client-secret fallback,
//! accepting HTTP success without a usable access token, or bypassing egress.

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde::Deserialize;

use crate::http_guard::{GuardedHttpClient, HttpClientPort, HttpRequest};
use crate::oauth::{OAuth2Config, TokenBundle, TokenEndpointPort};

const MAX_TOKEN_RESPONSE_BYTES: usize = 128 * 1024;
const MAX_TOKEN_VALUE_BYTES: usize = 16 * 1024;
const MAX_SCOPE_BYTES: usize = 4 * 1024;
const MAX_TOKEN_LIFETIME_SECS: u64 = 366 * 24 * 60 * 60;

/// Real OAuth token endpoint adapter for public/PKCE clients.
#[derive(Debug)]
pub struct HttpTokenEndpoint<C>
where
    C: HttpClientPort,
{
    client: GuardedHttpClient<C>,
}

impl<C> HttpTokenEndpoint<C>
where
    C: HttpClientPort,
{
    /// Uses the passed guarded transport exactly as configured by bootstrap.
    #[must_use]
    pub const fn new(client: GuardedHttpClient<C>) -> Self {
        Self { client }
    }

    async fn exchange_form(
        &self,
        config: &OAuth2Config,
        form: Vec<(String, String)>,
    ) -> Result<TokenBundle> {
        validate_form(&form)?;
        let encoded = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(
                form.iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            )
            .finish();
        let request = HttpRequest::post(config.token_endpoint.clone(), encoded.into_bytes())
            .with_header("content-type", "application/x-www-form-urlencoded")
            .with_header("accept", "application/json");
        let response = self.client.send(&request).await?;
        if !response.is_success() {
            return Err(AppError::external_service(
                "oauth_token_endpoint",
                format!("token endpoint returned HTTP status {}", response.status),
            ));
        }
        if response.body.len() > MAX_TOKEN_RESPONSE_BYTES {
            return Err(AppError::external_service(
                "oauth_token_endpoint",
                "token endpoint response exceeded the OAuth response size limit",
            ));
        }
        let parsed: TokenResponse = serde_json::from_slice(&response.body).map_err(|_| {
            AppError::external_service(
                "oauth_token_endpoint",
                "token endpoint returned malformed JSON",
            )
        })?;
        // Never reflect provider response fields in an error. Token error
        // fields are remote input and some non-conforming providers put
        // descriptive (occasionally material-bearing) text in them.
        if parsed.error.is_some() {
            return Err(AppError::unauthorized(
                "OAuth token endpoint rejected the authorization grant",
            ));
        }
        let token_type = parsed.token_type.ok_or_else(|| {
            AppError::external_service(
                "oauth_token_endpoint",
                "token endpoint response omitted token_type",
            )
        })?;
        if token_type.len() > 64
            || token_type.chars().any(char::is_control)
            || !token_type.eq_ignore_ascii_case("bearer")
        {
            return Err(AppError::external_service(
                "oauth_token_endpoint",
                "token endpoint returned an unsupported token type",
            ));
        }
        let access_token = parsed.access_token.ok_or_else(|| {
            AppError::external_service(
                "oauth_token_endpoint",
                "token endpoint response omitted access_token",
            )
        })?;
        validate_token_value(&access_token, "access token")?;
        if let Some(refresh_token) = &parsed.refresh_token {
            validate_token_value(refresh_token, "refresh token")?;
        }
        if parsed
            .expires_in
            .is_some_and(|seconds| seconds > MAX_TOKEN_LIFETIME_SECS)
        {
            return Err(AppError::external_service(
                "oauth_token_endpoint",
                "token endpoint returned an implausibly long token lifetime",
            ));
        }
        let scopes = parse_scopes(parsed.scope.as_deref())?;
        TokenBundle::from_parts(
            "Bearer",
            access_token,
            parsed.refresh_token,
            parsed.expires_in,
            scopes,
            &Timestamp::now(),
        )
    }
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    #[serde(default)]
    access_token: Option<String>,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

#[async_trait]
impl<C> TokenEndpointPort for HttpTokenEndpoint<C>
where
    C: HttpClientPort + 'static,
{
    async fn exchange_code(
        &self,
        config: &OAuth2Config,
        code: &str,
        code_verifier: &str,
    ) -> Result<TokenBundle> {
        self.exchange_form(
            config,
            vec![
                ("grant_type".to_owned(), "authorization_code".to_owned()),
                ("code".to_owned(), code.to_owned()),
                (
                    "redirect_uri".to_owned(),
                    config.redirect_uri.as_str().to_owned(),
                ),
                ("client_id".to_owned(), config.client_id.clone()),
                ("code_verifier".to_owned(), code_verifier.to_owned()),
            ],
        )
        .await
    }

    async fn refresh(&self, config: &OAuth2Config, refresh_token: &str) -> Result<TokenBundle> {
        self.exchange_form(
            config,
            vec![
                ("grant_type".to_owned(), "refresh_token".to_owned()),
                ("refresh_token".to_owned(), refresh_token.to_owned()),
                ("client_id".to_owned(), config.client_id.clone()),
            ],
        )
        .await
    }
}

fn validate_form(form: &[(String, String)]) -> Result<()> {
    if form.is_empty() || form.len() > 8 {
        return Err(AppError::invalid_field(
            "oauth_form",
            "invalid_shape",
            "OAuth token form has an invalid number of fields",
        ));
    }
    for (key, value) in form {
        if key.is_empty()
            || key.len() > 64
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
            || value.is_empty()
            || value.len() > MAX_TOKEN_VALUE_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(AppError::invalid_field(
                "oauth_form",
                "invalid_format",
                "OAuth token form contains an invalid bounded field",
            ));
        }
    }
    Ok(())
}

fn validate_token_value(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_TOKEN_VALUE_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(AppError::external_service(
            "oauth_token_endpoint",
            format!("token endpoint returned an invalid {label}"),
        ));
    }
    Ok(())
}

fn parse_scopes(raw: Option<&str>) -> Result<Vec<String>> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    if raw.len() > MAX_SCOPE_BYTES || raw.chars().any(char::is_control) {
        return Err(AppError::external_service(
            "oauth_token_endpoint",
            "token endpoint returned invalid granted scopes",
        ));
    }
    let scopes = raw
        .split_whitespace()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if scopes.len() > 128
        || scopes.iter().any(|scope| {
            scope.is_empty()
                || scope.len() > 128
                || !scope.chars().all(|character| {
                    character.is_ascii_alphanumeric()
                        || matches!(character, '.' | '_' | ':' | '-' | '+' | '/')
                })
        })
    {
        return Err(AppError::external_service(
            "oauth_token_endpoint",
            "token endpoint returned invalid granted scopes",
        ));
    }
    Ok(scopes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_parser_is_bounded_and_does_not_reflect_provider_text() {
        assert_eq!(
            parse_scopes(Some("chat:read files.read"))
                .expect("scopes")
                .len(),
            2
        );
        assert!(parse_scopes(Some("valid\nnot-valid")).is_err());
        assert!(validate_token_value("\n", "access token").is_err());
    }
}
