//! Token validation: HMAC-SHA256 (HS256) JWTs with strict claim checks.
//!
//! Deliberate scope decisions:
//! * **HS256 only.** `alg` headers for anything else (incl. `none`) are
//!   unauthenticated failures — never silently "supported".
//! * Only *verification* is production-shaped here; token issuance lives in
//!   the platform IdP. `encode_hs256_for_test` exists for dev/tests.
//! * Claims are pinned by configuration: issuer and audience are rejected
//!   unless they match when configured; expiry is mandatory; clock skew is
//!   bounded (default 60 s).

use crate::secret_string::SecretString;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::Sha256;
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

/// Validated token claims (everything the principal construction needs).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TokenClaims {
    /// `sub` — the actor id.
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    #[serde(default)]
    pub audiences: Vec<String>,
    pub expires_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issued_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<OrganizationId>,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    /// Unparsed extra claims (header-free, signature-covered).
    #[serde(default)]
    pub extras: Map<String, Value>,
}

impl TokenClaims {
    /// Whether the requested audience is present (or unrestricted).
    #[must_use]
    pub fn covers_audience(&self, audience: Option<&str>) -> bool {
        match audience {
            None => true,
            Some(required) => self.audiences.iter().any(|held| held == required),
        }
    }
}

/// HS256 JWT validator.
#[derive(Debug)]
pub struct JwtValidator {
    signing_key: SecretString,
    issuer: Option<String>,
    audience: Option<String>,
    skew: Duration,
}

impl JwtValidator {
    pub fn new(
        signing_key: SecretString,
        issuer: Option<String>,
        audience: Option<String>,
    ) -> Self {
        Self {
            signing_key,
            issuer,
            audience,
            skew: Duration::from_secs(60),
        }
    }

    #[must_use]
    pub const fn with_clock_skew(mut self, skew: Duration) -> Self {
        self.skew = skew;
        self
    }

    fn fail(message: impl Into<String>) -> AppError {
        AppError::unauthorized(message)
    }

    /// Validates a token: structure → signature → claims. Failure reasons
    /// are authentication-level (stable strings, no token content).
    pub fn validate(&self, token: &str) -> Result<TokenClaims> {
        // -- structure ------------------------------------------------------
        let segments: Vec<&str> = token.split('.').collect();
        if segments.len() != 3 {
            return Err(Self::fail(
                "token is not a compact JWS (expected 3 segments)",
            ));
        }
        let header_bytes = URL_SAFE_NO_PAD
            .decode(segments[0])
            .map_err(|_| Self::fail("token header is not valid base64url"))?;
        let header: Value = serde_json::from_slice(&header_bytes)
            .map_err(|_| Self::fail("token header is not valid JSON"))?;
        let alg = header
            .get("alg")
            .and_then(Value::as_str)
            .ok_or_else(|| Self::fail("token header is missing 'alg'"))?;
        if alg != "HS256" {
            return Err(Self::fail(format!(
                "unsupported token algorithm '{alg}' (HS256 only)"
            )));
        }

        // -- signature -------------------------------------------------------
        let signing_input = format!("{}.{}", segments[0], segments[1]);
        let signature = URL_SAFE_NO_PAD
            .decode(segments[2])
            .map_err(|_| Self::fail("token signature is not valid base64url"))?;
        let mut mac = HmacSha256::new_from_slice(self.signing_key.expose().as_bytes())
            .map_err(|e| AppError::internal(format!("invalid signing key: {e}")))?;
        mac.update(signing_input.as_bytes());
        mac.verify_slice(&signature)
            .map_err(|_| Self::fail("invalid token signature"))?;

        // -- claims ----------------------------------------------------------
        let payload_bytes = URL_SAFE_NO_PAD
            .decode(segments[1])
            .map_err(|_| Self::fail("token payload is not valid base64url"))?;
        let payload: Value = serde_json::from_slice(&payload_bytes)
            .map_err(|_| Self::fail("token payload is not valid JSON"))?;
        let object = payload
            .as_object()
            .ok_or_else(|| Self::fail("token payload must be a JSON object"))?;

        let get_str = |key: &str| object.get(key).and_then(Value::as_str).map(str::to_owned);

        let subject = get_str("sub").ok_or_else(|| Self::fail("token payload is missing 'sub'"))?;
        let expires_secs = object
            .get("exp")
            .and_then(Value::as_i64)
            .ok_or_else(|| Self::fail("token payload is missing 'exp'"))?;
        let expires_at = secs_to_timestamp(expires_secs)?;
        let skew = self.skew;
        let expiry_with_skew = expires_at
            .checked_add(skew)
            .ok_or_else(|| AppError::internal("expiry skew overflow"))?;
        if !expiry_with_skew.is_future() {
            return Err(Self::fail("token has expired"));
        }
        let not_before = object
            .get("nbf")
            .and_then(Value::as_i64)
            .map(secs_to_timestamp)
            .transpose()?;
        if let Some(nbf) = not_before {
            let nbf_minus_skew = nbf
                .checked_sub(skew)
                .ok_or_else(|| AppError::internal("nbf skew underflow"))?;
            if nbf_minus_skew.is_future() {
                return Err(Self::fail("token is not yet valid"));
            }
        }
        let issued_at = object
            .get("iat")
            .and_then(Value::as_i64)
            .map(secs_to_timestamp)
            .transpose()?;

        let issuer = get_str("iss");
        if let Some(required) = &self.issuer {
            match &issuer {
                Some(held) if held == required => {},
                _ => return Err(Self::fail("token issuer does not match")),
            }
        }
        let audiences: Vec<String> = match object.get("aud") {
            Some(Value::String(single)) => vec![single.clone()],
            Some(Value::Array(items)) => items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
            _ => Vec::new(),
        };
        if let Some(required) = &self.audience {
            if !audiences.iter().any(|held| held == required) {
                return Err(Self::fail("token audience does not match"));
            }
        }
        let roles: Vec<String> = object
            .get("roles")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        // `scope` (space-separated, RFC8693-style) or `scopes` (array).
        let scopes: Vec<String> = match (
            object.get("scope").and_then(Value::as_str),
            object.get("scopes").and_then(Value::as_array),
        ) {
            (Some(scope_str), _) => scope_str.split_whitespace().map(str::to_owned).collect(),
            (None, Some(array)) => array
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
            _ => Vec::new(),
        };

        const KNOWN: [&str; 12] = [
            "sub",
            "jti",
            "iss",
            "aud",
            "exp",
            "nbf",
            "iat",
            "tenant_id",
            "org_id",
            "roles",
            "scope",
            "scopes",
        ];
        let extras: Map<String, Value> = object
            .iter()
            .filter(|(key, _)| !KNOWN.contains(&key.as_str()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();

        Ok(TokenClaims {
            subject,
            jwt_id: get_str("jti"),
            issuer,
            audiences,
            expires_at,
            not_before,
            issued_at,
            tenant_id: object
                .get("tenant_id")
                .and_then(Value::as_str)
                .and_then(|raw| std::str::FromStr::from_str(raw).ok()),
            organization_id: object
                .get("org_id")
                .and_then(Value::as_str)
                .and_then(|raw| std::str::FromStr::from_str(raw).ok()),
            roles,
            scopes,
            extras,
        })
    }
}

fn secs_to_timestamp(secs: i64) -> Result<Timestamp> {
    Timestamp::from_unix_seconds(secs)
        .map_err(|_| AppError::unauthorized("token timestamp out of range"))
}

/// Test/dev helper: encodes + signs a compact HS256 JWT.
///
/// NOT for production issuance; the IdP owns signing keys in reality.
#[must_use]
pub fn encode_hs256_for_test(signing_key: &SecretString, claims: &Value) -> String {
    let header = serde_json::json!({"alg": "HS256", "typ": "JWT"});
    let header_seg =
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).expect("header serializes"));
    let payload_seg = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).expect("claims serialize"));
    let signing_input = format!("{header_seg}.{payload_seg}");
    let mut mac =
        HmacSha256::new_from_slice(signing_key.expose().as_bytes()).expect("test keys are valid");
    mac.update(signing_input.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{signing_input}.{signature}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::TenantId;

    fn key() -> SecretString {
        SecretString::new("test-signing-key-0123456789abcdef")
    }

    fn claims() -> Value {
        serde_json::json!({
            "sub": "user-42",
            "iss": "mas-idp",
            "aud": ["mas-api"],
            "exp": Timestamp::now()
                .checked_add(Duration::from_secs(300))
                .expect("future")
                .to_unix_seconds(),
            "tenant_id": TenantId::new().to_string(),
            "roles": ["operator"],
            "scope": "executions:read executions:cancel"
        })
    }

    #[test]
    fn valid_token_round_trip() {
        let validator = JwtValidator::new(key(), Some("mas-idp".into()), Some("mas-api".into()));
        let token = encode_hs256_for_test(&key(), &claims());
        let result = validator.validate(&token);
        assert!(result.is_ok(), "validate failed: {:?}", result.err());
        let claims = result.expect("validated");
        assert_eq!(claims.subject, "user-42");
        assert!(claims.tenant_id.is_some());
        assert_eq!(claims.roles, vec!["operator".to_owned()]);
        assert_eq!(
            claims.scopes,
            vec!["executions:read".to_owned(), "executions:cancel".to_owned()]
        );
    }

    #[test]
    fn rejects_alg_none_tampered_signature_and_wrong_pins() {
        let validator = JwtValidator::new(key(), Some("mas-idp".into()), Some("mas-api".into()));
        // alg: none
        let none_header = URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&serde_json::json!({"alg": "none"})).expect("json"));
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims()).expect("json"));
        let none_token = format!("{none_header}.{payload}.");
        assert!(validator.validate(&none_token).is_err());

        // tampered payload (signature no longer matches)
        let good = encode_hs256_for_test(&key(), &claims());
        let segs: Vec<&str> = good.split('.').collect();
        let mut tampered_claims = claims();
        tampered_claims["sub"] = Value::from("attacker");
        let tampered_payload =
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&tampered_claims).expect("json"));
        let tampered = format!("{}.{tampered_payload}.{}", segs[0], segs[2]);
        assert!(validator.validate(&tampered).is_err());

        // wrong key
        let wrong_key = SecretString::new("different-key");
        assert!(JwtValidator::new(wrong_key, None, None)
            .validate(&good)
            .is_err());

        // wrong issuer / audience pins
        assert!(JwtValidator::new(key(), Some("other-idp".into()), None)
            .validate(&good)
            .is_err());
        assert!(JwtValidator::new(key(), None, Some("mas-admin".into()))
            .validate(&good)
            .is_err());
    }

    #[test]
    fn expiry_and_structure_are_enforced() {
        let validator = JwtValidator::new(key(), None, None);
        let expired = serde_json::json!({
            "sub": "x",
            "exp": Timestamp::now()
                .checked_sub(Duration::from_secs(120))
                .expect("past")
                .to_unix_seconds()
        });
        assert!(validator
            .validate(&encode_hs256_for_test(&key(), &expired))
            .is_err());
        assert!(validator.validate("a.b").is_err());
        assert!(validator.validate("a.b.c.d").is_err());
        let missing_exp = serde_json::json!({"sub": "x"});
        assert!(validator
            .validate(&encode_hs256_for_test(&key(), &missing_exp))
            .is_err());
    }
}
