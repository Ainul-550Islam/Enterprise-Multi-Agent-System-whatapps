//! Asymmetric JSON Web Key Set validation for external OIDC access tokens.
//!
//! The module owns parsing, signature verification, key-selection and a
//! bounded in-process cache. Network I/O remains behind [`JwksFetchPort`] so
//! the security crate stays deterministic and callers can apply their own
//! transport policy (TLS roots, proxy settings and egress controls).
//!
//! The supported algorithms are intentionally narrow: `RS256` and `ES256`.
//! HMAC algorithms are never accepted on this path because the issuer, not an
//! API process, owns the asymmetric private key.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use mas_common::error::AppError;
use mas_common::result::Result;
use ring::signature;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;

use crate::tokens::{validate_claims, TokenClaims};

/// Maximum compact-JWS input accepted by this verifier.
///
/// An identity access token larger than this is not useful to this API and
/// accepting it would make header/payload decoding an allocation DoS surface.
pub const MAX_COMPACT_JWS_BYTES: usize = 16 * 1024;
/// Maximum JWKS response body the caller may pass to this module.
pub const MAX_JWKS_BODY_BYTES: usize = 1024 * 1024;
/// Maximum number of keys retained from one trusted JWKS document.
pub const MAX_JWKS_KEYS: usize = 128;
/// Cooldown on unknown-`kid` forced refreshes.
///
/// A forged token must not turn every request into an outbound request, while
/// a genuine key rotation should still become visible quickly.
pub const UNKNOWN_KID_REFRESH_COOLDOWN: Duration = Duration::from_secs(5);

/// Algorithms that the OIDC/JWKS verifier can accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum JwsAlgorithm {
    /// RSA PKCS#1 v1.5 with SHA-256.
    Rs256,
    /// NIST P-256 ECDSA with SHA-256 and the fixed-width JWS signature form.
    Es256,
}

impl JwsAlgorithm {
    /// Stable compact-JWS `alg` value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rs256 => "RS256",
            Self::Es256 => "ES256",
        }
    }
}

impl fmt::Display for JwsAlgorithm {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for JwsAlgorithm {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "RS256" => Ok(Self::Rs256),
            "ES256" => Ok(Self::Es256),
            _ => Err(AppError::validation(
                "OIDC algorithms must be RS256 and/or ES256",
            )),
        }
    }
}

/// Configuration that pins a remote issuer and its permitted signing schemes.
#[derive(Debug, Clone)]
pub struct JwksConfig {
    issuer: String,
    audience: Option<String>,
    algorithms: BTreeSet<JwsAlgorithm>,
    clock_skew: Duration,
    cache_ttl: Duration,
}

impl JwksConfig {
    /// Creates validated verifier configuration.
    ///
    /// `issuer` is always required for a remote IdP. An optional audience is
    /// useful where an access-token audience is deployment-specific.
    pub fn new(
        issuer: impl Into<String>,
        audience: Option<String>,
        algorithms: impl IntoIterator<Item = JwsAlgorithm>,
        clock_skew: Duration,
        cache_ttl: Duration,
    ) -> Result<Self> {
        let issuer = issuer.into();
        if issuer.trim().is_empty() {
            return Err(AppError::validation("OIDC issuer must not be empty"));
        }
        if audience
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(AppError::validation(
                "OIDC audience must be absent or non-empty",
            ));
        }
        let algorithms: BTreeSet<JwsAlgorithm> = algorithms.into_iter().collect();
        if algorithms.is_empty() {
            return Err(AppError::validation(
                "at least one OIDC asymmetric algorithm is required",
            ));
        }
        if cache_ttl.is_zero() {
            return Err(AppError::validation("JWKS cache TTL must be positive"));
        }
        if clock_skew > Duration::from_secs(300) {
            return Err(AppError::validation(
                "OIDC clock skew must not exceed five minutes",
            ));
        }
        Ok(Self {
            issuer,
            audience,
            algorithms,
            clock_skew,
            cache_ttl,
        })
    }

    /// Pinned token issuer.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Pinned token audience, if the deployment requires one.
    #[must_use]
    pub fn audience(&self) -> Option<&str> {
        self.audience.as_deref()
    }

    /// Permitted asymmetric JWS algorithms.
    #[must_use]
    pub fn algorithms(&self) -> &BTreeSet<JwsAlgorithm> {
        &self.algorithms
    }
}

/// Outbound JWKS retrieval boundary.
///
/// Implementations must retrieve only the configured endpoint, impose their
/// own connect/read timeouts, and return at most [`MAX_JWKS_BODY_BYTES`]. The
/// verifier normalizes failures at the authentication boundary so transport
/// details and endpoint information never leak to a caller.
#[async_trait]
pub trait JwksFetchPort: fmt::Debug + Send + Sync {
    /// Retrieves the complete JWKS document from the configured endpoint.
    async fn fetch(&self, endpoint: &str) -> Result<Vec<u8>>;
}

/// A cache-backed asymmetric JWT validator over an injected JWKS fetch port.
pub struct RemoteJwksValidator {
    endpoint: String,
    config: JwksConfig,
    fetcher: Arc<dyn JwksFetchPort>,
    cache: Mutex<Option<KeyCache>>,
    refresh_gate: AsyncMutex<()>,
}

impl fmt::Debug for RemoteJwksValidator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteJwksValidator")
            .field("endpoint", &self.endpoint)
            .field("issuer", &self.config.issuer)
            .field("algorithms", &self.config.algorithms)
            .finish_non_exhaustive()
    }
}

impl RemoteJwksValidator {
    /// Binds a verifier to one preconfigured JWKS endpoint.
    pub fn new(
        endpoint: impl Into<String>,
        config: JwksConfig,
        fetcher: Arc<dyn JwksFetchPort>,
    ) -> Result<Self> {
        let endpoint = endpoint.into();
        if endpoint.trim().is_empty() {
            return Err(AppError::validation("JWKS endpoint must not be empty"));
        }
        Ok(Self {
            endpoint,
            config,
            fetcher,
            cache: Mutex::new(None),
            refresh_gate: AsyncMutex::new(()),
        })
    }

    /// Verifies compact-JWS structure, a cached or freshly fetched asymmetric
    /// key, the signature, and all registered JWT claims.
    pub async fn validate(&self, token: &str) -> Result<TokenClaims> {
        let parsed = ParsedJws::parse(token)?;
        if !self.config.algorithms.contains(&parsed.algorithm) {
            return Err(authentication_failure("token algorithm is not permitted"));
        }
        let key = self.key_for(&parsed.kid).await?;
        verify_signature(&key, &parsed)?;
        validate_claims(
            &parsed.payload,
            Some(self.config.issuer()),
            self.config.audience(),
            self.config.clock_skew,
        )
    }

    async fn key_for(&self, kid: &str) -> Result<VerificationKey> {
        let now = Instant::now();
        if let Some(key) = self.cached_key(kid, now) {
            return Ok(key);
        }

        // All refreshes serialize. The double cache check after the gate means
        // a burst of requests for a newly-rotated key performs one fetch.
        let _gate = self.refresh_gate.lock().await;
        let now = Instant::now();
        if let Some(key) = self.cached_key(kid, now) {
            return Ok(key);
        }
        if self.unknown_kid_refresh_is_throttled(now) {
            return Err(authentication_failure("unknown token signing key"));
        }
        let forced_unknown_refresh = self.has_fresh_cache(now);

        let response = self
            .fetcher
            .fetch(&self.endpoint)
            .await
            .map_err(|_| authentication_failure("token signing keys are unavailable"))?;
        if response.len() > MAX_JWKS_BODY_BYTES {
            return Err(authentication_failure("token signing keys are invalid"));
        }
        let keys = parse_jwks(&response, &self.config.algorithms)
            .map_err(|_| authentication_failure("token signing keys are invalid"))?;
        let key = keys
            .get(kid)
            .cloned()
            .ok_or_else(|| authentication_failure("unknown token signing key"))?;
        let expires_at = Instant::now()
            .checked_add(self.config.cache_ttl)
            .ok_or_else(|| authentication_failure("token signing key cache overflow"))?;
        let next_unknown_kid_refresh = if forced_unknown_refresh {
            Some(
                Instant::now()
                    .checked_add(UNKNOWN_KID_REFRESH_COOLDOWN)
                    .ok_or_else(|| authentication_failure("token signing key cache overflow"))?,
            )
        } else {
            None
        };
        let mut cache = lock_recover(&self.cache);
        *cache = Some(KeyCache {
            keys,
            expires_at,
            next_unknown_kid_refresh,
        });
        Ok(key)
    }

    fn cached_key(&self, kid: &str, now: Instant) -> Option<VerificationKey> {
        let cache = lock_recover(&self.cache);
        cache.as_ref().and_then(|entry| {
            (entry.expires_at > now)
                .then(|| entry.keys.get(kid).cloned())
                .flatten()
        })
    }

    fn unknown_kid_refresh_is_throttled(&self, now: Instant) -> bool {
        let cache = lock_recover(&self.cache);
        cache.as_ref().is_some_and(|entry| {
            entry.expires_at > now
                && entry
                    .next_unknown_kid_refresh
                    .is_some_and(|next_refresh| next_refresh > now)
        })
    }

    fn has_fresh_cache(&self, now: Instant) -> bool {
        lock_recover(&self.cache)
            .as_ref()
            .is_some_and(|entry| entry.expires_at > now)
    }
}

#[derive(Debug)]
struct KeyCache {
    keys: HashMap<String, VerificationKey>,
    expires_at: Instant,
    next_unknown_kid_refresh: Option<Instant>,
}

#[derive(Debug, Clone)]
enum VerificationKey {
    Rs256(Vec<u8>),
    Es256(Vec<u8>),
}

#[derive(Debug)]
struct ParsedJws {
    algorithm: JwsAlgorithm,
    kid: String,
    signing_input: Vec<u8>,
    payload: Vec<u8>,
    signature: Vec<u8>,
}

impl ParsedJws {
    fn parse(token: &str) -> Result<Self> {
        if token.is_empty() || token.len() > MAX_COMPACT_JWS_BYTES || !token.is_ascii() {
            return Err(authentication_failure("token is malformed"));
        }
        let mut segments = token.split('.');
        let (Some(header_segment), Some(payload_segment), Some(signature_segment), None) = (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ) else {
            return Err(authentication_failure("token is not a compact JWS"));
        };
        if header_segment.is_empty() || payload_segment.is_empty() || signature_segment.is_empty() {
            return Err(authentication_failure("token is not a compact JWS"));
        }
        let header_bytes = URL_SAFE_NO_PAD
            .decode(header_segment)
            .map_err(|_| authentication_failure("token header is malformed"))?;
        let header: Value = serde_json::from_slice(&header_bytes)
            .map_err(|_| authentication_failure("token header is malformed"))?;
        let header = header
            .as_object()
            .ok_or_else(|| authentication_failure("token header is malformed"))?;
        reject_unsupported_critical_headers(header)?;
        let algorithm = header
            .get("alg")
            .and_then(Value::as_str)
            .ok_or_else(|| authentication_failure("token header is missing an algorithm"))?
            .parse()
            .map_err(|_| authentication_failure("token algorithm is unsupported"))?;
        let kid = header
            .get("kid")
            .and_then(Value::as_str)
            .filter(|value| {
                !value.is_empty()
                    && value.len() <= 512
                    && value.is_ascii()
                    && !value.chars().any(char::is_control)
            })
            .ok_or_else(|| authentication_failure("token header is missing a signing key id"))?
            .to_owned();
        let payload = URL_SAFE_NO_PAD
            .decode(payload_segment)
            .map_err(|_| authentication_failure("token payload is malformed"))?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature_segment)
            .map_err(|_| authentication_failure("token signature is malformed"))?;
        Ok(Self {
            algorithm,
            kid,
            signing_input: format!("{header_segment}.{payload_segment}").into_bytes(),
            payload,
            signature,
        })
    }
}

fn reject_unsupported_critical_headers(header: &serde_json::Map<String, Value>) -> Result<()> {
    if header
        .get("crit")
        .is_some_and(|value| value.as_array().is_none_or(|items| !items.is_empty()))
    {
        return Err(authentication_failure(
            "token declares unsupported critical headers",
        ));
    }
    if header
        .get("b64")
        .is_some_and(|value| value.as_bool() != Some(true))
    {
        return Err(authentication_failure(
            "token uses an unsupported payload encoding",
        ));
    }
    Ok(())
}

fn verify_signature(key: &VerificationKey, parsed: &ParsedJws) -> Result<()> {
    let valid = match (key, parsed.algorithm) {
        (VerificationKey::Rs256(public_key), JwsAlgorithm::Rs256) => {
            signature::UnparsedPublicKey::new(&signature::RSA_PKCS1_2048_8192_SHA256, public_key)
                .verify(&parsed.signing_input, &parsed.signature)
                .is_ok()
        },
        (VerificationKey::Es256(public_key), JwsAlgorithm::Es256) => {
            if parsed.signature.len() != 64 {
                false
            } else {
                signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, public_key)
                    .verify(&parsed.signing_input, &parsed.signature)
                    .is_ok()
            }
        },
        _ => false,
    };
    valid
        .then_some(())
        .ok_or_else(|| authentication_failure("invalid token signature"))
}

#[derive(Debug, Deserialize)]
struct RawJwks {
    keys: Vec<RawJwk>,
}

#[derive(Debug, Deserialize)]
struct RawJwk {
    kty: String,
    kid: String,
    #[serde(default, rename = "use")]
    key_use: Option<String>,
    #[serde(default)]
    alg: Option<String>,
    #[serde(default)]
    crv: Option<String>,
    #[serde(default)]
    n: Option<String>,
    #[serde(default)]
    e: Option<String>,
    #[serde(default)]
    x: Option<String>,
    #[serde(default)]
    y: Option<String>,
}

fn parse_jwks(
    body: &[u8],
    allowed_algorithms: &BTreeSet<JwsAlgorithm>,
) -> Result<HashMap<String, VerificationKey>> {
    let document: RawJwks = serde_json::from_slice(body)
        .map_err(|_| AppError::validation("JWKS document is not valid JSON"))?;
    if document.keys.is_empty() || document.keys.len() > MAX_JWKS_KEYS {
        return Err(AppError::validation("JWKS key count is out of bounds"));
    }
    let mut keys = HashMap::with_capacity(document.keys.len());
    for raw in document.keys {
        if raw.kid.is_empty() || raw.kid.len() > 512 || !raw.kid.is_ascii() {
            return Err(AppError::validation("JWKS contains an invalid key id"));
        }
        if keys.contains_key(&raw.kid) {
            return Err(AppError::validation("JWKS contains duplicate key ids"));
        }
        if raw.key_use.as_deref().is_some_and(|value| value != "sig") {
            continue;
        }
        let algorithm = match raw.alg.as_deref().map(str::parse).transpose()? {
            Some(algorithm) => algorithm,
            None => {
                // A key without `alg` can be used only when its key type itself
                // identifies one allowed algorithm. Supporting it avoids rejecting
                // standards-compliant IdPs while never widening the configured set.
                match raw.kty.as_str() {
                    "RSA" if allowed_algorithms.contains(&JwsAlgorithm::Rs256) => {
                        JwsAlgorithm::Rs256
                    },
                    "EC" if allowed_algorithms.contains(&JwsAlgorithm::Es256) => {
                        JwsAlgorithm::Es256
                    },
                    _ => continue,
                }
            },
        };
        if !allowed_algorithms.contains(&algorithm) {
            continue;
        }
        let verification_key = match (raw.kty.as_str(), algorithm) {
            ("RSA", JwsAlgorithm::Rs256) => VerificationKey::Rs256(rsa_public_key_der(
                raw.n
                    .as_deref()
                    .ok_or_else(|| AppError::validation("RSA JWKS key lacks n"))?,
                raw.e
                    .as_deref()
                    .ok_or_else(|| AppError::validation("RSA JWKS key lacks e"))?,
            )?),
            ("EC", JwsAlgorithm::Es256) => VerificationKey::Es256(ec_public_key(
                raw.crv
                    .as_deref()
                    .ok_or_else(|| AppError::validation("EC JWKS key lacks crv"))?,
                raw.x
                    .as_deref()
                    .ok_or_else(|| AppError::validation("EC JWKS key lacks x"))?,
                raw.y
                    .as_deref()
                    .ok_or_else(|| AppError::validation("EC JWKS key lacks y"))?,
            )?),
            _ => continue,
        };
        keys.insert(raw.kid, verification_key);
    }
    if keys.is_empty() {
        return Err(AppError::validation(
            "JWKS contains no usable configured signing keys",
        ));
    }
    Ok(keys)
}

fn rsa_public_key_der(modulus: &str, exponent: &str) -> Result<Vec<u8>> {
    let modulus = decode_jwk_integer(modulus, "RSA modulus")?;
    let exponent = decode_jwk_integer(exponent, "RSA exponent")?;
    if !(256..=1024).contains(&modulus.len()) {
        return Err(AppError::validation("RSA JWKS modulus size is unsupported"));
    }
    if exponent.len() > 8 {
        return Err(AppError::validation("RSA JWKS exponent is unsupported"));
    }
    let modulus = der_integer(&modulus);
    let exponent = der_integer(&exponent);
    let content_length = modulus
        .len()
        .checked_add(exponent.len())
        .ok_or_else(|| AppError::validation("RSA JWKS key is too large"))?;
    let mut der = Vec::with_capacity(content_length.saturating_add(8));
    der.push(0x30);
    der.extend(der_length(content_length)?);
    der.extend(modulus);
    der.extend(exponent);
    Ok(der)
}

fn ec_public_key(curve: &str, x: &str, y: &str) -> Result<Vec<u8>> {
    if curve != "P-256" {
        return Err(AppError::validation("EC JWKS curve must be P-256"));
    }
    let x = decode_jwk_integer(x, "EC x coordinate")?;
    let y = decode_jwk_integer(y, "EC y coordinate")?;
    if x.len() != 32 || y.len() != 32 {
        return Err(AppError::validation(
            "EC P-256 coordinates must be 32 bytes",
        ));
    }
    let mut public_key = Vec::with_capacity(65);
    public_key.push(0x04);
    public_key.extend(x);
    public_key.extend(y);
    Ok(public_key)
}

fn decode_jwk_integer(raw: &str, field: &str) -> Result<Vec<u8>> {
    if raw.is_empty() || raw.len() > 8192 || !raw.is_ascii() {
        return Err(AppError::validation(format!("{field} is invalid")));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| AppError::validation(format!("{field} is not base64url")))?;
    if bytes.is_empty() || bytes.iter().all(|byte| *byte == 0) {
        return Err(AppError::validation(format!("{field} is invalid")));
    }
    Ok(bytes)
}

fn der_integer(raw: &[u8]) -> Vec<u8> {
    let first_nonzero = raw
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(raw.len() - 1);
    let normalized = &raw[first_nonzero..];
    let needs_sign_pad = normalized.first().is_some_and(|byte| byte & 0x80 != 0);
    let value_len = normalized.len().saturating_add(usize::from(needs_sign_pad));
    let mut out = Vec::with_capacity(value_len.saturating_add(8));
    out.push(0x02);
    // Integer lengths are bounded by the RSA size check above, so this cannot
    // fail. Retaining the checked helper would make this otherwise pure helper
    // awkward without changing its public error contract.
    out.extend(der_length(value_len).expect("bounded DER integer length"));
    if needs_sign_pad {
        out.push(0);
    }
    out.extend(normalized);
    out
}

fn der_length(length: usize) -> Result<Vec<u8>> {
    if length < 128 {
        return Ok(vec![length as u8]);
    }
    let bytes = length.to_be_bytes();
    let first = bytes
        .iter()
        .position(|byte| *byte != 0)
        .ok_or_else(|| AppError::validation("DER length is invalid"))?;
    let encoded = &bytes[first..];
    if encoded.len() > 4 {
        return Err(AppError::validation("DER length is too large"));
    }
    let mut out = Vec::with_capacity(encoded.len().saturating_add(1));
    out.push(0x80 | encoded.len() as u8);
    out.extend(encoded);
    Ok(out)
}

fn authentication_failure(message: impl Into<String>) -> AppError {
    AppError::unauthorized(message)
}

fn lock_recover<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ring::rand::SystemRandom;
    use ring::signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};
    use serde_json::json;

    use super::*;

    #[derive(Debug)]
    struct FixtureFetcher {
        bodies: Mutex<VecDeque<Vec<u8>>>,
        calls: AtomicUsize,
    }

    impl FixtureFetcher {
        fn new(bodies: Vec<Vec<u8>>) -> Self {
            Self {
                bodies: Mutex::new(bodies.into()),
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl JwksFetchPort for FixtureFetcher {
        async fn fetch(&self, _endpoint: &str) -> Result<Vec<u8>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            lock_recover(&self.bodies)
                .pop_front()
                .ok_or_else(|| AppError::external_service("jwks", "fixture exhausted"))
        }
    }

    fn config() -> JwksConfig {
        JwksConfig::new(
            "https://issuer.example.test/realms/mas",
            Some("mas-api".to_owned()),
            [JwsAlgorithm::Es256],
            Duration::from_secs(30),
            Duration::from_secs(300),
        )
        .expect("config")
    }

    fn signing_key() -> EcdsaKeyPair {
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng)
            .expect("generate P-256 key");
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
            .expect("parse P-256 key")
    }

    fn jwks_for(pair: &EcdsaKeyPair, kid: &str) -> Vec<u8> {
        let public = pair.public_key().as_ref();
        assert_eq!(public.len(), 65);
        serde_json::to_vec(&json!({
            "keys": [{
                "kty": "EC",
                "kid": kid,
                "use": "sig",
                "alg": "ES256",
                "crv": "P-256",
                "x": URL_SAFE_NO_PAD.encode(&public[1..33]),
                "y": URL_SAFE_NO_PAD.encode(&public[33..65]),
            }]
        }))
        .expect("JWKS JSON")
    }

    fn token_for(pair: &EcdsaKeyPair, kid: &str, subject: &str) -> String {
        let header = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({"alg": "ES256", "kid": kid, "typ": "JWT"})).expect("header"),
        );
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "sub": subject,
                "iss": "https://issuer.example.test/realms/mas",
                "aud": ["mas-api"],
                "exp": mas_common::timestamps::Timestamp::now()
                    .checked_add(Duration::from_secs(300))
                    .expect("future")
                    .to_unix_seconds(),
                "scope": "executions:read"
            }))
            .expect("payload"),
        );
        let input = format!("{header}.{payload}");
        let rng = SystemRandom::new();
        let signature = pair.sign(&rng, input.as_bytes()).expect("sign");
        format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature.as_ref()))
    }

    #[tokio::test]
    async fn validates_es256_and_reuses_a_fresh_cache_entry() {
        let pair = signing_key();
        let fetcher = Arc::new(FixtureFetcher::new(vec![jwks_for(&pair, "key-1")]));
        let validator =
            RemoteJwksValidator::new("https://idp.example.test/jwks", config(), fetcher.clone())
                .expect("validator");
        let token = token_for(&pair, "key-1", "user-42");

        let claims = validator.validate(&token).await.expect("valid token");
        assert_eq!(claims.subject, "user-42");
        assert_eq!(claims.scopes, vec!["executions:read".to_owned()]);
        validator.validate(&token).await.expect("cached token");
        assert_eq!(fetcher.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn validates_a_standard_rs256_jwk_and_compact_jws_fixture() {
        const JWKS: &str = r#"{"keys":[{"kty":"RSA","kid":"rsa-fixture","use":"sig","alg":"RS256","n":"qsMFtNwB3BU7kXkWeYdb9bxlklvC4KKF4idKpcwUKOGXVfdLETPIuy5f5FqFwP9_-752CzdiHpXghtAd049Y_-Df5t_gOGz24lBeUP79X9gt5kPtzkQwsxeXiIIs1_48XPIa9rqBfNAl0qi98yaGJatQAwr6aahcol7U_RlUA8Jz9wE02eBuBJJ-qnunXUYurHoD15y7TlPCIi3ZyLjjO0Jig3qPWGwhsh3hCP6e8SfZxhG78kZMWCq4C5Gt5yd6TraCdyu41HSLIhMtH1sAnJFVPg4jvIus59lM34cIOS0rIHTELvFnNXcGka2DLBe6Hp56KM6Qf5H0lyZC0CQTPQ","e":"AQAB"}]}"#;
        const TOKEN: &str = "eyJhbGciOiJSUzI1NiIsImtpZCI6InJzYS1maXh0dXJlIiwidHlwIjoiSldUIn0.eyJzdWIiOiJyc2EtdXNlciIsImlzcyI6Imh0dHBzOi8vaXNzdWVyLmV4YW1wbGUudGVzdC9yZWFsbXMvbWFzIiwiYXVkIjpbIm1hcy1hcGkiXSwiZXhwIjoyNTI0NjA4MDAwLCJzY29wZSI6ImV4ZWN1dGlvbnM6cmVhZCJ9.IuEwKLk8DsufBkvOjFfrCRbeMS4TcC_4puIMzZ8aQUAGzuIPUbveK_o19oSraCfFv0qFoVJbQiugiAD1kZHON8foTjTPeWcAQO_5vR__h7l7dT0-ce5YUB88C3heGFWcFkhPU4HrBjrXNupPfwWgShZ54er4ZFCW0MtYDBzC7kZqhg1Zxsz5si-3EhY9p2bn0W-nTb3T4qSPaLhwdcNvLr-aL1avhrYj3mksf62U1FMb4VAJB_1xZzZMLiBnrdTWN1mC7dIr571cjzbbZSM-NbgFXP7cfJM-OuHMvhoWtm3AkAS_CIAaRpt5hoaPiDIgfQBgRfNMH4567Q-nQ71ctw";
        let fetcher = Arc::new(FixtureFetcher::new(vec![JWKS.as_bytes().to_vec()]));
        let rsa_config = JwksConfig::new(
            "https://issuer.example.test/realms/mas",
            Some("mas-api".to_owned()),
            [JwsAlgorithm::Rs256],
            Duration::from_secs(30),
            Duration::from_secs(300),
        )
        .expect("config");
        let validator =
            RemoteJwksValidator::new("https://idp.example.test/jwks", rsa_config, fetcher)
                .expect("validator");

        let claims = validator.validate(TOKEN).await.expect("RS256 token");
        assert_eq!(claims.subject, "rsa-user");
    }

    #[tokio::test]
    async fn unknown_kid_forces_one_rotation_refresh() {
        let old_pair = signing_key();
        let new_pair = signing_key();
        let fetcher = Arc::new(FixtureFetcher::new(vec![
            jwks_for(&old_pair, "old-key"),
            jwks_for(&new_pair, "new-key"),
        ]));
        let validator =
            RemoteJwksValidator::new("https://idp.example.test/jwks", config(), fetcher.clone())
                .expect("validator");
        validator
            .validate(&token_for(&old_pair, "old-key", "user-1"))
            .await
            .expect("old key first");
        validator
            .validate(&token_for(&new_pair, "new-key", "user-2"))
            .await
            .expect("rotation refresh");
        assert_eq!(fetcher.calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn rejects_none_wrong_alg_tampering_and_missing_kid() {
        let pair = signing_key();
        let fetcher = Arc::new(FixtureFetcher::new(vec![jwks_for(&pair, "key-1")]));
        let validator =
            RemoteJwksValidator::new("https://idp.example.test/jwks", config(), fetcher)
                .expect("validator");
        let valid = token_for(&pair, "key-1", "user-42");
        let parts: Vec<&str> = valid.split('.').collect();
        let none_header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","kid":"key-1"}"#);
        assert!(validator
            .validate(&format!("{none_header}.{}.{}", parts[1], parts[2]))
            .await
            .is_err());
        let missing_kid = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256"}"#);
        assert!(validator
            .validate(&format!("{missing_kid}.{}.{}", parts[1], parts[2]))
            .await
            .is_err());
        let mut tampered_payload = URL_SAFE_NO_PAD.decode(parts[1]).expect("payload bytes");
        tampered_payload.push(b' ');
        assert!(validator
            .validate(&format!(
                "{}.{}.{}",
                parts[0],
                URL_SAFE_NO_PAD.encode(tampered_payload),
                parts[2]
            ))
            .await
            .is_err());
    }
}
