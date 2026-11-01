//! PURPOSE: Black-box OIDC JWKS verification tests using real generated asymmetric signing keys.
//!
//! Owning layer: `mas-security` integration tests.
//! Production responsibility: ensure externally observable verification accepts correctly signed tokens and rejects malformed/algorithm-confused or invalid-claim tokens.
//! Data ownership: all keys and JWKS documents are generated in-process for this test; no network or production key material is used.
//! MUST NOT: replace the verifier with a test double, accept symmetric algorithms on the JWKS path, or fetch arbitrary endpoints.

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use mas_common::{error::AppError, result::Result, timestamps::Timestamp};
use mas_security::jwks::{JwksConfig, JwksFetchPort, JwsAlgorithm, RemoteJwksValidator};
use ring::{
    rand::SystemRandom,
    signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING},
};
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug)]
struct Fetcher(Mutex<VecDeque<Vec<u8>>>);

#[async_trait]
impl JwksFetchPort for Fetcher {
    async fn fetch(&self, _endpoint: &str) -> Result<Vec<u8>> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pop_front()
            .ok_or_else(|| AppError::external_service("jwks-test", "no fixture remains"))
    }
}

fn keypair() -> EcdsaKeyPair {
    let rng = SystemRandom::new();
    let pkcs8 =
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).expect("generate key");
    EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
        .expect("parse key")
}

fn jwks(pair: &EcdsaKeyPair, kid: &str) -> Vec<u8> {
    let point = pair.public_key().as_ref();
    serde_json::to_vec(&json!({"keys":[{"kty":"EC","kid":kid,"use":"sig","alg":"ES256","crv":"P-256","x":URL_SAFE_NO_PAD.encode(&point[1..33]),"y":URL_SAFE_NO_PAD.encode(&point[33..65])}]})).expect("JWKS")
}

fn signed_token(pair: &EcdsaKeyPair, kid: &str, claims: serde_json::Value, alg: &str) -> String {
    let header = URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&json!({"alg":alg,"kid":kid,"typ":"JWT"})).expect("header"));
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).expect("claims"));
    let signing_input = format!("{header}.{payload}");
    let signature = pair
        .sign(&SystemRandom::new(), signing_input.as_bytes())
        .expect("sign");
    format!(
        "{signing_input}.{}",
        URL_SAFE_NO_PAD.encode(signature.as_ref())
    )
}

fn standard_claims() -> serde_json::Value {
    json!({"sub":"user-test","iss":"https://issuer.example.test","aud":["mas-api"],"exp":Timestamp::now().checked_add(Duration::from_secs(600)).expect("future").to_unix_seconds(),"nbf":Timestamp::now().to_unix_seconds() - 1})
}

fn validator(pair: &EcdsaKeyPair, kid: &str) -> RemoteJwksValidator {
    let fetcher = Arc::new(Fetcher(Mutex::new(VecDeque::from([jwks(pair, kid)]))));
    let config = JwksConfig::new(
        "https://issuer.example.test",
        Some("mas-api".to_owned()),
        [JwsAlgorithm::Es256],
        Duration::ZERO,
        Duration::from_secs(60),
    )
    .expect("configuration");
    RemoteJwksValidator::new("https://issuer.example.test/jwks", config, fetcher)
        .expect("validator")
}

#[tokio::test]
async fn valid_asymmetric_token_verifies_against_the_generated_jwk() {
    let pair = keypair();
    let validator = validator(&pair, "current");
    let token = signed_token(&pair, "current", standard_claims(), "ES256");
    let claims = validator.validate(&token).await.expect("valid token");
    assert_eq!(claims.subject, "user-test");
}

#[tokio::test]
async fn unknown_kid_is_rejected_after_the_trusted_jwks_refresh() {
    let pair = keypair();
    let validator = validator(&pair, "known");
    let token = signed_token(&pair, "unknown", standard_claims(), "ES256");
    assert!(validator.validate(&token).await.is_err());
}

#[tokio::test]
async fn malformed_expired_future_and_wrong_issuer_or_audience_tokens_are_rejected() {
    let pair = keypair();
    let validator = validator(&pair, "claims-key");
    assert!(validator
        .validate("not.a.compact-jws.with.extra")
        .await
        .is_err());
    let cases = [
        json!({"sub":"expired","iss":"https://issuer.example.test","aud":["mas-api"],"exp":1}),
        json!({"sub":"future","iss":"https://issuer.example.test","aud":["mas-api"],"exp":Timestamp::now().checked_add(Duration::from_secs(600)).expect("future").to_unix_seconds(),"nbf":Timestamp::now().checked_add(Duration::from_secs(600)).expect("future").to_unix_seconds()}),
        json!({"sub":"wrong-issuer","iss":"https://evil.example.test","aud":["mas-api"],"exp":Timestamp::now().checked_add(Duration::from_secs(600)).expect("future").to_unix_seconds()}),
        json!({"sub":"wrong-audience","iss":"https://issuer.example.test","aud":["other-service"],"exp":Timestamp::now().checked_add(Duration::from_secs(600)).expect("future").to_unix_seconds()}),
    ];
    for claims in cases {
        let token = signed_token(&pair, "claims-key", claims, "ES256");
        assert!(validator.validate(&token).await.is_err());
    }
}

#[tokio::test]
async fn none_hs256_and_rs256_algorithm_confusion_are_rejected() {
    let pair = keypair();
    let validator = validator(&pair, "only-es256");
    for algorithm in ["none", "HS256", "RS256"] {
        let token = signed_token(&pair, "only-es256", standard_claims(), algorithm);
        assert!(
            validator.validate(&token).await.is_err(),
            "{algorithm} must be refused"
        );
    }
}

#[tokio::test]
async fn valid_rs256_token_verifies_with_a_real_rsa_jwks_fixture() {
    const JWKS: &str = r#"{"keys":[{"kty":"RSA","kid":"rsa-fixture","use":"sig","alg":"RS256","n":"qsMFtNwB3BU7kXkWeYdb9bxlklvC4KKF4idKpcwUKOGXVfdLETPIuy5f5FqFwP9_-752CzdiHpXghtAd049Y_-Df5t_gOGz24lBeUP79X9gt5kPtzkQwsxeXiIIs1_48XPIa9rqBfNAl0qi98yaGJatQAwr6aahcol7U_RlUA8Jz9wE02eBuBJJ-qnunXUYurHoD15y7TlPCIi3ZyLjjO0Jig3qPWGwhsh3hCP6e8SfZxhG78kZMWCq4C5Gt5yd6TraCdyu41HSLIhMtH1sAnJFVPg4jvIus59lM34cIOS0rIHTELvFnNXcGka2DLBe6Hp56KM6Qf5H0lyZC0CQTPQ","e":"AQAB"}]}"#;
    const TOKEN: &str = "eyJhbGciOiJSUzI1NiIsImtpZCI6InJzYS1maXh0dXJlIiwidHlwIjoiSldUIn0.eyJzdWIiOiJyc2EtdXNlciIsImlzcyI6Imh0dHBzOi8vaXNzdWVyLmV4YW1wbGUudGVzdC9yZWFsbXMvbWFzIiwiYXVkIjpbIm1hcy1hcGkiXSwiZXhwIjoyNTI0NjA4MDAwLCJzY29wZSI6ImV4ZWN1dGlvbnM6cmVhZCJ9.IuEwKLk8DsufBkvOjFfrCRbeMS4TcC_4puIMzZ8aQUAGzuIPUbveK_o19oSraCfFv0qFoVJbQiugiAD1kZHON8foTjTPeWcAQO_5vR__h7l7dT0-ce5YUB88C3heGFWcFkhPU4HrBjrXNupPfwWgShZ54er4ZFCW0MtYDBzC7kZqhg1Zxsz5si-3EhY9p2bn0W-nTb3T4qSPaLhwdcNvLr-aL1avhrYj3mksf62U1FMb4VAJB_1xZzZMLiBnrdTWN1mC7dIr571cjzbbZSM-NbgFXP7cfJM-OuHMvhoWtm3AkAS_CIAaRpt5hoaPiDIgfQBgRfNMH4567Q-nQ71ctw";
    let fetcher = Arc::new(Fetcher(Mutex::new(VecDeque::from([JWKS
        .as_bytes()
        .to_vec()]))));
    let config = JwksConfig::new(
        "https://issuer.example.test/realms/mas",
        Some("mas-api".to_owned()),
        [JwsAlgorithm::Rs256],
        Duration::from_secs(30),
        Duration::from_secs(300),
    )
    .expect("config");
    let validator = RemoteJwksValidator::new("https://issuer.example.test/jwks", config, fetcher)
        .expect("validator");
    assert_eq!(
        validator
            .validate(TOKEN)
            .await
            .expect("RS256 token")
            .subject,
        "rsa-user"
    );
}
