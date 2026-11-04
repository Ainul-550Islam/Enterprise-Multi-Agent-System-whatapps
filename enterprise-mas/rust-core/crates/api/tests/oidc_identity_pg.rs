//! Signed-token → issuer/subject mapping over the real least-privilege
//! directory role. Never point this test at a production database.
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use mas_api::{
    auth::TokenVerifierPort,
    oidc_auth::{OidcIdentityDirectory, OidcJwksTokenVerifier, PgOidcIdentityDirectory},
};
use mas_common::result::Result;
use mas_security::{JwksConfig, JwksFetchPort, JwsAlgorithm, RemoteJwksValidator};
use ring::{
    rand::SystemRandom,
    signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING},
};
use serde_json::json;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};

const ISSUER: &str = "https://issuer.example.test/realms/mas";
#[derive(Debug)]
struct Keys(Vec<u8>);
#[async_trait]
impl JwksFetchPort for Keys {
    async fn fetch(&self, _: &str) -> Result<Vec<u8>> {
        Ok(self.0.clone())
    }
}
fn sign(key: &EcdsaKeyPair, sub: &str, issuer: &str, audience: &str, scope: &str) -> String {
    let encode = |value| URL_SAFE_NO_PAD.encode(serde_json::to_vec(&value).unwrap());
    let head = encode(json!({"alg":"ES256", "kid":"test-key"}));
    let body = encode(json!({"iss":issuer, "sub":sub, "aud":audience,
        "exp":mas_common::timestamps::Timestamp::now().checked_add(Duration::from_secs(300)).unwrap().to_unix_seconds(), "scope":scope}));
    let message = format!("{head}.{body}");
    let signature = key.sign(&SystemRandom::new(), message.as_bytes()).unwrap();
    format!("{message}.{}", URL_SAFE_NO_PAD.encode(signature.as_ref()))
}
fn verifier(pool: PgPool, key: &EcdsaKeyPair) -> OidcJwksTokenVerifier {
    let public = key.public_key().as_ref();
    let jwks = serde_json::to_vec(&json!({"keys":[{"kty":"EC","kid":"test-key","alg":"ES256","use":"sig","crv":"P-256", "x":URL_SAFE_NO_PAD.encode(&public[1..33]), "y":URL_SAFE_NO_PAD.encode(&public[33..65])}]})).unwrap();
    let config = JwksConfig::new(
        ISSUER,
        Some("mas-api".into()),
        [JwsAlgorithm::Es256],
        Duration::from_secs(30),
        Duration::from_secs(300),
    )
    .unwrap();
    let validator = RemoteJwksValidator::new(
        "https://issuer.example.test/jwks",
        config,
        Arc::new(Keys(jwks)),
    )
    .unwrap();
    OidcJwksTokenVerifier::new(validator, Arc::new(PgOidcIdentityDirectory::new(pool)))
}

#[tokio::test]
async fn signed_identity_maps_only_exact_active_binding_under_directory_role() {
    let admin = PgPool::connect(
        &std::env::var("MAS_TEST_DATABASE_URL").expect("MAS_TEST_DATABASE_URL required"),
    )
    .await
    .unwrap();
    let directory = PgPool::connect(
        &std::env::var("MAS_TEST_DIRECTORY_DATABASE_URL")
            .expect("MAS_TEST_DIRECTORY_DATABASE_URL required"),
    )
    .await
    .unwrap();
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    let key =
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng).unwrap();
    let id = uuid::Uuid::now_v7();
    let subject = format!("opaque-sub-{id}");
    let email = format!("{id}@oidc-test.example");
    sqlx::query("INSERT INTO users (id,email,display_name,status,identity_provider,external_subject) VALUES ($1,$2,'OIDC test','active',$3,$4)")
        .bind(id).bind(email).bind(ISSUER).bind(&subject).execute(&admin).await.unwrap();
    let lookup = PgOidcIdentityDirectory::new(directory.clone());
    assert_eq!(
        lookup
            .lookup(ISSUER, &subject)
            .await
            .unwrap()
            .unwrap()
            .into_uuid(),
        id
    );
    assert!(lookup
        .lookup("https://other-issuer.example", &subject)
        .await
        .unwrap()
        .is_none());
    let service = verifier(directory.clone(), &key);
    let token = sign(&key, &subject, ISSUER, "mas-api", "executions:read");
    let principal = service.verify(&token).await.unwrap();
    assert_eq!(principal.actor_id, id.to_string());
    assert!(principal.memberships.is_empty());
    assert!(principal.permits("executions:read"));
    for token in [
        sign(&key, &id.to_string(), ISSUER, "mas-api", "executions:read"),
        sign(
            &key,
            &subject,
            "https://other-issuer.example",
            "mas-api",
            "executions:read",
        ),
        sign(&key, &subject, ISSUER, "other-api", "executions:read"),
        sign(&key, &subject, ISSUER, "mas-api", "platform:admin"),
        sign(&key, &subject, ISSUER, "mas-api", "platform:*"),
        sign(&key, &subject, ISSUER, "mas-api", "*"),
    ] {
        assert_eq!(
            service.verify(&token).await.unwrap_err().error_code(),
            "UNAUTHENTICATED"
        );
    }
    // The directory cannot read profile columns or mutate identities.
    assert!(sqlx::query("SELECT email FROM users WHERE id=$1")
        .bind(id)
        .fetch_optional(&directory)
        .await
        .is_err());
    assert!(
        sqlx::query("UPDATE users SET status='disabled' WHERE id=$1")
            .bind(id)
            .execute(&directory)
            .await
            .is_err()
    );
    let duplicate = sqlx::query("INSERT INTO users (email,display_name,status,identity_provider,external_subject) VALUES ($1,'duplicate','active',$2,$3)")
        .bind(format!("dup-{id}@oidc-test.example")).bind(ISSUER).bind(&subject).execute(&admin).await;
    assert!(duplicate.is_err(), "issuer/subject bindings must be unique");
    sqlx::query("UPDATE users SET status='disabled' WHERE id=$1")
        .bind(id)
        .execute(&admin)
        .await
        .unwrap();
    assert_eq!(
        service
            .verify(&sign(&key, &subject, ISSUER, "mas-api", "executions:read"))
            .await
            .unwrap_err()
            .error_code(),
        "UNAUTHENTICATED"
    );
    sqlx::query("DELETE FROM users WHERE id=$1")
        .bind(id)
        .execute(&admin)
        .await
        .unwrap();
}
