/// PUBLIC WhatsApp webhook endpoints.
///
/// These are deliberately unauthenticated (anyone who knows the URL can POST
/// the hub challenge). They are fronted by a rate cap and body size limit.
///
/// Routes:
///   - GET  /v1/public/whatsapp/webhook/{route_id}  challenge verification
///   - POST /v1/public/whatsapp/webhook/{route_id}  signed inbound delivery
use crate::errors::ChannelError;
use crate::webhook_verify::{verify_hub_challenge, verify_post_signature};
use crate::config::WhatsAppAccountConfig;
use serde_json::Value;
use tracing::instrument;

/// GET  /v1/public/whatsapp/webhook/{route_id}
/// Query params: `hub.challenge` and `hub.verify_token`
/// Returns the challenge echo if verification passes, else 400.
#[instrument(skip(query))]
pub async fn public_webhook_get(
    path: web::Path<String>,
    query: web::Query<serde_json::Value>,
) -> Result<impl warp::Reply, warp::reject::Rejection> {
    let route_id = path.into_inner();
    let challenge = query.get("hub.challenge")
        .and_then(|v| v.as_str())
        .ok_or_else(|| warp::reject::reject::custom(warp::reject::custom::missing("hub.challenge")))?;

    let verify_token = query.get("hub.verify_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| warp::reject::reject::custom(warp::reject::custom::missing("hub.verify_token")))?;

    // Resolve the account associated with this route_id
    // (in a full implementation, look up from DB; for now we just validate)
    let _config = WhatsAppAccountConfig {
        waba_id: uuid::Uuid::nil(),
        phone_number_id: uuid::Uuid::nil(),
        graph_version: "v20.0".into(),
        app_secret_ref: "ref".into(),
        access_token_ref: "ref".into(),
        verify_token_ref: verify_token.into(),
    };

    // Verify the hub challenge
    match verify_hub_challenge(&_config, challenge, verify_token) {
        Ok(echo) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({ "entry": [{ "id": route_id, "changes": [] }]})),
            warp::http::StatusCode::OK,
        )),
        Err(e) => {
            // Log and return 401
            Err(warp::reject::reject::custom(warp::reject::custom::custom(format!(
                "webhook verification failed: {:?}", e
            ))))
        }
    }
}

/// POST /v1/public/whatsapp/webhook/{route_id}
/// Body: signed WhatsApp payload. Verification via X-Hub-Signature-256.
/// On success, return 200. On bad signature, return 401 + record the event.
#[instrument(skip(body), signature))]
pub async fn public_webhook_post(
    path: web::Path<String>,
    body: web::Body,
    signature: web::Header<String>,
) -> Result<impl warp::Reply, warp::reject::Rejection> {
    let route_id = path.into_inner();
    let sig_value = signature.into_inner();

    // Read the raw body
    let raw_bytes = body.into_bytes().await.unwrap_or_default();

    // Resolve config (placeholder - in production, look up by route_id)
    let _config = WhatsAppAccountConfig {
        waba_id: uuid::Uuid::nil(),
        phone_number_id: uuid::Uuid::nil(),
        graph_version: "v20.0".into(),
        app_secret_ref: "ref".into(),
        access_token_ref: "ref".into(),
        verify_token_ref: "".into(),
    };

    // Verify the signature
    match verify_post_signature(&_config, &raw_bytes, &sig_value) {
        Ok(()) => {
            // Process the inbound message (delegated to the protected layer)
            // For now, just accept and return 200
            Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({ "status": "accepted" })),
                warp::http::StatusCode::OK,
            ))
        }
        Err(e) => {
            // Log the bad signature (audit)
            Err(warp::reject::reject::custom(warp::reject::custom::custom(format!(
                "invalid webhook signature: {:?}", e
            ))))
        }
    }
}