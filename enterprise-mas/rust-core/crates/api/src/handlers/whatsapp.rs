/// PROTECTED WhatsApp API handlers.
///
/// These routes require authentication + tenant authorization. They are
/// mounted behind the `/v1/whatsapp/*` prefix and the authenticated layer.
use crate::errors::ChannelError;
use crate::whatsapp_service::{handle_inbound, send_text, send_template, handle_status, handle_stop, human_handoff, rotate_credentials, submit_execution};
use crate::config::WhatsAppAccountConfig;
use serde_json::Value;
use tracing::instrument;

/// Register a new WhatsApp account (WABA + phone number) for a tenant.
/// Body: WhatsAppAccountConfig (with secret_refs, never inline secrets).
#[instrument(skip(body))]
pub async fn register_account(_body: web::Json<WhatsAppAccountConfig>) -> Result<Value, warp::reject::Rejection> {
    // In a full implementation, validate the secret refs via the secret gate,
    // persist the account, and return the account ID.
    let _account = _body.0;
    Ok(serde_json::json!({ "status": "account_registration_pending", "message": "account requires Meta onboarding flow" }))
}

/// List all WhatsApp accounts for the authenticated tenant.
pub async fn list_accounts() -> Result<Value, warp::reject::Rejection> {
    // Return empty for now; full impl queries whatsapp_accounts RLS-filtered.
    Ok(serde_json::json!({ "accounts": [] }))
}

/// Handle an inbound WhatsApp webhook POST (signed, authenticated).
/// This is the protected version of the public webhook — only reachable
/// after authZ. The body has already been verified by the middleware.
#[instrument(skip(body))]
pub async fn protected_inbound(
    body: web::Json<Value>,
) -> Result<Value, warp::reject::Rejection> {
    // The body is already verified by middleware; just normalise and delegate.
    let raw = body.0;
    // route_id comes from the request context/middleware
    let route_id = "default-route".into();
    let _account = WhatsAppAccountConfig {
        waba_id: uuid::Uuid::nil(),
        phone_number_id: uuid::Uuid::nil(),
        graph_version: "v20.0".into(),
        app_secret_ref: "ref".into(),
        access_token_ref: "ref".into(),
        verify_token_ref: "ref".into(),
    };

    match handle_inbound(&raw, &route_id, &_account) {
        Ok(msg) => Ok(serde_json::json!({ "status": "processed", "wamid": msg.wamid })),
        Err(e) => {
            let stable = e.to_stable_api_error();
            Err(warp::reject::reject::custom(warp::reject::custom::custom(format!(
                "inbound handling error: {:?}", stable
            ))))
        }
    }
}

/// Send a text message from the bot to a WhatsApp number.
pub async fn protected_send_text(
    account: WhatsAppAccountConfig,
    to: web::Query<String>,
    body: web::Query<String>,
) -> Result<Value, warp::reject::Rejection> {
    let text_body = body.into_inner();
    let result = send_text(&_account, &to.into_inner(), &text_body, None).await;
    match result {
        Ok(()) => Ok(serde_json::json!({ "status": "sent" })),
        Err(e) => {
            let stable = e.to_stable_api_error();
            Err(warp::reject::reject::custom(warp::reject::custom::custom(format!(
                "send text error: {:?}", stable
            ))))
        }
    }
}

/// Send a template message.
pub async fn protected_send_template(
    account: WhatsAppAccountConfig,
    to: web::Query<String>,
    template_name: web::Query<String>,
    language_code: web::Query<String>,
    variables: web::Query<serde_json::Value>,
) -> Result<Value, warp::reject::Rejection> {
    let vars = variables.into_inner();
    let result = send_template(&_account, &to.into_inner(), &template_name.into_inner(),
        &language_code.into_inner(), vars.as_array().map(|a| a.iter().map(|v| (v["key"].as_str().unwrap_or(""), v["value"].as_str().unwrap_or("")).collect::<Vec<(&str, &str)>>).unwrap_or_default(), &TemplateModel {
        name: "template".into(),
        language: "en".into(),
        category: crate::template::TemplateCategory::Text,
        status: crate::template::TemplateStatus::Approved,
        variables: vec![],
    }, None).await;
    match result {
        Ok(()) => Ok(serde_json::json!({ "status": "sent" })),
        Err(e) => {
            let stable = e.to_stable_api_error();
            Err(warp::reject::reject::custom(warp::reject::custom::custom(format!(
                "send template error: {:?}", stable
            ))))
        }
    }
}

/// Handle status update (delivery/read receipt)
pub async fn protected_handle_status(
    wamid: web::Query<String>,
    status: web::Query<String>,
) -> Result<Value, warp::reject::Rejection> {
    let result = handle_status(&wamid.into_inner(), &status.into_inner()).await;
    match result {
        Ok(()) => Ok(serde_json::json!({ "status": "acknowledged" })),
        Err(e) => {
            let stable = e.to_stable_api_error();
            Err(warp::reject::reject::custom(warp::reject::custom::custom(format!(
                "handle status error: {:?}", stable
            ))))
        }
    }
}

/// Handle STOP/opt-out from user
pub async fn protected_handle_stop(
    wamid: web::Query<String>,
) -> Result<Value, warp::reject::Rejection> {
    let result = handle_stop(&wamid.into_inner()).await;
    match result {
        Ok(()) => Ok(serde_json::json!({ "status": "opted_out" })),
        Err(e) => {
            let stable = e.to_stable_api_error();
            Err(warp::reject::reject::custom(warp::reject::custom::custom(format!(
                "handle stop error: {:?}", stable
            ))))
        }
    }
}

/// Human handoff: bot → human agent
pub async fn protected_human_handoff(
    conversation_id: web::Query<String>,
    new_agent_id: web::Query<String>,
) -> Result<Value, warp::reject::Rejection> {
    let result = human_handoff(&conversation_id.into_inner(), &new_agent_id.into_inner()).await;
    match result {
        Ok(()) => Ok(serde_json::json!({ "status": "handoff_initiated" })),
        Err(e) => {
            let stable = e.to_stable_api_error();
            Err(warp::reject::reject::custom(warp::reject::custom::custom(format!(
                "human handoff error: {:?}", stable
            ))))
        }
    }
}

/// Rotate credentials for a WhatsApp account
pub async fn protected_rotate_credentials(
    new_app_secret_ref: web::Query<String>,
    new_access_token_ref: web::Query<String>,
) -> Result<Value, warp::reject::Rejection> {
    let result = rotate_credentials(&new_app_secret_ref.into_inner(), &new_access_token_ref.into_inner()).await;
    match result {
        Ok(()) => Ok(serde_json::json!({ "status": "credentials_rotated" })),
        Err(e) => {
            let stable = e.to_stable_api_error();
            Err(warp::reject::reject::custom(warp::reject::custom::custom(format!(
                "rotate credentials error: {:?}", stable
            ))))
        }
    }
}

/// Submit execution with idempotency key
pub async fn protected_submit_execution(
    wamid: web::Query<String>,
    account: WhatsAppAccountConfig,
) -> Result<Value, warp::reject::Rejection> {
    let result = submit_execution(&wamid.into_inner(), &account).await;
    match result {
        Ok(()) => Ok(serde_json::json!({ "status": "execution_submitted" })),
        Err(e) => {
            let stable = e.to_stable_api_error();
            Err(warp::reject::reject::custom(warp::reject::custom::custom(format!(
                "submit execution error: {:?}", stable
            ))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WhatsAppAccountConfig;
    use uuid::Uuid;

    #[test]
    fn test_register_account() {
        let account = WhatsAppAccountConfig {
            waba_id: Uuid::new_v4(),
            phone_number_id: Uuid::new_v4(),
            graph_version: "v20.0".into(),
            app_secret_ref: "ref".into(),
            access_token_ref: "ref".into(),
            verify_token_ref: "ref".into(),
        };
        let result = register_account(serde_json::json(account));
        // Just verify it compiles and returns a JSON response
    }

    #[test]
    fn test_protected_inbound_structure() {
        let raw = serde_json::json!({ "object": "page" });
        let result = protected_inbound(serde_json::json(raw));
        // Should return an error or result; just verify it doesn't panic
    }
}