/// Protected WhatsApp routes (mounted under /v1/whatsapp/*).
use warp::Filter;

use crate::api::handlers::whatsapp::{
    protected_inbound, protected_send_text, protected_send_template,
    protected_handle_status, protected_handle_stop, protected_human_handoff,
    protected_rotate_credentials, protected_submit_execution,
    register_account, list_accounts,
};

/// Protected WhatsApp routes — require authentication + tenant authorization.
pub fn protected_routes() -> impl Filter<Extract = impl warp::Reply, Rejection = warp::Rejection> + Clone {
    // Protected routes (authenticated layer)
    let register = warp::post()
        .and(warp::path("whatsapp"))
        .and(warp::path("account"))
        .and(warp::body::json())
        .and_with(register_account);

    let list = warp::get()
        .and(warp::path("whatsapp"))
        .and(warp::path("accounts"))
        .and(warp::query())
        .and_with(list_accounts);

    let inbound = warp::post()
        .and(warp::path("whatsapp"))
        .and(warp::path("inbound"))
        .and(warp::body::json())
        .and(warp::any().map(|_| warp::http::Method::POST))
        .and_with(protected_inbound);

    let send_text = warp::post()
        .and(warp::path("whatsapp"))
        .and(warp::path("send"))
        .and(warp::path("text"))
        .and(warp::query())
        .and(warp::body::json())
        .and_with(protected_send_text);

    let send_template = warp::post()
        .and(warp::path("whatsapp"))
        .and(warp::path("send"))
        .and(warp::path("template"))
        .and(warp::query())
        .and(warp::body::json())
        .and_with(protected_send_template);

    let handle_status = warp::post()
        .and(warp::path("whatsapp"))
        .and(warp::path("status"))
        .and(warp::query())
        .and(warp::body::json())
        .and_with(protected_handle_status);

    let handle_stop = warp::post()
        .and(warp::path("whatsapp"))
        .and(warp::path("stop"))
        .and(warp::query())
        .and_with(protected_handle_stop);

    let human_handoff = warp::post()
        .and(warp::path("whatsapp"))
        .and(warp::path("handoff"))
        .and(warp::query())
        .and(warp::body::json())
        .and_with(human_handoff);

    let rotate_creds = warp::post()
        .and(warp::path("whatsapp"))
        .and(warp::path("rotate-credentials"))
        .and(warp::body::json())
        .and_with(rotate_credentials);

    let submit_exec = warp::post()
        .and(warp::path("whatsapp"))
        .and(warp::path("execution"))
        .and(warp::query())
        .and(warp::body::json())
        .and_with(submit_exec);

    // Combine all protected routes
    register
        .or(list)
        .or(inbound)
        .or(send_text)
        .or(send_template)
        .or(handle_status)
        .or(handle_stop)
        .or(human_handoff)
        .or(rotate_creds)
        .or(submit_exec)
}

// Public Whatsapp webhook routes — no authentication required (hub challenge + signed POST)
pub fn public_routes() -> impl Filter<Extract = impl warp::Reply, Rejection = warp::Rejection> + Clone {
    // Public GET challenge
    let public_get = warp::get()
        .and(warp::path("public"))
        .and(warp::path("whatsapp"))
        .and(warp::path("webhook"))
        .and(warp::path::param())
        .and(warp::query())
        .and_with(public_webhook_get);

    // Public POST signed webhook
    let public_post = warp::post()
        .and(warp::path("public"))
        .and(warp::path("whatsapp"))
        .and(warp::path("webhook"))
        .and(warp::path::param())
        .and(warp::header("X-Hub-Signature-256"))
        .and(warp::body::exact())
        .and_with(public_webhook_post);

    public_get.or(public_post)
}