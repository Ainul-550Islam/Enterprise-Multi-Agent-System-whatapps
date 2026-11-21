//! Client behaviour through a mock transport: URL pinning, auth header,
//! receipt parsing and error mapping — no network involved.

use mas_channel_whatsapp::client::{
    guard_egress_url, BytesResponse, ChannelClient, GraphClient, HttpTransport, TransportResponse,
    GRAPH_API_HOST, MAX_MEDIA_BYTES,
};
use mas_channel_whatsapp::outbound::OutboundPayload;
use mas_common::result::Result;
use std::sync::{Arc, Mutex};

/// Shared-capture mock transport: the test asserts on exactly what the
/// client sent.
struct MockTransport {
    calls: Arc<Mutex<Vec<(String, String, String)>>>,
    respond: TransportResponse,
    get_respond: TransportResponse,
    bytes_respond: BytesResponse,
}

impl HttpTransport for MockTransport {
    fn post_json(&self, url: &str, authorization: &str, body: &str) -> Result<TransportResponse> {
        self.calls.lock().expect("lock").push((
            url.to_owned(),
            authorization.to_owned(),
            body.to_owned(),
        ));
        Ok(self.respond.clone())
    }

    fn get(&self, url: &str, authorization: &str) -> Result<TransportResponse> {
        self.calls.lock().expect("lock").push((
            url.to_owned(),
            authorization.to_owned(),
            String::new(),
        ));
        Ok(self.get_respond.clone())
    }

    fn get_bytes(&self, url: &str, authorization: &str, max_bytes: usize) -> Result<BytesResponse> {
        self.calls.lock().expect("lock").push((
            url.to_owned(),
            authorization.to_owned(),
            format!("max={max_bytes}"),
        ));
        Ok(self.bytes_respond.clone())
    }
}

type RecordedRequest = (String, String, String);
type Recorded = Arc<Mutex<Vec<RecordedRequest>>>;

fn transport(status: u16, body: &str) -> (Recorded, MockTransport) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    (
        Arc::clone(&calls),
        MockTransport {
            calls,
            respond: TransportResponse {
                status,
                body: body.to_owned(),
            },
            get_respond: TransportResponse {
                status,
                body: body.to_owned(),
            },
            bytes_respond: BytesResponse {
                status,
                body: body.as_bytes().to_vec(),
                mime_type: None,
                redirected: false,
            },
        },
    )
}

fn text_payload() -> OutboundPayload {
    OutboundPayload::Text {
        to: "15551234567".to_owned(),
        body: "ping".to_owned(),
    }
}

#[test]
fn mock_graph_client_pins_the_meta_host_and_parses_receipts() {
    let (calls, t) = transport(
        200,
        r#"{"messaging_product":"whatsapp","messages":[{"id":"wamid.MOCK-1"}]}"#,
    );
    let client = GraphClient::new(Box::new(t), "token-abc");
    let receipt = client.send(&text_payload()).expect("send");
    assert_eq!(receipt.wamid, "wamid.MOCK-1");

    let sent = calls.lock().expect("lock");
    assert_eq!(sent.len(), 1);
    let (url, auth, body) = &sent[0];
    assert!(url.contains(GRAPH_API_HOST), "host must be pinned: {url}");
    assert!(url.ends_with("/me/messages"), "send path changed: {url}");
    assert_eq!(auth, "Bearer token-abc");
    assert!(body.contains("\"to\":\"15551234567\""), "body: {body}");
}

#[test]
fn mock_graph_client_redacts_the_token_in_debug() {
    let (_calls, t) = transport(200, "{}");
    let client = GraphClient::new(Box::new(t), "super-secret-token");
    let dbg = format!("{client:?}");
    assert!(dbg.contains("[redacted]"), "token leaked in Debug: {dbg}");
    assert!(
        !dbg.contains("super-secret-token"),
        "Debug leaked the token"
    );
}

#[test]
fn mock_graph_client_maps_provider_errors() {
    let (_calls, t) = transport(401, r#"{"error":{"message":"invalid token"}}"#);
    let client = GraphClient::new(Box::new(t), "expired");
    let err = client
        .send(&text_payload())
        .expect_err("401 must be an error");
    assert!(err.to_string().contains("whatsapp"), "got: {err}");
}

#[test]
fn mock_graph_client_maps_rate_limit_and_token_errors() {
    let (_calls, t) = transport(429, r#"{"error":{"code":130429,"message":"slow down"}}"#);
    let client = GraphClient::new(Box::new(t), "tok");
    let err = client.send(&text_payload()).expect_err("429");
    assert_eq!(err.error_code(), "RATE_LIMITED");

    let (_calls, t) = transport(401, r#"{"error":{"code":190,"message":"expired"}}"#);
    let client = GraphClient::new(Box::new(t), "tok");
    let err = client.send(&text_payload()).expect_err("401");
    assert_eq!(err.error_code(), "UNAUTHENTICATED");

    let (_calls, t) = transport(500, r#"{"error":{"code":1,"message":"boom"}}"#);
    let client = GraphClient::new(Box::new(t), "tok");
    let err = client.send(&text_payload()).expect_err("5xx");
    assert_eq!(err.error_code(), "EXTERNAL_SERVICE_ERROR");
}

#[test]
fn download_media_refuses_private_ips_redirects_and_oversize() {
    assert!(guard_egress_url("http://graph.facebook.com/v").is_err());
    assert!(guard_egress_url("https://127.0.0.1/media").is_err());
    assert!(guard_egress_url("https://10.0.0.5/media").is_err());
    assert!(guard_egress_url("https://evil.example/media").is_err());
    assert!(guard_egress_url("https://graph.facebook.com/v21.0/MEDIA-1").is_ok());

    let calls = Arc::new(Mutex::new(Vec::new()));
    let t = MockTransport {
        calls: Arc::clone(&calls),
        respond: TransportResponse {
            status: 200,
            body: "{}".into(),
        },
        get_respond: TransportResponse {
            status: 200,
            body: r#"{"url":"https://lookaside.fbsbx.com/media","mime_type":"image/jpeg","file_size":12}"#.into(),
        },
        bytes_respond: BytesResponse {
            status: 200,
            body: b"hello-image!".to_vec(),
            mime_type: Some("image/jpeg".into()),
            redirected: false,
        },
    };
    let client = GraphClient::new(Box::new(t), "tok");
    let media = client.download_media("MEDIA-1").expect("download");
    assert_eq!(media.mime_type, "image/jpeg");
    assert_eq!(media.bytes, b"hello-image!");

    let t = MockTransport {
        calls: Arc::new(Mutex::new(Vec::new())),
        respond: TransportResponse {
            status: 200,
            body: "{}".into(),
        },
        get_respond: TransportResponse {
            status: 200,
            body: r#"{"url":"https://lookaside.fbsbx.com/media","mime_type":"image/jpeg"}"#.into(),
        },
        bytes_respond: BytesResponse {
            status: 200,
            body: b"x".to_vec(),
            mime_type: None,
            redirected: true,
        },
    };
    let client = GraphClient::new(Box::new(t), "tok");
    let err = client.download_media("MEDIA-1").expect_err("redirect");
    assert!(err.to_string().contains("redirect"), "got: {err}");

    let t = MockTransport {
        calls: Arc::new(Mutex::new(Vec::new())),
        respond: TransportResponse {
            status: 200,
            body: "{}".into(),
        },
        get_respond: TransportResponse {
            status: 200,
            body: format!(
                r#"{{"url":"https://lookaside.fbsbx.com/media","mime_type":"image/jpeg","file_size":{}}}"#,
                MAX_MEDIA_BYTES + 1
            ),
        },
        bytes_respond: BytesResponse {
            status: 200,
            body: vec![],
            mime_type: None,
            redirected: false,
        },
    };
    let client = GraphClient::new(Box::new(t), "tok");
    assert!(client.download_media("MEDIA-1").is_err());
}

#[test]
fn mock_graph_client_requires_a_receipt_id() {
    let (_calls, t) = transport(200, r#"{"messages":[]}"#);
    let client = GraphClient::new(Box::new(t), "tok");
    assert!(client.send(&text_payload()).is_err());
}
