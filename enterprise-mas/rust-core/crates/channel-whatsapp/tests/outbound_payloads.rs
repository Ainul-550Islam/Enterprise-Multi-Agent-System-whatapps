//! Outbound payload construction: the Graph body shape is asserted as JSON.

use mas_channel_whatsapp::outbound::OutboundPayload;

#[test]
fn builds_a_text_payload() {
    let p = OutboundPayload::Text {
        to: "15551234567".to_owned(),
        body: "Your order shipped.".to_owned(),
    };
    let v = p.to_graph_json().expect("serializes");
    assert_eq!(v["messaging_product"], "whatsapp");
    assert_eq!(v["recipient_type"], "individual");
    assert_eq!(v["to"], "15551234567");
    assert_eq!(v["type"], "text");
    assert_eq!(v["text"]["body"], "Your order shipped.");
    assert_eq!(v["text"]["preview_url"], false);
}

#[test]
fn builds_templated_payload() {
    let p = OutboundPayload::Template {
        to: "15551234567".to_owned(),
        name: "order_shipped".to_owned(),
        language: "en".to_owned(),
        parameters: vec!["ORD-42".to_owned(), "tomorrow".to_owned()],
    };
    let v = p.to_graph_json().expect("serializes");
    assert_eq!(v["type"], "template");
    assert_eq!(v["template"]["name"], "order_shipped");
    assert_eq!(v["template"]["language"]["code"], "en");
    let params = v["template"]["components"][0]["parameters"]
        .as_array()
        .expect("components[0].parameters");
    assert_eq!(params.len(), 2);
    assert_eq!(
        params[0],
        serde_json::json!({"type": "text", "text": "ORD-42"})
    );
    assert_eq!(params[1]["text"], "tomorrow");
}

#[test]
fn rejects_a_template_without_identity() {
    let p = OutboundPayload::Template {
        to: "15551234567".to_owned(),
        name: String::new(),
        language: "en".to_owned(),
        parameters: Vec::new(),
    };
    assert!(p.to_graph_json().is_err());
}

#[test]
fn builds_interactive_button_list_media_and_mark_as_read() {
    let buttons = OutboundPayload::InteractiveButtons {
        to: "15551234567".to_owned(),
        body: "Pick one".to_owned(),
        buttons: vec![("yes".into(), "Yes".into()), ("no".into(), "No".into())],
    };
    let v = buttons.to_graph_json().expect("buttons");
    assert_eq!(v["type"], "interactive");
    assert_eq!(v["interactive"]["type"], "button");
    assert_eq!(
        v["interactive"]["action"]["buttons"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    let list = OutboundPayload::InteractiveList {
        to: "15551234567".to_owned(),
        body: "Choose".to_owned(),
        button: "Open".to_owned(),
        section_title: "Options".to_owned(),
        rows: vec![("a".into(), "Alpha".into())],
    };
    let v = list.to_graph_json().expect("list");
    assert_eq!(v["interactive"]["type"], "list");
    assert_eq!(
        v["interactive"]["action"]["sections"][0]["rows"][0]["id"],
        "a"
    );

    let media = OutboundPayload::Media {
        to: "15551234567".to_owned(),
        kind: "image".to_owned(),
        media_id: "MEDIA-1".to_owned(),
        caption: Some("photo".to_owned()),
    };
    let v = media.to_graph_json().expect("media");
    assert_eq!(v["type"], "image");
    assert_eq!(v["image"]["id"], "MEDIA-1");
    assert_eq!(v["image"]["caption"], "photo");

    let read = OutboundPayload::MarkAsRead {
        message_id: "wamid.1".to_owned(),
    };
    let v = read.to_graph_json().expect("read");
    assert_eq!(v["status"], "read");
    assert_eq!(v["message_id"], "wamid.1");
}

#[test]
fn rejects_oversized_text() {
    let p = OutboundPayload::Text {
        to: "15551234567".to_owned(),
        body: "x".repeat(4097),
    };
    assert!(p.to_graph_json().is_err());
}
