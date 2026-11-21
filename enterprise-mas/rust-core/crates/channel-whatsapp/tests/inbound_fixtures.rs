//! Fixture-driven parsing: every fixture under `tests/fixtures/` must parse
//! into the exact typed shape the pipeline consumes.

use mas_channel_whatsapp::errors::CODE_REENGAGEMENT_WINDOW;
use mas_channel_whatsapp::inbound::{parse_envelope, DeliveryStatus, InboundKind};

#[test]
fn parses_all_inbound_fixtures() {
    let text = include_str!("fixtures/inbound_text.json");
    let media = include_str!("fixtures/inbound_media.json");
    let button = include_str!("fixtures/inbound_button.json");
    let status = include_str!("fixtures/status_delivered.json");

    let b = parse_envelope(text.as_bytes()).expect("text fixture parses");
    assert_eq!(b.messages.len(), 1);
    let m = &b.messages[0];
    assert_eq!(m.from, "15551234567");
    assert!(m.wamid.starts_with("wamid."));
    assert_eq!(
        m.kind,
        InboundKind::Text {
            body: "Hello, is my order shipped?".to_owned()
        }
    );
    assert!(b.statuses.is_empty());

    let b = parse_envelope(media.as_bytes()).expect("media fixture parses");
    assert_eq!(b.messages.len(), 1);
    assert_eq!(
        b.messages[0].kind,
        InboundKind::Media {
            media_id: "MEDIA-AUDIO-1".to_owned(),
            mime_type: "audio/ogg; codecs=opus".to_owned(),
        }
    );

    let b = parse_envelope(button.as_bytes()).expect("button fixture parses");
    assert_eq!(b.messages.len(), 1);
    assert_eq!(
        b.messages[0].kind,
        InboundKind::Button {
            payload: "PAYLOAD-SHIP-YES".to_owned(),
            text: "Yes, ship it".to_owned(),
        }
    );

    let b = parse_envelope(status.as_bytes()).expect("status fixture parses");
    assert!(b.messages.is_empty());
    assert_eq!(b.statuses.len(), 1);
    assert_eq!(b.statuses[0].status, DeliveryStatus::Delivered);
    assert!(b.statuses[0].wamid.starts_with("wamid."));
    assert!(b.statuses[0].errors.is_empty());
}

#[test]
fn parses_location_reaction_unsupported_and_failed_status() {
    let loc = parse_envelope(include_str!("fixtures/inbound_location.json").as_bytes())
        .expect("location");
    match &loc.messages[0].kind {
        InboundKind::Location {
            latitude,
            longitude,
            name,
            ..
        } => {
            assert_eq!(latitude, "23.8103");
            assert_eq!(longitude, "90.4125");
            assert_eq!(name, "Dhaka");
        },
        other => panic!("expected location, got {other:?}"),
    }

    let reaction = parse_envelope(include_str!("fixtures/inbound_reaction.json").as_bytes())
        .expect("reaction");
    match &reaction.messages[0].kind {
        InboundKind::Reaction { emoji, message_id } => {
            assert_eq!(emoji, "👍");
            assert!(message_id.starts_with("wamid."));
        },
        other => panic!("expected reaction, got {other:?}"),
    }

    let unsupported = parse_envelope(include_str!("fixtures/inbound_unsupported.json").as_bytes())
        .expect("unsupported");
    match &unsupported.messages[0].kind {
        InboundKind::Unsupported { raw_type } => assert_eq!(raw_type, "unsupported"),
        other => panic!("expected unsupported, got {other:?}"),
    }

    let failed = parse_envelope(include_str!("fixtures/status_failed.json").as_bytes())
        .expect("failed status");
    assert_eq!(failed.statuses[0].status, DeliveryStatus::Failed);
    assert_eq!(failed.statuses[0].errors.len(), 1);
    assert_eq!(failed.statuses[0].errors[0].code, CODE_REENGAGEMENT_WINDOW);
}

#[test]
fn rejects_an_envelope_without_a_spine() {
    assert!(parse_envelope(br#"{"object":"whatsapp_business_account"}"#).is_err());
    assert!(parse_envelope(br#"{"entry":[{"id":"x"}]}"#).is_err());
    assert!(parse_envelope(b"not json at all").is_err());
}

#[test]
fn keeps_unknown_message_types_visible() {
    let unknown = include_str!("fixtures/inbound_text.json").replace("text", "sticker");
    let b = parse_envelope(unknown.as_bytes()).expect("parses");
    match &b.messages[0].kind {
        InboundKind::Other { raw_type } => assert_eq!(raw_type, "sticker"),
        other => panic!("expected Other, got {other:?}"),
    }
}
