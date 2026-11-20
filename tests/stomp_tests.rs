use futures_util::{SinkExt, StreamExt};
use rustfn::http::{AuthSessionSlot, BoxFuture, TokenProvider};
use rustfn::protocol::{AccountId, ConversationType, ProductUserId, RustfnError};
use rustfn::stomp::{
    EOSConnectNotification, STOMP, STOMPCommand, STOMPConfig, STOMPEvent, STOMPFrame,
    STOMPMessageDedupe, parse_connect_notification,
};
use secrecy::SecretString;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_util::sync::CancellationToken;

struct STOMPMockTokenProvider {
    token: Mutex<String>,
    refresh_calls: AtomicUsize,
}

impl STOMPMockTokenProvider {
    fn new(token: &str) -> Self {
        Self {
            token: Mutex::new(token.to_owned()),
            refresh_calls: AtomicUsize::new(0),
        }
    }
}

impl TokenProvider for STOMPMockTokenProvider {
    fn bearer_token(&self, _slot: AuthSessionSlot) -> BoxFuture<'_, Result<SecretString, RustfnError>> {
        Box::pin(async move { Ok(SecretString::from(self.token.lock().await.clone())) })
    }

    fn refresh_on_invalid_token<'a>(
        &'a self,
        _slot: AuthSessionSlot,
        _failed_token: &'a str,
    ) -> BoxFuture<'a, Result<SecretString, RustfnError>> {
        Box::pin(async move {
            let n = self.refresh_calls.fetch_add(1, Ordering::SeqCst) + 1;
            let mut guard = self.token.lock().await;
            *guard = format!("eos-refreshed-{n}");
            Ok(SecretString::from(guard.clone()))
        })
    }
}

#[test]
fn stomp_frame_roundtrip_and_dedupe_ring() {
    let raw = "MESSAGE\ndestination:deploymentId/abc/epicAccountId/def\nmessage-id:123\n\n{\"type\":\"presence.v1.UPDATE\"}\x00";
    let parsed = STOMPFrame::parse(raw).unwrap().unwrap();
    assert_eq!(parsed.command, STOMPCommand::Message);
    assert_eq!(
        parsed.header("destination"),
        Some("deploymentId/abc/epicAccountId/def")
    );
    assert_eq!(parsed.body, "{\"type\":\"presence.v1.UPDATE\"}");

    let reencoded = parsed.encode();
    let reparsed = STOMPFrame::parse(&reencoded).unwrap().unwrap();
    assert_eq!(parsed, reparsed);

    assert!(STOMPFrame::parse("\n").unwrap().is_none());
    assert_eq!(STOMPFrame::parse("MESSAGE\n\nhello\n\0").unwrap().unwrap().body, "hello\n");
    assert_eq!(STOMPFrame::parse("MESSAGE\ncontent-length:3\n\na\0b\0").unwrap().unwrap().body, "a\0b");
    assert!(STOMPFrame::parse("MESSAGE\ncontent-length:4\n\na\0b\0").is_err());

    let mut dedupe = STOMPMessageDedupe::new(3);
    assert!(dedupe.mark_if_new("m1"));
    assert!(!dedupe.mark_if_new("m1"));
    assert!(dedupe.mark_if_new("m2"));
    assert!(dedupe.mark_if_new("m3"));
    assert!(dedupe.mark_if_new("m4"));
    assert_eq!(dedupe.len(), 3);
    assert!(dedupe.mark_if_new("m1"));
}

#[tokio::test]
async fn stomp_client_subscribes_dedupes_and_recovers_4019() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conn_counter = Arc::new(AtomicUsize::new(0));
    let conn_counter_srv = Arc::clone(&conn_counter);

    let server_task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let conn_id_num = conn_counter_srv.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let ws = accept_hdr_async(stream, |req: &Request, resp: Response| {
                    assert!(req.headers().contains_key("authorization"));
                    assert_eq!(
                        req.headers().get("epic-connect-protocol").and_then(|v| v.to_str().ok()),
                        Some("stomp")
                    );
                    assert!(req.headers().contains_key("epic-connect-device-id"));
                    Ok(resp)
                })
                .await
                .unwrap();

                let (mut tx, mut rx) = ws.split();

                let connect_text = rx.next().await.unwrap().unwrap().into_text().unwrap();
                let connect_frame = STOMPFrame::parse(&connect_text).unwrap().unwrap();
                assert_eq!(connect_frame.command, STOMPCommand::Connect);

                let connected = STOMPFrame::new(STOMPCommand::Connected)
                    .with_header("version", "1.2")
                    .with_header("session", format!("sess-{conn_id_num}"))
                    .with_header("heart-beat", "0,50");
                tx.send(Message::text(connected.encode())).await.unwrap();

                let sub_pub = STOMPFrame::parse(&rx.next().await.unwrap().unwrap().into_text().unwrap())
                    .unwrap()
                    .unwrap();
                let sub_priv = STOMPFrame::parse(&rx.next().await.unwrap().unwrap().into_text().unwrap())
                    .unwrap()
                    .unwrap();
                let sub_puid = STOMPFrame::parse(&rx.next().await.unwrap().unwrap().into_text().unwrap())
                    .unwrap()
                    .unwrap();

                assert_eq!(sub_pub.command, STOMPCommand::Subscribe);
                assert!(sub_pub.header("id").unwrap().starts_with("sub-eas-"));
                assert_eq!(
                    sub_priv.header("ec-coord-temporary-subscription"),
                    Some("parties-internal")
                );
                assert!(sub_puid.header("destination").unwrap().contains("productUserId/0002abcdef"));

                let core_msg = STOMPFrame::new(STOMPCommand::Message).with_body(
                    json!({
                        "type": "core.connect.v1.connected",
                        "correlationId": "corr-1",
                        "timestamp": 1700000000_u64,
                        "connectionId": format!("conn-{conn_id_num}")
                    })
                    .to_string(),
                );
                tx.send(Message::text(core_msg.encode())).await.unwrap();

                if conn_id_num == 0 {
                    // Send duplicate party.v2.MEMBER_JOINED frames with identical id
                    let dup_body = json!({
                        "type": "party.v2.MEMBER_JOINED",
                        "id": "dup-id-42",
                        "correlationId": "c2",
                        "timestamp": 1700000001_u64,
                        "payload": {
                            "party_id": "party-xyz",
                            "account_id": "bbbb1111cccc2222dddd3333eeee4444",
                            "account_dn": "Joiner",
                            "revision": 0,
                            "joined_at": "2026-01-01T00:00:00Z",
                            "updated_at": "2026-01-01T00:00:00Z"
                        }
                    })
                    .to_string();

                    let m1 = STOMPFrame::new(STOMPCommand::Message)
                        .with_header("destination", sub_pub.header("destination").unwrap())
                        .with_body(dup_body.clone());
                    let m2 = STOMPFrame::new(STOMPCommand::Message)
                        .with_header("destination", sub_priv.header("destination").unwrap())
                        .with_body(dup_body);

                    tx.send(Message::text(m1.encode())).await.unwrap();
                    tx.send(Message::text(m2.encode())).await.unwrap();

                    // Trigger 4019 expired token ERROR to test refresh + reconnect
                    let err_frame = STOMPFrame::new(STOMPCommand::Error).with_body(
                        json!({
                            "statusCode": 4019,
                            "message": "Token expired"
                        })
                        .to_string(),
                    );
                    tx.send(Message::text(err_frame.encode())).await.unwrap();
                } else {
                    // On reconnected session, send one matchmaking notification on PUID destination
                    let mm_msg = STOMPFrame::new(STOMPCommand::Message)
                        .with_header("destination", sub_puid.header("destination").unwrap())
                        .with_body(
                            json!({
                                "type": "mms/v2.MatchmakingAssigned",
                                "id": "mm-1",
                                "payload": { "ticketId": "ticket-99" }
                            })
                            .to_string(),
                        );
                    tx.send(Message::text(mm_msg.encode())).await.unwrap();
                    while let Some(Ok(_)) = rx.next().await {}
                }
            });
        }
    });

    let provider = Arc::new(STOMPMockTokenProvider::new("eos-initial"));
    let account_id = AccountId::parse("aaaa1111bbbb2222cccc3333dddd4444").unwrap();
    let puid = ProductUserId::parse("0002abcdef0000000000000000000001").unwrap();
    let cfg = STOMPConfig {
        wss_url: format!("ws://{addr}"),
        default_heartbeat: Duration::from_millis(50),
        connection_timeout: Duration::from_secs(2),
        max_connection_retries: 2,
        retry_delay: Duration::from_millis(20),
        ..STOMPConfig::default()
    };

    let client = STOMP::new(cfg, account_id, Some(puid), provider.clone());
    let mut events = client.subscribe_events();
    let cancel = CancellationToken::new();

    let ids = client.connect(cancel.clone()).await.expect("initial STOMP connect succeeds");
    assert_eq!(ids.connection_id, "conn-0");
    assert!(ids.public_connection_id.starts_with("conn-0#sub-eas-"));
    assert!(ids.private_connection_id.starts_with("conn-0#sub-eas-private-"));

    let mut joined_count = 0usize;
    let mut reconnected_conn1 = false;
    let mut saw_mm = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);

    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(ev)) = tokio::time::timeout(Duration::from_millis(250), events.recv()).await {
            match &*ev {
                STOMPEvent::Notification(EOSConnectNotification::PartyMemberJoined { .. }) => {
                    joined_count += 1;
                }
                STOMPEvent::Connected(new_ids) if new_ids.connection_id == "conn-1" => {
                    reconnected_conn1 = true;
                }
                STOMPEvent::Notification(EOSConnectNotification::MatchmakingEvent {
                    event_type,
                    ..
                }) if event_type == "mms/v2.MatchmakingAssigned" => {
                    saw_mm = true;
                    if reconnected_conn1 {
                        break;
                    }
                }
                _ => {}
            }
        }
    }

    assert_eq!(joined_count, 1, "duplicate party.v2.MEMBER_JOINED id must be deduplicated");
    assert!(reconnected_conn1, "STOMP must reconnect after 4019");
    assert!(saw_mm, "PUID subscription must receive mms/v2 event after reconnect");
    assert_eq!(provider.refresh_calls.load(Ordering::SeqCst), 1);

    client.disconnect().await;
    cancel.cancel();
    server_task.abort();
}

#[test]
fn stomp_parses_epic_party_chat_message_notification() {
    let raw = json!({
        "type": "social.chat.v1.NEW_MESSAGE",
        "id": "chat-msg-1",
        "payload": {
            "namespace": "_",
            "conversation": {
                "conversationId": "ep-adedd33f895e42348e3dadf26a68218f",
                "type": "epic_party"
            },
            "message": {
                "body": "!ping",
                "senderId": "11112222333344445555666677778888",
                "time": 1700000000123_u64
            }
        }
    })
    .to_string();

    let (msg_id, notif) = parse_connect_notification(&raw, None).expect("epic_party parses cleanly");
    assert_eq!(msg_id.as_deref(), Some("chat-msg-1"));
    match notif {
        EOSConnectNotification::SocialChatNewMessage { payload, .. } => {
            assert_eq!(payload.conversation.conversation_type, ConversationType::PartyChat);
            assert_eq!(
                payload.conversation.conversation_id,
                "ep-adedd33f895e42348e3dadf26a68218f"
            );
            assert_eq!(payload.message.body, "!ping");
        }
        other => panic!("unexpected notification variant: {other:?}"),
    }
}
