use futures_util::{SinkExt, StreamExt};
use rustfn::http::{AuthSessionSlot, BoxFuture, TokenProvider};
use rustfn::protocol::{AccountId, PartyId, Platform, RustfnError};
use rustfn::xmpp::{
    QueuedMemberMetadataUpdate, XMPPAdminMessage, XMPP, XMPPConfig, XMPPEvent,
    XMPPIncomingStanza, XMPPMetadataStore, build_fortnite_resource, parse_admin_message,
    parse_incoming_stanza, presence_frame,
};
use secrecy::{ExposeSecret, SecretString};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_util::sync::CancellationToken;

struct StaticTokenProvider {
    token: Mutex<String>,
    refresh_calls: AtomicUsize,
}

impl StaticTokenProvider {
    fn new(token: &str) -> Self {
        Self {
            token: Mutex::new(token.to_owned()),
            refresh_calls: AtomicUsize::new(0),
        }
    }
}

impl TokenProvider for StaticTokenProvider {
    fn bearer_token(&self, _slot: AuthSessionSlot) -> BoxFuture<'_, Result<SecretString, RustfnError>> {
        Box::pin(async move { Ok(SecretString::from(self.token.lock().await.clone())) })
    }

    fn refresh_on_invalid_token<'a>(
        &'a self,
        _slot: AuthSessionSlot,
        _failed_token: &'a str,
    ) -> BoxFuture<'a, Result<SecretString, RustfnError>> {
        Box::pin(async move {
            self.refresh_calls.fetch_add(1, Ordering::SeqCst);
            let mut guard = self.token.lock().await;
            *guard = "refreshed-xmpp-token".to_owned();
            Ok(SecretString::from(guard.clone()))
        })
    }
}

#[test]
fn xmpp_stanza_codec_parses_and_escapes_cleanly() {
    let res = build_fortnite_resource(Platform::Win);
    assert!(res.starts_with("V2:Fortnite:WIN::"));
    assert_eq!(res.len(), "V2:Fortnite:WIN::".len() + 32);

    let pres = presence_frame(Some(r#"{"Status":"In & <Lobby>"}"#), Some("away"), false);
    assert!(pres.contains("<show>away</show>"));
    assert!(pres.contains("&amp;"));
    assert!(pres.contains("&lt;Lobby&gt;"));

    let feats = parse_incoming_stanza(
        "<stream:features xmlns:stream='http://etherx.jabber.org/streams'><mechanisms xmlns='urn:ietf:params:xml:ns:xmpp-sasl'><mechanism>PLAIN</mechanism></mechanisms></stream:features>",
    )
    .unwrap();
    assert_eq!(
        feats,
        XMPPIncomingStanza::StreamFeatures {
            mechanisms: vec!["PLAIN".to_owned()],
            has_bind: false,
        }
    );

    let bind = parse_incoming_stanza(
        "<iq type='result' id='bind_1'><bind xmlns='urn:ietf:params:xml:ns:xmpp-bind'><jid>aaaa@prod.ol.epicgames.com/V2:Fortnite:WIN::1234</jid></bind></iq>",
    )
    .unwrap();
    assert_eq!(
        bind,
        XMPPIncomingStanza::IqResult {
            id: Some("bind_1".to_owned()),
            jid: Some("aaaa@prod.ol.epicgames.com/V2:Fortnite:WIN::1234".to_owned()),
        }
    );
}

#[test]
fn xmpp_admin_message_decodes_friend_and_party_deltas() {
    let friend_json = json!({
        "type": "com.epicgames.friends.core.apiobjects.Friend",
        "payload": {
            "accountId": "bbbb1111cccc2222dddd3333eeee4444",
            "status": "PENDING",
            "direction": "INBOUND",
            "favorite": false,
            "created": "2026-01-01T00:00:00Z"
        }
    })
    .to_string();
    match parse_admin_message(&friend_json).unwrap() {
        XMPPAdminMessage::FriendUpdate(f) => {
            assert_eq!(f.status, "PENDING");
            assert_eq!(f.direction.as_deref(), Some("INBOUND"));
        }
        other => panic!("unexpected variant: {other:?}"),
    }

    let delta_json = json!({
        "type": "com.epicgames.social.party.notification.v0.MEMBER_STATE_UPDATED",
        "party_id": "party1234567890",
        "account_id": "bbbb1111cccc2222dddd3333eeee4444",
        "account_dn": "Tester",
        "revision": 4,
        "member_state_updated": {
            "Default:FrontendEmote_j": "{\"FrontendEmote\":{\"emoteItemDef\":\"EID_Dance\"}}"
        },
        "member_state_removed": ["Default:OldKey_s"]
    })
    .to_string();
    match parse_admin_message(&delta_json).unwrap() {
        XMPPAdminMessage::PartyMemberStateUpdated(delta) => {
            assert_eq!(delta.revision, 4);
            assert_eq!(delta.member_state_removed, vec!["Default:OldKey_s"]);
        }
        other => panic!("unexpected variant: {other:?}"),
    }
}

#[tokio::test]
async fn xmpp_metadata_store_sorts_by_revision_and_expires() {
    let store = XMPPMetadataStore::new(Duration::from_millis(60));
    let party = PartyId::from_raw("p1");
    let acc = AccountId::parse("aaaa1111bbbb2222cccc3333dddd4444").unwrap();

    for rev in [3_u64, 1, 2] {
        let mut updated = BTreeMap::new();
        updated.insert(format!("k{rev}_s"), json!(format!("v{rev}")).as_str().unwrap().to_owned());
        store.push(
            party.clone(),
            QueuedMemberMetadataUpdate {
                account_id: acc.clone(),
                account_dn: None,
                revision: rev,
                member_state_updated: updated.into(),
                member_state_removed: Vec::new(),
            },
        );
    }

    let drained = store.drain_sorted(&party, &acc, 2);
    let revs: Vec<u64> = drained.iter().map(|u| u.revision).collect();
    assert_eq!(revs, vec![2, 3]);
    assert!(store.drain_sorted(&party, &acc, 0).is_empty());

    store.push(
        party.clone(),
        QueuedMemberMetadataUpdate {
            account_id: acc.clone(),
            account_dn: None,
            revision: 9,
            member_state_updated: BTreeMap::new().into(),
            member_state_removed: Vec::new(),
        },
    );
    tokio::time::sleep(Duration::from_millis(90)).await;
    assert!(store.drain_sorted(&party, &acc, 0).is_empty());
}

#[tokio::test]
async fn xmpp_client_handshake_admin_messages_and_bounded_retries() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_count = Arc::new(AtomicUsize::new(0));
    let accept_count_srv = Arc::clone(&accept_count);

    let server_task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let conn_idx = accept_count_srv.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let ws = accept_hdr_async(stream, |req: &Request, mut resp: Response| {
                    assert_eq!(
                        req.headers().get("sec-websocket-protocol").and_then(|v| v.to_str().ok()),
                        Some("xmpp")
                    );
                    resp.headers_mut()
                        .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("xmpp"));
                    Ok(resp)
                })
                .await
                .unwrap();
                let (mut tx, mut rx) = ws.split();

                // Connection 0 succeeds full RFC 7395 handshake, sends 1 admin stanza, then drops pings.
                // Subsequent connections reject auth to exercise strictly bounded retry counter (< max_retries).
                if conn_idx == 0 {
                    let _ = rx.next().await;
                    tx.send(Message::text("<open xmlns='urn:ietf:params:xml:ns:xmpp-framing' id='s1' from='prod.ol.epicgames.com'/>")).await.unwrap();
                    tx.send(Message::text("<stream:features><mechanisms xmlns='urn:ietf:params:xml:ns:xmpp-sasl'><mechanism>PLAIN</mechanism></mechanisms></stream:features>")).await.unwrap();

                    let auth_msg = rx.next().await.unwrap().unwrap().into_text().unwrap();
                    assert!(auth_msg.contains("mechanism='PLAIN'"));
                    tx.send(Message::text("<success xmlns='urn:ietf:params:xml:ns:xmpp-sasl'/>")).await.unwrap();

                    let _ = rx.next().await;
                    tx.send(Message::text("<open xmlns='urn:ietf:params:xml:ns:xmpp-framing' id='s2' from='prod.ol.epicgames.com'/>")).await.unwrap();
                    tx.send(Message::text("<stream:features><bind xmlns='urn:ietf:params:xml:ns:xmpp-bind'/></stream:features>")).await.unwrap();

                    let bind_req = rx.next().await.unwrap().unwrap().into_text().unwrap();
                    assert!(bind_req.contains("id='bind_1'"));
                    tx.send(Message::text("<iq type='result' id='bind_1'><bind xmlns='urn:ietf:params:xml:ns:xmpp-bind'><jid>aaaa1111bbbb2222cccc3333dddd4444@prod.ol.epicgames.com/V2:Fortnite:WIN::TEST</jid></bind></iq>")).await.unwrap();

                    let sess_req = rx.next().await.unwrap().unwrap().into_text().unwrap();
                    assert!(sess_req.contains("id='sess_1'"));
                    tx.send(Message::text("<iq type='result' id='sess_1'/>")).await.unwrap();

                    let _ = rx.next().await;

                    let payload = json!({
                        "type": "USER_BLOCKLIST_UPDATE",
                        "accountId": "bbbb1111cccc2222dddd3333eeee4444",
                        "status": "BLOCKED"
                    });
                    tx.send(Message::text(format!(
                        "<message from='xmpp-admin@prod.ol.epicgames.com' type='normal'><body>{}</body></message>",
                        payload
                    ))).await.unwrap();

                    // Intentionally never answer pings so watchdog triggers timeout
                    while let Some(Ok(_)) = rx.next().await {}
                } else {
                    let _ = rx.next().await;
                    tx.send(Message::text("<stream:features><mechanisms xmlns='urn:ietf:params:xml:ns:xmpp-sasl'><mechanism>PLAIN</mechanism></mechanisms></stream:features>")).await.unwrap();
                    let _ = rx.next().await;
                    tx.send(Message::text("<failure xmlns='urn:ietf:params:xml:ns:xmpp-sasl'><not-authorized/></failure>")).await.unwrap();
                    let _ = tx.close().await;
                }
            });
        }
    });

    let provider = Arc::new(StaticTokenProvider::new("initial-xmpp-token"));
    let account_id = AccountId::parse("aaaa1111bbbb2222cccc3333dddd4444").unwrap();
    let cfg = XMPPConfig {
        wss_url: format!("ws://{addr}"),
        domain: "prod.ol.epicgames.com".to_owned(),
        keepalive_interval: Duration::from_millis(40),
        ping_timeout: Duration::from_millis(120),
        connection_timeout: Duration::from_secs(2),
        max_connection_retries: 2,
        retry_delay: Duration::from_millis(20),
        ..XMPPConfig::default()
    };

    let client = XMPP::new(cfg, account_id, provider.clone());
    let mut events = client.subscribe_events();
    let cancel = CancellationToken::new();

    client.connect(cancel.clone()).await.expect("initial XMPP connect succeeds");
    assert!(client.is_connected());

    let mut saw_admin = false;
    let mut saw_exhausted = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(ev)) = tokio::time::timeout(Duration::from_millis(250), events.recv()).await {
            match &*ev {
                XMPPEvent::AdminMessage(XMPPAdminMessage::UserBlocklistUpdate(upd)) => {
                    assert_eq!(upd.status, "BLOCKED");
                    saw_admin = true;
                }
                XMPPEvent::ReconnectExhausted { attempts, .. } => {
                    assert_eq!(*attempts, 2);
                    saw_exhausted = true;
                    break;
                }
                _ => {}
            }
        }
    }

    assert!(saw_admin, "expected xmpp-admin UserBlocklistUpdate stanza");
    assert!(saw_exhausted, "expected ReconnectExhausted at retry limit 2");
    assert!(provider.refresh_calls.load(Ordering::SeqCst) >= 1);
    assert_eq!(
        provider.bearer_token(AuthSessionSlot::Fortnite).await.unwrap().expose_secret(),
        "refreshed-xmpp-token"
    );

    client.disconnect().await;
    cancel.cancel();
    server_task.abort();
}
