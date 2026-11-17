use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use chrono::{Duration as ChronoDuration, Utc};
use futures_util::{SinkExt, StreamExt};
use rustfn_auth::{AuthMethod, DeviceAuthCredentials};
use rustfn_client::{ClientConfig, ExtraServiceEndpoints, Client};
use rustfn_party::ReceivedPartyInvitation;
use rustfn_protocol::{
    AccountId, AuthClient, PartyDiscoverability, PartyId, PartyJoinability, RustfnError,
    ServiceEndpoints,
};
use rustfn_stomp::{STOMPCommand, STOMPFrame};
use secrecy::SecretString;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request as WsRequest, Response as WsResponse};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn make_account_id(index: usize) -> AccountId {
    AccountId::parse(format!("{index:032x}")).expect("hex account id")
}

fn parse_form_params(req: &Request) -> HashMap<String, String> {
    url::form_urlencoded::parse(&req.body)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

fn extract_bearer_raw(req: &Request) -> Option<String> {
    let val = req
        .headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())?;
    val.strip_prefix("bearer ")
        .or_else(|| val.strip_prefix("Bearer "))
        .map(str::to_owned)
}

fn account_from_fn_token(token: &str) -> Option<String> {
    let rest = token.strip_prefix("fn-tok-")?;
    rest.split('-').next().map(str::to_owned)
}

fn account_from_eos_token(token: &str) -> Option<String> {
    let rest = token.strip_prefix("eos-tok-")?;
    rest.split('-').next().map(str::to_owned)
}

type STOMPClientMap = Arc<StdMutex<HashMap<String, (mpsc::UnboundedSender<String>, String)>>>;

struct MockEnv {
    http_server: MockServer,
    xmpp_addr: SocketAddr,
    stomp_addr: SocketAddr,
    stomp_clients: STOMPClientMap,
    fn_party_patches: Arc<StdMutex<Vec<Value>>>,
    eos_presence_patches: Arc<StdMutex<Vec<Value>>>,
    shutdown: CancellationToken,
}

impl Drop for MockEnv {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl MockEnv {
    async fn start() -> Self {
        Self::start_with_options(true).await
    }

    async fn start_without_default_fn_party_routes() -> Self {
        Self::start_with_options(false).await
    }

    async fn start_with_options(mount_default_fn_party_routes: bool) -> Self {
        let http_server = MockServer::start().await;
        let shutdown = CancellationToken::new();

        let xmpp_listener = TcpListener::bind("127.0.0.1:0").await.expect("xmpp bind");
        let xmpp_addr = xmpp_listener.local_addr().expect("xmpp addr");

        let stomp_listener = TcpListener::bind("127.0.0.1:0").await.expect("stomp bind");
        let stomp_addr = stomp_listener.local_addr().expect("stomp addr");

        let stomp_clients: STOMPClientMap = Arc::new(StdMutex::new(HashMap::new()));

        spawn_mock_xmpp_server(xmpp_listener, shutdown.clone());
        spawn_mock_stomp_server(stomp_listener, Arc::clone(&stomp_clients), shutdown.clone());

        let fn_party_patches = Arc::new(StdMutex::new(Vec::new()));
        let eos_presence_patches = Arc::new(StdMutex::new(Vec::new()));
        let token_seq = Arc::new(AtomicUsize::new(1));
        let party_seq = Arc::new(AtomicUsize::new(1));

        mount_mock_routes(
            &http_server,
            token_seq,
            party_seq,
            Arc::clone(&fn_party_patches),
            Arc::clone(&eos_presence_patches),
            mount_default_fn_party_routes,
        )
        .await;

        Self {
            http_server,
            xmpp_addr,
            stomp_addr,
            stomp_clients,
            fn_party_patches,
            eos_presence_patches,
            shutdown,
        }
    }

    fn client_config(&self) -> ClientConfig {
        let base = self.http_server.uri();
        let mut cfg = ClientConfig::default();
        cfg.check_eula = false;
        cfg.party_build_id = Some("1:3:39001234".to_owned());
        cfg.endpoints = ServiceEndpoints {
            oauth_token_create: format!("{base}/account/api/oauth/token").into(),
            oauth_token_verify: format!("{base}/account/api/oauth/verify").into(),
            oauth_token_kill: format!("{base}/account/api/oauth/sessions/kill").into(),
            oauth_exchange: format!("{base}/account/api/oauth/exchange").into(),
            oauth_device_code: format!("{base}/account/api/oauth/deviceAuthorization").into(),
            account_public: format!("{base}/account/api/public/account").into(),
            init_eula: format!("{base}/eulatracking/api/public/agreements/fn").into(),
            init_request_access: format!("{base}/fortnite/api/storeaccess/v1/request_access").into(),
            launcher_public: format!("{base}/launcher/api").into(),
            xmpp_wss: format!("ws://127.0.0.1:{}", self.xmpp_addr.port()).into(),
            eos_stomp_wss: format!("ws://127.0.0.1:{}", self.stomp_addr.port()).into(),
            eos_token: format!("{base}/epic/oauth/v2/token").into(),
            eos_token_info: format!("{base}/epic/oauth/v2/tokenInfo").into(),
            eos_token_revoke: format!("{base}/epic/oauth/v2/revoke").into(),
            eos_chat: format!("{base}/epic/chat").into(),
            eos_presence: format!("{base}/epic/presence/v1").into(),
            eos_party: format!("{base}/epic/party").into(),
            eos_party_internal: format!("{base}/epic/party/internal/v2").into(),
            eos_users: format!("{base}/auth/v1/users").into(),
            publickey: format!("{base}/publickey").into(),
            publickey_fallback: format!("{base}/publickey-fallback").into(),
            br_party: format!("{base}/party/api/v1/Fortnite").into(),
            friends: format!("{base}/friends/api/v1").into(),
            friends_public: format!("{base}/friends/api/public/friends").into(),
            friends_blocklist_public: format!("{base}/friends/api/public/blocklist").into(),
            ..ServiceEndpoints::default()
        };
        cfg.extra_endpoints = ExtraServiceEndpoints {
            account_avatar: format!("{base}/v1/avatar"),
            account_global_profile: format!("{base}/profiles"),
            br_matchmaking_request: format!("{base}/matchmaking/matchMakingRequest"),
            br_server_status: format!("{base}/lightswitch/status"),
            br_sac: format!("{base}/sac"),
            eos_presence_internal: format!("{base}/epic/presence/internal/v1/_"),
        };
        cfg.http.retry.rest_retry_limit = 1;
        cfg.http.retry.backoff_start = Duration::from_millis(5);
        cfg.retry = cfg.http.retry.clone();
        cfg.xmpp.keepalive_interval = Duration::from_secs(30);
        cfg.xmpp.ping_timeout = Duration::from_secs(30);
        cfg.xmpp.retry_delay = Duration::from_millis(20);
        cfg.stomp.retry_delay = Duration::from_millis(20);
        cfg
    }
}

async fn mount_mock_routes(
    server: &MockServer,
    token_seq: Arc<AtomicUsize>,
    party_seq: Arc<AtomicUsize>,
    fn_party_patches: Arc<StdMutex<Vec<Value>>>,
    eos_presence_patches: Arc<StdMutex<Vec<Value>>>,
    mount_default_fn_party_routes: bool,
) {
    let seq_oauth = Arc::clone(&token_seq);
    Mock::given(method("POST"))
        .and(path("/account/api/oauth/token"))
        .respond_with(move |req: &Request| {
            let params = parse_form_params(req);
            let grant = params.get("grant_type").map(String::as_str).unwrap_or("");
            let expires = (Utc::now() + ChronoDuration::hours(4)).to_rfc3339();
            let refresh_expires = (Utc::now() + ChronoDuration::days(30)).to_rfc3339();

            match grant {
                "device_auth" => {
                    let acc = params.get("account_id").cloned().unwrap_or_default();
                    let seq = seq_oauth.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(200).set_body_json(json!({
                        "access_token": format!("fn-tok-{acc}-{seq}"),
                        "expires_in": 14400,
                        "expires_at": expires,
                        "token_type": "bearer",
                        "refresh_token": format!("fn-ref-{acc}-{seq}"),
                        "refresh_expires": 2592000,
                        "refresh_expires_at": refresh_expires,
                        "account_id": acc,
                        "client_id": AuthClient::FortniteAndroidGameClient.client_id(),
                        "internal_client": true,
                        "client_service": "fortnite",
                        "displayName": format!("Bot_{}", &acc[acc.len().saturating_sub(4)..]),
                        "app": "fortnite",
                        "in_app_id": acc,
                        "device_id": params.get("device_id").cloned().unwrap_or_else(|| "dev".to_owned())
                    }))
                }
                "client_credentials" => ResponseTemplate::new(200).set_body_json(json!({
                    "access_token": "fn-cc-tok",
                    "expires_in": 14400,
                    "expires_at": expires,
                    "token_type": "bearer",
                    "client_id": AuthClient::FortniteAndroidGameClient.client_id(),
                    "internal_client": true,
                    "client_service": "fortnite",
                    "product_id": "prod-fn",
                    "application_id": "app-fn"
                })),
                _ => ResponseTemplate::new(400).set_body_json(json!({
                    "errorCode": "errors.com.epicgames.common.unsupported_grant_type",
                    "errorMessage": "Unsupported grant"
                })),
            }
        })
        .mount(server)
        .await;

    Mock::given(path("/account/api/oauth/exchange"))
        .respond_with(|req: &Request| {
            let bearer = extract_bearer_raw(req).unwrap_or_default();
            let acc = account_from_fn_token(&bearer).unwrap_or_else(|| "unknown".to_owned());
            ResponseTemplate::new(200).set_body_json(json!({
                "expiresInSeconds": 300,
                "code": format!("eos-ex-{acc}"),
                "creatingClientId": AuthClient::FortniteAndroidGameClient.client_id()
            }))
        })
        .mount(server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/account/api/oauth/sessions/kill"))
        .respond_with(ResponseTemplate::new(204))
        .mount(server)
        .await;

    Mock::given(method("DELETE"))
        .and(path_regex("^/account/api/oauth/sessions/kill/.+$"))
        .respond_with(ResponseTemplate::new(204))
        .mount(server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex("^/fortnite/api/storeaccess/v1/request_access/.+$"))
        .respond_with(ResponseTemplate::new(204))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex("^/eulatracking/api/public/agreements/fn/account/.+$"))
        .respond_with(ResponseTemplate::new(204))
        .mount(server)
        .await;

    let seq_eos = Arc::clone(&token_seq);
    Mock::given(method("POST"))
        .and(path("/epic/oauth/v2/token"))
        .respond_with(move |req: &Request| {
            let params = parse_form_params(req);
            let expires = (Utc::now() + ChronoDuration::hours(4)).to_rfc3339();
            let refresh_expires = (Utc::now() + ChronoDuration::days(30)).to_rfc3339();
            let acc = params
                .get("exchange_code")
                .and_then(|c| c.strip_prefix("eos-ex-"))
                .or_else(|| {
                    params
                        .get("refresh_token")
                        .and_then(|r| r.strip_prefix("eos-ref-"))
                        .and_then(|s| s.split('-').next())
                })
                .unwrap_or("00000000000000000000000000000001")
                .to_owned();
            let seq = seq_eos.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(json!({
                "access_token": format!("eos-tok-{acc}-{seq}"),
                "token_type": "bearer",
                "expires_in": 3600,
                "expires_at": expires,
                "refresh_token": format!("eos-ref-{acc}-{seq}"),
                "refresh_expires_at": refresh_expires,
                "account_id": acc,
                "product_user_id": format!("puid{acc}"),
                "client_id": AuthClient::FortniteAndroidGameClient.client_id(),
                "deployment_id": "62a9473a2dca46b29ccf17577fcf42d7",
                "merged_accounts": [],
                "scope": "basic_profile friends_list presence openid"
            }))
        })
        .mount(server)
        .await;

    Mock::given(method("POST"))
        .and(path("/epic/oauth/v2/revoke"))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/account/api/public/account"))
        .respond_with(|req: &Request| {
            let items: Vec<Value> = req
                .url
                .query_pairs()
                .filter(|(k, _)| k == "accountId")
                .map(|(_, id)| {
                    json!({
                        "id": id,
                        "displayName": format!("User_{}", &id[id.len().saturating_sub(4)..])
                    })
                })
                .collect();
            ResponseTemplate::new(200).set_body_json(items)
        })
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex("^/account/api/public/account/[0-9a-fA-F]{32}$"))
        .respond_with(|req: &Request| {
            let id = req.url.path().rsplit('/').next().unwrap_or("");
            ResponseTemplate::new(200).set_body_json(json!({
                "id": id,
                "displayName": format!("Bot_{}", &id[id.len().saturating_sub(4)..])
            }))
        })
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex("^/friends/api/v1/[^/]+/summary$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "friends": [],
            "incoming": [],
            "outgoing": [],
            "blocklist": []
        })))
        .mount(server)
        .await;

    if mount_default_fn_party_routes {
        Mock::given(method("GET"))
            .and(path_regex("^/epic/party/internal/v2/users/[^/]+$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "current": null,
                "invites": [],
                "join_requests": []
            })))
            .mount(server)
            .await;
    }

    let party_seq_clone = Arc::clone(&party_seq);
    Mock::given(method("POST"))
        .and(path("/epic/party/internal/v2/parties"))
        .respond_with(move |req: &Request| {
            let bearer = extract_bearer_raw(req).unwrap_or_default();
            let lead = account_from_eos_token(&bearer)
                .unwrap_or_else(|| "00000000000000000000000000000001".to_owned());
            let pid_num = party_seq_clone.fetch_add(1, Ordering::SeqCst);
            let eos_id = format!("aa{pid_num:030x}");
            ResponseTemplate::new(200).set_body_json(json!({
                "id": eos_id,
                "party_lead": lead,
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "config": { "joinability": "OPEN", "max_size": 16 },
                "meta": {},
                "revision": 1,
                "chat_conversation_id": format!("ep-{eos_id}"),
                "is_reportable": true,
                "members": []
            }))
        })
        .mount(server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex("^/epic/party/v2/.+/parties/.+/members/.+/connect$"))
        .respond_with(ResponseTemplate::new(204))
        .mount(server)
        .await;

    if mount_default_fn_party_routes {
        Mock::given(method("POST"))
            .and(path_regex("^/party/api/v1/Fortnite/epic-parties/[^/]+/lobbies/[^/]+/members/[^/]+/join$"))
            .respond_with(|req: &Request| {
                let segs: Vec<&str> = req.url.path().split('/').collect();
                let lobby_id = segs[segs.len() - 4];
                let account_id = segs[segs.len() - 2];
                ResponseTemplate::new(200).set_body_json(json!({
                    "id": lobby_id,
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                    "config": {
                        "type": "DEFAULT",
                        "joinability": "OPEN",
                        "discoverability": "ALL",
                        "sub_type": "default",
                        "max_size": 16,
                        "invite_ttl": 3600,
                        "join_confirmation": false
                    },
                    "members": [{
                        "id": account_id,
                        "account_id": account_id,
                        "account_dn": format!("Bot_{}", &account_id[account_id.len().saturating_sub(4)..]),
                        "meta": {},
                        "connections": [],
                        "revision": 0,
                        "joined_at": "2026-01-01T00:00:00Z",
                        "role": "CAPTAIN"
                    }],
                    "meta": {},
                    "invites": [],
                    "revision": 0
                }))
            })
            .mount(server)
            .await;
    }

    let party_patches_arc = Arc::clone(&fn_party_patches);
    Mock::given(method("PATCH"))
        .and(path_regex("^/party/api/v1/Fortnite/parties/[^/]+$"))
        .respond_with(move |req: &Request| {
            if let Ok(body) = req.body_json::<Value>() {
                party_patches_arc
                    .lock()
                    .expect("party patches lock")
                    .push(body);
            }
            ResponseTemplate::new(204)
        })
        .mount(server)
        .await;

    Mock::given(method("PATCH"))
        .and(path_regex("^/party/api/v1/Fortnite/parties/[^/]+/members/[^/]+/meta$"))
        .respond_with(ResponseTemplate::new(204))
        .mount(server)
        .await;

    Mock::given(method("PATCH"))
        .and(path_regex("^/epic/party/internal/v2/parties/[^/]+$"))
        .respond_with(|req: &Request| {
            let eos_id = req.url.path().rsplit('/').next().unwrap_or("");
            let bearer = extract_bearer_raw(req).unwrap_or_default();
            let lead = account_from_eos_token(&bearer)
                .unwrap_or_else(|| "00000000000000000000000000000001".to_owned());
            ResponseTemplate::new(200).set_body_json(json!({
                "id": eos_id,
                "party_lead": lead,
                "config": { "joinability": "OPEN", "max_size": 16 },
                "meta": {},
                "revision": 2,
                "chat_conversation_id": format!("ep-{eos_id}"),
                "is_reportable": true,
                "members": []
            }))
        })
        .mount(server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex("^/epic/party/internal/v2/parties/[^/]+/members/[^/]+/keep-alive$"))
        .respond_with(ResponseTemplate::new(204))
        .mount(server)
        .await;

    if mount_default_fn_party_routes {
        Mock::given(method("DELETE"))
            .and(path_regex("^/party/api/v1/Fortnite/parties/[^/]+/members/[^/]+$"))
            .respond_with(ResponseTemplate::new(204))
            .mount(server)
            .await;
    }

    Mock::given(method("DELETE"))
        .and(path_regex("^/epic/party/internal/v2/parties/[^/]+/members/[^/]+$"))
        .respond_with(ResponseTemplate::new(204))
        .mount(server)
        .await;

    let eos_presence_arc = Arc::clone(&eos_presence_patches);
    Mock::given(method("PATCH"))
        .and(path_regex("^/epic/presence/v1/.+/presence/.+$"))
        .respond_with(move |req: &Request| {
            if let Ok(body) = req.body_json::<Value>() {
                eos_presence_arc
                    .lock()
                    .expect("eos presence patches lock")
                    .push(body);
            }
            ResponseTemplate::new(204)
        })
        .mount(server)
        .await;

    Mock::given(method("PATCH"))
        .and(path_regex("^/epic/presence/internal/v1/_/[^/]+/presence/.+$"))
        .respond_with(ResponseTemplate::new(204))
        .mount(server)
        .await;
}

fn spawn_mock_xmpp_server(listener: TcpListener, shutdown: CancellationToken) {
    tokio::spawn(async move {
        let conn_seq = Arc::new(AtomicUsize::new(1));
        loop {
            let (stream, _) = tokio::select! {
                _ = shutdown.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok(pair) => pair,
                    Err(_) => break,
                }
            };
            let conn_id = conn_seq.fetch_add(1, Ordering::SeqCst);
            let conn_cancel = shutdown.child_token();

            tokio::spawn(async move {
                let Ok(ws) = accept_hdr_async(stream, |req: &WsRequest, mut resp: WsResponse| {
                    if let Some(proto) = req.headers().get("sec-websocket-protocol")
                        && proto.to_str().ok() == Some("xmpp")
                    {
                        resp.headers_mut()
                            .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("xmpp"));
                    }
                    Ok(resp)
                })
                .await
                else {
                    return;
                };

                let (mut tx, mut rx) = ws.split();

                if rx.next().await.is_none() {
                    return;
                }
                let _ = tx.send(Message::text("<open xmlns='urn:ietf:params:xml:ns:xmpp-framing' id='s1' from='prod.ol.epicgames.com'/>")).await;
                let _ = tx.send(Message::text("<stream:features><mechanisms xmlns='urn:ietf:params:xml:ns:xmpp-sasl'><mechanism>PLAIN</mechanism></mechanisms></stream:features>")).await;

                let Some(Ok(Message::Text(auth_xml))) = rx.next().await else {
                    return;
                };
                let account_id = extract_xmpp_plain_account(&auth_xml)
                    .unwrap_or_else(|| "00000000000000000000000000000001".to_owned());
                let _ = tx
                    .send(Message::text("<success xmlns='urn:ietf:params:xml:ns:xmpp-sasl'/>"))
                    .await;

                if rx.next().await.is_none() {
                    return;
                }
                let _ = tx.send(Message::text("<open xmlns='urn:ietf:params:xml:ns:xmpp-framing' id='s2' from='prod.ol.epicgames.com'/>")).await;
                let _ = tx.send(Message::text("<stream:features><bind xmlns='urn:ietf:params:xml:ns:xmpp-bind'/></stream:features>")).await;

                let Some(Ok(Message::Text(_bind_iq))) = rx.next().await else {
                    return;
                };
                let jid = format!("{account_id}@prod.ol.epicgames.com/V2:Fortnite:WIN::C{conn_id}");
                let _ = tx
                    .send(Message::text(format!(
                        "<iq type='result' id='bind_1'><bind xmlns='urn:ietf:params:xml:ns:xmpp-bind'><jid>{jid}</jid></bind></iq>"
                    )))
                    .await;

                if rx.next().await.is_none() {
                    return;
                }
                let _ = tx.send(Message::text("<iq type='result' id='sess_1'/>")).await;

                loop {
                    tokio::select! {
                        _ = conn_cancel.cancelled() => break,
                        frame = rx.next() => match frame {
                            Some(Ok(Message::Ping(p))) => {
                                let _ = tx.send(Message::Pong(p)).await;
                            }
                            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                            Some(Ok(_)) => {}
                        }
                    }
                }
            });
        }
    });
}

fn extract_xmpp_plain_account(xml: &str) -> Option<String> {
    let start = xml.find('>')? + 1;
    let end = xml[start..].find('<')? + start;
    let decoded = STANDARD.decode(xml[start..end].trim()).ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let mut parts = text.split('\0');
    let _authzid = parts.next()?;
    let authcid = parts.next()?;
    if authcid.is_empty() {
        None
    } else {
        Some(authcid.to_owned())
    }
}

fn spawn_mock_stomp_server(
    listener: TcpListener,
    stomp_clients: STOMPClientMap,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let conn_seq = Arc::new(AtomicUsize::new(1));
        loop {
            let (stream, _) = tokio::select! {
                _ = shutdown.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok(pair) => pair,
                    Err(_) => break,
                }
            };
            let conn_id = conn_seq.fetch_add(1, Ordering::SeqCst);
            let registry = Arc::clone(&stomp_clients);
            let conn_cancel = shutdown.child_token();

            tokio::spawn(async move {
                let Ok(ws) = accept_hdr_async(stream, |_req: &WsRequest, resp: WsResponse| Ok(resp)).await else {
                    return;
                };
                let (mut tx, mut rx) = ws.split();

                let Some(Ok(Message::Text(connect_txt))) = rx.next().await else {
                    return;
                };
                let Ok(Some(connect_frame)) = STOMPFrame::parse(&connect_txt) else {
                    return;
                };
                if connect_frame.command != STOMPCommand::Connect {
                    return;
                }

                let raw_auth = connect_frame.header("authorization").unwrap_or("");
                let raw_tok = raw_auth
                    .strip_prefix("bearer ")
                    .or_else(|| raw_auth.strip_prefix("Bearer "))
                    .unwrap_or(raw_auth);
                let account_id = account_from_eos_token(raw_tok)
                    .unwrap_or_else(|| "00000000000000000000000000000001".to_owned());

                let connected = STOMPFrame::new(STOMPCommand::Connected)
                    .with_header("version", "1.2")
                    .with_header("session", format!("stomp-sess-{conn_id}"))
                    .with_header("heart-beat", "0,15000");
                if tx.send(Message::text(connected.encode())).await.is_err() {
                    return;
                }

                let mut sub_count = 0usize;
                let mut eas_dest = String::new();
                while sub_count < 3 {
                    match rx.next().await {
                        Some(Ok(Message::Text(txt))) => {
                            if let Ok(Some(frame)) = STOMPFrame::parse(&txt)
                                && frame.command == STOMPCommand::Subscribe
                            {
                                if eas_dest.is_empty()
                                    && let Some(dest) = frame.header("destination")
                                {
                                    eas_dest = dest.to_owned();
                                }
                                sub_count += 1;
                            }
                        }
                        _ => return,
                    }
                }

                let core_connected = STOMPFrame::new(STOMPCommand::Message).with_body(
                    json!({
                        "type": "core.connect.v1.connected",
                        "correlationId": format!("corr-{conn_id}"),
                        "timestamp": 1700000000_u64,
                        "connectionId": format!("eos-conn-{account_id}-{conn_id}")
                    })
                    .to_string(),
                );
                if tx.send(Message::text(core_connected.encode())).await.is_err() {
                    return;
                }

                let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
                registry
                    .lock()
                    .expect("registry lock")
                    .insert(account_id.clone(), (out_tx, eas_dest));

                loop {
                    tokio::select! {
                        _ = conn_cancel.cancelled() => break,
                        Some(msg) = out_rx.recv() => {
                            if tx.send(Message::text(msg)).await.is_err() {
                                break;
                            }
                        }
                        incoming = rx.next() => match incoming {
                            Some(Ok(Message::Ping(p))) => {
                                let _ = tx.send(Message::Pong(p)).await;
                            }
                            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                            Some(Ok(_)) => {}
                        }
                    }
                }

                registry.lock().expect("registry lock").remove(&account_id);
            });
        }
    });
}

#[tokio::test]
async fn login_recovers_from_orphaned_fn_party_409_user_has_party() {
    let env = MockEnv::start_without_default_fn_party_routes().await;
    let account_id = make_account_id(42);
    let stale_lobby_1 = "bbbb1111111111111111111111111111-39001234-default".to_owned();
    let stale_lobby_2 = "cccc2222222222222222222222222222-39001234-default".to_owned();

    Mock::given(method("GET"))
        .and(path_regex("^/epic/party/internal/v2/users/[^/]+$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "current": [],
            "invites": [],
            "join_requests": []
        })))
        .mount(&env.http_server)
        .await;

    let user_state_calls = Arc::new(AtomicUsize::new(0));
    let user_state_calls_clone = Arc::clone(&user_state_calls);
    let s1 = stale_lobby_1.clone();
    let s2 = stale_lobby_2.clone();
    let acc_for_state = account_id.clone();
    Mock::given(method("GET"))
        .and(path_regex("^/party/api/v1/Fortnite/user/[^/]+$"))
        .respond_with(move |_req: &Request| {
            let n = user_state_calls_clone.fetch_add(1, Ordering::SeqCst);
            let current = match n {
                0 => vec![json!({
                    "id": s1,
                    "config": { "type": "DEFAULT", "joinability": "OPEN", "discoverability": "ALL", "sub_type": "default", "max_size": 16, "invite_ttl": 3600, "join_confirmation": false },
                    "members": [{ "account_id": acc_for_state.as_str(), "meta": {}, "connections": [], "revision": 0 }],
                    "meta": {},
                    "invites": [],
                    "revision": 0
                })],
                1 => vec![json!({
                    "id": s2,
                    "config": { "type": "DEFAULT", "joinability": "OPEN", "discoverability": "ALL", "sub_type": "default", "max_size": 16, "invite_ttl": 3600, "join_confirmation": false },
                    "members": [{ "account_id": acc_for_state.as_str(), "meta": {}, "connections": [], "revision": 0 }],
                    "meta": {},
                    "invites": [],
                    "revision": 0
                })],
                _ => Vec::new(),
            };
            ResponseTemplate::new(200).set_body_json(json!({
                "current": current,
                "pending": [],
                "invites": [],
                "pings": []
            }))
        })
        .mount(&env.http_server)
        .await;

    let deleted_lobbies: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
    let deleted_lobbies_clone = Arc::clone(&deleted_lobbies);
    Mock::given(method("DELETE"))
        .and(path_regex("^/party/api/v1/Fortnite/parties/[^/]+/members/[^/]+$"))
        .respond_with(move |req: &Request| {
            let segs: Vec<&str> = req.url.path().split('/').collect();
            let lobby_id = segs[segs.len() - 3].to_owned();
            deleted_lobbies_clone
                .lock()
                .expect("deleted lobbies lock")
                .push(lobby_id);
            ResponseTemplate::new(204)
        })
        .mount(&env.http_server)
        .await;

    let join_attempts = Arc::new(AtomicUsize::new(0));
    let join_attempts_clone = Arc::clone(&join_attempts);
    Mock::given(method("POST"))
        .and(path_regex("^/party/api/v1/Fortnite/epic-parties/[^/]+/lobbies/[^/]+/members/[^/]+/join$"))
        .respond_with(move |req: &Request| {
            let n = join_attempts_clone.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                return ResponseTemplate::new(409).set_body_json(json!({
                    "errorCode": "errors.com.epicgames.social.party.user_has_party",
                    "errorMessage": "User is already in a party with deployment id [Fortnite].",
                    "numericErrorCode": 51012
                }));
            }
            let segs: Vec<&str> = req.url.path().split('/').collect();
            let lobby_id = segs[segs.len() - 4];
            let acc_id = segs[segs.len() - 2];
            ResponseTemplate::new(200).set_body_json(json!({
                "id": lobby_id,
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "config": {
                    "type": "DEFAULT",
                    "joinability": "OPEN",
                    "discoverability": "ALL",
                    "sub_type": "default",
                    "max_size": 16,
                    "invite_ttl": 3600,
                    "join_confirmation": false
                },
                "members": [{
                    "id": acc_id,
                    "account_id": acc_id,
                    "account_dn": "RecoveredBot",
                    "meta": {},
                    "connections": [],
                    "revision": 0,
                    "joined_at": "2026-01-01T00:00:00Z",
                    "role": "CAPTAIN"
                }],
                "meta": {},
                "invites": [],
                "revision": 0
            }))
        })
        .mount(&env.http_server)
        .await;

    let client = Client::new(env.client_config()).expect("client init");
    client
        .login(AuthMethod::DeviceAuth(DeviceAuthCredentials::new(
            account_id.clone(),
            "test-device",
            SecretString::from("test-secret".to_owned()),
        )))
        .await
        .expect("login recovers from 409 user_has_party");

    assert!(client.is_ready());
    assert!(client.party().await.is_some());
    assert_eq!(join_attempts.load(Ordering::SeqCst), 2);
    {
        let recorded = deleted_lobbies.lock().expect("deleted lobbies lock");
        assert!(recorded.contains(&stale_lobby_1));
        assert!(recorded.contains(&stale_lobby_2));
    }

    client.logout().await.expect("logout");
}

#[tokio::test]
async fn create_party_applies_party_config_and_enforces_chat_enabled() {
    let env = MockEnv::start().await;
    let account_id = make_account_id(7);
    let mut cfg = env.client_config();
    cfg.party.discoverability = PartyDiscoverability::InvitedOnly;
    cfg.party.joinability = PartyJoinability::InviteAndFormer;
    cfg.party.max_size = 8;
    cfg.party.invite_ttl_secs = 900;
    cfg.party.intention_ttl_secs = 45;
    cfg.party.join_confirmation = true;
    cfg.party.chat_enabled = false;

    let client = Client::new(cfg).expect("client init");
    client
        .login(AuthMethod::DeviceAuth(DeviceAuthCredentials::new(
            account_id,
            "test-device",
            SecretString::from("test-secret".to_owned()),
        )))
        .await
        .expect("login succeeds");

    let party = client.party().await.expect("active party");
    assert!(!party.chat().is_enabled());

    let snap = party.snapshot().await.expect("party snapshot");
    assert_eq!(snap.config.discoverability, PartyDiscoverability::InvitedOnly);
    assert_eq!(snap.config.joinability, PartyJoinability::InviteAndFormer);
    assert_eq!(snap.config.max_size, 8);
    assert_eq!(snap.config.invite_ttl, 900);
    assert_eq!(snap.config.intention_ttl, 45);
    assert!(snap.config.join_confirmation);
    assert_eq!(
        snap.meta.as_meta().get_str("urn:epic:cfg:party-type-id_s"),
        Some("default")
    );
    assert_eq!(
        snap.meta.as_meta().get_str("urn:epic:cfg:chat-enabled_b"),
        Some("false")
    );
    assert_eq!(
        snap.meta.as_meta().get_str("urn:epic:cfg:can-join_b"),
        Some("true")
    );
    assert_eq!(
        snap.meta
            .as_meta()
            .get_str("urn:epic:cfg:join-request-action_s"),
        Some("AutoApprove")
    );
    assert!(
        snap.meta
            .as_meta()
            .get_str("urn:epic:cfg:build-id_s")
            .is_some()
    );
    let me_member = snap
        .members
        .values()
        .next()
        .expect("self member present in snapshot");
    let packed = me_member
        .meta
        .as_meta()
        .get_json::<Value>("Default:PackedState_j")
        .expect("valid PackedState_j")
        .expect("PackedState_j present");
    assert_ne!(
        packed["PackedState"]["eOSProductUserId"],
        json!(""),
        "self member eOSProductUserId should be populated"
    );

    {
        let patches = env.fn_party_patches.lock().expect("party patches lock");
        let patch = patches.last().expect("initial full_schema_patch sent");
        assert_eq!(patch["invite_ttl_seconds"], json!(900));
        assert_eq!(patch["max_number_of_members"], json!(8));
        assert_eq!(patch["config"]["join_confirmation"], json!(true));
        assert_eq!(
            patch["meta"]["update"]["urn:epic:cfg:party-type-id_s"],
            json!("default")
        );
        assert_eq!(
            patch["meta"]["update"]["urn:epic:cfg:chat-enabled_b"],
            json!("false")
        );
        assert_eq!(
            patch["meta"]["update"]["urn:epic:cfg:can-join_b"],
            json!("true")
        );
        assert_eq!(
            patch["meta"]["update"]["urn:epic:cfg:join-request-action_s"],
            json!("AutoApprove")
        );
        assert!(
            patch["meta"]["update"]["urn:epic:cfg:build-id_s"]
                .as_str()
                .is_some()
        );
    }

    let chat_err = party
        .send_message("hello")
        .await
        .expect_err("party chat disabled");
    assert!(matches!(chat_err, RustfnError::PartyState(_)));

    client.logout().await.expect("logout");
}

#[tokio::test]
async fn accept_and_decline_invite_methods_work_against_party_endpoints() {
    let env = MockEnv::start().await;
    let bot_id = make_account_id(10);
    let inviter_id = make_account_id(99);
    let target_eos_id = PartyId::parse("1234567890abcdef1234567890abcdef").expect("eos id");

    let declined_hits = Arc::new(AtomicUsize::new(0));
    let declined_clone = Arc::clone(&declined_hits);
    Mock::given(method("DELETE"))
        .and(path_regex("^/epic/party/internal/v2/users/[^/]+/invites/[^/]+$"))
        .respond_with(move |_req: &Request| {
            declined_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(204)
        })
        .mount(&env.http_server)
        .await;

    let eos_join_hits = Arc::new(AtomicUsize::new(0));
    let eos_join_clone = Arc::clone(&eos_join_hits);
    Mock::given(method("POST"))
        .and(path_regex("^/epic/party/internal/v2/parties/[^/]+/members/[^/]+/join$"))
        .respond_with(move |_req: &Request| {
            eos_join_clone.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(204)
        })
        .mount(&env.http_server)
        .await;

    let target_eos_str = target_eos_id.as_str().to_owned();
    let inviter_str = inviter_id.as_str().to_owned();
    Mock::given(method("GET"))
        .and(path_regex("^/epic/party/internal/v2/parties/[^/]+$"))
        .respond_with(move |_req: &Request| {
            ResponseTemplate::new(200).set_body_json(json!({
                "id": target_eos_str,
                "party_lead": inviter_str,
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "config": { "joinability": "OPEN", "max_size": 16 },
                "meta": {},
                "revision": 1,
                "chat_conversation_id": format!("ep-{target_eos_str}"),
                "is_reportable": true,
                "members": [{
                    "account_id": inviter_str,
                    "meta": {},
                    "connections": [],
                    "revision": 0,
                    "role": "CAPTAIN"
                }]
            }))
        })
        .mount(&env.http_server)
        .await;

    let client = Client::new(env.client_config()).expect("client init");
    client
        .login(AuthMethod::DeviceAuth(DeviceAuthCredentials::new(
            bot_id.clone(),
            "test-device",
            SecretString::from("test-secret".to_owned()),
        )))
        .await
        .expect("login succeeds");

    let invite = ReceivedPartyInvitation::from_stomp(
        target_eos_id.clone(),
        inviter_id,
        Some("InviterUser".to_owned()),
        bot_id.clone(),
        None,
        None,
        BTreeMap::new(),
        client.http().clone(),
    );

    client.decline_invite(&invite).await.expect("decline invite");
    assert_eq!(declined_hits.load(Ordering::SeqCst), 1);

    let joined_party = client.accept_invite(&invite).await.expect("accept invite");
    assert_eq!(joined_party.eos_id(), &target_eos_id);
    assert_eq!(eos_join_hits.load(Ordering::SeqCst), 1);

    let lobby_id = format!("{}-39009999-default", target_eos_id.as_str());
    let rejoined_party = client.join_party(&lobby_id).await.expect("join by lobby ID");
    assert_eq!(rejoined_party.fn_id().as_str(), lobby_id);

    Mock::given(method("GET"))
        .and(path(format!("/epic/party/internal/v2/users/{bot_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "current": {
                "id": target_eos_id.as_str(),
                "party_lead": bot_id.as_str(),
                "config": { "joinability": "OPEN", "max_size": 16 }
            }
        })))
        .with_priority(1)
        .mount(&env.http_server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/party/api/v1/Fortnite/parties/{lobby_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": lobby_id,
            "config": {},
            "members": [{ "account_id": bot_id.as_str(), "role": "CAPTAIN" }]
        })))
        .mount(&env.http_server)
        .await;
    client.init_party(false, false).await.expect("rebind joined lobby");
    assert_eq!(client.party().await.expect("joined party remains").fn_id().as_str(), lobby_id);

    client.logout().await.expect("logout");
}

#[tokio::test]
async fn set_status_publishes_fortnite_lobby_id_in_party_join_info() {
    let env = MockEnv::start().await;
    let account_id = make_account_id(15);
    let mut cfg = env.client_config();
    cfg.party.privacy = rustfn_protocol::PartyPrivacyPreset::Friends.to_privacy();

    let client = Client::new(cfg).expect("client init");
    client
        .login(AuthMethod::DeviceAuth(DeviceAuthCredentials::new(
            account_id,
            "test-device",
            SecretString::from("test-secret".to_owned()),
        )))
        .await
        .expect("login succeeds");

    let party = client.party().await.expect("active party");
    client.set_status(None, None).await.expect("set_status succeeds");

    let patches = env
        .eos_presence_patches
        .lock()
        .expect("eos presence patches lock");
    let last_patch = patches.last().expect("eos presence patch recorded");
    let raw_join = last_patch["props"]["party.joininfodata.286331153"]
        .as_str()
        .expect("joininfodata string");
    let json_body = raw_join.strip_prefix('m').expect("prefixed m-json");
    let parsed: Value = serde_json::from_str(json_body).expect("valid joininfodata json");
    assert_eq!(parsed["p"], json!(party.fn_id().as_str()));
    assert_ne!(parsed["p"], json!(party.eos_id().as_str()));
    assert_eq!(parsed["d"], json!("Fortnite"));
    assert_eq!(parsed["b"], json!("1:3:39001234"));
    assert_eq!(parsed["nAR"], json!(0));
    let fort_basic_info = last_patch["props"]["FortBasicInfo"]
        .as_str()
        .expect("FortBasicInfo string");
    assert!(fort_basic_info.contains("\"homeBaseRating\":0"));
    assert_eq!(last_patch["props"]["FortSubGame"], json!("i1"));

    let xmpp_join_info = rustfn_friends::PartyJoinInfoData {
        source_id: Some(make_account_id(15)),
        source_display_name: Some("Bot".to_owned()),
        source_platform: Some("WIN".to_owned()),
        party_id: Some(party.fn_id().as_str().to_owned()),
        party_type_id: Some(286331153),
        key: Some("k".to_owned()),
        app_id: Some("Fortnite".to_owned()),
        build_id: Some("1:3:39001234".to_owned()),
        party_flags: Some(286331153),
        not_accepting_members_reason: Some(0),
        pc: Some(1),
    };
    let xmpp_join_val = serde_json::to_value(&xmpp_join_info).expect("serialize PartyJoinInfoData");
    assert!(xmpp_join_val.get("notAcceptingReason").is_some());
    assert!(xmpp_join_val.get("notAcceptingMembersReason").is_none());

    drop(patches);
    client.logout().await.expect("logout");
}

#[tokio::test]
async fn init_party_keeps_party_after_guest_leaves_and_recovers_after_self_leaves() {
    let env = MockEnv::start().await;
    let bot_id = make_account_id(21);
    let guest_id = make_account_id(88);

    let client = Client::new(env.client_config()).expect("client init");
    client
        .login(AuthMethod::DeviceAuth(DeviceAuthCredentials::new(
            bot_id.clone(),
            "test-device",
            SecretString::from("test-secret".to_owned()),
        )))
        .await
        .expect("login succeeds");

    let first_party = client.party().await.expect("initial party");
    let first_eos_id = first_party.eos_id().clone();

    let (stomp_tx, stomp_dest) = env
        .stomp_clients
        .lock()
        .expect("stomp registry lock")
        .get(bot_id.as_str())
        .cloned()
        .expect("registered mock stomp connection");

    let send_stomp_party_event = |event_type: &str, msg_id: &str, member_id: &AccountId| {
        let frame = STOMPFrame::new(STOMPCommand::Message)
            .with_header("destination", stomp_dest.clone())
            .with_header("message-id", msg_id)
            .with_body(
                json!({
                    "type": event_type,
                    "id": msg_id,
                    "payload": {
                        "party_id": first_eos_id.as_str(),
                        "account_id": member_id.as_str(),
                        "account_dn": "GuestUser",
                        "revision": 1,
                        "joined_at": "2026-01-01T00:00:00Z",
                        "updated_at": "2026-01-01T00:00:00Z"
                    }
                })
                .to_string(),
            );
        stomp_tx.send(frame.encode()).expect("push stomp frame");
    };

    send_stomp_party_event("party.v2.MEMBER_JOINED", "msg-join-1", &guest_id);
    let join_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(snap) = first_party.snapshot().await
            && snap.member(&guest_id).is_some()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < join_deadline,
            "timed out waiting for guest join"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    send_stomp_party_event("party.v2.MEMBER_LEFT", "msg-left-1", &guest_id);
    let leave_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(snap) = first_party.snapshot().await
            && snap.member(&guest_id).is_none()
        {
            break;
        }
        assert!(tokio::time::Instant::now() < leave_deadline, "guest did not leave");
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        client.party().await.expect("host party remains").eos_id(),
        &first_eos_id,
        "guest departure must not replace the host party"
    );

    send_stomp_party_event("party.v2.MEMBER_LEFT", "msg-left-self", &bot_id);
    let recover_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let second_party = loop {
        if let Some(current) = client.party().await
            && current.eos_id() != &first_eos_id
        {
            break current;
        }
        assert!(
            tokio::time::Instant::now() < recover_deadline,
            "timed out waiting for init_party to recreate missing server party"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert_ne!(second_party.eos_id(), &first_eos_id);
    {
        let patches = env
            .eos_presence_patches
            .lock()
            .expect("eos presence patches lock");
        let last_patch = patches.last().expect("recreated party published presence");
        let raw_join = last_patch["props"]["party.joininfodata.286331153"]
            .as_str()
            .expect("joininfodata string");
        let parsed: Value =
            serde_json::from_str(raw_join.strip_prefix('m').expect("prefixed json"))
                .expect("valid joininfodata");
        assert_eq!(parsed["p"], json!(second_party.fn_id().as_str()));
    }

    client.leave_party(false).await.expect("explicit leave");
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        client.party().await.is_none(),
        "explicit leave_party(false) must not auto-recreate a party"
    );

    client.logout().await.expect("logout");
}
