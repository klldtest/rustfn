use rustfn::chat::{ChatManager, decode_stomp_message_body, verify_signed_message};
use rustfn::friends::{
    FriendManager, FriendPresence, FriendTransition, OutboundPresenceSpec, PartyJoinInfoData,
    build_eos_presence_props,
};
use rustfn::http::{
    AuthSessionSlot, BoxFuture, FortnitePartyConfig, FortnitePartyData, FortnitePartyMemberData,
    FortniteBuildResolver, HTTP, HttpClient, HttpClientConfig, TokenProvider,
};
use rustfn::party::{
    ClientParty, ClientPartyInit, CosmeticItem, CosmeticsUpdate, PartyEvent, PartyEventBus,
    PartyMemberSnapshot,
};
use rustfn::protocol::{
    AccountId, CacheConfig, ExtraServiceEndpoints, LobbyId, MetaMap, MetaPatch, PartyConfig,
    PartyId, PartyJoinability, PartyPrivacyPreset, Platform, PresenceOnlineType, PresencePropValue,
    Region, RustfnError, ServiceEndpoints, to_fortnite_lobby_id,
};
use rustfn::stomp::{
    EOSPartyData, EOSPartyDataConfig, EOSPartyMemberUpdateData, EOSPresenceActivity,
    EOSPresencePerNs, EOSPresenceUpdatePayload,
};
use rustfn::users::UserManager;
use rustfn::xmpp::{
    XMPPFriendNotification, XMPPFriendshipRemoveNotification, XMPPMetadataStore,
    XMPPPartyMemberDeltaNotification, XMPPUserBlocklistUpdate,
};
use secrecy::SecretString;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

struct StaticTokenProvider;

impl TokenProvider for StaticTokenProvider {
    fn bearer_token(&self, slot: AuthSessionSlot) -> BoxFuture<'_, Result<SecretString, RustfnError>> {
        Box::pin(async move {
            let tok = match slot {
                AuthSessionSlot::Fortnite => "fn-token",
                AuthSessionSlot::Launcher => "launcher-token",
                AuthSessionSlot::FortniteClientCredentials => "fn-cc-token",
                AuthSessionSlot::FortniteEos => "eos-token",
            };
            Ok(SecretString::new(tok.into()))
        })
    }

    fn refresh_on_invalid_token<'a>(
        &'a self,
        slot: AuthSessionSlot,
        _failed_token: &'a str,
    ) -> BoxFuture<'a, Result<SecretString, RustfnError>> {
        self.bearer_token(slot)
    }
}

fn make_account_id(index: usize) -> AccountId {
    AccountId::parse(format!("{index:032x}")).expect("hex account id")
}

fn make_party_id(index: usize) -> PartyId {
    PartyId::parse(format!("{index:032x}")).expect("hex party id")
}

fn make_http_with_pk(mock_uri: &str, pk_path: &str, pk_fallback_path: &str) -> HTTP {
    let mut http_cfg = HttpClientConfig::default();
    http_cfg.retry.rest_retry_limit = 1;
    http_cfg.retry.backoff_start = Duration::from_millis(5);
    let client = HttpClient::new(http_cfg)
        .expect("http client")
        .with_token_provider(Arc::new(StaticTokenProvider));

    let mut endpoints = ServiceEndpoints::default();
    endpoints.launcher_public = format!("{mock_uri}/launcher/api").into();
    endpoints.account_public = format!("{mock_uri}/account/api/public/account").into();
    endpoints.friends = format!("{mock_uri}/friends/api/v1").into();
    endpoints.friends_public = format!("{mock_uri}/friends/api/public/friends").into();
    endpoints.friends_blocklist_public = format!("{mock_uri}/friends/api/public/blocklist").into();
    endpoints.br_party = format!("{mock_uri}/party/api/v1/Fortnite").into();
    endpoints.eos_party = format!("{mock_uri}/epic/party").into();
    endpoints.eos_party_internal = format!("{mock_uri}/epic/party/internal/v2").into();
    endpoints.publickey = format!("{mock_uri}{pk_path}").into();
    endpoints.publickey_fallback = format!("{mock_uri}{pk_fallback_path}").into();
    endpoints.eos_chat = format!("{mock_uri}/epic/chat").into();

    let extra = ExtraServiceEndpoints {
        br_matchmaking_request: format!("{mock_uri}/matchmaking/matchMakingRequest"),
        ..ExtraServiceEndpoints::default()
    };

    HTTP::new(client, endpoints, extra)
}

#[tokio::test]
async fn build_resolver_falls_back_v2_to_v1_and_fetches_party_build_id() {
    let server = MockServer::start().await;
    let fhttp = make_http_with_pk(&server.uri(), "/primary-pk", "/fallback-pk");

    Mock::given(method("GET"))
        .and(path(
            "/launcher/api/public/assets/v2/platform/Windows/namespace/fn/catalogItem/4fe75bbc5a674f4f9b356b5c90567da5/app/Fortnite/label/Live",
        ))
        .respond_with(ResponseTemplate::new(500).set_body_string("down"))
        .expect(1..)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(
            "/launcher/api/public/assets/Windows/4fe75bbc5a674f4f9b356b5c90567da5/Fortnite",
        ))
        .and(query_param("label", "Live"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({
                "buildVersion": "++Fortnite+Release-33.10-CL-39001234-Windows"
            })),
        )
        .expect(1)
        .mount(&server)
        .await;

    let info = fhttp
        .fetch_build_version(Platform::Win)
        .await
        .expect("v1 fallback resolves build");
    assert_eq!(info.source, rustfn::http::BuildVersionSource::LauncherAssetsV1);
    assert_eq!(info.net_cl, "39001234");
    assert_eq!(info.party_build_id, "1:3:39001234");

    Mock::given(method("POST"))
        .and(path("/matchmaking/matchMakingRequest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "buildUniqueId": 39009999 }
        ])))
        .expect(1)
        .mount(&server)
        .await;

    let party_build_id = FortniteBuildResolver::fetch_party_build_id(
        fhttp.client(),
        &fhttp.extra_endpoints().br_matchmaking_request,
    )
    .await
    .expect("party build id resolves");
    assert_eq!(party_build_id, "1:3:39009999");
}

#[tokio::test]
async fn user_manager_chunks_batches_at_100_and_caches_results() {
    let server = MockServer::start().await;
    let fhttp = make_http_with_pk(&server.uri(), "/primary-pk", "/fallback-pk");
    let users = UserManager::new(
        fhttp,
        CacheConfig {
            user_ttl: Duration::from_secs(600),
            max_users: 512,
            presence_max_history: 5,
        },
    );

    let ids: Vec<AccountId> = (1..=140).map(make_account_id).collect();

    Mock::given(method("GET"))
        .and(path("/account/api/public/account"))
        .respond_with(|req: &wiremock::Request| {
            let chunk: Vec<Value> = req
                .url
                .query_pairs()
                .filter(|(k, _)| k == "accountId")
                .map(|(_, v)| json!({ "id": v, "displayName": format!("Bot_{v}") }))
                .collect();
            ResponseTemplate::new(200).set_body_json(chunk)
        })
        .expect(2)
        .mount(&server)
        .await;

    let fetched = users
        .fetch_multiple(&ids)
        .await
        .expect("batch fetch succeeds");
    assert_eq!(fetched.len(), 140);
    assert_eq!(
        fetched[0].display_name.as_deref(),
        Some(format!("Bot_{}", ids[0].as_str()).as_str())
    );

    let cached_second = users
        .fetch_multiple(&ids[..50])
        .await
        .expect("cache hit avoids extra HTTP call");
    assert_eq!(cached_second.len(), 50);

    let direct_id = users
        .resolve_id(ids[5].as_str())
        .await
        .expect("32-hex IDs resolve without HTTP");
    assert_eq!(direct_id, ids[5]);
}

#[tokio::test]
async fn friend_manager_syncs_summary_and_reconciles_xmpp_and_eos_presence() {
    let server = MockServer::start().await;
    let fhttp = make_http_with_pk(&server.uri(), "/primary-pk", "/fallback-pk");
    let users = UserManager::new(fhttp.clone(), CacheConfig::default());
    let self_id = make_account_id(1);
    let friend_id = make_account_id(2);
    let incoming_id = make_account_id(3);
    let blocked_id = make_account_id(4);

    let friends = FriendManager::new(
        self_id.clone(),
        fhttp,
        Arc::clone(&users),
        CacheConfig {
            user_ttl: Duration::from_secs(60),
            max_users: 128,
            presence_max_history: 2,
        },
    );

    Mock::given(method("GET"))
        .and(path(format!("/friends/api/v1/{}/summary", self_id.as_str())))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "friends": [
                { "accountId": friend_id.as_str(), "alias": "Duo", "favorite": true, "mutual": 3 }
            ],
            "incoming": [
                { "accountId": incoming_id.as_str() }
            ],
            "outgoing": [],
            "blocklist": [
                { "accountId": blocked_id.as_str() }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/account/api/public/account"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "id": friend_id.as_str(), "displayName": "FriendAlpha" },
            { "id": incoming_id.as_str(), "displayName": "IncomingBeta" },
            { "id": blocked_id.as_str(), "displayName": "BlockedGamma" }
        ])))
        .expect(1)
        .mount(&server)
        .await;

    let snap = friends.sync_summary().await.expect("summary sync succeeds");
    assert_eq!(snap.friends.len(), 1);
    assert_eq!(snap.friends[0].display_name.as_deref(), Some("FriendAlpha"));
    assert_eq!(snap.incoming.len(), 1);
    assert_eq!(snap.blocked.len(), 1);

    let accept_transition = friends
        .on_friend_notification(&XMPPFriendNotification {
            account_id: incoming_id.clone(),
            status: "ACCEPTED".to_owned(),
            direction: None,
            favorite: false,
            created: None,
        })
        .await;
    assert!(matches!(accept_transition, Some(FriendTransition::Added(_))));
    assert!(friends.get(&incoming_id).is_some());
    assert!(friends.incoming_requests().is_empty());

    let cancel_transition = friends.on_friendship_remove(&XMPPFriendshipRemoveNotification {
        from: incoming_id.clone(),
        to: self_id.clone(),
        reason: "DELETED".to_owned(),
        timestamp: None,
    });
    assert!(matches!(
        cancel_transition,
        Some(FriendTransition::Removed(_))
    ));
    assert!(friends.get(&incoming_id).is_none());

    let block_transition = friends
        .on_blocklist_update(&XMPPUserBlocklistUpdate {
            account_id: friend_id.clone(),
            status: "BLOCKED".to_owned(),
        })
        .await;
    assert!(matches!(block_transition, Some(FriendTransition::Blocked(_))));
    assert!(friends.get(&friend_id).is_none());

    let mut props = BTreeMap::new();
    props.insert("EOS_Platform".to_owned(), json!("sWIN"));
    props.insert("FortPartySize_i".to_owned(), json!("i2"));
    props.insert("IsInZone_b".to_owned(), json!("bfalse"));
    let eos_presence = FriendPresence::from_eos_update(&EOSPresenceUpdatePayload {
        account_id: friend_id.clone(),
        status: PresenceOnlineType::Online,
        per_ns: vec![EOSPresencePerNs {
            ns: "fn".to_owned(),
            product_id: Some("Fortnite".to_owned()),
            status: PresenceOnlineType::Online,
            activity: EOSPresenceActivity {
                value: "Lobby".to_owned(),
            },
            props,
        }],
    });
    assert_eq!(eos_presence.platform, Some(Platform::Win));
    assert_eq!(eos_presence.party_size, Some(2));
    assert_eq!(eos_presence.is_in_zone, Some(false));

    friends.record_presence(eos_presence.clone());
    friends.record_presence(eos_presence.clone());
    friends.record_presence(eos_presence);
    assert_eq!(friends.presence_history(&friend_id).len(), 2);

    let outbound_props = build_eos_presence_props(&OutboundPresenceSpec {
        status: "Lobby".to_owned(),
        online_type: PresenceOnlineType::Online,
        platform: Platform::Win,
        product_version: "31.00".to_owned(),
        sub_game: "Athena".to_owned(),
        party_size: 1,
        is_joinable: true,
        is_in_zone: false,
        in_unjoinable_match: false,
        island_code: String::new(),
        party_join_info: Some(PartyJoinInfoData {
            source_id: Some(self_id.clone()),
            source_display_name: Some("SelfBot".to_owned()),
            source_platform: Some("WIN".to_owned()),
            party_id: Some("1234567890abcdef1234567890abcdef-39001234-default".to_owned()),
            party_type_id: Some(286331153),
            key: Some("k".to_owned()),
            app_id: Some("Fortnite".to_owned()),
            build_id: Some("1:3:39001234".to_owned()),
            party_flags: Some(0),
            not_accepting_members_reason: Some(0),
            pc: Some(1),
        }),
    });
    assert_eq!(
        outbound_props.get("EOS_ProductName"),
        Some(&PresencePropValue::RawStr("Fortnite".to_owned()))
    );
    assert_eq!(
        outbound_props.get("FortPartySize"),
        Some(&PresencePropValue::Int(1))
    );
    assert_eq!(
        outbound_props.get("IslandCode"),
        Some(&PresencePropValue::Str("experience_br".to_owned()))
    );
    let join_prop = outbound_props
        .get("party.joininfodata.286331153")
        .and_then(|v| v.as_json())
        .expect("joininfodata present");
    assert_eq!(
        join_prop["p"],
        json!("1234567890abcdef1234567890abcdef-39001234-default")
    );
    let _unused: MetaMap = MetaMap::new();
}

#[tokio::test]
async fn chat_manager_falls_back_on_publickey_and_retries_reportable_mismatch() {
    let server = MockServer::start().await;
    let fhttp = make_http_with_pk(&server.uri(), "/primary-pk", "/fallback-pk");
    let users = UserManager::new(fhttp.clone(), CacheConfig::default());
    let self_id = make_account_id(10);
    let peer_id = make_account_id(11);
    let friends = FriendManager::new(self_id.clone(), fhttp.clone(), users, CacheConfig::default());
    let chat = ChatManager::new(
        self_id.clone(),
        Platform::Win,
        "test-dep".to_owned(),
        "prod.ol.epicgames.com".to_owned(),
        true,
        fhttp,
        friends,
    );

    Mock::given(method("POST"))
        .and(path("/primary-pk/v2/publickey"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1..)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/fallback-pk/v2/publickey"))
        .respond_with(|req: &wiremock::Request| {
            let body: Value = req.body_json().expect("json publickey req");
            let key = body["key"].as_str().unwrap().to_owned();
            ResponseTemplate::new(200).set_body_json(json!({
                "key": key,
                "algorithm": "ed25519",
                "jwt": "pub-jwt-value",
                "keyGuid": "guid-1"
            }))
        })
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/epic/chat/v1/public/_/conversations"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "conversationId": "dm-conv-1",
            "type": "dm",
            "isReportable": false,
            "members": [self_id.as_str(), peer_id.as_str()]
        })))
        .expect(2)
        .mount(&server)
        .await;

    let attempt_counter = Arc::new(AtomicUsize::new(0));
    let attempt_clone = Arc::clone(&attempt_counter);
    let verified_key = Arc::new(tokio::sync::Mutex::new(None::<String>));
    let verified_key_clone = Arc::clone(&verified_key);

    Mock::given(method("POST"))
        .and(path("/epic/chat/v1/public/_/conversations/dm-conv-1/messages"))
        .respond_with(move |req: &wiremock::Request| {
            let n = attempt_clone.fetch_add(1, Ordering::SeqCst);
            let payload: Value = req.body_json().expect("signed chat payload");
            if n == 0 {
                assert_eq!(payload["isReportable"], false);
                ResponseTemplate::new(400).set_body_json(json!({
                    "errorCode": "errors.com.epicgames.social.chat.is_reportable_mismatch",
                    "errorMessage": "Expected isReportable: true"
                }))
            } else {
                assert_eq!(payload["isReportable"], true);
                let body_b64 = payload["message"]["body"].as_str().unwrap().to_owned();
                let sig_b64 = payload["metadata"]["Sig"].as_str().unwrap().to_owned();
                let lock = verified_key_clone.try_lock();
                if let Ok(mut guard) = lock {
                    *guard = Some(format!("{body_b64}|{sig_b64}"));
                }
                ResponseTemplate::new(200).set_body_json(json!({
                    "messageId": "msg-delivered-1",
                    "timestamp": 1700000000u64
                }))
            }
        })
        .expect(2)
        .mount(&server)
        .await;

    let sent = chat
        .send_friend_message(peer_id.as_str(), "hello via eos")
        .await
        .expect("reportable mismatch retries cleanly");
    assert_eq!(sent.message_id, "msg-delivered-1");
    assert_eq!(sent.conversation_id, "dm-conv-1");

    let captured = verified_key.lock().await.take().expect("captured signed msg");
    let (body_b64, sig_b64) = captured.split_once('|').unwrap();
    assert!(!sig_b64.is_empty());
    assert_eq!(decode_stomp_message_body(body_b64), "hello via eos");
    let _unused_fn: fn(&str, &str, &str) -> Result<(), RustfnError> = verify_signed_message;
}

struct TestPartyHarness {
    self_id: AccountId,
    eos_id: PartyId,
    fn_id: LobbyId,
    party: ClientParty,
    events: Arc<PartyEventBus>,
    metadata_store: Arc<XMPPMetadataStore>,
    cancel: CancellationToken,
}

async fn spawn_test_party(
    mock_uri: &str,
    metadata_ttl: Duration,
    initial_rev: u64,
    initial_member_rev: u64,
    extra_members: Vec<AccountId>,
) -> TestPartyHarness {
    spawn_test_party_with_keepalive(
        mock_uri,
        metadata_ttl,
        Duration::from_secs(60),
        initial_rev,
        initial_member_rev,
        extra_members,
    )
    .await
}

async fn spawn_test_party_with_keepalive(
    mock_uri: &str,
    metadata_ttl: Duration,
    keepalive_interval: Duration,
    initial_rev: u64,
    initial_member_rev: u64,
    extra_members: Vec<AccountId>,
) -> TestPartyHarness {
    let self_id = make_account_id(1);
    let eos_id = make_party_id(0xaa);
    let fn_id = to_fortnite_lobby_id(&eos_id, "123456").expect("lobby id");
    let http = make_http_with_pk(mock_uri, "/publickey", "/publickey-fallback");
    let cache = CacheConfig::default();
    let users = UserManager::new(http.clone(), cache.clone());
    let friends = FriendManager::new(self_id.clone(), http.clone(), Arc::clone(&users), cache);
    let chat = ChatManager::new(
        self_id.clone(),
        Platform::Win,
        "62a9473a2dca46b29ccf17577fcf42d7".to_owned(),
        "prod.ol.epicgames.com".to_owned(),
        true,
        http.clone(),
        Arc::clone(&friends),
    );
    let metadata_store = Arc::new(XMPPMetadataStore::new(metadata_ttl));
    let events = Arc::new(PartyEventBus::new(128));
    let cancel = CancellationToken::new();

    let mut members = vec![FortnitePartyMemberData {
        id: Some(self_id.as_str().to_owned()),
        account_id: self_id.clone(),
        account_dn: Some("BotLeader".to_owned()),
        meta: MetaMap::new(),
        connections: Vec::new(),
        revision: initial_member_rev,
        updated_at: None,
        joined_at: Some("2026-01-01T00:00:00Z".to_owned()),
        role: Some("CAPTAIN".to_owned()),
    }];
    for member_id in extra_members {
        members.push(FortnitePartyMemberData {
            id: Some(member_id.as_str().to_owned()),
            account_id: member_id.clone(),
            account_dn: Some(format!("Member_{}", member_id.as_str())),
            meta: MetaMap::new(),
            connections: Vec::new(),
            revision: 0,
            updated_at: None,
            joined_at: Some("2026-01-01T00:00:00Z".to_owned()),
            role: Some("MEMBER".to_owned()),
        });
    }

    let fn_data = FortnitePartyData {
        id: fn_id.clone(),
        created_at: Some("2026-01-01T00:00:00Z".to_owned()),
        updated_at: None,
        config: FortnitePartyConfig::default(),
        members,
        meta: MetaMap::new(),
        invites: Vec::new(),
        revision: initial_rev,
    };

    let eos_data = EOSPartyData {
        id: eos_id.clone(),
        party_lead: self_id.clone(),
        created_at: Some("2026-01-01T00:00:00Z".to_owned()),
        updated_at: None,
        config: EOSPartyDataConfig {
            joinability: PartyJoinability::Open,
            max_size: 16,
        },
        meta: MetaMap::new(),
        revision: 1,
        chat_conversation_id: format!("ep-{}", eos_id.as_str()),
        is_reportable: true,
        members: Vec::new(),
    };

    let party_config = PartyConfig {
        eos_keep_alive_interval: keepalive_interval,
        ..Default::default()
    };

    let party = ClientParty::spawn(ClientPartyInit {
        self_id: self_id.clone(),
        self_puid: None,
        self_display_name: "BotLeader".to_owned(),
        platform: Platform::Win,
        region: Region::Eu,
        party_config,
        save_member_meta: true,
        fn_data,
        eos_data,
        http,
        metadata_store: Arc::clone(&metadata_store),
        users,
        friends,
        chat,
        events: Arc::clone(&events) as Arc<dyn rustfn::party::PartyEventSink>,
        snapshot_slot: Arc::new(RwLock::new(None)),
        saved_member_meta: Arc::new(RwLock::new(None)),
        parent_cancel: cancel.clone(),
    })
    .await;

    TestPartyHarness {
        self_id,
        eos_id,
        fn_id,
        party,
        events,
        metadata_store,
        cancel,
    }
}

struct RevisionedPatchResponder {
    server_rev: Arc<AtomicU64>,
    seen_chunks: Arc<Mutex<Vec<(u64, usize)>>>,
    is_member_patch: bool,
}

impl Respond for RevisionedPatchResponder {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let body: Value = req.body_json().expect("json patch body");
        let client_rev = body["revision"].as_u64().expect("revision");
        let update_map = if self.is_member_patch {
            body["update"].as_object().expect("member update object")
        } else {
            body["meta"]["update"]
                .as_object()
                .expect("party meta.update object")
        };

        let mut chunks = self.seen_chunks.lock().expect("chunks lock");
        chunks.push((client_rev, update_map.len()));

        let current = self.server_rev.load(Ordering::SeqCst);
        if client_rev != current {
            return ResponseTemplate::new(409).set_body_json(json!({
                "errorCode": "errors.com.epicgames.social.party.stale_revision",
                "errorMessage": "stale revision",
                "messageVars": [client_rev.to_string(), current.to_string()],
                "numericErrorCode": 51015
            }));
        }

        self.server_rev.store(current + 1, Ordering::SeqCst);
        ResponseTemplate::new(204)
    }
}

#[tokio::test]
async fn out_of_order_xmpp_deltas_replay_ascending_and_ttl_evicts_expired() {
    let server = MockServer::start().await;
    let remote_id = make_account_id(2);
    let expired_id = make_account_id(3);

    Mock::given(method("PATCH"))
        .and(path(format!(
            "/party/api/v1/Fortnite/parties/{}-123456-default",
            make_party_id(0xaa).as_str()
        )))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!(
            "/account/api/public/account/{}",
            expired_id.as_str()
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": expired_id.as_str(),
            "displayName": "ExpiredMember",
        })))
        .mount(&server)
        .await;

    let h = spawn_test_party(&server.uri(), Duration::from_secs(10), 0, 0, Vec::new()).await;
    let mut rx = h.events.subscribe();

    let mut delta_rev2 = MetaMap::new();
    let _ = delta_rev2.set_json(
        "Default:MpLoadout1_j",
        &json!({ "MpLoadout1": { "s": { "ac": { "i": "CID_Rev2_Second", "v": [] }, "ap": { "i": "Pickaxe_Rev1", "v": [] } } } }),
    );
    let mut delta_rev1 = MetaMap::new();
    let _ = delta_rev1.set_json(
        "Default:MpLoadout1_j",
        &json!({ "MpLoadout1": { "s": { "ac": { "i": "CID_Rev1_First", "v": [] }, "ap": { "i": "Pickaxe_Rev1", "v": [] } } } }),
    );

    h.party
        .handle_xmpp_member_delta(XMPPPartyMemberDeltaNotification {
            party_id: PartyId::from_raw(h.fn_id.as_str()),
            account_id: remote_id.clone(),
            account_dn: Some("RemotePlayer".to_owned()),
            revision: 2,
            joined_at: None,
            updated_at: None,
            sent: None,
            member_state_updated: delta_rev2,
            member_state_removed: Vec::new(),
        })
        .await
        .expect("buffer rev2");

    h.party
        .handle_xmpp_member_delta(XMPPPartyMemberDeltaNotification {
            party_id: PartyId::from_raw(h.fn_id.as_str()),
            account_id: remote_id.clone(),
            account_dn: None,
            revision: 1,
            joined_at: None,
            updated_at: None,
            sent: None,
            member_state_updated: delta_rev1,
            member_state_removed: Vec::new(),
        })
        .await
        .expect("buffer rev1");

    assert_eq!(h.metadata_store.active_entry_count(), 1);
    assert_eq!(h.party.snapshot().await.expect("snap").members.len(), 1);

    h.party
        .handle_stomp_member_joined(EOSPartyMemberUpdateData {
            party_id: h.eos_id.clone(),
            account_id: remote_id.clone(),
            account_dn: None,
            sent: None,
            revision: 0,
            joined_at: Some("2026-01-01T00:00:01Z".to_owned()),
            updated_at: None,
            suspended: None,
            member_state_overridden: MetaMap::new(),
            connection: None,
            member_state_updated: MetaMap::new(),
            member_state_removed: Vec::new(),
        })
        .await
        .expect("join remote_id");

    assert_eq!(h.metadata_store.active_entry_count(), 0);
    let snap = h.party.snapshot().await.expect("snap");
    let member = snap.members.get(&remote_id).expect("remote member");
    assert_eq!(member.revision, 2);
    assert_eq!(member.display_name.as_deref(), Some("RemotePlayer"));
    assert_eq!(member.outfit().as_deref(), Some("CID_Rev2_Second"));
    assert_eq!(member.pickaxe().as_deref(), Some("Pickaxe_Rev1"));

    let mut outfit_history: Vec<Option<String>> = Vec::new();
    let mut joined_seen = false;
    while let Ok(ev) = rx.try_recv() {
        match &*ev {
            PartyEvent::PartyMemberOutfitUpdated { after, .. } => {
                outfit_history.push(after.clone());
            }
            PartyEvent::PartyMemberJoined { member, .. } if member.account_id == remote_id => {
                joined_seen = true;
            }
            _ => {}
        }
    }
    assert!(joined_seen);
    assert_eq!(
        outfit_history,
        vec![
            Some("CID_Rev1_First".to_owned()),
            Some("CID_Rev2_Second".to_owned())
        ]
    );

    h.party.terminate().await;

    let ttl_harness =
        spawn_test_party(&server.uri(), Duration::from_millis(45), 0, 0, Vec::new()).await;

    let mut stale_delta = MetaMap::new();
    let _ = stale_delta.set_json(
        "Default:MpLoadout1_j",
        &json!({ "MpLoadout1": { "s": { "ac": { "i": "CID_ShouldExpire", "v": [] } } } }),
    );
    ttl_harness
        .party
        .handle_xmpp_member_delta(XMPPPartyMemberDeltaNotification {
            party_id: PartyId::from_raw(ttl_harness.fn_id.as_str()),
            account_id: expired_id.clone(),
            account_dn: None,
            revision: 3,
            joined_at: None,
            updated_at: None,
            sent: None,
            member_state_updated: stale_delta,
            member_state_removed: Vec::new(),
        })
        .await
        .expect("buffer expiring delta");

    tokio::time::sleep(Duration::from_millis(75)).await;

    ttl_harness
        .party
        .handle_stomp_member_joined(EOSPartyMemberUpdateData {
            party_id: ttl_harness.eos_id.clone(),
            account_id: expired_id.clone(),
            account_dn: None,
            sent: None,
            revision: 0,
            joined_at: Some("2026-01-01T00:00:02Z".to_owned()),
            updated_at: None,
            suspended: None,
            member_state_overridden: MetaMap::new(),
            connection: None,
            member_state_updated: MetaMap::new(),
            member_state_removed: Vec::new(),
        })
        .await
        .expect("join expired_id");

    let snap2 = ttl_harness.party.snapshot().await.expect("snap2");
    let expired_member: &PartyMemberSnapshot = snap2.members.get(&expired_id).expect("member");
    assert_eq!(expired_member.revision, 0);
    assert_eq!(expired_member.outfit(), None);
    assert_eq!(expired_member.display_name.as_deref(), Some("ExpiredMember"));
    ttl_harness.party.terminate().await;
}

#[tokio::test]
async fn patches_chunk_at_32_keys_and_recover_from_stale_revision() {
    let server = MockServer::start().await;
    let h = spawn_test_party(&server.uri(), Duration::from_secs(10), 1, 2, Vec::new()).await;

    let party_server_rev = Arc::new(AtomicU64::new(5));
    let party_chunks = Arc::new(Mutex::new(Vec::<(u64, usize)>::new()));
    let member_server_rev = Arc::new(AtomicU64::new(9));
    let member_chunks = Arc::new(Mutex::new(Vec::<(u64, usize)>::new()));

    Mock::given(method("PATCH"))
        .and(path(format!(
            "/party/api/v1/Fortnite/parties/{}",
            h.fn_id.as_str()
        )))
        .respond_with(RevisionedPatchResponder {
            server_rev: Arc::clone(&party_server_rev),
            seen_chunks: Arc::clone(&party_chunks),
            is_member_patch: false,
        })
        .mount(&server)
        .await;

    Mock::given(method("PATCH"))
        .and(path(format!(
            "/party/api/v1/Fortnite/parties/{}/members/{}/meta",
            h.fn_id.as_str(),
            h.self_id.as_str()
        )))
        .respond_with(RevisionedPatchResponder {
            server_rev: Arc::clone(&member_server_rev),
            seen_chunks: Arc::clone(&member_chunks),
            is_member_patch: true,
        })
        .mount(&server)
        .await;

    let mut party_updates = BTreeMap::new();
    for i in 0..45 {
        party_updates.insert(format!("Default:CustomParty_{i:02}_s"), format!("v_{i}"));
    }
    h.party
        .send_patch(MetaPatch {
            updated: party_updates,
            deleted: Vec::new(),
        })
        .await
        .expect("chunked party patch succeeds after stale_revision resync");

    let recorded_party = party_chunks.lock().expect("party chunks").clone();
    assert_eq!(recorded_party, vec![(1, 32), (5, 32), (6, 13)]);
    assert_eq!(h.party.snapshot().await.expect("snap").revision, 7);

    let mut member_updates = BTreeMap::new();
    for i in 0..40 {
        member_updates.insert(format!("Default:CustomMember_{i:02}_s"), format!("m_{i}"));
    }
    h.party
        .me()
        .send_patch(MetaPatch {
            updated: member_updates,
            deleted: Vec::new(),
        })
        .await
        .expect("chunked member patch succeeds after stale_revision resync");

    let recorded_member = member_chunks.lock().expect("member chunks").clone();
    assert_eq!(recorded_member, vec![(2, 32), (9, 32), (10, 8)]);
    let snap = h.party.snapshot().await.expect("snap");
    assert_eq!(snap.members.get(&h.self_id).expect("me").revision, 11);

    h.party.terminate().await;
}

async fn wait_for_keepalive_count(notify: &Notify, counter: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while counter.load(Ordering::SeqCst) < expected {
            notify.notified().await;
        }
    })
    .await
    .expect("timed out waiting for keepalive tick");
}

#[tokio::test]
async fn eos_keep_alive_ticks_every_60s_and_stops_cleanly_on_leave() {
    let server = MockServer::start().await;
    let keepalive_count = Arc::new(AtomicUsize::new(0));
    let count_clone = Arc::clone(&keepalive_count);
    let keepalive_notify = Arc::new(Notify::new());
    let notify_clone = Arc::clone(&keepalive_notify);
    let eos_id = make_party_id(0xaa);
    let self_id = make_account_id(1);
    let fn_id = to_fortnite_lobby_id(&eos_id, "123456").expect("lobby id");

    Mock::given(method("POST"))
        .and(path(format!(
            "/epic/party/internal/v2/parties/{}/members/{}/keep-alive",
            eos_id.as_str(),
            self_id.as_str()
        )))
        .respond_with(move |_req: &Request| {
            count_clone.fetch_add(1, Ordering::SeqCst);
            notify_clone.notify_waiters();
            ResponseTemplate::new(204)
        })
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path(format!(
            "/party/api/v1/Fortnite/parties/{}/members/{}",
            fn_id.as_str(),
            self_id.as_str()
        )))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path(format!(
            "/epic/party/internal/v2/parties/{}/members/{}",
            eos_id.as_str(),
            self_id.as_str()
        )))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let h = spawn_test_party_with_keepalive(
        &server.uri(),
        Duration::from_secs(10),
        Duration::from_millis(120),
        0,
        0,
        Vec::new(),
    )
    .await;

    assert_eq!(keepalive_count.load(Ordering::SeqCst), 0);

    wait_for_keepalive_count(&keepalive_notify, &keepalive_count, 2).await;
    assert_eq!(keepalive_count.load(Ordering::SeqCst), 2);

    h.party.leave().await.expect("leave party");
    assert!(h.party.snapshot().await.is_none());
    assert!(h.party.cancellation_token().is_cancelled());

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(keepalive_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn concurrent_cosmetics_privacy_squad_and_party_chat_flow() {
    let server = MockServer::start().await;
    let friend_id = make_account_id(2);

    let h_solo = spawn_test_party(&server.uri(), Duration::from_secs(10), 0, 0, Vec::new()).await;
    let err = h_solo
        .party
        .chat()
        .send("alone")
        .await
        .expect_err("solo party chat rejected");
    assert!(matches!(err, RustfnError::PartyState(_)));
    h_solo.party.terminate().await;

    let h = spawn_test_party(
        &server.uri(),
        Duration::from_secs(10),
        1,
        1,
        vec![friend_id.clone()],
    )
    .await;

    Mock::given(method("PATCH"))
        .and(path(format!(
            "/party/api/v1/Fortnite/parties/{}/members/{}/meta",
            h.fn_id.as_str(),
            h.self_id.as_str()
        )))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    Mock::given(method("PATCH"))
        .and(path(format!(
            "/party/api/v1/Fortnite/parties/{}",
            h.fn_id.as_str()
        )))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    Mock::given(method("PATCH"))
        .and(path(format!(
            "/epic/party/internal/v2/parties/{}",
            h.eos_id.as_str()
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": h.eos_id.as_str(),
            "party_lead": h.self_id.as_str(),
            "config": { "joinability": "INVITE_ONLY", "max_size": 16 },
            "meta": {},
            "revision": 2,
            "chat_conversation_id": format!("ep-{}", h.eos_id.as_str()),
            "is_reportable": true,
            "members": []
        })))
        .mount(&server)
        .await;

    let registered_pub_key = Arc::new(Mutex::new(None::<String>));
    let reg_key_clone = Arc::clone(&registered_pub_key);
    Mock::given(method("POST"))
        .and(path("/publickey/v2/publickey"))
        .respond_with(move |req: &Request| {
            let body: Value = req.body_json().expect("publickey body");
            let key = body["key"].as_str().expect("pub key").to_owned();
            *reg_key_clone.lock().expect("pub key lock") = Some(key.clone());
            ResponseTemplate::new(200).set_body_json(json!({
                "key": key,
                "algorithm": "ed25519",
                "jwt": "jwt-token",
                "keyGuid": "guid-1"
            }))
        })
        .mount(&server)
        .await;

    let chat_captured = Arc::new(Mutex::new(None::<Value>));
    let chat_cap_clone = Arc::clone(&chat_captured);
    Mock::given(method("POST"))
        .and(path(format!(
            "/epic/chat/v1/public/_/conversations/ep-{}/messages",
            h.eos_id.as_str()
        )))
        .respond_with(move |req: &Request| {
            *chat_cap_clone.lock().expect("chat capture lock") =
                Some(req.body_json().expect("chat body"));
            ResponseTemplate::new(200).set_body_json(json!({
                "messageId": "party-msg-1",
                "timestamp": 1700000001000u64
            }))
        })
        .mount(&server)
        .await;

    let me = h.party.me();
    let (r1, r2, r3, r4) = tokio::join!(
        me.set_cosmetics(CosmeticsUpdate {
            outfit: Some(CosmeticItem::new("CID_Test_Aura")),
            pickaxe: Some(CosmeticItem::new("Pickaxe_StarWand")),
            ..Default::default()
        }),
        me.set_emote("EID_TakeTheL", None),
        h.party
            .set_privacy(PartyPrivacyPreset::Private.to_privacy(), true),
        h.party.hide_member(friend_id.as_str(), true),
    );
    r1.expect("set_cosmetics");
    r2.expect("set_emote");
    r3.expect("set_privacy");
    r4.expect("hide_member");

    let snap = h.party.snapshot().await.expect("snap");
    let me_snap = snap.members.get(&h.self_id).expect("self");
    assert_eq!(me_snap.outfit().as_deref(), Some("CID_Test_Aura"));
    assert_eq!(me_snap.pickaxe().as_deref(), Some("Pickaxe_StarWand"));
    assert_eq!(me_snap.emote().as_deref(), Some("EID_TakeTheL"));
    assert!(snap.is_private());
    assert_eq!(snap.eos_config.joinability, PartyJoinability::InviteOnly);
    assert_eq!(snap.meta.squad_assignments().len(), 1);
    assert_eq!(snap.meta.squad_assignments()[0].member_id, h.self_id);

    let sent = h
        .party
        .send_message("hello squad")
        .await
        .expect("send party chat");
    assert_eq!(sent.id.as_deref(), Some("party-msg-1"));

    let sent_payload = chat_captured
        .lock()
        .expect("chat capture lock")
        .clone()
        .expect("chat body");
    let body_b64 = sent_payload["message"]["body"].as_str().expect("body");
    let meta = &sent_payload["metadata"];
    let pub_key = registered_pub_key
        .lock()
        .expect("pub key lock")
        .clone()
        .expect("registered public key");
    verify_signed_message(
        &pub_key,
        body_b64,
        meta["Sig"].as_str().expect("sig"),
    )
    .expect("party chat signature verifies");
    assert_eq!(meta["Pub"].as_str(), Some("jwt-token"));
    assert_eq!(decode_stomp_message_body(body_b64), "hello squad");

    h.cancel.cancel();
    h.party.terminate().await;
}
