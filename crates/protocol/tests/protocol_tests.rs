use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use rustfn_protocol::{
    AccountId, AuthClient, ConversationType, EOS_EAS_DEPLOYMENT_ID, EOSPartyUserState,
    EpicgamesAPIErrorData, FortnitePartyPrivacy, INVALID_TOKEN_CODES, IdFormatError, LobbyId,
    MetaCodecError, MetaMap, MetaPatch, MetaSuffix, MetaTypedValue, PartyId, PartyPrivacyPreset,
    Platform, Playlist, PresenceOnlineType, PresencePropValue, ReadyState, Region, RustfnError,
    ServiceEndpoints, V2Input, error_codes, meta_codec::keys, to_eos_party_id,
    to_fortnite_lobby_id,
};
use serde_json::json;
use std::collections::BTreeMap;

#[test]
fn epic_ids_parse_and_lobby_conversion() {
    let raw_upper = "AABBCCDDEEFF00112233445566778899";
    let account_id = AccountId::parse(raw_upper).expect("valid hex32 account ID");
    assert_eq!(account_id.as_str(), "aabbccddeeff00112233445566778899");

    let bad = AccountId::parse("not-valid-hex");
    assert!(matches!(bad, Err(IdFormatError::InvalidHex32 { .. })));

    let party_id = PartyId::parse("11112222333344445555666677778888").unwrap();
    let lobby_id = to_fortnite_lobby_id(&party_id, "1:3:37219843").expect("valid netCL build ID");
    assert_eq!(
        lobby_id.as_str(),
        "11112222333344445555666677778888-37219843-default"
    );

    let recovered = to_eos_party_id(&lobby_id).expect("recover party ID");
    assert_eq!(recovered, party_id);

    assert!(LobbyId::parse("11112222333344445555666677778888-abc-default").is_err());
    assert_eq!(EOS_EAS_DEPLOYMENT_ID.len(), 32);
}

#[test]
fn auth_clients_descriptors_and_basic_headers() {
    for client in AuthClient::ALL {
        assert!(!client.client_id().is_empty());
        assert!(!client.secret().is_empty());
        let header = client.basic_auth_header();
        let encoded = header
            .strip_prefix("Basic ")
            .expect("header starts with Basic ");
        let decoded = String::from_utf8(STANDARD.decode(encoded).expect("valid base64"))
            .expect("valid UTF-8");
        assert_eq!(decoded, format!("{}:{}", client.client_id(), client.secret()));
        assert_eq!(
            AuthClient::from_name_or_id(client.canonical_name()),
            Some(client)
        );
        assert_eq!(AuthClient::from_name_or_id(client.client_id()), Some(client));
    }

    assert!(AuthClient::FortniteIosGameClient.supports_device_auth());
    assert!(AuthClient::FortniteAndroidGameClient.supports_device_auth());
    assert!(AuthClient::FortniteSwitchGameClient.supports_device_auth());
    assert!(!AuthClient::FortnitePcGameClient.supports_device_auth());
    assert!(!AuthClient::FortniteNewSwitchGameClient.supports_device_auth());
    assert!(!AuthClient::LauncherAppClient2.supports_device_auth());
    assert!(!AuthClient::DieselDauntless.supports_device_auth());
}

#[test]
fn service_endpoints_build_expected_paths() {
    let ep = ServiceEndpoints::default();
    assert_eq!(
        ep.device_auth_collection("abc"),
        "https://account-public-service-prod.ol.epicgames.com/account/api/public/account/abc/deviceAuth"
    );
    assert_eq!(
        ep.br_party_member("p1", "u1"),
        "https://party-service-prod.ol.epicgames.com/party/api/v1/Fortnite/parties/p1/members/u1"
    );
}

#[test]
fn platform_privacy_and_playlist_serialization() {
    assert_eq!(serde_json::to_string(&Platform::Swt2).unwrap(), "\"SWT2\"");
    let unknown_plat: Platform = serde_json::from_str("\"FUTURE_CONSOLE\"").unwrap();
    assert_eq!(unknown_plat, Platform::Unknown);

    assert_eq!(serde_json::to_string(&Region::Eu).unwrap(), "\"EU\"");
    assert_eq!(
        serde_json::to_string(&PresenceOnlineType::Dnd).unwrap(),
        "\"dnd\""
    );
    assert_eq!(serde_json::to_string(&ReadyState::SittingOut).unwrap(), "\"SittingOut\"");
    assert_eq!(
        serde_json::to_string(&V2Input::KeyboardMouse).unwrap(),
        "\"keyboardmouse\""
    );

    let private_privacy: FortnitePartyPrivacy = PartyPrivacyPreset::Private.to_privacy();
    let encoded = serde_json::to_value(private_privacy).unwrap();
    assert_eq!(
        encoded,
        json!({
            "partyType": "Private",
            "inviteRestriction": "LeaderOnly",
            "onlyLeaderFriendsCanJoin": true,
            "presencePermission": "Noone",
            "invitePermission": "Leader",
            "acceptingMembers": false
        })
    );

    let playlist = Playlist::from("Playlist_NoBuildBR_Solo");
    assert_eq!(playlist.as_str(), "Playlist_NoBuildBR_Solo");
}

#[test]
fn api_error_deserialization_and_predicates() {
    let stale_json = r#"{
        "errorCode": "errors.com.epicgames.social.party.stale_revision",
        "errorMessage": "Stale revision",
        "messageVars": ["4", "7"],
        "numericErrorCode": 51015,
        "originatingService": "party",
        "intent": "prod",
        "validationFailures": {
            "revision": {
                "fieldName": "revision",
                "invalidValue": 4,
                "errorMessage": "Expected revision 7",
                "errorCode": "stale",
                "messageVars": ["7"]
            }
        }
    }"#;

    let api_err: EpicgamesAPIErrorData = serde_json::from_str(stale_json).unwrap();
    assert!(api_err.is_stale_revision());
    assert_eq!(api_err.parse_stale_revisions(), Some((4, 7)));
    assert_eq!(
        api_err
            .validation_failures
            .as_ref()
            .unwrap()
            .get("revision")
            .unwrap()
            .field_name,
        "revision"
    );

    let oauth_err: EpicgamesAPIErrorData = serde_json::from_str(
        r#"{"error": "invalid_grant", "error_description": "Authorization code not found"}"#,
    )
    .unwrap();
    assert_eq!(oauth_err.effective_code(), "invalid_grant");
    assert_eq!(oauth_err.effective_message(), "Authorization code not found");

    for code in INVALID_TOKEN_CODES {
        let mut token_err = EpicgamesAPIErrorData::default();
        token_err.error_code = code.to_owned();
        assert!(token_err.is_invalid_token());
        let wrapped = RustfnError::EpicApi {
            status: 401,
            body: Box::new(token_err),
        };
        assert!(wrapped.is_invalid_token());
    }
    assert_eq!(
        error_codes::CORRECTIVE_ACTION_REQUIRED,
        "errors.com.epicgames.oauth.corrective_action_required"
    );
}

#[test]
fn meta_map_suffix_codecs_diff_and_patch() {
    assert_eq!(MetaSuffix::from_key(keys::FRONTEND_EMOTE_J), MetaSuffix::Json);
    assert_eq!(MetaSuffix::from_key("Default:VoiceChatEnabled_b"), MetaSuffix::Bool);
    assert_eq!(MetaSuffix::from_key("Default:NumAthenaPlayersLeft_U"), MetaSuffix::UnsignedInt);
    assert_eq!(MetaSuffix::from_key("Default:PlatformUniqueId_s"), MetaSuffix::String);
    assert_eq!(MetaSuffix::from_key("Default:ArbitraryFloat_d"), MetaSuffix::Double);

    let mut meta = MetaMap::new();
    meta.set_json(
        keys::PRIVACY_SETTINGS_J,
        &json!({ "PrivacySettings": PartyPrivacyPreset::Public.to_privacy() }),
    )
    .unwrap();
    meta.set_bool("Default:AllowJoinInProgress_b", true);
    meta.set_u64("Default:NumPlayers_U", 1);
    meta.set_f64("Default:MatchTime_d", 12.5);
    meta.set_str(keys::REGION_ID_S, "EU");

    assert_eq!(meta.get_bool("Default:AllowJoinInProgress_b").unwrap(), Some(true));
    assert_eq!(meta.get_u64("Default:NumPlayers_U").unwrap(), Some(1));
    assert_eq!(meta.get_raw("Default:MatchTime_d"), Some("12.500000"));
    assert_eq!(meta.get_str(keys::REGION_ID_S), Some("EU"));

    let privacy_doc: serde_json::Value = meta.get_json(keys::PRIVACY_SETTINGS_J).unwrap().unwrap();
    assert_eq!(privacy_doc["PrivacySettings"]["partyType"], "Public");

    let mismatch = meta.set_typed("Default:IsReady_b", MetaTypedValue::Text("yes".into()));
    assert!(matches!(mismatch, Err(MetaCodecError::SuffixMismatch { .. })));

    let mut next = meta.clone();
    next.set_u64("Default:NumPlayers_U", 2);
    next.remove("Default:MatchTime_d");
    next.set_str("Default:NewKey_s", "added");

    let patch: MetaPatch = meta.diff_to(&next);
    assert_eq!(patch.deleted, vec!["Default:MatchTime_d".to_owned()]);
    assert_eq!(
        patch.updated,
        BTreeMap::from([
            ("Default:NewKey_s".to_owned(), "added".to_owned()),
            ("Default:NumPlayers_U".to_owned(), "2".to_owned()),
        ])
    );

    let mut applied = meta.clone();
    applied.apply_patch(&patch);
    assert_eq!(applied, next);
}

#[test]
fn presence_prop_value_wire_roundtrip() {
    let cases = [
        (PresencePropValue::Bool(true), "btrue"),
        (PresencePropValue::Bool(false), "bfalse"),
        (PresencePropValue::Int(-42), "i-42"),
        (PresencePropValue::Str("Playing Battle Royale".into()), "sPlaying Battle Royale"),
    ];

    for (val, expected_wire) in cases {
        assert_eq!(val.encode(), expected_wire);
        assert_eq!(PresencePropValue::decode(expected_wire), val);

        let serialized = serde_json::to_string(&val).unwrap();
        let deserialized: PresencePropValue = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, val);
    }

    let json_prop = PresencePropValue::Json(json!({"partyId": "abc"}));
    let wire_json = json_prop.encode();
    assert!(wire_json.starts_with('m'));
    assert_eq!(PresencePropValue::decode(&wire_json), json_prop);

    let raw_prop = PresencePropValue::RawStr("Fortnite".to_owned());
    assert_eq!(raw_prop.encode(), "Fortnite");
    assert_eq!(raw_prop.as_str(), Some("Fortnite"));

    assert_eq!(
        PresencePropValue::decode("in_lobby"),
        PresencePropValue::Str("in_lobby".to_owned())
    );
    assert_eq!(
        PresencePropValue::decode("battle_royale"),
        PresencePropValue::Str("battle_royale".to_owned())
    );
    assert_eq!(
        PresencePropValue::decode("m{not_valid_json"),
        PresencePropValue::Str("m{not_valid_json".to_owned())
    );
}

#[test]
fn epic_api_error_user_has_party_and_eos_current_party_flexible_deser() {
    let err_by_code = EpicgamesAPIErrorData {
        error_code: error_codes::USER_HAS_PARTY.to_owned(),
        numeric_error_code: None,
        ..EpicgamesAPIErrorData::default()
    };
    assert!(err_by_code.is_user_has_party());

    let err_by_num = EpicgamesAPIErrorData {
        error_code: String::new(),
        numeric_error_code: Some(51012),
        ..EpicgamesAPIErrorData::default()
    };
    assert!(err_by_num.is_user_has_party());

    let wrapped = RustfnError::EpicApi {
        status: 409,
        body: Box::new(err_by_code),
    };
    assert!(wrapped.is_user_has_party());

    let null_state: EOSPartyUserState = serde_json::from_value(json!({
        "current": null,
        "invites": [],
        "join_requests": []
    }))
    .unwrap();
    assert!(null_state.current.is_none());

    let empty_array_state: EOSPartyUserState = serde_json::from_value(json!({
        "current": [],
        "invites": [],
        "join_requests": []
    }))
    .unwrap();
    assert!(empty_array_state.current.is_none());

    let sample_party = json!({
        "id": "11112222333344445555666677778888",
        "party_lead": "aaaa1111bbbb2222cccc3333dddd4444",
        "config": { "joinability": "OPEN", "max_size": 16 }
    });
    let object_state: EOSPartyUserState = serde_json::from_value(json!({
        "current": sample_party,
        "invites": [],
        "join_requests": []
    }))
    .unwrap();
    assert_eq!(
        object_state.current.as_ref().map(|p| p.id.as_str()),
        Some("11112222333344445555666677778888")
    );

    let array_state: EOSPartyUserState = serde_json::from_value(json!({
        "current": [sample_party],
        "invites": [],
        "join_requests": []
    }))
    .unwrap();
    assert_eq!(
        array_state.current.as_ref().map(|p| p.id.as_str()),
        Some("11112222333344445555666677778888")
    );

    assert_eq!(
        serde_json::from_str::<ConversationType>("\"epic_party\"").unwrap(),
        ConversationType::PartyChat
    );
    assert_eq!(
        serde_json::from_str::<ConversationType>("\"dm\"").unwrap(),
        ConversationType::DirectMessage
    );
}
