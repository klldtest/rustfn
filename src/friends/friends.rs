use chrono::{DateTime, Utc};
use dashmap::DashMap;
use crate::http::{HTTP, FriendsSummaryResponse};
use crate::protocol::{
    AccountId, AwayStatus, CacheConfig, Platform, PresenceOnlineType, PresencePropValue,
    PresencePropsMap, RustfnError, XMPPPresenceStatusPayload,
};
use crate::stomp::EOSPresenceUpdatePayload;
use crate::users::{UserManager, UserProfile};
use crate::xmpp::{
    XMPPBlockListEntryPayload, XMPPFriendNotification, XMPPFriendshipRemoveNotification,
    XMPPUserBlocklistUpdate,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;

const ERR_FRIEND_NOT_FOUND: &str = "errors.com.epicgames.friends.friend_not_found";
const ERR_ACCOUNT_NOT_FOUND: &str = "errors.com.epicgames.account.account_not_found";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum FriendDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FriendRecord {
    pub id: AccountId,
    pub display_name: Option<String>,
    pub alias: Option<String>,
    pub note: Option<String>,
    pub mutual_count: Option<u32>,
    pub favorite: bool,
    pub created_at: Option<String>,
    pub groups: Vec<String>,
}

impl FriendRecord {
    pub fn apply_profile(&mut self, profile: &UserProfile) {
        if let Some(dn) = &profile.display_name {
            self.display_name = Some(dn.clone());
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingFriend {
    pub id: AccountId,
    pub display_name: Option<String>,
    pub direction: FriendDirection,
    pub mutual_count: Option<u32>,
    pub favorite: bool,
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockedUser {
    pub id: AccountId,
    pub display_name: Option<String>,
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FriendSummarySnapshot {
    pub friends: Vec<FriendRecord>,
    pub incoming: Vec<PendingFriend>,
    pub outgoing: Vec<PendingFriend>,
    pub blocked: Vec<BlockedUser>,
}

const PARTY_JOIN_INFO_PROP_KEY: &str = "party.joininfodata.286331153_j";

/// Structured party join metadata carried inside `party.joininfodata.286331153_j`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartyJoinInfoData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<AccountId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_platform: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub party_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub party_type_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub party_flags: Option<u32>,
    #[serde(
        rename = "notAcceptingReason",
        alias = "notAcceptingMembersReason",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub not_accepting_members_reason: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pc: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FortBasicInfoProp {
    #[serde(default)]
    source_platform: Option<String>,
    #[serde(default)]
    home_base_rating: Option<i32>,
}

/// Friend presence from XMPP or EOS Connect.
#[derive(Debug, Clone, PartialEq)]
pub struct FriendPresence {
    pub account_id: AccountId,
    pub namespace: String,
    pub online_type: PresenceOnlineType,
    pub away_status: AwayStatus,
    pub status: String,
    pub platform: Option<Platform>,
    pub product_name: Option<String>,
    pub session_id: Option<String>,
    pub is_playing: bool,
    pub is_joinable: bool,
    pub party_size: Option<i64>,
    pub sub_game: Option<String>,
    pub island_code: Option<String>,
    pub is_in_zone: Option<bool>,
    pub in_unjoinable_match: Option<bool>,
    pub social_status: Option<String>,
    pub party_join_info: Option<PartyJoinInfoData>,
    pub raw_properties: BTreeMap<String, Value>,
    pub received_at: DateTime<Utc>,
}

impl FriendPresence {
    /// Parses a friend's presence from an XMPP `<presence>` stanza payload.
    pub fn from_xmpp(
        account_id: AccountId,
        online_type: PresenceOnlineType,
        away_status: AwayStatus,
        payload: Option<&XMPPPresenceStatusPayload>,
        fallback_status_text: Option<&str>,
    ) -> Self {
        let mut presence = Self {
            account_id,
            namespace: "fn".to_owned(),
            online_type,
            away_status,
            status: payload
                .map(|p| p.status.clone())
                .or_else(|| fallback_status_text.map(ToOwned::to_owned))
                .unwrap_or_default(),
            platform: None,
            product_name: payload.and_then(|p| p.product_name.clone()),
            session_id: payload.and_then(|p| p.session_id.clone()),
            is_playing: payload.is_some_and(|p| p.is_playing),
            is_joinable: payload.is_some_and(|p| p.is_joinable),
            party_size: None,
            sub_game: None,
            island_code: None,
            is_in_zone: None,
            in_unjoinable_match: None,
            social_status: None,
            party_join_info: None,
            raw_properties: payload.map(|p| p.properties.clone()).unwrap_or_default(),
            received_at: Utc::now(),
        };

        if let Some(p) = payload {
            presence.apply_raw_props(&p.properties);
        }
        presence
    }

    /// Parses a friend's presence from an EOS Connect STOMP `presence.v1.UPDATE` payload.
    pub fn from_eos_update(payload: &EOSPresenceUpdatePayload) -> Self {
        let selected = payload
            .per_ns
            .iter()
            .find(|ns| ns.ns == "fn")
            .or_else(|| payload.per_ns.first());

        let online_type = selected.map(|s| s.status).unwrap_or(payload.status);
        let away_status = match online_type {
            PresenceOnlineType::Away => AwayStatus::Away,
            PresenceOnlineType::Xa => AwayStatus::ExtendedAway,
            PresenceOnlineType::Dnd => AwayStatus::DoNotDisturb,
            PresenceOnlineType::Chat => AwayStatus::Chat,
            PresenceOnlineType::Online | PresenceOnlineType::Offline => AwayStatus::Online,
        };

        let mut presence = Self {
            account_id: payload.account_id.clone(),
            namespace: selected.map(|s| s.ns.clone()).unwrap_or_else(|| "fn".to_owned()),
            online_type,
            away_status,
            status: selected.map(|s| s.activity.value.clone()).unwrap_or_default(),
            platform: None,
            product_name: selected.and_then(|s| s.product_id.clone()),
            session_id: None,
            is_playing: selected
                .and_then(|s| s.product_id.as_deref())
                .is_some_and(|pid| pid.eq_ignore_ascii_case("Fortnite"))
                || online_type != PresenceOnlineType::Offline,
            is_joinable: false,
            party_size: None,
            sub_game: None,
            island_code: None,
            is_in_zone: None,
            in_unjoinable_match: None,
            social_status: None,
            party_join_info: None,
            raw_properties: selected.map(|s| s.props.clone()).unwrap_or_default(),
            received_at: Utc::now(),
        };

        if let Some(ns) = selected {
            let decoded = ns.decoded_props();
            presence.apply_decoded_props(&decoded);
            presence.is_joinable = presence.party_join_info.is_some()
                && presence
                    .party_join_info
                    .as_ref()
                    .and_then(|info| info.not_accepting_members_reason)
                    .unwrap_or(0)
                    == 0;
            if let Some(PresencePropValue::Str(plat)) = decoded.get("EOS_Platform") {
                presence.platform = parse_platform_loose(plat);
            }
        }
        presence
    }

    fn apply_raw_props(&mut self, props: &BTreeMap<String, Value>) {
        let prop = |a, b| props.get(a).or_else(|| props.get(b));

        if let Some(join_val) = prop("party.joininfodata.286331153", PARTY_JOIN_INFO_PROP_KEY)
            && let Ok(mut parsed) = serde_json::from_value::<PartyJoinInfoData>(join_val.clone())
        {
            let str_field = |k| join_val.get(k).and_then(Value::as_str).map(ToOwned::to_owned);
            parsed.source_display_name = parsed.source_display_name.or_else(|| str_field("sDN"));
            parsed.source_platform = parsed.source_platform.or_else(|| str_field("sP"));
            parsed.party_id = parsed.party_id.or_else(|| str_field("p"));
            parsed.build_id = parsed.build_id.or_else(|| str_field("b"));
            parsed.not_accepting_members_reason = parsed
                .not_accepting_members_reason
                .or_else(|| join_val.get("nAR").and_then(Value::as_i64));
            parsed.pc = parsed
                .pc
                .or_else(|| join_val.get("pc").and_then(Value::as_u64).map(|n| n as u32));

            if let Some(ref plat) = parsed.source_platform {
                self.platform = parse_platform_loose(plat);
            }
            let is_private = join_val
                .get("bIsPrivate")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !is_private || parsed.party_id.is_some() {
                self.party_join_info = Some(parsed);
            }
        }

        if let Some(basic) = prop("FortBasicInfo", "FortBasicInfo_j")
            && let Ok(parsed) = serde_json::from_value::<FortBasicInfoProp>(basic.clone())
            && self.platform.is_none()
            && let Some(ref plat) = parsed.source_platform
        {
            self.platform = parse_platform_loose(plat);
        }

        self.party_size = prop("FortPartySize", "FortPartySize_i").and_then(Value::as_i64);
        self.sub_game = prop("FortSubGame", "FortSubGame_s").map(|v| match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        });
        self.island_code = prop("IslandCode", "IslandCode_s")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        self.is_in_zone = prop("IsInZone", "IsInZone_b").and_then(Value::as_bool);
        self.in_unjoinable_match =
            prop("InUnjoinableMatch", "InUnjoinableMatch_b").and_then(Value::as_bool);
        self.social_status = prop("SocialStatus", "SocialStatus_s")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
    }

    fn apply_decoded_props(&mut self, decoded: &BTreeMap<String, PresencePropValue>) {
        let mut raw_map = BTreeMap::new();
        for (k, v) in decoded {
            let json_val = match v {
                PresencePropValue::Bool(b) => Value::Bool(*b),
                PresencePropValue::Int(i) => Value::Number((*i).into()),
                PresencePropValue::Json(j) => j.clone(),
                PresencePropValue::Str(s) | PresencePropValue::RawStr(s) => {
                    Value::String(s.clone())
                }
            };
            raw_map.insert(k.clone(), json_val);
        }
        self.apply_raw_props(&raw_map);
    }
}

/// Parameters required to publish the local client's Fortnite presence across EOS & XMPP.
#[derive(Debug, Clone)]
pub struct OutboundPresenceSpec {
    pub status: String,
    pub online_type: PresenceOnlineType,
    pub platform: Platform,
    pub product_version: String,
    pub sub_game: String,
    pub party_size: i64,
    pub is_joinable: bool,
    pub is_in_zone: bool,
    pub in_unjoinable_match: bool,
    pub island_code: String,
    pub party_join_info: Option<PartyJoinInfoData>,
}

/// Builds EOS Connect presence `props` map using untagged `EOS_*` runtime strings and
/// `b`/`i`/`m`/`s`-encoded Fortnite game keys.
pub fn build_eos_presence_props(spec: &OutboundPresenceSpec) -> PresencePropsMap {
    let mut props = PresencePropsMap::new();
    props.insert(
        "EOS_Platform".to_owned(),
        PresencePropValue::RawStr(spec.platform.as_str().to_owned()),
    );
    props.insert(
        "EOS_IntegratedPlatform".to_owned(),
        PresencePropValue::RawStr("EGS".to_owned()),
    );
    props.insert(
        "EOS_OnlinePlatformType".to_owned(),
        PresencePropValue::RawStr("100".to_owned()),
    );
    props.insert(
        "EOS_ProductVersion".to_owned(),
        PresencePropValue::RawStr(spec.product_version.clone()),
    );
    props.insert(
        "EOS_ProductName".to_owned(),
        PresencePropValue::RawStr("Fortnite".to_owned()),
    );
    props.insert(
        "EOS_Session".to_owned(),
        PresencePropValue::RawStr("{\"version\":3}".to_owned()),
    );
    props.insert(
        "EOS_Lobby".to_owned(),
        PresencePropValue::RawStr("{\"version\":3}".to_owned()),
    );
    props.insert(
        "Event_PartySize".to_owned(),
        PresencePropValue::Str(spec.party_size.to_string()),
    );
    props.insert(
        "Event_PartyMaxSize".to_owned(),
        PresencePropValue::Str("16".to_owned()),
    );
    props.insert(
        "FortBasicInfo".to_owned(),
        PresencePropValue::Json(json!({ "homeBaseRating": 0 })),
    );
    props.insert("FortLFG".to_owned(), PresencePropValue::Int(0));
    props.insert(
        "FortPartySize".to_owned(),
        PresencePropValue::Int(spec.party_size),
    );
    props.insert("FortSubGame".to_owned(), PresencePropValue::Int(1));
    let island = if spec.island_code.is_empty() {
        "experience_br".to_owned()
    } else {
        spec.island_code.clone()
    };
    props.insert("IslandCode".to_owned(), PresencePropValue::Str(island));
    props.insert(
        "IsInZone".to_owned(),
        PresencePropValue::Bool(spec.is_in_zone),
    );
    props.insert(
        "FortGameplayStats".to_owned(),
        PresencePropValue::Json(json!({
            "state": "",
            "playlist": "None",
            "numKills": 0,
            "bFellToDeath": false,
        })),
    );
    props.insert(
        "SocialStatus".to_owned(),
        PresencePropValue::Json(json!({ "attendingSocialEventIds": [] })),
    );
    props.insert(
        "InUnjoinableMatch".to_owned(),
        PresencePropValue::Bool(spec.in_unjoinable_match),
    );

    let join_val = if spec.is_joinable
        && let Some(join_info) = &spec.party_join_info
        && let Some(party_id) = &join_info.party_id
    {
        json!({
            "sDN": join_info.source_display_name,
            "sP": join_info.source_platform.as_deref().unwrap_or(spec.platform.as_str()),
            "p": party_id.as_str(),
            "d": join_info.app_id.as_deref().unwrap_or("Fortnite"),
            "b": join_info.build_id,
            "f": join_info.party_flags.unwrap_or(6),
            "nAR": join_info.not_accepting_members_reason.unwrap_or(0),
            "pc": join_info.pc.unwrap_or(spec.party_size.max(1) as u32),
        })
    } else {
        json!({ "bIsPrivate": true })
    };
    props.insert(
        "party.joininfodata.286331153".to_owned(),
        PresencePropValue::Json(join_val),
    );

    props
}

/// Builds legacy XMPP `<status>` JSON matching Fortnite desktop/mobile client expectations.
pub fn build_xmpp_status_payload(spec: &OutboundPresenceSpec) -> XMPPPresenceStatusPayload {
    let island = if spec.island_code.is_empty() {
        "experience_br".to_owned()
    } else {
        spec.island_code.clone()
    };
    let mut properties = BTreeMap::new();
    properties.insert(
        "FortBasicInfo_j".to_owned(),
        json!({ "homeBaseRating": 0, "sourcePlatform": spec.platform.as_str() }),
    );
    properties.insert("FortLFG_I".to_owned(), Value::String("0".to_owned()));
    properties.insert("FortPartySize_i".to_owned(), json!(spec.party_size));
    properties.insert("FortSubGame_i".to_owned(), json!(1));
    properties.insert("IslandCode_s".to_owned(), Value::String(island));
    properties.insert("IsInZone_b".to_owned(), json!(spec.is_in_zone));
    properties.insert(
        "FortGameplayStats_j".to_owned(),
        json!({
            "state": "",
            "playlist": "None",
            "numKills": 0,
            "bFellToDeath": false,
        }),
    );
    properties.insert(
        "SocialStatus_j".to_owned(),
        json!({ "attendingSocialEventIds": [] }),
    );
    properties.insert("InUnjoinableMatch_b".to_owned(), json!(spec.in_unjoinable_match));
    if spec.is_joinable {
        if let Some(join_info) = &spec.party_join_info
            && let Ok(val) = serde_json::to_value(join_info)
        {
            properties.insert(PARTY_JOIN_INFO_PROP_KEY.to_owned(), val);
        }
    } else if spec.party_join_info.is_some() {
        properties.insert(
            PARTY_JOIN_INFO_PROP_KEY.to_owned(),
            json!({ "bIsPrivate": true }),
        );
    }

    XMPPPresenceStatusPayload {
        status: spec.status.clone(),
        is_playing: true,
        is_joinable: spec.is_joinable,
        has_voice_support: false,
        session_id: None,
        product_name: Some("Fortnite".to_owned()),
        properties,
    }
}

fn parse_platform_loose(raw: &str) -> Option<Platform> {
    serde_json::from_value(Value::String(raw.trim().to_owned())).ok()
}

/// Friend state changes produced by realtime notifications.
#[derive(Debug, Clone)]
pub enum FriendTransition {
    Added(FriendRecord),
    RequestAccepted(FriendRecord),
    RequestReceived(PendingFriend),
    Removed(FriendRecord),
    RequestCancelled(PendingFriend),
    RequestDeclined(PendingFriend),
    Blocked(BlockedUser),
    Unblocked(AccountId),
}

/// Friend, request, blocklist, and presence state.
pub struct FriendManager {
    self_id: AccountId,
    http: HTTP,
    users: Arc<UserManager>,
    config: CacheConfig,
    friends: DashMap<AccountId, FriendRecord>,
    incoming: DashMap<AccountId, PendingFriend>,
    outgoing: DashMap<AccountId, PendingFriend>,
    blocked: DashMap<AccountId, BlockedUser>,
    presences: DashMap<AccountId, VecDeque<FriendPresence>>,
}

impl FriendManager {
    pub fn new(
        self_id: AccountId,
        http: HTTP,
        users: Arc<UserManager>,
        config: CacheConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            self_id,
            http,
            users,
            config,
            friends: DashMap::new(),
            incoming: DashMap::new(),
            outgoing: DashMap::new(),
            blocked: DashMap::new(),
            presences: DashMap::new(),
        })
    }

    /// Loads friends, requests, and blocked users with their display names.
    pub async fn sync_summary(&self) -> Result<FriendSummarySnapshot, RustfnError> {
        let summary: FriendsSummaryResponse = self.http.fetch_friends_summary(&self.self_id).await?;

        let mut all_ids = Vec::with_capacity(
            summary.friends.len()
                + summary.incoming.len()
                + summary.outgoing.len()
                + summary.blocklist.len(),
        );
        let mut seen = HashSet::new();
        for id in summary
            .friends
            .iter()
            .map(|e| &e.account_id)
            .chain(summary.incoming.iter().map(|e| &e.account_id))
            .chain(summary.outgoing.iter().map(|e| &e.account_id))
            .chain(summary.blocklist.iter().map(|e| &e.account_id))
        {
            if seen.insert(id.clone()) {
                all_ids.push(id.clone());
            }
        }

        let profiles = self.users.fetch_multiple(&all_ids).await?;
        let profile_map: std::collections::HashMap<AccountId, Option<String>> = profiles
            .into_iter()
            .map(|p| (p.id, p.display_name))
            .collect();

        self.friends.clear();
        self.incoming.clear();
        self.outgoing.clear();
        self.blocked.clear();

        let mut snapshot = FriendSummarySnapshot::default();
        let dn = |id: &AccountId| profile_map.get(id).cloned().flatten();

        for item in summary.friends {
            let rec = FriendRecord {
                id: item.account_id.clone(),
                display_name: dn(&item.account_id),
                alias: item.alias.filter(|a| !a.is_empty()),
                note: item.note.filter(|n| !n.is_empty()),
                mutual_count: item.mutual,
                favorite: item.favorite.unwrap_or(false),
                created_at: item.created,
                groups: item.groups,
            };
            self.friends.insert(item.account_id, rec.clone());
            snapshot.friends.push(rec);
        }

        for item in summary.incoming {
            let pending = PendingFriend {
                id: item.account_id.clone(),
                display_name: dn(&item.account_id),
                direction: FriendDirection::Inbound,
                mutual_count: item.mutual,
                favorite: item.favorite.unwrap_or(false),
                created_at: item.created,
            };
            self.incoming.insert(item.account_id, pending.clone());
            snapshot.incoming.push(pending);
        }

        for item in summary.outgoing {
            let pending = PendingFriend {
                id: item.account_id.clone(),
                display_name: dn(&item.account_id),
                direction: FriendDirection::Outbound,
                mutual_count: item.mutual,
                favorite: item.favorite.unwrap_or(false),
                created_at: item.created,
            };
            self.outgoing.insert(item.account_id, pending.clone());
            snapshot.outgoing.push(pending);
        }

        for item in summary.blocklist {
            let blocked = BlockedUser {
                id: item.account_id.clone(),
                display_name: dn(&item.account_id),
                created_at: item.created,
            };
            self.blocked.insert(item.account_id, blocked.clone());
            snapshot.blocked.push(blocked);
        }

        Ok(snapshot)
    }

    pub fn get(&self, account_id: &AccountId) -> Option<FriendRecord> {
        self.friends.get(account_id).map(|r| r.value().clone())
    }

    pub fn find_by_display_name(&self, display_name: &str) -> Option<FriendRecord> {
        self.friends
            .iter()
            .find(|r| {
                r.value()
                    .display_name
                    .as_deref()
                    .is_some_and(|dn| dn.eq_ignore_ascii_case(display_name))
            })
            .map(|r| r.value().clone())
    }

    pub fn list(&self) -> Vec<FriendRecord> {
        self.friends.iter().map(|r| r.value().clone()).collect()
    }

    pub fn incoming_requests(&self) -> Vec<PendingFriend> {
        self.incoming.iter().map(|r| r.value().clone()).collect()
    }

    pub fn outgoing_requests(&self) -> Vec<PendingFriend> {
        self.outgoing.iter().map(|r| r.value().clone()).collect()
    }

    pub fn blocked_users(&self) -> Vec<BlockedUser> {
        self.blocked.iter().map(|r| r.value().clone()).collect()
    }

    pub fn last_presence(&self, account_id: &AccountId) -> Option<FriendPresence> {
        self.presences
            .get(account_id)
            .and_then(|q| q.back().cloned())
    }

    pub fn presence_history(&self, account_id: &AccountId) -> Vec<FriendPresence> {
        self.presences
            .get(account_id)
            .map(|q| q.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Resolves a friend ID locally or via `UserManager` if it is a display name.
    pub async fn resolve_friend_id(&self, id_or_name: &str) -> Result<AccountId, RustfnError> {
        if let Ok(id) = AccountId::parse(id_or_name) {
            return Ok(id);
        }
        if let Some(friend) = self.find_by_display_name(id_or_name) {
            return Ok(friend.id);
        }
        self.users.resolve_id(id_or_name).await
    }

    pub async fn add(&self, id_or_name: &str) -> Result<(), RustfnError> {
        let target_id = self.resolve_friend_id(id_or_name).await?;
        self.http
            .add_friend(&self.self_id, &target_id)
            .await
            .map_err(|err| map_friend_api_error(err, &target_id))
    }

    pub async fn remove(&self, id_or_name: &str) -> Result<(), RustfnError> {
        let target_id = self.resolve_friend_id(id_or_name).await?;
        self.http
            .remove_friend(&self.self_id, &target_id)
            .await
            .map_err(|err| map_friend_api_error(err, &target_id))
    }

    pub async fn block(&self, id_or_name: &str) -> Result<(), RustfnError> {
        let target_id = self.resolve_friend_id(id_or_name).await?;
        self.http.block_user(&self.self_id, &target_id).await
    }

    pub async fn unblock(&self, id_or_name: &str) -> Result<(), RustfnError> {
        let target_id = self.resolve_friend_id(id_or_name).await?;
        self.http.unblock_user(&self.self_id, &target_id).await
    }

    pub async fn set_alias(
        &self,
        target_id: &AccountId,
        alias: &str,
    ) -> Result<(), RustfnError> {
        self.http
            .set_friend_alias(&self.self_id, target_id, alias)
            .await?;
        if let Some(mut entry) = self.friends.get_mut(target_id) {
            entry.alias = (!alias.is_empty()).then(|| alias.to_owned());
        }
        Ok(())
    }

    pub async fn remove_alias(&self, target_id: &AccountId) -> Result<(), RustfnError> {
        self.http
            .remove_friend_alias(&self.self_id, target_id)
            .await?;
        if let Some(mut entry) = self.friends.get_mut(target_id) {
            entry.alias = None;
        }
        Ok(())
    }

    pub async fn set_note(&self, target_id: &AccountId, note: &str) -> Result<(), RustfnError> {
        self.http
            .set_friend_note(&self.self_id, target_id, note)
            .await?;
        if let Some(mut entry) = self.friends.get_mut(target_id) {
            entry.note = (!note.is_empty()).then(|| note.to_owned());
        }
        Ok(())
    }

    pub async fn remove_note(&self, target_id: &AccountId) -> Result<(), RustfnError> {
        self.http
            .remove_friend_note(&self.self_id, target_id)
            .await?;
        if let Some(mut entry) = self.friends.get_mut(target_id) {
            entry.note = None;
        }
        Ok(())
    }

    pub async fn get_mutual(&self, target_id: &AccountId) -> Result<Vec<AccountId>, RustfnError> {
        self.http
            .fetch_mutual_friends(&self.self_id, target_id)
            .await
    }

    /// Applies an incoming XMPP `com.epicgames.friends.core.apiobjects.Friend` notification.
    pub async fn on_friend_notification(
        &self,
        notif: &XMPPFriendNotification,
    ) -> Option<FriendTransition> {
        let display_name = self.resolve_cached_display_name(&notif.account_id).await;
        match notif.status.as_str() {
            "ACCEPTED" => {
                let was_outgoing = self.outgoing.remove(&notif.account_id).is_some();
                self.incoming.remove(&notif.account_id);
                let rec = FriendRecord {
                    id: notif.account_id.clone(),
                    display_name,
                    alias: None,
                    note: None,
                    mutual_count: None,
                    favorite: notif.favorite,
                    created_at: notif.created.clone(),
                    groups: Vec::new(),
                };
                self.friends.insert(notif.account_id.clone(), rec.clone());
                if was_outgoing {
                    Some(FriendTransition::RequestAccepted(rec))
                } else {
                    Some(FriendTransition::Added(rec))
                }
            }
            "PENDING" => {
                let direction = match notif.direction.as_deref() {
                    Some("OUTBOUND") => FriendDirection::Outbound,
                    _ => FriendDirection::Inbound,
                };
                let pending = PendingFriend {
                    id: notif.account_id.clone(),
                    display_name,
                    direction,
                    mutual_count: None,
                    favorite: notif.favorite,
                    created_at: notif.created.clone(),
                };
                match direction {
                    FriendDirection::Inbound => {
                        self.incoming.insert(notif.account_id.clone(), pending.clone());
                        Some(FriendTransition::RequestReceived(pending))
                    }
                    FriendDirection::Outbound => {
                        self.outgoing.insert(notif.account_id.clone(), pending);
                        None
                    }
                }
            }
            "DELETED" => {
                if let Some((_, removed)) = self.friends.remove(&notif.account_id) {
                    return Some(FriendTransition::Removed(removed));
                }
                if let Some((_, pending)) = self.incoming.remove(&notif.account_id) {
                    return Some(FriendTransition::RequestCancelled(pending));
                }
                if let Some((_, pending)) = self.outgoing.remove(&notif.account_id) {
                    return Some(FriendTransition::RequestDeclined(pending));
                }
                None
            }
            _ => None,
        }
    }

    /// Applies an incoming XMPP `FRIENDSHIP_REMOVE` notification.
    pub fn on_friendship_remove(
        &self,
        notif: &XMPPFriendshipRemoveNotification,
    ) -> Option<FriendTransition> {
        let target_id = if notif.from == self.self_id {
            &notif.to
        } else {
            &notif.from
        };

        if notif.reason == "ABORTED"
            && let Some((_, pending)) = self.incoming.remove(target_id)
        {
            return Some(FriendTransition::RequestCancelled(pending));
        }
        if notif.reason == "REJECTED"
            && let Some((_, pending)) = self.outgoing.remove(target_id)
        {
            return Some(FriendTransition::RequestDeclined(pending));
        }
        if let Some((_, friend)) = self.friends.remove(target_id) {
            return Some(FriendTransition::Removed(friend));
        }
        None
    }

    pub async fn on_blocklist_update(
        &self,
        notif: &XMPPUserBlocklistUpdate,
    ) -> Option<FriendTransition> {
        match notif.status.as_str() {
            "BLOCKED" => Some(self.apply_block(&notif.account_id, true).await),
            "UNBLOCKED" => Some(self.apply_block(&notif.account_id, false).await),
            _ => None,
        }
    }

    pub async fn on_legacy_blocklist_entry(
        &self,
        added: bool,
        payload: &XMPPBlockListEntryPayload,
    ) -> FriendTransition {
        self.apply_block(&payload.account_id, added).await
    }

    async fn apply_block(&self, account_id: &AccountId, blocked: bool) -> FriendTransition {
        if !blocked {
            self.blocked.remove(account_id);
            return FriendTransition::Unblocked(account_id.clone());
        }
        self.friends.remove(account_id);
        self.incoming.remove(account_id);
        self.outgoing.remove(account_id);
        let display_name = self.resolve_cached_display_name(account_id).await;
        let entry = BlockedUser {
            id: account_id.clone(),
            display_name,
            created_at: None,
        };
        self.blocked.insert(account_id.clone(), entry.clone());
        FriendTransition::Blocked(entry)
    }

    pub fn record_presence(&self, presence: FriendPresence) {
        let cap = self.config.presence_max_history.max(1);
        let mut queue = self
            .presences
            .entry(presence.account_id.clone())
            .or_default();
        if queue.len() >= cap {
            queue.pop_front();
        }
        queue.push_back(presence);
    }

    async fn resolve_cached_display_name(&self, account_id: &AccountId) -> Option<String> {
        if let Some(name) = self.users.get_cached(account_id).and_then(|user| user.display_name) {
            return Some(name);
        }
        self.users
            .fetch(account_id)
            .await
            .ok()
            .and_then(|u| u.display_name)
    }
}

fn map_friend_api_error(err: RustfnError, target_id: &AccountId) -> RustfnError {
    if let RustfnError::EpicApi { ref body, .. } = err {
        let code = body.effective_code();
        if code == ERR_FRIEND_NOT_FOUND || code == ERR_ACCOUNT_NOT_FOUND {
            return RustfnError::Protocol(format!("friend or account not found: {target_id}"));
        }
    }
    err
}
