use crate::protocol::enums::PartyJoinability;
use crate::protocol::endpoints::{AccountId, DeploymentId, PartyId};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, de::DeserializeOwned};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use thiserror::Error;

/// Canonical Fortnite party and member metadata keys present on Epic Party v1 payloads.
pub mod keys {
    // Member metadata keys
    pub const ATHENA_COSMETIC_LOADOUT_J: &str = "Default:AthenaCosmeticLoadout_j";
    pub const MP_LOADOUT_1_J: &str = "Default:MpLoadout1_j";
    pub const MP_LOADOUT_2_J: &str = "Default:MpLoadout2_j";
    pub const LOADOUT_META_J: &str = "Default:LoadoutMeta_j";
    pub const FRONTEND_EMOTE_J: &str = "Default:FrontendEmote_j";
    pub const FRONTEND_SPARKS_SONG_PART_J: &str = "Default:FrontendSparksSongPart_j";
    pub const BATTLE_PASS_INFO_J: &str = "Default:BattlePassInfo_j";
    pub const FORT_STATS_J: &str = "Default:FORTStats_j";
    pub const CAMPAIGN_HERO_J: &str = "Default:CampaignHero_j";
    pub const LOBBY_STATE_J: &str = "Default:LobbyState_j";
    pub const PLATFORM_DATA_J: &str = "Default:PlatformData_j";
    pub const ATHENA_BANNER_INFO_J: &str = "Default:AthenaBannerInfo_j";
    pub const SPECTATE_INFO_J: &str = "Default:SpectateInfo_j";
    pub const MEMBER_SQUAD_ASSIGNMENT_REQUEST_J: &str = "Default:MemberSquadAssignmentRequest_j";
    pub const JOIN_IN_PROGRESS_DATA_J: &str = "Default:JoinInProgressData_j";
    pub const MATCHMAKING_INFO_J: &str = "Default:MatchmakingInfo_j";
    pub const PACKED_STATE_J: &str = "Default:PackedState_j";

    // Party metadata keys
    pub const PRIVACY_SETTINGS_J: &str = "Default:PrivacySettings_j";
    pub const PARTY_BUILD_ID_S: &str = "Default:PartyBuildId_s";
    pub const REGION_ID_S: &str = "Default:RegionId_s";
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MetaCodecError {
    #[error("metadata key `{key}` has incompatible suffix `{actual:?}` for expected type `{expected}`")]
    SuffixMismatch {
        key: String,
        actual: MetaSuffix,
        expected: &'static str,
    },

    #[error("failed to parse boolean metadata value for `{key}`: `{raw}`")]
    InvalidBool { key: String, raw: String },

    #[error("failed to parse integer metadata value for `{key}`: `{raw}`")]
    InvalidInt { key: String, raw: String },

    #[error("failed to parse float metadata value for `{key}`: `{raw}`")]
    InvalidFloat { key: String, raw: String },

    #[error("JSON metadata codec error for `{key}`: {source_message}")]
    Json { key: String, source_message: String },
}

/// Epic Party v1 metadata key type suffix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetaSuffix {
    Json,
    Bool,
    UnsignedInt,
    SignedInt,
    Double,
    String,
    Untyped,
}

impl MetaSuffix {
    /// Determines the wire encoding rule from a metadata key's suffix (`_j`, `_b`, `_U`, `_i`, `_d`, `_s`).
    pub fn from_key(key: &str) -> Self {
        if key.ends_with("_j") {
            Self::Json
        } else if key.ends_with("_b") {
            Self::Bool
        } else if key.ends_with("_U") {
            Self::UnsignedInt
        } else if key.ends_with("_i") {
            Self::SignedInt
        } else if key.ends_with("_d") {
            Self::Double
        } else if key.ends_with("_s") {
            Self::String
        } else {
            Self::Untyped
        }
    }
}

/// Strongly-typed metadata scalar or JSON value compatible with Epic's suffix-encoded map.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetaTypedValue {
    Bool(bool),
    Unsigned(u64),
    Signed(i64),
    Float(f64),
    Json(serde_json::Value),
    Text(String),
}

/// Delta payload emitted to `PATCH /party/api/v1/Fortnite/parties/{id}` or member meta updates.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MetaPatch {
    pub updated: BTreeMap<String, String>,
    pub deleted: Vec<String>,
}

impl MetaPatch {
    pub fn is_empty(&self) -> bool {
        self.updated.is_empty() && self.deleted.is_empty()
    }
}

/// Ordered string-to-string key/value store implementing Epic Games' suffix-encoded party metadata rules.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MetaMap(BTreeMap<String, String>);

impl MetaMap {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn from_map(map: BTreeMap<String, String>) -> Self {
        Self(map)
    }

    pub fn into_map(self) -> BTreeMap<String, String> {
        self.0
    }

    pub fn as_map(&self) -> &BTreeMap<String, String> {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    pub fn get_raw(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    pub fn set_raw(&mut self, key: impl Into<String>, value: impl Into<String>) -> Option<String> {
        self.0.insert(key.into(), value.into())
    }

    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.0.remove(key)
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.get_raw(key)
    }

    pub fn set_str(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.0.insert(key.into(), value.into());
    }

    pub fn get_bool(&self, key: &str) -> Result<Option<bool>, MetaCodecError> {
        let Some(raw) = self.get_raw(key) else {
            return Ok(None);
        };
        if raw.eq_ignore_ascii_case("true") {
            Ok(Some(true))
        } else if raw.eq_ignore_ascii_case("false") {
            Ok(Some(false))
        } else {
            Err(MetaCodecError::InvalidBool {
                key: key.to_owned(),
                raw: raw.to_owned(),
            })
        }
    }

    pub fn set_bool(&mut self, key: impl Into<String>, value: bool) {
        self.0
            .insert(key.into(), if value { "true" } else { "false" }.to_owned());
    }

    pub fn get_u64(&self, key: &str) -> Result<Option<u64>, MetaCodecError> {
        let Some(raw) = self.get_raw(key) else {
            return Ok(None);
        };
        raw.parse::<u64>()
            .map(Some)
            .map_err(|_| MetaCodecError::InvalidInt {
                key: key.to_owned(),
                raw: raw.to_owned(),
            })
    }

    pub fn set_u64(&mut self, key: impl Into<String>, value: u64) {
        self.0.insert(key.into(), value.to_string());
    }

    pub fn get_i64(&self, key: &str) -> Result<Option<i64>, MetaCodecError> {
        let Some(raw) = self.get_raw(key) else {
            return Ok(None);
        };
        raw.parse::<i64>()
            .map(Some)
            .map_err(|_| MetaCodecError::InvalidInt {
                key: key.to_owned(),
                raw: raw.to_owned(),
            })
    }

    pub fn set_i64(&mut self, key: impl Into<String>, value: i64) {
        self.0.insert(key.into(), value.to_string());
    }

    pub fn get_f64(&self, key: &str) -> Result<Option<f64>, MetaCodecError> {
        let Some(raw) = self.get_raw(key) else {
            return Ok(None);
        };
        raw.parse::<f64>()
            .map(Some)
            .map_err(|_| MetaCodecError::InvalidFloat {
                key: key.to_owned(),
                raw: raw.to_owned(),
            })
    }

    /// Formats a `_d` floating-point entry using Epic's 6-decimal fixed wire format.
    pub fn set_f64(&mut self, key: impl Into<String>, value: f64) {
        self.0.insert(key.into(), format!("{value:.6}"));
    }

    pub fn get_json<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, MetaCodecError> {
        let Some(raw) = self.get_raw(key) else {
            return Ok(None);
        };
        serde_json::from_str(raw)
            .map(Some)
            .map_err(|err| MetaCodecError::Json {
                key: key.to_owned(),
                source_message: err.to_string(),
            })
    }

    pub fn set_json<T: Serialize>(
        &mut self,
        key: impl Into<String>,
        value: &T,
    ) -> Result<(), MetaCodecError> {
        let key = key.into();
        let encoded = serde_json::to_string(value).map_err(|err| MetaCodecError::Json {
            key: key.clone(),
            source_message: err.to_string(),
        })?;
        self.0.insert(key, encoded);
        Ok(())
    }

    /// Encodes a value according to `key`'s Epic metadata suffix (`_j`, `_b`, `_U`, `_i`, `_d`, `_s`).
    pub fn set_typed(
        &mut self,
        key: impl Into<String>,
        value: MetaTypedValue,
    ) -> Result<(), MetaCodecError> {
        let key = key.into();
        let suffix = MetaSuffix::from_key(&key);
        let encoded = match (suffix, value) {
            (MetaSuffix::Json, MetaTypedValue::Json(v)) => {
                serde_json::to_string(&v).map_err(|err| MetaCodecError::Json {
                    key: key.clone(),
                    source_message: err.to_string(),
                })?
            }
            (MetaSuffix::Json, MetaTypedValue::Text(s)) => s,
            (MetaSuffix::Bool, MetaTypedValue::Bool(b)) => {
                if b { "true" } else { "false" }.to_owned()
            }
            (MetaSuffix::UnsignedInt, MetaTypedValue::Unsigned(u)) => u.to_string(),
            (MetaSuffix::UnsignedInt, MetaTypedValue::Signed(i)) if i >= 0 => i.to_string(),
            (MetaSuffix::SignedInt, MetaTypedValue::Signed(i)) => i.to_string(),
            (MetaSuffix::SignedInt, MetaTypedValue::Unsigned(u)) => u.to_string(),
            (MetaSuffix::Double, MetaTypedValue::Float(d)) => format!("{d:.6}"),
            (MetaSuffix::Double, MetaTypedValue::Signed(i)) => format!("{}.000000", i),
            (MetaSuffix::Double, MetaTypedValue::Unsigned(u)) => format!("{u}.000000"),
            (MetaSuffix::String | MetaSuffix::Untyped, MetaTypedValue::Text(s)) => s,
            (MetaSuffix::String | MetaSuffix::Untyped, MetaTypedValue::Bool(b)) => b.to_string(),
            (MetaSuffix::String | MetaSuffix::Untyped, MetaTypedValue::Unsigned(u)) => {
                u.to_string()
            }
            (MetaSuffix::String | MetaSuffix::Untyped, MetaTypedValue::Signed(i)) => i.to_string(),
            (MetaSuffix::String | MetaSuffix::Untyped, MetaTypedValue::Float(d)) => {
                format!("{d:.6}")
            }
            (MetaSuffix::String | MetaSuffix::Untyped, MetaTypedValue::Json(v)) => {
                serde_json::to_string(&v).map_err(|err| MetaCodecError::Json {
                    key: key.clone(),
                    source_message: err.to_string(),
                })?
            }
            (actual, _) => {
                return Err(MetaCodecError::SuffixMismatch {
                    key,
                    actual,
                    expected: "compatible MetaTypedValue",
                });
            }
        };
        self.0.insert(key, encoded);
        Ok(())
    }

    /// Applies an incoming `updated` + `deleted` delta in place.
    pub fn apply_patch(&mut self, patch: &MetaPatch) {
        for key in &patch.deleted {
            self.0.remove(key);
        }
        for (key, value) in &patch.updated {
            self.0.insert(key.clone(), value.clone());
        }
    }

    /// Computes the minimal [`MetaPatch`] needed to transform `self` into `next`.
    pub fn diff_to(&self, next: &Self) -> MetaPatch {
        let mut updated = BTreeMap::new();
        let mut deleted = Vec::new();

        for key in self.0.keys() {
            if !next.0.contains_key(key) {
                deleted.push(key.clone());
            }
        }
        for (key, next_val) in &next.0 {
            if self.0.get(key) != Some(next_val) {
                updated.insert(key.clone(), next_val.clone());
            }
        }

        MetaPatch { updated, deleted }
    }
}

impl From<BTreeMap<String, String>> for MetaMap {
    fn from(value: BTreeMap<String, String>) -> Self {
        Self(value)
    }
}

impl From<MetaMap> for BTreeMap<String, String> {
    fn from(value: MetaMap) -> Self {
        value.0
    }
}

/// Typed value carried in Epic Games / Fortnite presence `Properties` maps.
///
/// Wire encoding:
/// - `btrue` / `bfalse` -> [`PresencePropValue::Bool`]
/// - `i<integer>` -> [`PresencePropValue::Int`]
/// - `m<json>` -> [`PresencePropValue::Json`]
/// - `s<text>` (or untagged text fallback) -> [`PresencePropValue::Str`]
#[derive(Debug, Clone, PartialEq)]
pub enum PresencePropValue {
    Bool(bool),
    Int(i64),
    Json(serde_json::Value),
    Str(String),
    RawStr(String),
}

impl PresencePropValue {
    /// Encodes this value into Epic's presence property wire string (`b`/`i`/`m`/`s` prefixed or raw EOS metadata).
    pub fn encode(&self) -> String {
        match self {
            Self::Bool(true) => "btrue".to_owned(),
            Self::Bool(false) => "bfalse".to_owned(),
            Self::Int(n) => format!("i{n}"),
            Self::Json(val) => format!("m{val}"),
            Self::Str(s) => format!("s{s}"),
            Self::RawStr(s) => s.clone(),
        }
    }

    /// Decodes a prefix-tagged presence property string, falling back to [`Self::Str`]
    /// for untagged strings or malformed prefixed payloads.
    pub fn decode(raw: &str) -> Self {
        if raw == "btrue" {
            return Self::Bool(true);
        }
        if raw == "bfalse" {
            return Self::Bool(false);
        }
        if let Some(rest) = raw.strip_prefix('i') {
            if let Ok(parsed) = rest.parse::<i64>() {
                return Self::Int(parsed);
            }
        }
        if let Some(rest) = raw.strip_prefix('m') {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(rest) {
                return Self::Json(parsed);
            }
        }
        if let Some(rest) = raw.strip_prefix('s') {
            return Self::Str(rest.to_owned());
        }
        Self::Str(raw.to_owned())
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(s) | Self::RawStr(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Json(val) => Some(val),
            _ => None,
        }
    }
}

impl Serialize for PresencePropValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.encode())
    }
}

impl<'de> Deserialize<'de> for PresencePropValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let val = serde_json::Value::deserialize(deserializer)?;
        match val {
            serde_json::Value::String(s) => Ok(Self::decode(&s)),
            serde_json::Value::Bool(b) => Ok(Self::Bool(b)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Self::Int(i))
                } else {
                    Ok(Self::Json(serde_json::Value::Number(n)))
                }
            }
            serde_json::Value::Object(_) | serde_json::Value::Array(_) => Ok(Self::Json(val)),
            serde_json::Value::Null => Err(de::Error::custom("null is not a valid presence prop")),
        }
    }
}

impl fmt::Display for PresencePropValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.encode())
    }
}

/// Ordered presence property map keyed by property identifier (for example `party.joininfodata.286331153_j`).
pub type PresencePropsMap = BTreeMap<String, PresencePropValue>;

/// Standard XMPP presence status JSON document emitted and consumed by Fortnite clients.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct XMPPPresenceStatusPayload {
    #[serde(rename = "Status", default)]
    pub status: String,
    #[serde(rename = "bIsPlaying", default)]
    pub is_playing: bool,
    #[serde(rename = "bIsJoinable", default)]
    pub is_joinable: bool,
    #[serde(rename = "bHasVoiceSupport", default)]
    pub has_voice_support: bool,
    #[serde(rename = "SessionId", default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(rename = "ProductName", default, skip_serializing_if = "Option::is_none")]
    pub product_name: Option<String>,
    #[serde(rename = "Properties", default)]
    pub properties: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EOSPartyDataConfig {
    pub joinability: PartyJoinability,
    pub max_size: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EOSPartyConnectionData {
    pub id: String,
    pub deployment_id: DeploymentId,
    #[serde(default)]
    pub connected_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub is_first_party: bool,
    #[serde(default)]
    pub meta: BTreeMap<String, Value>,
    #[serde(default)]
    pub active_in_party_voice_room: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EOSPartyMemberData {
    pub account_id: AccountId,
    #[serde(default)]
    pub meta: MetaMap,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub joined_at: Option<String>,
    #[serde(default)]
    pub suspended: bool,
    #[serde(default)]
    pub connections: Vec<EOSPartyConnectionData>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EOSPartyData {
    pub id: PartyId,
    pub party_lead: AccountId,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    pub config: EOSPartyDataConfig,
    #[serde(default)]
    pub meta: MetaMap,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub chat_conversation_id: String,
    #[serde(default)]
    pub is_reportable: bool,
    #[serde(default)]
    pub members: Vec<EOSPartyMemberData>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EOSPartyUpdateData {
    #[serde(default)]
    pub sent: Option<String>,
    pub party_id: PartyId,
    pub party_lead: AccountId,
    #[serde(default)]
    pub meta: MetaMap,
    pub party_privacy_type: PartyJoinability,
    pub max_number_of_members: u32,
    #[serde(default)]
    pub chat_conversation_id: String,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub is_reportable: bool,
    #[serde(default)]
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EOSPartyInviteData {
    pub party_id: PartyId,
    pub inviter_id: AccountId,
    #[serde(default)]
    pub inviter_dn: Option<String>,
    #[serde(default)]
    pub invitee_id: Option<AccountId>,
    #[serde(default)]
    pub sent_at: Option<String>,
    #[serde(default)]
    pub sent: Option<String>,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub auto: bool,
    #[serde(default)]
    pub platform: Option<i64>,
    #[serde(default)]
    pub meta: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EOSPartyJoinRequestData {
    #[serde(default)]
    pub party_id: Option<PartyId>,
    #[serde(default)]
    pub requester_id: Option<AccountId>,
    #[serde(default)]
    pub sent_by: Option<AccountId>,
    #[serde(default)]
    pub inviter_id: Option<AccountId>,
    #[serde(default)]
    pub account_id: Option<AccountId>,
    #[serde(default)]
    pub sent_at: Option<String>,
    #[serde(default)]
    pub sent: Option<String>,
    #[serde(default)]
    pub expires_at: Option<String>,
}

impl EOSPartyJoinRequestData {
    pub fn resolved_requester(&self) -> Option<&AccountId> {
        self.requester_id
            .as_ref()
            .or(self.sent_by.as_ref())
            .or(self.inviter_id.as_ref())
            .or(self.account_id.as_ref())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EOSPartyMemberUpdateData {
    pub party_id: PartyId,
    pub account_id: AccountId,
    #[serde(default)]
    pub account_dn: Option<String>,
    #[serde(default)]
    pub sent: Option<String>,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub joined_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub suspended: Option<bool>,
    #[serde(default)]
    pub member_state_overridden: MetaMap,
    #[serde(default)]
    pub connection: Option<Value>,
    #[serde(default)]
    pub member_state_updated: MetaMap,
    #[serde(default)]
    pub member_state_removed: Vec<String>,
}

/// Deserializes `EOSPartyUserState::current` from `null`, a single party map, or a sequence of parties.
pub fn deserialize_eos_current_party<'de, D>(
    deserializer: D,
) -> Result<Option<EOSPartyData>, D::Error>
where
    D: Deserializer<'de>,
{
    struct EOSCurrentPartyVisitor;

    impl<'de> de::Visitor<'de> for EOSCurrentPartyVisitor {
        type Value = Option<EOSPartyData>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("null, an EOS party object, or a list of EOS party objects")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }

        fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
        where
            A: de::MapAccess<'de>,
        {
            EOSPartyData::deserialize(de::value::MapAccessDeserializer::new(map)).map(Some)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let first = seq.next_element::<EOSPartyData>()?;
            while seq.next_element::<de::IgnoredAny>()?.is_some() {}
            Ok(first)
        }
    }

    deserializer.deserialize_any(EOSCurrentPartyVisitor)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EOSPartyUserState {
    #[serde(default, deserialize_with = "deserialize_eos_current_party")]
    pub current: Option<EOSPartyData>,
    #[serde(default)]
    pub invites: Vec<EOSPartyInviteData>,
    #[serde(default)]
    pub join_requests: Vec<EOSPartyJoinRequestData>,
}
