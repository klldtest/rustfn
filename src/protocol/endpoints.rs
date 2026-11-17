use crate::protocol::enums::{
    FortnitePartyPrivacy, PartyDiscoverability, PartyJoinability, PartyPrivacyPreset,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use zeroize::Zeroizing;

/// Default Epic Account Services deployment identifier used by Fortnite.
pub const EOS_EAS_DEPLOYMENT_ID: &str = "62a9473a2dca46b29ccf17577fcf42d7";

/// Fortnite storefront catalog item ID used by Launcher Assets V1/V2 build lookups.
pub const FORTNITE_CATALOG_ITEM_ID: &str = "4fe75bbc5a674f4f9b356b5c90567da5";

/// Default Epic Launcher/Storefront/Social URLs not stored in `ServiceEndpoints`.
pub mod extra_urls {
    pub const ACCOUNT_AVATAR: &str =
        "https://avatar-service-prod.identity.live.on.epicgames.com/v1/avatar";
    pub const ACCOUNT_GLOBAL_PROFILE: &str =
        "https://global-profile-service.game-social.epicgames.com/profiles";
    pub const BR_MATCHMAKING_REQUEST: &str =
        "https://fortnite-public-service-prod11.ol.epicgames.com/fortnite/api/matchmaking/session/matchMakingRequest";
    pub const BR_SERVER_STATUS: &str =
        "https://lightswitch-public-service-prod06.ol.epicgames.com/lightswitch/api/service/bulk/status?serviceId=Fortnite";
    pub const BR_SAC: &str =
        "https://affiliate-public-service-prod.ol.epicgames.com/affiliate/api/public/affiliates/slug";
    pub const EOS_PRESENCE_INTERNAL: &str =
        "https://api.epicgames.dev/epic/presence/internal/v1/_";
}

/// Additional Epic service base URLs configurable for testing and custom environments.
#[derive(Debug, Clone)]
pub struct ExtraServiceEndpoints {
    pub account_avatar: String,
    pub account_global_profile: String,
    pub br_matchmaking_request: String,
    pub br_server_status: String,
    pub br_sac: String,
    pub eos_presence_internal: String,
}

impl Default for ExtraServiceEndpoints {
    fn default() -> Self {
        Self {
            account_avatar: extra_urls::ACCOUNT_AVATAR.to_owned(),
            account_global_profile: extra_urls::ACCOUNT_GLOBAL_PROFILE.to_owned(),
            br_matchmaking_request: extra_urls::BR_MATCHMAKING_REQUEST.to_owned(),
            br_server_status: extra_urls::BR_SERVER_STATUS.to_owned(),
            br_sac: extra_urls::BR_SAC.to_owned(),
            eos_presence_internal: extra_urls::EOS_PRESENCE_INTERNAL.to_owned(),
        }
    }
}

/// Priority targets ordered by client preference when recalculating party squad positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SquadPositionPriority {
    SelfFirst,
    LeaderFirst,
}

/// Cache retention settings for users and friend presence history.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub user_ttl: Duration,
    pub max_users: usize,
    pub presence_max_history: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            user_ttl: Duration::from_secs(30 * 60),
            max_users: 4096,
            presence_max_history: 20,
        }
    }
}

/// Party defaults, metadata chunking limits, and keep-alive timing.
#[derive(Debug, Clone)]
pub struct PartyConfig {
    pub privacy: FortnitePartyPrivacy,
    pub discoverability: PartyDiscoverability,
    pub joinability: PartyJoinability,
    pub max_size: u32,
    pub invite_ttl_secs: u64,
    pub intention_ttl_secs: u64,
    pub join_confirmation: bool,
    pub chat_enabled: bool,
    pub patch_max_keys: usize,
    pub member_metadata_ttl: Duration,
    pub eos_keep_alive_interval: Duration,
    pub squad_position_priorities: Vec<SquadPositionPriority>,
}

impl Default for PartyConfig {
    fn default() -> Self {
        let preset = PartyPrivacyPreset::Public;
        Self {
            privacy: preset.to_privacy(),
            discoverability: preset.discoverability(),
            joinability: preset.joinability(),
            max_size: 16,
            invite_ttl_secs: 3600,
            intention_ttl_secs: 60,
            join_confirmation: false,
            chat_enabled: true,
            patch_max_keys: 32,
            member_metadata_ttl: Duration::from_secs(10),
            eos_keep_alive_interval: Duration::from_secs(60),
            squad_position_priorities: vec![
                SquadPositionPriority::SelfFirst,
                SquadPositionPriority::LeaderFirst,
            ],
        }
    }
}

/// Extracts the numeric changelist (`CL`) token from a launcher `buildVersion` string.
pub fn parse_build_version_cl(build_version: &str) -> Option<&str> {
    let (_, after_cl) = build_version.split_once("-CL-")?;
    let digits_len = after_cl
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    (digits_len > 0).then(|| &after_cl[..digits_len])
}

/// Canonical Epic Games, Fortnite, and EOS service URL constants.
pub mod urls {
    pub const OAUTH_TOKEN_CREATE: &str =
        "https://account-public-service-prod.ol.epicgames.com/account/api/oauth/token";
    pub const OAUTH_TOKEN_VERIFY: &str =
        "https://account-public-service-prod.ol.epicgames.com/account/api/oauth/verify";
    pub const OAUTH_TOKEN_KILL: &str =
        "https://account-public-service-prod.ol.epicgames.com/account/api/oauth/sessions/kill";
    pub const OAUTH_EXCHANGE: &str =
        "https://account-public-service-prod.ol.epicgames.com/account/api/oauth/exchange";
    pub const OAUTH_DEVICE_CODE: &str =
        "https://account-public-service-prod.ol.epicgames.com/account/api/oauth/deviceAuthorization";
    pub const ACCOUNT_PUBLIC: &str =
        "https://account-public-service-prod.ol.epicgames.com/account/api/public/account";

    pub const INIT_EULA: &str =
        "https://eulatracking-public-service-prod-m.ol.epicgames.com/eulatracking/api/public/agreements/fn";
    pub const INIT_REQUESTACCESS: &str =
        "https://fngw-mcp-gc-livefn.ol.epicgames.com/fortnite/api/storeaccess/v1/request_access";
    pub const LAUNCHER_PUBLIC: &str = "https://launcher-public-service-prod06.ol.epicgames.com";

    pub const XMPP_WSS: &str = "wss://xmpp-service-prod.ol.epicgames.com";
    pub const EPIC_PROD_ENV: &str = "prod.ol.epicgames.com";
    pub const EOS_STOMP_WSS: &str = "wss://connect.epicgames.dev/v2";

    pub const EOS_TOKEN: &str = "https://api.epicgames.dev/epic/oauth/v2/token";
    pub const EOS_TOKEN_INFO: &str = "https://api.epicgames.dev/epic/oauth/v2/tokenInfo";
    pub const EOS_TOKEN_REVOKE: &str = "https://api.epicgames.dev/epic/oauth/v2/revoke";
    pub const EOS_CHAT: &str = "https://api.epicgames.dev/epic/chat";
    pub const EOS_PRESENCE: &str = "https://api.epicgames.dev/epic/presence/v1";
    pub const EOS_PARTY: &str = "https://api.epicgames.dev/epic/party";
    pub const EOS_PARTY_INTERNAL: &str = "https://api.epicgames.dev/epic/party/internal/v2";
    pub const EOS_USERS: &str = "https://api.epicgames.dev/auth/v1/users";

    pub const PUBLICKEY: &str =
        "https://publickey-service-prod.ecbc.live.use1a.on.epicgames.com/publickey";
    pub const PUBLICKEY_FALLBACK: &str = "https://publickey-service-live.ecosec.on.epicgames.com";

    pub const BR_PARTY: &str = "https://party-service-prod.ol.epicgames.com/party/api/v1/Fortnite";
    pub const FRIENDS: &str = "https://friends-public-service-prod.ol.epicgames.com/friends/api/v1";
    pub const FRIENDS_PUBLIC: &str =
        "https://friends-public-service-prod.ol.epicgames.com/friends/api/public/friends";
    pub const FRIENDS_BLOCKLIST_PUBLIC: &str =
        "https://friends-public-service-prod.ol.epicgames.com/friends/api/public/blocklist";
    pub const USER_SEARCH: &str = "https://user-search-service-prod.ol.epicgames.com/api/v1/search";

    pub const MCP: &str =
        "https://fngw-mcp-gc-livefn.ol.epicgames.com/fortnite/api/game/v2/profile";
    pub const GRAPHQL: &str = "https://graphql.epicgames.com/graphql";
    pub const LIGHTSWITCH: &str = "https://lightswitch-public-service-prod06.ol.epicgames.com";
    pub const HABANERO: &str =
        "https://fn-service-habanero-live-public.ogs.live.on.epicgames.com";
}

/// Overridable endpoint catalog used by the HTTP, auth, XMPP, and STOMP modules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceEndpoints {
    pub oauth_token_create: Cow<'static, str>,
    pub oauth_token_verify: Cow<'static, str>,
    pub oauth_token_kill: Cow<'static, str>,
    pub oauth_exchange: Cow<'static, str>,
    pub oauth_device_code: Cow<'static, str>,
    pub account_public: Cow<'static, str>,
    pub init_eula: Cow<'static, str>,
    pub init_request_access: Cow<'static, str>,
    pub launcher_public: Cow<'static, str>,
    pub xmpp_wss: Cow<'static, str>,
    pub epic_prod_env: Cow<'static, str>,
    pub eos_stomp_wss: Cow<'static, str>,
    pub eos_token: Cow<'static, str>,
    pub eos_token_info: Cow<'static, str>,
    pub eos_token_revoke: Cow<'static, str>,
    pub eos_chat: Cow<'static, str>,
    pub eos_presence: Cow<'static, str>,
    pub eos_party: Cow<'static, str>,
    pub eos_party_internal: Cow<'static, str>,
    pub eos_users: Cow<'static, str>,
    pub publickey: Cow<'static, str>,
    pub publickey_fallback: Cow<'static, str>,
    pub br_party: Cow<'static, str>,
    pub friends: Cow<'static, str>,
    pub friends_public: Cow<'static, str>,
    pub friends_blocklist_public: Cow<'static, str>,
    pub user_search: Cow<'static, str>,
    pub mcp: Cow<'static, str>,
    pub graphql: Cow<'static, str>,
    pub lightswitch: Cow<'static, str>,
    pub habanero: Cow<'static, str>,
}

impl Default for ServiceEndpoints {
    fn default() -> Self {
        Self {
            oauth_token_create: Cow::Borrowed(urls::OAUTH_TOKEN_CREATE),
            oauth_token_verify: Cow::Borrowed(urls::OAUTH_TOKEN_VERIFY),
            oauth_token_kill: Cow::Borrowed(urls::OAUTH_TOKEN_KILL),
            oauth_exchange: Cow::Borrowed(urls::OAUTH_EXCHANGE),
            oauth_device_code: Cow::Borrowed(urls::OAUTH_DEVICE_CODE),
            account_public: Cow::Borrowed(urls::ACCOUNT_PUBLIC),
            init_eula: Cow::Borrowed(urls::INIT_EULA),
            init_request_access: Cow::Borrowed(urls::INIT_REQUESTACCESS),
            launcher_public: Cow::Borrowed(urls::LAUNCHER_PUBLIC),
            xmpp_wss: Cow::Borrowed(urls::XMPP_WSS),
            epic_prod_env: Cow::Borrowed(urls::EPIC_PROD_ENV),
            eos_stomp_wss: Cow::Borrowed(urls::EOS_STOMP_WSS),
            eos_token: Cow::Borrowed(urls::EOS_TOKEN),
            eos_token_info: Cow::Borrowed(urls::EOS_TOKEN_INFO),
            eos_token_revoke: Cow::Borrowed(urls::EOS_TOKEN_REVOKE),
            eos_chat: Cow::Borrowed(urls::EOS_CHAT),
            eos_presence: Cow::Borrowed(urls::EOS_PRESENCE),
            eos_party: Cow::Borrowed(urls::EOS_PARTY),
            eos_party_internal: Cow::Borrowed(urls::EOS_PARTY_INTERNAL),
            eos_users: Cow::Borrowed(urls::EOS_USERS),
            publickey: Cow::Borrowed(urls::PUBLICKEY),
            publickey_fallback: Cow::Borrowed(urls::PUBLICKEY_FALLBACK),
            br_party: Cow::Borrowed(urls::BR_PARTY),
            friends: Cow::Borrowed(urls::FRIENDS),
            friends_public: Cow::Borrowed(urls::FRIENDS_PUBLIC),
            friends_blocklist_public: Cow::Borrowed(urls::FRIENDS_BLOCKLIST_PUBLIC),
            user_search: Cow::Borrowed(urls::USER_SEARCH),
            mcp: Cow::Borrowed(urls::MCP),
            graphql: Cow::Borrowed(urls::GRAPHQL),
            lightswitch: Cow::Borrowed(urls::LIGHTSWITCH),
            habanero: Cow::Borrowed(urls::HABANERO),
        }
    }
}

impl ServiceEndpoints {
    /// Returns `{account_public}/{account_id}/deviceAuth`.
    pub fn device_auth_collection(&self, account_id: &str) -> String {
        format!("{}/{}/deviceAuth", self.account_public, account_id)
    }

    /// Returns `{account_public}/{account_id}/deviceAuth/{device_id}`.
    pub fn device_auth_item(&self, account_id: &str, device_id: &str) -> String {
        format!("{}/{}/deviceAuth/{}", self.account_public, account_id, device_id)
    }

    /// Returns `{br_party}/user/{account_id}`.
    pub fn br_party_user(&self, account_id: &str) -> String {
        format!("{}/user/{}", self.br_party, account_id)
    }

    /// Returns `{br_party}/parties/{party_id}`.
    pub fn br_party_by_id(&self, party_id: &str) -> String {
        format!("{}/parties/{}", self.br_party, party_id)
    }

    /// Returns `{br_party}/parties/{party_id}/members/{account_id}`.
    pub fn br_party_member(&self, party_id: &str, account_id: &str) -> String {
        format!("{}/parties/{}/members/{}", self.br_party, party_id, account_id)
    }
}

/// Known Epic Games and Fortnite OAuth2 client descriptors.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AuthClient {
    FortnitePcGameClient,
    FortniteIosGameClient,
    #[default]
    FortniteAndroidGameClient,
    FortniteSwitchGameClient,
    FortniteNewSwitchGameClient,
    FortniteCnGameClient,
    LauncherAppClient2,
    DieselDauntless,
}

impl AuthClient {
    pub const ALL: [Self; 8] = [
        Self::FortnitePcGameClient,
        Self::FortniteIosGameClient,
        Self::FortniteAndroidGameClient,
        Self::FortniteSwitchGameClient,
        Self::FortniteNewSwitchGameClient,
        Self::FortniteCnGameClient,
        Self::LauncherAppClient2,
        Self::DieselDauntless,
    ];

    pub const fn canonical_name(self) -> &'static str {
        match self {
            Self::FortnitePcGameClient => "fortnitePCGameClient",
            Self::FortniteIosGameClient => "fortniteIOSGameClient",
            Self::FortniteAndroidGameClient => "fortniteAndroidGameClient",
            Self::FortniteSwitchGameClient => "fortniteSwitchGameClient",
            Self::FortniteNewSwitchGameClient => "fortniteNewSwitchGameClient",
            Self::FortniteCnGameClient => "fortniteCNGameClient",
            Self::LauncherAppClient2 => "launcherAppClient2",
            Self::DieselDauntless => "Diesel - Dauntless",
        }
    }

    pub const fn client_id(self) -> &'static str {
        match self {
            Self::FortnitePcGameClient => "ec684b8c687f479fadea3cb2ad83f5c6",
            Self::FortniteIosGameClient => "3446cd72694c4a4485d81b77adbb2141",
            Self::FortniteAndroidGameClient => "3f69e56c7649492c8cc29f1af08a8a12",
            Self::FortniteSwitchGameClient => "5229dcd3ac3845208b496649092f251b",
            Self::FortniteNewSwitchGameClient => "98f7e42c2e3a4f86a74eb43fbb41ed39",
            Self::FortniteCnGameClient => "efe3cbb938804c74b20e109d0efc1548",
            Self::LauncherAppClient2 => "34a02cf8f4414e29b15921876da36f9a",
            Self::DieselDauntless => "b070f20729f84693b5d621c904fc5bc2",
        }
    }

    pub const fn secret(self) -> &'static str {
        match self {
            Self::FortnitePcGameClient => "e1f31c211f28413186262d37a13fc84d",
            Self::FortniteIosGameClient => "9209d4a5e25a457fb9b07489d313b41a",
            Self::FortniteAndroidGameClient => "b51ee9cb12234f50a69efa67ef53812e",
            Self::FortniteSwitchGameClient => "e3bd2d3e-bf8c-4857-9e7d-f3d947d220c7",
            Self::FortniteNewSwitchGameClient => "0a2449a2-001a-451e-afec-3e812901c4d7",
            Self::FortniteCnGameClient => "6e31bdbae6a44f74733db74f39ba",
            Self::LauncherAppClient2 => "daafbccc737745039dffe53d94fc76cf",
            Self::DieselDauntless => "HG@XE&TGCxEJsgT#&_p2]=aRo#~>=>+c6PhR)zXP",
        }
    }

    /// Indicates whether Epic permits `device_auth` credential creation/grant for this client.
    pub const fn supports_device_auth(self) -> bool {
        matches!(
            self,
            Self::FortniteIosGameClient
                | Self::FortniteAndroidGameClient
                | Self::FortniteSwitchGameClient
        )
    }

    /// Encodes `Basic <base64(client_id:secret)>` in zeroizing storage.
    pub fn basic_auth_header(self) -> Zeroizing<String> {
        let mut raw = Zeroizing::new(format!("{}:{}", self.client_id(), self.secret()));
        let encoded = STANDARD.encode(raw.as_bytes());
        raw.clear();
        Zeroizing::new(format!("Basic {encoded}"))
    }

    /// Looks up a known client by either its canonical string name or OAuth `client_id`.
    pub fn from_name_or_id(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|client| client.canonical_name() == value || client.client_id() == value)
    }
}

impl fmt::Display for AuthClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.canonical_name())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum IdFormatError {
    #[error("identifier `{value}` is not a valid 32-character hexadecimal Epic ID")]
    InvalidHex32 { value: String },

    #[error("cannot derive Fortnite lobby ID from EOS party ID `{party_id}` and build ID `{build_id}`")]
    InvalidLobbyBuildId { party_id: String, build_id: String },

    #[error("lobby ID `{lobby_id}` does not conform to `<eos_party_id>-<net_cl>-default`")]
    InvalidLobbyId { lobby_id: String },
}

fn normalize_hex32(input: &str) -> Result<String, IdFormatError> {
    if input.len() == 32 && input.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(input.to_ascii_lowercase())
    } else {
        Err(IdFormatError::InvalidHex32 {
            value: input.to_owned(),
        })
    }
}

macro_rules! define_hex32_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Validates and normalizes a 32-character hexadecimal Epic identifier.
            pub fn parse(value: impl AsRef<str>) -> Result<Self, IdFormatError> {
                normalize_hex32(value.as_ref()).map(Self)
            }

            /// Wraps a raw identifier without strict hex32 normalization.
            ///
            /// Intended for wire payloads where upstream test fixtures or legacy
            /// services may supply synthetic identifiers.
            pub fn from_raw(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl FromStr for $name {
            type Err = IdFormatError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::parse(s)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

define_hex32_id!(AccountId, "Epic Games 32-character hexadecimal account identifier.");
define_hex32_id!(ProductUserId, "Epic Online Services Product User ID (PUID).");
define_hex32_id!(PartyId, "Epic Games or EOS party identifier.");
define_hex32_id!(DeploymentId, "EOS deployment identifier.");

/// Fortnite party lobby identifier formatted as `<eos_party_id>-<net_cl>-default`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LobbyId(String);

impl LobbyId {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, IdFormatError> {
        let s = value.as_ref();
        let mut parts = s.split('-');
        let party_part = parts.next().unwrap_or_default();
        let net_cl_part = parts.next().unwrap_or_default();
        let suffix = parts.next().unwrap_or_default();
        if parts.next().is_some()
            || normalize_hex32(party_part).is_err()
            || net_cl_part.is_empty()
            || !net_cl_part.bytes().all(|b| b.is_ascii_digit())
            || suffix != "default"
        {
            return Err(IdFormatError::InvalidLobbyId {
                lobby_id: s.to_owned(),
            });
        }
        Ok(Self(format!(
            "{}-{}-default",
            party_part.to_ascii_lowercase(),
            net_cl_part
        )))
    }

    pub fn from_raw(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl FromStr for LobbyId {
    type Err = IdFormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl fmt::Display for LobbyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for LobbyId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Converts an EOS party ID and Fortnite `party:build_id` string into a Fortnite lobby ID
/// by extracting the numeric `netCL` token after the final `:` and formatting
/// `{eos_party_id}-{net_cl}-default`.
pub fn to_fortnite_lobby_id(
    eos_party_id: &PartyId,
    party_build_id: &str,
) -> Result<LobbyId, IdFormatError> {
    let net_cl = party_build_id
        .rsplit(':')
        .next()
        .map(str::trim)
        .filter(|segment| !segment.is_empty() && segment.bytes().all(|b| b.is_ascii_digit()))
        .ok_or_else(|| IdFormatError::InvalidLobbyBuildId {
            party_id: eos_party_id.as_str().to_owned(),
            build_id: party_build_id.to_owned(),
        })?;

    Ok(LobbyId(format!("{}-{}-default", eos_party_id.as_str(), net_cl)))
}

/// Extracts the underlying EOS [`PartyId`] from a Fortnite [`LobbyId`].
pub fn to_eos_party_id(lobby_id: &LobbyId) -> Result<PartyId, IdFormatError> {
    let first_segment = lobby_id.as_str().split('-').next().unwrap_or_default();
    PartyId::parse(first_segment).map_err(|_| IdFormatError::InvalidLobbyId {
        lobby_id: lobby_id.as_str().to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::parse_build_version_cl;

    #[test]
    fn parses_changelist_from_launcher_build_version() {
        assert_eq!(
            parse_build_version_cl("++Fortnite+Release-31.00-CL-37219843-Windows"),
            Some("37219843")
        );
        assert_eq!(
            parse_build_version_cl("++Fortnite+Release-32.10-CL-38001234"),
            Some("38001234")
        );
        assert_eq!(parse_build_version_cl("invalid-build"), None);
    }
}
