use serde::{Deserialize, Serialize};
use std::fmt;

/// Gaming platforms recognized by Epic Games Online Services and Fortnite party APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum Platform {
    #[serde(rename = "WIN")]
    #[default]
    Win,
    #[serde(rename = "MAC")]
    Mac,
    #[serde(rename = "PSN")]
    Psn,
    #[serde(rename = "PS5")]
    Ps5,
    #[serde(rename = "XBL")]
    Xbl,
    #[serde(rename = "XSX")]
    Xsx,
    #[serde(rename = "SWT")]
    Swt,
    #[serde(rename = "SWT2")]
    Swt2,
    #[serde(rename = "IOS")]
    Ios,
    #[serde(rename = "AND")]
    And,
    #[serde(rename = "LUNA")]
    Luna,
    #[serde(rename = "UNKNOWN")]
    #[serde(other)]
    Unknown,
}

impl Platform {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Win => "WIN",
            Self::Mac => "MAC",
            Self::Psn => "PSN",
            Self::Ps5 => "PS5",
            Self::Xbl => "XBL",
            Self::Xsx => "XSX",
            Self::Swt => "SWT",
            Self::Swt2 => "SWT2",
            Self::Ios => "IOS",
            Self::And => "AND",
            Self::Luna => "LUNA",
            Self::Unknown => "UNKNOWN",
        }
    }

    pub fn from_wire(value: &str) -> Self {
        match value.to_ascii_uppercase().as_str() {
            "WIN" | "WINDOWS" => Self::Win,
            "MAC" => Self::Mac,
            "PSN" | "PS4" => Self::Psn,
            "PS5" => Self::Ps5,
            "XBL" | "XBOX" => Self::Xbl,
            "XSX" => Self::Xsx,
            "SWT" | "SWITCH" => Self::Swt,
            "SWT2" => Self::Swt2,
            "IOS" => Self::Ios,
            "AND" | "ANDROID" => Self::And,
            "LUNA" => Self::Luna,
            _ => Self::Unknown,
        }
    }
}

impl fmt::Display for Platform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Fortnite matchmaking regions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum Region {
    #[serde(rename = "NAE")]
    #[default]
    Nae,
    #[serde(rename = "NAW")]
    Naw,
    #[serde(rename = "NAC")]
    Nac,
    #[serde(rename = "EU")]
    Eu,
    #[serde(rename = "BR")]
    Br,
    #[serde(rename = "OCE")]
    Oce,
    #[serde(rename = "ASIA")]
    Asia,
    #[serde(rename = "ME")]
    Me,
}

impl Region {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nae => "NAE",
            Self::Naw => "NAW",
            Self::Nac => "NAC",
            Self::Eu => "EU",
            Self::Br => "BR",
            Self::Oce => "OCE",
            Self::Asia => "ASIA",
            Self::Me => "ME",
        }
    }
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// XMPP/EOS presence availability states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PresenceOnlineType {
    #[default]
    Online,
    Away,
    Chat,
    Dnd,
    Xa,
    Offline,
}

impl PresenceOnlineType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Away => "away",
            Self::Chat => "chat",
            Self::Dnd => "dnd",
            Self::Xa => "xa",
            Self::Offline => "offline",
        }
    }
}

impl fmt::Display for PresenceOnlineType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Fortnite lobby readiness state carried in member metadata (`Default:LobbyState_j`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum ReadyState {
    Ready,
    #[default]
    NotReady,
    SittingOut,
    Away,
}

impl ReadyState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "Ready",
            Self::NotReady => "NotReady",
            Self::SittingOut => "SittingOut",
            Self::Away => "Away",
        }
    }
}

impl fmt::Display for ReadyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// XMPP show status helper mapping cleanly to presence `<show>` / status tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum AwayStatus {
    #[serde(rename = "online")]
    #[default]
    Online,
    #[serde(rename = "away")]
    Away,
    #[serde(rename = "xa")]
    ExtendedAway,
    #[serde(rename = "dnd")]
    DoNotDisturb,
    #[serde(rename = "chat")]
    Chat,
}

impl AwayStatus {
    pub const fn as_show_str(self) -> Option<&'static str> {
        match self {
            Self::Online => None,
            Self::Away => Some("away"),
            Self::ExtendedAway => Some("xa"),
            Self::DoNotDisturb => Some("dnd"),
            Self::Chat => Some("chat"),
        }
    }
}

/// Input method reported by a party member (`Default:LobbyState_j.LobbyState.currentInput`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum V2Input {
    #[serde(rename = "keyboardmouse")]
    #[default]
    KeyboardMouse,
    Gamepad,
    Touch,
}

impl V2Input {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::KeyboardMouse => "keyboardmouse",
            Self::Gamepad => "gamepad",
            Self::Touch => "touch",
        }
    }
}

/// Epic Party v2 discoverability configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum PartyDiscoverability {
    #[serde(rename = "ALL")]
    #[default]
    All,
    #[serde(rename = "INVITED_ONLY")]
    InvitedOnly,
}

/// Epic Party v2 joinability configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum PartyJoinability {
    #[serde(rename = "OPEN")]
    #[default]
    Open,
    #[serde(rename = "INVITE_ONLY")]
    InviteOnly,
    #[serde(rename = "INVITE_AND_FORMER")]
    InviteAndFormer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum PartyType {
    #[default]
    Public,
    FriendsOnly,
    Private,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum InviteRestriction {
    #[default]
    AnyMember,
    LeaderOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum PresencePermission {
    #[default]
    Anyone,
    Leader,
    Noone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum InvitePermission {
    #[default]
    Anyone,
    AnyMember,
    Leader,
}

/// Full Fortnite party privacy descriptor matching `Default:PrivacySettings_j.PrivacySettings`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FortnitePartyPrivacy {
    pub party_type: PartyType,
    pub invite_restriction: InviteRestriction,
    pub only_leader_friends_can_join: bool,
    pub presence_permission: PresencePermission,
    pub invite_permission: InvitePermission,
    pub accepting_members: bool,
}

impl Default for FortnitePartyPrivacy {
    fn default() -> Self {
        PartyPrivacyPreset::Public.to_privacy()
    }
}

/// Standard Fortnite party privacy presets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum PartyPrivacyPreset {
    #[default]
    Public,
    FriendsAllowFriendsOfFriends,
    Friends,
    PrivateAllowFriendsOfFriends,
    Private,
}

impl PartyPrivacyPreset {
    pub const fn to_privacy(self) -> FortnitePartyPrivacy {
        match self {
            Self::Public => FortnitePartyPrivacy {
                party_type: PartyType::Public,
                invite_restriction: InviteRestriction::AnyMember,
                only_leader_friends_can_join: false,
                presence_permission: PresencePermission::Anyone,
                invite_permission: InvitePermission::Anyone,
                accepting_members: true,
            },
            Self::FriendsAllowFriendsOfFriends => FortnitePartyPrivacy {
                party_type: PartyType::FriendsOnly,
                invite_restriction: InviteRestriction::AnyMember,
                only_leader_friends_can_join: false,
                presence_permission: PresencePermission::Anyone,
                invite_permission: InvitePermission::AnyMember,
                accepting_members: true,
            },
            Self::Friends => FortnitePartyPrivacy {
                party_type: PartyType::FriendsOnly,
                invite_restriction: InviteRestriction::LeaderOnly,
                only_leader_friends_can_join: true,
                presence_permission: PresencePermission::Leader,
                invite_permission: InvitePermission::Leader,
                accepting_members: true,
            },
            Self::PrivateAllowFriendsOfFriends => FortnitePartyPrivacy {
                party_type: PartyType::Private,
                invite_restriction: InviteRestriction::AnyMember,
                only_leader_friends_can_join: false,
                presence_permission: PresencePermission::Noone,
                invite_permission: InvitePermission::AnyMember,
                accepting_members: false,
            },
            Self::Private => FortnitePartyPrivacy {
                party_type: PartyType::Private,
                invite_restriction: InviteRestriction::LeaderOnly,
                only_leader_friends_can_join: true,
                presence_permission: PresencePermission::Noone,
                invite_permission: InvitePermission::Leader,
                accepting_members: false,
            },
        }
    }

    pub const fn discoverability(self) -> PartyDiscoverability {
        match self {
            Self::Public | Self::FriendsAllowFriendsOfFriends | Self::Friends => {
                PartyDiscoverability::All
            }
            Self::PrivateAllowFriendsOfFriends | Self::Private => PartyDiscoverability::InvitedOnly,
        }
    }

    pub const fn joinability(self) -> PartyJoinability {
        match self {
            Self::Public | Self::FriendsAllowFriendsOfFriends | Self::Friends => {
                PartyJoinability::Open
            }
            Self::PrivateAllowFriendsOfFriends | Self::Private => PartyJoinability::InviteAndFormer,
        }
    }
}

/// Platform filter used by Epic's user-search service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum UserSearchPlatform {
    #[default]
    Epic,
    Psn,
    Xbl,
}

/// Match mode used by Epic's user-search service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum UserSearchMatchType {
    #[default]
    Exact,
    Prefix,
}

/// Auth session token families managed by the auth module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AuthSessionType {
    #[default]
    Account,
    ClientCredentials,
    Eos,
}

/// Chat transport channels across XMPP and EOS Chat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationType {
    #[serde(alias = "dm", alias = "DirectMessage", alias = "direct-message")]
    DirectMessage,
    #[serde(
        alias = "party",
        alias = "Party",
        alias = "epic_party",
        alias = "EpicParty",
        alias = "epic-party",
        alias = "party_chat",
        alias = "persistent",
        alias = "Persistent"
    )]
    PartyChat,
}

/// Signed EOS party/chat message categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignedMessageType {
    MemberIntent,
    PartyUpdate,
    System,
}

/// Extensible Fortnite playlist identifier.
///
/// Fortnite introduces new playlists dynamically across live updates, so any string
/// is preserved while common Battle Royale / Zero Build presets are provided as
/// associated constants.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Playlist(String);

impl Playlist {
    pub const DEFAULT_SOLO: &'static str = "Playlist_DefaultSolo";
    pub const DEFAULT_DUO: &'static str = "Playlist_DefaultDuo";
    pub const DEFAULT_TRIO: &'static str = "Playlist_Trios";
    pub const DEFAULT_SQUAD: &'static str = "Playlist_DefaultSquad";
    pub const NO_BUILD_SOLO: &'static str = "Playlist_NoBuildBR_Solo";
    pub const NO_BUILD_DUO: &'static str = "Playlist_NoBuildBR_Duo";
    pub const NO_BUILD_TRIO: &'static str = "Playlist_NoBuildBR_Trio";
    pub const NO_BUILD_SQUAD: &'static str = "Playlist_NoBuildBR_Squad";
    pub const CREATIVE_PLAY_ONLY: &'static str = "Playlist_PlaygroundV2";

    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for Playlist {
    fn default() -> Self {
        Self(Self::DEFAULT_SQUAD.to_owned())
    }
}

impl From<&str> for Playlist {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for Playlist {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl fmt::Display for Playlist {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
