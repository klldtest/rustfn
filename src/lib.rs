pub mod protocol;
pub mod http;
pub mod auth;
pub mod xmpp;
pub mod stomp;
pub mod users;
pub mod friends;
pub mod chat;
pub mod party;
pub mod events;
pub mod client;
pub mod commands;

pub mod config {
    pub use crate::client::config::*;
}
pub use crate::auth::{
    AuthMethod, DeviceAuthCredentials, DeviceAuthStore, DeviceCodeAuth, JsonFileDeviceAuthStore,
    authenticate_device_code,
};
pub use crate::chat::{
    ChatManager, ChatTransportOrigin, ReceivedFriendMessage, SentFriendMessage, SentPartyChatResult,
};
pub use crate::client::{
    CacheConfig, ClientConfig, ExtraServiceEndpoints, Client, ClientBuilder,
    PartyConfig, SessionAuth, SquadPositionPriority,
};
pub use crate::commands::{
    Bot, BotCog, BotHandle, BotUser, BucketType, Command, CommandError, Context, Cooldown,
    CooldownMapping, FriendRequestContext, Group, HelpOptions, IncomingMessage, IntoBotResult,
    MaxConcurrency, MessageChannel, PartyInviteContext, PartyMemberContext, PrefixConfig,
    ReadyContext, StringView, dm_only, is_owner, party_only,
};
pub use crate::events::{ClientEvent, ClientEventBus, EventHandler};
pub use crate::friends::{
    BlockedUser, FriendDirection, FriendManager, FriendPresence, FriendRecord, FriendTransition,
    OutboundPresenceSpec, PartyJoinInfoData, PendingFriend,
};
pub use crate::http::{BuildInfo, BuildVersionSource, FortniteBuildResolver, HTTP};
pub use crate::party::{
    BannerSnapshot, BattlePassSnapshot, ClientParty, ClientPartyInit, ClientPartyMember,
    CosmeticEnlightment, CosmeticItem, CosmeticVariant, CosmeticsUpdate, IslandInfo,
    MemberDeltaChanges, MemberRemovalReason, PartyChat, PartyChatMessage, PartyEvent,
    PartyEventSink, ReceivedPartyJoinRequest, PartyMemberMeta, PartyMemberSnapshot, PartyMeta,
    PartyRuntimeConfig, PartySnapshot, ReceivedPartyInvitation, SentPartyInvitation,
    calculate_squad_assignments,
};
pub use crate::protocol::{
    AccountId, AuthClient, AwayStatus, ConversationType, FortnitePartyPrivacy, LobbyId, MetaMap,
    MetaPatch, PartyDiscoverability, PartyId, PartyJoinability, PartyPrivacyPreset, PartyType,
    Platform, Playlist, PresenceOnlineType, PresencePermission, ProductUserId, ReadyState, Region,
    RustfnError, ServiceEndpoints, V2Input,
};
pub use crate::users::{UserManager, UserProfile};
