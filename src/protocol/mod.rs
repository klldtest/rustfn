pub mod error;
pub mod enums;
pub mod meta;
pub mod endpoints;

pub use meta as meta_codec;

pub use error::{
    EpicgamesAPIErrorData, INVALID_TOKEN_CODES, RustfnError, ValidationFailure, error_codes,
};
pub use enums::{
    AuthSessionType, AwayStatus, ConversationType, FortnitePartyPrivacy, InvitePermission,
    InviteRestriction, PartyDiscoverability, PartyJoinability, PartyPrivacyPreset, PartyType,
    Platform, Playlist, PresenceOnlineType, PresencePermission, ReadyState, Region,
    SignedMessageType, UserSearchMatchType, UserSearchPlatform, V2Input,
};
pub use meta::{
    EOSPartyConnectionData, EOSPartyData, EOSPartyDataConfig, EOSPartyInviteData,
    EOSPartyJoinRequestData, EOSPartyMemberData, EOSPartyMemberUpdateData, EOSPartyUpdateData,
    EOSPartyUserState, MetaCodecError, MetaMap, MetaPatch, MetaSuffix, MetaTypedValue,
    PresencePropValue, PresencePropsMap, XMPPPresenceStatusPayload, deserialize_eos_current_party,
    keys,
};
pub use endpoints::{
    AccountId, AuthClient, CacheConfig, DeploymentId, EOS_EAS_DEPLOYMENT_ID,
    ExtraServiceEndpoints, FORTNITE_CATALOG_ITEM_ID, IdFormatError, LobbyId, PartyConfig,
    PartyId, ProductUserId, ServiceEndpoints, SquadPositionPriority, extra_urls,
    parse_build_version_cl, to_eos_party_id, to_fortnite_lobby_id, urls,
};
