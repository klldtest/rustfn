pub mod xmpp;
pub mod message;
pub mod metadata_store;

pub use xmpp::{
    XMPP, XMPPCommand, XMPPConfig, XMPPEvent, XMPPIncomingStanza, build_fortnite_resource,
    chat_message_frame, parse_incoming_stanza, presence_frame,
};
pub use message::{
    XMPPAdminMessage, XMPPBlockListEntryPayload, XMPPFriendNotification,
    XMPPFriendshipRemoveNotification, XMPPPartyInviteDeclinedNotification,
    XMPPPartyJoinIntentionNotification, XMPPPartyMemberConfirmationNotification,
    XMPPPartyMemberDeltaNotification, XMPPPartyMemberEventNotification, XMPPPartyPingNotification,
    XMPPPartyUpdatedNotification, XMPPUserBlocklistUpdate, parse_admin_message,
};
pub use metadata_store::{
    DEFAULT_MEMBER_METADATA_TTL, QueuedMemberMetadataUpdate, XMPPMetadataStore,
};

pub use crate::events::{
    RealtimeEventContext, spawn_stomp_event_handler, spawn_xmpp_event_handler,
};
