use crate::protocol::{AccountId, MetaMap, PartyId, RustfnError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Friend state change event emitted inside `com.epicgames.friends.core.apiobjects.Friend`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct XMPPFriendNotification {
    pub account_id: AccountId,
    pub status: String,
    #[serde(default)]
    pub direction: Option<String>,
    #[serde(default)]
    pub favorite: bool,
    #[serde(default)]
    pub created: Option<String>,
}

/// Friendship deletion or cancellation event (`FRIENDSHIP_REMOVE`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XMPPFriendshipRemoveNotification {
    pub from: AccountId,
    pub to: AccountId,
    pub reason: String,
    #[serde(default)]
    pub timestamp: Option<String>,
}

/// User blocklist add/remove notification (`USER_BLOCKLIST_UPDATE`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct XMPPUserBlocklistUpdate {
    pub account_id: AccountId,
    pub status: String,
}

/// Legacy blocklist entry payload (`BlockListEntryAdded` / `BlockListEntryRemoved`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct XMPPBlockListEntryPayload {
    pub account_id: AccountId,
}

/// Legacy Fortnite party ping notification (`com.epicgames.social.party.notification.v0.PING`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XMPPPartyPingNotification {
    pub pinger_id: AccountId,
    #[serde(default)]
    pub pinger_dn: Option<String>,
    #[serde(default)]
    pub expires: Option<String>,
    #[serde(default)]
    pub sent: Option<String>,
    #[serde(default)]
    pub meta: MetaMap,
}

/// Party member metadata delta notification (`MEMBER_JOINED` / `MEMBER_STATE_UPDATED`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XMPPPartyMemberDeltaNotification {
    pub party_id: PartyId,
    pub account_id: AccountId,
    #[serde(default)]
    pub account_dn: Option<String>,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub joined_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub sent: Option<String>,
    #[serde(default)]
    pub member_state_updated: MetaMap,
    #[serde(default)]
    pub member_state_removed: Vec<String>,
}

/// Party member event notification (`MEMBER_LEFT`, `MEMBER_EXPIRED`, `MEMBER_KICKED`, `MEMBER_NEW_CAPTAIN`, `MEMBER_DISCONNECTED`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XMPPPartyMemberEventNotification {
    pub party_id: PartyId,
    pub account_id: AccountId,
    #[serde(default)]
    pub captain_id: Option<AccountId>,
    #[serde(default)]
    pub revision: Option<u64>,
    #[serde(default)]
    pub sent: Option<String>,
    #[serde(default)]
    pub party_state_updated: MetaMap,
    #[serde(default)]
    pub party_state_removed: Vec<String>,
    #[serde(default)]
    pub party_privacy_type: Option<String>,
    #[serde(default)]
    pub max_number_of_members: Option<u32>,
}

/// Party metadata & config update (`com.epicgames.social.party.notification.v0.PARTY_UPDATED`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XMPPPartyUpdatedNotification {
    pub party_id: PartyId,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub captain_id: Option<AccountId>,
    #[serde(default)]
    pub party_type: Option<String>,
    #[serde(default)]
    pub party_sub_type: Option<String>,
    #[serde(default)]
    pub party_privacy_type: Option<String>,
    #[serde(default)]
    pub max_number_of_members: Option<u32>,
    #[serde(default)]
    pub invite_ttl_seconds: Option<u64>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub sent: Option<String>,
    #[serde(default)]
    pub party_state_updated: MetaMap,
    #[serde(default)]
    pub party_state_removed: Vec<String>,
    #[serde(default)]
    pub party_state_overridden: MetaMap,
}

/// Confirmation prompt sent to the party captain before letting a member finish joining.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XMPPPartyMemberConfirmationNotification {
    pub party_id: PartyId,
    pub account_id: AccountId,
    #[serde(default)]
    pub account_dn: Option<String>,
    #[serde(default)]
    pub sent: Option<String>,
    #[serde(default)]
    pub connection: Option<Value>,
}

/// Join intention request (`com.epicgames.social.party.notification.v0.INITIAL_INTENTION`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XMPPPartyJoinIntentionNotification {
    pub party_id: PartyId,
    pub requester_id: AccountId,
    #[serde(default)]
    pub requester_dn: Option<String>,
    #[serde(default)]
    pub sent: Option<String>,
    #[serde(default)]
    pub expires_at: Option<String>,
}

/// Party invite declined notification (`com.epicgames.social.party.notification.v0.INVITE_DECLINED`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XMPPPartyInviteDeclinedNotification {
    pub party_id: PartyId,
    pub invitee_id: AccountId,
    #[serde(default)]
    pub sent: Option<String>,
}

/// Strongly typed JSON message delivered from Epic's `xmpp-admin` JID.
#[derive(Debug, Clone, PartialEq)]
pub enum XMPPAdminMessage {
    FriendUpdate(XMPPFriendNotification),
    FriendshipRemove(XMPPFriendshipRemoveNotification),
    UserBlocklistUpdate(XMPPUserBlocklistUpdate),
    BlockListEntryAdded(XMPPBlockListEntryPayload),
    BlockListEntryRemoved(XMPPBlockListEntryPayload),
    PartyPing(XMPPPartyPingNotification),
    PartyMemberJoined(XMPPPartyMemberDeltaNotification),
    PartyMemberStateUpdated(XMPPPartyMemberDeltaNotification),
    PartyMemberLeft(XMPPPartyMemberEventNotification),
    PartyMemberExpired(XMPPPartyMemberEventNotification),
    PartyMemberKicked(XMPPPartyMemberEventNotification),
    PartyMemberNewCaptain(XMPPPartyMemberEventNotification),
    PartyMemberDisconnected(XMPPPartyMemberEventNotification),
    PartyUpdated(XMPPPartyUpdatedNotification),
    PartyMemberRequireConfirmation(XMPPPartyMemberConfirmationNotification),
    PartyInitialIntention(XMPPPartyJoinIntentionNotification),
    PartyInviteDeclined(XMPPPartyInviteDeclinedNotification),
    Other {
        message_type: String,
        raw: Value,
    },
}

#[derive(Deserialize)]
struct AdminEnvelopeHeader {
    #[serde(rename = "type")]
    message_type: String,
    #[serde(default)]
    payload: Option<Value>,
}

/// Parses a raw JSON `<message>` body originating from `xmpp-admin@prod.ol.epicgames.com`.
pub fn parse_admin_message(body: &str) -> Result<XMPPAdminMessage, RustfnError> {
    let raw: Value = serde_json::from_str(body)?;
    let header: AdminEnvelopeHeader = serde_json::from_value(raw.clone())?;

    let msg = match header.message_type.as_str() {
        "com.epicgames.friends.core.apiobjects.Friend" => {
            let payload = header.payload.ok_or_else(|| {
                RustfnError::Protocol("Friend XMPP notification missing payload field".to_owned())
            })?;
            XMPPAdminMessage::FriendUpdate(serde_json::from_value(payload)?)
        }
        "FRIENDSHIP_REMOVE" => {
            XMPPAdminMessage::FriendshipRemove(serde_json::from_value(raw)?)
        }
        "USER_BLOCKLIST_UPDATE" => {
            XMPPAdminMessage::UserBlocklistUpdate(serde_json::from_value(raw)?)
        }
        "com.epicgames.friends.core.apiobjects.BlockListEntryAdded" => {
            let payload = header.payload.unwrap_or_else(|| raw.clone());
            XMPPAdminMessage::BlockListEntryAdded(serde_json::from_value(payload)?)
        }
        "com.epicgames.friends.core.apiobjects.BlockListEntryRemoved" => {
            let payload = header.payload.unwrap_or_else(|| raw.clone());
            XMPPAdminMessage::BlockListEntryRemoved(serde_json::from_value(payload)?)
        }
        "com.epicgames.social.party.notification.v0.PING" => {
            XMPPAdminMessage::PartyPing(serde_json::from_value(raw)?)
        }
        "com.epicgames.social.party.notification.v0.MEMBER_JOINED" => {
            XMPPAdminMessage::PartyMemberJoined(serde_json::from_value(raw)?)
        }
        "com.epicgames.social.party.notification.v0.MEMBER_STATE_UPDATED" => {
            XMPPAdminMessage::PartyMemberStateUpdated(serde_json::from_value(raw)?)
        }
        "com.epicgames.social.party.notification.v0.MEMBER_LEFT" => {
            XMPPAdminMessage::PartyMemberLeft(serde_json::from_value(raw)?)
        }
        "com.epicgames.social.party.notification.v0.MEMBER_EXPIRED" => {
            XMPPAdminMessage::PartyMemberExpired(serde_json::from_value(raw)?)
        }
        "com.epicgames.social.party.notification.v0.MEMBER_KICKED" => {
            XMPPAdminMessage::PartyMemberKicked(serde_json::from_value(raw)?)
        }
        "com.epicgames.social.party.notification.v0.MEMBER_NEW_CAPTAIN" => {
            XMPPAdminMessage::PartyMemberNewCaptain(serde_json::from_value(raw)?)
        }
        "com.epicgames.social.party.notification.v0.MEMBER_DISCONNECTED" => {
            XMPPAdminMessage::PartyMemberDisconnected(serde_json::from_value(raw)?)
        }
        "com.epicgames.social.party.notification.v0.PARTY_UPDATED" => {
            XMPPAdminMessage::PartyUpdated(serde_json::from_value(raw)?)
        }
        "com.epicgames.social.party.notification.v0.MEMBER_REQUIRE_CONFIRMATION" => {
            XMPPAdminMessage::PartyMemberRequireConfirmation(serde_json::from_value(raw)?)
        }
        "com.epicgames.social.party.notification.v0.INITIAL_INTENTION" => {
            XMPPAdminMessage::PartyInitialIntention(serde_json::from_value(raw)?)
        }
        "com.epicgames.social.party.notification.v0.INVITE_DECLINED" => {
            XMPPAdminMessage::PartyInviteDeclined(serde_json::from_value(raw)?)
        }
        _ => XMPPAdminMessage::Other {
            message_type: header.message_type,
            raw,
        },
    };

    Ok(msg)
}
