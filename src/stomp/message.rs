pub use crate::protocol::{
    EOSPartyConnectionData, EOSPartyData, EOSPartyDataConfig, EOSPartyInviteData,
    EOSPartyJoinRequestData, EOSPartyMemberData, EOSPartyMemberUpdateData, EOSPartyUpdateData,
    EOSPartyUserState,
};
use crate::protocol::{
    AccountId, ConversationType, PresenceOnlineType, PresencePropValue, RustfnError,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EOSChatConversationInfo {
    pub conversation_id: String,
    #[serde(rename = "type")]
    pub conversation_type: ConversationType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EOSChatMessageBody {
    pub body: String,
    pub sender_id: AccountId,
    pub time: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EOSChatNewMessagePayload {
    #[serde(default)]
    pub namespace: String,
    pub conversation: EOSChatConversationInfo,
    pub message: EOSChatMessageBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EOSChatNewWhisperPayload {
    #[serde(default)]
    pub namespace: String,
    pub message: EOSChatMessageBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EOSChatConversationCreatedPayload {
    #[serde(default)]
    pub namespace: String,
    pub conversation_id: String,
    #[serde(rename = "type")]
    pub conversation_type: ConversationType,
    #[serde(default)]
    pub members: Vec<AccountId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EOSChatMembersLeftPayload {
    #[serde(default)]
    pub namespace: String,
    pub conversation_id: String,
    #[serde(default)]
    pub members: Vec<AccountId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EOSPresenceActivity {
    #[serde(default)]
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EOSPresencePerNs {
    pub ns: String,
    #[serde(default)]
    pub product_id: Option<String>,
    pub status: PresenceOnlineType,
    #[serde(default)]
    pub activity: EOSPresenceActivity,
    #[serde(default)]
    pub props: BTreeMap<String, Value>,
}

impl EOSPresencePerNs {
    /// Decodes a single EOS presence property key using `PresencePropValue` (`b`/`i`/`m`/`s` prefix).
    pub fn decoded_props(&self) -> BTreeMap<String, PresencePropValue> {
        self.props
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), PresencePropValue::decode(s))))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EOSPresenceUpdatePayload {
    pub account_id: AccountId,
    pub status: PresenceOnlineType,
    #[serde(default)]
    pub per_ns: Vec<EOSPresencePerNs>,
}

/// Typed representation of messages delivered inside EOS Connect STOMP `MESSAGE` frames.
#[derive(Debug, Clone, PartialEq)]
pub enum EOSConnectNotification {
    CoreConnected {
        id: Option<String>,
        correlation_id: Option<String>,
        timestamp: Option<u64>,
        connection_id: String,
    },
    CoreConnectFailed {
        id: Option<String>,
        correlation_id: Option<String>,
        timestamp: Option<u64>,
        status_code: i64,
        message: String,
    },
    SocialChatNewWhisper {
        id: Option<String>,
        payload: EOSChatNewWhisperPayload,
    },
    SocialChatNewMessage {
        id: Option<String>,
        payload: EOSChatNewMessagePayload,
    },
    SocialChatConversationCreated {
        id: Option<String>,
        payload: EOSChatConversationCreatedPayload,
    },
    SocialChatMembersLeft {
        id: Option<String>,
        payload: EOSChatMembersLeftPayload,
    },
    PresenceUpdate {
        id: Option<String>,
        payload: EOSPresenceUpdatePayload,
    },
    PartyInviteCreated {
        id: Option<String>,
        payload: EOSPartyInviteData,
    },
    PartyInviteExpired {
        id: Option<String>,
        payload: EOSPartyInviteData,
    },
    PartyJoinRequestCreated {
        id: Option<String>,
        payload: EOSPartyJoinRequestData,
    },
    PartyMemberJoined {
        id: Option<String>,
        payload: EOSPartyMemberUpdateData,
    },
    PartyMemberLeft {
        id: Option<String>,
        payload: EOSPartyMemberUpdateData,
    },
    PartyMemberStateUpdated {
        id: Option<String>,
        payload: EOSPartyMemberUpdateData,
    },
    PartyMemberDisconnected {
        id: Option<String>,
        payload: EOSPartyMemberUpdateData,
    },
    PartyMemberExpiredAfterDisconnect {
        id: Option<String>,
        payload: EOSPartyMemberUpdateData,
    },
    PartyMemberExpiredPartyDisbanded {
        id: Option<String>,
        payload: EOSPartyMemberUpdateData,
    },
    PartyMemberKicked {
        id: Option<String>,
        payload: EOSPartyMemberUpdateData,
    },
    PartyUpdated {
        id: Option<String>,
        payload: EOSPartyUpdateData,
    },
    PartyMemberConnected {
        id: Option<String>,
        payload: Value,
    },
    PartyMemberRefreshSummary {
        id: Option<String>,
        payload: Value,
    },
    MatchmakingEvent {
        event_type: String,
        id: Option<String>,
        payload: Value,
    },
    Unknown {
        event_type: String,
        id: Option<String>,
        raw: Value,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct STOMPEnvelopeHead {
    #[serde(rename = "type")]
    event_type: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    timestamp: Option<u64>,
    #[serde(default)]
    connection_id: Option<String>,
    #[serde(default)]
    status_code: Option<i64>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    payload: Option<Value>,
}

/// Parses an EOS Connect STOMP `MESSAGE` JSON body. Returns an extracted message ID when present
/// alongside the decoded notification variant.
pub fn parse_connect_notification(
    body: &str,
    destination: Option<&str>,
) -> Result<(Option<String>, EOSConnectNotification), RustfnError> {
    let raw: Value = serde_json::from_str(body)?;
    let head: STOMPEnvelopeHead = serde_json::from_value(raw.clone())?;
    let event_type = head.event_type.unwrap_or_default();
    let msg_id = head.id.clone();

    if event_type.starts_with("mms/v2.")
        || event_type.starts_with("mms.")
        || destination.is_some_and(|d| d.contains("productUserId/"))
    {
        return Ok((
            msg_id.clone(),
            EOSConnectNotification::MatchmakingEvent {
                event_type,
                id: msg_id,
                payload: head.payload.unwrap_or(raw),
            },
        ));
    }

    let payload_val = head.payload.unwrap_or(Value::Null);
    let parsed = match event_type.as_str() {
        "core.connect.v1.connected" => EOSConnectNotification::CoreConnected {
            id: msg_id.clone(),
            correlation_id: head.correlation_id,
            timestamp: head.timestamp,
            connection_id: head.connection_id.ok_or_else(|| {
                RustfnError::Protocol("core.connect.v1.connected missing connectionId".to_owned())
            })?,
        },
        "core.connect.v1.connect-failed" => EOSConnectNotification::CoreConnectFailed {
            id: msg_id.clone(),
            correlation_id: head.correlation_id,
            timestamp: head.timestamp,
            status_code: head.status_code.unwrap_or(0),
            message: head.message.unwrap_or_default(),
        },
        "social.chat.v1.NEW_WHISPER" => EOSConnectNotification::SocialChatNewWhisper {
            id: msg_id.clone(),
            payload: serde_json::from_value(payload_val)?,
        },
        "social.chat.v1.NEW_MESSAGE" => EOSConnectNotification::SocialChatNewMessage {
            id: msg_id.clone(),
            payload: serde_json::from_value(payload_val)?,
        },
        "social.chat.v1.CONVERSATION_CREATED" => {
            EOSConnectNotification::SocialChatConversationCreated {
                id: msg_id.clone(),
                payload: serde_json::from_value(payload_val)?,
            }
        }
        "social.chat.v1.MEMBERS_LEFT" => EOSConnectNotification::SocialChatMembersLeft {
            id: msg_id.clone(),
            payload: serde_json::from_value(payload_val)?,
        },
        "presence.v1.UPDATE" => EOSConnectNotification::PresenceUpdate {
            id: msg_id.clone(),
            payload: serde_json::from_value(payload_val)?,
        },
        "party.v2.INVITE_CREATED" => EOSConnectNotification::PartyInviteCreated {
            id: msg_id.clone(),
            payload: serde_json::from_value(payload_val)?,
        },
        "party.v2.INVITE_EXPIRED" => EOSConnectNotification::PartyInviteExpired {
            id: msg_id.clone(),
            payload: serde_json::from_value(payload_val)?,
        },
        "party.v2.JOIN_REQUEST_CREATED" => EOSConnectNotification::PartyJoinRequestCreated {
            id: msg_id.clone(),
            payload: serde_json::from_value(payload_val)?,
        },
        "party.v2.MEMBER_JOINED" => EOSConnectNotification::PartyMemberJoined {
            id: msg_id.clone(),
            payload: serde_json::from_value(payload_val)?,
        },
        "party.v2.MEMBER_LEFT" => EOSConnectNotification::PartyMemberLeft {
            id: msg_id.clone(),
            payload: serde_json::from_value(payload_val)?,
        },
        "party.v2.MEMBER_STATE_UPDATED" => EOSConnectNotification::PartyMemberStateUpdated {
            id: msg_id.clone(),
            payload: serde_json::from_value(payload_val)?,
        },
        "party.v2.MEMBER_DISCONNECTED" => EOSConnectNotification::PartyMemberDisconnected {
            id: msg_id.clone(),
            payload: serde_json::from_value(payload_val)?,
        },
        "party.v2.MEMBER_EXPIRED_AFTER_DISCONNECT" => {
            EOSConnectNotification::PartyMemberExpiredAfterDisconnect {
                id: msg_id.clone(),
                payload: serde_json::from_value(payload_val)?,
            }
        }
        "party.v2.MEMBER_EXPIRED_PARTY_DISBANDED" => {
            EOSConnectNotification::PartyMemberExpiredPartyDisbanded {
                id: msg_id.clone(),
                payload: serde_json::from_value(payload_val)?,
            }
        }
        "party.v2.MEMBER_KICKED" => EOSConnectNotification::PartyMemberKicked {
            id: msg_id.clone(),
            payload: serde_json::from_value(payload_val)?,
        },
        "party.v2.PARTY_UPDATED" => EOSConnectNotification::PartyUpdated {
            id: msg_id.clone(),
            payload: serde_json::from_value(payload_val)?,
        },
        "party.v2.MEMBER_CONNECTED" => EOSConnectNotification::PartyMemberConnected {
            id: msg_id.clone(),
            payload: payload_val,
        },
        "party.v2.MEMBER_REFRESH_SUMMARY" => EOSConnectNotification::PartyMemberRefreshSummary {
            id: msg_id.clone(),
            payload: payload_val,
        },
        _ => EOSConnectNotification::Unknown {
            event_type,
            id: msg_id.clone(),
            raw,
        },
    };

    Ok((msg_id, parsed))
}
