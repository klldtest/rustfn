use chrono::{DateTime, Utc};
use crate::auth::AuthEvent;
use crate::chat::{
    ChatManager, ChatTransportOrigin, ReceivedFriendMessage, decode_stomp_message_body,
};
use crate::friends::{
    BlockedUser, FriendManager, FriendPresence, FriendRecord, FriendTransition, PendingFriend,
};
use crate::http::HTTP;
use crate::party::{
    ClientParty, MemberRemovalReason, PartyChatMessage, PartyEvent, PartyEventSink,
    ReceivedPartyJoinRequest, PartyMemberSnapshot, PartySnapshot, PendingMemberConfirmation,
    ReceivedPartyInvitation,
};
use crate::protocol::{
    AccountId, AwayStatus, ConversationType, LobbyId, PartyId, PresenceOnlineType,
    PresencePermission, XMPPPresenceStatusPayload,
};
use crate::stomp::{EOSConnectNotification, EOSPartyMemberUpdateData, STOMP, STOMPEvent};
use crate::users::UserManager;
use crate::xmpp::{XMPPAdminMessage, XMPP, XMPPEvent};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};
use uuid::Uuid;

/// High-level client, social, presence, party, and chat events emitted by a session.
#[derive(Debug, Clone)]
pub enum ClientEvent {
    Ready {
        account_id: AccountId,
        display_name: String,
    },
    Disconnected,
    Auth(AuthEvent),
    DeviceCodePrompt {
        verification_uri: String,
        verification_uri_complete: String,
        user_code: String,
    },
    FriendAdded(FriendRecord),
    FriendRemoved(FriendRecord),
    FriendRequestReceived(PendingFriend),
    FriendRequestAccepted(FriendRecord),
    FriendRequestCancelled(PendingFriend),
    FriendRequestDeclined(PendingFriend),
    BlockedUserAdded(BlockedUser),
    BlockedUserRemoved(AccountId),
    FriendPresenceUpdated(FriendPresence),
    FriendMessage(ReceivedFriendMessage),
    PartyUpdated(PartySnapshot),
    PartyLeft {
        party_id: LobbyId,
        eos_party_id: PartyId,
    },
    PartyMemberJoined {
        party_id: LobbyId,
        member: PartyMemberSnapshot,
    },
    PartyMemberLeft {
        party_id: LobbyId,
        member: PartyMemberSnapshot,
    },
    PartyMemberKicked {
        party_id: LobbyId,
        member: PartyMemberSnapshot,
    },
    PartyMemberExpired {
        party_id: LobbyId,
        member: PartyMemberSnapshot,
    },
    PartyMemberPromoted {
        party_id: LobbyId,
        new_leader: PartyMemberSnapshot,
    },
    PartyMemberUpdated {
        party_id: LobbyId,
        member: PartyMemberSnapshot,
    },
    PartyMemberOutfitUpdated {
        party_id: LobbyId,
        member: PartyMemberSnapshot,
        after: Option<String>,
        before: Option<String>,
    },
    PartyMemberBackpackUpdated {
        party_id: LobbyId,
        member: PartyMemberSnapshot,
        after: Option<String>,
        before: Option<String>,
    },
    PartyMemberPickaxeUpdated {
        party_id: LobbyId,
        member: PartyMemberSnapshot,
        after: Option<String>,
        before: Option<String>,
    },
    PartyMemberEmoteUpdated {
        party_id: LobbyId,
        member: PartyMemberSnapshot,
        after: Option<String>,
        before: Option<String>,
    },
    PartyMemberReadinessUpdated {
        party_id: LobbyId,
        member: PartyMemberSnapshot,
        ready: bool,
    },
    PartyMemberMatchStateUpdated {
        party_id: LobbyId,
        member: PartyMemberSnapshot,
    },
    PartyInviteReceived(ReceivedPartyInvitation),
    PartyInviteDeclined {
        party_id: PartyId,
        invitee_id: AccountId,
    },
    PartyInviteExpired {
        party_id: PartyId,
        inviter_id: AccountId,
    },
    PartyMemberDisconnected {
        party_id: LobbyId,
        member: PartyMemberSnapshot,
    },
    PartyJoinRequestReceived(ReceivedPartyJoinRequest),
    PartyMemberConfirmationReceived(PendingMemberConfirmation),
    PartyMessage(PartyChatMessage),
    MatchmakingEvent {
        event_type: String,
        payload: serde_json::Value,
    },
    RawXmppAdmin(XMPPAdminMessage),
}

/// Async handler interface for structured consumers preferring trait dispatch over stream polling.
pub trait EventHandler: Send + Sync + 'static {
    fn handle<'a>(
        &'a self,
        event: Arc<ClientEvent>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// Broadcast bus distributing immutable event snapshots to concurrent subscribers.
#[derive(Clone)]
pub struct ClientEventBus {
    sender: broadcast::Sender<Arc<ClientEvent>>,
}

impl ClientEventBus {
    pub const DEFAULT_CAPACITY: usize = 1024;

    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(16));
        Self { sender }
    }

    pub fn emit(&self, event: ClientEvent) -> usize {
        let _ = self.sender.send(Arc::new(event));
        self.sender.receiver_count()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<ClientEvent>> {
        self.sender.subscribe()
    }

    pub fn spawn_handler<H: EventHandler>(
        &self,
        handler: Arc<H>,
        cancel: CancellationToken,
    ) -> JoinHandle<()> {
        let mut rx = self.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    recv = rx.recv() => {
                        match recv {
                            Ok(event) => handler.handle(event).await,
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                tracing::warn!(skipped, "client event subscriber lagged behind broadcast buffer");
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }
        })
    }

    pub fn spawn_fn<F, Fut>(
        &self,
        mut handler: F,
        cancel: CancellationToken,
    ) -> JoinHandle<()>
    where
        F: FnMut(Arc<ClientEvent>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut rx = self.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    recv = rx.recv() => {
                        match recv {
                            Ok(event) => handler(event).await,
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                tracing::warn!(skipped, "client event subscriber lagged behind broadcast buffer");
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }
        })
    }
}

impl Default for ClientEventBus {
    fn default() -> Self {
        Self::new(Self::DEFAULT_CAPACITY)
    }
}

impl From<PartyEvent> for ClientEvent {
    fn from(event: PartyEvent) -> Self {
        match event {
            PartyEvent::PartyUpdated(snapshot) => Self::PartyUpdated(snapshot),
            PartyEvent::PartyLeft {
                party_id,
                eos_party_id,
            } => Self::PartyLeft {
                party_id,
                eos_party_id,
            },
            PartyEvent::PartyMemberJoined { party_id, member } => {
                Self::PartyMemberJoined { party_id, member }
            }
            PartyEvent::PartyMemberLeft { party_id, member } => {
                Self::PartyMemberLeft { party_id, member }
            }
            PartyEvent::PartyMemberKicked { party_id, member } => {
                Self::PartyMemberKicked { party_id, member }
            }
            PartyEvent::PartyMemberExpired { party_id, member } => {
                Self::PartyMemberExpired { party_id, member }
            }
            PartyEvent::PartyMemberPromoted {
                party_id,
                new_leader,
            } => Self::PartyMemberPromoted {
                party_id,
                new_leader,
            },
            PartyEvent::PartyMemberUpdated { party_id, member } => {
                Self::PartyMemberUpdated { party_id, member }
            }
            PartyEvent::PartyMemberOutfitUpdated {
                party_id,
                member,
                after,
                before,
            } => Self::PartyMemberOutfitUpdated {
                party_id,
                member,
                after,
                before,
            },
            PartyEvent::PartyMemberBackpackUpdated {
                party_id,
                member,
                after,
                before,
            } => Self::PartyMemberBackpackUpdated {
                party_id,
                member,
                after,
                before,
            },
            PartyEvent::PartyMemberPickaxeUpdated {
                party_id,
                member,
                after,
                before,
            } => Self::PartyMemberPickaxeUpdated {
                party_id,
                member,
                after,
                before,
            },
            PartyEvent::PartyMemberEmoteUpdated {
                party_id,
                member,
                after,
                before,
            } => Self::PartyMemberEmoteUpdated {
                party_id,
                member,
                after,
                before,
            },
            PartyEvent::PartyMemberReadinessUpdated {
                party_id,
                member,
                ready,
            } => Self::PartyMemberReadinessUpdated {
                party_id,
                member,
                ready,
            },
            PartyEvent::PartyMemberMatchStateUpdated { party_id, member } => {
                Self::PartyMemberMatchStateUpdated { party_id, member }
            }
            PartyEvent::PartyMemberConfirmationReceived(confirmation) => {
                Self::PartyMemberConfirmationReceived(confirmation)
            }
        }
    }
}

impl PartyEventSink for ClientEventBus {
    fn emit_party_event(&self, event: PartyEvent) -> usize {
        self.emit(ClientEvent::from(event))
    }
}

/// Shared session dependencies inspected when translating inbound XMPP and STOMP frames into state updates.
#[derive(Clone)]
pub struct RealtimeEventContext {
    pub self_id: AccountId,
    pub http: HTTP,
    pub users: Arc<UserManager>,
    pub friends: Arc<FriendManager>,
    pub chat: Arc<ChatManager>,
    pub events: ClientEventBus,
    pub xmpp: Option<Arc<XMPP>>,
    pub stomp: Option<Arc<STOMP>>,
    pub party_slot: Arc<RwLock<Option<ClientParty>>>,
    pub presence_permission: PresencePermission,
}

/// Spawns the XMPP WebSocket event dispatcher under `cancel`.
///
/// The caller must pass `rx = xmpp.subscribe_events()` captured prior to `xmpp.connect`
/// so early server admin/presence stanzas are never missed.
pub fn spawn_xmpp_event_handler(
    ctx: RealtimeEventContext,
    mut rx: broadcast::Receiver<Arc<XMPPEvent>>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                recv = rx.recv() => match recv {
                    Ok(event) => handle_xmpp_event(&ctx, (*event).clone()).await,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(account_id = %ctx.self_id, skipped, "XMPP handler lagged behind broadcast buffer");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    })
}

/// Spawns the EOS Connect STOMP event dispatcher under `cancel`.
///
/// The caller must pass `rx = stomp.subscribe_events()` captured prior to `stomp.connect`
/// so immediate `presence.v1`, `social.chat.v1`, and `party.v2` pushes are never dropped.
pub fn spawn_stomp_event_handler(
    ctx: RealtimeEventContext,
    mut rx: broadcast::Receiver<Arc<STOMPEvent>>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                recv = rx.recv() => match recv {
                    Ok(event) => handle_stomp_event(&ctx, (*event).clone()).await,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(account_id = %ctx.self_id, skipped, "STOMP handler lagged behind broadcast buffer");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    })
}

async fn handle_xmpp_event(ctx: &RealtimeEventContext, event: XMPPEvent) {
    match event {
        XMPPEvent::Connected { jid, resource } => {
            debug!(account_id = %ctx.self_id, ?jid, %resource, "XMPP session ready");
        }
        XMPPEvent::Disconnected {
            will_reconnect: true,
            reason,
        } => {
            debug!(account_id = %ctx.self_id, %reason, "XMPP disconnected cleanly; reconnecting");
        }
        XMPPEvent::Disconnected {
            will_reconnect: false,
            reason,
        } => {
            warn!(account_id = %ctx.self_id, %reason, "XMPP disconnected permanently");
            ctx.events.emit(ClientEvent::Disconnected);
        }
        XMPPEvent::ReconnectExhausted {
            attempts,
            last_error,
        } => {
            warn!(account_id = %ctx.self_id, attempts, %last_error, "XMPP reconnect retries exhausted");
            ctx.events.emit(ClientEvent::Disconnected);
        }
        XMPPEvent::ChatMessage { from, id, body, .. } => {
            let Some(author_id) = parse_account_from_jid(&from) else {
                trace!(%from, "ignoring XMPP chat from unparseable JID");
                return;
            };
            if author_id == ctx.self_id {
                return;
            }
            let author_display_name = resolve_display_name(ctx, &author_id, None).await;
            ctx.events
                .emit(ClientEvent::FriendMessage(ReceivedFriendMessage {
                    message_id: id.unwrap_or_else(|| Uuid::new_v4().simple().to_string()),
                    author_id,
                    author_display_name,
                    conversation_id: None,
                    content: body,
                    sent_at: Utc::now(),
                    origin: ChatTransportOrigin::Xmpp,
                }));
        }
        XMPPEvent::RawPresence {
            from,
            presence_type,
            show,
            status,
        } => {
            let Some(sender_id) = parse_account_from_jid(&from) else {
                return;
            };
            if sender_id == ctx.self_id {
                return;
            }

            let online_type = if presence_type.as_deref() == Some("unavailable") {
                PresenceOnlineType::Offline
            } else {
                match show.as_deref() {
                    Some("away") => PresenceOnlineType::Away,
                    Some("xa") => PresenceOnlineType::Xa,
                    Some("dnd") => PresenceOnlineType::Dnd,
                    Some("chat") => PresenceOnlineType::Chat,
                    _ => PresenceOnlineType::Online,
                }
            };
            let away_status = match show.as_deref() {
                Some("away") => AwayStatus::Away,
                Some("xa") => AwayStatus::ExtendedAway,
                Some("dnd") => AwayStatus::DoNotDisturb,
                Some("chat") => AwayStatus::Chat,
                _ => AwayStatus::Online,
            };

            let parsed_status = status
                .as_deref()
                .and_then(|raw| serde_json::from_str::<XMPPPresenceStatusPayload>(raw).ok());

            let presence = FriendPresence::from_xmpp(
                sender_id,
                online_type,
                away_status,
                parsed_status.as_ref(),
                status.as_deref(),
            );
            ctx.friends.record_presence(presence.clone());
            ctx.events.emit(ClientEvent::FriendPresenceUpdated(presence));
        }
        XMPPEvent::AdminMessage(admin) => {
            handle_xmpp_admin_message(ctx, admin).await;
        }
        XMPPEvent::StanzaError(err) => {
            debug!(account_id = %ctx.self_id, %err, "received XMPP stanza error");
        }
    }
}

async fn handle_xmpp_admin_message(ctx: &RealtimeEventContext, admin: XMPPAdminMessage) {
    match admin {
        XMPPAdminMessage::FriendUpdate(notif) => {
            if let Some(transition) = ctx.friends.on_friend_notification(&notif).await {
                emit_friend_transition(&ctx.events, transition);
            }
        }
        XMPPAdminMessage::FriendshipRemove(notif) => {
            if let Some(transition) = ctx.friends.on_friendship_remove(&notif) {
                emit_friend_transition(&ctx.events, transition);
            }
        }
        XMPPAdminMessage::UserBlocklistUpdate(notif) => {
            if let Some(transition) = ctx.friends.on_blocklist_update(&notif).await {
                emit_friend_transition(&ctx.events, transition);
            }
        }
        XMPPAdminMessage::BlockListEntryAdded(payload) => {
            let transition = ctx.friends.on_legacy_blocklist_entry(true, &payload).await;
            emit_friend_transition(&ctx.events, transition);
        }
        XMPPAdminMessage::BlockListEntryRemoved(payload) => {
            let transition = ctx.friends.on_legacy_blocklist_entry(false, &payload).await;
            emit_friend_transition(&ctx.events, transition);
        }
        XMPPAdminMessage::PartyMemberJoined(delta)
        | XMPPAdminMessage::PartyMemberStateUpdated(delta) => {
            let active_party = ctx.party_slot.read().await.clone();
            if let Some(party) = active_party
                && party.fn_id().as_str() == delta.party_id.as_str()
            {
                if let Err(err) = party.handle_xmpp_member_delta(delta).await {
                    debug!(error = %err, "failed to apply XMPP party member delta");
                }
            } else if let Some(ref xmpp) = ctx.xmpp {
                xmpp.metadata_store()
                    .push(delta.party_id.clone(), delta.into());
            }
        }
        XMPPAdminMessage::PartyUpdated(msg) => {
            let active_party = ctx.party_slot.read().await.clone();
            if let Some(party) = active_party
                && party.fn_id().as_str() == msg.party_id.as_str()
                && let Err(err) = party.handle_xmpp_party_updated(msg).await
            {
                debug!(error = %err, "failed to apply XMPP party update");
            }
        }
        XMPPAdminMessage::PartyMemberRequireConfirmation(msg) => {
            let active_party = ctx.party_slot.read().await.clone();
            if let Some(party) = active_party
                && party.fn_id().as_str() == msg.party_id.as_str()
                && let Err(err) = party.handle_xmpp_confirmation(msg).await
            {
                debug!(error = %err, "failed to process XMPP party confirmation");
            }
        }
        XMPPAdminMessage::PartyInviteDeclined(declined) => {
            ctx.events.emit(ClientEvent::PartyInviteDeclined {
                party_id: declined.party_id,
                invitee_id: declined.invitee_id,
            });
        }
        other @ (XMPPAdminMessage::PartyMemberLeft(_)
        | XMPPAdminMessage::PartyMemberExpired(_)
        | XMPPAdminMessage::PartyMemberKicked(_)
        | XMPPAdminMessage::PartyMemberNewCaptain(_)
        | XMPPAdminMessage::PartyMemberDisconnected(_)
        | XMPPAdminMessage::PartyPing(_)
        | XMPPAdminMessage::PartyInitialIntention(_)
        | XMPPAdminMessage::Other { .. }) => {
            ctx.events.emit(ClientEvent::RawXmppAdmin(other));
        }
    }
}

async fn handle_stomp_event(ctx: &RealtimeEventContext, event: STOMPEvent) {
    match event {
        STOMPEvent::Connected(ids) => {
            debug!(
                account_id = %ctx.self_id,
                public_connection_id = %ids.public_connection_id,
                "STOMP session ready"
            );
        }
        STOMPEvent::Disconnected {
            will_reconnect: true,
            reason,
        } => {
            debug!(account_id = %ctx.self_id, %reason, "STOMP disconnected cleanly; reconnecting");
        }
        STOMPEvent::Disconnected {
            will_reconnect: false,
            reason,
        } => {
            warn!(account_id = %ctx.self_id, %reason, "STOMP disconnected permanently");
            ctx.events.emit(ClientEvent::Disconnected);
        }
        STOMPEvent::ReconnectExhausted {
            attempts,
            last_error,
        } => {
            warn!(account_id = %ctx.self_id, attempts, %last_error, "STOMP reconnect retries exhausted");
            ctx.events.emit(ClientEvent::Disconnected);
        }
        STOMPEvent::Receipt { receipt_id } => {
            trace!(account_id = %ctx.self_id, %receipt_id, "STOMP receipt acknowledged");
        }
        STOMPEvent::ErrorFrame {
            status_code,
            message,
            ..
        } => {
            warn!(account_id = %ctx.self_id, ?status_code, %message, "STOMP ERROR frame received");
        }
        STOMPEvent::ParseError(err) => {
            warn!(account_id = %ctx.self_id, %err, "STOMP frame parse error");
        }
        STOMPEvent::Notification(notif) => {
            handle_stomp_notification(ctx, notif).await;
        }
    }
}

async fn handle_stomp_notification(
    ctx: &RealtimeEventContext,
    notif: EOSConnectNotification,
) {
    match notif {
        EOSConnectNotification::CoreConnected { .. }
        | EOSConnectNotification::CoreConnectFailed { .. } => {}
        EOSConnectNotification::PresenceUpdate { payload, .. } => {
            if payload.account_id == ctx.self_id {
                return;
            }
            let presence = FriendPresence::from_eos_update(&payload);
            ctx.friends.record_presence(presence.clone());
            ctx.events.emit(ClientEvent::FriendPresenceUpdated(presence));
        }
        EOSConnectNotification::SocialChatConversationCreated { payload, .. } => {
            if payload.conversation_type == ConversationType::DirectMessage
                && let Some(peer_id) = payload
                    .members
                    .into_iter()
                    .find(|member_id| member_id != &ctx.self_id)
            {
                ctx.chat
                    .cache_dm_conversation(peer_id, payload.conversation_id, false);
            }
        }
        EOSConnectNotification::SocialChatNewWhisper { id, payload } => {
            if payload.message.sender_id == ctx.self_id {
                return;
            }
            let content = decode_stomp_message_body(&payload.message.body);
            let author_display_name =
                resolve_display_name(ctx, &payload.message.sender_id, None).await;
            let sent_at = stomp_time_to_datetime(payload.message.time);
            ctx.events
                .emit(ClientEvent::FriendMessage(ReceivedFriendMessage {
                    message_id: id.unwrap_or_else(|| payload.message.time.to_string()),
                    author_id: payload.message.sender_id,
                    author_display_name,
                    conversation_id: None,
                    content,
                    sent_at,
                    origin: ChatTransportOrigin::EOSWhisper,
                }));
        }
        EOSConnectNotification::SocialChatNewMessage { id, payload } => {
            if payload.message.sender_id == ctx.self_id {
                return;
            }
            let content = decode_stomp_message_body(&payload.message.body);
            let sent_at = stomp_time_to_datetime(payload.message.time);
            let conv_id = payload.conversation.conversation_id;

            match payload.conversation.conversation_type {
                ConversationType::DirectMessage => {
                    ctx.chat.cache_dm_conversation(
                        payload.message.sender_id.clone(),
                        conv_id.clone(),
                        false,
                    );
                    let author_display_name =
                        resolve_display_name(ctx, &payload.message.sender_id, None).await;
                    ctx.events
                        .emit(ClientEvent::FriendMessage(ReceivedFriendMessage {
                            message_id: id.unwrap_or_else(|| payload.message.time.to_string()),
                            author_id: payload.message.sender_id,
                            author_display_name,
                            conversation_id: Some(conv_id),
                            content,
                            sent_at,
                            origin: ChatTransportOrigin::EOSConversation,
                        }));
                }
                ConversationType::PartyChat => {
                    let active_party = ctx.party_slot.read().await.clone();
                    let Some(party) = active_party else {
                        return;
                    };
                    let matches_party = if let Some(bare_eos) = conv_id.strip_prefix("ep-") {
                        party.eos_id().as_str() == bare_eos
                    } else if let Some(bare_fn) = conv_id.strip_prefix("p-") {
                        party.fn_id().as_str() == bare_fn
                    } else {
                        party.eos_id().as_str() == conv_id || party.fn_id().as_str() == conv_id
                    };
                    if !matches_party {
                        return;
                    }
                    let timestamp_ms = u64::try_from(sent_at.timestamp_millis()).unwrap_or(0);
                    ctx.events.emit(ClientEvent::PartyMessage(PartyChatMessage {
                        id,
                        party_id: party.fn_id().clone(),
                        eos_party_id: party.eos_id().clone(),
                        author_id: payload.message.sender_id,
                        content,
                        timestamp_ms,
                    }));
                }
            }
        }
        EOSConnectNotification::PartyInviteCreated { payload, .. } => {
            if matches!(ctx.presence_permission, PresencePermission::Noone) {
                trace!(party_id = %payload.party_id, "ignoring EOS invite under PresencePermission::Noone");
                return;
            }
            let inviter_display_name =
                resolve_display_name(ctx, &payload.inviter_id, payload.inviter_dn.clone()).await;
            let invite = ReceivedPartyInvitation::from_stomp(
                payload.party_id,
                payload.inviter_id,
                inviter_display_name,
                ctx.self_id.clone(),
                payload.sent_at.or(payload.sent),
                payload.expires_at,
                payload.meta,
                ctx.http.clone(),
            );
            ctx.events.emit(ClientEvent::PartyInviteReceived(invite));
        }
        EOSConnectNotification::PartyInviteExpired { payload, .. } => {
            ctx.events.emit(ClientEvent::PartyInviteExpired {
                party_id: payload.party_id,
                inviter_id: payload.inviter_id,
            });
        }
        EOSConnectNotification::PartyJoinRequestCreated { payload, .. } => {
            let Some(requester_id) = payload.resolved_requester().cloned() else {
                return;
            };
            let requester_display_name = resolve_display_name(ctx, &requester_id, None).await;
            let request = ReceivedPartyJoinRequest::from_stomp(
                requester_id,
                requester_display_name,
                ctx.self_id.clone(),
                payload.sent_at.or(payload.sent),
                payload.expires_at,
                ctx.http.clone(),
            );
            ctx.events
                .emit(ClientEvent::PartyJoinRequestReceived(request));
        }
        EOSConnectNotification::PartyMemberJoined { payload, .. } => {
            let active_party = ctx.party_slot.read().await.clone();
            if let Some(party) = active_party
                && party.eos_id() == &payload.party_id
            {
                if let Err(err) = party.handle_stomp_member_joined(payload).await {
                    debug!(error = %err, "failed to process STOMP MEMBER_JOINED");
                }
                sync_eos_internal_presence(ctx).await;
            }
        }
        EOSConnectNotification::PartyMemberLeft { payload, .. } => {
            handle_stomp_member_removal(ctx, payload, MemberRemovalReason::Left).await;
        }
        EOSConnectNotification::PartyMemberKicked { payload, .. } => {
            handle_stomp_member_removal(ctx, payload, MemberRemovalReason::Kicked).await;
        }
        EOSConnectNotification::PartyMemberExpiredAfterDisconnect { payload, .. } => {
            handle_stomp_member_removal(ctx, payload, MemberRemovalReason::Expired).await;
        }
        EOSConnectNotification::PartyMemberExpiredPartyDisbanded { payload, .. } => {
            handle_stomp_party_disbanded(ctx, payload.party_id).await;
        }
        EOSConnectNotification::PartyMemberDisconnected { payload, .. } => {
            let active_party = ctx.party_slot.read().await.clone();
            if let Some(party) = active_party
                && party.eos_id() == &payload.party_id
                && let Some(snap) = party.snapshot().await
                && let Some(member) = snap.member(&payload.account_id).cloned()
            {
                ctx.events.emit(ClientEvent::PartyMemberDisconnected {
                    party_id: snap.id.clone(),
                    member,
                });
            }
        }
        EOSConnectNotification::PartyUpdated { payload, .. } => {
            let active_party = ctx.party_slot.read().await.clone();
            if let Some(party) = active_party
                && party.eos_id() == &payload.party_id
            {
                if let Err(err) = party.handle_stomp_party_updated(payload).await {
                    debug!(error = %err, "failed to process STOMP PARTY_UPDATED");
                }
                sync_eos_internal_presence(ctx).await;
            }
        }
        EOSConnectNotification::MatchmakingEvent {
            event_type,
            payload,
            ..
        } => {
            ctx.events.emit(ClientEvent::MatchmakingEvent {
                event_type,
                payload,
            });
        }
        EOSConnectNotification::SocialChatMembersLeft { .. }
        | EOSConnectNotification::PartyMemberStateUpdated { .. }
        | EOSConnectNotification::PartyMemberConnected { .. }
        | EOSConnectNotification::PartyMemberRefreshSummary { .. }
        | EOSConnectNotification::Unknown { .. } => {}
    }
}

async fn handle_stomp_member_removal(
    ctx: &RealtimeEventContext,
    payload: EOSPartyMemberUpdateData,
    reason: MemberRemovalReason,
) {
    let active_party = ctx.party_slot.read().await.clone();
    let Some(party) = active_party else {
        return;
    };
    if party.eos_id() != &payload.party_id {
        return;
    }
    let was_self = payload.account_id == ctx.self_id;
    let eos_id = payload.party_id.clone();

    if let Err(err) = party.handle_stomp_member_removed(payload, reason).await {
        debug!(error = %err, ?reason, "failed to handle STOMP member removal");
    }

    if was_self {
        let mut slot_guard = ctx.party_slot.write().await;
        if slot_guard.as_ref().is_some_and(|p| p.eos_id() == &eos_id) {
            *slot_guard = None;
        }
        drop(slot_guard);
    }

    sync_eos_internal_presence(ctx).await;
}

async fn handle_stomp_party_disbanded(ctx: &RealtimeEventContext, party_id: PartyId) {
    let disbanded = {
        let mut slot_guard = ctx.party_slot.write().await;
        if slot_guard.as_ref().is_some_and(|p| p.eos_id() == &party_id) {
            slot_guard.take()
        } else {
            None
        }
    };

    if let Some(party) = disbanded {
        let fn_id = party.fn_id().clone();
        party.terminate().await;
        ctx.events.emit(ClientEvent::PartyLeft {
            party_id: fn_id,
            eos_party_id: party_id,
        });
        sync_eos_internal_presence(ctx).await;
    }
}

async fn sync_eos_internal_presence(ctx: &RealtimeEventContext) {
    let Some(stomp) = &ctx.stomp else {
        return;
    };
    let Some(conn) = stomp.connection_ids() else {
        return;
    };
    let active_party = ctx.party_slot.read().await.clone();
    let presence = match active_party {
        Some(party) => party.snapshot().await.map(|s| s.to_eos_internal_presence()),
        None => None,
    };
    if let Err(err) = ctx
        .http
        .patch_eos_internal_presence(&ctx.self_id, &conn.private_connection_id, presence.as_ref(), None)
        .await
    {
        debug!(account_id = %ctx.self_id, error = %err, "background EOS internal presence sync failed");
    }
}

fn emit_friend_transition(bus: &ClientEventBus, transition: FriendTransition) {
    let event = match transition {
        FriendTransition::Added(friend) => ClientEvent::FriendAdded(friend),
        FriendTransition::RequestAccepted(friend) => ClientEvent::FriendRequestAccepted(friend),
        FriendTransition::RequestReceived(pending) => ClientEvent::FriendRequestReceived(pending),
        FriendTransition::Removed(friend) => ClientEvent::FriendRemoved(friend),
        FriendTransition::RequestCancelled(pending) => ClientEvent::FriendRequestCancelled(pending),
        FriendTransition::RequestDeclined(pending) => ClientEvent::FriendRequestDeclined(pending),
        FriendTransition::Blocked(user) => ClientEvent::BlockedUserAdded(user),
        FriendTransition::Unblocked(account_id) => ClientEvent::BlockedUserRemoved(account_id),
    };
    bus.emit(event);
}

async fn resolve_display_name(
    ctx: &RealtimeEventContext,
    account_id: &AccountId,
    fallback: Option<String>,
) -> Option<String> {
    if let Some(friend) = ctx.friends.get(account_id)
        && friend.display_name.is_some()
    {
        return friend.display_name;
    }
    if let Some(cached) = ctx.users.get_cached(account_id)
        && cached.display_name.is_some()
    {
        return cached.display_name;
    }
    if fallback.is_some() {
        return fallback;
    }
    ctx.users
        .fetch(account_id)
        .await
        .ok()
        .and_then(|profile| profile.display_name)
}

fn parse_account_from_jid(jid: &str) -> Option<AccountId> {
    let node = jid.split('@').next()?;
    let bare = node.split('/').next()?;
    AccountId::parse(bare).ok()
}

fn stomp_time_to_datetime(raw: u64) -> DateTime<Utc> {
    if raw == 0 {
        return Utc::now();
    }
    if raw >= 100_000_000_000 {
        DateTime::from_timestamp_millis(raw as i64).unwrap_or_else(Utc::now)
    } else {
        DateTime::from_timestamp(raw as i64, 0).unwrap_or_else(Utc::now)
    }
}
