use crate::party_member::{ClientPartyMember, MemberDeltaChanges, PartyMemberSnapshot};
use crate::party_member_meta::PartyMemberMeta;
use crate::party_meta::{IslandInfo, PartyMeta, calculate_squad_assignments};
use chrono::{DateTime, Utc};
use rustfn_chat::ChatManager;
use rustfn_friends::FriendManager;
use rustfn_http::{
    EOSInternalPartyPresence, FortnitePartyConfig, FortnitePartyData, HTTP, RetryDecision,
    RetryDecisionHook,
};
use rustfn_protocol::{
    AccountId, FortnitePartyPrivacy, LobbyId, MetaMap, MetaPatch, PartyConfig,
    PartyDiscoverability, PartyId, PartyJoinability, PartyPrivacyPreset, PartyType, Platform,
    ProductUserId, Region, RustfnError, SquadPositionPriority,
};
use rustfn_stomp::{
    EOSPartyData, EOSPartyDataConfig, EOSPartyMemberUpdateData, EOSPartyUpdateData,
};
use rustfn_users::UserManager;
use rustfn_xmpp::{
    XMPPMetadataStore, XMPPPartyMemberConfirmationNotification, XMPPPartyMemberDeltaNotification,
    XMPPPartyUpdatedNotification,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicI8, Ordering};
use tokio::sync::{Mutex, RwLock, broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Outgoing EOS Party v2 invitation record created after invoking `ClientParty::invite`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SentPartyInvitation {
    pub eos_party_id: PartyId,
    pub sender_id: AccountId,
    pub recipient_id: AccountId,
    pub sent_at: String,
    pub expires_at: String,
}

/// Incoming EOS Party v2 invitation received via STOMP (`party.v2.INVITE_CREATED`).
#[derive(Debug, Clone)]
pub struct ReceivedPartyInvitation {
    pub eos_party_id: PartyId,
    pub inviter_id: AccountId,
    pub inviter_display_name: Option<String>,
    pub self_id: AccountId,
    pub sent_at: Option<String>,
    pub expires_at: Option<String>,
    pub meta: BTreeMap<String, String>,
    pub(crate) http: Option<HTTP>,
}

impl ReceivedPartyInvitation {
    pub fn from_stomp(
        eos_party_id: PartyId,
        inviter_id: AccountId,
        inviter_display_name: Option<String>,
        self_id: AccountId,
        sent_at: Option<String>,
        expires_at: Option<String>,
        meta: BTreeMap<String, String>,
        http: HTTP,
    ) -> Self {
        Self {
            eos_party_id,
            inviter_id,
            inviter_display_name,
            self_id,
            sent_at,
            expires_at,
            meta,
            http: Some(http),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .is_some_and(|dt| dt.with_timezone(&Utc) <= Utc::now())
    }

    /// Declines and removes the pending EOS Party v2 invite on Epic's servers.
    pub async fn decline(&self) -> Result<(), RustfnError> {
        let http = self.http.as_ref().ok_or_else(|| {
            RustfnError::PartyState("invitation does not hold an HTTP client reference".to_owned())
        })?;
        http.delete_eos_party_invite(&self.self_id, &self.inviter_id)
            .await
    }
}

/// Incoming EOS Party v2 join request (`party.v2.JOIN_REQUEST_CREATED`).
#[derive(Debug, Clone)]
pub struct ReceivedPartyJoinRequest {
    pub requester_id: AccountId,
    pub requester_display_name: Option<String>,
    pub self_id: AccountId,
    pub sent_at: Option<String>,
    pub expires_at: Option<String>,
    pub(crate) http: Option<HTTP>,
}

impl ReceivedPartyJoinRequest {
    pub fn from_stomp(
        requester_id: AccountId,
        requester_display_name: Option<String>,
        self_id: AccountId,
        sent_at: Option<String>,
        expires_at: Option<String>,
        http: HTTP,
    ) -> Self {
        Self {
            requester_id,
            requester_display_name,
            self_id,
            sent_at,
            expires_at,
            http: Some(http),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .is_some_and(|dt| dt.with_timezone(&Utc) <= Utc::now())
    }

    /// Accepts a join request by sending an EOS Party v2 invite back to the requester.
    pub async fn accept(&self) -> Result<(), RustfnError> {
        let http = self.http.as_ref().ok_or_else(|| {
            RustfnError::PartyState("join request does not hold an HTTP client reference".to_owned())
        })?;
        http.invite_eos_party(&self.requester_id, &self.self_id)
            .await
    }

    /// Declines and deletes the pending EOS join request.
    pub async fn decline(&self) -> Result<(), RustfnError> {
        let http = self.http.as_ref().ok_or_else(|| {
            RustfnError::PartyState("join request does not hold an HTTP client reference".to_owned())
        })?;
        http.decline_eos_join_request(&self.self_id, &self.requester_id)
            .await
    }
}

/// Pending leader confirmation (`MEMBER_REQUIRE_CONFIRMATION`) when `join_confirmation` is active.
#[derive(Debug, Clone)]
pub struct PendingMemberConfirmation {
    pub party_id: LobbyId,
    pub account_id: AccountId,
    pub account_display_name: Option<String>,
    pub sent_at: Option<String>,
    pub(crate) http: Option<HTTP>,
}

impl PendingMemberConfirmation {
    pub fn new(
        party_id: LobbyId,
        account_id: AccountId,
        account_display_name: Option<String>,
        sent_at: Option<String>,
        http: HTTP,
    ) -> Self {
        Self {
            party_id,
            account_id,
            account_display_name,
            sent_at,
            http: Some(http),
        }
    }

    /// Approves the pending member join request so they finish entering the lobby.
    pub async fn confirm(&self) -> Result<(), RustfnError> {
        let http = self.http.as_ref().ok_or_else(|| {
            RustfnError::PartyState(
                "member confirmation does not hold an HTTP client reference".to_owned(),
            )
        })?;
        http.confirm_fn_party_member(&self.party_id, &self.account_id, None)
            .await
    }

    /// Rejects the pending member join request.
    pub async fn reject(&self) -> Result<(), RustfnError> {
        let http = self.http.as_ref().ok_or_else(|| {
            RustfnError::PartyState(
                "member confirmation does not hold an HTTP client reference".to_owned(),
            )
        })?;
        http.reject_fn_party_member(&self.party_id, &self.account_id, None)
            .await
    }
}

/// Message sent or received within a Fortnite / EOS party conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartyChatMessage {
    pub id: Option<String>,
    pub party_id: LobbyId,
    pub eos_party_id: PartyId,
    pub author_id: AccountId,
    pub content: String,
    pub timestamp_ms: u64,
}

/// Signed EOS party chat sender scoped to the active party snapshot.
#[derive(Clone)]
pub struct PartyChat {
    self_id: AccountId,
    fn_party_id: LobbyId,
    eos_party_id: PartyId,
    chat: Arc<ChatManager>,
    snapshot_slot: Arc<RwLock<Option<PartySnapshot>>>,
    is_reportable_cache: Arc<AtomicI8>,
    chat_enabled: bool,
}

impl std::fmt::Debug for PartyChat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartyChat")
            .field("fn_party_id", &self.fn_party_id)
            .field("eos_party_id", &self.eos_party_id)
            .field("chat_enabled", &self.chat_enabled)
            .finish()
    }
}

impl PartyChat {
    pub fn new(
        self_id: AccountId,
        fn_party_id: LobbyId,
        eos_party_id: PartyId,
        initial_is_reportable: Option<bool>,
        chat: Arc<ChatManager>,
        snapshot_slot: Arc<RwLock<Option<PartySnapshot>>>,
        chat_enabled: bool,
    ) -> Self {
        let initial = match initial_is_reportable {
            Some(true) => 1,
            Some(false) => 0,
            None => -1,
        };
        Self {
            self_id,
            fn_party_id,
            eos_party_id,
            chat,
            snapshot_slot,
            is_reportable_cache: Arc::new(AtomicI8::new(initial)),
            chat_enabled,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.chat_enabled
    }

    /// Sends a signed party chat message to all other party members.
    ///
    /// Fails on single-member parties with no other recipients or when `PartyConfig::chat_enabled` is false.
    pub async fn send(&self, content: &str) -> Result<PartyChatMessage, RustfnError> {
        if !self.chat_enabled {
            return Err(RustfnError::PartyState(
                "party chat is disabled by PartyConfig::chat_enabled".to_owned(),
            ));
        }
        let recipient_ids: Vec<AccountId> = {
            let guard = self.snapshot_slot.read().await;
            let snapshot = guard.as_ref().ok_or_else(|| {
                RustfnError::PartyState("cannot send party chat after leaving party".to_owned())
            })?;
            if snapshot.id != self.fn_party_id {
                return Err(RustfnError::PartyState(
                    "party snapshot no longer matches chat session".to_owned(),
                ));
            }
            if snapshot.members.len() < 2 {
                return Err(RustfnError::PartyState(
                    "cannot send party chat with fewer than 2 party members".to_owned(),
                ));
            }
            snapshot
                .members
                .keys()
                .filter(|id| *id != &self.self_id)
                .cloned()
                .collect()
        };

        let hint = match self.is_reportable_cache.load(Ordering::Relaxed) {
            1 => Some(true),
            0 => Some(false),
            _ => None,
        };

        let sent = self
            .chat
            .send_party_message(
                &self.fn_party_id.as_party_id(),
                (!self.eos_party_id.as_str().is_empty()).then_some(&self.eos_party_id),
                &recipient_ids,
                hint,
                content,
            )
            .await?;

        self.is_reportable_cache.store(
            if sent.resolved_is_reportable { 1 } else { 0 },
            Ordering::Relaxed,
        );

        let now_ms = u64::try_from(Utc::now().timestamp_millis().max(0)).unwrap_or(0);
        Ok(PartyChatMessage {
            id: Some(sent.message_id),
            party_id: self.fn_party_id.clone(),
            eos_party_id: self.eos_party_id.clone(),
            author_id: self.self_id.clone(),
            content: content.to_owned(),
            timestamp_ms: now_ms,
        })
    }
}

trait LobbyIdExt {
    fn as_party_id(&self) -> PartyId;
}

impl LobbyIdExt for LobbyId {
    fn as_party_id(&self) -> PartyId {
        PartyId::from_raw(self.as_str())
    }
}

/// Combined Fortnite + Privacy party configuration snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartyRuntimeConfig {
    pub party_type: String,
    pub joinability: PartyJoinability,
    pub discoverability: PartyDiscoverability,
    pub sub_type: String,
    pub max_size: u32,
    pub invite_ttl: u64,
    pub intention_ttl: u64,
    pub join_confirmation: bool,
    pub privacy: FortnitePartyPrivacy,
}

impl PartyRuntimeConfig {
    pub fn from_fn_config(cfg: FortnitePartyConfig, meta: &PartyMeta) -> Self {
        let privacy = meta.matched_privacy().unwrap_or_else(|| {
            if cfg.joinability == PartyJoinability::Open {
                PartyPrivacyPreset::Public.to_privacy()
            } else {
                PartyPrivacyPreset::Private.to_privacy()
            }
        });
        Self {
            party_type: cfg.r#type,
            joinability: cfg.joinability,
            discoverability: cfg.discoverability,
            sub_type: cfg.sub_type,
            max_size: cfg.max_size,
            invite_ttl: cfg.invite_ttl,
            intention_ttl: cfg.intention_ttl,
            join_confirmation: cfg.join_confirmation,
            privacy,
        }
    }

    pub fn to_fn_config(&self) -> FortnitePartyConfig {
        FortnitePartyConfig {
            r#type: self.party_type.clone(),
            joinability: self.joinability,
            discoverability: self.discoverability,
            sub_type: self.sub_type.clone(),
            max_size: self.max_size,
            invite_ttl: self.invite_ttl,
            join_confirmation: self.join_confirmation,
            intention_ttl: self.intention_ttl,
        }
    }
}

/// Immutable read-only snapshot of a Fortnite + EOS dual-protocol party.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartySnapshot {
    pub id: LobbyId,
    pub eos_id: PartyId,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub leader_id: AccountId,
    pub chat_conversation_id: String,
    pub is_reportable: bool,
    pub revision: u64,
    pub eos_revision: u64,
    pub config: PartyRuntimeConfig,
    pub eos_config: EOSPartyDataConfig,
    pub meta: PartyMeta,
    pub members: BTreeMap<AccountId, PartyMemberSnapshot>,
    pub hidden_member_ids: BTreeSet<AccountId>,
}

impl PartySnapshot {
    pub fn size(&self) -> usize {
        self.members.len()
    }

    pub fn max_size(&self) -> u32 {
        self.config.max_size
    }

    pub fn is_private(&self) -> bool {
        self.config.privacy.party_type == PartyType::Private
    }

    pub fn leader(&self) -> Option<&PartyMemberSnapshot> {
        self.members.get(&self.leader_id)
    }

    pub fn member(&self, id: &AccountId) -> Option<&PartyMemberSnapshot> {
        self.members.get(id)
    }

    pub fn find_member(&self, id_or_name: &str) -> Option<&PartyMemberSnapshot> {
        if let Ok(id) = AccountId::parse(id_or_name)
            && let Some(member) = self.members.get(&id)
        {
            return Some(member);
        }
        self.members.values().find(|member| {
            member.account_id.as_str() == id_or_name
                || member
                    .display_name
                    .as_deref()
                    .is_some_and(|dn| dn.eq_ignore_ascii_case(id_or_name))
        })
    }

    pub fn to_eos_internal_presence(&self) -> EOSInternalPartyPresence {
        EOSInternalPartyPresence {
            party_type: self.eos_config.joinability,
            id: self.eos_id.clone(),
            client_joinable: self.eos_config.joinability == PartyJoinability::Open,
            member_count: self.members.len(),
            timestamp: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        }
    }
}

#[derive(Debug, Clone)]
pub struct STOMPJoinOutcome {
    pub member: PartyMemberSnapshot,
    pub replayed_deltas: Vec<MemberDeltaChanges>,
    pub is_self: bool,
    pub needs_profile_fetch: bool,
}

/// Mutable party state owned by `ClientParty`'s background task.
#[derive(Debug)]
pub struct PartyState {
    pub self_id: AccountId,
    pub self_display_name: String,
    pub platform: Platform,
    pub priorities: Vec<SquadPositionPriority>,
    pub snapshot: PartySnapshot,
    pub pending_confirmations: HashMap<AccountId, PendingMemberConfirmation>,
}

impl PartyState {
    pub fn from_joined_data(
        self_id: AccountId,
        self_puid: Option<&ProductUserId>,
        self_display_name: String,
        platform: Platform,
        region: Region,
        party_cfg: &PartyConfig,
        fn_data: FortnitePartyData,
        eos_data: EOSPartyData,
        saved_member_meta: Option<&MetaMap>,
        metadata_store: &XMPPMetadataStore,
    ) -> Self {
        let mut party_meta = PartyMeta::default_for_client(
            &eos_data.id,
            region,
            &party_cfg.privacy,
            Some(&fn_data.meta),
        );
        let build_id = fn_data
            .id
            .as_str()
            .split('-')
            .nth(1)
            .map(|cl| format!("1:3:{cl}"));
        party_meta.set_cfg_identity(build_id.as_deref(), party_cfg.chat_enabled);
        let runtime_cfg = PartyRuntimeConfig::from_fn_config(fn_data.config, &party_meta);
        let store_party_key = PartyId::from_raw(fn_data.id.as_str());

        let mut members = BTreeMap::new();
        for fn_member in fn_data.members {
            let member_id = fn_member.account_id.clone();
            let mut member = PartyMemberSnapshot::from_fn_data(fn_member, Some(&eos_data.party_lead));
            if member_id == self_id {
                let mut merged = PartyMemberMeta::default_for_client(platform, None);
                let server_patch = MetaPatch {
                    updated: member.meta.as_meta().as_map().clone(),
                    deleted: Vec::new(),
                };
                merged.apply_patch(&server_patch);
                if let Some(saved) = saved_member_meta {
                    let saved_patch = MetaPatch {
                        updated: saved.as_map().clone(),
                        deleted: Vec::new(),
                    };
                    merged.apply_patch(&saved_patch);
                }
                if let Some(puid) = self_puid {
                    merged.set_eos_product_user_id(puid.as_str());
                }
                member.meta = merged;
                if member.display_name.is_none() {
                    member.display_name = Some(self_display_name.clone());
                }
            } else {
                let buffered =
                    metadata_store.drain_sorted(&store_party_key, &member_id, member.revision);
                for delta in buffered {
                    let _ = member.apply_delta(
                        delta.revision,
                        delta.account_dn,
                        &delta.member_state_updated,
                        &delta.member_state_removed,
                    );
                }
            }
            members.insert(member_id, member);
        }

        let member_ids: Vec<AccountId> = members.keys().cloned().collect();
        let hidden = BTreeSet::new();
        let hidden_hash = std::collections::HashSet::new();
        let assignments = calculate_squad_assignments(
            &self_id,
            Some(&eos_data.party_lead),
            &member_ids,
            &hidden_hash,
            &party_cfg.squad_position_priorities,
        );
        let _ = party_meta.build_squad_assignments_patch(assignments);

        let snapshot = PartySnapshot {
            id: fn_data.id,
            eos_id: eos_data.id,
            created_at: fn_data.created_at,
            updated_at: fn_data.updated_at,
            leader_id: eos_data.party_lead,
            chat_conversation_id: eos_data.chat_conversation_id,
            is_reportable: eos_data.is_reportable,
            revision: fn_data.revision,
            eos_revision: eos_data.revision,
            config: runtime_cfg,
            eos_config: eos_data.config,
            meta: party_meta,
            members,
            hidden_member_ids: hidden,
        };

        Self {
            self_id,
            self_display_name,
            platform,
            priorities: party_cfg.squad_position_priorities.clone(),
            snapshot,
            pending_confirmations: HashMap::new(),
        }
    }

    pub fn apply_xmpp_party_updated(
        &mut self,
        msg: &XMPPPartyUpdatedNotification,
    ) -> Option<PartySnapshot> {
        if msg.party_id.as_str() != self.snapshot.id.as_str() {
            return None;
        }
        if msg.revision > self.snapshot.revision {
            self.snapshot.revision = msg.revision;
        }
        let patch = MetaPatch {
            updated: msg.party_state_updated.as_map().clone(),
            deleted: msg.party_state_removed.clone(),
        };
        self.snapshot.meta.apply_patch(&patch);

        if let Some(raw_joinability) = &msg.party_privacy_type
            && let Ok(parsed) = serde_json::from_str::<PartyJoinability>(&format!("\"{raw_joinability}\""))
        {
            self.snapshot.config.joinability = parsed;
        }
        if let Some(max_members) = msg.max_number_of_members {
            self.snapshot.config.max_size = max_members;
        }
        if let Some(party_type) = &msg.party_type {
            self.snapshot.config.party_type = party_type.clone();
        }
        if let Some(sub_type) = &msg.party_sub_type {
            self.snapshot.config.sub_type = sub_type.clone();
        }
        if let Some(ttl) = msg.invite_ttl_seconds {
            self.snapshot.config.invite_ttl = ttl;
        }
        if let Some(privacy) = self.snapshot.meta.matched_privacy() {
            self.snapshot.config.privacy = privacy;
        }
        if msg.updated_at.is_some() {
            self.snapshot.updated_at = msg.updated_at.clone();
        }
        Some(self.snapshot.clone())
    }

    pub fn apply_stomp_party_updated(
        &mut self,
        msg: &EOSPartyUpdateData,
    ) -> Option<(Option<PartyMemberSnapshot>, PartySnapshot)> {
        if msg.party_id != self.snapshot.eos_id || msg.revision <= self.snapshot.eos_revision {
            return None;
        }

        let prev_leader = self.snapshot.leader_id.clone();
        self.snapshot.eos_revision = msg.revision;
        self.snapshot.leader_id = msg.party_lead.clone();
        if !msg.chat_conversation_id.is_empty() {
            self.snapshot.chat_conversation_id = msg.chat_conversation_id.clone();
        }
        self.snapshot.is_reportable = msg.is_reportable;
        self.snapshot.eos_config = EOSPartyDataConfig {
            joinability: msg.party_privacy_type,
            max_size: msg.max_number_of_members,
        };
        if msg.updated_at.is_some() {
            self.snapshot.updated_at = msg.updated_at.clone();
        }

        for (id, member) in &mut self.snapshot.members {
            member.is_leader = id == &msg.party_lead;
        }

        let promoted = (msg.party_lead != prev_leader)
            .then(|| self.snapshot.members.get(&msg.party_lead).cloned())
            .flatten();
        Some((promoted, self.snapshot.clone()))
    }

    /// Reconciles an XMPP `MEMBER_JOINED` or `MEMBER_STATE_UPDATED` delta.
    ///
    /// If the target member has not yet arrived via STOMP `party.v2.MEMBER_JOINED`, pushes the
    /// delta into `metadata_store` for ordered replay once STOMP creates the slot.
    pub fn apply_xmpp_member_delta(
        &mut self,
        msg: XMPPPartyMemberDeltaNotification,
        metadata_store: &XMPPMetadataStore,
    ) -> Option<(PartyMemberSnapshot, MemberDeltaChanges)> {
        if msg.party_id.as_str() != self.snapshot.id.as_str() {
            return None;
        }

        if let Some(member) = self.snapshot.members.get_mut(&msg.account_id) {
            let changes = member.apply_delta(
                msg.revision,
                msg.account_dn,
                &msg.member_state_updated,
                &msg.member_state_removed,
            )?;
            return Some((member.clone(), changes));
        }

        metadata_store.push(self.fn_store_party_key(), msg.into());
        None
    }

    pub fn apply_stomp_member_joined(
        &mut self,
        msg: &EOSPartyMemberUpdateData,
        metadata_store: &XMPPMetadataStore,
    ) -> Option<STOMPJoinOutcome> {
        if msg.party_id != self.snapshot.eos_id {
            return None;
        }

        let member_id = msg.account_id.clone();
        let is_self = member_id == self.self_id;
        let is_leader = member_id == self.snapshot.leader_id;

        if is_self {
            self.snapshot
                .members
                .entry(member_id.clone())
                .or_insert_with(|| {
                    let mut m = PartyMemberSnapshot::new_skeleton(
                        member_id.clone(),
                        msg.account_dn
                            .clone()
                            .or_else(|| Some(self.self_display_name.clone())),
                        msg.joined_at.clone(),
                        msg.updated_at.clone(),
                        is_leader,
                    );
                    m.meta = PartyMemberMeta::default_for_client(self.platform, None);
                    m
                });
        } else {
            self.snapshot.members.insert(
                member_id.clone(),
                PartyMemberSnapshot::new_skeleton(
                    member_id.clone(),
                    msg.account_dn.clone(),
                    msg.joined_at.clone(),
                    msg.updated_at.clone(),
                    is_leader,
                ),
            );
        }

        let buffered = metadata_store.drain_sorted(&self.fn_store_party_key(), &member_id, 0);
        let mut replayed_deltas = Vec::with_capacity(buffered.len());
        if let Some(member) = self.snapshot.members.get_mut(&member_id) {
            for delta in buffered {
                if let Some(changes) = member.apply_delta(
                    delta.revision,
                    delta.account_dn,
                    &delta.member_state_updated,
                    &delta.member_state_removed,
                ) {
                    replayed_deltas.push(changes);
                }
            }
        }

        let member = self.snapshot.members.get(&member_id)?.clone();
        let needs_profile_fetch = member.display_name.is_none();
        Some(STOMPJoinOutcome {
            member,
            replayed_deltas,
            is_self,
            needs_profile_fetch,
        })
    }

    pub fn apply_stomp_member_removed(
        &mut self,
        party_id: &PartyId,
        account_id: &AccountId,
        metadata_store: &XMPPMetadataStore,
    ) -> Option<(PartyMemberSnapshot, bool)> {
        if party_id != &self.snapshot.eos_id {
            return None;
        }
        metadata_store.discard(&self.fn_store_party_key(), account_id);
        self.snapshot.hidden_member_ids.remove(account_id);
        self.pending_confirmations.remove(account_id);
        let removed = self.snapshot.members.remove(account_id)?;
        let was_self = removed.account_id == self.self_id;
        Some((removed, was_self))
    }

    pub fn compute_squad_assignments_patch(&mut self) -> Result<MetaPatch, RustfnError> {
        let member_ids: Vec<AccountId> = self.snapshot.members.keys().cloned().collect();
        let hidden_set: std::collections::HashSet<AccountId> =
            self.snapshot.hidden_member_ids.iter().cloned().collect();
        let assignments = calculate_squad_assignments(
            &self.self_id,
            Some(&self.snapshot.leader_id),
            &member_ids,
            &hidden_set,
            &self.priorities,
        );
        self.snapshot.meta.build_squad_assignments_patch(assignments)
    }

    pub fn is_self_leader(&self) -> bool {
        self.self_id == self.snapshot.leader_id
    }

    fn fn_store_party_key(&self) -> PartyId {
        PartyId::from_raw(self.snapshot.id.as_str())
    }
}

const MAX_STALE_REVISION_RETRIES: usize = 5;
const ACTOR_QUEUE_CAPACITY: usize = 256;

/// Reason a remote or local member was removed from an EOS Party v2 session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberRemovalReason {
    Left,
    Kicked,
    Expired,
}

type MemberMetaUpdater =
    Box<dyn FnOnce(&mut PartyMemberMeta) -> Result<MetaPatch, RustfnError> + Send>;

enum PartyCommand {
    SendPartyPatch {
        patch: MetaPatch,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    UpdateSelfMeta {
        updater: MemberMetaUpdater,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    SendSelfMemberPatch {
        patch: MetaPatch,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    SetSelfEmote {
        emote_id: String,
        path: Option<String>,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    SetPrivacy {
        privacy: FortnitePartyPrivacy,
        send_patch: bool,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    SetPlaylist {
        mnemonic: String,
        region: Option<String>,
        version: Option<i64>,
        options: Option<IslandInfo>,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    SetRegion {
        region: Region,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    SetCustomMatchmakingKey {
        key: Option<String>,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    SetSquadFill {
        fill: bool,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    SetMaxSize {
        max_size: u32,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    RefreshSquadAssignments {
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    HideMember {
        member: String,
        hide: bool,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    HideAllMembers {
        hide: bool,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    Kick {
        member: String,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    Promote {
        member: String,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    Invite {
        friend: String,
        tx: oneshot::Sender<Result<SentPartyInvitation, RustfnError>>,
    },
    Leave {
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    XMPPMemberDelta {
        msg: XMPPPartyMemberDeltaNotification,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    XMPPPartyUpdated {
        msg: XMPPPartyUpdatedNotification,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    XMPPConfirmation {
        msg: XMPPPartyMemberConfirmationNotification,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    STOMPMemberJoined {
        msg: EOSPartyMemberUpdateData,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    STOMPMemberRemoved {
        msg: EOSPartyMemberUpdateData,
        reason: MemberRemovalReason,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
    STOMPPartyUpdated {
        msg: EOSPartyUpdateData,
        tx: oneshot::Sender<Result<(), RustfnError>>,
    },
}

/// Party-domain events emitted by [`ClientParty`] without depending on downstream `rustfn-events`.
#[derive(Debug, Clone)]
pub enum PartyEvent {
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
    PartyMemberConfirmationReceived(PendingMemberConfirmation),
}

/// Sink receiving [`PartyEvent`] updates from an active [`ClientParty`].
pub trait PartyEventSink: Send + Sync + 'static {
    fn emit_party_event(&self, event: PartyEvent) -> usize;
}

/// Standalone broadcast bus implementing [`PartyEventSink`] for direct `rustfn-party` consumers and tests.
#[derive(Clone)]
pub struct PartyEventBus {
    sender: broadcast::Sender<Arc<PartyEvent>>,
}

impl PartyEventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(16));
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<PartyEvent>> {
        self.sender.subscribe()
    }
}

impl PartyEventSink for PartyEventBus {
    fn emit_party_event(&self, event: PartyEvent) -> usize {
        let _ = self.sender.send(Arc::new(event));
        self.sender.receiver_count()
    }
}

/// Dependencies bundled to initialize an active [`ClientParty`].
pub struct ClientPartyInit {
    pub self_id: AccountId,
    pub self_puid: Option<ProductUserId>,
    pub self_display_name: String,
    pub platform: Platform,
    pub region: Region,
    pub party_config: PartyConfig,
    pub save_member_meta: bool,
    pub fn_data: FortnitePartyData,
    pub eos_data: EOSPartyData,
    pub http: HTTP,
    pub metadata_store: Arc<XMPPMetadataStore>,
    pub users: Arc<UserManager>,
    pub friends: Arc<FriendManager>,
    pub chat: Arc<ChatManager>,
    pub events: Arc<dyn PartyEventSink>,
    pub snapshot_slot: Arc<RwLock<Option<PartySnapshot>>>,
    pub saved_member_meta: Arc<RwLock<Option<MetaMap>>>,
    pub parent_cancel: CancellationToken,
}

/// Controller for the joined Fortnite v1 + EOS Party v2 session.
#[derive(Clone)]
pub struct ClientParty {
    fn_id: LobbyId,
    eos_id: PartyId,
    snapshot_slot: Arc<RwLock<Option<PartySnapshot>>>,
    chat: PartyChat,
    cmd_tx: mpsc::Sender<PartyCommand>,
    party_cancel: CancellationToken,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl std::fmt::Debug for ClientParty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientParty")
            .field("fn_id", &self.fn_id)
            .field("eos_id", &self.eos_id)
            .field("cancelled", &self.party_cancel.is_cancelled())
            .finish()
    }
}

impl ClientParty {
    pub async fn spawn(init: ClientPartyInit) -> Self {
        let saved_guard = init.saved_member_meta.read().await.clone();
        let state = PartyState::from_joined_data(
            init.self_id.clone(),
            init.self_puid.as_ref(),
            init.self_display_name,
            init.platform,
            init.region,
            &init.party_config,
            init.fn_data,
            init.eos_data,
            saved_guard.as_ref(),
            &init.metadata_store,
        );

        let fn_id = state.snapshot.id.clone();
        let eos_id = state.snapshot.eos_id.clone();
        let is_reportable = Some(state.snapshot.is_reportable);

        *init.snapshot_slot.write().await = Some(state.snapshot.clone());

        let party_cancel = init.parent_cancel.child_token();
        let (cmd_tx, cmd_rx) = mpsc::channel(ACTOR_QUEUE_CAPACITY);

        let chat = PartyChat::new(
            init.self_id.clone(),
            fn_id.clone(),
            eos_id.clone(),
            is_reportable,
            Arc::clone(&init.chat),
            Arc::clone(&init.snapshot_slot),
            init.party_config.chat_enabled,
        );

        let retry_cancel = party_cancel.clone();
        let retry_hook: RetryDecisionHook = Arc::new(move || {
            if retry_cancel.is_cancelled() {
                RetryDecision::Abandon
            } else {
                RetryDecision::Retry
            }
        });

        let keepalive_handle = spawn_eos_keep_alive(
            init.http.clone(),
            fn_id.clone(),
            eos_id.clone(),
            init.self_id.clone(),
            init.party_config.eos_keep_alive_interval,
            retry_hook.clone(),
            Arc::clone(&init.events),
            Arc::clone(&init.snapshot_slot),
            party_cancel.child_token(),
        );

        let mut party_task = PartyTask {
            state,
            party_config: init.party_config,
            save_member_meta: init.save_member_meta,
            http: init.http,
            metadata_store: init.metadata_store,
            users: init.users,
            friends: init.friends,
            events: init.events,
            snapshot_slot: Arc::clone(&init.snapshot_slot),
            saved_member_meta: init.saved_member_meta,
            party_cancel: party_cancel.clone(),
            retry_hook,
        };

        let task_cancel = party_cancel.clone();
        let task_handle = tokio::spawn(async move {
            party_task.run(cmd_rx, task_cancel).await;
        });

        Self {
            fn_id,
            eos_id,
            snapshot_slot: init.snapshot_slot,
            chat,
            cmd_tx,
            party_cancel,
            tasks: Arc::new(Mutex::new(vec![keepalive_handle, task_handle])),
        }
    }

    pub fn fn_id(&self) -> &LobbyId {
        &self.fn_id
    }

    pub fn eos_id(&self) -> &PartyId {
        &self.eos_id
    }

    pub fn me(&self) -> ClientPartyMember {
        ClientPartyMember::new(self.clone())
    }

    pub fn chat(&self) -> &PartyChat {
        &self.chat
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.party_cancel.clone()
    }

    pub async fn snapshot(&self) -> Option<PartySnapshot> {
        self.snapshot_slot.read().await.clone()
    }

    async fn call<T>(&self, make_cmd: impl FnOnce(oneshot::Sender<Result<T, RustfnError>>) -> PartyCommand) -> Result<T, RustfnError> {
        if self.party_cancel.is_cancelled() {
            return Err(RustfnError::Cancelled);
        }
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(make_cmd(tx))
            .await
            .map_err(|_| RustfnError::Cancelled)?;
        rx.await.map_err(|_| RustfnError::Cancelled)?
    }

    pub async fn send_patch(&self, patch: MetaPatch) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::SendPartyPatch { patch, tx })
            .await
    }

    pub async fn send_member_patch(&self, patch: MetaPatch) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::SendSelfMemberPatch { patch, tx })
            .await
    }

    pub async fn update_self_meta<F>(&self, f: F) -> Result<(), RustfnError>
    where
        F: FnOnce(&mut PartyMemberMeta) -> Result<MetaPatch, RustfnError> + Send + 'static,
    {
        self.call(|tx| PartyCommand::UpdateSelfMeta {
            updater: Box::new(f),
            tx,
        })
        .await
    }

    pub(crate) async fn set_self_emote(
        &self,
        emote_id: String,
        path: Option<String>,
    ) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::SetSelfEmote { emote_id, path, tx })
            .await
    }

    pub async fn send_message(&self, content: &str) -> Result<PartyChatMessage, RustfnError> {
        self.chat.send(content).await
    }

    pub async fn set_privacy(
        &self,
        privacy: FortnitePartyPrivacy,
        send_patch: bool,
    ) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::SetPrivacy {
            privacy,
            send_patch,
            tx,
        })
        .await
    }

    pub async fn set_playlist(
        &self,
        mnemonic: impl Into<String>,
        region: Option<String>,
        version: Option<i64>,
        options: Option<IslandInfo>,
    ) -> Result<(), RustfnError> {
        let mnemonic = mnemonic.into();
        self.call(|tx| PartyCommand::SetPlaylist {
            mnemonic,
            region,
            version,
            options,
            tx,
        })
        .await
    }

    pub async fn set_region(&self, region: Region) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::SetRegion { region, tx }).await
    }

    pub async fn set_custom_matchmaking_key(
        &self,
        key: Option<String>,
    ) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::SetCustomMatchmakingKey { key, tx })
            .await
    }

    pub async fn set_squad_fill(&self, fill: bool) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::SetSquadFill { fill, tx }).await
    }

    pub async fn set_max_size(&self, max_size: u32) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::SetMaxSize { max_size, tx })
            .await
    }

    pub async fn refresh_squad_assignments(&self) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::RefreshSquadAssignments { tx })
            .await
    }

    pub async fn hide_member(
        &self,
        member: impl Into<String>,
        hide: bool,
    ) -> Result<(), RustfnError> {
        let member = member.into();
        self.call(|tx| PartyCommand::HideMember { member, hide, tx })
            .await
    }

    pub async fn hide_all_members(&self, hide: bool) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::HideAllMembers { hide, tx })
            .await
    }

    pub async fn kick(&self, member: impl Into<String>) -> Result<(), RustfnError> {
        let member = member.into();
        self.call(|tx| PartyCommand::Kick { member, tx }).await
    }

    pub async fn promote(&self, member: impl Into<String>) -> Result<(), RustfnError> {
        let member = member.into();
        self.call(|tx| PartyCommand::Promote { member, tx }).await
    }

    pub async fn invite(
        &self,
        friend: impl Into<String>,
    ) -> Result<SentPartyInvitation, RustfnError> {
        let friend = friend.into();
        self.call(|tx| PartyCommand::Invite { friend, tx }).await
    }

    pub async fn handle_xmpp_member_delta(
        &self,
        msg: XMPPPartyMemberDeltaNotification,
    ) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::XMPPMemberDelta { msg, tx })
            .await
    }

    pub async fn handle_xmpp_party_updated(
        &self,
        msg: XMPPPartyUpdatedNotification,
    ) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::XMPPPartyUpdated { msg, tx })
            .await
    }

    pub async fn handle_xmpp_confirmation(
        &self,
        msg: XMPPPartyMemberConfirmationNotification,
    ) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::XMPPConfirmation { msg, tx })
            .await
    }

    pub async fn handle_stomp_member_joined(
        &self,
        msg: EOSPartyMemberUpdateData,
    ) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::STOMPMemberJoined { msg, tx })
            .await
    }

    pub async fn handle_stomp_member_removed(
        &self,
        msg: EOSPartyMemberUpdateData,
        reason: MemberRemovalReason,
    ) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::STOMPMemberRemoved { msg, reason, tx })
            .await
    }

    pub async fn handle_stomp_party_updated(
        &self,
        msg: EOSPartyUpdateData,
    ) -> Result<(), RustfnError> {
        self.call(|tx| PartyCommand::STOMPPartyUpdated { msg, tx })
            .await
    }

    /// Leaves the Fortnite and EOS party cleanly and stops background tasks.
    pub async fn leave(&self) -> Result<(), RustfnError> {
        if self.party_cancel.is_cancelled() {
            self.join_tasks().await;
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        if self.cmd_tx.send(PartyCommand::Leave { tx }).await.is_ok() {
            let res = rx.await.map_err(|_| RustfnError::Cancelled)?;
            self.join_tasks().await;
            res
        } else {
            self.party_cancel.cancel();
            self.join_tasks().await;
            Ok(())
        }
    }

    /// Cancels background tasks without performing an HTTP leave.
    pub async fn terminate(&self) {
        self.party_cancel.cancel();
        *self.snapshot_slot.write().await = None;
        self.join_tasks().await;
    }

    async fn join_tasks(&self) {
        let handles = {
            let mut guard = self.tasks.lock().await;
            std::mem::take(&mut *guard)
        };
        for handle in handles {
            let _ = handle.await;
        }
    }
}

fn spawn_eos_keep_alive(
    http: HTTP,
    fn_id: LobbyId,
    eos_id: PartyId,
    self_id: AccountId,
    period: std::time::Duration,
    retry_hook: RetryDecisionHook,
    events: Arc<dyn PartyEventSink>,
    snapshot_slot: Arc<RwLock<Option<PartySnapshot>>>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    let period = period.max(std::time::Duration::from_millis(10));
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(err) = http.keep_alive_eos_party(&eos_id, &self_id, Some(retry_hook.clone())).await {
                        if cancel.is_cancelled() {
                            break;
                        }
                        warn!(party_id = %eos_id, error = %err, "EOS party keep-alive tick failed");
                        if err.is_not_found() {
                            *snapshot_slot.write().await = None;
                            cancel.cancel();
                            events.emit_party_event(PartyEvent::PartyLeft {
                                party_id: fn_id,
                                eos_party_id: eos_id,
                            });
                            break;
                        }
                    }
                }
            }
        }
    })
}

struct PartyTask {
    state: PartyState,
    party_config: PartyConfig,
    save_member_meta: bool,
    http: HTTP,
    metadata_store: Arc<XMPPMetadataStore>,
    users: Arc<UserManager>,
    friends: Arc<FriendManager>,
    events: Arc<dyn PartyEventSink>,
    snapshot_slot: Arc<RwLock<Option<PartySnapshot>>>,
    saved_member_meta: Arc<RwLock<Option<MetaMap>>>,
    party_cancel: CancellationToken,
    retry_hook: RetryDecisionHook,
}

impl PartyTask {
    async fn run(&mut self, mut cmd_rx: mpsc::Receiver<PartyCommand>, cancel: CancellationToken) {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                cmd = cmd_rx.recv() => {
                    let Some(cmd) = cmd else {
                        break;
                    };
                    if self.handle_command(cmd).await {
                        break;
                    }
                }
            }
        }
    }

    async fn publish_snapshot(&self) {
        *self.snapshot_slot.write().await = Some(self.state.snapshot.clone());
    }

    async fn clear_on_not_found(&self) {
        *self.snapshot_slot.write().await = None;
        self.party_cancel.cancel();
        self.events.emit_party_event(PartyEvent::PartyLeft {
            party_id: self.state.snapshot.id.clone(),
            eos_party_id: self.state.snapshot.eos_id.clone(),
        });
    }

    fn require_leader(&self) -> Result<(), RustfnError> {
        if self.state.is_self_leader() {
            Ok(())
        } else {
            Err(RustfnError::PartyState(
                "client is not the party leader".to_owned(),
            ))
        }
    }

    async fn handle_command(&mut self, cmd: PartyCommand) -> bool {
        match cmd {
            PartyCommand::SendPartyPatch { patch, tx } => {
                let res = self.send_fn_party_patch(patch, None).await;
                let _ = tx.send(res);
            }
            PartyCommand::UpdateSelfMeta { updater, tx } => {
                let res = self.update_self_meta(updater).await;
                let _ = tx.send(res);
            }
            PartyCommand::SendSelfMemberPatch { patch, tx } => {
                let res = self.apply_and_send_self_patch(patch).await;
                let _ = tx.send(res);
            }
            PartyCommand::SetSelfEmote { emote_id, path, tx } => {
                let res = self.set_self_emote(&emote_id, path.as_deref()).await;
                let _ = tx.send(res);
            }
            PartyCommand::SetPrivacy {
                privacy,
                send_patch,
                tx,
            } => {
                let res = self.set_privacy(privacy, send_patch).await;
                let _ = tx.send(res);
            }
            PartyCommand::SetPlaylist {
                mnemonic,
                region,
                version,
                options,
                tx,
            } => {
                let res = self
                    .set_playlist(&mnemonic, region.as_deref(), version, options)
                    .await;
                let _ = tx.send(res);
            }
            PartyCommand::SetRegion { region, tx } => {
                let res = self.set_region(region).await;
                let _ = tx.send(res);
            }
            PartyCommand::SetCustomMatchmakingKey { key, tx } => {
                let res = self.set_custom_matchmaking_key(key.as_deref()).await;
                let _ = tx.send(res);
            }
            PartyCommand::SetSquadFill { fill, tx } => {
                let res = self.set_squad_fill(fill).await;
                let _ = tx.send(res);
            }
            PartyCommand::SetMaxSize { max_size, tx } => {
                let res = self.set_max_size(max_size).await;
                let _ = tx.send(res);
            }
            PartyCommand::RefreshSquadAssignments { tx } => {
                let res = self.refresh_squad_assignments().await;
                let _ = tx.send(res);
            }
            PartyCommand::HideMember { member, hide, tx } => {
                let res = self.hide_member(&member, hide).await;
                let _ = tx.send(res);
            }
            PartyCommand::HideAllMembers { hide, tx } => {
                let res = self.hide_all_members(hide).await;
                let _ = tx.send(res);
            }
            PartyCommand::Kick { member, tx } => {
                let res = self.kick(&member).await;
                let _ = tx.send(res);
            }
            PartyCommand::Promote { member, tx } => {
                let res = self.promote(&member).await;
                let _ = tx.send(res);
            }
            PartyCommand::Invite { friend, tx } => {
                let res = self.invite(&friend).await;
                let _ = tx.send(res);
            }
            PartyCommand::Leave { tx } => {
                let res = self.leave().await;
                let _ = tx.send(res);
                return true;
            }
            PartyCommand::XMPPMemberDelta { msg, tx } => {
                self.on_xmpp_member_delta(msg).await;
                let _ = tx.send(Ok(()));
            }
            PartyCommand::XMPPPartyUpdated { msg, tx } => {
                self.on_xmpp_party_updated(msg).await;
                let _ = tx.send(Ok(()));
            }
            PartyCommand::XMPPConfirmation { msg, tx } => {
                self.on_xmpp_confirmation(msg).await;
                let _ = tx.send(Ok(()));
            }
            PartyCommand::STOMPMemberJoined { msg, tx } => {
                self.on_stomp_member_joined(msg).await;
                let _ = tx.send(Ok(()));
            }
            PartyCommand::STOMPMemberRemoved { msg, reason, tx } => {
                let exited = self.on_stomp_member_removed(msg, reason).await;
                let _ = tx.send(Ok(()));
                return exited;
            }
            PartyCommand::STOMPPartyUpdated { msg, tx } => {
                self.on_stomp_party_updated(msg).await;
                let _ = tx.send(Ok(()));
            }
        }
        false
    }

    async fn send_fn_party_patch(
        &mut self,
        patch: MetaPatch,
        config_override: Option<FortnitePartyConfig>,
    ) -> Result<(), RustfnError> {
        let chunks = split_meta_patch(&patch, self.party_config.patch_max_keys);
        let party_id = self.state.snapshot.id.clone();
        let config = config_override.unwrap_or_else(|| self.state.snapshot.config.to_fn_config());

        for chunk in chunks {
            self.send_fn_party_patch_chunk(&party_id, &config, &chunk)
                .await?;
        }
        self.state.snapshot.meta.apply_patch(&patch);
        self.publish_snapshot().await;
        Ok(())
    }

    async fn send_fn_party_patch_chunk(
        &mut self,
        party_id: &LobbyId,
        config: &FortnitePartyConfig,
        chunk: &MetaPatch,
    ) -> Result<(), RustfnError> {
        let mut attempt = 0;
        loop {
            let revision = self.state.snapshot.revision;
            match self
                .http
                .patch_fn_party_meta(
                    party_id,
                    config,
                    chunk,
                    revision,
                    Some(self.retry_hook.clone()),
                )
                .await
            {
                Ok(()) => {
                    self.state.snapshot.revision = revision + 1;
                    return Ok(());
                }
                Err(err) if is_stale_revision(&err) && attempt < MAX_STALE_REVISION_RETRIES => {
                    attempt += 1;
                    if let Some((_, server_rev)) =
                        err.as_epic_api().and_then(|(_, api)| api.parse_stale_revisions())
                    {
                        self.state.snapshot.revision = server_rev;
                    } else {
                        self.state.snapshot.revision += 1;
                    }
                    self.publish_snapshot().await;
                }
                Err(err) if is_party_change_forbidden(&err) => {
                    return Err(RustfnError::PartyState(
                        "client is not the party leader".to_owned(),
                    ));
                }
                Err(err) => {
                    if err.is_not_found() {
                        self.clear_on_not_found().await;
                    }
                    return Err(err);
                }
            }
        }
    }

    async fn send_eos_party_patch(
        &mut self,
        config: Option<&EOSPartyDataConfig>,
        meta: Option<&Value>,
    ) -> Result<(), RustfnError> {
        let eos_id = self.state.snapshot.eos_id.clone();
        for attempt in 0..=1 {
            let revision = self.state.snapshot.eos_revision;
            match self
                .http
                .patch_eos_party(
                    &eos_id,
                    revision,
                    config,
                    meta,
                    Some(self.retry_hook.clone()),
                )
                .await
            {
                Ok(updated) => {
                    if updated.revision > self.state.snapshot.eos_revision {
                        self.apply_eos_data(updated);
                    } else if let Ok(current) = self.http.get_eos_party(&eos_id).await
                        && current.revision >= self.state.snapshot.eos_revision
                    {
                        self.apply_eos_data(current);
                    }
                    self.publish_snapshot().await;
                    return Ok(());
                }
                Err(err) if is_stale_revision(&err) && attempt == 0 => {
                    let current = self.http.get_eos_party(&eos_id).await?;
                    if current.revision >= self.state.snapshot.eos_revision {
                        self.apply_eos_data(current);
                    }
                    self.publish_snapshot().await;
                }
                Err(err) => {
                    if err.is_not_found() {
                        self.clear_on_not_found().await;
                    }
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    fn apply_eos_data(&mut self, data: EOSPartyData) {
        self.state.snapshot.eos_revision = data.revision;
        self.state.snapshot.leader_id = data.party_lead.clone();
        if !data.chat_conversation_id.is_empty() {
            self.state.snapshot.chat_conversation_id = data.chat_conversation_id;
        }
        self.state.snapshot.is_reportable = data.is_reportable;
        self.state.snapshot.eos_config = data.config;
        for (id, member) in &mut self.state.snapshot.members {
            member.is_leader = id == &data.party_lead;
        }
    }

    async fn update_self_meta(&mut self, updater: MemberMetaUpdater) -> Result<(), RustfnError> {
        let self_id = self.state.self_id.clone();
        let me = self
            .state
            .snapshot
            .members
            .get_mut(&self_id)
            .ok_or_else(|| RustfnError::PartyState("client member missing from party".to_owned()))?;
        let patch = updater(&mut me.meta)?;
        self.send_member_patch_chunks(patch).await
    }

    async fn apply_and_send_self_patch(&mut self, patch: MetaPatch) -> Result<(), RustfnError> {
        let self_id = self.state.self_id.clone();
        let me = self
            .state
            .snapshot
            .members
            .get_mut(&self_id)
            .ok_or_else(|| RustfnError::PartyState("client member missing from party".to_owned()))?;
        me.meta.apply_patch(&patch);
        self.send_member_patch_chunks(patch).await
    }

    async fn set_self_emote(
        &mut self,
        emote_id: &str,
        path: Option<&str>,
    ) -> Result<(), RustfnError> {
        let has_active_emote = self
            .state
            .snapshot
            .members
            .get(&self.state.self_id)
            .and_then(|m| m.emote())
            .is_some();
        if has_active_emote {
            let self_id = self.state.self_id.clone();
            let clear_patch = {
                let me = self.state.snapshot.members.get_mut(&self_id).ok_or_else(|| {
                    RustfnError::PartyState("client member missing from party".to_owned())
                })?;
                me.meta.build_clear_emote_patch()?
            };
            self.send_member_patch_chunks(clear_patch).await?;
        }
        let emote_owned = emote_id.to_owned();
        let path_owned = path.map(str::to_owned);
        self.update_self_meta(Box::new(move |meta| {
            meta.build_emote_patch(&emote_owned, path_owned.as_deref())
        }))
        .await
    }

    async fn send_member_patch_chunks(&mut self, patch: MetaPatch) -> Result<(), RustfnError> {
        let has_eos = !self.state.snapshot.eos_id.as_str().is_empty();
        let filtered = if has_eos {
            MetaPatch {
                updated: patch
                    .updated
                    .into_iter()
                    .filter(|(k, _)| k.starts_with("Default:"))
                    .collect(),
                deleted: patch
                    .deleted
                    .into_iter()
                    .filter(|k| k.starts_with("Default:"))
                    .collect(),
            }
        } else {
            patch
        };

        if filtered.is_empty() {
            return Ok(());
        }

        let chunks = split_meta_patch(&filtered, self.party_config.patch_max_keys);
        let party_id = self.state.snapshot.id.clone();
        let self_id = self.state.self_id.clone();

        for chunk in chunks {
            let mut attempt = 0;
            loop {
                let rev = self
                    .state
                    .snapshot
                    .members
                    .get(&self_id)
                    .map(|m| m.revision)
                    .unwrap_or(0);
                match self
                    .http
                    .patch_fn_member_meta(
                        &party_id,
                        &self_id,
                        &chunk,
                        rev,
                        Some(self.retry_hook.clone()),
                    )
                    .await
                {
                    Ok(()) => {
                        if let Some(me) = self.state.snapshot.members.get_mut(&self_id) {
                            me.revision = rev + 1;
                        }
                        break;
                    }
                    Err(err)
                        if is_stale_revision(&err) && attempt < MAX_STALE_REVISION_RETRIES =>
                    {
                        attempt += 1;
                        if let Some((_, server_rev)) =
                            err.as_epic_api().and_then(|(_, api)| api.parse_stale_revisions())
                            && let Some(me) = self.state.snapshot.members.get_mut(&self_id)
                        {
                            me.revision = server_rev;
                        } else if let Some(me) = self.state.snapshot.members.get_mut(&self_id) {
                            me.revision += 1;
                        }
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        if self.save_member_meta
            && let Some(me) = self.state.snapshot.members.get(&self_id)
        {
            *self.saved_member_meta.write().await = Some(me.meta.as_meta().clone());
        }
        self.publish_snapshot().await;
        Ok(())
    }

    async fn set_privacy(
        &mut self,
        privacy: FortnitePartyPrivacy,
        send_patch: bool,
    ) -> Result<(), RustfnError> {
        self.require_leader()?;

        let prev_config = self.state.snapshot.config.clone();
        let prev_eos_config = self.state.snapshot.eos_config.clone();
        let prev_meta = self.state.snapshot.meta.clone();

        let patch = self.state.snapshot.meta.build_privacy_patch(&privacy)?;
        if privacy.party_type == PartyType::Private {
            self.state.snapshot.config.discoverability = PartyDiscoverability::InvitedOnly;
            self.state.snapshot.config.joinability = PartyJoinability::InviteAndFormer;
        } else {
            self.state.snapshot.config.discoverability = PartyDiscoverability::All;
            self.state.snapshot.config.joinability = PartyJoinability::Open;
        }
        self.state.snapshot.config.privacy = privacy;
        let eos_joinability = if privacy.party_type == PartyType::Private {
            PartyJoinability::InviteOnly
        } else {
            PartyJoinability::Open
        };

        if !send_patch {
            self.state.snapshot.eos_config.joinability = eos_joinability;
            self.publish_snapshot().await;
            return Ok(());
        }

        if let Err(err) = self.send_fn_party_patch(patch, None).await {
            self.state.snapshot.config = prev_config;
            self.state.snapshot.eos_config = prev_eos_config;
            self.state.snapshot.meta = prev_meta;
            self.publish_snapshot().await;
            return Err(err);
        }

        let next_eos_cfg = EOSPartyDataConfig {
            joinability: eos_joinability,
            max_size: self.state.snapshot.eos_config.max_size,
        };
        if let Err(err) = self.send_eos_party_patch(Some(&next_eos_cfg), None).await {
            return Err(RustfnError::PartyState(format!(
                "Fortnite party privacy updated, but EOS party privacy update failed: {err}"
            )));
        }
        Ok(())
    }

    async fn set_playlist(
        &mut self,
        mnemonic: &str,
        region: Option<&str>,
        version: Option<i64>,
        options: Option<IslandInfo>,
    ) -> Result<(), RustfnError> {
        self.require_leader()?;
        let patch = self
            .state
            .snapshot
            .meta
            .build_playlist_patch(mnemonic, region, version, options)?;
        self.send_fn_party_patch(patch, None).await
    }

    async fn set_region(&mut self, region: Region) -> Result<(), RustfnError> {
        self.require_leader()?;
        let patch = self.state.snapshot.meta.build_region_patch(region.as_str());
        self.send_fn_party_patch(patch, None).await
    }

    async fn set_custom_matchmaking_key(&mut self, key: Option<&str>) -> Result<(), RustfnError> {
        self.require_leader()?;
        let patch = self
            .state
            .snapshot
            .meta
            .build_custom_matchmaking_key_patch(key);
        self.send_fn_party_patch(patch, None).await
    }

    async fn set_squad_fill(&mut self, fill: bool) -> Result<(), RustfnError> {
        self.require_leader()?;
        let patch = self.state.snapshot.meta.build_squad_fill_patch(fill);
        self.send_fn_party_patch(patch, None).await
    }

    async fn set_max_size(&mut self, max_size: u32) -> Result<(), RustfnError> {
        self.require_leader()?;
        if !(1..=16).contains(&max_size) {
            return Err(RustfnError::PartyState(
                "party max size must be between 1 and 16".to_owned(),
            ));
        }
        if (max_size as usize) < self.state.snapshot.size() {
            return Err(RustfnError::PartyState(
                "party max size cannot be smaller than current member count".to_owned(),
            ));
        }

        let prev_max = self.state.snapshot.config.max_size;
        self.state.snapshot.config.max_size = max_size;
        let empty_patch = MetaPatch::default();
        if let Err(err) = self.send_fn_party_patch(empty_patch, None).await {
            self.state.snapshot.config.max_size = prev_max;
            self.publish_snapshot().await;
            return Err(err);
        }

        let next_eos_cfg = EOSPartyDataConfig {
            joinability: self.state.snapshot.eos_config.joinability,
            max_size,
        };
        if let Err(err) = self.send_eos_party_patch(Some(&next_eos_cfg), None).await {
            return Err(RustfnError::PartyState(format!(
                "Fortnite party max size updated, but EOS party max size update failed: {err}"
            )));
        }
        Ok(())
    }

    async fn refresh_squad_assignments(&mut self) -> Result<(), RustfnError> {
        self.require_leader()?;
        let patch = self.state.compute_squad_assignments_patch()?;
        self.send_fn_party_patch(patch, None).await
    }

    async fn hide_member(&mut self, member: &str, hide: bool) -> Result<(), RustfnError> {
        self.require_leader()?;
        let target_id = self
            .state
            .snapshot
            .find_member(member)
            .map(|m| m.account_id.clone())
            .ok_or_else(|| RustfnError::PartyState(format!("party member `{member}` not found")))?;
        if hide {
            self.state.snapshot.hidden_member_ids.insert(target_id);
        } else {
            self.state.snapshot.hidden_member_ids.remove(&target_id);
        }
        self.refresh_squad_assignments().await
    }

    async fn hide_all_members(&mut self, hide: bool) -> Result<(), RustfnError> {
        self.require_leader()?;
        if hide {
            let ids: Vec<AccountId> = self
                .state
                .snapshot
                .members
                .keys()
                .filter(|id| *id != &self.state.self_id)
                .cloned()
                .collect();
            for id in ids {
                self.state.snapshot.hidden_member_ids.insert(id);
            }
        } else {
            self.state.snapshot.hidden_member_ids.clear();
        }
        self.refresh_squad_assignments().await
    }

    async fn kick(&self, member: &str) -> Result<(), RustfnError> {
        self.require_leader()?;
        let target_id = self
            .state
            .snapshot
            .find_member(member)
            .map(|m| m.account_id.clone())
            .ok_or_else(|| RustfnError::PartyState(format!("party member `{member}` not found")))?;
        self.http
            .remove_eos_party_member(
                &self.state.snapshot.eos_id,
                &target_id,
                Some(self.retry_hook.clone()),
            )
            .await
    }

    async fn promote(&self, member: &str) -> Result<(), RustfnError> {
        self.require_leader()?;
        let target_id = self
            .state
            .snapshot
            .find_member(member)
            .map(|m| m.account_id.clone())
            .ok_or_else(|| RustfnError::PartyState(format!("party member `{member}` not found")))?;

        let fn_fut = self.http.promote_fn_party_member(
            &self.state.snapshot.id,
            &target_id,
            Some(self.retry_hook.clone()),
        );
        let eos_fut = self.http.promote_eos_party_member(
            &self.state.snapshot.eos_id,
            &target_id,
            Some(self.retry_hook.clone()),
        );
        let (fn_res, eos_res) = tokio::join!(fn_fut, eos_fut);
        match (fn_res, eos_res) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), _) | (_, Err(err)) if is_party_change_forbidden(&err) => Err(
                RustfnError::PartyState("client is not the party leader".to_owned()),
            ),
            (Err(err), _) | (_, Err(err)) => Err(err),
        }
    }

    async fn invite(&self, friend: &str) -> Result<SentPartyInvitation, RustfnError> {
        let resolved = if let Ok(id) = AccountId::parse(friend) {
            self.friends.get(&id)
        } else {
            self.friends.find_by_display_name(friend)
        }
        .ok_or_else(|| RustfnError::PartyState(format!("friend `{friend}` not found")))?;

        if self.state.snapshot.members.contains_key(&resolved.id) {
            return Err(RustfnError::PartyState(
                "user is already a member of this party".to_owned(),
            ));
        }
        if self.state.snapshot.size() >= self.state.snapshot.max_size() as usize {
            return Err(RustfnError::PartyState(
                "party has reached its maximum size".to_owned(),
            ));
        }

        self.http
            .invite_eos_party(&resolved.id, &self.state.self_id)
            .await?;

        let now = Utc::now();
        let expires = now + chrono::Duration::minutes(5);
        Ok(SentPartyInvitation {
            eos_party_id: self.state.snapshot.eos_id.clone(),
            sender_id: self.state.self_id.clone(),
            recipient_id: resolved.id,
            sent_at: now.to_rfc3339(),
            expires_at: expires.to_rfc3339(),
        })
    }

    async fn leave(&mut self) -> Result<(), RustfnError> {
        let fn_id = self.state.snapshot.id.clone();
        let eos_id = self.state.snapshot.eos_id.clone();
        let self_id = self.state.self_id.clone();

        if let Err(err) = self
            .http
            .leave_fn_party_member(&fn_id, &self_id, Some(self.retry_hook.clone()))
            .await
            && !err.is_not_found()
        {
            self.party_cancel.cancel();
            return Err(err);
        }

        if let Err(err) = self
            .http
            .remove_eos_party_member(&eos_id, &self_id, Some(self.retry_hook.clone()))
            .await
            && !err.is_not_found()
        {
            self.party_cancel.cancel();
            return Err(err);
        }

        self.state.pending_confirmations.clear();
        *self.snapshot_slot.write().await = None;
        self.party_cancel.cancel();
        self.events.emit_party_event(PartyEvent::PartyLeft {
            party_id: fn_id,
            eos_party_id: eos_id,
        });
        Ok(())
    }

    async fn on_xmpp_member_delta(&mut self, msg: XMPPPartyMemberDeltaNotification) {
        if let Some((member, changes)) = self
            .state
            .apply_xmpp_member_delta(msg, &self.metadata_store)
        {
            self.publish_snapshot().await;
            self.emit_member_delta_events(member, changes);
        }
    }

    async fn on_xmpp_party_updated(&mut self, msg: XMPPPartyUpdatedNotification) {
        if let Some(updated) = self.state.apply_xmpp_party_updated(&msg) {
            self.publish_snapshot().await;
            self.events.emit_party_event(PartyEvent::PartyUpdated(updated));
        }
    }

    async fn on_xmpp_confirmation(&mut self, msg: XMPPPartyMemberConfirmationNotification) {
        if msg.party_id.as_str() != self.state.snapshot.id.as_str() {
            return;
        }
        let account_dn = match self.users.fetch(&msg.account_id).await {
            Ok(profile) => profile.display_name.or(msg.account_dn),
            Err(_) => msg.account_dn,
        };
        let confirmation = PendingMemberConfirmation::new(
            self.state.snapshot.id.clone(),
            msg.account_id.clone(),
            account_dn,
            msg.sent,
            self.http.clone(),
        );
        self.state
            .pending_confirmations
            .insert(msg.account_id, confirmation.clone());
        let subscribers = self
            .events
            .emit_party_event(PartyEvent::PartyMemberConfirmationReceived(confirmation.clone()));
        if subscribers == 0
            && let Err(err) = confirmation.confirm().await
        {
            warn!(party_id = %self.state.snapshot.id, error = %err, "auto-confirm member failed");
        }
    }

    async fn on_stomp_member_joined(&mut self, msg: EOSPartyMemberUpdateData) {
        let Some(mut outcome) = self
            .state
            .apply_stomp_member_joined(&msg, &self.metadata_store)
        else {
            return;
        };

        if outcome.needs_profile_fetch
            && let Ok(profile) = self.users.fetch(&outcome.member.account_id).await
            && let Some(member_mut) = self.state.snapshot.members.get_mut(&outcome.member.account_id)
        {
            member_mut.apply_profile(&profile);
            outcome.member = member_mut.clone();
        }

        self.publish_snapshot().await;

        if outcome.is_self
            && outcome.member.revision == 0
            && let Some(me_meta) = self
                .state
                .snapshot
                .members
                .get(&self.state.self_id)
                .map(|m| m.meta.as_meta().clone())
        {
            let full_patch = MetaPatch {
                updated: me_meta.into_map(),
                deleted: Vec::new(),
            };
            if let Err(err) = self.send_member_patch_chunks(full_patch).await {
                warn!(error = %err, "failed to push initial self member meta after STOMP join");
            }
        }

        let party_id = self.state.snapshot.id.clone();
        for changes in outcome.replayed_deltas {
            self.emit_member_delta_events(outcome.member.clone(), changes);
        }
        self.events.emit_party_event(PartyEvent::PartyMemberJoined {
            party_id,
            member: outcome.member,
        });

        if self.state.is_self_leader()
            && let Err(err) = self.refresh_squad_assignments().await
        {
            warn!(error = %err, "failed to refresh squad assignments after member join");
        }
    }

    async fn on_stomp_member_removed(
        &mut self,
        msg: EOSPartyMemberUpdateData,
        reason: MemberRemovalReason,
    ) -> bool {
        let Some((removed, was_self)) = self.state.apply_stomp_member_removed(
            &msg.party_id,
            &msg.account_id,
            &self.metadata_store,
        ) else {
            return false;
        };

        let party_id = self.state.snapshot.id.clone();
        let eos_id = self.state.snapshot.eos_id.clone();
        if was_self {
            *self.snapshot_slot.write().await = None;
            self.party_cancel.cancel();
            self.events.emit_party_event(match reason {
                MemberRemovalReason::Left => PartyEvent::PartyMemberLeft {
                    party_id: party_id.clone(),
                    member: removed,
                },
                MemberRemovalReason::Kicked => PartyEvent::PartyMemberKicked {
                    party_id: party_id.clone(),
                    member: removed,
                },
                MemberRemovalReason::Expired => PartyEvent::PartyMemberExpired {
                    party_id: party_id.clone(),
                    member: removed,
                },
            });
            self.events.emit_party_event(PartyEvent::PartyLeft {
                party_id,
                eos_party_id: eos_id,
            });
            return true;
        }

        self.publish_snapshot().await;
        self.events.emit_party_event(match reason {
            MemberRemovalReason::Left => PartyEvent::PartyMemberLeft {
                party_id,
                member: removed,
            },
            MemberRemovalReason::Kicked => PartyEvent::PartyMemberKicked {
                party_id,
                member: removed,
            },
            MemberRemovalReason::Expired => PartyEvent::PartyMemberExpired {
                party_id,
                member: removed,
            },
        });

        if self.state.is_self_leader()
            && let Err(err) = self.refresh_squad_assignments().await
        {
            warn!(error = %err, "failed to refresh squad assignments after member removal");
        }
        false
    }

    async fn on_stomp_party_updated(&mut self, msg: EOSPartyUpdateData) {
        let Some((promoted, updated)) = self.state.apply_stomp_party_updated(&msg) else {
            return;
        };
        let party_id = updated.id.clone();
        self.publish_snapshot().await;
        if let Some(new_leader) = promoted {
            self.events.emit_party_event(PartyEvent::PartyMemberPromoted {
                party_id,
                new_leader,
            });
        }
        self.events.emit_party_event(PartyEvent::PartyUpdated(updated));
    }

    fn emit_member_delta_events(&self, member: PartyMemberSnapshot, changes: MemberDeltaChanges) {
        let party_id = self.state.snapshot.id.clone();

        if let Some((after, before)) = changes.outfit_changed {
            self.events.emit_party_event(PartyEvent::PartyMemberOutfitUpdated {
                party_id: party_id.clone(),
                member: member.clone(),
                after,
                before,
            });
        }
        if let Some((after, before)) = changes.backpack_changed {
            self.events.emit_party_event(PartyEvent::PartyMemberBackpackUpdated {
                party_id: party_id.clone(),
                member: member.clone(),
                after,
                before,
            });
        }
        if let Some((after, before)) = changes.pickaxe_changed {
            self.events.emit_party_event(PartyEvent::PartyMemberPickaxeUpdated {
                party_id: party_id.clone(),
                member: member.clone(),
                after,
                before,
            });
        }
        if let Some((after, before)) = changes.emote_changed {
            self.events.emit_party_event(PartyEvent::PartyMemberEmoteUpdated {
                party_id: party_id.clone(),
                member: member.clone(),
                after,
                before,
            });
        }
        if let Some(ready) = changes.readiness_changed {
            self.events.emit_party_event(PartyEvent::PartyMemberReadinessUpdated {
                party_id: party_id.clone(),
                member: member.clone(),
                ready,
            });
        }
        if changes.match_state_changed {
            self.events.emit_party_event(PartyEvent::PartyMemberMatchStateUpdated {
                party_id: party_id.clone(),
                member: member.clone(),
            });
        }
        self.events.emit_party_event(PartyEvent::PartyMemberUpdated { party_id, member });
    }
}

fn split_meta_patch(patch: &MetaPatch, max_keys: usize) -> Vec<MetaPatch> {
    let limit = max_keys.max(1);
    if patch.updated.is_empty() {
        return vec![MetaPatch {
            updated: BTreeMap::new(),
            deleted: patch.deleted.clone(),
        }];
    }

    let entries: Vec<(String, String)> = patch
        .updated
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let mut chunks = Vec::new();
    for (idx, slice) in entries.chunks(limit).enumerate() {
        chunks.push(MetaPatch {
            updated: slice.iter().cloned().collect(),
            deleted: if idx == 0 {
                patch.deleted.clone()
            } else {
                Vec::new()
            },
        });
    }
    chunks
}

fn is_stale_revision(err: &RustfnError) -> bool {
    err.as_epic_api()
        .is_some_and(|(_, api)| api.is_stale_revision())
}

fn is_party_change_forbidden(err: &RustfnError) -> bool {
    err.as_epic_api().is_some_and(|(_, api)| {
        api.effective_code() == "errors.com.epicgames.social.party.party_change_forbidden"
    })
}
