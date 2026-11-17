use crate::party::client_party::ClientParty;
use crate::party::party_member_meta::{
    BannerSnapshot, BattlePassSnapshot, CosmeticEnlightment, CosmeticItem, CosmeticVariant,
    CosmeticsUpdate, MatchStateSnapshot, PartyMemberMeta,
};
use chrono::{DateTime, Utc};
use crate::http::{FnPartyMemberConnection, FortnitePartyMemberData};
use crate::protocol::{AccountId, MetaMap, MetaPatch, Platform, ReadyState, RustfnError};
use crate::users::UserProfile;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Computed change flags emitted after applying a member metadata revision delta.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemberDeltaChanges {
    pub outfit_changed: Option<(Option<String>, Option<String>)>,
    pub backpack_changed: Option<(Option<String>, Option<String>)>,
    pub pickaxe_changed: Option<(Option<String>, Option<String>)>,
    pub emote_changed: Option<(Option<String>, Option<String>)>,
    pub readiness_changed: Option<bool>,
    pub match_state_changed: bool,
}

/// Immutable point-in-time snapshot of a Fortnite lobby member.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartyMemberSnapshot {
    pub account_id: AccountId,
    pub display_name: Option<String>,
    pub external_auths: BTreeMap<String, Value>,
    pub avatar_id: Option<String>,
    pub joined_at: Option<String>,
    pub updated_at: Option<String>,
    pub revision: u64,
    pub is_leader: bool,
    pub received_initial_state_update: bool,
    pub connections: Vec<FnPartyMemberConnection>,
    pub meta: PartyMemberMeta,
}

impl PartyMemberSnapshot {
    pub fn from_fn_data(data: FortnitePartyMemberData, leader_id: Option<&AccountId>) -> Self {
        let is_leader = leader_id == Some(&data.account_id)
            || data
                .role
                .as_deref()
                .is_some_and(|r| r.eq_ignore_ascii_case("CAPTAIN"));
        let dn = data.account_dn.or_else(|| {
            data.meta
                .get_str("urn:epic:member:dn_s")
                .map(str::to_owned)
        });
        Self {
            account_id: data.account_id,
            display_name: dn,
            external_auths: BTreeMap::new(),
            avatar_id: None,
            joined_at: data.joined_at,
            updated_at: data.updated_at,
            revision: data.revision,
            is_leader,
            received_initial_state_update: !data.meta.is_empty(),
            connections: data.connections,
            meta: PartyMemberMeta::from_meta(data.meta),
        }
    }

    pub fn new_skeleton(
        account_id: AccountId,
        display_name: Option<String>,
        joined_at: Option<String>,
        updated_at: Option<String>,
        is_leader: bool,
    ) -> Self {
        let mut meta = MetaMap::new();
        if let Some(dn) = &display_name {
            meta.set_str("urn:epic:member:dn_s", dn);
        }
        Self {
            account_id,
            display_name,
            external_auths: BTreeMap::new(),
            avatar_id: None,
            joined_at,
            updated_at,
            revision: 0,
            is_leader,
            received_initial_state_update: false,
            connections: Vec::new(),
            meta: PartyMemberMeta::from_meta(meta),
        }
    }

    pub fn apply_profile(&mut self, profile: &UserProfile) {
        if profile.display_name.is_some() {
            self.display_name = profile.display_name.clone();
        }
        if !profile.external_auths.is_empty() {
            self.external_auths = profile.external_auths.clone();
        }
        if profile.avatar_id.is_some() {
            self.avatar_id = profile.avatar_id.clone();
        }
    }

    /// Applies an ordered XMPP member metadata delta if `revision >= self.revision`.
    pub fn apply_delta(
        &mut self,
        revision: u64,
        account_dn: Option<String>,
        updated: &MetaMap,
        removed: &[String],
    ) -> Option<MemberDeltaChanges> {
        if revision < self.revision {
            return None;
        }

        let prev_outfit = self.outfit();
        let prev_backpack = self.backpack();
        let prev_pickaxe = self.pickaxe();
        let prev_emote = self.emote();
        let prev_ready = self.is_ready();
        let prev_match = self.match_meta();

        self.revision = revision;
        if let Some(dn) = account_dn.filter(|s| !s.is_empty()) {
            self.display_name = Some(dn);
        } else if let Some(dn) = updated.get_str("urn:epic:member:dn_s") {
            if !dn.is_empty() {
                self.display_name = Some(dn.to_owned());
            }
        }

        let patch = MetaPatch {
            updated: updated.as_map().clone(),
            deleted: removed.to_vec(),
        };
        self.meta.apply_patch(&patch);
        self.received_initial_state_update = true;

        let new_outfit = self.outfit();
        let new_backpack = self.backpack();
        let new_pickaxe = self.pickaxe();
        let new_emote = self.emote();
        let new_ready = self.is_ready();
        let new_match = self.match_meta();

        Some(MemberDeltaChanges {
            outfit_changed: (prev_outfit != new_outfit).then_some((new_outfit, prev_outfit)),
            backpack_changed: (prev_backpack != new_backpack)
                .then_some((new_backpack, prev_backpack)),
            pickaxe_changed: (prev_pickaxe != new_pickaxe).then_some((new_pickaxe, prev_pickaxe)),
            emote_changed: (prev_emote != new_emote).then_some((new_emote, prev_emote)),
            readiness_changed: (prev_ready != new_ready).then_some(new_ready),
            match_state_changed: prev_match != new_match,
        })
    }

    pub fn outfit(&self) -> Option<String> {
        self.meta.outfit()
    }

    pub fn pickaxe(&self) -> Option<String> {
        self.meta.pickaxe()
    }

    pub fn backpack(&self) -> Option<String> {
        self.meta.backpack()
    }

    pub fn shoes(&self) -> Option<String> {
        self.meta.shoes()
    }

    pub fn emote(&self) -> Option<String> {
        self.meta.emote()
    }

    pub fn is_ready(&self) -> bool {
        self.meta.is_ready()
    }

    pub fn is_sitting_out(&self) -> bool {
        self.meta.is_sitting_out()
    }

    pub fn ready_state(&self) -> ReadyState {
        self.meta.ready_state()
    }

    pub fn input(&self) -> Option<String> {
        self.meta.input()
    }

    pub fn banner(&self) -> Option<BannerSnapshot> {
        self.meta.banner()
    }

    pub fn battle_pass(&self) -> Option<BattlePassSnapshot> {
        self.meta.battle_pass()
    }

    pub fn platform(&self) -> Option<Platform> {
        self.meta.platform()
    }

    pub fn match_meta(&self) -> MatchStateSnapshot {
        self.meta.match_meta()
    }

    pub fn is_marker_set(&self) -> bool {
        self.meta.is_marker_set()
    }

    pub fn marker_location(&self) -> (f64, f64) {
        self.meta.marker_location()
    }

    pub fn has_purchased_stw(&self) -> bool {
        self.meta.has_purchased_stw()
    }
}

const DEFAULT_PET_PATH: &str = "/BRCosmetics/Athena/Items/Cosmetics/PetCarriers";
const DEFAULT_EMOJI_PATH: &str = "/BRCosmetics/Athena/Items/Cosmetics/Dances/Emoji";

/// Ergonomic async handle to the local client's party member state and loadout mutators.
#[derive(Debug, Clone)]
pub struct ClientPartyMember {
    party: ClientParty,
}

impl ClientPartyMember {
    pub(crate) fn new(party: ClientParty) -> Self {
        Self { party }
    }

    pub async fn set_readiness(&self, ready: bool) -> Result<(), RustfnError> {
        self.party
            .update_self_meta(move |meta| meta.build_readiness_patch(ready))
            .await
    }

    pub async fn set_sitting_out(&self, sitting_out: bool) -> Result<(), RustfnError> {
        self.party
            .update_self_meta(move |meta| meta.build_sitting_out_patch(sitting_out))
            .await
    }

    pub async fn set_level(&self, level: u32) -> Result<(), RustfnError> {
        self.party
            .update_self_meta(move |meta| {
                let purchased = meta
                    .battle_pass()
                    .map(|bp| bp.b_has_purchased_pass)
                    .unwrap_or(false);
                meta.build_battle_pass_patch(purchased, level)
            })
            .await
    }

    pub async fn set_battle_pass(&self, is_purchased: bool, level: u32) -> Result<(), RustfnError> {
        self.party
            .update_self_meta(move |meta| meta.build_battle_pass_patch(is_purchased, level))
            .await
    }

    pub async fn set_banner(
        &self,
        banner_id: impl Into<String>,
        color_id: impl Into<String>,
    ) -> Result<(), RustfnError> {
        let banner_id = banner_id.into();
        let color_id = color_id.into();
        self.party
            .update_self_meta(move |meta| meta.build_banner_patch(&banner_id, &color_id))
            .await
    }

    pub async fn set_cosmetics(&self, update: CosmeticsUpdate) -> Result<(), RustfnError> {
        self.party
            .update_self_meta(move |meta| meta.build_cosmetics_patch(&update))
            .await
    }

    pub async fn set_outfit(
        &self,
        id: impl Into<String>,
        variants: Vec<CosmeticVariant>,
        enlightment: Option<CosmeticEnlightment>,
    ) -> Result<(), RustfnError> {
        self.set_cosmetics(CosmeticsUpdate {
            outfit: Some(CosmeticItem {
                id: id.into(),
                variants,
                enlightment,
                path: None,
            }),
            ..Default::default()
        })
        .await
    }

    pub async fn set_backpack(
        &self,
        id: impl Into<String>,
        variants: Vec<CosmeticVariant>,
        path: Option<String>,
    ) -> Result<(), RustfnError> {
        self.set_cosmetics(CosmeticsUpdate {
            backpack: Some(Some(CosmeticItem {
                id: id.into(),
                variants,
                enlightment: None,
                path,
            })),
            ..Default::default()
        })
        .await
    }

    pub async fn clear_backpack(&self) -> Result<(), RustfnError> {
        self.set_cosmetics(CosmeticsUpdate {
            backpack: Some(None),
            ..Default::default()
        })
        .await
    }

    pub async fn set_pet(
        &self,
        id: impl Into<String>,
        variants: Vec<CosmeticVariant>,
        path: Option<String>,
    ) -> Result<(), RustfnError> {
        self.set_backpack(
            id,
            variants,
            Some(path.unwrap_or_else(|| DEFAULT_PET_PATH.to_owned())),
        )
        .await
    }

    pub async fn set_pickaxe(
        &self,
        id: impl Into<String>,
        variants: Vec<CosmeticVariant>,
        path: Option<String>,
    ) -> Result<(), RustfnError> {
        self.set_cosmetics(CosmeticsUpdate {
            pickaxe: Some(CosmeticItem {
                id: id.into(),
                variants,
                enlightment: None,
                path,
            }),
            ..Default::default()
        })
        .await
    }

    pub async fn set_shoes(
        &self,
        id: impl Into<String>,
        path: Option<String>,
    ) -> Result<(), RustfnError> {
        self.set_cosmetics(CosmeticsUpdate {
            shoes: Some(Some(CosmeticItem {
                id: id.into(),
                variants: Vec::new(),
                enlightment: None,
                path,
            })),
            ..Default::default()
        })
        .await
    }

    pub async fn clear_shoes(&self) -> Result<(), RustfnError> {
        self.set_cosmetics(CosmeticsUpdate {
            shoes: Some(None),
            ..Default::default()
        })
        .await
    }

    pub async fn set_emote(
        &self,
        id: impl Into<String>,
        path: Option<&str>,
    ) -> Result<(), RustfnError> {
        self.party
            .set_self_emote(id.into(), path.map(str::to_owned))
            .await
    }

    pub async fn set_emoji(
        &self,
        id: impl Into<String>,
        path: Option<&str>,
    ) -> Result<(), RustfnError> {
        self.set_emote(id, Some(path.unwrap_or(DEFAULT_EMOJI_PATH)))
            .await
    }

    pub async fn clear_emote(&self) -> Result<(), RustfnError> {
        self.party
            .update_self_meta(PartyMemberMeta::build_clear_emote_patch)
            .await
    }

    pub async fn set_playing(
        &self,
        is_playing: bool,
        player_count: u8,
        started_at: DateTime<Utc>,
    ) -> Result<(), RustfnError> {
        self.party
            .update_self_meta(move |meta| {
                meta.build_playing_patch(is_playing, player_count, started_at)
            })
            .await
    }

    pub async fn set_marker(
        &self,
        is_set: bool,
        location_x: f64,
        location_y: f64,
    ) -> Result<(), RustfnError> {
        self.party
            .update_self_meta(move |meta| meta.build_marker_patch(is_set, location_x, location_y))
            .await
    }

    pub async fn set_cosmetic_stats(
        &self,
        crowns: i64,
        ranked_progression: i64,
    ) -> Result<(), RustfnError> {
        self.party
            .update_self_meta(move |meta| meta.build_cosmetic_stats_patch(crowns, ranked_progression))
            .await
    }

    pub async fn send_patch(&self, patch: MetaPatch) -> Result<(), RustfnError> {
        self.party.send_member_patch(patch).await
    }
}
