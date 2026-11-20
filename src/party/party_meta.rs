use crate::protocol::{
    AccountId, FortnitePartyPrivacy, InviteRestriction, MetaMap, MetaPatch, PartyId,
    PartyPrivacyPreset, PartyType, Playlist, Region, RustfnError, SquadPositionPriority,
    meta_codec::keys,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SquadAssignmentEntry {
    pub member_id: AccountId,
    pub absolute_member_idx: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SquadInfoData {
    pub jam_tempo: u32,
    pub jam_key: u32,
    pub jam_mode: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SquadInformationPayload {
    pub raw_squad_assignments: Vec<SquadAssignmentEntry>,
    pub squad_data: Vec<SquadInfoData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SquadInformationWrapper {
    #[serde(rename = "SquadInformation")]
    pub squad_information: SquadInformationPayload,
}

/// Computes deterministic lobby stand positions honoring visibility rules and priority ordering.
pub fn calculate_squad_assignments(
    self_id: &AccountId,
    leader_id: Option<&AccountId>,
    member_ids: &[AccountId],
    hidden_ids: &HashSet<AccountId>,
    priorities: &[SquadPositionPriority],
) -> Vec<SquadAssignmentEntry> {
    let mut seen: HashSet<AccountId> = HashSet::with_capacity(member_ids.len());
    let mut ordered: Vec<AccountId> = Vec::with_capacity(member_ids.len());

    let mut push_if_eligible = |candidate: &AccountId| {
        if member_ids.contains(candidate)
            && !hidden_ids.contains(candidate)
            && seen.insert(candidate.clone())
        {
            ordered.push(candidate.clone());
        }
    };

    for priority in priorities {
        match priority {
            SquadPositionPriority::SelfFirst => {
                push_if_eligible(self_id);
            }
            SquadPositionPriority::LeaderFirst => {
                if let Some(leader) = leader_id {
                    push_if_eligible(leader);
                }
            }
        }
    }

    for member_id in member_ids {
        if !hidden_ids.contains(member_id) && seen.insert(member_id.clone()) {
            ordered.push(member_id.clone());
        }
    }

    ordered
        .into_iter()
        .enumerate()
        .map(|(idx, member_id)| SquadAssignmentEntry {
            member_id,
            absolute_member_idx: idx,
        })
        .collect()
}

pub const DEFAULT_PLAYLIST_MNEMONIC: &str = "playlist_defaultsquad";
pub const KEY_ALLOW_JOIN_IN_PROGRESS_B: &str = "Default:AllowJoinInProgress_b";
pub const KEY_ATHENA_SQUAD_FILL_B: &str = "Default:AthenaSquadFill_b";
const KEY_CAMPAIGN_INFO_J: &str = "Default:CampaignInfo_j";
pub const KEY_CREATIVE_DISCOVERY_SURFACE_REVISIONS_J: &str =
    "Default:CreativeDiscoverySurfaceRevisions_j";
pub const KEY_CREATIVE_IN_GAME_READY_CHECK_STATUS_S: &str =
    "Default:CreativeInGameReadyCheckStatus_s";
pub const KEY_CURRENT_REGION_ID_S: &str = "Default:CurrentRegionId_s";
pub const KEY_CUSTOM_MATCH_KEY_S: &str = "Default:CustomMatchKey_s";
pub const KEY_GAME_SESSION_KEY_S: &str = "Default:GameSessionKey_s";
pub const KEY_LFG_TIME_S: &str = "Default:LFGTime_s";
pub const KEY_PARTY_IS_JOINED_IN_PROGRESS_B: &str = "Default:PartyIsJoinedInProgress_b";
pub const KEY_PARTY_MATCHMAKING_INFO_J: &str = "Default:PartyMatchmakingInfo_j";
pub const KEY_PARTY_STATE_S: &str = "Default:PartyState_s";
pub const KEY_PLATFORM_SESSIONS_J: &str = "Default:PlatformSessions_j";
pub const KEY_PLAYLIST_DATA_J: &str = "Default:PlaylistData_j";
pub const KEY_PREFERRED_PRIVACY_S: &str = "Default:PreferredPrivacy_s";
pub const KEY_PRIMARY_GAME_SESSION_ID_S: &str = "Default:PrimaryGameSessionId_s";
pub const KEY_SQUAD_INFORMATION_J: &str = "Default:SquadInformation_j";
pub const KEY_SELECTED_ISLAND_J: &str = "Default:SelectedIsland_j";
pub const KEY_ZONE_INSTANCE_ID_S: &str = "Default:ZoneInstanceId_s";
pub const KEY_LEADER_DATA_J: &str = "Default:LeaderData_j";
pub const KEY_CFG_ACCEPTING_MEMBERS_B: &str = "urn:epic:cfg:accepting-members_b";
pub const KEY_CFG_BUILD_ID_S: &str = "urn:epic:cfg:build-id_s";
pub const KEY_CFG_CAN_JOIN_B: &str = "urn:epic:cfg:can-join_b";
pub const KEY_CFG_CHAT_ENABLED_B: &str = "urn:epic:cfg:chat-enabled_b";
pub const KEY_CFG_INVITE_PERM_S: &str = "urn:epic:cfg:invite-perm_s";
pub const KEY_CFG_JOIN_REQUEST_ACTION_S: &str = "urn:epic:cfg:join-request-action_s";
pub const KEY_CFG_PARTY_TYPE_ID_S: &str = "urn:epic:cfg:party-type-id_s";
pub const KEY_CFG_PRESENCE_PERM_S: &str = "urn:epic:cfg:presence-perm_s";
pub const KEY_CFG_NOT_ACCEPTING_MEMBERS: &str = "urn:epic:cfg:not-accepting-members";
pub const KEY_CFG_NOT_ACCEPTING_MEMBERS_REASON_I: &str =
    "urn:epic:cfg:not-accepting-members-reason_i";

fn default_neg_one() -> i64 {
    -1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct IslandLinkId {
    pub mnemonic: String,
    #[serde(default = "default_neg_one")]
    pub version: i64,
}

impl Default for IslandLinkId {
    fn default() -> Self {
        Self {
            mnemonic: DEFAULT_PLAYLIST_MNEMONIC.to_owned(),
            version: -1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct IslandWorldId {
    #[serde(rename = "iD")]
    pub id: String,
    pub owner_id: String,
    pub name: String,
}

impl Default for IslandWorldId {
    fn default() -> Self {
        Self {
            id: String::new(),
            owner_id: "INVALID".to_owned(),
            name: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct IslandJoinInfo {
    pub island_joinability: String,
    pub b_is_world_joinable: bool,
    pub session_key: String,
}

impl Default for IslandJoinInfo {
    fn default() -> Self {
        Self {
            island_joinability: "CanNotBeJoinedOrWatched".to_owned(),
            b_is_world_joinable: false,
            session_key: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct IslandInfo {
    pub link_id: IslandLinkId,
    pub world_id: IslandWorldId,
    pub session_id: String,
    pub join_info: IslandJoinInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
struct SelectedIslandWrapper {
    #[serde(rename = "SelectedIsland")]
    selected_island: IslandInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PrivacySettingsPayload {
    pub party_type: PartyType,
    pub party_invite_restriction: InviteRestriction,
    pub b_only_leader_friends_can_join: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
struct PrivacySettingsWrapper {
    #[serde(rename = "PrivacySettings")]
    privacy_settings: PrivacySettingsPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct LeaderDataInner {
    epic_party_id: Option<PartyId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
struct LeaderDataWrapper {
    #[serde(rename = "LeaderData")]
    leader_data: LeaderDataInner,
}

/// Typed accessor and delta builder over Fortnite Party v1 `meta` maps.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PartyMeta(MetaMap);

impl PartyMeta {
    pub fn from_meta(meta: MetaMap) -> Self {
        Self(meta)
    }

    pub fn as_meta(&self) -> &MetaMap {
        &self.0
    }

    pub fn into_meta(self) -> MetaMap {
        self.0
    }

    pub fn apply_patch(&mut self, patch: &MetaPatch) {
        self.0.apply_patch(patch);
    }

    pub fn default_for_client(
        eos_party_id: &PartyId,
        region: Region,
        privacy: &FortnitePartyPrivacy,
        initial: Option<&MetaMap>,
    ) -> Self {
        let mut meta = MetaMap::new();
        let region_code = region.as_str().to_owned();

        meta.set_bool(KEY_ALLOW_JOIN_IN_PROGRESS_B, false);
        meta.set_bool(KEY_ATHENA_SQUAD_FILL_B, true);
        let _ = meta.set_json(
            KEY_CAMPAIGN_INFO_J,
            &json!({
                "CampaignInfo": {
                    "lobbyConnectionStarted": false,
                    "matchmakingResult": "NotStarted",
                    "matchmakingState": "NotMatchmaking",
                    "sessionIsCriticalMission": false,
                    "zoneTileIndex": -1,
                    "theaterId": "",
                    "tileStates": { "tileStates": [], "numSetBits": 0 },
                }
            }),
        );
        let _ = meta.set_json(
            KEY_CREATIVE_DISCOVERY_SURFACE_REVISIONS_J,
            &json!({ "CreativeDiscoverySurfaceRevisions": [] }),
        );
        meta.set_str(KEY_CREATIVE_IN_GAME_READY_CHECK_STATUS_S, "None");
        meta.set_str(KEY_CURRENT_REGION_ID_S, &region_code);
        meta.set_str(KEY_CUSTOM_MATCH_KEY_S, "");
        meta.set_str(KEY_GAME_SESSION_KEY_S, "");
        meta.set_str(KEY_LFG_TIME_S, "0001-01-01T00:00:00.000Z");
        meta.set_bool(KEY_PARTY_IS_JOINED_IN_PROGRESS_B, false);
        let _ = meta.set_json(
            KEY_PARTY_MATCHMAKING_INFO_J,
            &json!({
                "PartyMatchmakingInfo": {
                    "buildId": -1,
                    "hotfixVersion": -1,
                    "regionId": "",
                    "playlistName": "None",
                    "playlistRevision": 0,
                    "tournamentId": "",
                    "eventWindowId": "",
                    "linkCode": "",
                }
            }),
        );
        meta.set_str(KEY_PARTY_STATE_S, "BattleRoyaleView");
        let _ = meta.set_json(KEY_PLATFORM_SESSIONS_J, &json!({ "PlatformSessions": [] }));
        let _ = meta.set_json(
            KEY_PLAYLIST_DATA_J,
            &json!({
                "PlaylistData": {
                    "playlistName": Playlist::DEFAULT_SQUAD,
                    "tournamentId": "",
                    "eventWindowId": "",
                    "linkId": { "mnemonic": DEFAULT_PLAYLIST_MNEMONIC, "version": -1 },
                    "bGracefullyUpgraded": false,
                    "matchmakingRulePreset": "RespectParties",
                }
            }),
        );
        meta.set_str(KEY_PREFERRED_PRIVACY_S, "Fill");
        meta.set_str(KEY_PRIMARY_GAME_SESSION_ID_S, "");
        let _ = meta.set_json(
            KEY_SQUAD_INFORMATION_J,
            &SquadInformationWrapper {
                squad_information: SquadInformationPayload {
                    raw_squad_assignments: Vec::new(),
                    squad_data: vec![SquadInfoData::default()],
                },
            },
        );
        meta.set_str(keys::REGION_ID_S, &region_code);
        let _ = meta.set_json(
            KEY_SELECTED_ISLAND_J,
            &SelectedIslandWrapper {
                selected_island: IslandInfo::default(),
            },
        );
        meta.set_str(KEY_ZONE_INSTANCE_ID_S, "");
        let _ = meta.set_json(
            KEY_LEADER_DATA_J,
            &LeaderDataWrapper {
                leader_data: LeaderDataInner {
                    epic_party_id: Some(eos_party_id.clone()),
                },
            },
        );

        meta.set_str(KEY_CFG_PARTY_TYPE_ID_S, "default");

        let mut party_meta = Self(meta);
        let _ = party_meta.build_privacy_patch(privacy);

        if let Some(extra) = initial {
            for (k, v) in extra.as_map() {
                party_meta.0.set_raw(k.clone(), v.clone());
            }
        }
        party_meta
    }

    pub fn set_cfg_identity(&mut self, build_id: Option<&str>, chat_enabled: bool) {
        self.0.set_str(KEY_CFG_PARTY_TYPE_ID_S, "default");
        self.0.set_str(
            KEY_CFG_CHAT_ENABLED_B,
            if chat_enabled { "true" } else { "false" },
        );
        if let Some(bid) = build_id.filter(|s| !s.is_empty()) {
            self.0.set_str(KEY_CFG_BUILD_ID_S, bid);
        }
    }

    pub fn island(&self) -> Option<IslandInfo> {
        self.0
            .get_json::<SelectedIslandWrapper>(KEY_SELECTED_ISLAND_J)
            .ok()
            .flatten()
            .map(|w| w.selected_island)
    }

    pub fn region(&self) -> Option<&str> {
        self.0
            .get_str(keys::REGION_ID_S)
            .filter(|s| !s.is_empty())
    }

    pub fn custom_matchmaking_key(&self) -> Option<&str> {
        self.0
            .get_str(KEY_CUSTOM_MATCH_KEY_S)
            .filter(|s| !s.is_empty())
    }

    pub fn squad_fill(&self) -> bool {
        self.0
            .get_bool(KEY_ATHENA_SQUAD_FILL_B)
            .ok()
            .flatten()
            .unwrap_or(false)
    }

    pub fn squad_assignments(&self) -> Vec<SquadAssignmentEntry> {
        self.0
            .get_json::<SquadInformationWrapper>(KEY_SQUAD_INFORMATION_J)
            .ok()
            .flatten()
            .map(|w| w.squad_information.raw_squad_assignments)
            .unwrap_or_default()
    }

    pub fn privacy_settings(&self) -> Option<PrivacySettingsPayload> {
        self.0
            .get_json::<PrivacySettingsWrapper>(keys::PRIVACY_SETTINGS_J)
            .ok()
            .flatten()
            .map(|w| w.privacy_settings)
    }

    pub fn matched_privacy(&self) -> Option<FortnitePartyPrivacy> {
        let settings = self.privacy_settings()?;
        const PRESETS: [PartyPrivacyPreset; 5] = [
            PartyPrivacyPreset::Public,
            PartyPrivacyPreset::FriendsAllowFriendsOfFriends,
            PartyPrivacyPreset::Friends,
            PartyPrivacyPreset::PrivateAllowFriendsOfFriends,
            PartyPrivacyPreset::Private,
        ];
        PRESETS
            .into_iter()
            .map(PartyPrivacyPreset::to_privacy)
            .find(|preset| {
                preset.party_type == settings.party_type
                    && preset.invite_restriction == settings.party_invite_restriction
                    && preset.only_leader_friends_can_join
                        == settings.b_only_leader_friends_can_join
            })
    }

    pub fn build_privacy_patch(
        &mut self,
        privacy: &FortnitePartyPrivacy,
    ) -> Result<MetaPatch, RustfnError> {
        let mut updated = BTreeMap::new();
        let mut deleted = Vec::new();

        let wrapper = PrivacySettingsWrapper {
            privacy_settings: PrivacySettingsPayload {
                party_type: privacy.party_type,
                party_invite_restriction: privacy.invite_restriction,
                b_only_leader_friends_can_join: privacy.only_leader_friends_can_join,
            },
        };
        let privacy_json = serde_json::to_string(&wrapper)?;
        self.0.set_raw(keys::PRIVACY_SETTINGS_J, privacy_json.clone());
        updated.insert(keys::PRIVACY_SETTINGS_J.to_owned(), privacy_json);

        let presence_perm = enum_wire_string(&privacy.presence_permission)?;
        self.0
            .set_str(KEY_CFG_PRESENCE_PERM_S, presence_perm.clone());
        updated.insert(KEY_CFG_PRESENCE_PERM_S.to_owned(), presence_perm);

        let accepting_str = if privacy.accepting_members {
            "true"
        } else {
            "false"
        }
        .to_owned();
        self.0
            .set_str(KEY_CFG_ACCEPTING_MEMBERS_B, accepting_str.clone());
        updated.insert(KEY_CFG_ACCEPTING_MEMBERS_B.to_owned(), accepting_str);

        let invite_perm = enum_wire_string(&privacy.invite_permission)?;
        self.0.set_str(KEY_CFG_INVITE_PERM_S, invite_perm.clone());
        updated.insert(KEY_CFG_INVITE_PERM_S.to_owned(), invite_perm);

        if privacy.party_type == PartyType::Private {
            self.0.set_str(KEY_CFG_CAN_JOIN_B, "false");
            updated.insert(KEY_CFG_CAN_JOIN_B.to_owned(), "false".to_owned());

            self.0.set_str(KEY_CFG_JOIN_REQUEST_ACTION_S, "Manual");
            updated.insert(KEY_CFG_JOIN_REQUEST_ACTION_S.to_owned(), "Manual".to_owned());

            self.0.remove(KEY_CFG_NOT_ACCEPTING_MEMBERS);
            deleted.push(KEY_CFG_NOT_ACCEPTING_MEMBERS.to_owned());

            self.0.set_i64(KEY_CFG_NOT_ACCEPTING_MEMBERS_REASON_I, 7);
            updated.insert(
                KEY_CFG_NOT_ACCEPTING_MEMBERS_REASON_I.to_owned(),
                "7".to_owned(),
            );
        } else {
            self.0.set_str(KEY_CFG_CAN_JOIN_B, "true");
            updated.insert(KEY_CFG_CAN_JOIN_B.to_owned(), "true".to_owned());

            self.0.set_str(KEY_CFG_JOIN_REQUEST_ACTION_S, "AutoApprove");
            updated.insert(
                KEY_CFG_JOIN_REQUEST_ACTION_S.to_owned(),
                "AutoApprove".to_owned(),
            );

            self.0.remove(KEY_CFG_NOT_ACCEPTING_MEMBERS_REASON_I);
            deleted.push(KEY_CFG_NOT_ACCEPTING_MEMBERS_REASON_I.to_owned());
        }

        Ok(MetaPatch { updated, deleted })
    }

    pub fn build_playlist_patch(
        &mut self,
        mnemonic: &str,
        region: Option<&str>,
        version: Option<i64>,
        options: Option<IslandInfo>,
    ) -> Result<MetaPatch, RustfnError> {
        let mut updated = BTreeMap::new();

        if let Some(region_id) = region {
            self.0.set_str(keys::REGION_ID_S, region_id);
            updated.insert(keys::REGION_ID_S.to_owned(), region_id.to_owned());
        } else if let Some(current) = self.0.get_str(keys::REGION_ID_S) {
            updated.insert(keys::REGION_ID_S.to_owned(), current.to_owned());
        }

        let mut island = options.unwrap_or_else(|| self.island().unwrap_or_default());
        island.link_id = IslandLinkId {
            mnemonic: mnemonic.to_owned(),
            version: version.unwrap_or(-1),
        };
        let encoded = serde_json::to_string(&SelectedIslandWrapper {
            selected_island: island,
        })?;
        self.0.set_raw(KEY_SELECTED_ISLAND_J, encoded.clone());
        updated.insert(KEY_SELECTED_ISLAND_J.to_owned(), encoded);

        Ok(MetaPatch {
            updated,
            deleted: Vec::new(),
        })
    }

    pub fn build_region_patch(&mut self, region_id: &str) -> MetaPatch {
        let mut updated = BTreeMap::new();
        self.0.set_str(keys::REGION_ID_S, region_id);
        self.0.set_str(KEY_CURRENT_REGION_ID_S, region_id);
        updated.insert(keys::REGION_ID_S.to_owned(), region_id.to_owned());
        updated.insert(KEY_CURRENT_REGION_ID_S.to_owned(), region_id.to_owned());
        MetaPatch {
            updated,
            deleted: Vec::new(),
        }
    }

    pub fn build_custom_matchmaking_key_patch(&mut self, key: Option<&str>) -> MetaPatch {
        let value = key.unwrap_or_default().to_owned();
        self.0.set_str(KEY_CUSTOM_MATCH_KEY_S, &value);
        let mut updated = BTreeMap::new();
        updated.insert(KEY_CUSTOM_MATCH_KEY_S.to_owned(), value);
        MetaPatch {
            updated,
            deleted: Vec::new(),
        }
    }

    pub fn build_squad_fill_patch(&mut self, fill: bool) -> MetaPatch {
        self.0.set_bool(KEY_ATHENA_SQUAD_FILL_B, fill);
        let mut updated = BTreeMap::new();
        updated.insert(
            KEY_ATHENA_SQUAD_FILL_B.to_owned(),
            if fill { "true" } else { "false" }.to_owned(),
        );
        MetaPatch {
            updated,
            deleted: Vec::new(),
        }
    }

    pub fn build_squad_assignments_patch(
        &mut self,
        assignments: Vec<SquadAssignmentEntry>,
    ) -> Result<MetaPatch, RustfnError> {
        let mut wrapper = self
            .0
            .get_json::<SquadInformationWrapper>(KEY_SQUAD_INFORMATION_J)
            .ok()
            .flatten()
            .unwrap_or_else(|| SquadInformationWrapper {
                squad_information: SquadInformationPayload {
                    raw_squad_assignments: Vec::new(),
                    squad_data: vec![SquadInfoData::default()],
                },
            });
        wrapper.squad_information.raw_squad_assignments = assignments;
        let encoded = serde_json::to_string(&wrapper)?;
        self.0.set_raw(KEY_SQUAD_INFORMATION_J, encoded.clone());
        let mut updated = BTreeMap::new();
        updated.insert(KEY_SQUAD_INFORMATION_J.to_owned(), encoded);
        Ok(MetaPatch {
            updated,
            deleted: Vec::new(),
        })
    }
}

fn enum_wire_string<T: Serialize>(value: &T) -> Result<String, RustfnError> {
    let json_val = serde_json::to_value(value)?;
    Ok(match json_val {
        Value::String(s) => s,
        other => other.to_string(),
    })
}
