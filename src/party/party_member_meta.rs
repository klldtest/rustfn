use chrono::{DateTime, Utc};
use rand_core::{OsRng, RngCore};
use crate::protocol::{MetaMap, MetaPatch, Platform, ReadyState, RustfnError, meta_codec::keys};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use uuid::Uuid;

pub const KEY_FRONTEND_MAP_MARKER_J: &str = "Default:FrontEndMapMarker_j";
pub const KEY_NUM_ATHENA_PLAYERS_LEFT_U: &str = "Default:NumAthenaPlayersLeft_U";
pub const KEY_CAMPAIGN_INFO_J: &str = "Default:CampaignInfo_j";
pub const KEY_CAMPAIGN_BACKPACK_RATING_D: &str = "Default:CampaignBackpackRating_d";
pub const KEY_CAMPAIGN_COMMANDER_LOADOUT_RATING_D: &str = "Default:CampaignCommanderLoadoutRating_d";
pub const KEY_EXTENSIONS_J: &str = "Default:Extensions_j";
pub const KEY_FEAT_DEFINITION_S: &str = "Default:FeatDefinition_s";
pub const KEY_B_IS_PARTY_USING_PARTY_SIGNAL_B: &str = "Default:bIsPartyUsingPartySignal_b";
pub const KEY_CROSSPLAY_PREFERENCE_S: &str = "Default:CrossplayPreference_s";
pub const KEY_DOWNLOAD_ON_DEMAND_PROGRESS_D: &str = "Default:DownloadOnDemandProgress_d";
pub const KEY_JOIN_METHOD_S: &str = "Default:JoinMethod_s";
pub const KEY_UTC_TIME_STARTED_MATCH_ATHENA_S: &str = "Default:UtcTimeStartedMatchAthena_s";
pub const KEY_FRONTEND_MIMOSA_J: &str = "Default:FrontendMimosa_j";
pub const KEY_EPV: &str = "ePV";

pub const DEFAULT_OUTFIT_CIDS: [&str; 1] = [
    "CID_DefaultOutfit"
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum VariantValue {
    Int(i64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CosmeticVariant {
    pub channel_index: usize,
    pub variant_index: VariantValue,
}

impl CosmeticVariant {
    pub fn int(channel_index: usize, variant_index: i64) -> Self {
        Self {
            channel_index,
            variant_index: VariantValue::Int(variant_index),
        }
    }

    pub fn text(channel_index: usize, value: impl Into<String>) -> Self {
        Self {
            channel_index,
            variant_index: VariantValue::Text(value.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CosmeticEnlightment {
    pub season_or_type: u32,
    pub level: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CosmeticItem {
    pub id: String,
    #[serde(default)]
    pub variants: Vec<CosmeticVariant>,
    #[serde(default)]
    pub enlightment: Option<CosmeticEnlightment>,
    #[serde(default)]
    pub path: Option<String>,
}

impl CosmeticItem {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            variants: Vec::new(),
            enlightment: None,
            path: None,
        }
    }
}

/// Multi-slot cosmetic update descriptor (`None` = unchanged, `Some(None)` = clear).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CosmeticsUpdate {
    pub outfit: Option<CosmeticItem>,
    pub backpack: Option<Option<CosmeticItem>>,
    pub pickaxe: Option<CosmeticItem>,
    pub shoes: Option<Option<CosmeticItem>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MpLoadoutSlot {
    #[serde(default)]
    pub i: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub v: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MpLoadout1Inner {
    #[serde(default)]
    pub s: BTreeMap<String, MpLoadoutSlot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
struct MpLoadout1Wrapper {
    #[serde(rename = "MpLoadout1")]
    mp_loadout1: MpLoadout1Inner,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct LoadoutStat {
    pub stat_name: String,
    pub stat_value: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct LoadoutMetaInner {
    pub en_keys: Vec<Value>,
    pub rand: u64,
    pub v_assets: Vec<Value>,
    pub scratchpad: Vec<Value>,
    pub stats: Vec<LoadoutStat>,
    pub force: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
struct LoadoutMetaWrapper {
    #[serde(rename = "LoadoutMeta")]
    loadout_meta: LoadoutMetaInner,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct FrontendEmoteInner {
    pub emote_e_key: String,
    pub emote_section: i32,
    pub multipurpose_emote_data: i32,
    pub pickable: String,
}

impl Default for FrontendEmoteInner {
    fn default() -> Self {
        Self {
            emote_e_key: String::new(),
            emote_section: -1,
            multipurpose_emote_data: -1,
            pickable: "None".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
struct FrontendEmoteWrapper {
    #[serde(rename = "FrontendEmote")]
    frontend_emote: FrontendEmoteInner,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct BattlePassSnapshot {
    pub b_has_purchased_pass: bool,
    pub pass_level: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
struct BattlePassWrapper {
    #[serde(rename = "BattlePassInfo")]
    battle_pass_info: BattlePassSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BannerSnapshot {
    pub banner_icon_id: Option<String>,
    pub banner_color_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct MatchStateSnapshot {
    pub location: Option<String>,
    pub has_preloaded_athena: bool,
    pub player_count: Option<u64>,
    pub match_started_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct FrontEndMapMarkerLocation {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct FrontEndMapMarkerInner {
    pub marker_location: FrontEndMapMarkerLocation,
    pub b_is_set: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
struct FrontEndMapMarkerWrapper {
    #[serde(rename = "FrontEndMapMarker")]
    front_end_map_marker: FrontEndMapMarkerInner,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PartyMemberMeta(MetaMap);

impl Default for PartyMemberMeta {
    fn default() -> Self {
        Self(MetaMap::new())
    }
}

impl PartyMemberMeta {
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

    pub fn default_for_client(platform: Platform, saved_meta: Option<&MetaMap>) -> Self {
        let mut meta = MetaMap::new();
        let mm_id = Uuid::new_v4().simple().to_string().to_ascii_uppercase();
        let rand_idx = (OsRng.next_u32() as usize) % DEFAULT_OUTFIT_CIDS.len();
        let default_outfit = DEFAULT_OUTFIT_CIDS[rand_idx];
        let loadout_rand = 100_000_000_u64 + (OsRng.next_u64() % 9_899_999_999);

        let _ = meta.set_json(
            KEY_FRONTEND_MAP_MARKER_J,
            &FrontEndMapMarkerWrapper::default(),
        );
        let _ = meta.set_json(
            keys::FORT_STATS_J,
            &json!({
                "FORTStats": {
                    "fortitude": 0, "fortitude_Phoenix": 0,
                    "offense": 0, "offense_Phoenix": 0,
                    "resistance": 0, "resistance_Phoenix": 0,
                    "teamFortitude": 0, "teamFortitude_Phoenix": 0,
                    "teamOffense": 0, "teamOffense_Phoenix": 0,
                    "teamResistance": 0, "teamResistance_Phoenix": 0,
                    "teamTech": 0, "teamTech_Phoenix": 0,
                    "tech": 0, "tech_Phoenix": 0,
                }
            }),
        );
        meta.set_u64(KEY_NUM_ATHENA_PLAYERS_LEFT_U, 0);
        let _ = meta.set_json(
            KEY_CAMPAIGN_INFO_J,
            &json!({
                "CampaignInfo": {
                    "matchmakingLevel": 0,
                    "zoneInstanceId": "",
                    "homeBaseVersion": 1,
                    "bIsMatchmakingIntoHestiaBeauty": false,
                    "hestiaBeautySessionId": "",
                }
            }),
        );
        meta.set_f64(KEY_CAMPAIGN_BACKPACK_RATING_D, 0.0);
        meta.set_f64(KEY_CAMPAIGN_COMMANDER_LOADOUT_RATING_D, 0.0);
        let _ = meta.set_json(
            keys::BATTLE_PASS_INFO_J,
            &BattlePassWrapper {
                battle_pass_info: BattlePassSnapshot {
                    b_has_purchased_pass: false,
                    pass_level: 0,
                },
            },
        );
        let _ = meta.set_json(
            keys::PACKED_STATE_J,
            &json!({
                "PackedState": {
                    "bAllowEmoteBeatSyncing": true,
                    "bDownloadOnDemandActive": false,
                    "bIsInAllSelectExperiment": false,
                    "bIsPartyLFG": false,
                    "bRecText": false,
                    "bRecVoice": false,
                    "bUploadLogs": false,
                    "eOSProductUserId": "",
                    "gameMode": "None",
                    "hasCompletedSTWTutorial": true,
                    "hasPurchasedSTW": false,
                    "location": "PreLobby",
                    "platformSupportsSTW": true,
                    "subGame": "Athena",
                    "voiceChatStatus": "Enabled",
                }
            }),
        );
        meta.set_str(KEY_EPV, "1.0");
        let _ = meta.set_json(
            keys::ATHENA_BANNER_INFO_J,
            &json!({
                "AthenaBannerInfo": {
                    "bannerIconId": "",
                    "bannerColorId": "",
                    "seasonLevel": 1,
                }
            }),
        );
        let _ = meta.set_json(KEY_EXTENSIONS_J, &json!({ "Extensions": { "channels": {} } }));
        meta.set_str(KEY_FEAT_DEFINITION_S, "None");
        let _ = meta.set_json(
            keys::CAMPAIGN_HERO_J,
            &json!({
                "CampaignHero": {
                    "heroItemInstanceId": "",
                    "heroType": "None",
                }
            }),
        );
        let _ = meta.set_json(
            keys::SPECTATE_INFO_J,
            &json!({ "SpectateInfo": { "gameSessionId": "", "gameSessionKey": "" } }),
        );
        let _ = meta.set_json(keys::FRONTEND_EMOTE_J, &FrontendEmoteWrapper::default());
        let _ = meta.set_json(
            keys::FRONTEND_SPARKS_SONG_PART_J,
            &json!({
                "FrontendSparksSongPart": {
                    "pickable": "None",
                    "emoteEKey": "",
                    "emoteSection": -1,
                    "multipurposeEmoteData": -1,
                }
            }),
        );
        meta.set_bool(KEY_B_IS_PARTY_USING_PARTY_SIGNAL_B, false);
        let _ = meta.set_json(
            keys::MEMBER_SQUAD_ASSIGNMENT_REQUEST_J,
            &json!({
                "MemberSquadAssignmentRequest": {
                    "startingAbsoluteIdx": -1,
                    "swapTargetMemberId": "INVALID",
                    "targetAbsoluteIdx": -1,
                    "version": 0,
                }
            }),
        );
        meta.set_str(KEY_CROSSPLAY_PREFERENCE_S, "OptedIn");
        let _ = meta.set_json(
            keys::PLATFORM_DATA_J,
            &json!({
                "PlatformData": {
                    "platform": {
                        "platformDescription": {
                            "crossplayPool": "DESKTOP",
                            "externalAccountType": "",
                            "name": platform.as_str(),
                            "onlineSubsystem": "None",
                            "platformType": "DESKTOP",
                            "sessionType": "",
                        }
                    },
                    "sessionId": "",
                    "uniqueId": "INVALID",
                }
            }),
        );
        let current_island = serde_json::to_string(&json!({
            "LinkId": "",
            "MatchmakingSettingsV2": { "/Fortnite.com/Matchmaking:Region": "EU" },
            "Session": {
                "iD": "",
                "joinInfo": { "joinability": "CanNotBeJoinedOrWatched", "sessionKey": "" },
            },
        }))
        .unwrap_or_default();
        let selection_island = serde_json::to_string(&json!({
            "LinkId": "",
            "MatchmakingSettingsV2": {
                "/Fortnite.com/BattleRoyale/Matchmaking:BuildMode": "Build",
                "/Fortnite.com/BattleRoyale/Matchmaking:TeamSize": "Solo",
                "/Fortnite.com/Matchmaking:Ranked": "Unranked",
                "/Fortnite.com/Matchmaking:Region": "EU",
                "/Fortnite.com/Matchmaking:SquadFill": "NoFill",
            },
            "Session": {
                "iD": "",
                "joinInfo": { "joinability": "CanNotBeJoinedOrWatched", "sessionKey": "" },
            },
        }))
        .unwrap_or_default();
        let _ = meta.set_json(
            keys::MATCHMAKING_INFO_J,
            &json!({
                "MatchmakingInfo": {
                    "bHasOwnerStartedMM": false,
                    "bIsEligible": true,
                    "currentIsland": {
                        "island": current_island,
                        "bUsingGracefulUpgrade": true,
                        "matchmakingId": mm_id,
                        "timestamp": 0,
                    },
                    "islandSelection": {
                        "island": selection_island,
                        "bUsingGracefulUpgrade": true,
                        "matchmakingId": mm_id,
                        "timestamp": 0,
                    },
                    "maxMatchmakingDelay": 0,
                    "playlistVersion": 0,
                    "readyStatus": "NotReady",
                    "readyStatusMMId": "",
                    "result": "None",
                    "stayTogetherHash": 0,
                    "travelId": "",
                    "worldSessionId": "",
                }
            }),
        );
        meta.set_f64(KEY_DOWNLOAD_ON_DEMAND_PROGRESS_D, 0.0);
        meta.set_str(KEY_JOIN_METHOD_S, "Creation");
        meta.set_str(
            KEY_UTC_TIME_STARTED_MATCH_ATHENA_S,
            "0001-01-01T00:00:00.000Z",
        );
        let _ = meta.set_json(
            KEY_FRONTEND_MIMOSA_J,
            &json!({
                "FrontendMimosa": {
                    "frontendMimosaAnimType": "None",
                    "frontendMimosaInstanceId": "",
                }
            }),
        );
        let _ = meta.set_json(
            keys::LOBBY_STATE_J,
            &json!({
                "LobbyState": {
                    "currentInputType": "MouseAndKeyboard",
                    "hasPreloadedAthena": false,
                    "hiddenMatchmakingDelayMax": 0,
                    "inGameReadyCheckStatus": "None",
                    "readyInputType": "Count",
                }
            }),
        );

        let mut slots = BTreeMap::new();
        let slot_entries: [(&str, &str, Vec<Value>); 16] = [
            ("ac", default_outfit, Vec::new()),
            ("ap", "DefaultPickaxe", Vec::new()),
            ("ag", "DefaultGlider", Vec::new()),
            ("li", "BRS17_Alt_1", Vec::new()),
            ("lc", "DefaultColor22", Vec::new()),
            ("vo", "ID_Booster_Standard", vec![json!("0")]),
            ("vd", "ID_DriftTrail_Standard", vec![json!("0")]),
            ("vw", "ID_Wheel_OEM", vec![json!("0")]),
            ("vws", "ID_Wheel_OEM", vec![json!("0")]),
            ("vds", "ID_DriftTrail_Standard", vec![json!("0")]),
            ("vos", "ID_Booster_Standard", vec![json!("0")]),
            ("sb", "Sparks_Bass_Generic", vec![json!("0")]),
            ("sg", "Sparks_Guitar_Generic", vec![json!("0")]),
            ("sd", "Sparks_Drum_Generic", vec![json!("0")]),
            ("sk", "Sparks_Keytar_Generic", vec![json!("0")]),
            ("sm", "Sparks_Mic_Generic", vec![json!("0")]),
        ];
        for (slot_key, item_id, variants) in slot_entries {
            slots.insert(
                slot_key.to_owned(),
                MpLoadoutSlot {
                    i: item_id.to_owned(),
                    v: variants,
                },
            );
        }
        let _ = meta.set_json(
            keys::MP_LOADOUT_1_J,
            &MpLoadout1Wrapper {
                mp_loadout1: MpLoadout1Inner { s: slots },
            },
        );
        let _ = meta.set_json(keys::MP_LOADOUT_2_J, &json!({ "MpLoadout2": { "s": {} } }));
        let _ = meta.set_json(
            keys::LOADOUT_META_J,
            &LoadoutMetaWrapper {
                loadout_meta: LoadoutMetaInner {
                    en_keys: Vec::new(),
                    rand: loadout_rand,
                    v_assets: Vec::new(),
                    scratchpad: Vec::new(),
                    stats: vec![
                        LoadoutStat {
                            stat_name: "TotalVictoryCrowns".to_owned(),
                            stat_value: 0,
                        },
                        LoadoutStat {
                            stat_name: "TotalRoyalRoyales".to_owned(),
                            stat_value: 0,
                        },
                        LoadoutStat {
                            stat_name: "HasCrown".to_owned(),
                            stat_value: 0,
                        },
                        LoadoutStat {
                            stat_name: "HabaneroProgression".to_owned(),
                            stat_value: 0,
                        },
                    ],
                    force: 0,
                },
            },
        );
        let _ = meta.set_json(
            keys::JOIN_IN_PROGRESS_DATA_J,
            &json!({
                "JoinInProgressData": {
                    "request": { "target": "INVALID", "time": 0 },
                    "responses": [],
                }
            }),
        );

        if let Some(saved) = saved_meta {
            for (k, v) in saved.as_map() {
                meta.set_raw(k.clone(), v.clone());
            }
        }

        Self(meta)
    }

    pub fn set_eos_product_user_id(&mut self, puid: &str) {
        if let Ok(Some(mut val)) = self.0.get_json::<Value>(keys::PACKED_STATE_J) {
            if let Some(packed) = val.get_mut("PackedState").and_then(|v| v.as_object_mut()) {
                packed.insert("eOSProductUserId".to_owned(), Value::String(puid.to_owned()));
                let _ = self.0.set_json(keys::PACKED_STATE_J, &val);
            }
        }
    }

    fn loadout_slots(&self) -> Option<BTreeMap<String, MpLoadoutSlot>> {
        self.0
            .get_json::<MpLoadout1Wrapper>(keys::MP_LOADOUT_1_J)
            .ok()
            .flatten()
            .map(|w| w.mp_loadout1.s)
    }

    fn slot_item_id(&self, slot: &str) -> Option<String> {
        self.loadout_slots()
            .and_then(|slots| slots.get(slot).map(|entry| entry.i.clone()))
            .filter(|id| !id.is_empty())
    }

    pub fn outfit(&self) -> Option<String> {
        self.slot_item_id("ac")
    }

    pub fn pickaxe(&self) -> Option<String> {
        self.slot_item_id("ap")
    }

    pub fn backpack(&self) -> Option<String> {
        self.slot_item_id("ab")
    }

    pub fn shoes(&self) -> Option<String> {
        self.slot_item_id("as")
    }

    pub fn emote(&self) -> Option<String> {
        let pickable = self
            .0
            .get_json::<FrontendEmoteWrapper>(keys::FRONTEND_EMOTE_J)
            .ok()
            .flatten()?
            .frontend_emote
            .pickable;
        if pickable.is_empty() || pickable == "None" {
            return None;
        }
        let after_dot = pickable.rsplit_once('.').map(|(_, id)| id).unwrap_or(&pickable);
        (!after_dot.is_empty()).then(|| after_dot.to_owned())
    }

    pub fn ready_state(&self) -> ReadyState {
        self.0
            .get_json::<Value>(keys::MATCHMAKING_INFO_J)
            .ok()
            .flatten()
            .and_then(|v| {
                v.get("MatchmakingInfo")
                    .and_then(|m| m.get("readyStatus"))
                    .and_then(|s| serde_json::from_value::<ReadyState>(s.clone()).ok())
            })
            .unwrap_or(ReadyState::NotReady)
    }

    pub fn is_ready(&self) -> bool {
        self.ready_state() == ReadyState::Ready
    }

    pub fn is_sitting_out(&self) -> bool {
        self.ready_state() == ReadyState::SittingOut
    }

    pub fn input(&self) -> Option<String> {
        self.0
            .get_json::<Value>(keys::LOBBY_STATE_J)
            .ok()
            .flatten()
            .and_then(|v| {
                v.get("LobbyState")
                    .and_then(|l| l.get("currentInputType"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
    }

    pub fn banner(&self) -> Option<BannerSnapshot> {
        let slots = self.loadout_slots()?;
        let icon = slots.get("li").map(|s| s.i.clone());
        let color = slots.get("lc").map(|s| s.i.clone());
        if icon.is_none() && color.is_none() {
            return None;
        }
        Some(BannerSnapshot {
            banner_icon_id: icon,
            banner_color_id: color,
        })
    }

    pub fn battle_pass(&self) -> Option<BattlePassSnapshot> {
        self.0
            .get_json::<BattlePassWrapper>(keys::BATTLE_PASS_INFO_J)
            .ok()
            .flatten()
            .map(|w| w.battle_pass_info)
    }

    pub fn platform(&self) -> Option<Platform> {
        self.0
            .get_json::<Value>(keys::PLATFORM_DATA_J)
            .ok()
            .flatten()
            .and_then(|v| {
                v.pointer("/PlatformData/platform/platformDescription/name")
                    .and_then(Value::as_str)
                    .map(Platform::from_wire)
            })
    }

    pub fn match_meta(&self) -> MatchStateSnapshot {
        let packed = self.0.get_json::<Value>(keys::PACKED_STATE_J).ok().flatten();
        let lobby = self.0.get_json::<Value>(keys::LOBBY_STATE_J).ok().flatten();
        let player_count = self.0.get_u64(KEY_NUM_ATHENA_PLAYERS_LEFT_U).ok().flatten();
        let match_started_at = self
            .0
            .get_str(KEY_UTC_TIME_STARTED_MATCH_ATHENA_S)
            .map(str::to_owned);

        MatchStateSnapshot {
            location: packed
                .as_ref()
                .and_then(|v| v.pointer("/PackedState/location").and_then(Value::as_str))
                .map(str::to_owned),
            has_preloaded_athena: lobby
                .as_ref()
                .and_then(|v| {
                    v.pointer("/LobbyState/hasPreloadedAthena")
                        .and_then(Value::as_bool)
                })
                .unwrap_or(false),
            player_count,
            match_started_at,
        }
    }

    pub fn is_marker_set(&self) -> bool {
        self.0
            .get_json::<FrontEndMapMarkerWrapper>(KEY_FRONTEND_MAP_MARKER_J)
            .ok()
            .flatten()
            .map(|w| w.front_end_map_marker.b_is_set)
            .unwrap_or(false)
    }

    pub fn marker_location(&self) -> (f64, f64) {
        self.0
            .get_json::<FrontEndMapMarkerWrapper>(KEY_FRONTEND_MAP_MARKER_J)
            .ok()
            .flatten()
            .map(|w| {
                let loc = w.front_end_map_marker.marker_location;
                (loc.y, loc.x)
            })
            .unwrap_or((0.0, 0.0))
    }

    pub fn has_purchased_stw(&self) -> bool {
        self.0
            .get_json::<Value>(keys::PACKED_STATE_J)
            .ok()
            .flatten()
            .and_then(|v| {
                v.pointer("/PackedState/hasPurchasedSTW")
                    .and_then(Value::as_bool)
            })
            .unwrap_or(false)
    }

    fn record_json_update(
        &mut self,
        key: &str,
        value: &Value,
        updated: &mut BTreeMap<String, String>,
    ) -> Result<(), RustfnError> {
        let encoded = serde_json::to_string(value)?;
        self.0.set_raw(key, encoded.clone());
        updated.insert(key.to_owned(), encoded);
        Ok(())
    }

    fn build_ready_patch(
        &mut self,
        ready_status: &str,
        ready_input_type: &str,
        ready_mm_id: &str,
    ) -> Result<MetaPatch, RustfnError> {
        let mut info = self
            .0
            .get_json::<Value>(keys::MATCHMAKING_INFO_J)
            .ok()
            .flatten()
            .unwrap_or_else(|| json!({ "MatchmakingInfo": {} }));
        if let Some(obj) = info
            .get_mut("MatchmakingInfo")
            .and_then(Value::as_object_mut)
        {
            obj.insert("readyStatus".to_owned(), json!(ready_status));
            obj.insert("readyInputType".to_owned(), json!(ready_input_type));
            obj.insert("readyStatusMMId".to_owned(), json!(ready_mm_id));
        }

        let mut updated = BTreeMap::new();
        self.record_json_update(keys::MATCHMAKING_INFO_J, &info, &mut updated)?;
        Ok(MetaPatch {
            updated,
            deleted: Vec::new(),
        })
    }

    pub fn build_readiness_patch(&mut self, ready: bool) -> Result<MetaPatch, RustfnError> {
        if ready {
            let mm_id = Uuid::new_v4().simple().to_string().to_ascii_uppercase();
            self.build_ready_patch("Ready", "Touch", &mm_id)
        } else {
            self.build_ready_patch("NotReady", "Count", "")
        }
    }

    pub fn build_sitting_out_patch(&mut self, sitting_out: bool) -> Result<MetaPatch, RustfnError> {
        let status = if sitting_out {
            "SittingOut"
        } else {
            "NotReady"
        };
        self.build_ready_patch(status, "Count", "")
    }

    pub fn build_battle_pass_patch(
        &mut self,
        purchased: bool,
        level: u32,
    ) -> Result<MetaPatch, RustfnError> {
        let wrapper = BattlePassWrapper {
            battle_pass_info: BattlePassSnapshot {
                b_has_purchased_pass: purchased,
                pass_level: level,
            },
        };
        let encoded = serde_json::to_string(&wrapper)?;
        self.0.set_raw(keys::BATTLE_PASS_INFO_J, encoded.clone());
        let mut updated = BTreeMap::new();
        updated.insert(keys::BATTLE_PASS_INFO_J.to_owned(), encoded);
        Ok(MetaPatch {
            updated,
            deleted: Vec::new(),
        })
    }

    pub fn build_banner_patch(
        &mut self,
        banner_id: &str,
        color_id: &str,
    ) -> Result<MetaPatch, RustfnError> {
        let mut wrapper = self
            .0
            .get_json::<MpLoadout1Wrapper>(keys::MP_LOADOUT_1_J)
            .ok()
            .flatten()
            .unwrap_or_default();
        wrapper
            .mp_loadout1
            .s
            .entry("li".to_owned())
            .or_default()
            .i = banner_id.to_owned();
        wrapper
            .mp_loadout1
            .s
            .entry("lc".to_owned())
            .or_default()
            .i = color_id.to_owned();
        let encoded = serde_json::to_string(&wrapper)?;
        self.0.set_raw(keys::MP_LOADOUT_1_J, encoded.clone());
        let mut updated = BTreeMap::new();
        updated.insert(keys::MP_LOADOUT_1_J.to_owned(), encoded);
        Ok(MetaPatch {
            updated,
            deleted: Vec::new(),
        })
    }

    pub fn build_cosmetics_patch(
        &mut self,
        update: &CosmeticsUpdate,
    ) -> Result<MetaPatch, RustfnError> {
        let mut loadout = self
            .0
            .get_json::<MpLoadout1Wrapper>(keys::MP_LOADOUT_1_J)
            .ok()
            .flatten()
            .unwrap_or_default();
        let mut loadout_meta = self
            .0
            .get_json::<LoadoutMetaWrapper>(keys::LOADOUT_META_J)
            .ok()
            .flatten()
            .unwrap_or_default();
        let mut updated = BTreeMap::new();

        if let Some(outfit) = &update.outfit {
            loadout.mp_loadout1.s.insert(
                "ac".to_owned(),
                MpLoadoutSlot {
                    i: outfit.id.clone(),
                    v: build_variant_array(&outfit.variants),
                },
            );
            loadout_meta.loadout_meta.scratchpad = match outfit.enlightment {
                Some(enl) => vec![json!({ "t": enl.season_or_type, "v": enl.level })],
                None => Vec::new(),
            };
            let encoded_meta = serde_json::to_string(&loadout_meta)?;
            self.0.set_raw(keys::LOADOUT_META_J, encoded_meta.clone());
            updated.insert(keys::LOADOUT_META_J.to_owned(), encoded_meta);
        }

        if let Some(backpack) = &update.backpack {
            match backpack {
                Some(backpack) => {
                    loadout.mp_loadout1.s.insert(
                        "ab".to_owned(),
                        MpLoadoutSlot {
                            i: backpack.id.clone(),
                            v: build_variant_array(&backpack.variants),
                        },
                    );
                }
                None => {
                    loadout.mp_loadout1.s.remove("ab");
                }
            }
        }

        if let Some(pickaxe) = &update.pickaxe {
            loadout.mp_loadout1.s.insert(
                "ap".to_owned(),
                MpLoadoutSlot {
                    i: pickaxe.id.clone(),
                    v: build_variant_array(&pickaxe.variants),
                },
            );
        }

        if let Some(shoes) = &update.shoes {
            match shoes {
                Some(shoes) => {
                    loadout.mp_loadout1.s.insert(
                        "as".to_owned(),
                        MpLoadoutSlot {
                            i: shoes.id.clone(),
                            v: Vec::new(),
                        },
                    );
                }
                None => {
                    loadout.mp_loadout1.s.remove("as");
                }
            }
        }

        let encoded_loadout = serde_json::to_string(&loadout)?;
        self.0
            .set_raw(keys::MP_LOADOUT_1_J, encoded_loadout.clone());
        updated.insert(keys::MP_LOADOUT_1_J.to_owned(), encoded_loadout);

        Ok(MetaPatch {
            updated,
            deleted: Vec::new(),
        })
    }

    pub fn build_emote_patch(
        &mut self,
        emote_id: &str,
        path: Option<&str>,
    ) -> Result<MetaPatch, RustfnError> {
        let base_path = path
            .unwrap_or("/BRCosmetics/Athena/Items/Cosmetics/Dances")
            .trim_end_matches('/');
        let mut wrapper = self
            .0
            .get_json::<FrontendEmoteWrapper>(keys::FRONTEND_EMOTE_J)
            .ok()
            .flatten()
            .unwrap_or_default();
        wrapper.frontend_emote.pickable = format!("{base_path}/{emote_id}.{emote_id}");
        wrapper.frontend_emote.emote_section = -2;
        let encoded = serde_json::to_string(&wrapper)?;
        self.0.set_raw(keys::FRONTEND_EMOTE_J, encoded.clone());
        let mut updated = BTreeMap::new();
        updated.insert(keys::FRONTEND_EMOTE_J.to_owned(), encoded);
        Ok(MetaPatch {
            updated,
            deleted: Vec::new(),
        })
    }

    pub fn build_clear_emote_patch(&mut self) -> Result<MetaPatch, RustfnError> {
        let mut wrapper = self
            .0
            .get_json::<FrontendEmoteWrapper>(keys::FRONTEND_EMOTE_J)
            .ok()
            .flatten()
            .unwrap_or_default();
        wrapper.frontend_emote.pickable = "None".to_owned();
        wrapper.frontend_emote.emote_section = -1;
        let encoded = serde_json::to_string(&wrapper)?;
        self.0.set_raw(keys::FRONTEND_EMOTE_J, encoded.clone());
        let mut updated = BTreeMap::new();
        updated.insert(keys::FRONTEND_EMOTE_J.to_owned(), encoded);
        Ok(MetaPatch {
            updated,
            deleted: Vec::new(),
        })
    }

    pub fn build_playing_patch(
        &mut self,
        is_playing: bool,
        player_count: u8,
        started_at: DateTime<Utc>,
    ) -> Result<MetaPatch, RustfnError> {
        let mut packed = self
            .0
            .get_json::<Value>(keys::PACKED_STATE_J)
            .ok()
            .flatten()
            .unwrap_or_else(|| json!({ "PackedState": {} }));
        if let Some(obj) = packed.get_mut("PackedState").and_then(Value::as_object_mut) {
            obj.insert(
                "location".to_owned(),
                json!(if is_playing { "InGame" } else { "PreLobby" }),
            );
            obj.insert(
                "gameMode".to_owned(),
                json!(if is_playing { "InBattleRoyale" } else { "None" }),
            );
        }

        let mut updated = BTreeMap::new();
        self.record_json_update(keys::PACKED_STATE_J, &packed, &mut updated)?;

        let count_str = player_count.to_string();
        self.0.set_raw(KEY_NUM_ATHENA_PLAYERS_LEFT_U, &count_str);
        updated.insert(KEY_NUM_ATHENA_PLAYERS_LEFT_U.to_owned(), count_str);

        let started_str = started_at.to_rfc3339();
        self.0
            .set_raw(KEY_UTC_TIME_STARTED_MATCH_ATHENA_S, &started_str);
        updated.insert(KEY_UTC_TIME_STARTED_MATCH_ATHENA_S.to_owned(), started_str);

        Ok(MetaPatch {
            updated,
            deleted: Vec::new(),
        })
    }

    pub fn build_marker_patch(
        &mut self,
        is_set: bool,
        location_x: f64,
        location_y: f64,
    ) -> Result<MetaPatch, RustfnError> {
        let wrapper = FrontEndMapMarkerWrapper {
            front_end_map_marker: FrontEndMapMarkerInner {
                b_is_set: is_set,
                marker_location: FrontEndMapMarkerLocation {
                    x: location_y,
                    y: location_x,
                },
            },
        };
        let encoded = serde_json::to_string(&wrapper)?;
        self.0
            .set_raw(KEY_FRONTEND_MAP_MARKER_J, encoded.clone());
        let mut updated = BTreeMap::new();
        updated.insert(KEY_FRONTEND_MAP_MARKER_J.to_owned(), encoded);
        Ok(MetaPatch {
            updated,
            deleted: Vec::new(),
        })
    }

    pub fn build_cosmetic_stats_patch(
        &mut self,
        crowns: i64,
        ranked_progression: i64,
    ) -> Result<MetaPatch, RustfnError> {
        let mut wrapper = self
            .0
            .get_json::<LoadoutMetaWrapper>(keys::LOADOUT_META_J)
            .ok()
            .flatten()
            .unwrap_or_default();
        let stats = &mut wrapper.loadout_meta.stats;
        upsert_stat(stats, "HabaneroProgression", ranked_progression);
        upsert_stat(stats, "TotalRoyalRoyales", crowns);
        upsert_stat(stats, "TotalVictoryCrowns", crowns);
        upsert_stat(stats, "HasCrown", if crowns > 0 { 1 } else { 0 });

        let encoded = serde_json::to_string(&wrapper)?;
        self.0.set_raw(keys::LOADOUT_META_J, encoded.clone());
        let mut updated = BTreeMap::new();
        updated.insert(keys::LOADOUT_META_J.to_owned(), encoded);
        Ok(MetaPatch {
            updated,
            deleted: Vec::new(),
        })
    }
}

fn upsert_stat(stats: &mut Vec<LoadoutStat>, name: &str, value: i64) {
    if let Some(entry) = stats.iter_mut().find(|s| s.stat_name == name) {
        entry.stat_value = value;
    } else {
        stats.push(LoadoutStat {
            stat_name: name.to_owned(),
            stat_value: value,
        });
    }
}

fn build_variant_array(variants: &[CosmeticVariant]) -> Vec<Value> {
    if variants.is_empty() {
        return Vec::new();
    }
    let highest = variants.iter().map(|v| v.channel_index).max().unwrap_or(0);
    let mut array = vec![json!(0); highest + 1];
    for variant in variants {
        array[variant.channel_index] = match &variant.variant_index {
            VariantValue::Int(i) => json!(i),
            VariantValue::Text(s) => json!(s),
        };
    }
    array
}
