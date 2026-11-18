use crate::http::{AuthSessionSlot, HttpClient, HttpRequestSpec, RetryDecisionHook};
use crate::protocol::{
    AccountId, ConversationType, EOSPartyData, EOSPartyDataConfig, EOSPartyUserState,
    ExtraServiceEndpoints, FORTNITE_CATALOG_ITEM_ID, LobbyId, MetaMap, MetaPatch,
    PartyDiscoverability, PartyId, PartyJoinability, Platform, PresenceOnlineType,
    PresencePropsMap, RustfnError, ServiceEndpoints, UserSearchMatchType, UserSearchPlatform,
    parse_build_version_cl,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tracing::{debug, warn};
use url::form_urlencoded;
use uuid::Uuid;

/// Backend source that supplied the live Fortnite build version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildVersionSource {
    LauncherAssetsV2,
    LauncherAssetsV1,
}

/// Resolved live Fortnite build string, changelist (`netCL`), and party build ID (`1:3:<CL>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildInfo {
    pub build_version: String,
    pub net_cl: String,
    pub party_build_id: String,
    pub source: BuildVersionSource,
}

impl BuildInfo {
    pub fn from_build_version(build_version: String, source: BuildVersionSource) -> Self {
        let net_cl = parse_build_version_cl(&build_version)
            .unwrap_or("0")
            .to_owned();
        let party_build_id = format!("1:3:{net_cl}");
        Self {
            build_version,
            net_cl,
            party_build_id,
            source,
        }
    }
}

#[derive(Debug, Deserialize)]
struct AssetsV2Response {
    #[serde(default)]
    elements: Vec<AssetsElement>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssetsElement {
    #[serde(default)]
    build_version: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MatchmakingSessionSummary {
    #[serde(default)]
    build_unique_id: Option<Value>,
}

/// Maps internal [`Platform`] codes to the launcher Assets API platform string.
pub fn launcher_assets_platform(platform: Platform) -> &'static str {
    match platform {
        Platform::Win => "Windows",
        Platform::Mac => "Mac",
        Platform::And => "Android",
        Platform::Ios => "IOS",
        _ => "Windows",
    }
}

/// Resolves live Fortnite build metadata and party matchmaking build IDs.
pub struct FortniteBuildResolver;

impl FortniteBuildResolver {
    /// Queries Launcher Assets V2 -> Launcher Assets V1 until a build version resolves.
    pub async fn fetch_build_version(
        http: &HttpClient,
        launcher_public_base: &str,
        platform: Platform,
    ) -> Result<BuildInfo, RustfnError> {
        let api_platform = launcher_assets_platform(platform);
        let launcher_base = launcher_public_base.trim_end_matches('/');

        let url_v2 = format!(
            "{launcher_base}/public/assets/v2/platform/{api_platform}/namespace/fn/catalogItem/{FORTNITE_CATALOG_ITEM_ID}/app/Fortnite/label/Live"
        );
        match http
            .execute_json::<AssetsV2Response>(
                HttpRequestSpec::get(url_v2)
                    .with_auth(AuthSessionSlot::Launcher)
                    .with_route_key("GET launcher/public/assets/v2"),
            )
            .await
        {
            Ok(resp) => {
                if let Some(version) = resp
                    .elements
                    .into_iter()
                    .find_map(|el| el.build_version.filter(|s| !s.is_empty()))
                {
                    debug!(%version, "resolved Fortnite build via Launcher Assets V2");
                    return Ok(BuildInfo::from_build_version(
                        version,
                        BuildVersionSource::LauncherAssetsV2,
                    ));
                }
            }
            Err(err) => {
                warn!(error = %err, "Launcher Assets V2 build lookup failed; falling back to V1");
            }
        }

        let url_v1 = format!(
            "{launcher_base}/public/assets/{api_platform}/{FORTNITE_CATALOG_ITEM_ID}/Fortnite"
        );
        let resp: AssetsElement = http
            .execute_json(
                HttpRequestSpec::get(url_v1)
                    .with_query("label", "Live")
                    .with_auth(AuthSessionSlot::Launcher)
                    .with_route_key("GET launcher/public/assets/v1"),
            )
            .await?;

        let version = resp
            .build_version
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                RustfnError::Protocol(
                    "Launcher Assets V1 response did not contain a build version".to_owned(),
                )
            })?;
        debug!(%version, "resolved Fortnite build via Launcher Assets V1");
        Ok(BuildInfo::from_build_version(
            version,
            BuildVersionSource::LauncherAssetsV1,
        ))
    }

    /// Queries matchmaking sessions to obtain the active party `1:3:<buildUniqueId>` identifier.
    pub async fn fetch_party_build_id(
        http: &HttpClient,
        matchmaking_request_url: &str,
    ) -> Result<String, RustfnError> {
        let spec = HttpRequestSpec::post(matchmaking_request_url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_route_key("POST fortnite/matchmaking/session/matchMakingRequest")
            .with_json(&json!({
                "criteria": [],
                "openPlayersRequired": 1,
                "buildUniqueId": "",
                "maxResults": 1,
            }))?;

        let entries: Vec<MatchmakingSessionSummary> = http.execute_json(spec).await?;
        let raw_id = entries
            .into_iter()
            .find_map(|entry| entry.build_unique_id)
            .ok_or_else(|| {
                RustfnError::Protocol(
                    "matchmaking response did not contain a party buildUniqueId".to_owned(),
                )
            })?;

        let net_cl = match raw_id {
            Value::String(s) if !s.is_empty() => s,
            Value::Number(n) => n.to_string(),
            other => {
                return Err(RustfnError::Protocol(format!(
                    "unexpected buildUniqueId value in matchmaking response: {other}"
                )));
            }
        };

        Ok(format!("1:3:{net_cl}"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountRecordResponse {
    pub id: AccountId,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub external_auths: BTreeMap<String, Value>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub last_name: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub country: Option<String>,
    #[serde(default)]
    pub preferred_language: Option<String>,
    #[serde(default)]
    pub minor_verified: Option<bool>,
    #[serde(default)]
    pub minor_expected: Option<bool>,
    #[serde(default)]
    pub minor_status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AvatarResponse {
    pub account_id: AccountId,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub avatar_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserSearchEntryResponse {
    pub account_id: AccountId,
    #[serde(default)]
    pub matches: Vec<UserSearchMatchEntry>,
    #[serde(default)]
    pub match_type: Option<UserSearchMatchType>,
    #[serde(default)]
    pub epic_mutuals: Option<u32>,
    #[serde(default)]
    pub sort_position: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserSearchMatchEntry {
    pub value: String,
    pub platform: UserSearchPlatform,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlobalProfileItemResponse {
    pub account_id: AccountId,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub avatar_id: Option<String>,
    #[serde(default)]
    pub preferred_language: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GlobalProfileBatchResponse {
    #[serde(default)]
    profiles: Vec<GlobalProfileItemResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FriendsSummaryEntry {
    pub account_id: AccountId,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(default)]
    pub mutual: Option<u32>,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub favorite: Option<bool>,
    #[serde(default)]
    pub created: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingFriendSummaryEntry {
    pub account_id: AccountId,
    #[serde(default)]
    pub mutual: Option<u32>,
    #[serde(default)]
    pub favorite: Option<bool>,
    #[serde(default)]
    pub created: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockedUserSummaryEntry {
    pub account_id: AccountId,
    #[serde(default)]
    pub created: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FriendsSummaryResponse {
    #[serde(default)]
    pub friends: Vec<FriendsSummaryEntry>,
    #[serde(default)]
    pub incoming: Vec<PendingFriendSummaryEntry>,
    #[serde(default)]
    pub outgoing: Vec<PendingFriendSummaryEntry>,
    #[serde(default)]
    pub blocklist: Vec<BlockedUserSummaryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DmConversationResponse {
    pub conversation_id: String,
    #[serde(rename = "type")]
    pub conversation_type: ConversationType,
    #[serde(default)]
    pub is_reportable: bool,
    #[serde(default)]
    pub members: Vec<AccountId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PublicKeyResponse {
    pub key: String,
    #[serde(default, alias = "type")]
    pub algorithm: String,
    pub jwt: String,
    #[serde(default, rename = "keyGuid", alias = "key_guid")]
    pub key_guid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatMessageMetadata {
    #[serde(rename = "TmV")]
    pub tm_v: String,
    #[serde(rename = "Pub")]
    pub pub_jwt: String,
    #[serde(rename = "Sig")]
    pub sig: String,
    #[serde(rename = "NPM", skip_serializing_if = "Option::is_none")]
    pub npm: Option<String>,
    #[serde(rename = "PlfNm")]
    pub plf_nm: String,
    #[serde(rename = "PlfId")]
    pub plf_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatSendMessageRequest {
    pub allowed_recipients: Vec<AccountId>,
    pub message: ChatSendMessageBody,
    pub is_reportable: bool,
    pub metadata: ChatMessageMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatSendMessageBody {
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatSendMessageResponse {
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub timestamp: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EOSInternalPartyPresence {
    #[serde(rename = "type")]
    pub party_type: crate::protocol::PartyJoinability,
    pub id: PartyId,
    pub client_joinable: bool,
    pub member_count: usize,
    pub timestamp: String,
}

fn default_party_type() -> String {
    "DEFAULT".to_owned()
}

fn default_sub_type() -> String {
    "default".to_owned()
}

fn default_max_size() -> u32 {
    16
}

fn default_invite_ttl() -> u64 {
    14400
}

fn default_intention_ttl() -> u64 {
    60
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FortnitePartyConfig {
    #[serde(default = "default_party_type")]
    pub r#type: String,
    #[serde(default)]
    pub joinability: PartyJoinability,
    #[serde(default)]
    pub discoverability: PartyDiscoverability,
    #[serde(default = "default_sub_type")]
    pub sub_type: String,
    #[serde(default = "default_max_size")]
    pub max_size: u32,
    #[serde(default = "default_invite_ttl")]
    pub invite_ttl: u64,
    #[serde(default)]
    pub join_confirmation: bool,
    #[serde(default = "default_intention_ttl")]
    pub intention_ttl: u64,
}

impl Default for FortnitePartyConfig {
    fn default() -> Self {
        Self {
            r#type: default_party_type(),
            joinability: PartyJoinability::Open,
            discoverability: PartyDiscoverability::All,
            sub_type: default_sub_type(),
            max_size: default_max_size(),
            invite_ttl: default_invite_ttl(),
            join_confirmation: false,
            intention_ttl: default_intention_ttl(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FnPartyMemberConnection {
    pub id: String,
    #[serde(default)]
    pub connected_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub yield_leadership: bool,
    #[serde(default)]
    pub meta: MetaMap,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FortnitePartyMemberData {
    #[serde(default)]
    pub id: Option<String>,
    pub account_id: AccountId,
    #[serde(default)]
    pub account_dn: Option<String>,
    #[serde(default)]
    pub meta: MetaMap,
    #[serde(default)]
    pub connections: Vec<FnPartyMemberConnection>,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub joined_at: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FortnitePartyData {
    pub id: LobbyId,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    pub config: FortnitePartyConfig,
    #[serde(default)]
    pub members: Vec<FortnitePartyMemberData>,
    #[serde(default)]
    pub meta: MetaMap,
    #[serde(default)]
    pub invites: Vec<Value>,
    #[serde(default)]
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FnPartyUserState {
    #[serde(default)]
    pub current: Vec<FortnitePartyData>,
    #[serde(default)]
    pub pending: Vec<Value>,
    #[serde(default)]
    pub invites: Vec<Value>,
    #[serde(default)]
    pub pings: Vec<Value>,
}

#[derive(Debug, Serialize)]
struct WireMetaPatch<'a> {
    delete: &'a [String],
    update: &'a BTreeMap<String, String>,
}

/// Epic Games and EOS HTTP client built atop [`HttpClient`].
#[derive(Clone)]
pub struct HTTP {
    http: HttpClient,
    endpoints: ServiceEndpoints,
    extra: ExtraServiceEndpoints,
}

impl std::fmt::Debug for HTTP {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HTTP")
            .field("br_party", &self.endpoints.br_party)
            .field("eos_party", &self.endpoints.eos_party)
            .finish()
    }
}

impl HTTP {
    pub fn new(
        http: HttpClient,
        endpoints: ServiceEndpoints,
        extra: ExtraServiceEndpoints,
    ) -> Self {
        Self {
            http,
            endpoints,
            extra,
        }
    }

    pub fn client(&self) -> &HttpClient {
        &self.http
    }

    pub fn endpoints(&self) -> &ServiceEndpoints {
        &self.endpoints
    }

    pub fn extra_endpoints(&self) -> &ExtraServiceEndpoints {
        &self.extra
    }

    pub async fn fetch_build_version(
        &self,
        platform: Platform,
    ) -> Result<BuildInfo, RustfnError> {
        FortniteBuildResolver::fetch_build_version(
            &self.http,
            self.endpoints.launcher_public.as_ref(),
            platform,
        )
        .await
    }

    pub async fn fetch_party_build_id(&self) -> Result<String, RustfnError> {
        FortniteBuildResolver::fetch_party_build_id(&self.http, &self.extra.br_matchmaking_request)
            .await
    }

    pub async fn fetch_account_by_id(
        &self,
        account_id: &AccountId,
    ) -> Result<AccountRecordResponse, RustfnError> {
        let url = format!(
            "{}/{}",
            self.endpoints.account_public.trim_end_matches('/'),
            account_id.as_str()
        );
        self.http
            .execute_json(
                HttpRequestSpec::get(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_route_key("GET account/public/account/{id}"),
            )
            .await
    }

    pub async fn fetch_account_by_display_name(
        &self,
        display_name: &str,
    ) -> Result<AccountRecordResponse, RustfnError> {
        let encoded: String =
            form_urlencoded::byte_serialize(display_name.as_bytes()).collect();
        let url = format!(
            "{}/displayName/{}",
            self.endpoints.account_public.trim_end_matches('/'),
            encoded
        );
        self.http
            .execute_json(
                HttpRequestSpec::get(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_route_key("GET account/public/account/displayName/{dn}"),
            )
            .await
    }

    pub async fn fetch_accounts_multiple(
        &self,
        account_ids: &[AccountId],
    ) -> Result<Vec<AccountRecordResponse>, RustfnError> {
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let url = self.endpoints.account_public.trim_end_matches('/').to_owned();
        let mut spec = HttpRequestSpec::get(url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_route_key("GET account/public/account/multiple");
        for id in account_ids {
            spec = spec.with_query("accountId", id.as_str());
        }
        self.http.execute_json(spec).await
    }

    pub async fn search_users(
        &self,
        client_id: &AccountId,
        prefix: &str,
        platform: UserSearchPlatform,
    ) -> Result<Vec<UserSearchEntryResponse>, RustfnError> {
        let url = format!(
            "{}/{}",
            self.endpoints.user_search.trim_end_matches('/'),
            client_id.as_str()
        );
        let platform_str = match platform {
            UserSearchPlatform::Epic => "epic",
            UserSearchPlatform::Psn => "psn",
            UserSearchPlatform::Xbl => "xbl",
        };
        self.http
            .execute_json(
                HttpRequestSpec::get(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_route_key("GET user-search/v1/search/{id}")
                    .with_query("prefix", prefix)
                    .with_query("platform", platform_str),
            )
            .await
    }

    pub async fn fetch_avatars_multiple(
        &self,
        account_ids: &[AccountId],
    ) -> Result<Vec<AvatarResponse>, RustfnError> {
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }
        let joined = account_ids
            .iter()
            .map(AccountId::as_str)
            .collect::<Vec<_>>()
            .join(",");
        let url = format!(
            "{}/fortnite/ids",
            self.extra.account_avatar.trim_end_matches('/')
        );
        self.http
            .execute_json(
                HttpRequestSpec::get(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_route_key("GET avatar/fortnite/ids")
                    .with_query("accountIds", joined),
            )
            .await
    }

    pub async fn fetch_global_profiles_multiple(
        &self,
        account_ids: &[AccountId],
    ) -> Result<Vec<GlobalProfileItemResponse>, RustfnError> {
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }
        let spec = HttpRequestSpec::put(&self.extra.account_global_profile)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_route_key("PUT global-profile/profiles")
            .with_json(&json!({
                "namespace": "Fortnite",
                "accountIds": account_ids,
            }))?;
        let resp: GlobalProfileBatchResponse = self.http.execute_json(spec).await?;
        Ok(resp.profiles)
    }

    pub async fn fetch_friends_summary(
        &self,
        client_id: &AccountId,
    ) -> Result<FriendsSummaryResponse, RustfnError> {
        let url = format!(
            "{}/{}/summary",
            self.endpoints.friends.trim_end_matches('/'),
            client_id.as_str()
        );
        self.http
            .execute_json(
                HttpRequestSpec::get(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_account_scope(client_id.clone())
                    .with_route_key("GET friends/v1/{id}/summary"),
            )
            .await
    }

    pub async fn add_friend(
        &self,
        client_id: &AccountId,
        target_id: &AccountId,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/{}/{}",
            self.endpoints.friends_public.trim_end_matches('/'),
            client_id.as_str(),
            target_id.as_str()
        );
        self.http
            .execute_empty(
                HttpRequestSpec::post(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_account_scope(client_id.clone())
                    .with_route_key("POST friends/public/{id}/{target}"),
            )
            .await
    }

    pub async fn remove_friend(
        &self,
        client_id: &AccountId,
        target_id: &AccountId,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/{}/friends/{}",
            self.endpoints.friends.trim_end_matches('/'),
            client_id.as_str(),
            target_id.as_str()
        );
        self.http
            .execute_empty(
                HttpRequestSpec::delete(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_account_scope(client_id.clone())
                    .with_route_key("DELETE friends/v1/{id}/friends/{target}"),
            )
            .await
    }

    pub async fn block_user(
        &self,
        client_id: &AccountId,
        target_id: &AccountId,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/{}/{}",
            self.endpoints.friends_blocklist_public.trim_end_matches('/'),
            client_id.as_str(),
            target_id.as_str()
        );
        self.http
            .execute_empty(
                HttpRequestSpec::post(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_account_scope(client_id.clone())
                    .with_route_key("POST friends/public/blocklist/{id}/{target}"),
            )
            .await
    }

    pub async fn unblock_user(
        &self,
        client_id: &AccountId,
        target_id: &AccountId,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/{}/{}",
            self.endpoints.friends_blocklist_public.trim_end_matches('/'),
            client_id.as_str(),
            target_id.as_str()
        );
        self.http
            .execute_empty(
                HttpRequestSpec::delete(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_account_scope(client_id.clone())
                    .with_route_key("DELETE friends/public/blocklist/{id}/{target}"),
            )
            .await
    }

    async fn put_friend_text_subresource(
        &self,
        client_id: &AccountId,
        target_id: &AccountId,
        subresource: &str,
        value: &str,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/{}/friends/{}/{}",
            self.endpoints.friends.trim_end_matches('/'),
            client_id.as_str(),
            target_id.as_str(),
            subresource
        );
        self.http
            .execute_empty(
                HttpRequestSpec::put(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_account_scope(client_id.clone())
                    .with_route_key(format!("PUT friends/v1/{{id}}/friends/{{target}}/{subresource}"))
                    .with_text_body("text/plain", value),
            )
            .await
    }

    async fn delete_friend_subresource(
        &self,
        client_id: &AccountId,
        target_id: &AccountId,
        subresource: &str,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/{}/friends/{}/{}",
            self.endpoints.friends.trim_end_matches('/'),
            client_id.as_str(),
            target_id.as_str(),
            subresource
        );
        self.http
            .execute_empty(
                HttpRequestSpec::delete(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_account_scope(client_id.clone())
                    .with_route_key(format!("DELETE friends/v1/{{id}}/friends/{{target}}/{subresource}")),
            )
            .await
    }

    pub async fn set_friend_alias(
        &self,
        client_id: &AccountId,
        target_id: &AccountId,
        alias: &str,
    ) -> Result<(), RustfnError> {
        self.put_friend_text_subresource(client_id, target_id, "alias", alias)
            .await
    }

    pub async fn remove_friend_alias(
        &self,
        client_id: &AccountId,
        target_id: &AccountId,
    ) -> Result<(), RustfnError> {
        self.delete_friend_subresource(client_id, target_id, "alias")
            .await
    }

    pub async fn set_friend_note(
        &self,
        client_id: &AccountId,
        target_id: &AccountId,
        note: &str,
    ) -> Result<(), RustfnError> {
        self.put_friend_text_subresource(client_id, target_id, "note", note)
            .await
    }

    pub async fn remove_friend_note(
        &self,
        client_id: &AccountId,
        target_id: &AccountId,
    ) -> Result<(), RustfnError> {
        self.delete_friend_subresource(client_id, target_id, "note")
            .await
    }

    pub async fn fetch_mutual_friends(
        &self,
        client_id: &AccountId,
        target_id: &AccountId,
    ) -> Result<Vec<AccountId>, RustfnError> {
        let url = format!(
            "{}/{}/friends/{}/mutual",
            self.endpoints.friends.trim_end_matches('/'),
            client_id.as_str(),
            target_id.as_str()
        );
        self.http
            .execute_json(
                HttpRequestSpec::get(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_account_scope(client_id.clone())
                    .with_route_key("GET friends/v1/{id}/friends/{target}/mutual"),
            )
            .await
    }

    pub async fn register_public_key(
        &self,
        public_key_b64: &str,
        allow_fallback: bool,
    ) -> Result<PublicKeyResponse, RustfnError> {
        let primary_url = format!(
            "{}/v2/publickey",
            self.endpoints.publickey.trim_end_matches('/')
        );
        let body = json!({
            "key": public_key_b64,
            "algorithm": "ed25519",
        });

        let primary_spec = HttpRequestSpec::post(primary_url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_route_key("POST publickey/v2/publickey")
            .with_json(&body)?;

        match self.http.execute_json::<PublicKeyResponse>(primary_spec).await {
            Ok(resp) => Ok(resp),
            Err(err) if allow_fallback && should_try_publickey_fallback(&err) => {
                let fallback_url = format!(
                    "{}/v2/publickey",
                    self.endpoints.publickey_fallback.trim_end_matches('/')
                );
                let fallback_spec = HttpRequestSpec::post(fallback_url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_route_key("POST publickey/v2/publickey/fallback")
                    .with_json(&body)?;
                self.http.execute_json(fallback_spec).await
            }
            Err(err) => Err(err),
        }
    }

    pub async fn create_dm_conversation(
        &self,
        self_id: &AccountId,
        recipient_id: &AccountId,
        create_if_exists: bool,
    ) -> Result<DmConversationResponse, RustfnError> {
        let url = format!(
            "{}/v1/public/_/conversations",
            self.endpoints.eos_chat.trim_end_matches('/')
        );
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_query("createIfExists", if create_if_exists { "true" } else { "false" })
            .with_route_key("POST epic/chat/v1/public/_/conversations")
            .with_json(&json!({
                "title": "",
                "type": "dm",
                "members": [self_id, recipient_id],
            }))?;
        self.http.execute_json(spec).await
    }

    pub async fn send_eos_conversation_message(
        &self,
        scope: &str,
        conversation_id: &str,
        sender_id: &AccountId,
        request: &ChatSendMessageRequest,
    ) -> Result<ChatSendMessageResponse, RustfnError> {
        let encoded_conv: String =
            form_urlencoded::byte_serialize(conversation_id.as_bytes()).collect();
        let url = format!(
            "{}/v1/public/{}/conversations/{}/messages",
            self.endpoints.eos_chat.trim_end_matches('/'),
            scope,
            encoded_conv
        );
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_query("fromAccountId", sender_id.as_str())
            .with_account_scope(sender_id.clone())
            .with_route_key("POST epic/chat/v1/public/{scope}/conversations/{id}/messages")
            .with_json(request)?;
        let resp = self.http.execute_bytes(spec).await?;
        if resp.body.iter().all(|byte| byte.is_ascii_whitespace()) {
            return Ok(ChatSendMessageResponse::default());
        }
        serde_json::from_slice(&resp.body).map_err(RustfnError::Serialization)
    }

    pub async fn send_eos_whisper(
        &self,
        namespace: &str,
        sender_id: &AccountId,
        recipient_id: &AccountId,
        body: &Value,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/v1/public/{}/whispers/{}/{}",
            self.endpoints.eos_chat.trim_end_matches('/'),
            namespace,
            sender_id.as_str(),
            recipient_id.as_str()
        );
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_account_scope(sender_id.clone())
            .with_route_key("POST epic/chat/v1/public/{ns}/whispers/{sender}/{recipient}")
            .with_json(body)?;
        self.http.execute_empty(spec).await
    }

    pub async fn patch_eos_presence(
        &self,
        deployment_id: &str,
        account_id: &AccountId,
        public_connection_id: &str,
        online_type: PresenceOnlineType,
        activity_value: &str,
        props: &PresencePropsMap,
        retry_decision: Option<RetryDecisionHook>,
    ) -> Result<(), RustfnError> {
        let encoded_conn: String =
            form_urlencoded::byte_serialize(public_connection_id.as_bytes()).collect();
        let url = format!(
            "{}/{}/{}/presence/{}",
            self.endpoints.eos_presence.trim_end_matches('/'),
            deployment_id,
            account_id.as_str(),
            encoded_conn
        );
        let mut spec = HttpRequestSpec::patch(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_account_scope(account_id.clone())
            .with_route_key("PATCH epic/presence/v1/{ns}/{id}/presence/{conn}")
            .with_json(&json!({
                "status": online_type,
                "activity": { "value": activity_value },
                "conn": { "props": {} },
                "props": props,
            }))?;
        if let Some(hook) = retry_decision {
            spec = spec.with_retry_decision(hook);
        }
        self.http.execute_empty(spec).await
    }

    pub async fn patch_eos_internal_presence(
        &self,
        account_id: &AccountId,
        private_connection_id: &str,
        party: Option<&EOSInternalPartyPresence>,
        retry_decision: Option<RetryDecisionHook>,
    ) -> Result<(), RustfnError> {
        let encoded_conn: String =
            form_urlencoded::byte_serialize(private_connection_id.as_bytes()).collect();
        let url = format!(
            "{}/{}/presence/{}",
            self.extra.eos_presence_internal.trim_end_matches('/'),
            account_id.as_str(),
            encoded_conn
        );
        let mut payload = json!({
            "status": "online",
            "activity": {},
            "conn": { "props": {} },
        });
        if let Some(party_info) = party {
            payload["party"] = serde_json::to_value(party_info)?;
        }

        let mut spec = HttpRequestSpec::patch(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_account_scope(account_id.clone())
            .with_route_key("PATCH epic/presence/internal/v1/_/{id}/presence/{conn}")
            .with_json(&payload)?;
        if let Some(hook) = retry_decision {
            spec = spec.with_retry_decision(hook);
        }
        self.http.execute_empty(spec).await
    }

    pub async fn get_eos_party_user_state(
        &self,
        account_id: &AccountId,
    ) -> Result<EOSPartyUserState, RustfnError> {
        let url = format!(
            "{}/users/{}",
            self.endpoints.eos_party_internal.trim_end_matches('/'),
            account_id.as_str()
        );
        self.http
            .execute_json(
                HttpRequestSpec::get(url)
                    .with_auth(AuthSessionSlot::FortniteEos)
                    .with_account_scope(account_id.clone())
                    .with_route_key("GET epic/party/internal/v2/users/{id}"),
            )
            .await
    }

    pub async fn get_eos_party(&self, eos_party_id: &PartyId) -> Result<EOSPartyData, RustfnError> {
        let url = format!(
            "{}/parties/{}",
            self.endpoints.eos_party_internal.trim_end_matches('/'),
            eos_party_id.as_str()
        );
        self.http
            .execute_json(
                HttpRequestSpec::get(url)
                    .with_auth(AuthSessionSlot::FortniteEos)
                    .with_route_key("GET epic/party/internal/v2/parties/{id}"),
            )
            .await
    }

    pub async fn create_eos_party(
        &self,
        private_connection_id: &str,
        deployment_id: &str,
        platform: Platform,
        config: &EOSPartyDataConfig,
    ) -> Result<EOSPartyData, RustfnError> {
        let url = format!(
            "{}/parties",
            self.endpoints.eos_party_internal.trim_end_matches('/')
        );
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_route_key("POST epic/party/internal/v2/parties")
            .with_json(&json!({
                "join_info": {
                    "connection": {
                        "meta": { "platform": platform.as_str(), "game": "fn" },
                        "deployment_id": deployment_id,
                        "id": private_connection_id,
                    },
                },
                "config": config,
            }))?;
        self.http.execute_json(spec).await
    }

    pub async fn join_eos_party(
        &self,
        eos_party_id: &PartyId,
        self_id: &AccountId,
        private_connection_id: &str,
        deployment_id: &str,
        platform: Platform,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/parties/{}/members/{}/join",
            self.endpoints.eos_party_internal.trim_end_matches('/'),
            eos_party_id.as_str(),
            self_id.as_str(),
        );
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_route_key("POST epic/party/internal/v2/parties/{id}/members/{id}/join")
            .with_json(&json!({
                "connection": {
                    "meta": { "platform": platform.as_str(), "game": "fn" },
                    "deployment_id": deployment_id,
                    "id": private_connection_id,
                },
            }))?;
        self.http.execute_empty(spec).await
    }

    pub async fn connect_eos_party(
        &self,
        deployment_id: &str,
        eos_party_id: &PartyId,
        self_id: &AccountId,
        public_connection_id: &str,
        retry_decision: Option<RetryDecisionHook>,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/v2/{}/parties/{}/members/{}/connect",
            self.endpoints.eos_party.trim_end_matches('/'),
            deployment_id,
            eos_party_id.as_str(),
            self_id.as_str(),
        );
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_route_key("POST epic/party/v2/{ns}/parties/{id}/members/{id}/connect")
            .with_json(&json!({
                "connection_id": public_connection_id,
                "yield_leadership": false,
            }))?;
        self.http
            .execute_empty(apply_retry(spec, retry_decision))
            .await
    }

    pub async fn keep_alive_eos_party(
        &self,
        eos_party_id: &PartyId,
        self_id: &AccountId,
        retry_decision: Option<RetryDecisionHook>,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/parties/{}/members/{}/keep-alive",
            self.endpoints.eos_party_internal.trim_end_matches('/'),
            eos_party_id.as_str(),
            self_id.as_str(),
        );
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_route_key("POST epic/party/internal/v2/parties/{id}/members/{id}/keep-alive")
            .with_json(&json!({}))?;
        self.http
            .execute_empty(apply_retry(spec, retry_decision))
            .await
    }

    pub async fn patch_eos_party(
        &self,
        eos_party_id: &PartyId,
        revision: u64,
        config: Option<&EOSPartyDataConfig>,
        meta: Option<&Value>,
        retry_decision: Option<RetryDecisionHook>,
    ) -> Result<EOSPartyData, RustfnError> {
        let url = format!(
            "{}/parties/{}",
            self.endpoints.eos_party_internal.trim_end_matches('/'),
            eos_party_id.as_str(),
        );
        let mut body = json!({ "revision": revision });
        if let Some(cfg) = config {
            body["config"] = serde_json::to_value(cfg)?;
        }
        if let Some(meta_val) = meta {
            body["meta"] = meta_val.clone();
        }
        let spec = HttpRequestSpec::patch(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_route_key("PATCH epic/party/internal/v2/parties/{id}")
            .with_json(&body)?;
        self.http
            .execute_json(apply_retry(spec, retry_decision))
            .await
    }

    pub async fn remove_eos_party_member(
        &self,
        eos_party_id: &PartyId,
        account_id: &AccountId,
        retry_decision: Option<RetryDecisionHook>,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/parties/{}/members/{}",
            self.endpoints.eos_party_internal.trim_end_matches('/'),
            eos_party_id.as_str(),
            account_id.as_str(),
        );
        let spec = HttpRequestSpec::delete(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_route_key("DELETE epic/party/internal/v2/parties/{id}/members/{id}");
        self.http
            .execute_empty(apply_retry(spec, retry_decision))
            .await
    }

    pub async fn promote_eos_party_member(
        &self,
        eos_party_id: &PartyId,
        account_id: &AccountId,
        retry_decision: Option<RetryDecisionHook>,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/parties/{}/members/{}/promote",
            self.endpoints.eos_party_internal.trim_end_matches('/'),
            eos_party_id.as_str(),
            account_id.as_str(),
        );
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_route_key("POST epic/party/internal/v2/parties/{id}/members/{id}/promote");
        self.http
            .execute_empty(apply_retry(spec, retry_decision))
            .await
    }

    pub async fn invite_eos_party(
        &self,
        friend_id: &AccountId,
        self_id: &AccountId,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/users/{}/invites/{}",
            self.endpoints.eos_party_internal.trim_end_matches('/'),
            friend_id.as_str(),
            self_id.as_str(),
        );
        let guid = Uuid::new_v4().simple().to_string().to_ascii_uppercase();
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_account_scope(friend_id.clone())
            .with_route_key("POST epic/party/internal/v2/users/{id}/invites/{id}")
            .with_query("auto", "false")
            .with_query("platform", "0")
            .with_json(&json!({
                "guid": guid,
                "SocialMenuContext": "Profile",
                "epv": "1",
            }))?;
        self.http.execute_empty(spec).await
    }

    pub async fn delete_eos_party_invite(
        &self,
        self_id: &AccountId,
        sender_id: &AccountId,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/users/{}/invites/{}",
            self.endpoints.eos_party_internal.trim_end_matches('/'),
            self_id.as_str(),
            sender_id.as_str(),
        );
        let spec = HttpRequestSpec::delete(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_account_scope(self_id.clone())
            .with_route_key("DELETE epic/party/internal/v2/users/{id}/invites/{id}");
        self.http.execute_empty(spec).await
    }

    pub async fn send_eos_join_request(
        &self,
        target_id: &AccountId,
        self_id: &AccountId,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/users/{}/joinRequests/{}",
            self.endpoints.eos_party_internal.trim_end_matches('/'),
            target_id.as_str(),
            self_id.as_str(),
        );
        let guid = Uuid::new_v4().simple().to_string().to_ascii_uppercase();
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_account_scope(target_id.clone())
            .with_route_key("POST epic/party/internal/v2/users/{id}/joinRequests/{id}")
            .with_query("auto", "false")
            .with_query("platform", "0")
            .with_json(&json!({
                "guid": guid,
                "SocialMenuContext": "Profile",
                "epv": "1",
            }))?;
        self.http.execute_empty(spec).await
    }

    pub async fn decline_eos_join_request(
        &self,
        self_id: &AccountId,
        requester_id: &AccountId,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/users/{}/joinRequests/{}",
            self.endpoints.eos_party_internal.trim_end_matches('/'),
            self_id.as_str(),
            requester_id.as_str(),
        );
        let spec = HttpRequestSpec::delete(url)
            .with_auth(AuthSessionSlot::FortniteEos)
            .with_account_scope(self_id.clone())
            .with_route_key("DELETE epic/party/internal/v2/users/{id}/joinRequests/{id}");
        self.http.execute_empty(spec).await
    }

    pub async fn join_fn_lobby(
        &self,
        eos_party_id: &PartyId,
        lobby_id: &LobbyId,
        self_id: &AccountId,
        display_name: &str,
        xmpp_jid: &str,
        platform: Platform,
        join_confirmation: bool,
        max_size: u32,
    ) -> Result<FortnitePartyData, RustfnError> {
        let url = format!(
            "{}/epic-parties/{}/lobbies/{}/members/{}/join",
            self.endpoints.br_party.trim_end_matches('/'),
            eos_party_id.as_str(),
            lobby_id.as_str(),
            self_id.as_str(),
        );
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_route_key("POST party/Fortnite/epic-parties/{id}/lobbies/{id}/members/{id}/join")
            .with_json(&json!({
                "config": {
                    "discoverability": PartyDiscoverability::All,
                    "join_confirmation": join_confirmation,
                    "joinability": PartyJoinability::Open,
                    "max_size": max_size,
                },
                "join_info": {
                    "connection": {
                        "id": xmpp_jid,
                        "meta": {
                            "urn:epic:conn:platform_s": platform.as_str(),
                        },
                    },
                    "meta": {
                        "CrossplayPreference_i": "1",
                        "SubGame_u": "1",
                        "urn:epic:member:dn_s": display_name,
                    },
                },
            }))?;
        self.http.execute_json(spec).await
    }

    pub async fn get_fn_party_user_state(
        &self,
        account_id: &AccountId,
    ) -> Result<FnPartyUserState, RustfnError> {
        let url = format!(
            "{}/user/{}",
            self.endpoints.br_party.trim_end_matches('/'),
            account_id.as_str(),
        );
        self.http
            .execute_json(
                HttpRequestSpec::get(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_account_scope(account_id.clone())
                    .with_route_key("GET party/Fortnite/user/{id}"),
            )
            .await
    }

    pub async fn get_fn_party(&self, party_id: &LobbyId) -> Result<FortnitePartyData, RustfnError> {
        let url = format!(
            "{}/parties/{}",
            self.endpoints.br_party.trim_end_matches('/'),
            party_id.as_str(),
        );
        self.http
            .execute_json(
                HttpRequestSpec::get(url)
                    .with_auth(AuthSessionSlot::Fortnite)
                    .with_route_key("GET party/Fortnite/parties/{id}"),
            )
            .await
    }

    pub async fn leave_fn_party_member(
        &self,
        party_id: &LobbyId,
        account_id: &AccountId,
        retry_decision: Option<RetryDecisionHook>,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/parties/{}/members/{}",
            self.endpoints.br_party.trim_end_matches('/'),
            party_id.as_str(),
            account_id.as_str(),
        );
        let spec = HttpRequestSpec::delete(url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_route_key("DELETE party/Fortnite/parties/{id}/members/{id}");
        self.http
            .execute_empty(apply_retry(spec, retry_decision))
            .await
    }

    pub async fn promote_fn_party_member(
        &self,
        party_id: &LobbyId,
        account_id: &AccountId,
        retry_decision: Option<RetryDecisionHook>,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/parties/{}/members/{}/promote",
            self.endpoints.br_party.trim_end_matches('/'),
            party_id.as_str(),
            account_id.as_str(),
        );
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_route_key("POST party/Fortnite/parties/{id}/members/{id}/promote")
            .with_json(&json!({}))?;
        self.http
            .execute_empty(apply_retry(spec, retry_decision))
            .await
    }

    pub async fn patch_fn_party_meta(
        &self,
        party_id: &LobbyId,
        config: &FortnitePartyConfig,
        patch: &MetaPatch,
        revision: u64,
        retry_decision: Option<RetryDecisionHook>,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/parties/{}",
            self.endpoints.br_party.trim_end_matches('/'),
            party_id.as_str(),
        );
        let body = json!({
            "config": {
                "join_confirmation": config.join_confirmation,
                "joinability": config.joinability,
                "max_size": config.max_size,
                "discoverability": config.discoverability,
            },
            "meta": WireMetaPatch {
                delete: &patch.deleted,
                update: &patch.updated,
            },
            "party_state_overridden": json!({}),
            "party_privacy_type": config.joinability,
            "party_type": config.r#type,
            "party_sub_type": config.sub_type,
            "max_number_of_members": config.max_size,
            "invite_ttl_seconds": config.invite_ttl,
            "revision": revision,
        });
        let spec = HttpRequestSpec::patch(url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_route_key("PATCH party/Fortnite/parties/{id}")
            .with_json(&body)?;
        self.http
            .execute_empty(apply_retry(spec, retry_decision))
            .await
    }

    pub async fn patch_fn_member_meta(
        &self,
        party_id: &LobbyId,
        member_id: &AccountId,
        patch: &MetaPatch,
        revision: u64,
        retry_decision: Option<RetryDecisionHook>,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/parties/{}/members/{}/meta",
            self.endpoints.br_party.trim_end_matches('/'),
            party_id.as_str(),
            member_id.as_str(),
        );
        let spec = HttpRequestSpec::patch(url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_route_key("PATCH party/Fortnite/parties/{id}/members/{id}/meta")
            .with_json(&json!({
                "delete": &patch.deleted,
                "revision": revision,
                "update": &patch.updated,
            }))?;
        self.http
            .execute_empty(apply_retry(spec, retry_decision))
            .await
    }

    pub async fn send_fn_party_invite(
        &self,
        party_id: &LobbyId,
        target_id: &AccountId,
        sender_display_name: &str,
        platform: Platform,
        party_build_id: &str,
        send_ping: bool,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/parties/{}/invites/{}",
            self.endpoints.br_party.trim_end_matches('/'),
            party_id.as_str(),
            target_id.as_str(),
        );
        let guid = Uuid::new_v4().simple().to_string().to_ascii_uppercase();
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_route_key("POST party/Fortnite/parties/{id}/invites/{id}")
            .with_query("sendPing", if send_ping { "true" } else { "false" })
            .with_json(&json!({
                "urn:epic:invite:platformdata_s": format!("guid={guid}"),
                "urn:epic:member:dn_s": sender_display_name,
                "urn:epic:conn:platform_s": platform.as_str(),
                "urn:epic:cfg:build-id_s": party_build_id,
            }))?;
        self.http.execute_empty(spec).await
    }

    pub async fn decline_fn_party_invite(
        &self,
        party_id: &LobbyId,
        self_id: &AccountId,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/parties/{}/invites/{}/decline",
            self.endpoints.br_party.trim_end_matches('/'),
            party_id.as_str(),
            self_id.as_str(),
        );
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_route_key("POST party/Fortnite/parties/{id}/invites/{id}/decline")
            .with_json(&json!({}))?;
        self.http.execute_empty(spec).await
    }

    pub async fn confirm_fn_party_member(
        &self,
        party_id: &LobbyId,
        member_id: &AccountId,
        retry_decision: Option<RetryDecisionHook>,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/parties/{}/members/{}/confirm",
            self.endpoints.br_party.trim_end_matches('/'),
            party_id.as_str(),
            member_id.as_str(),
        );
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_route_key("POST party/Fortnite/parties/{id}/members/{id}/confirm")
            .with_json(&json!({}))?;
        self.http
            .execute_empty(apply_retry(spec, retry_decision))
            .await
    }

    pub async fn reject_fn_party_member(
        &self,
        party_id: &LobbyId,
        member_id: &AccountId,
        retry_decision: Option<RetryDecisionHook>,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/parties/{}/members/{}/reject",
            self.endpoints.br_party.trim_end_matches('/'),
            party_id.as_str(),
            member_id.as_str(),
        );
        let spec = HttpRequestSpec::post(url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_route_key("POST party/Fortnite/parties/{id}/members/{id}/reject")
            .with_json(&json!({}))?;
        self.http
            .execute_empty(apply_retry(spec, retry_decision))
            .await
    }

    pub async fn delete_fn_party_ping(
        &self,
        self_id: &AccountId,
        sender_id: &AccountId,
    ) -> Result<(), RustfnError> {
        let url = format!(
            "{}/user/{}/pings/{}",
            self.endpoints.br_party.trim_end_matches('/'),
            self_id.as_str(),
            sender_id.as_str(),
        );
        let spec = HttpRequestSpec::delete(url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_account_scope(self_id.clone())
            .with_route_key("DELETE party/Fortnite/user/{id}/pings/{id}");
        self.http.execute_empty(spec).await
    }
}

fn apply_retry(mut spec: HttpRequestSpec, hook: Option<RetryDecisionHook>) -> HttpRequestSpec {
    if let Some(decision) = hook {
        spec = spec.with_retry_decision(decision);
    }
    spec
}

fn should_try_publickey_fallback(err: &RustfnError) -> bool {
    match err {
        RustfnError::Transport(_) => true,
        RustfnError::HttpStatus { status, .. } | RustfnError::EpicApi { status, .. } => {
            *status == 404 || *status >= 500
        }
        _ => false,
    }
}
