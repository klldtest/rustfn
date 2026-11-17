use rustfn_auth::{AuthConfig, DeviceAuthStore};
use rustfn_http::{HttpClientConfig, RetryConfig};
pub use rustfn_protocol::{
    CacheConfig, ExtraServiceEndpoints, FORTNITE_CATALOG_ITEM_ID, PartyConfig,
    SquadPositionPriority, extra_urls, parse_build_version_cl,
};
use rustfn_protocol::{
    AuthClient, DeploymentId, EOS_EAS_DEPLOYMENT_ID, Platform, PresenceOnlineType, Region,
    ServiceEndpoints,
};
use rustfn_stomp::STOMPConfig;
use rustfn_xmpp::XMPPConfig;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Top-level configuration for a single [`crate::client::Client`] session.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub platform: Platform,
    pub region: Region,
    pub language: String,
    pub default_status: String,
    pub default_online_type: PresenceOnlineType,
    pub eos_deployment_id: String,
    pub product_version: String,
    pub game_auth_client: AuthClient,
    pub fetch_friends: bool,
    pub create_party: bool,
    pub disable_party_service: bool,
    pub leave_party_on_shutdown: bool,
    pub save_party_member_meta: bool,
    pub connect_to_xmpp: bool,
    pub connect_to_stomp: bool,
    pub create_device_auth: bool,
    pub create_launcher_session: bool,
    pub check_eula: bool,
    pub kill_other_tokens: bool,
    pub party_build_id: Option<String>,
    pub user_agent: String,
    pub endpoints: ServiceEndpoints,
    pub extra_endpoints: ExtraServiceEndpoints,
    pub http: HttpClientConfig,
    pub retry: RetryConfig,
    pub cache: CacheConfig,
    pub party: PartyConfig,
    pub xmpp: XMPPConfig,
    pub stomp: STOMPConfig,
    pub auth_refresh_lead_time: Duration,
    pub publickey_fallback_enabled: bool,
    pub device_auth_path: Option<PathBuf>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        let http = HttpClientConfig::default();
        let retry = http.retry.clone();
        Self {
            platform: Platform::And,
            region: Region::Eu,
            language: "en".to_owned(),
            default_status: "In Lobby".to_owned(),
            default_online_type: PresenceOnlineType::Online,
            eos_deployment_id: EOS_EAS_DEPLOYMENT_ID.to_owned(),
            product_version: "++Fortnite+Release-31.00-CL-37219843".to_owned(),
            game_auth_client: AuthClient::FortniteAndroidGameClient,
            fetch_friends: true,
            create_party: true,
            disable_party_service: false,
            leave_party_on_shutdown: true,
            save_party_member_meta: true,
            connect_to_xmpp: true,
            connect_to_stomp: true,
            create_device_auth: false,
            create_launcher_session: false,
            check_eula: true,
            kill_other_tokens: true,
            party_build_id: None,
            user_agent: http.user_agent.clone(),
            endpoints: ServiceEndpoints::default(),
            extra_endpoints: ExtraServiceEndpoints::default(),
            http,
            retry,
            cache: CacheConfig::default(),
            party: PartyConfig::default(),
            xmpp: XMPPConfig::default(),
            stomp: STOMPConfig::default(),
            auth_refresh_lead_time: Duration::from_secs(300),
            publickey_fallback_enabled: true,
            device_auth_path: None,
        }
    }
}

impl ClientConfig {
    /// Builds the effective [`HttpClientConfig`] incorporating top-level language, user-agent, and retry overrides.
    pub fn effective_http_config(&self) -> HttpClientConfig {
        let default_http = HttpClientConfig::default();
        let default_retry = RetryConfig::default();
        let mut cfg = self.http.clone();

        if self.http.user_agent == default_http.user_agent
            && self.user_agent != default_http.user_agent
        {
            cfg.user_agent = self.user_agent.clone();
        }
        if self.http.accept_language == default_http.accept_language && self.language != "en" {
            cfg.accept_language = self.language.clone();
        }
        if retry_eq(&self.http.retry, &default_retry) && !retry_eq(&self.retry, &default_retry) {
            cfg.retry = self.retry.clone();
        }
        cfg
    }

    /// Builds the effective [`XMPPConfig`] synchronized with configured endpoints, platform, and metadata TTL.
    pub fn effective_xmpp_config(&self) -> XMPPConfig {
        let def = XMPPConfig::default();
        let mut cfg = self.xmpp.clone();
        if cfg.wss_url == def.wss_url {
            cfg.wss_url = self.endpoints.xmpp_wss.to_string();
        }
        if cfg.domain == def.domain {
            cfg.domain = self.endpoints.epic_prod_env.to_string();
        }
        cfg.platform = self.platform;
        if cfg.metadata_ttl == def.metadata_ttl {
            cfg.metadata_ttl = self.party.member_metadata_ttl;
        }
        cfg
    }

    /// Builds the effective [`STOMPConfig`] synchronized with configured endpoints and deployment ID.
    pub fn effective_stomp_config(&self) -> STOMPConfig {
        let def = STOMPConfig::default();
        let mut cfg = self.stomp.clone();
        if cfg.wss_url == def.wss_url {
            cfg.wss_url = self.endpoints.eos_stomp_wss.to_string();
        }
        if cfg.deployment_id.as_str() == EOS_EAS_DEPLOYMENT_ID {
            cfg.deployment_id = DeploymentId::from_raw(self.eos_deployment_id.clone());
        }
        cfg
    }

    /// Builds [`AuthConfig`] derived from this client configuration and optional persistent device store.
    pub fn effective_auth_config(&self, device_store: Option<Arc<dyn DeviceAuthStore>>) -> AuthConfig {
        AuthConfig {
            auth_client: self.game_auth_client,
            eos_deployment_id: DeploymentId::from_raw(self.eos_deployment_id.clone()),
            endpoints: self.endpoints.clone(),
            create_launcher_session: self.create_launcher_session,
            create_eos_session: self.connect_to_stomp || !self.disable_party_service,
            create_device_auth: self.create_device_auth,
            kill_other_tokens: self.kill_other_tokens,
            check_eula: self.check_eula,
            refresh_lead_time: self.auth_refresh_lead_time,
            device_store,
        }
    }
}

fn retry_eq(a: &RetryConfig, b: &RetryConfig) -> bool {
    a.max_retry_attempts == b.max_retry_attempts
        && a.rest_retry_limit == b.rest_retry_limit
        && a.max_wait_time == b.max_wait_time
        && a.max_retry_after == b.max_retry_after
        && a.backoff_start == b.backoff_start
        && a.backoff_factor.to_bits() == b.backoff_factor.to_bits()
        && a.backoff_cap == b.backoff_cap
        && a.retry_after_padding == b.retry_after_padding
        && a.handle_rate_limits == b.handle_rate_limits
        && a.handle_capacity_throttling == b.handle_capacity_throttling
        && a.other_requests_wait == b.other_requests_wait
}
