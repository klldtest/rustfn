pub mod auth;
pub mod session;
pub mod fortnite;

pub use auth::{
    AuthConfig, AuthEvent, Auth, AuthMethod, AuthProgressHooks, AuthTokenCoordinator,
    DeviceCodeAuth, FortniteSessionInfo, TokenRefreshLoop,
};
pub use session::{
    AuthSessionStore, AuthSessionSummary, DeviceAuthCredentials, DeviceAuthStore,
    DeviceCodeInitResponse, EpicOAuthTokenResponse, ExchangeCodeResponse,
    JsonFileDeviceAuthStore, StoredAuthSession,
};
pub use fortnite::{
    EOSOAuthTokenResponse, EOSTokenInfoResponse, EulaCheckOutcome, authenticate_device_code,
    authenticate_device_code_with_switch, authenticate_eos_exchange, ensure_eula_accepted,
    extract_authorization_code, grant_eos_external_auth, open_url_in_browser,
    refresh_eos_token, revoke_eos_token, verify_epic_token, verify_eos_token,
};
