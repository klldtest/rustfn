use crate::auth::session::{
    AuthSessionStore, AuthSessionSummary, DeviceAuthCredentials, DeviceAuthStore,
    DeviceCodeInitResponse, EpicOAuthTokenResponse, StoredAuthSession,
};
use crate::auth::fortnite::{
    EulaCheckOutcome, authenticate_device_code_with_switch, authenticate_eos_exchange,
    create_exchange_code, delete_device_auth, ensure_eula_accepted, eos_response_to_stored,
    extract_authorization_code, generate_device_auth, grant_authorization_code,
    grant_client_credentials, grant_device_auth, grant_exchange_code, grant_refresh_token,
    kill_epic_token, kill_other_epic_sessions, open_url_in_browser, refresh_eos_token,
    revoke_eos_token,
};
use chrono::{DateTime, Utc};
use crate::http::{AuthSessionSlot, BoxFuture, HttpClient, HttpClientConfig, TokenProvider};
use crate::protocol::{
    AccountId, AuthClient, DeploymentId, EOS_EAS_DEPLOYMENT_ID, ProductUserId, RustfnError,
    ServiceEndpoints,
};
use secrecy::{ExposeSecret, SecretString};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{Notify, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Device-code OAuth2 authentication configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceCodeAuth {
    pub open_link_in_browser: bool,
    pub switch_client: AuthClient,
    pub ios_client: Option<AuthClient>,
}

impl Default for DeviceCodeAuth {
    fn default() -> Self {
        Self::new()
    }
}

impl DeviceCodeAuth {
    pub const fn new() -> Self {
        Self {
            open_link_in_browser: true,
            switch_client: AuthClient::FortniteNewSwitchGameClient,
            ios_client: None,
        }
    }

    pub const fn headless() -> Self {
        Self {
            open_link_in_browser: false,
            switch_client: AuthClient::FortniteNewSwitchGameClient,
            ios_client: None,
        }
    }

    pub const fn with_open_link_in_browser(mut self, open: bool) -> Self {
        self.open_link_in_browser = open;
        self
    }

    pub const fn with_switch_client(mut self, switch_client: AuthClient) -> Self {
        self.switch_client = switch_client;
        self
    }

    pub const fn with_ios_client(mut self, ios_client: AuthClient) -> Self {
        self.ios_client = Some(ios_client);
        self
    }
}

/// Early identity payload emitted as soon as the primary Fortnite OAuth token lands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FortniteSessionInfo {
    pub account_id: AccountId,
    pub display_name: Option<String>,
}

/// Optional oneshot notifications letting callers overlap downstream setup before EOS/CC grants finish.
#[derive(Default)]
pub struct AuthProgressHooks {
    pub on_fortnite: Option<tokio::sync::oneshot::Sender<FortniteSessionInfo>>,
    pub on_eos: Option<tokio::sync::oneshot::Sender<Option<ProductUserId>>>,
}

/// Authentication grant strategy chosen by the caller when logging in an Epic account.
#[derive(Clone)]
pub enum AuthMethod {
    DeviceAuth(DeviceAuthCredentials),
    AuthorizationCode(String),
    ExchangeCode(String),
    RefreshToken(SecretString),
    LauncherRefreshToken(SecretString),
    DeviceCode,
    DeviceCodeAuth(DeviceCodeAuth),
}

impl From<DeviceCodeAuth> for AuthMethod {
    fn from(value: DeviceCodeAuth) -> Self {
        Self::DeviceCodeAuth(value)
    }
}

impl AuthMethod {
    pub const fn device_code_config(&self) -> Option<DeviceCodeAuth> {
        match self {
            Self::DeviceCode => Some(DeviceCodeAuth::headless()),
            Self::DeviceCodeAuth(cfg) => Some(*cfg),
            _ => None,
        }
    }
}

impl std::fmt::Debug for AuthMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeviceAuth(creds) => f.debug_tuple("DeviceAuth").field(creds).finish(),
            Self::AuthorizationCode(_) => f.write_str("AuthorizationCode([REDACTED])"),
            Self::ExchangeCode(_) => f.write_str("ExchangeCode([REDACTED])"),
            Self::RefreshToken(_) => f.write_str("RefreshToken([REDACTED])"),
            Self::LauncherRefreshToken(_) => f.write_str("LauncherRefreshToken([REDACTED])"),
            Self::DeviceCode => f.write_str("DeviceCode"),
            Self::DeviceCodeAuth(cfg) => f.debug_tuple("DeviceCodeAuth").field(cfg).finish(),
        }
    }
}

/// Asynchronous event notifications published by [`Auth`].
#[derive(Debug, Clone)]
pub enum AuthEvent {
    DeviceAuthCreated(DeviceAuthCredentials),
    LauncherRefreshTokenCreated {
        account_id: Option<AccountId>,
        refresh_token: String,
    },
    SessionRefreshed {
        slot: AuthSessionSlot,
        summary: AuthSessionSummary,
    },
    RefreshFailed {
        slot: AuthSessionSlot,
        error: String,
    },
}

/// Configuration parameters controlling OAuth session bootstrap and proactive token renewal.
#[derive(Clone)]
pub struct AuthConfig {
    pub auth_client: AuthClient,
    pub eos_deployment_id: DeploymentId,
    pub endpoints: ServiceEndpoints,
    pub create_launcher_session: bool,
    pub create_eos_session: bool,
    pub create_device_auth: bool,
    pub kill_other_tokens: bool,
    pub check_eula: bool,
    pub refresh_lead_time: std::time::Duration,
    pub device_store: Option<Arc<dyn DeviceAuthStore>>,
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("auth_client", &self.auth_client)
            .field("eos_deployment_id", &self.eos_deployment_id)
            .field("create_launcher_session", &self.create_launcher_session)
            .field("create_eos_session", &self.create_eos_session)
            .field("create_device_auth", &self.create_device_auth)
            .field("kill_other_tokens", &self.kill_other_tokens)
            .field("check_eula", &self.check_eula)
            .field("refresh_lead_time", &self.refresh_lead_time)
            .field("has_device_store", &self.device_store.is_some())
            .finish()
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            auth_client: AuthClient::FortniteAndroidGameClient,
            eos_deployment_id: DeploymentId::from_raw(EOS_EAS_DEPLOYMENT_ID),
            endpoints: ServiceEndpoints::default(),
            create_launcher_session: false,
            create_eos_session: true,
            create_device_auth: false,
            kill_other_tokens: false,
            check_eula: true,
            refresh_lead_time: std::time::Duration::from_secs(15 * 60),
            device_store: None,
        }
    }
}

/// Acyclic coordinator implementing [`TokenProvider`] for `HttpClient` without retaining [`Auth`].
pub struct AuthTokenCoordinator {
    http_raw: HttpClient,
    endpoints: ServiceEndpoints,
    store: Arc<AuthSessionStore>,
    event_tx: broadcast::Sender<AuthEvent>,
    wake_scheduler: Arc<Notify>,
}

impl AuthTokenCoordinator {
    async fn refresh_slot_internal(
        &self,
        slot: AuthSessionSlot,
        expected_failed_token: Option<&str>,
    ) -> Result<StoredAuthSession, RustfnError> {
        let gate = self.store.refresh_gate(slot);
        let _guard = gate.lock().await;

        let current = self.store.get_session_unguarded(slot).await.ok_or_else(|| {
            RustfnError::Auth(format!("cannot refresh missing session slot {slot:?}"))
        })?;

        if let Some(failed) = expected_failed_token
            && current.access_token.expose_secret() != failed
        {
            return Ok(current);
        }

        let refreshed = match slot {
            AuthSessionSlot::Fortnite | AuthSessionSlot::Launcher => {
                let refresh_token = current.refresh_token.as_ref().ok_or_else(|| {
                    RustfnError::Auth(format!("slot {slot:?} has no refresh token"))
                })?;
                let resp = grant_refresh_token(
                    &self.http_raw,
                    &self.endpoints,
                    current.client,
                    refresh_token,
                )
                .await?;
                let updated = epic_response_to_stored(slot, current.client, resp);
                if slot == AuthSessionSlot::Launcher
                    && let Some(rt) = updated.refresh_token.as_ref()
                {
                    let _ = self.event_tx.send(AuthEvent::LauncherRefreshTokenCreated {
                        account_id: updated.account_id.clone(),
                        refresh_token: rt.expose_secret().to_owned(),
                    });
                }
                updated
            }
            AuthSessionSlot::FortniteClientCredentials => {
                let resp =
                    grant_client_credentials(&self.http_raw, &self.endpoints, current.client).await?;
                epic_response_to_stored(slot, current.client, resp)
            }
            AuthSessionSlot::FortniteEos => {
                let refresh_token = current.refresh_token.as_ref().ok_or_else(|| {
                    RustfnError::Auth("EOS session has no refresh token".to_owned())
                })?;
                let resp = refresh_eos_token(
                    &self.http_raw,
                    &self.endpoints,
                    current.client,
                    refresh_token,
                    &current.eos_base_payload,
                )
                .await?;
                eos_response_to_stored(current.client, resp, current.eos_base_payload)
            }
        };

        self.store.insert_session(refreshed.clone()).await;
        let _ = self.event_tx.send(AuthEvent::SessionRefreshed {
            slot,
            summary: refreshed.summary(),
        });
        self.wake_scheduler.notify_one();
        Ok(refreshed)
    }
}

impl TokenProvider for AuthTokenCoordinator {
    fn bearer_token(&self, slot: AuthSessionSlot) -> BoxFuture<'_, Result<SecretString, RustfnError>> {
        Box::pin(async move { self.store.bearer_secret(slot).await })
    }

    fn refresh_on_invalid_token<'a>(
        &'a self,
        slot: AuthSessionSlot,
        failed_token: &'a str,
    ) -> BoxFuture<'a, Result<SecretString, RustfnError>> {
        Box::pin(async move {
            match self.refresh_slot_internal(slot, Some(failed_token)).await {
                Ok(session) => Ok(session.access_token),
                Err(err) => {
                    let _ = self.event_tx.send(AuthEvent::RefreshFailed {
                        slot,
                        error: err.to_string(),
                    });
                    Err(err)
                }
            }
        })
    }
}

/// High-level OAuth2/EOS lifecycle manager for a single Epic Games account.
pub struct Auth {
    config: AuthConfig,
    http_raw: HttpClient,
    http_authed: HttpClient,
    coordinator: Arc<AuthTokenCoordinator>,
    store: Arc<AuthSessionStore>,
    event_tx: broadcast::Sender<AuthEvent>,
    wake_scheduler: Arc<Notify>,
}

impl Auth {
    pub fn new(config: AuthConfig, http_config: HttpClientConfig) -> Result<Arc<Self>, RustfnError> {
        let http_raw = HttpClient::new(http_config)?;
        Self::with_shared_http_pool(config, &http_raw)
    }

    /// Constructs an [`Auth`] that reuses sockets and global throttle state from `shared_http_pool`.
    pub fn with_shared_http_pool(
        config: AuthConfig,
        shared_http_pool: &HttpClient,
    ) -> Result<Arc<Self>, RustfnError> {
        let http_raw = shared_http_pool.share_pool_anonymous();
        let store = Arc::new(AuthSessionStore::new());
        let (event_tx, _) = broadcast::channel(64);
        let wake_scheduler = Arc::new(Notify::new());

        let coordinator = Arc::new(AuthTokenCoordinator {
            http_raw: http_raw.clone(),
            endpoints: config.endpoints.clone(),
            store: Arc::clone(&store),
            event_tx: event_tx.clone(),
            wake_scheduler: Arc::clone(&wake_scheduler),
        });

        let provider: Arc<dyn TokenProvider> = Arc::clone(&coordinator) as Arc<dyn TokenProvider>;
        let http_authed = shared_http_pool.share_pool(provider);

        Ok(Arc::new(Self {
            config,
            http_raw,
            http_authed,
            coordinator,
            store,
            event_tx,
            wake_scheduler,
        }))
    }

    pub fn token_provider(&self) -> Arc<dyn TokenProvider> {
        Arc::clone(&self.coordinator) as Arc<dyn TokenProvider>
    }

    pub fn http_client(&self) -> &HttpClient {
        &self.http_authed
    }

    pub fn store(&self) -> &Arc<AuthSessionStore> {
        &self.store
    }

    pub fn endpoints(&self) -> &ServiceEndpoints {
        &self.config.endpoints
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<AuthEvent> {
        self.event_tx.subscribe()
    }

    /// Attempts persisted [`DeviceAuthCredentials`] first if available on disk, falling back to `fallback`.
    pub async fn authenticate_with_store_or(
        &self,
        account_id: Option<&AccountId>,
        fallback: AuthMethod,
    ) -> Result<(), RustfnError> {
        if let (Some(store), Some(id)) = (self.config.device_store.as_ref(), account_id)
            && let Some(creds) = store.load(id).await?
        {
            return self.authenticate(AuthMethod::DeviceAuth(creds)).await;
        }
        self.authenticate(fallback).await
    }

    pub async fn authenticate(&self, method: AuthMethod) -> Result<(), RustfnError> {
        self.authenticate_with_device_prompt(method, CancellationToken::new(), |_| async {})
            .await
    }

    pub async fn authenticate_device_code(&self, auth: DeviceCodeAuth) -> Result<(), RustfnError> {
        self.authenticate(AuthMethod::DeviceCodeAuth(auth)).await
    }

    pub async fn authenticate_with_device_prompt<F, Fut>(
        &self,
        method: AuthMethod,
        cancel: CancellationToken,
        on_device_prompt: F,
    ) -> Result<(), RustfnError>
    where
        F: FnOnce(DeviceCodeInitResponse) -> Fut,
        Fut: Future<Output = ()>,
    {
        self.authenticate_with_hooks(method, cancel, AuthProgressHooks::default(), on_device_prompt)
            .await
    }

    pub async fn authenticate_with_hooks<F, Fut>(
        &self,
        method: AuthMethod,
        cancel: CancellationToken,
        mut hooks: AuthProgressHooks,
        on_device_prompt: F,
    ) -> Result<(), RustfnError>
    where
        F: FnOnce(DeviceCodeInitResponse) -> Fut,
        Fut: Future<Output = ()>,
    {
        info!(client = %self.config.auth_client, method = ?method, "Starting Epic OAuth authentication");

        let is_device_auth = matches!(method, AuthMethod::DeviceAuth(_));
        let is_launcher_rt = matches!(method, AuthMethod::LauncherRefreshToken(_));
        let device_code_cfg = method.device_code_config();
        let effective_client = device_code_cfg
            .and_then(|cfg| cfg.ios_client)
            .unwrap_or(self.config.auth_client);

        match method {
            AuthMethod::DeviceAuth(creds) => {
                let resp = grant_device_auth(
                    &self.http_raw,
                    &self.config.endpoints,
                    self.config.auth_client,
                    &creds,
                )
                .await?;
                self.store
                    .insert_session(epic_response_to_stored(
                        AuthSessionSlot::Fortnite,
                        self.config.auth_client,
                        resp,
                    ))
                    .await;
            }
            AuthMethod::AuthorizationCode(input) => {
                let code = extract_authorization_code(&input)?;
                let resp = grant_authorization_code(
                    &self.http_raw,
                    &self.config.endpoints,
                    self.config.auth_client,
                    &code,
                )
                .await?;
                self.store
                    .insert_session(epic_response_to_stored(
                        AuthSessionSlot::Fortnite,
                        self.config.auth_client,
                        resp,
                    ))
                    .await;
            }
            AuthMethod::ExchangeCode(raw_code) => {
                let code = SecretString::from(raw_code.trim());
                let resp = grant_exchange_code(
                    &self.http_raw,
                    &self.config.endpoints,
                    self.config.auth_client,
                    &code,
                )
                .await?;
                self.store
                    .insert_session(epic_response_to_stored(
                        AuthSessionSlot::Fortnite,
                        self.config.auth_client,
                        resp,
                    ))
                    .await;
            }
            AuthMethod::RefreshToken(rt) => {
                let resp = grant_refresh_token(
                    &self.http_raw,
                    &self.config.endpoints,
                    self.config.auth_client,
                    &rt,
                )
                .await?;
                self.store
                    .insert_session(epic_response_to_stored(
                        AuthSessionSlot::Fortnite,
                        self.config.auth_client,
                        resp,
                    ))
                    .await;
            }
            AuthMethod::LauncherRefreshToken(rt) => {
                let launcher_resp = grant_refresh_token(
                    &self.http_raw,
                    &self.config.endpoints,
                    AuthClient::LauncherAppClient2,
                    &rt,
                )
                .await?;
                let launcher_session = epic_response_to_stored(
                    AuthSessionSlot::Launcher,
                    AuthClient::LauncherAppClient2,
                    launcher_resp,
                );
                let exchange = create_exchange_code(
                    &self.http_raw,
                    &self.config.endpoints,
                    &launcher_session.access_token,
                )
                .await?;
                self.store.insert_session(launcher_session).await;

                let fn_resp = grant_exchange_code(
                    &self.http_raw,
                    &self.config.endpoints,
                    self.config.auth_client,
                    &exchange.code,
                )
                .await?;
                self.store
                    .insert_session(epic_response_to_stored(
                        AuthSessionSlot::Fortnite,
                        self.config.auth_client,
                        fn_resp,
                    ))
                    .await;
            }
            AuthMethod::DeviceCode | AuthMethod::DeviceCodeAuth(_) => {
                let cfg = device_code_cfg.unwrap_or_else(DeviceCodeAuth::headless);
                let resp = authenticate_device_code_with_switch(
                    &self.http_raw,
                    &self.config.endpoints,
                    cfg.switch_client,
                    effective_client,
                    &cancel,
                    move |init| async move {
                        if cfg.open_link_in_browser {
                            let url = init
                                .verification_uri_complete
                                .as_deref()
                                .unwrap_or(&init.verification_uri);
                            open_url_in_browser(url);
                        }
                        on_device_prompt(init).await;
                    },
                )
                .await?;
                self.store
                    .insert_session(epic_response_to_stored(
                        AuthSessionSlot::Fortnite,
                        effective_client,
                        resp,
                    ))
                    .await;
            }
        }

        let fortnite_session = self
            .store
            .get_session_unguarded(AuthSessionSlot::Fortnite)
            .await
            .ok_or_else(|| RustfnError::Auth("Fortnite session missing after login".to_owned()))?;

        if let Some(tx) = hooks.on_fortnite.take() {
            if let Some(account_id) = fortnite_session.account_id.clone() {
                let _ = tx.send(FortniteSessionInfo {
                    account_id,
                    display_name: fortnite_session.display_name.clone(),
                });
            }
        }

        if !is_launcher_rt && self.config.create_launcher_session {
            let exchange = create_exchange_code(
                &self.http_raw,
                &self.config.endpoints,
                &fortnite_session.access_token,
            )
            .await?;
            let launcher_resp = grant_exchange_code(
                &self.http_raw,
                &self.config.endpoints,
                AuthClient::LauncherAppClient2,
                &exchange.code,
            )
            .await?;
            let launcher_stored = epic_response_to_stored(
                AuthSessionSlot::Launcher,
                AuthClient::LauncherAppClient2,
                launcher_resp,
            );
            if let Some(rt) = launcher_stored.refresh_token.as_ref() {
                let _ = self.event_tx.send(AuthEvent::LauncherRefreshTokenCreated {
                    account_id: launcher_stored.account_id.clone(),
                    refresh_token: rt.expose_secret().to_owned(),
                });
            }
            self.store.insert_session(launcher_stored).await;
        }

        let kill_other = self.config.kill_other_tokens;
        let check_eula = self.config.check_eula;
        let should_create_device_auth = !is_device_auth
            && self.config.create_device_auth
            && effective_client.supports_device_auth();
        let session_account = fortnite_session.account_id.as_ref();

        let kill_fut = async {
            if kill_other {
                kill_other_epic_sessions(
                    &self.http_raw,
                    &self.config.endpoints,
                    &fortnite_session.access_token,
                )
                .await?;
            }
            Ok::<_, RustfnError>(())
        };

        let eula_fut = async {
            if check_eula && let Some(account_id) = session_account {
                match ensure_eula_accepted(&self.http_authed, &self.config.endpoints, account_id)
                    .await?
                {
                    EulaCheckOutcome::Accepted { version } => {
                        debug!(version, "Accepted Fortnite EULA");
                    }
                    EulaCheckOutcome::AlreadyAccepted => {}
                }
            }
            Ok::<_, RustfnError>(())
        };

        let device_auth_fut = async {
            if should_create_device_auth && let Some(account_id) = session_account {
                let creds = generate_device_auth(
                    &self.http_raw,
                    &self.config.endpoints,
                    account_id,
                    &fortnite_session.access_token,
                )
                .await?;
                if let Some(store) = self.config.device_store.as_ref() {
                    store.save(&creds).await?;
                }
                let _ = self.event_tx.send(AuthEvent::DeviceAuthCreated(creds));
            }
            Ok::<_, RustfnError>(())
        };

        let cc_fut = grant_client_credentials(
            &self.http_raw,
            &self.config.endpoints,
            effective_client,
        );

        let eos_ready_tx = hooks.on_eos.take();
        let eos_fut = async move {
            if self.config.create_eos_session {
                self.authenticate_eos(effective_client).await?;
                if let Some(tx) = eos_ready_tx {
                    let _ = tx.send(self.store.product_user_id().await);
                }
            } else if let Some(tx) = eos_ready_tx {
                let _ = tx.send(None);
            }
            Ok::<_, RustfnError>(())
        };

        let (_, _, _, cc_resp, _) = tokio::try_join!(
            Box::pin(kill_fut),
            Box::pin(eula_fut),
            Box::pin(device_auth_fut),
            Box::pin(cc_fut),
            Box::pin(eos_fut),
        )?;
        self.store
            .insert_session(epic_response_to_stored(
                AuthSessionSlot::FortniteClientCredentials,
                effective_client,
                cc_resp,
            ))
            .await;

        self.wake_scheduler.notify_one();
        Ok(())
    }

    async fn authenticate_eos(&self, auth_client: AuthClient) -> Result<(), RustfnError> {
        let fortnite_token = self.store.bearer_secret(AuthSessionSlot::Fortnite).await?;
        let exchange =
            create_exchange_code(&self.http_raw, &self.config.endpoints, &fortnite_token).await?;
        let deployment_id = self.config.eos_deployment_id.as_str().to_owned();

        let eos_resp = authenticate_eos_exchange(
            &self.http_raw,
            &self.config.endpoints,
            auth_client,
            &exchange.code,
            &deployment_id,
        )
        .await?;

        let mut base_payload = BTreeMap::new();
        base_payload.insert("token_type".to_owned(), "epic_id".to_owned());
        base_payload.insert("deployment_id".to_owned(), deployment_id);

        self.store
            .insert_session(eos_response_to_stored(
                auth_client,
                eos_resp,
                base_payload,
            ))
            .await;
        Ok(())
    }

    pub async fn create_exchange_code(&self) -> Result<SecretString, RustfnError> {
        let access = self.store.bearer_secret(AuthSessionSlot::Fortnite).await?;
        let res = create_exchange_code(&self.http_raw, &self.config.endpoints, &access).await?;
        Ok(res.code)
    }

    pub async fn create_device_auth(&self) -> Result<DeviceAuthCredentials, RustfnError> {
        let account_id = self
            .store
            .account_id()
            .await
            .ok_or_else(|| RustfnError::Auth("missing authenticated account_id".to_owned()))?;
        let access = self.store.bearer_secret(AuthSessionSlot::Fortnite).await?;
        generate_device_auth(&self.http_raw, &self.config.endpoints, &account_id, &access).await
    }

    pub async fn remove_device_auth(&self, device_id: &str) -> Result<(), RustfnError> {
        let account_id = self
            .store
            .account_id()
            .await
            .ok_or_else(|| RustfnError::Auth("missing authenticated account_id".to_owned()))?;
        let access = self.store.bearer_secret(AuthSessionSlot::Fortnite).await?;
        delete_device_auth(
            &self.http_raw,
            &self.config.endpoints,
            &account_id,
            device_id,
            &access,
        )
        .await
    }

    pub async fn refresh_slot(
        &self,
        slot: AuthSessionSlot,
    ) -> Result<AuthSessionSummary, RustfnError> {
        self.coordinator
            .refresh_slot_internal(slot, None)
            .await
            .map(|s| s.summary())
    }

    pub async fn refresh_all(&self) -> Result<(), RustfnError> {
        let snapshots = self.store.all_snapshots().await;
        let mut first_err: Option<RustfnError> = None;

        for session in snapshots {
            let slot = session.slot;
            if let Err(err) = self.coordinator.refresh_slot_internal(slot, None).await {
                warn!(?slot, error = %err, "Failed to refresh auth slot");
                let _ = self.event_tx.send(AuthEvent::RefreshFailed {
                    slot,
                    error: err.to_string(),
                });
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }

        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub fn spawn_refresh_loop(self: &Arc<Self>, cancel: CancellationToken) -> TokenRefreshLoop {
        TokenRefreshLoop::spawn(
            Arc::downgrade(self),
            Arc::clone(&self.wake_scheduler),
            cancel,
            self.config.refresh_lead_time,
        )
    }

    pub async fn revoke_all(&self) -> Result<(), RustfnError> {
        let snapshots = self.store.all_snapshots().await;
        let mut first_err = None;
        for session in snapshots {
            let result = match session.slot {
                AuthSessionSlot::Launcher => Ok(()),
                AuthSessionSlot::Fortnite | AuthSessionSlot::FortniteClientCredentials => {
                    kill_epic_token(
                        &self.http_raw,
                        &self.config.endpoints,
                        &session.access_token,
                    )
                    .await
                }
                AuthSessionSlot::FortniteEos => {
                    revoke_eos_token(
                        &self.http_raw,
                        &self.config.endpoints,
                        &session.access_token,
                    )
                    .await
                }
            };
            match result {
                Ok(()) => {
                    self.store.remove_session(session.slot).await;
                }
                Err(err) if err.is_not_found() || err.is_invalid_token() => {
                    self.store.remove_session(session.slot).await;
                }
                Err(err) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    pub async fn account_id(&self) -> Option<AccountId> {
        self.store.account_id().await
    }

    pub async fn display_name(&self) -> Option<String> {
        self.store.display_name().await
    }

    pub async fn product_user_id(&self) -> Option<ProductUserId> {
        self.store.product_user_id().await
    }
}

fn epic_response_to_stored(
    slot: AuthSessionSlot,
    client: AuthClient,
    resp: EpicOAuthTokenResponse,
) -> StoredAuthSession {
    StoredAuthSession {
        slot,
        client,
        access_token: resp.access_token,
        expires_at: resp.expires_at,
        refresh_token: resp.refresh_token,
        refresh_expires_at: resp.refresh_expires_at,
        account_id: resp.account_id,
        display_name: resp.display_name,
        device_id: resp.device_id,
        product_user_id: None,
        deployment_id: None,
        scope: resp.scope.into_vec(),
        eos_base_payload: BTreeMap::new(),
    }
}

/// Schedules proactive token renewals ahead of the earliest expiring active session.
pub struct TokenRefreshLoop {
    cancel: CancellationToken,
    wake: Arc<Notify>,
    handle: Option<JoinHandle<()>>,
}

impl TokenRefreshLoop {
    pub fn spawn(
        auth: Weak<Auth>,
        wake: Arc<Notify>,
        cancel: CancellationToken,
        lead_time: Duration,
    ) -> Self {
        let loop_cancel = cancel.clone();
        let loop_wake = Arc::clone(&wake);
        let handle = tokio::spawn(async move {
            run_loop(auth, loop_wake, loop_cancel, lead_time).await;
        });

        Self {
            cancel,
            wake,
            handle: Some(handle),
        }
    }

    pub fn trigger_now(&self) {
        self.wake.notify_one();
    }

    pub async fn stop(mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for TokenRefreshLoop {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

async fn run_loop(
    weak_auth: Weak<Auth>,
    wake: Arc<Notify>,
    cancel: CancellationToken,
    lead_time: Duration,
) {
    // Discard any wakeup permit stored before the loop started (for example right at initial login).
    tokio::select! {
        biased;
        _ = wake.notified() => {}
        _ = std::future::ready(()) => {}
    }

    loop {
        let Some(auth) = weak_auth.upgrade() else {
            return;
        };

        let sleep_dur = compute_next_sleep(&auth, lead_time).await;
        drop(auth);

        debug!(sleep_secs = sleep_dur.as_secs_f64(), "Auth refresh timer armed");

        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = wake.notified() => {
                debug!("Auth refresh woken explicitly");
            }
            _ = tokio::time::sleep(sleep_dur) => {}
        }

        let Some(auth) = weak_auth.upgrade() else {
            return;
        };
        if let Err(err) = auth.refresh_all().await {
            warn!(error = %err, "Scheduled token refresh encountered an error");
        }
    }
}

async fn compute_next_sleep(auth: &Auth, lead_time: Duration) -> Duration {
    let snapshots = auth.store().all_snapshots().await;
    let earliest: Option<DateTime<Utc>> = snapshots.into_iter().map(|s| s.expires_at).min();

    let Some(earliest_expiry) = earliest else {
        return Duration::from_secs(300);
    };

    let remaining = (earliest_expiry - Utc::now())
        .to_std()
        .unwrap_or(Duration::ZERO);

    if remaining > lead_time {
        remaining - lead_time
    } else if remaining > Duration::from_secs(10) {
        remaining / 2
    } else {
        Duration::from_secs(1)
    }
}
