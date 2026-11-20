use crate::client::config::{ClientConfig, ExtraServiceEndpoints, parse_build_version_cl};
use crate::auth::{
    Auth, AuthMethod, AuthProgressHooks, DeviceAuthStore, DeviceCodeAuth,
    JsonFileDeviceAuthStore, TokenRefreshLoop,
};
use crate::chat::{ChatManager, SentFriendMessage};
use crate::events::{
    ClientEvent, ClientEventBus, RealtimeEventContext, spawn_stomp_event_handler,
    spawn_xmpp_event_handler,
};
use crate::friends::{
    FriendManager, OutboundPresenceSpec, PartyJoinInfoData, build_eos_presence_props,
    build_xmpp_status_payload,
};
use crate::http::{FortnitePartyData, HTTP, HttpClient, TokenProvider};
use crate::party::{
    ClientParty, ClientPartyInit, PartyEventSink, PartySnapshot, PartyState,
    ReceivedPartyInvitation, SentPartyInvitation,
};
use crate::protocol::{
    AccountId, FortnitePartyPrivacy, LobbyId, MetaMap, MetaPatch, PartyDiscoverability, PartyId,
    PartyJoinability, PartyType, Platform, PresenceOnlineType, PresencePermission, ProductUserId,
    Region, RustfnError, ServiceEndpoints, to_eos_party_id, to_fortnite_lobby_id,
};
use crate::stomp::{EOSPartyDataConfig, STOMP, STOMPConnectionIds, STOMPEvent};
use crate::users::{UserManager, UserProfile};
use crate::xmpp::{XMPP, XMPPEvent, XMPPMetadataStore};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, broadcast, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Fluent builder constructing and optionally logging in a single [`Client`].
pub struct ClientBuilder {
    config: ClientConfig,
    auth_method: Option<AuthMethod>,
    account_hint: Option<AccountId>,
    device_store: Option<Arc<dyn DeviceAuthStore>>,
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("platform", &self.config.platform)
            .field("region", &self.config.region)
            .field("has_auth", &self.auth_method.is_some())
            .field("account_hint", &self.account_hint)
            .finish()
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self {
            config: ClientConfig::default(),
            auth_method: None,
            account_hint: None,
            device_store: None,
        }
    }

    pub fn config(mut self, config: ClientConfig) -> Self {
        self.config = config;
        self
    }

    pub fn platform(mut self, platform: Platform) -> Self {
        self.config.platform = platform;
        self
    }

    pub fn region(mut self, region: Region) -> Self {
        self.config.region = region;
        self
    }

    pub fn endpoints(mut self, endpoints: ServiceEndpoints) -> Self {
        self.config.endpoints = endpoints;
        self
    }

    pub fn extra_endpoints(mut self, extra: ExtraServiceEndpoints) -> Self {
        self.config.extra_endpoints = extra;
        self
    }

    pub fn auth(mut self, method: impl Into<AuthMethod>) -> Self {
        self.auth_method = Some(method.into());
        self
    }

    pub fn account_hint(mut self, account_id: AccountId) -> Self {
        self.account_hint = Some(account_id);
        self
    }

    pub fn device_store(mut self, store: Arc<dyn DeviceAuthStore>) -> Self {
        self.device_store = Some(store);
        self
    }

    pub fn party_build_id(mut self, build_id: impl Into<String>) -> Self {
        self.config.party_build_id = Some(build_id.into());
        self
    }

    pub fn create_party(mut self, enabled: bool) -> Self {
        self.config.create_party = enabled;
        self
    }

    pub fn connect_to_xmpp(mut self, enabled: bool) -> Self {
        self.config.connect_to_xmpp = enabled;
        self
    }

    pub fn connect_to_stomp(mut self, enabled: bool) -> Self {
        self.config.connect_to_stomp = enabled;
        self
    }

    /// Instantiates an idle [`Client`] without logging in yet.
    pub fn build(self) -> Result<Client, RustfnError> {
        Client::from_parts(self.config, self.account_hint, self.device_store)
    }

    /// Instantiates [`Client`] and completes the configured [`AuthMethod`] login sequence.
    pub async fn login(mut self) -> Result<Client, RustfnError> {
        let auth = self.auth_method.take().ok_or_else(|| {
            RustfnError::Auth("ClientBuilder::login called without .auth(...)".to_owned())
        })?;
        let client = self.build()?;
        client.login(auth).await?;
        Ok(client)
    }
}

struct ClientRuntimeState {
    account_id: Option<AccountId>,
    display_name: Option<String>,
    self_profile: Option<UserProfile>,
    friends: Option<Arc<FriendManager>>,
    chat: Option<Arc<ChatManager>>,
    xmpp: Option<Arc<XMPP>>,
    stomp: Option<Arc<STOMP>>,
    party_build_id: Option<String>,
    leaving_eos_party_id: Option<PartyId>,
    default_status: String,
    default_online_type: PresenceOnlineType,
}

struct ClientInner {
    config: ClientConfig,
    events: ClientEventBus,
    session_auth: SessionAuth,
    http: HTTP,
    users: Arc<UserManager>,
    account_hint: Option<AccountId>,
    runtime: RwLock<ClientRuntimeState>,
    party_slot: Arc<RwLock<Option<ClientParty>>>,
    saved_member_meta: Arc<RwLock<Option<MetaMap>>>,
    fallback_metadata_store: Arc<XMPPMetadataStore>,
    session_cancel: Mutex<Option<CancellationToken>>,
    driver_tasks: Mutex<Vec<JoinHandle<()>>>,
    session_lock: Mutex<()>,
    party_lock: Arc<Mutex<()>>,
    last_auth: RwLock<Option<AuthMethod>>,
    ready_tx: watch::Sender<bool>,
    ready_rx: watch::Receiver<bool>,
}

/// High-level asynchronous single-account Fortnite & EOS social client.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("platform", &self.inner.config.platform)
            .field("ready", &self.is_ready())
            .finish()
    }
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub fn new(config: ClientConfig) -> Result<Self, RustfnError> {
        Self::from_parts(config, None, None)
    }

    pub(crate) fn from_parts(
        config: ClientConfig,
        account_hint: Option<AccountId>,
        device_store: Option<Arc<dyn DeviceAuthStore>>,
    ) -> Result<Self, RustfnError> {
        let session_auth = SessionAuth::new(&config, device_store)?;

        let http = HTTP::new(
            session_auth.http_client().clone(),
            config.endpoints.clone(),
            config.extra_endpoints.clone(),
        );

        let users = UserManager::new(http.clone(), config.cache.clone());

        let (ready_tx, ready_rx) = watch::channel(false);
        let party_build_id = config.party_build_id.clone();
        let default_status = config.default_status.clone();
        let default_online_type = config.default_online_type;

        Ok(Self {
            inner: Arc::new(ClientInner {
                config,
                events: ClientEventBus::default(),
                session_auth,
                http,
                users,
                account_hint,
                runtime: RwLock::new(ClientRuntimeState {
                    account_id: None,
                    display_name: None,
                    self_profile: None,
                    friends: None,
                    chat: None,
                    xmpp: None,
                    stomp: None,
                    party_build_id,
                    leaving_eos_party_id: None,
                    default_status,
                    default_online_type,
                }),
                party_slot: Arc::new(RwLock::new(None)),
                saved_member_meta: Arc::new(RwLock::new(None)),
                fallback_metadata_store: Arc::new(XMPPMetadataStore::new(
                    Duration::from_secs(10),
                )),
                session_cancel: Mutex::new(None),
                driver_tasks: Mutex::new(Vec::new()),
                session_lock: Mutex::new(()),
                party_lock: Arc::new(Mutex::new(())),
                last_auth: RwLock::new(None),
                ready_tx,
                ready_rx,
            }),
        })
    }

    pub fn config(&self) -> &ClientConfig {
        &self.inner.config
    }

    pub fn events(&self) -> &ClientEventBus {
        &self.inner.events
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<Arc<ClientEvent>> {
        self.inner.events.subscribe()
    }

    pub fn is_ready(&self) -> bool {
        *self.inner.ready_rx.borrow()
    }

    pub async fn wait_until_ready(&self, wait_timeout: Duration) -> Result<(), RustfnError> {
        let mut rx = self.inner.ready_rx.clone();
        if *rx.borrow_and_update() {
            return Ok(());
        }
        tokio::time::timeout(wait_timeout, async move {
            loop {
                if rx.changed().await.is_err() {
                    return Err(RustfnError::Cancelled);
                }
                if *rx.borrow() {
                    return Ok(());
                }
            }
        })
        .await
        .map_err(|_| RustfnError::Timeout(format!("timed out waiting {wait_timeout:?} for ready")))?
    }

    pub fn auth(&self) -> &SessionAuth {
        &self.inner.session_auth
    }

    pub fn http(&self) -> &HTTP {
        &self.inner.http
    }

    pub fn users(&self) -> &Arc<UserManager> {
        &self.inner.users
    }

    pub async fn account_id(&self) -> Option<AccountId> {
        self.inner.runtime.read().await.account_id.clone()
    }

    pub async fn display_name(&self) -> Option<String> {
        self.inner.runtime.read().await.display_name.clone()
    }

    pub async fn self_user(&self) -> Option<UserProfile> {
        self.inner.runtime.read().await.self_profile.clone()
    }

    pub async fn friends(&self) -> Option<Arc<FriendManager>> {
        self.inner.runtime.read().await.friends.clone()
    }

    pub async fn chat(&self) -> Option<Arc<ChatManager>> {
        self.inner.runtime.read().await.chat.clone()
    }

    pub async fn party(&self) -> Option<ClientParty> {
        self.inner.party_slot.read().await.clone()
    }

    pub async fn xmpp(&self) -> Option<Arc<XMPP>> {
        self.inner.runtime.read().await.xmpp.clone()
    }

    pub async fn stomp(&self) -> Option<Arc<STOMP>> {
        self.inner.runtime.read().await.stomp.clone()
    }

    /// Authenticates this client and brings up friends, chat, XMPP/STOMP transports, and party state.
    pub async fn login(&self, auth: impl Into<AuthMethod>) -> Result<(), RustfnError> {
        self.login_with_cancel(auth, CancellationToken::new()).await
    }

    /// Authenticates using OAuth device-code authorization (`DeviceCodeAuth`).
    pub async fn authenticate_device_code(&self, auth: DeviceCodeAuth) -> Result<(), RustfnError> {
        self.login(AuthMethod::DeviceCodeAuth(auth)).await
    }

    /// Variant of [`Client::login`] linked under a caller-owned `parent_cancel` token.
    pub async fn login_with_cancel(
        &self,
        auth: impl Into<AuthMethod>,
        parent_cancel: CancellationToken,
    ) -> Result<(), RustfnError> {
        // Offload login's nested async state machines off small OS main-thread stacks onto Tokio's heap/worker pool.
        let client = self.clone();
        let auth = auth.into();
        tokio::spawn(async move { client.login_with_cancel_inner(auth, parent_cancel).await })
            .await
            .map_err(|err| RustfnError::Transport(format!("login task failed: {err}")))?
    }

    async fn login_with_cancel_inner(
        &self,
        auth: AuthMethod,
        parent_cancel: CancellationToken,
    ) -> Result<(), RustfnError> {
        let _session_guard = self.inner.session_lock.lock().await;
        self.destroy(false).await?;

        *self.inner.last_auth.write().await = Some(auth.clone());

        let cancel = parent_cancel.child_token();
        *self.inner.session_cancel.lock().await = Some(cancel.clone());

        let (fn_tx, fn_rx) = tokio::sync::oneshot::channel();
        let (eos_tx, eos_rx) = tokio::sync::oneshot::channel();
        let hooks = AuthProgressHooks {
            on_fortnite: Some(fn_tx),
            on_eos: Some(eos_tx),
        };

        let auth_client = self.clone();
        let auth_cancel = cancel.clone();
        let mut auth_handle = tokio::spawn(async move {
            auth_client
                .inner
                .session_auth
                .authenticate_with_hooks(
                    auth,
                    auth_client.inner.account_hint.as_ref(),
                    &auth_client.inner.events,
                    &auth_cancel,
                    hooks,
                )
                .await
        });
        let auth_fut = async {
            match (&mut auth_handle).await {
                Ok(res) => res,
                Err(err) => Err(RustfnError::Transport(format!("auth task failed: {err}"))),
            }
        };

        let connect_fut = async {
            let Ok(fn_info) = fn_rx.await else {
                // Auth failed before issuing a Fortnite token; let `auth_fut` surface the error.
                return Ok::<_, RustfnError>(None);
            };

            let account_id = fn_info.account_id;
            let token_dn = fn_info.display_name;

            self.inner.session_auth.start_refresh_loop(&cancel).await;

            let cfg = &self.inner.config;
            let friends = FriendManager::new(
                account_id.clone(),
                self.inner.http.clone(),
                Arc::clone(&self.inner.users),
                cfg.cache.clone(),
            );
            let xmpp_cfg = cfg.effective_xmpp_config();
            let chat = ChatManager::new(
                account_id.clone(),
                cfg.platform,
                cfg.eos_deployment_id.clone(),
                xmpp_cfg.domain.clone(),
                cfg.publickey_fallback_enabled,
                self.inner.http.clone(),
                Arc::clone(&friends),
            );

            let xmpp = cfg.connect_to_xmpp.then(|| {
                Arc::new(XMPP::new(
                    xmpp_cfg,
                    account_id.clone(),
                    self.inner.session_auth.token_provider(),
                ))
            });

            let stomp = cfg.connect_to_stomp.then(|| {
                Arc::new(STOMP::new(
                    cfg.effective_stomp_config(),
                    account_id.clone(),
                    None,
                    self.inner.session_auth.token_provider(),
                ))
            });

            {
                let mut rt = self.inner.runtime.write().await;
                rt.account_id = Some(account_id.clone());
                rt.display_name = Some(
                    token_dn
                        .clone()
                        .unwrap_or_else(|| account_id.as_str().to_owned()),
                );
                rt.friends = Some(Arc::clone(&friends));
                rt.chat = Some(Arc::clone(&chat));
                rt.xmpp = xmpp.clone();
                rt.stomp = stomp.clone();
                rt.party_build_id = cfg.party_build_id.clone();
                rt.default_status = cfg.default_status.clone();
                rt.default_online_type = cfg.default_online_type;
            }

            let xmpp_rx = xmpp.as_ref().map(|client| client.subscribe_events());
            let stomp_rx = stomp.as_ref().map(|client| client.subscribe_events());

            let ctx = RealtimeEventContext {
                self_id: account_id.clone(),
                http: self.inner.http.clone(),
                users: Arc::clone(&self.inner.users),
                friends: Arc::clone(&friends),
                chat: Arc::clone(&chat),
                events: self.inner.events.clone(),
                xmpp: xmpp.clone(),
                stomp: stomp.clone(),
                party_slot: Arc::clone(&self.inner.party_slot),
                party_lock: Arc::clone(&self.inner.party_lock),
                default_presence_permission: cfg.party.privacy.presence_permission,
            };

            {
                let mut tasks = self.inner.driver_tasks.lock().await;
                if let Some(rx) = xmpp_rx {
                    tasks.push(spawn_xmpp_event_handler(
                        ctx.clone(),
                        rx,
                        cancel.child_token(),
                    ));
                }
                if let Some(rx) = stomp_rx {
                    tasks.push(spawn_stomp_event_handler(
                        ctx,
                        rx,
                        cancel.child_token(),
                    ));
                }
            }

            if let Some(ref xmpp_client) = xmpp {
                let init_payload = self.build_initial_xmpp_status().await;
                xmpp_client
                    .set_initial_presence(Some(init_payload), None)
                    .await;
            }

            let pre_destroy_party = !cfg.disable_party_service && cfg.connect_to_stomp;
            let warm_build_id = !cfg.disable_party_service && cfg.party_build_id.is_none();

            let profile_fut = async {
                let profile = self.load_self_profile(&account_id, token_dn.clone()).await;
                let resolved_dn = profile
                    .display_name
                    .clone()
                    .or_else(|| token_dn.clone())
                    .unwrap_or_else(|| account_id.as_str().to_owned());
                {
                    let mut rt = self.inner.runtime.write().await;
                    rt.display_name = Some(resolved_dn.clone());
                    rt.self_profile = Some(profile);
                }
                Ok::<_, RustfnError>(resolved_dn)
            };

            let friends_fut = async {
                if cfg.fetch_friends {
                    let _ = friends.sync_summary().await?;
                }
                Ok::<_, RustfnError>(())
            };

            let xmpp_fut = async {
                if let Some(ref x) = xmpp {
                    x.connect(cancel.clone()).await?;
                }
                Ok::<_, RustfnError>(())
            };

            let build_id_fut = async {
                if warm_build_id {
                    let _ = self.ensure_party_build_id().await?;
                }
                Ok::<_, RustfnError>(())
            };
            let fn_cleanup_fut = async {
                if pre_destroy_party {
                    self.leave_fn_parties_except(&account_id, None).await?;
                }
                Ok::<_, RustfnError>(())
            };
            let eos_prep_fut = Box::pin(async {
                let Ok(puid) = eos_rx.await else {
                    // Auth failed before EOS token; `auth_fut` returns the real error.
                    return Ok::<_, RustfnError>(false);
                };
                if let Some(ref s) = stomp {
                    s.set_product_user_id(puid).await;
                }
                let stomp_connect_fut = async {
                    if let Some(ref s) = stomp {
                        let _ = s.connect(cancel.clone()).await?;
                    }
                    Ok::<_, RustfnError>(())
                };
                let eos_cleanup_fut = async {
                    if pre_destroy_party {
                        self.leave_eos_party_only(&account_id).await?;
                    }
                    Ok::<_, RustfnError>(())
                };
                tokio::try_join!(stomp_connect_fut, eos_cleanup_fut)?;
                Ok(true)
            });

            let (display_name, _, _, _, _, eos_ready) = tokio::try_join!(
                Box::pin(profile_fut),
                Box::pin(friends_fut),
                Box::pin(xmpp_fut),
                Box::pin(build_id_fut),
                Box::pin(fn_cleanup_fut),
                eos_prep_fut,
            )?;

            if eos_ready && !cfg.disable_party_service {
                Box::pin(self.init_party_on_login(&account_id, pre_destroy_party, false)).await?;
            }
            Ok(Some((account_id, display_name)))
        };

        let join_res = tokio::try_join!(Box::pin(auth_fut), Box::pin(connect_fut));
        let (account_id, display_name) = match join_res {
            Ok(((), Some(identity))) => identity,
            Ok(((), None)) => {
                auth_handle.abort();
                let _ = self.destroy(false).await;
                return Err(RustfnError::Auth(
                    "authenticated Fortnite session missing account_id".to_owned(),
                ));
            }
            Err(err) => {
                auth_handle.abort();
                let _ = self.destroy(false).await;
                return Err(err);
            }
        };

        let presence_client = self.clone();
        let presence_cancel = cancel.child_token();
        let mut presence_rx = self.inner.events.subscribe();
        let mut stomp_rx = self
            .inner
            .runtime
            .read()
            .await
            .stomp
            .as_ref()
            .map(|s| s.subscribe_events());
        let mut xmpp_rx = self
            .inner
            .runtime
            .read()
            .await
            .xmpp
            .as_ref()
            .map(|x| x.subscribe_events());
        let presence_task = tokio::spawn(async move {
            const PRESENCE_REFRESH_INTERVAL: Duration = Duration::from_secs(600);
            if let Err(err) = presence_client.set_status(None, None).await {
                debug!(error = %err, "initial presence sync deferred");
            }
            let mut last_party: Option<(LobbyId, AccountId, usize, PresencePermission)> = None;
            let mut presence_refresh_ticker = tokio::time::interval_at(
                tokio::time::Instant::now() + PRESENCE_REFRESH_INTERVAL,
                PRESENCE_REFRESH_INTERVAL,
            );
            presence_refresh_ticker
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = presence_cancel.cancelled() => break,
                    _ = presence_refresh_ticker.tick(), if presence_client.is_ready() => {
                        if let Err(err) = presence_client.set_status(None, None).await {
                            if err.is_not_found() {
                                warn!(error = %err, "periodic presence refresh returned not found; forcing STOMP reconnect");
                                let stomp = { presence_client.inner.runtime.read().await.stomp.clone() };
                                if let Some(stomp) = stomp {
                                    let _ = stomp.force_reconnect().await;
                                }
                            } else {
                                warn!(error = %err, "periodic presence refresh failed");
                            }
                        }
                    }
                    recv = async {
                        match stomp_rx.as_mut() {
                            Some(rx) => rx.recv().await,
                            None => std::future::pending().await,
                        }
                    }, if stomp_rx.is_some() => match recv {
                        Ok(stomp_ev) => {
                            if let STOMPEvent::Connected(_) = &*stomp_ev && presence_client.is_ready() {
                                let client = presence_client.clone();
                                let task_cancel = presence_cancel.clone();
                                tokio::spawn(async move {
                                    tokio::select! {
                                        _ = task_cancel.cancelled() => {}
                                        () = client.rebind_eos_party_connection() => {}
                                    }
                                });
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            debug!(skipped, "presence task lagged on STOMP events");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            stomp_rx = None;
                        }
                    },
                    recv = async {
                        match xmpp_rx.as_mut() {
                            Some(rx) => rx.recv().await,
                            None => std::future::pending().await,
                        }
                    }, if xmpp_rx.is_some() => match recv {
                        Ok(xmpp_ev) => {
                            if let XMPPEvent::Connected { .. } = &*xmpp_ev && presence_client.is_ready() {
                                let client = presence_client.clone();
                                let task_cancel = presence_cancel.clone();
                                tokio::spawn(async move {
                                    tokio::select! {
                                        _ = task_cancel.cancelled() => {}
                                        _ = client.set_status(None, None) => {}
                                    }
                                });
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            debug!(skipped, "presence task lagged on XMPP events");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            xmpp_rx = None;
                        }
                    },
                    recv = presence_rx.recv() => match recv {
                        Ok(ev) => {
                            let mut check_party = party_event_requires_init_party(&ev);
                            let Some(mut sync) = should_sync_presence(&ev, &mut last_party) else {
                                break;
                            };
                            while let Ok(queued) = presence_rx.try_recv() {
                                if let Some(eos_id) = party_event_requires_init_party(&queued) {
                                    check_party = Some(eos_id);
                                }
                                match should_sync_presence(&queued, &mut last_party) {
                                    Some(true) => sync = true,
                                    Some(false) => {}
                                    None => return,
                                }
                            }
                            if let Some(left_eos_id) = check_party
                                && !presence_client.inner.config.disable_party_service
                            {
                                let client = presence_client.clone();
                                let task_cancel = presence_cancel.clone();
                                tokio::spawn(async move {
                                    tokio::select! {
                                        _ = task_cancel.cancelled() => {}
                                        () = async {
                                            let mut rt = client.inner.runtime.write().await;
                                            if rt.leaving_eos_party_id.as_ref() == Some(&left_eos_id) {
                                                rt.leaving_eos_party_id = None;
                                                return;
                                            }
                                            drop(rt);
                                            let _ = client
                                                .init_party(client.inner.config.create_party, false)
                                                .await;
                                            if client.party().await.is_none() {
                                                let _ = client.set_status(None, None).await;
                                            }
                                        } => {}
                                    }
                                });
                            }
                            if sync {
                                let _ = presence_client.set_status(None, None).await;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        });
        self.inner.driver_tasks.lock().await.push(presence_task);

        let _ = self.inner.ready_tx.send(true);
        self.inner.events.emit(ClientEvent::Ready {
            account_id: account_id.clone(),
            display_name: display_name.clone(),
        });
        info!(account_id = %account_id, %display_name, "Client session ready");
        Ok(())
    }

    /// Gracefully leaves/terminates any active party, closes transports, stops background tasks,
    /// and revokes held Epic/EOS OAuth tokens.
    pub async fn logout(&self) -> Result<(), RustfnError> {
        let _session_guard = self.inner.session_lock.lock().await;
        self.destroy(true).await
    }

    /// Re-authenticates and reconnects using the credentials provided on the prior [`Self::login`].
    pub async fn restart(&self) -> Result<(), RustfnError> {
        let auth = self
            .inner
            .last_auth
            .read()
            .await
            .clone()
            .ok_or_else(|| RustfnError::Auth("restart requires a prior login".to_owned()))?;
        self.logout().await?;
        self.login(auth).await
    }

    /// Updates and broadcasts the local client's presence across EOS Presence v1, EOS internal party presence,
    /// and XMPP `<presence>`.
    pub async fn set_status(
        &self,
        status: Option<&str>,
        online_type: Option<PresenceOnlineType>,
    ) -> Result<(), RustfnError> {
        let (account_id, display_name, xmpp, stomp, build_id, cur_status, cur_online) = {
            let mut rt = self.inner.runtime.write().await;
            if let Some(s) = status {
                rt.default_status = s.to_owned();
            }
            if let Some(o) = online_type {
                rt.default_online_type = o;
            }
            (
                rt.account_id.clone(),
                rt.display_name.clone(),
                rt.xmpp.clone(),
                rt.stomp.clone(),
                rt.party_build_id.clone(),
                rt.default_status.clone(),
                rt.default_online_type,
            )
        };

        let Some(account_id) = account_id else {
            return Err(RustfnError::Auth("client is not logged in".to_owned()));
        };

        let active_party = self.inner.party_slot.read().await.clone();
        let party_snapshot = match &active_party {
            Some(p) => p.snapshot().await,
            None => None,
        };

        let party_size = party_snapshot.as_ref().map_or(1, |s| s.size() as i64);
        let is_joinable = party_snapshot.as_ref().is_some_and(|snap| {
            let perm = snap.config.privacy.presence_permission;
            let self_is_leader = snap.leader_id == account_id;
            perm == PresencePermission::Anyone
                || (perm == PresencePermission::Leader && self_is_leader)
        });
        let party_join_info = party_snapshot.as_ref().map(|snap| {
            if is_joinable {
                PartyJoinInfoData {
                    source_id: Some(account_id.clone()),
                    source_display_name: display_name.clone(),
                    source_platform: Some(self.inner.config.platform.as_str().to_owned()),
                    party_id: Some(snap.id.as_str().to_owned()),
                    party_type_id: Some(286_331_153),
                    key: Some("k".to_owned()),
                    app_id: Some("Fortnite".to_owned()),
                    build_id: build_id.clone(),
                    party_flags: Some(6),
                    not_accepting_members_reason: Some(0),
                    pc: Some(snap.size() as u32),
                }
            } else {
                PartyJoinInfoData {
                    source_id: None,
                    source_display_name: None,
                    source_platform: None,
                    party_id: None,
                    party_type_id: None,
                    key: None,
                    app_id: None,
                    build_id: None,
                    party_flags: None,
                    not_accepting_members_reason: Some(1),
                    pc: None,
                }
            }
        });
        let island_code = party_snapshot
            .as_ref()
            .and_then(|s| s.meta.island())
            .map(|i| i.link_id.mnemonic)
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| "experience_br".to_owned());

        let spec = OutboundPresenceSpec {
            status: cur_status.clone(),
            online_type: cur_online,
            platform: self.inner.config.platform,
            product_version: self.inner.config.product_version.clone(),
            sub_game: "Athena".to_owned(),
            party_size,
            is_joinable,
            is_in_zone: false,
            in_unjoinable_match: false,
            island_code,
            party_join_info,
        };

        let eos_presence_fut = async {
            if let Some(stomp_client) = stomp.as_ref()
                && let Some(conn) = stomp_client.connection_ids()
            {
                let props = build_eos_presence_props(&spec);
                let internal_party = party_snapshot
                    .as_ref()
                    .map(|s| s.to_eos_internal_presence());
                let public_fut = self.inner.http.patch_eos_presence(
                    &self.inner.config.eos_deployment_id,
                    &account_id,
                    &conn.public_connection_id,
                    cur_online,
                    &cur_status,
                    &props,
                    None,
                );
                let internal_fut = self.inner.http.patch_eos_internal_presence(
                    &account_id,
                    &conn.private_connection_id,
                    internal_party.as_ref(),
                    None,
                );
                tokio::try_join!(public_fut, internal_fut)?;
            }
            Ok::<_, RustfnError>(())
        };

        let xmpp_presence_fut = async {
            if let Some(xmpp_client) = xmpp {
                let payload = build_xmpp_status_payload(&spec);
                let encoded = serde_json::to_string(&payload)?;
                let show = match cur_online {
                    PresenceOnlineType::Away => Some("away".to_owned()),
                    PresenceOnlineType::Xa => Some("xa".to_owned()),
                    PresenceOnlineType::Dnd => Some("dnd".to_owned()),
                    PresenceOnlineType::Chat => Some("chat".to_owned()),
                    PresenceOnlineType::Online | PresenceOnlineType::Offline => None,
                };
                xmpp_client
                    .set_presence(
                        Some(encoded),
                        show,
                        cur_online == PresenceOnlineType::Offline,
                    )
                    .await?;
            }
            Ok::<_, RustfnError>(())
        };

        tokio::try_join!(eos_presence_fut, xmpp_presence_fut)?;
        Ok(())
    }

    pub async fn reset_status(&self) -> Result<(), RustfnError> {
        let default_status = self.inner.config.default_status.clone();
        let default_online = self.inner.config.default_online_type;
        self.set_status(Some(&default_status), Some(default_online))
            .await
    }

    /// Creates a new EOS Party v2 + linked Fortnite v1 party, leaving any existing party first.
    pub async fn create_party(
        &self,
        privacy: Option<FortnitePartyPrivacy>,
    ) -> Result<ClientParty, RustfnError> {
        let _party_guard = self.inner.party_lock.lock().await;
        self.create_party_unlocked(privacy).await
    }

    /// Joins an existing EOS Party v2 + linked Fortnite v1 party by EOS party ID or Fortnite lobby ID.
    pub async fn join_party(&self, fn_id_or_eos_id: &str) -> Result<ClientParty, RustfnError> {
        let _party_guard = self.inner.party_lock.lock().await;
        self.join_party_unlocked(fn_id_or_eos_id).await
    }

    /// Leaves the active party cleanly and optionally creates a fresh replacement lobby.
    pub async fn leave_party(&self, create_new: bool) -> Result<(), RustfnError> {
        let _party_guard = self.inner.party_lock.lock().await;
        if !create_new {
            let eos_id = self
                .inner
                .party_slot
                .read()
                .await
                .as_ref()
                .map(|p| p.eos_id().clone());
            self.inner.runtime.write().await.leaving_eos_party_id = eos_id;
        }
        self.leave_party_unlocked().await?;
        if create_new {
            let _ = self.create_party_unlocked(None).await?;
        }
        Ok(())
    }

    /// Removes the local account from any active Fortnite/EOS party on Epic's servers even without local state.
    pub async fn destroy_party(&self) -> Result<(), RustfnError> {
        let _party_guard = self.inner.party_lock.lock().await;
        let account_id = self
            .account_id()
            .await
            .ok_or_else(|| RustfnError::Auth("client is not logged in".to_owned()))?;
        if let Some(existing) = self.inner.party_slot.write().await.take() {
            existing.terminate().await;
        }
        self.destroy_party_unlocked(&account_id).await
    }

    /// Initializes [`Client::party`] from the active server party or creates a new one.
    pub async fn init_party(&self, create_new: bool, force_new: bool) -> Result<(), RustfnError> {
        let _party_guard = self.inner.party_lock.lock().await;
        self.init_party_unlocked(create_new, force_new).await
    }

    /// Fetches a snapshot of the client's active EOS + Fortnite party from Epic's servers,
    /// returning `None` if not in a party (without spawning background keep-alive tasks).
    pub async fn get_client_party(&self) -> Result<Option<PartySnapshot>, RustfnError> {
        let Some((fn_data, eos_data)) = self.fetch_client_party_data().await? else {
            return Ok(None);
        };
        let account_id = self
            .account_id()
            .await
            .ok_or_else(|| RustfnError::Auth("client is not logged in".to_owned()))?;
        let display_name = self
            .display_name()
            .await
            .unwrap_or_else(|| account_id.as_str().to_owned());
        let saved = self.inner.saved_member_meta.read().await.clone();
        let state = PartyState::from_joined_data(
            account_id,
            None,
            display_name,
            self.inner.config.platform,
            self.inner.config.region,
            &self.inner.config.party,
            fn_data,
            eos_data,
            saved.as_ref(),
            &self.inner.fallback_metadata_store,
        );
        Ok(Some(state.snapshot))
    }

    /// Fetches raw Fortnite party data by Fortnite lobby ID.
    pub async fn get_raw_fortnite_party(&self, id: &LobbyId) -> Result<FortnitePartyData, RustfnError> {
        self.inner.http.get_fn_party(id).await
    }

    async fn fetch_client_party_data(
        &self,
    ) -> Result<Option<(FortnitePartyData, crate::stomp::EOSPartyData)>, RustfnError> {
        let account_id = self
            .account_id()
            .await
            .ok_or_else(|| RustfnError::Auth("client is not logged in".to_owned()))?;
        let state = self
            .inner
            .http
            .get_eos_party_user_state(&account_id)
            .await?;
        let Some(eos_data) = state.current else {
            return Ok(None);
        };
        let active_lobby = self
            .inner
            .party_slot
            .read()
            .await
            .as_ref()
            .filter(|party| party.eos_id() == &eos_data.id)
            .map(|party| party.fn_id().clone());
        let lobby_id = match active_lobby {
            Some(lobby_id) => lobby_id,
            None => to_fortnite_lobby_id(&eos_data.id, &self.ensure_party_build_id().await?)?,
        };
        match self.get_raw_fortnite_party(&lobby_id).await {
            Ok(fn_data) => Ok(Some((fn_data, eos_data))),
            Err(err) if err.is_not_found() => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn init_party_unlocked(
        &self,
        create_new: bool,
        force_new: bool,
    ) -> Result<(), RustfnError> {
        if !force_new
            && let Some((fn_data, eos_data)) = self.fetch_client_party_data().await?
        {
            let existing_matches = self
                .inner
                .party_slot
                .read()
                .await
                .as_ref()
                .is_some_and(|p| p.eos_id() == &eos_data.id && p.fn_id() == &fn_data.id);
            if !existing_matches {
                if let Some(old) = self.inner.party_slot.write().await.take() {
                    old.terminate().await;
                }
                let party = self.spawn_client_party(fn_data, eos_data.clone(), None).await?;
                *self.inner.party_slot.write().await = Some(party);
            }

            let account_id = self
                .account_id()
                .await
                .ok_or_else(|| RustfnError::Auth("client is not logged in".to_owned()))?;
            let public_conn_id = self
                .inner
                .runtime
                .read()
                .await
                .stomp
                .as_ref()
                .and_then(|s| s.connection_ids())
                .map(|ids| ids.public_connection_id);
            if let Some(conn_id) = public_conn_id {
                match self
                    .inner
                    .http
                    .connect_eos_party(
                        &self.inner.config.eos_deployment_id,
                        &eos_data.id,
                        &account_id,
                        &conn_id,
                        None,
                    )
                    .await
                {
                    Err(err) if err.is_not_found() && create_new => {
                        if let Some(stale) = self.inner.party_slot.write().await.take() {
                            stale.terminate().await;
                        }
                        self.destroy_party_unlocked(&account_id).await?;
                        let _ = self.create_party_inner(None, true).await?;
                        return Ok(());
                    }
                    other => other?,
                }
            }
            return Ok(());
        }

        if let Some(stale) = self.inner.party_slot.write().await.take() {
            stale.terminate().await;
        }
        if create_new {
            let account_id = self
                .account_id()
                .await
                .ok_or_else(|| RustfnError::Auth("client is not logged in".to_owned()))?;
            self.destroy_party_unlocked(&account_id).await?;
            let _ = self.create_party_inner(None, true).await?;
        }
        Ok(())
    }

    /// Rebinds the active EOS party connection after STOMP reconnects.
    async fn rebind_eos_party_connection(&self) {
        if self.inner.config.disable_party_service {
            return;
        }
        let Some(party) = self.party().await else {
            return;
        };
        let (account_id, public_conn_id) = {
            let rt = self.inner.runtime.read().await;
            (
                rt.account_id.clone(),
                rt.stomp
                    .as_ref()
                    .and_then(|s| s.connection_ids())
                    .map(|ids| ids.public_connection_id),
            )
        };
        let (Some(account_id), Some(public_conn_id)) = (account_id, public_conn_id) else {
            return;
        };

        if let Err(err) = self
            .inner
            .http
            .connect_eos_party(
                &self.inner.config.eos_deployment_id,
                party.eos_id(),
                &account_id,
                &public_conn_id,
                None,
            )
            .await
        {
            debug!(error = %err, "rebind_eos_party_connection failed; reinitializing party");
            let _ = self.init_party(self.inner.config.create_party, false).await;
            return;
        }
        if let Err(err) = self.set_status(None, None).await {
            if err.is_not_found() {
                warn!(error = %err, "set_status after EOS party rebind returned not found");
            } else {
                debug!(error = %err, "set_status after EOS party rebind failed");
            }
        }
    }

    /// Fetches a read-only snapshot of an arbitrary accessible party by Fortnite lobby ID or EOS party ID.
    pub async fn get_party(&self, fn_id_or_eos_id: &str) -> Result<PartySnapshot, RustfnError> {
        let account_id = self
            .account_id()
            .await
            .ok_or_else(|| RustfnError::Auth("client is not logged in".to_owned()))?;
        let display_name = self
            .display_name()
            .await
            .unwrap_or_else(|| account_id.as_str().to_owned());

        let (lobby_id, eos_id) = if fn_id_or_eos_id.contains('-') {
            let lid = LobbyId::parse(fn_id_or_eos_id)?;
            let eid = to_eos_party_id(&lid)?;
            (lid, eid)
        } else {
            let build_id = self.ensure_party_build_id().await?;
            let eid = PartyId::parse(fn_id_or_eos_id)?;
            let lid = to_fortnite_lobby_id(&eid, &build_id)?;
            (lid, eid)
        };

        let (fn_res, eos_res) = tokio::join!(
            self.inner.http.get_fn_party(&lobby_id),
            self.inner.http.get_eos_party(&eos_id),
        );
        let fn_data = fn_res?;
        let eos_data = eos_res?;
        let state = PartyState::from_joined_data(
            account_id,
            None,
            display_name,
            self.inner.config.platform,
            self.inner.config.region,
            &self.inner.config.party,
            fn_data,
            eos_data,
            None,
            &self.inner.fallback_metadata_store,
        );
        Ok(state.snapshot)
    }

    pub async fn fetch_party_build_id(&self) -> Result<String, RustfnError> {
        let build_id = self.inner.http.fetch_party_build_id().await?;
        self.inner.runtime.write().await.party_build_id = Some(build_id.clone());
        Ok(build_id)
    }

    pub async fn invite(&self, friend: &str) -> Result<SentPartyInvitation, RustfnError> {
        let party = self
            .party()
            .await
            .ok_or_else(|| RustfnError::PartyState("client is not in a party".to_owned()))?;
        party.invite(friend).await
    }

    /// Accepts an incoming party invitation by joining the invitation's EOS party.
    pub async fn accept_invite(
        &self,
        invite: &ReceivedPartyInvitation,
    ) -> Result<ClientParty, RustfnError> {
        self.join_party(invite.eos_party_id.as_str()).await
    }

    /// Declines an incoming party invitation on Epic's servers.
    pub async fn decline_invite(&self, invite: &ReceivedPartyInvitation) -> Result<(), RustfnError> {
        invite.decline().await
    }

    pub async fn send_join_request(&self, target_id_or_name: &str) -> Result<(), RustfnError> {
        let self_id = self
            .account_id()
            .await
            .ok_or_else(|| RustfnError::Auth("client is not logged in".to_owned()))?;
        let target_id = if let Some(friends) = self.friends().await {
            friends.resolve_friend_id(target_id_or_name).await?
        } else {
            self.inner.users.resolve_id(target_id_or_name).await?
        };
        self.inner
            .http
            .send_eos_join_request(&target_id, &self_id)
            .await
    }

    pub async fn add_friend(&self, id_or_name: &str) -> Result<(), RustfnError> {
        let friends = self
            .friends()
            .await
            .ok_or_else(|| RustfnError::Auth("friend manager unavailable before login".to_owned()))?;
        friends.add(id_or_name).await
    }

    pub async fn remove_friend(&self, id_or_name: &str) -> Result<(), RustfnError> {
        let friends = self
            .friends()
            .await
            .ok_or_else(|| RustfnError::Auth("friend manager unavailable before login".to_owned()))?;
        friends.remove(id_or_name).await
    }

    pub async fn send_friend_message(
        &self,
        id_or_name: &str,
        content: &str,
    ) -> Result<SentFriendMessage, RustfnError> {
        let chat = self
            .chat()
            .await
            .ok_or_else(|| RustfnError::Auth("chat manager unavailable before login".to_owned()))?;
        chat.send_friend_message(id_or_name, content).await
    }

    async fn load_self_profile(
        &self,
        account_id: &AccountId,
        token_dn: Option<String>,
    ) -> UserProfile {
        let result = self.inner.users.fetch_self(account_id).await;

        match result {
            Ok(mut profile) => {
                if profile.display_name.is_none() {
                    profile.display_name = token_dn;
                }
                profile
            }
            Err(err) => {
                debug!(account_id = %account_id, error = %err, "self profile fetch unavailable; using token identity");
                UserProfile::partial(account_id.clone(), token_dn)
            }
        }
    }

    async fn ensure_party_build_id(&self) -> Result<String, RustfnError> {
        if let Some(existing) = self.inner.runtime.read().await.party_build_id.clone() {
            return Ok(existing);
        }
        match self.inner.http.fetch_party_build_id().await {
            Ok(build_id) => {
                self.inner.runtime.write().await.party_build_id = Some(build_id.clone());
                Ok(build_id)
            }
            Err(err) => {
                if let Some(cl) = parse_build_version_cl(&self.inner.config.product_version) {
                    let derived = format!("1:3:{cl}");
                    debug!(%derived, error = %err, "using configured product_version CL as party build ID");
                    self.inner.runtime.write().await.party_build_id = Some(derived.clone());
                    Ok(derived)
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn require_party_context(
        &self,
    ) -> Result<(AccountId, String, STOMPConnectionIds, String), RustfnError> {
        let (account_id, display_name, xmpp, stomp) = {
            let rt = self.inner.runtime.read().await;
            (
                rt.account_id.clone(),
                rt.display_name.clone(),
                rt.xmpp.clone(),
                rt.stomp.clone(),
            )
        };
        let account_id = account_id
            .ok_or_else(|| RustfnError::Auth("client is not logged in".to_owned()))?;
        let display_name =
            display_name.unwrap_or_else(|| account_id.as_str().to_owned());

        let stomp_client = stomp.ok_or_else(|| {
            RustfnError::PartyState("EOS party operations require STOMP transport".to_owned())
        })?;
        let conn_ids = stomp_client.connection_ids().ok_or_else(|| {
            RustfnError::PartyState("STOMP connection IDs not established yet".to_owned())
        })?;

        let xmpp_jid = if let Some(ref x) = xmpp {
            let domain = self.inner.config.effective_xmpp_config().domain;
            format!("{}@{}/{}", account_id.as_str(), domain, x.resource())
        } else {
            let domain = self.inner.config.effective_xmpp_config().domain;
            format!("{}@{}", account_id.as_str(), domain)
        };

        Ok((account_id, display_name, conn_ids, xmpp_jid))
    }

    async fn init_party_on_login(
        &self,
        account_id: &AccountId,
        already_destroyed: bool,
        publish_status: bool,
    ) -> Result<(), RustfnError> {
        let _party_guard = self.inner.party_lock.lock().await;
        let cfg = &self.inner.config;
        if cfg.connect_to_stomp && !already_destroyed {
            self.destroy_party_unlocked(account_id).await?;
        }
        if cfg.create_party {
            let _ = self.create_party_inner(None, publish_status).await?;
        }
        Ok(())
    }

    async fn create_party_unlocked(
        &self,
        privacy_override: Option<FortnitePartyPrivacy>,
    ) -> Result<ClientParty, RustfnError> {
        self.create_party_inner(privacy_override, true).await
    }

    async fn create_party_inner(
        &self,
        privacy_override: Option<FortnitePartyPrivacy>,
        publish_status: bool,
    ) -> Result<ClientParty, RustfnError> {
        self.leave_party_unlocked().await?;

        let (account_id, display_name, conn, xmpp_jid) = self.require_party_context().await?;
        let build_id = self.ensure_party_build_id().await?;

        let cfg = &self.inner.config;
        let privacy = privacy_override.unwrap_or(cfg.party.privacy);
        let eos_joinability = if privacy.party_type == PartyType::Private {
            PartyJoinability::InviteOnly
        } else {
            cfg.party.joinability
        };
        let eos_cfg = EOSPartyDataConfig {
            joinability: eos_joinability,
            max_size: cfg.party.max_size,
        };

        let eos_data = match self
            .inner
            .http
            .create_eos_party(
                &conn.private_connection_id,
                &cfg.eos_deployment_id,
                cfg.platform,
                &eos_cfg,
            )
            .await
        {
            Ok(data) => data,
            Err(err) if err.is_user_has_party() => {
                self.destroy_party_unlocked(&account_id).await?;
                self.inner
                    .http
                    .create_eos_party(
                        &conn.private_connection_id,
                        &cfg.eos_deployment_id,
                        cfg.platform,
                        &eos_cfg,
                    )
                    .await?
            }
            Err(err) => return Err(err),
        };

        let lobby_id = to_fortnite_lobby_id(&eos_data.id, &build_id)?;
        let mut fn_data = match self
            .connect_and_join_fn_lobby(
                &eos_data.id,
                &lobby_id,
                &account_id,
                &display_name,
                &conn.public_connection_id,
                &xmpp_jid,
            )
            .await
        {
            Ok(data) => data,
            Err(err) => {
                let _ = self.destroy_party_unlocked(&account_id).await;
                return Err(err);
            }
        };

        fn_data.config.max_size = cfg.party.max_size;
        fn_data.config.invite_ttl = cfg.party.invite_ttl_secs;
        fn_data.config.intention_ttl = cfg.party.intention_ttl_secs;
        fn_data.config.join_confirmation = cfg.party.join_confirmation;

        let custom_privacy = (privacy != cfg.party.privacy).then_some(privacy);
        if privacy.party_type == PartyType::Private {
            fn_data.config.discoverability = PartyDiscoverability::InvitedOnly;
            fn_data.config.joinability = PartyJoinability::InviteAndFormer;
        } else if custom_privacy.is_some() {
            fn_data.config.discoverability = PartyDiscoverability::All;
            fn_data.config.joinability = PartyJoinability::Open;
        } else {
            fn_data.config.discoverability = cfg.party.discoverability;
            fn_data.config.joinability = cfg.party.joinability;
        }

        let party = self
            .spawn_client_party(fn_data, eos_data, custom_privacy)
            .await?;
        *self.inner.party_slot.write().await = Some(party.clone());

        let patch_fut = async {
            if let Some(snap) = party.snapshot().await {
                let full_schema_patch = MetaPatch {
                    updated: snap.meta.as_meta().as_map().clone(),
                    deleted: Vec::new(),
                };
                party.send_patch(full_schema_patch).await?;
                if let Some(me) = snap.members.get(&account_id) {
                    let member_patch = MetaPatch {
                        updated: me.meta.as_meta().as_map().clone(),
                        deleted: Vec::new(),
                    };
                    if !member_patch.is_empty() {
                        party.send_member_patch(member_patch).await?;
                    }
                }
            }
            Ok::<_, RustfnError>(())
        };

        if !publish_status {
            patch_fut.await?;
        } else {
            let (patch_res, status_res) = tokio::join!(patch_fut, self.set_status(None, None));
            patch_res?;
            if let Err(err) = status_res {
                debug!(account_id = %account_id, error = %err, "presence sync after create_party deferred");
            }
        }
        Ok(party)
    }

    async fn connect_and_join_fn_lobby(
        &self,
        eos_party_id: &PartyId,
        lobby_id: &LobbyId,
        account_id: &AccountId,
        display_name: &str,
        public_connection_id: &str,
        xmpp_jid: &str,
    ) -> Result<FortnitePartyData, RustfnError> {
        let cfg = &self.inner.config;
        // Epic requires EOS-to-FN connection registration before joining the linked FN lobby.
        self.inner
            .http
            .connect_eos_party(
                &cfg.eos_deployment_id,
                eos_party_id,
                account_id,
                public_connection_id,
                None,
            )
            .await?;
        self.join_fn_lobby_with_recovery(
            eos_party_id,
            lobby_id,
            account_id,
            display_name,
            xmpp_jid,
        )
        .await
    }

    async fn join_party_unlocked(&self, fn_id_or_eos_id: &str) -> Result<ClientParty, RustfnError> {
        let (eos_party_id, lobby_id) = if fn_id_or_eos_id.contains('-') {
            let lid = LobbyId::parse(fn_id_or_eos_id)?;
            (to_eos_party_id(&lid)?, Some(lid))
        } else {
            (PartyId::parse(fn_id_or_eos_id)?, None)
        };

        self.leave_party_unlocked().await?;

        let (account_id, display_name, conn, xmpp_jid) = self.require_party_context().await?;
        let lobby_id = match lobby_id {
            Some(lobby_id) => lobby_id,
            None => to_fortnite_lobby_id(&eos_party_id, &self.ensure_party_build_id().await?)?,
        };
        let cfg = &self.inner.config;

        match self
            .inner
            .http
            .join_eos_party(
                &eos_party_id,
                &account_id,
                &conn.private_connection_id,
                &cfg.eos_deployment_id,
                cfg.platform,
            )
            .await
        {
            Ok(()) => {}
            Err(err) if err.is_user_has_party() => {
                self.destroy_party_unlocked(&account_id).await?;
                self.inner
                    .http
                    .join_eos_party(
                        &eos_party_id,
                        &account_id,
                        &conn.private_connection_id,
                        &cfg.eos_deployment_id,
                        cfg.platform,
                    )
                    .await?;
            }
            Err(err) => return Err(err),
        }

        let (fn_data, eos_data) = match tokio::try_join!(
            self.connect_and_join_fn_lobby(
                &eos_party_id,
                &lobby_id,
                &account_id,
                &display_name,
                &conn.public_connection_id,
                &xmpp_jid,
            ),
            self.inner.http.get_eos_party(&eos_party_id),
        ) {
            Ok(pair) => pair,
            Err(err) => {
                let _ = self.destroy_party_unlocked(&account_id).await;
                return Err(err);
            }
        };

        let party = self.spawn_client_party(fn_data, eos_data, None).await?;
        *self.inner.party_slot.write().await = Some(party.clone());

        let saved = self.inner.saved_member_meta.read().await.clone();
        if let Some(saved) = saved
            && !saved.is_empty()
        {
            let _ = party
                .send_member_patch(MetaPatch {
                    updated: saved.into_map(),
                    deleted: Vec::new(),
                })
                .await;
        }

        if let Err(err) = self.set_status(None, None).await {
            debug!(account_id = %account_id, error = %err, "presence sync after join_party deferred");
        }
        Ok(party)
    }

    async fn join_fn_lobby_with_recovery(
        &self,
        eos_party_id: &PartyId,
        lobby_id: &LobbyId,
        account_id: &AccountId,
        display_name: &str,
        xmpp_jid: &str,
    ) -> Result<FortnitePartyData, RustfnError> {
        let cfg = &self.inner.config;
        match self
            .inner
            .http
            .join_fn_lobby(
                eos_party_id,
                lobby_id,
                account_id,
                display_name,
                xmpp_jid,
                cfg.platform,
                cfg.party.join_confirmation,
                cfg.party.max_size,
            )
            .await
        {
            Ok(data) => Ok(data),
            Err(err) if err.is_user_has_party() => {
                self.leave_fn_parties_except(account_id, Some(lobby_id))
                    .await?;
                self.inner
                    .http
                    .join_fn_lobby(
                        eos_party_id,
                        lobby_id,
                        account_id,
                        display_name,
                        xmpp_jid,
                        cfg.platform,
                        cfg.party.join_confirmation,
                        cfg.party.max_size,
                    )
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn leave_fn_parties_except(
        &self,
        account_id: &AccountId,
        keep_lobby_id: Option<&LobbyId>,
    ) -> Result<(), RustfnError> {
        let fn_state = match self.inner.http.get_fn_party_user_state(account_id).await {
            Ok(state) => state,
            Err(err) if err.is_not_found() => return Ok(()),
            Err(err) => return Err(err),
        };
        for lobby in fn_state.current {
            if keep_lobby_id.is_some_and(|keep| keep == &lobby.id) {
                continue;
            }
            if let Err(err) = self
                .inner
                .http
                .leave_fn_party_member(&lobby.id, account_id, None)
                .await
                && !err.is_not_found()
            {
                return Err(err);
            }
        }
        Ok(())
    }

    async fn leave_party_unlocked(&self) -> Result<(), RustfnError> {
        let existing = self.inner.party_slot.write().await.take();
        if let Some(party) = existing {
            party.leave().await?;
        }
        Ok(())
    }

    async fn leave_eos_party_only(&self, account_id: &AccountId) -> Result<(), RustfnError> {
        let eos_state = match self.inner.http.get_eos_party_user_state(account_id).await {
            Ok(state) => state,
            Err(err) if err.is_not_found() => return Ok(()),
            Err(err) => return Err(err),
        };
        if let Some(eos_party) = eos_state.current
            && let Err(err) = self
                .inner
                .http
                .remove_eos_party_member(&eos_party.id, account_id, None)
                .await
            && !err.is_not_found()
        {
            return Err(err);
        }
        Ok(())
    }

    async fn destroy_party_unlocked(&self, account_id: &AccountId) -> Result<(), RustfnError> {
        let _ = tokio::try_join!(
            self.leave_fn_parties_except(account_id, None),
            self.leave_eos_party_only(account_id),
        )?;
        Ok(())
    }

    async fn spawn_client_party(
        &self,
        fn_data: FortnitePartyData,
        eos_data: crate::stomp::EOSPartyData,
        privacy_override: Option<FortnitePartyPrivacy>,
    ) -> Result<ClientParty, RustfnError> {
        let rt = self.inner.runtime.read().await;
        let account_id = rt
            .account_id
            .clone()
            .ok_or_else(|| RustfnError::Auth("client is not logged in".to_owned()))?;
        let display_name = rt
            .display_name
            .clone()
            .unwrap_or_else(|| account_id.as_str().to_owned());
        let friends = rt.friends.clone().ok_or_else(|| {
            RustfnError::PartyState("friend manager missing during party spawn".to_owned())
        })?;
        let chat = rt.chat.clone().ok_or_else(|| {
            RustfnError::PartyState("chat manager missing during party spawn".to_owned())
        })?;
        let metadata_store = rt
            .xmpp
            .as_ref()
            .map(|x| Arc::clone(x.metadata_store()))
            .unwrap_or_else(|| Arc::clone(&self.inner.fallback_metadata_store));
        drop(rt);

        let parent_cancel = self
            .inner
            .session_cancel
            .lock()
            .await
            .clone()
            .unwrap_or_default();

        let mut party_config = self.inner.config.party.clone();
        if let Some(privacy) = privacy_override {
            if privacy.party_type == PartyType::Private {
                party_config.discoverability = PartyDiscoverability::InvitedOnly;
                party_config.joinability = PartyJoinability::InviteAndFormer;
            } else {
                party_config.discoverability = PartyDiscoverability::All;
                party_config.joinability = PartyJoinability::Open;
            }
            party_config.privacy = privacy;
        } else if party_config.privacy.party_type == PartyType::Private {
            party_config.discoverability = PartyDiscoverability::InvitedOnly;
            party_config.joinability = PartyJoinability::InviteAndFormer;
        }

        let self_puid = self.inner.session_auth.product_user_id().await;

        let party = ClientParty::spawn(ClientPartyInit {
            self_id: account_id,
            self_puid,
            self_display_name: display_name,
            platform: self.inner.config.platform,
            region: self.inner.config.region,
            party_config,
            save_member_meta: self.inner.config.save_party_member_meta,
            fn_data,
            eos_data,
            http: self.inner.http.clone(),
            metadata_store,
            users: Arc::clone(&self.inner.users),
            friends,
            chat,
            events: Arc::new(self.inner.events.clone()) as Arc<dyn PartyEventSink>,
            snapshot_slot: Arc::new(RwLock::new(None)),
            saved_member_meta: Arc::clone(&self.inner.saved_member_meta),
            parent_cancel,
        })
        .await;

        Ok(party)
    }

    async fn build_initial_xmpp_status(&self) -> String {
        let cfg = &self.inner.config;
        let spec = OutboundPresenceSpec {
            status: cfg.default_status.clone(),
            online_type: cfg.default_online_type,
            platform: cfg.platform,
            product_version: cfg.product_version.clone(),
            sub_game: "Athena".to_owned(),
            party_size: 1,
            is_joinable: false,
            is_in_zone: false,
            in_unjoinable_match: false,
            island_code: String::new(),
            party_join_info: None,
        };
        serde_json::to_string(&build_xmpp_status_payload(&spec)).unwrap_or_default()
    }

    async fn destroy(&self, revoke_tokens: bool) -> Result<(), RustfnError> {
        let was_ready = *self.inner.ready_rx.borrow();
        let mut first_err = None;
        let _ = self.inner.ready_tx.send(false);

        let existing_party = self.inner.party_slot.write().await.take();
        if let Some(party) = existing_party {
            if self.inner.config.leave_party_on_shutdown && revoke_tokens {
                if let Err(err) = party.leave().await {
                    debug!(error = %err, "party leave during logout failed; terminating");
                    party.terminate().await;
                    first_err = Some(err);
                }
            } else {
                party.terminate().await;
            }
        }

        if let Some(cancel) = self.inner.session_cancel.lock().await.take() {
            cancel.cancel();
        }

        let (xmpp, stomp) = {
            let mut rt = self.inner.runtime.write().await;
            rt.account_id = None;
            rt.display_name = None;
            rt.self_profile = None;
            rt.friends = None;
            rt.chat = None;
            rt.leaving_eos_party_id = None;
            (rt.xmpp.take(), rt.stomp.take())
        };

        if let Some(x) = xmpp {
            x.disconnect().await;
        }
        if let Some(s) = stomp {
            s.disconnect().await;
        }

        let handles = {
            let mut guard = self.inner.driver_tasks.lock().await;
            std::mem::take(&mut *guard)
        };
        for handle in handles {
            let _ = handle.await;
        }

        if let Err(err) = self.inner.session_auth.shutdown(revoke_tokens).await {
            debug!(error = %err, "auth shutdown failed");
            if first_err.is_none() {
                first_err = Some(err);
            }
        }

        if was_ready {
            self.inner.events.emit(ClientEvent::Disconnected);
        }
        first_err.map_or(Ok(()), Err)
    }
}

/// Manages authentication, stored device credentials, and token renewal for a client.
pub struct SessionAuth {
    manager: Arc<Auth>,
    device_store: Option<Arc<dyn DeviceAuthStore>>,
    refresh_loop: Mutex<Option<TokenRefreshLoop>>,
    event_cancel: Mutex<Option<CancellationToken>>,
    event_task: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for SessionAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionAuth")
            .field("has_device_store", &self.device_store.is_some())
            .finish()
    }
}

impl SessionAuth {
    pub fn new(
        config: &ClientConfig,
        device_store: Option<Arc<dyn DeviceAuthStore>>,
    ) -> Result<Self, RustfnError> {
        let resolved_store = device_store.or_else(|| {
            config.device_auth_path.as_ref().map(|path| {
                Arc::new(JsonFileDeviceAuthStore::new(path)) as Arc<dyn DeviceAuthStore>
            })
        });
        let auth_cfg = config.effective_auth_config(resolved_store.clone());
        let manager = Auth::new(auth_cfg, config.effective_http_config())?;
        Ok(Self {
            manager,
            device_store: resolved_store,
            refresh_loop: Mutex::new(None),
            event_cancel: Mutex::new(None),
            event_task: Mutex::new(None),
        })
    }

    pub fn manager(&self) -> &Arc<Auth> {
        &self.manager
    }

    pub fn http_client(&self) -> &HttpClient {
        self.manager.http_client()
    }

    pub fn token_provider(&self) -> Arc<dyn TokenProvider> {
        self.manager.token_provider()
    }

    pub async fn account_id(&self) -> Option<AccountId> {
        self.manager.account_id().await
    }

    pub async fn display_name(&self) -> Option<String> {
        self.manager.display_name().await
    }

    pub async fn product_user_id(&self) -> Option<ProductUserId> {
        self.manager.product_user_id().await
    }

    /// Authenticates this session (preferring stored device auth if `account_hint` matches an entry)
    /// and forwards all [`crate::auth::AuthEvent`] and [`ClientEvent::DeviceCodePrompt`] notifications onto `events`.
    pub async fn authenticate(
        &self,
        method: AuthMethod,
        account_hint: Option<&AccountId>,
        events: &ClientEventBus,
        parent_cancel: &CancellationToken,
    ) -> Result<(), RustfnError> {
        self.authenticate_with_hooks(
            method,
            account_hint,
            events,
            parent_cancel,
            AuthProgressHooks::default(),
        )
        .await
    }

    pub async fn authenticate_with_hooks(
        &self,
        method: AuthMethod,
        account_hint: Option<&AccountId>,
        events: &ClientEventBus,
        parent_cancel: &CancellationToken,
        hooks: AuthProgressHooks,
    ) -> Result<(), RustfnError> {
        self.start_event_forwarder(events, parent_cancel).await;

        let res = match (account_hint, &self.device_store, &method) {
            (Some(hint), Some(store), m) if !matches!(m, AuthMethod::DeviceAuth(_)) => {
                if let Some(creds) = store.load(hint).await? {
                    self.manager
                        .authenticate_with_hooks(
                            AuthMethod::DeviceAuth(creds),
                            parent_cancel.clone(),
                            hooks,
                            |_| async {},
                        )
                        .await
                } else {
                    self.authenticate_direct(method, events, parent_cancel, hooks)
                        .await
                }
            }
            _ => {
                self.authenticate_direct(method, events, parent_cancel, hooks)
                    .await
            }
        };

        if res.is_err() {
            self.stop_event_forwarder().await;
        }
        res
    }

    /// Authenticates this session explicitly using [`DeviceCodeAuth`] and emits [`ClientEvent::DeviceCodePrompt`].
    pub async fn authenticate_device_code(
        &self,
        auth: DeviceCodeAuth,
        events: &ClientEventBus,
        parent_cancel: &CancellationToken,
    ) -> Result<(), RustfnError> {
        self.authenticate(AuthMethod::DeviceCodeAuth(auth), None, events, parent_cancel)
            .await
    }

    async fn authenticate_direct(
        &self,
        method: AuthMethod,
        events: &ClientEventBus,
        parent_cancel: &CancellationToken,
        hooks: AuthProgressHooks,
    ) -> Result<(), RustfnError> {
        if method.device_code_config().is_some() {
            let events_bus = events.clone();
            self.manager
                .authenticate_with_hooks(method, parent_cancel.clone(), hooks, move |prompt| {
                    let bus = events_bus.clone();
                    async move {
                        bus.emit(ClientEvent::DeviceCodePrompt {
                            verification_uri_complete: prompt
                                .verification_uri_complete
                                .unwrap_or_else(|| prompt.verification_uri.clone()),
                            verification_uri: prompt.verification_uri,
                            user_code: prompt.user_code,
                        });
                    }
                })
                .await
        } else {
            self.manager
                .authenticate_with_hooks(method, parent_cancel.clone(), hooks, |_| async {})
                .await
        }
    }

    /// Starts (or restarts) the background token renewal scheduler under `parent_cancel`.
    pub async fn start_refresh_loop(&self, parent_cancel: &CancellationToken) {
        self.stop_refresh_loop().await;
        let loop_handle = self.manager.spawn_refresh_loop(parent_cancel.child_token());
        *self.refresh_loop.lock().await = Some(loop_handle);
    }

    /// Stops background auth tasks and optionally revokes held tokens on Epic's OAuth endpoints.
    pub async fn shutdown(&self, revoke_tokens: bool) -> Result<(), RustfnError> {
        self.stop_refresh_loop().await;
        self.stop_event_forwarder().await;
        if revoke_tokens {
            self.manager.revoke_all().await
        } else {
            self.manager.store().clear().await;
            Ok(())
        }
    }

    async fn start_event_forwarder(
        &self,
        events: &ClientEventBus,
        parent_cancel: &CancellationToken,
    ) {
        self.stop_event_forwarder().await;
        let cancel = parent_cancel.child_token();
        let mut rx = self.manager.subscribe_events();
        let bus = events.clone();
        let task_cancel = cancel.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = task_cancel.cancelled() => break,
                    recv = rx.recv() => match recv {
                        Ok(ev) => {
                            bus.emit(ClientEvent::Auth(ev));
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::warn!(skipped, "auth event forwarder lagged behind broadcast buffer");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        });

        *self.event_cancel.lock().await = Some(cancel);
        *self.event_task.lock().await = Some(handle);
    }

    async fn stop_event_forwarder(&self) {
        if let Some(cancel) = self.event_cancel.lock().await.take() {
            cancel.cancel();
        }
        if let Some(handle) = self.event_task.lock().await.take() {
            let _ = handle.await;
        }
    }

    async fn stop_refresh_loop(&self) {
        if let Some(refresh_loop) = self.refresh_loop.lock().await.take() {
            refresh_loop.stop().await;
        }
    }
}

fn party_event_requires_init_party(event: &ClientEvent) -> Option<PartyId> {
    match event {
        ClientEvent::PartyLeft { eos_party_id, .. } => Some(eos_party_id.clone()),
        _ => None,
    }
}

fn should_sync_presence(
    event: &ClientEvent,
    last_party: &mut Option<(LobbyId, AccountId, usize, PresencePermission)>,
) -> Option<bool> {
    match event {
        ClientEvent::PartyMemberJoined { .. }
        | ClientEvent::PartyMemberLeft { .. }
        | ClientEvent::PartyMemberKicked { .. }
        | ClientEvent::PartyMemberExpired { .. } => Some(true),
        ClientEvent::PartyLeft { .. } => {
            *last_party = None;
            Some(true)
        }
        ClientEvent::PartyUpdated(snap) => {
            let key = (
                snap.id.clone(),
                snap.leader_id.clone(),
                snap.size(),
                snap.config.privacy.presence_permission,
            );
            if last_party.as_ref() != Some(&key) {
                *last_party = Some(key);
                Some(true)
            } else {
                Some(false)
            }
        }
        ClientEvent::Disconnected => None,
        _ => Some(false),
    }
}
