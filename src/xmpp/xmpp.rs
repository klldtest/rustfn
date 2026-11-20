use crate::xmpp::message::{XMPPAdminMessage, parse_admin_message};
use crate::xmpp::metadata_store::{DEFAULT_MEMBER_METADATA_TTL, XMPPMetadataStore};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use futures_util::{SinkExt, StreamExt};
use quick_xml::Reader;
use quick_xml::escape::escape;
use quick_xml::events::{BytesStart, Event};
use crate::http::{AuthSessionSlot, TokenProvider};
use crate::protocol::{AccountId, Platform, RustfnError, ServiceEndpoints};
use secrecy::zeroize::Zeroizing;
use secrecy::{ExposeSecret, SecretString};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval_at, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

/// Configuration for [`XMPP`] RFC 7395 connections, pings, and reconnect limits.
#[derive(Debug, Clone)]
pub struct XMPPConfig {
    pub wss_url: String,
    pub domain: String,
    pub platform: Platform,
    pub keepalive_interval: Duration,
    pub ping_timeout: Duration,
    pub connection_timeout: Duration,
    pub max_connection_retries: u32,
    pub retry_delay: Duration,
    pub metadata_ttl: Duration,
    pub event_channel_capacity: usize,
}

impl Default for XMPPConfig {
    fn default() -> Self {
        let endpoints = ServiceEndpoints::default();
        Self {
            wss_url: endpoints.xmpp_wss.into_owned(),
            domain: endpoints.epic_prod_env.into_owned(),
            platform: Platform::Win,
            keepalive_interval: Duration::from_secs(10),
            ping_timeout: Duration::from_secs(45),
            connection_timeout: Duration::from_secs(60),
            max_connection_retries: 20,
            retry_delay: Duration::from_secs(5),
            metadata_ttl: DEFAULT_MEMBER_METADATA_TTL,
            event_channel_capacity: 512,
        }
    }
}

type InitialPresencePayload = Option<(Option<String>, Option<String>)>;

/// Asynchronous XMPP-over-WebSocket manager with bounded reconnection (`< max_retries`),
/// fresh bearer token acquisition via [`TokenProvider`], and buffered party delta storage.
pub struct XMPP {
    config: XMPPConfig,
    account_id: AccountId,
    resource: String,
    token_provider: Arc<dyn TokenProvider>,
    metadata_store: Arc<XMPPMetadataStore>,
    event_tx: broadcast::Sender<Arc<XMPPEvent>>,
    connected_tx: watch::Sender<bool>,
    connected_rx: watch::Receiver<bool>,
    cmd_tx: Mutex<Option<mpsc::Sender<XMPPCommand>>>,
    initial_presence: Arc<Mutex<InitialPresencePayload>>,
    task_handle: Mutex<Option<JoinHandle<()>>>,
    cancel_token: Mutex<Option<CancellationToken>>,
}

impl std::fmt::Debug for XMPP {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XMPP")
            .field("account_id", &self.account_id)
            .field("resource", &self.resource)
            .field("wss_url", &self.config.wss_url)
            .field("connected", &*self.connected_rx.borrow())
            .finish()
    }
}

impl XMPP {
    pub fn new(
        config: XMPPConfig,
        account_id: AccountId,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Self {
        let resource = build_fortnite_resource(config.platform);
        let (event_tx, _) = broadcast::channel(config.event_channel_capacity.max(32));
        let (connected_tx, connected_rx) = watch::channel(false);
        let metadata_store = Arc::new(XMPPMetadataStore::new(config.metadata_ttl));

        Self {
            config,
            account_id,
            resource,
            token_provider,
            metadata_store,
            event_tx,
            connected_tx,
            connected_rx,
            cmd_tx: Mutex::new(None),
            initial_presence: Arc::new(Mutex::new(None)),
            task_handle: Mutex::new(None),
            cancel_token: Mutex::new(None),
        }
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn resource(&self) -> &str {
        &self.resource
    }

    pub fn metadata_store(&self) -> &Arc<XMPPMetadataStore> {
        &self.metadata_store
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<Arc<XMPPEvent>> {
        self.event_tx.subscribe()
    }

    pub fn is_connected(&self) -> bool {
        *self.connected_rx.borrow()
    }

    pub fn watch_connected(&self) -> watch::Receiver<bool> {
        self.connected_rx.clone()
    }

    pub async fn set_initial_presence(&self, status: Option<String>, show: Option<String>) {
        *self.initial_presence.lock().await = Some((status, show));
    }

    /// Spawns the XMPP WebSocket driver and waits until the initial session is established
    /// (or fails within [`XMPPConfig::connection_timeout`]).
    pub async fn connect(&self, parent_cancel: CancellationToken) -> Result<(), RustfnError> {
        self.disconnect().await;

        let cancel = parent_cancel.child_token();
        let (cmd_tx, mut cmd_rx) = mpsc::channel(128);
        *self.cmd_tx.lock().await = Some(cmd_tx);
        *self.cancel_token.lock().await = Some(cancel.clone());

        let config = self.config.clone();
        let account_id = self.account_id.clone();
        let resource = self.resource.clone();
        let token_provider = Arc::clone(&self.token_provider);
        let event_tx = self.event_tx.clone();
        let connected_tx = self.connected_tx.clone();
        let initial_presence = Arc::clone(&self.initial_presence);
        let (first_ready_tx, first_ready_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        let mut first_ready_tx = Some(first_ready_tx);

        let handle = tokio::spawn(async move {
            let mut retry_count: u32 = 0;

            loop {
                if cancel.is_cancelled() {
                    break;
                }

                let access_token = match token_provider.bearer_token(AuthSessionSlot::Fortnite).await {
                    Ok(tok) => tok,
                    Err(err) => {
                        let msg = err.to_string();
                        if let Some(tx) = first_ready_tx.take() {
                            let _ = tx.send(Err(msg.clone()));
                            break;
                        }
                        if !schedule_xmpp_reconnect(
                            &mut retry_count,
                            &config,
                            &event_tx,
                            &cancel,
                            msg,
                        )
                        .await
                        {
                            break;
                        }
                        continue;
                    }
                };

                let pres_snapshot = initial_presence.lock().await.clone();
                let mut first_tx_for_stream = first_ready_tx.take();
                let mut established_this_attempt = false;

                let params = SessionParams {
                    wss_url: &config.wss_url,
                    domain: &config.domain,
                    account_id: account_id.as_str(),
                    resource: &resource,
                    access_token: &access_token,
                    initial_presence: pres_snapshot,
                    connection_timeout: config.connection_timeout,
                    keepalive_interval: config.keepalive_interval,
                    ping_timeout: config.ping_timeout,
                };

                let event_tx_ref = &event_tx;
                let outcome = run_connection(
                    params,
                    &mut cmd_rx,
                    &connected_tx,
                    |ev| {
                        if matches!(ev, XMPPEvent::Connected { .. }) {
                            established_this_attempt = true;
                            if let Some(tx) = first_tx_for_stream.take() {
                                let _ = tx.send(Ok(()));
                            }
                        }
                        let _ = event_tx_ref.send(Arc::new(ev));
                    },
                    &cancel,
                )
                .await;

                if established_this_attempt {
                    retry_count = 0;
                }

                match outcome {
                    ConnectionExit::Clean(
                        StreamExitReason::Cancelled | StreamExitReason::ClientDisconnected,
                    ) => {
                        let _ = event_tx.send(Arc::new(XMPPEvent::Disconnected {
                            will_reconnect: false,
                            reason: "client disconnected".to_owned(),
                        }));
                        break;
                    }
                    ConnectionExit::Clean(StreamExitReason::ForcedReconnect) => {
                        let _ = event_tx.send(Arc::new(XMPPEvent::Disconnected {
                            will_reconnect: true,
                            reason: "forced reconnect".to_owned(),
                        }));
                        continue;
                    }
                    ConnectionExit::Clean(StreamExitReason::StreamClosed) => {
                        if let Some(tx) = first_tx_for_stream.take() {
                            let _ = tx.send(Err("XMPP stream closed before establishment".to_owned()));
                            break;
                        }
                        if !schedule_xmpp_reconnect(
                            &mut retry_count,
                            &config,
                            &event_tx,
                            &cancel,
                            "stream closed by peer".to_owned(),
                        )
                        .await
                        {
                            break;
                        }
                    }
                    ConnectionExit::Error { reason, auth_failed } => {
                        if auth_failed {
                            let _ = token_provider
                                .refresh_on_invalid_token(
                                    AuthSessionSlot::Fortnite,
                                    access_token.expose_secret(),
                                )
                                .await;
                        }
                        if let Some(tx) = first_tx_for_stream.take() {
                            let _ = tx.send(Err(reason));
                            break;
                        }
                        info!(
                            account_id = %account_id,
                            retry = retry_count + 1,
                            max_retries = config.max_connection_retries,
                            reason = %reason,
                            "XMPP disconnected"
                        );
                        if !schedule_xmpp_reconnect(
                            &mut retry_count,
                            &config,
                            &event_tx,
                            &cancel,
                            reason,
                        )
                        .await
                        {
                            break;
                        }
                    }
                }
            }

            let _ = connected_tx.send(false);
        });

        *self.task_handle.lock().await = Some(handle);

        match first_ready_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(msg)) => Err(RustfnError::Transport(msg)),
            Err(_) => Err(RustfnError::Transport(
                "XMPP task exited before establishing session".to_owned(),
            )),
        }
    }

    pub async fn send_command(&self, cmd: XMPPCommand) -> Result<(), RustfnError> {
        let tx = self.cmd_tx.lock().await.clone().ok_or_else(|| {
            RustfnError::Transport("XMPP client is not connected".to_owned())
        })?;
        tx.send(cmd)
            .await
            .map_err(|_| RustfnError::Transport("XMPP command channel closed".to_owned()))
    }

    pub async fn set_presence(
        &self,
        status: Option<String>,
        show: Option<String>,
        unavailable: bool,
    ) -> Result<(), RustfnError> {
        self.set_initial_presence(status.clone(), show.clone()).await;
        self.send_command(XMPPCommand::UpdatePresence {
            status,
            show,
            unavailable,
        })
        .await
    }

    pub async fn send_stanza(&self, xml: impl Into<String>) -> Result<(), RustfnError> {
        self.send_command(XMPPCommand::SendStanza(xml.into())).await
    }

    pub async fn disconnect(&self) {
        if let Some(tx) = self.cmd_tx.lock().await.take() {
            let _ = tx.send(XMPPCommand::Disconnect).await;
        }
        if let Some(cancel) = self.cancel_token.lock().await.take() {
            cancel.cancel();
        }
        if let Some(handle) = self.task_handle.lock().await.take() {
            let _ = handle.await;
        }
        self.metadata_store.clear();
    }
}

/// Outbound command queued for the active XMPP WebSocket stream.
#[derive(Debug, Clone)]
pub enum XMPPCommand {
    SendStanza(String),
    SendChat {
        to_jid: String,
        id: String,
        body: String,
    },
    UpdatePresence {
        status: Option<String>,
        show: Option<String>,
        unavailable: bool,
    },
    ForceReconnect,
    Disconnect,
}

/// High-level events produced by the XMPP WebSocket stream.
#[derive(Debug, Clone)]
pub enum XMPPEvent {
    Connected {
        jid: Option<String>,
        resource: String,
    },
    Disconnected {
        will_reconnect: bool,
        reason: String,
    },
    ReconnectExhausted {
        attempts: u32,
        last_error: String,
    },
    AdminMessage(XMPPAdminMessage),
    ChatMessage {
        from: String,
        to: Option<String>,
        id: Option<String>,
        body: String,
    },
    RawPresence {
        from: String,
        presence_type: Option<String>,
        show: Option<String>,
        status: Option<String>,
    },
    StanzaError(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamExitReason {
    Cancelled,
    ClientDisconnected,
    ForcedReconnect,
    StreamClosed,
}

async fn schedule_xmpp_reconnect(
    retry_count: &mut u32,
    config: &XMPPConfig,
    event_tx: &broadcast::Sender<Arc<XMPPEvent>>,
    cancel: &CancellationToken,
    reason: String,
) -> bool {
    if *retry_count >= config.max_connection_retries {
        let _ = event_tx.send(Arc::new(XMPPEvent::ReconnectExhausted {
            attempts: *retry_count,
            last_error: reason,
        }));
        return false;
    }
    *retry_count += 1;
    let _ = event_tx.send(Arc::new(XMPPEvent::Disconnected {
        will_reconnect: true,
        reason,
    }));
    let delay = config
        .retry_delay
        .saturating_mul(1u32 << retry_count.saturating_sub(1).min(3))
        .min(Duration::from_secs(30));
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandshakeStage {
    AwaitAuthFeatures,
    AwaitSaslOutcome,
    AwaitBindFeatures,
    AwaitBindResult,
    AwaitSessionResult,
    Established,
}

pub struct SessionParams<'a> {
    pub wss_url: &'a str,
    pub domain: &'a str,
    pub account_id: &'a str,
    pub resource: &'a str,
    pub access_token: &'a SecretString,
    pub initial_presence: Option<(Option<String>, Option<String>)>,
    pub connection_timeout: Duration,
    pub keepalive_interval: Duration,
    pub ping_timeout: Duration,
}

pub enum ConnectionExit {
    Clean(StreamExitReason),
    Error {
        reason: String,
        auth_failed: bool,
    },
}

/// Runs a single RFC 7395 XMPP-over-WebSocket connection from TLS/WS upgrade through
/// handshake, keep-alive pinging, 45s pong watchdog, and inbound stanza dispatch.
pub async fn run_connection(
    params: SessionParams<'_>,
    cmd_rx: &mut tokio::sync::mpsc::Receiver<XMPPCommand>,
    on_established: &tokio::sync::watch::Sender<bool>,
    mut on_event: impl FnMut(XMPPEvent),
    cancel: &CancellationToken,
) -> ConnectionExit {
    let mut request = match params.wss_url.into_client_request() {
        Ok(req) => req,
        Err(err) => {
            return ConnectionExit::Error {
                reason: format!("invalid XMPP WebSocket URL `{}`: {err}", params.wss_url),
                auth_failed: false,
            };
        }
    };

    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("xmpp"));
    let auth_header = match HeaderValue::from_str(&format!("Bearer {}", params.access_token.expose_secret())) {
        Ok(val) => val,
        Err(err) => {
            return ConnectionExit::Error {
                reason: format!("invalid XMPP Authorization header: {err}"),
                auth_failed: true,
            };
        }
    };
    request.headers_mut().insert("Authorization", auth_header);

    let connect_res = match timeout(params.connection_timeout, connect_async(request)).await {
        Ok(Ok((ws, _resp))) => ws,
        Ok(Err(tokio_tungstenite::tungstenite::Error::Http(resp))) => {
            let status = resp.status();
            let auth_failed = status.as_u16() == 401 || status.as_u16() == 403;
            return ConnectionExit::Error {
                reason: format!("XMPP WebSocket HTTP upgrade rejected: {status}"),
                auth_failed,
            };
        }
        Ok(Err(err)) => {
            return ConnectionExit::Error {
                reason: format!("XMPP WebSocket connect error: {err}"),
                auth_failed: false,
            };
        }
        Err(_) => {
            return ConnectionExit::Error {
                reason: format!("XMPP connection timed out after {:?}", params.connection_timeout),
                auth_failed: false,
            };
        }
    };

    let (mut sink, mut stream) = connect_res.split();
    if let Err(err) = sink.send(Message::text(open_frame(params.domain))).await {
        return ConnectionExit::Error {
            reason: format!("failed to send XMPP <open>: {err}"),
            auth_failed: false,
        };
    }

    let mut stage = HandshakeStage::AwaitAuthFeatures;
    let mut ping_ticker = interval_at(
        Instant::now() + params.keepalive_interval,
        params.keepalive_interval,
    );
    ping_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let watchdog_step = Duration::from_secs(5).min(params.ping_timeout / 2).max(Duration::from_millis(50));
    let mut watchdog_ticker = interval_at(Instant::now() + watchdog_step, watchdog_step);
    watchdog_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let handshake_deadline = Instant::now() + params.connection_timeout;
    let mut last_pong = Instant::now();
    let mut bound_jid: Option<String> = None;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                let _ = sink.send(Message::text(presence_frame(None, None, true))).await;
                let _ = sink.send(Message::text(close_frame())).await;
                let _ = sink.close().await;
                let _ = on_established.send(false);
                return ConnectionExit::Clean(StreamExitReason::Cancelled);
            }
            _ = tokio::time::sleep_until(handshake_deadline), if stage != HandshakeStage::Established => {
                let _ = sink.close().await;
                return ConnectionExit::Error {
                    reason: format!("XMPP handshake timed out in stage {stage:?}"),
                    auth_failed: false,
                };
            }
            _ = ping_ticker.tick(), if stage == HandshakeStage::Established => {
                if let Err(err) = sink.send(Message::text(ping_iq_frame("ping"))).await {
                    let _ = on_established.send(false);
                    return ConnectionExit::Error {
                        reason: format!("XMPP ping write failed: {err}"),
                        auth_failed: false,
                    };
                }
            }
            _ = watchdog_ticker.tick(), if stage == HandshakeStage::Established => {
                if last_pong.elapsed() > params.ping_timeout {
                    warn!(account_id = params.account_id, "XMPP ping watchdog expired");
                    let _ = sink.close().await;
                    let _ = on_established.send(false);
                    return ConnectionExit::Error {
                        reason: format!("XMPP ping timeout after {:?}", params.ping_timeout),
                        auth_failed: false,
                    };
                }
            }
            cmd = cmd_rx.recv(), if stage == HandshakeStage::Established => {
                match cmd {
                    Some(XMPPCommand::SendStanza(stanza)) => {
                        if let Err(err) = sink.send(Message::text(stanza)).await {
                            let _ = on_established.send(false);
                            return ConnectionExit::Error {
                                reason: format!("XMPP stanza write failed: {err}"),
                                auth_failed: false,
                            };
                        }
                    }
                    Some(XMPPCommand::SendChat { to_jid, id, body }) => {
                        let frame = chat_message_frame(&to_jid, &id, &body);
                        if let Err(err) = sink.send(Message::text(frame)).await {
                            let _ = on_established.send(false);
                            return ConnectionExit::Error {
                                reason: format!("XMPP chat write failed: {err}"),
                                auth_failed: false,
                            };
                        }
                    }
                    Some(XMPPCommand::UpdatePresence { status, show, unavailable }) => {
                        let frame = presence_frame(status.as_deref(), show.as_deref(), unavailable);
                        if let Err(err) = sink.send(Message::text(frame)).await {
                            let _ = on_established.send(false);
                            return ConnectionExit::Error {
                                reason: format!("XMPP presence write failed: {err}"),
                                auth_failed: false,
                            };
                        }
                    }
                    Some(XMPPCommand::ForceReconnect) => {
                        let _ = sink.send(Message::text(close_frame())).await;
                        let _ = sink.close().await;
                        let _ = on_established.send(false);
                        return ConnectionExit::Clean(StreamExitReason::ForcedReconnect);
                    }
                    Some(XMPPCommand::Disconnect) | None => {
                        let _ = sink.send(Message::text(presence_frame(None, None, true))).await;
                        let _ = sink.send(Message::text(close_frame())).await;
                        let _ = sink.close().await;
                        let _ = on_established.send(false);
                        return ConnectionExit::Clean(StreamExitReason::ClientDisconnected);
                    }
                }
            }
            msg = stream.next() => {
                let frame = match msg {
                    Some(Ok(msg)) => msg,
                    Some(Err(err)) => {
                        let _ = on_established.send(false);
                        return ConnectionExit::Error {
                            reason: format!("XMPP WebSocket read failed: {err}"),
                            auth_failed: false,
                        };
                    }
                    None => {
                        let _ = on_established.send(false);
                        return ConnectionExit::Error {
                            reason: "XMPP WebSocket stream closed by remote host".to_owned(),
                            auth_failed: false,
                        };
                    }
                };

                let xml_text = match frame {
                    Message::Text(text) => text.to_string(),
                    Message::Binary(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                    Message::Ping(payload) => {
                        last_pong = Instant::now();
                        let _ = sink.send(Message::Pong(payload)).await;
                        continue;
                    }
                    Message::Pong(_) => {
                        last_pong = Instant::now();
                        continue;
                    }
                    Message::Close(_) => {
                        let _ = on_established.send(false);
                        return ConnectionExit::Clean(StreamExitReason::StreamClosed);
                    }
                    Message::Frame(_) => continue,
                };

                trace!(account_id = params.account_id, stanza = %xml_text, "XMPP IN");

                let parsed = match parse_incoming_stanza(&xml_text) {
                    Ok(stanza) => stanza,
                    Err(err) => {
                        on_event(XMPPEvent::StanzaError(err.to_string()));
                        continue;
                    }
                };

                match (&mut stage, parsed) {
                    (_, XMPPIncomingStanza::Close) => {
                        let _ = on_established.send(false);
                        return ConnectionExit::Clean(StreamExitReason::StreamClosed);
                    }
                    (_, XMPPIncomingStanza::StreamError { condition, text }) => {
                        let _ = on_established.send(false);
                        return ConnectionExit::Error {
                            reason: format!(
                                "XMPP stream:error ({}): {}",
                                condition.unwrap_or_else(|| "unknown".to_owned()),
                                text.unwrap_or_default()
                            ),
                            auth_failed: false,
                        };
                    }
                    (HandshakeStage::AwaitAuthFeatures, XMPPIncomingStanza::Open { .. }) => {}
                    (HandshakeStage::AwaitAuthFeatures, XMPPIncomingStanza::StreamFeatures { mechanisms, .. }) => {
                        if !mechanisms.is_empty() && !mechanisms.iter().any(|m| m == "PLAIN") {
                            return ConnectionExit::Error {
                                reason: format!("server did not advertise SASL PLAIN ({mechanisms:?})"),
                                auth_failed: true,
                            };
                        }
                        let sasl = sasl_plain_frame(params.account_id, params.access_token);
                        if let Err(err) = sink.send(Message::text(sasl)).await {
                            return ConnectionExit::Error {
                                reason: format!("failed to send SASL PLAIN stanza: {err}"),
                                auth_failed: false,
                            };
                        }
                        stage = HandshakeStage::AwaitSaslOutcome;
                    }
                    (HandshakeStage::AwaitSaslOutcome, XMPPIncomingStanza::SaslSuccess) => {
                        if let Err(err) = sink.send(Message::text(open_frame(params.domain))).await {
                            return ConnectionExit::Error {
                                reason: format!("failed to send post-SASL <open>: {err}"),
                                auth_failed: false,
                            };
                        }
                        stage = HandshakeStage::AwaitBindFeatures;
                    }
                    (HandshakeStage::AwaitSaslOutcome, XMPPIncomingStanza::SaslFailure { condition, text }) => {
                        return ConnectionExit::Error {
                            reason: format!(
                                "XMPP SASL authentication failed ({}): {}",
                                condition.unwrap_or_else(|| "not-authorized".to_owned()),
                                text.unwrap_or_default()
                            ),
                            auth_failed: true,
                        };
                    }
                    (HandshakeStage::AwaitBindFeatures, XMPPIncomingStanza::Open { .. }) => {}
                    (HandshakeStage::AwaitBindFeatures, XMPPIncomingStanza::StreamFeatures { has_bind, .. }) => {
                        if !has_bind {
                            debug!(account_id = params.account_id, "XMPP features omitted explicit <bind/>; sending bind_1 anyway");
                        }
                        let bind = bind_iq_frame("bind_1", params.resource);
                        if let Err(err) = sink.send(Message::text(bind)).await {
                            return ConnectionExit::Error {
                                reason: format!("failed to send bind IQ: {err}"),
                                auth_failed: false,
                            };
                        }
                        stage = HandshakeStage::AwaitBindResult;
                    }
                    (HandshakeStage::AwaitBindResult, XMPPIncomingStanza::IqResult { id, jid })
                        if id.as_deref() == Some("bind_1") =>
                    {
                        if let Err(err) = sink.send(Message::text(session_iq_frame("sess_1"))).await {
                            return ConnectionExit::Error {
                                reason: format!("failed to send session IQ: {err}"),
                                auth_failed: false,
                            };
                        }
                        if let Some(ref j) = jid {
                            debug!(account_id = params.account_id, jid = %j, "XMPP resource bound");
                        }
                        bound_jid = jid;
                        stage = HandshakeStage::AwaitSessionResult;
                    }
                    (HandshakeStage::AwaitBindResult | HandshakeStage::AwaitSessionResult, XMPPIncomingStanza::IqError { id, condition, text }) => {
                        return ConnectionExit::Error {
                            reason: format!(
                                "XMPP handshake IQ {:?} error ({}): {}",
                                id,
                                condition.unwrap_or_default(),
                                text.unwrap_or_default()
                            ),
                            auth_failed: false,
                        };
                    }
                    (HandshakeStage::AwaitSessionResult, XMPPIncomingStanza::IqResult { id, jid })
                        if id.as_deref() == Some("sess_1") =>
                    {
                        let presence = if let Some((status, show)) = &params.initial_presence {
                            presence_frame(status.as_deref(), show.as_deref(), false)
                        } else {
                            presence_frame(None, None, false)
                        };
                        if let Err(err) = sink.send(Message::text(presence)).await {
                            return ConnectionExit::Error {
                                reason: format!("XMPP presence write failed: {err}"),
                                auth_failed: false,
                            };
                        }
                        last_pong = Instant::now();
                        stage = HandshakeStage::Established;
                        let _ = on_established.send(true);
                        on_event(XMPPEvent::Connected {
                            jid: bound_jid.take().or(jid),
                            resource: params.resource.to_owned(),
                        });
                    }
                    (HandshakeStage::Established, XMPPIncomingStanza::IqResult { id, .. })
                        if id.as_deref() == Some("ping") =>
                    {
                        last_pong = Instant::now();
                    }
                    (HandshakeStage::Established, XMPPIncomingStanza::IqPing { id, from }) => {
                        last_pong = Instant::now();
                        let reply = pong_iq_frame(id.as_deref(), from.as_deref());
                        let _ = sink.send(Message::text(reply)).await;
                    }
                    (HandshakeStage::Established, XMPPIncomingStanza::Message { from, to, id, message_type, body }) => {
                        if let (Some(sender), Some(body_str)) = (from, body) {
                            if is_xmpp_admin_jid(&sender, params.domain) {
                                match parse_admin_message(&body_str) {
                                    Ok(admin_msg) => on_event(XMPPEvent::AdminMessage(admin_msg)),
                                    Err(err) => on_event(XMPPEvent::StanzaError(format!(
                                        "failed parsing xmpp-admin JSON: {err}"
                                    ))),
                                }
                            } else if message_type.as_deref() == Some("chat") || message_type.is_none() {
                                on_event(XMPPEvent::ChatMessage {
                                    from: sender,
                                    to,
                                    id,
                                    body: body_str,
                                });
                            }
                        }
                    }
                    (HandshakeStage::Established, XMPPIncomingStanza::Presence { from, presence_type, show, status, .. }) => {
                        if let Some(sender) = from {
                            on_event(XMPPEvent::RawPresence {
                                from: sender,
                                presence_type,
                                show,
                                status,
                            });
                        }
                    }
                    (_, other) => trace!(?other, "ignored unexpected XMPP stanza"),
                }
            }
        }
    }
}

/// RFC 7395 XMPP-over-WebSocket framing namespace.
pub const NS_FRAMING: &str = "urn:ietf:params:xml:ns:xmpp-framing";
pub const NS_SASL: &str = "urn:ietf:params:xml:ns:xmpp-sasl";
pub const NS_BIND: &str = "urn:ietf:params:xml:ns:xmpp-bind";
pub const NS_SESSION: &str = "urn:ietf:params:xml:ns:xmpp-session";
pub const NS_PING: &str = "urn:xmpp:ping";
pub const EPIC_XMPP_ADMIN_LOCALPART: &str = "xmpp-admin";

/// Parsed top-level XMPP stanza or framing element emitted by Epic's WebSocket gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XMPPIncomingStanza {
    Open {
        id: Option<String>,
        from: Option<String>,
    },
    Close,
    StreamFeatures {
        mechanisms: Vec<String>,
        has_bind: bool,
    },
    SaslSuccess,
    SaslFailure {
        condition: Option<String>,
        text: Option<String>,
    },
    StreamError {
        condition: Option<String>,
        text: Option<String>,
    },
    IqResult {
        id: Option<String>,
        jid: Option<String>,
    },
    IqError {
        id: Option<String>,
        condition: Option<String>,
        text: Option<String>,
    },
    IqPing {
        id: Option<String>,
        from: Option<String>,
    },
    Message {
        from: Option<String>,
        to: Option<String>,
        id: Option<String>,
        message_type: Option<String>,
        body: Option<String>,
    },
    Presence {
        from: Option<String>,
        to: Option<String>,
        presence_type: Option<String>,
        show: Option<String>,
        status: Option<String>,
    },
    Unknown(String),
}

/// Builds a Fortnite XMPP client resource string (`V2:Fortnite:<PLATFORM>::<32_HEX>`).
pub fn build_fortnite_resource(platform: Platform) -> String {
    let hex = uuid::Uuid::new_v4().simple().to_string().to_uppercase();
    format!("V2:Fortnite:{}::{hex}", platform.as_str())
}

/// Returns `true` when a JID belongs to Epic's `xmpp-admin` notification service.
pub fn is_xmpp_admin_jid(jid: &str, domain: &str) -> bool {
    let expected = format!("{EPIC_XMPP_ADMIN_LOCALPART}@{domain}");
    jid.eq_ignore_ascii_case(&expected) || jid.eq_ignore_ascii_case("xmpp-admin@prod.ol.epicgames.com")
}

pub fn open_frame(domain: &str) -> String {
    format!(
        "<open xmlns='{}' to='{}' version='1.0'/>",
        NS_FRAMING,
        escape(domain)
    )
}

pub fn close_frame() -> &'static str {
    "<close xmlns='urn:ietf:params:xml:ns:xmpp-framing'/>"
}

/// Builds a SASL PLAIN authentication stanza containing zeroized `\0{account_id}\0{access_token}`.
pub fn sasl_plain_frame(account_id: &str, access_token: &SecretString) -> String {
    let payload = Zeroizing::new(format!("\0{account_id}\0{}", access_token.expose_secret()));
    let encoded = Zeroizing::new(STANDARD.encode(payload.as_bytes()));
    format!(
        "<auth xmlns='{}' mechanism='PLAIN'>{}</auth>",
        NS_SASL,
        encoded.as_str()
    )
}

pub fn bind_iq_frame(id: &str, resource: &str) -> String {
    format!(
        "<iq type='set' id='{}'><bind xmlns='{}'><resource>{}</resource></bind></iq>",
        escape(id),
        NS_BIND,
        escape(resource)
    )
}

pub fn session_iq_frame(id: &str) -> String {
    format!(
        "<iq type='set' id='{}'><session xmlns='{}'/></iq>",
        escape(id),
        NS_SESSION
    )
}

pub fn ping_iq_frame(id: &str) -> String {
    format!(
        "<iq type='get' id='{}'><ping xmlns='{}'/></iq>",
        escape(id),
        NS_PING
    )
}

pub fn pong_iq_frame(id: Option<&str>, to: Option<&str>) -> String {
    let mut attrs = String::from("type='result'");
    if let Some(id) = id {
        attrs.push_str(&format!(" id='{}'", escape(id)));
    }
    if let Some(to) = to {
        attrs.push_str(&format!(" to='{}'", escape(to)));
    }
    format!("<iq {attrs}/>")
}

pub fn presence_frame(status: Option<&str>, show: Option<&str>, unavailable: bool) -> String {
    if unavailable {
        return match status {
            Some(s) => format!("<presence type='unavailable'><status>{}</status></presence>", escape(s)),
            None => "<presence type='unavailable'/>".to_owned(),
        };
    }

    let mut inner = String::new();
    if let Some(show_val) = show.filter(|s| !s.is_empty() && *s != "online") {
        inner.push_str(&format!("<show>{}</show>", escape(show_val)));
    }
    if let Some(status_val) = status.filter(|s| !s.is_empty()) {
        inner.push_str(&format!("<status>{}</status>", escape(status_val)));
    }
    if inner.is_empty() {
        "<presence/>".to_owned()
    } else {
        format!("<presence>{inner}</presence>")
    }
}

pub fn chat_message_frame(to_jid: &str, id: &str, body: &str) -> String {
    format!(
        "<message to='{}' id='{}' type='chat'><body>{}</body></message>",
        escape(to_jid),
        escape(id),
        escape(body)
    )
}

fn attr_value(elem: &BytesStart<'_>, name: &[u8]) -> Result<Option<String>, RustfnError> {
    for attr_res in elem.attributes() {
        let attr = attr_res.map_err(|e| RustfnError::Protocol(format!("XML attr parse error: {e}")))?;
        if attr.key.as_ref() == name {
            let val = attr
                .unescape_value()
                .map_err(|e| RustfnError::Protocol(format!("XML attr unescape error: {e}")))?;
            return Ok(Some(val.into_owned()));
        }
    }
    Ok(None)
}

/// Parses an incoming RFC 7395 XMPP WebSocket text frame using streaming `quick-xml` events.
pub fn parse_incoming_stanza(raw: &str) -> Result<XMPPIncomingStanza, RustfnError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(XMPPIncomingStanza::Unknown(String::new()));
    }

    let mut reader = Reader::from_str(trimmed);
    reader.config_mut().trim_text(false);

    let mut root_qname: Option<String> = None;
    let mut root_local: Option<String> = None;
    let mut root_from: Option<String> = None;
    let mut root_to: Option<String> = None;
    let mut root_id: Option<String> = None;
    let mut root_type: Option<String> = None;

    let mut depth: usize = 0;
    let mut current_child: Option<String> = None;
    let mut text_buf = String::new();

    let mut mechanisms = Vec::new();
    let mut has_bind = false;
    let mut has_ping_child = false;
    let mut jid: Option<String> = None;
    let mut body: Option<String> = None;
    let mut status: Option<String> = None;
    let mut show: Option<String> = None;
    let mut error_condition: Option<String> = None;
    let mut error_text: Option<String> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref elem)) => {
                let qname = String::from_utf8_lossy(elem.name().as_ref()).into_owned();
                let local = String::from_utf8_lossy(elem.local_name().as_ref()).into_owned();
                if depth == 0 {
                    root_qname = Some(qname);
                    root_local = Some(local);
                    root_from = attr_value(elem, b"from")?;
                    root_to = attr_value(elem, b"to")?;
                    root_id = attr_value(elem, b"id")?;
                    root_type = attr_value(elem, b"type")?;
                } else {
                    if local == "bind" {
                        has_bind = true;
                    } else if local == "ping" {
                        has_ping_child = true;
                    }
                    if local != "text"
                        && error_condition.is_none()
                        && ((matches!(root_local.as_deref(), Some("failure") | Some("error"))
                            && depth == 1)
                            || (root_local.as_deref() == Some("iq")
                                && root_type.as_deref() == Some("error")
                                && depth == 2))
                    {
                        error_condition = Some(local.clone());
                    }
                    current_child = Some(local);
                    text_buf.clear();
                }
                depth += 1;
            }
            Ok(Event::Empty(ref elem)) => {
                let qname = String::from_utf8_lossy(elem.name().as_ref()).into_owned();
                let local = String::from_utf8_lossy(elem.local_name().as_ref()).into_owned();
                if depth == 0 {
                    root_qname = Some(qname);
                    root_local = Some(local);
                    root_from = attr_value(elem, b"from")?;
                    root_to = attr_value(elem, b"to")?;
                    root_id = attr_value(elem, b"id")?;
                    root_type = attr_value(elem, b"type")?;
                    break;
                } else {
                    if local == "bind" {
                        has_bind = true;
                    } else if local == "ping" {
                        has_ping_child = true;
                    } else if local != "text"
                        && error_condition.is_none()
                        && ((matches!(root_local.as_deref(), Some("failure") | Some("error"))
                            && depth == 1)
                            || (root_local.as_deref() == Some("iq")
                                && root_type.as_deref() == Some("error")
                                && depth == 2))
                    {
                        error_condition = Some(local);
                    }
                }
            }
            Ok(Event::Text(ref t)) => {
                if depth > 0 {
                    let unescaped = t
                        .unescape()
                        .map_err(|e| RustfnError::Protocol(format!("XML text unescape error: {e}")))?;
                    text_buf.push_str(unescaped.as_ref());
                }
            }
            Ok(Event::CData(ref c)) => {
                if depth > 0 {
                    text_buf.push_str(&String::from_utf8_lossy(c.as_ref()));
                }
            }
            Ok(Event::End(ref elem)) => {
                let local = String::from_utf8_lossy(elem.local_name().as_ref()).into_owned();
                if depth > 1 && current_child.as_deref() == Some(local.as_str()) {
                    let collected = std::mem::take(&mut text_buf);
                    match local.as_str() {
                        "mechanism" if !collected.trim().is_empty() => {
                            mechanisms.push(collected.trim().to_owned());
                        }
                        "jid" => jid = Some(collected),
                        "body" => body = Some(collected),
                        "status" => status = Some(collected),
                        "show" => show = Some(collected),
                        "text" if !collected.is_empty() => error_text = Some(collected),
                        _ => {}
                    }
                    current_child = None;
                }
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    break;
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(RustfnError::Protocol(format!("invalid XMPP XML stanza: {e}")));
            }
            Ok(_) => {}
        }
    }

    let Some(local) = root_local else {
        return Ok(XMPPIncomingStanza::Unknown(trimmed.to_owned()));
    };

    match local.as_str() {
        "open" => Ok(XMPPIncomingStanza::Open {
            id: root_id,
            from: root_from,
        }),
        "close" => Ok(XMPPIncomingStanza::Close),
        "features" => Ok(XMPPIncomingStanza::StreamFeatures {
            mechanisms,
            has_bind,
        }),
        "success" => Ok(XMPPIncomingStanza::SaslSuccess),
        "failure" => Ok(XMPPIncomingStanza::SaslFailure {
            condition: error_condition,
            text: error_text,
        }),
        "error" if root_qname.as_deref() == Some("stream:error") => {
            Ok(XMPPIncomingStanza::StreamError {
                condition: error_condition,
                text: error_text,
            })
        }
        "iq" => match root_type.as_deref() {
            Some("result") => Ok(XMPPIncomingStanza::IqResult { id: root_id, jid }),
            Some("error") => Ok(XMPPIncomingStanza::IqError {
                id: root_id,
                condition: error_condition,
                text: error_text,
            }),
            Some("get") if has_ping_child => Ok(XMPPIncomingStanza::IqPing {
                id: root_id,
                from: root_from,
            }),
            _ => Ok(XMPPIncomingStanza::Unknown(trimmed.to_owned())),
        },
        "message" => Ok(XMPPIncomingStanza::Message {
            from: root_from,
            to: root_to,
            id: root_id,
            message_type: root_type,
            body,
        }),
        "presence" => Ok(XMPPIncomingStanza::Presence {
            from: root_from,
            to: root_to,
            presence_type: root_type,
            show,
            status,
        }),
        _ => Ok(XMPPIncomingStanza::Unknown(trimmed.to_owned())),
    }
}
