use crate::stomp::message::{EOSConnectNotification, parse_connect_notification};
use futures_util::{SinkExt, StreamExt};
use rand_core::{OsRng, RngCore};
use crate::http::{AuthSessionSlot, TokenProvider};
use crate::protocol::{
    AccountId, DeploymentId, EOS_EAS_DEPLOYMENT_ID, ProductUserId, RustfnError, ServiceEndpoints,
};
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use std::collections::{HashSet, VecDeque};
use std::fmt;
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

/// Configuration for [`STOMP`] EOS Connect STOMP 1.2 WebSocket sessions.
#[derive(Debug, Clone)]
pub struct STOMPConfig {
    pub wss_url: String,
    pub deployment_id: DeploymentId,
    pub ws_subprotocol: Option<String>,
    pub subscribe_puid: bool,
    pub default_heartbeat: Duration,
    pub connection_timeout: Duration,
    pub max_connection_retries: u32,
    pub retry_delay: Duration,
    pub dedupe_capacity: usize,
    pub event_channel_capacity: usize,
}

impl Default for STOMPConfig {
    fn default() -> Self {
        let endpoints = ServiceEndpoints::default();
        Self {
            wss_url: endpoints.eos_stomp_wss.into_owned(),
            deployment_id: DeploymentId::from_raw(EOS_EAS_DEPLOYMENT_ID),
            ws_subprotocol: None,
            subscribe_puid: true,
            default_heartbeat: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(60),
            max_connection_retries: 5,
            retry_delay: Duration::from_secs(5),
            dedupe_capacity: DEFAULT_DEDUPE_CAPACITY,
            event_channel_capacity: 512,
        }
    }
}

/// Active EOS Connect connection identifiers emitted once `core.connect.v1.connected` arrives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct STOMPConnectionIds {
    pub connection_id: String,
    pub public_connection_id: String,
    pub private_connection_id: String,
    pub public_subscription_id: String,
    pub private_subscription_id: String,
    pub puid_subscription_id: Option<String>,
}

/// Asynchronous event notifications emitted by [`STOMP`].
#[derive(Debug, Clone)]
pub enum STOMPEvent {
    Connected(STOMPConnectionIds),
    Disconnected {
        will_reconnect: bool,
        reason: String,
    },
    ReconnectExhausted {
        attempts: u32,
        last_error: String,
    },
    Receipt {
        receipt_id: String,
    },
    Notification(EOSConnectNotification),
    ErrorFrame {
        status_code: Option<i64>,
        message: String,
        raw: Value,
    },
    ParseError(String),
}

enum STOMPCommandMsg {
    EnsurePuidSubscription(ProductUserId),
    SendFrame(STOMPFrame),
    ForceReconnect,
    Disconnect,
}

enum ConnectionExit {
    Cancelled,
    ClientDisconnected,
    ForcedReconnect,
    StreamClosed {
        established: bool,
    },
    Error {
        reason: String,
        auth_failed: bool,
        established: bool,
    },
}

fn next_subscription_hex() -> String {
    format!("{:08x}", OsRng.next_u32())
}

fn build_subscribe_frame(
    id: &str,
    destination: &str,
    token: &SecretString,
    temporary_internal: bool,
    receipt: Option<String>,
) -> STOMPFrame {
    let mut frame = STOMPFrame::new(STOMPCommand::Subscribe)
        .with_header("id", id)
        .with_header("destination", destination);
    if temporary_internal {
        frame = frame.with_header("ec-coord-temporary-subscription", "parties-internal");
    }
    if let Some(rcp) = receipt {
        frame = frame.with_header("receipt", rcp);
    }
    frame
        .with_header("authorization", format!("Bearer {}", token.expose_secret()))
        .with_header("ec-coord-accept-language", "en")
}

fn negotiate_heartbeat_delay(headers_heartbeat: Option<&str>, fallback: Duration) -> Duration {
    let min_ms = fallback.as_millis() as u64;
    let Some(raw) = headers_heartbeat else {
        return fallback;
    };
    let server_read_ms = raw
        .split(',')
        .nth(1)
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0);
    if server_read_ms == 0 {
        fallback
    } else {
        Duration::from_millis(server_read_ms.max(min_ms))
    }
}

/// Native EOS Connect STOMP 1.2 WebSocket driver with dual EAS subscriptions, optional PUID
/// matchmaking subscription, `\n` heartbeat emitter, 100-entry dedupe cache, and `connectionId` watch.
pub struct STOMP {
    config: STOMPConfig,
    account_id: AccountId,
    product_user_id: Arc<Mutex<Option<ProductUserId>>>,
    token_provider: Arc<dyn TokenProvider>,
    event_tx: broadcast::Sender<Arc<STOMPEvent>>,
    connection_ids_tx: watch::Sender<Option<STOMPConnectionIds>>,
    connection_ids_rx: watch::Receiver<Option<STOMPConnectionIds>>,
    cmd_tx: Mutex<Option<mpsc::Sender<STOMPCommandMsg>>>,
    task_handle: Mutex<Option<JoinHandle<()>>>,
    cancel_token: Mutex<Option<CancellationToken>>,
}

impl std::fmt::Debug for STOMP {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("STOMP")
            .field("account_id", &self.account_id)
            .field("wss_url", &self.config.wss_url)
            .field("connection_ids", &*self.connection_ids_rx.borrow())
            .finish()
    }
}

impl STOMP {
    pub fn new(
        config: STOMPConfig,
        account_id: AccountId,
        product_user_id: Option<ProductUserId>,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(config.event_channel_capacity.max(32));
        let (connection_ids_tx, connection_ids_rx) = watch::channel(None);

        Self {
            config,
            account_id,
            product_user_id: Arc::new(Mutex::new(product_user_id)),
            token_provider,
            event_tx,
            connection_ids_tx,
            connection_ids_rx,
            cmd_tx: Mutex::new(None),
            task_handle: Mutex::new(None),
            cancel_token: Mutex::new(None),
        }
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<Arc<STOMPEvent>> {
        self.event_tx.subscribe()
    }

    pub fn connection_ids(&self) -> Option<STOMPConnectionIds> {
        self.connection_ids_rx.borrow().clone()
    }

    pub fn watch_connection_ids(&self) -> watch::Receiver<Option<STOMPConnectionIds>> {
        self.connection_ids_rx.clone()
    }

    pub async fn wait_until_connected(
        &self,
        wait_timeout: Duration,
    ) -> Result<STOMPConnectionIds, RustfnError> {
        let mut rx = self.connection_ids_rx.clone();
        if let Some(ids) = rx.borrow_and_update().clone() {
            return Ok(ids);
        }
        timeout(wait_timeout, async move {
            loop {
                if rx.changed().await.is_err() {
                    return Err(RustfnError::Transport(
                        "STOMP connection watcher closed".to_owned(),
                    ));
                }
                if let Some(ids) = rx.borrow().clone() {
                    return Ok(ids);
                }
            }
        })
        .await
        .map_err(|_| {
            RustfnError::Timeout(format!("timed out waiting {wait_timeout:?} for STOMP connection ID"))
        })?
    }

    /// Connects to EOS Connect STOMP and blocks until `core.connect.v1.connected` provides
    /// `public_connection_id` and `private_connection_id`.
    pub async fn connect(
        &self,
        parent_cancel: CancellationToken,
    ) -> Result<STOMPConnectionIds, RustfnError> {
        self.disconnect().await;

        let cancel = parent_cancel.child_token();
        let (cmd_tx, mut cmd_rx) = mpsc::channel(64);
        *self.cmd_tx.lock().await = Some(cmd_tx);
        *self.cancel_token.lock().await = Some(cancel.clone());

        let config = self.config.clone();
        let account_id = self.account_id.clone();
        let product_user_id = Arc::clone(&self.product_user_id);
        let token_provider = Arc::clone(&self.token_provider);
        let event_tx = self.event_tx.clone();
        let ids_tx = self.connection_ids_tx.clone();
        let (first_ready_tx, first_ready_rx) =
            tokio::sync::oneshot::channel::<Result<STOMPConnectionIds, String>>();
        let mut first_ready_tx = Some(first_ready_tx);

        let handle = tokio::spawn(async move {
            let mut dedupe = STOMPMessageDedupe::new(config.dedupe_capacity);
            let mut retry_count: u32 = 0;

            loop {
                if cancel.is_cancelled() {
                    break;
                }

                let eos_token = match token_provider.bearer_token(AuthSessionSlot::FortniteEos).await {
                    Ok(tok) => tok,
                    Err(err) => {
                        let msg = err.to_string();
                        if let Some(tx) = first_ready_tx.take() {
                            let _ = tx.send(Err(msg));
                            break;
                        }
                        if retry_count < config.max_connection_retries {
                            retry_count += 1;
                            let _ = event_tx.send(Arc::new(STOMPEvent::Disconnected {
                                will_reconnect: true,
                                reason: msg,
                            }));
                            tokio::select! {
                                _ = cancel.cancelled() => break,
                                _ = tokio::time::sleep(config.retry_delay) => continue,
                            }
                        } else {
                            let _ = event_tx.send(Arc::new(STOMPEvent::ReconnectExhausted {
                                attempts: retry_count,
                                last_error: msg,
                            }));
                            break;
                        }
                    }
                };

                let puid_snapshot = product_user_id.lock().await.clone();
                let mut first_tx_for_conn = first_ready_tx.take();

                let exit = run_single_stomp_connection(
                    &config,
                    &account_id,
                    puid_snapshot,
                    &eos_token,
                    &mut dedupe,
                    &mut cmd_rx,
                    &ids_tx,
                    &event_tx,
                    &mut first_tx_for_conn,
                    &cancel,
                )
                .await;

                let _ = ids_tx.send(None);

                match exit {
                    ConnectionExit::Cancelled | ConnectionExit::ClientDisconnected => {
                        let _ = event_tx.send(Arc::new(STOMPEvent::Disconnected {
                            will_reconnect: false,
                            reason: "client disconnected".to_owned(),
                        }));
                        break;
                    }
                    ConnectionExit::ForcedReconnect => {
                        retry_count = 0;
                        let _ = event_tx.send(Arc::new(STOMPEvent::Disconnected {
                            will_reconnect: true,
                            reason: "forced reconnect".to_owned(),
                        }));
                        continue;
                    }
                    ConnectionExit::StreamClosed { established } => {
                        if established {
                            retry_count = 0;
                        }
                        if let Some(tx) = first_tx_for_conn.take() {
                            let _ = tx.send(Err("STOMP connection closed before establishment".to_owned()));
                            break;
                        }
                        if retry_count < config.max_connection_retries {
                            retry_count += 1;
                            let _ = event_tx.send(Arc::new(STOMPEvent::Disconnected {
                                will_reconnect: true,
                                reason: "STOMP stream closed".to_owned(),
                            }));
                            tokio::select! {
                                _ = cancel.cancelled() => break,
                                _ = tokio::time::sleep(config.retry_delay) => continue,
                            }
                        } else {
                            let _ = event_tx.send(Arc::new(STOMPEvent::ReconnectExhausted {
                                attempts: retry_count,
                                last_error: "STOMP stream closed".to_owned(),
                            }));
                            break;
                        }
                    }
                    ConnectionExit::Error {
                        reason,
                        auth_failed,
                        established,
                    } => {
                        if established {
                            retry_count = 0;
                        }
                        if auth_failed {
                            let _ = token_provider
                                .refresh_on_invalid_token(
                                    AuthSessionSlot::FortniteEos,
                                    eos_token.expose_secret(),
                                )
                                .await;
                        }
                        if let Some(tx) = first_tx_for_conn.take() {
                            let _ = tx.send(Err(reason));
                            break;
                        }
                        if retry_count < config.max_connection_retries {
                            retry_count += 1;
                            info!(
                                account_id = %account_id,
                                retry = retry_count,
                                max_retries = config.max_connection_retries,
                                reason = %reason,
                                "STOMP disconnected; scheduling reconnect"
                            );
                            let _ = event_tx.send(Arc::new(STOMPEvent::Disconnected {
                                will_reconnect: true,
                                reason,
                            }));
                            tokio::select! {
                                _ = cancel.cancelled() => break,
                                _ = tokio::time::sleep(config.retry_delay) => continue,
                            }
                        } else {
                            warn!(
                                account_id = %account_id,
                                max_retries = config.max_connection_retries,
                                reason = %reason,
                                "STOMP reconnect retries exhausted"
                            );
                            let _ = event_tx.send(Arc::new(STOMPEvent::ReconnectExhausted {
                                attempts: retry_count,
                                last_error: reason,
                            }));
                            break;
                        }
                    }
                }
            }
        });

        *self.task_handle.lock().await = Some(handle);

        match first_ready_rx.await {
            Ok(Ok(ids)) => Ok(ids),
            Ok(Err(err)) => Err(RustfnError::Transport(err)),
            Err(_) => Err(RustfnError::Transport(
                "STOMP task exited before receiving core.connect.v1.connected".to_owned(),
            )),
        }
    }

    pub async fn ensure_puid_subscription(&self, puid: ProductUserId) -> Result<(), RustfnError> {
        *self.product_user_id.lock().await = Some(puid.clone());
        let tx = self.cmd_tx.lock().await.clone().ok_or_else(|| {
            RustfnError::Transport("STOMP client is not connected".to_owned())
        })?;
        tx.send(STOMPCommandMsg::EnsurePuidSubscription(puid))
            .await
            .map_err(|_| RustfnError::Transport("STOMP command channel closed".to_owned()))
    }

    /// Sets the `ProductUserId` before [`Self::connect`] is called so the initial subscription binds immediately.
    pub async fn set_product_user_id(&self, puid: Option<ProductUserId>) {
        *self.product_user_id.lock().await = puid;
    }

    pub async fn send_frame(&self, frame: STOMPFrame) -> Result<(), RustfnError> {
        let tx = self.cmd_tx.lock().await.clone().ok_or_else(|| {
            RustfnError::Transport("STOMP client is not connected".to_owned())
        })?;
        tx.send(STOMPCommandMsg::SendFrame(frame))
            .await
            .map_err(|_| RustfnError::Transport("STOMP command channel closed".to_owned()))
    }

    pub async fn force_reconnect(&self) -> Result<(), RustfnError> {
        let tx = self.cmd_tx.lock().await.clone().ok_or_else(|| {
            RustfnError::Transport("STOMP client is not connected".to_owned())
        })?;
        tx.send(STOMPCommandMsg::ForceReconnect)
            .await
            .map_err(|_| RustfnError::Transport("STOMP command channel closed".to_owned()))
    }

    pub async fn disconnect(&self) {
        if let Some(tx) = self.cmd_tx.lock().await.take() {
            let _ = tx.send(STOMPCommandMsg::Disconnect).await;
        }
        if let Some(cancel) = self.cancel_token.lock().await.take() {
            cancel.cancel();
        }
        if let Some(handle) = self.task_handle.lock().await.take() {
            let _ = handle.await;
        }
        let _ = self.connection_ids_tx.send(None);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_single_stomp_connection(
    config: &STOMPConfig,
    account_id: &AccountId,
    puid_initial: Option<ProductUserId>,
    eos_token: &SecretString,
    dedupe: &mut STOMPMessageDedupe,
    cmd_rx: &mut mpsc::Receiver<STOMPCommandMsg>,
    ids_tx: &watch::Sender<Option<STOMPConnectionIds>>,
    event_tx: &broadcast::Sender<Arc<STOMPEvent>>,
    first_ready_tx: &mut Option<tokio::sync::oneshot::Sender<Result<STOMPConnectionIds, String>>>,
    cancel: &CancellationToken,
) -> ConnectionExit {
    let mut request = match config.wss_url.as_str().into_client_request() {
        Ok(req) => req,
        Err(err) => {
            return ConnectionExit::Error {
                reason: format!("invalid STOMP WebSocket URL `{}`: {err}", config.wss_url),
                auth_failed: false,
                established: false,
            };
        }
    };

    let bearer = format!("Bearer {}", eos_token.expose_secret());
    let auth_header = match HeaderValue::from_str(&bearer) {
        Ok(v) => v,
        Err(err) => {
            return ConnectionExit::Error {
                reason: format!("invalid STOMP bearer header: {err}"),
                auth_failed: true,
                established: false,
            };
        }
    };
    request.headers_mut().insert("Authorization", auth_header);
    if let Some(ref proto) = config.ws_subprotocol {
        if let Ok(proto_val) = HeaderValue::from_str(proto) {
            request.headers_mut().insert("Sec-WebSocket-Protocol", proto_val);
        }
    }
    request
        .headers_mut()
        .insert("Epic-Connect-Protocol", HeaderValue::from_static("stomp"));
    request
        .headers_mut()
        .insert("Epic-Connect-Device-Id", HeaderValue::from_static(" "));

    let ws = match timeout(config.connection_timeout, connect_async(request)).await {
        Ok(Ok((stream, _resp))) => stream,
        Ok(Err(tokio_tungstenite::tungstenite::Error::Http(resp))) => {
            let status = resp.status();
            return ConnectionExit::Error {
                reason: format!("STOMP HTTP upgrade rejected: {status}"),
                auth_failed: status.as_u16() == 401 || status.as_u16() == 403,
                established: false,
            };
        }
        Ok(Err(err)) => {
            return ConnectionExit::Error {
                reason: format!("STOMP WebSocket connect error: {err}"),
                auth_failed: false,
                established: false,
            };
        }
        Err(_) => {
            return ConnectionExit::Error {
                reason: format!("STOMP connection timed out after {:?}", config.connection_timeout),
                auth_failed: false,
                established: false,
            };
        }
    };

    let (mut sink, mut stream) = ws.split();

    let hb_ms = config.default_heartbeat.as_millis() as u64;
    let connect_frame = STOMPFrame::new(STOMPCommand::Connect)
        .with_header("accept-version", "1.0,1.1,1.2")
        .with_header("heart-beat", format!("{hb_ms},0"))
        .with_header("authorization", bearer);

    if let Err(err) = sink.send(Message::text(connect_frame.encode())).await {
        return ConnectionExit::Error {
            reason: format!("failed to send STOMP CONNECT frame: {err}"),
            auth_failed: false,
            established: false,
        };
    }

    let startup_deadline = Instant::now() + config.connection_timeout;
    let mut established = false;
    let mut heartbeat_active = false;
    let mut heartbeat_ticker = interval_at(
        Instant::now() + config.default_heartbeat,
        config.default_heartbeat,
    );
    heartbeat_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut public_sub_id: Option<String> = None;
    let mut private_sub_id: Option<String> = None;
    let mut puid_sub_id: Option<String> = None;
    let mut stomp_session_id: Option<String> = None;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                let disconnect = STOMPFrame::new(STOMPCommand::Disconnect).encode();
                let _ = sink.send(Message::text(disconnect)).await;
                let _ = sink.close().await;
                return ConnectionExit::Cancelled;
            }
            _ = tokio::time::sleep_until(startup_deadline), if !established => {
                let _ = sink.close().await;
                return ConnectionExit::Error {
                    reason: format!("STOMP startup handshake timed out after {:?}", config.connection_timeout),
                    auth_failed: false,
                    established: false,
                };
            }
            _ = heartbeat_ticker.tick(), if heartbeat_active => {
                if let Err(err) = sink.send(Message::text("\n")).await {
                    return ConnectionExit::Error {
                        reason: format!("STOMP heartbeat write failed: {err}"),
                        auth_failed: false,
                        established,
                    };
                }
            }
            cmd = cmd_rx.recv(), if heartbeat_active => {
                match cmd {
                    Some(STOMPCommandMsg::EnsurePuidSubscription(puid)) => {
                        if let Some(old_sub) = puid_sub_id.take() {
                            let unsub = STOMPFrame::new(STOMPCommand::Unsubscribe)
                                .with_header("id", old_sub);
                            let _ = sink.send(Message::text(unsub.encode())).await;
                        }
                        let sub_id = format!("sub-eosu-{}", next_subscription_hex());
                        let dest = format!(
                            "deploymentId/{}/productUserId/{}",
                            config.deployment_id, puid
                        );
                        let receipt = stomp_session_id.as_ref().map(|s| format!("sub-2-{s}"));
                        let frame = build_subscribe_frame(&sub_id, &dest, eos_token, false, receipt);
                        if let Err(err) = sink.send(Message::text(frame.encode())).await {
                            return ConnectionExit::Error {
                                reason: format!("STOMP PUID subscribe write failed: {err}"),
                                auth_failed: false,
                                established,
                            };
                        }
                        puid_sub_id = Some(sub_id);
                    }
                    Some(STOMPCommandMsg::SendFrame(frame)) => {
                        if let Err(err) = sink.send(Message::text(frame.encode())).await {
                            return ConnectionExit::Error {
                                reason: format!("STOMP frame write failed: {err}"),
                                auth_failed: false,
                                established,
                            };
                        }
                    }
                    Some(STOMPCommandMsg::ForceReconnect) => {
                        let _ = sink.close().await;
                        return ConnectionExit::ForcedReconnect;
                    }
                    Some(STOMPCommandMsg::Disconnect) | None => {
                        let disconnect = STOMPFrame::new(STOMPCommand::Disconnect).encode();
                        let _ = sink.send(Message::text(disconnect)).await;
                        let _ = sink.close().await;
                        return ConnectionExit::ClientDisconnected;
                    }
                }
            }
            frame = stream.next() => {
                let raw_text = match frame {
                    Some(Ok(Message::Text(text))) => text.to_string(),
                    Some(Ok(Message::Binary(bytes))) => String::from_utf8_lossy(&bytes).into_owned(),
                    Some(Ok(Message::Ping(p))) => {
                        let _ = sink.send(Message::Pong(p)).await;
                        continue;
                    }
                    Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => continue,
                    Some(Ok(Message::Close(_))) | None => {
                        return ConnectionExit::StreamClosed { established };
                    }
                    Some(Err(err)) => {
                        return ConnectionExit::Error {
                            reason: format!("STOMP WebSocket read failed: {err}"),
                            auth_failed: false,
                            established,
                        };
                    }
                };

                trace!(account_id = %account_id, frame = %raw_text, "STOMP IN");

                let frame = match STOMPFrame::parse(&raw_text) {
                    Ok(Some(frame)) => frame,
                    Ok(None) => continue,
                    Err(err) => {
                        let _ = event_tx.send(Arc::new(STOMPEvent::ParseError(err.to_string())));
                        continue;
                    }
                };

                match frame.command {
                    STOMPCommand::Connected if !heartbeat_active => {
                        let delay = negotiate_heartbeat_delay(
                            frame.header("heart-beat"),
                            config.default_heartbeat,
                        );
                        heartbeat_ticker = interval_at(Instant::now() + delay, delay);
                        heartbeat_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
                        heartbeat_active = true;

                        let sess_id = frame.header("session").map(ToOwned::to_owned);
                        stomp_session_id = sess_id.clone();

                        let suffix = next_subscription_hex();
                        let pub_id = format!("sub-eas-{suffix}");
                        let priv_id = format!("sub-eas-private-{suffix}");
                        let dest = format!(
                            "deploymentId/{}/epicAccountId/{}",
                            config.deployment_id, account_id
                        );

                        let sub_public = build_subscribe_frame(
                            &pub_id,
                            &dest,
                            eos_token,
                            false,
                            sess_id.as_ref().map(|s| format!("sub-0-{s}")),
                        );
                        if let Err(err) = sink.send(Message::text(sub_public.encode())).await {
                            return ConnectionExit::Error {
                                reason: format!("STOMP public subscribe write failed: {err}"),
                                auth_failed: false,
                                established,
                            };
                        }

                        let sub_private = build_subscribe_frame(
                            &priv_id,
                            &dest,
                            eos_token,
                            true,
                            sess_id.as_ref().map(|s| format!("sub-1-{s}")),
                        );
                        if let Err(err) = sink.send(Message::text(sub_private.encode())).await {
                            return ConnectionExit::Error {
                                reason: format!("STOMP private subscribe write failed: {err}"),
                                auth_failed: false,
                                established,
                            };
                        }

                        public_sub_id = Some(pub_id);
                        private_sub_id = Some(priv_id);

                        if config.subscribe_puid {
                            if let Some(ref puid) = puid_initial {
                                let puid_id = format!("sub-eosu-{}", next_subscription_hex());
                                let puid_dest = format!(
                                    "deploymentId/{}/productUserId/{}",
                                    config.deployment_id, puid
                                );
                                let sub_puid = build_subscribe_frame(
                                    &puid_id,
                                    &puid_dest,
                                    eos_token,
                                    false,
                                    sess_id.as_ref().map(|s| format!("sub-2-{s}")),
                                );
                                if sink.send(Message::text(sub_puid.encode())).await.is_ok() {
                                    puid_sub_id = Some(puid_id);
                                }
                            }
                        }
                    }
                    STOMPCommand::Receipt => {
                        if let Some(receipt_id) = frame.header("receipt-id") {
                            let _ = event_tx.send(Arc::new(STOMPEvent::Receipt {
                                receipt_id: receipt_id.to_owned(),
                            }));
                        }
                    }
                    STOMPCommand::Error => {
                        let parsed: Value = serde_json::from_str(&frame.body)
                            .unwrap_or_else(|_| Value::String(frame.body.clone()));
                        let status_code = parsed.get("statusCode").and_then(|v| v.as_i64());
                        let message = parsed
                            .get("message")
                            .and_then(|v| v.as_str())
                            .or_else(|| frame.header("message"))
                            .unwrap_or("STOMP error frame")
                            .to_owned();
                        let auth_failed = status_code == Some(4019);

                        let _ = event_tx.send(Arc::new(STOMPEvent::ErrorFrame {
                            status_code,
                            message: message.clone(),
                            raw: parsed,
                        }));
                        let _ = sink.close().await;
                        return ConnectionExit::Error {
                            reason: if auth_failed {
                                "STOMP authentication token expired (4019)".to_owned()
                            } else {
                                message
                            },
                            auth_failed,
                            established,
                        };
                    }
                    STOMPCommand::Message => {
                        if frame.body.is_empty() {
                            continue;
                        }
                        let dest = frame.header("destination");
                        let (msg_id, notification) =
                            match parse_connect_notification(&frame.body, dest) {
                                Ok(res) => res,
                                Err(err) => {
                                    let _ = event_tx
                                        .send(Arc::new(STOMPEvent::ParseError(err.to_string())));
                                    continue;
                                }
                            };

                        if let Some(ref msg_id) = msg_id {
                            if !dedupe.mark_if_new(msg_id.clone()) {
                                trace!(message_id = %msg_id, "dropped duplicate STOMP notification");
                                continue;
                            }
                        }

                        match notification {
                            EOSConnectNotification::CoreConnected { connection_id, .. } => {
                                let (Some(pub_sub), Some(priv_sub)) =
                                    (public_sub_id.clone(), private_sub_id.clone())
                                else {
                                    return ConnectionExit::Error {
                                        reason: "received core.connect.v1.connected before EAS subscriptions".to_owned(),
                                        auth_failed: false,
                                        established,
                                    };
                                };
                                let ids = STOMPConnectionIds {
                                    public_connection_id: format!("{connection_id}#{pub_sub}"),
                                    private_connection_id: format!("{connection_id}#{priv_sub}"),
                                    connection_id,
                                    public_subscription_id: pub_sub,
                                    private_subscription_id: priv_sub,
                                    puid_subscription_id: puid_sub_id.clone(),
                                };
                                established = true;
                                debug!(account_id = %account_id, connection_id = %ids.connection_id, "STOMP connected");
                                let _ = ids_tx.send(Some(ids.clone()));
                                if let Some(tx) = first_ready_tx.take() {
                                    let _ = tx.send(Ok(ids.clone()));
                                }
                                let _ = event_tx.send(Arc::new(STOMPEvent::Connected(ids)));
                            }
                            EOSConnectNotification::CoreConnectFailed {
                                status_code,
                                message,
                                ..
                            } => {
                                let auth_failed = status_code == 4019 || status_code == 401 || status_code == 403;
                                let _ = sink.close().await;
                                return ConnectionExit::Error {
                                    reason: format!("EOS Connect failed ({status_code}): {message}"),
                                    auth_failed,
                                    established,
                                };
                            }
                            other => {
                                let _ = event_tx.send(Arc::new(STOMPEvent::Notification(other)));
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

/// STOMP 1.2 frame command verbs used on EOS Connect (`wss://connect.epicgames.dev/v2`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum STOMPCommand {
    Connect,
    Connected,
    Subscribe,
    Unsubscribe,
    Send,
    Ack,
    Nack,
    Disconnect,
    Message,
    Receipt,
    Error,
    Other(String),
}

impl STOMPCommand {
    pub fn from_wire(raw: &str) -> Self {
        match raw.trim() {
            "CONNECT" => Self::Connect,
            "CONNECTED" => Self::Connected,
            "SUBSCRIBE" => Self::Subscribe,
            "UNSUBSCRIBE" => Self::Unsubscribe,
            "SEND" => Self::Send,
            "ACK" => Self::Ack,
            "NACK" => Self::Nack,
            "DISCONNECT" => Self::Disconnect,
            "MESSAGE" => Self::Message,
            "RECEIPT" => Self::Receipt,
            "ERROR" => Self::Error,
            other => Self::Other(other.to_owned()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Connect => "CONNECT",
            Self::Connected => "CONNECTED",
            Self::Subscribe => "SUBSCRIBE",
            Self::Unsubscribe => "UNSUBSCRIBE",
            Self::Send => "SEND",
            Self::Ack => "ACK",
            Self::Nack => "NACK",
            Self::Disconnect => "DISCONNECT",
            Self::Message => "MESSAGE",
            Self::Receipt => "RECEIPT",
            Self::Error => "ERROR",
            Self::Other(s) => s.as_str(),
        }
    }
}

impl fmt::Display for STOMPCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Complete STOMP 1.2 frame preserving header ordering and optional body payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct STOMPFrame {
    pub command: STOMPCommand,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl STOMPFrame {
    pub fn new(command: STOMPCommand) -> Self {
        Self {
            command,
            headers: Vec::new(),
            body: String::new(),
        }
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    pub fn with_body(mut self, body: impl Into<String>) -> Self {
        self.body = body.into();
        self
    }

    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.as_str())
    }

    /// Encodes this frame as `<COMMAND>\n<key>:<val>\n\n<body>\x00`.
    pub fn encode(&self) -> String {
        let mut out = String::with_capacity(64 + self.headers.len() * 32 + self.body.len());
        out.push_str(self.command.as_str());
        out.push('\n');
        for (k, v) in &self.headers {
            out.push_str(k);
            out.push(':');
            out.push_str(v);
            out.push('\n');
        }
        out.push('\n');
        if !self.body.is_empty() {
            out.push_str(&self.body);
        }
        out.push('\0');
        out
    }

    /// Parses a raw WebSocket STOMP payload. Pure heartbeat frames (`\n` / `\r\n`) return `Ok(None)`.
    pub fn parse(raw: &str) -> Result<Option<Self>, RustfnError> {
        if raw.trim().is_empty() {
            return Ok(None);
        }

        let trimmed = raw.trim_start_matches(['\r', '\n']);
        let (head, body_and_nul) = if let Some(pair) = trimmed.split_once("\r\n\r\n") {
            pair
        } else if let Some(pair) = trimmed.split_once("\n\n") {
            pair
        } else {
            return Err(RustfnError::Protocol(format!(
                "malformed STOMP frame missing header/body delimiter: `{trimmed}`"
            )));
        };

        let mut lines = head.lines();
        let command_line = lines.next().ok_or_else(|| {
            RustfnError::Protocol("malformed STOMP frame missing command line".to_owned())
        })?;
        let command = STOMPCommand::from_wire(command_line);

        let mut headers = Vec::new();
        for line in lines {
            if let Some((key, val)) = line.split_once(':') {
                headers.push((key.trim().to_owned(), val.trim().to_owned()));
            }
        }

        let body = body_and_nul
            .trim_end_matches(['\0', '\r', '\n'])
            .trim_end_matches('\0')
            .to_owned();

        Ok(Some(Self {
            command,
            headers,
            body,
        }))
    }
}

/// Default FIFO ring capacity for STOMP message deduplication.
pub const DEFAULT_DEDUPE_CAPACITY: usize = 100;

/// Fixed-capacity FIFO deduplication ring preventing duplicate notification processing
/// across EOS Connect's public and temporary internal subscriptions.
#[derive(Debug, Clone)]
pub struct STOMPMessageDedupe {
    capacity: usize,
    order: VecDeque<String>,
    seen: HashSet<String>,
}

impl Default for STOMPMessageDedupe {
    fn default() -> Self {
        Self::new(DEFAULT_DEDUPE_CAPACITY)
    }
}

impl STOMPMessageDedupe {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            capacity: cap,
            order: VecDeque::with_capacity(cap),
            seen: HashSet::with_capacity(cap),
        }
    }

    pub fn contains(&self, id: &str) -> bool {
        self.seen.contains(id)
    }

    /// Inserts `id` and returns `true` if newly seen, or returns `false` if already present.
    pub fn mark_if_new(&mut self, id: impl Into<String>) -> bool {
        let id = id.into();
        if self.seen.contains(&id) {
            return false;
        }
        if self.order.len() == self.capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.seen.remove(&evicted);
            }
        }
        self.seen.insert(id.clone());
        self.order.push_back(id);
        true
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    pub fn clear(&mut self) {
        self.order.clear();
        self.seen.clear();
    }
}
