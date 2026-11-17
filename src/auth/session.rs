use chrono::{DateTime, Utc};
use rand_core::{OsRng, RngCore};
use crate::http::AuthSessionSlot;
use crate::protocol::{AccountId, AuthClient, DeploymentId, ProductUserId, RustfnError};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};
use zeroize::Zeroizing;

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ScopeWire {
    List(Vec<String>),
    SpaceSeparated(String),
}

impl Default for ScopeWire {
    fn default() -> Self {
        Self::List(Vec::new())
    }
}

impl ScopeWire {
    pub fn into_vec(self) -> Vec<String> {
        match self {
            Self::List(list) => list,
            Self::SpaceSeparated(s) => s.split_whitespace().map(str::to_owned).collect(),
        }
    }
}

#[derive(Deserialize)]
pub struct EpicOAuthTokenResponse {
    #[serde(deserialize_with = "deserialize_secret_string")]
    pub access_token: SecretString,
    #[serde(default)]
    pub expires_in: i64,
    pub expires_at: DateTime<Utc>,
    #[serde(default)]
    pub token_type: String,
    #[serde(default, deserialize_with = "deserialize_opt_secret_string")]
    pub refresh_token: Option<SecretString>,
    #[serde(default)]
    pub refresh_expires: Option<i64>,
    #[serde(default)]
    pub refresh_expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub account_id: Option<AccountId>,
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub internal_client: bool,
    #[serde(default)]
    pub client_service: String,
    #[serde(rename = "displayName", default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub app: Option<String>,
    #[serde(default)]
    pub in_app_id: Option<String>,
    #[serde(default)]
    pub device_id: Option<String>,
    #[serde(default)]
    pub scope: ScopeWire,
    #[serde(default)]
    pub product_id: Option<String>,
    #[serde(default)]
    pub application_id: Option<String>,
}

#[derive(Deserialize)]
pub struct ExchangeCodeResponse {
    #[serde(rename = "expiresInSeconds", default)]
    pub expires_in_seconds: u64,
    #[serde(deserialize_with = "deserialize_secret_string")]
    pub code: SecretString,
    #[serde(rename = "creatingClientId", default)]
    pub creating_client_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeviceCodeInitResponse {
    pub user_code: String,
    pub device_code: String,
    pub verification_uri: String,
    #[serde(default)]
    pub verification_uri_complete: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    pub expires_in: u64,
    pub interval: u64,
    #[serde(default)]
    pub client_id: Option<String>,
}

/// Active OAuth credential state retained per session slot.
#[derive(Debug, Clone)]
pub struct StoredAuthSession {
    pub slot: AuthSessionSlot,
    pub client: AuthClient,
    pub access_token: SecretString,
    pub expires_at: DateTime<Utc>,
    pub refresh_token: Option<SecretString>,
    pub refresh_expires_at: Option<DateTime<Utc>>,
    pub account_id: Option<AccountId>,
    pub display_name: Option<String>,
    pub device_id: Option<String>,
    pub product_user_id: Option<ProductUserId>,
    pub deployment_id: Option<DeploymentId>,
    pub scope: Vec<String>,
    pub eos_base_payload: BTreeMap<String, String>,
}

impl StoredAuthSession {
    pub fn is_expired(&self) -> bool {
        self.expires_at <= Utc::now()
    }

    pub fn summary(&self) -> AuthSessionSummary {
        AuthSessionSummary {
            slot: self.slot,
            client: self.client,
            expires_at: self.expires_at,
            refresh_expires_at: self.refresh_expires_at,
            account_id: self.account_id.clone(),
            display_name: self.display_name.clone(),
            product_user_id: self.product_user_id.clone(),
        }
    }
}

/// Secret-free diagnostic snapshot of an active OAuth session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthSessionSummary {
    pub slot: AuthSessionSlot,
    pub client: AuthClient,
    pub expires_at: DateTime<Utc>,
    pub refresh_expires_at: Option<DateTime<Utc>>,
    pub account_id: Option<AccountId>,
    pub display_name: Option<String>,
    pub product_user_id: Option<ProductUserId>,
}

/// Thread-safe session registry guarded by per-slot single-flight refresh mutexes.
#[derive(Debug)]
pub struct AuthSessionStore {
    sessions: RwLock<HashMap<AuthSessionSlot, StoredAuthSession>>,
    refresh_gates: RefreshGates,
}

#[derive(Debug)]
struct RefreshGates {
    fortnite: Arc<Mutex<()>>,
    launcher: Arc<Mutex<()>>,
    fortnite_client_credentials: Arc<Mutex<()>>,
    fortnite_eos: Arc<Mutex<()>>,
}

impl RefreshGates {
    fn new() -> Self {
        Self {
            fortnite: Arc::new(Mutex::new(())),
            launcher: Arc::new(Mutex::new(())),
            fortnite_client_credentials: Arc::new(Mutex::new(())),
            fortnite_eos: Arc::new(Mutex::new(())),
        }
    }

    fn get(&self, slot: AuthSessionSlot) -> Arc<Mutex<()>> {
        let gate = match slot {
            AuthSessionSlot::Fortnite => &self.fortnite,
            AuthSessionSlot::Launcher => &self.launcher,
            AuthSessionSlot::FortniteClientCredentials => &self.fortnite_client_credentials,
            AuthSessionSlot::FortniteEos => &self.fortnite_eos,
        };
        Arc::clone(gate)
    }
}

impl Default for AuthSessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthSessionStore {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            refresh_gates: RefreshGates::new(),
        }
    }

    pub fn refresh_gate(&self, slot: AuthSessionSlot) -> Arc<Mutex<()>> {
        self.refresh_gates.get(slot)
    }

    pub async fn get_session(&self, slot: AuthSessionSlot) -> Option<StoredAuthSession> {
        let gate = self.refresh_gate(slot);
        let _guard = gate.lock().await;
        drop(_guard);

        self.sessions.read().await.get(&slot).cloned()
    }

    pub async fn get_session_unguarded(&self, slot: AuthSessionSlot) -> Option<StoredAuthSession> {
        self.sessions.read().await.get(&slot).cloned()
    }

    pub async fn insert_session(&self, session: StoredAuthSession) {
        self.sessions.write().await.insert(session.slot, session);
    }

    pub async fn remove_session(&self, slot: AuthSessionSlot) -> Option<StoredAuthSession> {
        self.sessions.write().await.remove(&slot)
    }

    pub async fn clear(&self) {
        self.sessions.write().await.clear();
    }

    pub async fn bearer_secret(&self, slot: AuthSessionSlot) -> Result<SecretString, RustfnError> {
        self.get_session(slot)
            .await
            .map(|s| s.access_token)
            .ok_or_else(|| RustfnError::Auth(format!("missing active auth session for {slot:?}")))
    }

    pub async fn account_id(&self) -> Option<AccountId> {
        self.sessions
            .read()
            .await
            .get(&AuthSessionSlot::Fortnite)
            .and_then(|s| s.account_id.clone())
    }

    pub async fn display_name(&self) -> Option<String> {
        self.sessions
            .read()
            .await
            .get(&AuthSessionSlot::Fortnite)
            .and_then(|s| s.display_name.clone())
    }

    pub async fn product_user_id(&self) -> Option<ProductUserId> {
        self.sessions
            .read()
            .await
            .get(&AuthSessionSlot::FortniteEos)
            .and_then(|s| s.product_user_id.clone())
    }

    pub async fn all_snapshots(&self) -> Vec<StoredAuthSession> {
        self.sessions.read().await.values().cloned().collect()
    }

    pub async fn all_summaries(&self) -> Vec<AuthSessionSummary> {
        self.sessions
            .read()
            .await
            .values()
            .map(StoredAuthSession::summary)
            .collect()
    }
}

pub fn deserialize_secret_string<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<SecretString, D::Error> {
    let raw = Zeroizing::new(String::deserialize(deserializer)?);
    Ok(SecretString::from(raw.as_str()))
}

pub fn deserialize_opt_secret_string<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<SecretString>, D::Error> {
    let opt = Option::<String>::deserialize(deserializer)?;
    Ok(opt.map(|s| {
        let zeroed = Zeroizing::new(s);
        SecretString::from(zeroed.as_str())
    }))
}

/// Persistent DeviceAuth credentials usable for unattended Epic OAuth logins.
#[derive(Clone, Deserialize)]
pub struct DeviceAuthCredentials {
    #[serde(alias = "accountId")]
    pub account_id: AccountId,
    #[serde(alias = "deviceId")]
    pub device_id: String,
    #[serde(deserialize_with = "deserialize_secret_string")]
    pub secret: SecretString,
}

impl std::fmt::Debug for DeviceAuthCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceAuthCredentials")
            .field("account_id", &self.account_id)
            .field("device_id", &self.device_id)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

impl DeviceAuthCredentials {
    pub fn new(
        account_id: AccountId,
        device_id: impl Into<String>,
        secret: impl Into<SecretString>,
    ) -> Self {
        Self {
            account_id,
            device_id: device_id.into(),
            secret: secret.into(),
        }
    }

    pub fn to_export_json(&self) -> Result<Zeroizing<String>, serde_json::Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Export<'a> {
            account_id: &'a str,
            device_id: &'a str,
            secret: &'a str,
        }
        serde_json::to_string_pretty(&Export {
            account_id: self.account_id.as_str(),
            device_id: &self.device_id,
            secret: self.secret.expose_secret(),
        })
        .map(Zeroizing::new)
    }
}

pub type DeviceStoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Persistence backend for saving and reloading account DeviceAuth credentials.
pub trait DeviceAuthStore: Send + Sync {
    fn load(
        &self,
        account_id: &AccountId,
    ) -> DeviceStoreFuture<'_, Result<Option<DeviceAuthCredentials>, RustfnError>>;

    fn save(
        &self,
        creds: &DeviceAuthCredentials,
    ) -> DeviceStoreFuture<'_, Result<(), RustfnError>>;
}

/// Stores DeviceAuth JSON records inside a directory (`{dir}/{account_id}.json`) via atomic rename.
#[derive(Debug, Clone)]
pub struct JsonFileDeviceAuthStore {
    directory: PathBuf,
}

impl JsonFileDeviceAuthStore {
    pub fn new(directory: impl AsRef<Path>) -> Self {
        Self {
            directory: directory.as_ref().to_path_buf(),
        }
    }

    fn path_for(&self, account_id: &AccountId) -> PathBuf {
        self.directory.join(format!("{}.json", account_id.as_str()))
    }
}

impl DeviceAuthStore for JsonFileDeviceAuthStore {
    fn load(
        &self,
        account_id: &AccountId,
    ) -> DeviceStoreFuture<'_, Result<Option<DeviceAuthCredentials>, RustfnError>> {
        let target = self.path_for(account_id);
        let expected_account_id = account_id.clone();
        Box::pin(async move {
            match tokio::fs::read(&target).await {
                Ok(bytes) => {
                    let creds: DeviceAuthCredentials = serde_json::from_slice(&bytes)?;
                    if creds.account_id != expected_account_id {
                        return Err(RustfnError::Auth(format!(
                            "device auth file {} contains credentials for another account",
                            target.display()
                        )));
                    }
                    Ok(Some(creds))
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(RustfnError::Auth(format!(
                    "failed to read device auth file {}: {err}",
                    target.display()
                ))),
            }
        })
    }

    fn save(
        &self,
        creds: &DeviceAuthCredentials,
    ) -> DeviceStoreFuture<'_, Result<(), RustfnError>> {
        let target = self.path_for(&creds.account_id);
        let serialized = creds.to_export_json();
        Box::pin(async move {
            let payload = serialized?;
            if let Some(parent) = target.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|err| {
                    RustfnError::Auth(format!(
                        "failed to create device auth directory {}: {err}",
                        parent.display()
                    ))
                })?;
            }

            let temp_path = target.with_extension(format!("json.{:016x}.tmp", OsRng.next_u64()));
            let mut options = tokio::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            let mut file = options.open(&temp_path).await.map_err(|err| {
                RustfnError::Auth(format!(
                    "failed to create temporary device auth {}: {err}",
                    temp_path.display()
                ))
            })?;
            let write_result = async {
                file.write_all(payload.as_bytes()).await?;
                file.sync_all().await?;
                drop(file);
                tokio::fs::rename(&temp_path, &target).await
            }
            .await;
            if let Err(err) = write_result {
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(RustfnError::Auth(format!(
                    "failed to save device auth {}: {err}",
                    target.display()
                )));
            }
            Ok(())
        })
    }
}
