use crate::auth::session::{
    DeviceAuthCredentials, DeviceCodeInitResponse, EpicOAuthTokenResponse, ExchangeCodeResponse,
    ScopeWire, StoredAuthSession, deserialize_opt_secret_string, deserialize_secret_string,
};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use rand_core::{OsRng, RngCore};
use crate::http::{AuthSessionSlot, HttpClient, HttpRequestSpec};
use crate::protocol::{
    AccountId, AuthClient, DeploymentId, ProductUserId, RustfnError, ServiceEndpoints, error_codes,
};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use url::Url;
use zeroize::Zeroizing;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EulaCheckOutcome {
    AlreadyAccepted,
    Accepted { version: i64 },
}

#[derive(Debug, Deserialize)]
struct PendingEulaAgreement {
    version: i64,
    locale: String,
}

/// Extracts an OAuth authorization code from either a raw code or a redirect URL containing `?code=...`.
pub fn extract_authorization_code(input: &str) -> Result<SecretString, RustfnError> {
    let trimmed = input.trim();
    if trimmed.contains("?code=") || trimmed.contains("&code=") {
        let parsed = Url::parse(trimmed)
            .map_err(|err| RustfnError::Auth(format!("invalid authorization redirect URL: {err}")))?;
        let code = parsed
            .query_pairs()
            .find_map(|(k, v)| (k == "code").then(|| v.into_owned()))
            .filter(|c| !c.is_empty())
            .ok_or_else(|| {
                RustfnError::Auth("redirect URL does not contain `code` parameter".to_owned())
            })?;
        return Ok(SecretString::from(Zeroizing::new(code).as_str()));
    }

    if trimmed.is_empty() {
        return Err(RustfnError::Auth("authorization code must not be empty".to_owned()));
    }

    Ok(SecretString::from(Zeroizing::new(trimmed.to_owned()).as_str()))
}

pub async fn oauth_grant_epic(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    client: AuthClient,
    fields: Vec<(String, String)>,
) -> Result<EpicOAuthTokenResponse, RustfnError> {
    let basic = client.basic_auth_header();
    let spec = HttpRequestSpec::post(endpoints.oauth_token_create.as_ref())
        .with_authorization(basic.as_str())?
        .with_form(fields);
    http.execute_json(spec).await
}

pub async fn grant_device_auth(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    client: AuthClient,
    creds: &DeviceAuthCredentials,
) -> Result<EpicOAuthTokenResponse, RustfnError> {
    oauth_grant_epic(
        http,
        endpoints,
        client,
        vec![
            ("grant_type".into(), "device_auth".into()),
            ("device_id".into(), creds.device_id.clone()),
            ("account_id".into(), creds.account_id.as_str().to_owned()),
            ("secret".into(), creds.secret.expose_secret().to_owned()),
            ("token_type".into(), "eg1".into()),
        ],
    )
    .await
}

pub async fn grant_authorization_code(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    client: AuthClient,
    code: &SecretString,
) -> Result<EpicOAuthTokenResponse, RustfnError> {
    oauth_grant_epic(
        http,
        endpoints,
        client,
        vec![
            ("grant_type".into(), "authorization_code".into()),
            ("code".into(), code.expose_secret().to_owned()),
            ("token_type".into(), "eg1".into()),
        ],
    )
    .await
}

pub async fn grant_exchange_code(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    client: AuthClient,
    exchange_code: &SecretString,
) -> Result<EpicOAuthTokenResponse, RustfnError> {
    oauth_grant_epic(
        http,
        endpoints,
        client,
        vec![
            ("grant_type".into(), "exchange_code".into()),
            ("exchange_code".into(), exchange_code.expose_secret().to_owned()),
            ("token_type".into(), "eg1".into()),
        ],
    )
    .await
}

pub async fn grant_refresh_token(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    client: AuthClient,
    refresh_token: &SecretString,
) -> Result<EpicOAuthTokenResponse, RustfnError> {
    oauth_grant_epic(
        http,
        endpoints,
        client,
        vec![
            ("grant_type".into(), "refresh_token".into()),
            ("refresh_token".into(), refresh_token.expose_secret().to_owned()),
            ("token_type".into(), "eg1".into()),
        ],
    )
    .await
}

pub async fn grant_client_credentials(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    client: AuthClient,
) -> Result<EpicOAuthTokenResponse, RustfnError> {
    oauth_grant_epic(
        http,
        endpoints,
        client,
        vec![
            ("grant_type".into(), "client_credentials".into()),
            ("token_type".into(), "eg1".into()),
        ],
    )
    .await
}

pub async fn create_exchange_code(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    access_token: &SecretString,
) -> Result<ExchangeCodeResponse, RustfnError> {
    let spec = HttpRequestSpec::get(endpoints.oauth_exchange.as_ref())
        .with_authorization(format!("bearer {}", access_token.expose_secret()))?;
    http.execute_json(spec).await
}

pub async fn generate_device_auth(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    account_id: &AccountId,
    access_token: &SecretString,
) -> Result<DeviceAuthCredentials, RustfnError> {
    let url = endpoints.device_auth_collection(account_id.as_str());
    let spec = HttpRequestSpec::post(url)
        .with_authorization(format!("bearer {}", access_token.expose_secret()))?;
    http.execute_json(spec).await
}

pub async fn delete_device_auth(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    account_id: &AccountId,
    device_id: &str,
    access_token: &SecretString,
) -> Result<(), RustfnError> {
    let url = endpoints.device_auth_item(account_id.as_str(), device_id);
    let spec = HttpRequestSpec::delete(url)
        .with_authorization(format!("bearer {}", access_token.expose_secret()))?;
    http.execute_empty(spec).await
}

pub async fn verify_epic_token(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    access_token: &SecretString,
) -> Result<bool, RustfnError> {
    let spec = HttpRequestSpec::get(endpoints.oauth_token_verify.as_ref())
        .with_authorization(format!("bearer {}", access_token.expose_secret()))?;
    match http.execute_bytes(spec).await {
        Ok(_) => Ok(true),
        Err(err) if err.is_invalid_token() => Ok(false),
        Err(err) => Err(err),
    }
}

pub async fn kill_epic_token(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    access_token: &SecretString,
) -> Result<(), RustfnError> {
    let url = format!("{}/{}", endpoints.oauth_token_kill, access_token.expose_secret());
    let spec = HttpRequestSpec::delete(url)
        .with_route_key("DELETE account/api/oauth/sessions/kill/{token}")
        .with_authorization(format!("bearer {}", access_token.expose_secret()))?;
    match http.execute_empty(spec).await {
        Ok(()) => Ok(()),
        Err(err) if err.is_invalid_token() => Ok(()),
        Err(err) => Err(err),
    }
}

pub async fn kill_other_epic_sessions(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    access_token: &SecretString,
) -> Result<(), RustfnError> {
    let spec = HttpRequestSpec::delete(endpoints.oauth_token_kill.as_ref())
        .with_query("killType", "OTHERS_ACCOUNT_CLIENT_SERVICE")
        .with_authorization(format!("bearer {}", access_token.expose_secret()))?;
    http.execute_empty(spec).await
}

/// Requests Fortnite store access and accepts the pending Fortnite EULA if present.
pub async fn ensure_eula_accepted(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    account_id: &AccountId,
) -> Result<EulaCheckOutcome, RustfnError> {
    let request_access_url = format!("{}/{}", endpoints.init_request_access, account_id.as_str());
    let access_spec = HttpRequestSpec::post(request_access_url)
        .with_auth(AuthSessionSlot::Fortnite)
        .with_account_scope(account_id.clone())
        .with_json(&json!({}))?;

    if let Err(err) = http.execute_empty(access_spec).await {
        let is_expected_bad_request = err
            .as_epic_api()
            .is_some_and(|(_, api)| api.effective_code() == "errors.com.epicgames.bad_request");
        if !is_expected_bad_request {
            return Err(err);
        }
    }

    let check_url = format!("{}/account/{}", endpoints.init_eula, account_id.as_str());
    let check_resp = http
        .execute_bytes(
            HttpRequestSpec::get(check_url)
                .with_auth(AuthSessionSlot::Fortnite)
                .with_account_scope(account_id.clone()),
        )
        .await?;

    let trimmed = String::from_utf8_lossy(&check_resp.body);
    if check_resp.body.is_empty() || trimmed.trim().is_empty() || trimmed.trim() == "null" {
        return Ok(EulaCheckOutcome::AlreadyAccepted);
    }

    let agreement: Option<PendingEulaAgreement> =
        serde_json::from_slice(&check_resp.body).map_err(RustfnError::Serialization)?;
    let Some(agreement) = agreement else {
        return Ok(EulaCheckOutcome::AlreadyAccepted);
    };

    let accept_url = format!(
        "{}/version/{}/account/{}/accept",
        endpoints.init_eula,
        agreement.version,
        account_id.as_str()
    );
    http.execute_empty(
        HttpRequestSpec::post(accept_url)
            .with_auth(AuthSessionSlot::Fortnite)
            .with_account_scope(account_id.clone())
            .with_query("locale", agreement.locale),
    )
    .await?;

    Ok(EulaCheckOutcome::Accepted {
        version: agreement.version,
    })
}

/// Opens a verification URL in the host default browser, ignoring spawn failures in headless environments.
pub fn open_url_in_browser(url: &str) {
    if !Url::parse(url).is_ok_and(|parsed| matches!(parsed.scheme(), "http" | "https")) {
        return;
    }

    #[cfg(target_os = "windows")]
    let _ = std::process::Command::new("rundll32")
        .args(["url.dll,FileProtocolHandler", url])
        .spawn();

    #[cfg(target_os = "macos")]
    let _ = std::process::Command::new("open").arg(url).spawn();

    #[cfg(all(unix, not(target_os = "macos")))]
    let _ = std::process::Command::new("xdg-open").arg(url).spawn();
}

/// Runs the OAuth2 RFC 8628 device authorization flow using `AuthClient::FortniteNewSwitchGameClient`,
/// invoking `on_prompt` once with the verification URL and user code, polling until completion or cancellation,
/// and exchanging the Switch token onto `client`.
pub async fn authenticate_device_code<F, Fut>(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    client: AuthClient,
    cancel: &CancellationToken,
    on_prompt: F,
) -> Result<EpicOAuthTokenResponse, RustfnError>
where
    F: FnOnce(DeviceCodeInitResponse) -> Fut,
    Fut: Future<Output = ()>,
{
    authenticate_device_code_with_switch(
        http,
        endpoints,
        AuthClient::FortniteNewSwitchGameClient,
        client,
        cancel,
        on_prompt,
    )
    .await
}

/// Runs the OAuth2 device authorization flow with an explicit `switch_client` and exchanges
/// the resulting Switch grant onto `client`.
pub async fn authenticate_device_code_with_switch<F, Fut>(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    switch_client: AuthClient,
    client: AuthClient,
    cancel: &CancellationToken,
    on_prompt: F,
) -> Result<EpicOAuthTokenResponse, RustfnError>
where
    F: FnOnce(DeviceCodeInitResponse) -> Fut,
    Fut: Future<Output = ()>,
{
    let switch_creds = grant_client_credentials(http, endpoints, switch_client).await?;
    let init_spec = HttpRequestSpec::post(endpoints.oauth_device_code.as_ref())
        .with_authorization(format!(
            "bearer {}",
            switch_creds.access_token.expose_secret()
        ))?
        .with_form(vec![("prompt".into(), "login".into())]);
    let init: DeviceCodeInitResponse = http.execute_json(init_spec).await?;
    let poll_interval = Duration::from_secs(init.interval.max(1));
    let expires_in = Duration::from_secs(init.expires_in.max(10));
    let device_code = init.device_code.clone();
    on_prompt(init).await;
    let switch_token = poll_device_code(
        http,
        endpoints,
        switch_client,
        &device_code,
        poll_interval,
        tokio::time::Instant::now() + expires_in,
        cancel,
    )
    .await?;

    if client == switch_client {
        return Ok(switch_token);
    }

    let exchange = create_exchange_code(http, endpoints, &switch_token.access_token).await?;
    grant_exchange_code(http, endpoints, client, &exchange.code).await
}

#[allow(clippy::too_many_arguments)]
async fn poll_device_code(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    switch_client: AuthClient,
    device_code: &str,
    poll_interval: Duration,
    deadline: tokio::time::Instant,
    cancel: &CancellationToken,
) -> Result<EpicOAuthTokenResponse, RustfnError> {
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(RustfnError::Timeout(
                "device authorization expired before user confirmation".to_owned(),
            ));
        }

        tokio::select! {
            _ = cancel.cancelled() => return Err(RustfnError::Cancelled),
            _ = tokio::time::sleep(poll_interval) => {}
        }

        match oauth_grant_epic(
            http,
            endpoints,
            switch_client,
            vec![
                ("grant_type".into(), "device_code".into()),
                ("device_code".into(), device_code.to_owned()),
                ("token_type".into(), "eg1".into()),
            ],
        )
        .await
        {
            Ok(token) => return Ok(token),
            Err(RustfnError::EpicApi { ref body, .. })
                if body.effective_code() == error_codes::AUTHORIZATION_PENDING
                    || body.effective_code() == error_codes::NOT_FOUND =>
            {
                continue;
            }
            Err(RustfnError::EpicApi { ref body, .. })
                if body.effective_code() == error_codes::CORRECTIVE_ACTION_REQUIRED =>
            {
                return Err(RustfnError::Auth(format!(
                    "corrective action required before completing device login: {}",
                    body.effective_message()
                )));
            }
            Err(err) => return Err(err),
        }
    }
}

/// Token payload returned by `api.epicgames.dev/epic/oauth/v2/token`.
#[derive(Deserialize)]
pub struct EOSOAuthTokenResponse {
    #[serde(deserialize_with = "deserialize_secret_string")]
    pub access_token: SecretString,
    #[serde(default)]
    pub token_type: String,
    #[serde(default)]
    pub expires_in: i64,
    pub expires_at: DateTime<Utc>,
    #[serde(default, deserialize_with = "deserialize_opt_secret_string")]
    pub refresh_token: Option<SecretString>,
    #[serde(default)]
    pub refresh_expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub account_id: Option<AccountId>,
    #[serde(default)]
    pub product_user_id: Option<ProductUserId>,
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub application_id: Option<String>,
    #[serde(default)]
    pub deployment_id: Option<DeploymentId>,
    #[serde(default)]
    pub merged_accounts: Vec<String>,
    #[serde(default)]
    pub scope: ScopeWire,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EOSTokenInfoResponse {
    pub active: bool,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub application_id: Option<String>,
}

/// Exchanges a short-lived Epic exchange code for an EOS `epic_id` Connect token.
pub async fn authenticate_eos_exchange(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    client: AuthClient,
    exchange_code: &SecretString,
    deployment_id: &str,
) -> Result<EOSOAuthTokenResponse, RustfnError> {
    let basic = client.basic_auth_header();
    let spec = HttpRequestSpec::post(endpoints.eos_token.as_ref())
        .with_authorization(basic.as_str())?
        .with_form(vec![
            ("grant_type".into(), "exchange_code".into()),
            ("exchange_code".into(), exchange_code.expose_secret().to_owned()),
            ("token_type".into(), "epic_id".into()),
            ("deployment_id".into(), deployment_id.to_owned()),
        ]);
    http.execute_json(spec).await
}

/// Exchanges an external Epic Games access token for an EOS Connect token,
/// automatically continuing `/auth/v1/users` if EOS returns `user_not_found`.
pub async fn grant_eos_external_auth(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    client: AuthClient,
    external_auth_token: &SecretString,
    deployment_id: &str,
) -> Result<EOSOAuthTokenResponse, RustfnError> {
    let mut nonce_bytes = [0u8; 22];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = URL_SAFE_NO_PAD.encode(nonce_bytes);

    let basic = client.basic_auth_header();
    let spec = HttpRequestSpec::post(endpoints.eos_token.as_ref())
        .with_authorization(basic.as_str())?
        .with_form(vec![
            ("grant_type".into(), "external_auth".into()),
            ("external_auth_type".into(), "epicgames_access_token".into()),
            (
                "external_auth_token".into(),
                external_auth_token.expose_secret().to_owned(),
            ),
            ("deployment_id".into(), deployment_id.to_owned()),
            ("nonce".into(), nonce),
        ]);

    match http.execute_json(spec).await {
        Ok(tok) => Ok(tok),
        Err(RustfnError::EpicApi { ref body, .. })
            if body.effective_code() == "errors.com.epicgames.eos.auth.user_not_found" =>
        {
            let continuation = body.continuation_token.as_deref().ok_or_else(|| {
                RustfnError::Auth(
                    "EOS user_not_found returned without continuation_token".to_owned(),
                )
            })?;
            let cont_spec = HttpRequestSpec::post(endpoints.eos_users.as_ref())
                .with_authorization(format!("Bearer {continuation}"))?;
            http.execute_json(cont_spec).await
        }
        Err(err) => Err(err),
    }
}

pub async fn refresh_eos_token(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    client: AuthClient,
    refresh_token: &SecretString,
    base_payload: &BTreeMap<String, String>,
) -> Result<EOSOAuthTokenResponse, RustfnError> {
    let mut form: Vec<(String, String)> = base_payload
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    form.push(("grant_type".into(), "refresh_token".into()));
    form.push(("refresh_token".into(), refresh_token.expose_secret().to_owned()));

    let basic = client.basic_auth_header();
    let spec = HttpRequestSpec::post(endpoints.eos_token.as_ref())
        .with_authorization(basic.as_str())?
        .with_form(form);
    http.execute_json(spec).await
}

pub async fn verify_eos_token(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    access_token: &SecretString,
) -> Result<bool, RustfnError> {
    let spec = HttpRequestSpec::post(endpoints.eos_token_info.as_ref())
        .with_form(vec![("token".into(), access_token.expose_secret().to_owned())]);
    let info: EOSTokenInfoResponse = http.execute_json(spec).await?;
    Ok(info.active)
}

pub async fn revoke_eos_token(
    http: &HttpClient,
    endpoints: &ServiceEndpoints,
    access_token: &SecretString,
) -> Result<(), RustfnError> {
    let spec = HttpRequestSpec::post(endpoints.eos_token_revoke.as_ref())
        .with_form(vec![("token".into(), access_token.expose_secret().to_owned())]);
    http.execute_empty(spec).await
}

pub(crate) fn eos_response_to_stored(
    client: AuthClient,
    resp: EOSOAuthTokenResponse,
    base_payload: BTreeMap<String, String>,
) -> StoredAuthSession {
    StoredAuthSession {
        slot: AuthSessionSlot::FortniteEos,
        client,
        access_token: resp.access_token,
        expires_at: resp.expires_at,
        refresh_token: resp.refresh_token,
        refresh_expires_at: resp.refresh_expires_at,
        account_id: resp.account_id,
        display_name: None,
        device_id: None,
        product_user_id: resp.product_user_id,
        deployment_id: resp.deployment_id,
        scope: resp.scope.into_vec(),
        eos_base_payload: base_payload,
    }
}
