use crate::http::rate_limiter::{
    EndpointRateLimiter, RetryConfig, RetryDecision, RetryDecisionHook, ThrottleKey,
    extract_retry_after,
};
use bytes::Bytes;
use reqwest::Method;
use reqwest::header::{
    ACCEPT_LANGUAGE, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT,
};
use crate::protocol::{AccountId, EpicgamesAPIErrorData, RustfnError};
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Identifies which stored credential session provides bearer authentication for a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthSessionSlot {
    Fortnite,
    Launcher,
    FortniteClientCredentials,
    FortniteEos,
}

/// Decoupled token source implemented by the auth module and consumed by [`HttpClient`].
pub trait TokenProvider: Send + Sync {
    fn bearer_token(&self, slot: AuthSessionSlot) -> BoxFuture<'_, Result<SecretString, RustfnError>>;

    fn refresh_on_invalid_token<'a>(
        &'a self,
        slot: AuthSessionSlot,
        failed_token: &'a str,
    ) -> BoxFuture<'a, Result<SecretString, RustfnError>>;
}

#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    pub user_agent: String,
    pub accept_language: String,
    pub request_timeout: Duration,
    pub retry: RetryConfig,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            user_agent: "Fortnite/++Fortnite+Release-31.00-CL-37219843 Windows/10.0.22631.1.768.64bit"
                .to_owned(),
            accept_language: "en".to_owned(),
            request_timeout: Duration::from_secs(30),
            retry: RetryConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub enum RequestBody {
    #[default]
    Empty,
    Json(serde_json::Value),
    Form(Vec<(String, String)>),
    Raw {
        content_type: String,
        data: Bytes,
    },
}

/// Fluent specification of an outbound Epic/EOS HTTP request.
#[derive(Clone)]
pub struct HttpRequestSpec {
    pub method: Method,
    pub url: String,
    pub route_key: Option<String>,
    pub account_scope: Option<AccountId>,
    pub auth_slot: Option<AuthSessionSlot>,
    pub headers: HeaderMap,
    pub query: Vec<(String, String)>,
    pub body: RequestBody,
    pub retry_decision: Option<RetryDecisionHook>,
}

impl HttpRequestSpec {
    pub fn new(method: Method, url: impl Into<String>) -> Self {
        Self {
            method,
            url: url.into(),
            route_key: None,
            account_scope: None,
            auth_slot: None,
            headers: HeaderMap::new(),
            query: Vec::new(),
            body: RequestBody::Empty,
            retry_decision: None,
        }
    }

    pub fn get(url: impl Into<String>) -> Self {
        Self::new(Method::GET, url)
    }

    pub fn post(url: impl Into<String>) -> Self {
        Self::new(Method::POST, url)
    }

    pub fn put(url: impl Into<String>) -> Self {
        Self::new(Method::PUT, url)
    }

    pub fn patch(url: impl Into<String>) -> Self {
        Self::new(Method::PATCH, url)
    }

    pub fn delete(url: impl Into<String>) -> Self {
        Self::new(Method::DELETE, url)
    }

    pub fn with_auth(mut self, slot: AuthSessionSlot) -> Self {
        self.auth_slot = Some(slot);
        self
    }

    pub fn with_route_key(mut self, route_key: impl Into<String>) -> Self {
        self.route_key = Some(route_key.into());
        self
    }

    pub fn with_account_scope(mut self, account_id: AccountId) -> Self {
        self.account_scope = Some(account_id);
        self
    }

    pub fn with_header(
        mut self,
        name: HeaderName,
        value: impl AsRef<str>,
    ) -> Result<Self, RustfnError> {
        let header_val = HeaderValue::from_str(value.as_ref())
            .map_err(|err| RustfnError::Protocol(format!("invalid header value: {err}")))?;
        self.headers.insert(name, header_val);
        Ok(self)
    }

    pub fn with_authorization(self, header_value: impl AsRef<str>) -> Result<Self, RustfnError> {
        self.with_header(AUTHORIZATION, header_value)
    }

    pub fn with_query(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.push((key.into(), value.into()));
        self
    }

    pub fn with_json<T: Serialize>(mut self, payload: &T) -> Result<Self, RustfnError> {
        self.body = RequestBody::Json(serde_json::to_value(payload)?);
        Ok(self)
    }

    pub fn with_form(mut self, fields: Vec<(String, String)>) -> Self {
        self.body = RequestBody::Form(fields);
        self
    }

    pub fn with_raw_body(mut self, content_type: impl Into<String>, data: impl Into<Bytes>) -> Self {
        self.body = RequestBody::Raw {
            content_type: content_type.into(),
            data: data.into(),
        };
        self
    }

    pub fn with_text_body(self, content_type: impl Into<String>, text: impl Into<String>) -> Self {
        self.with_raw_body(content_type, Bytes::from(text.into()))
    }

    pub fn with_retry_decision(mut self, hook: RetryDecisionHook) -> Self {
        self.retry_decision = Some(hook);
        self
    }

}

/// Raw HTTP response payload returned by [`HttpClient::execute_bytes`].
#[derive(Debug, Clone)]
pub struct HttpResponseBytes {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Bytes,
}

/// Pooled asynchronous HTTP transport with Epic rate-limit barriers, backoff, and token refresh.
#[derive(Clone)]
pub struct HttpClient {
    inner: reqwest::Client,
    config: HttpClientConfig,
    rate_limiter: Arc<EndpointRateLimiter>,
    token_provider: Option<Arc<dyn TokenProvider>>,
}

impl HttpClient {
    pub fn new(config: HttpClientConfig) -> Result<Self, RustfnError> {
        let inner = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(30))
            .pool_max_idle_per_host(16)
            .pool_idle_timeout(Duration::from_secs(90))
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(Some(Duration::from_secs(15)))
            .http2_keep_alive_while_idle(true)
            .build()
            .map_err(|err| RustfnError::Transport(err.to_string()))?;

        Ok(Self {
            inner,
            config,
            rate_limiter: Arc::new(EndpointRateLimiter::new()),
            token_provider: None,
        })
    }

    /// Creates a sibling client sharing the same pooled sockets and global rate-limit registry,
    /// bound to a separate [`TokenProvider`].
    pub fn share_pool(&self, provider: Arc<dyn TokenProvider>) -> Self {
        Self {
            inner: self.inner.clone(),
            config: self.config.clone(),
            rate_limiter: Arc::clone(&self.rate_limiter),
            token_provider: Some(provider),
        }
    }

    /// Creates an unauthenticated sibling client sharing the same pooled sockets and global
    /// rate-limit registry, suitable for OAuth grant and token refresh calls.
    pub fn share_pool_anonymous(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            config: self.config.clone(),
            rate_limiter: Arc::clone(&self.rate_limiter),
            token_provider: None,
        }
    }

    pub fn with_token_provider(mut self, provider: Arc<dyn TokenProvider>) -> Self {
        self.token_provider = Some(provider);
        self
    }

    pub fn set_token_provider(&mut self, provider: Arc<dyn TokenProvider>) {
        self.token_provider = Some(provider);
    }

    pub fn config(&self) -> &HttpClientConfig {
        &self.config
    }

    pub async fn execute_bytes(&self, spec: HttpRequestSpec) -> Result<HttpResponseBytes, RustfnError> {
        let throttle_key = ThrottleKey::from_request(
            &spec.method,
            &spec.url,
            spec.route_key.as_deref(),
            spec.account_scope.as_ref(),
        );

        let retry = &self.config.retry;
        let mut tries: usize = 0;
        let mut rest_5xx_retries: usize = 0;
        let mut auth_retried = false;
        let mut total_slept = Duration::ZERO;
        let mut backoff = retry.backoff_start;

        loop {
            if retry.other_requests_wait {
                self.rate_limiter.wait_if_limited(&throttle_key).await;
            }

            let mut used_bearer: Option<SecretString> = None;
            let mut builder = self.inner.request(spec.method.clone(), &spec.url);

            builder = builder
                .header(USER_AGENT, &self.config.user_agent)
                .header(ACCEPT_LANGUAGE, &self.config.accept_language);

            for (name, value) in &spec.headers {
                builder = builder.header(name, value);
            }

            if let Some(slot) = spec.auth_slot {
                let provider = self.token_provider.as_ref().ok_or_else(|| {
                    RustfnError::Auth(format!(
                        "authenticated request requires a TokenProvider for slot {slot:?}"
                    ))
                })?;
                let token = provider.bearer_token(slot).await?;
                builder = builder.header(AUTHORIZATION, format!("bearer {}", token.expose_secret()));
                used_bearer = Some(token);
            }

            if !spec.query.is_empty() {
                builder = builder.query(&spec.query);
            }

            builder = match &spec.body {
                RequestBody::Empty => builder,
                RequestBody::Json(val) => builder.json(val),
                RequestBody::Form(fields) => builder.form(fields),
                RequestBody::Raw { content_type, data } => {
                    builder.header(CONTENT_TYPE, content_type).body(data.clone())
                }
            };

            let response = match builder.send().await {
                Ok(resp) => resp,
                Err(err) => {
                    let err = err.without_url();
                    if (err.is_timeout() || err.is_connect())
                        && rest_5xx_retries < retry.rest_retry_limit
                    {
                        rest_5xx_retries += 1;
                        let wait =
                            next_backoff(&mut backoff, retry.backoff_factor, retry.backoff_cap);
                        if total_slept + wait <= retry.max_wait_time {
                            total_slept += wait;
                            warn!(method = %spec.method, wait_ms = wait.as_millis(), error = %err, "Retrying after HTTP transport error");
                            tokio::time::sleep(wait).await;
                            continue;
                        }
                    }
                    return Err(RustfnError::Transport(err.to_string()));
                }
            };

            let status = response.status();
            let headers = response.headers().clone();
            let body = response
                .bytes()
                .await
                .map_err(|err| RustfnError::Transport(err.without_url().to_string()))?;

            debug!(method = %spec.method, status = status.as_u16(), "HTTP response received");

            if status.is_success() {
                return Ok(HttpResponseBytes {
                    status: status.as_u16(),
                    headers,
                    body,
                });
            }

            tries += 1;
            let parsed_epic_error = parse_epic_error(&body);

            if !auth_retried
                && let Some(slot) = spec.auth_slot
                && let Some(failed_token) = used_bearer.as_ref()
                && (status.as_u16() == 401
                    || parsed_epic_error
                        .as_ref()
                        .is_some_and(EpicgamesAPIErrorData::is_invalid_token))
            {
                let provider = self.token_provider.as_ref().ok_or_else(|| {
                    RustfnError::Auth("TokenProvider missing during token refresh".to_owned())
                })?;
                provider
                    .refresh_on_invalid_token(slot, failed_token.expose_secret())
                    .await?;
                auth_retried = true;
                continue;
            }

            let is_throttled = status.as_u16() == 429
                || parsed_epic_error
                    .as_ref()
                    .is_some_and(EpicgamesAPIErrorData::is_throttled);

            if is_throttled && retry.handle_rate_limits && tries <= retry.max_retry_attempts {
                if let Some(retry_after) =
                    extract_retry_after(&headers, parsed_epic_error.as_ref())
                {
                    if retry_after <= retry.max_retry_after {
                        let wait = retry_after + retry.retry_after_padding;
                        if total_slept + wait <= retry.max_wait_time {
                            if let Some(hook) = spec.retry_decision.as_ref() {
                                match hook() {
                                    RetryDecision::Retry => {}
                                    RetryDecision::Abandon => return Err(RustfnError::Cancelled),
                                    RetryDecision::Throw => {
                                        return Err(build_http_error(
                                            status.as_u16(),
                                            parsed_epic_error,
                                            &body,
                                        ));
                                    }
                                }
                            }

                            total_slept += wait;
                            let _guard = self
                                .rate_limiter
                                .enter_cooldown(throttle_key.clone(), wait);
                            tokio::time::sleep(wait).await;

                            if let Some(hook) = spec.retry_decision.as_ref() {
                                match hook() {
                                    RetryDecision::Retry => {}
                                    RetryDecision::Abandon => return Err(RustfnError::Cancelled),
                                    RetryDecision::Throw => {
                                        return Err(build_http_error(
                                            status.as_u16(),
                                            parsed_epic_error,
                                            &body,
                                        ));
                                    }
                                }
                            }
                            continue;
                        }
                    }
                } else if retry.handle_capacity_throttling {
                    let wait = next_backoff(&mut backoff, retry.backoff_factor, retry.backoff_cap);
                    if wait <= retry.backoff_cap && total_slept + wait <= retry.max_wait_time {
                        total_slept += wait;
                        tokio::time::sleep(wait).await;
                        continue;
                    }
                }
            }

            if parsed_epic_error.as_ref().is_some_and(|err| {
                err.is_concurrent_modification()
                    || err.effective_code() == "errors.com.epicgames.common.server_error"
            }) && tries <= retry.max_retry_attempts
            {
                let wait = Duration::from_millis(500) + Duration::from_secs(2 * (tries as u64 - 1));
                if total_slept + wait <= retry.max_wait_time {
                    total_slept += wait;
                    tokio::time::sleep(wait).await;
                    continue;
                }
            }

            if status.is_server_error() && rest_5xx_retries < retry.rest_retry_limit {
                rest_5xx_retries += 1;
                let wait = next_backoff(&mut backoff, retry.backoff_factor, retry.backoff_cap);
                if total_slept + wait <= retry.max_wait_time {
                    total_slept += wait;
                    tokio::time::sleep(wait).await;
                    continue;
                }
            }

            return Err(build_http_error(status.as_u16(), parsed_epic_error, &body));
        }
    }

    pub async fn execute_json<T: DeserializeOwned>(
        &self,
        spec: HttpRequestSpec,
    ) -> Result<T, RustfnError> {
        let resp = self.execute_bytes(spec).await?;
        serde_json::from_slice(&resp.body).map_err(RustfnError::Serialization)
    }

    pub async fn execute_empty(&self, spec: HttpRequestSpec) -> Result<(), RustfnError> {
        let _ = self.execute_bytes(spec).await?;
        Ok(())
    }
}

fn parse_epic_error(body: &[u8]) -> Option<EpicgamesAPIErrorData> {
    if body.is_empty() {
        return None;
    }
    let parsed: EpicgamesAPIErrorData = serde_json::from_slice(body).ok()?;
    if parsed.error_code.is_empty()
        && parsed.error.is_none()
        && parsed.error_message.is_empty()
        && parsed.error_description.is_none()
    {
        return None;
    }
    Some(parsed)
}

fn build_http_error(
    status: u16,
    parsed_epic_error: Option<EpicgamesAPIErrorData>,
    raw_body: &[u8],
) -> RustfnError {
    match parsed_epic_error {
        Some(api_err) => RustfnError::EpicApi {
            status,
            body: Box::new(api_err),
        },
        None => RustfnError::HttpStatus {
            status,
            body: String::from_utf8_lossy(raw_body).into_owned(),
        },
    }
}

fn next_backoff(current: &mut Duration, factor: f64, cap: Duration) -> Duration {
    let wait = (*current).min(cap);
    let next_secs = (current.as_secs_f64() * factor).min(cap.as_secs_f64());
    *current = Duration::from_secs_f64(next_secs.max(0.05));
    wait
}
