use dashmap::DashMap;
use reqwest::Method;
use reqwest::header::HeaderMap;
use rustfn_protocol::{AccountId, EpicgamesAPIErrorData};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use url::Url;

/// Rate-limit barrier isolation scope across sessions sharing an [`crate::HttpClient`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ThrottleScope {
    Global,
    PerAccount(AccountId),
}

/// Normalized endpoint key indexing rate-limit barriers across requests.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ThrottleKey {
    pub scope: ThrottleScope,
    pub method: Method,
    pub route_template: String,
}

impl ThrottleKey {
    pub fn from_request(
        method: &Method,
        url: &str,
        explicit_route: Option<&str>,
        account_scope: Option<&AccountId>,
    ) -> Self {
        let route_template = explicit_route
            .map(str::to_owned)
            .unwrap_or_else(|| sanitize_route_template(url));
        let scope = match account_scope {
            Some(account_id) => ThrottleScope::PerAccount(account_id.clone()),
            None => ThrottleScope::Global,
        };
        Self {
            scope,
            method: method.clone(),
            route_template,
        }
    }
}

/// Strips query strings and normalizes 32-char hexadecimal path segments into `{id}`
/// so shared endpoints coalesce accurately onto one throttle bucket.
pub fn sanitize_route_template(raw_url: &str) -> String {
    match Url::parse(raw_url) {
        Ok(url) => {
            let normalized_path = url
                .path()
                .split('/')
                .map(|seg| {
                    if is_hex32(seg) {
                        "{id}"
                    } else if seg.len() > 40 && seg.ends_with("-default") {
                        "{lobby_id}"
                    } else {
                        seg
                    }
                })
                .collect::<Vec<_>>()
                .join("/");
            format!(
                "{}://{}{}",
                url.scheme(),
                url.host_str().unwrap_or_default(),
                normalized_path
            )
        }
        Err(_) => raw_url.split('?').next().unwrap_or(raw_url).to_owned(),
    }
}

fn is_hex32(segment: &str) -> bool {
    segment.len() == 32 && segment.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Caller-supplied hook indicating how a rate-limit pause should proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    Retry,
    Abandon,
    Throw,
}

/// Optional callback consulted before sleeping on HTTP 429 / Epic throttling.
pub type RetryDecisionHook = Arc<dyn Fn() -> RetryDecision + Send + Sync>;

/// Retry and backoff thresholds for Epic REST requests.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retry_attempts: usize,
    pub rest_retry_limit: usize,
    pub max_wait_time: Duration,
    pub max_retry_after: Duration,
    pub backoff_start: Duration,
    pub backoff_factor: f64,
    pub backoff_cap: Duration,
    pub retry_after_padding: Duration,
    pub handle_rate_limits: bool,
    pub handle_capacity_throttling: bool,
    pub other_requests_wait: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retry_attempts: 5,
            rest_retry_limit: 5,
            max_wait_time: Duration::from_secs(65),
            max_retry_after: Duration::from_secs(60),
            backoff_start: Duration::from_secs(1),
            backoff_factor: 1.5,
            backoff_cap: Duration::from_secs(20),
            retry_after_padding: Duration::from_millis(100),
            handle_rate_limits: true,
            handle_capacity_throttling: true,
            other_requests_wait: true,
        }
    }
}

/// Extracts retry-after duration from HTTP response headers or Epic API error payloads.
///
/// Precedence:
/// 1. `Retry-After` HTTP header (integer seconds)
/// 2. `messageVars[0]` when numeric
/// 3. `"in <N> second"` inside `errorMessage`
pub fn extract_retry_after(
    headers: &HeaderMap,
    api_error: Option<&EpicgamesAPIErrorData>,
) -> Option<Duration> {
    if let Some(val) = headers.get(reqwest::header::RETRY_AFTER)
        && let Ok(text) = val.to_str()
        && let Ok(secs) = text.trim().parse::<u64>()
    {
        return Some(Duration::from_secs(secs));
    }

    let err = api_error?;
    if let Some(first_var) = err.message_vars.first()
        && let Ok(secs) = first_var.trim().parse::<u64>()
    {
        return Some(Duration::from_secs(secs));
    }

    extract_in_seconds(&err.error_message).map(Duration::from_secs)
}

fn extract_in_seconds(message: &str) -> Option<u64> {
    let mut search_from = 0;
    while let Some(offset) = message[search_from..].find("in ") {
        let num_start = search_from + offset + 3;
        let rest = &message[num_start..];
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if !digits.is_empty() {
            let after_digits = &rest[digits.len()..];
            if after_digits.starts_with(" second")
                && let Ok(secs) = digits.parse::<u64>()
            {
                return Some(secs);
            }
        }
        search_from = num_start;
    }
    None
}

#[derive(Debug, Clone)]
struct BarrierState {
    notify: Arc<Notify>,
    ends_at: Instant,
}

/// Coordinates global and per-account rate-limit backoff barriers across concurrent requests.
#[derive(Debug, Default)]
pub struct EndpointRateLimiter {
    active: DashMap<ThrottleKey, BarrierState>,
}

/// RAII guard releasing waiting requests on a throttled endpoint when dropped.
pub struct ThrottleBarrierGuard<'a> {
    limiter: &'a EndpointRateLimiter,
    key: ThrottleKey,
    notify: Arc<Notify>,
}

impl Drop for ThrottleBarrierGuard<'_> {
    fn drop(&mut self) {
        self.limiter
            .active
            .remove_if(&self.key, |_, existing| Arc::ptr_eq(&existing.notify, &self.notify));
        self.notify.notify_waiters();
    }
}

impl EndpointRateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Blocks until any active rate-limit window on `key` clears.
    pub async fn wait_if_limited(&self, key: &ThrottleKey) {
        loop {
            let state = match self.active.get(key) {
                Some(entry) => entry.clone(),
                None => return,
            };

            let now = Instant::now();
            if now >= state.ends_at {
                self.active
                    .remove_if(key, |_, current| Arc::ptr_eq(&current.notify, &state.notify));
                state.notify.notify_waiters();
                return;
            }

            let notified = state.notify.notified();
            tokio::pin!(notified);
            tokio::select! {
                _ = &mut notified => {}
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(state.ends_at)) => {}
            }
        }
    }

    /// Registers a rate-limit cooldown window of `duration`, returning an RAII guard
    /// that wakes waiters on completion or cancellation.
    pub fn enter_cooldown(
        &self,
        key: ThrottleKey,
        duration: Duration,
    ) -> ThrottleBarrierGuard<'_> {
        let notify = Arc::new(Notify::new());
        let ends_at = Instant::now() + duration;

        if let Some(prev) = self.active.insert(
            key.clone(),
            BarrierState {
                notify: Arc::clone(&notify),
                ends_at,
            },
        ) {
            prev.notify.notify_waiters();
        }

        ThrottleBarrierGuard {
            limiter: self,
            key,
            notify,
        }
    }
}
