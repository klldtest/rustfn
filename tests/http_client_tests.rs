use reqwest::header::HeaderMap;
use rustfn::http::{
    AuthSessionSlot, BoxFuture, ChatMessageMetadata, ChatSendMessageBody, ChatSendMessageRequest,
    ChatSendMessageResponse, HTTP, HttpClient, HttpClientConfig, HttpRequestSpec,
    PublicKeyResponse, RetryConfig, RetryDecision, TokenProvider, extract_retry_after,
    sanitize_route_template, EndpointRateLimiter, ThrottleKey,
};
use rustfn::protocol::{
    AccountId, EpicgamesAPIErrorData, ExtraServiceEndpoints, RustfnError, ServiceEndpoints,
};
use secrecy::SecretString;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(Debug, Deserialize, PartialEq)]
struct PingPayload {
    ok: bool,
    attempt: usize,
}

#[tokio::test]
async fn overlapping_cooldowns_keep_the_latest_deadline() {
    let limiter = EndpointRateLimiter::new();
    let key = ThrottleKey::from_request(&reqwest::Method::GET, "https://example.com/test", None, None);
    let short = limiter.enter_cooldown(key.clone(), Duration::from_millis(40));
    let long = limiter.enter_cooldown(key.clone(), Duration::from_millis(180));
    drop(short);
    assert!(tokio::time::timeout(Duration::from_millis(80), limiter.wait_if_limited(&key)).await.is_err());
    tokio::time::timeout(Duration::from_secs(1), limiter.wait_if_limited(&key)).await.unwrap();
    drop(long);
}

struct MockTokenProvider {
    current: Mutex<String>,
    refresh_calls: AtomicUsize,
}

impl MockTokenProvider {
    fn new(initial: &str) -> Self {
        Self {
            current: Mutex::new(initial.to_owned()),
            refresh_calls: AtomicUsize::new(0),
        }
    }
}

impl TokenProvider for MockTokenProvider {
    fn bearer_token(&self, _slot: AuthSessionSlot) -> BoxFuture<'_, Result<SecretString, RustfnError>> {
        Box::pin(async move {
            let guard = self.current.lock().await;
            Ok(SecretString::from(guard.clone()))
        })
    }

    fn refresh_on_invalid_token<'a>(
        &'a self,
        _slot: AuthSessionSlot,
        failed_token: &'a str,
    ) -> BoxFuture<'a, Result<SecretString, RustfnError>> {
        Box::pin(async move {
            let mut guard = self.current.lock().await;
            if guard.as_str() == failed_token {
                self.refresh_calls.fetch_add(1, Ordering::SeqCst);
                *guard = "fresh-token-v2".to_owned();
            }
            Ok(SecretString::from(guard.clone()))
        })
    }
}

fn fast_test_http_config() -> HttpClientConfig {
    HttpClientConfig {
        retry: RetryConfig {
            backoff_start: Duration::from_millis(20),
            backoff_cap: Duration::from_millis(100),
            retry_after_padding: Duration::from_millis(10),
            max_wait_time: Duration::from_secs(10),
            ..RetryConfig::default()
        },
        ..HttpClientConfig::default()
    }
}

#[test]
fn retry_after_extraction_precedence() {
    let mut headers = HeaderMap::new();
    headers.insert("retry-after", "3".parse().unwrap());
    let api_err = EpicgamesAPIErrorData {
        error_message: "Operation throttled, retry in 9 seconds".into(),
        message_vars: vec!["5".into()],
        ..Default::default()
    };

    assert_eq!(
        extract_retry_after(&headers, Some(&api_err)),
        Some(Duration::from_secs(3))
    );

    headers.clear();
    assert_eq!(
        extract_retry_after(&headers, Some(&api_err)),
        Some(Duration::from_secs(5))
    );

    let msg_only = EpicgamesAPIErrorData {
        error_message: "Rate limit hit; try again in 7 seconds.".into(),
        ..Default::default()
    };
    assert_eq!(
        extract_retry_after(&headers, Some(&msg_only)),
        Some(Duration::from_secs(7))
    );
}

#[tokio::test]
async fn recovers_from_5xx_and_429_rate_limits() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_clone = Arc::clone(&hits);

    Mock::given(method("GET"))
        .and(path("/status-sequence"))
        .respond_with(move |_req: &wiremock::Request| {
            let n = hits_clone.fetch_add(1, Ordering::SeqCst);
            match n {
                0 => ResponseTemplate::new(503).set_body_string("upstream busy"),
                1 => ResponseTemplate::new(429)
                    .insert_header("Retry-After", "0")
                    .set_body_json(json!({
                        "errorCode": "errors.com.epicgames.common.throttled",
                        "errorMessage": "Throttled",
                        "messageVars": ["0"]
                    })),
                _ => ResponseTemplate::new(200).set_body_json(json!({
                    "ok": true,
                    "attempt": n + 1
                })),
            }
        })
        .mount(&server)
        .await;

    let client = HttpClient::new(fast_test_http_config()).unwrap();
    let res: PingPayload = client
        .execute_json(HttpRequestSpec::get(format!("{}/status-sequence", server.uri())))
        .await
        .expect("should recover on 3rd attempt");

    assert_eq!(res, PingPayload { ok: true, attempt: 3 });
    assert_eq!(hits.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn refreshes_bearer_token_once_on_invalid_token() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/protected"))
        .and(header("authorization", "bearer stale-token-v1"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "errorCode": "errors.com.epicgames.common.oauth.invalid_token",
            "errorMessage": "Token expired"
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/protected"))
        .and(header("authorization", "bearer fresh-token-v2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ok": true,
            "attempt": 2
        })))
        .mount(&server)
        .await;

    let provider = Arc::new(MockTokenProvider::new("stale-token-v1"));
    let client = HttpClient::new(fast_test_http_config())
        .unwrap()
        .with_token_provider(provider.clone());

    let res: PingPayload = client
        .execute_json(
            HttpRequestSpec::get(format!("{}/protected", server.uri()))
                .with_auth(AuthSessionSlot::Fortnite),
        )
        .await
        .expect("request succeeds after token refresh");

    assert_eq!(res, PingPayload { ok: true, attempt: 2 });
    assert_eq!(provider.refresh_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retry_decision_hook_can_abandon_or_throw() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/limited"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "1")
                .set_body_json(json!({
                    "errorCode": "errors.com.epicgames.common.throttled",
                    "errorMessage": "Throttled",
                    "messageVars": ["1"]
                })),
        )
        .mount(&server)
        .await;

    let client = HttpClient::new(fast_test_http_config()).unwrap();

    let abandon_err = client
        .execute_empty(
            HttpRequestSpec::post(format!("{}/limited", server.uri()))
                .with_retry_decision(Arc::new(|| RetryDecision::Abandon)),
        )
        .await
        .unwrap_err();
    assert!(matches!(abandon_err, RustfnError::Cancelled));

    let throw_err = client
        .execute_empty(
            HttpRequestSpec::post(format!("{}/limited", server.uri()))
                .with_retry_decision(Arc::new(|| RetryDecision::Throw)),
        )
        .await
        .unwrap_err();
    assert!(matches!(throw_err, RustfnError::EpicApi { status: 429, .. }));
}

#[test]
fn route_sanitizer_strips_entity_ids_and_queries() {
    let raw = "https://party-service-prod.ol.epicgames.com/party/api/v1/Fortnite/parties/11112222333344445555666677778888/members/aaaa1111bbbb2222cccc3333dddd4444?sendPing=true";
    assert_eq!(
        sanitize_route_template(raw),
        "https://party-service-prod.ol.epicgames.com/party/api/v1/Fortnite/parties/{id}/members/{id}"
    );
}

#[tokio::test]
async fn register_public_key_parses_live_epic_schema() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/publickey/v2/publickey"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "key": "dGVzdC1wdWIta2V5",
            "account_id": "0ea5bd466d6743cc8a41d1a0f69bcd59",
            "key_guid": "guid-live-123",
            "kid": "kid-live-123",
            "expiration": "2026-11-17T00:00:00.000Z",
            "jwt": "eyJhbGciOiJFZERTQSJ9.test.sig",
            "type": "ed25519"
        })))
        .mount(&server)
        .await;

    let provider = Arc::new(MockTokenProvider::new("valid-token"));
    let client = HttpClient::new(fast_test_http_config())
        .unwrap()
        .with_token_provider(provider);
    let endpoints = ServiceEndpoints {
        publickey: format!("{}/publickey", server.uri()).into(),
        ..Default::default()
    };
    let api = HTTP::new(client, endpoints, ExtraServiceEndpoints::default());

    let parsed: PublicKeyResponse = api
        .register_public_key("dGVzdC1wdWIta2V5", false)
        .await
        .expect("live Epic publickey schema deserializes");

    assert_eq!(parsed.key, "dGVzdC1wdWIta2V5");
    assert_eq!(parsed.algorithm, "ed25519");
    assert_eq!(parsed.jwt, "eyJhbGciOiJFZERTQSJ9.test.sig");
    assert_eq!(parsed.key_guid.as_deref(), Some("guid-live-123"));
}

#[tokio::test]
async fn send_eos_conversation_message_accepts_204_empty_body() {
    let server = MockServer::start().await;
    let sender_id = AccountId::from_raw("0ea5bd466d6743cc8a41d1a0f69bcd59");

    Mock::given(method("POST"))
        .and(path("/epic/chat/v1/public/_/conversations/ep-party1/messages"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let provider = Arc::new(MockTokenProvider::new("valid-eos-token"));
    let client = HttpClient::new(fast_test_http_config())
        .unwrap()
        .with_token_provider(provider);
    let endpoints = ServiceEndpoints {
        eos_chat: format!("{}/epic/chat", server.uri()).into(),
        ..Default::default()
    };
    let api = HTTP::new(client, endpoints, ExtraServiceEndpoints::default());

    let req = ChatSendMessageRequest {
        allowed_recipients: vec![AccountId::from_raw("1d284c5b84ca4f349a2e3334f1dd0236")],
        message: ChatSendMessageBody {
            body: "pong".to_owned(),
        },
        is_reportable: false,
        metadata: ChatMessageMetadata {
            tm_v: "2".to_owned(),
            pub_jwt: "jwt".to_owned(),
            sig: "sig".to_owned(),
            npm: Some("1".to_owned()),
            plf_nm: "WIN".to_owned(),
            plf_id: String::new(),
        },
    };

    let resp = api
        .send_eos_conversation_message("_", "ep-party1", &sender_id, &req)
        .await
        .expect("HTTP 204 empty body returns default ChatSendMessageResponse");

    assert_eq!(resp, ChatSendMessageResponse::default());
}
