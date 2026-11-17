use chrono::{Duration as ChronoDuration, Utc};
use rustfn::auth::{
    AuthConfig, AuthEvent, Auth, AuthMethod, DeviceAuthCredentials, DeviceAuthStore,
    DeviceCodeAuth, JsonFileDeviceAuthStore, StoredAuthSession, authenticate_device_code,
    extract_authorization_code, grant_eos_external_auth,
};
use rustfn::http::{AuthSessionSlot, HttpClient, HttpClientConfig, HttpRequestSpec};
use rustfn::protocol::{AccountId, AuthClient, EOS_EAS_DEPLOYMENT_ID, ServiceEndpoints};
use secrecy::{ExposeSecret, SecretString};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;
use wiremock::matchers::{body_string_contains, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn mock_endpoints(server: &MockServer) -> ServiceEndpoints {
    let base = server.uri();
    ServiceEndpoints {
        oauth_token_create: format!("{base}/account/api/oauth/token").into(),
        oauth_token_verify: format!("{base}/account/api/oauth/verify").into(),
        oauth_token_kill: format!("{base}/account/api/oauth/sessions/kill").into(),
        oauth_exchange: format!("{base}/account/api/oauth/exchange").into(),
        oauth_device_code: format!("{base}/account/api/oauth/deviceAuthorization").into(),
        account_public: format!("{base}/account/api/public/account").into(),
        init_eula: format!("{base}/eulatracking/api/public/agreements/fn").into(),
        init_request_access: format!("{base}/fortnite/api/storeaccess/v1/request_access").into(),
        eos_token: format!("{base}/epic/oauth/v2/token").into(),
        eos_token_info: format!("{base}/epic/oauth/v2/tokenInfo").into(),
        eos_token_revoke: format!("{base}/epic/oauth/v2/revoke").into(),
        eos_users: format!("{base}/auth/v1/users").into(),
        ..ServiceEndpoints::default()
    }
}

#[test]
fn authorization_code_extractor_handles_raw_and_urls() {
    let raw = extract_authorization_code("0123456789abcdef0123456789abcdef").unwrap();
    assert_eq!(raw.expose_secret(), "0123456789abcdef0123456789abcdef");

    let url = extract_authorization_code(
        "https://www.epicgames.com/id/api/redirect?code=fedcba9876543210fedcba9876543210",
    )
    .unwrap();
    assert_eq!(url.expose_secret(), "fedcba9876543210fedcba9876543210");
}

#[tokio::test]
async fn device_auth_full_bootstrap_refresh_and_revoke() {
    let server = MockServer::start().await;
    let account_id = "11112222333344445555666677778888";
    let puid = "99990000111122223333444455556666";
    let expires = (Utc::now() + ChronoDuration::hours(4)).to_rfc3339();
    let refresh_expires = (Utc::now() + ChronoDuration::days(30)).to_rfc3339();

    Mock::given(method("POST"))
        .and(path("/account/api/oauth/token"))
        .and(body_string_contains("grant_type=device_auth"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "fn-access-1",
            "expires_in": 14400,
            "expires_at": expires,
            "token_type": "bearer",
            "refresh_token": "fn-refresh-1",
            "refresh_expires": 2592000,
            "refresh_expires_at": refresh_expires,
            "account_id": account_id,
            "client_id": AuthClient::FortniteAndroidGameClient.client_id(),
            "internal_client": true,
            "client_service": "fortnite",
            "displayName": "RustBot",
            "app": "fortnite",
            "in_app_id": account_id,
            "device_id": "dev-123"
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/account/api/oauth/token"))
        .and(body_string_contains("grant_type=refresh_token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "fn-access-2",
            "expires_in": 14400,
            "expires_at": expires,
            "token_type": "bearer",
            "refresh_token": "fn-refresh-2",
            "refresh_expires": 2592000,
            "refresh_expires_at": refresh_expires,
            "account_id": account_id,
            "client_id": AuthClient::FortniteAndroidGameClient.client_id(),
            "displayName": "RustBot"
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/account/api/oauth/token"))
        .and(body_string_contains("grant_type=client_credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "fn-cc-1",
            "expires_in": 14400,
            "expires_at": expires,
            "token_type": "bearer",
            "client_id": AuthClient::FortniteAndroidGameClient.client_id(),
            "internal_client": true,
            "client_service": "fortnite",
            "product_id": "prod-fn",
            "application_id": "app-fn"
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path(format!("/fortnite/api/storeaccess/v1/request_access/{account_id}")))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "errorCode": "errors.com.epicgames.bad_request",
            "errorMessage": "Already has access"
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/eulatracking/api/public/agreements/fn/account/{account_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "version": 12,
            "locale": "en"
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path(format!(
            "/eulatracking/api/public/agreements/fn/version/12/account/{account_id}/accept"
        )))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/account/api/oauth/exchange"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "expiresInSeconds": 300,
            "code": "exchange-xyz",
            "creatingClientId": AuthClient::FortniteAndroidGameClient.client_id()
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/epic/oauth/v2/token"))
        .and(body_string_contains("grant_type=exchange_code"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "eos-access-1",
            "token_type": "bearer",
            "expires_in": 3600,
            "expires_at": expires,
            "refresh_token": "eos-refresh-1",
            "refresh_expires_at": refresh_expires,
            "account_id": account_id,
            "product_user_id": puid,
            "client_id": AuthClient::FortniteAndroidGameClient.client_id(),
            "deployment_id": "62a9473a2dca46b29ccf17577fcf42d7",
            "merged_accounts": [],
            "scope": "basic_profile friends_list presence openid"
        })))
        .mount(&server)
        .await;

    // Simulate expired bearer on first protected call triggering automatic refresh.
    Mock::given(method("GET"))
        .and(path("/party/probe"))
        .and(header("authorization", "bearer fn-access-1"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "errorCode": "errors.com.epicgames.common.oauth.invalid_token",
            "errorMessage": "Expired token"
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/party/probe"))
        .and(header("authorization", "bearer fn-access-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "ok": true })))
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/account/api/oauth/sessions/kill/fn-access-2"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/account/api/oauth/sessions/kill/fn-cc-1"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/epic/oauth/v2/revoke"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let auth = Auth::new(
        AuthConfig {
            endpoints: mock_endpoints(&server),
            ..AuthConfig::default()
        },
        HttpClientConfig::default(),
    )
    .unwrap();

    let mut events = auth.subscribe_events();

    let creds = DeviceAuthCredentials::new(
        AccountId::parse(account_id).unwrap(),
        "dev-123",
        SecretString::from("dev-secret"),
    );

    auth.authenticate(AuthMethod::DeviceAuth(creds))
        .await
        .expect("bootstrap succeeds");

    assert_eq!(auth.display_name().await.as_deref(), Some("RustBot"));
    assert_eq!(
        auth.product_user_id().await.as_ref().map(|p| p.as_str()),
        Some(puid)
    );

    let res: serde_json::Value = auth
        .http_client()
        .execute_json(
            HttpRequestSpec::get(format!("{}/party/probe", server.uri()))
                .with_auth(AuthSessionSlot::Fortnite),
        )
        .await
        .expect("401 triggers auto-refresh and retry");
    assert_eq!(res, json!({ "ok": true }));

    let evt = events.recv().await.expect("refresh event emitted");
    assert!(matches!(
        evt,
        AuthEvent::SessionRefreshed {
            slot: AuthSessionSlot::Fortnite,
            ..
        }
    ));

    auth.revoke_all().await.expect("revoke_all succeeds");
    assert!(auth.store().all_snapshots().await.is_empty());
}

#[tokio::test]
async fn failed_revocation_keeps_token_for_retry() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/account/api/oauth/sessions/kill/test-token"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;

    let mut http_config = HttpClientConfig::default();
    http_config.retry.rest_retry_limit = 0;
    let auth = Auth::new(
        AuthConfig {
            endpoints: mock_endpoints(&server),
            ..AuthConfig::default()
        },
        http_config,
    )
    .expect("auth client");
    auth.store()
        .insert_session(StoredAuthSession {
            slot: AuthSessionSlot::Fortnite,
            client: AuthClient::FortniteAndroidGameClient,
            access_token: SecretString::from("test-token".to_owned()),
            expires_at: Utc::now() + ChronoDuration::hours(1),
            refresh_token: None,
            refresh_expires_at: None,
            account_id: None,
            display_name: None,
            device_id: None,
            product_user_id: None,
            deployment_id: None,
            scope: Vec::new(),
            eos_base_payload: Default::default(),
        })
        .await;

    assert!(auth.revoke_all().await.is_err());
    assert!(
        auth.store()
            .get_session_unguarded(AuthSessionSlot::Fortnite)
            .await
            .is_some()
    );
}

#[tokio::test]
async fn device_code_flow_polls_pending_and_prompts_user() {
    let server = MockServer::start().await;
    let poll_count = Arc::new(AtomicUsize::new(0));
    let poll_count_clone = Arc::clone(&poll_count);
    let expires = (Utc::now() + ChronoDuration::hours(2)).to_rfc3339();

    Mock::given(method("POST"))
        .and(path("/account/api/oauth/token"))
        .and(body_string_contains("grant_type=client_credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "cc-device-init",
            "expires_in": 3600,
            "expires_at": expires,
            "token_type": "bearer"
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/account/api/oauth/deviceAuthorization"))
        .and(header("authorization", "bearer cc-device-init"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "user_code": "USER-1234",
            "device_code": "dev-code-abc",
            "verification_uri": "https://epicgames.com/activate",
            "verification_uri_complete": "https://epicgames.com/activate?userCode=USER-1234",
            "prompt": "login",
            "expires_in": 600,
            "interval": 1
        })))
        .mount(&server)
        .await;

    let expires_poll = expires.clone();
    Mock::given(method("POST"))
        .and(path("/account/api/oauth/token"))
        .and(body_string_contains("grant_type=device_code"))
        .respond_with(move |_req: &wiremock::Request| {
            let n = poll_count_clone.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                ResponseTemplate::new(400).set_body_json(json!({
                    "errorCode": "errors.com.epicgames.account.oauth.authorization_pending",
                    "errorMessage": "Authorization pending"
                }))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({
                    "access_token": "switch-devcode-access",
                    "expires_in": 14400,
                    "expires_at": expires_poll,
                    "token_type": "bearer",
                    "refresh_token": "switch-devcode-refresh",
                    "account_id": "aaaa1111bbbb2222cccc3333dddd4444",
                    "displayName": "SwitchTempName"
                }))
            }
        })
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/account/api/oauth/exchange"))
        .and(header("authorization", "bearer switch-devcode-access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "switch-to-android-exchange",
            "creatingClientId": "98f7e42c2e3a4f86a74eb43fbb41ed39",
            "expiresInSeconds": 300
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/account/api/oauth/token"))
        .and(body_string_contains("grant_type=exchange_code"))
        .and(body_string_contains("exchange_code=switch-to-android-exchange"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "fn-devcode-access",
            "expires_in": 14400,
            "expires_at": expires,
            "token_type": "bearer",
            "refresh_token": "fn-devcode-refresh",
            "account_id": "aaaa1111bbbb2222cccc3333dddd4444",
            "displayName": "DeviceCodeBot"
        })))
        .mount(&server)
        .await;

    let endpoints = mock_endpoints(&server);
    let auth = Auth::new(
        AuthConfig {
            endpoints: endpoints.clone(),
            check_eula: false,
            create_eos_session: false,
            ..AuthConfig::default()
        },
        HttpClientConfig::default(),
    )
    .unwrap();

    let captured_uri = Arc::new(Mutex::new(None::<String>));
    let captured_clone = Arc::clone(&captured_uri);

    auth.authenticate_with_device_prompt(
        AuthMethod::DeviceCodeAuth(DeviceCodeAuth::headless()),
        tokio_util::sync::CancellationToken::new(),
        move |prompt| async move {
            *captured_clone.lock().await = prompt.verification_uri_complete;
        },
    )
    .await
    .expect("device code auth succeeds after pending poll");

    assert_eq!(
        captured_uri.lock().await.as_deref(),
        Some("https://epicgames.com/activate?userCode=USER-1234")
    );
    assert_eq!(auth.display_name().await.as_deref(), Some("DeviceCodeBot"));
    assert_eq!(poll_count.load(Ordering::SeqCst), 2);

    let direct_http = HttpClient::new(HttpClientConfig::default()).unwrap();
    let direct_resp = authenticate_device_code(
        &direct_http,
        &endpoints,
        AuthClient::FortniteAndroidGameClient,
        &tokio_util::sync::CancellationToken::new(),
        |_| async {},
    )
    .await
    .expect("authenticate_device_code helper completes switch->client exchange");
    assert_eq!(direct_resp.display_name.as_deref(), Some("DeviceCodeBot"));
}

#[tokio::test]
async fn device_code_flow_requires_user_to_complete_dob_correction() {
    let server = MockServer::start().await;
    let expires = (Utc::now() + ChronoDuration::hours(2)).to_rfc3339();

    Mock::given(method("POST"))
        .and(path("/account/api/oauth/token"))
        .and(body_string_contains("grant_type=client_credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "cc-dob-token",
            "expires_in": 3600,
            "expires_at": expires,
            "token_type": "bearer"
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/account/api/oauth/deviceAuthorization"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "user_code": "USER-DOB",
            "device_code": "dev-code-dob",
            "verification_uri": "https://epicgames.com/activate",
            "verification_uri_complete": "https://epicgames.com/activate?userCode=USER-DOB",
            "prompt": "login",
            "expires_in": 600,
            "interval": 1
        })))
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path("/account/api/public/corrections/dateOfBirth"))
        .and(header("authorization", "bearer cc-dob-token"))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/account/api/oauth/token"))
        .and(body_string_contains("grant_type=device_code"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "errorCode": "errors.com.epicgames.oauth.corrective_action_required",
            "errorMessage": "Corrective action required",
            "correctiveAction": "DATE_OF_BIRTH",
            "continuation": "dob-cont-123"
        })))
        .mount(&server)
        .await;

    let auth = Auth::new(
        AuthConfig {
            endpoints: mock_endpoints(&server),
            check_eula: false,
            create_eos_session: false,
            ..AuthConfig::default()
        },
        HttpClientConfig::default(),
    )
    .unwrap();

    let error = auth.authenticate_device_code(DeviceCodeAuth::headless())
        .await
        .expect_err("DOB correction needs the account owner");
    assert!(error.to_string().contains("Corrective action required"));
}

#[tokio::test]
async fn json_file_device_auth_store_roundtrip_atomic() {
    let unique = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let dir = std::env::temp_dir().join(format!("rustfn_device_auth_test_{unique}"));
    let store = JsonFileDeviceAuthStore::new(&dir);
    let account_id = AccountId::parse("aaaa1111bbbb2222cccc3333dddd4444").unwrap();

    assert!(store.load(&account_id).await.unwrap().is_none());

    let creds = DeviceAuthCredentials::new(
        account_id.clone(),
        "dev-1",
        SecretString::from("secret-v1"),
    );
    store.save(&creds).await.unwrap();

    let loaded = store.load(&account_id).await.unwrap().expect("creds stored");
    assert_eq!(loaded.account_id, account_id);
    assert_eq!(loaded.device_id, "dev-1");
    assert_eq!(loaded.secret.expose_secret(), "secret-v1");

    let updated = DeviceAuthCredentials::new(
        account_id.clone(),
        "dev-2",
        SecretString::from("secret-v2"),
    );
    store.save(&updated).await.unwrap();
    let reloaded = store.load(&account_id).await.unwrap().expect("creds updated");
    assert_eq!(reloaded.device_id, "dev-2");
    assert_eq!(reloaded.secret.expose_secret(), "secret-v2");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = tokio::fs::metadata(dir.join(format!("{account_id}.json")))
            .await
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    let other_id = AccountId::parse("bbbb1111cccc2222dddd3333eeee4444").unwrap();
    store
        .save(&DeviceAuthCredentials::new(
            other_id.clone(),
            "other-device",
            SecretString::from("other-secret"),
        ))
        .await
        .unwrap();
    tokio::fs::rename(
        dir.join(format!("{other_id}.json")),
        dir.join(format!("{account_id}.json")),
    )
    .await
    .unwrap();
    assert!(store.load(&account_id).await.is_err());

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn eos_external_auth_user_not_found_continues_auth_v1_users() {
    let server = MockServer::start().await;
    let endpoints = mock_endpoints(&server);
    let expires_at = (Utc::now() + ChronoDuration::hours(2)).to_rfc3339();

    Mock::given(method("POST"))
        .and(path("/epic/oauth/v2/token"))
        .and(body_string_contains("grant_type=external_auth"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "errorCode": "errors.com.epicgames.eos.auth.user_not_found",
            "numericErrorCode": 1001,
            "errorMessage": "EOS user not found",
            "continuation_token": "eos-cont-jwt-999"
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/auth/v1/users"))
        .and(header("authorization", "Bearer eos-cont-jwt-999"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "eos-cont-access",
            "token_type": "bearer",
            "expires_in": 7200,
            "expires_at": expires_at,
            "refresh_token": "eos-cont-refresh",
            "account_id": "aaaa1111bbbb2222cccc3333dddd4444",
            "product_user_id": "0002continuedpuid000000000000001",
            "client_id": AuthClient::FortniteAndroidGameClient.client_id(),
            "deployment_id": EOS_EAS_DEPLOYMENT_ID,
            "scope": ["basic_profile"]
        })))
        .mount(&server)
        .await;

    let http = HttpClient::new(HttpClientConfig::default()).unwrap();
    let ext_token = SecretString::from("eas-external-token-1");
    let tok = grant_eos_external_auth(
        &http,
        &endpoints,
        AuthClient::FortniteAndroidGameClient,
        &ext_token,
        EOS_EAS_DEPLOYMENT_ID,
    )
    .await
    .expect("continuation succeeds");

    assert_eq!(tok.access_token.expose_secret(), "eos-cont-access");
    assert_eq!(
        tok.product_user_id.as_ref().map(|p| p.as_str()),
        Some("0002continuedpuid000000000000001")
    );
}
