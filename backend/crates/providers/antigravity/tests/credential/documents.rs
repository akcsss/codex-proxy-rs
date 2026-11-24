//! 导入使用真实 OAuth 交换边界，导出可重导入且凭据轮换保持租约与主体约束

use super::{
    CredentialService,
    oauth::{GoogleOAuth, OAuthRegistration},
};
use crate::support::account_record as account;
use crate::support::{Fixture, MemoryPorts};
use chrono::{Duration, Utc};
use gateway_admin::{
    model::{
        Revision,
        accounts::AccountRecord,
        provider_credentials::{
            PrepareCredentialImport, PrepareCredentialRotation, ProviderDocument,
            ProviderExportCredentialInput,
        },
    },
    ports::provider::ProviderAdminErrorKind as Kind,
};
use gateway_core::{
    account::{CredentialState, OpaqueProviderData, QuotaState},
    routing::ProviderKind,
};
use serde_json::{Value, json};
use std::sync::{Arc, atomic::Ordering};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_string_contains, header, method, path},
};

async fn fixture() -> (MockServer, CredentialService, Arc<MemoryPorts>) {
    let server = MockServer::start().await;
    let origin = server.uri();
    let config: OAuthRegistration = serde_json::from_value(json!({
        "client_id":"fixture-client", "client_secret":"fixture-secret",
        "redirect_uri":"http://127.0.0.1:8020/callback",
        "resource_scopes":["fixture-resource"], "resource_origin":origin,
    }))
    .unwrap();
    let store = Fixture::new(&origin).store;
    let credentials = CredentialService {
        kind: ProviderKind::new("antigravity").unwrap(),
        accounts: store.clone(),
        leases: store.clone(),
        runtime_policy: store.clone(),
        credential_state: store.clone(),
        oauth: Arc::new(GoogleOAuth::with_endpoints(
            config,
            &format!("{origin}/token"),
            &format!("{origin}/userinfo"),
        )),
    };
    (server, credentials, store)
}

fn document(value: Value) -> ProviderDocument {
    ProviderDocument::new(OpaqueProviderData::new(value.as_object().unwrap().clone()))
}

fn import(document: ProviderDocument) -> PrepareCredentialImport {
    PrepareCredentialImport {
        default_outbound_proxy: None,
        document,
    }
}

fn file() -> Value {
    json!({"provider":"antigravity","version":1,"accounts":[{"name":"Local name","refreshToken":"fixture-refresh"}]})
}

async fn token(server: &MockServer, response: Value) {
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=refresh_token"))
        .and(body_string_contains("client_id=fixture-client"))
        .and(body_string_contains("client_secret=fixture-secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .expect(1)
        .mount(server)
        .await;
}

async fn identity(server: &MockServer, subject: &str) {
    Mock::given(method("GET"))
        .and(path("/userinfo"))
        .and(header("authorization", "Bearer fresh-access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "sub":subject, "email":"verified@example.org", "email_verified":true,
        })))
        .expect(1)
        .mount(server)
        .await;
}

fn token_response() -> Value {
    json!({"access_token":"fresh-access","token_type":"Bearer","expires_in":3600,"scope":"openid email fixture-resource"})
}

#[tokio::test]
async fn expired_access_exports_as_refreshable_file_and_reimports_with_local_name() {
    let (server, credentials, _) = fixture().await;
    let material = document(json!({
        "registration":credentials.oauth.registration(), "subject":"fixture-subject",
        "access_token":"expired-access", "refresh_token":"fixture-refresh", "scopes":["untrusted-scope"],
    }));
    let exported = credentials
        .export_credentials(vec![ProviderExportCredentialInput {
            account: account(),
            provider_material: material,
        }])
        .unwrap();
    assert!(!format!("{exported:?}").contains("fixture-refresh"));
    assert_eq!(
        Value::Object(exported.document.clone().into_provider_data().into_inner()),
        file()
    );
    token(&server, token_response()).await;
    identity(&server, "fixture-subject").await;
    let result = credentials
        .prepare_import(import(exported.document))
        .await
        .unwrap();
    let loaded = &result.credentials[0];
    assert_eq!(loaded.name, "Local name");
    assert_eq!(loaded.email.as_deref(), Some("verified@example.org"));
    assert_eq!(loaded.upstream_user_id.as_deref(), Some("fixture-subject"));
    assert!(loaded.has_refresh_token && loaded.access_token_expires_at.unwrap() > Utc::now());
    let stored = loaded
        .provider_material
        .clone()
        .into_provider_data()
        .into_inner();
    assert_eq!(stored["access_token"], "fresh-access");
    assert_eq!(stored["refresh_token"], "fixture-refresh");
    assert_eq!(
        stored["scopes"],
        json!(["openid", "email", "fixture-resource"])
    );
}

#[tokio::test]
async fn malformed_files_fail_before_any_refresh() {
    let (server, credentials, _) = fixture().await;
    let mut inputs = vec![];
    for (key, value) in [
        ("provider", json!("openai")),
        ("version", json!(2)),
        ("accounts", json!([])),
    ] {
        let mut input = file();
        input[key] = value;
        inputs.push(input);
    }
    let mut forged = file();
    forged["accounts"][0]["subject"] = json!("forged");
    inputs.push(forged);
    let mut duplicate = file();
    duplicate["accounts"]
        .as_array_mut()
        .unwrap()
        .push(file()["accounts"][0].clone());
    inputs.push(duplicate);
    let mut missing_offline = file();
    missing_offline["accounts"][0]["refreshToken"] = json!("");
    inputs.push(missing_offline);
    for input in inputs {
        assert_eq!(
            credentials
                .prepare_import(import(document(input)))
                .await
                .unwrap_err()
                .kind(),
            Kind::Invalid
        );
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn imported_grants_cannot_inherit_missing_or_reduced_scopes() {
    let (server, credentials, _) = fixture().await;
    for scope in [Value::Null, json!("openid email")] {
        server.reset().await;
        let mut response = token_response();
        response["scope"] = scope;
        token(&server, response).await;
        assert_eq!(
            credentials
                .prepare_import(import(document(file())))
                .await
                .unwrap_err()
                .kind(),
            Kind::Invalid
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn upstream_identity_failure_does_not_prepare_an_import() {
    let (server, credentials, _) = fixture().await;
    token(&server, token_response()).await;
    Mock::given(method("GET"))
        .and(path("/userinfo"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    assert_eq!(
        credentials
            .prepare_import(import(document(file())))
            .await
            .unwrap_err()
            .kind(),
        Kind::Unavailable
    );
}

#[tokio::test]
async fn a_later_failed_grant_rejects_the_whole_import_without_exposing_tokens() {
    let (server, credentials, _) = fixture().await;
    let mut input = file();
    input["accounts"].as_array_mut().unwrap().push(json!({
        "name":"Second account", "refreshToken":"revoked-refresh",
    }));
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("refresh_token=fixture-refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_response()))
        .expect(1)
        .mount(&server)
        .await;
    identity(&server, "fixture-subject").await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("refresh_token=revoked-refresh"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error":"invalid_grant", "error_description":"sensitive-response-marker",
        })))
        .expect(1)
        .mount(&server)
        .await;
    let error = credentials
        .prepare_import(import(document(input)))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), Kind::CredentialRefreshRequired);
    assert!(!format!("{error:?}").contains("sensitive-response-marker"));
    assert!(!format!("{error:?}").contains("revoked-refresh"));
}

#[tokio::test]
async fn different_refresh_tokens_cannot_import_the_same_google_subject_twice() {
    let (server, credentials, _) = fixture().await;
    let mut input = file();
    input["accounts"].as_array_mut().unwrap().push(json!({
        "name":"Duplicate account", "refreshToken":"another-refresh",
    }));
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_response()))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/userinfo"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"sub":"same-subject"})))
        .expect(2)
        .mount(&server)
        .await;
    assert_eq!(
        credentials
            .prepare_import(import(document(input)))
            .await
            .unwrap_err()
            .kind(),
        Kind::Conflict
    );
}

#[tokio::test]
async fn rotation_keeps_subject_and_holds_leases_until_commit() {
    let (server, credentials, store) = fixture().await;
    token(&server, token_response()).await;
    identity(&server, "another-subject").await;
    let request = || PrepareCredentialRotation {
        account: account(),
        provider_material: document(file()["accounts"][0].clone()),
    };
    assert_eq!(
        credentials
            .prepare_rotation(request())
            .await
            .unwrap_err()
            .kind(),
        Kind::Conflict
    );
    assert_eq!(store.active_leases.load(Ordering::SeqCst), 0);
    server.reset().await;
    let mut response = token_response();
    response["refresh_token"] = json!("rotated-refresh");
    token(&server, response).await;
    identity(&server, "fixture-subject").await;
    let prepared = credentials.prepare_rotation(request()).await.unwrap();
    assert_eq!(store.active_leases.load(Ordering::SeqCst), 2);
    assert_eq!(prepared.facts().name, "Fixture");
    let (_, guard) = prepared.into_parts();
    guard.finish();
    assert_eq!(store.active_leases.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn export_rejects_foreign_registration_or_mismatched_subject() {
    let (_, credentials, _) = fixture().await;
    for (registration, subject) in [
        ("wrong-client".to_owned(), "fixture-subject"),
        (credentials.oauth.registration(), "wrong-subject"),
    ] {
        let result = credentials.export_credentials(vec![ProviderExportCredentialInput {
            account: account(), provider_material: document(json!({"registration":registration,"subject":subject,"access_token":"access","refresh_token":"refresh","scopes":[]})),
        }]);
        assert_eq!(result.unwrap_err().kind(), Kind::Invalid);
    }
}
