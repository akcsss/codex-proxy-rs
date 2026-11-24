//! 真实 Provider 的发送时机、画像、取消与错误诊断合同

use crate::{
    profile::{Selection, object},
    support::{Fixture, context, request},
};
use futures::{StreamExt, TryStreamExt};
use gateway_core::{
    account::{
        AccountSelectionPolicy, CredentialState, OpaqueProviderData, QuotaState, RotationStrategy,
    },
    engine::{
        AccountAttemptContext, AttemptContext, ModelRequestId, RequestAttemptContext,
        probe::AccountProbeTarget, provider::Provider,
    },
    error::ProviderErrorKind,
    lifecycle::CancellationToken,
    policy::ClientApiKeyId,
    routing::ProviderKind,
    upstream::UpstreamSendState,
};
use serde_json::json;
use std::{
    num::NonZeroU32,
    sync::atomic::Ordering,
    time::{Duration, SystemTime},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_partial_json, header, method, path},
};

fn frozen_profile() -> OpaqueProviderData {
    Selection::parse(&object(json!({"client":"cli","platform":"linux","versionMode":"fixed","clientVersion":"1.5.14","arch":null}))).unwrap().resolve("9.9.9").frozen()
}

async fn released(fixture: &Fixture) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while fixture.store.active_leases.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stream releases its account lease");
}

#[tokio::test]
async fn returning_and_dropping_unpolled_stream_never_sends_and_releases_lease() {
    let upstream = MockServer::start().await;
    let fixture = Fixture::new(&upstream.uri());
    let stream = fixture
        .provider
        .clone()
        .execute(
            request(false),
            context(frozen_profile(), CancellationToken::new()),
        )
        .await
        .unwrap();
    assert_eq!(fixture.store.active_leases.load(Ordering::SeqCst), 1);
    assert!(upstream.received_requests().await.unwrap().is_empty());
    drop(stream);
    released(&fixture).await;
    assert!(upstream.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn polling_sends_the_frozen_profile_and_authorized_credential() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST")).and(path("/v1internal:generateContent"))
        .and(header("authorization", "Bearer fixture-access"))
        .and(header("user-agent", "antigravity/cli/1.5.14 (aidev_client; os_type=linux; arch=amd64; auth_method=consumer)"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"response":{"responseId":"fixture-response","candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}]}})))
        .expect(1).mount(&upstream).await;
    let fixture = Fixture::new(&upstream.uri());
    let stream = fixture
        .provider
        .clone()
        .execute(
            request(false),
            context(frozen_profile(), CancellationToken::new()),
        )
        .await
        .unwrap();
    assert!(upstream.received_requests().await.unwrap().is_empty());
    let events = stream.try_collect::<Vec<_>>().await.unwrap();
    assert!(events.iter().any(|event| event.has_client_event()));
    released(&fixture).await;
}

#[tokio::test]
async fn cancellation_before_poll_does_not_send_or_leak_the_lease() {
    let upstream = MockServer::start().await;
    let fixture = Fixture::new(&upstream.uri());
    let cancellation = CancellationToken::new();
    let mut stream = fixture
        .provider
        .clone()
        .execute(
            request(false),
            context(frozen_profile(), cancellation.clone()),
        )
        .await
        .unwrap();
    cancellation.cancel();
    let error = stream.next().await.unwrap().expect_err("cancelled stream");
    assert_eq!(error.kind(), ProviderErrorKind::Cancelled);
    assert_eq!(error.send_state(), UpstreamSendState::NotSent);
    drop(stream);
    released(&fixture).await;
    assert!(upstream.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn upstream_rejections_preserve_status_without_exposing_sensitive_diagnostics() {
    for (status, kind, body, has_details) in [
        (
            401,
            ProviderErrorKind::Unauthorized,
            "upstream-sensitive rejection",
            true,
        ),
        (
            429,
            ProviderErrorKind::RateLimited,
            "upstream-sensitive throttle",
            true,
        ),
        (
            429,
            ProviderErrorKind::RateLimited,
            "access_token=fixture-access",
            false,
        ),
    ] {
        let upstream = MockServer::start().await;
        Mock::given(path("/v1internal:generateContent"))
            .respond_with(ResponseTemplate::new(status).set_body_string(body))
            .expect(1)
            .mount(&upstream)
            .await;
        let fixture = Fixture::new(&upstream.uri());
        let stream = fixture
            .provider
            .clone()
            .execute(
                request(false),
                context(frozen_profile(), CancellationToken::new()),
            )
            .await
            .unwrap();
        let error = stream
            .try_collect::<Vec<_>>()
            .await
            .expect_err("upstream rejection");
        assert_eq!(error.kind(), kind);
        assert_eq!(fixture.store.expired.load(Ordering::SeqCst), status == 401);
        assert_eq!(error.upstream_status(), Some(status));
        assert_eq!(error.send_state(), UpstreamSendState::Sent);
        let ordinary = format!("{error:?} {error}");
        assert!(!ordinary.contains("upstream-sensitive"));
        assert!(!ordinary.contains("fixture-access"));
        let details = error.error_details().unwrap_or_default();
        assert_eq!(details.contains("upstream-sensitive"), has_details);
        assert!(!details.contains("fixture-access"));
        released(&fixture).await;
    }
}

fn diagnostic_context(target: AccountProbeTarget) -> AttemptContext {
    let request_context = RequestAttemptContext::new(
        ModelRequestId::new("req_probe").unwrap(),
        ClientApiKeyId::new("key_probe").unwrap(),
    );
    AttemptContext::new(
        request_context,
        NonZeroU32::MIN,
        SystemTime::now() + Duration::from_secs(5),
        AccountSelectionPolicy::new(RotationStrategy::Smart, NonZeroU32::MIN),
        AccountAttemptContext::diagnostic(Default::default(), target, None),
        None,
        CancellationToken::new(),
    )
}

async fn mock_project(upstream: &MockServer) {
    Mock::given(path("/v1internal:loadCodeAssist"))
        .and(header("authorization", "Bearer fixture-access"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"cloudaicompanionProject":"probe-project"})),
        )
        .expect(1)
        .mount(upstream)
        .await;
}

#[tokio::test]
async fn disabled_account_probe_discovers_its_own_project_on_first_poll() {
    let upstream = MockServer::start().await;
    mock_project(&upstream).await;
    Mock::given(path("/v1internal:generateContent"))
        .and(header("authorization", "Bearer fixture-access"))
        .and(body_partial_json(json!({"model":"fixture-model","project":"probe-project"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"response":{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}]}})))
        .expect(1).mount(&upstream).await;
    let fixture = Fixture::new(&upstream.uri());
    let selected = {
        let mut loaded = fixture.store.loaded.lock().unwrap();
        loaded.account = loaded.account.clone().with_account_facts(
            false,
            CredentialState::Ready,
            QuotaState::unknown(),
            None,
            None,
        );
        loaded.account.id().clone()
    };
    fixture.store.catalog.lock().unwrap().take();
    assert!(
        fixture
            .provider
            .query_model_capabilities()
            .await
            .unwrap()
            .is_empty()
    );
    let ordinary = fixture
        .provider
        .clone()
        .execute(
            request(false),
            context(frozen_profile(), CancellationToken::new()),
        )
        .await;
    assert!(matches!(ordinary, Err(error) if error.kind() == ProviderErrorKind::NoEligibleAccount));

    let stream = fixture
        .provider
        .clone()
        .execute(
            request(false),
            diagnostic_context(AccountProbeTarget::ConnectionTest(selected.clone())),
        )
        .await
        .unwrap();
    assert_eq!(stream.metadata().provider_account_id(), &selected);
    assert_eq!(fixture.store.active_leases.load(Ordering::SeqCst), 1);
    assert!(upstream.received_requests().await.unwrap().is_empty());
    let events = stream.try_collect::<Vec<_>>().await.unwrap();
    assert!(events.iter().any(|event| event.has_client_event()));
    released(&fixture).await;
}

#[tokio::test]
async fn recovery_probe_does_not_mutate_credentials_cooldowns_or_scheduling_feedback() {
    for (status, kind) in [
        (401, ProviderErrorKind::Unauthorized),
        (429, ProviderErrorKind::RateLimited),
    ] {
        let upstream = MockServer::start().await;
        mock_project(&upstream).await;
        Mock::given(path("/v1internal:generateContent"))
            .and(body_partial_json(json!({"project":"probe-project"})))
            .respond_with(ResponseTemplate::new(status).insert_header("retry-after", "30"))
            .expect(1)
            .mount(&upstream)
            .await;
        let fixture = Fixture::new(&upstream.uri());
        let account = fixture.store.loaded.lock().unwrap().account.id().clone();
        let probe = diagnostic_context(AccountProbeTarget::FreezeRecovery(account.clone()));
        let stream = fixture
            .provider
            .clone()
            .execute(request(false), probe)
            .await
            .unwrap();
        let error = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert_eq!(error.kind(), kind);
        assert_eq!(error.upstream_status(), Some(status));
        assert!(!fixture.store.expired.load(Ordering::SeqCst));
        assert!(fixture.store.cooldowns.lock().unwrap().is_empty());
        assert_eq!(
            fixture
                .feedback
                .scheduling_signals(&ProviderKind::new("antigravity").unwrap(), &account),
            (None, None)
        );
        released(&fixture).await;
    }
}

#[tokio::test]
async fn native_json_and_sse_meter_final_usage_with_requested_model_prices() {
    use gateway_core::{
        engine::{ModelRequestId, RequestAttemptContext},
        event::{EventSequenceValidator, GatewayEvent},
        policy::ClientApiKeyId,
    };
    use std::{collections::BTreeMap, sync::Arc};

    for streaming in [false, true] {
        let upstream = MockServer::start().await;
        let response = json!({
            "modelVersion": "different-returned-version",
            "candidates": [{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],
            "usageMetadata": {
                "promptTokenCount":100,"toolUsePromptTokenCount":10,
                "cachedContentTokenCount":20,"candidatesTokenCount":10,
                "thoughtsTokenCount":30,"totalTokenCount":150
            }
        });
        let (route, body, content_type) = if streaming {
            (
                "/v1internal:streamGenerateContent",
                format!(
                    "data: {}\n\ndata: {}\n\n",
                    json!({"response":{"usageMetadata":{"promptTokenCount":100,"candidatesTokenCount":6,"thoughtsTokenCount":10}}}),
                    json!({"response":response})
                ),
                "text/event-stream",
            )
        } else {
            (
                "/v1internal:generateContent",
                json!({"response":response}).to_string(),
                "application/json",
            )
        };
        Mock::given(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, content_type))
            .expect(1)
            .mount(&upstream)
            .await;
        let fixture = Fixture::new(&upstream.uri());
        let attempt = RequestAttemptContext::new(
            ModelRequestId::new("req_fixture").unwrap(),
            ClientApiKeyId::new("key_fixture").unwrap(),
        )
        .with_request_profile(Some(frozen_profile()))
        .with_pricing(Arc::new(BTreeMap::from([(
            "antigravity".into(),
            BTreeMap::from([("fixture-model".into(), crate::billing_tests::pricing())]),
        )])));
        let events = fixture
            .provider
            .clone()
            .execute(
                request(streaming),
                crate::support::context_with_request(attempt, CancellationToken::new()),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut validator = EventSequenceValidator::default();
        let mut costs = Vec::new();
        let mut observed = None;
        for fact in events.iter().flat_map(|event| event.canonical_facts()) {
            validator.observe(fact).unwrap();
            match fact {
                GatewayEvent::CalculatedCost(cost) => costs.push(cost),
                GatewayEvent::Usage(usage) => observed = Some(usage),
                _ => {}
            }
        }
        assert_eq!(costs.len(), 1);
        assert_eq!(costs[0].total().amount().canonical(), "0.000765");
        let recalculated = crate::billing::calculate(
            "unknown-model",
            observed.unwrap(),
            Some(&crate::billing_tests::pricing()),
        )
        .unwrap();
        assert_eq!(costs[0].total(), recalculated.total_amount());
        assert_eq!(observed.unwrap().reasoning_output_tokens, Some(30));
        assert_eq!(observed.unwrap().output_tokens, Some(40));
        released(&fixture).await;
    }
}

#[tokio::test]
async fn rate_limit_feedback_uses_model_scope_and_the_full_upstream_retry_delay() {
    let upstream = MockServer::start().await;
    Mock::given(path("/v1internal:generateContent")).respond_with(ResponseTemplate::new(429)
        .insert_header("retry-after", "2")
        .set_body_json(json!({"error":{"code":429,"status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"90000.125s"}]}})))
        .expect(1).mount(&upstream).await;
    let fixture = Fixture::new(&upstream.uri());
    let started = std::time::SystemTime::now();
    let stream = fixture
        .provider
        .clone()
        .execute(
            request(false),
            context(frozen_profile(), CancellationToken::new()),
        )
        .await
        .unwrap();
    let error = stream.try_collect::<Vec<_>>().await.unwrap_err();
    assert_eq!(error.retry_after(), Some(Duration::from_millis(90_000_125)));
    {
        let values = fixture.store.cooldowns.lock().unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(
            values[0].scope(),
            &gateway_core::provider_ports::ProviderCooldownScope::upstream_model(
                gateway_core::routing::UpstreamModelId::new("fixture-model").unwrap()
            )
        );
        assert!(values[0].until() >= started + Duration::from_secs(90_000));
    }
    released(&fixture).await;
    assert!(!fixture.store.expired.load(Ordering::SeqCst));
    let blocked = fixture
        .provider
        .clone()
        .execute(
            request(false),
            context(frozen_profile(), CancellationToken::new()),
        )
        .await;
    assert!(matches!(blocked, Err(e) if e.kind() == ProviderErrorKind::NoEligibleAccount));
}
