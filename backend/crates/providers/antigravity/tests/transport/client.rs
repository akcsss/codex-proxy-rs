//! 通过回环 HTTP 验证授权凭据、原生请求与响应透传

use crate::{
    credential::refreshed_credential,
    profile::{self, ReleaseService},
    transport::{self, CloudCodeClient},
};
use futures::{StreamExt, TryStreamExt};
use gateway_core::{
    event::{EventSequenceValidator, GatewayEvent},
    lifecycle::{CancellationToken, Deadline},
    routing::UpstreamModelId,
};
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

#[tokio::test]
async fn oauth_refresh_and_native_generate_preserve_grant_profile_and_wire_payload() {
    let server = MockServer::start().await;
    let (origin, access_token) = refreshed_credential(&server).await;
    let profile = profile::default_profile(&ReleaseService::new().unwrap());
    // 官方 macOS arm64 Desktop 2.34.0 在默认参数下实际发送的已认证请求头
    let user_agent =
        "antigravity/hub/2.34.0 (aidev_client; os_type=darwin; arch=arm64; cl=1026637980)";
    let client = CloudCodeClient::new(&origin, None, &profile).unwrap();
    let post = |p| {
        Mock::given(method("POST"))
            .and(path(p))
            .and(header("authorization", "Bearer renewed-access"))
            .and(header("user-agent", user_agent))
    };
    post("/v1internal:loadCodeAssist")
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"cloudaicompanionProject":"project-1"})),
        )
        .expect(1)
        .mount(&server)
        .await;
    post("/v1internal:fetchAvailableModels")
        .and(wiremock::matchers::body_json(
            json!({"project":"project-1"}),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"models":{"fixture-model":{"displayName":"Fixture Model"}}})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let catalog = client.catalog(&access_token).await.unwrap();
    assert_eq!(catalog.models["fixture-model"].name, "Fixture Model");
    let body=br#"{ "contents":[{"parts":[{"functionResponse":{"name":"lookup","response":{"x":1}},"thoughtSignature":"opaque"}]}],"unknown":98765432109876543210987654 }"#;
    let reply = r#"{ "responseId":"r","candidates":[{"content":{"parts":[{"text":"hidden","thought":true},{"text":"ok"},{"functionCall":{"name":"lookup","args":{"text":"not output"}}}]},"finishReason":"STOP"},{"index":1,"content":{"parts":[{"text":"other candidate"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2},"extra":98765432109876543210987654 }"#;
    let received_body = move |r: &wiremock::Request| {
        r.body.windows(body.len()).any(|w| w == body)
            && String::from_utf8_lossy(&r.body)
                .starts_with(r#"{"project":"project-1","model":"fixture-model","request":"#)
    };
    post("/v1internal:generateContent")
        .and(received_body)
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(format!(r#"{{"response":{reply}}}"#), "application/json"),
        )
        .expect(1)
        .mount(&server)
        .await;
    post("/v1internal:streamGenerateContent")
        .and(query_param("alt", "sse"))
        .and(received_body)
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            format!("id: evt-1\ndata: {{\"response\":{reply}}}\n\n"),
            "text/event-stream",
        ))
        .expect(1)
        .mount(&server)
        .await;
    for stream in [false, true] {
        let res = client
            .generate(
                &catalog.project,
                "fixture-model",
                &access_token,
                body,
                stream,
                &[],
            )
            .await
            .unwrap();
        let events = transport::events(
            res,
            stream,
            UpstreamModelId::new("fixture-model").unwrap(),
            Deadline::default(),
            CancellationToken::new(),
            access_token.clone(),
            None,
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        let facts: Vec<_> = events
            .iter()
            .flat_map(|event| event.canonical_facts())
            .collect();
        let mut validator = EventSequenceValidator::default();
        for fact in &facts {
            validator.observe(fact).unwrap();
        }
        assert!(matches!(facts.first(), Some(GatewayEvent::Started(_))));
        assert!(facts.iter().any(|fact| matches!(fact, GatewayEvent::Usage(u) if u.input_tokens == Some(4) && u.output_tokens == Some(2))));
        let text: String = facts
            .iter()
            .filter_map(|fact| match fact {
                GatewayEvent::TextDelta(delta) => Some(delta.text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(text, "ok");
        assert!(matches!(
            events.last().unwrap().canonical_facts(),
            [GatewayEvent::Completed(_)]
        ));
        let wire = events[0].wire_event().unwrap();
        if stream {
            assert!(
                std::str::from_utf8(wire.raw_sse_frame().unwrap())
                    .unwrap()
                    .contains(&format!("id: evt-1\ndata: {reply}\n\n"))
            );
        } else {
            assert_eq!(wire.raw_json_body().unwrap().as_ref(), reply.as_bytes());
        }
    }
}

#[tokio::test]
async fn streamed_text_is_projected_once_across_native_frames() {
    let server = MockServer::start().await;
    let profile = profile::default_profile(&ReleaseService::new().unwrap());
    let client = CloudCodeClient::new(&server.uri().parse().unwrap(), None, &profile).unwrap();
    let frames = [
        json!({"responseId":"r","candidates":[{"content":{"parts":[{"text":"hidden","thought":true}]}}]}),
        json!({"candidates":[{"index":1,"content":{"parts":[{"text":"other"}]},"finishReason":"STOP"},{"index":0,"content":{"parts":[{"text":"O"}]}}]}),
        json!({"candidates":[{"index":0,"content":{"parts":[{"text":"K"}]},"finishReason":"STOP"}]}),
        json!({"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2}}),
    ];
    let body: String = frames
        .iter()
        .map(|frame| format!("data: {{\"response\":{frame}}}\n\n"))
        .collect();
    Mock::given(path("/v1internal:streamGenerateContent"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
        .expect(1)
        .mount(&server)
        .await;
    let response = client
        .generate("p", "m", "fixture", b"{}", true, &[])
        .await
        .unwrap();
    let events = transport::events(
        response,
        true,
        UpstreamModelId::new("m").unwrap(),
        Deadline::default(),
        CancellationToken::new(),
        "fixture".into(),
        None,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    let mut validator = EventSequenceValidator::default();
    let mut text = String::new();
    let mut content_count = 0;
    for fact in events.iter().flat_map(|event| event.canonical_facts()) {
        validator.observe(fact).unwrap();
        match fact {
            GatewayEvent::ContentAdded(_) => content_count += 1,
            GatewayEvent::TextDelta(delta) => text.push_str(&delta.text),
            _ => {}
        }
    }
    assert_eq!(content_count, 1);
    assert_eq!(text, "OK");
    let wire: Vec<_> = events
        .iter()
        .filter_map(|event| event.wire_event())
        .collect();
    assert_eq!(wire.len(), frames.len());
    for (wire, frame) in wire.iter().zip(frames) {
        assert_eq!(
            wire.raw_sse_frame().unwrap().as_ref(),
            format!("data: {frame}\n\n").as_bytes()
        );
    }
}

#[tokio::test]
async fn incomplete_native_stream_and_redirect_are_rejected() {
    let server = MockServer::start().await;
    let profile = profile::default_profile(&ReleaseService::new().unwrap());
    let client = CloudCodeClient::new(&server.uri().parse().unwrap(), None, &profile).unwrap();
    Mock::given(path("/v1internal:streamGenerateContent")).respond_with(ResponseTemplate::new(200).set_body_raw("data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"partial\"}]}}]}}\n\n","text/event-stream")).mount(&server).await;
    let res = client
        .generate("p", "m", "fixture", b"{}", true, &[])
        .await
        .unwrap();
    let result = transport::events(
        res,
        true,
        UpstreamModelId::new("m").unwrap(),
        Deadline::default(),
        CancellationToken::new(),
        "fixture".into(),
        None,
    )
    .try_collect::<Vec<_>>()
    .await;
    assert_eq!(
        result.unwrap_err().kind(),
        gateway_core::error::ProviderErrorKind::Protocol
    );
    let other = MockServer::start().await;
    Mock::given(path("/v1internal:generateContent"))
        .respond_with(ResponseTemplate::new(307).insert_header("Location", other.uri()))
        .mount(&server)
        .await;
    assert!(
        client
            .generate("p", "m", "fixture", b"{}", false, &[])
            .await
            .is_err()
    );
    assert!(other.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn incomplete_stream_never_emits_a_calculated_cost() {
    let server = MockServer::start().await;
    let frame = json!({"response":{
        "candidates":[{"content":{"parts":[{"text":"partial"}]}}],
        "usageMetadata":{"promptTokenCount":100,"candidatesTokenCount":20}
    }});
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(format!("data: {frame}\n\n"), "text/event-stream"),
        )
        .mount(&server)
        .await;
    let response = reqwest::get(server.uri()).await.unwrap();
    let events = transport::events(
        response,
        true,
        UpstreamModelId::new("fixture-model").unwrap(),
        Deadline::default(),
        CancellationToken::new(),
        "fixture".into(),
        Some(crate::billing_tests::pricing()),
    )
    .collect::<Vec<_>>()
    .await;
    assert_eq!(
        events.last().unwrap().as_ref().unwrap_err().kind(),
        gateway_core::error::ProviderErrorKind::Protocol
    );
    assert!(
        !events
            .iter()
            .filter_map(|event| event.as_ref().ok())
            .flat_map(|event| event.canonical_facts())
            .any(|fact| matches!(fact, GatewayEvent::CalculatedCost(_)))
    );
}
