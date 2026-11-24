//! 去除资源服务响应外层，协议正文与候选完成、用量观测独立处理

use crate::transport::{sent, unwrap_frame};
use bytes::Bytes;
use futures::Stream;
use gateway_core::{
    error::{ProviderError, ProviderErrorKind as Kind},
    event::{
        ContentItem, ContentKind, FinishReason, GatewayEvent, ProtocolWireEvent, ProviderEvent,
        ResponseMeta, TextDelta,
    },
    lifecycle::{CancellationToken, Deadline},
    metering::{ModelPriceOverride, Usage},
    routing::UpstreamModelId,
};
use gateway_protocol::{
    genai::{GENAI_PROTOCOL, GenerateContentObservation, GenerationFinish, primary_text_parts},
    openai::sse::{
        MAX_SSE_EVENT_BUFFER_BYTES, SseEvent, SseEventDecoder, encode_sse_event_with_metadata,
    },
};
use reqwest::Response;
use serde_json::Value;

pub(crate) fn events(
    mut response: Response,
    stream: bool,
    model: UpstreamModelId,
    deadline: Deadline,
    cancel: CancellationToken,
    token: String,
    pricing: Option<ModelPriceOverride>,
) -> impl Stream<Item = Result<ProviderEvent, ProviderError>> + Send + 'static {
    async_stream::try_stream! {
        let mut decoder = SseEventDecoder::default();
        let mut json_body = Vec::new();
        let mut observed = GenerateContentObservation::default();
        let mut started = None;
        let mut text_started = false;
        let mut calculated_cost = None;
        let mut priceable = pricing.is_some();
        loop {
            let chunk = tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(sent(Kind::Cancelled)),
                _ = deadline.wait() => Err(sent(Kind::Timeout)),
                result = response.chunk() => result.map_err(|e| sent(Kind::Transport).with_source(e.without_url())),
            }?;
            let eof = chunk.is_none();
            let frames = if stream {
                match chunk {
                    Some(bytes) => decoder.push(&bytes),
                    None => decoder.finish(),
                }.map_err(|_| sent(Kind::Protocol))?
            } else if let Some(bytes) = chunk {
                if json_body.len().saturating_add(bytes.len()) > MAX_SSE_EVENT_BUFFER_BYTES {
                    Err(sent(Kind::MessageTooBig))?;
                }
                json_body.extend_from_slice(&bytes);
                Vec::new()
            } else {
                let data = String::from_utf8(std::mem::take(&mut json_body))
                    .map_err(|_| sent(Kind::Protocol))?;
                vec![SseEvent { event: None, data, id: None, retry: None }]
            };
            for frame in frames {
                let inner = unwrap_frame(&frame.data)?;
                let value: Value = serde_json::from_slice(&inner).map_err(|_| sent(Kind::Protocol))?;
                if value.get("error").is_some_and(Value::is_object) {
                    let status=value.get("error").and_then(|e|e.get("code")).and_then(Value::as_u64).and_then(|code|u16::try_from(code).ok());
                    Err(super::failure::body_error(status,&frame.data,&token))?;
                }
                priceable &= crate::billing::supports_pricing(&value);
                if !priceable { calculated_cost = None; }
                let first_frame = started.is_none();
                let meta = started.get_or_insert_with(|| {
                    let id = value.get("responseId").and_then(Value::as_str)
                        .map_or_else(|| uuid::Uuid::now_v7().to_string(), str::to_owned);
                    let model = value.get("modelVersion").and_then(Value::as_str).unwrap_or(model.as_str());
                    ResponseMeta::new(id, model)
                });
                let usage = observed.observe(&value).map(usage);
                let text = primary_text_parts(&value).collect::<String>();
                let wire = if stream {
                    let data = std::str::from_utf8(&inner).map_err(|_| sent(Kind::Protocol))?;
                    let raw = encode_sse_event_with_metadata(
                        frame.event.as_deref().unwrap_or(""), data, frame.id.as_deref(), frame.retry,
                    );
                    ProtocolWireEvent::json_with_raw_sse_metadata(
                        GENAI_PROTOCOL, frame.event, value, Bytes::from(raw), frame.id, frame.retry,
                    )
                } else {
                    ProtocolWireEvent::raw_json(GENAI_PROTOCOL, inner)
                }.map_err(|_| sent(Kind::Protocol))?;
                let mut event = ProviderEvent::wire(wire);
                // 首批必须含可交付的协议帧，开始事实随首帧一起提交
                if first_frame {
                    event = event.with_fact(GatewayEvent::Started(meta.clone()));
                }
                // 探测与首输出观测使用通用文本事实，协议正文仍由原生帧完整交付
                if !text.is_empty() {
                    if !text_started {
                        event = event.with_fact(GatewayEvent::ContentAdded(ContentItem::new(
                            0, ContentKind::Text,
                        )));
                        text_started = true;
                    }
                    event = event.with_fact(GatewayEvent::TextDelta(TextDelta {
                        content_index: 0,
                        text,
                    }));
                }
                if let Some(usage) = usage {
                    calculated_cost = if priceable { crate::billing::calculate(model.as_str(), &usage, pricing.as_ref()) } else { None };
                    event = event.with_fact(GatewayEvent::Usage(usage));
                }
                yield event;
            }
            if eof {
                break;
            }
        }
        let ending = observed.finish().ok_or_else(|| sent(Kind::Protocol))?;
        let meta = started.ok_or_else(|| sent(Kind::Protocol))?;
        if let Some(breakdown) = calculated_cost {
            yield ProviderEvent::canonical(GatewayEvent::CalculatedCost(breakdown.calculated_cost()));
        }
        yield ProviderEvent::canonical(GatewayEvent::Completed(meta.with_finish_reason(finish_reason(ending))));
    }
}

fn usage(facts: &gateway_protocol::genai::UsageMetadata) -> Usage {
    let mut usage = Usage::new();
    usage.input_tokens = facts.input_tokens();
    usage.cached_input_tokens = facts.cached;
    usage.output_tokens = facts.output_tokens();
    usage.reasoning_output_tokens = facts.thoughts;
    usage.total_tokens = facts.total;
    usage.image_input_tokens = facts.image_input_tokens();
    usage.image_output_tokens = facts.candidate_image;
    usage
}

fn finish_reason(ending: GenerationFinish<'_>) -> FinishReason {
    match ending {
        GenerationFinish::Blocked => FinishReason::ContentFilter,
        GenerationFinish::Candidate {
            reason: "MAX_TOKENS",
            ..
        } => FinishReason::Length,
        GenerationFinish::Candidate {
            reason: "STOP",
            tool_call: true,
        } => FinishReason::ToolCall,
        GenerationFinish::Candidate { reason: "STOP", .. } => FinishReason::Stop,
        GenerationFinish::Candidate {
            reason: "SAFETY" | "BLOCKLIST" | "PROHIBITED_CONTENT" | "SPII" | "RECITATION",
            ..
        } => FinishReason::ContentFilter,
        _ => FinishReason::Other,
    }
}
