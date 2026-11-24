//! 冻结账号事实后返回惰性出站流，等待 Core 完成范围检查再发送

use super::{AntigravityProvider, candidates::Project};
use crate::transport::{CloudCodeClient, unsent};
use futures::StreamExt;
use gateway_core::{
    account::AccountHistorySnapshot,
    engine::{
        AttemptContext,
        provider::{
            ProviderCallMetadata, ProviderRequest, ProviderSelectionObservation, ProviderStream,
        },
    },
    error::{ProviderError, ProviderErrorKind as Kind},
    operation::Operation,
    upstream::{UpstreamSendState, UpstreamTransport},
};
use gateway_protocol::genai::GENAI_PROTOCOL;
use std::{sync::Arc, time::Instant};

impl AntigravityProvider {
    pub(super) async fn execute_native(
        self: Arc<Self>,
        request: ProviderRequest,
        ctx: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        if request.operation().protocol() != GENAI_PROTOCOL {
            return Err(unsent(Kind::Unsupported));
        }
        let model = request
            .candidate()
            .upstream_model()
            .cloned()
            .ok_or_else(|| unsent(Kind::InvalidRequest))?;
        let started = Instant::now();
        let selected = self.select_account(&model, &ctx).await?;
        let selection = ProviderSelectionObservation::new(
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            selected.capacity,
        );
        let profile = ctx
            .request_profile()
            .map(crate::profile::ClientProfile::from_frozen)
            .transpose()
            .map_err(|e| unsent(Kind::ProviderInfrastructureUnavailable).with_source(e))?
            .unwrap_or_else(|| crate::profile::default_profile(&self.release));
        let resource =
            CloudCodeClient::new(&self.origin, selected.account.outbound_proxy(), &profile)?;
        let metadata = ProviderCallMetadata::new(
            self.kind.clone(),
            model.clone(),
            selected.account.id().clone(),
            UpstreamTransport::new("http")
                .map_err(|_| unsent(Kind::ProviderInfrastructureUnavailable))?,
        )
        .with_credential_revision(selected.account.revision())
        .with_selection_observation(selection)
        .with_account_snapshot(Some(AccountHistorySnapshot::from_account(
            &selected.account,
        )));
        let cancellation = ctx.cancellation().clone();
        let deadline = ctx.deadline();
        let records_account_feedback = ctx
            .account_eligibility()
            .allows_account_state_mutation(selected.account.enabled());
        let stats = records_account_feedback.then(|| self.feedback.clone());
        let feedback = self.clone();
        let pricing = crate::billing::effective_pricing(
            model.as_str(),
            ctx.pricing()
                .get(self.kind.as_str())
                .and_then(|models| models.get(model.as_str())),
        );
        ctx.execute_middleware(
            request.operation().clone(),
            self.kind.clone(),
            Some(model.as_str().to_owned()),
            selected.account.id().clone(),
            Box::new(move |operation, headers| Box::pin(async move {
                let Operation::Generate(generate) = &operation else {
                    return Err(unsent(Kind::Unsupported));
                };
                let payload = generate.protocol_payload();
                if payload.protocol() != GENAI_PROTOCOL {
                    return Err(unsent(Kind::Unsupported));
                }
                let stream = payload.context().get("stream").and_then(serde_json::Value::as_bool).unwrap_or(false);
                let pricing = pricing.filter(|_| crate::billing::supports_pricing_fields(payload.body()));
                let body = operation.middleware_body().map_err(|_| unsent(Kind::InvalidRequest))?;
                let events = async_stream::try_stream! {
                    if cancellation.is_cancelled() {
                        Err(unsent(Kind::Cancelled))?;
                    }
                    if deadline.is_elapsed() {
                        Err(unsent(Kind::Timeout))?;
                    }
                    let record_failure = async |error| {
                        feedback.record_failure(&selected.account, &model, records_account_feedback, error).await
                    };
                    let response = tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => Err(ProviderError::new(Kind::Cancelled, UpstreamSendState::Ambiguous)),
                        _ = deadline.wait() => Err(ProviderError::new(Kind::Timeout, UpstreamSendState::Ambiguous)),
                        response = async {
                            let token = &selected.material.access_token;
                            let project = match selected.project {
                                Project::Catalog(project) => project,
                                // 固定探测使用该账号实际授权的项目，包括尚无公开目录的停用账号
                                Project::Discover => resource.account_info(token).await?.project,
                            };
                            resource.generate(&project, model.as_str(), token, &body, stream, &headers).await
                        } => response,
                    };
                    let response = match response {
                        Ok(response) => response,
                        Err(error) => Err(record_failure(error).await)?,
                    };
                    let events = crate::transport::events(
                        response, stream, model.clone(), deadline, cancellation, selected.material.access_token, pricing,
                    );
                    futures::pin_mut!(events);
                    while let Some(event) = events.next().await {
                        match event {
                            Ok(event) => yield event,
                            Err(error) => Err(record_failure(error).await)?,
                        }
                    }
                };
                let stream = ProviderStream::new(metadata, Box::pin(events), selected.lease);
                Ok(match stats {
                    Some(stats) => stream.with_account_feedback(stats),
                    None => stream,
                })
            })),
        ).await
    }
}
