//! 已注册资源服务的项目发现、模型查询和原生 HTTP 请求

use bytes::Bytes;
use gateway_core::{
    account::OutboundProxy,
    error::{ProviderError, ProviderErrorKind as Kind},
};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{json, value::RawValue};
use std::{collections::BTreeMap, time::Duration};
use url::Url;

use super::failure::{network, sent, successful, unsent};
use crate::profile::ClientProfile;
use gateway_core::engine::middleware::MiddlewareHeader;
const MAX_CONTROL_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
pub(crate) struct Catalog {
    pub(crate) project: String,
    pub(crate) models: BTreeMap<String, Model>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Model {
    #[serde(rename = "displayName", default)]
    pub(crate) name: String,
    #[serde(rename = "quotaInfo", default)]
    pub(crate) quota: Option<super::account::ModelQuota>,
}

pub(crate) struct CloudCodeClient {
    client: Client,
    origin: Url,
}

impl CloudCodeClient {
    pub(crate) fn new(
        origin: &Url,
        proxy: Option<&OutboundProxy>,
        profile: &ClientProfile,
    ) -> Result<Self, ProviderError> {
        let client = gateway_egress::http_client(proxy)
            .map_err(|e| unsent(Kind::ProviderInfrastructureUnavailable).with_source(e))?
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .user_agent(profile.user_agent())
            .build()
            .map_err(|e| {
                unsent(Kind::ProviderInfrastructureUnavailable).with_source(e.without_url())
            })?;
        Ok(Self {
            client,
            origin: origin.clone(),
        })
    }

    pub(super) fn post(&self, action: &str, token: &str) -> reqwest::RequestBuilder {
        let mut url = self.origin.clone();
        url.set_path(&format!("/v1internal:{action}"));
        self.client.post(url).bearer_auth(token)
    }

    pub(crate) async fn catalog(&self, token: &str) -> Result<Catalog, ProviderError> {
        let project = self.account_info(token).await?.project;
        self.catalog_for_project(token, project).await
    }

    pub(crate) async fn catalog_for_project(
        &self,
        token: &str,
        project: String,
    ) -> Result<Catalog, ProviderError> {
        #[derive(Deserialize)]
        struct Models {
            models: BTreeMap<String, Model>,
        }
        let models: Models = control(
            self.post("fetchAvailableModels", token)
                .json(&json!({"project":project})),
            token,
        )
        .await?;
        if models
            .models
            .keys()
            .any(|id| gateway_core::routing::UpstreamModelId::new(id).is_err())
        {
            return Err(sent(Kind::Protocol));
        }
        Ok(Catalog {
            project,
            models: models.models,
        })
    }

    pub(crate) async fn generate(
        &self,
        project: &str,
        model: &str,
        token: &str,
        body: &[u8],
        stream: bool,
        headers: &[MiddlewareHeader],
    ) -> Result<Response, ProviderError> {
        #[derive(Serialize)]
        struct Envelope<'a> {
            project: &'a str,
            model: &'a str,
            request: &'a RawValue,
        }
        let request = serde_json::from_slice(body)
            .map_err(|e| unsent(Kind::InvalidRequest).with_source(e))?;
        let (send, media) = if stream {
            (
                self.post("streamGenerateContent", token)
                    .query(&[("alt", "sse")]),
                "text/event-stream",
            )
        } else {
            (self.post("generateContent", token), "application/json")
        };
        let mut send = send;
        for header in headers {
            if protected_header(header.name()) {
                continue;
            }
            let name = reqwest::header::HeaderName::from_bytes(header.name().as_bytes())
                .map_err(|e| unsent(Kind::InvalidRequest).with_source(e))?;
            let value = reqwest::header::HeaderValue::from_bytes(header.value())
                .map_err(|e| unsent(Kind::InvalidRequest).with_source(e))?;
            send = send.header(name, value);
        }
        let response = send
            .header(reqwest::header::ACCEPT, media)
            .json(&Envelope {
                project,
                model,
                request,
            })
            .send()
            .await
            .map_err(network)?;
        let response = successful(response, token).await?;
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(';').next())
            .is_some_and(|v| v.trim().eq_ignore_ascii_case(media))
        {
            return Err(sent(Kind::Protocol));
        }
        Ok(response)
    }
}

fn protected_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "authorization"
            | "proxy-authorization"
            | "cookie"
            | "host"
            | "connection"
            | "proxy-connection"
            | "keep-alive"
            | "transfer-encoding"
            | "content-length"
            | "content-type"
            | "accept"
            | "accept-encoding"
            | "user-agent"
            | "te"
            | "trailer"
            | "upgrade"
            | "x-goog-api-key"
            | "x-api-key"
    )
}

pub(super) async fn control<T: DeserializeOwned>(
    request: reqwest::RequestBuilder,
    token: &str,
) -> Result<T, ProviderError> {
    let mut response = successful(
        request
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(network)?,
        token,
    )
    .await?;
    let mut data = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| sent(Kind::Transport).with_source(e.without_url()))?
    {
        if data.len().saturating_add(chunk.len()) > MAX_CONTROL_RESPONSE_BYTES {
            return Err(sent(Kind::MessageTooBig));
        }
        data.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&data).map_err(|e| sent(Kind::Protocol).with_source(e))
}

#[derive(Deserialize)]
struct Reply<'a> {
    #[serde(borrow)]
    response: Option<&'a RawValue>,
    #[serde(borrow)]
    error: Option<&'a RawValue>,
}

pub(crate) fn unwrap_frame(data: &str) -> Result<Bytes, ProviderError> {
    let envelope: Reply =
        serde_json::from_str(data).map_err(|e| sent(Kind::Protocol).with_source(e))?;
    match (envelope.response, envelope.error) {
        (None, Some(err)) if err.get().starts_with('{') => {
            Ok(Bytes::copy_from_slice(data.as_bytes()))
        }
        (Some(body), None) if body.get().starts_with('{') => {
            Ok(Bytes::copy_from_slice(body.get().as_bytes()))
        }
        _ => Err(sent(Kind::Protocol)),
    }
}
