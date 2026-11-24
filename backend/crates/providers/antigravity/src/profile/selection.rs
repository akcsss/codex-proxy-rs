//! 客户端选择与冻结身份，不读取网络或运行时设置

use chrono::{DateTime, TimeZone, Utc};
use gateway_admin::ports::provider::{ProviderAdminError, ProviderAdminErrorKind};
use gateway_core::account::OpaqueProviderData;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ClientKind {
    Desktop,
    Cli,
}
pub(crate) struct ClientBuild {
    pub(crate) version: &'static str,
    changelist: u64,
}
impl ClientKind {
    pub(crate) const fn product(self) -> &'static str {
        match self {
            Self::Desktop => "Antigravity Desktop",
            Self::Cli => "Antigravity CLI",
        }
    }
    pub(crate) const fn baseline(self) -> ClientBuild {
        // 版本与 CL 成对取自官方二进制及已认证请求，下载目录的编号不是 CL
        match self {
            Self::Desktop => ClientBuild {
                version: "2.34.0",
                changelist: 1_026_637_980,
            },
            Self::Cli => ClientBuild {
                version: "1.5.15",
                changelist: 1_027_820_220,
            },
        }
    }
    const fn subclient(self) -> &'static str {
        match self {
            Self::Desktop => "hub",
            Self::Cli => "cli",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Platform {
    Macos,
    Linux,
    Windows,
}
impl Platform {
    pub(crate) const fn os_type(self) -> &'static str {
        match self {
            Self::Macos => "darwin",
            Self::Linux => "linux",
            Self::Windows => "windows",
        }
    }
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Macos => "macOS",
            Self::Linux => "Linux",
            Self::Windows => "Windows",
        }
    }
    pub(crate) const fn default_arch(self) -> Architecture {
        match self {
            Self::Macos => Architecture::Arm64,
            _ => Architecture::Amd64,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Architecture {
    Arm64,
    Amd64,
}
impl Architecture {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Arm64 => "arm64",
            Self::Amd64 => "amd64",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum VersionMode {
    Latest,
    Fixed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Selection {
    pub(crate) client: ClientKind,
    pub(crate) platform: Platform,
    pub(crate) version_mode: VersionMode,
    pub(crate) client_version: Option<String>,
    pub(crate) arch: Option<Architecture>,
}
impl Default for Selection {
    fn default() -> Self {
        Self {
            client: ClientKind::Desktop,
            platform: Platform::Macos,
            version_mode: VersionMode::Latest,
            client_version: None,
            arch: None,
        }
    }
}
impl Selection {
    pub(crate) fn parse(value: &OpaqueProviderData) -> Result<Self, ProviderAdminError> {
        let selection: Self =
            serde_json::from_value(Value::Object(value.expose_to_provider().clone()))
                .map_err(|e| invalid().with_source(e))?;
        match (&selection.version_mode, &selection.client_version) {
            (VersionMode::Latest, None) => Ok(selection),
            (VersionMode::Fixed, Some(version)) if valid_version(version) => Ok(selection),
            _ => Err(invalid()),
        }
    }
    pub(crate) fn configuration(&self) -> OpaqueProviderData {
        object(json!(self))
    }
    pub(crate) fn target(&self) -> ReleaseTarget {
        ReleaseTarget {
            client: self.client,
            platform: self.platform,
            arch: self.arch.unwrap_or_else(|| self.platform.default_arch()),
        }
    }
    pub(crate) fn resolve(&self, latest: &str) -> ClientProfile {
        let version = self.client_version.as_deref().unwrap_or(latest);
        let baseline = self.client.baseline();
        ClientProfile {
            client: self.client,
            platform: self.platform,
            arch: self.target().arch,
            client_version: version.into(),
            // 发布资料只有版本号；未知版本不沿用另一个版本的构建号
            changelist: (version == baseline.version).then_some(baseline.changelist),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ReleaseTarget {
    pub(crate) client: ClientKind,
    pub(crate) platform: Platform,
    pub(crate) arch: Architecture,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ClientProfile {
    pub(crate) client: ClientKind,
    pub(crate) platform: Platform,
    pub(crate) arch: Architecture,
    pub(crate) client_version: String,
    changelist: Option<u64>,
}
impl ClientProfile {
    pub(crate) fn from_frozen(value: &OpaqueProviderData) -> Result<Self, ProviderAdminError> {
        let profile: Self =
            serde_json::from_value(Value::Object(value.expose_to_provider().clone()))
                .map_err(|e| invalid().with_source(e))?;
        if !valid_version(&profile.client_version) || profile.changelist == Some(0) {
            return Err(invalid());
        }
        Ok(profile)
    }
    pub(crate) fn frozen(&self) -> OpaqueProviderData {
        object(json!(self))
    }
    pub(crate) fn verified_at(&self) -> Option<DateTime<Utc>> {
        let baseline = self.client.baseline();
        if self.client != ClientKind::Desktop
            || self.platform != Platform::Macos
            || self.arch != Architecture::Arm64
            || self.client_version != baseline.version
            || self.changelist != Some(baseline.changelist)
        {
            return None;
        }
        // Desktop 完整 UA 已实测；CLI 的 ACP 采样不足以核验普通交互入口的完整画像
        // 核验时间独立于官方发布资料的轮询时间
        Utc.with_ymd_and_hms(2026, 11, 23, 11, 11, 15).single()
    }
    pub(crate) fn client_identifier(&self) -> String {
        format!(
            "antigravity/{}/{}",
            self.client.subclient(),
            self.client_version
        )
    }
    pub(crate) fn user_agent(&self) -> String {
        let mut user_agent = format!(
            "{} (aidev_client; os_type={}; arch={}",
            self.client_identifier(),
            self.platform.os_type(),
            self.arch.label()
        );
        if let Some(changelist) = self.changelist {
            user_agent.push_str(&format!("; cl={changelist}"));
        }
        if self.client == ClientKind::Cli {
            // CLI 的个人 OAuth 身份携带此字段；Desktop 的已认证请求未携带
            user_agent.push_str("; auth_method=consumer");
        }
        user_agent.push(')');
        user_agent
    }
}
pub(crate) fn valid_version(version: &str) -> bool {
    let parts = version.split('.');
    parts.clone().count() == 3
        && parts
            .into_iter()
            .all(|v| !v.is_empty() && v.len() <= 9 && v.bytes().all(|c| c.is_ascii_digit()))
}
pub(crate) fn object(value: Value) -> OpaqueProviderData {
    OpaqueProviderData::new(value.as_object().cloned().unwrap_or_default())
}
fn invalid() -> ProviderAdminError {
    ProviderAdminError::new(ProviderAdminErrorKind::Invalid)
        .with_public_message("Antigravity 客户端配置无效；固定版本须为三段数字")
}
