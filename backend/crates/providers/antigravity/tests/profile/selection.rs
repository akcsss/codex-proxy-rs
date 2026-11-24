//! 三平台客户端身份、冻结版本与非法配置边界

use crate::profile::{ClientProfile, Selection, object};
use serde_json::json;

#[test]
fn client_presets_resolve_platform_and_frozen_version() {
    for (platform, os, arch) in [
        ("macos", "darwin", "arm64"),
        ("linux", "linux", "amd64"),
        ("windows", "windows", "amd64"),
    ] {
        for (client, subclient, auth) in [
            ("desktop", "hub", ""),
            ("cli", "cli", "; auth_method=consumer"),
        ] {
            let selection = Selection::parse(&object(json!({"client":client,"platform":platform,"versionMode":"latest","clientVersion":null,"arch":null}))).unwrap();
            let initial = selection
                .resolve(selection.client.baseline().version)
                .frozen();
            let frozen = selection.resolve("2.35.0").frozen();
            let profile = ClientProfile::from_frozen(&frozen).unwrap();
            // 发布版本变化后不混入旧 CL，也不把未知版本标记为实测画像
            assert!(profile.verified_at().is_none());
            assert_eq!(
                profile.user_agent(),
                format!(
                    "antigravity/{subclient}/2.35.0 (aidev_client; os_type={os}; arch={arch}{auth})"
                )
            );
            let original = ClientProfile::from_frozen(&initial).unwrap();
            assert!(original.user_agent().contains("; cl="));
            assert_eq!(
                original.verified_at().is_some(),
                client == "desktop" && platform == "macos"
            );
            assert_ne!(original.user_agent(), profile.user_agent());
        }
    }
}

#[test]
fn fixed_version_rejects_header_injection_and_incomplete_selection() {
    for value in [
        json!({"client":"desktop","platform":"macos","versionMode":"fixed","clientVersion":"1.0.0\r\nother","arch":null}),
        json!({"client":"desktop","platform":"macos","versionMode":"fixed","clientVersion":null,"arch":null}),
        json!({"client":"desktop","platform":"macos","versionMode":"latest","clientVersion":"1.0.0","arch":null}),
    ] {
        assert!(Selection::parse(&object(value)).is_err());
    }
    let selected = Selection::parse(&object(json!({"client":"cli","platform":"linux","versionMode":"fixed","clientVersion":"1.5.15","arch":"arm64"}))).unwrap();
    assert_eq!(
        selected.resolve("9.9.9").user_agent(),
        "antigravity/cli/1.5.15 (aidev_client; os_type=linux; arch=arm64; cl=1027820220; auth_method=consumer)"
    );
}
