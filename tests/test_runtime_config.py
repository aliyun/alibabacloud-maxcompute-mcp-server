"""Behavior tests for startup runtime selection and legacy compatibility."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from maxcompute_catalog_mcp.runtime_config import RuntimeMode, load_runtime_config


def _write_config(tmp_path: Path, document: dict[str, object]) -> str:
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(document), encoding="utf-8")
    return str(config_path)


def _legacy_config(endpoint: str, **extra: object) -> dict[str, object]:
    maxcompute: dict[str, object] = {
        "maxcompute_endpoint": endpoint,
        "catalogapi_endpoint": "https://catalogapi.example.com",
        "accessKeyId": "fixture-ak",
        "accessKeySecret": "fixture-sk",
    }
    maxcompute.update(extra)
    return {"maxcompute": maxcompute}


def test_legacy_public_config_automatically_derives_regional_mcp(
    tmp_path: Path,
) -> None:
    """An old config needs no new fields to derive public MCP endpoints."""
    path = _write_config(
        tmp_path,
        _legacy_config(
            "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
        ),
    )

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.profile == "default"
    assert config.remote is not None
    assert config.remote.url == ("https://mcp.cn-hangzhou.maxcompute.aliyun.com/mcp")


def test_legacy_odps_alias_is_still_loaded_for_remote_selection(
    tmp_path: Path,
) -> None:
    """The released top-level odps alias remains a valid old config shape."""
    document = _legacy_config(
        "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
    )
    document["odps"] = document.pop("maxcompute")
    path = _write_config(tmp_path, document)

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is not None
    assert "cn-hangzhou" in config.remote.url


def test_legacy_vpc_config_uses_only_same_region_vpc_mcp(
    tmp_path: Path,
) -> None:
    """A VPC MaxCompute endpoint cannot cross a Region or network boundary."""
    path = _write_config(
        tmp_path,
        _legacy_config(
            "https://service.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/api",
        ),
    )

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is not None
    assert config.remote.url == (
        "https://mcp.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/mcp"
    )


def test_legacy_intranet_endpoints_contribute_no_network_evidence(
    tmp_path: Path,
) -> None:
    """Internal-plane endpoints are outside the public endpoint registry."""
    path = _write_config(
        tmp_path,
        _legacy_config(
            "https://service.cn-hangzhou-intranet.maxcompute.aliyun-inc.com/api",
            catalogapi_endpoint=(
                "https://catalogapi.cn-hangzhou-intranet.maxcompute.aliyun-inc.com"
            ),
        ),
    )

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is None


def test_legacy_catalog_endpoint_can_supply_network_when_fe_is_custom(
    tmp_path: Path,
) -> None:
    path = _write_config(
        tmp_path,
        _legacy_config(
            "https://custom-fe.example.com/api",
            catalogapi_endpoint=(
                "https://catalogapi.ap-southeast-1.maxcompute.aliyun.com"
            ),
        ),
    )

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is not None
    assert config.remote.url == (
        "https://mcp-intl.ap-southeast-1.maxcompute.aliyun.com/mcp"
    )


def test_legacy_vpc_catalog_endpoint_can_supply_network_when_fe_is_custom(
    tmp_path: Path,
) -> None:
    path = _write_config(
        tmp_path,
        _legacy_config(
            "https://custom-fe.example.com/api",
            catalogapi_endpoint=(
                "https://catalogapi.cn-hongkong-vpc.maxcompute.aliyun-inc.com"
            ),
        ),
    )

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is not None
    assert config.remote.url == (
        "https://mcp-intl.cn-hongkong-vpc.maxcompute.aliyun-inc.com/mcp"
    )


def test_conflicting_legacy_fe_and_catalog_networks_do_not_guess(
    tmp_path: Path,
) -> None:
    path = _write_config(
        tmp_path,
        _legacy_config(
            "https://service.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/api",
            catalogapi_endpoint=(
                "https://catalogapi.cn-hangzhou.maxcompute.aliyun.com"
            ),
        ),
    )

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is None


def test_conflicting_legacy_fe_and_catalog_regions_do_not_guess(
    tmp_path: Path,
) -> None:
    path = _write_config(
        tmp_path,
        _legacy_config(
            "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
            catalogapi_endpoint=(
                "https://catalogapi.ap-southeast-1.maxcompute.aliyun.com"
            ),
        ),
    )

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is None


@pytest.mark.parametrize(
    ("network", "expected_url"),
    [
        (
            "public",
            "https://mcp.cn-hangzhou.maxcompute.aliyun.com/mcp",
        ),
        (
            "vpc",
            "https://mcp.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/mcp",
        ),
    ],
)
def test_simple_region_network_config_selects_matching_mcp(
    tmp_path: Path,
    network: str,
    expected_url: str,
) -> None:
    path = _write_config(
        tmp_path,
        {
            "maxcompute": {
                "region": "cn-hangzhou",
                "network": network,
            }
        },
    )

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is not None
    assert config.remote.url == expected_url


@pytest.mark.parametrize(
    ("endpoint", "expected_url"),
    [
        (
            "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
            "https://mcp.cn-hangzhou.maxcompute.aliyun.com/mcp",
        ),
        (
            "https://service.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/api",
            "https://mcp.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/mcp",
        ),
        (
            "https://service.cn-hongkong.maxcompute.aliyun.com/api",
            "https://mcp-intl.cn-hongkong.maxcompute.aliyun.com/mcp",
        ),
        (
            "https://service.cn-hongkong-vpc.maxcompute.aliyun-inc.com/api",
            "https://mcp-intl.cn-hongkong-vpc.maxcompute.aliyun-inc.com/mcp",
        ),
        (
            "https://service.ap-southeast-1.maxcompute.aliyun.com/api",
            "https://mcp-intl.ap-southeast-1.maxcompute.aliyun.com/mcp",
        ),
        (
            "https://service.ap-southeast-1-vpc.maxcompute.aliyun-inc.com/api",
            "https://mcp-intl.ap-southeast-1-vpc.maxcompute.aliyun-inc.com/mcp",
        ),
        (
            "https://service.cn-beijing.maxcompute.aliyun.com/api",
            "https://mcp.cn-beijing.maxcompute.aliyun.com/mcp",
        ),
        (
            "https://service.ap-northeast-1.maxcompute.aliyun.com/api",
            "https://mcp-intl.ap-northeast-1.maxcompute.aliyun.com/mcp",
        ),
        (
            "https://service.eu-central-1-vpc.maxcompute.aliyun-inc.com/api",
            "https://mcp-intl.eu-central-1-vpc.maxcompute.aliyun-inc.com/mcp",
        ),
        (
            "https://service.us-west-1.maxcompute.aliyun.com/api",
            "https://mcp-intl.us-west-1.maxcompute.aliyun.com/mcp",
        ),
    ],
)
def test_default_derives_one_mcp_endpoint_for_any_region(
    tmp_path: Path,
    endpoint: str,
    expected_url: str,
) -> None:
    path = _write_config(tmp_path, _legacy_config(endpoint))

    config = load_runtime_config(path)

    assert config.remote is not None
    assert config.remote.url == expected_url


def test_explicit_network_must_match_recognized_legacy_endpoints(
    tmp_path: Path,
) -> None:
    path = _write_config(
        tmp_path,
        _legacy_config(
            "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
            region="cn-hangzhou",
            network="vpc",
        ),
    )

    with pytest.raises(ValueError, match="conflicts with configured endpoints"):
        load_runtime_config(path)


def test_explicit_region_must_match_recognized_legacy_endpoints(
    tmp_path: Path,
) -> None:
    path = _write_config(
        tmp_path,
        _legacy_config(
            "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
            region="ap-southeast-1",
        ),
    )

    with pytest.raises(ValueError, match="conflicts with configured endpoints"):
        load_runtime_config(path)


def test_legacy_singapore_vpc_config_derives_regional_mcp(
    tmp_path: Path,
) -> None:
    """An overseas VPC Region derives the international MCP hostname."""
    path = _write_config(
        tmp_path,
        _legacy_config(
            "https://service.ap-southeast-1-vpc.maxcompute.aliyun-inc.com/api",
        ),
    )

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is not None
    assert config.remote.url == (
        "https://mcp-intl.ap-southeast-1-vpc.maxcompute.aliyun-inc.com/mcp"
    )


@pytest.mark.parametrize(
    ("maxcompute_endpoint", "remote_url"),
    [
        (
            "https://service.ap-southeast-1.maxcompute.aliyun.com/api",
            "https://mcp-intl.ap-southeast-1.maxcompute.aliyun.com/mcp",
        ),
        (
            "https://service.ap-southeast-1-vpc.maxcompute.aliyun-inc.com/api",
            ("https://mcp-intl.ap-southeast-1-vpc.maxcompute.aliyun-inc.com/mcp"),
        ),
    ],
)
def test_catalog_token_remote_accepts_either_oauth_site_entry(
    tmp_path: Path,
    maxcompute_endpoint: str,
    remote_url: str,
) -> None:
    """Catalog-token proxying depends on Region/network, not OAuth site."""
    document = _legacy_config(maxcompute_endpoint)
    document.update({"mode": "remote", "remote": {"url": remote_url}})
    path = _write_config(tmp_path, document)

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.REMOTE
    assert config.remote is not None
    assert config.remote.url == remote_url


def test_default_mode_derives_endpoint_without_a_region_registry(
    tmp_path: Path,
) -> None:
    document = _legacy_config(
        "https://service.cn-shanghai-vpc.maxcompute.aliyun-inc.com/api",
    )
    path = _write_config(
        tmp_path,
        document,
    )

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is not None
    assert config.remote.url == (
        "https://mcp.cn-shanghai-vpc.maxcompute.aliyun-inc.com/mcp"
    )


def test_explicit_remote_without_url_derives_regional_endpoint(
    tmp_path: Path,
) -> None:
    document = _legacy_config(
        "https://service.eu-west-1.maxcompute.aliyun.com/api",
    )
    document["mode"] = "remote"
    path = _write_config(tmp_path, document)

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.REMOTE
    assert config.remote is not None
    assert config.remote.url == ("https://mcp-intl.eu-west-1.maxcompute.aliyun.com/mcp")


def test_unknown_legacy_endpoint_stays_local_instead_of_guessing(
    tmp_path: Path,
) -> None:
    """Custom and historical endpoints are not mapped to a guessed MCP host."""
    path = _write_config(
        tmp_path,
        _legacy_config("https://service.odps.aliyun.com/api"),
    )

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is None


def test_explicit_local_mode_preserves_legacy_runtime(tmp_path: Path) -> None:
    document = _legacy_config(
        "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
    )
    document["mode"] = "local"
    path = _write_config(tmp_path, document)

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.LOCAL
    assert config.remote is None


def test_default_remote_selection_is_transport_agnostic(tmp_path: Path) -> None:
    """Default mode keeps its Remote candidate for Streamable HTTP."""
    path = _write_config(
        tmp_path,
        _legacy_config(
            "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
        ),
    )

    config = load_runtime_config(path, allow_remote=False)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is not None
    assert config.remote.url == ("https://mcp.cn-hangzhou.maxcompute.aliyun.com/mcp")


def test_explicit_remote_is_transport_agnostic(tmp_path: Path) -> None:
    document = _legacy_config(
        "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
    )
    document["mode"] = "remote"
    path = _write_config(tmp_path, document)

    config = load_runtime_config(path, allow_remote=False)

    assert config.mode is RuntimeMode.REMOTE
    assert config.remote is not None
    assert config.remote.url == ("https://mcp.cn-hangzhou.maxcompute.aliyun.com/mcp")


def test_named_profile_drives_network_and_region_selection(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path,
        {
            "default": "public",
            "configs": {
                "public": {
                    "maxcompute_endpoint": (
                        "https://service.cn-hangzhou.maxcompute.aliyun.com/api"
                    ),
                    "catalogapi_endpoint": "https://catalog-public.example.com",
                    "accessKeyId": "public-ak",
                    "accessKeySecret": "public-sk",
                },
                "vpc": {
                    "maxcompute_endpoint": (
                        "https://service.cn-hongkong-vpc.maxcompute.aliyun-inc.com/api"
                    ),
                    "catalogapi_endpoint": "https://catalog-vpc.example.com",
                    "accessKeyId": "vpc-ak",
                    "accessKeySecret": "vpc-sk",
                },
            },
        },
    )

    config = load_runtime_config(path, profile="vpc")

    assert config.mode is RuntimeMode.DEFAULT
    assert config.profile == "vpc"
    assert config.remote is not None
    assert config.remote.url == (
        "https://mcp-intl.cn-hongkong-vpc.maxcompute.aliyun-inc.com/mcp"
    )


def test_environment_only_legacy_config_gets_a_default_remote_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "MAXCOMPUTE_ENDPOINT",
        "https://service.ap-southeast-1.maxcompute.aliyun.com/api",
    )
    monkeypatch.setenv(
        "MAXCOMPUTE_CATALOG_API_ENDPOINT",
        "https://catalogapi.ap-southeast-1.maxcompute.aliyun.com",
    )
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "fixture-ak")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "fixture-sk")

    config = load_runtime_config(None)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is not None
    assert config.remote.url == (
        "https://mcp-intl.ap-southeast-1.maxcompute.aliyun.com/mcp"
    )


def test_environment_only_simple_config_gets_a_default_remote_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAXCOMPUTE_REGION", "ap-southeast-1")
    monkeypatch.setenv("MAXCOMPUTE_NETWORK", "vpc")

    config = load_runtime_config(None)

    assert config.mode is RuntimeMode.DEFAULT
    assert config.remote is not None
    assert config.remote.url == (
        "https://mcp-intl.ap-southeast-1-vpc.maxcompute.aliyun-inc.com/mcp"
    )


def test_vpc_config_rejects_explicit_public_remote_url(tmp_path: Path) -> None:
    document = _legacy_config(
        "https://service.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/api",
    )
    document.update(
        {
            "mode": "remote",
            "remote": {
                "url": "https://mcp.cn-hangzhou.maxcompute.aliyun.com/mcp",
            },
        }
    )
    path = _write_config(tmp_path, document)

    with pytest.raises(ValueError, match="network type"):
        load_runtime_config(path)


def test_vpc_config_rejects_explicit_other_region_vpc_url(
    tmp_path: Path,
) -> None:
    document = _legacy_config(
        "https://service.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/api",
    )
    document.update(
        {
            "mode": "remote",
            "remote": {
                "url": ("https://mcp.cn-hongkong-vpc.maxcompute.aliyun-inc.com/mcp"),
            },
        }
    )
    path = _write_config(tmp_path, document)

    with pytest.raises(ValueError, match="same Region"):
        load_runtime_config(path)


def test_public_config_rejects_explicit_vpc_remote_url(tmp_path: Path) -> None:
    document = _legacy_config(
        "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
    )
    document.update(
        {
            "mode": "remote",
            "remote": {
                "url": ("https://mcp.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/mcp"),
            },
        }
    )
    path = _write_config(tmp_path, document)

    with pytest.raises(ValueError, match="network type"):
        load_runtime_config(path)


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("http://remote.example.com/mcp", "must use HTTPS"),
        ("https://user@remote.example.com/mcp", "userinfo"),
        ("https://remote.example.com/mcp?x=1", "query"),
        ("https://remote.example.com/other", "path must be /mcp"),
    ],
)
def test_explicit_remote_rejects_unsafe_targets(
    tmp_path: Path,
    url: str,
    message: str,
) -> None:
    document = _legacy_config(
        "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
    )
    document.update({"mode": "remote", "remote": {"url": url}})
    path = _write_config(tmp_path, document)

    with pytest.raises(ValueError, match=message):
        load_runtime_config(path)


def test_unrecognized_remote_url_stays_fail_closed(tmp_path: Path) -> None:
    document = _legacy_config(
        "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
    )
    document.update(
        {
            "mode": "remote",
            "remote": {"url": "https://mcp-slot.internal.example.net/mcp"},
        }
    )
    path = _write_config(tmp_path, document)

    with pytest.raises(ValueError, match="cannot be verified"):
        load_runtime_config(path)


def test_unrecognized_remote_url_opt_in_proceeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("MAXCOMPUTE_ALLOW_UNVERIFIED_REMOTE_URL", "1")
    url = "https://mcp-slot.internal.example.net/mcp"
    document = _legacy_config(
        "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
    )
    document.update({"mode": "remote", "remote": {"url": url}})
    path = _write_config(tmp_path, document)

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.REMOTE
    assert config.remote is not None
    assert config.remote.url == url
    assert "network type cannot be verified" in capsys.readouterr().err


def test_explicit_loopback_remote_is_available_for_local_integration(
    tmp_path: Path,
) -> None:
    document = _legacy_config("https://custom.example.com/api")
    document.update(
        {
            "mode": "remote",
            "remote": {"url": "http://127.0.0.1:8080/mcp"},
        }
    )
    path = _write_config(tmp_path, document)

    config = load_runtime_config(path)

    assert config.mode is RuntimeMode.REMOTE
    assert config.remote is not None
    assert config.remote.url == "http://127.0.0.1:8080/mcp"


def test_obsolete_remote_policy_is_rejected(tmp_path: Path) -> None:
    document = _legacy_config(
        "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
    )
    document.update(
        {
            "mode": "remote",
            "remote": {
                "url": "https://mcp.cn-hangzhou.maxcompute.aliyun.com/mcp",
                "tool_policy": {"tools": {}},
            },
        }
    )
    path = _write_config(tmp_path, document)

    with pytest.raises(ValueError, match="unsupported field"):
        load_runtime_config(path)


def test_obsolete_auto_mode_is_rejected(tmp_path: Path) -> None:
    document = _legacy_config(
        "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
    )
    document["mode"] = "auto"
    path = _write_config(tmp_path, document)

    with pytest.raises(ValueError, match="'default', 'remote', or 'local'"):
        load_runtime_config(path)
