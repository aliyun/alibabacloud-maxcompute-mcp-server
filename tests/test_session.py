"""Unit tests for session tools: list_configs / use_config / get_current_config.

Uses an injected fake client-set builder so no real network/credentials are needed.
Verifies switching behaviour and that AccessKey/secret are never returned.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from maxcompute_catalog_mcp.client_factory import ClientSet
from maxcompute_catalog_mcp.config import MaxComputeCatalogConfig
from maxcompute_catalog_mcp.mcp_protocol import JsonRpcError
from maxcompute_catalog_mcp.tools import Tools


def _payload(result):
    """Extract the JSON payload from an mcp_text_result envelope."""
    return json.loads(result["content"][0]["text"])


def _cfg(endpoint, ak, sk, *, region="", description="", project=""):
    return MaxComputeCatalogConfig(
        catalogapi_endpoint="",
        maxcompute_endpoint=endpoint,
        access_key_id=ak,
        access_key_secret=sk,
        default_project=project,
        region=region,
        description=description,
    )


def _make_tools():
    configs = {
        "hz": _cfg("https://hz.example.com", "AK_HZ", "SECRET_HZ",
                   region="cn-hangzhou", description="Hangzhou", project="p_hz"),
        "sg": _cfg("https://sg.example.com", "AK_SG", "SECRET_SG",
                   region="ap-southeast-1", description="Singapore", project="p_sg"),
    }
    built = []

    def fake_builder(cfg):
        built.append(cfg)
        return ClientSet(
            sdk=MagicMock(name="sdk"),
            maxcompute_client=MagicMock(name="mc"),
            credential_client=MagicMock(name="cred"),
            default_project=cfg.default_project,
            namespace_id=cfg.namespace_id,
        )

    tools = Tools(
        sdk=MagicMock(name="sdk-hz"),
        default_project="p_hz",
        namespace_id="",
        maxcompute_client=MagicMock(name="mc-hz"),
        credential_client=MagicMock(name="cred-hz"),
        configs=configs,
        default_name="hz",
        client_set_builder=fake_builder,
    )
    return tools, built


def test_list_configs_no_secret_leak():
    tools, _ = _make_tools()
    payload = _payload(tools.call("list_configs", {}))
    assert payload["success"] is True
    data = payload["data"]
    assert data["current"] == "hz"
    assert data["default"] == "hz"
    assert {c["name"] for c in data["configs"]} == {"hz", "sg"}
    hz = next(c for c in data["configs"] if c["name"] == "hz")
    assert hz["region"] == "cn-hangzhou"
    assert hz["maxcompute_endpoint"] == "https://hz.example.com"
    assert hz["is_current"] is True and hz["is_default"] is True
    # No AccessKey id/secret anywhere in the serialized payload
    blob = json.dumps(payload)
    for leak in ("SECRET_HZ", "SECRET_SG", "AK_HZ", "AK_SG"):
        assert leak not in blob


def test_get_current_config():
    tools, _ = _make_tools()
    payload = _payload(tools.call("get_current_config", {}))
    assert payload["success"] is True
    assert payload["data"]["name"] == "hz"
    assert payload["data"]["maxcompute_endpoint"] == "https://hz.example.com"
    assert "AK_HZ" not in json.dumps(payload)


def test_use_config_switches_active_client():
    tools, built = _make_tools()
    payload = _payload(tools.call("use_config", {"name": "sg"}))
    assert payload["success"] is True
    assert tools._current_name == "sg"
    assert tools.default_project == "p_sg"
    # builder was invoked for sg (hz was seeded at construction, not built)
    assert any(c.maxcompute_endpoint == "https://sg.example.com" for c in built)
    cur = _payload(tools.call("get_current_config", {}))["data"]
    assert cur["name"] == "sg" and cur["is_current"] is True


def test_use_config_same_is_noop():
    tools, built = _make_tools()
    payload = _payload(tools.call("use_config", {"name": "hz"}))
    assert payload["success"] is True
    assert tools._current_name == "hz"
    assert built == []  # no rebuild for the already-active config


def test_use_config_unknown_keeps_current():
    tools, _ = _make_tools()
    payload = _payload(tools.call("use_config", {"name": "nope"}))
    assert payload["success"] is False
    assert "Unknown config" in payload["error"]
    assert tools._current_name == "hz"


def test_use_config_build_failure_keeps_current():
    configs = {
        "hz": _cfg("https://hz.example.com", "AK", "SK", project="p_hz"),
        "bad": _cfg("https://bad.example.com", "AK", "SK"),
    }

    def boom_builder(cfg):
        if cfg.maxcompute_endpoint == "https://bad.example.com":
            raise RuntimeError("connect failed")
        return ClientSet(sdk=MagicMock(), maxcompute_client=MagicMock(),
                         credential_client=MagicMock(),
                         default_project=cfg.default_project, namespace_id="")

    tools = Tools(
        sdk=MagicMock(), default_project="p_hz", namespace_id="",
        maxcompute_client=MagicMock(), credential_client=MagicMock(),
        configs=configs, default_name="hz", client_set_builder=boom_builder,
    )
    payload = _payload(tools.call("use_config", {"name": "bad"}))
    assert payload["success"] is False
    assert "connect failed" in payload["error"]
    assert tools._current_name == "hz"  # unchanged after failure


def test_use_config_missing_name_raises():
    tools, _ = _make_tools()
    with pytest.raises(JsonRpcError):
        tools.call("use_config", {})


def test_session_tools_registered_in_specs():
    tools, _ = _make_tools()
    names = {s.name for s in tools.specs()}
    assert {"list_configs", "use_config", "get_current_config"} <= names
