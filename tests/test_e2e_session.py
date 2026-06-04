# -*- coding: utf-8 -*-
"""E2E tests: session management tools (list_configs / get_current_config / use_config).

Validates the named-config runtime switching feature.

Uses the shared real_tools fixture from conftest.py, while overriding its
real_configs dependency to load the dedicated session-switch test config.

- Single-config (via MAXCOMPUTE_CATALOG_CONFIG override): all basic session tests run;
  cross-region tests are skipped.
- Multi-config (default config.multiconfig.json, or MAXCOMPUTE_CATALOG_CONFIG override):
  all tests run, including cross-region switch verification.

Requires config.multiconfig.json or MAXCOMPUTE_CATALOG_CONFIG env var pointing to a valid config file.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pytest

from maxcompute_catalog_mcp.config import MaxComputeCatalogConfig, load_configs
from maxcompute_catalog_mcp.mcp_protocol import JsonRpcError
from tests.conftest import text_payload as _text_payload

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _config_path() -> str:
    return os.environ.get("MAXCOMPUTE_CATALOG_CONFIG") or str(
        _PROJECT_ROOT / "config.multiconfig.json"
    )


def _load_all_configs():
    """Load configs; returns (configs_dict, default_name) or None on error."""
    path = Path(_config_path())
    if not path.exists():
        return None
    try:
        return load_configs(str(path))
    except Exception as e:
        logger.warning("config file %s exists but failed to load: %s", path, e)
        return None


_CONFIGS_RESULT = _load_all_configs()
_DEFAULT_NAME = _CONFIGS_RESULT[1] if _CONFIGS_RESULT is not None else "default"
_CONFIG_NAMES = sorted(_CONFIGS_RESULT[0].keys()) if _CONFIGS_RESULT is not None else []
_has_config = Path(_config_path()).exists()
_has_multi = len(_CONFIG_NAMES) >= 2


def _assert_no_config_secrets(
    payload: dict,
    configs: dict[str, MaxComputeCatalogConfig],
    context: str,
) -> None:
    blob = json.dumps(payload)
    for name, cfg in configs.items():
        if cfg.access_key_id:
            assert cfg.access_key_id not in blob, f"AccessKey ID leaked in {context}: {name}"
        if cfg.access_key_secret:
            assert cfg.access_key_secret not in blob, (
                f"AccessKey Secret leaked in {context}: {name}"
            )
        if cfg.security_token:
            assert cfg.security_token not in blob, f"STS token leaked in {context}: {name}"


# ---------------------------------------------------------------------------
# Fixtures for secret-leak assertions
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def all_configs() -> tuple[dict[str, MaxComputeCatalogConfig], str]:
    """Return all config objects and the default name."""
    if _CONFIGS_RESULT is None:
        pytest.skip("cannot load config for session tests")
    return _CONFIGS_RESULT


@pytest.fixture(scope="module")
def real_configs() -> tuple[dict[str, MaxComputeCatalogConfig], str]:
    """Use the dedicated multi-config file for session-switch integration tests."""
    if _CONFIGS_RESULT is None:
        pytest.skip("cannot load config for session tests")
    return _CONFIGS_RESULT


@pytest.fixture(scope="module")
def default_cfg(all_configs) -> MaxComputeCatalogConfig:
    """Return the default config object."""
    configs, default_name = all_configs
    return configs[default_name]


# ---------------------------------------------------------------------------
# 1. list_configs
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config, reason="no config file for session tests")
class TestListConfigs:
    """list_configs: returns config list with correct structure, no secrets."""

    def test_list_configs_returns_default(self, real_tools) -> None:
        r = real_tools.call("list_configs", {})
        payload = _text_payload(r)
        assert payload.get("success") is True, f"list_configs failed: {payload}"
        data = payload["data"]
        expected = _DEFAULT_NAME
        assert data["current"] == expected
        assert data["default"] == expected
        configs = data["configs"]
        assert len(configs) >= 1
        names = [c["name"] for c in configs]
        assert expected in names

    def test_list_configs_no_secret_leak(self, real_tools, all_configs) -> None:
        r = real_tools.call("list_configs", {})
        payload = _text_payload(r)
        configs, _ = all_configs
        _assert_no_config_secrets(payload, configs, "list_configs response")

    def test_list_configs_fields_present(self, real_tools) -> None:
        r = real_tools.call("list_configs", {})
        payload = _text_payload(r)
        assert payload.get("success") is True
        for cfg in payload["data"]["configs"]:
            assert "name" in cfg
            assert "region" in cfg
            assert "maxcompute_endpoint" in cfg
            assert "default_project" in cfg
            assert "is_default" in cfg
            assert "is_current" in cfg


# ---------------------------------------------------------------------------
# 2. get_current_config
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config, reason="no config file for session tests")
class TestGetCurrentConfig:
    """get_current_config: returns current active config info."""

    def test_get_current_config_success(self, real_tools) -> None:
        r = real_tools.call("get_current_config", {})
        payload = _text_payload(r)
        assert payload.get("success") is True, f"get_current_config failed: {payload}"
        data = payload["data"]
        expected = _DEFAULT_NAME
        assert data["name"] == expected
        assert data["maxcompute_endpoint"]
        assert data["is_current"] is True

    def test_get_current_config_no_secret(self, real_tools, default_cfg) -> None:
        r = real_tools.call("get_current_config", {})
        payload = _text_payload(r)
        _assert_no_config_secrets(payload, {_DEFAULT_NAME: default_cfg}, "get_current_config")


# ---------------------------------------------------------------------------
# 3. use_config
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config, reason="no config file for session tests")
class TestUseConfig:
    """use_config: switch behavior, error handling."""

    def test_use_config_same_name_noop(self, real_tools) -> None:
        expected = _DEFAULT_NAME
        reset = real_tools.call("use_config", {"name": expected})
        reset_payload = _text_payload(reset)
        assert reset_payload.get("success") is True, f"use_config reset failed: {reset_payload}"

        r = real_tools.call("use_config", {"name": expected})
        payload = _text_payload(r)
        assert payload.get("success") is True
        assert f"Already using config {expected!r}" in (payload.get("summary") or "")
        data = payload.get("data") or {}
        assert data.get("name") == expected
        assert data.get("is_current") is True

    def test_use_config_unknown_name_fails(self, real_tools) -> None:
        r = real_tools.call("use_config", {"name": "nonexistent_config_xyz_99999"})
        payload = _text_payload(r)
        assert payload.get("success") is False
        assert "Unknown config" in (payload.get("error") or "")

    def test_use_config_no_secret_leak(self, real_tools, all_configs) -> None:
        r = real_tools.call("use_config", {"name": _DEFAULT_NAME})
        payload = _text_payload(r)
        assert payload.get("success") is True, f"use_config failed: {payload}"
        configs, _ = all_configs
        _assert_no_config_secrets(payload, configs, "use_config response")

    def test_use_config_missing_name_raises_error(self, real_tools) -> None:
        with pytest.raises(JsonRpcError):
            real_tools.call("use_config", {})


# ---------------------------------------------------------------------------
# 4. Integration: session switch does not break catalog tools
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config, reason="no config file for session tests")
class TestSessionToolsIntegration:
    """Verify catalog tools still work after a session operation."""

    def test_session_switch_does_not_break_catalog_tools(self, real_tools) -> None:
        expected = _DEFAULT_NAME
        r1 = real_tools.call("use_config", {"name": expected})
        p1 = _text_payload(r1)
        assert p1.get("success") is True

        r2 = real_tools.call("list_projects", {"pageSize": 1})
        p2 = _text_payload(r2)
        assert "error" not in p2
        projects = (p2.get("data") or p2).get("projects")
        assert projects is not None


# ---------------------------------------------------------------------------
# 5. Cross-region switch (requires multi-config with 2+ named configs)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config, reason="no config file for session tests")
@pytest.mark.skipif(not _has_multi, reason="multi-config required (2+ named configs)")
class TestCrossRegionSwitch:
    """Verify actual cross-region switching with different named configs."""

    @pytest.fixture(autouse=True)
    def restore_default_config(self, real_tools):
        """Keep the module-scoped Tools active config isolated per test."""
        def switch_to_default(phase: str) -> None:
            result = real_tools.call("use_config", {"name": _DEFAULT_NAME})
            payload = _text_payload(result)
            assert payload.get("success") is True, (
                f"use_config default failed {phase}: {payload}"
            )

        switch_to_default("before test")
        yield
        try:
            switch_to_default("after test")
        except Exception as e:
            logger.warning("teardown: failed to restore default config: %s", e)

    def test_switch_to_second_config_succeeds(self, real_tools) -> None:
        default = _DEFAULT_NAME
        other = [n for n in _CONFIG_NAMES if n != default][0]
        r = real_tools.call("use_config", {"name": other})
        payload = _text_payload(r)
        assert payload.get("success") is True
        data = payload.get("data") or {}
        assert data.get("name") == other
        assert data.get("is_current") is True

        r2 = real_tools.call("get_current_config", {})
        p2 = _text_payload(r2)
        assert p2["data"]["name"] == other

    def test_list_projects_after_cross_region_switch(self, real_tools) -> None:
        default = _DEFAULT_NAME
        other = [n for n in _CONFIG_NAMES if n != default][0]
        switch_result = real_tools.call("use_config", {"name": other})
        switch_payload = _text_payload(switch_result)
        assert switch_payload.get("success") is True, f"use_config failed: {switch_payload}"
        assert (switch_payload.get("data") or {}).get("name") == other

        r = real_tools.call("list_projects", {"pageSize": 5})
        payload = _text_payload(r)
        assert "error" not in payload
        projects = (payload.get("data") or payload).get("projects")
        assert projects is not None
        logger.info("list_projects in %s returned %d project(s)", other, len(projects))

    def test_switch_back_to_default(self, real_tools) -> None:
        default = _DEFAULT_NAME
        r = real_tools.call("use_config", {"name": default})
        payload = _text_payload(r)
        assert payload.get("success") is True
        assert payload["data"]["name"] == default
