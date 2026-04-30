"""Unit tests for config.py — load_config(), resolve_catalogapi_endpoint_with_client(), and per-client protocol/endpoint resolver."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from maxcompute_catalog_mcp.config import (
    MaxComputeCatalogConfig,
    ResolvedEndpoints,
    load_config,
    resolve_catalogapi_endpoint_with_client,
    resolve_protocol_and_endpoints,
    split_scheme,
)


def _cfg(
    *,
    maxcompute_endpoint: str = "",
    catalogapi_endpoint: str = "",
    protocol: str = "",
) -> MaxComputeCatalogConfig:
    return MaxComputeCatalogConfig(
        catalogapi_endpoint=catalogapi_endpoint,
        maxcompute_endpoint=maxcompute_endpoint,
        access_key_id="ak",
        access_key_secret="sk",
        protocol=protocol,
    )


# ---------------------------------------------------------------------------
# split_scheme() tests
# ---------------------------------------------------------------------------


class TestSplitScheme:
    def test_https_lowercase(self):
        assert split_scheme("https://host.example.com/api") == (
            "https",
            "host.example.com/api",
        )

    def test_http_lowercase(self):
        assert split_scheme("http://host:8080/p") == ("http", "host:8080/p")

    def test_case_insensitive(self):
        assert split_scheme("HTTPS://Host.example.com") == ("https", "Host.example.com")
        assert split_scheme("Http://h") == ("http", "h")

    def test_no_scheme(self):
        assert split_scheme("host.example.com") == (None, "host.example.com")

    def test_whitespace_trimmed(self):
        assert split_scheme("  https://x  ") == ("https", "x")
        assert split_scheme("  https://x/y  ") == ("https", "x/y")

    def test_empty(self):
        assert split_scheme("") == (None, "")
        assert split_scheme("   ") == (None, "")


# ---------------------------------------------------------------------------
# resolve_protocol_and_endpoints() tests
# ---------------------------------------------------------------------------


class TestResolverMaxComputeChain:
    """maxcompute_protocol priority: explicit protocol > mc scheme > https."""

    def test_explicit_protocol_wins(self):
        r = resolve_protocol_and_endpoints(
            _cfg(maxcompute_endpoint="http://mc.example.com/api", protocol="https")
        )
        assert r.maxcompute_protocol == "https"
        assert r.maxcompute_url == "https://mc.example.com/api"

    def test_mc_scheme_when_no_explicit(self):
        r = resolve_protocol_and_endpoints(
            _cfg(maxcompute_endpoint="http://mc.example.com/api")
        )
        assert r.maxcompute_protocol == "http"
        assert r.maxcompute_url == "http://mc.example.com/api"

    def test_default_https_when_nothing_set(self):
        r = resolve_protocol_and_endpoints(_cfg(maxcompute_endpoint="mc.example.com/api"))
        assert r.maxcompute_protocol == "https"
        assert r.maxcompute_url == "https://mc.example.com/api"


class TestResolverCatalogapiChain:
    """catalogapi_protocol priority: explicit protocol > catalog scheme > mc scheme > https."""

    def test_explicit_protocol_wins_over_all(self):
        r = resolve_protocol_and_endpoints(
            _cfg(
                maxcompute_endpoint="http://mc.example.com",
                catalogapi_endpoint="http://cat.example.com",
                protocol="https",
            )
        )
        assert r.catalogapi_protocol == "https"
        assert r.catalogapi_host == "cat.example.com"

    def test_catalog_scheme_when_no_explicit(self):
        r = resolve_protocol_and_endpoints(
            _cfg(
                maxcompute_endpoint="https://mc.example.com",
                catalogapi_endpoint="http://cat.example.com",
            )
        )
        # per-client: catalogapi uses its own scheme
        assert r.catalogapi_protocol == "http"
        assert r.catalogapi_host == "cat.example.com"
        assert r.maxcompute_protocol == "https"

    def test_follow_mc_scheme_when_catalog_has_none(self):
        r = resolve_protocol_and_endpoints(
            _cfg(
                maxcompute_endpoint="http://mc.example.com",
                catalogapi_endpoint="cat.example.com",
            )
        )
        assert r.catalogapi_protocol == "http"
        assert r.catalogapi_host == "cat.example.com"

    def test_default_https_when_no_scheme_anywhere(self):
        r = resolve_protocol_and_endpoints(
            _cfg(
                maxcompute_endpoint="mc.example.com",
                catalogapi_endpoint="cat.example.com",
            )
        )
        assert r.catalogapi_protocol == "https"
        assert r.catalogapi_host == "cat.example.com"

    def test_both_endpoints_keep_own_schemes(self):
        """Different schemes on each endpoint must not cross-contaminate."""
        r = resolve_protocol_and_endpoints(
            _cfg(
                maxcompute_endpoint="http://mc.example.com",
                catalogapi_endpoint="https://cat.example.com",
            )
        )
        assert r.maxcompute_protocol == "http"
        assert r.catalogapi_protocol == "https"


class TestResolvedEndpointsFields:
    def test_is_resolved_dataclass(self):
        r = resolve_protocol_and_endpoints(
            _cfg(
                maxcompute_endpoint="https://mc",
                catalogapi_endpoint="https://cat",
            )
        )
        assert isinstance(r, ResolvedEndpoints)
        assert r.maxcompute_protocol == "https"
        assert r.maxcompute_url == "https://mc"
        assert r.catalogapi_protocol == "https"
        assert r.catalogapi_host == "cat"

    def test_empty_mc_host_yields_empty_url(self):
        r = resolve_protocol_and_endpoints(_cfg(maxcompute_endpoint=""))
        assert r.maxcompute_url == ""


class TestLoadConfigProtocol:
    def _write(self, tmp_path: Path, **maxcompute) -> Path:
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"maxcompute": maxcompute}), encoding="utf-8")
        return p

    def test_protocol_field_loaded_lowercase(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("MAXCOMPUTE_PROTOCOL", raising=False)
        p = self._write(
            tmp_path,
            maxcompute_endpoint="https://mc.example.com",
            accessKeyId="ak",
            accessKeySecret="sk",
            protocol="HTTPS",
        )
        cfg = load_config(str(p))
        assert cfg.protocol == "https"

    def test_invalid_protocol_rejected(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("MAXCOMPUTE_PROTOCOL", raising=False)
        p = self._write(
            tmp_path,
            maxcompute_endpoint="https://mc.example.com",
            accessKeyId="ak",
            accessKeySecret="sk",
            protocol="ftp",
        )
        with pytest.raises(ValueError, match="Invalid protocol"):
            load_config(str(p))

    def test_env_overrides_file(self, tmp_path: Path, monkeypatch):
        p = self._write(
            tmp_path,
            maxcompute_endpoint="https://mc.example.com",
            accessKeyId="ak",
            accessKeySecret="sk",
            protocol="https",
        )
        monkeypatch.setenv("MAXCOMPUTE_PROTOCOL", "http")
        cfg = load_config(str(p))
        assert cfg.protocol == "http"

    def test_missing_protocol_defaults_empty(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("MAXCOMPUTE_PROTOCOL", raising=False)
        p = self._write(
            tmp_path,
            maxcompute_endpoint="https://mc.example.com",
            accessKeyId="ak",
            accessKeySecret="sk",
        )
        cfg = load_config(str(p))
        assert cfg.protocol == ""


# ---------------------------------------------------------------------------
# load_config() tests
# ---------------------------------------------------------------------------


def test_load_config_from_json_file(tmp_path: Path) -> None:
    """Load config from a JSON file with 'maxcompute' key."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({
        "maxcompute": {
            "catalogapi_endpoint": "https://catalog.example.com",
            "maxcompute_endpoint": "https://mc.example.com",
            "accessKeyId": "AK_ID",
            "accessKeySecret": "AK_SECRET",
            "securityToken": "TOKEN",
            "defaultProject": "proj1",
            "namespaceId": "ns123",
        }
    }))
    cfg = load_config(str(cfg_file))
    assert cfg.catalogapi_endpoint == "https://catalog.example.com"
    assert cfg.maxcompute_endpoint == "https://mc.example.com"
    assert cfg.access_key_id == "AK_ID"
    assert cfg.access_key_secret == "AK_SECRET"
    assert cfg.security_token == "TOKEN"
    assert cfg.default_project == "proj1"
    assert cfg.namespace_id == "ns123"


def test_load_config_from_odps_key(tmp_path: Path) -> None:
    """Load config from a JSON file with 'odps' key instead of 'maxcompute'."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({
        "odps": {
            "maxcompute_endpoint": "https://mc.example.com",
            "access_key_id": "AK",
            "access_key_secret": "SK",
        }
    }))
    cfg = load_config(str(cfg_file))
    assert cfg.maxcompute_endpoint == "https://mc.example.com"
    assert cfg.access_key_id == "AK"
    assert cfg.access_key_secret == "SK"


def test_load_config_env_overrides_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Environment variables override file values."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({
        "maxcompute": {
            "maxcompute_endpoint": "https://file.example.com",
            "accessKeyId": "FILE_AK",
            "accessKeySecret": "FILE_SK",
        }
    }))
    monkeypatch.setenv("MAXCOMPUTE_ENDPOINT", "https://env.example.com")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "ENV_AK")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "ENV_SK")
    cfg = load_config(str(cfg_file))
    assert cfg.maxcompute_endpoint == "https://env.example.com"
    assert cfg.access_key_id == "ENV_AK"
    assert cfg.access_key_secret == "ENV_SK"


def test_load_config_missing_endpoint_raises(tmp_path: Path) -> None:
    """Missing maxcompute_endpoint raises ValueError."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({"maxcompute": {"accessKeyId": "AK"}}))
    with pytest.raises(ValueError, match="maxcompute_endpoint"):
        load_config(str(cfg_file))


def test_load_config_no_file_env_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Load config without file, env-only."""
    # Point to a non-existent file
    monkeypatch.setenv("MAXCOMPUTE_ENDPOINT", "https://mc.example.com")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "ENV_AK")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "ENV_SK")
    monkeypatch.setenv("MAXCOMPUTE_DEFAULT_PROJECT", "envproj")
    cfg = load_config(str(tmp_path / "nonexistent.json"))
    assert cfg.maxcompute_endpoint == "https://mc.example.com"
    assert cfg.access_key_id == "ENV_AK"
    assert cfg.default_project == "envproj"


def test_load_config_alias_fields(tmp_path: Path) -> None:
    """Verify alias field names work (catalogapiEndpoint, sdkEndpoint, etc.)."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({
        "maxcompute": {
            "catalogapiEndpoint": "https://cat.example.com",
            "sdkEndpoint": "https://sdk.example.com",
            "accessKeyId": "AK1",
            "accessKeySecret": "SK1",
            "defaultProject": "proj_alias",
            "namespaceId": "ns_alias",
        }
    }))
    cfg = load_config(str(cfg_file))
    assert cfg.catalogapi_endpoint == "https://cat.example.com"
    assert cfg.maxcompute_endpoint == "https://sdk.example.com"
    assert cfg.default_project == "proj_alias"
    assert cfg.namespace_id == "ns_alias"


def test_load_config_empty_json_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty JSON file ({}) — env must still provide required fields."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text("{}")
    monkeypatch.setenv("MAXCOMPUTE_ENDPOINT", "https://mc.example.com")
    cfg = load_config(str(cfg_file))
    assert cfg.maxcompute_endpoint == "https://mc.example.com"


# ---------------------------------------------------------------------------
# resolve_catalogapi_endpoint_with_client() tests
# ---------------------------------------------------------------------------


def test_resolve_endpoint_success() -> None:
    """Normal resolution returns response body."""
    client = MagicMock()
    resp = MagicMock()
    resp.ok = True
    resp.text = "https://catalog-resolved.example.com"
    client.rest.get.return_value = resp
    result = resolve_catalogapi_endpoint_with_client(client, "https://mc.example.com")
    assert result == "https://catalog-resolved.example.com"
    client.rest.get.assert_called_once_with("https://mc.example.com/catalogapi", timeout=10.0)


def test_resolve_endpoint_empty_maxcompute_endpoint() -> None:
    """Empty maxcompute_endpoint raises ValueError."""
    with pytest.raises(ValueError, match="maxcompute_url is empty"):
        resolve_catalogapi_endpoint_with_client(MagicMock(), "")


def test_resolve_endpoint_strips_trailing_slash() -> None:
    """Trailing slash on endpoint is stripped before appending /catalogapi."""
    client = MagicMock()
    resp = MagicMock()
    resp.ok = True
    resp.text = "resolved"
    client.rest.get.return_value = resp
    resolve_catalogapi_endpoint_with_client(client, "https://mc.example.com/")
    client.rest.get.assert_called_once_with("https://mc.example.com/catalogapi", timeout=10.0)


def test_resolve_endpoint_network_error() -> None:
    """Network error wrapped as ValueError."""
    client = MagicMock()
    client.rest.get.side_effect = ConnectionError("connection refused")
    with pytest.raises(ValueError, match="Can't get catalog api server address"):
        resolve_catalogapi_endpoint_with_client(client, "https://mc.example.com")


def test_resolve_endpoint_http_error() -> None:
    """HTTP error (resp.ok=False) raises ValueError with status code."""
    client = MagicMock()
    resp = MagicMock()
    resp.ok = False
    resp.status_code = 404
    client.rest.get.return_value = resp
    with pytest.raises(ValueError, match="HTTP 404"):
        resolve_catalogapi_endpoint_with_client(client, "https://mc.example.com")


def test_resolve_endpoint_empty_body() -> None:
    """Empty response body raises ValueError."""
    client = MagicMock()
    resp = MagicMock()
    resp.ok = True
    resp.text = "   "
    client.rest.get.return_value = resp
    with pytest.raises(ValueError, match="empty response body"):
        resolve_catalogapi_endpoint_with_client(client, "https://mc.example.com")


def test_resolve_endpoint_content_decode_fallback() -> None:
    """M5: resp has no `text` attr → falls back to resp.content.decode('utf-8')."""
    client = MagicMock()
    # spec=["ok", "content"] ensures hasattr(resp, "text") is False.
    resp = MagicMock(spec=["ok", "content"])
    resp.ok = True
    resp.content = b"  https://catalog-fallback.example.com  "
    client.rest.get.return_value = resp
    result = resolve_catalogapi_endpoint_with_client(client, "https://mc.example.com")
    assert result == "https://catalog-fallback.example.com"
