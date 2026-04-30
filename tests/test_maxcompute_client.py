"""Unit tests for maxcompute_client.py."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from maxcompute_catalog_mcp.config import (
    MaxComputeCatalogConfig,
    ResolvedEndpoints,
    resolve_protocol_and_endpoints,
)
from maxcompute_catalog_mcp.maxcompute_client import (
    MaxComputeCatalogSdk,
    MaxComputeClient,
    _build_catalog_client,
)


# ---------------------------------------------------------------------------
# _build_catalog_client()
# ---------------------------------------------------------------------------


class TestBuildCatalogClient:
    @patch("maxcompute_catalog_mcp.maxcompute_client.CatalogClient")
    @patch("maxcompute_catalog_mcp.maxcompute_client.open_api_models")
    def test_with_credential_client(self, mock_api, mock_catalog) -> None:
        cred = MagicMock()
        _build_catalog_client("catalog.example.com", credential_client=cred)
        config_call = mock_api.Config.call_args
        assert config_call.kwargs.get("credential") is cred
        assert "access_key_id" not in config_call.kwargs

    @patch("maxcompute_catalog_mcp.maxcompute_client.CatalogClient")
    @patch("maxcompute_catalog_mcp.maxcompute_client.open_api_models")
    def test_fallback_static_ak(self, mock_api, mock_catalog) -> None:
        _build_catalog_client(
            "catalog.example.com",
            access_key_id="AK", access_key_secret="SK", security_token="TOK",
        )
        config_call = mock_api.Config.call_args
        assert config_call.kwargs.get("access_key_id") == "AK"
        assert config_call.kwargs.get("security_token") == "TOK"

    @patch("maxcompute_catalog_mcp.maxcompute_client.CatalogClient")
    @patch("maxcompute_catalog_mcp.maxcompute_client.open_api_models")
    def test_protocol_passed_when_nonempty(self, mock_api, mock_catalog) -> None:
        _build_catalog_client(
            "catalog.example.com",
            credential_client=MagicMock(),
            protocol="HTTPS",
        )
        config_call = mock_api.Config.call_args
        assert config_call.kwargs.get("protocol") == "HTTPS"

    @patch("maxcompute_catalog_mcp.maxcompute_client.CatalogClient")
    @patch("maxcompute_catalog_mcp.maxcompute_client.open_api_models")
    def test_protocol_omitted_when_empty(self, mock_api, mock_catalog) -> None:
        _build_catalog_client(
            "catalog.example.com",
            credential_client=MagicMock(),
            protocol="",
        )
        config_call = mock_api.Config.call_args
        assert "protocol" not in config_call.kwargs

    @patch("maxcompute_catalog_mcp.maxcompute_client.CatalogClient")
    @patch("maxcompute_catalog_mcp.maxcompute_client.open_api_models")
    def test_fallback_on_type_error(self, mock_api, mock_catalog) -> None:
        """TypeError from Config → retry without credential/security_token."""
        call_count = 0
        def config_side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise TypeError("unexpected keyword")
            return MagicMock()
        mock_api.Config.side_effect = config_side_effect
        _build_catalog_client("ep.com", credential_client=MagicMock())
        assert call_count == 2


# ---------------------------------------------------------------------------
# MaxComputeCatalogSdk
# ---------------------------------------------------------------------------


class TestMaxComputeCatalogSdk:
    def test_create_no_credential_raises(self) -> None:
        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="catalog.example.com",
            maxcompute_endpoint="https://mc.example.com",
            access_key_id="AK", access_key_secret="SK",
        )
        with pytest.raises(RuntimeError, match="credential_client is required"):
            MaxComputeCatalogSdk.create(cfg, credential_client=None)

    @patch("maxcompute_catalog_mcp.maxcompute_client._build_catalog_client")
    def test_create_success(self, mock_build) -> None:
        mock_build.return_value = MagicMock()
        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="catalog.example.com",
            maxcompute_endpoint="https://mc.example.com",
            access_key_id="AK", access_key_secret="SK",
        )
        sdk = MaxComputeCatalogSdk.create(cfg, credential_client=MagicMock())
        assert sdk.client is mock_build.return_value

    @patch("maxcompute_catalog_mcp.maxcompute_client._build_catalog_client")
    def test_create_uses_resolved_endpoints(self, mock_build) -> None:
        mock_build.return_value = MagicMock()
        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="catalog.example.com",
            maxcompute_endpoint="https://mc.example.com",
            access_key_id="AK", access_key_secret="SK",
        )
        resolved = ResolvedEndpoints(
            maxcompute_protocol="https",
            maxcompute_url="https://mc.example.com",
            catalogapi_protocol="http",
            catalogapi_host="catalog.example.com",
        )
        MaxComputeCatalogSdk.create(cfg, credential_client=MagicMock(), resolved=resolved)
        # _build_catalog_client should be called with catalogapi_host and protocol
        call_args = mock_build.call_args
        assert call_args[0][0] == "catalog.example.com"
        assert call_args.kwargs.get("protocol") == "HTTP"

    def test_client_property(self) -> None:
        mock_client = MagicMock()
        sdk = MaxComputeCatalogSdk(catalog_client=mock_client)
        assert sdk.client is mock_client


# ---------------------------------------------------------------------------
# MaxComputeClient
# ---------------------------------------------------------------------------


class TestMaxComputeClient:
    def test_create_no_odps(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ODPS=None → returns None."""
        import maxcompute_catalog_mcp.maxcompute_client as mc_mod
        monkeypatch.setattr(mc_mod, "ODPS", None)
        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="", maxcompute_endpoint="https://mc.example.com",
            access_key_id="AK", access_key_secret="SK", default_project="proj",
        )
        assert MaxComputeClient.create(cfg, credential_client=MagicMock()) is None

    def test_create_no_project(self) -> None:
        """Empty default_project → returns None."""
        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="", maxcompute_endpoint="https://mc.example.com",
            access_key_id="AK", access_key_secret="SK", default_project="",
        )
        assert MaxComputeClient.create(cfg, credential_client=MagicMock()) is None

    def test_create_no_endpoint(self) -> None:
        """Empty maxcompute_endpoint with default_project → None (after strip)."""
        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="", maxcompute_endpoint="  ",
            access_key_id="AK", access_key_secret="SK", default_project="proj",
        )
        assert MaxComputeClient.create(cfg, credential_client=MagicMock()) is None

    def test_create_no_credential_client_raises(self) -> None:
        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="", maxcompute_endpoint="https://mc.example.com",
            access_key_id="AK", access_key_secret="SK", default_project="proj",
        )
        with pytest.raises(RuntimeError, match="credential_client is required"):
            MaxComputeClient.create(cfg, credential_client=None)

    def test_create_credential_provider_account_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """H9: CredentialProviderAccount is None → RuntimeError (older pyodps)."""
        import maxcompute_catalog_mcp.maxcompute_client as mc_mod

        monkeypatch.setattr(mc_mod, "ODPS", MagicMock())
        monkeypatch.setattr(mc_mod, "CredentialProviderAccount", None)
        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="",
            maxcompute_endpoint="https://mc.example.com",
            access_key_id="AK", access_key_secret="SK",
            default_project="proj",
        )
        with pytest.raises(RuntimeError, match="CredentialProviderAccount not available"):
            MaxComputeClient.create(cfg, credential_client=MagicMock())

    def test_create_success_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """H8: ODPS/CredentialProviderAccount available → constructs ODPS client with correct args."""
        import maxcompute_catalog_mcp.maxcompute_client as mc_mod

        odps_instance = MagicMock(name="odps_instance")
        mock_odps_cls = MagicMock(return_value=odps_instance)
        mock_cpa_cls = MagicMock(return_value="fake-account")
        monkeypatch.setattr(mc_mod, "ODPS", mock_odps_cls)
        monkeypatch.setattr(mc_mod, "CredentialProviderAccount", mock_cpa_cls)
        monkeypatch.setattr(mc_mod, "_FULL_USER_AGENT", "ua-test/1.0")

        cred = MagicMock(name="credential_client")
        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="",
            maxcompute_endpoint="https://mc.example.com",
            access_key_id="AK", access_key_secret="SK",
            default_project="my_proj",
        )
        result = MaxComputeClient.create(cfg, credential_client=cred)

        assert isinstance(result, MaxComputeClient)
        assert result.odps_client is odps_instance

        mock_cpa_cls.assert_called_once_with(cred)

        odps_call = mock_odps_cls.call_args
        assert odps_call.kwargs["account"] == "fake-account"
        assert odps_call.kwargs["project"] == "my_proj"
        assert odps_call.kwargs["endpoint"] == "https://mc.example.com"
        assert odps_call.kwargs["rest_client_kwargs"] == {"user_agent": "ua-test/1.0"}

    def test_create_with_resolved_endpoints(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Explicit resolved endpoints override config-derived ones."""
        import maxcompute_catalog_mcp.maxcompute_client as mc_mod

        odps_instance = MagicMock(name="odps_instance")
        mock_odps_cls = MagicMock(return_value=odps_instance)
        mock_cpa_cls = MagicMock(return_value="fake-account")
        monkeypatch.setattr(mc_mod, "ODPS", mock_odps_cls)
        monkeypatch.setattr(mc_mod, "CredentialProviderAccount", mock_cpa_cls)
        monkeypatch.setattr(mc_mod, "_FULL_USER_AGENT", "ua-test/1.0")

        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="",
            maxcompute_endpoint="http://mc.example.com",
            access_key_id="AK", access_key_secret="SK",
            default_project="my_proj",
            protocol="https",
        )
        resolved = ResolvedEndpoints(
            maxcompute_protocol="https",
            maxcompute_url="https://mc.example.com",
            catalogapi_protocol="https",
            catalogapi_host="catalog.example.com",
        )
        result = MaxComputeClient.create(cfg, credential_client=MagicMock(), resolved=resolved)
        assert isinstance(result, MaxComputeClient)
        odps_call = mock_odps_cls.call_args
        # resolved.maxcompute_url should be used
        assert odps_call.kwargs["endpoint"] == "https://mc.example.com"

    def test_delegates_to_odps(self) -> None:
        """__getattr__ delegates to underlying ODPS client."""
        inner = MagicMock()
        inner.some_method.return_value = "result"
        client = MaxComputeClient(_client=inner)
        assert client.some_method() == "result"

    def test_odps_client_property(self) -> None:
        inner = MagicMock()
        client = MaxComputeClient(_client=inner)
        assert client.odps_client is inner
