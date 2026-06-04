"""Unit tests for client_factory.build_client_set (mocked credentials/clients/SDK).

These cover the credential / endpoint-resolution / SDK-creation behaviour that
used to live inside server.build_tools() before the named-config refactor.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from maxcompute_catalog_mcp.client_factory import ClientSet, build_client_set
from maxcompute_catalog_mcp.config import MaxComputeCatalogConfig


def _cfg(**kw) -> MaxComputeCatalogConfig:
    base = dict(
        catalogapi_endpoint="https://catalog.example.com",
        maxcompute_endpoint="https://mc.example.com",
        access_key_id="AK", access_key_secret="SK",
        default_project="proj", namespace_id="ns",
    )
    base.update(kw)
    return MaxComputeCatalogConfig(**base)


@patch("maxcompute_catalog_mcp.client_factory.MaxComputeCatalogSdk")
@patch("maxcompute_catalog_mcp.client_factory.MaxComputeClient")
@patch("maxcompute_catalog_mcp.client_factory.get_credentials_client")
def test_success(mock_creds, mock_mc, mock_sdk) -> None:
    mock_creds.return_value = MagicMock()
    mock_mc.create.return_value = MagicMock()
    mock_sdk.create.return_value = MagicMock()

    cs = build_client_set(_cfg())
    assert isinstance(cs, ClientSet)
    assert cs.default_project == "proj"
    assert cs.namespace_id == "ns"
    mock_creds.assert_called_once()
    mock_sdk.create.assert_called_once()


@patch("maxcompute_catalog_mcp.client_factory.get_credentials_client")
def test_credential_failure_raises_valueerror(mock_creds) -> None:
    mock_creds.side_effect = ValueError("no credentials")
    with pytest.raises(ValueError, match="no credentials"):
        build_client_set(_cfg(access_key_id="", access_key_secret=""))


@patch("maxcompute_catalog_mcp.client_factory.resolve_catalogapi_endpoint_with_client")
@patch("maxcompute_catalog_mcp.client_factory.MaxComputeCatalogSdk")
@patch("maxcompute_catalog_mcp.client_factory.MaxComputeClient")
@patch("maxcompute_catalog_mcp.client_factory.get_credentials_client")
def test_resolves_endpoint_when_empty(mock_creds, mock_mc, mock_sdk, mock_resolve) -> None:
    mock_creds.return_value = MagicMock()
    mc_client = MagicMock()
    mc_client.odps_client = MagicMock()
    mock_mc.create.return_value = mc_client
    mock_resolve.return_value = "https://resolved.example.com"
    mock_sdk.create.return_value = MagicMock()

    cs = build_client_set(_cfg(catalogapi_endpoint=""))
    assert isinstance(cs, ClientSet)
    mock_resolve.assert_called_once()


@patch("maxcompute_catalog_mcp.client_factory.resolve_catalogapi_endpoint_with_client")
@patch("maxcompute_catalog_mcp.client_factory.MaxComputeClient")
@patch("maxcompute_catalog_mcp.client_factory.get_credentials_client")
def test_resolve_failure_raises_runtimeerror(mock_creds, mock_mc, mock_resolve) -> None:
    mock_creds.return_value = MagicMock()
    mc_client = MagicMock()
    mc_client.odps_client = MagicMock()
    mock_mc.create.return_value = mc_client
    mock_resolve.side_effect = ValueError("resolve failed")  # resolver raises ValueError

    with pytest.raises(RuntimeError, match="Failed to resolve catalogapi endpoint"):
        build_client_set(_cfg(catalogapi_endpoint=""))


@patch("maxcompute_catalog_mcp.client_factory.MaxComputeClient")
@patch("maxcompute_catalog_mcp.client_factory.get_credentials_client")
def test_no_mc_client_no_endpoint_raises_runtimeerror(mock_creds, mock_mc) -> None:
    mock_creds.return_value = MagicMock()
    mock_mc.create.return_value = None  # no compute client

    with pytest.raises(RuntimeError, match="Cannot resolve catalogapi_endpoint"):
        build_client_set(_cfg(catalogapi_endpoint=""))


@patch("maxcompute_catalog_mcp.client_factory.MaxComputeCatalogSdk")
@patch("maxcompute_catalog_mcp.client_factory.MaxComputeClient")
@patch("maxcompute_catalog_mcp.client_factory.get_credentials_client")
def test_sdk_creation_failure_propagates(mock_creds, mock_mc, mock_sdk) -> None:
    mock_creds.return_value = MagicMock()
    mock_mc.create.return_value = MagicMock()
    mock_sdk.create.side_effect = RuntimeError("SDK init failed")

    with pytest.raises(RuntimeError, match="SDK init failed"):
        build_client_set(_cfg())
