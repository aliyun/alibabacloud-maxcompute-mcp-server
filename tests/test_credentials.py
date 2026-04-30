"""Unit tests for credential module (get_credentials_from_default_chain, ResolvedCredentials)."""
from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from maxcompute_catalog_mcp.credentials import (
    ResolvedCredentials,
    get_credentials_client,
    get_credentials_from_default_chain,
)


def test_resolved_credentials_dataclass() -> None:
    c = ResolvedCredentials(
        access_key_id="ak",
        access_key_secret="sk",
        security_token="sts",
    )
    assert c.access_key_id == "ak"
    assert c.access_key_secret == "sk"
    assert c.security_token == "sts"

    c2 = ResolvedCredentials(access_key_id="a", access_key_secret="b")
    assert c2.security_token == ""


def test_get_credentials_from_default_chain_success() -> None:
    """Test successful credential retrieval via SDK default chain."""
    mock_cred = MagicMock()
    mock_cred.get_access_key_id.return_value = "test-ak-id"
    mock_cred.get_access_key_secret.return_value = "test-ak-secret"
    mock_cred.get_security_token.return_value = "test-sts-token"

    mock_client = MagicMock()
    mock_client.get_credential.return_value = mock_cred

    with patch(
        "alibabacloud_credentials.client.Client", return_value=mock_client
    ):
        creds = get_credentials_from_default_chain()

    assert creds.access_key_id == "test-ak-id"
    assert creds.access_key_secret == "test-ak-secret"
    assert creds.security_token == "test-sts-token"


def test_get_credentials_from_default_chain_without_sts() -> None:
    """Test credential retrieval without STS (no security_token)."""
    mock_cred = MagicMock()
    mock_cred.get_access_key_id.return_value = "ak-no-sts"
    mock_cred.get_access_key_secret.return_value = "sk-no-sts"
    mock_cred.get_security_token.return_value = None

    mock_client = MagicMock()
    mock_client.get_credential.return_value = mock_cred

    with patch(
        "alibabacloud_credentials.client.Client", return_value=mock_client
    ):
        creds = get_credentials_from_default_chain()

    assert creds.access_key_id == "ak-no-sts"
    assert creds.access_key_secret == "sk-no-sts"
    assert creds.security_token == ""


def test_get_credentials_from_default_chain_empty_ak_raises() -> None:
    """Test that empty AK from SDK raises an exception."""
    mock_cred = MagicMock()
    mock_cred.get_access_key_id.return_value = ""
    mock_cred.get_access_key_secret.return_value = "sk-only"
    mock_cred.get_security_token.return_value = None

    mock_client = MagicMock()
    mock_client.get_credential.return_value = mock_cred

    with patch(
        "alibabacloud_credentials.client.Client", return_value=mock_client
    ):
        with pytest.raises(ValueError, match="did not return valid"):
            get_credentials_from_default_chain()


def test_get_credentials_from_default_chain_sdk_not_installed() -> None:
    """Test clear error when SDK is not installed."""
    # Setting sys.modules[name] = None makes future imports of that name raise ModuleNotFoundError.
    # This is safer than patching builtins.__import__ globally (which can break parallel test runs).
    with patch.dict(sys.modules, {"alibabacloud_credentials.client": None}):
        with pytest.raises(ValueError, match="alibabacloud-credentials is required"):
            get_credentials_from_default_chain()


def test_get_credentials_from_default_chain_sdk_error() -> None:
    """Test that SDK internal errors propagate correctly."""
    with patch(
        "alibabacloud_credentials.client.Client",
        side_effect=RuntimeError("SDK internal error"),
    ):
        with pytest.raises(ValueError, match="Failed to get credential"):
            get_credentials_from_default_chain()


# ---------------------------------------------------------------------------
# get_credentials_client() tests
# ---------------------------------------------------------------------------


class TestGetCredentialsClient:
    """Cover get_credentials_client(): static AK/SK, STS, default chain, probe failure."""

    @patch("alibabacloud_credentials.models.Config")
    @patch("alibabacloud_credentials.client.Client")
    def test_static_ak_sk(self, mock_client_cls, mock_config_cls) -> None:
        """Static AK/SK → Config(type='access_key')."""
        result = get_credentials_client(access_key_id="myak", access_key_secret="mysk")
        mock_config_cls.assert_called_once_with(
            type="access_key", access_key_id="myak", access_key_secret="mysk",
        )
        mock_client_cls.assert_called_once_with(config=mock_config_cls.return_value)
        assert result is mock_client_cls.return_value

    @patch("alibabacloud_credentials.models.Config")
    @patch("alibabacloud_credentials.client.Client")
    def test_static_sts(self, mock_client_cls, mock_config_cls) -> None:
        """Static AK/SK + STS token → Config(type='sts')."""
        result = get_credentials_client(
            access_key_id="ak", access_key_secret="sk", security_token="tok",
        )
        mock_config_cls.assert_called_once_with(
            type="sts",
            access_key_id="ak",
            access_key_secret="sk",
            security_token="tok",
        )
        assert result is mock_client_cls.return_value

    @patch("alibabacloud_credentials.client.Client")
    def test_default_chain_success(self, mock_client_cls) -> None:
        """No static creds → default chain probe succeeds → return client."""
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        result = get_credentials_client()
        mock_client_cls.assert_called_once_with()
        mock_client.get_credential.assert_called_once()
        assert result is mock_client

    @patch("alibabacloud_credentials.client.Client")
    def test_default_chain_probe_failure(self, mock_client_cls) -> None:
        """Default chain probe fails → ValueError with diagnostic message."""
        mock_client = MagicMock()
        mock_client.get_credential.side_effect = RuntimeError("no creds available")
        mock_client_cls.return_value = mock_client
        with pytest.raises(ValueError, match="Default credential chain returned no valid credentials"):
            get_credentials_client()

    def test_sdk_not_installed(self) -> None:
        """alibabacloud-credentials not importable → ValueError."""
        # Block both submodules used by get_credentials_client.
        with patch.dict(sys.modules, {
            "alibabacloud_credentials.client": None,
            "alibabacloud_credentials.models": None,
        }):
            with pytest.raises(ValueError, match="alibabacloud-credentials is required"):
                get_credentials_client()
