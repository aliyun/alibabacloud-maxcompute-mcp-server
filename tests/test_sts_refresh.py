"""Unit tests and integration tests for automatic credential refresh.

Test coverage:
1. _build_catalog_client: credential_client path vs static ak/sk/token path
2. MaxComputeCatalogSdk.create(): with/without credential_client
3. MaxComputeClient.create(): using CredentialProviderAccount
4. get_credentials_client(): returns Client instance
5. E2E: Tea SDK holds credential_client reference, automatically uses new token after refresh
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---- Inline minimal mock credentials server (no dependency on mock_credentials_server.py) ----

class _CredHandler(BaseHTTPRequestHandler):
    """Minimal credentials HTTP server; generates Expiration dynamically on each request
    so that expire_seconds is relative to when the HTTP request is served, not when
    set_credentials() was called. This avoids race conditions in fast-running tests.
    """
    def do_GET(self) -> None:
        if "/credentials" in self.path:
            expire_seconds = self.server._expire_seconds
            exp = datetime.now(timezone.utc) + timedelta(seconds=expire_seconds)
            creds = {
                "Code": "Success",
                "AccessKeyId": "mock-ak-id",
                "AccessKeySecret": "mock-ak-secret",
                "SecurityToken": self.server._token,
                "Expiration": exp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            data = json.dumps(creds, ensure_ascii=False).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            self.server.request_count += 1
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):
        pass  # Suppress HTTP log output


class _MockCredServer:
    """HTTP server that allows dynamic credential switching for SDK refresh testing.

    Token and expire_seconds are stored separately; _CredHandler computes Expiration
    at request time so expire_seconds is always relative to the actual HTTP fetch,
    not to when set_credentials() was called.
    """

    def __init__(self, port: int = 0):
        self._server = HTTPServer(("127.0.0.1", port), _CredHandler)
        self._server._token = "token-v1"
        self._server._expire_seconds = 3600
        self._server.request_count = 0
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def start(self):
        self._thread.start()
        return self

    def port(self) -> int:
        return self._server.server_address[1]

    def url(self) -> str:
        return f"http://127.0.0.1:{self.port()}/credentials"

    def set_credentials(self, token: str, expire_seconds: int = 3600):
        """Dynamically switch returned credentials (without restarting server)."""
        self._server._token = token
        self._server._expire_seconds = expire_seconds

    def request_count(self) -> int:
        return self._server.request_count

    def stop(self):
        self._server.shutdown()


# ==================== Unit Tests ====================

class TestCredentialClientFunction:
    """Unit tests for get_credentials_client()."""

    def test_returns_client_instance_with_default_chain(self):
        """get_credentials_client() should return a Client instance with default credential chain."""
        from maxcompute_catalog_mcp.credentials import get_credentials_client
        from alibabacloud_credentials.client import Client as CredentialClient
        mock_client = MagicMock(spec=CredentialClient)
        with patch("alibabacloud_credentials.client.Client", return_value=mock_client):
            result = get_credentials_client()
        assert result is mock_client

    def test_returns_client_with_static_credentials(self):
        """get_credentials_client() with AK/SK should return a Client configured with static credentials."""
        from maxcompute_catalog_mcp.credentials import get_credentials_client
        from alibabacloud_credentials.client import Client as CredentialClient
        from alibabacloud_credentials.models import Config

        mock_client = MagicMock(spec=CredentialClient)
        with patch("alibabacloud_credentials.client.Client", return_value=mock_client) as mock_cls:
            result = get_credentials_client(
                access_key_id="test-ak",
                access_key_secret="test-sk",
            )
        assert result is mock_client
        # Verify Config(type='access_key', ...) was passed
        call_args = mock_cls.call_args
        assert call_args.kwargs.get("config") is not None
        config = call_args.kwargs["config"]
        assert config.type == "access_key"
        assert config.access_key_id == "test-ak"

    def test_static_credentials_with_security_token(self):
        """get_credentials_client() with STS token should use type='sts' and carry security_token."""
        from maxcompute_catalog_mcp.credentials import get_credentials_client
        from alibabacloud_credentials.models import Config

        mock_client = MagicMock()
        with patch("alibabacloud_credentials.client.Client", return_value=mock_client) as mock_cls:
            result = get_credentials_client(
                access_key_id="test-ak",
                access_key_secret="test-sk",
                security_token="test-token",
            )
        call_args = mock_cls.call_args
        config = call_args.kwargs.get("config")
        # Must use type="sts" so CredentialProviderAccount.get_credential() returns the STS token;
        # type="access_key" creates AccessKeyCredential which silently discards security_token.
        assert config.type == "sts"
        assert config.access_key_id == "test-ak"
        assert config.security_token == "test-token"

    def test_import_error_raises_value_error(self):
        """Should raise ValueError with friendly message when SDK is not installed."""
        import builtins
        original = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "alibabacloud_credentials.client":
                raise ImportError("No module named 'alibabacloud_credentials'")
            return original(name, *args, **kwargs)

        from maxcompute_catalog_mcp.credentials import get_credentials_client
        with patch.object(builtins, "__import__", mock_import):
            with pytest.raises(ValueError, match="alibabacloud-credentials is required"):
                get_credentials_client()


class TestMaxComputeCatalogSdkRefresh:
    """Unit tests for _build_catalog_client and MaxComputeCatalogSdk.

    New design: credential_client is passed directly to Tea OpenAPI Config(credential=...),
    Tea SDK calls get_credential() on each HTTP request, no external detection needed.
    """

    def test_build_uses_credential_field_when_client_provided(self):
        """_build_catalog_client should assign credential_client to Config.credential when provided."""
        from maxcompute_catalog_mcp.maxcompute_client import _build_catalog_client
        mock_cred_client = MagicMock()

        catalog_client = _build_catalog_client("catalogapi.example.com", credential_client=mock_cred_client)
        # pyodps_catalog.Client is itself a Tea SDK client, _credential directly stores credential_client
        assert catalog_client._credential is mock_cred_client

    def test_build_uses_static_ak_when_no_client(self):
        """_build_catalog_client should use static ak/sk when credential_client is not provided."""
        from maxcompute_catalog_mcp.maxcompute_client import _build_catalog_client
        # Should not raise (won't error even without real endpoint)
        catalog_client = _build_catalog_client(
            "catalogapi.example.com",
            access_key_id="test-ak",
            access_key_secret="test-sk",
            security_token="test-token",
        )
        assert catalog_client is not None

    def test_sdk_client_property_returns_directly(self):
        """MaxComputeCatalogSdk.client should return _catalog_client directly with no extra logic."""
        from maxcompute_catalog_mcp.maxcompute_client import MaxComputeCatalogSdk
        mock_cat = MagicMock(name="catalog_client")
        sdk = MaxComputeCatalogSdk(catalog_client=mock_cat)
        assert sdk.client is mock_cat
        assert sdk.client is mock_cat  # Multiple accesses return same result

    def test_create_with_credential_client_passes_to_build(self):
        """create() with credential_client should call _build_catalog_client with credential_client=,
        with scheme stripped to a bare host and protocol derived from the endpoint scheme."""
        from maxcompute_catalog_mcp.maxcompute_client import MaxComputeCatalogSdk
        from maxcompute_catalog_mcp.config import MaxComputeCatalogConfig

        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="https://catalogapi.example.com",
            maxcompute_endpoint="https://service.example.com/api",
            access_key_id="ak",
            access_key_secret="sk",
        )
        mock_cred_client = MagicMock()
        mock_catalog = MagicMock()

        with patch("maxcompute_catalog_mcp.maxcompute_client._build_catalog_client",
                   return_value=mock_catalog) as mock_build:
            sdk = MaxComputeCatalogSdk.create(cfg, credential_client=mock_cred_client)

        assert sdk.client is mock_catalog
        # Verify _build_catalog_client received credential_client, bare host and uppercase protocol
        call_args = mock_build.call_args
        assert call_args.kwargs.get("credential_client") is mock_cred_client
        assert call_args.kwargs.get("protocol") == "HTTPS"
        # Positional arg[0] is the bare host (no scheme)
        assert call_args.args[0] == "catalogapi.example.com"

    def test_create_requires_credential_client(self):
        """MaxComputeCatalogSdk.create() should raise RuntimeError when credential_client=None."""
        from maxcompute_catalog_mcp.maxcompute_client import MaxComputeCatalogSdk
        from maxcompute_catalog_mcp.config import MaxComputeCatalogConfig

        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="https://catalogapi.example.com",
            maxcompute_endpoint="https://service.example.com/api",
            access_key_id="static-ak",
            access_key_secret="static-sk",
        )
        with pytest.raises(RuntimeError, match="credential_client is required"):
            MaxComputeCatalogSdk.create(cfg, credential_client=None)


class TestMaxComputeClientCreate:
    """Unit tests for MaxComputeClient.create() with CredentialProviderAccount."""

    def test_uses_credential_provider_account_when_client_provided(self):
        """Should use CredentialProviderAccount to build ODPS client when credential_client is provided."""
        from maxcompute_catalog_mcp import maxcompute_client as mc_module
        from maxcompute_catalog_mcp.maxcompute_client import MaxComputeClient
        from maxcompute_catalog_mcp.config import MaxComputeCatalogConfig

        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="https://catalogapi.example.com",
            maxcompute_endpoint="https://service.example.maxcompute.aliyun.com/api",
            access_key_id="ak",
            access_key_secret="sk",
            default_project="my_project",
        )
        mock_credential_client = MagicMock()
        mock_account = MagicMock()
        mock_odps = MagicMock()

        # save originals
        orig_cpa = mc_module.CredentialProviderAccount
        orig_odps = mc_module.ODPS
        
        try:
            # patch module-level symbols directly
            mc_module.CredentialProviderAccount = lambda c: mock_account
            mc_module.ODPS = lambda **kwargs: mock_odps
        
            result = MaxComputeClient.create(cfg, credential_client=mock_credential_client)
        
            assert result is not None
        finally:
            # restore originals
            mc_module.CredentialProviderAccount = orig_cpa
            mc_module.ODPS = orig_odps

    def test_fallback_to_static_credentials_without_client(self):
        """Should raise RuntimeError when credential_client=None (no silent fallback)."""
        from maxcompute_catalog_mcp.maxcompute_client import MaxComputeClient
        from maxcompute_catalog_mcp.config import MaxComputeCatalogConfig

        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="https://catalogapi.example.com",
            maxcompute_endpoint="https://service.example.maxcompute.aliyun.com/api",
            access_key_id="ak",
            access_key_secret="sk",
            default_project="my_project",
        )
        with pytest.raises(RuntimeError, match="credential_client is required"):
            MaxComputeClient.create(cfg, credential_client=None)

    def test_raises_runtime_error_when_credential_provider_account_unavailable(self):
        """Should raise RuntimeError when CredentialProviderAccount is unavailable (pyodps < 0.12.0)."""
        from maxcompute_catalog_mcp import maxcompute_client as mc_module
        from maxcompute_catalog_mcp.maxcompute_client import MaxComputeClient
        from maxcompute_catalog_mcp.config import MaxComputeCatalogConfig

        cfg = MaxComputeCatalogConfig(
            catalogapi_endpoint="https://catalogapi.example.com",
            maxcompute_endpoint="https://service.example.maxcompute.aliyun.com/api",
            access_key_id="ak",
            access_key_secret="sk",
            default_project="my_project",
        )
        mock_credential_client = MagicMock()

        # save original
        orig_cpa = mc_module.CredentialProviderAccount
        
        try:
            # set to None to simulate pyodps < 0.12.0
            mc_module.CredentialProviderAccount = None
        
            with pytest.raises(RuntimeError, match="CredentialProviderAccount not available"):
                MaxComputeClient.create(cfg, credential_client=mock_credential_client)
        finally:
            # restore original
            mc_module.CredentialProviderAccount = orig_cpa


# ==================== E2E: Mock Credentials Server Tests ====================

class TestCredentialsAutoRefreshE2E:
    """E2E: Start mock credentials server and verify SDK automatically detects credential changes."""

    @pytest.fixture(autouse=True)
    def _server(self):
        """Start inline mock credentials server."""
        self.server = _MockCredServer(port=0)
        self.server.start()
        yield
        self.server.stop()

    def test_credentials_uri_reachable(self):
        """Mock credentials server should respond to GET /credentials normally."""
        import urllib.request
        resp = urllib.request.urlopen(self.server.url(), timeout=5)
        data = json.loads(resp.read())
        assert data["Code"] == "Success"
        assert data["AccessKeyId"] == "mock-ak-id"
        assert data["SecurityToken"] == "token-v1"

    def test_tea_sdk_holds_credential_client_reference(self):
        """
        Verify Tea OpenAPI SDK holds credential_client reference and uses new token after refresh.

        Flow:
        1. Set ALIBABA_CLOUD_CREDENTIALS_URI to mock server (returns token-v1 with short expiry)
        2. Create CredentialClient -> _build_catalog_client(credential_client=...) builds CatalogClient
        3. Verify Tea SDK's internal _credential is the credential_client we passed
        4. Mock server switches to token-v2, verify same credential_client can return new token

        Important: auth_util.py reads env vars at module level (frozen on import),
        so we must patch module variable environment_credentials_uri, not rely on os.environ dynamically.
        """
        import alibabacloud_credentials.utils.auth_util as auth_util
        uri = self.server.url()
        self.server.set_credentials("token-v1", expire_seconds=1)

        with patch.object(auth_util, "environment_credentials_uri", uri), \
             patch.object(auth_util, "environment_ecs_metadata_disabled", "true"), \
             patch.object(auth_util, "environment_access_key_id", None), \
             patch.object(auth_util, "environment_access_key_secret", None):

            from alibabacloud_credentials.client import Client as CredentialClient
            from maxcompute_catalog_mcp.maxcompute_client import _build_catalog_client

            cred_client = CredentialClient()

            # Build CatalogClient, Tea SDK should hold cred_client reference internally
            catalog_client = _build_catalog_client(
                "catalogapi.example.com", credential_client=cred_client
            )
            # Verify Tea SDK's _credential is the cred_client we passed
            assert catalog_client._credential is cred_client, \
                "Tea SDK should hold credential_client reference"

            # Verify initial token
            cred1 = cred_client.get_credential()
            assert cred1.get_security_token() == "token-v1"

            # Mock server switches token, verify same cred_client can detect new token.
            # With expire_seconds=1, the cached value's stale_time=now, so next
            # get_credential() sees _cache_is_stale()=True → synchronous refresh → token-v2.
            self.server.set_credentials("token-v2", expire_seconds=1)
            time.sleep(0.1)

            cred2 = cred_client.get_credential()
            assert cred2.get_security_token() == "token-v2", \
                "After token switch, credential_client should return new token (Tea SDK uses it on next request)"

    def test_catalog_sdk_create_with_credentials_uri(self):
        """MaxComputeCatalogSdk.create() with credential_client should call _build_catalog_client with credential_client=."""
        import alibabacloud_credentials.utils.auth_util as auth_util
        uri = self.server.url()
        self.server.set_credentials("token-create-test", expire_seconds=3600)

        with patch.object(auth_util, "environment_credentials_uri", uri), \
             patch.object(auth_util, "environment_ecs_metadata_disabled", "true"), \
             patch.object(auth_util, "environment_access_key_id", None), \
             patch.object(auth_util, "environment_access_key_secret", None):

            from alibabacloud_credentials.client import Client as CredentialClient
            from maxcompute_catalog_mcp.maxcompute_client import MaxComputeCatalogSdk
            from maxcompute_catalog_mcp.config import MaxComputeCatalogConfig

            cred_client = CredentialClient()
            cfg = MaxComputeCatalogConfig(
                catalogapi_endpoint="https://catalogapi.example.com",
                maxcompute_endpoint="https://service.example.maxcompute.aliyun.com/api",
                access_key_id="",
                access_key_secret="",
                default_project="test_project",
            )

            mock_catalog_client = MagicMock()
            with patch("maxcompute_catalog_mcp.maxcompute_client._build_catalog_client",
                       return_value=mock_catalog_client) as mock_build:
                sdk = MaxComputeCatalogSdk.create(cfg, credential_client=cred_client)

            assert sdk.client is mock_catalog_client
            # Verify _build_catalog_client received credential_client parameter (not ak/sk/token)
            call_kwargs = mock_build.call_args
            assert call_kwargs.kwargs.get("credential_client") is cred_client, \
                f"Should call with credential_client=, actual: {call_kwargs}"
            assert "access_key_id" not in call_kwargs.kwargs or call_kwargs.kwargs.get("access_key_id") in (None, ""), \
                "credential_client path should not pass static ak"
