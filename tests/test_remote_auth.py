"""Behavior tests for CatalogAPI MCP token issuance and renewal."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from Tea.exceptions import TeaException

from maxcompute_catalog_mcp.remote_auth import (
    AccessToken,
    CatalogAccessTokenProvider,
    CatalogMCPAccessTokenClient,
    CatalogTokenRequestError,
)
from maxcompute_catalog_mcp.request_ids import request_id_from_exception
from maxcompute_catalog_mcp.runtime_config import load_runtime_config
from maxcompute_catalog_mcp.server import build_remote_token_provider

_READ_SCOPES = (
    "maxcompute:metadata:read",
    "maxcompute:sql:read",
    "maxcompute:ops:read",
    "maxcompute:knowledge:read",
    "maxcompute:semanticspec:read",
)


def _valid_response(
    fixture_id: str = "fixture-token",
    *,
    scopes: tuple[str, ...] = _READ_SCOPES,
    issued_at: int | None = None,
    **claims: object,
) -> dict[str, object]:
    if issued_at is None:
        issued_at = int(time.time())
    payload = {
        "authz_version": 2,
        "tool_policy_version": 1,
        "scope": list(scopes),
        "iat": issued_at,
        "exp": issued_at + 300,
        **claims,
    }
    encoded = (
        base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    )
    return {
        "accessToken": "mcpc_" + f"e30.{encoded}.{fixture_id}",
        "tokenType": "Bearer",
        "expiresIn": 300,
        "scope": list(scopes),
    }


@dataclass
class RecordingCatalogTokenClient:
    """Return queued Catalog responses and record issuance attempts."""

    responses: list[object] = field(default_factory=lambda: [_valid_response()])
    calls: int = 0

    async def issue_access_token(self, *, scopes: tuple[str, ...] = ()) -> object:
        self.calls += 1
        await asyncio.sleep(0.01)
        response = self.responses[min(self.calls - 1, len(self.responses) - 1)]
        if isinstance(response, Exception):
            raise response
        return response


def test_catalog_access_token_renewal_is_single_flight() -> None:
    """Concurrent MCP requests share one signed CatalogAPI issuance."""

    async def scenario() -> None:
        response = _valid_response()
        client = RecordingCatalogTokenClient(responses=[response])
        provider = CatalogAccessTokenProvider(client=client)

        tokens = await asyncio.gather(*(provider.get_access_token() for _ in range(12)))

        assert tokens == [response["accessToken"]] * 12
        assert client.calls == 1

    asyncio.run(scenario())


def test_cached_access_token_renews_inside_expiry_skew() -> None:
    """A cached token is reused only while it exceeds the renewal skew."""
    token = AccessToken(value="mcpc_fixture", expires_at=1000.0)

    assert token.is_usable(now=900.0, expiry_skew=60.0)
    assert not token.is_usable(now=950.0, expiry_skew=60.0)


def test_access_token_repr_redacts_bearer() -> None:
    """Incidental object formatting cannot disclose the cached bearer."""
    token = AccessToken(value="mcpc_fixture-secret-token", expires_at=1000.0)

    rendered = repr(token)

    assert "mcpc_fixture-secret-token" not in rendered
    assert "expires_at=1000.0" in rendered


def test_renewal_honors_jwt_expiry_if_issuance_returns_an_older_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A response with only 60 seconds left must not be cached for 300."""
    wall = 1_700_000_000
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(
        "maxcompute_catalog_mcp.remote_auth.time",
        SimpleNamespace(
            monotonic=lambda: clock.now, time=lambda: wall + clock.now - 100
        ),
    )

    async def scenario() -> None:
        response = _valid_response("short-lived", issued_at=wall - 240)
        client = RecordingCatalogTokenClient(
            responses=[response, RuntimeError("catalog unavailable")]
        )
        provider = CatalogAccessTokenProvider(client=client, expiry_skew=0)
        assert await provider.get_access_token() == response["accessToken"]
        clock.now += 59.0
        assert await provider.get_access_token() == response["accessToken"]
        clock.now += 2.0

        with pytest.raises(RuntimeError, match="CatalogAPI MCP token request failed"):
            await provider.get_access_token()

        assert client.calls == 2

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("response", "message"),
    [
        ({}, "invalid response"),
        (
            {
                **_valid_response(),
                "accessToken": "not-a-catalog-token",
            },
            "invalid access token",
        ),
        (
            {
                **_valid_response(),
                "tokenType": "bearer",
            },
            "wrong tokenType",
        ),
        (
            {
                **_valid_response(),
                "expiresIn": 600,
            },
            "wrong expiresIn",
        ),
        ({**_valid_response(), "scope": ["maxcompute:*"]}, "wrong scope"),
        (_valid_response(authz_version=1), "wrong authorization version"),
        (_valid_response(authz_version="2"), "wrong authorization version"),
        (_valid_response(tool_policy_version=True), "wrong authorization version"),
        (_valid_response(scope=["maxcompute:*"]), "wrong scope"),
        (
            _valid_response(issued_at=1_700_000_000, exp=1_700_000_600),
            "wrong expiresIn",
        ),
        (_valid_response(issued_at=1_700_000_000), "expired access token"),
        (
            {**_valid_response(), "accessToken": "mcpc_invalid-encoding"},
            "invalid access token",
        ),
        (
            {
                **_valid_response(),
                "refreshToken": "must-not-be-accepted",
            },
            "refresh token",
        ),
    ],
)
def test_catalog_access_token_rejects_contract_mismatch(
    response: object,
    message: str,
) -> None:
    """A bearer is unusable if the reply or its claims broaden the request."""

    async def scenario() -> None:
        provider = CatalogAccessTokenProvider(
            client=RecordingCatalogTokenClient(responses=[response]),
        )

        with pytest.raises(RuntimeError, match=message):
            await provider.get_access_token()

    asyncio.run(scenario())


def test_catalog_sdk_adapter_sends_explicit_v2_default_policy() -> None:
    """An unchanged config requests explicit read-only grants through the SDK."""

    async def scenario() -> None:
        catalog_client = AsyncMock()
        expected_response = _valid_response()
        catalog_client.call_api_async.return_value = expected_response
        client = CatalogMCPAccessTokenClient(catalog_client)

        response = await client.issue_access_token()

        assert response == expected_response
        catalog_client.call_api_async.assert_awaited_once()
        params, request, runtime = catalog_client.call_api_async.await_args.args
        assert params.pathname == "/api/catalog/v1alpha/mcpAccessToken"
        assert params.method == "POST"
        assert params.auth_type == "AK"
        assert params.body_type == "json"
        assert request.body == {
            "expiresIn": 300,
            "policy": [
                "maxcompute:metadata:read",
                "maxcompute:sql:read",
                "maxcompute:ops:read",
                "maxcompute:knowledge:read",
                "maxcompute:semanticspec:read",
            ],
        }
        assert request.query is None
        assert params.req_body_type == "json"
        assert runtime.autoretry is False

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "expected_scopes", [(), ("maxcompute:metadata:read", "maxcompute:sql:write")]
)
def test_selected_profile_policy_reaches_catalog_on_issue_and_renewal(
    expected_scopes: tuple[str, ...], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the real SDK with synthetic credentials against an isolated issuer."""
    for name in ("HTTP_PROXY", "http_proxy"):
        monkeypatch.delenv(name, raising=False)
    observed: list[tuple[str, str | None, bool, object]] = []
    response = _valid_response(scopes=expected_scopes)

    class CatalogFixture(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers["Content-Length"]))
            observed.append(
                (
                    self.path,
                    self.headers.get_content_type(),
                    self.headers.get("Authorization", "").startswith(
                        "ODPS fixture-ak:"
                    ),
                    json.loads(body),
                )
            )
            payload = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args: object) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), CatalogFixture) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connect = {
                "region": "cn-hangzhou",
                "network": "public",
                "catalogapi_endpoint": f"http://127.0.0.1:{server.server_port}",
                "accessKeyId": "fixture-ak",
                "accessKeySecret": "synthetic-test-value",
            }
            path = tmp_path / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "default": "unscoped",
                        "configs": {
                            "unscoped": connect,
                            "selected": {
                                **connect,
                                "mcp": {"scopes": list(expected_scopes)},
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            runtime = load_runtime_config(str(path), profile="selected")
            provider = build_remote_token_provider(runtime.catalog_config)
            clock = SimpleNamespace(now=100.0)
            monkeypatch.setattr(
                "maxcompute_catalog_mcp.remote_auth.time",
                SimpleNamespace(monotonic=lambda: clock.now, time=time.time),
            )

            async def scenario() -> None:
                assert await provider.get_access_token() == response["accessToken"]
                assert await provider.get_access_token() == response["accessToken"]
                clock.now += 250.0  # Enter the renewal skew before expiry.
                path.write_text("{}", encoding="utf-8")  # No hot reload.
                assert await provider.get_access_token() == response["accessToken"]

            asyncio.run(scenario())
        finally:
            server.shutdown()
            thread.join(timeout=5)

    assert (
        observed
        == [
            (
                "/api/catalog/v1alpha/mcpAccessToken",
                "application/json",
                True,
                {"expiresIn": 300, "policy": list(expected_scopes)},
            )
        ]
        * 2
    )


def test_catalog_sdk_adapter_sanitizes_provider_and_http_errors() -> None:
    """Credential, signature, and provider details do not enter public errors."""

    async def scenario() -> None:
        catalog_client = AsyncMock()
        catalog_client.call_api_async.side_effect = RuntimeError(
            "fixture-secret-ak-and-token"
        )
        provider = CatalogAccessTokenProvider(
            client=CatalogMCPAccessTokenClient(catalog_client),
        )

        with pytest.raises(RuntimeError) as exc_info:
            await provider.get_access_token()

        assert str(exc_info.value) == "CatalogAPI MCP token request failed"
        assert exc_info.value.__cause__ is None
        assert "fixture-secret" not in str(exc_info.value)

    asyncio.run(scenario())


def test_catalog_sdk_adapter_preserves_only_safe_provider_request_id() -> None:
    """Catalog failures retain correlation metadata without leaking SDK details."""

    async def scenario() -> None:
        catalog_client = AsyncMock()
        catalog_client.call_api_async.side_effect = TeaException(
            {
                "code": "Forbidden",
                "message": "fixture-secret-ak-and-token",
                "data": {"RequestId": "catalog-request-403"},
            }
        )
        provider = CatalogAccessTokenProvider(
            client=CatalogMCPAccessTokenClient(catalog_client),
        )

        with pytest.raises(CatalogTokenRequestError) as exc_info:
            await provider.get_access_token()

        assert exc_info.value.request_id == "catalog-request-403"
        assert str(exc_info.value) == (
            "CatalogAPI MCP token request failed (request_id=catalog-request-403)"
        )
        assert "fixture-secret" not in str(exc_info.value)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ({"requestId": "catalog-lower-camel"}, "catalog-lower-camel"),
        ({"RequestId": "catalog-upper-camel"}, "catalog-upper-camel"),
        (
            {"request_id": "bad\nvalue", "requestId": "catalog-fallback"},
            "catalog-fallback",
        ),
        ({"message": "no correlation metadata"}, None),
    ],
)
def test_provider_request_id_extraction_supports_explicit_sdk_data_shapes(
    data: dict[str, object],
    expected: str | None,
) -> None:
    """Known Alibaba Cloud SDK data keys are accepted without parsing messages."""

    error = RuntimeError("provider detail must stay private")
    error.data = data

    assert request_id_from_exception(error) == expected


def test_provider_request_id_extraction_tolerates_hostile_exception_properties() -> (
    None
):
    """A broken external exception object cannot mask the sanitized failure."""

    class HostileProviderError(RuntimeError):
        @property
        def request_id(self) -> str:
            raise RuntimeError("property access failed")

    assert request_id_from_exception(HostileProviderError()) is None
