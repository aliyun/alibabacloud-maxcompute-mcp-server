"""Behavior tests for CatalogAPI MCP token issuance and renewal."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from unittest.mock import AsyncMock

import pytest
from alibabacloud_tea_openapi.exceptions import ClientException

from maxcompute_catalog_mcp.remote_auth import (
    AccessToken,
    CatalogAccessTokenProvider,
    CatalogMCPAccessTokenClient,
    CatalogTokenRequestError,
)
from maxcompute_catalog_mcp.request_ids import request_id_from_exception


def _valid_response(token: str = "mcpc_fixture-token") -> dict[str, object]:
    return {
        "accessToken": token,
        "tokenType": "Bearer",
        "expiresIn": 300,
        "scope": ["maxcompute:read", "maxcompute:sql"],
    }


@dataclass
class RecordingCatalogTokenClient:
    """Return queued Catalog responses and record issuance attempts."""

    responses: list[object] = field(default_factory=lambda: [_valid_response()])
    calls: int = 0

    async def issue_access_token(self) -> object:
        self.calls += 1
        await asyncio.sleep(0.01)
        response = self.responses[min(self.calls - 1, len(self.responses) - 1)]
        if isinstance(response, Exception):
            raise response
        return response


def test_catalog_access_token_renewal_is_single_flight() -> None:
    """Concurrent MCP requests share one signed CatalogAPI issuance."""

    async def scenario() -> None:
        client = RecordingCatalogTokenClient()
        provider = CatalogAccessTokenProvider(client=client)

        tokens = await asyncio.gather(*(provider.get_access_token() for _ in range(12)))

        assert tokens == ["mcpc_fixture-token"] * 12
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


def test_expired_access_token_is_not_reused_after_renewal_failure() -> None:
    """Renewal failure surfaces instead of falling back to a stale bearer."""

    async def scenario() -> None:
        client = RecordingCatalogTokenClient(
            responses=[
                _valid_response("mcpc_short-lived"),
                RuntimeError("catalog unavailable"),
            ]
        )
        provider = CatalogAccessTokenProvider(
            client=client,
            expiry_skew=0,
        )
        assert await provider.get_access_token() == "mcpc_short-lived"
        provider._token = AccessToken(
            value="mcpc_short-lived",
            expires_at=0,
        )

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
        (
            {
                **_valid_response(),
                "scope": ["maxcompute:sql", "maxcompute:read"],
            },
            "wrong scope",
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
    """Only the merged bodyless CatalogAPI v1 response is accepted."""

    async def scenario() -> None:
        provider = CatalogAccessTokenProvider(
            client=RecordingCatalogTokenClient(responses=[response]),
        )

        with pytest.raises(RuntimeError, match=message):
            await provider.get_access_token()

    asyncio.run(scenario())


def test_catalog_sdk_adapter_sends_bodyless_signed_operation() -> None:
    """The adapter reuses the generated SDK signing path without a body."""

    async def scenario() -> None:
        catalog_client = AsyncMock()
        catalog_client.call_api_async.return_value = _valid_response()
        client = CatalogMCPAccessTokenClient(catalog_client)

        response = await client.issue_access_token()

        assert response == _valid_response()
        catalog_client.call_api_async.assert_awaited_once()
        params, request, runtime = catalog_client.call_api_async.await_args.args
        assert params.pathname == "/api/catalog/v1alpha/mcpAccessToken"
        assert params.method == "POST"
        assert params.auth_type == "AK"
        assert params.body_type == "json"
        assert request.body is None
        assert request.query is None
        assert request.headers == {
            "content-type": "application/octet-stream",
        }
        assert runtime.autoretry is False

    asyncio.run(scenario())


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
        catalog_client.call_api_async.side_effect = ClientException(
            status_code=403,
            code="Forbidden",
            message="fixture-secret-ak-and-token",
            request_id="catalog-request-403",
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
