"""Short-lived CatalogAPI access tokens for the remote MCP runtime."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

from alibabacloud_tea_util import models as util_models
from maxcompute_tea_openapi import models as openapi_models

_MCP_ACCESS_TOKEN_PATH = "/api/catalog/v1alpha/mcpAccessToken"
_EXPECTED_SCOPES = ["maxcompute:read", "maxcompute:sql"]
_TOKEN_LIFETIME_SECONDS = 300
_DEFAULT_EXPIRY_SKEW_SECONDS = 60.0
_REQUEST_TIMEOUT_MILLISECONDS = 5000
_MAX_ACCESS_TOKEN_CHARS = 16 * 1024
_EMPTY_BODY_CONTENT_TYPE = "application/octet-stream"


class CatalogTokenClient(Protocol):
    """Issue one token through the authenticated CatalogAPI path."""

    async def issue_access_token(self) -> object: ...


@dataclass(frozen=True)
class AccessToken:
    """Cached Catalog bearer with a monotonic expiry deadline."""

    value: str = field(repr=False)
    expires_at: float

    def is_usable(self, *, now: float, expiry_skew: float) -> bool:
        """Return whether the token remains usable beyond renewal skew."""

        return bool(self.value) and now + expiry_skew < self.expires_at


class CatalogMCPAccessTokenClient:
    """Narrow bodyless operation over the generated Catalog SDK client."""

    def __init__(self, catalog_client: Any) -> None:
        self._catalog_client = catalog_client

    async def issue_access_token(self) -> object:
        """Use the SDK's normal dynamic-credential signing path."""

        params = openapi_models.Params(
            action="McpAccessToken",
            version="v1alpha",
            pathname=_MCP_ACCESS_TOKEN_PATH,
            method="POST",
            auth_type="AK",
            body_type="json",
        )
        # Tea sends an empty stream as application/octet-stream even when the
        # generated request model has no body. CatalogAPI signs the received
        # Content-Type, so declare the transport default before the SDK builds
        # its canonical string or the server will reject the signature.
        request = openapi_models.OpenApiRequest(
            headers={"content-type": _EMPTY_BODY_CONTENT_TYPE},
        )
        runtime = util_models.RuntimeOptions(
            autoretry=False,
            read_timeout=_REQUEST_TIMEOUT_MILLISECONDS,
            connect_timeout=_REQUEST_TIMEOUT_MILLISECONDS,
        )
        try:
            return await self._catalog_client.call_api_async(
                params,
                request,
                runtime,
            )
        except Exception:  # noqa: BLE001 -- sanitize the external SDK boundary.
            raise RuntimeError("CatalogAPI MCP token request failed") from None


class CatalogAccessTokenProvider:
    """Cache and renew CatalogAPI MCP access tokens single-flight."""

    def __init__(
        self,
        *,
        client: CatalogTokenClient,
        expiry_skew: float = _DEFAULT_EXPIRY_SKEW_SECONDS,
    ) -> None:
        if expiry_skew < 0 or expiry_skew >= _TOKEN_LIFETIME_SECONDS:
            raise ValueError("expiry skew must be within the token lifetime")
        self._client = client
        self._expiry_skew = expiry_skew
        self._token: AccessToken | None = None
        self._renew_lock = asyncio.Lock()

    async def get_access_token(self) -> str:
        """Return a usable bearer, renewing once for concurrent callers."""

        token = self._token
        now = time.monotonic()
        if token is not None and token.is_usable(
            now=now,
            expiry_skew=self._expiry_skew,
        ):
            return token.value

        async with self._renew_lock:
            token = self._token
            now = time.monotonic()
            if token is not None and token.is_usable(
                now=now,
                expiry_skew=self._expiry_skew,
            ):
                return token.value
            renewed = await self._renew()
            self._token = renewed
            return renewed.value

    async def _renew(self) -> AccessToken:
        try:
            payload = await self._client.issue_access_token()
        except Exception:  # noqa: BLE001 -- CatalogTokenClient is a provider boundary.
            raise RuntimeError("CatalogAPI MCP token request failed") from None
        return self._parse_access_token(payload)

    @staticmethod
    def _parse_access_token(payload: object) -> AccessToken:
        if not isinstance(payload, dict):
            raise TypeError("CatalogAPI MCP token returned an invalid response")
        required_fields = {"accessToken", "tokenType", "expiresIn", "scope"}
        if not required_fields.issubset(payload):
            raise RuntimeError("CatalogAPI MCP token returned an invalid response")
        if "refreshToken" in payload or "refresh_token" in payload:
            raise RuntimeError("CatalogAPI MCP token returned a refresh token")

        value = payload.get("accessToken")
        if (
            not isinstance(value, str)
            or not value.startswith("mcpc_")
            or len(value) <= len("mcpc_")
            or len(value) > _MAX_ACCESS_TOKEN_CHARS
        ):
            raise RuntimeError("CatalogAPI MCP token returned an invalid access token")
        if payload.get("tokenType") != "Bearer":
            raise RuntimeError("CatalogAPI MCP token returned wrong tokenType")

        expires_in = payload.get("expiresIn")
        if (
            isinstance(expires_in, bool)
            or not isinstance(expires_in, int)
            or expires_in != _TOKEN_LIFETIME_SECONDS
        ):
            raise RuntimeError("CatalogAPI MCP token returned wrong expiresIn")
        if payload.get("scope") != _EXPECTED_SCOPES:
            raise RuntimeError("CatalogAPI MCP token returned wrong scope")

        return AccessToken(
            value=value,
            expires_at=time.monotonic() + expires_in,
        )
