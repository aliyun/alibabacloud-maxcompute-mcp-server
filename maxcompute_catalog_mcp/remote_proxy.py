"""Transparent stdio to Streamable HTTP MCP relay."""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterable
from dataclasses import replace
from typing import Any, Protocol

import anyio
import httpx2 as httpx
from mcp import types as mcp_types
from mcp.shared.inbound import (
    MCP_METHOD_HEADER,
    MCP_NAME_HEADER,
    MCP_PROTOCOL_VERSION_HEADER,
    NAME_BEARING_METHODS,
    PROTOCOL_VERSION_META_KEY,
)
from mcp.shared.message import ClientMessageMetadata, SessionMessage

from .runtime_config import RemoteRuntimeConfig

_REMOTE_INITIALIZATION_TIMEOUT_SECONDS = 10.0


class AccessTokenProvider(Protocol):
    """Return a current gateway bearer token."""

    async def get_access_token(self) -> str: ...


class DynamicBearerAuth(httpx.Auth):
    """Resolve a renewable bearer token independently for every HTTP request."""

    def __init__(self, provider: AccessTokenProvider) -> None:
        self._provider = provider

    async def async_auth_flow(
        self,
        request: httpx.Request,
    ) -> AsyncGenerator[httpx.Request, httpx.Response]:
        token = await self._provider.get_access_token()
        request.headers["Authorization"] = f"Bearer {token}"
        yield request


class ProtocolMetadataBridge:
    """Project stdio JSON-RPC metadata onto Streamable HTTP request headers."""

    def __init__(self) -> None:
        self._initialize_request_id: int | str | None = None
        self._legacy_protocol_version: str | None = None

    def prepare_outbound(self, message: SessionMessage) -> SessionMessage:
        """Attach transport metadata while preserving the JSON-RPC object."""

        payload = message.message
        if not isinstance(
            payload,
            mcp_types.JSONRPCRequest | mcp_types.JSONRPCNotification,
        ):
            return message

        if (
            isinstance(payload, mcp_types.JSONRPCRequest)
            and payload.method == "initialize"
        ):
            self._initialize_request_id = payload.id
            self._legacy_protocol_version = None
            return message

        self_describing_version = self._self_describing_version(payload.params)
        protocol_version = self_describing_version or self._legacy_protocol_version
        if protocol_version is None:
            return message

        headers = self._existing_headers(message)
        headers[MCP_PROTOCOL_VERSION_HEADER] = protocol_version
        if self_describing_version is not None:
            headers[MCP_METHOD_HEADER] = payload.method
            name_field = NAME_BEARING_METHODS.get(payload.method)
            if name_field is not None and isinstance(payload.params, dict):
                name = payload.params.get(name_field)
                if isinstance(name, str):
                    headers[MCP_NAME_HEADER] = name
        self._set_headers(message, headers)
        return message

    def observe_inbound(self, message: SessionMessage) -> SessionMessage:
        """Remember the server-selected legacy version from initialize."""

        payload = message.message
        if (
            not isinstance(payload, mcp_types.JSONRPCResponse)
            or payload.id != self._initialize_request_id
        ):
            return message
        protocol_version = payload.result.get("protocolVersion")
        if isinstance(protocol_version, str) and protocol_version:
            self._legacy_protocol_version = protocol_version
        return message

    @staticmethod
    def _self_describing_version(params: object) -> str | None:
        if not isinstance(params, dict):
            return None
        metadata = params.get("_meta")
        if not isinstance(metadata, dict):
            return None
        protocol_version = metadata.get(PROTOCOL_VERSION_META_KEY)
        if not isinstance(protocol_version, str) or not protocol_version:
            return None
        return protocol_version

    @staticmethod
    def _existing_headers(message: SessionMessage) -> dict[str, str]:
        metadata = message.metadata
        if not isinstance(metadata, ClientMessageMetadata):
            return {}
        return dict(metadata.headers or {})

    @staticmethod
    def _set_headers(
        message: SessionMessage,
        headers: dict[str, str],
    ) -> None:
        metadata = message.metadata
        if isinstance(metadata, ClientMessageMetadata):
            message.metadata = replace(metadata, headers=headers)
            return
        message.metadata = ClientMessageMetadata(headers=headers)


async def probe_remote_mcp(
    config: RemoteRuntimeConfig,
    token_provider: AccessTokenProvider,
) -> None:
    """Issue an authenticated MCP initialize before default-mode selection."""

    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    with anyio.fail_after(_REMOTE_INITIALIZATION_TIMEOUT_SECONDS):
        async with (
            httpx.AsyncClient(
                auth=DynamicBearerAuth(token_provider),
                follow_redirects=False,
                trust_env=False,
            ) as mcp_client,
            streamable_http_client(
                config.url,
                http_client=mcp_client,
            ) as (remote_read, remote_write),
            ClientSession(
                remote_read,
                remote_write,
            ) as session,
        ):
            await session.initialize()


async def relay_client_messages(
    source: AsyncIterable[Any],
    target: Any,
    bridge: ProtocolMetadataBridge,
) -> None:
    """Forward stdio client messages with matching HTTP transport metadata."""

    async for message in source:
        if isinstance(message, Exception):
            raise message
        await target.send(bridge.prepare_outbound(message))


async def relay_server_messages(
    source: AsyncIterable[Any],
    target: Any,
    bridge: ProtocolMetadataBridge,
) -> None:
    """Forward remote responses and observe legacy protocol negotiation."""

    async for message in source:
        if isinstance(message, Exception):
            raise message
        await target.send(bridge.observe_inbound(message))


async def relay_bidirectional(
    local_read: Any,
    local_write: Any,
    remote_read: Any,
    remote_write: Any,
) -> None:
    """Relay both MCP directions until either transport closes or fails."""

    bridge = ProtocolMetadataBridge()
    async with anyio.create_task_group() as tasks:

        async def relay_until_closed(relay: Any, source: Any, target: Any) -> None:
            await relay(source, target, bridge)
            tasks.cancel_scope.cancel()

        tasks.start_soon(
            relay_until_closed,
            relay_client_messages,
            local_read,
            remote_write,
        )
        tasks.start_soon(
            relay_until_closed,
            relay_server_messages,
            remote_read,
            local_write,
        )


async def run_remote_proxy(
    config: RemoteRuntimeConfig,
    token_provider: AccessTokenProvider,
) -> None:
    """Run the stdio relay with a CatalogAPI-backed token provider."""

    await _run_remote_proxy_with_provider(config, token_provider)


async def _run_remote_proxy_with_provider(
    config: RemoteRuntimeConfig,
    token_provider: AccessTokenProvider,
) -> None:
    from mcp.client.streamable_http import streamable_http_client
    from mcp.server.stdio import stdio_server

    async with (
        httpx.AsyncClient(
            auth=DynamicBearerAuth(token_provider),
            follow_redirects=False,
            trust_env=False,
        ) as mcp_client,
        stdio_server() as (
            local_read,
            local_write,
        ),
        streamable_http_client(
            config.url,
            http_client=mcp_client,
        ) as (remote_read, remote_write),
    ):
        await relay_bidirectional(
            local_read,
            local_write,
            remote_read,
            remote_write,
        )
