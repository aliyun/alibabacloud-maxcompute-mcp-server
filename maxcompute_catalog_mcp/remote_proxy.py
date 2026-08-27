"""Transparent stdio to Streamable HTTP MCP relay."""

from __future__ import annotations

import json
import logging
import time
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

from .request_ids import sanitize_request_id
from .runtime_config import RemoteRuntimeConfig

_REMOTE_INITIALIZATION_TIMEOUT_SECONDS = 10.0
_REMOTE_CONNECT_TIMEOUT_SECONDS = 30.0
_RELAY_FIRST_RESPONSE_TIMEOUT_SECONDS = 30.0
_RELAY_WATCHDOG_POLL_SECONDS = 1.0
_REMOTE_REQUEST_ERROR_CODE = -32000
_REQUEST_ID_HEADER = "X-Request-ID"
_REQUEST_ID_META_KEY = "com.aliyun.maxcompute/requestId"
_LOGGER = logging.getLogger(__name__)


class AccessTokenProvider(Protocol):
    """Return a current gateway bearer token."""

    async def get_access_token(self) -> str: ...


class RemoteMCPInitializationFailure(RuntimeError):
    """Remote initialize failure with safe gateway correlation metadata."""

    def __init__(self, request_id: str) -> None:
        self.request_id = sanitize_request_id(request_id)
        message = "Remote MCP initialization failed"
        if self.request_id is not None:
            message = f"{message} (request_id={self.request_id})"
        super().__init__(message)


class RelayFirstExchangeStalled(RuntimeError):
    """The first forwarded request never received any remote response."""


class RemoteRequestIdTracker:
    """Correlate MCP POST responses with their gateway request IDs."""

    def __init__(self) -> None:
        self._pending: dict[int | str, str] = {}
        self.latest_request_id: str | None = None

    async def observe_response(self, response: httpx.Response) -> None:
        """Record and safely log one gateway HTTP response."""

        request_id = sanitize_request_id(response.headers.get(_REQUEST_ID_HEADER))
        if response.status_code >= 400:
            if request_id is None:
                _LOGGER.warning(
                    "Remote MCP HTTP response status=%d",
                    response.status_code,
                )
            else:
                _LOGGER.warning(
                    "Remote MCP HTTP response status=%d request_id=%s",
                    response.status_code,
                    request_id,
                )
        elif request_id is not None:
            _LOGGER.debug(
                "Remote MCP HTTP response status=%d request_id=%s",
                response.status_code,
                request_id,
            )

        if request_id is None:
            return
        self.latest_request_id = request_id
        jsonrpc_id = self._request_jsonrpc_id(response.request)
        if jsonrpc_id is not None:
            self._pending[jsonrpc_id] = request_id

    def pop(self, jsonrpc_id: object) -> str | None:
        """Consume the response Request ID for one exact JSON-RPC ID."""

        if isinstance(jsonrpc_id, bool) or not isinstance(jsonrpc_id, int | str):
            return None
        return self._pending.pop(jsonrpc_id, None)

    @staticmethod
    def _request_jsonrpc_id(request: httpx.Request) -> int | str | None:
        if request.method != "POST":
            return None
        try:
            payload = json.loads(request.content)
        except (json.JSONDecodeError, UnicodeDecodeError, httpx.RequestNotRead):
            return None
        if not isinstance(payload, dict):
            return None
        jsonrpc_id = payload.get("id")
        if isinstance(jsonrpc_id, bool) or not isinstance(jsonrpc_id, int | str):
            return None
        return jsonrpc_id


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

    def __init__(self, request_ids: RemoteRequestIdTracker | None = None) -> None:
        self._initialize_request_id: int | str | None = None
        self._legacy_protocol_version: str | None = None
        self._request_ids = request_ids
        self._pending_requests: dict[int | str, float] = {}
        self.first_response_observed = False

    def stalled_request_id(self, timeout: float) -> int | str | None:
        """Return the oldest unanswered request ID once it exceeds the timeout.

        Only the first exchange is watched: once any remote response has been
        observed the session is proven live and later long-running tool calls
        are not bounded here.
        """

        if self.first_response_observed:
            return None
        now = time.monotonic()
        for request_id, forwarded_at in self._pending_requests.items():
            if now - forwarded_at >= timeout:
                return request_id
        return None

    def prepare_outbound(self, message: SessionMessage) -> SessionMessage:
        """Attach transport metadata while preserving the JSON-RPC object."""

        payload = message.message
        if not isinstance(
            payload,
            mcp_types.JSONRPCRequest | mcp_types.JSONRPCNotification,
        ):
            return message

        if isinstance(payload, mcp_types.JSONRPCRequest):
            self._pending_requests.setdefault(payload.id, time.monotonic())

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
        """Remember protocol state and add HTTP correlation to failures."""

        payload = message.message
        if isinstance(payload, mcp_types.JSONRPCResponse | mcp_types.JSONRPCError):
            self.first_response_observed = True
            if payload.id is not None:
                self._pending_requests.pop(payload.id, None)

        if (
            isinstance(payload, mcp_types.JSONRPCResponse)
            and payload.id == self._initialize_request_id
        ):
            protocol_version = payload.result.get("protocolVersion")
            if isinstance(protocol_version, str) and protocol_version:
                self._legacy_protocol_version = protocol_version

        if not isinstance(payload, mcp_types.JSONRPCResponse | mcp_types.JSONRPCError):
            return message
        request_id = (
            self._request_ids.pop(payload.id) if self._request_ids is not None else None
        )
        if request_id is None:
            return message
        if isinstance(payload, mcp_types.JSONRPCError):
            self._add_jsonrpc_error_request_id(payload.error, request_id)
        elif payload.result.get("isError") is True:
            self._add_tool_error_request_id(payload.result, request_id)
        return message

    @staticmethod
    def _add_jsonrpc_error_request_id(
        error: mcp_types.ErrorData,
        request_id: str,
    ) -> None:
        data = error.data
        if isinstance(data, dict):
            if "request_id" not in data:
                error.data = {**data, "request_id": request_id}
            return
        if data is None:
            error.data = {"request_id": request_id}
            return
        error.data = {"details": data, "request_id": request_id}

    @staticmethod
    def _add_tool_error_request_id(
        result: dict[str, Any],
        request_id: str,
    ) -> None:
        structured_content = result.get("structuredContent")
        if (
            isinstance(structured_content, dict)
            and sanitize_request_id(structured_content.get("request_id")) is not None
        ):
            return
        metadata = result.get("_meta")
        if isinstance(metadata, dict):
            if _REQUEST_ID_META_KEY not in metadata:
                metadata[_REQUEST_ID_META_KEY] = request_id
            return
        result["_meta"] = {_REQUEST_ID_META_KEY: request_id}

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

    request_ids = RemoteRequestIdTracker()
    try:
        with anyio.fail_after(_REMOTE_INITIALIZATION_TIMEOUT_SECONDS):
            async with (
                httpx.AsyncClient(
                    auth=DynamicBearerAuth(token_provider),
                    event_hooks={"response": [request_ids.observe_response]},
                    follow_redirects=False,
                    timeout=httpx.Timeout(_REMOTE_INITIALIZATION_TIMEOUT_SECONDS),
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
    except Exception:
        if request_ids.latest_request_id is not None:
            raise RemoteMCPInitializationFailure(
                request_ids.latest_request_id
            ) from None
        raise


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


async def _watch_first_exchange(
    local_write: Any,
    bridge: ProtocolMetadataBridge,
) -> None:
    """Fail fast when the first forwarded request is silently dropped.

    Some transport races cancel the initial POST inside the Streamable HTTP
    client without ever notifying the stdio peer, leaving the MCP client
    hanging on an unanswered initialize. When no response arrives within the
    budget, surface a JSON-RPC error to the peer and abort the relay instead
    of stalling silently.
    """

    while not bridge.first_response_observed:
        await anyio.sleep(_RELAY_WATCHDOG_POLL_SECONDS)
        stalled_id = bridge.stalled_request_id(_RELAY_FIRST_RESPONSE_TIMEOUT_SECONDS)
        if stalled_id is None:
            continue
        _LOGGER.error(
            "Remote MCP relay stall: request id=%r received no response within %.0fs",
            stalled_id,
            _RELAY_FIRST_RESPONSE_TIMEOUT_SECONDS,
        )
        error = mcp_types.JSONRPCError(
            jsonrpc="2.0",
            id=stalled_id,
            error=mcp_types.ErrorData(
                code=_REMOTE_REQUEST_ERROR_CODE,
                message="remote relay delivered no response for this request",
            ),
        )
        await local_write.send(SessionMessage(error))
        raise RelayFirstExchangeStalled(
            f"request id={stalled_id!r} received no remote response within "
            f"{_RELAY_FIRST_RESPONSE_TIMEOUT_SECONDS:.0f}s"
        )


async def relay_bidirectional(
    local_read: Any,
    local_write: Any,
    remote_read: Any,
    remote_write: Any,
    request_ids: RemoteRequestIdTracker | None = None,
) -> None:
    """Relay both MCP directions until either transport closes or fails."""

    bridge = ProtocolMetadataBridge(request_ids)
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
        tasks.start_soon(_watch_first_exchange, local_write, bridge)


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

    request_ids = RemoteRequestIdTracker()
    async with (
        httpx.AsyncClient(
            auth=DynamicBearerAuth(token_provider),
            event_hooks={"response": [request_ids.observe_response]},
            follow_redirects=False,
            # Long-running MCP tool calls (SQL synchronous waits, KB/LLM
            # answers) exceed any small default; only the connect phase is
            # bounded here, matching the Streamable HTTP reverse proxy.
            timeout=httpx.Timeout(_REMOTE_CONNECT_TIMEOUT_SECONDS, read=None),
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
            request_ids,
        )
