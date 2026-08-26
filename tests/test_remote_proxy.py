"""Protocol-preservation tests for the transparent remote relay."""

from __future__ import annotations

import asyncio
import json
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import httpx2 as httpx
import pytest
from mcp import types as mcp_types
from mcp.shared.message import ClientMessageMetadata, SessionMessage

from maxcompute_catalog_mcp.remote_proxy import (
    DynamicBearerAuth,
    ProtocolMetadataBridge,
    _run_remote_proxy_with_provider,
    probe_remote_mcp,
    relay_client_messages,
    relay_server_messages,
)
from maxcompute_catalog_mcp.runtime_config import RemoteRuntimeConfig

MODERN_PROTOCOL_VERSION = "2026-07-28"
LEGACY_PROTOCOL_VERSION = "2025-06-18"
PROTOCOL_VERSION_META_KEY = "io.modelcontextprotocol/protocolVersion"


@dataclass
class RotatingTokenProvider:
    calls: int = 0

    async def get_access_token(self) -> str:
        self.calls += 1
        return f"gateway-token-{self.calls}"


def test_dynamic_bearer_auth_resolves_token_for_each_http_request() -> None:
    """The HTTP transport never snapshots a renewable gateway token."""

    async def scenario() -> None:
        provider = RotatingTokenProvider()
        seen: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.headers["Authorization"])
            return httpx.Response(200)

        async with httpx.AsyncClient(
            auth=DynamicBearerAuth(provider),
            transport=httpx.MockTransport(handler),
        ) as client:
            await client.get("https://gateway.example.com/mcp")
            await client.get("https://gateway.example.com/mcp")

        assert seen == ["Bearer gateway-token-1", "Bearer gateway-token-2"]
        assert provider.calls == 2

    asyncio.run(scenario())


def test_remote_proxy_uses_exact_hardened_http_client_and_relay_wiring() -> None:
    """The transparent proxy pins its target and disables redirects/proxy env."""

    async def scenario() -> None:
        provider = RotatingTokenProvider()
        config = RemoteRuntimeConfig(url="https://gateway.example.com/mcp")
        http_client = object()
        local_read = object()
        local_write = object()
        remote_read = object()
        remote_write = object()

        class HttpClientContext:
            async def __aenter__(self):
                return http_client

            async def __aexit__(self, *_args):
                return None

        async_client_factory = MagicMock(return_value=HttpClientContext())

        @asynccontextmanager
        async def fake_stdio_server():
            yield local_read, local_write

        @asynccontextmanager
        async def fake_streamable_http_client(url, *, http_client):
            assert url == config.url
            assert http_client is scenario_http_client
            yield remote_read, remote_write

        scenario_http_client = http_client
        relay_mock = AsyncMock()
        fake_modules = {
            "mcp.client.streamable_http": MagicMock(
                streamable_http_client=fake_streamable_http_client,
            ),
            "mcp.server.stdio": MagicMock(stdio_server=fake_stdio_server),
        }
        with (
            patch(
                "maxcompute_catalog_mcp.remote_proxy.httpx.AsyncClient",
                async_client_factory,
            ),
            patch(
                "maxcompute_catalog_mcp.remote_proxy.relay_bidirectional",
                relay_mock,
            ),
            patch.dict(sys.modules, fake_modules),
        ):
            await _run_remote_proxy_with_provider(config, provider)

        async_client_factory.assert_called_once()
        kwargs = async_client_factory.call_args.kwargs
        assert isinstance(kwargs["auth"], DynamicBearerAuth)
        assert kwargs["auth"]._provider is provider
        assert kwargs["follow_redirects"] is False
        assert kwargs["trust_env"] is False
        relay_mock.assert_awaited_once_with(
            local_read,
            local_write,
            remote_read,
            remote_write,
        )

    asyncio.run(scenario())


def test_remote_probe_requires_authenticated_mcp_initialize() -> None:
    """A successful TCP/HTTP connection alone is not enough for default mode."""

    async def scenario() -> None:
        provider = RotatingTokenProvider()
        requests: list[tuple[str, str]] = []
        real_async_client = httpx.AsyncClient

        async def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            requests.append(
                (payload["method"], request.headers["Authorization"]),
            )
            if payload["method"] == "initialize":
                return httpx.Response(
                    200,
                    headers={"content-type": "application/json"},
                    json={
                        "jsonrpc": "2.0",
                        "id": payload["id"],
                        "result": {
                            "protocolVersion": LEGACY_PROTOCOL_VERSION,
                            "capabilities": {},
                            "serverInfo": {
                                "name": "gateway-test",
                                "version": "1.0.0",
                            },
                        },
                    },
                )
            assert payload["method"] == "notifications/initialized"
            return httpx.Response(202)

        def client_factory(**kwargs):
            return real_async_client(
                transport=httpx.MockTransport(handler),
                **kwargs,
            )

        with patch(
            "maxcompute_catalog_mcp.remote_proxy.httpx.AsyncClient",
            side_effect=client_factory,
        ):
            await probe_remote_mcp(
                RemoteRuntimeConfig(url="https://gateway.example.com/mcp"),
                provider,
            )

        assert requests == [
            ("initialize", "Bearer gateway-token-1"),
            ("notifications/initialized", "Bearer gateway-token-2"),
        ]

    asyncio.run(scenario())


def test_modern_stdio_request_gains_matching_http_routing_headers() -> None:
    """Modern body metadata drives headers without changing the JSON-RPC body."""

    bridge = ProtocolMetadataBridge()
    request = mcp_types.JSONRPCRequest(
        jsonrpc="2.0",
        id=1,
        method="tools/call",
        params={
            "name": "maxcompute_health_ping",
            "arguments": {},
            "_meta": {PROTOCOL_VERSION_META_KEY: MODERN_PROTOCOL_VERSION},
        },
    )
    message = SessionMessage(request)

    prepared = bridge.prepare_outbound(message)

    assert prepared is message
    assert prepared.message is request
    assert prepared.metadata is not None
    assert prepared.metadata.headers == {
        "mcp-protocol-version": MODERN_PROTOCOL_VERSION,
        "mcp-method": "tools/call",
        "mcp-name": "maxcompute_health_ping",
    }
    assert request.params["_meta"][PROTOCOL_VERSION_META_KEY] == MODERN_PROTOCOL_VERSION


def test_bridge_preserves_non_requests_and_unversioned_notifications() -> None:
    """Messages without request routing metadata remain unchanged."""

    bridge = ProtocolMetadataBridge()
    response = SessionMessage(mcp_types.JSONRPCResponse(jsonrpc="2.0", id=1, result={}))
    notification = SessionMessage(
        mcp_types.JSONRPCNotification(
            jsonrpc="2.0",
            method="notifications/initialized",
        )
    )
    invalid_version = SessionMessage(
        mcp_types.JSONRPCNotification(
            jsonrpc="2.0",
            method="notifications/progress",
            params={"_meta": {PROTOCOL_VERSION_META_KEY: 42}},
        )
    )

    assert bridge.prepare_outbound(response) is response
    assert bridge.prepare_outbound(notification) is notification
    assert bridge.prepare_outbound(invalid_version) is invalid_version
    assert response.metadata is None
    assert notification.metadata is None
    assert invalid_version.metadata is None


def test_modern_bridge_preserves_existing_headers_and_omits_invalid_name() -> None:
    """Transport headers survive bridging and invalid tool names are not projected."""

    bridge = ProtocolMetadataBridge()
    request = mcp_types.JSONRPCRequest(
        jsonrpc="2.0",
        id=1,
        method="tools/call",
        params={
            "name": 42,
            "arguments": {},
            "_meta": {PROTOCOL_VERSION_META_KEY: MODERN_PROTOCOL_VERSION},
        },
    )
    message = SessionMessage(
        request,
        metadata=ClientMessageMetadata(headers={"x-client-trace": "trace-1"}),
    )

    prepared = bridge.prepare_outbound(message)

    assert prepared.metadata is not None
    assert prepared.metadata.headers == {
        "x-client-trace": "trace-1",
        "mcp-protocol-version": MODERN_PROTOCOL_VERSION,
        "mcp-method": "tools/call",
    }


def test_legacy_bridge_ignores_unrelated_and_invalid_initialize_results() -> None:
    """Only a valid matching initialize response can set the legacy version."""

    bridge = ProtocolMetadataBridge()
    initialize = SessionMessage(
        mcp_types.JSONRPCRequest(
            jsonrpc="2.0",
            id="init-1",
            method="initialize",
            params={
                "protocolVersion": LEGACY_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "legacy-client", "version": "1.0.0"},
            },
        )
    )
    bridge.prepare_outbound(initialize)

    unrelated = SessionMessage(
        mcp_types.JSONRPCResponse(
            jsonrpc="2.0",
            id="other-request",
            result={"protocolVersion": LEGACY_PROTOCOL_VERSION},
        )
    )
    invalid = SessionMessage(
        mcp_types.JSONRPCResponse(
            jsonrpc="2.0",
            id="init-1",
            result={"protocolVersion": ""},
        )
    )

    assert bridge.observe_inbound(unrelated) is unrelated
    assert bridge.observe_inbound(invalid) is invalid

    listed = bridge.prepare_outbound(
        SessionMessage(
            mcp_types.JSONRPCRequest(
                jsonrpc="2.0",
                id=2,
                method="tools/list",
                params={},
            )
        )
    )
    assert listed.metadata is None


def test_legacy_stdio_requests_reuse_negotiated_protocol_version() -> None:
    """Legacy initialize remains headerless; later requests carry its result version."""

    bridge = ProtocolMetadataBridge()
    initialize = SessionMessage(
        mcp_types.JSONRPCRequest(
            jsonrpc="2.0",
            id="init-1",
            method="initialize",
            params={
                "protocolVersion": LEGACY_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "legacy-client", "version": "1.0.0"},
            },
        )
    )

    assert bridge.prepare_outbound(initialize).metadata is None
    bridge.observe_inbound(
        SessionMessage(
            mcp_types.JSONRPCResponse(
                jsonrpc="2.0",
                id="init-1",
                result={"protocolVersion": LEGACY_PROTOCOL_VERSION},
            )
        )
    )
    listed = bridge.prepare_outbound(
        SessionMessage(
            mcp_types.JSONRPCRequest(
                jsonrpc="2.0",
                id=2,
                method="tools/list",
                params={},
            )
        )
    )

    assert listed.metadata is not None
    assert listed.metadata.headers == {
        "mcp-protocol-version": LEGACY_PROTOCOL_VERSION,
    }


def test_relays_forward_messages_and_propagate_transport_errors() -> None:
    """Both relay directions forward normal traffic and surface transport failures."""

    async def scenario() -> None:
        bridge = ProtocolMetadataBridge()
        request = SessionMessage(
            mcp_types.JSONRPCRequest(
                jsonrpc="2.0",
                id=1,
                method="tools/list",
                params={},
            )
        )
        response = SessionMessage(
            mcp_types.JSONRPCResponse(jsonrpc="2.0", id=1, result={})
        )

        async def source(*items):
            for item in items:
                yield item

        client_target = MagicMock(send=AsyncMock())
        await relay_client_messages(source(request), client_target, bridge)
        client_target.send.assert_awaited_once_with(request)

        server_target = MagicMock(send=AsyncMock())
        await relay_server_messages(source(response), server_target, bridge)
        server_target.send.assert_awaited_once_with(response)

        client_failure = RuntimeError("client transport failed")
        with pytest.raises(RuntimeError, match="client transport failed"):
            await relay_client_messages(source(client_failure), client_target, bridge)

        server_failure = RuntimeError("server transport failed")
        with pytest.raises(RuntimeError, match="server transport failed"):
            await relay_server_messages(source(server_failure), server_target, bridge)

    asyncio.run(scenario())


def test_streamable_http_400_jsonrpc_error_returns_without_timeout() -> None:
    """MCP 2.x surfaces a non-2xx JSON-RPC error promptly to the stdio relay."""

    async def scenario() -> None:
        from mcp.client.streamable_http import streamable_http_client

        async def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers["mcp-protocol-version"] == MODERN_PROTOCOL_VERSION
            assert request.headers["mcp-method"] == "server/discover"
            return httpx.Response(
                400,
                headers={"content-type": "application/json"},
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32600, "message": "invalid request"},
                },
            )

        async with (
            httpx.AsyncClient(
                transport=httpx.MockTransport(handler),
                follow_redirects=False,
                trust_env=False,
            ) as client,
            streamable_http_client(
                "https://gateway.example.com/mcp",
                http_client=client,
            ) as (read_stream, write_stream),
        ):
            request = mcp_types.JSONRPCRequest(
                jsonrpc="2.0",
                id=1,
                method="server/discover",
                params={
                    "_meta": {
                        PROTOCOL_VERSION_META_KEY: MODERN_PROTOCOL_VERSION,
                    }
                },
            )
            await write_stream.send(
                ProtocolMetadataBridge().prepare_outbound(SessionMessage(request))
            )
            with anyio.fail_after(1):
                received = await read_stream.receive()

            assert isinstance(received.message, mcp_types.JSONRPCError)
            assert received.message.id == 1
            assert received.message.error.code == -32600

    asyncio.run(scenario())
