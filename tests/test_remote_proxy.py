"""Protocol-preservation tests for the transparent remote relay."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, call, patch

import anyio
import httpx2 as httpx
import pytest
from mcp import types as mcp_types
from mcp.shared.message import ClientMessageMetadata, SessionMessage

from maxcompute_catalog_mcp.remote_proxy import (
    DynamicBearerAuth,
    ProtocolMetadataBridge,
    RemoteMCPInitializationFailure,
    RemoteRequestIdTracker,
    _run_remote_proxy_with_provider,
    probe_remote_mcp,
    relay_client_messages,
    relay_server_messages,
)
from maxcompute_catalog_mcp.request_ids import sanitize_request_id
from maxcompute_catalog_mcp.runtime_config import RemoteRuntimeConfig

MODERN_PROTOCOL_VERSION = "2026-07-28"
LEGACY_PROTOCOL_VERSION = "2025-06-18"
PROTOCOL_VERSION_META_KEY = "io.modelcontextprotocol/protocolVersion"
REQUEST_ID_META_KEY = "com.aliyun.maxcompute/requestId"


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
        timeout = kwargs["timeout"]
        assert isinstance(timeout, httpx.Timeout)
        assert timeout.connect == 30.0
        assert timeout.read is None
        response_hooks = kwargs["event_hooks"]["response"]
        assert len(response_hooks) == 1
        assert isinstance(response_hooks[0].__self__, RemoteRequestIdTracker)
        relay_mock.assert_awaited_once_with(
            local_read,
            local_write,
            remote_read,
            remote_write,
            response_hooks[0].__self__,
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

        observed_hooks: list[object] = []

        def client_factory(**kwargs):
            observed_hooks.extend(kwargs["event_hooks"]["response"])
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
        assert len(observed_hooks) == 1
        assert isinstance(observed_hooks[0].__self__, RemoteRequestIdTracker)

    asyncio.run(scenario())


def test_remote_probe_failure_surfaces_sanitized_request_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Forced remote startup exposes the gateway correlation ID, not credentials."""

    async def scenario() -> None:
        real_async_client = httpx.AsyncClient

        async def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers["Authorization"] == "Bearer gateway-token-1"
            return httpx.Response(
                401,
                headers={
                    "content-type": "application/json",
                    "x-request-id": "gateway-request-401",
                },
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32001, "message": "unauthorized"},
                },
            )

        def client_factory(**kwargs):
            return real_async_client(
                transport=httpx.MockTransport(handler),
                **kwargs,
            )

        with (
            patch(
                "maxcompute_catalog_mcp.remote_proxy.httpx.AsyncClient",
                side_effect=client_factory,
            ),
            pytest.raises(
                RemoteMCPInitializationFailure,
                match="gateway-request-401",
            ) as exc_info,
        ):
            await probe_remote_mcp(
                RemoteRuntimeConfig(url="https://gateway.example.com/mcp"),
                RotatingTokenProvider(),
            )

        assert exc_info.value.request_id == "gateway-request-401"

    caplog.set_level(logging.DEBUG)
    asyncio.run(scenario())

    assert "status=401" in caplog.text
    assert "request_id=gateway-request-401" in caplog.text
    assert "gateway-token-1" not in caplog.text
    assert "Authorization" not in caplog.text


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        " ",
        "request\nid",
        "request\x00id",
        "请求-id",
        "r" * 257,
        42,
        "None",
    ],
)
def test_request_id_sanitizer_rejects_untrusted_values(value: object) -> None:
    """Untrusted response metadata cannot inject logs or oversized errors."""

    assert sanitize_request_id(value) is None


def test_request_id_tracker_logs_success_only_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Successful request IDs remain available for diagnostics without warning."""

    async def scenario() -> None:
        tracker = RemoteRequestIdTracker()
        request = httpx.Request(
            "POST",
            "https://gateway.example.com/mcp",
            json={"jsonrpc": "2.0", "id": 7, "method": "tools/list"},
        )
        await tracker.observe_response(
            httpx.Response(
                200,
                headers={"x-request-id": "gateway-request-200"},
                request=request,
            )
        )

        assert tracker.pop(7) == "gateway-request-200"

    caplog.set_level(logging.DEBUG)
    asyncio.run(scenario())

    assert "DEBUG" in caplog.text
    assert "status=200" in caplog.text
    assert "request_id=gateway-request-200" in caplog.text


@pytest.mark.parametrize(
    ("method", "content"),
    [
        ("GET", b""),
        ("POST", b"not-json"),
        ("POST", b"[]"),
        ("POST", b'{"jsonrpc":"2.0","id":true,"method":"tools/list"}'),
    ],
)
def test_request_id_tracker_ignores_uncorrelatable_http_responses(
    method: str,
    content: bytes,
) -> None:
    """Non-request traffic is logged but never assigned to a JSON-RPC response."""

    async def scenario() -> None:
        tracker = RemoteRequestIdTracker()
        request = httpx.Request(
            method,
            "https://gateway.example.com/mcp",
            content=content,
        )
        await tracker.observe_response(
            httpx.Response(
                200,
                headers={"x-request-id": "gateway-uncorrelated"},
                request=request,
            )
        )

        assert tracker.latest_request_id == "gateway-uncorrelated"
        assert tracker.pop(True) is None
        assert tracker.pop(None) is None

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("original_data", "expected_data"),
    [
        (None, {"request_id": "gateway-error-1"}),
        (
            {"reason": "denied"},
            {"reason": "denied", "request_id": "gateway-error-1"},
        ),
        (
            "provider detail",
            {"details": "provider detail", "request_id": "gateway-error-1"},
        ),
        (
            {"request_id": "backend-request"},
            {"request_id": "backend-request"},
        ),
    ],
)
def test_jsonrpc_errors_include_correlated_gateway_request_id(
    original_data: object,
    expected_data: dict[str, object],
) -> None:
    """Every JSON-RPC error carries the matching HTTP request ID when available."""

    async def scenario() -> None:
        tracker = RemoteRequestIdTracker()
        for request_id, gateway_request_id in (
            (1, "gateway-error-1"),
            ("1", "gateway-error-string-1"),
        ):
            request = httpx.Request(
                "POST",
                "https://gateway.example.com/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "tools/call",
                },
            )
            await tracker.observe_response(
                httpx.Response(
                    400,
                    headers={"x-request-id": gateway_request_id},
                    request=request,
                )
            )

        bridge = ProtocolMetadataBridge(tracker)
        string_id_error = SessionMessage(
            mcp_types.JSONRPCError(
                jsonrpc="2.0",
                id="1",
                error=mcp_types.ErrorData(
                    code=-32600,
                    message="invalid request",
                ),
            )
        )
        bridge.observe_inbound(string_id_error)
        assert string_id_error.message.error.data == {
            "request_id": "gateway-error-string-1"
        }

        error = SessionMessage(
            mcp_types.JSONRPCError(
                jsonrpc="2.0",
                id=1,
                error=mcp_types.ErrorData(
                    code=-32600,
                    message="invalid request",
                    data=original_data,
                ),
            )
        )

        assert bridge.observe_inbound(error) is error
        assert error.message.error.data == expected_data
        assert tracker.pop(1) is None

    asyncio.run(scenario())


def test_tool_errors_use_result_metadata_without_overwriting_existing_ids() -> None:
    """Tool error results expose transport IDs unless the tool already supplied one."""

    async def scenario() -> None:
        tracker = RemoteRequestIdTracker()
        for request_id in (2, 3, 4, 5):
            request = httpx.Request(
                "POST",
                "https://gateway.example.com/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "tools/call",
                },
            )
            await tracker.observe_response(
                httpx.Response(
                    200,
                    headers={"x-request-id": f"gateway-tool-{request_id}"},
                    request=request,
                )
            )

        bridge = ProtocolMetadataBridge(tracker)
        without_body_id = SessionMessage(
            mcp_types.JSONRPCResponse(
                jsonrpc="2.0",
                id=2,
                result={
                    "content": [{"type": "text", "text": "failed"}],
                    "isError": True,
                },
            )
        )
        with_body_id = SessionMessage(
            mcp_types.JSONRPCResponse(
                jsonrpc="2.0",
                id=3,
                result={
                    "content": [{"type": "text", "text": "failed"}],
                    "structuredContent": {"request_id": "backend-tool-request"},
                    "isError": True,
                },
            )
        )
        with_metadata = SessionMessage(
            mcp_types.JSONRPCResponse(
                jsonrpc="2.0",
                id=4,
                result={
                    "_meta": {"existing": "metadata"},
                    "content": [{"type": "text", "text": "failed"}],
                    "isError": True,
                },
            )
        )
        with_transport_id = SessionMessage(
            mcp_types.JSONRPCResponse(
                jsonrpc="2.0",
                id=5,
                result={
                    "_meta": {REQUEST_ID_META_KEY: "original-transport-request"},
                    "content": [{"type": "text", "text": "failed"}],
                    "isError": True,
                },
            )
        )

        bridge.observe_inbound(without_body_id)
        bridge.observe_inbound(with_body_id)
        bridge.observe_inbound(with_metadata)
        bridge.observe_inbound(with_transport_id)

        assert without_body_id.message.result["_meta"] == {
            REQUEST_ID_META_KEY: "gateway-tool-2"
        }
        assert "_meta" not in with_body_id.message.result
        assert with_body_id.message.result["structuredContent"] == {
            "request_id": "backend-tool-request"
        }
        assert with_metadata.message.result["_meta"] == {
            "existing": "metadata",
            REQUEST_ID_META_KEY: "gateway-tool-4",
        }
        assert with_transport_id.message.result["_meta"] == {
            REQUEST_ID_META_KEY: "original-transport-request"
        }

    asyncio.run(scenario())


def test_success_and_headerless_responses_remain_unchanged() -> None:
    """The proxy adds metadata only to failures with a usable correlation ID."""

    async def scenario() -> None:
        tracker = RemoteRequestIdTracker()
        success_request = httpx.Request(
            "POST",
            "https://gateway.example.com/mcp",
            json={"jsonrpc": "2.0", "id": 4, "method": "tools/call"},
        )
        await tracker.observe_response(
            httpx.Response(
                200,
                headers={"x-request-id": "gateway-success-4"},
                request=success_request,
            )
        )
        headerless_request = httpx.Request(
            "POST",
            "https://gateway.example.com/mcp",
            json={"jsonrpc": "2.0", "id": 5, "method": "tools/call"},
        )
        await tracker.observe_response(httpx.Response(500, request=headerless_request))
        bridge = ProtocolMetadataBridge(tracker)
        success_result = {
            "content": [{"type": "text", "text": "ok"}],
            "isError": False,
        }
        success = SessionMessage(
            mcp_types.JSONRPCResponse(
                jsonrpc="2.0",
                id=4,
                result=success_result.copy(),
            )
        )
        headerless_error = SessionMessage(
            mcp_types.JSONRPCError(
                jsonrpc="2.0",
                id=5,
                error=mcp_types.ErrorData(code=-32603, message="failed"),
            )
        )

        bridge.observe_inbound(success)
        bridge.observe_inbound(headerless_error)
        notification = SessionMessage(
            mcp_types.JSONRPCNotification(
                jsonrpc="2.0",
                method="notifications/progress",
            )
        )
        assert bridge.observe_inbound(notification) is notification

        assert success.message.result == success_result
        assert headerless_error.message.error.data is None
        assert tracker.pop(4) is None

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


def test_stdio_bridge_preserves_modern_mrtr_fields_without_interpreting_them() -> None:
    """The stdio relay treats current and future MRTR payload fields as opaque."""
    bridge = ProtocolMetadataBridge()
    params = {
        "name": "maxcompute_generate_sql",
        "arguments": {"question": "revenue?"},
        "requestState": "opaque.sealed.state",
        "inputResponses": {
            "business_clarifications": {
                "action": "accept",
                "content": {"time_semantics": "calendar month"},
            }
        },
        "_meta": {
            PROTOCOL_VERSION_META_KEY: MODERN_PROTOCOL_VERSION,
            "io.modelcontextprotocol/clientCapabilities": {"elicitation": {"form": {}}},
        },
        "futureRequestField": {"must": "survive"},
    }
    request = mcp_types.JSONRPCRequest(
        jsonrpc="2.0",
        id=31,
        method="tools/call",
        params=params,
    )
    outbound = SessionMessage(request)

    assert bridge.prepare_outbound(outbound) is outbound
    assert outbound.message.params == params

    result = {
        "content": None,
        "resultType": "input_required",
        "requestState": "next.opaque.state",
        "inputRequests": {
            "business_clarifications": {
                "type": "object",
                "futureKeyword": {"nested": [1, True, None]},
            }
        },
        "futureResultField": {"must": "survive"},
    }
    inbound = SessionMessage(
        mcp_types.JSONRPCResponse(jsonrpc="2.0", id=31, result=result)
    )

    assert bridge.observe_inbound(inbound) is inbound
    assert inbound.message.result == result


def test_stdio_relay_forwards_mrtr_progress_and_input_required_in_order() -> None:
    """The stdio relay does not filter MRTR progress or final result messages."""

    async def scenario() -> None:
        progress_params = {
            "progressToken": "mrtr-round-2",
            "progress": 1,
            "total": 2,
            "message": "working",
            "_meta": {"futureProgressField": {"must": "survive"}},
        }
        progress = SessionMessage(
            mcp_types.JSONRPCNotification(
                jsonrpc="2.0",
                method="notifications/progress",
                params=progress_params,
            )
        )
        result = {
            "content": None,
            "resultType": "input_required",
            "requestState": "next.opaque.state",
            "inputRequests": {
                "business_clarifications": {
                    "type": "object",
                    "futureKeyword": {"nested": [1, True, None]},
                }
            },
            "futureResultField": {"must": "survive"},
        }
        input_required = SessionMessage(
            mcp_types.JSONRPCResponse(jsonrpc="2.0", id=31, result=result)
        )

        async def source():
            yield progress
            yield input_required

        target = MagicMock(send=AsyncMock())
        await relay_server_messages(source(), target, ProtocolMetadataBridge())

        assert target.send.await_args_list == [call(progress), call(input_required)]
        assert progress.message.params == progress_params
        assert input_required.message.result == result

    asyncio.run(scenario())


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

        request_ids = RemoteRequestIdTracker()

        async def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers["mcp-protocol-version"] == MODERN_PROTOCOL_VERSION
            assert request.headers["mcp-method"] == "server/discover"
            return httpx.Response(
                400,
                headers={
                    "content-type": "application/json",
                    "x-request-id": "gateway-invalid-request-1",
                },
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32600, "message": "invalid request"},
                },
            )

        async with (
            httpx.AsyncClient(
                transport=httpx.MockTransport(handler),
                event_hooks={"response": [request_ids.observe_response]},
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
            bridge = ProtocolMetadataBridge(request_ids)
            await write_stream.send(bridge.prepare_outbound(SessionMessage(request)))
            with anyio.fail_after(1):
                received = await read_stream.receive()
            bridge.observe_inbound(received)

            assert isinstance(received.message, mcp_types.JSONRPCError)
            assert received.message.id == 1
            assert received.message.error.code == -32600
            assert received.message.error.data == {
                "request_id": "gateway-invalid-request-1"
            }

    asyncio.run(scenario())
