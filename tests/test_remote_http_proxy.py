"""Streamable HTTP integration tests for the transparent Remote proxy."""

from __future__ import annotations

import asyncio
import gzip
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from unittest.mock import patch

import httpx2 as httpx
import pytest
from starlette.testclient import TestClient

from maxcompute_catalog_mcp.remote_auth import CatalogTokenRequestError
from maxcompute_catalog_mcp.remote_http_proxy import (
    build_remote_http_proxy_app,
    run_remote_http_proxy,
)
from maxcompute_catalog_mcp.remote_proxy import DynamicBearerAuth
from maxcompute_catalog_mcp.runtime_config import RemoteRuntimeConfig

REMOTE_URL = "https://gateway.example.com/mcp"
LOCAL_BASE_URL = "http://127.0.0.1:8123"
PROTOCOL_VERSION = "2025-06-18"
REAL_ASYNC_CLIENT = httpx.AsyncClient


@dataclass
class RotatingTokenProvider:
    calls: int = 0

    async def get_access_token(self) -> str:
        self.calls += 1
        return f"catalog-token-{self.calls}"


class FailingTokenProvider:
    async def get_access_token(self) -> str:
        raise CatalogTokenRequestError("catalog-token-request-403")


class StaticAsyncStream(httpx.AsyncByteStream):
    def __init__(self, *chunks: bytes) -> None:
        self._chunks = chunks

    async def __aiter__(self):
        for chunk in self._chunks:
            yield chunk


class TrackingAsyncStream(StaticAsyncStream):
    def __init__(self, *chunks: bytes) -> None:
        super().__init__(*chunks)
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


RequestHandler = Callable[[httpx.Request], Awaitable[httpx.Response]]


def _patched_client_factory(
    handler: RequestHandler,
    observed_options: list[dict[str, Any]] | None = None,
) -> Callable[..., httpx.AsyncClient]:
    def factory(**kwargs: Any) -> httpx.AsyncClient:
        if observed_options is not None:
            observed_options.append(kwargs.copy())
        return REAL_ASYNC_CLIENT(
            transport=httpx.MockTransport(handler),
            **kwargs,
        )

    return factory


def _app(provider: Any = None):
    return build_remote_http_proxy_app(
        RemoteRuntimeConfig(url=REMOTE_URL),
        provider or RotatingTokenProvider(),
        host="127.0.0.1",
        port=8123,
    )


def test_post_json_is_transparently_proxied_with_dynamic_catalog_auth() -> None:
    """POST preserves MCP semantics but replaces caller-supplied authorization."""
    provider = RotatingTokenProvider()
    observed_options: list[dict[str, Any]] = []
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        await request.aread()
        requests.append(request)
        return httpx.Response(
            200,
            headers={
                "content-type": "application/json",
                "mcp-session-id": "remote-session-1",
                "x-request-id": "gateway-post-1",
            },
            json={"jsonrpc": "2.0", "id": 1, "result": {"tools": []}},
        )

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler, observed_options),
        ),
        TestClient(_app(provider), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.post(
            "/mcp?untrusted=query",
            headers={
                "Accept": "application/json, text/event-stream",
                "Authorization": "Bearer caller-controlled-token",
                "Content-Type": "application/json",
                "MCP-Protocol-Version": PROTOCOL_VERSION,
                "Mcp-Method": "tools/list",
                "Mcp-Future-Extension": "preserve-me",
                "Origin": LOCAL_BASE_URL,
            },
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        )

    assert response.status_code == 200
    assert response.json() == {"jsonrpc": "2.0", "id": 1, "result": {"tools": []}}
    assert response.headers["mcp-session-id"] == "remote-session-1"
    assert response.headers["x-request-id"] == "gateway-post-1"
    assert len(requests) == 1
    upstream = requests[0]
    assert str(upstream.url) == REMOTE_URL
    assert upstream.headers["authorization"] == "Bearer catalog-token-1"
    assert upstream.headers["accept"] == "application/json, text/event-stream"
    assert upstream.headers["content-type"].startswith("application/json")
    assert upstream.headers["mcp-protocol-version"] == PROTOCOL_VERSION
    assert upstream.headers["mcp-method"] == "tools/list"
    assert upstream.headers["mcp-future-extension"] == "preserve-me"
    assert "origin" not in upstream.headers
    assert provider.calls == 1

    assert len(observed_options) == 1
    options = observed_options[0]
    assert isinstance(options["auth"], DynamicBearerAuth)
    assert options["auth"]._provider is provider
    assert options["follow_redirects"] is False
    assert options["trust_env"] is False


def test_get_sse_preserves_session_resumption_and_streaming_headers() -> None:
    """GET keeps the downstream session and Last-Event-ID isolated end to end."""
    provider = RotatingTokenProvider()

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.headers["authorization"] == "Bearer catalog-token-1"
        assert request.headers["mcp-session-id"] == "remote-session-get"
        assert request.headers["last-event-id"] == "event-41"
        assert request.headers["mcp-protocol-version"] == PROTOCOL_VERSION
        return httpx.Response(
            200,
            headers={
                "content-type": "text/event-stream",
                "cache-control": "no-cache",
                "mcp-session-id": "remote-session-get",
            },
            stream=StaticAsyncStream(
                b"id: event-42\n"
                b"event: message\n"
                b'data: {"jsonrpc":"2.0","method":"notifications/progress"}\n\n'
            ),
        )

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(provider), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.get(
            "/mcp",
            headers={
                "Accept": "text/event-stream",
                "Authorization": "Bearer caller-controlled-token",
                "MCP-Protocol-Version": PROTOCOL_VERSION,
                "Mcp-Session-Id": "remote-session-get",
                "Last-Event-ID": "event-41",
            },
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["mcp-session-id"] == "remote-session-get"
    assert "id: event-42" in response.text
    assert "notifications/progress" in response.text


def test_delete_is_forwarded_without_sharing_session_state() -> None:
    """DELETE terminates exactly the session supplied by the downstream client."""

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "DELETE"
        assert request.headers["mcp-session-id"] == "remote-session-delete"
        assert request.headers["authorization"] == "Bearer catalog-token-1"
        return httpx.Response(204)

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.delete(
            "/mcp",
            headers={
                "Mcp-Session-Id": "remote-session-delete",
                "MCP-Protocol-Version": PROTOCOL_VERSION,
            },
        )

    assert response.status_code == 204
    assert response.content == b""


def test_post_sse_keeps_streamable_http_response_semantics() -> None:
    """A POST may return SSE and must not be collapsed into a JSON response."""

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=StaticAsyncStream(
                b"event: message\n"
                b'data: {"jsonrpc":"2.0","id":8,"result":{"ok":true}}\n\n'
            ),
        )

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.post(
            "/mcp",
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
            },
            json={"jsonrpc": "2.0", "id": 8, "method": "tools/list"},
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert '"id":8' in response.text
    assert '"ok":true' in response.text


def test_invalid_origin_and_host_are_rejected_before_upstream_auth() -> None:
    """The local listener enforces the MCP DNS-rebinding boundary."""
    provider = RotatingTokenProvider()
    upstream_calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal upstream_calls
        upstream_calls += 1
        return httpx.Response(202, stream=StaticAsyncStream())

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(provider), base_url=LOCAL_BASE_URL) as client,
    ):
        bad_origin = client.post(
            "/mcp",
            headers={
                "Content-Type": "application/json",
                "Origin": "https://attacker.example.com",
            },
            json={"jsonrpc": "2.0", "method": "notifications/initialized"},
        )
        bad_host = client.post(
            "/mcp",
            headers={
                "Content-Type": "application/json",
                "Host": "attacker.example.com",
            },
            json={"jsonrpc": "2.0", "method": "notifications/initialized"},
        )

    assert bad_origin.status_code == 403
    assert bad_host.status_code == 421
    assert upstream_calls == 0
    assert provider.calls == 0


def test_concurrent_clients_keep_their_remote_sessions_isolated() -> None:
    """The proxy carries session state per request instead of process globally."""
    provider = RotatingTokenProvider()
    seen_sessions: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        session_id = request.headers["mcp-session-id"]
        seen_sessions.append(session_id)
        await asyncio.sleep(0)
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={"jsonrpc": "2.0", "id": session_id, "result": {}},
        )

    async def scenario() -> None:
        with patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ):
            app = _app(provider)
            async with (
                app.router.lifespan_context(app),
                REAL_ASYNC_CLIENT(
                    transport=httpx.ASGITransport(app=app),
                    base_url=LOCAL_BASE_URL,
                ) as client,
            ):
                responses = await asyncio.gather(
                    client.post(
                        "/mcp",
                        headers={
                            "Content-Type": "application/json",
                            "Mcp-Session-Id": "session-a",
                        },
                        json={"jsonrpc": "2.0", "id": "session-a", "method": "ping"},
                    ),
                    client.post(
                        "/mcp",
                        headers={
                            "Content-Type": "application/json",
                            "Mcp-Session-Id": "session-b",
                        },
                        json={"jsonrpc": "2.0", "id": "session-b", "method": "ping"},
                    ),
                )

        assert {response.json()["id"] for response in responses} == {
            "session-a",
            "session-b",
        }

    asyncio.run(scenario())

    assert set(seen_sessions) == {"session-a", "session-b"}
    assert provider.calls == 2


def test_jsonrpc_error_body_gains_gateway_request_id() -> None:
    """A gateway request ID is carried in both the HTTP header and MCP error body."""

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            headers={
                "content-type": "application/json",
                "x-request-id": "gateway-jsonrpc-400",
            },
            json={
                "jsonrpc": "2.0",
                "id": 11,
                "error": {"code": -32600, "message": "invalid request"},
            },
        )

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.post(
            "/mcp",
            headers={"Content-Type": "application/json"},
            json={"jsonrpc": "2.0", "id": 11, "method": "tools/call"},
        )

    assert response.status_code == 400
    assert response.headers["x-request-id"] == "gateway-jsonrpc-400"
    assert response.json()["error"]["data"] == {"request_id": "gateway-jsonrpc-400"}


def test_sse_error_event_gains_gateway_request_id() -> None:
    """Request ID enrichment also works when the MCP response uses SSE."""

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "content-type": "text/event-stream",
                "x-request-id": "gateway-sse-error",
            },
            stream=StaticAsyncStream(
                b"event: message\n"
                b'data: {"jsonrpc":"2.0","id":12,"error":'
                b'{"code":-32603,"message":"failed"}}\n\n'
            ),
        )

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.post(
            "/mcp",
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
            },
            json={"jsonrpc": "2.0", "id": 12, "method": "tools/call"},
        )

    data_line = next(
        line.removeprefix("data: ")
        for line in response.text.splitlines()
        if line.startswith("data: ")
    )
    payload = json.loads(data_line)
    assert payload["error"]["data"] == {"request_id": "gateway-sse-error"}


def test_catalog_token_failure_returns_safe_request_id_error_body() -> None:
    """Token refresh failures are fail-closed and expose only safe correlation data."""

    async def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("upstream must not run without a Catalog token")

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(FailingTokenProvider()), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.post(
            "/mcp",
            headers={"Content-Type": "application/json"},
            json={"jsonrpc": "2.0", "id": 13, "method": "tools/list"},
        )

    assert response.status_code == 502
    assert response.json() == {
        "jsonrpc": "2.0",
        "id": 13,
        "error": {
            "code": -32000,
            "message": "remote MCP request failed",
            "data": {"request_id": "catalog-token-request-403"},
        },
    }
    assert "Bearer" not in response.text


@pytest.mark.parametrize(
    ("upstream_payload", "expected_payload"),
    [
        (
            {
                "jsonrpc": "2.0",
                "id": 21,
                "error": {
                    "code": -32001,
                    "message": "denied",
                    "data": {"reason": "policy"},
                },
            },
            {
                "jsonrpc": "2.0",
                "id": 21,
                "error": {
                    "code": -32001,
                    "message": "denied",
                    "data": {
                        "reason": "policy",
                        "request_id": "gateway-payload-21",
                    },
                },
            },
        ),
        (
            {
                "jsonrpc": "2.0",
                "id": 21,
                "error": {
                    "code": -32001,
                    "message": "denied",
                    "data": {"request_id": "backend-request-id"},
                },
            },
            {
                "jsonrpc": "2.0",
                "id": 21,
                "error": {
                    "code": -32001,
                    "message": "denied",
                    "data": {"request_id": "backend-request-id"},
                },
            },
        ),
        (
            {
                "jsonrpc": "2.0",
                "id": 21,
                "error": {
                    "code": -32001,
                    "message": "denied",
                    "data": "provider detail",
                },
            },
            {
                "jsonrpc": "2.0",
                "id": 21,
                "error": {
                    "code": -32001,
                    "message": "denied",
                    "data": {
                        "details": "provider detail",
                        "request_id": "gateway-payload-21",
                    },
                },
            },
        ),
        (
            {
                "jsonrpc": "2.0",
                "id": 21,
                "result": {
                    "content": [{"type": "text", "text": "failed"}],
                    "structuredContent": {"request_id": "backend-tool-request"},
                    "isError": True,
                },
            },
            {
                "jsonrpc": "2.0",
                "id": 21,
                "result": {
                    "content": [{"type": "text", "text": "failed"}],
                    "structuredContent": {"request_id": "backend-tool-request"},
                    "isError": True,
                },
            },
        ),
        (
            {
                "jsonrpc": "2.0",
                "id": 21,
                "result": {
                    "_meta": {"existing": "metadata"},
                    "content": [{"type": "text", "text": "failed"}],
                    "isError": True,
                },
            },
            {
                "jsonrpc": "2.0",
                "id": 21,
                "result": {
                    "_meta": {
                        "existing": "metadata",
                        "com.aliyun.maxcompute/requestId": "gateway-payload-21",
                    },
                    "content": [{"type": "text", "text": "failed"}],
                    "isError": True,
                },
            },
        ),
        (
            {
                "jsonrpc": "2.0",
                "id": 21,
                "result": {
                    "content": [{"type": "text", "text": "failed"}],
                    "isError": True,
                },
            },
            {
                "jsonrpc": "2.0",
                "id": 21,
                "result": {
                    "_meta": {"com.aliyun.maxcompute/requestId": "gateway-payload-21"},
                    "content": [{"type": "text", "text": "failed"}],
                    "isError": True,
                },
            },
        ),
    ],
)
def test_json_failures_preserve_details_and_existing_request_ids(
    upstream_payload: dict[str, Any],
    expected_payload: dict[str, Any],
) -> None:
    """Correlation enrichment never discards provider details or body IDs."""

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "content-type": "application/json",
                "x-request-id": "gateway-payload-21",
            },
            json=upstream_payload,
        )

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.post(
            "/mcp",
            headers={"Content-Type": "application/json"},
            json={"jsonrpc": "2.0", "id": 21, "method": "tools/call"},
        )

    assert response.json() == expected_payload


@pytest.mark.parametrize(
    "upstream_body",
    [
        b"not-json",
        b"[]",
        b'{"jsonrpc":"2.0","id":22,"result":{"ok":true}}',
    ],
)
def test_non_error_json_bodies_remain_byte_for_byte_unchanged(
    upstream_body: bytes,
) -> None:
    """A response header alone does not mutate malformed or successful payloads."""

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "content-type": "application/json",
                "x-request-id": "gateway-non-error-22",
            },
            content=upstream_body,
        )

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.post(
            "/mcp",
            headers={"Content-Type": "application/json"},
            json={"jsonrpc": "2.0", "id": 22, "method": "tools/list"},
        )

    assert response.content == upstream_body


def test_sse_parser_preserves_heartbeats_and_enriches_chunked_crlf_error() -> None:
    """SSE framing survives chunk boundaries, CRLF, comments, and partial tails."""

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "content-type": "text/event-stream",
                "x-request-id": "gateway-chunked-sse",
            },
            stream=StaticAsyncStream(
                b": heartbeat\r\n\r\nevent: message\r\ndata:not-json\r\n\r\n",
                b'event: message\r\ndata: {"jsonrpc":"2.0",',
                b'\r\ndata: "id":23,"error":{"code":-32603,',
                b'"message":"failed"}}\r\n\r\n: trailing',
            ),
        )

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.post(
            "/mcp",
            headers={"Content-Type": "application/json"},
            json={"jsonrpc": "2.0", "id": 23, "method": "tools/call"},
        )

    assert ": heartbeat\r\n\r\n" in response.text
    assert "data:not-json\r\n\r\n" in response.text
    assert ": trailing" in response.text
    enriched_line = next(
        line.removeprefix("data: ")
        for line in response.text.splitlines()
        if '"id":23' in line
    )
    assert json.loads(enriched_line)["error"]["data"] == {
        "request_id": "gateway-chunked-sse"
    }


def test_non_mcp_response_body_streams_and_closes_upstream() -> None:
    """Unexpected bodies remain transparent and release the upstream stream."""
    upstream_stream = TrackingAsyncStream(b"first-", b"second")

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            418,
            headers={"content-type": "application/octet-stream"},
            stream=upstream_stream,
        )

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.get("/mcp")

    assert response.status_code == 418
    assert response.content == b"first-second"
    assert upstream_stream.closed is True


def test_compressed_sse_is_decoded_before_protocol_processing() -> None:
    """Gateway compression cannot hide SSE framing from request-ID enrichment."""
    compressed = gzip.compress(
        b"event: message\n"
        b'data: {"jsonrpc":"2.0","id":24,"error":'
        b'{"code":-32603,"message":"failed"}}\n\n'
    )

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "content-encoding": "gzip",
                "content-type": "text/event-stream",
                "x-request-id": "gateway-compressed-sse",
            },
            stream=StaticAsyncStream(compressed),
        )

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.post(
            "/mcp",
            headers={"Content-Type": "application/json"},
            json={"jsonrpc": "2.0", "id": 24, "method": "tools/call"},
        )

    assert "content-encoding" not in response.headers
    data_line = next(
        line.removeprefix("data: ")
        for line in response.text.splitlines()
        if line.startswith("data: ")
    )
    assert json.loads(data_line)["error"]["data"] == {
        "request_id": "gateway-compressed-sse"
    }


def test_mrtr_request_and_sse_messages_are_preserved_byte_for_byte() -> None:
    """MRTR state, input, progress, and input-required results remain opaque."""
    request_body = (
        b'{ "jsonrpc": "2.0", "id": 31, "method": "tools/call", '
        b'"params": { "name": "maxcompute_generate_sql", '
        b'"arguments": {"question":"revenue?"}, '
        b'"requestState": "opaque.sealed.state", '
        b'"inputResponses": {"business_clarifications": '
        b'{"action":"accept","content":{"time_semantics":"calendar month"}}}, '
        b'"_meta": {"progressToken":"mrtr-round-2", '
        b'"io.modelcontextprotocol/protocolVersion":"2026-07-28", '
        b'"io.modelcontextprotocol/clientCapabilities":'
        b'{"elicitation":{"form":{}}}} } }'
    )
    response_body = (
        b"event: message\n"
        b'data: {"jsonrpc":"2.0","method":"notifications/progress",'
        b'"params":{"progressToken":"mrtr-round-2","progress":1,'
        b'"total":2,"message":"working","_meta":{"future":"value"}}}\n\n'
        b"event: message\n"
        b'data: {"jsonrpc":"2.0","id":31,"result":{"content":null,'
        b'"resultType":"input_required","requestState":"next.opaque.state",'
        b'"inputRequests":{"business_clarifications":{"type":"object",'
        b'"futureKeyword":{"nested":[1,true,null]}}},'
        b'"futureResultField":{"must":"survive"}}}\n\n'
    )
    seen_bodies: list[bytes] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_bodies.append(await request.aread())
        assert request.headers["mcp-protocol-version"] == "2026-07-28"
        assert request.headers["mcp-method"] == "tools/call"
        assert request.headers["mcp-name"] == "maxcompute_generate_sql"
        assert request.headers["mcp-mrtr-round"] == "retry"
        return httpx.Response(
            200,
            headers={
                "content-type": "text/event-stream",
                "mcp-future-response": "preserve-me-too",
                "x-request-id": "gateway-mrtr-31",
            },
            stream=StaticAsyncStream(response_body[:97], response_body[97:]),
        )

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.post(
            "/mcp",
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
                "Mcp-Method": "tools/call",
                "Mcp-Name": "maxcompute_generate_sql",
                "Mcp-Protocol-Version": "2026-07-28",
                "Mcp-MRTR-Round": "retry",
            },
            content=request_body,
        )

    assert seen_bodies == [request_body]
    assert response.content == response_body
    assert response.headers["mcp-future-response"] == "preserve-me-too"


@pytest.mark.parametrize(
    "request_body",
    [b"not-json", b"[]", b'{"jsonrpc":"2.0","id":true}'],
)
def test_transport_failure_without_request_id_returns_safe_generic_error(
    request_body: bytes,
) -> None:
    """Uncorrelated network failures never invent or leak diagnostic details."""

    async def handler(_request: httpx.Request) -> httpx.Response:
        raise RuntimeError("secret upstream diagnostic")

    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(_app(), base_url=LOCAL_BASE_URL) as client,
    ):
        response = client.post(
            "/mcp",
            headers={"Content-Type": "application/json"},
            content=request_body,
        )

    assert response.status_code == 502
    assert response.json() == {
        "jsonrpc": "2.0",
        "id": None,
        "error": {
            "code": -32000,
            "message": "remote MCP request failed",
        },
    }
    assert "secret upstream diagnostic" not in response.text


def test_explicit_non_loopback_listener_validates_its_configured_host() -> None:
    """An explicitly exposed listener still pins Host and Origin validation."""
    upstream_calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal upstream_calls
        upstream_calls += 1
        return httpx.Response(202, stream=StaticAsyncStream())

    app = build_remote_http_proxy_app(
        RemoteRuntimeConfig(url=REMOTE_URL),
        RotatingTokenProvider(),
        host="0.0.0.0",
        port=8123,
    )
    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.httpx.AsyncClient",
            side_effect=_patched_client_factory(handler),
        ),
        TestClient(app, base_url="http://0.0.0.0:8123") as client,
    ):
        response = client.post(
            "/mcp",
            headers={
                "Content-Type": "application/json",
                "Origin": "http://0.0.0.0:8123",
            },
            json={"jsonrpc": "2.0", "method": "notifications/initialized"},
        )

    assert response.status_code == 202
    assert upstream_calls == 1


def test_remote_http_runner_serves_exact_app_and_listener() -> None:
    """The process runner exposes the proxy app on the requested host and port."""
    config = RemoteRuntimeConfig(url=REMOTE_URL)
    provider = RotatingTokenProvider()
    with (
        patch(
            "maxcompute_catalog_mcp.remote_http_proxy.build_remote_http_proxy_app",
        ) as build_app_mock,
        patch("uvicorn.run") as uvicorn_run_mock,
    ):
        run_remote_http_proxy(
            config,
            provider,
            host="127.0.0.1",
            port=8123,
        )

    build_app_mock.assert_called_once_with(
        config,
        provider,
        host="127.0.0.1",
        port=8123,
    )
    uvicorn_run_mock.assert_called_once_with(
        build_app_mock.return_value,
        host="127.0.0.1",
        port=8123,
    )
