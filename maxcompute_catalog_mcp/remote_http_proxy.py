"""Transparent local Streamable HTTP proxy for the remote MCP gateway."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import httpx2 as httpx
from mcp.server.transport_security import (
    TransportSecurityMiddleware,
    TransportSecuritySettings,
)
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from .remote_proxy import (
    AccessTokenProvider,
    DynamicBearerAuth,
    RemoteRequestIdTracker,
)
from .request_ids import request_id_from_exception, sanitize_request_id
from .runtime_config import RemoteRuntimeConfig

_MCP_PATH = "/mcp"
_MCP_REQUEST_HEADERS = frozenset(
    {
        "accept",
        "content-type",
        "last-event-id",
        "mcp-method",
        "mcp-name",
        "mcp-protocol-version",
        "mcp-session-id",
    }
)
_MCP_RESPONSE_HEADERS = frozenset(
    {
        "cache-control",
        "content-type",
        "last-event-id",
        "mcp-protocol-version",
        "mcp-session-id",
        "retry-after",
        "www-authenticate",
        "x-acs-request-id",
        "x-odps-request-id",
        "x-request-id",
    }
)
_REQUEST_ID_HEADER = "X-Request-ID"
_REQUEST_ID_META_KEY = "com.aliyun.maxcompute/requestId"
_REMOTE_REQUEST_ERROR_CODE = -32000
_REMOTE_CONNECT_TIMEOUT_SECONDS = 30.0
_LOGGER = logging.getLogger(__name__)


def _forwarded_headers(
    headers: Mapping[str, str],
    allowed: frozenset[str],
) -> dict[str, str]:
    return {
        name: value
        for name, value in headers.items()
        if name.lower() in allowed or name.lower().startswith("mcp-")
    }


def _transport_security(host: str, port: int) -> TransportSecurityMiddleware:
    host_names = {host}
    if host in {"127.0.0.1", "::1", "localhost"}:
        host_names.update({"127.0.0.1", "::1", "localhost"})

    allowed_hosts: list[str] = []
    allowed_origins: list[str] = []
    for host_name in sorted(host_names):
        rendered_host = f"[{host_name}]" if ":" in host_name else host_name
        allowed_hosts.extend(
            [
                rendered_host,
                f"{rendered_host}:{port}",
                f"{rendered_host}:*",
            ]
        )
        allowed_origins.extend(
            [
                f"http://{rendered_host}:{port}",
                f"http://{rendered_host}:*",
            ]
        )

    return TransportSecurityMiddleware(
        TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=allowed_hosts,
            allowed_origins=allowed_origins,
        )
    )


def _jsonrpc_id(body: bytes) -> int | str | None:
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    request_id = payload.get("id")
    if isinstance(request_id, bool) or not isinstance(request_id, int | str):
        return None
    return request_id


def _add_request_id_to_error_data(error: dict[str, Any], request_id: str) -> None:
    data = error.get("data")
    if isinstance(data, dict):
        if "request_id" not in data:
            error["data"] = {**data, "request_id": request_id}
        return
    if data is None:
        error["data"] = {"request_id": request_id}
        return
    error["data"] = {"details": data, "request_id": request_id}


def _add_request_id_to_tool_error(result: dict[str, Any], request_id: str) -> None:
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


def _add_request_id_to_payload(payload: object, request_id: str) -> bool:
    if not isinstance(payload, dict):
        return False
    error = payload.get("error")
    if isinstance(error, dict):
        _add_request_id_to_error_data(error, request_id)
        return True
    result = payload.get("result")
    if isinstance(result, dict) and result.get("isError") is True:
        _add_request_id_to_tool_error(result, request_id)
        return True
    return False


def _enrich_json_body(body: bytes, request_id: str | None) -> bytes:
    if request_id is None:
        return body
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return body
    if not _add_request_id_to_payload(payload, request_id):
        return body
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


def _sse_data(line: bytes) -> bytes | None:
    stripped = line.rstrip(b"\r\n")
    if not stripped.startswith(b"data:"):
        return None
    value = stripped[len(b"data:") :]
    if value.startswith(b" "):
        value = value[1:]
    return value


def _enrich_sse_event(
    lines: list[bytes],
    terminator: bytes,
    request_id: str | None,
) -> bytes:
    if request_id is None:
        return b"".join(lines) + terminator
    data_parts: list[bytes] = []
    data_indices: list[int] = []
    for index, line in enumerate(lines):
        data = _sse_data(line)
        if data is not None:
            data_indices.append(index)
            data_parts.append(data)
    if not data_indices:
        return b"".join(lines) + terminator
    try:
        payload = json.loads(b"\n".join(data_parts))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return b"".join(lines) + terminator
    if not _add_request_id_to_payload(payload, request_id):
        return b"".join(lines) + terminator

    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    first_data_index = data_indices[0]
    data_index_set = set(data_indices)
    rewritten: list[bytes] = []
    for index, line in enumerate(lines):
        if index == first_data_index:
            line_ending = b"\r\n" if line.endswith(b"\r\n") else b"\n"
            rewritten.append(b"data: " + encoded + line_ending)
        elif index not in data_index_set:
            rewritten.append(line)
    return b"".join(rewritten) + terminator


async def _stream_sse_response(
    response: httpx.Response,
    request_id: str | None,
) -> AsyncIterator[bytes]:
    buffer = b""
    event_lines: list[bytes] = []
    try:
        async for chunk in response.aiter_bytes():
            buffer += chunk
            while True:
                newline_index = buffer.find(b"\n")
                if newline_index < 0:
                    break
                line = buffer[: newline_index + 1]
                buffer = buffer[newline_index + 1 :]
                if line.rstrip(b"\r\n"):
                    event_lines.append(line)
                    continue
                yield _enrich_sse_event(event_lines, line, request_id)
                event_lines = []
        if event_lines or buffer:
            yield b"".join(event_lines) + buffer
    finally:
        await response.aclose()


async def _stream_response(response: httpx.Response) -> AsyncIterator[bytes]:
    try:
        async for chunk in response.aiter_bytes():
            yield chunk
    finally:
        await response.aclose()


def _remote_error_response(body: bytes, error: Exception) -> JSONResponse:
    request_id = request_id_from_exception(error)
    if request_id is None:
        _LOGGER.warning(
            "Remote MCP request failed (%s)",
            type(error).__name__,
        )
    else:
        _LOGGER.warning(
            "Remote MCP request failed (%s, request_id=%s)",
            type(error).__name__,
            request_id,
        )
    error_payload: dict[str, Any] = {
        "code": _REMOTE_REQUEST_ERROR_CODE,
        "message": "remote MCP request failed",
    }
    if request_id is not None:
        error_payload["data"] = {"request_id": request_id}
    return JSONResponse(
        {
            "jsonrpc": "2.0",
            "id": _jsonrpc_id(body),
            "error": error_payload,
        },
        status_code=502,
    )


async def _proxy_response(response: httpx.Response) -> Response:
    headers = _forwarded_headers(response.headers, _MCP_RESPONSE_HEADERS)
    content_type = response.headers.get("content-type", "").lower()
    request_id = sanitize_request_id(response.headers.get(_REQUEST_ID_HEADER))
    if content_type.startswith("application/json"):
        try:
            body = await response.aread()
        finally:
            await response.aclose()
        return Response(
            _enrich_json_body(body, request_id),
            status_code=response.status_code,
            headers=headers,
        )
    if response.status_code in {204, 304}:
        await response.aclose()
        return Response(status_code=response.status_code, headers=headers)
    if content_type.startswith("text/event-stream"):
        return StreamingResponse(
            _stream_sse_response(response, request_id),
            status_code=response.status_code,
            headers=headers,
        )
    return StreamingResponse(
        _stream_response(response),
        status_code=response.status_code,
        headers=headers,
    )


def build_remote_http_proxy_app(
    config: RemoteRuntimeConfig,
    token_provider: AccessTokenProvider,
    *,
    host: str,
    port: int,
) -> Starlette:
    """Build a hardened ASGI reverse proxy for one fixed Remote MCP endpoint."""

    request_ids = RemoteRequestIdTracker()
    security = _transport_security(host, port)

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        timeout = httpx.Timeout(_REMOTE_CONNECT_TIMEOUT_SECONDS, read=None)
        async with httpx.AsyncClient(
            auth=DynamicBearerAuth(token_provider),
            event_hooks={"response": [request_ids.observe_response]},
            follow_redirects=False,
            timeout=timeout,
            trust_env=False,
        ) as client:
            app.state.remote_mcp_client = client
            yield

    async def proxy(request: Request) -> Response:
        validation_error = await security.validate_request(
            request,
            is_post=request.method == "POST",
        )
        if validation_error is not None:
            return validation_error
        body = await request.body()
        try:
            upstream_request = request.app.state.remote_mcp_client.build_request(
                request.method,
                config.url,
                content=body,
                headers=_forwarded_headers(request.headers, _MCP_REQUEST_HEADERS),
            )
            upstream_response = await request.app.state.remote_mcp_client.send(
                upstream_request,
                stream=True,
                follow_redirects=False,
            )
        except Exception as error:  # noqa: BLE001 -- sanitize the network boundary.
            return _remote_error_response(body, error)
        return await _proxy_response(upstream_response)

    return Starlette(
        routes=[Route(_MCP_PATH, proxy, methods=["DELETE", "GET", "POST"])],
        lifespan=lifespan,
    )


def run_remote_http_proxy(
    config: RemoteRuntimeConfig,
    token_provider: AccessTokenProvider,
    *,
    host: str,
    port: int,
) -> None:
    """Serve the Remote MCP reverse proxy until process shutdown."""

    import uvicorn

    app = build_remote_http_proxy_app(
        config,
        token_provider,
        host=host,
        port=port,
    )
    uvicorn.run(app, host=host, port=port)
