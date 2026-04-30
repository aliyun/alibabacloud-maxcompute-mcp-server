from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from dataclasses import replace
from typing import Optional

from .config import (
    load_config,
    resolve_catalogapi_endpoint_with_client,
    resolve_protocol_and_endpoints,
)
from .credentials import get_credentials_client
from .maxcompute_client import MaxComputeCatalogSdk, MaxComputeClient
from .mcp_protocol import JsonRpcError
from .tools import Tools


_KNOWN_TRANSPORTS = ("stdio", "http", "streamable-http")


def _parse_args() -> tuple[Optional[str], str, str, int]:
    """Parse CLI arguments: (config_path, transport, host, port)."""
    parser = argparse.ArgumentParser(
        prog="alibabacloud-maxcompute-mcp-server",
        description="MaxCompute Catalog MCP server",
    )
    parser.add_argument(
        "--config",
        type=lambda p: os.path.abspath(p),
        default=None,
        help="Path to config.json",
    )
    parser.add_argument(
        "--transport",
        choices=_KNOWN_TRANSPORTS,
        default="stdio",
        help="Transport mode (default: stdio)",
    )
    parser.add_argument("--host", default="127.0.0.1", help="HTTP server host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="HTTP server port (default: 8000)")
    args, _ = parser.parse_known_args()
    return args.config, args.transport, args.host, args.port


def build_tools(config_path: Optional[str] = None) -> Tools:
    """Build and return a Tools instance.

    Initialization order:
    1. Load config (catalogapi_endpoint may be empty)
    2. Create credentials client
    3. Create ODPS client (for SQL execution)
    4. Resolve catalogapi_endpoint via ODPS client if not configured
    5. Create Catalog SDK client
    """
    cfg = load_config(config_path)

    # Create a unified Credentials Client singleton shared by all components,
    # supporting automatic credential refresh.
    # Priority: static AK/SK from config.json > default credential chain
    # (environment variables / credentials_uri / ECS RAM Role / etc.)
    try:
        credential_client = get_credentials_client(
            access_key_id=cfg.access_key_id,
            access_key_secret=cfg.access_key_secret,
            security_token=cfg.security_token,
        )
    except ValueError as e:
        sys.exit(
            f"Failed to initialize credentials: {e}\n"
            "Hint: provide credentials via one of the following methods:\n"
            "  1. Set access_key_id / access_key_secret in config.json\n"
            "  2. Set ALIBABA_CLOUD_ACCESS_KEY_ID and ALIBABA_CLOUD_ACCESS_KEY_SECRET env vars\n"
            "  3. Set ALIBABA_CLOUD_CREDENTIALS_URI to a credentials endpoint (for STS auto-refresh)\n"
            "  4. Run on an ECS instance with a RAM role attached\n"
            "Ensure 'alibabacloud-credentials' is installed: pip install alibabacloud-credentials"
        )

    # create ODPS client first; reused below to resolve catalogapi_endpoint if needed
    resolved = resolve_protocol_and_endpoints(cfg)
    maxcompute_client = MaxComputeClient.create(
        cfg, credential_client=credential_client, resolved=resolved,
    )

    # use configured catalogapi_endpoint if set; otherwise resolve via ODPS client
    catalogapi_endpoint = cfg.catalogapi_endpoint
    if not catalogapi_endpoint:
        if maxcompute_client is None:
            sys.exit(
                "Failed to create MaxCompute client. "
                "Cannot resolve catalogapi_endpoint without a valid ODPS client. "
                "Please ensure default_project and maxcompute_endpoint are configured, "
                "or explicitly set MAXCOMPUTE_CATALOG_API_ENDPOINT."
            )
        try:
            catalogapi_endpoint = resolve_catalogapi_endpoint_with_client(
                maxcompute_client.odps_client,
                resolved.maxcompute_url,
            )
        except Exception as e:
            sys.exit(f"Failed to resolve catalogapi endpoint: {e}")

    # update config with resolved endpoint, then re-resolve so catalogapi_protocol/host
    # reflect any scheme embedded in the probed value.
    cfg = replace(cfg, catalogapi_endpoint=catalogapi_endpoint)
    resolved = resolve_protocol_and_endpoints(cfg)

    # create Catalog SDK client
    try:
        sdk = MaxComputeCatalogSdk.create(
            cfg, credential_client=credential_client, resolved=resolved,
        )
    except RuntimeError as e:
        sys.exit(f"Failed to initialize MaxCompute Catalog SDK: {e}")

    return Tools(
        sdk=sdk,
        default_project=cfg.default_project,
        namespace_id=cfg.namespace_id,
        maxcompute_client=maxcompute_client,
        credential_client=credential_client,
    )


def _build_mcp_server(tools: Tools) -> McpServer:
    """Build an mcp.server.Server wired to the given Tools instance."""
    from mcp import types as mcp_types
    from mcp.server import Server as McpServer

    server = McpServer("maxcompute-catalog-server-python")

    @server.list_tools()
    async def list_tools() -> list[mcp_types.Tool]:
        return [
            mcp_types.Tool(
                name=s.name,
                description=s.description,
                inputSchema=s.input_schema,
            )
            for s in tools.specs()
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[mcp_types.Content]:
        try:
            result = tools.call(name, arguments or {})
            content = result.get("content", [])
            return [
                mcp_types.TextContent(type="text", text=c["text"])
                for c in content
                if c.get("type") == "text"
            ]
        except JsonRpcError as e:
            raise ValueError(f"{e.message}: {e.data}") from e

    return server


async def _run_stdio(tools: Tools) -> None:
    from mcp.server.stdio import stdio_server

    mcp_server = _build_mcp_server(tools)
    async with stdio_server() as (read_stream, write_stream):
        await mcp_server.run(
            read_stream,
            write_stream,
            mcp_server.create_initialization_options(),
        )


def _run_http(tools: Tools, host: str, port: int) -> None:
    """Start a Streamable HTTP server on host:port.

    Endpoint:
      /mcp  — Streamable HTTP (GET/POST/DELETE)
    """
    import uvicorn
    from starlette.applications import Starlette
    from starlette.routing import Mount, Route

    from mcp.server.fastmcp.server import StreamableHTTPASGIApp
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

    mcp_server = _build_mcp_server(tools)
    session_manager = StreamableHTTPSessionManager(
        app=mcp_server,
        stateless=True,
    )
    asgi_app = StreamableHTTPASGIApp(session_manager)

    app = Starlette(
        routes=[
            Mount("/mcp", routes=[
                Route("/", endpoint=asgi_app, methods=["GET", "POST", "DELETE"]),
            ]),
        ],
        lifespan=lambda _app: session_manager.run(),
    )
    uvicorn.run(app, host=host, port=port)


def main() -> None:
    """Entry point.

    Transport modes:
      stdio (default):
        alibabacloud-maxcompute-mcp-server --config /path/to/config.json

      Streamable HTTP:
        alibabacloud-maxcompute-mcp-server --transport http [--host 0.0.0.0] [--port 8000] --config /path/to/config.json
        alibabacloud-maxcompute-mcp-server --transport streamable-http [--host 0.0.0.0] [--port 8000] --config /path/to/config.json

    The Streamable HTTP server exposes a single endpoint:
      /mcp  — GET/POST/DELETE (MCP Streamable HTTP transport)
    """
    logging.basicConfig(
        level=logging.WARNING,
        stream=sys.stderr,
        format="%(levelname)s: %(name)s: %(message)s",
    )
    config_path, transport, host, port = _parse_args()
    tools = build_tools(config_path)

    if transport in ("http", "streamable-http"):
        _run_http(tools, host=host, port=port)
    else:
        asyncio.run(_run_stdio(tools))
