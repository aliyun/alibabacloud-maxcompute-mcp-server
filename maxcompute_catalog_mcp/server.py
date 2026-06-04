from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import Optional

from .client_factory import build_client_set
from .config import load_configs
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
    """Build a Tools instance from one-or-many named configs.

    Loads all named configs (load_configs), builds the client set for the
    default config so the server is immediately usable, and hands the full
    registry to Tools so the user can switch configs at runtime via use_config.
    Backward compatible: a legacy single config surfaces as one config "default".
    """
    try:
        configs, default_name = load_configs(config_path)
    except ValueError as e:
        sys.exit(f"Invalid MaxCompute config: {e}")

    try:
        cs = build_client_set(configs[default_name])
    except ValueError as e:
        # credential failure (get_credentials_client)
        sys.exit(
            f"Failed to initialize credentials for config {default_name!r}: {e}\n"
            "Hint: provide credentials via one of the following methods:\n"
            "  1. Set access_key_id / access_key_secret in config.json\n"
            "  2. Set ALIBABA_CLOUD_ACCESS_KEY_ID and ALIBABA_CLOUD_ACCESS_KEY_SECRET env vars\n"
            "  3. Set ALIBABA_CLOUD_CREDENTIALS_URI to a credentials endpoint (for STS auto-refresh)\n"
            "  4. Run on an ECS instance with a RAM role attached\n"
            "Ensure 'alibabacloud-credentials' is installed: pip install alibabacloud-credentials"
        )
    except RuntimeError as e:
        # endpoint resolution / Catalog SDK initialization failure
        sys.exit(f"Failed to initialize config {default_name!r}: {e}")
    except Exception as e:  # network/import/other — fail with a clear message, not a raw traceback
        sys.exit(f"Failed to initialize config {default_name!r}: {type(e).__name__}: {e}")

    return Tools(
        sdk=cs.sdk,
        default_project=cs.default_project,
        namespace_id=cs.namespace_id,
        maxcompute_client=cs.maxcompute_client,
        credential_client=cs.credential_client,
        configs=configs,
        default_name=default_name,
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
