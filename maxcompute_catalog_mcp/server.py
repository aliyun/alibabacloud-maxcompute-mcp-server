from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mcp.server import Server as McpServer

from .client_factory import build_catalog_client_set, build_client_set
from .config import load_configs
from .mcp_protocol import JsonRpcError
from .remote_auth import (
    CatalogAccessTokenProvider,
    CatalogMCPAccessTokenClient,
)
from .runtime_config import (
    RemoteRuntimeConfig,
    RuntimeConfig,
    RuntimeMode,
    load_runtime_config,
)

if TYPE_CHECKING:
    from mcp.server.context import ServerRequestContext

    from .tools import Tools

_KNOWN_TRANSPORTS = ("stdio", "http", "streamable-http")
_LOGGER = logging.getLogger(__name__)


class RemoteInitializationError(RuntimeError):
    """Remote token issuance or MCP initialize failed before selection."""


class LocalDependenciesMissingError(RuntimeError):
    """The optional local SDK dependency set is not installed."""


@dataclass(frozen=True)
class ServerOptions:
    """Command-line startup options before config precedence is applied."""

    config_path: str | None
    transport: str
    host: str
    port: int
    mode: str | None
    remote_url: str | None
    profile: str | None


def _parse_server_options() -> ServerOptions:
    """Parse all process-level CLI options."""

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
    parser.add_argument(
        "--host", default="127.0.0.1", help="HTTP server host (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", type=int, default=8000, help="HTTP server port (default: 8000)"
    )
    parser.add_argument(
        "--mode",
        choices=(
            RuntimeMode.DEFAULT.value,
            RuntimeMode.REMOTE.value,
            RuntimeMode.LOCAL.value,
        ),
        default=None,
        help=(
            "Runtime mode; default prefers authenticated remote MCP "
            "then falls back locally"
        ),
    )
    parser.add_argument(
        "--remote-url",
        default=None,
        help="Remote MCP Streamable HTTP URL used in remote mode",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Named MaxCompute config selected at startup",
    )
    args, _ = parser.parse_known_args()
    return ServerOptions(
        config_path=args.config,
        transport=args.transport,
        host=args.host,
        port=args.port,
        mode=args.mode,
        remote_url=args.remote_url,
        profile=args.profile,
    )


def _parse_args() -> tuple[str | None, str, str, int]:
    """Parse the legacy transport tuple used by existing callers and tests."""

    options = _parse_server_options()
    return options.config_path, options.transport, options.host, options.port


def build_tools(
    config_path: str | None = None,
    profile: str | None = None,
) -> Tools:
    """Build a Tools instance from one-or-many named configs.

    Loads all named configs (load_configs), builds the client set for the
    default config so the server is immediately usable, and hands the full
    registry to Tools so the user can switch configs at runtime via use_config.
    Backward compatible: a legacy single config surfaces as one config "default".
    """
    try:
        tools_type = _load_local_tools_type()
        configs, default_name = load_configs(config_path)
    except LocalDependenciesMissingError as error:
        sys.exit(str(error))
    except (TypeError, ValueError) as e:
        sys.exit(f"Invalid MaxCompute config: {e}")

    if profile:
        if profile not in configs:
            sys.exit(f"Unknown MaxCompute profile: {profile!r}")
        default_name = profile

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
    except Exception as e:  # noqa: BLE001 -- startup must normalize SDK failures.
        sys.exit(
            f"Failed to initialize config {default_name!r}: {type(e).__name__}: {e}"
        )

    return tools_type(
        sdk=cs.sdk,
        default_project=cs.default_project,
        namespace_id=cs.namespace_id,
        maxcompute_client=cs.maxcompute_client,
        credential_client=cs.credential_client,
        configs=configs,
        default_name=default_name,
    )


def _load_local_tools_type() -> type[Tools]:
    """Load the SDK-backed tool implementation only when local mode is selected."""

    try:
        from .tools import Tools as LocalTools
    except ModuleNotFoundError as error:
        missing = error.name or ""
        if missing in {"odps", "pyarrow"} or missing.startswith(("odps.", "pyarrow.")):
            raise LocalDependenciesMissingError(
                "Local MCP mode requires optional SDK dependencies. Install them "
                "with: pip install 'alibabacloud-maxcompute-mcp-server[local]'"
            ) from None
        raise
    return LocalTools


def build_remote_token_provider(
    config_path: str | None,
    profile: str,
) -> CatalogAccessTokenProvider:
    """Build only the credential and Catalog clients needed by remote mode."""

    try:
        configs, default_name = load_configs(config_path)
        selected_name = profile or default_name
        if selected_name not in configs:
            raise ValueError(f"unknown MaxCompute profile: {selected_name!r}")
        client_set = build_catalog_client_set(configs[selected_name])
    except ValueError:
        raise RuntimeError(
            "Failed to initialize credentials for remote MCP token issuance. "
            "Check the selected MaxCompute profile and Alibaba Cloud credential provider."
        ) from None
    except Exception:  # noqa: BLE001 -- credential SDK exceptions are not stable.
        raise RuntimeError(
            "Failed to initialize the CatalogAPI client for remote MCP token issuance."
        ) from None
    return CatalogAccessTokenProvider(
        client=CatalogMCPAccessTokenClient(client_set.sdk.client),
    )


def _build_mcp_server(tools: Tools) -> McpServer:
    """Build an mcp.server.Server wired to the given Tools instance."""
    from mcp import types as mcp_types

    async def list_tools(
        _context: ServerRequestContext[Any],
        _params: mcp_types.PaginatedRequestParams | None,
    ) -> mcp_types.ListToolsResult:
        return mcp_types.ListToolsResult(
            tools=[
                mcp_types.Tool(
                    name=spec.name,
                    description=spec.description,
                    input_schema=spec.input_schema,
                )
                for spec in tools.specs()
            ]
        )

    async def call_tool(
        _context: ServerRequestContext[Any],
        params: mcp_types.CallToolRequestParams,
    ) -> mcp_types.CallToolResult:
        try:
            result = tools.call(params.name, params.arguments or {})
            content = result.get("content", [])
            typed_content: list[mcp_types.ContentBlock] = [
                mcp_types.TextContent(type="text", text=c["text"])
                for c in content
                if c.get("type") == "text"
            ]
            return mcp_types.CallToolResult(
                content=typed_content,
                structured_content=result.get("structuredContent"),
                is_error=bool(result.get("isError", False)),
            )
        except JsonRpcError as error:
            return mcp_types.CallToolResult(
                content=[
                    mcp_types.TextContent(
                        type="text",
                        text=f"{error.message}: {error.data}",
                    )
                ],
                structured_content=None,
                is_error=True,
            )

    return McpServer(
        "maxcompute-catalog-server-python",
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )


async def _run_stdio(tools: Tools) -> None:
    from mcp.server.stdio import stdio_server

    mcp_server = _build_mcp_server(tools)
    async with stdio_server() as (read_stream, write_stream):
        await mcp_server.run(
            read_stream,
            write_stream,
            mcp_server.create_initialization_options(),
        )


async def _run_remote_proxy(
    config: RemoteRuntimeConfig,
    token_provider: CatalogAccessTokenProvider,
) -> None:
    """Run the transparent stdio-to-remote-MCP adapter."""

    from .remote_proxy import run_remote_proxy

    await run_remote_proxy(config, token_provider)


async def _probe_remote_mcp(
    config: RemoteRuntimeConfig,
    token_provider: CatalogAccessTokenProvider,
) -> None:
    """Verify Catalog token issuance and an authenticated MCP initialize."""

    from .remote_proxy import probe_remote_mcp

    await probe_remote_mcp(config, token_provider)


async def _initialize_remote_mcp(
    config: RemoteRuntimeConfig,
    token_provider: CatalogAccessTokenProvider,
) -> None:
    """Issue a Catalog token and authenticate one regional MCP endpoint."""

    try:
        await token_provider.get_access_token()
    except Exception:  # noqa: BLE001 -- normalize SDK and transport failures.
        raise RemoteInitializationError(
            "Remote MCP token issuance failed during initialization"
        ) from None
    try:
        await _probe_remote_mcp(config, token_provider)
    except Exception:  # noqa: BLE001 -- normalize remote transport failures.
        raise RemoteInitializationError("Remote MCP initialization failed") from None


async def _run_forced_remote_stdio(
    config: RemoteRuntimeConfig,
    token_provider: CatalogAccessTokenProvider,
) -> None:
    """Initialize and run remote mode without a local fallback."""

    await _initialize_remote_mcp(config, token_provider)
    await _run_remote_proxy(config, token_provider)


async def _run_default_stdio(
    runtime_config: RuntimeConfig,
    config_path: str | None,
) -> None:
    """Initialize remote once at startup, otherwise start original local stdio."""

    remote = runtime_config.remote
    if remote is not None:
        try:
            token_provider = build_remote_token_provider(
                config_path,
                runtime_config.profile,
            )
            await _initialize_remote_mcp(remote, token_provider)
        except Exception as error:  # noqa: BLE001 -- default mode must fall back.
            _LOGGER.warning(
                "Remote MCP initialization failed; falling back to local mode (%s)",
                type(error).__name__,
            )
        else:
            await _run_remote_proxy(remote, token_provider)
            return

    tools = build_tools(config_path, profile=runtime_config.profile)
    await _run_stdio(tools)


def _run_http(tools: Tools, host: str, port: int) -> None:
    """Start a Streamable HTTP server on host:port.

    Endpoint:
      /mcp  — Streamable HTTP (GET/POST/DELETE)
    """
    import uvicorn

    mcp_server = _build_mcp_server(tools)
    app = mcp_server.streamable_http_app(
        streamable_http_path="/mcp",
        stateless_http=True,
        host=host,
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
    options = _parse_server_options()
    try:
        runtime_config = load_runtime_config(
            options.config_path,
            mode=options.mode,
            remote_url=options.remote_url,
            profile=options.profile,
            allow_remote=options.transport == "stdio",
        )
    except (TypeError, ValueError) as error:
        sys.exit(f"Invalid MCP runtime config: {error}")

    if runtime_config.mode is RuntimeMode.REMOTE:
        if options.transport != "stdio":
            sys.exit("Remote MCP proxy mode supports only stdio transport")
        if runtime_config.remote is None:
            sys.exit("Remote MCP proxy configuration is missing")
        try:
            token_provider = build_remote_token_provider(
                options.config_path,
                runtime_config.profile,
            )
        except RuntimeError:
            sys.exit("Remote MCP token provider initialization failed")
        try:
            asyncio.run(
                _run_forced_remote_stdio(
                    runtime_config.remote,
                    token_provider,
                )
            )
        except RemoteInitializationError as error:
            sys.exit(str(error))
        return

    if runtime_config.mode is RuntimeMode.DEFAULT and runtime_config.remote is not None:
        asyncio.run(_run_default_stdio(runtime_config, options.config_path))
        return

    tools = build_tools(options.config_path, profile=runtime_config.profile)

    if options.transport in ("http", "streamable-http"):
        _run_http(tools, host=options.host, port=options.port)
    else:
        asyncio.run(_run_stdio(tools))
