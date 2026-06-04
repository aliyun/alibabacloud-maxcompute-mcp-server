"""Unit tests for server.py — _parse_args(), build_tools(), _build_mcp_server()."""
from __future__ import annotations

import asyncio
import inspect
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from maxcompute_catalog_mcp.config import MaxComputeCatalogConfig
from maxcompute_catalog_mcp.server import (
    _build_mcp_server,
    _parse_args,
    _run_http,
    _run_stdio,
    build_tools,
    main,
)


# ---------------------------------------------------------------------------
# _parse_args() tests
# ---------------------------------------------------------------------------

class TestParseArgs:
    def test_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys, "argv", ["alibabacloud-maxcompute-mcp-server"])
        config_path, transport, host, port = _parse_args()
        assert config_path is None
        assert transport == "stdio"
        assert host == "127.0.0.1"
        assert port == 8000

    def test_custom(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys, "argv", [
            "alibabacloud-maxcompute-mcp-server",
            "--config", "/tmp/c.json",
            "--transport", "http",
            "--host", "0.0.0.0",
            "--port", "9000",
        ])
        config_path, transport, host, port = _parse_args()
        assert config_path is not None and config_path.endswith("c.json")
        assert transport == "http"
        assert host == "0.0.0.0"
        assert port == 9000

    def test_streamable_http(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys, "argv", [
            "alibabacloud-maxcompute-mcp-server", "--transport", "streamable-http",
        ])
        _, transport, _, _ = _parse_args()
        assert transport == "streamable-http"


# ---------------------------------------------------------------------------
# build_tools() tests
# ---------------------------------------------------------------------------

class TestBuildTools:
    """build_tools() now orchestrates load_configs() + build_client_set().

    Detailed credential/endpoint/SDK behaviour lives in client_factory and is
    tested in test_client_factory.py; here we test the orchestration + the two
    sys.exit branches + that the named-config registry is handed to Tools.
    """

    def _cfg(self, **kw):
        base = dict(
            catalogapi_endpoint="https://catalog.example.com",
            maxcompute_endpoint="https://mc.example.com",
            access_key_id="AK", access_key_secret="SK",
            default_project="proj", namespace_id="ns",
        )
        base.update(kw)
        return MaxComputeCatalogConfig(**base)

    def _client_set(self, **kw):
        from maxcompute_catalog_mcp.client_factory import ClientSet
        base = dict(
            sdk=MagicMock(), maxcompute_client=MagicMock(), credential_client=MagicMock(),
            default_project="proj", namespace_id="ns",
        )
        base.update(kw)
        return ClientSet(**base)

    @patch("maxcompute_catalog_mcp.server.build_client_set")
    @patch("maxcompute_catalog_mcp.server.load_configs")
    def test_success(self, mock_load, mock_build) -> None:
        mock_load.return_value = ({"default": self._cfg()}, "default")
        mock_build.return_value = self._client_set()

        tools = build_tools("/fake/config.json")
        assert tools is not None
        assert tools.default_project == "proj"
        assert tools.namespace_id == "ns"
        mock_load.assert_called_once_with("/fake/config.json")
        mock_build.assert_called_once()
        # registry handed to Tools
        assert tools._default_name == "default"
        assert tools._current_name == "default"
        assert "default" in tools._configs

    @patch("maxcompute_catalog_mcp.server.build_client_set")
    @patch("maxcompute_catalog_mcp.server.load_configs")
    def test_credential_failure_exits(self, mock_load, mock_build) -> None:
        mock_load.return_value = ({"default": self._cfg(access_key_id="", access_key_secret="")}, "default")
        mock_build.side_effect = ValueError("no credentials")

        with pytest.raises(SystemExit) as exc_info:
            build_tools("/fake/config.json")
        assert "Failed to initialize credentials" in str(exc_info.value.code)

    @patch("maxcompute_catalog_mcp.server.build_client_set")
    @patch("maxcompute_catalog_mcp.server.load_configs")
    def test_runtime_failure_exits(self, mock_load, mock_build) -> None:
        """Endpoint resolution / SDK init failure (RuntimeError) → sys.exit."""
        mock_load.return_value = ({"default": self._cfg()}, "default")
        mock_build.side_effect = RuntimeError("resolve/sdk failed")

        with pytest.raises(SystemExit) as exc_info:
            build_tools()
        assert "Failed to initialize config" in str(exc_info.value.code)
        assert "resolve/sdk failed" in str(exc_info.value.code)

    @patch("maxcompute_catalog_mcp.server.build_client_set")
    @patch("maxcompute_catalog_mcp.server.load_configs")
    def test_builds_only_default_config(self, mock_load, mock_build) -> None:
        """With multiple configs, build_tools builds the client set for the default only."""
        cfg_a = self._cfg(maxcompute_endpoint="https://a.example.com", default_project="pa")
        cfg_b = self._cfg(maxcompute_endpoint="https://b.example.com", default_project="pb")
        mock_load.return_value = ({"a": cfg_a, "b": cfg_b}, "b")
        mock_build.return_value = self._client_set(default_project="pb")

        tools = build_tools()
        # default is "b" → build_client_set called with cfg_b
        assert mock_build.call_args.args[0] is cfg_b
        assert tools._default_name == "b" and tools._current_name == "b"
        assert set(tools._configs) == {"a", "b"}

    @patch("maxcompute_catalog_mcp.server.load_configs")
    def test_invalid_config_exits(self, mock_load) -> None:
        mock_load.side_effect = ValueError("default config 'x' not found")
        with pytest.raises(SystemExit) as exc_info:
            build_tools()
        assert "Invalid MaxCompute config" in str(exc_info.value.code)

    @patch("maxcompute_catalog_mcp.server.build_client_set")
    @patch("maxcompute_catalog_mcp.server.load_configs")
    def test_unexpected_error_exits(self, mock_load, mock_build) -> None:
        mock_load.return_value = ({"default": self._cfg()}, "default")
        mock_build.side_effect = ConnectionError("network down")
        with pytest.raises(SystemExit) as exc_info:
            build_tools()
        assert "ConnectionError" in str(exc_info.value.code)


# ---------------------------------------------------------------------------
# _build_mcp_server() tests — actually invoke the registered handlers
# ---------------------------------------------------------------------------

class TestBuildMcpServer:
    def test_list_tools_handler_invokes_tools_specs(self) -> None:
        """The registered ListToolsRequest handler must delegate to tools.specs()."""
        from mcp import types as mcp_types

        spec1 = MagicMock(description="desc1", input_schema={"type": "object"})
        spec1.name = "tool_one"
        spec2 = MagicMock(description="desc2", input_schema={"type": "object"})
        spec2.name = "tool_two"

        mock_tools = MagicMock()
        mock_tools.specs.return_value = [spec1, spec2]

        server = _build_mcp_server(mock_tools)
        assert mcp_types.ListToolsRequest in server.request_handlers

        handler = server.request_handlers[mcp_types.ListToolsRequest]
        req = mcp_types.ListToolsRequest(method="tools/list")
        result = asyncio.run(handler(req))
        # result is ServerResult wrapping ListToolsResult
        tools_result = result.root
        names = [t.name for t in tools_result.tools]
        assert names == ["tool_one", "tool_two"]
        mock_tools.specs.assert_called_once()

    def test_call_tool_handler_returns_text_content(self) -> None:
        """CallToolRequest handler must call tools.call and return TextContent list."""
        from mcp import types as mcp_types

        spec = MagicMock(description="d", input_schema={"type": "object"})
        spec.name = "echo"
        mock_tools = MagicMock()
        mock_tools.specs.return_value = [spec]
        mock_tools.call.return_value = {
            "content": [{"type": "text", "text": "hello"}],
        }

        server = _build_mcp_server(mock_tools)
        assert mcp_types.CallToolRequest in server.request_handlers

        # Prime the tool cache by calling list_tools first (SDK validates against it)
        list_handler = server.request_handlers[mcp_types.ListToolsRequest]
        asyncio.run(list_handler(mcp_types.ListToolsRequest(method="tools/list")))

        call_handler = server.request_handlers[mcp_types.CallToolRequest]
        req = mcp_types.CallToolRequest(
            method="tools/call",
            params=mcp_types.CallToolRequestParams(name="echo", arguments={"x": 1}),
        )
        result = asyncio.run(call_handler(req))
        call_result = result.root
        mock_tools.call.assert_called_once_with("echo", {"x": 1})
        texts = [c.text for c in call_result.content if c.type == "text"]
        assert texts == ["hello"]

    def test_call_tool_handler_jsonrpc_error_propagates(self) -> None:
        """JsonRpcError from tools.call is converted to ValueError (ToolError)."""
        from mcp import types as mcp_types
        from maxcompute_catalog_mcp.mcp_protocol import JsonRpcError

        spec = MagicMock(description="d", input_schema={"type": "object"})
        spec.name = "boom"
        mock_tools = MagicMock()
        mock_tools.specs.return_value = [spec]
        mock_tools.call.side_effect = JsonRpcError(
            code=-32000, message="bad input", data={"field": "x"},
        )

        server = _build_mcp_server(mock_tools)

        # Prime the tool cache
        list_handler = server.request_handlers[mcp_types.ListToolsRequest]
        asyncio.run(list_handler(mcp_types.ListToolsRequest(method="tools/list")))

        call_handler = server.request_handlers[mcp_types.CallToolRequest]
        req = mcp_types.CallToolRequest(
            method="tools/call",
            params=mcp_types.CallToolRequestParams(name="boom", arguments={}),
        )
        # SDK wraps ValueError from user handler into a CallToolResult with isError=True
        result = asyncio.run(call_handler(req))
        call_result = result.root
        assert call_result.isError is True
        # Error text should contain the JsonRpcError message
        joined = " ".join(c.text for c in call_result.content if c.type == "text")
        assert "bad input" in joined


# ---------------------------------------------------------------------------
# _run_stdio() tests
# ---------------------------------------------------------------------------

class TestRunStdio:
    def test_run_stdio_calls_server_run(self) -> None:
        """_run_stdio builds MCP server and calls server.run with stdio streams."""
        from contextlib import asynccontextmanager

        mock_read = MagicMock()
        mock_write = MagicMock()

        @asynccontextmanager
        async def fake_stdio_server(*_a, **_kw):
            yield mock_read, mock_write

        mock_tools = MagicMock()
        with patch("maxcompute_catalog_mcp.server._build_mcp_server") as mock_build, \
             patch.dict("sys.modules", {"mcp.server.stdio": MagicMock(stdio_server=fake_stdio_server)}):

            mock_server = MagicMock()
            mock_server.run = AsyncMock()
            mock_server.create_initialization_options.return_value = {"init": True}
            mock_build.return_value = mock_server

            asyncio.run(_run_stdio(mock_tools))

            mock_build.assert_called_once_with(mock_tools)
            mock_server.run.assert_called_once_with(
                mock_read, mock_write, {"init": True},
            )


# ---------------------------------------------------------------------------
# _run_http() tests
# ---------------------------------------------------------------------------

class TestRunHttp:
    def test_run_http_starts_uvicorn(self) -> None:
        """_run_http builds MCP server, creates ASGI app, and calls uvicorn.run.

        Asserts on the full wiring contract:
          - SessionManager must be stateless=True (critical for correctness)
          - Route is mounted at /mcp (public contract)
          - uvicorn.run receives host/port via kwargs
        """
        mock_tools = MagicMock()

        mock_uvicorn = MagicMock()
        mock_sm_cls = MagicMock()
        mock_asgi_cls = MagicMock()
        mock_starlette_cls = MagicMock()
        mock_mount = MagicMock()
        mock_route = MagicMock()

        fake_http_mods = {
            "uvicorn": mock_uvicorn,
            "starlette.applications": MagicMock(Starlette=mock_starlette_cls),
            "starlette.routing": MagicMock(Mount=mock_mount, Route=mock_route),
            "mcp.server.fastmcp.server": MagicMock(StreamableHTTPASGIApp=mock_asgi_cls),
            "mcp.server.streamable_http_manager": MagicMock(StreamableHTTPSessionManager=mock_sm_cls),
        }

        with patch("maxcompute_catalog_mcp.server._build_mcp_server") as mock_build, \
             patch.dict("sys.modules", fake_http_mods):
            mock_build.return_value = MagicMock()

            _run_http(mock_tools, host="0.0.0.0", port=9999)

            mock_build.assert_called_once_with(mock_tools)

            # SessionManager MUST be stateless (otherwise concurrent requests interfere)
            assert mock_sm_cls.call_args.kwargs["stateless"] is True

            # Route mounted at /mcp with GET/POST/DELETE methods
            mount_call = mock_mount.call_args
            assert mount_call.args[0] == "/mcp"
            route_call = mock_route.call_args
            assert route_call.args[0] == "/"
            assert set(route_call.kwargs["methods"]) == {"GET", "POST", "DELETE"}

            # uvicorn.run called with exact host/port via kwargs
            mock_uvicorn.run.assert_called_once()
            call_args = mock_uvicorn.run.call_args
            assert call_args.kwargs["host"] == "0.0.0.0"
            assert call_args.kwargs["port"] == 9999


# ---------------------------------------------------------------------------
# main() tests
# ---------------------------------------------------------------------------

class TestMain:
    def test_main_stdio_transport(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() with default transport calls asyncio.run with the _run_stdio coroutine."""
        monkeypatch.setattr(sys, "argv", ["alibabacloud-maxcompute-mcp-server"])
        mock_tools = MagicMock()

        with patch("maxcompute_catalog_mcp.server.build_tools", return_value=mock_tools), \
             patch("maxcompute_catalog_mcp.server.asyncio") as mock_asyncio:
            main()
            mock_asyncio.run.assert_called_once()
            # Verify the actual coroutine passed to asyncio.run is from _run_stdio
            call_arg = mock_asyncio.run.call_args[0][0]
            assert inspect.iscoroutine(call_arg)
            assert call_arg.__qualname__ == "_run_stdio"
            # Close the coroutine to avoid "coroutine was never awaited" warning
            call_arg.close()

    def test_main_http_transport(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() with --transport http calls _run_http."""
        monkeypatch.setattr(sys, "argv", [
            "alibabacloud-maxcompute-mcp-server", "--transport", "http",
            "--host", "0.0.0.0", "--port", "9000",
        ])
        mock_tools = MagicMock()

        with patch("maxcompute_catalog_mcp.server.build_tools", return_value=mock_tools), \
             patch("maxcompute_catalog_mcp.server._run_http") as mock_run_http:
            main()
            mock_run_http.assert_called_once_with(mock_tools, host="0.0.0.0", port=9000)

    def test_main_streamable_http_transport(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() with --transport streamable-http calls _run_http."""
        monkeypatch.setattr(sys, "argv", [
            "alibabacloud-maxcompute-mcp-server", "--transport", "streamable-http",
        ])
        mock_tools = MagicMock()

        with patch("maxcompute_catalog_mcp.server.build_tools", return_value=mock_tools), \
             patch("maxcompute_catalog_mcp.server._run_http") as mock_run_http:
            main()
            mock_run_http.assert_called_once_with(mock_tools, host="127.0.0.1", port=8000)

    def test_main_configures_logging(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """main() configures logging before anything else."""
        import logging as real_logging

        # Use http transport + patch _run_http to avoid creating an un-awaited
        # _run_stdio coroutine (which would leak a RuntimeWarning).
        monkeypatch.setattr(sys, "argv", [
            "alibabacloud-maxcompute-mcp-server", "--transport", "http",
        ])
        mock_tools = MagicMock()

        with patch("maxcompute_catalog_mcp.server.build_tools", return_value=mock_tools), \
             patch("maxcompute_catalog_mcp.server._run_http"), \
             patch("maxcompute_catalog_mcp.server.logging.basicConfig") as mock_basic_config:
            main()
            mock_basic_config.assert_called_once()
            kwargs = mock_basic_config.call_args.kwargs
            assert kwargs.get("level") == real_logging.WARNING
