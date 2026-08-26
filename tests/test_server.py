"""Unit tests for server.py — _parse_args(), build_tools(), _build_mcp_server()."""
from __future__ import annotations

import asyncio
import inspect
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from maxcompute_catalog_mcp.config import MaxComputeCatalogConfig
from maxcompute_catalog_mcp.runtime_config import (
    RemoteRuntimeConfig,
    RuntimeConfig,
    RuntimeMode,
)
from maxcompute_catalog_mcp.server import (
    RemoteInitializationError,
    _build_mcp_server,
    _parse_args,
    _parse_server_options,
    _run_default_stdio,
    _run_forced_remote_stdio,
    _run_http,
    _run_stdio,
    build_remote_token_provider,
    build_tools,
    main,
)
from maxcompute_catalog_mcp.tools import Tools
from maxcompute_catalog_mcp.tools_common import ToolSpec

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

    def test_remote_runtime_cli_options(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "alibabacloud-maxcompute-mcp-server",
                "--mode",
                "remote",
                "--remote-url",
                "https://remote.example.com/mcp",
                "--profile",
                "local-dev",
            ],
        )

        options = _parse_server_options()

        assert options.mode == "remote"
        assert options.remote_url == "https://remote.example.com/mcp"
        assert options.profile == "local-dev"

    @pytest.mark.parametrize("mode", ["default", "remote", "local"])
    def test_exact_runtime_modes(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mode: str,
    ) -> None:
        monkeypatch.setattr(
            sys,
            "argv",
            ["alibabacloud-maxcompute-mcp-server", "--mode", mode],
        )

        assert _parse_server_options().mode == mode

    def test_obsolete_auto_runtime_mode_is_rejected(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            sys,
            "argv",
            ["alibabacloud-maxcompute-mcp-server", "--mode", "auto"],
        )

        with pytest.raises(SystemExit):
            _parse_server_options()


def test_main_remote_mode_builds_token_provider_but_not_local_tools(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remote startup builds Catalog auth but never registers legacy tools."""
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "mode": "remote",
                "remote": {"url": "http://127.0.0.1:8080/mcp"},
                "maxcompute": {
                    "maxcompute_endpoint": (
                        "https://service.cn-hangzhou.maxcompute.aliyun.com/api"
                    ),
                    "catalogapi_endpoint": "https://catalog.example.com",
                    "accessKeyId": "fixture-ak",
                    "accessKeySecret": "fixture-sk",
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "alibabacloud-maxcompute-mcp-server",
            "--config",
            str(config_path),
        ],
    )

    token_provider = MagicMock()
    with patch("maxcompute_catalog_mcp.server.build_tools") as build_tools_mock, \
         patch(
             "maxcompute_catalog_mcp.server.build_remote_token_provider",
             return_value=token_provider,
         ) as build_provider_mock, \
         patch(
             "maxcompute_catalog_mcp.server._run_remote_proxy",
             new_callable=AsyncMock,
         ) as run_remote_mock, \
         patch(
             "maxcompute_catalog_mcp.server._probe_remote_mcp",
             new_callable=AsyncMock,
         ) as probe_remote_mock:
        main()

    build_tools_mock.assert_not_called()
    build_provider_mock.assert_called_once_with(str(config_path), "default")
    probe_remote_mock.assert_awaited_once()
    run_remote_mock.assert_awaited_once()
    assert run_remote_mock.await_args.args[1] is token_provider


class TestDefaultMode:
    @staticmethod
    def _runtime() -> RuntimeConfig:
        return RuntimeConfig(
            mode=RuntimeMode.DEFAULT,
            profile="daily",
            remote=RemoteRuntimeConfig(
                url="https://mcp.cn-hangzhou.maxcompute.aliyun.com/mcp",
            ),
        )

    def test_successful_remote_initialize_selects_transparent_proxy(self) -> None:
        provider = MagicMock()
        with patch(
            "maxcompute_catalog_mcp.server.build_remote_token_provider",
            return_value=provider,
        ) as build_provider_mock, patch(
            "maxcompute_catalog_mcp.server._probe_remote_mcp",
            new_callable=AsyncMock,
        ) as probe_mock, patch(
            "maxcompute_catalog_mcp.server._run_remote_proxy",
            new_callable=AsyncMock,
        ) as remote_mock, patch(
            "maxcompute_catalog_mcp.server.build_tools",
        ) as build_tools_mock:
            asyncio.run(_run_default_stdio(self._runtime(), "/fake/config.json"))

        build_provider_mock.assert_called_once_with("/fake/config.json", "daily")
        probe_mock.assert_awaited_once_with(self._runtime().remote, provider)
        remote_mock.assert_awaited_once_with(self._runtime().remote, provider)
        build_tools_mock.assert_not_called()

    def test_catalog_token_failure_falls_back_to_original_local_server(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        local_tools = MagicMock()
        with patch(
            "maxcompute_catalog_mcp.server.build_remote_token_provider",
            side_effect=RuntimeError("token issuance failed"),
        ), patch(
            "maxcompute_catalog_mcp.server._probe_remote_mcp",
            new_callable=AsyncMock,
        ) as probe_mock, patch(
            "maxcompute_catalog_mcp.server.build_tools",
            return_value=local_tools,
        ) as build_tools_mock, patch(
            "maxcompute_catalog_mcp.server._run_stdio",
            new_callable=AsyncMock,
        ) as local_mock:
            asyncio.run(_run_default_stdio(self._runtime(), "/fake/config.json"))

        probe_mock.assert_not_awaited()
        build_tools_mock.assert_called_once_with(
            "/fake/config.json",
            profile="daily",
        )
        local_mock.assert_awaited_once_with(local_tools)
        assert "falling back to local mode" in caplog.text

    def test_remote_initialize_failure_falls_back_to_original_local_server(
        self,
    ) -> None:
        provider = MagicMock()
        local_tools = MagicMock()
        with patch(
            "maxcompute_catalog_mcp.server.build_remote_token_provider",
            return_value=provider,
        ), patch(
            "maxcompute_catalog_mcp.server._probe_remote_mcp",
            new_callable=AsyncMock,
            side_effect=RuntimeError("gateway authentication failed"),
        ), patch(
            "maxcompute_catalog_mcp.server._run_remote_proxy",
            new_callable=AsyncMock,
        ) as remote_mock, patch(
            "maxcompute_catalog_mcp.server.build_tools",
            return_value=local_tools,
        ), patch(
            "maxcompute_catalog_mcp.server._run_stdio",
            new_callable=AsyncMock,
        ) as local_mock:
            asyncio.run(_run_default_stdio(self._runtime(), None))

        remote_mock.assert_not_awaited()
        local_mock.assert_awaited_once_with(local_tools)

    def test_remote_runtime_failure_after_selection_does_not_fall_back(
        self,
    ) -> None:
        provider = MagicMock()
        with patch(
            "maxcompute_catalog_mcp.server.build_remote_token_provider",
            return_value=provider,
        ), patch(
            "maxcompute_catalog_mcp.server._probe_remote_mcp",
            new_callable=AsyncMock,
        ), patch(
            "maxcompute_catalog_mcp.server._run_remote_proxy",
            new_callable=AsyncMock,
            side_effect=RuntimeError("remote session lost"),
        ), patch(
            "maxcompute_catalog_mcp.server.build_tools",
        ) as build_tools_mock, pytest.raises(
            RuntimeError,
            match="remote session lost",
        ):
            asyncio.run(_run_default_stdio(self._runtime(), None))

        build_tools_mock.assert_not_called()


def test_forced_remote_initialize_failure_is_fail_closed() -> None:
    config = RemoteRuntimeConfig(url="https://gateway.example.com/mcp")
    provider = MagicMock()
    with patch(
        "maxcompute_catalog_mcp.server._probe_remote_mcp",
        new_callable=AsyncMock,
        side_effect=RuntimeError("unauthorized"),
    ), patch(
        "maxcompute_catalog_mcp.server._run_remote_proxy",
        new_callable=AsyncMock,
    ) as remote_mock, pytest.raises(
        RemoteInitializationError,
        match="failed in remote mode",
    ):
        asyncio.run(_run_forced_remote_stdio(config, provider))

    remote_mock.assert_not_awaited()


@patch("maxcompute_catalog_mcp.server.build_client_set")
@patch("maxcompute_catalog_mcp.server.load_configs")
def test_build_remote_token_provider_reuses_selected_catalog_sdk(
    mock_load,
    mock_build,
) -> None:
    config = MaxComputeCatalogConfig(
        catalogapi_endpoint="https://catalog.example.com",
        maxcompute_endpoint=(
            "https://service.cn-hangzhou.maxcompute.aliyun.com/api"
        ),
        access_key_id="fixture-ak",
        access_key_secret="fixture-sk",
    )
    catalog_client = MagicMock()
    client_set = MagicMock()
    client_set.sdk.client = catalog_client
    mock_load.return_value = ({"daily": config}, "daily")
    mock_build.return_value = client_set

    provider = build_remote_token_provider("/fake/config.json", "daily")

    assert provider._client._catalog_client is catalog_client
    mock_build.assert_called_once_with(config)


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
        base = {
            "catalogapi_endpoint": "https://catalog.example.com",
            "maxcompute_endpoint": "https://mc.example.com",
            "access_key_id": "AK",
            "access_key_secret": "SK",
            "default_project": "proj",
            "namespace_id": "ns",
        }
        base.update(kw)
        return MaxComputeCatalogConfig(**base)

    def _client_set(self, **kw):
        from maxcompute_catalog_mcp.client_factory import ClientSet

        base = {
            "sdk": MagicMock(),
            "maxcompute_client": MagicMock(),
            "credential_client": MagicMock(),
            "default_project": "proj",
            "namespace_id": "ns",
        }
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
        assert isinstance(tools, Tools)
        names = {spec.name for spec in tools.specs()}
        assert "execute_sql" in names
        assert "list_configs" in names

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

    @patch("maxcompute_catalog_mcp.server.build_client_set")
    @patch("maxcompute_catalog_mcp.server.load_configs")
    def test_profile_selects_named_config_at_startup(self, mock_load, mock_build) -> None:
        cfg_a = self._cfg(maxcompute_endpoint="https://a.example.com")
        cfg_b = self._cfg(maxcompute_endpoint="https://b.example.com")
        mock_load.return_value = ({"a": cfg_a, "b": cfg_b}, "a")
        mock_build.return_value = self._client_set()

        tools = build_tools(profile="b")

        assert mock_build.call_args.args[0] is cfg_b
        assert tools._default_name == "b"
        assert tools._current_name == "b"

    @patch("maxcompute_catalog_mcp.server.load_configs")
    def test_unknown_profile_exits_before_client_construction(self, mock_load) -> None:
        mock_load.return_value = ({"a": self._cfg()}, "a")

        with pytest.raises(SystemExit, match="Unknown MaxCompute profile"):
            build_tools(profile="missing")

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

        spec1 = ToolSpec("tool_one", "desc1", {"type": "object"})
        spec2 = ToolSpec("tool_two", "desc2", {"type": "object"})

        mock_tools = MagicMock()
        mock_tools.specs.return_value = [spec1, spec2]

        server = _build_mcp_server(mock_tools)
        entry = server.get_request_handler("tools/list")
        assert entry is not None

        result = asyncio.run(entry.handler(MagicMock(), None))
        names = [tool.name for tool in result.tools]
        assert names == ["tool_one", "tool_two"]
        mock_tools.specs.assert_called_once()

    def test_call_tool_handler_returns_text_content(self) -> None:
        """CallToolRequest handler must call tools.call and return TextContent list."""
        from mcp import types as mcp_types

        spec = ToolSpec("echo", "d", {"type": "object"})
        mock_tools = MagicMock()
        mock_tools.specs.return_value = [spec]
        mock_tools.call.return_value = {
            "content": [{"type": "text", "text": "hello"}],
        }

        server = _build_mcp_server(mock_tools)
        entry = server.get_request_handler("tools/call")
        assert entry is not None

        result = asyncio.run(
            entry.handler(
                MagicMock(),
                mcp_types.CallToolRequestParams(
                    name="echo",
                    arguments={"x": 1},
                ),
            )
        )
        mock_tools.call.assert_called_once_with("echo", {"x": 1})
        texts = [content.text for content in result.content if content.type == "text"]
        assert texts == ["hello"]

    def test_call_tool_handler_jsonrpc_error_propagates(self) -> None:
        """JsonRpcError from tools.call is converted to ValueError (ToolError)."""
        from mcp import types as mcp_types

        from maxcompute_catalog_mcp.mcp_protocol import JsonRpcError

        spec = ToolSpec("boom", "d", {"type": "object"})
        mock_tools = MagicMock()
        mock_tools.specs.return_value = [spec]
        mock_tools.call.side_effect = JsonRpcError(
            code=-32000, message="bad input", data={"field": "x"},
        )

        server = _build_mcp_server(mock_tools)

        call_entry = server.get_request_handler("tools/call")
        assert call_entry is not None
        call_result = asyncio.run(
            call_entry.handler(
                MagicMock(),
                mcp_types.CallToolRequestParams(name="boom", arguments={}),
            )
        )
        assert call_result.is_error is True
        joined = " ".join(
            content.text
            for content in call_result.content
            if content.type == "text"
        )
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
          - MCP 2.x app must be stateless (critical for correctness)
          - Route is mounted at /mcp (public contract)
          - uvicorn.run receives host/port via kwargs
        """
        mock_tools = MagicMock()

        mock_uvicorn = MagicMock()

        fake_http_mods = {
            "uvicorn": mock_uvicorn,
        }

        with patch("maxcompute_catalog_mcp.server._build_mcp_server") as mock_build, \
             patch.dict("sys.modules", fake_http_mods):
            mock_server = MagicMock()
            mock_build.return_value = mock_server

            _run_http(mock_tools, host="0.0.0.0", port=9999)

            mock_build.assert_called_once_with(mock_tools)
            mock_server.streamable_http_app.assert_called_once_with(
                streamable_http_path="/mcp",
                stateless_http=True,
                host="0.0.0.0",
            )

            # uvicorn.run called with exact host/port via kwargs
            mock_uvicorn.run.assert_called_once_with(
                mock_server.streamable_http_app.return_value,
                host="0.0.0.0",
                port=9999,
            )

    def test_run_http_serves_modern_discovery_at_exact_mcp_path(self) -> None:
        """The MCP 2.x ASGI wiring handles a real modern request at /mcp."""
        from starlette.testclient import TestClient

        mock_tools = MagicMock()
        mock_tools.specs.return_value = []

        with patch("uvicorn.run") as run_mock:
            _run_http(mock_tools, host="127.0.0.1", port=8000)

        app = run_mock.call_args.args[0]
        with TestClient(app, base_url="http://127.0.0.1:8000") as client:
            response = client.post(
                "/mcp",
                headers={
                    "Accept": "application/json, text/event-stream",
                    "Content-Type": "application/json",
                    "MCP-Protocol-Version": "2026-07-28",
                    "Mcp-Method": "server/discover",
                },
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "server/discover",
                    "params": {
                        "_meta": {
                            "io.modelcontextprotocol/protocolVersion": (
                                "2026-07-28"
                            ),
                            "io.modelcontextprotocol/clientCapabilities": {},
                            "io.modelcontextprotocol/clientInfo": {
                                "name": "local-http-test",
                                "version": "1.0.0",
                            },
                        }
                    },
                },
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["jsonrpc"] == "2.0"
        assert payload["id"] == 1
        assert "2026-07-28" in payload["result"]["supportedVersions"]


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
