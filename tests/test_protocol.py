"""Protocol and server wiring tests."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from maxcompute_catalog_mcp.tools import Tools


def _send(proc: subprocess.Popen, obj: dict) -> dict:
    proc.stdin.write(json.dumps(obj, ensure_ascii=False) + "\n")
    proc.stdin.flush()
    line = proc.stdout.readline().strip()
    if not line:
        time.sleep(0.1)
        if proc.poll() is not None:
            err_tail = proc.stderr.read() if proc.stderr else ""
            raise RuntimeError(
                f"server exited with code {proc.returncode}\n[stderr]\n{err_tail.strip() or '<empty>'}"
            )
        raise RuntimeError("no response")
    return json.loads(line)


class TestToolsSpecs:
    """Unit tests: Tools.specs() returns all expected tools."""

    def test_specs_returns_all_tool_names(self, tools: Tools) -> None:
        specs = tools.specs()
        names = [s.name for s in specs]
        expected = [
            "list_projects",
            "get_project",
            "list_schemas",
            "get_schema",
            "list_tables",
            "get_table_schema",
            "get_partition_info",
            "cost_sql",
            "execute_sql",
            "get_instance_status",
            "get_instance",
            "search_meta_data",
            "check_access",
            "create_table",
            "insert_values",
            "update_table",
        ]
        for n in expected:
            assert n in names, f"missing tool: {n}"
        assert len(names) == len(expected)

    def test_each_spec_has_name_description_input_schema(self, tools: Tools) -> None:
        for s in tools.specs():
            assert s.name
            assert s.description
            assert "properties" in s.input_schema


_project_root = Path(__file__).resolve().parent.parent
_config_path = Path(os.environ.get("MAXCOMPUTE_CATALOG_CONFIG", _project_root / "config.json"))
_has_config = _config_path.exists()
_has_env = all(
    os.environ.get(k)
    for k in (
        "MAXCOMPUTE_CATALOG_API_ENDPOINT",
        "ALIBABA_CLOUD_ACCESS_KEY_ID",
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
    )
)


@pytest.mark.skipif(
    not _has_config and not _has_env,
    reason="no config file or env for server startup",
)
class TestProtocolViaSubprocess:
    """Integration: MCP server responds to initialize and tools/list."""

    def test_initialize_and_tools_list(self) -> None:
        env = dict(os.environ)
        cwd = Path(__file__).resolve().parent.parent
        # Prefer venv Python to ensure subprocess can find mcp module
        venv_python = cwd / ".venv" / "bin" / "python"
        python_exe = str(venv_python) if venv_python.exists() else sys.executable
        proc = subprocess.Popen(
            [python_exe, "-m", "maxcompute_catalog_mcp"],
            cwd=str(cwd),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        try:
            r1 = _send(proc, {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1.0"},
                },
            })
            assert "result" in r1
            assert r1["result"].get("protocolVersion") == "2024-11-05"

            r2 = _send(proc, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
            names = [t["name"] for t in r2["result"]["tools"]]
            assert "list_projects" in names
            assert "get_project" in names
            assert "list_tables" in names
        finally:
            proc.kill()
