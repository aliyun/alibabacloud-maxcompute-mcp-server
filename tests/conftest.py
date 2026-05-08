"""Pytest fixtures: mock SDK/client and Tools instance for unit tests; real Tools when config exists."""
from __future__ import annotations

import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any, Callable
from unittest.mock import MagicMock

import pytest

from maxcompute_catalog_mcp.tools_common import (
    _UNSAFE_KW_PATTERNS,
    _normalize_sql,
)


class ReadOnlyViolation(RuntimeError):
    """Simulated ODPS server error when DML/DDL is submitted with odps.sql.read.only=true."""


def _make_readonly_guard(get_instance: Callable[[], Any]) -> Callable[..., Any]:
    """Create a run_sql side-effect that checks ReadOnly, calling *get_instance* on success.

    The returned callable mirrors the real ODPS server: if hints contain
    odps.sql.read.only=true and the SQL contains DML/DDL (outside string
    literals), it raises ReadOnlyViolation.  Otherwise it calls
    *get_instance()* so callers control what instance is returned.

    Args:
        get_instance: zero-arg callable returning the MagicMock instance.
            Use ``lambda: inst`` for a fixed instance or
            ``lambda: mc.run_sql.return_value`` for mock-configurable one.
    """
    def _run_sql(sql: str, *, hints: dict | None = None, **_kwargs: Any) -> Any:
        if hints and hints.get("odps.sql.read.only") == "true":
            # _normalize_sql already strips string literals, so scanning
            # the normalized text is safe against DML keywords embedded
            # inside string values.
            normalized = _normalize_sql(sql)
            for kw, pattern in _UNSAFE_KW_PATTERNS.items():
                if pattern.search(normalized):
                    raise ReadOnlyViolation(
                        f"ODPS-0130161:SQL syntax error - {kw} not allowed in read-only mode "
                        f"(odps.sql.read.only=true)"
                    )
        return get_instance()
    return _run_sql


def text_payload(result: dict) -> dict:
    """Parse MCP result content[0].text as JSON."""
    content = result.get("content") or []
    assert len(content) >= 1, f"expected content, got {result}"
    assert content[0].get("type") == "text", content[0]
    return json.loads(content[0]["text"])


def data(payload: dict) -> dict:
    """Return inner data when tool returns envelope { success, data }; else payload."""
    if payload.get("success") is True and "data" in payload:
        return payload["data"]
    return payload


def has_config() -> bool:
    """Check whether a config file exists for integration tests."""
    import os
    p = os.environ.get("MAXCOMPUTE_CATALOG_CONFIG") or str(Path(__file__).resolve().parent.parent / "config.json")
    return Path(p).exists()


def uniq(prefix: str) -> str:
    """Generate a unique table name with the given prefix."""
    return f"{prefix}_{int(time.time())}_{uuid.uuid4().hex[:6]}"


def drop_table(real_tools: Any, table: str) -> None:
    """Drop a table via pyodps directly (execute_sql MCP tool blocks DDL)."""
    logger = logging.getLogger(__name__)
    try:
        real_tools.maxcompute_client.execute_sql(f"DROP TABLE IF EXISTS {table};")
    except Exception as e:
        logger.warning("cleanup DROP TABLE %s failed: %s", table, e)


def count_rows(real_tools: Any, project: str, table: str, *, partition: str | None = None, hints: dict | None = None) -> int:
    """Count rows via execute_sql MCP tool (sync)."""
    where = f" WHERE {partition}" if partition else ""
    args: dict = {
        "project": project,
        "sql": f"SELECT COUNT(*) AS c FROM {table}{where}",
        "async": False,
        "timeout": 120,
    }
    if hints:
        args["hints"] = hints
    r = real_tools.call("execute_sql", args)
    p = text_payload(r)
    assert p.get("success") is True, f"SELECT COUNT failed for {table}: {p}"
    rows = p.get("data") or []
    assert rows, f"empty result from COUNT(*) on {table}: {p}"
    row = rows[0]
    val = row.get("c") if isinstance(row, dict) else row
    return int(val)


def _get_tools_class():
    from maxcompute_catalog_mcp.tools import Tools
    return Tools


def _make_to_map_result(**kwargs: Any) -> MagicMock:
    m = MagicMock()
    m.to_map.return_value = dict(kwargs)
    return m


@pytest.fixture
def mock_sdk() -> MagicMock:
    """Catalog SDK with client that has list_projects, get_project, etc."""
    client = MagicMock()
    client.list_projects.return_value = _make_to_map_result(
        projects=[{"projectId": "p1", "name": "projects/p1"}],
        next_page_token=None,
    )
    client.get_project.return_value = _make_to_map_result(
        projectId="p1",
        name="projects/p1",
        owner="ALIYUN$test",
    )
    client.list_schemas.return_value = _make_to_map_result(
        schemas=[{"schemaName": "default"}],
        next_page_token=None,
    )
    client.get_schema.return_value = _make_to_map_result(
        schemaName="default",
        name="projects/p1/schemas/default",
    )
    client.list_tables.return_value = _make_to_map_result(
        tables=[{"tableName": "t1", "name": "projects/p1/schemas/default/tables/t1"}],
        next_page_token=None,
    )
    client.get_table.return_value = _make_to_map_result(
        tableName="t1",
        projectId="p1",
        schemaName="default",
        tableSchema={"fields": []},
    )
    client.list_partitions.return_value = _make_to_map_result(
        partitions=[{"spec": "ds=20250101"}],
        next_page_token=None,
    )
    client.search.return_value = _make_to_map_result(
        entries=[],
        next_page_token=None,
    )
    sdk = MagicMock()
    sdk.client = client
    return sdk


@pytest.fixture
def mock_maxcompute_client() -> MagicMock:
    """MaxCompute compute client (pyodps) for cost_sql, execute_sql, etc.

    run_sql uses a side_effect that:
    1. Checks the server-side ReadOnly guard (odps.sql.read.only=true)
    2. Returns the same ``inst`` instance that tests can configure via
       ``mc.run_sql.return_value`` — because side_effect takes precedence,
       the guard reuses mc.run_sql.return_value as the instance to return.
    """
    mc = MagicMock()
    mc.execute_sql_cost.return_value = "0.1"
    inst = MagicMock()
    inst.id = "inst-001"  # must be a string so JSON serialization works
    inst.is_terminated.return_value = True
    inst.is_successful.return_value = True
    inst.get_task_results.return_value = {}
    # get_instance_status: status / logView / is_terminated() / is_successful() must be JSON-serializable
    inst.status = MagicMock()
    inst.status.name = "Running"
    inst.get_logview_address.return_value = None
    # _run_dml / execute_sql sync path calls inst.wait_for_success(); explicitly
    # mock it so tests don't rely on MagicMock default behavior.
    inst.wait_for_success = MagicMock(return_value=None)
    mc.execute_sql = MagicMock(return_value=inst)
    mc.run_sql.return_value = inst
    # side_effect: guard returns mc.run_sql.return_value (i.e. inst) on success,
    # so tests can configure inst.wait_for_success etc. via mc.run_sql.return_value.
    mc.run_sql.side_effect = _make_readonly_guard(lambda: mc.run_sql.return_value)
    mc.get_instance.return_value = inst
    mc.create_table = MagicMock()
    return mc


@pytest.fixture
def tools(mock_sdk: MagicMock, mock_maxcompute_client: MagicMock):
    """Tools instance with mocked SDK and compute client."""
    Tools = _get_tools_class()
    return Tools(
        sdk=mock_sdk,
        default_project="p1",
        namespace_id="test_namespace_id",
        maxcompute_client=mock_maxcompute_client,
        credential_client=None,
    )


@pytest.fixture
def tools_no_compute(mock_sdk: MagicMock):
    """Tools without maxcompute_client (for unsupported tool paths)."""
    Tools = _get_tools_class()
    return Tools(
        sdk=mock_sdk,
        default_project="p1",
        namespace_id="test_namespace_id",
        maxcompute_client=None,
    )


@pytest.fixture
def tools_no_namespace(mock_sdk: MagicMock, mock_maxcompute_client: MagicMock):
    """Tools without namespace_id (search_meta_data returns unsupported)."""
    Tools = _get_tools_class()
    return Tools(
        sdk=mock_sdk,
        default_project="p1",
        namespace_id="",
        maxcompute_client=mock_maxcompute_client,
    )


def call_safe(tools: Any, tool_name: str, args: dict) -> dict:
    """Call a tool, wrapping any exception into a failure MCP response dict.

    Some SDK methods raise exceptions (e.g. TeaException for 404, TypeError for
    unsupported parameters) instead of returning a structured success=false
    response. This helper normalises both cases so tests can always use
    _text_payload() and _assert_failure().
    """
    try:
        return tools.call(tool_name, args)
    except Exception as e:
        return {"content": [{"type": "text", "text": json.dumps({"success": False, "error": str(e)})}]}


def async_wait_instance(
    real_tools: Any,
    project: str,
    instance_id: str,
    *,
    timeout: int = 120,
    poll_interval: int = 3,
) -> dict:
    """Poll get_instance_status until isTerminated=True or timeout.

    Returns the last status payload.
    Raises AssertionError if the instance does not terminate within *timeout* seconds.
    Useful in E2E tests that follow the async workflow:
    execute_sql(async=True) → async_wait_instance() → get_instance().
    """
    deadline = time.time() + timeout
    while True:
        r = real_tools.call("get_instance_status", {
            "project": project,
            "instanceId": instance_id,
        })
        p = text_payload(r)
        if p.get("success") is False:
            return p
        if p.get("isTerminated"):
            return p
        remaining = deadline - time.time()
        if remaining <= 0:
            raise AssertionError(
                f"Instance {instance_id!r} did not terminate within {timeout}s. "
                f"Last status: {p}"
            )
        time.sleep(min(poll_interval, remaining))


# ---- Real config / integration ----

@pytest.fixture(scope="module")
def real_config():
    """Load config from file; skip if missing or invalid."""
    import os
    from maxcompute_catalog_mcp.config import load_config
    project_root = Path(__file__).resolve().parent.parent
    config_path = os.environ.get("MAXCOMPUTE_CATALOG_CONFIG") or str(project_root / "config.json")
    if not Path(config_path).exists():
        pytest.skip("no config file (config.json or MAXCOMPUTE_CATALOG_CONFIG) for integration tests")
    try:
        return load_config(config_path)
    except Exception as e:
        pytest.skip(f"config load failed: {e}")


@pytest.fixture(scope="module")
def real_tools(real_config):
    """Tools instance built from real config (real Catalog + compute API). Skip if no config."""
    from dataclasses import replace
    from maxcompute_catalog_mcp.config import (
        resolve_catalogapi_endpoint_with_client,
        resolve_protocol_and_endpoints,
    )
    from maxcompute_catalog_mcp.credentials import get_credentials_client
    from maxcompute_catalog_mcp.maxcompute_client import MaxComputeCatalogSdk, MaxComputeClient
    Tools = _get_tools_class()
    # Prefer AK/SK from config.json; fall back to default credential chain
    credential_client = get_credentials_client(
        access_key_id=real_config.access_key_id,
        access_key_secret=real_config.access_key_secret,
        security_token=real_config.security_token,
    )
    # Resolve transport protocol + endpoints once, then thread through both factories
    resolved = resolve_protocol_and_endpoints(real_config)
    # Create MaxCompute client first
    maxcompute_client = MaxComputeClient.create(
        real_config, credential_client=credential_client, resolved=resolved,
    )

    # Resolve catalogapi_endpoint
    cfg = real_config
    if not cfg.catalogapi_endpoint:
        if maxcompute_client is None:
            pytest.skip("Cannot create MaxCompute client to resolve catalogapi_endpoint")
        try:
            catalogapi_endpoint = resolve_catalogapi_endpoint_with_client(
                maxcompute_client._client,
                resolved.maxcompute_url,
            )
            cfg = replace(cfg, catalogapi_endpoint=catalogapi_endpoint)
            resolved = resolve_protocol_and_endpoints(cfg)
        except Exception as e:
            pytest.skip(f"Failed to resolve catalogapi_endpoint: {e}")

    sdk = MaxComputeCatalogSdk.create(
        cfg, credential_client=credential_client, resolved=resolved,
    )
    return Tools(
        sdk=sdk,
        default_project=cfg.default_project or "",
        namespace_id=cfg.namespace_id or "",
        maxcompute_client=maxcompute_client,
        credential_client=credential_client,
    )
