"""Unit tests for each MCP tool (mocked SDK/client)."""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from maxcompute_catalog_mcp.tools import Tools

# Import shared test helpers from conftest (pytest loads conftest automatically;
# direct import works because tests/ is on sys.path during pytest collection)
from tests.conftest import data as _data, text_payload as _text_payload


# ---- Explorer ----

def test_list_projects(tools: Tools) -> None:
    r = tools.call("list_projects", {"pageSize": 10})
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    data = _data(payload)
    assert "projects" in data
    assert len(data["projects"]) >= 1


def test_get_project(tools: Tools) -> None:
    r = tools.call("get_project", {"project": "p1"})
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    data = _data(payload)
    assert data.get("projectId") == "p1"


def test_get_project_missing_project_raises() -> None:
    from maxcompute_catalog_mcp.mcp_protocol import JsonRpcError
    from maxcompute_catalog_mcp.tools import Tools
    t = Tools(sdk=MagicMock(), default_project="", namespace_id="", maxcompute_client=None)
    with pytest.raises(JsonRpcError):
        t.call("get_project", {})


def test_list_schemas(tools: Tools) -> None:
    r = tools.call("list_schemas", {"project": "p1"})
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    data = _data(payload)
    assert "schemas" in data


def test_get_schema(tools: Tools) -> None:
    r = tools.call("get_schema", {"project": "p1", "schema": "default"})
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    data = _data(payload)
    assert data.get("schemaName") == "default"


def test_list_tables(tools: Tools) -> None:
    r = tools.call("list_tables", {"project": "p1", "schema": "default"})
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    data = _data(payload)
    assert "tables" in data


def test_get_table_schema(tools: Tools) -> None:
    r = tools.call("get_table_schema", {"project": "p1", "schema": "default", "table": "t1"})
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    data = _data(payload)
    # _get_table_via_catalog always returns a "columns" list; the "raw" branch is dead
    assert "columns" in data
    assert isinstance(data["columns"], list)


def test_get_partition_info(tools: Tools) -> None:
    r = tools.call(
        "get_partition_info",
        {"project": "p1", "schema": "default", "table": "t1", "pageSize": 10},
    )
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    data = _data(payload)
    assert "partitions" in data


# ---- Data query ----

def test_cost_sql(tools: Tools) -> None:
    r = tools.call("cost_sql", {"project": "p1", "sql": "SELECT 1"})
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    assert "costEstimate" in payload


def test_cost_sql_forwards_hints(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """cost_sql must forward user-supplied hints to execute_sql_cost.

    Without this, 3-level (schema-enabled) projects fail to resolve tables
    during cost estimation even when callers pass odps.namespace.schema=true,
    silently returning stub=true and breaking maxCU protection.
    """
    hints = {"odps.namespace.schema": "true"}
    tools.call("cost_sql", {"project": "p1", "sql": "SELECT 1", "hints": hints})
    mock_maxcompute_client.execute_sql_cost.assert_called_once()
    call_kwargs = mock_maxcompute_client.execute_sql_cost.call_args.kwargs
    assert call_kwargs.get("hints") == hints


def test_execute_sql_max_cu_check_forwards_hints(
    tools: Tools, mock_maxcompute_client: MagicMock
) -> None:
    """execute_sql's maxCU pre-check must forward user-supplied hints.

    Otherwise the pre-submit cost check on 3-level projects silently
    estimates zero and the maxCU ceiling never triggers.
    """
    hints = {"odps.namespace.schema": "true"}
    tools.call(
        "execute_sql",
        {"project": "p1", "sql": "SELECT 1", "maxCU": 100, "hints": hints},
    )
    mock_maxcompute_client.execute_sql_cost.assert_called_once()
    call_kwargs = mock_maxcompute_client.execute_sql_cost.call_args.kwargs
    assert call_kwargs.get("hints") == hints


def test_execute_sql(tools: Tools) -> None:
    r = tools.call("execute_sql", {"project": "p1", "sql": "SELECT 1"})
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    # Async mode (default): response must contain an instanceId
    assert "instanceId" in payload or "instanceId" in _data(payload)


def test_execute_sql_rejects_non_select(tools: Tools) -> None:
    r = tools.call("execute_sql", {"project": "p1", "sql": "INSERT INTO t VALUES (1)"})
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "error" in payload


def test_execute_sql_injects_readonly_hint(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """execute_sql must inject odps.sql.read.only=true in hints passed to run_sql."""
    tools.call("execute_sql", {"project": "p1", "sql": "SELECT 1"})
    mock_maxcompute_client.run_sql.assert_called_once()
    call_kwargs = mock_maxcompute_client.run_sql.call_args.kwargs
    hints = call_kwargs.get("hints", {})
    assert hints.get("odps.sql.read.only") == "true", (
        f"odps.sql.read.only must be 'true', got: {hints}"
    )


def test_execute_sql_readonly_hint_not_overridable(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """User-supplied hints must NOT be able to override odps.sql.read.only."""
    tools.call("execute_sql", {
        "project": "p1",
        "sql": "SELECT 1",
        "hints": {"odps.sql.read.only": "false", "odps.sql.submit.mode": "interactive"},
    })
    mock_maxcompute_client.run_sql.assert_called_once()
    call_kwargs = mock_maxcompute_client.run_sql.call_args.kwargs
    hints = call_kwargs.get("hints", {})
    assert hints.get("odps.sql.read.only") == "true", (
        "odps.sql.read.only must always be 'true', even when caller tries to set it to 'false'"
    )
    # User's other hint should be preserved
    assert hints.get("odps.sql.submit.mode") == "interactive"
    # Default hint should be present (overridden by user in this case)
    assert "odps.sql.submit.mode" in hints


def test_execute_sql_readonly_hint_with_user_hints_merge(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """User hints are merged with defaults, then readonly is enforced on top."""
    tools.call("execute_sql", {
        "project": "p1",
        "sql": "SELECT 1",
        "hints": {"odps.sql.type.system.odps2": "true"},
    })
    mock_maxcompute_client.run_sql.assert_called_once()
    call_kwargs = mock_maxcompute_client.run_sql.call_args.kwargs
    hints = call_kwargs.get("hints", {})
    assert hints.get("odps.sql.read.only") == "true"
    assert hints.get("odps.sql.submit.mode") == "script"
    assert hints.get("odps.sql.type.system.odps2") == "true"


def test_cost_sql_does_not_inject_readonly_hint(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """cost_sql is estimation only and must NOT include odps.sql.read.only hint."""
    tools.call("cost_sql", {"project": "p1", "sql": "SELECT 1"})
    mock_maxcompute_client.execute_sql_cost.assert_called_once()
    call_kwargs = mock_maxcompute_client.execute_sql_cost.call_args.kwargs
    hints = call_kwargs.get("hints") or {}
    assert "odps.sql.read.only" not in hints, (
        f"cost_sql should not have readonly hint, got: {hints}"
    )


def test_execute_sql_rejects_set_statement(tools: Tools) -> None:
    """SET statements must be rejected — they can override the server-side read-only hint."""
    r = tools.call("execute_sql", {"project": "p1", "sql": "SET odps.sql.read.only=false"})
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "SET" in payload.get("error", "") or "Only SELECT" in payload.get("error", "")


def test_execute_sql_rejects_cte_insert(tools: Tools) -> None:
    """WITH ... INSERT must be rejected at client-side guard (CTE body DML detection)."""
    r = tools.call("execute_sql", {
        "project": "p1",
        "sql": "WITH tmp AS (SELECT 1 AS id) INSERT INTO t SELECT id FROM tmp",
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "INSERT" in payload.get("error", "").upper()


def test_execute_sql_rejects_cte_delete(tools: Tools) -> None:
    """WITH ... DELETE must be rejected at client-side guard."""
    r = tools.call("execute_sql", {
        "project": "p1",
        "sql": "WITH tmp AS (SELECT 1 AS id) DELETE FROM t WHERE id IN (SELECT id FROM tmp)",
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "DELETE" in payload.get("error", "").upper()


def test_execute_sql_rejects_cte_update(tools: Tools) -> None:
    """WITH ... UPDATE must be rejected at client-side guard."""
    r = tools.call("execute_sql", {
        "project": "p1",
        "sql": "WITH tmp AS (SELECT 1 AS id) UPDATE t SET val='x' WHERE id IN (SELECT id FROM tmp)",
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "UPDATE" in payload.get("error", "").upper()


def test_execute_sql_rejects_cte_merge(tools: Tools) -> None:
    """WITH ... MERGE must be rejected at client-side guard."""
    r = tools.call("execute_sql", {
        "project": "p1",
        "sql": "WITH src AS (SELECT 1 AS id, 'x' AS val) MERGE INTO t USING src ON t.id = src.id WHEN MATCHED THEN UPDATE SET val = src.val",
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    # The guard detects MERGE or UPDATE in the CTE body (either is valid)
    err_upper = payload.get("error", "").upper()
    assert "MERGE" in err_upper or "UPDATE" in err_upper


def test_execute_sql_allows_cte_select(tools: Tools) -> None:
    """WITH ... SELECT must still be allowed."""
    r = tools.call("execute_sql", {"project": "p1", "sql": "WITH tmp AS (SELECT 1 AS id) SELECT * FROM tmp"})
    payload = _text_payload(r)
    assert payload.get("success") is True or "error" not in payload


def test_execute_sql_allows_cte_select_with_dml_word_in_string(tools: Tools) -> None:
    """CTE with DML keyword inside a string literal must NOT be falsely rejected."""
    r = tools.call("execute_sql", {
        "project": "p1",
        "sql": "WITH tmp AS (SELECT 'INSERT' AS action_type) SELECT * FROM tmp",
    })
    payload = _text_payload(r)
    # Should NOT be rejected — 'INSERT' is inside a string literal
    assert payload.get("success") is True or "error" not in payload


def test_execute_sql_allows_semicolon_in_string_literal(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """SELECT with ';' inside a string literal must NOT be split into multiple statements."""
    r = tools.call("execute_sql", {
        "project": "p1",
        "sql": "SELECT 'a;INSERT b' FROM t",
    })
    payload = _text_payload(r)
    # Must not be rejected — the ';' is inside a string literal, not a statement separator
    assert payload.get("success") is True, f"Unexpected rejection: {payload.get('error')}"
    mock_maxcompute_client.run_sql.assert_called_once()


def test_execute_sql_allows_backslash_escape_in_string(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """Backslash-escaped single quote inside string must not leak DML keywords."""
    r = tools.call("execute_sql", {
        "project": "p1",
        "sql": r"SELECT 'it\'s INSERT demo' FROM t",
    })
    payload = _text_payload(r)
    assert payload.get("success") is True, f"Unexpected rejection: {payload.get('error')}"
    mock_maxcompute_client.run_sql.assert_called_once()


def test_execute_sql_allows_double_quoted_dml_keyword_in_cte(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """Double-quoted string containing DML keyword in CTE must not trigger false-positive."""
    r = tools.call("execute_sql", {
        "project": "p1",
        "sql": 'WITH tmp AS (SELECT "INSERT" AS x) SELECT * FROM tmp',
    })
    payload = _text_payload(r)
    assert payload.get("success") is True, f"Unexpected rejection: {payload.get('error')}"
    mock_maxcompute_client.run_sql.assert_called_once()


def test_execute_sql_allows_backtick_identifier_named_after_keyword(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """Backtick-escaped column named after a DML keyword should be allowed."""
    r = tools.call("execute_sql", {
        "project": "p1",
        "sql": "WITH tmp AS (SELECT `insert` FROM t) SELECT * FROM tmp",
    })
    payload = _text_payload(r)
    assert payload.get("success") is True, f"Unexpected rejection: {payload.get('error')}"
    mock_maxcompute_client.run_sql.assert_called_once()


def test_execute_sql_allows_string_with_line_comment_marker(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """String literal containing '--' must not be mis-treated as a line comment."""
    r = tools.call("execute_sql", {
        "project": "p1",
        "sql": "SELECT 'foo -- bar' AS s FROM t",
    })
    payload = _text_payload(r)
    assert payload.get("success") is True, f"Unexpected rejection: {payload.get('error')}"
    mock_maxcompute_client.run_sql.assert_called_once()


def test_get_instance_status(tools: Tools) -> None:
    r = tools.call("get_instance_status", {"project": "p1", "instanceId": "inst-001"})
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    assert "instanceId" in payload


def test_get_instance(tools: Tools) -> None:
    r = tools.call("get_instance", {"project": "p1", "instanceId": "inst-001"})
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    assert "instanceId" in payload
    # conftest mock: is_terminated=True + is_successful=True → results branch
    assert "results" in payload


# ---- Data insights ----

def test_search_meta_data(tools: Tools) -> None:
    r = tools.call("search_meta_data", {"query": "test", "project": "p1"})
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    # Search result envelope must expose the entries list
    d = _data(payload)
    assert "entries" in d


def test_search_meta_data_without_namespace_returns_unsupported(tools_no_namespace: Tools) -> None:
    r = tools_no_namespace.call("search_meta_data", {"query": "test"})
    payload = _text_payload(r)
    assert payload.get("error") == "unsupported"
    assert "namespace_id" in str(payload.get("message", ""))


# ---- Table designer ----

def test_create_table(tools: Tools) -> None:
    r = tools.call(
        "create_table",
        {
            "project": "p1",
            "schema": "default",
            "table": "test_t",
            "columns": [{"name": "id", "type": "BIGINT"}, {"name": "name", "type": "STRING"}],
        },
    )
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    assert payload.get("success") is True


def test_create_table_without_compute_returns_unsupported(tools_no_compute: Tools) -> None:
    r = tools_no_compute.call(
        "create_table",
        {
            "project": "p1",
            "table": "t1",
            "columns": [{"name": "a", "type": "STRING"}],
        },
    )
    payload = _text_payload(r)
    assert payload.get("error") == "unsupported"


def test_insert_values(tools: Tools) -> None:
    r = tools.call(
        "insert_values",
        {
            "project": "p1",
            "schema": "default",
            "table": "t1",
            "columns": ["a", "b"],
            "values": [["1", "x"]],
        },
    )
    payload = _text_payload(r)
    assert "error" not in payload, f"Expected success, should not contain error: {payload.get('error')}"
    assert payload.get("success") is True


def test_insert_values_empty_columns_returns_error(tools: Tools) -> None:
    r = tools.call("insert_values", {"project": "p1", "table": "t1", "columns": [], "values": []})
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "error" in payload


def test_insert_values_async_returns_instance_id(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """async=True must return instanceId immediately without waiting."""
    r = tools.call(
        "insert_values",
        {
            "project": "p1",
            "table": "t1",
            "columns": ["a", "b"],
            "values": [["1", "x"]],
            "async": True,
        },
    )
    payload = _text_payload(r)
    assert payload.get("success") is True, payload
    assert payload.get("instanceId") == "inst-001"
    assert payload.get("status") == "submitted"
    # run_sql must have been called exactly once; wait_for_success must NOT be called
    mock_maxcompute_client.run_sql.assert_called_once()
    mock_maxcompute_client.run_sql.return_value.wait_for_success.assert_not_called()


def test_insert_values_async_partition_returns_instance_ids(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """async=True with partitioned table returns batches list (one per partition batch)."""
    r = tools.call(
        "insert_values",
        {
            "project": "p1",
            "table": "pt",
            "columns": ["id", "name", "dt"],
            "partitionColumns": ["dt"],
            "values": [
                [1, "a", "2025-01-01"],
                [2, "b", "2025-01-01"],
                [3, "c", "2025-01-02"],
            ],
            "async": True,
        },
    )
    payload = _text_payload(r)
    assert payload.get("success") is True, payload
    assert payload.get("status") == "submitted"
    batches = payload.get("batches")
    assert isinstance(batches, list) and len(batches) == 2
    # Each batch has partitionKey and instanceId
    for b in batches:
        assert "partitionKey" in b
        assert b.get("instanceId") == "inst-001"
    partition_keys = [b["partitionKey"] for b in batches]
    assert ["2025-01-01"] in partition_keys
    assert ["2025-01-02"] in partition_keys
    # wait_for_success must NOT be called in async mode
    mock_maxcompute_client.run_sql.return_value.wait_for_success.assert_not_called()


def test_insert_values_sync_timeout_returns_instance_id(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """Sync mode: WaitTimeoutError → success=False, timeout=True, instanceId returned."""
    from odps.errors import WaitTimeoutError

    mock_maxcompute_client.run_sql.return_value.wait_for_success.side_effect = WaitTimeoutError("timed out")
    r = tools.call(
        "insert_values",
        {
            "project": "p1",
            "table": "t1",
            "columns": ["a"],
            "values": [["v1"]],
            "timeout": 5,
        },
    )
    payload = _text_payload(r)
    assert payload.get("success") is False, payload
    assert payload.get("timeout") is True
    assert payload.get("instanceId") == "inst-001"
    assert "5s" in payload.get("message", "")


def test_execute_sql_sync_timeout_returns_instance_id(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """Sync execute_sql: WaitTimeoutError → success=False, timeout=True, instanceId returned."""
    from odps.errors import WaitTimeoutError

    mock_maxcompute_client.run_sql.return_value.wait_for_success.side_effect = WaitTimeoutError("timed out")
    r = tools.call(
        "execute_sql",
        {"project": "p1", "sql": "SELECT 1", "async": False, "timeout": 5},
    )
    payload = _text_payload(r)
    assert payload.get("success") is False, payload
    assert payload.get("timeout") is True
    assert payload.get("instanceId") == "inst-001"
    assert "5s" in payload.get("message", "")


def test_insert_values_partition_column_not_in_columns(tools: Tools) -> None:
    """partitionColumns entry not present in columns → error."""
    r = tools.call(
        "insert_values",
        {
            "project": "p1",
            "table": "pt",
            "columns": ["id", "name"],
            "partitionColumns": ["dt"],  # 'dt' not in columns
            "values": [[1, "a"]],
        },
    )
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "dt" in payload.get("error", "")


def test_insert_values_all_partition_columns_error(tools: Tools) -> None:
    """All columns are partition columns → no data columns → error."""
    r = tools.call(
        "insert_values",
        {
            "project": "p1",
            "table": "pt",
            "columns": ["dt"],
            "partitionColumns": ["dt"],
            "values": [["2025-01-01"]],
        },
    )
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "data column" in payload.get("error", "").lower()


def test_insert_values_row_length_mismatch(tools: Tools) -> None:
    """Row with fewer values than columns → error."""
    r = tools.call(
        "insert_values",
        {
            "project": "p1",
            "table": "pt",
            "columns": ["id", "name", "dt"],
            "partitionColumns": ["dt"],
            "values": [[1, "a"]],  # only 2 values for 3 columns
        },
    )
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "length" in payload.get("error", "").lower()


def test_insert_values_partition_batches_by_partition_key(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """Partitioned table: groups by partition key, generates INSERT INTO ... PARTITION (...) (...) VALUES ..."""
    r = tools.call(
        "insert_values",
        {
            "project": "p1",
            "table": "pt",
            "columns": ["id", "name", "dt"],
            "partitionColumns": ["dt"],
            "values": [
                [1, "a", "2025-01-01"],
                [2, "b", "2025-01-01"],
                [3, "c", "2025-01-02"],
            ],
        },
    )
    payload = _text_payload(r)
    assert payload.get("success") is True, payload
    assert payload.get("rowsInserted") == 3
    assert payload.get("partitionBatches") == 2
    calls = mock_maxcompute_client.run_sql.call_args_list
    assert len(calls) == 2
    sql_a, sql_b = calls[0][0][0], calls[1][0][0]
    assert "PARTITION" in sql_a and "`dt`='2025-01-01'" in sql_a and "`id`" in sql_a
    assert "`dt`='2025-01-02'" in sql_b


def test_execute_sql_invalid_async_type_returns_error(tools: Tools) -> None:
    """async must be a boolean; passing a string must fail with a clear error."""
    r = tools.call("execute_sql", {"project": "p1", "sql": "SELECT 1", "async": "true"})
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "async" in payload.get("error", "").lower()


def test_insert_values_invalid_async_type_returns_error(tools: Tools) -> None:
    """async must be a boolean; passing an integer must fail with a clear error."""
    r = tools.call(
        "insert_values",
        {"project": "p1", "table": "t1", "columns": ["a"], "values": [["v"]], "async": 1},
    )
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "async" in payload.get("error", "").lower()


def test_insert_values_partition_batch_limit_configurable(
    tools: Tools, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_MAX_PARTITION_BATCHES can be patched; exceeding the limit returns an error."""
    import maxcompute_catalog_mcp.tools_designer as td

    monkeypatch.setattr(td, "_MAX_PARTITION_BATCHES", 1)
    r = tools.call(
        "insert_values",
        {
            "project": "p1",
            "table": "pt",
            "columns": ["id", "dt"],
            "partitionColumns": ["dt"],
            "values": [[1, "2025-01-01"], [2, "2025-01-02"]],  # 2 batches > limit 1
        },
    )
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "partition" in payload.get("error", "").lower()


def test_insert_values_async_partition_partial_failure(
    tools: Tools, mock_maxcompute_client: MagicMock
) -> None:
    """Async partition: second batch submit failure → success=False, batches+errors both returned."""
    call_count = 0
    ok_inst = mock_maxcompute_client.run_sql.return_value

    def _run_sql_side_effect(sql: str, **kwargs: Any) -> MagicMock:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("ODPS quota exceeded")
        return ok_inst

    mock_maxcompute_client.run_sql.side_effect = _run_sql_side_effect
    r = tools.call(
        "insert_values",
        {
            "project": "p1",
            "table": "pt",
            "columns": ["id", "name", "dt"],
            "partitionColumns": ["dt"],
            "values": [
                [1, "a", "2025-01-01"],
                [2, "b", "2025-01-02"],
            ],
            "async": True,
        },
    )
    payload = _text_payload(r)
    assert payload.get("success") is False, payload
    assert isinstance(payload.get("batches"), list) and len(payload["batches"]) == 1
    assert isinstance(payload.get("errors"), list) and len(payload["errors"]) == 1
    err = payload["errors"][0]
    assert "partitionKey" in err
    assert "quota exceeded" in err.get("error", "").lower()


# ---- Unknown tool ----

def test_unknown_tool_raises() -> None:
    from maxcompute_catalog_mcp.mcp_protocol import JsonRpcError
    from maxcompute_catalog_mcp.tools import Tools
    t = Tools(sdk=MagicMock(), default_project="", namespace_id="", maxcompute_client=None)
    with pytest.raises(JsonRpcError):
        t.call("no_such_tool", {})


# ---- 2-level project detection via get_project.schemaEnabled ----

def _make_two_level_sdk(project: str = "p2") -> MagicMock:
    """SDK where get_project returns schemaEnabled=false for the given project."""
    sdk = MagicMock()
    sdk.client.get_project.return_value = MagicMock(
        to_map=lambda: {"projectId": project, "schemaEnabled": False}
    )
    sdk.client.list_tables.return_value = MagicMock(
        to_map=lambda: {"tables": [{"tableName": "t1"}], "next_page_token": None}
    )
    sdk.client.get_table.return_value = MagicMock(
        to_map=lambda: {"tableName": "t1", "schema": []}
    )
    sdk.client.list_partitions.return_value = MagicMock(
        to_map=lambda: {"partitions": [{"spec": "ds=20250101"}]}
    )
    return sdk


def test_is_schema_enabled_caches_result() -> None:
    """_is_schema_enabled calls get_project once and caches the result."""
    sdk = _make_two_level_sdk("p2")
    tools = Tools(sdk=sdk, default_project="p2", namespace_id="")
    assert tools._is_schema_enabled("p2") is False
    assert tools._is_schema_enabled("p2") is False
    sdk.client.get_project.assert_called_once_with(project_id="p2")


def test_is_schema_enabled_defaults_true_on_error() -> None:
    """_is_schema_enabled returns True (assume 3-level) when get_project fails,
    and does NOT cache the error so the next call retries."""
    sdk = MagicMock()
    sdk.client.get_project.side_effect = Exception("network error")
    tools = Tools(sdk=sdk, default_project="p3", namespace_id="")
    assert tools._is_schema_enabled("p3") is True
    assert tools._is_schema_enabled("p3") is True
    # called each time because transient errors must not be cached
    assert sdk.client.get_project.call_count == 2


def test_is_schema_enabled_defaults_true_when_field_absent() -> None:
    """_is_schema_enabled returns True when schemaEnabled key is absent from response."""
    sdk = MagicMock()
    sdk.client.get_project.return_value = MagicMock(
        to_map=lambda: {"projectId": "p4"}  # no schemaEnabled field
    )
    tools = Tools(sdk=sdk, default_project="p4", namespace_id="")
    assert tools._is_schema_enabled("p4") is True


def test_list_schemas_two_level_returns_default_schema() -> None:
    """list_schemas returns synthetic default schema for 2-level project."""
    tools = Tools(sdk=_make_two_level_sdk(), default_project="p2", namespace_id="")
    r = tools.call("list_schemas", {"project": "p2"})
    payload = _text_payload(r)
    data = _data(payload)
    assert data.get("schemas") == [{"name": "default"}]
    # SDK list_schemas must never be called for a 2-level project
    tools.sdk.client.list_schemas.assert_not_called()


def test_list_schemas_two_level_uses_cache() -> None:
    """Second call to list_schemas for 2-level project does not call get_project again."""
    sdk = _make_two_level_sdk()
    tools = Tools(sdk=sdk, default_project="p2", namespace_id="")
    tools.call("list_schemas", {"project": "p2"})
    tools.call("list_schemas", {"project": "p2"})
    sdk.client.get_project.assert_called_once()  # cached after first call


def test_get_schema_two_level_returns_synthetic() -> None:
    """get_schema returns synthetic schema object for 2-level project."""
    tools = Tools(sdk=_make_two_level_sdk(), default_project="p2", namespace_id="")
    r = tools.call("get_schema", {"project": "p2", "schema": "default"})
    payload = _text_payload(r)
    data = _data(payload)
    assert data.get("name") == "default"
    assert "2-level" in data.get("description", "")
    tools.sdk.client.get_schema.assert_not_called()


def test_list_tables_two_level_calls_api_with_schema() -> None:
    """list_tables for 2-level project calls API with schema_name as-is (no schema stripping)."""
    tools = Tools(sdk=_make_two_level_sdk(), default_project="p2", namespace_id="")
    r = tools.call("list_tables", {"project": "p2", "schema": "default"})
    payload = _text_payload(r)
    data = _data(payload)
    assert "tables" in data
    call_kwargs = tools.sdk.client.list_tables.call_args.kwargs
    assert call_kwargs.get("schema_name") == "default"


def test_get_table_schema_two_level_calls_api_with_schema() -> None:
    """get_table_schema for 2-level project passes schema to API unchanged."""
    tools = Tools(sdk=_make_two_level_sdk(), default_project="p2", namespace_id="")
    r = tools.call("get_table_schema", {"project": "p2", "schema": "default", "table": "t1"})
    payload = _text_payload(r)
    data = _data(payload)
    assert "columns" in data
    call_arg = tools.sdk.client.get_table.call_args.args[0]
    assert getattr(call_arg, "schema_name", None) == "default"


def test_get_partition_info_two_level_omits_schema() -> None:
    """get_partition_info for 2-level projects passes 'default' schema_name (not empty string to avoid double-slash in URL)."""
    tools = Tools(sdk=_make_two_level_sdk(), default_project="p2", namespace_id="")
    r = tools.call("get_partition_info", {"project": "p2", "schema": "default", "table": "t1"})
    payload = _text_payload(r)
    # get_partition_info returns mcp_text_result(m), so payload is the raw dict
    assert "partitions" in payload
    call_kwargs = tools.sdk.client.list_partitions.call_args.kwargs
    assert call_kwargs.get("schema_name") == "default"


def test_get_table_schema_no_empty_partition_keys() -> None:
    """_get_table_via_catalog must not emit empty-string partition keys."""
    from pyodps_catalog import models as catalog_models
    pd = catalog_models.PartitionDefinition()
    valid = catalog_models.PartitionedColumn()
    valid.field = "ds"
    empty = catalog_models.PartitionedColumn()  # field is None → must be filtered
    pd.partitioned_columns = [valid, empty]

    t = catalog_models.Table(
        project_id="p1", schema_name="default", table_name="t1",
    )
    t.partition_definition = pd

    sdk = MagicMock()
    sdk.client.get_table.return_value = t
    tools = Tools(sdk=sdk, default_project="p1", namespace_id="")
    r = tools.call("get_table_schema", {"project": "p1", "schema": "default", "table": "t1"})
    payload = _text_payload(r)
    data = _data(payload)
    partition_keys = data.get("partitionKeys", [])
    assert "" not in partition_keys, f"Empty partition key found: {partition_keys}"
    assert partition_keys == ["ds"]


def test_execute_sql_open_reader_exception_logged_not_swallowed(
    tools: Tools, caplog: pytest.LogCaptureFixture
) -> None:
    """open_reader() failure must be logged at DEBUG level and fall through to raw output."""
    import logging

    # execute_sql (and insert_values) call compute.run_sql(); wire up the same inst
    inst = tools.maxcompute_client.run_sql.return_value
    inst.open_reader.side_effect = RuntimeError("schema unavailable")
    inst.is_terminated.return_value = True
    inst.get_task_results.return_value = {"AnonymousSQLTask": "col1\tval1"}

    with caplog.at_level(logging.DEBUG, logger="maxcompute_catalog_mcp.tools_compute"):
        # async=False forces the sync path where open_reader is actually called
        r = tools.call("execute_sql", {"project": "p1", "sql": "SHOW TABLES", "async": False})

    payload = _text_payload(r)
    assert payload.get("success") is True
    assert any("open_reader" in m for m in caplog.messages), (
        "Expected a DEBUG log mentioning open_reader(), got: " + str(caplog.messages)
    )


# ---- Row cap + output_uri (large-result safety) ----

def _build_mock_reader(columns: list[str], row_count: int) -> MagicMock:
    """Create a MagicMock reader: context-manager + _schema.columns + iterable records.

    ``__iter__`` returns a fresh iterator on each call so re-iteration (e.g. in retry
    paths) doesn't silently yield empty.
    """
    records = [{c: f"{c}_{i}" for c in columns} for i in range(row_count)]
    schema = MagicMock()
    schema.columns = [MagicMock(name=c) for c in columns]
    for col_mock, name in zip(schema.columns, columns):
        col_mock.name = name
    reader = MagicMock()
    reader._schema = schema
    reader.__iter__.side_effect = lambda: iter(records)
    reader.__enter__.return_value = reader
    reader.__exit__.return_value = False
    return reader


def test_execute_sql_sync_inline_truncates_large_result(
    tools: Tools, mock_maxcompute_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No output_uri + result exceeds cap → truncated=True, rowCount reports full total."""
    monkeypatch.setenv("MAXC_RESULT_ROW_CAP", "3")
    inst = mock_maxcompute_client.run_sql.return_value
    inst.is_terminated.return_value = True
    inst.open_reader.return_value = _build_mock_reader(["c1"], row_count=10)

    r = tools.call("execute_sql", {"project": "p1", "sql": "SELECT c1 FROM t", "async": False})
    payload = _text_payload(r)
    assert payload.get("success") is True
    assert payload.get("truncated") is True
    assert payload.get("rowCount") == 10  # full count even though only 3 kept
    assert payload.get("rowsReturned") == 3
    assert len(payload.get("data", [])) == 3
    assert "message" in payload and "output_uri" in payload["message"]


def test_execute_sql_sync_inline_no_truncation_when_under_cap(
    tools: Tools, mock_maxcompute_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MAXC_RESULT_ROW_CAP", "100")
    inst = mock_maxcompute_client.run_sql.return_value
    inst.is_terminated.return_value = True
    inst.open_reader.return_value = _build_mock_reader(["c1"], row_count=5)

    r = tools.call("execute_sql", {"project": "p1", "sql": "SELECT c1 FROM t", "async": False})
    payload = _text_payload(r)
    assert payload.get("truncated") is False
    assert payload.get("rowCount") == 5
    assert payload.get("rowsReturned") == 5


def test_execute_sql_sync_with_output_uri_streams_to_file(
    tools: Tools, mock_maxcompute_client: MagicMock, tmp_path: Any,
) -> None:
    """outputPath is decorated with instanceId to avoid collisions across calls."""
    out = tmp_path / "result.jsonl"
    expected = tmp_path / "result.inst-001.jsonl"
    inst = mock_maxcompute_client.run_sql.return_value
    inst.is_terminated.return_value = True
    inst.open_reader.return_value = _build_mock_reader(["c1", "c2"], row_count=30)

    r = tools.call("execute_sql", {
        "project": "p1", "sql": "SELECT c1, c2 FROM t",
        "async": False, "output_uri": f"file://{out}",
    })
    payload = _text_payload(r)
    assert payload.get("success") is True
    assert payload.get("truncated") is False  # everything went to disk
    assert payload.get("rowCount") == 30
    assert payload.get("outputPath") == str(expected)
    assert payload.get("previewRows") == 20  # PREVIEW_ROWS constant
    assert expected.exists()
    assert not out.exists()  # original path unused; decorated path used instead
    import json as _json
    lines = expected.read_text().strip().split("\n")
    assert len(lines) == 30
    assert _json.loads(lines[0]) == {"c1": "c1_0", "c2": "c2_0"}


def test_execute_sql_rejects_unsupported_output_uri_scheme(tools: Tools) -> None:
    r = tools.call("execute_sql", {
        "project": "p1", "sql": "SELECT 1", "async": False,
        "output_uri": "s3://bucket/key",
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "file://" in payload.get("error", "")


def test_execute_sql_rejects_empty_output_uri(tools: Tools) -> None:
    r = tools.call("execute_sql", {
        "project": "p1", "sql": "SELECT 1", "async": False,
        "output_uri": "file://",
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "empty" in payload.get("error", "").lower()


def test_execute_sql_rejects_system_path_output_uri(tools: Tools) -> None:
    """output_uri pointing to sensitive system directories must be rejected."""
    for uri in ("file:///etc/passwd", "/bin/output.jsonl", "file:///proc/self/maps"):
        r = tools.call("execute_sql", {
            "project": "p1", "sql": "SELECT 1", "async": False,
            "output_uri": uri,
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, f"Expected rejection for {uri}"
        assert "restricted" in payload.get("error", "").lower(), f"Missing 'restricted' in error for {uri}"


def test_execute_sql_accepts_bare_path_output_uri(
    tools: Tools, mock_maxcompute_client: MagicMock, tmp_path: Any,
) -> None:
    """output_uri without scheme (bare path) is accepted as a local file path."""
    out = tmp_path / "bare.jsonl"
    expected = tmp_path / "bare.inst-001.jsonl"
    inst = mock_maxcompute_client.run_sql.return_value
    inst.is_terminated.return_value = True
    inst.open_reader.return_value = _build_mock_reader(["c1"], row_count=3)

    r = tools.call("execute_sql", {
        "project": "p1", "sql": "SELECT c1 FROM t",
        "async": False, "output_uri": str(out),
    })
    payload = _text_payload(r)
    assert payload.get("success") is True
    assert payload.get("outputPath") == str(expected)
    assert expected.exists()


def _build_failing_reader(columns: list[str], rows_before_fail: int) -> MagicMock:
    """Reader that yields ``rows_before_fail`` records then raises mid-iteration."""
    records = [{c: f"{c}_{i}" for c in columns} for i in range(rows_before_fail)]

    def _gen():
        for r in records:
            yield r
        raise RuntimeError("simulated network failure mid-stream")

    schema = MagicMock()
    schema.columns = [MagicMock(name=c) for c in columns]
    for col_mock, name in zip(schema.columns, columns):
        col_mock.name = name
    reader = MagicMock()
    reader._schema = schema
    reader.__iter__.side_effect = lambda: _gen()
    reader.__enter__.return_value = reader
    reader.__exit__.return_value = False
    return reader


def test_execute_sql_partial_write_cleaned_up_on_reader_failure(
    tools: Tools, mock_maxcompute_client: MagicMock, tmp_path: Any,
) -> None:
    """Reader raises mid-stream → no file at final path, no .partial leftover."""
    out = tmp_path / "result.jsonl"
    final = tmp_path / "result.inst-001.jsonl"
    partial = tmp_path / "result.inst-001.jsonl.partial"
    inst = mock_maxcompute_client.run_sql.return_value
    inst.is_terminated.return_value = True
    inst.open_reader.return_value = _build_failing_reader(["c1"], rows_before_fail=5)

    r = tools.call("execute_sql", {
        "project": "p1", "sql": "SELECT c1 FROM t",
        "async": False, "output_uri": f"file://{out}",
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "simulated" in payload.get("error", "")
    assert not final.exists(), "final path must not exist after mid-stream failure"
    assert not partial.exists(), "partial file must be cleaned up"


def test_execute_sql_async_with_output_uri_does_not_create_parent_dir(
    tools: Tools, mock_maxcompute_client: MagicMock, tmp_path: Any,
) -> None:
    """Async mode must validate output_uri format but not mkdir — no file will be written."""
    deep = tmp_path / "never" / "created" / "here"
    assert not deep.exists()
    out = deep / "result.jsonl"
    inst = mock_maxcompute_client.run_sql.return_value
    inst.is_terminated.return_value = True

    r = tools.call("execute_sql", {
        "project": "p1", "sql": "SELECT 1",
        "async": True, "output_uri": f"file://{out}",
    })
    payload = _text_payload(r)
    assert payload.get("success") is True
    assert payload.get("status") == "submitted"
    assert not deep.exists(), "parent dir must not be created in async mode"


def test_execute_sql_async_still_rejects_bad_output_uri_scheme(tools: Tools) -> None:
    """Async mode should still do format/scheme validation (create_dir=False ≠ skip validation)."""
    r = tools.call("execute_sql", {
        "project": "p1", "sql": "SELECT 1",
        "async": True, "output_uri": "s3://bucket/key",
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "file://" in payload.get("error", "")


def test_get_instance_caps_rows_and_reports_truncation(
    tools: Tools, mock_maxcompute_client: MagicMock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Async path: get_instance without output_uri must cap rows just like sync path."""
    monkeypatch.setenv("MAXC_RESULT_ROW_CAP", "2")
    task_result = MagicMock()
    task_result.open_reader.return_value = _build_mock_reader(["c1"], row_count=7)
    mock_maxcompute_client.get_instance.return_value.is_terminated.return_value = True
    mock_maxcompute_client.get_instance.return_value.get_task_results.return_value = {
        "AnonymousSQLTask": task_result,
    }

    r = tools.call("get_instance", {"project": "p1", "instanceId": "inst-001"})
    payload = _text_payload(r)
    task_entry = payload["results"]["AnonymousSQLTask"]
    assert task_entry["truncated"] is True
    assert task_entry["rowCount"] == 7
    assert task_entry["rowsReturned"] == 2


def test_get_instance_with_output_uri_streams_all_rows(
    tools: Tools, mock_maxcompute_client: MagicMock, tmp_path: Any,
) -> None:
    """Single-task: outputPath = <stem>.<instanceId><suffix>."""
    out = tmp_path / "async_result.jsonl"
    expected = tmp_path / "async_result.inst-001.jsonl"
    task_result = MagicMock()
    task_result.open_reader.return_value = _build_mock_reader(["c1", "c2"], row_count=50)
    mock_maxcompute_client.get_instance.return_value.is_terminated.return_value = True
    mock_maxcompute_client.get_instance.return_value.get_task_results.return_value = {
        "AnonymousSQLTask": task_result,
    }

    r = tools.call("get_instance", {
        "project": "p1", "instanceId": "inst-001",
        "output_uri": f"file://{out}",
    })
    payload = _text_payload(r)
    task_entry = payload["results"]["AnonymousSQLTask"]
    assert task_entry["rowCount"] == 50
    assert task_entry["truncated"] is False
    assert task_entry["outputPath"] == str(expected)
    assert expected.exists()
    assert not out.exists()
    assert len(expected.read_text().strip().split("\n")) == 50


def test_get_instance_multi_task_disambiguates_with_instanceid_and_task_name(
    tools: Tools, mock_maxcompute_client: MagicMock, tmp_path: Any,
) -> None:
    """Multi-task: each task writes to <stem>.<instanceId>.<task_name><suffix>."""
    out = tmp_path / "bundle.jsonl"
    task_a = MagicMock()
    task_a.open_reader.return_value = _build_mock_reader(["c1"], row_count=3)
    task_b = MagicMock()
    task_b.open_reader.return_value = _build_mock_reader(["c1"], row_count=5)
    mock_maxcompute_client.get_instance.return_value.is_terminated.return_value = True
    mock_maxcompute_client.get_instance.return_value.get_task_results.return_value = {
        "TaskA": task_a,
        "TaskB": task_b,
    }

    r = tools.call("get_instance", {
        "project": "p1", "instanceId": "inst-001",
        "output_uri": f"file://{out}",
    })
    payload = _text_payload(r)
    a_path = tmp_path / "bundle.inst-001.TaskA.jsonl"
    b_path = tmp_path / "bundle.inst-001.TaskB.jsonl"
    assert payload["results"]["TaskA"]["outputPath"] == str(a_path)
    assert payload["results"]["TaskB"]["outputPath"] == str(b_path)
    assert a_path.exists() and b_path.exists()
    assert len(a_path.read_text().strip().split("\n")) == 3
    assert len(b_path.read_text().strip().split("\n")) == 5


def test_get_instance_reports_error_when_schema_missing(
    tools: Tools, mock_maxcompute_client: MagicMock,
) -> None:
    """Reader without _schema attribute → task entry carries an error, not empty-dict rows."""
    task_result = MagicMock()
    reader_without_schema = MagicMock()
    # MagicMock auto-creates attributes, so explicitly remove _schema
    del reader_without_schema._schema
    reader_without_schema.__enter__.return_value = reader_without_schema
    reader_without_schema.__exit__.return_value = False
    task_result.open_reader.return_value = reader_without_schema

    mock_maxcompute_client.get_instance.return_value.is_terminated.return_value = True
    mock_maxcompute_client.get_instance.return_value.get_task_results.return_value = {
        "AnonymousSQLTask": task_result,
    }

    r = tools.call("get_instance", {"project": "p1", "instanceId": "inst-001"})
    payload = _text_payload(r)
    entry = payload["results"]["AnonymousSQLTask"]
    assert "error" in entry, f"expected error, got: {entry}"
    assert "schema" in entry["error"].lower()
    # Key assertion: no misleading empty-dict data
    assert "data" not in entry and "rowCount" not in entry


def test_get_instance_reports_error_when_columns_empty(
    tools: Tools, mock_maxcompute_client: MagicMock,
) -> None:
    """Reader with _schema.columns == [] → error, not empty-dict rows."""
    task_result = MagicMock()
    reader = MagicMock()
    empty_schema = MagicMock()
    empty_schema.columns = []
    reader._schema = empty_schema
    reader.__enter__.return_value = reader
    reader.__exit__.return_value = False
    task_result.open_reader.return_value = reader

    mock_maxcompute_client.get_instance.return_value.is_terminated.return_value = True
    mock_maxcompute_client.get_instance.return_value.get_task_results.return_value = {
        "AnonymousSQLTask": task_result,
    }

    r = tools.call("get_instance", {"project": "p1", "instanceId": "inst-001"})
    payload = _text_payload(r)
    entry = payload["results"]["AnonymousSQLTask"]
    assert "error" in entry
    assert "data" not in entry and "rowCount" not in entry


# ---- Table metadata (comment / labels / columns / expiration) ----

def _make_table_model(
    *,
    description: str = "old desc",
    labels: dict | None = None,
    fields: list | None = None,
    etag: str = "etag-v1",
) -> Any:
    """Build a real pyodps_catalog Table model for get_table/update_table mocks."""
    from pyodps_catalog import models as catalog_models
    t = catalog_models.Table(
        project_id="p1",
        schema_name="default",
        table_name="t1",
        etag=etag,
        description=description,
        labels=dict(labels) if labels is not None else {"env": "prod"},
    )
    schema = catalog_models.TableFieldSchema()
    schema.fields = []
    if fields is None:
        fields = [
            {"field_name": "id", "sql_type_definition": "BIGINT", "mode": "REQUIRED"},
            {
                "field_name": "addr",
                "sql_type_definition": "STRUCT<city:STRING>",
                "mode": "NULLABLE",
                "description": "address",
                "children": [
                    {"field_name": "city", "sql_type_definition": "STRING",
                     "description": "city old"},
                ],
            },
        ]
    for f in fields:
        sub = catalog_models.TableFieldSchema(
            field_name=f["field_name"],
            sql_type_definition=f["sql_type_definition"],
            mode=f.get("mode") or "NULLABLE",
            description=f.get("description") or "",
        )
        children = []
        for cf in f.get("children", []) or []:
            children.append(catalog_models.TableFieldSchema(
                field_name=cf["field_name"],
                sql_type_definition=cf["sql_type_definition"],
                description=cf.get("description") or "",
            ))
        sub.fields = children
        schema.fields.append(sub)
    t.table_schema = schema
    return t


@pytest.fixture
def meta_tools(mock_sdk: MagicMock, mock_maxcompute_client: MagicMock) -> Tools:
    """Tools fixture with ``sdk.client.get_table`` / ``update_table`` returning real Table models."""
    current = _make_table_model()
    mock_sdk.client.get_table = MagicMock(return_value=current)
    # update_table: echo back the model it received so the assertions below can
    # inspect the exact patch that would be persisted.
    mock_sdk.client.update_table = MagicMock(side_effect=lambda t: t)
    return Tools(
        sdk=mock_sdk,
        default_project="p1",
        namespace_id="test_namespace_id",
        maxcompute_client=mock_maxcompute_client,
        credential_client=None,
    )


def test_get_table_schema_full_metadata(meta_tools: Tools) -> None:
    """get_table_schema should return SQL view + compact business-semantic metadata."""
    r = meta_tools.call("get_table_schema", {"project": "p1", "schema": "default", "table": "t1"})
    payload = _text_payload(r)
    assert "error" not in payload, payload
    data = _data(payload)

    # SQL-oriented view (unchanged contract)
    assert data["sqlTableRef"] in ("default.t1", "t1")
    assert data["sqlExample"].startswith("SELECT ")
    assert data["namingModel"] in ("3-level", "2-level")
    assert [c["name"] for c in data["columns"]] == ["id", "addr"]
    assert "description" in data["columns"][0]

    # Business-semantic view
    assert data["etag"] == "etag-v1"
    assert data["description"] == "old desc"
    assert data["labels"] == {"env": "prod"}
    assert data["expiration"] == {}

    # columns now carries mode + nested struct children inline (no separate 'fields')
    assert "fields" not in data, "duplicated top-level fields[] should be merged into columns[]"
    assert "clustering" not in data
    assert "tableConstraints" not in data
    assert "tableFormatDefinition" not in data

    addr = next(c for c in data["columns"] if c["name"] == "addr")
    assert [sub["name"] for sub in addr["fields"]] == ["city"]
    id_col = next(c for c in data["columns"] if c["name"] == "id")
    assert id_col["mode"] == "REQUIRED"


def test_update_table_description_and_labels_merge(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1",
        "schema": "default",
        "table": "t1",
        "description": "new desc",
        "labels": {"set": {"owner": "alice"}},
    })
    payload = _text_payload(r)
    assert payload.get("success") is True, payload
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    assert sent.description == "new desc"
    # default mode = merge: existing key preserved, new key added
    assert sent.labels == {"env": "prod", "owner": "alice"}


def test_update_table_labels_replace(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "labels": {"set": {"owner": "alice"}, "mode": "replace"},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    assert sent.labels == {"owner": "alice"}


def test_update_table_labels_delete(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "labels": {"set": {"env": "ignored"}, "mode": "delete"},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    assert sent.labels == {}


def test_update_table_column_description_nested(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"setComments": {"id": "primary id", "addr.city": "city name"}},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    id_field = next(f for f in sent.table_schema.fields if f.field_name == "id")
    assert id_field.description == "primary id"
    addr = next(f for f in sent.table_schema.fields if f.field_name == "addr")
    city = next(f for f in addr.fields if f.field_name == "city")
    assert city.description == "city name"


def test_update_table_set_nullable(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"setNullable": ["id"]},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    id_field = next(f for f in sent.table_schema.fields if f.field_name == "id")
    assert id_field.mode == "NULLABLE"


def test_update_table_set_nullable_rejects_nested(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"setNullable": ["addr.city"]},
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "nested" in payload["error"].lower()
    meta_tools.sdk.client.update_table.assert_not_called()


def test_update_table_add_columns(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "age", "type": "BIGINT", "description": "years"}]},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    names = [f.field_name for f in sent.table_schema.fields]
    assert names == ["id", "addr", "age"]
    age = sent.table_schema.fields[-1]
    assert age.mode == "NULLABLE"
    assert age.description == "years"
    assert age.sql_type_definition == "BIGINT"
    # type_category must be set so the Catalog PUT API accepts the new field
    assert age.type_category == "BIGINT", (
        f"Expected type_category='BIGINT' for new BIGINT column, got {age.type_category!r}"
    )


def test_update_table_add_columns_rejects_duplicate(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "id", "type": "BIGINT"}]},
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "already exists" in payload["error"]


def test_update_table_expiration(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "expiration": {"days": 7, "partitionDays": 3},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    assert sent.expiration_options.expiration_days == 7
    assert sent.expiration_options.partition_expiration_days == 3


def test_update_table_expiration_rejects_negative(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "expiration": {"days": -1},
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert ">=" in payload["error"] or "0" in payload["error"]
    meta_tools.sdk.client.update_table.assert_not_called()


def test_update_table_no_fields_errors(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    # No get_table / update_table should happen when the plan is empty.
    meta_tools.sdk.client.update_table.assert_not_called()


def test_update_table_description_null_rejected(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "description": None,
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "null" in payload["error"].lower()
    meta_tools.sdk.client.update_table.assert_not_called()


def test_update_table_uses_latest_etag(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "description": "new",
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    # etag copied from the get_table response, not supplied by caller
    assert sent.etag == "etag-v1"


def test_update_table_etag_override(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "description": "new",
        "etag": "forced-etag",
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    assert sent.etag == "forced-etag"


def test_update_table_add_and_set_comment_same_request(meta_tools: Tools) -> None:
    """columns.add + setComments for the new column in one request must succeed.

    Bug: _apply_plan used to run setComments before add, so targeting a
    newly-added column would raise 'column path not found' even though the
    column was present in the same request.
    Fix: add is now executed first.
    """
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {
            "add": [{"name": "age", "type": "BIGINT"}],
            "setComments": {"age": "用户年龄"},
        },
    })
    payload = _text_payload(r)
    assert payload["success"] is True, (
        f"add+setComments for new column should succeed, got: {payload.get('error')}"
    )
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    age = next(f for f in sent.table_schema.fields if f.field_name == "age")
    assert age.type_category == "BIGINT"
    assert age.description == "用户年龄"


def test_update_table_add_and_set_nullable_same_request(meta_tools: Tools) -> None:
    """columns.add + setNullable for the new column in one request must succeed.

    New columns are NULLABLE by default, but setNullable on a just-added column
    should not fail — add runs before setNullable.
    """
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {
            "add": [{"name": "score", "type": "DOUBLE"}],
            "setNullable": ["score"],
        },
    })
    payload = _text_payload(r)
    assert payload["success"] is True, (
        f"add+setNullable for new column should succeed, got: {payload.get('error')}"
    )
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    score = next(f for f in sent.table_schema.fields if f.field_name == "score")
    assert score.mode == "NULLABLE"


def test_update_table_set_comment_unknown_column_still_errors(meta_tools: Tools) -> None:
    """setComments for a column that truly doesn't exist must still fail."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"setComments": {"ghost": "desc"}},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "ghost" in payload["error"]
    meta_tools.sdk.client.update_table.assert_not_called()


def test_update_table_missing_column_errors(meta_tools: Tools) -> None:
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"setComments": {"nope": "x"}},
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "nope" in payload["error"]
    meta_tools.sdk.client.update_table.assert_not_called()


def test_update_table_response_includes_context_fields(meta_tools: Tools) -> None:
    """update_table response data must contain project/schema/table/updatedFields."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "description": "new",
    })
    payload = _text_payload(r)
    assert payload["success"] is True
    data = payload["data"]
    assert data["project"] == "p1"
    assert data["schema"] == "default"
    assert data["table"] == "t1"
    assert data["updatedFields"] == ["description"]
    # serialize_table_meta fields also present
    assert data["etag"] == "etag-v1"
    assert "columns" in data


def test_update_table_add_column_without_description(meta_tools: Tools) -> None:
    """columns.add without description -> SDK receives None (field omitted on serialization)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "score", "type": "DOUBLE"}]},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    score = next(f for f in sent.table_schema.fields if f.field_name == "score")
    assert score.description is None
    # type_category must also be set for DOUBLE
    assert score.type_category == "DOUBLE", (
        f"Expected type_category='DOUBLE' for new DOUBLE column, got {score.type_category!r}"
    )


# ============================================================================
# tools_table_meta.py — additional coverage for uncovered branches
# ============================================================================


def test_find_field_by_path_empty_path() -> None:
    """_find_field_by_path returns None for empty path."""
    from maxcompute_catalog_mcp.tools_table_meta import _find_field_by_path
    assert _find_field_by_path([], []) is None


def test_update_table_get_table_fails(meta_tools: Tools) -> None:
    """get_table raises exception → error response (L181-183)."""
    meta_tools.sdk.client.get_table.side_effect = RuntimeError("table not found")
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "description": "new",
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "Failed to fetch current table state" in payload["error"]


def test_update_table_sdk_update_fails(meta_tools: Tools) -> None:
    """sdk.client.update_table raises exception → error response (L198-200)."""
    meta_tools.sdk.client.update_table.side_effect = RuntimeError("server error")
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "description": "new",
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "server error" in payload["error"]


def test_update_table_description_not_string(meta_tools: Tools) -> None:
    """description is not a string → ValueError (L234)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "description": 123,
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "must be a string" in payload["error"]


def test_update_table_labels_not_dict(meta_tools: Tools) -> None:
    """labels is not a dict → ValueError (L240)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "labels": "invalid",
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "labels must be an object" in payload["error"]


def test_update_table_labels_set_not_dict(meta_tools: Tools) -> None:
    """labels.set is not a dict → ValueError (L246)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "labels": {"set": "invalid"},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "labels.set must be an object" in payload["error"]


def test_update_table_labels_invalid_mode(meta_tools: Tools) -> None:
    """labels.mode is invalid → ValueError (L248)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "labels": {"set": {"k": "v"}, "mode": "upsert"},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "labels.mode must be one of" in payload["error"]


def test_update_table_expiration_not_dict(meta_tools: Tools) -> None:
    """expiration is not a dict → ValueError (L259)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "expiration": "invalid",
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "expiration must be an object" in payload["error"]


def test_update_table_expiration_non_integer(meta_tools: Tools) -> None:
    """expiration.days is not an integer → ValueError (L263)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "expiration": {"days": "abc"},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "expiration.days must be an integer" in payload["error"]


def test_update_table_expiration_partition_days_negative(meta_tools: Tools) -> None:
    """expiration.partitionDays is negative → ValueError (L266-267)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "expiration": {"partitionDays": -5},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert ">=" in payload["error"]


def test_update_table_columns_not_dict(meta_tools: Tools) -> None:
    """columns is not a dict → ValueError (L279)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": "invalid",
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "columns must be an object" in payload["error"]


def test_update_table_set_comments_not_dict(meta_tools: Tools) -> None:
    """columns.setComments is not a dict → ValueError (L287)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"setComments": "invalid"},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "columns.setComments must be an object" in payload["error"]


def test_update_table_set_nullable_not_list(meta_tools: Tools) -> None:
    """columns.setNullable is not a list → ValueError (L295)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"setNullable": "invalid"},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "columns.setNullable must be an array" in payload["error"]


def test_update_table_columns_add_not_list(meta_tools: Tools) -> None:
    """columns.add is not a list → ValueError (L307)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": "invalid"},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "columns.add must be an array" in payload["error"]


def test_update_table_columns_add_item_not_dict(meta_tools: Tools) -> None:
    """columns.add[i] is not a dict → ValueError (L311)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": ["not_a_dict"]},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "must be an object" in payload["error"]


def test_update_table_columns_add_missing_name(meta_tools: Tools) -> None:
    """columns.add[i] missing 'name' → ValueError (L313)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"type": "BIGINT"}]},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "missing 'name'" in payload["error"]


def test_update_table_columns_add_missing_type(meta_tools: Tools) -> None:
    """columns.add[i] missing 'type' → ValueError (L315)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "col"}]},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "missing 'type'" in payload["error"]


def test_update_table_set_nullable_column_not_found(meta_tools: Tools) -> None:
    """setNullable for non-existent column → ValueError (L377)."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"setNullable": ["nonexistent_col"]},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "not found" in payload["error"]


def test_update_table_ensure_fields_schema_none(meta_tools: Tools) -> None:
    """Table with table_schema=None → _ensure_fields creates schema (L415)."""
    from pyodps_catalog import models as catalog_models
    # Build a table model with no table_schema
    current = catalog_models.Table(
        project_id="p1", schema_name="default", table_name="t1",
    )
    assert current.table_schema is None
    meta_tools.sdk.client.get_table.return_value = current
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "newcol", "type": "STRING"}]},
    })
    payload = _text_payload(r)
    assert payload["success"] is True


def test_update_table_ensure_fields_fields_none(meta_tools: Tools) -> None:
    """Table with table_schema.fields=None → _ensure_fields creates list (L417)."""
    from pyodps_catalog import models as catalog_models
    current = catalog_models.Table(
        project_id="p1", schema_name="default", table_name="t1",
    )
    schema = catalog_models.TableFieldSchema()
    schema.fields = None
    current.table_schema = schema
    meta_tools.sdk.client.get_table.return_value = current
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "newcol", "type": "STRING"}]},
    })
    payload = _text_payload(r)
    assert payload["success"] is True


def test_update_table_add_columns_type_category_parametrized(meta_tools: Tools) -> None:
    """columns.add: type_category must strip parameters/generics from the type string.

    Why this test exists:
    The Catalog PUT API rejects new columns unless typeCategory is set to the
    base type (without precision/scale/generic parameters). Before the fix,
    type_category was never set, causing 400 errors in production when callers
    added DECIMAL, VARCHAR, or ARRAY columns.
    """
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [
            {"name": "price",  "type": "DECIMAL(10,2)"},
            {"name": "tag",    "type": "VARCHAR(255)"},
            {"name": "items",  "type": "ARRAY<STRING>"},
            {"name": "kv",     "type": "MAP<STRING,BIGINT>"},
        ]},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    fields = {f.field_name: f for f in sent.table_schema.fields}

    assert fields["price"].type_category == "DECIMAL", (
        f"DECIMAL(10,2) base type should be 'DECIMAL', got {fields['price'].type_category!r}"
    )
    assert fields["tag"].type_category == "VARCHAR", (
        f"VARCHAR(255) base type should be 'VARCHAR', got {fields['tag'].type_category!r}"
    )
    assert fields["items"].type_category == "ARRAY", (
        f"ARRAY<STRING> base type should be 'ARRAY', got {fields['items'].type_category!r}"
    )
    assert fields["kv"].type_category == "MAP", (
        f"MAP<STRING,BIGINT> base type should be 'MAP', got {fields['kv'].type_category!r}"
    )


# ---------------------------------------------------------------------------
# Unit tests for _parse_sql_type parser
# ---------------------------------------------------------------------------

class TestParseSqlType:
    """Unit tests for the module-level _parse_sql_type() parser."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from maxcompute_catalog_mcp.tools_table_meta import _parse_sql_type
        self.parse = _parse_sql_type

    # -- simple types --

    @pytest.mark.parametrize("type_str", [
        "BIGINT", "INT", "TINYINT", "SMALLINT",
        "FLOAT", "DOUBLE",
        "BOOLEAN",
        "STRING", "BINARY",
        "DATE", "DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ",
        "JSON", "BLOB",
        "INTERVAL_DAY_TIME", "INTERVAL_YEAR_MONTH",
    ])
    def test_simple_types(self, type_str):
        f = self.parse(type_str)
        assert f.type_category == type_str
        assert f.fields is None or f.fields == []

    def test_simple_type_case_insensitive(self):
        """Parser must accept lowercase or mixed-case input."""
        f = self.parse("bigint")
        assert f.type_category == "BIGINT"
        f2 = self.parse("String")
        assert f2.type_category == "STRING"

    def test_simple_type_whitespace_tolerant(self):
        f = self.parse("  BIGINT  ")
        assert f.type_category == "BIGINT"

    # -- parameterised types --

    def test_decimal(self):
        f = self.parse("DECIMAL(10,2)")
        assert f.type_category == "DECIMAL"
        assert f.precision == "10"
        assert f.scale == "2"

    def test_decimal_whitespace(self):
        f = self.parse("DECIMAL( 18 , 6 )")
        assert f.type_category == "DECIMAL"
        assert f.precision == "18"
        assert f.scale == "6"

    def test_varchar(self):
        f = self.parse("VARCHAR(255)")
        assert f.type_category == "VARCHAR"
        assert f.max_length == "255"

    def test_char(self):
        f = self.parse("CHAR(10)")
        assert f.type_category == "CHAR"
        assert f.max_length == "10"

    # -- ARRAY --

    def test_array_string(self):
        f = self.parse("ARRAY<STRING>")
        assert f.type_category == "ARRAY"
        assert len(f.fields) == 1
        elem = f.fields[0]
        assert elem.field_name == "element"
        assert elem.type_category == "STRING"
        assert elem.mode is None  # Catalog API: mode only on top-level fields

    def test_array_decimal(self):
        f = self.parse("ARRAY<DECIMAL(10,2)>")
        assert f.type_category == "ARRAY"
        elem = f.fields[0]
        assert elem.field_name == "element"
        assert elem.type_category == "DECIMAL"
        assert elem.precision == "10"
        assert elem.scale == "2"

    # -- MAP --

    def test_map_string_bigint(self):
        f = self.parse("MAP<STRING,BIGINT>")
        assert f.type_category == "MAP"
        assert len(f.fields) == 2
        by_name = {c.field_name: c for c in f.fields}
        assert by_name["key"].type_category == "STRING"
        assert by_name["value"].type_category == "BIGINT"

    def test_map_key_value_modes(self):
        f = self.parse("MAP<STRING,BIGINT>")
        by_name = {c.field_name: c for c in f.fields}
        # Catalog API: mode only on top-level fields; nested key/value must be None
        assert by_name["key"].mode is None
        assert by_name["value"].mode is None

    # -- STRUCT --

    def test_struct_simple(self):
        f = self.parse("STRUCT<name:STRING,age:INT>")
        assert f.type_category == "STRUCT"
        assert len(f.fields) == 2
        by_name = {c.field_name: c for c in f.fields}
        assert by_name["name"].type_category == "STRING"
        assert by_name["age"].type_category == "INT"

    def test_struct_field_names_lowercased(self):
        """STRUCT field names are stored lowercase (canonical form)."""
        f = self.parse("STRUCT<MyField:STRING>")
        assert f.fields[0].field_name == "myfield"

    def test_struct_field_modes(self):
        f = self.parse("STRUCT<a:INT,b:STRING>")
        for sub in f.fields:
            # Catalog API: mode only on top-level fields; nested fields must be None
            assert sub.mode is None

    # -- nested complex types --

    def test_array_of_struct(self):
        f = self.parse("ARRAY<STRUCT<a:INT,b:VARCHAR(64)>>")
        assert f.type_category == "ARRAY"
        elem = f.fields[0]
        assert elem.field_name == "element"
        assert elem.type_category == "STRUCT"
        by_name = {c.field_name: c for c in elem.fields}
        assert by_name["a"].type_category == "INT"
        assert by_name["b"].type_category == "VARCHAR"
        assert by_name["b"].max_length == "64"

    def test_map_string_array(self):
        f = self.parse("MAP<STRING,ARRAY<DECIMAL(10,2)>>")
        assert f.type_category == "MAP"
        by_name = {c.field_name: c for c in f.fields}
        val = by_name["value"]
        assert val.type_category == "ARRAY"
        assert val.fields[0].type_category == "DECIMAL"
        assert val.fields[0].precision == "10"

    def test_struct_with_complex_children(self):
        f = self.parse("STRUCT<tags:ARRAY<STRING>,meta:MAP<STRING,BIGINT>>")
        assert f.type_category == "STRUCT"
        by_name = {c.field_name: c for c in f.fields}
        assert by_name["tags"].type_category == "ARRAY"
        assert by_name["tags"].fields[0].type_category == "STRING"
        assert by_name["meta"].type_category == "MAP"

    def test_deeply_nested(self):
        """ARRAY<MAP<STRING,STRUCT<x:INT>>> - 3 levels deep."""
        f = self.parse("ARRAY<MAP<STRING,STRUCT<x:INT>>>")
        assert f.type_category == "ARRAY"
        map_f = f.fields[0]
        assert map_f.type_category == "MAP"
        by_name = {c.field_name: c for c in map_f.fields}
        assert by_name["value"].type_category == "STRUCT"
        assert by_name["value"].fields[0].field_name == "x"

    # -- error cases --

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown type"):
            self.parse("NOSUCHTYPE")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="empty"):
            self.parse("")

    def test_decimal_missing_scale_raises(self):
        with pytest.raises(ValueError, match="precision, scale"):
            self.parse("DECIMAL(10)")

    def test_decimal_empty_params_raises(self):
        with pytest.raises(ValueError, match="precision, scale"):
            self.parse("DECIMAL()")

    def test_varchar_empty_length_raises(self):
        with pytest.raises(ValueError, match="length"):
            self.parse("VARCHAR()")

    def test_type_with_params_and_wrong_syntax_raises(self):
        """BIGINT(10) is invalid — BIGINT does not accept parameters."""
        with pytest.raises(ValueError):
            self.parse("BIGINT(10)")

    def test_map_wrong_arg_count_raises(self):
        with pytest.raises(ValueError, match="two type arguments"):
            self.parse("MAP<STRING,BIGINT,INT>")

    def test_map_single_arg_raises(self):
        with pytest.raises(ValueError, match="two type arguments"):
            self.parse("MAP<STRING>")

    def test_struct_missing_colon_raises(self):
        with pytest.raises(ValueError, match="name:type"):
            self.parse("STRUCT<name STRING>")

    def test_struct_empty_raises(self):
        with pytest.raises(ValueError, match="at least one field"):
            self.parse("STRUCT<>")

    def test_unmatched_bracket_raises(self):
        with pytest.raises(ValueError, match="Unmatched"):
            self.parse("ARRAY<STRING")

    def test_unknown_complex_base_raises(self):
        with pytest.raises(ValueError, match="Unknown complex type"):
            self.parse("LIST<STRING>")

    # -- nesting depth guard --

    def test_nesting_depth_at_exact_limit(self):
        """Nesting at exactly _MAX_TYPE_NESTING_DEPTH complex-type layers must succeed."""
        from maxcompute_catalog_mcp.tools_table_meta import _MAX_TYPE_NESTING_DEPTH
        # Guard is _depth > _MAX_TYPE_NESTING_DEPTH, so depth == _MAX_TYPE_NESTING_DEPTH
        # is still allowed.  _MAX_TYPE_NESTING_DEPTH ARRAY wrappings drive the
        # innermost _parse_sql_type call to _depth == _MAX_TYPE_NESTING_DEPTH.
        type_str = "ARRAY<" * _MAX_TYPE_NESTING_DEPTH + "INT" + ">" * _MAX_TYPE_NESTING_DEPTH
        f = self.parse(type_str)
        assert f.type_category == "ARRAY"

    def test_nesting_depth_one_over_limit_raises(self):
        """One level over _MAX_TYPE_NESTING_DEPTH must raise ValueError."""
        from maxcompute_catalog_mcp.tools_table_meta import _MAX_TYPE_NESTING_DEPTH
        type_str = "ARRAY<" * (_MAX_TYPE_NESTING_DEPTH + 1) + "INT" + ">" * (_MAX_TYPE_NESTING_DEPTH + 1)
        with pytest.raises(ValueError, match="nesting depth"):
            self.parse(type_str)


# ---------------------------------------------------------------------------
# Integration tests: columns.add with complex types via update_table tool
# ---------------------------------------------------------------------------

def test_update_table_add_column_decimal_sets_precision_scale(meta_tools: Tools) -> None:
    """columns.add with DECIMAL(p,s): new field must have precision + scale set."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "price", "type": "DECIMAL(18,6)"}]},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    price = next(f for f in sent.table_schema.fields if f.field_name == "price")
    assert price.type_category == "DECIMAL"
    assert price.precision == "18"
    assert price.scale == "6"
    assert price.mode == "NULLABLE"


def test_update_table_add_column_varchar_sets_max_length(meta_tools: Tools) -> None:
    """columns.add with VARCHAR(n): new field must have max_length set."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "tag", "type": "VARCHAR(255)"}]},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    tag = next(f for f in sent.table_schema.fields if f.field_name == "tag")
    assert tag.type_category == "VARCHAR"
    assert tag.max_length == "255"


def test_update_table_add_column_array_type(meta_tools: Tools) -> None:
    """columns.add with ARRAY<STRING>: must produce one child field named 'element'."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "items", "type": "ARRAY<STRING>"}]},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    items = next(f for f in sent.table_schema.fields if f.field_name == "items")
    assert items.type_category == "ARRAY"
    assert items.mode == "NULLABLE"
    assert len(items.fields) == 1
    elem = items.fields[0]
    assert elem.field_name == "element"
    assert elem.type_category == "STRING"


def test_update_table_add_column_map_type(meta_tools: Tools) -> None:
    """columns.add with MAP<STRING,BIGINT>: must have 'key' and 'value' child fields."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "kv", "type": "MAP<STRING,BIGINT>"}]},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    kv = next(f for f in sent.table_schema.fields if f.field_name == "kv")
    assert kv.type_category == "MAP"
    by_name = {c.field_name: c for c in kv.fields}
    assert set(by_name.keys()) == {"key", "value"}
    assert by_name["key"].type_category == "STRING"
    assert by_name["value"].type_category == "BIGINT"


def test_update_table_add_column_struct_type(meta_tools: Tools) -> None:
    """columns.add with STRUCT<...>: must produce named child fields, each with typeCategory."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "addr", "type": "STRUCT<city:STRING,zip:INT>"}]},
    })
    # Note: 'addr' also exists in the original mock table; check for struct-typed one
    payload = _text_payload(r)
    # The mock table already has 'addr' — this should fail with duplicate error
    assert payload["success"] is False
    assert "already exists" in payload["error"]
    meta_tools.sdk.client.update_table.assert_not_called()


def test_update_table_add_column_struct_type_new_name(meta_tools: Tools) -> None:
    """columns.add with STRUCT<...>: new column, must produce named child fields."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "location", "type": "STRUCT<city:STRING,zip:INT>"}]},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    loc = next(f for f in sent.table_schema.fields if f.field_name == "location")
    assert loc.type_category == "STRUCT"
    by_name = {c.field_name: c for c in loc.fields}
    assert by_name["city"].type_category == "STRING"
    assert by_name["zip"].type_category == "INT"


def test_update_table_add_column_nested_array_struct(meta_tools: Tools) -> None:
    """columns.add with ARRAY<STRUCT<...>>: nested field must be fully populated."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "records", "type": "ARRAY<STRUCT<a:INT,b:VARCHAR(64)>>"}]},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    records = next(f for f in sent.table_schema.fields if f.field_name == "records")
    assert records.type_category == "ARRAY"
    elem = records.fields[0]
    assert elem.field_name == "element"
    assert elem.type_category == "STRUCT"
    by_name = {c.field_name: c for c in elem.fields}
    assert by_name["a"].type_category == "INT"
    assert by_name["b"].type_category == "VARCHAR"
    assert by_name["b"].max_length == "64"


def test_update_table_add_column_invalid_type_returns_error(meta_tools: Tools) -> None:
    """columns.add with unknown type: must return success=False, no SDK call."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "x", "type": "NOSUCHTYPE"}]},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "NOSUCHTYPE" in payload["error"] or "unknown" in payload["error"].lower()
    meta_tools.sdk.client.update_table.assert_not_called()


def test_update_table_add_column_invalid_decimal_returns_error(meta_tools: Tools) -> None:
    """columns.add with DECIMAL(10) — missing scale — must return error."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "x", "type": "DECIMAL(10)"}]},
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "DECIMAL" in payload["error"]
    meta_tools.sdk.client.update_table.assert_not_called()


def test_update_table_add_column_case_insensitive_type(meta_tools: Tools) -> None:
    """columns.add: lowercase type string 'bigint' must be accepted and normalized."""
    r = meta_tools.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "cnt", "type": "bigint"}]},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools.sdk.client.update_table.call_args.args[0]
    cnt = next(f for f in sent.table_schema.fields if f.field_name == "cnt")
    assert cnt.type_category == "BIGINT"


@pytest.fixture
def meta_tools_pyodps_table(mock_sdk: MagicMock, mock_maxcompute_client: MagicMock) -> Tools:
    """Tools fixture simulating a table originally created via PyODPS (not Catalog API).

    PyODPS-created tables return sql_type_definition in lowercase from Catalog GET
    (e.g. 'bigint'), while type_category is always uppercase ('BIGINT').
    E2E-confirmed: the Catalog PUT API is case-insensitive, so these values are
    passed through to PUT without modification.
    """
    from pyodps_catalog import models as catalog_models

    t = catalog_models.Table(
        project_id="p1", schema_name="default", table_name="t1",
        etag="etag-pyodps",
        description="created by pyodps",
    )
    schema = catalog_models.TableFieldSchema()
    # Simulate PyODPS GET response: type_category uppercase, sql_type_definition lowercase
    id_field = catalog_models.TableFieldSchema(field_name="id", type_category="BIGINT", mode="REQUIRED")
    id_field.sql_type_definition = "bigint"  # lowercase — real PyODPS table behaviour
    name_field = catalog_models.TableFieldSchema(field_name="name", type_category="STRING", mode="NULLABLE")
    name_field.sql_type_definition = None    # None — another possible PyODPS behaviour
    schema.fields = [id_field, name_field]
    t.table_schema = schema

    mock_sdk.client.get_table = MagicMock(return_value=t)
    mock_sdk.client.update_table = MagicMock(side_effect=lambda tbl: tbl)
    return Tools(
        sdk=mock_sdk,
        default_project="p1",
        namespace_id="test_namespace_id",
        maxcompute_client=mock_maxcompute_client,
        credential_client=None,
    )


def test_update_table_pyodps_fields_pass_through(meta_tools_pyodps_table: Tools) -> None:
    """update_table on PyODPS-created table: existing fields are passed through unchanged.

    E2E-confirmed: the Catalog PUT API is case-insensitive for sqlTypeDefinition.
    We trust the GET response and do NOT modify existing fields.
    """
    r = meta_tools_pyodps_table.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "description": "updated",
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools_pyodps_table.sdk.client.update_table.call_args.args[0]
    fields = {f.field_name: f for f in sent.table_schema.fields}

    # Existing fields passed through unchanged (server is case-insensitive)
    assert fields["id"].sql_type_definition == "bigint", (
        f"lowercase sqlTypeDefinition should be preserved as-is, got {fields['id'].sql_type_definition!r}"
    )
    assert fields["name"].sql_type_definition is None, (
        f"None sqlTypeDefinition should be preserved as-is, got {fields['name'].sql_type_definition!r}"
    )


def test_update_table_add_column_on_pyodps_table(meta_tools_pyodps_table: Tools) -> None:
    """columns.add on PyODPS-created table: new column must have type_category set.

    The real bug: columns.add did not set type_category, causing Catalog PUT API 400.
    Existing fields are passed through unchanged (server is case-insensitive).
    """
    r = meta_tools_pyodps_table.call("update_table", {
        "project": "p1", "schema": "default", "table": "t1",
        "columns": {"add": [{"name": "age", "type": "BIGINT", "description": "user age"}]},
    })
    assert _text_payload(r)["success"] is True
    sent = meta_tools_pyodps_table.sdk.client.update_table.call_args.args[0]
    fields = {f.field_name: f for f in sent.table_schema.fields}

    # New column has both sql_type_definition and type_category
    assert fields["age"].sql_type_definition == "BIGINT"
    assert fields["age"].type_category == "BIGINT", (
        f"New column must have type_category='BIGINT', got {fields['age'].type_category!r}. "
        "Without type_category the Catalog PUT API returns 400."
    )
    assert fields["age"].description == "user age"


def test_execute_sql_maxcu_exceeds_limit(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """maxCU check: estimatedCU > maxCU → overLimit response."""
    cost_obj = MagicMock()
    cost_obj.input_size = 10 * (1024 ** 3)  # 10 GB
    cost_obj.complexity = 5.0
    cost_obj.udf_num = 0
    mock_maxcompute_client.execute_sql_cost.return_value = cost_obj
    r = tools.call("execute_sql", {"project": "p1", "sql": "SELECT 1", "maxCU": 1})
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert payload.get("overLimit") is True
    assert payload.get("suggestedMaxCU") is not None


def test_execute_sql_maxcu_invalid_type(tools: Tools) -> None:
    """maxCU with invalid string → ignored (not crash); async flow returns instanceId."""
    r = tools.call("execute_sql", {"project": "p1", "sql": "SELECT 1", "maxCU": "abc"})
    payload = _text_payload(r)
    # maxCU becomes None after int() fails; no overLimit check, async flow succeeds
    assert "overLimit" not in payload
    assert "instanceId" in payload


def test_execute_sql_no_compute_returns_unsupported(tools_no_compute: Tools) -> None:
    """execute_sql without compute client returns unsupported."""
    r = tools_no_compute.call("execute_sql", {"project": "p1", "sql": "SELECT 1"})
    payload = _text_payload(r)
    assert payload.get("error") == "unsupported"
    assert "unsupported" in payload.get("message", "").lower() or \
           "compute" in payload.get("message", "").lower()


def test_execute_sql_sync_with_structured_reader(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """Sync mode: open_reader with schema → structured columns + data."""
    inst = mock_maxcompute_client.run_sql.return_value
    inst.is_terminated.return_value = True

    reader_mock = MagicMock()
    schema_mock = MagicMock()
    col_a = MagicMock(); col_a.name = "col_a"
    col_b = MagicMock(); col_b.name = "col_b"
    schema_mock.columns = [col_a, col_b]
    reader_mock._schema = schema_mock

    record = MagicMock()
    record.__getitem__ = lambda self, key: {"col_a": 42, "col_b": "hello"}[key]
    reader_mock.__iter__ = lambda self: iter([record])
    reader_mock.__enter__ = lambda self: self
    reader_mock.__exit__ = MagicMock(return_value=False)
    inst.open_reader.return_value = reader_mock

    r = tools.call("execute_sql", {"project": "p1", "sql": "SELECT 1", "async": False})
    payload = _text_payload(r)
    assert payload.get("success") is True
    assert payload.get("columns") == ["col_a", "col_b"]
    assert len(payload.get("data", [])) == 1


def test_execute_sql_compute_client_none_for_project(tools: Tools, monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_compute_client_for_project returns None → error."""
    monkeypatch.setattr(tools, "_get_compute_client_for_project", lambda p: None)
    r = tools.call("execute_sql", {"project": "other_project", "sql": "SELECT 1"})
    payload = _text_payload(r)
    assert payload.get("success") is False


def test_execute_sql_custom_hints(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """Custom hints are merged with defaults."""
    r = tools.call("execute_sql", {
        "project": "p1", "sql": "SELECT 1",
        "hints": {"odps.sql.hive.compatible": "true"},
    })
    payload = _text_payload(r)
    # Async mode should return instanceId without error
    assert "instanceId" in payload
    assert "error" not in payload
    call_kwargs = mock_maxcompute_client.run_sql.call_args
    hints = call_kwargs.kwargs.get("hints") or call_kwargs[1].get("hints", {})
    assert hints.get("odps.sql.hive.compatible") == "true"
    assert hints.get("odps.sql.submit.mode") == "script"


def test_execute_sql_invalid_timeout(tools: Tools) -> None:
    """Invalid timeout in sync mode → error."""
    r = tools.call("execute_sql", {
        "project": "p1", "sql": "SELECT 1", "async": False, "timeout": "abc",
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "timeout" in payload.get("error", "").lower()


def test_cost_sql_no_compute_stub(tools_no_compute: Tools) -> None:
    """cost_sql without compute returns stub with message."""
    r = tools_no_compute.call("cost_sql", {"project": "p1", "sql": "SELECT 1"})
    payload = _text_payload(r)
    estimate = payload.get("costEstimate", {})
    assert estimate.get("stub") is True
    assert "message" in estimate


def test_cost_sql_compute_client_none(tools: Tools, monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_compute_client_for_project returns None → stub."""
    monkeypatch.setattr(tools, "_get_compute_client_for_project", lambda p: None)
    r = tools.call("cost_sql", {"project": "p1", "sql": "SELECT 1"})
    payload = _text_payload(r)
    assert payload["costEstimate"].get("stub") is True


def test_cost_sql_estimation_exception(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """execute_sql_cost raises → fallback stub."""
    mock_maxcompute_client.execute_sql_cost.side_effect = RuntimeError("cost error")
    r = tools.call("cost_sql", {"project": "p1", "sql": "SELECT 1"})
    payload = _text_payload(r)
    assert payload["costEstimate"].get("stub") is True
    assert "cost error" in payload["costEstimate"].get("message", "")


def test_get_instance_not_terminated(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """Instance not terminated → message about waiting."""
    inst = mock_maxcompute_client.get_instance.return_value
    inst.is_terminated.return_value = False
    r = tools.call("get_instance", {"project": "p1", "instanceId": "inst-001"})
    payload = _text_payload(r)
    assert "not terminated" in payload.get("message", "").lower() or "wait" in payload.get("message", "").lower()


def test_get_instance_no_results(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """Terminated instance with no task results."""
    inst = mock_maxcompute_client.get_instance.return_value
    inst.is_terminated.return_value = True
    inst.get_task_results.return_value = {}
    r = tools.call("get_instance", {"project": "p1", "instanceId": "inst-001"})
    payload = _text_payload(r)
    assert "No task results" in payload.get("message", "") or payload.get("results") == {}


def test_get_instance_status_no_compute(tools_no_compute: Tools) -> None:
    """get_instance_status without compute → unsupported."""
    r = tools_no_compute.call("get_instance_status", {"project": "p1", "instanceId": "inst-001"})
    payload = _text_payload(r)
    assert payload.get("error") == "unsupported"


def test_get_instance_no_compute(tools_no_compute: Tools) -> None:
    """get_instance without compute → unsupported."""
    r = tools_no_compute.call("get_instance", {"project": "p1", "instanceId": "inst-001"})
    payload = _text_payload(r)
    assert payload.get("error") == "unsupported"


def test_get_instance_status_exception(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """get_instance raises → error response."""
    mock_maxcompute_client.get_instance.side_effect = RuntimeError("instance error")
    r = tools.call("get_instance_status", {"project": "p1", "instanceId": "inst-002"})
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "instance error" in payload.get("error", "")


def test_get_instance_logview_address_fails(tools: Tools, mock_maxcompute_client: MagicMock) -> None:
    """get_logview_address exception → logView=None."""
    inst = mock_maxcompute_client.get_instance.return_value
    inst.get_logview_address.side_effect = RuntimeError("logview error")
    r = tools.call("get_instance_status", {"project": "p1", "instanceId": "inst-001"})
    payload = _text_payload(r)
    assert payload.get("logView") is None


# ============================================================================
# tools.py — _get_compute_client_for_project() coverage
# ============================================================================


def test_get_compute_client_default_project(tools: Tools) -> None:
    """project == default_project → returns maxcompute_client."""
    result = tools._get_compute_client_for_project("p1")
    assert result is tools.maxcompute_client


def test_get_compute_client_empty_project(tools: Tools) -> None:
    """Empty project → returns maxcompute_client."""
    result = tools._get_compute_client_for_project("")
    assert result is tools.maxcompute_client


def test_get_compute_client_no_maxcompute_client(tools_no_compute: Tools) -> None:
    """maxcompute_client=None, non-default project → returns None."""
    result = tools_no_compute._get_compute_client_for_project("other_proj")
    assert result is None


# Note: real cache write + reuse + LRU behavior is exercised in TestComputeClientCache below.


# ============================================================================
# _create_odps_client_with_credentials fallback paths
# ============================================================================


class TestCreateOdpsClientFallback:
    """Cover the three credential fallback paths in _create_odps_client_with_credentials."""

    @staticmethod
    def _make_tools(mock_sdk, credential_client=None):
        mc = MagicMock()
        return Tools(
            sdk=mock_sdk, default_project="p1",
            maxcompute_client=mc, credential_client=credential_client,
        )

    def test_credential_client_fallback(self, mock_sdk: MagicMock) -> None:
        """underlying.account is None → use credential_client + CredentialProviderAccount."""
        cred_client = MagicMock()
        t = self._make_tools(mock_sdk, credential_client=cred_client)
        underlying = MagicMock()
        underlying.account = None  # skip first path

        with patch("maxcompute_catalog_mcp.tools.ODPS") as mock_odps, \
             patch("odps.accounts.CredentialProviderAccount") as mock_cpa:
            result = t._create_odps_client_with_credentials(underlying, "proj2", "http://ep")
            mock_cpa.assert_called_once_with(cred_client)
            mock_odps.assert_called_once()
            assert mock_odps.call_args.kwargs["account"] is mock_cpa.return_value
            assert mock_odps.call_args.kwargs["project"] == "proj2"
            assert result is mock_odps.return_value

    def test_credential_provider_import_error(self, mock_sdk: MagicMock) -> None:
        """CredentialProviderAccount not importable → RuntimeError."""
        import types
        cred_client = MagicMock()
        t = self._make_tools(mock_sdk, credential_client=cred_client)
        underlying = MagicMock()
        underlying.account = None

        # Replace odps.accounts with a fake module that lacks CredentialProviderAccount
        fake_accounts = types.ModuleType("odps.accounts")
        with patch.dict("sys.modules", {"odps.accounts": fake_accounts}):
            with pytest.raises(RuntimeError, match="CredentialProviderAccount not available"):
                t._create_odps_client_with_credentials(underlying, "proj2", "http://ep")

    def test_default_chain_with_sts(self, mock_sdk: MagicMock) -> None:
        """No account, no credential_client → default chain returns STS creds."""
        from maxcompute_catalog_mcp.credentials import ResolvedCredentials
        t = self._make_tools(mock_sdk, credential_client=None)
        underlying = MagicMock()
        underlying.account = None

        creds = ResolvedCredentials(access_key_id="ak", access_key_secret="sk", security_token="tok")
        with patch("maxcompute_catalog_mcp.credentials.get_credentials_from_default_chain", return_value=creds), \
             patch("maxcompute_catalog_mcp.tools.ODPS") as mock_odps, \
             patch("odps.accounts.StsAccount") as mock_sts:
            result = t._create_odps_client_with_credentials(underlying, "proj2", "http://ep")
            mock_sts.assert_called_once_with("ak", "sk", "tok")
            assert mock_odps.call_args.kwargs["account"] is mock_sts.return_value
            assert result is mock_odps.return_value

    def test_default_chain_without_sts(self, mock_sdk: MagicMock) -> None:
        """No account, no credential_client → default chain returns AK/SK only."""
        from maxcompute_catalog_mcp.credentials import ResolvedCredentials
        t = self._make_tools(mock_sdk, credential_client=None)
        underlying = MagicMock()
        underlying.account = None

        creds = ResolvedCredentials(access_key_id="ak", access_key_secret="sk")
        with patch("maxcompute_catalog_mcp.credentials.get_credentials_from_default_chain", return_value=creds), \
             patch("maxcompute_catalog_mcp.tools.ODPS") as mock_odps:
            result = t._create_odps_client_with_credentials(underlying, "proj2", "http://ep")
            assert mock_odps.call_args.kwargs["access_id"] == "ak"
            assert mock_odps.call_args.kwargs["secret_access_key"] == "sk"
            assert result is mock_odps.return_value

    def test_default_chain_failure(self, mock_sdk: MagicMock) -> None:
        """Default chain raises → RuntimeError."""
        t = self._make_tools(mock_sdk, credential_client=None)
        underlying = MagicMock()
        underlying.account = None

        with patch("maxcompute_catalog_mcp.credentials.get_credentials_from_default_chain",
                   side_effect=ValueError("no creds")):
            with pytest.raises(RuntimeError, match="No valid credentials found"):
                t._create_odps_client_with_credentials(underlying, "proj2", "http://ep")


# ============================================================================
# _get_compute_client_for_project cache write + LRU eviction
# ============================================================================


class TestComputeClientCache:
    """Cover cache write, LRU eviction, and concurrent double-check lock."""

    @staticmethod
    def _make_tools(mock_sdk):
        mc = MagicMock()
        mc.odps_client = MagicMock()
        mc.odps_client.endpoint = "http://ep"
        return Tools(sdk=mock_sdk, default_project="p1", maxcompute_client=mc)

    def test_cache_write_and_reuse(self, mock_sdk: MagicMock) -> None:
        """New project → create client → cache → reuse on second call."""
        t = self._make_tools(mock_sdk)
        with patch.object(t, "_create_odps_client_with_credentials", return_value=MagicMock()):
            result1 = t._get_compute_client_for_project("proj_new")
            assert result1 is not None
            assert "proj_new" in t._compute_client_cache

            result2 = t._get_compute_client_for_project("proj_new")
            assert result2 is result1

    def test_lru_eviction(self, mock_sdk: MagicMock) -> None:
        """Cache exceeds max size → oldest entries evicted."""
        t = self._make_tools(mock_sdk)
        t._max_compute_client_cache_size = 3

        with patch.object(t, "_create_odps_client_with_credentials", return_value=MagicMock()):
            for i in range(5):
                t._get_compute_client_for_project(f"proj_{i}")

            assert len(t._compute_client_cache) == 3
            assert "proj_0" not in t._compute_client_cache
            assert "proj_1" not in t._compute_client_cache
            assert "proj_4" in t._compute_client_cache

    def test_concurrent_double_check(self, mock_sdk: MagicMock) -> None:
        """Another thread writes cache during lock acquisition → use cached value."""
        t = self._make_tools(mock_sdk)
        existing_client = MagicMock()

        def inject_cache_entry(*_args, **_kwargs):
            """Simulate concurrent write: inject cache entry during client creation."""
            t._compute_client_cache["contested_proj"] = existing_client
            return MagicMock()  # newly created client (will be discarded)

        with patch.object(t, "_create_odps_client_with_credentials", side_effect=inject_cache_entry):
            result = t._get_compute_client_for_project("contested_proj")
            assert result is existing_client

    def test_creation_failure_propagates(self, mock_sdk: MagicMock) -> None:
        """Client creation raises → RuntimeError propagated."""
        t = self._make_tools(mock_sdk)
        with patch.object(t, "_create_odps_client_with_credentials",
                         side_effect=ValueError("bad creds")):
            with pytest.raises(RuntimeError, match="Cannot create compute client"):
                t._get_compute_client_for_project("proj_fail")


# ============================================================================
# tools_compute.py — SQL truncation, execute_sql paths, get_instance reader
# ============================================================================


def test_cost_sql_truncated(tools: Tools) -> None:
    """SQL > 200 chars → sqlTruncated flag in response."""
    long_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(50)) + " FROM t1"
    assert len(long_sql) > 200
    r = tools.call("cost_sql", {"sql": long_sql})
    payload = _text_payload(r)
    d = _data(payload)
    assert d.get("sqlTruncated") is True
    assert len(d["sql"]) == 200


def test_execute_sql_sync_structured_reader(
    mock_sdk: MagicMock, mock_maxcompute_client: MagicMock,
) -> None:
    """Sync mode SELECT → structured reader returns columns + rows."""
    col_mock = MagicMock()
    col_mock.name = "val"
    schema_mock = MagicMock()
    schema_mock.columns = [col_mock]

    class FakeRecord:
        def __getitem__(self, key):
            return 42

    reader = MagicMock()
    reader._schema = schema_mock
    reader.__enter__ = MagicMock(return_value=reader)
    reader.__exit__ = MagicMock(return_value=False)
    reader.__iter__ = MagicMock(return_value=iter([FakeRecord()]))

    inst = MagicMock()
    inst.id = "inst-sync"
    inst.wait_for_success = MagicMock()
    inst.open_reader = MagicMock(return_value=reader)
    mock_maxcompute_client.run_sql.return_value = inst

    t = Tools(
        sdk=mock_sdk, default_project="p1",
        maxcompute_client=mock_maxcompute_client,
    )
    r = t.call("execute_sql", {"sql": "SELECT 1", "async": False})
    payload = _text_payload(r)
    assert payload["success"] is True
    assert payload["columns"] == ["val"]
    assert payload["data"] == [{"val": 42}]


def test_execute_sql_outer_exception(
    mock_sdk: MagicMock, mock_maxcompute_client: MagicMock,
) -> None:
    """_get_compute_client_for_project raises → caught by outer except."""
    t = Tools(
        sdk=mock_sdk, default_project="p1",
        maxcompute_client=mock_maxcompute_client,
    )
    with patch.object(t, "_get_compute_client_for_project",
                     side_effect=RuntimeError("client creation failed")):
        r = t.call("execute_sql", {"sql": "SELECT 1"})
        payload = _text_payload(r)
        assert payload["success"] is False
        assert "client creation failed" in payload["error"]


def test_get_instance_structured_reader(
    mock_sdk: MagicMock, mock_maxcompute_client: MagicMock,
) -> None:
    """get_instance with terminated instance + open_reader → structured data."""
    col1 = MagicMock()
    col1.name = "id"
    col2 = MagicMock()
    col2.name = "name"
    schema_mock = MagicMock()
    schema_mock.columns = [col1, col2]

    class FakeRecord:
        def __getitem__(self, key):
            return {"id": 1, "name": "alice"}[key]

    reader = MagicMock()
    reader._schema = schema_mock
    reader.__enter__ = MagicMock(return_value=reader)
    reader.__exit__ = MagicMock(return_value=False)
    reader.__iter__ = MagicMock(return_value=iter([FakeRecord()]))

    task_result = MagicMock()
    task_result.open_reader = MagicMock(return_value=reader)

    inst = MagicMock()
    inst.is_terminated.return_value = True
    inst.get_task_results.return_value = {"SQLTask": task_result}
    mock_maxcompute_client.get_instance.return_value = inst

    t = Tools(
        sdk=mock_sdk, default_project="p1",
        maxcompute_client=mock_maxcompute_client,
    )
    r = t.call("get_instance", {"instanceId": "inst-001"})
    payload = _text_payload(r)
    results = payload["results"]
    assert "SQLTask" in results
    assert results["SQLTask"]["columns"] == ["id", "name"]
    assert results["SQLTask"]["data"] == [{"id": 1, "name": "alice"}]


def test_get_instance_no_reader_fallback(
    mock_sdk: MagicMock, mock_maxcompute_client: MagicMock,
) -> None:
    """get_instance: task_result without open_reader → str() fallback."""
    inst = MagicMock()
    inst.is_terminated.return_value = True
    task_result = "raw text output"
    inst.get_task_results.return_value = {"TextTask": task_result}
    mock_maxcompute_client.get_instance.return_value = inst

    t = Tools(
        sdk=mock_sdk, default_project="p1",
        maxcompute_client=mock_maxcompute_client,
    )
    r = t.call("get_instance", {"instanceId": "inst-002"})
    payload = _text_payload(r)
    assert payload["results"]["TextTask"] == "raw text output"


def test_get_instance_reader_exception(
    mock_sdk: MagicMock, mock_maxcompute_client: MagicMock,
) -> None:
    """get_instance: open_reader raises → error dict per task."""
    task_result = MagicMock()
    task_result.open_reader.side_effect = RuntimeError("reader broken")

    inst = MagicMock()
    inst.is_terminated.return_value = True
    inst.get_task_results.return_value = {"BadTask": task_result}
    mock_maxcompute_client.get_instance.return_value = inst

    t = Tools(
        sdk=mock_sdk, default_project="p1",
        maxcompute_client=mock_maxcompute_client,
    )
    r = t.call("get_instance", {"instanceId": "inst-003"})
    payload = _text_payload(r)
    assert "error" in payload["results"]["BadTask"]
    assert "reader broken" in payload["results"]["BadTask"]["error"]
