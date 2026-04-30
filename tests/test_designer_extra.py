"""Additional boundary tests for tools_designer.py."""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import data as _data, text_payload as _text_payload


def _make_tools_with_compute() -> Tools:
    sdk = MagicMock()
    sdk.client.get_project.return_value = MagicMock(
        to_map=lambda: {"projectId": "p1", "schemaEnabled": True}
    )
    mc = MagicMock()
    inst = MagicMock()
    inst.id = "inst-001"
    inst.is_terminated.return_value = True
    inst.is_successful.return_value = True
    inst.get_task_results.return_value = {}
    mc.run_sql.return_value = inst
    mc.create_table = MagicMock()
    return Tools(sdk=sdk, default_project="p1", namespace_id="ns1", maxcompute_client=mc)


def test_create_table_invalid_table_properties_type() -> None:
    """tableProperties must be dict."""
    t = _make_tools_with_compute()
    r = t.call("create_table", {
        "project": "p1", "table": "t1",
        "columns": [{"name": "id", "type": "BIGINT"}],
        "tableProperties": "not-a-dict",
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "tableProperties" in payload.get("error", "")


def test_create_table_invalid_hints_type() -> None:
    """hints must be dict."""
    t = _make_tools_with_compute()
    r = t.call("create_table", {
        "project": "p1", "table": "t1",
        "columns": [{"name": "id", "type": "BIGINT"}],
        "hints": [1, 2, 3],
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "hints" in payload.get("error", "")


def test_create_table_invalid_primary_key_type() -> None:
    """primaryKey must be list."""
    t = _make_tools_with_compute()
    r = t.call("create_table", {
        "project": "p1", "table": "t1",
        "columns": [{"name": "id", "type": "BIGINT"}],
        "primaryKey": "id",
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "primaryKey" in payload.get("error", "")


def test_create_table_empty_primary_key_list() -> None:
    """primaryKey=[] normalized to None (no primary key)."""
    t = _make_tools_with_compute()
    r = t.call("create_table", {
        "project": "p1", "table": "t1",
        "columns": [{"name": "id", "type": "BIGINT"}],
        "primaryKey": [],
    })
    payload = _text_payload(r)
    assert payload.get("success") is True


def test_create_table_with_partition_columns() -> None:
    """Create table with partition columns."""
    t = _make_tools_with_compute()
    r = t.call("create_table", {
        "project": "p1", "schema": "default", "table": "pt",
        "columns": [{"name": "id", "type": "BIGINT"}],
        "partitionColumns": [{"name": "ds", "type": "STRING"}],
    })
    payload = _text_payload(r)
    assert payload.get("success") is True


def test_create_table_with_all_options() -> None:
    """Create table with lifecycle, comment, storageTier, transactional, primaryKey, tableProperties, hints."""
    t = _make_tools_with_compute()
    r = t.call("create_table", {
        "project": "p1", "schema": "default", "table": "full_t",
        "columns": [
            {"name": "id", "type": "BIGINT", "notNull": True},
            {"name": "name", "type": "STRING", "comment": "user name"},
        ],
        "lifecycle": 30,
        "comment": "test table",
        "storageTier": "standard",
        "transactional": True,
        "primaryKey": ["id"],
        "tableProperties": {"transactional": "true"},
        "hints": {"odps.sql.type.system.odps2": "true"},
    })
    payload = _text_payload(r)
    assert payload.get("success") is True
    # Verify create_table was called with primary_key and storage_tier
    mc = t.maxcompute_client
    call_kwargs = mc.create_table.call_args
    assert call_kwargs.kwargs, "create_table was called without keyword arguments"
    assert call_kwargs.kwargs.get("primary_key") == ["id"]
    assert call_kwargs.kwargs.get("storage_tier") == "standard"


def test_create_table_column_missing_name() -> None:
    """Column without 'name' → error."""
    t = _make_tools_with_compute()
    r = t.call("create_table", {
        "project": "p1", "table": "t1",
        "columns": [{"type": "BIGINT"}],  # no name
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "name" in payload.get("error", "").lower()


def test_create_table_string_column() -> None:
    """Column as plain string → name=string, type=STRING."""
    t = _make_tools_with_compute()
    r = t.call("create_table", {
        "project": "p1", "table": "t1",
        "columns": [{"name": "id", "type": "BIGINT"}],
        "partitionColumns": ["ds"],  # string partition column
    })
    payload = _text_payload(r)
    assert payload.get("success") is True


def test_create_table_columns_missing_key() -> None:
    """'columns' key not in args at all → error."""
    t = _make_tools_with_compute()
    r = t.call("create_table", {"project": "p1", "table": "t1"})
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "columns" in payload.get("error", "").lower()


def test_create_table_empty_columns() -> None:
    """columns=[] → error."""
    t = _make_tools_with_compute()
    r = t.call("create_table", {
        "project": "p1", "table": "t1", "columns": [],
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "empty" in payload.get("error", "").lower()


def test_create_table_invalid_bool_args() -> None:
    """ifNotExists with non-bool → error."""
    t = _make_tools_with_compute()
    r = t.call("create_table", {
        "project": "p1", "table": "t1",
        "columns": [{"name": "id", "type": "BIGINT"}],
        "ifNotExists": "yes",
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "boolean" in payload.get("error", "").lower()


def test_insert_values_sync_partition_timeout() -> None:
    """Sync partition insert: WaitTimeoutError → timeout response."""
    from odps.errors import WaitTimeoutError

    t = _make_tools_with_compute()
    inst = t.maxcompute_client.run_sql.return_value
    inst.wait_for_success.side_effect = WaitTimeoutError("timed out")

    r = t.call("insert_values", {
        "project": "p1", "table": "pt",
        "columns": ["id", "name", "dt"],
        "partitionColumns": ["dt"],
        "values": [[1, "a", "2025-01-01"]],
        "timeout": 5,
    })
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert payload.get("timeout") is True
    assert payload.get("instanceId") == "inst-001"


def test_create_table_with_generate_expression() -> None:
    """Column with generateExpression is passed through."""
    t = _make_tools_with_compute()
    r = t.call("create_table", {
        "project": "p1", "table": "t1",
        "columns": [{"name": "id", "type": "BIGINT"}],
        "partitionColumns": [
            {"name": "ds", "type": "DATE", "generateExpression": "TRUNC_TIME(sale_date, 'month')"}
        ],
    })
    payload = _text_payload(r)
    assert payload.get("success") is True


def test_insert_values_non_default_schema() -> None:
    """Insert with schema != 'default' → full_name includes schema."""
    t = _make_tools_with_compute()
    r = t.call("insert_values", {
        "project": "p1", "schema": "my_schema", "table": "t1",
        "columns": ["a"], "values": [["v1"]],
    })
    payload = _text_payload(r)
    assert payload.get("success") is True
    sql = t.maxcompute_client.run_sql.call_args[0][0]
    assert "`my_schema`" in sql


# ---------------------------------------------------------------------------
# insert_values boundary cases
# ---------------------------------------------------------------------------


def test_insert_values_no_compute_client() -> None:
    """maxcompute_client is None → unsupported response."""
    t = Tools(
        sdk=MagicMock(), default_project="p1",
        maxcompute_client=None,
    )
    r = t.call("insert_values", {
        "table": "t1", "columns": ["a"], "values": [["v1"]],
    })
    payload = _text_payload(r)
    assert payload.get("error") == "unsupported"


def test_insert_values_compute_returns_none() -> None:
    """_get_compute_client_for_project returns None → error."""
    t = _make_tools_with_compute()
    with patch.object(t, "_get_compute_client_for_project", return_value=None):
        r = t.call("insert_values", {
            "table": "t1", "columns": [{"name": "a"}], "values": [["v1"]],
        })
        payload = _text_payload(r)
        assert payload["success"] is False
        assert "compute client" in payload["error"].lower()


def test_insert_values_empty_column_name() -> None:
    """Column with empty name → error."""
    t = _make_tools_with_compute()
    r = t.call("insert_values", {
        "table": "t1",
        "columns": [{"name": "a"}, {"name": ""}],
        "values": [[1, 2]],
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "Empty column name" in payload["error"]


def test_insert_values_exception_in_execution() -> None:
    """Unexpected exception during SQL execution → caught at L242-244."""
    t = _make_tools_with_compute()
    t.maxcompute_client.run_sql.side_effect = RuntimeError("exec boom")
    r = t.call("insert_values", {
        "table": "t1",
        "columns": [{"name": "a"}],
        "values": [["v1"]],
    })
    payload = _text_payload(r)
    assert payload["success"] is False
    assert "exec boom" in payload["error"]
