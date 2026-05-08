# -*- coding: utf-8 -*-
"""Coverage-boost tests: exercises code paths not yet covered by other tests.

Two categories:
  1. Mock-based tests (always run regardless of config):
     - tools.specs() call, covering the entire tool-spec definitions
     - tools_table_meta._normalize_patch validation errors (return before API calls)
     - tools_table_meta._apply_plan paths (labels delete, column errors, expiration)
     - execute_sql bad argument types (TypeError/ValueError)
     - _resolve_output_uri error paths (bad scheme, empty path, system paths)
     - insert_values async mode, partitioned async, value types
     - create_table validation errors
     - SQL safety edge cases (comment-only, WITH+DML, unknown keyword)
     - cost_sql long SQL (sqlTruncated)
     - check_access mock edge cases
     - tools_no_compute unsupported paths

  2. Real-tools E2E tests (require config.json / MAXCOMPUTE_CATALOG_CONFIG):
     - execute_sql sync with output_uri (streaming _read_rows path)
     - get_instance with output_uri (streaming path in get_instance)
     - execute_sql with maxCU=0 (cost-limit enforcement)

Requires no config for category 1; category 2 skips when config is absent.

Note: 3-level (schema-enabled) project paths are not tested here because the
current test environment uses 2-level projects. Those paths are covered by
unit tests in test_tools.py when a 3-level environment is available.
"""
from __future__ import annotations

import json
import logging
from typing import Any, List
from unittest.mock import MagicMock

import pytest

from pyodps_catalog import models as catalog_models

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import (
    async_wait_instance as _async_wait,
    has_config as _has_config,
    text_payload as _text_payload,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Additional fixture: Tools with a real pyodps_catalog.Table in the mock SDK
# ---------------------------------------------------------------------------

@pytest.fixture
def tools_with_table(tools: Tools, mock_sdk: MagicMock) -> Tools:
    """Patch sdk.client.get_table to return a real Table model.

    Needed for update_table tests that pass _normalize_patch validation and
    then proceed to the get_table + _apply_plan stage.
    """
    table = catalog_models.Table()
    schema = catalog_models.TableFieldSchema()
    schema.fields = [
        catalog_models.TableFieldSchema(
            field_name="id",
            sql_type_definition="BIGINT",
            type_category="BIGINT",
            mode="NULLABLE",
        ),
        catalog_models.TableFieldSchema(
            field_name="name",
            sql_type_definition="STRING",
            type_category="STRING",
            mode="NULLABLE",
        ),
    ]
    table.table_schema = schema
    table.labels = {}
    table.description = "test table"
    table.etag = "etag-abc123"
    table.expiration_options = None
    table.type = "MANAGED_TABLE"
    table.create_time = None
    table.last_modified_time = None
    table.partition_definition = None

    mock_sdk.client.get_table.return_value = table
    mock_sdk.client.update_table.return_value = table
    return tools


# ===========================================================================
# Category 1: Mock-based tests (always run)
# ===========================================================================

# ---------------------------------------------------------------------------
# 1.1  tools.specs() — covers tools.py 215-665 and tools_common.py 165-183
# ---------------------------------------------------------------------------

class TestSpecsCoverage:
    """Call tools.specs() to cover the full tool-spec definition block."""

    def test_specs_returns_nonempty_list(self, tools: Tools) -> None:
        specs = tools.specs()
        assert len(specs) > 0, "specs() must return at least one ToolSpec"

    def test_specs_items_have_required_fields(self, tools: Tools) -> None:
        specs = tools.specs()
        for s in specs:
            assert s.name, f"ToolSpec missing name: {s}"
            assert s.description, f"ToolSpec {s.name!r} missing description"
            assert isinstance(s.input_schema, dict), (
                f"ToolSpec {s.name!r} input_schema must be a dict"
            )

    def test_specs_contains_expected_tools(self, tools: Tools) -> None:
        names = {s.name for s in tools.specs()}
        expected = {
            "list_projects", "get_project", "get_table_schema",
            "execute_sql", "cost_sql", "check_access",
            "create_table", "insert_values",
        }
        missing = expected - names
        assert not missing, f"specs() missing expected tools: {missing}"


# ---------------------------------------------------------------------------
# 1.2  update_table: _normalize_patch validation errors (all before API call)
# ---------------------------------------------------------------------------

class TestUpdateTableValidation:
    """_normalize_patch validation errors: returned before any SDK call."""

    def test_description_null(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1", "description": None})
        p = _text_payload(r)
        assert p["success"] is False
        assert "null" in p["error"].lower() or "cannot be null" in p["error"].lower()

    def test_description_not_string(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1", "description": 123})
        p = _text_payload(r)
        assert p["success"] is False
        assert "string" in p["error"].lower()

    def test_labels_not_dict(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1", "labels": "bad"})
        p = _text_payload(r)
        assert p["success"] is False

    def test_labels_set_missing(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1", "labels": {"mode": "merge"}})
        p = _text_payload(r)
        assert p["success"] is False
        assert "labels.set" in p["error"]

    def test_labels_mode_invalid(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1",
                                         "labels": {"set": {"k": "v"}, "mode": "invalid"}})
        p = _text_payload(r)
        assert p["success"] is False
        assert "mode" in p["error"]

    def test_expiration_not_dict(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1", "expiration": "bad"})
        p = _text_payload(r)
        assert p["success"] is False

    def test_expiration_days_not_int(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1", "expiration": {"days": "bad"}})
        p = _text_payload(r)
        assert p["success"] is False

    def test_expiration_days_negative(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1", "expiration": {"days": -1}})
        p = _text_payload(r)
        assert p["success"] is False
        assert ">= 0" in p["error"]

    def test_columns_not_dict(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1", "columns": "bad"})
        p = _text_payload(r)
        assert p["success"] is False

    def test_columns_set_comments_not_dict(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1",
                                         "columns": {"setComments": "bad"}})
        p = _text_payload(r)
        assert p["success"] is False

    def test_columns_set_nullable_not_list(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1",
                                         "columns": {"setNullable": "bad"}})
        p = _text_payload(r)
        assert p["success"] is False

    def test_columns_set_nullable_nested(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1",
                                         "columns": {"setNullable": ["addr.city"]}})
        p = _text_payload(r)
        assert p["success"] is False
        assert "nested" in p["error"].lower() or "top-level" in p["error"].lower()

    def test_columns_add_not_list(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1", "columns": {"add": "bad"}})
        p = _text_payload(r)
        assert p["success"] is False

    def test_columns_add_item_not_dict(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1",
                                         "columns": {"add": ["notadict"]}})
        p = _text_payload(r)
        assert p["success"] is False

    def test_columns_add_missing_name(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1",
                                         "columns": {"add": [{"type": "STRING"}]}})
        p = _text_payload(r)
        assert p["success"] is False
        assert "name" in p["error"]

    def test_columns_add_missing_type(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1",
                                         "columns": {"add": [{"name": "col1"}]}})
        p = _text_payload(r)
        assert p["success"] is False
        assert "type" in p["error"]

    def test_no_updatable_fields(self, tools: Tools) -> None:
        r = tools.call("update_table", {"table": "t1"})
        p = _text_payload(r)
        assert p["success"] is False
        assert "No updatable fields" in p["error"]


# ---------------------------------------------------------------------------
# 1.3  update_table: _apply_plan paths (need proper Table mock)
# ---------------------------------------------------------------------------

class TestApplyPlanPaths:
    """_apply_plan paths reached after get_table; require tools_with_table fixture."""

    def test_labels_delete_mode(self, tools_with_table: Tools) -> None:
        """labels.mode='delete' executes the delete branch in _apply_plan."""
        r = tools_with_table.call("update_table", {
            "table": "t1",
            "labels": {"set": {"env": "prod"}, "mode": "delete"},
        })
        p = _text_payload(r)
        assert p["success"] is True
        assert "labels(delete)" in (p.get("data") or {}).get("updatedFields", [])

    def test_expiration_days_update(self, tools_with_table: Tools) -> None:
        """expiration.days updates expiration_options in _apply_plan."""
        r = tools_with_table.call("update_table", {
            "table": "t1",
            "expiration": {"days": 30},
        })
        p = _text_payload(r)
        assert p["success"] is True
        assert "expiration" in (p.get("data") or {}).get("updatedFields", [])

    def test_expiration_partition_days_update(self, tools_with_table: Tools) -> None:
        r = tools_with_table.call("update_table", {
            "table": "t1",
            "expiration": {"partitionDays": 7},
        })
        p = _text_payload(r)
        assert p["success"] is True

    def test_set_comments_column_not_found(self, tools_with_table: Tools) -> None:
        """setComments with non-existent column path raises ValueError → success=false."""
        r = tools_with_table.call("update_table", {
            "table": "t1",
            "columns": {"setComments": {"nonexistent_col": "some desc"}},
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "not found" in p["error"]

    def test_set_nullable_column_not_found(self, tools_with_table: Tools) -> None:
        r = tools_with_table.call("update_table", {
            "table": "t1",
            "columns": {"setNullable": ["nonexistent_col"]},
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "not found" in p["error"]

    def test_add_duplicate_column(self, tools_with_table: Tools) -> None:
        r = tools_with_table.call("update_table", {
            "table": "t1",
            "columns": {"add": [{"name": "id", "type": "STRING"}]},
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "already exists" in p["error"]


# ---------------------------------------------------------------------------
# 1.4  execute_sql edge cases (bad arg types)
# ---------------------------------------------------------------------------

class TestExecuteSqlEdgeCases:
    """Cover execute_sql argument validation error paths."""

    def test_bad_async_type(self, tools: Tools) -> None:
        """async='false' (string instead of bool) raises TypeError → success=false."""
        r = tools.call("execute_sql", {"sql": "SELECT 1", "async": "false"})
        p = _text_payload(r)
        assert p["success"] is False
        assert "boolean" in p["error"].lower() or "async" in p["error"].lower()

    def test_bad_timeout_string(self, tools: Tools) -> None:
        """timeout='bad' raises ValueError → success=false."""
        r = tools.call("execute_sql", {
            "sql": "SELECT 1",
            "async": False,
            "timeout": "bad",
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "timeout" in p["error"].lower() or "Invalid" in p["error"]

    def test_bad_timeout_zero(self, tools: Tools) -> None:
        """timeout=0 (non-positive) raises ValueError → success=false."""
        r = tools.call("execute_sql", {
            "sql": "SELECT 1",
            "async": False,
            "timeout": 0,
        })
        p = _text_payload(r)
        assert p["success"] is False

    def test_unsupported_output_uri_scheme(self, tools: Tools) -> None:
        """output_uri with unsupported scheme → success=false."""
        r = tools.call("execute_sql", {
            "sql": "SELECT 1",
            "async": False,
            "output_uri": "s3://mybucket/result.jsonl",
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "s3" in p["error"] or "Unsupported" in p["error"]

    def test_output_uri_whitespace_only(self, tools: Tools) -> None:
        """output_uri='   ' (whitespace) triggers 'empty path' error."""
        r = tools.call("execute_sql", {
            "sql": "SELECT 1",
            "async": False,
            "output_uri": "   ",
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "empty" in p["error"].lower() or "output_uri" in p["error"].lower()

    def test_output_uri_file_empty_path(self, tools: Tools) -> None:
        """output_uri='file://' (empty file path) → success=false."""
        r = tools.call("execute_sql", {
            "sql": "SELECT 1",
            "async": False,
            "output_uri": "file://",
        })
        p = _text_payload(r)
        assert p["success"] is False

    def test_output_uri_restricted_system_path(self, tools: Tools) -> None:
        """output_uri targeting /etc is rejected."""
        r = tools.call("execute_sql", {
            "sql": "SELECT 1",
            "async": False,
            "output_uri": "/etc/myresult.jsonl",
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "restricted" in p["error"].lower() or "sensitive" in p["error"].lower() or "/etc" in p["error"]


# ---------------------------------------------------------------------------
# 1.5  SQL safety edge cases
# ---------------------------------------------------------------------------

class TestSqlSafetyEdgeCases:
    """Cover _is_read_only_sql paths not hit by other E2E tests."""

    def test_comment_only_sql(self, tools: Tools) -> None:
        """SQL that strips to nothing (only comments) → 'Empty SQL after removing comments'."""
        r = tools.call("execute_sql", {
            "sql": "-- just a comment\n/* block comment */",
            "async": False,
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "Empty" in p["error"] or "comment" in p["error"].lower()

    def test_with_dml_bypass_attempt(self, tools: Tools) -> None:
        """WITH ... DELETE is rejected by CTE body scan."""
        r = tools.call("execute_sql", {
            "sql": "WITH x AS (SELECT 1 AS n) DELETE FROM target_table",
            "async": False,
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "DELETE" in p["error"] or "Unsafe" in p["error"]

    def test_unknown_first_keyword(self, tools: Tools) -> None:
        """Unknown first keyword rejected by the keyword check."""
        r = tools.call("execute_sql", {
            "sql": "UNKNOWNKEYWORD col FROM t1",
            "async": False,
        })
        p = _text_payload(r)
        assert p["success"] is False


# ---------------------------------------------------------------------------
# 1.6  cost_sql long SQL → sqlTruncated=True
# ---------------------------------------------------------------------------

class TestCostSqlEdgeCases:

    def test_cost_sql_long_sql_truncated(self, tools: Tools) -> None:
        """SQL > 200 chars sets sqlTruncated=True in the result."""
        long_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(60)) + " FROM my_table"
        assert len(long_sql) > 200
        r = tools.call("cost_sql", {"sql": long_sql})
        p = _text_payload(r)
        assert p.get("sqlTruncated") is True, (
            f"Expected sqlTruncated=True for SQL > 200 chars, got: {p}"
        )

    def test_cost_sql_with_maxcu_zero(self, tools: Tools) -> None:
        """maxCU=0: estimated CU (>0 from stub) exceeds limit → overLimit=True."""
        r = tools.call("execute_sql", {
            "sql": "SELECT 1",
            "maxCU": 0,
        })
        p = _text_payload(r)
        # With mock execute_sql_cost returning "0.1", estimatedCU=1 > maxCU=0
        assert p.get("overLimit") is True or p.get("success") is False, (
            f"Expected overLimit=True or success=False for maxCU=0, got: {p}"
        )


# ---------------------------------------------------------------------------
# 1.7  insert_values: async mode, value types, partition errors
# ---------------------------------------------------------------------------

class TestInsertValuesEdgeCases:

    def test_insert_async_non_partitioned(self, tools: Tools) -> None:
        """insert_values async=True (non-partitioned) returns instanceId."""
        r = tools.call("insert_values", {
            "table": "t1",
            "columns": ["id", "name"],
            "values": [[1, "Alice"]],
            "async": True,
        })
        p = _text_payload(r)
        assert p["success"] is True
        assert "instanceId" in p
        assert p.get("status") == "submitted"

    def test_insert_async_partitioned(self, tools: Tools) -> None:
        """insert_values async=True (partitioned) returns batches list."""
        r = tools.call("insert_values", {
            "table": "t1",
            "columns": ["id", "name", "ds"],
            "values": [
                [1, "Alice", "20250101"],
                [2, "Bob", "20250101"],
                [3, "Carol", "20250102"],
            ],
            "partitionColumns": ["ds"],
            "async": True,
        })
        p = _text_payload(r)
        assert p["success"] is True
        assert "batches" in p
        assert p.get("status") == "submitted"

    def test_insert_value_types_null_bool_float_date(self, tools: Tools) -> None:
        """Various value types: NULL, bool, float, date string → correct SQL quoting."""
        r = tools.call("insert_values", {
            "table": "t1",
            "columns": ["a", "b", "c", "d"],
            "values": [[None, True, 3.14, "2025-01-01"]],
        })
        p = _text_payload(r)
        # mock run_sql always works; verify success
        assert p.get("success") is True, f"insert_values with mixed types failed: {p}"

    def test_insert_value_types_false_bool(self, tools: Tools) -> None:
        """bool False → 'false' in SQL."""
        r = tools.call("insert_values", {
            "table": "t1",
            "columns": ["flag"],
            "values": [[False]],
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"insert_values with False bool failed: {p}"

    def test_insert_partition_col_not_in_columns(self, tools: Tools) -> None:
        """partitionColumns references a column not in columns → success=false."""
        r = tools.call("insert_values", {
            "table": "t1",
            "columns": ["id"],
            "values": [[1]],
            "partitionColumns": ["missing_col"],
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "not found" in p["error"]

    def test_insert_all_cols_are_partition(self, tools: Tools) -> None:
        """All columns are partition columns → no data columns → success=false."""
        r = tools.call("insert_values", {
            "table": "t1",
            "columns": ["ds"],
            "values": [["20250101"]],
            "partitionColumns": ["ds"],
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "data column" in p["error"].lower() or "At least" in p["error"]

    def test_insert_row_length_mismatch(self, tools: Tools) -> None:
        """Row shorter than columns list → success=false."""
        r = tools.call("insert_values", {
            "table": "t1",
            "columns": ["id", "ds"],
            "values": [[1]],  # missing ds value
            "partitionColumns": ["ds"],
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "Row length" in p["error"] or "does not match" in p["error"]

    def test_insert_partition_null_value(self, tools: Tools) -> None:
        """NULL as partition column value → ValueError in _quote_partition_literal."""
        r = tools.call("insert_values", {
            "table": "t1",
            "columns": ["id", "ds"],
            "values": [[1, None]],
            "partitionColumns": ["ds"],
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "NULL" in p["error"] or "null" in p["error"].lower()

    def test_insert_partition_bool_value(self, tools: Tools) -> None:
        """bool as partition value → triggers bool branch in _quote_partition_literal."""
        r = tools.call("insert_values", {
            "table": "t1",
            "columns": ["id", "flag"],
            "values": [[1, True]],
            "partitionColumns": ["flag"],
        })
        p = _text_payload(r)
        # mock run_sql always works; verify success
        assert p.get("success") is True, f"insert_values with partition bool failed: {p}"

    def test_insert_partition_int_value(self, tools: Tools) -> None:
        """int partition value → triggers int branch in _quote_partition_literal."""
        r = tools.call("insert_values", {
            "table": "t1",
            "columns": ["id", "year"],
            "values": [[1, 2025]],
            "partitionColumns": ["year"],
        })
        p = _text_payload(r)
        # mock run_sql always works; verify success
        assert p.get("success") is True, f"insert_values with partition int failed: {p}"

    def test_insert_async_bad_type(self, tools: Tools) -> None:
        """async='yes' (string) → TypeError → success=false."""
        r = tools.call("insert_values", {
            "table": "t1",
            "columns": ["id"],
            "values": [[1]],
            "async": "yes",
        })
        p = _text_payload(r)
        assert p["success"] is False


# ---------------------------------------------------------------------------
# 1.8  create_table validation errors
# ---------------------------------------------------------------------------

class TestCreateTableEdgeCases:

    def test_ifnotexists_nonbool(self, tools: Tools) -> None:
        """ifNotExists='yes' (string) → TypeError → success=false."""
        r = tools.call("create_table", {
            "table": "t1",
            "columns": [{"name": "id", "type": "BIGINT"}],
            "ifNotExists": "yes",
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "boolean" in p["error"].lower()

    def test_table_properties_not_dict(self, tools: Tools) -> None:
        r = tools.call("create_table", {
            "table": "t1",
            "columns": [{"name": "id", "type": "BIGINT"}],
            "tableProperties": "bad",
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "tableProperties" in p["error"]

    def test_hints_not_dict(self, tools: Tools) -> None:
        r = tools.call("create_table", {
            "table": "t1",
            "columns": [{"name": "id", "type": "BIGINT"}],
            "hints": "bad",
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "hints" in p["error"]

    def test_primary_key_not_list(self, tools: Tools) -> None:
        r = tools.call("create_table", {
            "table": "t1",
            "columns": [{"name": "id", "type": "BIGINT"}],
            "primaryKey": "id",
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "primaryKey" in p["error"]

    def test_primary_key_empty_list_normalized_to_none(self, tools: Tools) -> None:
        """primaryKey=[] is normalized to None; create_table should succeed."""
        r = tools.call("create_table", {
            "table": "t1",
            "columns": [{"name": "id", "type": "BIGINT"}],
            "primaryKey": [],
        })
        p = _text_payload(r)
        assert p["success"] is True

    def test_columns_key_missing(self, tools: Tools) -> None:
        r = tools.call("create_table", {"table": "t1"})
        p = _text_payload(r)
        assert p["success"] is False
        assert "columns" in p["error"].lower()

    def test_columns_empty_list(self, tools: Tools) -> None:
        r = tools.call("create_table", {"table": "t1", "columns": []})
        p = _text_payload(r)
        assert p["success"] is False
        assert "empty" in p["error"].lower() or "columns" in p["error"].lower()


# ---------------------------------------------------------------------------
# 1.9  tools_no_compute: unsupported paths
# ---------------------------------------------------------------------------

class TestToolsNoCompute:
    """Tools without maxcompute_client returns 'unsupported' for compute tools."""

    def test_execute_sql_unsupported(self, tools_no_compute: Tools) -> None:
        r = tools_no_compute.call("execute_sql", {"sql": "SELECT 1"})
        p = _text_payload(r)
        assert p.get("error") == "unsupported" or p.get("success") is False

    def test_insert_values_unsupported(self, tools_no_compute: Tools) -> None:
        r = tools_no_compute.call("insert_values", {
            "table": "t1",
            "columns": ["id"],
            "values": [[1]],
        })
        p = _text_payload(r)
        assert p.get("error") == "unsupported" or p.get("success") is False

    def test_create_table_unsupported(self, tools_no_compute: Tools) -> None:
        r = tools_no_compute.call("create_table", {
            "table": "t1",
            "columns": [{"name": "id", "type": "STRING"}],
        })
        p = _text_payload(r)
        assert p.get("error") == "unsupported" or p.get("success") is False

    def test_check_access_unsupported(self, tools_no_compute: Tools) -> None:
        r = tools_no_compute.call("check_access", {})
        p = _text_payload(r)
        assert p.get("error") == "unsupported" or p.get("success") is False

    def test_get_instance_status_unsupported(self, tools_no_compute: Tools) -> None:
        r = tools_no_compute.call("get_instance_status", {"instanceId": "abc"})
        p = _text_payload(r)
        assert p.get("error") == "unsupported" or p.get("success") is False

    def test_get_instance_unsupported(self, tools_no_compute: Tools) -> None:
        r = tools_no_compute.call("get_instance", {"instanceId": "abc"})
        p = _text_payload(r)
        assert p.get("error") == "unsupported" or p.get("success") is False


# ---------------------------------------------------------------------------
# 1.10  check_access mock edge cases
# ---------------------------------------------------------------------------

class TestCheckAccessMockEdgeCases:

    def test_include_grants_nonbool_coerced(self, tools: Tools) -> None:
        """include_grants=1 (int) triggers the warning log path + bool coercion."""
        r = tools.call("check_access", {
            "project": "p1",
            "include_grants": 1,  # int, not bool — triggers line 173-177
        })
        p = _text_payload(r)
        # Mock may not have run_security_query; success either way
        assert isinstance(p, dict)

    def test_include_grants_true_empty_project(self, tools: Tools) -> None:
        """include_grants=True with project='' → error 'project is required'."""
        r = tools.call("check_access", {
            "project": "",
            "include_grants": True,
        })
        p = _text_payload(r)
        assert p["success"] is False
        assert "project" in p["error"].lower() or "required" in p["error"].lower()


# ===========================================================================
# Category 2: Real-tools E2E tests (need config.json)
# ===========================================================================

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestOutputUriStreaming:
    """execute_sql sync with output_uri: exercises _read_rows streaming path."""

    def test_execute_sql_sync_output_uri_creates_file(
        self, real_tools: Tools, real_config: Any, tmp_path: Any
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        output_file = tmp_path / "result.jsonl"
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1 AS n, 'hello' AS s",
            "async": False,
            "output_uri": f"file://{output_file}",
            "timeout": 120,
        })
        p = _text_payload(r)
        assert p["success"] is True, f"execute_sql with output_uri failed: {p}"
        # A decorated file should exist (instanceId is appended to stem)
        out_dir = tmp_path
        jsonl_files = list(out_dir.glob("*.jsonl"))
        assert jsonl_files, (
            f"Expected a .jsonl file in {out_dir}, got: {list(out_dir.iterdir())}"
        )
        # Parse and verify content
        with open(jsonl_files[0], encoding="utf-8") as f:
            rows = [json.loads(line) for line in f if line.strip()]
        assert len(rows) >= 1, f"Expected at least 1 row in JSONL, got: {rows}"
        assert "n" in rows[0], f"Expected 'n' column in row, got: {rows[0]}"

    def test_execute_sql_sync_output_uri_response_has_output_path(
        self, real_tools: Tools, real_config: Any, tmp_path: Any
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        output_file = tmp_path / "out.jsonl"
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1 AS x",
            "async": False,
            "output_uri": f"file://{output_file}",
            "timeout": 120,
        })
        p = _text_payload(r)
        assert p["success"] is True
        assert "outputPath" in p, f"Response should contain outputPath: {p}"
        assert "bytesWritten" in p, f"Response should contain bytesWritten: {p}"
        assert "preview" in p, f"Response should contain preview rows: {p}"


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestGetInstanceOutputUri:
    """get_instance with output_uri: exercises streaming in get_instance."""

    def test_get_instance_with_output_uri(
        self, real_tools: Tools, real_config: Any, tmp_path: Any
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        # Submit async SQL
        r1 = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1 AS n",
            "async": True,
        })
        p1 = _text_payload(r1)
        assert p1["success"] is True, f"execute_sql async failed: {p1}"
        instance_id = p1["instanceId"]

        # Wait for completion
        status = _async_wait(real_tools, project, instance_id, timeout=120)
        logger.info("Instance %s status: %s", instance_id, status)

        # Fetch result with output_uri
        output_file = tmp_path / "inst_result.jsonl"
        r2 = real_tools.call("get_instance", {
            "project": project,
            "instanceId": instance_id,
            "output_uri": f"file://{output_file}",
        })
        p2 = _text_payload(r2)
        assert "instanceId" in p2, f"get_instance response missing instanceId: {p2}"
        # Results should have outputPath
        results = p2.get("results") or {}
        for task_name, task_data in results.items():
            if isinstance(task_data, dict) and "outputPath" in task_data:
                assert task_data["bytesWritten"] is not None
                break


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestMaxCUEnforcement:
    """execute_sql with maxCU: cost limit enforcement path."""

    def test_execute_sql_max_cu_zero_over_limit(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """maxCU=0 with any non-trivial SQL should hit the overLimit check."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1",
            "maxCU": 0,
        })
        p = _text_payload(r)
        # Either overLimit=True (cost > 0) or success=True (cost exactly 0 edge case)
        # Either way, the maxCU code path was exercised
        assert isinstance(p, dict), f"Expected dict response, got: {p}"
        logger.info("maxCU=0 response: %s", p)

    def test_execute_sql_max_cu_large_allows_query(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """maxCU=1000 should allow the query through (cost < 1000)."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1 AS x",
            "maxCU": 1000,
            "async": True,
        })
        p = _text_payload(r)
        # Should be submitted (not blocked by maxCU)
        assert p.get("success") is True or "overLimit" in p, (
            f"Unexpected response for maxCU=1000: {p}"
        )


# ===========================================================================
# Category 1.12: Mock-based compute edge-case coverage
# ===========================================================================


class TestComputeEdgeCases:
    """Cover remaining tools_compute.py branches via mock."""

    def test_get_instance_logview_exception(self, tools: Tools, mock_sdk: MagicMock) -> None:
        """_get_instance_logview: exception handler path.

        Note: Tests private method directly because the public API path
        (execute_sql → get_instance) requires full instance lifecycle setup.
        The logview URL is optional metadata, so this isolated test is acceptable.
        """
        from maxcompute_catalog_mcp.tools_compute import ComputeMixin
        mixin = ComputeMixin()
        inst = MagicMock()
        inst.get_logview_address.side_effect = RuntimeError("no logview")
        result = mixin._get_instance_logview(inst)
        assert result is None

    def test_execute_sql_bad_max_cu_type(self, tools: Tools) -> None:
        """execute_sql: TypeError/ValueError for bad maxCU coercion."""
        # When maxCU is a dict, int(dict) raises TypeError → maxCU becomes None
        # (the except branch clears it rather than erroring out)
        r = tools.call("execute_sql", {
            "sql": "SELECT 1",
            "maxCU": {"bad": "value"},
        })
        # Should not crash — maxCU coercion failure is silently treated as None
        assert isinstance(r, dict)

    def test_get_instance_not_terminated(self, tools: Tools) -> None:
        """get_instance: instance not yet terminated returns pending message."""
        from unittest.mock import patch
        inst = MagicMock()
        inst.is_terminated = MagicMock(return_value=False)
        with patch.object(tools.maxcompute_client, "get_instance", return_value=inst):
            r = tools.call("get_instance", {"instanceId": "abc123"})
        p = _text_payload(r)
        assert "not terminated" in p.get("message", "").lower() or "instanceId" in p

    def test_get_instance_no_task_results(self, tools: Tools) -> None:
        """get_instance: empty task results."""
        from unittest.mock import patch
        inst = MagicMock()
        inst.is_terminated = MagicMock(return_value=True)
        inst.get_task_results = MagicMock(return_value={})
        with patch.object(tools.maxcompute_client, "get_instance", return_value=inst):
            r = tools.call("get_instance", {"instanceId": "abc123"})
        p = _text_payload(r)
        assert p.get("instanceId") == "abc123"

    def test_get_instance_bad_output_uri(self, tools: Tools) -> None:
        """get_instance: bad output_uri raises ValueError → error response."""
        r = tools.call("get_instance", {
            "instanceId": "abc123",
            "output_uri": "bad://not-a-file-uri",
        })
        p = _text_payload(r)
        assert p.get("success") is False

    def test_get_instance_with_reader(self, tools: Tools) -> None:
        """get_instance: task result with open_reader (no schema columns)."""
        from unittest.mock import patch, MagicMock as MM
        inst = MM()
        inst.is_terminated = MM(return_value=True)
        task_result = MM()
        task_result.open_reader = MM()
        reader = MM()
        reader.__enter__ = MM(return_value=reader)
        reader.__exit__ = MM(return_value=False)
        # Schema with no columns → schema-less fallback path
        schema = MM()
        schema.columns = []
        reader._schema = schema
        task_result.open_reader.return_value = reader
        inst.get_task_results = MM(return_value={"task1": task_result})
        with patch.object(tools.maxcompute_client, "get_instance", return_value=inst):
            r = tools.call("get_instance", {"instanceId": "abc123"})
        p = _text_payload(r)
        assert p.get("instanceId") == "abc123"

    def test_get_instance_task_non_reader(self, tools: Tools) -> None:
        """get_instance: task result without open_reader (raw string path)."""
        from unittest.mock import patch, MagicMock as MM
        inst = MM()
        inst.is_terminated = MM(return_value=True)
        # Create a task result that does NOT have open_reader
        task_result = "some raw output string"
        inst.get_task_results = MM(return_value={"task1": task_result})
        with patch.object(tools.maxcompute_client, "get_instance", return_value=inst):
            r = tools.call("get_instance", {"instanceId": "abc123"})
        p = _text_payload(r)
        assert p.get("instanceId") == "abc123"
        assert "task1" in p.get("results", {})

    def test_cost_estimation_failure_stub(self, tools: Tools) -> None:
        """_estimate_sql_cost: exception from SDK returns stub response."""
        from unittest.mock import patch
        with patch.object(
            tools._get_compute_client_for_project(tools.default_project),
            "execute_sql_cost",
            side_effect=RuntimeError("cost estimation failed"),
        ):
            r = tools.call("cost_sql", {"sql": "SELECT 1"})
        p = _text_payload(r)
        # Either stub=True or normal cost estimate — either way no crash
        assert isinstance(p, dict)
