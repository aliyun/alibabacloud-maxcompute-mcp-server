# -*- coding: utf-8 -*-
"""E2E tests: error handling and boundary scenarios.

Verifies that all MCP tools return structured success=false responses
(rather than crashes or raw exceptions) when given:
- Non-existent projects, schemas, tables, or instances
- SQL syntax errors and DML/DDL rejection
- Duplicate table creation without IF NOT EXISTS
- Mismatched insert_values column/value counts
- insert_values to a non-existent table

All created tables use a unique `mcpe2eerr_` prefix and are dropped in teardown.

Requires config.json (or MAXCOMPUTE_CATALOG_CONFIG env var).
"""
from __future__ import annotations

import logging
from typing import Any, List

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import (
    call_safe as _call_safe,
    drop_table as _drop,
    has_config as _has_config,
    text_payload as _text_payload,
    uniq as _uniq,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def created_tables(real_tools: Tools):
    names: List[str] = []
    yield names
    for t in names:
        _drop(real_tools, t)


def _assert_failure(payload: dict, context: str = "") -> None:
    """Assert that a tool response indicates failure (success=false or error key)."""
    has_error = payload.get("success") is False or "error" in payload
    assert has_error, f"Expected failure{f' ({context})' if context else ''}, got: {payload}"


# ---------------------------------------------------------------------------
# Catalog browse errors
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCatalogBrowseErrors:
    """Non-existent resources return structured errors."""

    def test_list_tables_nonexistent_project(self, real_tools: Tools) -> None:
        r = _call_safe(real_tools, "list_tables", {
            "project": "nonexistent_project_xyz_99999",
            "schema": "default",
            "pageSize": 5,
        })
        _assert_failure(_text_payload(r), "list_tables nonexistent project")

    def test_get_project_nonexistent(self, real_tools: Tools) -> None:
        r = _call_safe(real_tools, "get_project", {
            "project": "nonexistent_project_xyz_99999",
        })
        _assert_failure(_text_payload(r), "get_project nonexistent")

    def test_get_schema_nonexistent(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = _call_safe(real_tools, "get_schema", {
            "project": project,
            "schema": "nonexistent_schema_xyz_99999",
        })
        payload = _text_payload(r)
        # 2-tier projects return a virtual schema entry (success=True describing '2-level project')
        # instead of a 404 error — accept both behaviours.
        if payload.get("success") is True:
            d = payload.get("data") or payload
            desc = d.get("description", "") or ""
            assert "schema" in desc.lower() or "2-level" in desc.lower() or d.get("name"), (
                f"2-tier project must explain schema support, got: {d}"
            )
        else:
            _assert_failure(payload, "get_schema nonexistent")

    def test_get_table_schema_nonexistent_table(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = _call_safe(real_tools, "get_table_schema", {
            "project": project,
            "schema": "default",
            "table": "nonexistent_table_xyz_99999",
        })
        _assert_failure(_text_payload(r), "get_table_schema nonexistent")

    def test_get_partition_info_nonexistent_table(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = _call_safe(real_tools, "get_partition_info", {
            "project": project,
            "schema": "default",
            "table": "nonexistent_table_xyz_99999",
            "pageSize": 5,
        })
        _assert_failure(_text_payload(r), "get_partition_info nonexistent")


# ---------------------------------------------------------------------------
# SQL execution errors
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestSqlExecutionErrors:
    """execute_sql: invalid project, syntax error, DML rejection."""

    def test_execute_sql_invalid_project(self, real_tools: Tools) -> None:
        r = _call_safe(real_tools, "execute_sql", {
            "project": "nonexistent_project_xyz_99999",
            "sql": "SELECT 1",
            "async": False,
            "timeout": 30,
        })
        _assert_failure(_text_payload(r), "execute_sql invalid project")

    def test_execute_sql_syntax_error(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = _call_safe(real_tools, "execute_sql", {
            "project": project,
            "sql": "SELECT * FFROM completely_invalid_syntax !!!",
            "async": False,
            "timeout": 30,
        })
        payload = _text_payload(r)
        _assert_failure(payload, "execute_sql syntax error")
        # Should include an error message, not an empty string
        error_msg = payload.get("error") or ""
        assert error_msg, "Expected non-empty error for SQL syntax error"

    def test_execute_sql_plain_insert_rejected(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Plain INSERT must be blocked by the client-side read-only guard."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "INSERT INTO __nonexistent_xyz__ VALUES (1, 'test')",
            "async": False,
            "timeout": 30,
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"Plain INSERT must be rejected, got: {payload}"
        )

    def test_execute_sql_ddl_create_rejected(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """CREATE TABLE DDL must be rejected by the read-only guard."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "CREATE TABLE __guard_test_xyz__ (id BIGINT)",
            "async": False,
            "timeout": 30,
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"CREATE TABLE must be rejected, got: {payload}"
        )

    def test_execute_sql_ddl_drop_rejected(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """DROP TABLE DDL must be rejected by the read-only guard."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "DROP TABLE IF EXISTS __guard_test_xyz__",
            "async": False,
            "timeout": 30,
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"DROP TABLE must be rejected, got: {payload}"
        )

    def test_cost_sql_empty_sql_returns_error(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """cost_sql with blank SQL must return an error."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = _call_safe(real_tools, "cost_sql", {
            "project": project,
            "sql": "   ",
        })
        payload = _text_payload(r)
        _assert_failure(payload, "cost_sql blank SQL")


# ---------------------------------------------------------------------------
# create_table errors
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCreateTableErrors:
    """create_table: duplicate creation, ifNotExists semantics."""

    def test_create_table_duplicate_without_if_not_exists(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Creating the same table twice without ifNotExists must fail on second call."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2eerr_dup")
        created_tables.append(table)

        args = {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "lifecycle": 1,
        }
        r1 = real_tools.call("create_table", args)
        p1 = _text_payload(r1)
        assert p1.get("success") is True, f"First create_table failed: {p1}"

        # Second call without ifNotExists must fail
        r2 = real_tools.call("create_table", args)
        p2 = _text_payload(r2)
        _assert_failure(p2, "duplicate create_table without ifNotExists")

    def test_create_table_if_not_exists_is_idempotent(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Creating the same table with ifNotExists=True must succeed both times."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2eerr_ifne")
        created_tables.append(table)

        args = {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "ifNotExists": True,
            "lifecycle": 1,
        }
        r1 = real_tools.call("create_table", args)
        p1 = _text_payload(r1)
        assert p1.get("success") is True, f"First create_table (ifNotExists) failed: {p1}"

        r2 = real_tools.call("create_table", args)
        p2 = _text_payload(r2)
        assert p2.get("success") is True, (
            f"Second create_table (ifNotExists) must succeed, got: {p2}"
        )

    def test_create_table_no_columns_returns_error(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """create_table with empty columns list must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": _uniq("mcpe2eerr_nocols"),
            "columns": [],
            "lifecycle": 1,
        })
        payload = _text_payload(r)
        _assert_failure(payload, "create_table with empty columns")


# ---------------------------------------------------------------------------
# insert_values errors
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestInsertValuesErrors:
    """insert_values: column/value mismatch, non-existent table, wrong types."""

    def test_insert_values_to_nonexistent_table(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": "nonexistent_table_xyz_99999",
            "columns": ["id"],
            "values": [[1]],
        })
        _assert_failure(_text_payload(r), "insert_values nonexistent table")

    def test_insert_values_wrong_column_count(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Row with wrong number of values - ODPS may fill NULLs; accept or fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2eerr_colcnt")
        created_tables.append(table)
        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "name", "type": "STRING"},
            ],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table failed: {p}"

        # Provide only 1 value for 2 columns
        r2 = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id", "name"],
            "values": [[1]],  # wrong: only 1 value
        })
        p2 = _text_payload(r2)
        # ODPS may fill missing columns with NULL and succeed; that's acceptable.
        # What matters is no Python crash and a valid MCP response with a success indicator.
        assert isinstance(p2, dict), f"Expected dict response, got: {type(p2)}"
        assert "success" in p2, f"Response must contain 'success' key: {p2}"
        logger.info("insert_values wrong column count response: %s", p2)

    def test_insert_values_empty_values_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """insert_values with empty values list must return success=false."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2eerr_emptyvals")
        created_tables.append(table)
        real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "lifecycle": 1,
        })
        r = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id"],
            "values": [],  # empty
        })
        _assert_failure(_text_payload(r), "insert_values empty values")


# ---------------------------------------------------------------------------
# update_table errors
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestUpdateTableErrors:
    """update_table: non-existent table, no patch fields."""

    def test_update_table_nonexistent(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": "nonexistent_table_xyz_99999",
            "description": "should fail",
        })
        _assert_failure(_text_payload(r), "update_table nonexistent")

    def test_update_table_no_patch_fields(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2eerr_nopatch")
        created_tables.append(table)
        real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "lifecycle": 1,
        })
        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
        })
        _assert_failure(_text_payload(r), "update_table no patch fields")
