"""Real integration tests: verify execute_sql read-only guard bypasses against real ODPS.

Tests that _is_read_only_sql correctly blocks DML/DDL even when crafted to
bypass the first-keyword heuristic.  Each test:

1. Creates a target table via create_table MCP tool (or pyodps directly).
2. Attempts a write via execute_sql MCP tool with a crafted bypass SQL.
3. Asserts the MCP layer rejects the SQL (success=False).
4. (For CTE+INSERT bypass) Also verifies via SELECT that no data was written,
   confirming the SQL was truly blocked before reaching ODPS.

Requires config.json with a real MaxCompute project.
All created tables use a unique `mcpguard_` prefix and are dropped in teardown.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, List

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import text_payload as _text_payload
from tests.conftest import has_config as _has_config
from tests.conftest import uniq as _uniq
from tests.conftest import drop_table as _drop
from tests.conftest import count_rows as _count_rows

logger = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def created_tables(real_tools: Tools):
    names: List[str] = []
    yield names
    for t in names:
        _drop(real_tools, t)


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestExecuteSqlGuardBypass:
    """Verify _is_read_only_sql blocks DML/DDL bypass attempts against real ODPS.

    Each test crafts a SQL that bypasses the first-keyword heuristic, calls
    execute_sql, and asserts the MCP layer rejects it before the SQL reaches ODPS.
    """

    # ---- WITH ... INSERT CTE bypass ----

    def test_with_insert_bypass(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        """WITH ... INSERT bypass: CTE followed by INSERT should be rejected.

        If the guard fails, the INSERT would actually write data into ODPS.
        We verify both that MCP rejects it AND that no data was written.
        """
        project = real_config.default_project
        table = _uniq("mcpguard_ins")
        created_tables.append(table)

        # Create target table
        r = real_tools.call("create_table", {
            "project": project, "schema": "default", "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}, {"name": "name", "type": "STRING"}],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table failed: {p}"

        # Attempt WITH ... INSERT bypass
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": f"WITH tmp AS (SELECT 1 AS id, 'bypass' AS name) INSERT INTO {table} SELECT id, name FROM tmp",
        })
        p = _text_payload(r)
        assert p.get("success") is False, f"WITH ... INSERT should be rejected, but got: {p}"

        # Verify no data was written
        assert _count_rows(real_tools, project, table) == 0

    # ---- WITH ... DELETE CTE bypass ----

    def test_with_delete_bypass(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        """WITH ... DELETE bypass: CTE followed by DELETE should be rejected."""
        project = real_config.default_project
        table = _uniq("mcpguard_del")
        created_tables.append(table)

        # Create table and seed one row
        r = real_tools.call("create_table", {
            "project": project, "schema": "default", "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}, {"name": "name", "type": "STRING"}],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table failed: {p}"

        r = real_tools.call("insert_values", {
            "project": project, "schema": "default", "table": table,
            "columns": ["id", "name"], "values": [[1, "keep"]],
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"insert_values failed: {p}"

        # Attempt WITH ... DELETE bypass
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": f"WITH tmp AS (SELECT 1 AS id) DELETE FROM {table} WHERE id IN (SELECT id FROM tmp)",
        })
        p = _text_payload(r)
        assert p.get("success") is False, f"WITH ... DELETE should be rejected, but got: {p}"

        # Verify the row was not deleted
        assert _count_rows(real_tools, project, table) == 1

    # ---- WITH ... UPDATE CTE bypass ----

    def test_with_update_bypass(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        """WITH ... UPDATE bypass: CTE followed by UPDATE should be rejected."""
        project = real_config.default_project
        table = _uniq("mcpguard_upd")
        created_tables.append(table)

        # Create table and seed one row
        r = real_tools.call("create_table", {
            "project": project, "schema": "default", "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}, {"name": "val", "type": "STRING"}],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table failed: {p}"

        r = real_tools.call("insert_values", {
            "project": project, "schema": "default", "table": table,
            "columns": ["id", "val"], "values": [[1, "original"]],
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"insert_values failed: {p}"

        # Attempt WITH ... UPDATE bypass
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": f"WITH tmp AS (SELECT 1 AS id) UPDATE {table} SET val='modified' WHERE id IN (SELECT id FROM tmp)",
        })
        p = _text_payload(r)
        assert p.get("success") is False, f"WITH ... UPDATE should be rejected, but got: {p}"

        # Verify the value was not changed
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": f"SELECT val FROM {table} WHERE id=1",
            "async": False, "timeout": 60,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"SELECT failed: {p}"
        rows = p.get("data") or []
        assert len(rows) == 1
        assert rows[0].get("val") == "original", "Value should not have been modified"

    # ---- WITH ... MERGE CTE bypass ----

    def test_with_merge_bypass(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        """WITH ... MERGE bypass: CTE followed by MERGE should be rejected."""
        project = real_config.default_project
        table = _uniq("mcpguard_merge")
        created_tables.append(table)

        # Create table
        r = real_tools.call("create_table", {
            "project": project, "schema": "default", "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}, {"name": "val", "type": "STRING"}],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table failed: {p}"

        # Attempt WITH ... MERGE bypass
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": f"WITH src AS (SELECT 1 AS id, 'x' AS val) MERGE INTO {table} USING src ON {table}.id = src.id WHEN MATCHED THEN UPDATE SET val = src.val WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val)",
        })
        p = _text_payload(r)
        assert p.get("success") is False, f"WITH ... MERGE should be rejected, but got: {p}"

        # Verify no data was written
        assert _count_rows(real_tools, project, table) == 0

    # ---- Plain DML/DDL (no CTE) — should also be rejected (baseline) ----

    def test_plain_insert_rejected(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        """Baseline: plain INSERT should be rejected by the guard."""
        project = real_config.default_project
        table = _uniq("mcpguard_plain")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project, "schema": "default", "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table failed: {p}"

        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": f"INSERT INTO {table} VALUES (1)",
        })
        p = _text_payload(r)
        assert p.get("success") is False, f"plain INSERT should be rejected: {p}"
        assert _count_rows(real_tools, project, table) == 0

    # ---- Legitimate WITH ... SELECT should still pass ----

    def test_with_select_allowed(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        """Legitimate WITH ... SELECT must NOT be blocked."""
        project = real_config.default_project

        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "WITH tmp AS (SELECT 1 AS id) SELECT * FROM tmp",
            "async": False,
            "timeout": 60,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"WITH ... SELECT should be allowed: {p}"

    # ---- Multi-statement with semicolons: second stmt is DML ----

    def test_multi_stmt_insert_rejected(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        """Multi-statement SQL with INSERT as second statement should be rejected."""
        project = real_config.default_project
        table = _uniq("mcpguard_multi")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project, "schema": "default", "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table failed: {p}"

        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": f"SELECT 1; INSERT INTO {table} VALUES (1)",
        })
        p = _text_payload(r)
        assert p.get("success") is False, f"multi-stmt INSERT should be rejected: {p}"
        assert _count_rows(real_tools, project, table) == 0
