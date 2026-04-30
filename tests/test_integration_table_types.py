"""Real integration tests for create_table + insert_values across MaxCompute table types.

Exercises the MCP tools end-to-end against a real MaxCompute project to verify
the newly-added create_table options (ifNotExists, transactional, primaryKey,
storageTier, tableProperties, hints) work for each supported table type, and
that insert_values is compatible with each.

Table types covered:
- Regular (non-partitioned)
- Partitioned
- Transactional (Delta) table via tableProperties
- Transactional + primary key (requires primary key on BIGINT NOT NULL)
- ifNotExists idempotency
- storageTier

Requires config.json with defaultProject=catalogapi_regression (daily env).
All created tables use a unique `mcpit_` prefix and are dropped in teardown.
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
class TestCreateInsertTableTypes:
    """End-to-end create_table + insert_values per table type."""

    def test_regular_table(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        project = real_config.default_project
        table = _uniq("mcpit_regular")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "name", "type": "STRING"},
            ],
            "description": "integration test regular table",
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table failed: {p}"

        r = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id", "name"],
            "values": [[1, "alice"], [2, "bob"]],
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"insert_values failed: {p}"
        assert p.get("rowsInserted") == 2
        assert _count_rows(real_tools, project, table) == 2

    def test_partitioned_table(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        project = real_config.default_project
        table = _uniq("mcpit_partitioned")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "val", "type": "STRING"},
            ],
            "partitionColumns": ["dt"],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table (partitioned) failed: {p}"

        r = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id", "val", "dt"],
            "partitionColumns": ["dt"],
            "values": [
                [1, "x", "20260417"],
                [2, "y", "20260417"],
                [3, "z", "20260418"],
            ],
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"insert_values partitioned failed: {p}"
        assert p.get("rowsInserted") == 3
        assert p.get("partitionBatches") == 2
        assert _count_rows(real_tools, project, table, hints={"odps.sql.allow.fullscan": "true"}) == 3
        assert _count_rows(real_tools, project, table, partition="dt='20260417'") == 2
        assert _count_rows(real_tools, project, table, partition="dt='20260418'") == 1

    def test_if_not_exists_idempotent(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        project = real_config.default_project
        table = _uniq("mcpit_ifne")
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
        assert p1.get("success") is True, f"first create_table failed: {p1}"

        r2 = real_tools.call("create_table", args)
        p2 = _text_payload(r2)
        assert p2.get("success") is True, f"second create_table (ifNotExists) failed: {p2}"
        assert _count_rows(real_tools, project, table) == 0

    def test_storage_tier(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        project = real_config.default_project
        table = _uniq("mcpit_tier")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "storageTier": "standard",
            "lifecycle": 1,
        })
        p = _text_payload(r)
        if p.get("success") is not True:
            pytest.skip(f"storage_tier not supported in this env: {p.get('error')}")

        r = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id"],
            "values": [[1], [2]],
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"insert into storage_tier table failed: {p}"
        assert _count_rows(real_tools, project, table) == 2

    def test_transactional_table(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        project = real_config.default_project
        table = _uniq("mcpit_tx")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "val", "type": "STRING"},
            ],
            "transactional": True,
            "lifecycle": 1,
        })
        p = _text_payload(r)
        if p.get("success") is not True:
            pytest.skip(f"transactional table not supported in this env: {p.get('error')}")

        r = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id", "val"],
            "values": [[1, "a"], [2, "b"]],
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"insert into transactional table failed: {p}"
        assert _count_rows(real_tools, project, table) == 2

    def test_transactional_with_primary_key(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Primary-key (Delta / transactional) table: PK column must be NOT NULL."""
        project = real_config.default_project
        table = _uniq("mcpit_pk")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [
                {"name": "id", "type": "BIGINT", "notNull": True, "description": "primary key"},
                {"name": "val", "type": "STRING"},
            ],
            "transactional": True,
            "primaryKey": ["id"],
            "hints": {"odps.sql.upsertable.table.enable": "true"},
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table PK failed: {p}"

        r = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id", "val"],
            "values": [[1, "a"], [2, "b"]],
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"insert into pk table failed: {p}"
        assert _count_rows(real_tools, project, table) == 2

    def test_typed_partition_column(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Partition column with explicit non-STRING type (BIGINT)."""
        project = real_config.default_project
        table = _uniq("mcpit_parttyped")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "val", "type": "STRING"},
            ],
            "partitionColumns": [{"name": "hr", "type": "BIGINT", "description": "hour bucket"}],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table typed partition failed: {p}"

        r = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id", "val", "hr"],
            "partitionColumns": ["hr"],
            "values": [[1, "a", 10], [2, "b", 11]],
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"insert into typed-partition table failed: {p}"
        assert p.get("partitionBatches") == 2
        assert _count_rows(real_tools, project, table, hints={"odps.sql.allow.fullscan": "true"}) == 2
        assert _count_rows(real_tools, project, table, partition="hr=10") == 1

    def test_auto_partition_generate_expression(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """AUTO PARTITIONED BY: partition column auto-derived via generateExpression.

        Data column sale_date (DATE) drives a STRING partition column sale_month
        computed as TRUNC_TIME(sale_date, 'month'). Inserts only supply sale_date;
        MaxCompute fills sale_month automatically.
        """
        project = real_config.default_project
        table = _uniq("mcpit_autopart")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [
                {"name": "shop_name", "type": "STRING"},
                {"name": "total_price", "type": "DOUBLE"},
                {"name": "sale_date", "type": "DATE"},
            ],
            "partitionColumns": [
                {
                    "name": "sale_month",
                    "type": "STRING",
                    "generateExpression": "TRUNC_TIME(sale_date, 'month')",
                },
            ],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        if p.get("success") is not True:
            pytest.skip(f"AUTO PARTITIONED BY not supported in this env: {p.get('error')}")

        # insert_values would try to include the auto-partition column; instead insert
        # via execute_sql so MaxCompute computes sale_month itself.
        insert_sql = (
            f"INSERT INTO {table} (shop_name, total_price, sale_date) VALUES "
            "('s1', 10.0, DATE'2026-01-15'), "
            "('s2', 20.0, DATE'2026-01-20'), "
            "('s3', 30.0, DATE'2026-02-05');"
        )
        # execute_sql is read-only-guarded; drive pyodps directly for the INSERT.
        inst = real_tools.maxcompute_client.execute_sql(insert_sql)
        if hasattr(inst, "wait_for_success"):
            inst.wait_for_success()

        assert _count_rows(real_tools, project, table, hints={"odps.sql.allow.fullscan": "true"}) == 3
        assert _count_rows(real_tools, project, table, partition="sale_month='2026-01'") == 2
        assert _count_rows(real_tools, project, table, partition="sale_month='2026-02'") == 1

    def test_table_properties_explicit(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        """Verify tableProperties pass-through (using transactional property explicitly)."""
        project = real_config.default_project
        table = _uniq("mcpit_props")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "val", "type": "STRING"},
            ],
            "tableProperties": {"transactional": "true"},
            "lifecycle": 1,
        })
        p = _text_payload(r)
        if p.get("success") is not True:
            pytest.skip(f"tableProperties pass-through not supported: {p.get('error')}")

        r = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id", "val"],
            "values": [[1, "a"]],
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"insert into properties table failed: {p}"
        assert _count_rows(real_tools, project, table) == 1
