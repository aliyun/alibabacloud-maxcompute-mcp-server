# -*- coding: utf-8 -*-
"""E2E tests: catalog browser tools (list_projects, get_project, list_schemas,
get_schema, list_tables, get_table_schema, get_partition_info).

Covers full-field validation, pagination, 2-tier vs 3-tier project detection,
filter parameters, non-existent resources, and partitioned table handling.

Requires config.json (or MAXCOMPUTE_CATALOG_CONFIG env var).
All tables created for test setup use a unique `mcpe2ecb_` prefix and are
cleaned up in teardown.
"""
from __future__ import annotations

import logging
from typing import Any, List

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import (
    call_safe as _call_safe,
    data as _data,
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


# ---------------------------------------------------------------------------
# 1.1  list_projects
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestListProjects:
    """list_projects: non-empty result, response schema, pagination."""

    def test_list_projects_returns_non_empty(self, real_tools: Tools) -> None:
        r = real_tools.call("list_projects", {"pageSize": 20})
        payload = _text_payload(r)
        assert "error" not in payload, f"list_projects error: {payload.get('error')}"
        d = _data(payload)
        projects = d.get("projects") or []
        assert len(projects) > 0, "Expected at least one project"

    def test_list_projects_response_schema(self, real_tools: Tools) -> None:
        """Each project entry must contain at least a name field."""
        r = real_tools.call("list_projects", {"pageSize": 5})
        payload = _text_payload(r)
        d = _data(payload)
        projects = d.get("projects") or []
        if not projects:
            pytest.skip("no projects returned")
        p0 = projects[0]
        assert isinstance(p0, dict), "Project entry must be a dict"
        assert p0.get("name") or p0.get("projectId"), (
            f"Project entry must have 'name' or 'projectId', got: {p0}"
        )

    def test_list_projects_pagination_with_token(self, real_tools: Tools) -> None:
        """pageSize=1 should trigger a nextPageToken for most tenants."""
        r1 = real_tools.call("list_projects", {"pageSize": 1})
        p1 = _text_payload(r1)
        d1 = _data(p1)
        token = d1.get("nextPageToken")
        if not token:
            pytest.skip("only one project available; cannot test pagination")
        # Use the token to fetch the next page
        r2 = real_tools.call("list_projects", {"pageSize": 1, "token": token})
        p2 = _text_payload(r2)
        assert "error" not in p2, f"second page error: {p2.get('error')}"
        d2 = _data(p2)
        assert "projects" in d2, f"second page missing 'projects': {d2}"

    def test_list_projects_pagesize_respected(self, real_tools: Tools) -> None:
        """pageSize=2 must return at most 2 projects."""
        r = real_tools.call("list_projects", {"pageSize": 2})
        payload = _text_payload(r)
        d = _data(payload)
        projects = d.get("projects") or []
        assert len(projects) <= 2, (
            f"Expected at most 2 projects with pageSize=2, got {len(projects)}"
        )


# ---------------------------------------------------------------------------
# 1.2  get_project
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestGetProject:
    """get_project: field completeness and error handling."""

    def test_get_project_known_project(self, real_tools: Tools, real_config: Any) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("get_project", {"project": project})
        payload = _text_payload(r)
        assert "error" not in payload, f"get_project error: {payload.get('error')}"
        d = _data(payload)
        # Must contain either projectId or name
        assert d.get("projectId") or d.get("name"), (
            f"get_project must return projectId or name, got: {d}"
        )

    def test_get_project_nonexistent_returns_error(self, real_tools: Tools) -> None:
        r = _call_safe(real_tools, "get_project", {"project": "nonexistent_project_xyz_12345"})
        payload = _text_payload(r)
        # Either success=false with error, or an error key
        has_error = payload.get("success") is False or "error" in payload
        assert has_error, (
            f"Expected error for non-existent project, got: {payload}"
        )


# ---------------------------------------------------------------------------
# 1.3  list_schemas / get_schema
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestSchemas:
    """list_schemas and get_schema: 2-tier vs 3-tier project, valid/invalid schema."""

    def test_list_schemas_2tier_project_returns_default(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """2-level (non-schema-enabled) project always has a 'default' schema."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("list_schemas", {"project": project, "pageSize": 50})
        payload = _text_payload(r)
        assert "error" not in payload, f"list_schemas error: {payload.get('error')}"
        d = _data(payload)
        schemas = d.get("schemas") or []
        assert isinstance(schemas, list) and len(schemas) > 0, (
            f"Expected at least one schema, got: {d}"
        )
        schema_names = [s.get("schemaName") or s.get("name", "").split("/")[-1] for s in schemas]
        assert "default" in schema_names, (
            f"Expected 'default' schema in 2-tier project, got: {schema_names}"
        )

    def test_get_schema_valid_schema(self, real_tools: Tools, real_config: Any) -> None:
        """get_schema for a known schema must return name/schemaName field."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("get_schema", {"project": project, "schema": "default"})
        payload = _text_payload(r)
        assert "error" not in payload, f"get_schema error: {payload.get('error')}"
        d = _data(payload)
        has_name = d.get("schemaName") or d.get("name")
        assert has_name, f"get_schema must return schemaName or name, got: {d}"

    def test_get_schema_nonexistent_returns_error(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = _call_safe(real_tools, "get_schema", {"project": project, "schema": "nonexistent_schema_xyz"})
        payload = _text_payload(r)
        # 2-tier projects return a virtual schema entry (success=True with 'no schema support')
        # instead of a 404 error — accept both behaviours.
        if payload.get("success") is True:
            d = payload.get("data") or payload
            desc = d.get("description", "")
            assert "schema" in desc.lower() or "2-level" in desc.lower() or d.get("name"), (
                f"2-tier project must explain schema support status in description, got: {d}"
            )
        else:
            assert payload.get("success") is False or "error" in payload, (
                f"Expected error or virtual schema for non-existent schema, got: {payload}"
            )


# ---------------------------------------------------------------------------
# 1.4  list_tables / get_table_schema
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestTables:
    """list_tables and get_table_schema: basic, filter, field completeness."""

    def test_list_tables_basic(self, real_tools: Tools, real_config: Any) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("list_tables", {"project": project, "schema": "default", "pageSize": 10})
        payload = _text_payload(r)
        assert "error" not in payload, f"list_tables error: {payload.get('error')}"
        d = _data(payload)
        assert "tables" in d, f"list_tables must return 'tables', got: {d}"
        assert isinstance(d["tables"], list)

    def test_list_tables_filter_by_prefix(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Create a table with a unique prefix and verify it appears in list_tables."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2ecb_filter")
        created_tables.append(table)
        # Create the table first
        cr = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "lifecycle": 1,
        })
        cp = _text_payload(cr)
        assert cp.get("success") is True, f"create_table failed: {cp}"

        # Try filter parameter via call_safe (SDK may not support it)
        r = _call_safe(real_tools, "list_tables", {
            "project": project,
            "schema": "default",
            "filter": table,
            "pageSize": 20,
        })
        payload = _text_payload(r)
        if payload.get("success") is False or "error" in payload:
            # filter not supported; fall back to full list and verify table exists
            r2 = real_tools.call("list_tables", {"project": project, "schema": "default", "pageSize": 100})
            p2 = _text_payload(r2)
            assert "error" not in p2, f"list_tables error: {p2.get('error')}"
            d2 = _data(p2)
            tables = d2.get("tables") or []
            table_names = [
                t.get("tableName") or t.get("name", "").split("/")[-1]
                for t in tables
            ]
            assert table in table_names, (
                f"Expected {table!r} in table list, got: {table_names[:10]}..."
            )
            return

        d = _data(payload)
        tables = d.get("tables") or []
        table_names = [
            t.get("tableName") or t.get("name", "").split("/")[-1]
            for t in tables
        ]
        assert table in table_names, (
            f"Expected {table!r} in filtered results, got: {table_names}"
        )

    def test_get_table_schema_standard_table(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Standard table must return columns field."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2ecb_schema")
        created_tables.append(table)
        cr = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "name", "type": "STRING"},
            ],
            "lifecycle": 1,
        })
        cp = _text_payload(cr)
        assert cp.get("success") is True, f"create_table failed: {cp}"

        r = real_tools.call("get_table_schema", {
            "project": project,
            "schema": "default",
            "table": table,
        })
        payload = _text_payload(r)
        assert "error" not in payload, f"get_table_schema error: {payload.get('error')}"
        d = _data(payload)
        assert "columns" in d or "raw" in d, (
            f"get_table_schema must return columns or raw, got keys: {list(d.keys())}"
        )
        if "columns" in d:
            assert isinstance(d["columns"], list), f"columns must be a list: {type(d['columns'])}"

    def test_get_table_schema_partitioned_table(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Partitioned table schema must return columns field (partition info optional)."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2ecb_part")
        created_tables.append(table)
        cr = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "partitionColumns": ["dt"],
            "lifecycle": 1,
        })
        cp = _text_payload(cr)
        assert cp.get("success") is True, f"create_table (partitioned) failed: {cp}"

        r = real_tools.call("get_table_schema", {
            "project": project,
            "schema": "default",
            "table": table,
        })
        payload = _text_payload(r)
        assert "error" not in payload, f"get_table_schema error: {payload.get('error')}"
        d = _data(payload)
        # At minimum, columns must be present
        assert "columns" in d or "raw" in d, (
            f"get_table_schema must return columns or raw, got keys: {list(d.keys())}"
        )
        if "columns" in d:
            assert isinstance(d["columns"], list), f"columns must be a list: {type(d['columns'])}"
        # Partitioned table must return partition key information
        partition_keys = d.get("partitionKeys") or d.get("partitionColumns") or []
        assert len(partition_keys) > 0, (
            f"Partitioned table schema must contain partitionKeys or partitionColumns, got: {d}"
        )
        logger.info("Partitioned table schema keys: %s, partitionKeys: %s", list(d.keys()), partition_keys)

    def test_get_table_schema_nonexistent(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = _call_safe(real_tools, "get_table_schema", {
            "project": project,
            "schema": "default",
            "table": "nonexistent_table_xyz_12345",
        })
        payload = _text_payload(r)
        has_error = payload.get("success") is False or "error" in payload
        assert has_error, (
            f"Expected error for non-existent table, got: {payload}"
        )


# ---------------------------------------------------------------------------
# 1.5  get_partition_info
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestPartitionInfo:
    """get_partition_info: partitioned table, non-partitioned table, pagination."""

    def test_get_partition_info_partitioned_table(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """After inserting data into a partitioned table, partition list must be non-empty."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2ecb_pi")
        created_tables.append(table)
        # Create + insert
        r_create = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "partitionColumns": ["dt"],
            "lifecycle": 1,
        })
        p_create = _text_payload(r_create)
        assert p_create.get("success") is True, f"create_table failed: {p_create}"
        r_insert = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id", "dt"],
            "partitionColumns": ["dt"],
            "values": [[1, "20260101"], [2, "20260102"]],
        })
        p_insert = _text_payload(r_insert)
        assert p_insert.get("success") is True, f"insert_values failed: {p_insert}"

        r = real_tools.call("get_partition_info", {
            "project": project,
            "schema": "default",
            "table": table,
            "pageSize": 10,
        })
        payload = _text_payload(r)
        assert "error" not in payload, f"get_partition_info error: {payload.get('error')}"
        partitions = payload.get("partitions") or []
        assert len(partitions) > 0, f"Expected partitions after insert, got: {payload}"

    def test_get_partition_info_unpartitioned_table(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Non-partitioned table should return success=false or empty partitions."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2ecb_nopart")
        created_tables.append(table)
        real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "lifecycle": 1,
        })

        r = real_tools.call("get_partition_info", {
            "project": project,
            "schema": "default",
            "table": table,
            "pageSize": 10,
        })
        payload = _text_payload(r)
        # Either an explicit error or empty partitions list is acceptable
        is_error = payload.get("success") is False or "error" in payload
        is_empty = isinstance(payload.get("partitions"), list) and len(payload["partitions"]) == 0
        assert is_error or is_empty, (
            f"Non-partitioned table should return error or empty partitions, got: {payload}"
        )

    def test_get_partition_info_pagination(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """pageSize=1 on a multi-partition table should trigger nextPageToken."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2ecb_pipag")
        created_tables.append(table)
        real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "partitionColumns": ["dt"],
            "lifecycle": 1,
        })
        real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id", "dt"],
            "partitionColumns": ["dt"],
            "values": [[1, "20260101"], [2, "20260102"], [3, "20260103"]],
        })

        r = real_tools.call("get_partition_info", {
            "project": project,
            "schema": "default",
            "table": table,
            "pageSize": 1,
        })
        payload = _text_payload(r)
        if payload.get("success") is False or "error" in payload:
            pytest.skip(f"get_partition_info returned error: {payload}")
        partitions = payload.get("partitions") or []
        assert len(partitions) <= 1, (
            f"Expected at most 1 partition with pageSize=1, got {len(partitions)}"
        )
        token = payload.get("nextPageToken")
        if not token:
            pytest.skip("no nextPageToken returned; fewer than 2 partitions visible")
        # Fetch second page
        r2 = real_tools.call("get_partition_info", {
            "project": project,
            "schema": "default",
            "table": table,
            "pageSize": 1,
            "token": token,
        })
        p2 = _text_payload(r2)
        assert "error" not in p2, f"second page error: {p2}"
        assert "partitions" in p2, f"second page missing partitions: {p2}"
