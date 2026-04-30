"""Integration tests: real API calls when config file is present."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import data as _data, text_payload as _text_payload
from tests.conftest import has_config as _has_config

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCatalogReal:
    """Real Catalog API calls (list_projects, get_project, list_schemas, list_tables, get_table_schema, get_partition_info)."""

    def test_list_projects_real(self, real_tools: Tools) -> None:
        r = real_tools.call("list_projects", {"pageSize": 20})
        payload = _text_payload(r)
        assert "error" not in payload, f"list_projects should succeed, got error: {payload.get('error')}"
        data = _data(payload)
        assert "projects" in data
        assert isinstance(data["projects"], list)

    def test_get_project_real(self, real_tools: Tools, real_config: Any) -> None:
        project = real_config.default_project or "p1"
        r = real_tools.call("get_project", {"project": project})
        payload = _text_payload(r)
        assert "error" not in payload, f"get_project should succeed, got error: {payload.get('error')}"
        data = _data(payload)
        assert "projectId" in data or "name" in data
        assert data.get("projectId") == project or project in str(data.get("name", ""))

    def test_list_schemas_real(self, real_tools: Tools, real_config: Any) -> None:
        project = real_config.default_project or "p1"
        r = real_tools.call("list_schemas", {"project": project, "pageSize": 50})
        payload = _text_payload(r)
        assert "error" not in payload, f"list_schemas should succeed, got error: {payload.get('error')}"
        data = _data(payload)
        assert "schemas" in data
        assert isinstance(data["schemas"], list)

    def test_list_tables_real(self, real_tools: Tools, real_config: Any) -> None:
        project = real_config.default_project or "p1"
        r = real_tools.call("list_tables", {"project": project, "schema": "default", "pageSize": 50})
        payload = _text_payload(r)
        assert "error" not in payload, f"list_tables should succeed, got error: {payload.get('error')}"
        data = _data(payload)
        assert "tables" in data
        assert isinstance(data["tables"], list)

    def test_get_table_schema_real(self, real_tools: Tools, real_config: Any) -> None:
        project = real_config.default_project or "p1"
        list_r = real_tools.call("list_tables", {"project": project, "schema": "default", "pageSize": 5})
        list_payload = _text_payload(list_r)
        assert "error" not in list_payload
        list_data = _data(list_payload)
        tables = list_data.get("tables") or []
        if not tables:
            pytest.skip("no tables to get_table_schema")
        table_name = tables[0].get("tableName") or tables[0].get("name", "").split("/")[-1]
        r = real_tools.call("get_table_schema", {"project": project, "schema": "default", "table": table_name})
        payload = _text_payload(r)
        assert "error" not in payload, f"get_table_schema should succeed, got error: {payload.get('error')}"
        data = _data(payload)
        assert "columns" in data or "raw" in data

    def test_get_partition_info_real(self, real_tools: Tools, real_config: Any) -> None:
        project = real_config.default_project or "p1"
        list_r = real_tools.call("list_tables", {"project": project, "schema": "default", "pageSize": 20})
        list_payload = _text_payload(list_r)
        assert "error" not in list_payload
        list_data = _data(list_payload)
        tables = list_data.get("tables") or []
        if not tables:
            pytest.skip("no tables to get_partition_info")
        # Some tables may not be partitioned; try to find one that is
        for t in tables:
            table_name = t.get("tableName") or t.get("name", "").split("/")[-1]
            r = real_tools.call(
                "get_partition_info",
                {"project": project, "schema": "default", "table": table_name, "pageSize": 10},
            )
            payload = _text_payload(r)
            if "error" in payload:
                continue  # e.g. "table is not partitioned", try next table
            assert "partitions" in payload, f"get_partition_info should return partitions: {payload}"
            assert isinstance(payload["partitions"], list)
            return  # got valid response
        pytest.skip("no partitioned table found in first page; get_partition_info requires a partitioned table")


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestSearchReal:
    """Real search_meta_data (requires namespace_id in config + pyodps_catalog Client with search method)."""

    def test_search_meta_data_real(self, real_tools: Tools, real_config: Any) -> None:
        if not real_config.namespace_id:
            pytest.skip("namespace_id not set in config; search_meta_data requires it")
        r = real_tools.call("search_meta_data", {"query": "type=table,name:test", "pageSize": 5})
        payload = _text_payload(r)
        assert "error" not in payload, f"search_meta_data should succeed, got error: {payload.get('error')}"
        assert "entries" in payload, f"search_meta_data should return entries: {payload}"
        assert isinstance(payload["entries"], list)


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestComputeReal:
    """Real compute API (cost_sql, execute_sql optional)."""

    def test_cost_sql_real(self, real_tools: Tools, real_config: Any) -> None:
        project = real_config.default_project or "p1"
        r = real_tools.call("cost_sql", {"project": project, "sql": "SELECT 1"})
        payload = _text_payload(r)
        assert "error" not in payload, f"cost_sql should succeed, got error: {payload.get('error')}"
        assert "costEstimate" in payload

    def test_execute_sql_real(self, real_tools: Tools, real_config: Any) -> None:
        project = real_config.default_project or "p1"
        r = real_tools.call("execute_sql", {"project": project, "sql": "SELECT 1"})
        payload = _text_payload(r)
        assert "error" not in payload, f"execute_sql should succeed, got error: {payload.get('error')}"
        assert payload.get("success") is True or "columns" in payload
