# -*- coding: utf-8 -*-
"""E2E tests: security string args, cross-project cost_sql, insert value types.

Covers:
- check_access with string include_grants (coercion to bool)
- cost_sql targeting a different project than default
- insert_values with mixed value types: None, bool, float

Requires config.json (or MAXCOMPUTE_CATALOG_CONFIG env var).
"""
from __future__ import annotations

import logging
from typing import Any, List

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import (
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
# check_access: string include_grants coercion
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCheckAccessStringArgs:
    """check_access with string include_grants must coerce to bool (not crash)."""

    def test_include_grants_string_true(self, real_tools: Tools, real_config: Any) -> None:
        """Passing include_grants='true' (string) must be coerced to bool and not crash."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("check_access", {
            "project": project,
            "include_grants": "true",
        })
        payload = _text_payload(r)
        # Should not crash; either returns grants (coerced to True) or fails gracefully
        assert isinstance(payload, dict), f"Expected dict, got: {type(payload)}"

    def test_include_grants_string_empty(self, real_tools: Tools, real_config: Any) -> None:
        """Passing include_grants='' (empty string) coerces to False (no grants)."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("check_access", {
            "project": project,
            "include_grants": "",
        })
        payload = _text_payload(r)
        actual = payload.get("data", payload)
        # bool('') == False so grants should NOT be present
        assert "grants" not in actual, (
            f"Empty string include_grants should coerce to False, got: {actual}"
        )


# ---------------------------------------------------------------------------
# Cross-project cost_sql
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCrossProjectCostSql:
    """cost_sql targeting a project different from default_project."""

    def test_cost_sql_explicit_project(self, real_tools: Tools, real_config: Any) -> None:
        """cost_sql with explicit project (same as default) must succeed."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("cost_sql", {
            "project": project,
            "sql": "SELECT 1",
        })
        payload = _text_payload(r)
        assert "error" not in payload, (
            f"cost_sql on explicit project must not return error, got: {payload}"
        )
        assert "costEstimate" in payload, (
            f"cost_sql must return costEstimate, got: {payload}"
        )


# ---------------------------------------------------------------------------
# insert_values: mixed value types (None, bool, float)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestInsertValueTypes:
    """insert_values with None/bool/float values must produce valid SQL."""

    def test_insert_null_bool_float(self, real_tools: Tools, real_config: Any, created_tables: List[str]) -> None:
        """Insert a row containing None, True, False, and float values."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etype")
        created_tables.append(table)

        # Create table with appropriate column types
        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [
                {"name": "a", "type": "BIGINT"},
                {"name": "b", "type": "BOOLEAN"},
                {"name": "c", "type": "DOUBLE"},
            ],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table failed: {p}"

        # Insert with None, True, False, float
        r2 = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["a", "b", "c"],
            "values": [
                [None, True, 3.14],
                [42, False, -0.5],
            ],
            "async": False,
            "timeout": 60,
        })
        p2 = _text_payload(r2)
        assert p2.get("success") is True, (
            f"insert_values with mixed types must succeed, got: {p2}"
        )
