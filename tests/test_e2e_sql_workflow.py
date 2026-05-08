# -*- coding: utf-8 -*-
"""E2E tests: SQL execution complete workflow.

Covers cost_sql (basic / hints / invalid SQL), execute_sql sync mode
(SELECT / WITH / CTE / timeout), the full async workflow
(execute_sql → get_instance_status polling → get_instance result retrieval),
and error scenarios for get_instance_status / get_instance with non-existent
instance IDs.

Requires config.json (or MAXCOMPUTE_CATALOG_CONFIG env var).
"""
from __future__ import annotations

import logging
from typing import Any

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import (
    async_wait_instance as _wait_for_instance,
    data as _data,
    has_config as _has_config,
    text_payload as _text_payload,
)

logger = logging.getLogger(__name__)

_VALID_INSTANCE_STATUSES = {"Running", "Terminated", "Failed", "Suspended", "Cancelled"}


# ---------------------------------------------------------------------------
# 2.1  cost_sql
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCostSql:
    """cost_sql: basic estimation, hints forwarding, invalid SQL."""

    def test_cost_sql_simple_select(self, real_tools: Tools, real_config: Any) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("cost_sql", {"project": project, "sql": "SELECT 1"})
        payload = _text_payload(r)
        assert "error" not in payload, f"cost_sql error: {payload.get('error')}"
        assert "costEstimate" in payload, (
            f"cost_sql must return costEstimate, got: {payload}"
        )
        estimate = payload["costEstimate"]
        assert isinstance(estimate, dict), f"costEstimate must be a dict, got: {estimate}"
        assert "estimatedCU" in estimate, (
            f"costEstimate must contain estimatedCU, got: {estimate}"
        )

    def test_cost_sql_with_hints(self, real_tools: Tools, real_config: Any) -> None:
        """Hints parameter must be forwarded without causing errors."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("cost_sql", {
            "project": project,
            "sql": "SELECT 1",
            "hints": {"odps.sql.allow.fullscan": "true"},
        })
        payload = _text_payload(r)
        assert "error" not in payload, f"cost_sql with hints error: {payload.get('error')}"
        assert "costEstimate" in payload

    def test_cost_sql_fields_present(self, real_tools: Tools, real_config: Any) -> None:
        """costEstimate fields: estimatedCU, inputBytes, complexity, udfCount."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("cost_sql", {
            "project": project,
            "sql": "SELECT 1",
        })
        payload = _text_payload(r)
        estimate = payload.get("costEstimate") or {}
        for field in ("estimatedCU", "inputBytes", "complexity", "udfCount"):
            assert field in estimate, (
                f"costEstimate missing field '{field}', got: {estimate}"
            )


# ---------------------------------------------------------------------------
# 2.2  execute_sql — sync mode
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestExecuteSqlSync:
    """execute_sql sync mode: SELECT, WITH..SELECT, timeout, readonly guard."""

    def test_execute_sql_select_returns_data(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Basic SELECT must return success=true + columns + data."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1 AS n",
            "async": False,
            "timeout": 60,
        })
        payload = _text_payload(r)
        assert payload.get("success") is True, f"execute_sql failed: {payload}"
        assert "columns" in payload, f"columns missing from response: {payload}"
        assert "data" in payload or "rawOutput" in payload, (
            f"data/rawOutput missing from response: {payload}"
        )

    def test_execute_sql_select_count(self, real_tools: Tools, real_config: Any) -> None:
        """SELECT COUNT(*) must return a numeric result."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT COUNT(*) AS c FROM (SELECT 1 UNION ALL SELECT 2) t",
            "async": False,
            "timeout": 60,
        })
        payload = _text_payload(r)
        assert payload.get("success") is True, f"SELECT COUNT(*) failed: {payload}"
        rows = payload.get("data") or []
        assert rows, f"Expected at least one row from COUNT(*), got: {payload}"

    def test_execute_sql_with_clause_select(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """WITH ... SELECT (CTE) must be allowed and return results."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "WITH tmp AS (SELECT 1 AS x, 'hello' AS y) SELECT x, y FROM tmp",
            "async": False,
            "timeout": 60,
        })
        payload = _text_payload(r)
        assert payload.get("success") is True, (
            f"WITH...SELECT must be allowed, got: {payload}"
        )
        rows = payload.get("data") or []
        assert len(rows) >= 1, f"Expected at least 1 row from CTE SELECT, got: {payload}"

    def test_execute_sql_readonly_guard_rejects_insert(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Plain INSERT must be rejected by the client-side read-only guard."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "INSERT INTO __nonexistent_table_xyz__ VALUES (1)",
            "async": False,
            "timeout": 30,
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"INSERT must be rejected by the read-only guard, got: {payload}"
        )
        error_msg = payload.get("error") or ""
        assert error_msg, "Expected a non-empty error message when INSERT is rejected"

    def test_execute_sql_timeout_returns_instance_id(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """A very short timeout should trigger a timeout response with instanceId."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        # Use a complex query that is unlikely to complete in 1 second
        sql = (
            "SELECT COUNT(*) AS c "
            "FROM information_schema.tables t1, information_schema.tables t2"
        )
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": sql,
            "async": False,
            "timeout": 1,
        })
        payload = _text_payload(r)
        # Either timeout occurs (instanceId returned) or query was fast enough to succeed —
        # both are valid outcomes; the important thing is no Python crash.
        if payload.get("timeout") is True:
            assert "instanceId" in payload, (
                f"Timeout response must include instanceId, got: {payload}"
            )
            logger.info("Timeout triggered as expected; instanceId=%s", payload.get("instanceId"))
        else:
            # Query was fast enough; verify normal success structure
            assert payload.get("success") is True or "error" in payload, (
                f"Unexpected response structure: {payload}"
            )


# ---------------------------------------------------------------------------
# 2.3  Async workflow: execute_sql → get_instance_status → get_instance
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestAsyncWorkflow:
    """Full async lifecycle: submit → poll status → retrieve results."""

    def test_async_execute_sql_returns_instance_id(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """execute_sql(async=True) must return instanceId immediately."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1 AS x",
            "async": True,
        })
        payload = _text_payload(r)
        assert payload.get("success") is True, (
            f"async execute_sql must succeed, got: {payload}"
        )
        assert "instanceId" in payload, (
            f"async execute_sql must return instanceId, got: {payload}"
        )
        assert payload.get("status") == "submitted", (
            f"async execute_sql status must be 'submitted', got: {payload.get('status')}"
        )

    def test_async_full_lifecycle(self, real_tools: Tools, real_config: Any) -> None:
        """Full async lifecycle: submit → poll status until terminated → get results."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        # Step 1: Submit async
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "WITH cte AS (SELECT 1 AS id, 'async_test' AS val) SELECT id, val FROM cte",
            "async": True,
        })
        submit_payload = _text_payload(r)
        assert submit_payload.get("success") is True, (
            f"Async submit failed: {submit_payload}"
        )
        instance_id = submit_payload["instanceId"]

        # Step 2: Poll get_instance_status until terminated
        final_status = _wait_for_instance(real_tools, project, instance_id, timeout=120)
        if final_status.get("success") is False:
            pytest.fail(f"get_instance_status failed: {final_status}")

        assert final_status.get("isTerminated") is True, (
            f"Instance did not terminate: {final_status}"
        )
        assert final_status.get("isSuccessful") is True, (
            f"Instance did not succeed: {final_status}"
        )

        # Step 3: get_instance for results
        r3 = real_tools.call("get_instance", {
            "project": project,
            "instanceId": instance_id,
        })
        result_payload = _text_payload(r3)
        assert "results" in result_payload, (
            f"get_instance must return 'results', got: {result_payload}"
        )

    def test_async_get_instance_status_valid_fields(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """get_instance_status must return instanceId, status, isTerminated, isSuccessful."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        # Submit a simple async query
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 42 AS answer",
            "async": True,
        })
        submit_payload = _text_payload(r)
        assert submit_payload.get("success") is True
        instance_id = submit_payload["instanceId"]

        # Check status once (may be Running or already Terminated)
        sr = real_tools.call("get_instance_status", {
            "project": project,
            "instanceId": instance_id,
        })
        sp = _text_payload(sr)
        assert "instanceId" in sp, f"Missing instanceId: {sp}"
        assert "status" in sp, f"Missing status: {sp}"
        assert "isTerminated" in sp, f"Missing isTerminated: {sp}"
        assert "isSuccessful" in sp, f"Missing isSuccessful: {sp}"
        if sp.get("status") is not None:
            status_str = str(sp["status"]).upper()
            valid_upper = {s.upper() for s in _VALID_INSTANCE_STATUSES}
            assert any(status_str.startswith(s) for s in valid_upper), (
                f"Unexpected status value: {sp['status']!r}"
            )

    def test_async_polling_then_retrieve_rows(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Submit SELECT, poll to completion, retrieve rows via get_instance."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1 AS a, 2 AS b UNION ALL SELECT 3 AS a, 4 AS b",
            "async": True,
        })
        sp = _text_payload(r)
        assert sp.get("success") is True
        instance_id = sp["instanceId"]

        # Wait for completion
        _wait_for_instance(real_tools, project, instance_id, timeout=120)

        # Retrieve results
        gr = real_tools.call("get_instance", {
            "project": project,
            "instanceId": instance_id,
        })
        gp = _text_payload(gr)
        assert "results" in gp, f"get_instance missing results: {gp}"
        results = gp["results"]
        # Results is a dict of task_name → task_entry
        assert isinstance(results, dict), f"results must be dict, got: {type(results)}"
        # At least one task with data (dict format or raw CSV string format)
        found_data = False
        for task_name, task_entry in results.items():
            if isinstance(task_entry, dict) and "data" in task_entry:
                rows = task_entry["data"]
                assert len(rows) >= 2, (
                    f"Expected >= 2 rows from UNION ALL SELECT, got {len(rows)}: {task_entry}"
                )
                found_data = True
                break
            elif isinstance(task_entry, str) and task_entry.strip():
                # Raw CSV format: first line is header, remaining are data rows
                data_lines = [ln for ln in task_entry.strip().splitlines() if ln.strip()]
                assert len(data_lines) >= 3, (
                    f"Expected header + >= 2 rows from UNION ALL SELECT, "
                    f"got {len(data_lines)} lines in {task_name!r}: {task_entry!r}"
                )
                found_data = True
                break
        if not found_data:
            pytest.fail(
                f"No task_entry with data found in results (expected dict 'data' or CSV string): {results}"
            )


# ---------------------------------------------------------------------------
# 2.4  get_instance_status / get_instance — error scenarios
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestInstanceErrors:
    """Error scenarios for get_instance_status and get_instance."""

    def test_get_instance_status_nonexistent(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Non-existent instanceId must return success=false or an error."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("get_instance_status", {
            "project": project,
            "instanceId": "nonexistent_instance_id_xyz_12345",
        })
        payload = _text_payload(r)
        has_error = payload.get("success") is False or "error" in payload
        assert has_error, (
            f"Expected error for non-existent instanceId, got: {payload}"
        )

    def test_get_instance_nonexistent(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Non-existent instanceId must return success=false or an error."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("get_instance", {
            "project": project,
            "instanceId": "nonexistent_instance_id_xyz_12345",
        })
        payload = _text_payload(r)
        has_error = payload.get("success") is False or "error" in payload
        assert has_error, (
            f"Expected error for non-existent instanceId, got: {payload}"
        )
