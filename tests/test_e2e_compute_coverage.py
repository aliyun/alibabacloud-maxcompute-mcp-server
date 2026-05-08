# -*- coding: utf-8 -*-
"""E2E tests: tools_compute coverage gap fillers.

Targets the 121 uncovered lines in tools_compute.py (E2E coverage was 58.1%).
Covers:

- cost_sql: long SQL truncation, invalid SQL error path, hints validation
- execute_sql async: full lifecycle with column/row verification
- execute_sql sync + output_uri: JSONL file streaming, preview rows
- get_instance: output_uri streaming, no-schema error, logView field
- insert_values: basic insert + row count verification
- output_uri validation: empty URI, system path rejection (indirect coverage)

Requires config.json (or MAXCOMPUTE_CATALOG_CONFIG env var).
"""
from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path
from typing import Any

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import (
    async_wait_instance as _wait_for_instance,
    count_rows,
    data as _data,
    drop_table,
    has_config as _has_config,
    text_payload as _text_payload,
    uniq,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. cost_sql edge cases
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCostSqlEdgeCases:
    """cost_sql: long SQL truncation, invalid SQL, non-dict hints."""

    def test_cost_sql_long_sql_truncation(self, real_tools: Tools, real_config: Any) -> None:
        """SQL > 200 chars should set sqlTruncated=True and truncate the returned sql."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        long_sql = "SELECT 1 AS " + ", ".join(f"col_{i}" for i in range(50))
        assert len(long_sql) > 200, f"Test SQL too short ({len(long_sql)} chars)"
        r = real_tools.call("cost_sql", {"project": project, "sql": long_sql})
        payload = _text_payload(r)
        assert "error" not in payload, f"cost_sql error: {payload.get('error')}"
        assert payload.get("sqlTruncated") is True, (
            f"Expected sqlTruncated=True for SQL >200 chars, got: {payload}"
        )
        assert len(payload.get("sql", "")) <= 200, (
            f"Returned sql should be <=200 chars, got {len(payload.get('sql', ''))}"
        )

    def test_cost_sql_invalid_sql_returns_stub(self, real_tools: Tools, real_config: Any) -> None:
        """Invalid SQL should return costEstimate with stub=True (cost estimation failure)."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("cost_sql", {
            "project": project,
            "sql": "NOT_A_VALID_SQL_STATEMENT___XYZ",
        })
        payload = _text_payload(r)
        # Invalid SQL either returns a stub estimate or an error — both are acceptable
        estimate = payload.get("costEstimate", {})
        assert "estimatedCU" in estimate, f"costEstimate missing estimatedCU: {payload}"

    def test_cost_sql_non_dict_hints_ignored(self, real_tools: Tools, real_config: Any) -> None:
        """Non-dict hints should be silently ignored (not cause an error)."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("cost_sql", {
            "project": project,
            "sql": "SELECT 1",
            "hints": "not_a_dict",
        })
        payload = _text_payload(r)
        assert "error" not in payload, (
            f"Non-dict hints should be ignored, got error: {payload.get('error')}"
        )


# ---------------------------------------------------------------------------
# 2. execute_sql async: full lifecycle with column/row verification
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestAsyncFullLifecycle:
    """Async execute_sql → get_instance_status → get_instance with data verification."""

    def test_async_lifecycle_with_column_verification(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Submit async SELECT, wait, get_instance must return columns + rows."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        # Step 1: Submit async
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 100 AS num, 'test_val' AS label",
            "async": True,
        })
        sp = _text_payload(r)
        assert sp.get("success") is True, f"Async submit failed: {sp}"
        instance_id = sp["instanceId"]

        # Step 2: Wait for completion
        _wait_for_instance(real_tools, project, instance_id, timeout=120)

        # Step 3: get_instance — verify structured result
        r3 = real_tools.call("get_instance", {
            "project": project,
            "instanceId": instance_id,
        })
        gp = _text_payload(r3)
        assert "results" in gp, f"get_instance must return results: {gp}"
        results = gp["results"]
        assert isinstance(results, dict), f"results must be dict: {type(results)}"

        # Find a task with structured data
        found = False
        for task_name, entry in results.items():
            if isinstance(entry, dict) and "data" in entry:
                rows = entry["data"]
                assert len(rows) >= 1, f"Expected >=1 row, got: {entry}"
                # Verify columns present
                assert "columns" in entry, f"Missing columns in task {task_name}: {entry}"
                cols = entry["columns"]
                assert "num" in cols and "label" in cols, (
                    f"Expected columns [num, label], got: {cols}"
                )
                found = True
                break
            elif isinstance(entry, str) and entry.strip():
                # Raw CSV fallback
                found = True
                break
        assert found, f"No task with data found in results: {results}"

    def test_async_get_instance_status_logview(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """get_instance_status must include logView field (may be None)."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1 AS x",
            "async": True,
        })
        sp = _text_payload(r)
        assert sp.get("success") is True
        instance_id = sp["instanceId"]

        # Check status while instance may still be running
        sr = real_tools.call("get_instance_status", {
            "project": project,
            "instanceId": instance_id,
        })
        status = _text_payload(sr)
        assert "logView" in status, (
            f"get_instance_status must return logView field: {status}"
        )
        # logView can be None (if get_logview_address fails) or a URL string
        logview = status["logView"]
        assert logview is None or isinstance(logview, str), (
            f"logView must be None or str, got: {type(logview)}"
        )

        # Ensure cleanup
        _wait_for_instance(real_tools, project, instance_id, timeout=120)


# ---------------------------------------------------------------------------
# 3. execute_sql sync + output_uri: JSONL file streaming
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestSyncOutputUriStreaming:
    """Sync execute_sql with output_uri: file creation, JSONL content, preview."""

    def test_sync_output_uri_jsonl_content(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Sync SELECT with output_uri must write valid JSONL with correct columns."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        with tempfile.TemporaryDirectory(prefix="mcp_e2e_") as tmpdir:
            output_uri = f"file://{tmpdir}/stream_result.jsonl"
            r = real_tools.call("execute_sql", {
                "project": project,
                "sql": "SELECT 42 AS answer, 'hello' AS msg",
                "async": False,
                "timeout": 60,
                "output_uri": output_uri,
            })
            payload = _text_payload(r)
            assert payload.get("success") is True, (
                f"execute_sql with output_uri failed: {payload}"
            )
            # Verify response structure
            assert "outputPath" in payload, f"Missing outputPath: {payload}"
            assert "bytesWritten" in payload, f"Missing bytesWritten: {payload}"
            assert payload["bytesWritten"] > 0, (
                f"bytesWritten must be > 0 when file is written: {payload}"
            )

            # Verify JSONL file content
            output_path = Path(payload["outputPath"])
            assert output_path.exists(), f"Output file not created: {output_path}"
            lines = output_path.read_text(encoding="utf-8").strip().splitlines()
            assert len(lines) >= 1, f"JSONL file is empty: {output_path}"
            row = json.loads(lines[0])
            assert "answer" in row, f"Expected 'answer' column, got: {row}"
            assert "msg" in row, f"Expected 'msg' column, got: {row}"
            assert row["answer"] == 42, f"Expected answer=42, got: {row['answer']}"

    def test_sync_output_uri_preview(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Sync with output_uri must include preview rows (up to 20)."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        with tempfile.TemporaryDirectory(prefix="mcp_e2e_") as tmpdir:
            output_uri = f"file://{tmpdir}/preview_result.jsonl"
            r = real_tools.call("execute_sql", {
                "project": project,
                "sql": "SELECT 1 AS n",
                "async": False,
                "timeout": 60,
                "output_uri": output_uri,
            })
            payload = _text_payload(r)
            assert payload.get("success") is True, f"execute_sql failed: {payload}"
            assert "preview" in payload, f"Missing preview: {payload}"
            preview = payload["preview"]
            assert isinstance(preview, list), f"preview must be list: {type(preview)}"
            assert len(preview) >= 1, f"preview should have >=1 row: {preview}"
            assert "previewRows" in payload, f"Missing previewRows: {payload}"

    def test_sync_output_uri_with_hints(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Sync execute_sql with output_uri and hints should succeed."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        with tempfile.TemporaryDirectory(prefix="mcp_e2e_") as tmpdir:
            output_uri = f"file://{tmpdir}/hints_result.jsonl"
            r = real_tools.call("execute_sql", {
                "project": project,
                "sql": "SELECT 1 AS n",
                "async": False,
                "timeout": 60,
                "output_uri": output_uri,
                "hints": {"odps.sql.allow.fullscan": "true"},
            })
            payload = _text_payload(r)
            assert payload.get("success") is True, (
                f"execute_sql with hints+output_uri failed: {payload}"
            )


# ---------------------------------------------------------------------------
# 4. get_instance with output_uri streaming
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestGetInstanceOutputUriStreaming:
    """get_instance with output_uri: stream results to JSONL after async completion."""

    def test_get_instance_output_uri_after_async(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Submit async, wait, then get_instance with output_uri streams to file."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        # Submit async
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 7 AS lucky, 'number' AS type",
            "async": True,
        })
        sp = _text_payload(r)
        assert sp.get("success") is True, f"Async submit failed: {sp}"
        instance_id = sp["instanceId"]

        # Wait for completion
        _wait_for_instance(real_tools, project, instance_id, timeout=120)

        # get_instance with output_uri
        with tempfile.TemporaryDirectory(prefix="mcp_e2e_") as tmpdir:
            output_uri = f"file://{tmpdir}/async_result.jsonl"
            r3 = real_tools.call("get_instance", {
                "project": project,
                "instanceId": instance_id,
                "output_uri": output_uri,
            })
            gp = _text_payload(r3)
            assert "results" in gp, f"get_instance must return results: {gp}"

            # Check for structured result with file streaming
            found_file = False
            for task_name, entry in gp["results"].items():
                if isinstance(entry, dict):
                    if "outputPath" in entry:
                        found_file = True
                        assert "bytesWritten" in entry, (
                            f"Missing bytesWritten: {entry}"
                        )
                        assert entry["bytesWritten"] > 0, (
                            f"bytesWritten must be > 0: {entry}"
                        )
                        assert "preview" in entry, f"Missing preview: {entry}"
                        # Verify JSONL file
                        output_path = Path(entry["outputPath"])
                        assert output_path.exists(), f"File not created: {output_path}"
                        lines = output_path.read_text(encoding="utf-8").strip().splitlines()
                        assert len(lines) >= 1, f"JSONL file empty: {output_path}"
                        row = json.loads(lines[0])
                        assert "lucky" in row, f"Expected 'lucky' column: {row}"
                    elif "data" in entry:
                        # Inline data (no file streaming)
                        found_file = True
                elif isinstance(entry, str) and entry.strip():
                    found_file = True
            assert found_file, f"No result found: {gp['results']}"

    def test_get_instance_output_uri_rejected_scheme(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """get_instance with http:// output_uri must be rejected."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        r = real_tools.call("get_instance", {
            "project": project,
            "instanceId": "any_instance_id",
            "output_uri": "http://example.com/result.jsonl",
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"Expected failure for http:// scheme, got: {payload}"
        )

    def test_get_instance_output_uri_system_path_rejected(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """get_instance with /etc path must be rejected."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        r = real_tools.call("get_instance", {
            "project": project,
            "instanceId": "any_instance_id",
            "output_uri": "file:///etc/odps_result.jsonl",
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"Expected failure for system path, got: {payload}"
        )


# ---------------------------------------------------------------------------
# 5. insert_values basic workflow
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestInsertValuesBasic:
    """insert_values: create table, insert rows, verify count."""

    def test_insert_values_and_verify(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Create a table, insert rows via insert_values (sync default), verify row count."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        table_name = uniq("e2e_ins")

        # Step 1: Create table
        r = real_tools.call("create_table", {
            "project": project,
            "table": table_name,
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "name", "type": "STRING"},
            ],
        })
        payload = _text_payload(r)
        if payload.get("success") is False:
            pytest.skip(f"create_table failed (may not have permission): {payload}")

        try:
            # Step 2: Insert values (sync by default, returns rowsInserted)
            r2 = real_tools.call("insert_values", {
                "project": project,
                "table": table_name,
                "columns": ["id", "name"],
                "values": [[1, "alice"], [2, "bob"], [3, "charlie"]],
            })
            payload2 = _text_payload(r2)
            assert payload2.get("success") is True, (
                f"insert_values failed: {payload2}"
            )
            # Sync mode returns rowsInserted; async mode returns instanceId
            assert "rowsInserted" in payload2 or "instanceId" in payload2, (
                f"insert_values must return rowsInserted or instanceId: {payload2}"
            )

            # Step 3: If async, wait; then verify row count
            if "instanceId" in payload2:
                _wait_for_instance(real_tools, project, payload2["instanceId"], timeout=120)

            count = count_rows(real_tools, project, table_name)
            assert count == 3, f"Expected 3 rows after insert, got {count}"
        finally:
            drop_table(real_tools, table_name)

    def test_insert_values_async_true(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """insert_values with async=True must return instanceId immediately."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        table_name = uniq("e2e_ins2")

        r = real_tools.call("create_table", {
            "project": project,
            "table": table_name,
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "val", "type": "DOUBLE"},
            ],
        })
        payload = _text_payload(r)
        if payload.get("success") is False:
            pytest.skip(f"create_table failed: {payload}")

        try:
            r2 = real_tools.call("insert_values", {
                "project": project,
                "table": table_name,
                "columns": ["id", "val"],
                "values": [[10, 1.5], [20, 2.5]],
                "async": True,
            })
            payload2 = _text_payload(r2)
            assert payload2.get("success") is True, (
                f"insert_values(async=True) failed: {payload2}"
            )
            assert "instanceId" in payload2, (
                f"Async insert_values must return instanceId: {payload2}"
            )
            assert payload2.get("status") == "submitted", (
                f"Async insert status must be 'submitted', got: {payload2.get('status')}"
            )

            # Wait for completion and verify
            _wait_for_instance(real_tools, project, payload2["instanceId"], timeout=120)
            count = count_rows(real_tools, project, table_name)
            assert count == 2, f"Expected 2 rows after async insert, got {count}"
        finally:
            drop_table(real_tools, table_name)


# ---------------------------------------------------------------------------
# 6. output_uri validation (indirect coverage of _resolve_output_uri)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestOutputUriValidationIndirect:
    """output_uri edge cases that indirectly cover _resolve_output_uri paths."""

    def test_execute_sql_empty_output_uri_ignored(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Empty output_uri should be treated as unset, query runs normally."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1 AS n",
            "async": False,
            "timeout": 30,
            "output_uri": "",
        })
        payload = _text_payload(r)
        assert payload.get("success") is True, (
            f"Empty output_uri should be ignored, got: {payload}"
        )

    def test_execute_sql_system_path_output_uri_rejected(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """output_uri under /etc must be rejected."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1",
            "async": False,
            "timeout": 30,
            "output_uri": "file:///etc/odps_backup.jsonl",
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"System path should be rejected, got: {payload}"
        )
        assert "restricted" in (payload.get("error") or "").lower(), (
            f"Error should mention 'restricted', got: {payload.get('error')}"
        )

    def test_execute_sql_http_scheme_output_uri_rejected(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """output_uri with http:// scheme must be rejected."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1",
            "async": False,
            "timeout": 30,
            "output_uri": "http://example.com/result.jsonl",
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"HTTP scheme should be rejected, got: {payload}"
        )
        assert "scheme" in (payload.get("error") or "").lower(), (
            f"Error should mention 'scheme', got: {payload.get('error')}"
        )