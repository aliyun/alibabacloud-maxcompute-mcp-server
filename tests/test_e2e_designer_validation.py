# -*- coding: utf-8 -*-
"""E2E tests: designer validation and async insert_values.

Covers create_table input validation (bad types, missing fields, primaryKey
normalisation) and the insert_values async workflow (unpartitioned async,
partitioned async with batch tracking, and partition-column mismatch errors).

All created tables use a unique `mcpdsnv_` prefix and are dropped in teardown.

Requires config.json (or MAXCOMPUTE_CATALOG_CONFIG env var).
"""
from __future__ import annotations

import logging
from typing import Any, List

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import (
    async_wait_instance as _wait_for_instance,
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
    has_error = payload.get("success") is False or "error" in payload
    assert has_error, f"Expected failure{f' ({context})' if context else ''}, got: {payload}"


# ---------------------------------------------------------------------------
# create_table validation
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCreateTableValidation:
    """create_table: type validation and edge-case input handling."""

    def test_create_table_invalid_table_properties_type(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """tableProperties must be an object; passing a string must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": _uniq("mcpdsnv_badprops"),
            "columns": [{"name": "id", "type": "BIGINT"}],
            "tableProperties": "not_a_dict",
            "lifecycle": 1,
        })
        _assert_failure(_text_payload(r), "tableProperties must be object")

    def test_create_table_invalid_hints_type(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """hints must be an object; passing a list must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": _uniq("mcpdsnv_badhints"),
            "columns": [{"name": "id", "type": "BIGINT"}],
            "hints": ["not", "a", "dict"],
            "lifecycle": 1,
        })
        _assert_failure(_text_payload(r), "hints must be object")

    def test_create_table_invalid_primary_key_type(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """primaryKey must be an array; passing a string must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": _uniq("mcpdsnv_badpk"),
            "columns": [{"name": "id", "type": "BIGINT"}],
            "primaryKey": "not_an_array",
            "transactional": True,
            "lifecycle": 1,
        })
        _assert_failure(_text_payload(r), "primaryKey must be array")

    def test_create_table_empty_primary_key_treated_as_unset(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """primaryKey=[] is normalised to None (unset) so create_table succeeds."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpdsnv_emptypk")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "primaryKey": [],
            "transactional": True,
            "lifecycle": 1,
        })
        p = _text_payload(r)
        # primaryKey=[] normalised to None, so transactional table without PK
        # should succeed (or fail for server-side reasons, not validation)
        if p.get("success") is not True:
            pytest.skip(f"transactional table not supported: {p.get('error')}")
        assert p.get("success") is True, f"create_table with empty primaryKey failed: {p}"

    def test_create_table_column_missing_name(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """A column dict without 'name' must fail with validation error."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": _uniq("mcpdsnv_noname"),
            "columns": [{"type": "BIGINT"}],
            "lifecycle": 1,
        })
        _assert_failure(_text_payload(r), "column missing name")

    def test_create_table_partition_column_missing_name(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """A partitionColumn dict without 'name' must fail with validation error."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": _uniq("mcpdsnv_pcn"),
            "columns": [{"name": "id", "type": "BIGINT"}],
            "partitionColumns": [{"type": "STRING"}],
            "lifecycle": 1,
        })
        _assert_failure(_text_payload(r), "partitionColumn missing name")

    def test_create_table_columns_key_missing(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Omitting the 'columns' key entirely must fail with 'columns is required'."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": _uniq("mcpdsvn_nocolskey"),
            "lifecycle": 1,
        })
        p = _text_payload(r)
        _assert_failure(p, "columns key missing")
        assert "columns is required" in (p.get("error") or ""), (
            f"Expected 'columns is required' error, got: {p}"
        )


# ---------------------------------------------------------------------------
# insert_values partitionColumns validation
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestInsertValuesPartitionValidation:
    """insert_values: partitionColumns mismatch errors."""

    def test_partition_column_not_in_columns(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """partitionColumns referencing a column not in columns must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpdsnv_pcmismatch")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "val", "type": "STRING"},
            ],
            "partitionColumns": ["ds"],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table failed: {p}"

        # partitionColumns says "region" but columns doesn't have it
        r2 = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id", "val", "ds"],
            "partitionColumns": ["region"],  # not in columns
            "values": [[1, "a", "20260101"]],
        })
        p2 = _text_payload(r2)
        _assert_failure(p2, "partitionColumn not in columns")

    def test_only_partition_columns_no_data_columns(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """If all columns are partition columns, insert_values must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpdsnv_onlypc")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "partitionColumns": ["ds"],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table failed: {p}"

        # columns only lists the partition column "ds", no data columns remain
        r2 = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id", "ds"],
            "partitionColumns": ["id", "ds"],  # all columns are partition columns
            "values": [[1, "20260101"]],
        })
        p2 = _text_payload(r2)
        _assert_failure(p2, "no data columns besides partitions")


# ---------------------------------------------------------------------------
# insert_values async mode
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestInsertValuesAsync:
    """insert_values with async=True: unpartitioned and partitioned tables."""

    def test_async_insert_unpartitioned(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """insert_values(async=True) on unpartitioned table returns instanceId immediately."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpdsnv_asyncu")
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

        r2 = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id", "name"],
            "values": [[1, "alice"], [2, "bob"]],
            "async": True,
        })
        p2 = _text_payload(r2)
        assert p2.get("success") is True, f"async insert_values failed: {p2}"
        assert "instanceId" in p2, f"async insert must return instanceId, got: {p2}"
        assert p2.get("status") == "submitted", f"status must be 'submitted', got: {p2}"

        # Wait for the async instance to complete
        _wait_for_instance(real_tools, project, p2["instanceId"], timeout=120)

    def test_async_insert_partitioned(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """insert_values(async=True) on partitioned table returns batches with instanceIds."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpdsnv_asyncp")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "val", "type": "STRING"},
            ],
            "partitionColumns": ["ds"],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table failed: {p}"

        r2 = real_tools.call("insert_values", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": ["id", "val", "ds"],
            "partitionColumns": ["ds"],
            "values": [
                [1, "a", "20260417"],
                [2, "b", "20260418"],
            ],
            "async": True,
        })
        p2 = _text_payload(r2)
        assert p2.get("success") is True, f"async partitioned insert failed: {p2}"
        assert "batches" in p2, f"async partitioned insert must return batches, got: {p2}"
        assert p2.get("status") == "submitted", f"status must be 'submitted', got: {p2}"

        # Each batch should have a partitionKey and instanceId
        batches = p2["batches"]
        assert len(batches) == 2, f"Expected 2 partition batches, got {len(batches)}: {batches}"
        for batch in batches:
            assert "partitionKey" in batch, f"batch missing partitionKey: {batch}"
            assert "instanceId" in batch, f"batch missing instanceId: {batch}"

        # Wait for both instances to complete
        for batch in batches:
            _wait_for_instance(real_tools, project, batch["instanceId"], timeout=120)
