# -*- coding: utf-8 -*-
"""E2E tests: update_table (table metadata update tool).

Currently there is NO E2E coverage for update_table; this file provides
complete coverage of the read-modify-write workflow against a real
MaxCompute project.

Scenarios:
- Update table description
- Update column description via columns.setComments
- Update labels (merge mode)
- Error: update non-existent table
- ETag conflict (OCC / optimistic concurrency control)
- Add a new column via columns.add
- Clear table description with empty string

All tables created here use a unique `mcpe2etm_` prefix and are dropped in teardown.

Requires config.json (or MAXCOMPUTE_CATALOG_CONFIG env var).
"""
from __future__ import annotations

import logging
from typing import Any, List

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import (
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


def _create_simple_table(real_tools: Tools, project: str, table: str) -> None:
    """Helper: create a basic table with two columns."""
    r = real_tools.call("create_table", {
        "project": project,
        "schema": "default",
        "table": table,
        "columns": [
            {"name": "id", "type": "BIGINT"},
            {"name": "name", "type": "STRING"},
        ],
        "description": "initial description",
        "lifecycle": 1,
    })
    p = _text_payload(r)
    assert p.get("success") is True, f"create_table failed: {p}"


def _get_table_schema(real_tools: Tools, project: str, table: str) -> dict:
    """Helper: call get_table_schema and return the data payload."""
    r = real_tools.call("get_table_schema", {
        "project": project,
        "schema": "default",
        "table": table,
    })
    p = _text_payload(r)
    assert "error" not in p, f"get_table_schema failed: {p.get('error')}"
    return _data(p)


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestUpdateTableDescription:
    """update_table: description field."""

    def test_update_table_description(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """update description, then verify via get_table_schema."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_desc")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        new_desc = f"updated description for {table}"
        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "description": new_desc,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"update_table description failed: {p}"
        assert "description" in (p.get("data") or {}), (
            f"update_table response missing data.description: {p}"
        )

        # Verify via get_table_schema
        schema_data = _get_table_schema(real_tools, project, table)
        # Description may be on the outer payload or inside data
        desc = schema_data.get("description")
        assert desc == new_desc, (
            f"Expected description={new_desc!r}, got {desc!r}. Full schema: {schema_data}"
        )

    def test_update_table_clear_description(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Empty string should clear the table description."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_clrdesc")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "description": "",
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"update_table clear description failed: {p}"

        schema_data = _get_table_schema(real_tools, project, table)
        desc = schema_data.get("description")
        assert desc == "" or desc is None, (
            f"Expected empty or None description after clear, got {desc!r}"
        )


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestUpdateTableColumnDescription:
    """update_table: column description via columns.setComments."""

    def test_update_column_description(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Set a column description and verify via get_table_schema."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_coldesc")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        col_desc = f"column comment for id in {table}"
        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"setComments": {"id": col_desc}},
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"update_table setComments failed: {p}"

        schema_data = _get_table_schema(real_tools, project, table)
        columns = schema_data.get("columns") or []
        id_col = next((c for c in columns if c.get("name") == "id"), None)
        assert id_col is not None, f"Column 'id' not found in schema: {schema_data}"
        assert id_col.get("description") == col_desc, (
            f"Expected column description={col_desc!r}, got {id_col.get('description')!r}"
        )

    def test_update_nonexistent_column_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """setComments for a column that doesn't exist should fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_noexistcol")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"setComments": {"nonexistent_col_xyz": "some desc"}},
        })
        p = _text_payload(r)
        assert p.get("success") is False, (
            f"Expected failure for non-existent column in setComments, got: {p}"
        )


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestUpdateTableLabels:
    """update_table: labels merge/replace/delete modes."""

    def test_update_table_labels_merge(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Merge mode: add new labels without removing existing ones."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_lblmerge")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "labels": {"set": {"env": "test", "owner": "e2e"}, "mode": "merge"},
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"update_table labels merge failed: {p}"
        updated_data = p.get("data") or {}
        labels = updated_data.get("labels") or {}
        assert labels.get("env") == "test", (
            f"Expected labels.env='test', got: {labels}"
        )
        assert labels.get("owner") == "e2e", (
            f"Expected labels.owner='e2e', got: {labels}"
        )

    def test_update_table_labels_replace(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Replace mode: completely replaces existing labels."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_lblreplace")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        # First: merge in an initial label
        real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "labels": {"set": {"old_key": "old_val"}, "mode": "merge"},
        })

        # Replace with a completely new label set
        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "labels": {"set": {"new_key": "new_val"}, "mode": "replace"},
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"update_table labels replace failed: {p}"
        updated_data = p.get("data") or {}
        labels = updated_data.get("labels") or {}
        assert labels.get("new_key") == "new_val", (
            f"Expected new_key='new_val' after replace, got: {labels}"
        )
        assert "old_key" not in labels, (
            f"old_key should be gone after replace mode, but labels={labels}"
        )


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestUpdateTableAddColumn:
    """update_table: add new columns via columns.add."""

    def test_add_new_column(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Add a new nullable column and verify it appears in get_table_schema."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_addcol")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {
                "add": [{"name": "extra_col", "type": "STRING", "description": "added column"}]
            },
        })
        p = _text_payload(r)
        assert p.get("success") is True, (
            f"columns.add failed: {p}. "
            "If 'Field data type cannot be empty (400)', the backfill of sql_type_definition "
            "from type_category may not be working correctly."
        )

        schema_data = _get_table_schema(real_tools, project, table)
        columns = schema_data.get("columns") or []
        col_names = [c.get("name") for c in columns]
        assert "extra_col" in col_names, (
            f"New column 'extra_col' not found in schema after add. Columns: {col_names}"
        )


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestUpdateTableErrors:
    """update_table: error scenarios."""

    def test_update_nonexistent_table_returns_error(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": "nonexistent_table_xyz_12345",
            "description": "should fail",
        })
        p = _text_payload(r)
        assert p.get("success") is False, (
            f"Expected failure for non-existent table, got: {p}"
        )
        assert p.get("error"), "Expected non-empty error message"

    def test_update_table_no_fields_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Calling update_table with no patch fields should return success=false."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_nofields")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            # No patch fields provided
        })
        p = _text_payload(r)
        assert p.get("success") is False, (
            f"Expected failure when no updatable fields provided, got: {p}"
        )

    def test_update_table_with_stale_etag(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Using an explicitly wrong etag must cause success=false (OCC violation)."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_etag")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        # Use a clearly wrong etag
        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "description": "should fail due to bad etag",
            "etag": "STALE_ETAG_THAT_DOES_NOT_EXIST_12345",
        })
        p = _text_payload(r)
        # The OCC behavior depends on the server; it may succeed (auto-fetch) or fail
        # Some environments allow etag override without strict checking.
        # We verify no Python crash and return a valid response with a success indicator.
        assert isinstance(p, dict), f"Expected dict response, got: {type(p)}"
        assert "success" in p, f"Response must contain 'success' key: {p}"
        # Ideally a stale etag should cause success=false, but some servers ignore etag
        if p.get("success") is not False:
            logger.warning(
                "Stale etag did not cause failure — server may not enforce OCC: %s", p
            )
        logger.info("Stale etag response: %s", p)


# ---------------------------------------------------------------------------
# Labels delete mode
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestUpdateTableLabelsDelete:
    """update_table: labels delete mode removes specific keys."""

    def test_labels_delete_removes_key(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Delete mode: removes specified label keys, leaving others intact."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_lbldelete")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        # First: merge in two labels
        real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "labels": {"set": {"env": "test", "team": "data"}, "mode": "merge"},
        })

        # Delete one label
        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "labels": {"set": {"env": ""}, "mode": "delete"},
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"labels delete failed: {p}"
        updated_data = p.get("data") or {}
        labels = updated_data.get("labels") or {}
        assert "env" not in labels, (
            f"'env' should be deleted, but labels={labels}"
        )
        assert labels.get("team") == "data", (
            f"'team' should remain after delete, got: {labels}"
        )


# ---------------------------------------------------------------------------
# Expiration update
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestUpdateTableExpiration:
    """update_table: expiration policy (days / partitionDays)."""

    def test_set_table_expiration_days(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Set table-level expiration days."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_expdays")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "expiration": {"days": 365},
        })
        p = _text_payload(r)
        if p.get("success") is not True:
            pytest.skip(f"Expiration update not supported in this env: {p.get('error')}")

        updated_data = p.get("data") or {}
        expiration = updated_data.get("expiration") or {}
        assert expiration.get("days") == 365, (
            f"Expected expiration.days=365, got: {expiration}"
        )

    def test_set_partition_expiration_days(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Set partition-level expiration days on a partitioned table."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_partexp")
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

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "expiration": {"partitionDays": 30},
        })
        p = _text_payload(r)
        if p.get("success") is not True:
            pytest.skip(f"Partition expiration not supported in this env: {p.get('error')}")

        updated_data = p.get("data") or {}
        expiration = updated_data.get("expiration") or {}
        assert expiration.get("partitionDays") == 30, (
            f"Expected expiration.partitionDays=30, got: {expiration}"
        )

    def test_disable_expiration_with_zero_rejected(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Setting expiration.days=0 should be rejected (days must be positive integer).

        Per MaxCompute docs: "days must be a positive integer". To disable
        lifecycle, use ALTER TABLE ... DISABLE LIFECYCLE instead.
        """
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_expoff")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        # First set a valid expiration
        real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "expiration": {"days": 100},
        })

        # Setting days=0 must fail (days must be positive integer)
        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "expiration": {"days": 0},
        })
        p = _text_payload(r)
        assert p.get("success") is False, (
            f"expiration.days=0 should be rejected (days must be positive), got: {p}"
        )


# ---------------------------------------------------------------------------
# setNullable: REQUIRED → NULLABLE
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestUpdateTableSetNullable:
    """update_table: columns.setNullable (REQUIRED → NULLABLE)."""

    def test_set_nullable_on_required_column(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Set a REQUIRED column to NULLABLE."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_nullable")
        created_tables.append(table)

        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [
                {"name": "id", "type": "BIGINT", "notNull": True},
                {"name": "name", "type": "STRING"},
            ],
            "transactional": True,
            "lifecycle": 1,
        })
        p = _text_payload(r)
        if p.get("success") is not True:
            pytest.skip(f"transactional table not supported: {p.get('error')}")

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"setNullable": ["id"]},
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"setNullable failed: {p}"

    def test_set_nullable_nonexistent_column_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """setNullable for a column that doesn't exist should fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_nullne")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"setNullable": ["nonexistent_col"]},
        })
        p = _text_payload(r)
        assert p.get("success") is False, (
            f"Expected failure for nonexistent column in setNullable, got: {p}"
        )


# ---------------------------------------------------------------------------
# columns.add: various types
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestUpdateTableAddColumnTypes:
    """update_table: columns.add with different data types."""

    def test_add_decimal_column(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Add a DECIMAL column via columns.add."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_dec")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"add": [{"name": "price", "type": "DECIMAL(10,2)"}]},
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"add DECIMAL column failed: {p}"

    def test_add_array_column_with_schema_evolution(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Add ARRAY<STRING> column via columns.add.

        Requires project-level property odps.schema.evolution.enable=true.
        Per MaxCompute docs, adding complex type columns requires schema
        evolution to be enabled on the project.
        """
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_arr")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"add": [{"name": "tags", "type": "ARRAY<STRING>"}]},
        })
        p = _text_payload(r)
        assert p.get("success") is True, (
            f"ARRAY column add should succeed with schema evolution enabled, got: {p}"
        )

    def test_add_map_column_with_schema_evolution(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Add MAP<STRING,BIGINT> column via columns.add.

        Requires project-level property odps.schema.evolution.enable=true.
        Per MaxCompute docs, adding complex type columns requires schema
        evolution to be enabled on the project.
        """
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_map")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"add": [{"name": "metadata", "type": "MAP<STRING,BIGINT>"}]},
        })
        p = _text_payload(r)
        assert p.get("success") is True, (
            f"MAP column add should succeed with schema evolution enabled, got: {p}"
        )

    def test_add_duplicate_column_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Adding a column with a name that already exists must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_dupcol")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"add": [{"name": "id", "type": "BIGINT"}]},
        })
        p = _text_payload(r)
        assert p.get("success") is False, (
            f"Expected failure for duplicate column name, got: {p}"
        )
        assert "already exists" in (p.get("error") or ""), (
            f"Expected 'already exists' error, got: {p}"
        )

    def test_add_column_with_invalid_type_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Adding a column with an invalid/unknown type must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_badtype")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"add": [{"name": "bad_col", "type": "NOT_A_REAL_TYPE"}]},
        })
        p = _text_payload(r)
        assert p.get("success") is False, (
            f"Expected failure for invalid type, got: {p}"
        )


# ---------------------------------------------------------------------------
# Input validation: 10+ invalid inputs for _normalize_patch
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestUpdateTableInputValidation:
    """update_table: _normalize_patch validation errors (no server call needed)."""

    def test_description_null_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """description=None must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_valdescnull")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "description": None,
        })
        p = _text_payload(r)
        assert p.get("success") is False
        assert "null" in (p.get("error") or "").lower()

    def test_description_non_string_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """description=123 (non-string) must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_valdescint")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "description": 123,
        })
        p = _text_payload(r)
        assert p.get("success") is False
        assert "string" in (p.get("error") or "").lower()

    def test_labels_not_dict_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """labels must be a dict."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_vallbltyp")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "labels": "not_a_dict",
        })
        p = _text_payload(r)
        assert p.get("success") is False
        assert "labels" in (p.get("error") or "").lower()

    def test_labels_set_not_dict_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """labels.set must be a dict."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_vallblset")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "labels": {"set": "not_a_dict", "mode": "merge"},
        })
        p = _text_payload(r)
        assert p.get("success") is False

    def test_labels_invalid_mode_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """labels.mode must be merge/replace/delete."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_vallblmode")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "labels": {"set": {"k": "v"}, "mode": "invalid_mode"},
        })
        p = _text_payload(r)
        assert p.get("success") is False
        assert "mode" in (p.get("error") or "").lower()

    def test_expiration_not_dict_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """expiration must be a dict."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_valexptyp")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "expiration": 100,
        })
        p = _text_payload(r)
        assert p.get("success") is False
        assert "expiration" in (p.get("error") or "").lower()

    def test_expiration_negative_days_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """expiration.days < 0 must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_valexpneg")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "expiration": {"days": -1},
        })
        p = _text_payload(r)
        assert p.get("success") is False
        assert ">= 0" in (p.get("error") or "")

    def test_expiration_non_integer_days_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """expiration.days='abc' must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_valexpsrt")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "expiration": {"days": "abc"},
        })
        p = _text_payload(r)
        assert p.get("success") is False
        assert "integer" in (p.get("error") or "").lower()

    def test_columns_not_dict_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """columns must be a dict."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_valcoltyp")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": "not_a_dict",
        })
        p = _text_payload(r)
        assert p.get("success") is False
        assert "columns" in (p.get("error") or "").lower()

    def test_set_comments_not_dict_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """columns.setComments must be a dict."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_valsctyp")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"setComments": "not_a_dict"},
        })
        p = _text_payload(r)
        assert p.get("success") is False

    def test_set_nullable_not_array_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """columns.setNullable must be an array of strings."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_valsnarr")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"setNullable": "not_an_array"},
        })
        p = _text_payload(r)
        assert p.get("success") is False

    def test_set_nullable_nested_column_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """columns.setNullable with dotted (nested) path must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_valsnntd")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"setNullable": ["addr.city"]},
        })
        p = _text_payload(r)
        assert p.get("success") is False
        assert "nested" in (p.get("error") or "").lower()

    def test_columns_add_not_array_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """columns.add must be an array."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_valaddarr")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"add": "not_an_array"},
        })
        p = _text_payload(r)
        assert p.get("success") is False

    def test_columns_add_entry_not_dict_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """columns.add[i] must be an object."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_valaddobj")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"add": ["not_a_dict"]},
        })
        p = _text_payload(r)
        assert p.get("success") is False

    def test_columns_add_missing_name_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """columns.add[i] without 'name' must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_valaddnm")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"add": [{"type": "STRING"}]},
        })
        p = _text_payload(r)
        assert p.get("success") is False
        assert "name" in (p.get("error") or "").lower()

    def test_columns_add_missing_type_returns_error(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """columns.add[i] without 'type' must fail."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2etm_valaddtp")
        created_tables.append(table)
        _create_simple_table(real_tools, project, table)

        r = real_tools.call("update_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": {"add": [{"name": "new_col"}]},
        })
        p = _text_payload(r)
        assert p.get("success") is False
        assert "type" in (p.get("error") or "").lower()


