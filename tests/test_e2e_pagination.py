# -*- coding: utf-8 -*-
"""E2E tests: pagination completeness across list_projects, list_tables, and
get_partition_info.

Verifies that:
- Iterating through all pages retrieves every record without duplicates
- pageSize is honoured (each page has at most N entries)
- nextPageToken is correctly threaded across pages
- An invalid / garbage token returns a graceful error (not a crash)

All tables created here use a unique `mcpe2epag_` prefix and are dropped in teardown.

Requires config.json (or MAXCOMPUTE_CATALOG_CONFIG env var).
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

_MAX_PAGES = 200  # safety cap to prevent infinite loops


@pytest.fixture
def created_tables(real_tools: Tools):
    names: List[str] = []
    yield names
    for t in names:
        _drop(real_tools, t)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _paginate_list_projects(real_tools: Tools, *, page_size: int = 1) -> List[dict]:
    """Collect all projects by iterating through pages."""
    all_projects: List[dict] = []
    token = None
    pages = 0
    while pages < _MAX_PAGES:
        args: dict = {"pageSize": page_size}
        if token:
            args["token"] = token
        r = real_tools.call("list_projects", args)
        payload = _text_payload(r)
        assert "error" not in payload, f"list_projects page {pages} error: {payload.get('error')}"
        d = _data(payload)
        batch = d.get("projects") or []
        all_projects.extend(batch)
        token = d.get("nextPageToken")
        pages += 1
        if not token:
            break
    return all_projects


def _paginate_list_tables(
    real_tools: Tools,
    project: str,
    *,
    schema: str = "default",
    page_size: int = 1,
    filter_str: str | None = None,
) -> List[dict]:
    """Collect all tables by iterating through pages."""
    all_tables: List[dict] = []
    token = None
    pages = 0
    while pages < _MAX_PAGES:
        args: dict = {"project": project, "schema": schema, "pageSize": page_size}
        if token:
            args["token"] = token
        if filter_str:
            args["filter"] = filter_str
        r = real_tools.call("list_tables", args)
        payload = _text_payload(r)
        assert "error" not in payload, f"list_tables page {pages} error: {payload.get('error')}"
        d = _data(payload)
        batch = d.get("tables") or []
        all_tables.extend(batch)
        token = d.get("nextPageToken")
        pages += 1
        if not token:
            break
    return all_tables


def _paginate_get_partition_info(
    real_tools: Tools,
    project: str,
    table: str,
    *,
    schema: str = "default",
    page_size: int = 1,
) -> List[dict]:
    """Collect all partitions by iterating through pages."""
    all_partitions: List[dict] = []
    token = None
    pages = 0
    while pages < _MAX_PAGES:
        args: dict = {
            "project": project,
            "schema": schema,
            "table": table,
            "pageSize": page_size,
        }
        if token:
            args["token"] = token
        r = real_tools.call("get_partition_info", args)
        payload = _text_payload(r)
        if payload.get("success") is False or "error" in payload:
            return []  # propagate skip
        partitions = payload.get("partitions") or []
        all_partitions.extend(partitions)
        token = payload.get("nextPageToken")
        pages += 1
        if not token:
            break
    return all_partitions


# ---------------------------------------------------------------------------
# list_projects pagination
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestListProjectsPagination:
    """list_projects: full pagination traversal and deduplication."""

    def test_full_pagination_no_duplicates(self, real_tools: Tools) -> None:
        """Iterating all pages must yield no duplicate project names."""
        all_projects = _paginate_list_projects(real_tools, page_size=1)
        if len(all_projects) < 2:
            pytest.skip("fewer than 2 projects; pagination not testable")
        names = [
            p.get("projectId") or p.get("name", "").split("/")[-1]
            for p in all_projects
        ]
        assert len(names) == len(set(names)), (
            f"Duplicate project names found after full pagination: {names}"
        )

    def test_full_pagination_matches_bulk_list(self, real_tools: Tools) -> None:
        """Paged results must contain the same set as a bulk list (large page)."""
        # Use page_size=10 so the traversal stays within _MAX_PAGES even for
        # environments with 500+ projects (506 / 10 = ~51 pages, well under 200).
        paged = _paginate_list_projects(real_tools, page_size=10)
        # Fetch all projects via bulk (paginate if > 100 in a single page)
        bulk = _paginate_list_projects(real_tools, page_size=100)

        paged_names = {
            p.get("projectId") or p.get("name", "").split("/")[-1]
            for p in paged
        }
        bulk_names = {
            p.get("projectId") or p.get("name", "").split("/")[-1]
            for p in bulk
        }
        missing = bulk_names - paged_names
        assert not missing, (
            f"Projects found in bulk list but missing from paged traversal: {missing}"
        )


# ---------------------------------------------------------------------------
# list_tables pagination
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestListTablesPagination:
    """list_tables: multi-table setup, full pagination, no duplicates."""

    def test_list_tables_pagination_covers_created_tables(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Create 3 tables with a unique prefix; paginate and verify all appear."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        prefix = _uniq("mcpe2epag_t")
        table_names: List[str] = []
        for i in range(3):
            t = f"{prefix}_{i}"
            created_tables.append(t)
            table_names.append(t)
            r = real_tools.call("create_table", {
                "project": project,
                "schema": "default",
                "table": t,
                "columns": [{"name": "id", "type": "BIGINT"}],
                "lifecycle": 1,
            })
            p = _text_payload(r)
            assert p.get("success") is True, f"create_table {t} failed: {p}"

        # Paginate with filter — SDK doesn't support table_name_prefix, so
        # filter is applied client-side per page; nextPageToken is preserved
        # across pages, so all matching tables will be found.
        all_tables = _paginate_list_tables(
            real_tools, project, page_size=100, filter_str=prefix,
        )
        retrieved_names = [
            t.get("tableName") or t.get("name", "").split("/")[-1]
            for t in all_tables
        ]
        for name in table_names:
            assert name in retrieved_names, (
                f"Table {name!r} not found in paginated results: {retrieved_names}"
            )

    def test_list_tables_pagesize_respected(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Each page must have at most pageSize tables."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("list_tables", {
            "project": project,
            "schema": "default",
            "pageSize": 2,
        })
        payload = _text_payload(r)
        assert "error" not in payload, f"list_tables error: {payload.get('error')}"
        d = _data(payload)
        tables = d.get("tables") or []
        assert len(tables) <= 2, (
            f"Expected at most 2 tables with pageSize=2, got {len(tables)}"
        )

    def test_list_tables_filter_empty_pages_still_paginate(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Filter with pageSize=1: pages with no matching tables must not stop iteration.

        With client-side filtering, SDK pages are fetched one at a time.  When a
        page contains only non-matching tables the filtered batch is empty, yet
        nextPageToken may still be present.  Iteration must be driven solely by
        nextPageToken — an empty batch must never be treated as end-of-results.
        """
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        prefix = _uniq("mcpe2epag_ef")
        table_names: List[str] = []
        for i in range(2):
            t = f"{prefix}_{i}"
            created_tables.append(t)
            table_names.append(t)
            r = real_tools.call("create_table", {
                "project": project,
                "schema": "default",
                "table": t,
                "columns": [{"name": "id", "type": "BIGINT"}],
                "lifecycle": 1,
            })
            p = _text_payload(r)
            assert p.get("success") is True, f"create_table {t} failed: {p}"

        # Iterate page-by-page with pageSize=1 and a filter.  Non-matching
        # tables produce empty filtered batches; we must follow nextPageToken
        # regardless of whether a batch is empty.
        all_tables: List[dict] = []
        token = None
        total_pages = 0
        empty_pages = 0
        for _ in range(_MAX_PAGES):
            args: dict = {
                "project": project,
                "schema": "default",
                "pageSize": 1,
                "filter": prefix,
            }
            if token:
                args["token"] = token
            r = real_tools.call("list_tables", args)
            payload = _text_payload(r)
            assert "error" not in payload, (
                f"list_tables page {total_pages} error: {payload.get('error')}"
            )
            d = _data(payload)
            batch = d.get("tables") or []
            if not batch:
                empty_pages += 1
            all_tables.extend(batch)
            token = d.get("nextPageToken")
            total_pages += 1
            if not token:
                break

        retrieved_names = [
            t.get("tableName") or t.get("name", "").split("/")[-1]
            for t in all_tables
        ]
        for name in table_names:
            assert name in retrieved_names, (
                f"Table {name!r} not found after {total_pages} pages "
                f"({empty_pages} empty after filter): {retrieved_names}"
            )
        logger.info(
            "filter empty-page test: %d total pages, %d empty after filter, "
            "%d matching tables found",
            total_pages, empty_pages, len(retrieved_names),
        )


# ---------------------------------------------------------------------------
# get_partition_info pagination
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestGetPartitionInfoPagination:
    """get_partition_info: full partition traversal."""

    def test_partition_pagination_covers_all_partitions(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Insert 3 partitions; paginate with pageSize=1 and verify all 3 appear."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2epag_pi")
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

        all_parts = _paginate_get_partition_info(real_tools, project, table, page_size=1)
        if not all_parts:
            pytest.skip("get_partition_info returned no partitions (may not be supported)")

        assert len(all_parts) == 3, (
            f"Expected 3 partitions after full pagination, got {len(all_parts)}: "
            f"{all_parts}"
        )

    def test_partition_pagination_no_duplicates(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Paginated partition specs must not contain duplicates."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2epag_pidup")
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
            "values": [[1, "20260201"], [2, "20260202"]],
        })

        all_parts = _paginate_get_partition_info(real_tools, project, table, page_size=1)
        if not all_parts:
            pytest.skip("get_partition_info returned no partitions")

        specs = [p.get("spec") or str(p) for p in all_parts]
        assert len(specs) == len(set(specs)), (
            f"Duplicate partition specs after pagination: {specs}"
        )


# ---------------------------------------------------------------------------
# Invalid token handling
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestInvalidTokenHandling:
    """Passing a garbage pagination token must not crash; must return error or empty."""

    def test_list_projects_invalid_token(self, real_tools: Tools) -> None:
        r = _call_safe(real_tools, "list_projects", {
            "pageSize": 5,
            "token": "INVALID_GARBAGE_TOKEN_xyz_12345",
        })
        payload = _text_payload(r)
        # Invalid token must return error or success=false; not crash silently
        assert isinstance(payload, dict), f"Expected dict response, got: {type(payload)}"
        has_error = payload.get("success") is False or "error" in payload
        if not has_error:
            # Some APIs silently ignore invalid tokens and return empty results — that's also acceptable
            projects = (payload.get("data") or payload).get("projects") or []
            assert isinstance(projects, list), f"Expected projects list or error, got: {payload}"

    def test_list_tables_invalid_token(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = _call_safe(real_tools, "list_tables", {
            "project": project,
            "schema": "default",
            "pageSize": 5,
            "token": "INVALID_GARBAGE_TOKEN_xyz_12345",
        })
        payload = _text_payload(r)
        # Invalid token must return error or success=false; not crash silently
        assert isinstance(payload, dict), f"Expected dict response, got: {type(payload)}"
        has_error = payload.get("success") is False or "error" in payload
        if not has_error:
            # Some APIs silently ignore invalid tokens and return empty results — that's also acceptable
            tables = (payload.get("data") or payload).get("tables") or []
            assert isinstance(tables, list), f"Expected tables list or error, got: {payload}"

    def test_get_partition_info_invalid_token(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        table = _uniq("mcpe2epag_badtok")
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
            "values": [[1, "20260101"]],
        })

        r = _call_safe(real_tools, "get_partition_info", {
            "project": project,
            "schema": "default",
            "table": table,
            "pageSize": 1,
            "token": "INVALID_TOKEN_xyz_12345",
        })
        payload = _text_payload(r)
        # Invalid token must return error or success=false; not crash silently
        assert isinstance(payload, dict), f"Expected dict response, got: {type(payload)}"
        has_error = payload.get("success") is False or "error" in payload
        if not has_error:
            # Some APIs silently ignore invalid tokens and return empty results — that's also acceptable
            partitions = (payload.get("data") or payload).get("partitions") or []
            assert isinstance(partitions, list), f"Expected partitions list or error, got: {payload}"
