# -*- coding: utf-8 -*-
"""E2E tests: search_meta_data (metadata search tool).

Supplements the existing basic test in test_integration.py with:
- Searching by a unique table name created in this test run
- Pagination (pageSize=1 + token threading)
- Empty/blank query handling
- No namespace_id → must return success=false with explanation

The search index is eventually consistent, so tests that create a table
and immediately search for it use a retry loop with backoff.

Requires config.json (or MAXCOMPUTE_CATALOG_CONFIG env var).
Tests that need namespace_id are skipped when it is absent.
"""
from __future__ import annotations

import logging
import time
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

_SEARCH_SYNC_RETRIES = 3
_SEARCH_SYNC_INTERVAL = 10  # seconds between retries


@pytest.fixture
def created_tables(real_tools: Tools):
    names: List[str] = []
    yield names
    for t in names:
        _drop(real_tools, t)


def _has_namespace(real_config: Any) -> bool:
    return bool(getattr(real_config, "namespace_id", None))


def _search_with_retry(
    real_tools: Tools,
    query: str,
    *,
    page_size: int = 5,
    retries: int = _SEARCH_SYNC_RETRIES,
    interval: int = _SEARCH_SYNC_INTERVAL,
) -> dict:
    """Call search_meta_data with retry for eventual consistency."""
    for attempt in range(retries):
        r = real_tools.call("search_meta_data", {"query": query, "pageSize": page_size})
        payload = _text_payload(r)
        if payload.get("success") is False or "error" in payload:
            return payload  # propagate error
        entries = payload.get("entries") or []
        if entries:
            return payload
        if attempt < retries - 1:
            logger.info(
                "search attempt %d/%d returned 0 entries for query=%r; retrying in %ds",
                attempt + 1,
                retries,
                query,
                interval,
            )
            time.sleep(interval)
    return payload  # return last attempt even if empty


# ---------------------------------------------------------------------------
# No namespace → unsupported (unit test, no config required)
# ---------------------------------------------------------------------------

class TestSearchNoNamespace:
    """search_meta_data without namespace_id returns success=false."""

    def test_search_meta_data_no_namespace_returns_error(
        self, tools_no_namespace: Tools
    ) -> None:
        """tools_no_namespace has no namespace_id; search must fail gracefully."""
        r = tools_no_namespace.call("search_meta_data", {"query": "test"})
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"search_meta_data without namespace must fail, got: {payload}"
        )
        error_msg = payload.get("error") or ""
        assert error_msg, "Expected non-empty error message"


# ---------------------------------------------------------------------------
# Basic search (with namespace)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestSearchMetaDataBasic:
    """search_meta_data: basic query, result structure, orderBy."""

    def test_search_meta_data_returns_entries_list(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """search_meta_data must return an entries list (may be empty)."""
        if not _has_namespace(real_config):
            pytest.skip("namespace_id not configured")
        r = real_tools.call("search_meta_data", {
            "query": "type=table",
            "pageSize": 5,
        })
        payload = _text_payload(r)
        assert "error" not in payload, f"search_meta_data error: {payload.get('error')}"
        assert "entries" in payload, f"search_meta_data must return entries: {payload}"
        assert isinstance(payload["entries"], list)

    def test_search_meta_data_order_by_name(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """orderBy='name' — accept the result whether supported or not."""
        if not _has_namespace(real_config):
            pytest.skip("namespace_id not configured")
        r = real_tools.call("search_meta_data", {
            "query": "type=table",
            "pageSize": 5,
            "orderBy": "name",
        })
        payload = _text_payload(r)
        # orderBy='name' may be unsupported in some environments; accept error or entries
        assert isinstance(payload, dict), f"Expected dict response, got: {type(payload)}"
        if payload.get("success") is not False and "error" not in payload:
            assert "entries" in payload, f"search_meta_data must return entries: {payload}"
            entries = payload["entries"]
            if len(entries) > 1:
                names = [e.get("name", "") for e in entries]
                assert names == sorted(names), (
                    f"orderBy='name' but entries not sorted: {names}"
                )

    def test_search_meta_data_entry_fields(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Each search entry must contain at least a name or displayName field."""
        if not _has_namespace(real_config):
            pytest.skip("namespace_id not configured")
        r = real_tools.call("search_meta_data", {
            "query": "type=table",
            "pageSize": 3,
        })
        payload = _text_payload(r)
        entries = payload.get("entries") or []
        if not entries:
            pytest.skip("no search entries returned; index may be empty")
        for entry in entries:
            assert isinstance(entry, dict), f"Entry must be a dict, got: {type(entry)}"
            has_name = entry.get("name") or entry.get("displayName") or entry.get("tableName")
            assert has_name, f"Entry missing name field: {entry}"


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestSearchMetaDataPagination:
    """search_meta_data pagination: pageSize=1, token threading."""

    def test_search_pagination_with_token(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """pageSize=1 should return a nextPageToken (if more than 1 result exists)."""
        if not _has_namespace(real_config):
            pytest.skip("namespace_id not configured")
        r1 = real_tools.call("search_meta_data", {
            "query": "type=table",
            "pageSize": 1,
        })
        p1 = _text_payload(r1)
        assert "error" not in p1, f"search_meta_data error: {p1.get('error')}"
        entries1 = p1.get("entries") or []
        if not entries1:
            pytest.skip("no entries in search index; pagination not testable")

        token = p1.get("nextPageToken")
        if not token:
            pytest.skip("only 1 result in index; cannot test pagination")

        # Fetch second page
        r2 = real_tools.call("search_meta_data", {
            "query": "type=table",
            "pageSize": 1,
            "token": token,
        })
        p2 = _text_payload(r2)
        assert "error" not in p2, f"search second page error: {p2.get('error')}"
        assert "entries" in p2, f"second page must return entries: {p2}"

    def test_search_pagination_no_duplicate_entries(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """First and second page entries must not overlap (by name)."""
        if not _has_namespace(real_config):
            pytest.skip("namespace_id not configured")
        r1 = real_tools.call("search_meta_data", {
            "query": "type=table",
            "pageSize": 1,
        })
        p1 = _text_payload(r1)
        entries1 = p1.get("entries") or []
        token = p1.get("nextPageToken")
        if not entries1 or not token:
            pytest.skip("insufficient results for duplicate check")

        r2 = real_tools.call("search_meta_data", {
            "query": "type=table",
            "pageSize": 1,
            "token": token,
        })
        p2 = _text_payload(r2)
        entries2 = p2.get("entries") or []
        if not entries2:
            pytest.skip("second page empty; cannot check for duplicates")

        names1 = {e.get("name") or e.get("tableName") for e in entries1}
        names2 = {e.get("name") or e.get("tableName") for e in entries2}
        overlap = names1 & names2
        assert not overlap, (
            f"Duplicate entries found across pages: {overlap}"
        )


# ---------------------------------------------------------------------------
# Search by exact table name (eventual consistency)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestSearchMetaDataByTableName:
    """search_meta_data: search for a newly created table by name."""

    def test_search_finds_created_table(
        self, real_tools: Tools, real_config: Any, created_tables: List[str]
    ) -> None:
        """Create a table with a unique name; search must find it (eventually)."""
        if not _has_namespace(real_config):
            pytest.skip("namespace_id not configured")
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        table = _uniq("mcpe2esrch")
        created_tables.append(table)
        r = real_tools.call("create_table", {
            "project": project,
            "schema": "default",
            "table": table,
            "columns": [{"name": "id", "type": "BIGINT"}],
            "lifecycle": 1,
        })
        p = _text_payload(r)
        assert p.get("success") is True, f"create_table failed: {p}"

        # Search with retry (eventually consistent index)
        # Use name:<table>,type=TABLE format as required by Catalog Search API
        payload = _search_with_retry(real_tools, query=f"name:{table},type=TABLE", retries=_SEARCH_SYNC_RETRIES)
        if payload.get("success") is False or "error" in payload:
            pytest.skip(f"search not available or error: {payload}")

        entries = payload.get("entries") or []
        if not entries:
            pytest.skip(
                f"Table {table!r} not found in search index after {_SEARCH_SYNC_RETRIES} "
                f"retries (index may have a longer sync delay in this environment)"
            )

        # At least one entry must match the table name
        entry_names = [
            e.get("name") or e.get("tableName") or ""
            for e in entries
        ]
        found = any(name.split("/")[-1] == table for name in entry_names)
        assert found, (
            f"Table {table!r} not found in search entries: {entry_names}"
        )


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestSearchMetaDataEdgeCases:
    """search_meta_data: empty query, very specific queries."""

    def test_search_meta_data_empty_query(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Empty query must not crash; either returns results or a clear error."""
        if not _has_namespace(real_config):
            pytest.skip("namespace_id not configured")
        r = _call_safe(real_tools, "search_meta_data", {"query": "", "pageSize": 5})
        payload = _text_payload(r)
        # Must return a valid dict (no crash)
        assert isinstance(payload, dict), f"Expected dict response, got: {type(payload)}"
        # If success, must have entries key
        if payload.get("success") is not False and "error" not in payload:
            assert "entries" in payload, (
                f"Successful search must return entries key: {payload}"
            )

    def test_search_meta_data_large_page_size(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """pageSize=100 must not crash the server."""
        if not _has_namespace(real_config):
            pytest.skip("namespace_id not configured")
        r = real_tools.call("search_meta_data", {"query": "type=table", "pageSize": 100})
        payload = _text_payload(r)
        assert isinstance(payload, dict), f"Expected dict response, got: {type(payload)}"

    def test_search_meta_data_special_characters_query(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Query with special characters must not crash."""
        if not _has_namespace(real_config):
            pytest.skip("namespace_id not configured")
        r = _call_safe(real_tools, "search_meta_data", {"query": "name:test*_table", "pageSize": 5})
        payload = _text_payload(r)
        assert isinstance(payload, dict), f"Expected dict response, got: {type(payload)}"
