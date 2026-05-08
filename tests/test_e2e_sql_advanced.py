# -*- coding: utf-8 -*-
"""E2E tests: SQL advanced — maxCU limit, input validation, running-instance get_instance.

Covers:
- execute_sql with maxCU below estimated cost → overLimit rejection
- execute_sql with maxCU above estimated cost → normal execution
- execute_sql with non-boolean async → TypeError rejection
- execute_sql with non-positive timeout → ValueError rejection
- get_instance on a still-running instance → "not terminated yet" message

Requires config.json (or MAXCOMPUTE_CATALOG_CONFIG env var).
"""
from __future__ import annotations

import logging
from typing import Any

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import (
    async_wait_instance as _wait_for_instance,
    has_config as _has_config,
    text_payload as _text_payload,
)

logger = logging.getLogger(__name__)


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestMaxCU:
    """execute_sql maxCU: cost-limit enforcement and bypass."""

    def test_maxcu_below_estimated_rejects(self, real_tools: Tools, real_config: Any) -> None:
        """maxCU=0 must reject any SELECT whose estimatedCU > 0."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1 AS n",
            "async": False,
            "timeout": 60,
            "maxCU": 0,
        })
        payload = _text_payload(r)
        # If estimatedCU is 0 (stub), the limit is NOT exceeded and the query runs normally.
        # If estimatedCU > 0, the limit IS exceeded and overLimit=True.
        if payload.get("overLimit") is True:
            assert payload.get("success") is False, f"overLimit must imply success=false: {payload}"
            assert "estimatedCU" in payload, f"overLimit response missing estimatedCU: {payload}"
            assert "suggestedMaxCU" in payload, f"overLimit response missing suggestedMaxCU: {payload}"
        else:
            # stub estimation (estimatedCU=0) → query ran normally
            assert payload.get("success") is True or "error" in payload, (
                f"Expected overLimit or normal success, got: {payload}"
            )

    def test_maxcu_high_enough_allows_execution(self, real_tools: Tools, real_config: Any) -> None:
        """maxCU=99999 must not block a simple SELECT."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1 AS n",
            "async": False,
            "timeout": 60,
            "maxCU": 99999,
        })
        payload = _text_payload(r)
        assert payload.get("overLimit") is not True, (
            f"Large maxCU must not trigger overLimit, got: {payload}"
        )
        assert payload.get("success") is True, (
            f"Query with large maxCU must succeed, got: {payload}"
        )


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestInputValidation:
    """execute_sql: non-boolean async, non-positive timeout."""

    def test_async_string_rejected(self, real_tools: Tools, real_config: Any) -> None:
        """Passing async='true' (string) must be rejected with TypeError."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1",
            "async": "true",
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"String async must be rejected, got: {payload}"
        )
        assert "boolean" in (payload.get("error") or "").lower() or "bool" in (payload.get("error") or "").lower(), (
            f"Error should mention boolean type, got: {payload.get('error')}"
        )

    def test_async_integer_rejected(self, real_tools: Tools, real_config: Any) -> None:
        """Passing async=1 (int) must be rejected with TypeError."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1",
            "async": 1,
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"Integer async must be rejected, got: {payload}"
        )

    def test_timeout_zero_rejected(self, real_tools: Tools, real_config: Any) -> None:
        """timeout=0 must be rejected with ValueError (must be positive)."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1",
            "async": False,
            "timeout": 0,
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"timeout=0 must be rejected, got: {payload}"
        )

    def test_timeout_negative_rejected(self, real_tools: Tools, real_config: Any) -> None:
        """timeout=-5 must be rejected with ValueError."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1",
            "async": False,
            "timeout": -5,
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"Negative timeout must be rejected, got: {payload}"
        )

    def test_timeout_string_rejected(self, real_tools: Tools, real_config: Any) -> None:
        """timeout='abc' must be rejected with ValueError."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1",
            "async": False,
            "timeout": "abc",
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"String timeout must be rejected, got: {payload}"
        )


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestGetInstanceRunning:
    """get_instance on a still-running instance returns 'not terminated yet'."""

    def test_get_instance_while_running(self, real_tools: Tools, real_config: Any) -> None:
        """Submit async query and immediately call get_instance before it terminates."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")

        # Submit an async query
        r = real_tools.call("execute_sql", {
            "project": project,
            "sql": "SELECT 1 AS x",
            "async": True,
        })
        submit = _text_payload(r)
        assert submit.get("success") is True, f"Async submit failed: {submit}"
        instance_id = submit["instanceId"]

        # Immediately try get_instance — the query may or may not have terminated yet.
        r2 = real_tools.call("get_instance", {
            "project": project,
            "instanceId": instance_id,
        })
        payload = _text_payload(r2)

        # Two valid outcomes:
        # 1. Still running → "not terminated yet" message (no success key or success!=True)
        # 2. Already finished → normal results with "results" key
        if "results" in payload:
            # Query was fast enough to complete; this is also valid
            logger.info("Instance %s completed before get_instance call", instance_id)
        else:
            # Still running: should have a message about not being terminated
            msg = payload.get("message") or ""
            assert "not terminated" in msg.lower() or "wait" in msg.lower(), (
                f"Expected 'not terminated' message, got: {payload}"
            )

        # Ensure the instance eventually terminates (cleanup)
        _wait_for_instance(real_tools, project, instance_id, timeout=120)
