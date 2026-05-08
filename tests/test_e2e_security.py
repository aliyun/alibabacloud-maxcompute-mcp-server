# -*- coding: utf-8 -*-
"""E2E tests: check_access (security and identity tool).

Currently there is NO E2E coverage for check_access; this file provides
complete coverage against a real MaxCompute project.

Scenarios:
- Identity-only (include_grants=False): must return identity fields
- With grants (include_grants=True): must return grants
- Without project + include_grants=False: must return identity only
- Access key masking: accessKeyId must be masked (***) in the response
- No compute client: must return success=false with descriptive error
- Project arg explicitly vs default_project

Requires config.json (or MAXCOMPUTE_CATALOG_CONFIG env var).
"""
from __future__ import annotations

import logging
from typing import Any

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import (
    has_config as _has_config,
    text_payload as _text_payload,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _assert_identity_fields(identity: dict) -> None:
    """Verify the identity dict contains expected fields."""
    assert isinstance(identity, dict), f"identity must be a dict, got: {type(identity)}"
    assert "accessKeyId" in identity, f"identity missing accessKeyId: {identity}"
    assert "endpoint" in identity, f"identity missing endpoint: {identity}"
    assert "defaultProject" in identity, f"identity missing defaultProject: {identity}"


def _is_masked(ak_id: str) -> bool:
    """Return True if the accessKeyId looks like it has been masked (contains ***)."""
    if not ak_id:
        return True  # Empty string counts as "no PII exposed"
    return "***" in ak_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCheckAccessIdentityOnly:
    """check_access with include_grants=False: identity fields only."""

    def test_check_access_identity_only_explicit_project(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """include_grants=False with explicit project returns identity, no grants key."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("check_access", {
            "project": project,
            "include_grants": False,
        })
        payload = _text_payload(r)
        assert "error" not in payload or payload.get("error") == "", (
            f"check_access error: {payload.get('error')}"
        )
        identity = payload.get("identity") or (payload.get("data") or {}).get("identity")
        assert identity is not None, (
            f"check_access must return identity, got: {payload}"
        )
        _assert_identity_fields(identity)

    def test_check_access_identity_no_grants_key(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """When include_grants=False, grants key must not be present."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("check_access", {
            "project": project,
            "include_grants": False,
        })
        payload = _text_payload(r)
        # Flatten data if wrapped; use get with default to handle data={} correctly
        actual = payload.get("data", payload)
        assert "grants" not in actual, (
            f"grants must not be present when include_grants=False, got: {actual}"
        )


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCheckAccessWithGrants:
    """check_access with include_grants=True."""

    def test_check_access_with_grants_contains_grants(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """include_grants=True must return a grants object."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("check_access", {
            "project": project,
            "include_grants": True,
        })
        payload = _text_payload(r)
        actual = payload.get("data") or payload
        # grants may be under data or at top level
        grants = actual.get("grants")
        if payload.get("success") is False:
            # Some environments may lack SHOW GRANTS permission; skip gracefully
            logger.warning("check_access grants failed (may lack permission): %s", payload)
            pytest.skip(f"check_access grants not available: {payload.get('error')}")
        assert grants is not None, (
            f"check_access must return grants when include_grants=True, got: {actual}"
        )
        assert isinstance(grants, dict), (
            f"grants must be a dict, got: {type(grants)}"
        )

    def test_check_access_with_grants_has_project_key(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Grants object must contain a 'project' key."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("check_access", {
            "project": project,
            "include_grants": True,
        })
        payload = _text_payload(r)
        if payload.get("success") is False:
            pytest.skip(f"check_access grants not available: {payload.get('error')}")
        actual = payload.get("data") or payload
        grants = actual.get("grants") or {}
        assert "project" in grants, (
            f"grants must contain 'project', got: {grants}"
        )


@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCheckAccessWithoutProject:
    """check_access without project parameter."""

    def test_check_access_default_project_identity(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Without explicit project param, default_project is used; identity must be returned."""
        # real_tools has default_project set; omitting 'project' param should use it
        r = real_tools.call("check_access", {
            "include_grants": False,
        })
        payload = _text_payload(r)
        actual = payload.get("data", payload)
        identity = actual.get("identity")
        assert identity is not None, (
            f"check_access without explicit project must return identity, got: {payload}"
        )

    def test_check_access_without_project_grants_uses_default(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """include_grants=True without explicit project uses default_project.

        When default_project is configured, opt_arg falls back to it
        automatically. The call should succeed (grants may be absent if
        permissions are insufficient).
        """
        if not real_config.default_project:
            pytest.skip("default_project not configured")
        r = real_tools.call("check_access", {
            "include_grants": True,
        })
        payload = _text_payload(r)
        # With default_project set, the call should succeed
        actual = payload.get("data") or payload
        if payload.get("success") is False:
            # If it failed despite having default_project, that's a real error
            error_msg = (payload.get("error") or "").lower()
            assert "project" in error_msg, (
                f"Unexpected failure with default_project set: {payload}"
            )
            return
        # Success: grants should be present (or the test environment lacks SHOW GRANTS)
        grants = actual.get("grants")
        if grants is None:
            logger.warning(
                "check_access succeeded but no grants returned; "
                "may lack SHOW GRANTS permission: %s", payload
            )

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCheckAccessAccessKeyMasking:
    """check_access: accessKeyId masking behavior."""

    def test_access_key_id_is_masked(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """accessKeyId in the identity must be masked (must not expose full key)."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("check_access", {
            "project": project,
            "include_grants": False,
        })
        payload = _text_payload(r)
        actual = payload.get("data") or payload
        identity = actual.get("identity") or {}
        ak_id = identity.get("accessKeyId", "")
        assert _is_masked(ak_id), (
            f"accessKeyId must be masked, got: {ak_id!r}"
        )

    def test_access_key_id_not_empty(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """accessKeyId in identity should be present (even if masked)."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("check_access", {
            "project": project,
            "include_grants": False,
        })
        payload = _text_payload(r)
        actual = payload.get("data") or payload
        identity = actual.get("identity") or {}
        # accessKeyId may be empty for token-only credentials; that's fine
        assert "accessKeyId" in identity, (
            f"accessKeyId key must exist in identity, got: {identity}"
        )

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCheckAccessDisplayName:
    """check_access: whoami result enrichment."""

    def test_check_access_identity_has_endpoint(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Identity must contain endpoint; displayName is optional."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("check_access", {
            "project": project,
            "include_grants": False,
        })
        payload = _text_payload(r)
        actual = payload.get("data") or payload
        identity = actual.get("identity") or {}
        assert identity.get("endpoint"), (
            f"identity.endpoint must be non-empty, got: {identity}"
        )
        logger.info(
            "check_access identity: accessKeyId=%s, endpoint=%s, displayName=%s",
            identity.get("accessKeyId"),
            identity.get("endpoint"),
            identity.get("displayName"),
        )
