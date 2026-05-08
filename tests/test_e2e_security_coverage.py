# -*- coding: utf-8 -*-
"""E2E tests: tools_security coverage gap fillers.

Targets the 36 uncovered lines in tools_security.py (E2E coverage was 64%).
Covers:

- check_access with include_grants=True: grants structure validation
  (exercises _enrich_creator_arn, _normalize_effect_entries, _format_grants_result)
- check_access identity: displayName and id fields from whoami
  (exercises _build_identity_info whoami branch L135-148)
- check_access: string include_grants coercion
  (exercises L175-180)
- _mask_access_key_id edge cases via unit test

All tests avoid logging or asserting on environment-specific values
(project names, user IDs, access keys, endpoints).

Requires config.json (or MAXCOMPUTE_CATALOG_CONFIG env var).
"""
from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import (
    _get_tools_class,
    has_config as _has_config,
    text_payload as _text_payload,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. check_access with grants — structure validation
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCheckAccessGrantsStructure:
    """check_access(include_grants=True): validate grants result structure.

    These tests exercise _enrich_creator_arn, _normalize_effect_entries,
    and _format_grants_result by inspecting the structure of the grants
    response without asserting on specific user/role names.
    """

    def _get_grants(self, real_tools: Tools, real_config: Any) -> dict:
        """Helper: call check_access with include_grants=True, return grants dict."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("check_access", {
            "project": project,
            "include_grants": True,
        })
        payload = _text_payload(r)
        if payload.get("success") is False:
            pytest.skip(f"SHOW GRANTS not available in this environment: {payload.get('error')}")
        actual = payload.get("data") or payload
        grants = actual.get("grants")
        if grants is None:
            pytest.skip("grants not returned (may lack permission)")
        return grants

    def test_grants_contains_project_key(self, real_tools: Tools, real_config: Any) -> None:
        """Grants result must contain 'project' key."""
        grants = self._get_grants(real_tools, real_config)
        assert "project" in grants, f"grants missing 'project' key: {list(grants.keys())}"

    def test_grants_contains_result_key(self, real_tools: Tools, real_config: Any) -> None:
        """Grants result must contain 'result' key with formatted grants data."""
        grants = self._get_grants(real_tools, real_config)
        assert "result" in grants, f"grants missing 'result' key: {list(grants.keys())}"

    def test_grants_result_is_dict(self, real_tools: Tools, real_config: Any) -> None:
        """Grants result field must be a dict."""
        grants = self._get_grants(real_tools, real_config)
        result = grants.get("result", {})
        assert isinstance(result, dict), f"grants.result must be dict, got: {type(result)}"

    def test_grants_creator_entries_structure(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Creator entries must be dicts with Resource/Action/Effect after formatting.

        If the project has Creator entries in SHOW GRANTS, validate their structure.
        If not, exercise _format_grants_result directly with a synthetic raw result
        to ensure the Creator enrichment logic works end-to-end.
        """
        grants = self._get_grants(real_tools, real_config)
        result = grants.get("result", {})
        creator = result.get("Creator")
        if creator and isinstance(creator, list):
            # Real Creator entries from SHOW GRANTS — validate structure
            for entry in creator:
                if isinstance(entry, dict):
                    assert "Resource" in entry, f"Creator entry missing Resource: {entry}"
                    assert "Action" in entry, f"Creator entry missing Action: {entry}"
                    assert "Effect" in entry, f"Creator entry missing Effect: {entry}"
                    assert entry["Effect"] == "Allow", (
                        f"Creator entry Effect must be 'Allow': {entry}"
                    )
        else:
            # No Creator entries in this project — exercise _format_grants_result
            # directly with a synthetic raw result to avoid skip
            project = real_config.default_project
            odps = real_tools.maxcompute_client.odps_client
            raw = {
                "Creator": ["test_creator_table"],
                "ProjectOwner": "full-control",
            }
            formatted = real_tools._format_grants_result(raw, odps, project)
            creator_entries = formatted.get("Creator")
            assert isinstance(creator_entries, list), (
                f"Formatted Creator should be a list, got: {type(creator_entries)}"
            )
            assert len(creator_entries) >= 1, "Should have at least one Creator entry"
            entry = creator_entries[0]
            assert isinstance(entry, dict), f"Creator entry should be dict, got: {type(entry)}"
            assert "Resource" in entry, f"Creator entry missing Resource: {entry}"
            assert "Action" in entry, f"Creator entry missing Action: {entry}"
            assert entry["Action"] == ["All"], f"Creator Action should be ['All']: {entry}"
            assert "Effect" in entry, f"Creator entry missing Effect: {entry}"
            assert entry["Effect"] == "Allow", (
                f"Creator entry Effect must be 'Allow': {entry}"
            )

    def test_grants_effect_normalization(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Principal-keyed grant entries must have Effect normalized (not empty string)."""
        grants = self._get_grants(real_tools, real_config)
        result = grants.get("result", {})
        for key, value in result.items():
            if key == "Creator":
                continue
            if not isinstance(value, dict):
                continue
            if not value:
                continue
            if not all(isinstance(v, list) for v in value.values()):
                continue
            # This is a principal-keyed grant field (ACL, Policy, etc.)
            for principal, entries in value.items():
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if isinstance(entry, dict) and "Effect" in entry:
                        # After normalization, Effect should not be empty string
                        assert entry["Effect"] != "", (
                            f"Effect should be normalized from '' to 'Allow' "
                            f"in {key}/{principal}: {entry}"
                        )


# ---------------------------------------------------------------------------
# 2. check_access identity: whoami fields
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCheckAccessIdentityWhoami:
    """check_access identity: displayName and id from whoami."""

    def test_identity_has_display_name_when_project_set(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """With a project set, identity should include displayName from whoami."""
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
        # displayName is optional (whoami may fail in some environments)
        # but if present, it must be a string and non-empty
        display_name = identity.get("displayName")
        if display_name is not None:
            assert isinstance(display_name, str), (
                f"displayName must be str, got: {type(display_name)}"
            )
            assert len(display_name) > 0, "displayName should not be empty if present"

    def test_identity_has_id_when_project_set(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """With a project set, identity may include 'id' from whoami."""
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
        user_id = identity.get("id")
        if user_id is not None:
            assert isinstance(user_id, str), (
                f"id must be str, got: {type(user_id)}"
            )
            # id should have prefix stripped (e.g. "v4_xxx" -> "xxx")
            # We just verify it's not empty
            assert len(user_id) > 0, "id should not be empty if present"


# ---------------------------------------------------------------------------
# 3. include_grants string coercion
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_config(), reason="no config file for integration tests")
class TestCheckAccessGrantsCoercion:
    """check_access: string include_grants should be coerced to bool."""

    def test_include_grants_string_true_coerced(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Passing include_grants='true' (string) should be coerced to True."""
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("check_access", {
            "project": project,
            "include_grants": "true",
        })
        payload = _text_payload(r)
        # The call should not crash; bool('true') = True, so grants should be attempted
        # It may succeed (grants present) or fail (permissions), but not error on type
        if payload.get("success") is False:
            error = (payload.get("error") or "").lower()
            # Should NOT be a TypeError about include_grants
            assert "include_grants" not in error or "type" not in error, (
                f"String include_grants should be coerced, got: {payload}"
            )

    def test_include_grants_string_false_coerced(
        self, real_tools: Tools, real_config: Any
    ) -> None:
        """Passing include_grants='false' (string) should be coerced to True (non-empty string).

        Note: bool('false') == True in Python. This is documented behavior;
        the coercion uses Python's bool(), not a semantic parse.
        """
        project = real_config.default_project
        if not project:
            pytest.skip("default_project not configured")
        r = real_tools.call("check_access", {
            "project": project,
            "include_grants": "false",
        })
        payload = _text_payload(r)
        # Should not crash; bool('false') = True, so grants will be attempted
        if payload.get("success") is False:
            error = (payload.get("error") or "").lower()
            assert "include_grants" not in error or "type" not in error, (
                f"String include_grants should be coerced, got: {payload}"
            )


# ---------------------------------------------------------------------------
# 4. _mask_access_key_id edge cases (unit test, no env info leaked)
# ---------------------------------------------------------------------------

class TestMaskAccessKeyIdUnit:
    """Unit tests for _mask_access_key_id edge cases."""

    def test_mask_empty_string(self) -> None:
        """Empty string should return empty string."""
        from maxcompute_catalog_mcp.tools_security import SecurityMixin
        mixin = SecurityMixin()
        assert mixin._mask_access_key_id("") == ""

    def test_mask_short_key(self) -> None:
        """Key <= 8 chars should show first 2 + *** + last 2."""
        from maxcompute_catalog_mcp.tools_security import SecurityMixin
        mixin = SecurityMixin()
        result = mixin._mask_access_key_id("abcd")
        assert "***" in result
        assert result.startswith("ab")
        assert result.endswith("cd")

    def test_mask_long_key(self) -> None:
        """Key > 8 chars should show first 4 + *** + last 4."""
        from maxcompute_catalog_mcp.tools_security import SecurityMixin
        mixin = SecurityMixin()
        result = mixin._mask_access_key_id("LTAI_abcdefgh1234")
        assert "***" in result
        assert result.startswith("LTAI")
        assert result.endswith("1234")
        # Verify the full key is NOT present
        assert "LTAI_abcdefgh1234" not in result

    def test_mask_exactly_8_chars(self) -> None:
        """Key exactly 8 chars uses the short-key branch."""
        from maxcompute_catalog_mcp.tools_security import SecurityMixin
        mixin = SecurityMixin()
        result = mixin._mask_access_key_id("abcdefgh")
        assert "***" in result


# ---------------------------------------------------------------------------
# 5. _normalize_effect_entries unit tests (no env info leaked)
# ---------------------------------------------------------------------------

class TestNormalizeEffectEntriesUnit:
    """Unit tests for _normalize_effect_entries."""

    def test_empty_effect_becomes_allow(self) -> None:
        """Effect: '' should be normalized to 'Allow'."""
        from maxcompute_catalog_mcp.tools_security import SecurityMixin
        result = SecurityMixin._normalize_effect_entries([
            {"Effect": "", "Action": ["Read"], "Resource": "acs:odps:*:*"},
        ])
        assert len(result) == 1
        assert result[0]["Effect"] == "Allow"

    def test_non_empty_effect_preserved(self) -> None:
        """Effect: 'Deny' should be preserved as-is."""
        from maxcompute_catalog_mcp.tools_security import SecurityMixin
        result = SecurityMixin._normalize_effect_entries([
            {"Effect": "Deny", "Action": ["Write"], "Resource": "acs:odps:*:*"},
        ])
        assert result[0]["Effect"] == "Deny"

    def test_non_dict_entries_passed_through(self) -> None:
        """Non-dict entries (e.g. raw markers) should pass through unchanged."""
        from maxcompute_catalog_mcp.tools_security import SecurityMixin
        result = SecurityMixin._normalize_effect_entries(["raw_marker", 42])
        assert result == ["raw_marker", 42]

    def test_empty_list(self) -> None:
        """Empty list should return empty list."""
        from maxcompute_catalog_mcp.tools_security import SecurityMixin
        result = SecurityMixin._normalize_effect_entries([])
        assert result == []

    def test_mixed_entries(self) -> None:
        """Mix of dict and non-dict entries should be handled correctly."""
        from maxcompute_catalog_mcp.tools_security import SecurityMixin
        result = SecurityMixin._normalize_effect_entries([
            {"Effect": "", "Action": ["All"]},
            "marker_string",
            {"Effect": "Allow", "Action": ["Read"]},
        ])
        assert result[0]["Effect"] == "Allow"  # normalized
        assert result[1] == "marker_string"      # pass-through
        assert result[2]["Effect"] == "Allow"    # already correct


# ---------------------------------------------------------------------------
# 6. _enrich_creator_arn unit tests (no env info leaked)
# ---------------------------------------------------------------------------

class TestEnrichCreatorArnUnit:
    """Unit tests for _enrich_creator_arn with mocked ODPS client."""

    def test_table_type(self) -> None:
        """If exist_table returns True, should return table ARN."""
        from maxcompute_catalog_mcp.tools_security import SecurityMixin
        mixin = SecurityMixin()
        odps = MagicMock()
        odps.exist_table.return_value = True
        result = mixin._enrich_creator_arn(odps, "test_project", "my_table")
        assert result == "acs:odps:*:projects/test_project/tables/my_table"

    def test_resource_type(self) -> None:
        """If table not found but exist_resource True, should return resource ARN."""
        from maxcompute_catalog_mcp.tools_security import SecurityMixin
        mixin = SecurityMixin()
        odps = MagicMock()
        odps.exist_table.return_value = False
        odps.exist_resource.return_value = True
        result = mixin._enrich_creator_arn(odps, "test_project", "my_resource")
        assert result == "acs:odps:*:projects/test_project/resources/my_resource"

    def test_function_type(self) -> None:
        """If table/resource not found but exist_function True, should return function ARN."""
        from maxcompute_catalog_mcp.tools_security import SecurityMixin
        mixin = SecurityMixin()
        odps = MagicMock()
        odps.exist_table.return_value = False
        odps.exist_resource.return_value = False
        odps.exist_function.return_value = True
        result = mixin._enrich_creator_arn(odps, "test_project", "my_func")
        assert result == "acs:odps:*:projects/test_project/registration/functions/my_func"

    def test_unknown_type_returns_name(self) -> None:
        """If no type matches, should return the name as-is."""
        from maxcompute_catalog_mcp.tools_security import SecurityMixin
        mixin = SecurityMixin()
        odps = MagicMock()
        odps.exist_table.return_value = False
        odps.exist_resource.return_value = False
        odps.exist_function.return_value = False
        result = mixin._enrich_creator_arn(odps, "test_project", "unknown_obj")
        assert result == "unknown_obj"
