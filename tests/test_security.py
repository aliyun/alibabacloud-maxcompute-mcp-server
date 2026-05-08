"""Unit tests for tools_security.py — SecurityMixin helpers and check_access handler."""
from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import data as _data, text_payload as _text_payload


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_security_tools(
    *,
    default_project: str = "p1",
    maxcompute_client: Any = None,
    credential_client: Any = None,
) -> Tools:
    """Build a Tools instance with mock SDK and optional compute client for security tests."""
    sdk = MagicMock()
    sdk.client.get_project.return_value = MagicMock(
        to_map=lambda: {"projectId": default_project, "schemaEnabled": True}
    )
    if maxcompute_client is None:
        mc = MagicMock()
        # ODPS client with account
        mc.odps_client = MagicMock()
        mc.odps_client.account = MagicMock()
        mc.odps_client.account.access_id = "ABCDEFGHIJKL"
        mc.odps_client.project = default_project
        mc.odps_client.endpoint = "https://mc.example.com"
        mc.odps_client.run_security_query.return_value = {
            "DisplayName": "TestUser",
            "ID": "v4_1234567890abcdef",
        }
        # run_security_query on the compute client itself (used by _query_grants)
        mc.run_security_query.return_value = {
            "Creator": ["my_table"],
            "ACL": {"user1": [{"Effect": "", "Action": "Read"}]},
            "Roles": ["role_viewer"],
        }
        maxcompute_client = mc
    return Tools(
        sdk=sdk,
        default_project=default_project,
        namespace_id="ns_test",
        maxcompute_client=maxcompute_client,
        credential_client=credential_client,
    )


# ---------------------------------------------------------------------------
# 2.1 Helper method tests
# ---------------------------------------------------------------------------

class TestMaskAccessKeyId:
    def test_normal(self) -> None:
        t = _make_security_tools()
        assert t._mask_access_key_id("ABCDEFGHIJKL") == "ABCD***IJKL"

    def test_short(self) -> None:
        t = _make_security_tools()
        assert t._mask_access_key_id("ABCD1234") == "AB***34"

    def test_empty(self) -> None:
        t = _make_security_tools()
        assert t._mask_access_key_id("") == ""
        assert t._mask_access_key_id(None) == ""

    def test_exactly_8(self) -> None:
        t = _make_security_tools()
        assert t._mask_access_key_id("12345678") == "12***78"

    def test_9_chars(self) -> None:
        t = _make_security_tools()
        assert t._mask_access_key_id("123456789") == "1234***6789"


class TestNormalizeEffectEntries:
    def test_empty_effect(self) -> None:
        t = _make_security_tools()
        result = t._normalize_effect_entries([{"Effect": "", "Action": "Read"}])
        assert result == [{"Effect": "Allow", "Action": "Read"}]

    def test_non_dict(self) -> None:
        t = _make_security_tools()
        result = t._normalize_effect_entries(["raw_marker", 42])
        assert result == ["raw_marker", 42]

    def test_already_set(self) -> None:
        t = _make_security_tools()
        result = t._normalize_effect_entries([{"Effect": "Deny"}])
        assert result == [{"Effect": "Deny"}]

    def test_mixed(self) -> None:
        t = _make_security_tools()
        result = t._normalize_effect_entries([
            {"Effect": ""},
            "marker",
            {"Effect": "Deny"},
        ])
        assert result == [
            {"Effect": "Allow"},
            "marker",
            {"Effect": "Deny"},
        ]


class TestEnrichCreatorArn:
    def _make_odps(self, *, table: bool = False, resource: bool = False, function: bool = False) -> MagicMock:
        odps = MagicMock()
        odps.exist_table.return_value = table
        odps.exist_resource.return_value = resource
        odps.exist_function.return_value = function
        return odps

    def test_table(self) -> None:
        t = _make_security_tools()
        odps = self._make_odps(table=True)
        result = t._enrich_creator_arn(odps, "p1", "my_tbl")
        assert result == "acs:odps:*:projects/p1/tables/my_tbl"

    def test_resource(self) -> None:
        t = _make_security_tools()
        odps = self._make_odps(resource=True)
        result = t._enrich_creator_arn(odps, "p1", "my_res")
        assert result == "acs:odps:*:projects/p1/resources/my_res"

    def test_function(self) -> None:
        t = _make_security_tools()
        odps = self._make_odps(function=True)
        result = t._enrich_creator_arn(odps, "p1", "my_fn")
        assert result == "acs:odps:*:projects/p1/registration/functions/my_fn"

    def test_unknown(self) -> None:
        t = _make_security_tools()
        odps = self._make_odps()
        result = t._enrich_creator_arn(odps, "p1", "unknown_item")
        assert result == "unknown_item"


# ---------------------------------------------------------------------------
# 2.2 _format_grants_result() tests
# ---------------------------------------------------------------------------

class TestFormatGrantsResult:
    def test_with_creator(self) -> None:
        t = _make_security_tools()
        odps = MagicMock()
        odps.exist_table.return_value = True
        raw = {"Creator": ["tbl1", "tbl2"]}
        result = t._format_grants_result(raw, odps, "p1")
        assert len(result["Creator"]) == 2
        for entry in result["Creator"]:
            assert entry["Action"] == ["All"]
            assert entry["Effect"] == "Allow"
            assert "Resource" in entry

    def test_creator_over_max_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import maxcompute_catalog_mcp.tools_security as ts
        monkeypatch.setattr(ts, "_MAX_CREATOR_ENRICHMENTS", 2)
        t = _make_security_tools()
        odps = MagicMock()
        odps.exist_table.return_value = True
        raw = {"Creator": ["t1", "t2", "t3"]}
        result = t._format_grants_result(raw, odps, "p1")
        # t3 (index 2) should NOT have ARN enrichment (uses name as-is)
        assert result["Creator"][2]["Resource"] == "t3"
        # t1 and t2 should have full ARN
        assert "tables/t1" in result["Creator"][0]["Resource"]

    def test_acl_effect_normalization(self) -> None:
        t = _make_security_tools()
        raw = {
            "ACL": {
                "user1": [{"Effect": "", "Action": "Read"}, {"Effect": "Deny", "Action": "Write"}]
            }
        }
        result = t._format_grants_result(raw, MagicMock(), "p1")
        assert result["ACL"]["user1"][0]["Effect"] == "Allow"
        assert result["ACL"]["user1"][1]["Effect"] == "Deny"

    def test_non_grant_fields_passthrough(self) -> None:
        t = _make_security_tools()
        raw = {"Roles": ["admin", "viewer"], "SuperPrivileges": ["All"]}
        result = t._format_grants_result(raw, MagicMock(), "p1")
        assert result["Roles"] == ["admin", "viewer"]
        assert result["SuperPrivileges"] == ["All"]


# ---------------------------------------------------------------------------
# 2.3 _build_identity_info() tests
# ---------------------------------------------------------------------------

class TestBuildIdentityInfo:
    def test_with_access_id(self) -> None:
        t = _make_security_tools()
        odps = MagicMock()
        odps.account = MagicMock(spec=["access_id"])
        odps.account.access_id = "ABCDEFGHIJKL"
        odps.project = "p1"
        odps.endpoint = "https://mc.example.com"
        identity = t._build_identity_info(odps, None)
        assert identity["accessKeyId"] == "ABCD***IJKL"

    def test_with_access_key_id(self) -> None:
        t = _make_security_tools()
        odps = MagicMock()
        odps.account = MagicMock(spec=["access_key_id"])
        odps.account.access_key_id = "XYZW12345678"
        odps.project = "p1"
        odps.endpoint = "https://mc.example.com"
        identity = t._build_identity_info(odps, None)
        assert identity["accessKeyId"] == "XYZW***5678"

    def test_with_whoami(self) -> None:
        t = _make_security_tools()
        odps = MagicMock()
        odps.account = None
        odps.project = "p1"
        odps.endpoint = "https://mc.example.com"
        odps.run_security_query.return_value = {"DisplayName": "TestUser", "ID": "v4_123456"}
        identity = t._build_identity_info(odps, "p1")
        assert identity["displayName"] == "TestUser"
        assert identity["id"] == "123456"

    def test_whoami_failure(self, caplog: pytest.LogCaptureFixture) -> None:
        t = _make_security_tools()
        odps = MagicMock()
        odps.account = None
        odps.project = "p1"
        odps.endpoint = "https://mc.example.com"
        odps.run_security_query.side_effect = RuntimeError("network error")
        with caplog.at_level(logging.WARNING):
            identity = t._build_identity_info(odps, "p1")
        assert "displayName" not in identity
        assert any("whoami" in m for m in caplog.messages)

    def test_whoami_id_with_underscore_strips_prefix(self) -> None:
        """ID containing underscore splits on first '_' and keeps the suffix."""
        t = _make_security_tools()
        odps = MagicMock()
        odps.account = None
        odps.project = "p1"
        odps.endpoint = "https://mc.example.com"
        odps.run_security_query.return_value = {"DisplayName": "User", "ID": "plain_id_no_underscore"}
        identity = t._build_identity_info(odps, "p1")
        # "plain_id_no_underscore" contains underscore -> split on first _
        assert identity["id"] == "id_no_underscore"

    def test_whoami_id_no_underscore(self) -> None:
        t = _make_security_tools()
        odps = MagicMock()
        odps.account = None
        odps.project = "p1"
        odps.endpoint = "https://mc.example.com"
        odps.run_security_query.return_value = {"DisplayName": "User", "ID": "123456"}
        identity = t._build_identity_info(odps, "p1")
        assert identity["id"] == "123456"


# ---------------------------------------------------------------------------
# 2.4 check_access() full flow tests
# ---------------------------------------------------------------------------

class TestCheckAccess:
    def test_no_compute_returns_unsupported(self) -> None:
        """maxcompute_client is None → error=unsupported."""
        t = _make_security_tools(maxcompute_client=None)
        # _make_security_tools builds a default mock when maxcompute_client=None; force it to None here.
        t.maxcompute_client = None
        r = t.call("check_access", {})
        payload = _text_payload(r)
        assert payload.get("error") == "unsupported"

    def test_identity_only(self) -> None:
        t = _make_security_tools()
        r = t.call("check_access", {"include_grants": False})
        payload = _text_payload(r)
        d = _data(payload)
        assert "identity" in d
        assert "grants" not in d

    def test_with_grants(self) -> None:
        t = _make_security_tools()
        r = t.call("check_access", {"include_grants": True, "project": "p1"})
        payload = _text_payload(r)
        d = _data(payload)
        assert "identity" in d
        assert "grants" in d

    def test_no_project_grants_error(self) -> None:
        t = _make_security_tools(default_project="")
        t.maxcompute_client = MagicMock()
        t.maxcompute_client.odps_client = MagicMock()
        t.maxcompute_client.odps_client.account = None
        t.maxcompute_client.odps_client.project = ""
        t.maxcompute_client.odps_client.endpoint = "https://mc.example.com"
        r = t.call("check_access", {"include_grants": True})
        payload = _text_payload(r)
        assert payload.get("success") is False
        assert "project" in str(payload.get("error", "")).lower()

    def test_include_grants_non_bool_coercion(self, caplog: pytest.LogCaptureFixture) -> None:
        """Non-bool include_grants triggers a warning log."""
        t = _make_security_tools()
        with caplog.at_level(logging.WARNING):
            r = t.call("check_access", {"include_grants": "yes", "project": "p1"})
        _text_payload(r)  # ensure the call completes and returns valid JSON
        assert any("include_grants" in m for m in caplog.messages), (
            "expected a warning mentioning include_grants when a non-bool value is passed"
        )

    def test_exception_returns_error(self) -> None:
        t = _make_security_tools()
        # Force an exception during execution by mocking the internal builder
        t._build_identity_info = MagicMock(side_effect=RuntimeError("identity boom"))
        r = t.call("check_access", {})
        payload = _text_payload(r)
        assert payload.get("success") is False
        assert "identity boom" in payload.get("error", "")


# ---------------------------------------------------------------------------
# 2.5 _query_grants() tests
# ---------------------------------------------------------------------------

class TestQueryGrants:
    def test_success(self) -> None:
        t = _make_security_tools()
        result = t._query_grants("p1")
        assert result["project"] == "p1"
        assert "result" in result

    def test_no_compute_raises(self) -> None:
        t = _make_security_tools()
        t.maxcompute_client = None
        with pytest.raises(RuntimeError, match="Failed to create compute client"):
            t._query_grants("p1")


# ---------------------------------------------------------------------------
# 3. check_access error paths (unit tests, no real config needed)
# ---------------------------------------------------------------------------

class TestCheckAccessNoProjectGrantsError:
    """check_access: include_grants=True with no project (and no default_project) must error."""

    def test_check_access_no_project_no_default_grants_error(
        self, mock_sdk: MagicMock, mock_maxcompute_client: MagicMock
    ) -> None:
        """When default_project is empty and project is not passed,
        include_grants=True must return success=False with error about project."""
        tools = Tools(
            sdk=mock_sdk,
            default_project="",  # no default_project
            namespace_id="test_namespace_id",
            maxcompute_client=mock_maxcompute_client,
        )
        r = tools.call("check_access", {
            "include_grants": True,
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"check_access with include_grants=True and no project must fail, got: {payload}"
        )
        error_msg = (payload.get("error") or "").lower()
        assert "project" in error_msg, (
            f"Error message should mention 'project', got: {payload}"
        )


class TestCheckAccessNoComputeClient:
    """check_access: no compute client must return descriptive error."""

    def test_check_access_no_compute_client(
        self, tools_no_compute: Tools
    ) -> None:
        """tools_no_compute has no maxcompute_client; check_access must fail gracefully."""
        r = tools_no_compute.call("check_access", {
            "project": "any_project",
            "include_grants": False,
        })
        payload = _text_payload(r)
        assert payload.get("success") is False, (
            f"check_access without compute client must fail, got: {payload}"
        )
        error_msg = payload.get("error") or ""
        message = payload.get("message") or ""
        assert error_msg or message, "Expected non-empty error message when no compute client"
        # _unsupported() returns error='unsupported' and message=<reason>
        combined = f"{error_msg} {message}".lower()
        assert (
            "compute" in combined
            or "maxcompute" in combined
            or "unsupported" in combined
        ), (
            f"Error message should mention compute/MaxCompute or be unsupported, "
            f"got error={error_msg!r}, message={message!r}"
        )
