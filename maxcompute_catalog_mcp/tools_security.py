"""Security and identity tool handlers.

Provides SecurityMixin with the check_access handler and related helpers
for identity verification and permission querying.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .mcp_protocol import mcp_ok_result, mcp_text_result
from .tools_common import _unsupported, opt_arg

logger = logging.getLogger(__name__)

# Maximum number of creator names to enrich with ARN lookups (each requires up to 3 API calls)
_MAX_CREATOR_ENRICHMENTS = 50


class SecurityMixin:
    """Mixin providing security/identity handlers.

    Expects the host class to provide: maxcompute_client, default_project,
    _get_compute_client_for_project().
    """

    def _mask_access_key_id(self, ak_id: str) -> str:
        """Mask AccessKeyId for privacy, showing only first 4 and last 4 characters."""
        if not ak_id:
            return ""
        if len(ak_id) <= 8:
            return f"{ak_id[:2]}***{ak_id[-2:]}"
        return f"{ak_id[:4]}***{ak_id[-4:]}"

    def _enrich_creator_arn(self, odps_client: Any, project: str, name: str) -> str:
        """Look up the type of a Creator item and construct its resource ARN.

        Probe order: table → resource → function.
        Returns the original name as-is if none of the known types match.
        """
        arn_prefix = f"acs:odps:*:projects/{project}"
        if odps_client.exist_table(name, project=project):
            return f"{arn_prefix}/tables/{name}"
        if odps_client.exist_resource(name, project=project):
            return f"{arn_prefix}/resources/{name}"
        if odps_client.exist_function(name, project=project):
            return f"{arn_prefix}/registration/functions/{name}"
        return name

    @staticmethod
    def _normalize_effect_entries(entries: List[Any]) -> List[Any]:
        """Normalize Effect field in a list of ACL/Policy entries.

        Server returns empty string for Allow; replace with explicit "Allow" for clarity.
        Non-dict entries (e.g. raw server markers) are passed through as-is to preserve completeness.
        """
        result = []
        for entry in entries:
            if not isinstance(entry, dict):
                result.append(entry)
                continue
            normalized = dict(entry)
            if normalized.get("Effect", None) == "":
                normalized["Effect"] = "Allow"
            result.append(normalized)
        return result

    def _format_grants_result(
        self, raw: Dict[str, Any], odps_client: Any, project: str
    ) -> Dict[str, Any]:
        """Format raw SHOW GRANTS result:
        - Creator bare names → structured ARN entries with Action: All + Effect: Allow
        - Any dict[principal → list[entry]] field (ACL, Policy, Download, etc.):
          Effect: "" → "Allow" for clarity; unknown extra fields (Condition, etc.) passed through
        - Roles / SuperPrivileges and other list-of-strings fields passed through as-is
        """
        formatted = dict(raw)

        # Enrich Creator with full ARNs; ObjectCreator always implies Allow All
        creator_names = raw.get("Creator", [])
        if creator_names:
            enriched = []
            for i, name in enumerate(creator_names):
                resource = (
                    self._enrich_creator_arn(odps_client, project, name)
                    if i < _MAX_CREATOR_ENRICHMENTS
                    else name
                )
                enriched.append({"Resource": resource, "Action": ["All"], "Effect": "Allow"})
            if len(creator_names) > _MAX_CREATOR_ENRICHMENTS:
                logger.info(
                    "Skipped ARN enrichment for %d creator(s) beyond limit of %d",
                    len(creator_names) - _MAX_CREATOR_ENRICHMENTS,
                    _MAX_CREATOR_ENRICHMENTS,
                )
            formatted["Creator"] = enriched

        # Normalize Effect for all principal-keyed grant fields (ACL, Policy, Download, etc.)
        # Detect by structure: dict[str, list[dict]] — same shape as ACL/Policy
        for key, value in raw.items():
            if key == "Creator":
                continue  # handled separately above
            if (
                isinstance(value, dict)
                and value  # non-empty
                and all(isinstance(v, list) for v in value.values())
            ):
                formatted[key] = {
                    principal: self._normalize_effect_entries(entries)
                    for principal, entries in value.items()
                }

        return formatted

    def _build_identity_info(self, odps_client: Any, project: Optional[str]) -> Dict[str, Any]:
        """Build identity dict from account credentials and whoami result.

        Reads AK ID from the account object (masked) and, if a project is available,
        runs whoami to obtain the server-side authenticated display name and user ID.
        """
        account = getattr(odps_client, "account", None)
        access_key_id = ""
        if account is not None:
            if hasattr(account, "access_id"):
                access_key_id = account.access_id or ""
            elif hasattr(account, "access_key_id"):
                access_key_id = account.access_key_id or ""

        identity: Dict[str, Any] = {
            "accessKeyId": self._mask_access_key_id(access_key_id),
            "defaultProject": self.default_project or odps_client.project or "",
            "endpoint": odps_client.endpoint or "",
        }

        if project:
            try:
                whoami_result = odps_client.run_security_query("whoami;", project=project)
                if isinstance(whoami_result, dict):
                    if "DisplayName" in whoami_result:
                        identity["displayName"] = whoami_result["DisplayName"]
                    if "ID" in whoami_result:
                        raw_id = str(whoami_result["ID"])
                        # Strip prefix before first underscore (e.g. "v4_1234567890xxxxxxxx" -> "1234567890xxxxxxxx")
                        identity["id"] = raw_id.split("_", 1)[-1] if "_" in raw_id else raw_id
            except Exception as whoami_err:
                logger.warning("whoami query failed: %s", whoami_err)

        return identity

    def _query_grants(self, project: str) -> Dict[str, Any]:
        """Execute SHOW GRANTS for the current user in the given project.

        Raises RuntimeError if the compute client cannot be created.
        """
        compute = self._get_compute_client_for_project(project)
        if compute is None:
            raise RuntimeError(
                f"Failed to create compute client for project '{project}'. "
                "Check credentials and endpoint configuration."
            )
        # SHOW GRANTS is a security command; must use run_security_query(), not execute_sql()
        grants_result = compute.run_security_query("SHOW GRANTS;", project=project)
        odps = compute.odps_client
        return {
            "project": project,
            "result": self._format_grants_result(grants_result, odps, project),
        }

    def check_access(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Check the identity and permissions of the current MCP MaxCompute access."""
        if not self.maxcompute_client:
            return _unsupported("check_access requires MaxCompute compute engine configuration (default_project + sdk_endpoint).")

        include_grants_raw = args.get("include_grants", True)
        if "include_grants" in args and not isinstance(include_grants_raw, bool):
            logger.warning(
                "include_grants should be boolean, got %s; coercing to bool",
                type(include_grants_raw).__name__,
            )
        include_grants = bool(include_grants_raw)
        project = opt_arg(args, "project", self.default_project)

        try:
            odps_client = self.maxcompute_client.odps_client
            result: Dict[str, Any] = {
                "identity": self._build_identity_info(odps_client, project),
            }

            if include_grants:
                if not project:
                    return mcp_text_result({
                        "success": False,
                        "error": "'project' is required when include_grants=true. Specify a project or configure default_project.",
                    })
                result["grants"] = self._query_grants(project)

            return mcp_ok_result(result)
        except Exception as e:
            logger.exception("check_access failed")
            return mcp_text_result({"success": False, "error": str(e)})
