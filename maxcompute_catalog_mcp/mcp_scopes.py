"""Requested tool permissions for CatalogAPI MCP tokens."""

from __future__ import annotations

import json
import re
from fnmatch import fnmatchcase

DEFAULT_MCP_SCOPES = (
    "maxcompute:metadata:read",
    "maxcompute:sql:read",
    "maxcompute:ops:read",
    "maxcompute:knowledge:read",
    "maxcompute:semanticspec:read",
)

_SCOPE_NAMESPACE = "maxcompute:"
_SCOPE_PATTERN = re.compile(r"[A-Za-z0-9_.*-]+")
_SCOPE_DOMAINS = frozenset({"metadata", "sql", "ops", "knowledge", "semanticspec"})
_MAX_SCOPES = 64
_MAX_SCOPE_LENGTH = 128
_MAX_POLICY_BYTES = 4096


def _valid_scope(scope: str) -> bool:
    if not scope.startswith(_SCOPE_NAMESPACE):
        return False
    group, separator, grant = scope.removeprefix(_SCOPE_NAMESPACE).partition(":")
    if not separator:
        return _SCOPE_PATTERN.fullmatch(group) is not None and any(
            fnmatchcase(name, group) for name in ("read", "sql", "*")
        )
    if group in {"tool", "read"}:
        return _SCOPE_PATTERN.fullmatch(grant) is not None
    return group in _SCOPE_DOMAINS and (
        grant in {"read", "*"} or (grant == "write" and group != "knowledge")
    )


def parse_mcp_scopes(settings: object) -> tuple[str, ...] | None:
    """Return explicitly requested scopes, or None if the field is absent."""
    if not isinstance(settings, dict):
        raise TypeError("mcp must be an object")
    if settings.keys() - {"scopes"}:
        raise ValueError("mcp only accepts the scopes field")
    if "scopes" not in settings:
        return None
    scopes = settings["scopes"]
    if not isinstance(scopes, list) or len(scopes) > _MAX_SCOPES:
        raise ValueError("mcp.scopes must be an array of at most 64 scope strings")
    for index, scope in enumerate(scopes):
        if (
            not isinstance(scope, str)
            or len(scope) > _MAX_SCOPE_LENGTH
            or not _valid_scope(scope)
        ):
            raise ValueError(f"invalid mcp.scopes entry at index {index}")
    if len(set(scopes)) != len(scopes):
        raise ValueError("mcp.scopes must not contain duplicate entries")
    # Valid scope expressions are ASCII; this is the compact JSON policy size.
    if len(json.dumps(scopes, separators=(",", ":"))) > _MAX_POLICY_BYTES:
        raise ValueError("mcp.scopes exceeds the 4096-byte policy limit")
    return tuple(scopes)
