"""Shared utilities for MCP tool implementations.

Contains ToolSpec, argument helpers, SQL safety checking, and common formatting.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from .mcp_protocol import JsonRpcError, mcp_text_result

logger = logging.getLogger(__name__)


def _env(key: str, default: str = "") -> str:
    """Read an environment variable, stripping whitespace and falling back to default."""
    return os.environ.get(key, default).strip() or default


def _escape_identifier(name: str) -> str:
    """Escape a SQL identifier (table name, column name, etc.) by wrapping in backticks.

    Per MaxCompute convention, identifiers can be wrapped in backticks to prevent
    SQL injection. Internal backticks are escaped as double backticks.

    Args:
        name: Identifier name; must be a string.

    Returns:
        Escaped identifier, e.g. `my_table` or `col``name`.
        Returns empty string if input is invalid (None or non-string).
    """
    if not isinstance(name, str):
        return ""
    if not name:
        return ""
    # Replace internal backticks with double backticks for escaping
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


# Only allow read-only SQL (SELECT, WITH, etc.); aligned with maxcompute_ai_agent sql_executor.
# NOTE: "SET" is intentionally excluded — in script mode (odps.sql.submit.mode=script),
# SET statements within the SQL body can override session parameters, including the
# server-side odps.sql.read.only=true hint.  Users who need to set runtime parameters
# should use the `hints` parameter instead.
_UNSAFE_SQL_KEYWORDS = frozenset([
    "INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "TRUNCATE",
    "CREATE", "DROP", "ALTER", "RENAME", "GRANT", "REVOKE",
    "CALL", "EXEC", "EXECUTE", "LOAD", "UNLOAD", "COPY", "MSCK", "REPAIR",
])
_ALLOWED_SQL_PREFIXES = frozenset(["SELECT", "WITH", "SHOW", "DESC", "DESCRIBE", "EXPLAIN", "VALUES"])

# Pre-compiled patterns for SQL normalization and safety checks
_SQL_LINE_COMMENT = re.compile(r"--[^\n]*")
# NOTE: non-greedy match; does NOT handle nested block comments like
# '/* outer /* inner */ still_comment */'.  MaxCompute does not support
# nested block comments (matches ANSI SQL), so this is not a concern in
# practice.
_SQL_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
# String literals and quoted identifiers.  Supports:
#   - single-quoted strings with backslash escape ('it\'s')
#   - double-quoted strings (MaxCompute odps2 mode treats them as strings)
#   - backtick-escaped identifiers (``foo``)
# We strip their contents before any keyword / split(";") / comment scan so
# user data (e.g. 'a;b', 'INSERT demo', column named `insert`) cannot trigger
# false positives.  Known limitation: we do NOT attempt to reject based on
# these — the server-side odps.sql.read.only=true hint is the authoritative
# guard; the client check exists only to catch obvious DML/DDL typos early.
_SQL_SINGLE_QUOTED = re.compile(r"'(?:\\.|[^'\\])*'", re.DOTALL)
_SQL_DOUBLE_QUOTED = re.compile(r'"(?:\\.|[^"\\])*"', re.DOTALL)
_SQL_BACKTICK_IDENT = re.compile(r"`(?:``|[^`])*`")
# One regex per unsafe keyword, pre-compiled for CTE body scanning
_UNSAFE_KW_PATTERNS = {kw: re.compile(rf"\b{kw}\b") for kw in _UNSAFE_SQL_KEYWORDS}


def _strip_string_literals(sql: str) -> str:
    """Replace quoted content with empty placeholders.

    Handles single-quoted strings (with backslash escape), double-quoted
    strings (MC odps2 mode), and backtick-escaped identifiers.  This
    prevents user data that happens to contain ';', '--', '/*' or DML
    keywords from poisoning downstream regex / split operations.
    """
    s = _SQL_SINGLE_QUOTED.sub("''", sql)
    s = _SQL_DOUBLE_QUOTED.sub('""', s)
    s = _SQL_BACKTICK_IDENT.sub("``", s)
    return s


def _normalize_sql(sql: str) -> str:
    """Strip quoted content and comments, collapse whitespace, and uppercase.

    Order matters: quoted content is stripped *before* comment regex so that
    '--' / '/*' inside a string literal does not cause the comment regex to
    consume beyond the string boundary.  After this step, split(';') is safe
    to use because any ';' left in the SQL is a true statement separator.
    """
    s = _strip_string_literals(sql.strip())
    s = _SQL_LINE_COMMENT.sub("", s)
    s = _SQL_BLOCK_COMMENT.sub("", s)
    return " ".join(s.split()).upper()


def _is_read_only_sql(sql: str) -> tuple[bool, Optional[str]]:
    """Best-effort check whether *sql* is read-only.

    Returns (is_safe, error_message).

    This is a heuristic guard, NOT a full SQL parser.  It strips comments,
    splits on semicolons, and checks each statement's first keyword against
    allow/deny lists.  For CTE statements (WITH ...), it also scans the body
    for unsafe keywords to prevent CTE-based DML bypass.

    Edge cases (dynamic SQL, exotic syntax, obfuscation) may bypass this
    check.  It is intended as a first line of defence to prevent accidental
    DML/DDL; the server-side odps.sql.read.only=true hint provides the
    authoritative access control.

    Known limitation of CTE body scan: the scan uses ``\\b<KW>\\b`` regex
    across the entire body, so a CTE that references a bare reserved word
    as a table / column name (e.g. ``SELECT * FROM REPAIR`` — no qualifier,
    no underscore suffix) may be falsely rejected.  Table names like
    ``repair_log`` or ``copy_history`` are NOT affected because ``_`` is a
    word character.  Since using bare reserved words as identifiers is
    extremely rare and the server-side guard is authoritative, we accept
    this minor false-positive surface.
    """
    if not sql or not sql.strip():
        return False, "Empty SQL statement"
    normalized = _normalize_sql(sql)
    if not normalized:
        return False, "Empty SQL after removing comments"
    for stmt in [s.strip() for s in normalized.split(";") if s.strip()]:
        first = (stmt.split() or [""])[0]
        if first in _ALLOWED_SQL_PREFIXES:
            # For WITH (CTE) statements, also check the body for DML keywords
            # to prevent "WITH ... INSERT/UPDATE/DELETE/MERGE" bypass.
            # String literals were already stripped by _normalize_sql, so
            # keyword matching here only sees SQL syntax, not user data.
            if first == "WITH":
                body = stmt[len("WITH"):]
                for kw, pattern in _UNSAFE_KW_PATTERNS.items():
                    if pattern.search(body):
                        return False, f"Unsafe SQL operation detected in CTE body: {kw}"
            continue
        for kw in _UNSAFE_SQL_KEYWORDS:
            if first == kw or stmt.startswith(kw + " "):
                return False, f"Unsafe SQL operation detected: {kw}"
        if first and first not in _ALLOWED_SQL_PREFIXES:
            return False, f"Only SELECT queries are allowed. Found: {first}"
    return True, None


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    input_schema: Dict[str, Any]


def string_prop(description: str, default: Optional[str] = None) -> Dict[str, Any]:
    d: Dict[str, Any] = {"type": "string", "description": description}
    if default is not None:
        d["default"] = default
    return d


def int_prop(description: str, default: Optional[int] = None) -> Dict[str, Any]:
    d: Dict[str, Any] = {"type": "integer", "description": description}
    if default is not None:
        d["default"] = default
    return d


def input_schema(properties: Dict[str, Any], required: Optional[List[str]] = None) -> Dict[str, Any]:
    schema: Dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def require_arg(args: Dict[str, Any], key: str, msg: str) -> str:
    v = args.get(key)
    if v is None or str(v).strip() == "":
        raise JsonRpcError(-32602, "Invalid params", msg)
    return str(v)


def opt_arg(args: Dict[str, Any], key: str, default: Optional[str] = None) -> Optional[str]:
    v = args.get(key, default)
    if v is None:
        return None
    return str(v)


def opt_int(args: Dict[str, Any], key: str, default: int) -> int:
    v = args.get(key, default)
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def parse_timeout(args: Dict[str, Any], default: int) -> int:
    """Parse timeout from args. Raises ValueError on invalid or non-positive value."""
    raw = args.get("timeout")
    if raw is not None:
        try:
            val = int(raw)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid timeout value {raw!r}; must be a positive integer.")
        if val <= 0:
            raise ValueError(f"Invalid timeout value {val}; must be a positive integer.")
        return val
    return default


def parse_bool(args: Dict[str, Any], key: str, default: bool) -> bool:
    """Parse a boolean arg. Raises TypeError if the value is not a bool.

    Note: bool("false") == True in Python, so strict type checking is required
    to avoid silent misinterpretation of string inputs.
    """
    v = args.get(key, default)
    if not isinstance(v, bool):
        raise TypeError(
            f"Argument {key!r} must be boolean (true/false), got {type(v).__name__!r}."
        )
    return v


def _build_timeout_response(
    inst: Any,
    project: str,
    timeout_secs: int,
    operation: str,
    detail: str = "",
) -> Dict[str, Any]:
    """Build a standard timeout error response."""
    return mcp_text_result({
        "success": False,
        "timeout": True,
        "instanceId": inst.id,
        "project": project,
        "message": (
            f"{operation} timed out after {timeout_secs}s{detail}. "
            "Use get_instance_status or get_instance with instanceId to poll for results."
        ),
    })


def _unsupported(reason: str) -> Dict[str, Any]:
    """Return a uniform 'unsupported' result for client/agent to recognize."""
    return mcp_text_result({"success": False, "error": "unsupported", "message": reason})


# Pre-compiled regex for DATE pattern matching (avoid recompilation on each call)
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _quote_sql_value(v: Any) -> str:
    """Quote a value for SQL VALUES clause.

    Note: bool check must come before int (bool is subclass of int).
    """
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return f"CAST({v} AS INT)"
    if isinstance(v, float):
        return str(v)
    s = str(v).strip()
    if _DATE_PATTERN.match(s):
        return f"CAST('{s}' AS DATE)"
    s = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def _quote_partition_literal(v: Any) -> str:
    """Quote a literal for PARTITION (col=...) clause.

    Unlike VALUES clause, partition literals cannot be NULL and don't use CAST.
    """
    if v is None:
        raise ValueError("Partition column value cannot be NULL")
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return str(v)
    s = str(v).strip()
    s = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


ToolHandler = Callable[[Dict[str, Any]], Dict[str, Any]]
