"""Unit tests for tools_common.py — boundary/edge cases for shared utilities."""
from __future__ import annotations

import pytest

from maxcompute_catalog_mcp.tools_common import (
    _escape_identifier,
    _is_read_only_sql,
    _quote_partition_literal,
    _quote_sql_value,
    opt_arg,
    opt_int,
    parse_bool,
    parse_timeout,
    require_arg,
    string_prop,
    int_prop,
    input_schema,
    _build_timeout_response,
    _unsupported,
)
from maxcompute_catalog_mcp.mcp_protocol import JsonRpcError


# ---------------------------------------------------------------------------
# _quote_sql_value()
# ---------------------------------------------------------------------------

class TestQuoteSqlValue:
    def test_none(self) -> None:
        assert _quote_sql_value(None) == "NULL"

    def test_bool_true(self) -> None:
        assert _quote_sql_value(True) == "true"

    def test_bool_false(self) -> None:
        assert _quote_sql_value(False) == "false"

    def test_int(self) -> None:
        assert _quote_sql_value(42) == "CAST(42 AS INT)"

    def test_float(self) -> None:
        assert _quote_sql_value(3.14) == "3.14"

    def test_date_string(self) -> None:
        assert _quote_sql_value("2025-01-01") == "CAST('2025-01-01' AS DATE)"

    def test_string_with_quotes(self) -> None:
        result = _quote_sql_value("it's a test")
        assert "\\'" in result

    def test_string_with_backslash(self) -> None:
        result = _quote_sql_value("path\\to\\file")
        assert "\\\\" in result

    def test_regular_string(self) -> None:
        assert _quote_sql_value("hello") == "'hello'"

    def test_zero(self) -> None:
        assert _quote_sql_value(0) == "CAST(0 AS INT)"


# ---------------------------------------------------------------------------
# _quote_partition_literal()
# ---------------------------------------------------------------------------

class TestQuotePartitionLiteral:
    def test_none_raises(self) -> None:
        with pytest.raises(ValueError, match="cannot be NULL"):
            _quote_partition_literal(None)

    def test_bool(self) -> None:
        assert _quote_partition_literal(True) == "true"
        assert _quote_partition_literal(False) == "false"

    def test_int(self) -> None:
        assert _quote_partition_literal(42) == "42"

    def test_float(self) -> None:
        assert _quote_partition_literal(3.14) == "3.14"

    def test_string(self) -> None:
        assert _quote_partition_literal("abc") == "'abc'"

    def test_string_with_special_chars(self) -> None:
        result = _quote_partition_literal("it's a\\test")
        assert "\\\\'" in result or "\\'" in result


# ---------------------------------------------------------------------------
# _is_read_only_sql()
# ---------------------------------------------------------------------------

class TestIsReadOnlySql:
    def test_select(self) -> None:
        is_safe, msg = _is_read_only_sql("SELECT * FROM t")
        assert is_safe is True
        assert msg is None

    def test_with_comments(self) -> None:
        sql = "-- comment\nSELECT 1 /* block comment */"
        is_safe, _ = _is_read_only_sql(sql)
        assert is_safe is True

    def test_multiple_safe_statements(self) -> None:
        is_safe, _ = _is_read_only_sql("SELECT 1; SELECT 2")
        assert is_safe is True

    def test_mixed_unsafe(self) -> None:
        is_safe, msg = _is_read_only_sql("SELECT 1; DROP TABLE t")
        assert is_safe is False
        assert "DROP" in (msg or "")

    def test_empty(self) -> None:
        is_safe, msg = _is_read_only_sql("")
        assert is_safe is False

    def test_whitespace_only(self) -> None:
        is_safe, _ = _is_read_only_sql("   ")
        assert is_safe is False

    def test_comments_only(self) -> None:
        is_safe, _ = _is_read_only_sql("-- just a comment")
        assert is_safe is False

    def test_insert(self) -> None:
        is_safe, _ = _is_read_only_sql("INSERT INTO t VALUES (1)")
        assert is_safe is False

    def test_show(self) -> None:
        is_safe, _ = _is_read_only_sql("SHOW TABLES")
        assert is_safe is True

    def test_desc(self) -> None:
        is_safe, _ = _is_read_only_sql("DESC t1")
        assert is_safe is True

    def test_explain(self) -> None:
        is_safe, _ = _is_read_only_sql("EXPLAIN SELECT 1")
        assert is_safe is True

    def test_with_cte(self) -> None:
        is_safe, _ = _is_read_only_sql("WITH cte AS (SELECT 1) SELECT * FROM cte")
        assert is_safe is True

    def test_grant(self) -> None:
        is_safe, _ = _is_read_only_sql("GRANT ALL ON t TO user1")
        assert is_safe is False

    def test_unknown_prefix(self) -> None:
        is_safe, msg = _is_read_only_sql("FOOBAR something")
        assert is_safe is False
        assert "FOOBAR" in (msg or "")


# ---------------------------------------------------------------------------
# _escape_identifier()
# ---------------------------------------------------------------------------

class TestEscapeIdentifier:
    def test_normal(self) -> None:
        assert _escape_identifier("my_table") == "`my_table`"

    def test_with_backtick(self) -> None:
        assert _escape_identifier("col`name") == "`col``name`"

    def test_none(self) -> None:
        assert _escape_identifier(None) == ""

    def test_empty(self) -> None:
        assert _escape_identifier("") == ""

    def test_non_string(self) -> None:
        assert _escape_identifier(123) == ""


# ---------------------------------------------------------------------------
# Argument helpers
# ---------------------------------------------------------------------------

class TestRequireArg:
    def test_present(self) -> None:
        assert require_arg({"key": "val"}, "key", "msg") == "val"

    def test_missing_raises(self) -> None:
        with pytest.raises(JsonRpcError):
            require_arg({}, "key", "missing key")

    def test_empty_string_raises(self) -> None:
        with pytest.raises(JsonRpcError):
            require_arg({"key": "  "}, "key", "empty")

    def test_none_raises(self) -> None:
        with pytest.raises(JsonRpcError):
            require_arg({"key": None}, "key", "null")


class TestOptArg:
    def test_present(self) -> None:
        assert opt_arg({"k": "v"}, "k") == "v"

    def test_default(self) -> None:
        assert opt_arg({}, "k", "def") == "def"

    def test_none(self) -> None:
        assert opt_arg({"k": None}, "k") is None


class TestOptInt:
    def test_present(self) -> None:
        assert opt_int({"k": 42}, "k", 10) == 42

    def test_string_number(self) -> None:
        assert opt_int({"k": "42"}, "k", 10) == 42

    def test_invalid(self) -> None:
        assert opt_int({"k": "abc"}, "k", 10) == 10

    def test_default(self) -> None:
        assert opt_int({}, "k", 99) == 99


class TestParseTimeout:
    def test_valid(self) -> None:
        assert parse_timeout({"timeout": 30}, 10) == 30

    def test_default(self) -> None:
        assert parse_timeout({}, 10) == 10

    def test_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid timeout"):
            parse_timeout({"timeout": "abc"}, 10)

    def test_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="must be a positive"):
            parse_timeout({"timeout": 0}, 10)

    def test_negative_raises(self) -> None:
        with pytest.raises(ValueError):
            parse_timeout({"timeout": -5}, 10)


class TestParseBool:
    def test_true(self) -> None:
        assert parse_bool({"k": True}, "k", False) is True

    def test_false(self) -> None:
        assert parse_bool({"k": False}, "k", True) is False

    def test_default(self) -> None:
        assert parse_bool({}, "k", True) is True

    def test_string_raises(self) -> None:
        with pytest.raises(TypeError, match="must be boolean"):
            parse_bool({"k": "true"}, "k", False)

    def test_int_raises(self) -> None:
        with pytest.raises(TypeError):
            parse_bool({"k": 1}, "k", False)


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

class TestSchemaHelpers:
    def test_string_prop(self) -> None:
        p = string_prop("desc")
        assert p["type"] == "string"
        assert "default" not in p

    def test_string_prop_default(self) -> None:
        p = string_prop("desc", "val")
        assert p["default"] == "val"

    def test_int_prop(self) -> None:
        p = int_prop("desc")
        assert p["type"] == "integer"
        assert "default" not in p

    def test_int_prop_default(self) -> None:
        p = int_prop("desc", 42)
        assert p["default"] == 42

    def test_input_schema(self) -> None:
        s = input_schema({"a": string_prop("test")})
        assert s["type"] == "object"
        assert "a" in s["properties"]


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

class TestBuildTimeoutResponse:
    def test_basic(self) -> None:
        from unittest.mock import MagicMock
        inst = MagicMock()
        inst.id = "inst-123"
        result = _build_timeout_response(inst, "proj", 30, "Query")
        content = result["content"]
        import json
        payload = json.loads(content[0]["text"])
        assert payload["success"] is False
        assert payload["timeout"] is True
        assert payload["instanceId"] == "inst-123"
        assert "30s" in payload["message"]


class TestUnsupported:
    def test_basic(self) -> None:
        import json
        result = _unsupported("reason text")
        payload = json.loads(result["content"][0]["text"])
        assert payload["error"] == "unsupported"
        assert "reason text" in payload["message"]
