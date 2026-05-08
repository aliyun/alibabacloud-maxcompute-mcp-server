"""Table metadata (comment, labels, column descriptions, expiration, ...) tool handlers.

Provides :class:`TableMetaMixin` with ``update_table`` plus the shared
:func:`serialize_table_meta` helper used by ``get_table_schema`` to render a
business-semantic view.

The underlying Catalog SDK's ``update_table`` supports a specific set of mutable
fields (see docs/SDK notes): description, labels, expirationOptions, clustering,
tableConstraints, tableFormatDefinition, column description (including nested
struct fields), top-level column mode REQUIRED → NULLABLE, and appending new
NULLABLE columns. Other column operations (delete/reorder/insert/type change,
NULLABLE → REQUIRED, appending REQUIRED, setting nested column mode) are
rejected by the service.

This mixin wraps the read-modify-write + etag pattern so that LLM callers only
need to specify a JSON patch; the client automatically fetches the latest etag
and reapplies changes on top of the current server state.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pyodps_catalog import models as catalog_models

from .mcp_protocol import mcp_text_result
from .tools_common import opt_arg, require_arg

logger = logging.getLogger(__name__)


# Fields explicitly allowed to set via this tool's labelsMode semantics.
_LABELS_MODES = {"merge", "replace", "delete"}


def _field_to_dict(field: catalog_models.TableFieldSchema) -> Dict[str, Any]:
    """Serialize a TableFieldSchema (recursively) into a plain dict for display.

    Only business-facing fields are emitted: name/type/description/mode plus
    nested struct children. Internal physical attributes (typeCategory,
    precision, scale, maxLength, defaultValueExpression) are intentionally
    omitted to keep responses focused on the semantic layer.
    """
    out: Dict[str, Any] = {
        "name": field.field_name or "",
        "type": field.sql_type_definition or "",
        "description": field.description or "",
    }
    if field.mode:
        out["mode"] = field.mode
    nested = [_field_to_dict(c) for c in (field.fields or [])]
    if nested:
        out["fields"] = nested
    return out


def _find_field_by_path(
    fields: List[catalog_models.TableFieldSchema],
    path: List[str],
) -> Optional[catalog_models.TableFieldSchema]:
    """Resolve a dotted field path against a list of fields.

    For example ``["addr", "city"]`` navigates ``addr.fields -> city``.
    Returns ``None`` if any component is missing.
    """
    if not path:
        return None
    head, *rest = path
    for f in fields:
        if f.field_name == head:
            if not rest:
                return f
            return _find_field_by_path(f.fields or [], rest)
    return None


# ---------------------------------------------------------------------------
# SQL type string parser
#
# The Catalog PUT API requires every TableFieldSchema to carry:
#   - typeCategory  (FieldDataType enum, always uppercase)
#   - precision/scale  for DECIMAL
#   - max_length       for CHAR / VARCHAR
#   - fields[]         for ARRAY / MAP / STRUCT (nested, each with typeCategory)
#
# This module-level parser turns a user-supplied type string (e.g.
# "ARRAY<STRUCT<a:INT,b:VARCHAR(64)>>") into a fully-populated
# TableFieldSchema so that columns.add never hits a 400.
# ---------------------------------------------------------------------------

_SIMPLE_TYPE_NAMES: frozenset = frozenset({
    "BIGINT", "INT", "TINYINT", "SMALLINT",
    "FLOAT", "DOUBLE",
    "BOOLEAN",
    "STRING", "BINARY",
    "DATE", "DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ",
    "JSON", "BLOB",
    "INTERVAL_DAY_TIME", "INTERVAL_YEAR_MONTH",
})

# Maximum nesting depth for complex types (ARRAY / MAP / STRUCT).
# _parse_sql_type and _parse_complex_type are mutually recursive; without a
# depth cap a pathological input like ARRAY<ARRAY<...<INT>...>> would overflow
# Python's call stack.
#
# MaxCompute 2.0 data-type edition officially supports at most 20 levels of
# nesting (https://help.aliyun.com/zh/maxcompute/user-guide/
# maxcompute-v2-0-data-type-edition).  The constant is set to 20 and the
# guard uses strict inequality (`_depth > _MAX_TYPE_NESTING_DEPTH`) so the
# constant directly reads as "maximum 20 levels allowed".
_MAX_TYPE_NESTING_DEPTH: int = 20


def _find_close_bracket(s: str, open_pos: int, open_ch: str, close_ch: str) -> int:
    """Return the index of the bracket matching ``s[open_pos]``.

    Raises ``ValueError`` if no matching bracket is found.
    """
    depth = 0
    for i in range(open_pos, len(s)):
        if s[i] == open_ch:
            depth += 1
        elif s[i] == close_ch:
            depth -= 1
            if depth == 0:
                return i
    raise ValueError(f"Unmatched '{open_ch}' in type string: {s!r}")


def _split_top_level(s: str, sep: str = ",") -> List[str]:
    """Split *s* by *sep*, ignoring separators inside ``<>`` or ``()``."""
    depth = 0
    parts: List[str] = []
    buf: List[str] = []
    for ch in s:
        if ch in "<(":
            depth += 1
            buf.append(ch)
        elif ch in ">)":
            depth -= 1
            buf.append(ch)
        elif ch == sep and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf).strip())
    return [p for p in parts if p]


def _find_colon_top_level(s: str) -> int:
    """Return the index of the first ``':'`` not inside ``<>`` / ``()``, or ``-1``."""
    depth = 0
    for i, ch in enumerate(s):
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        elif ch == ":" and depth == 0:
            return i
    return -1


def _parse_sql_type(type_str: str, _depth: int = 0) -> catalog_models.TableFieldSchema:
    """Parse a SQL type string into a fully-populated ``TableFieldSchema``.

    Sets ``type_category`` (required by the Catalog PUT API) and all
    type-specific attributes so the resulting schema can be sent directly
    in a PUT without further transformation.

    Supported formats (case-insensitive, whitespace-tolerant)::

        BIGINT, INT, TINYINT, SMALLINT, FLOAT, DOUBLE, BOOLEAN
        STRING, BINARY, DATE, DATETIME, TIMESTAMP, TIMESTAMP_NTZ
        JSON, BLOB, INTERVAL_DAY_TIME, INTERVAL_YEAR_MONTH
        DECIMAL(precision, scale)
        CHAR(n), VARCHAR(n)
        ARRAY<element_type>
        MAP<key_type, value_type>
        STRUCT<col1:type1, col2:type2, ...>

    Nested complex types are supported up to ``_MAX_TYPE_NESTING_DEPTH`` levels
    deep, e.g.::

        ARRAY<STRUCT<a:INT, b:VARCHAR(64)>>
        MAP<STRING, ARRAY<DECIMAL(10,2)>>
        STRUCT<tags:ARRAY<STRING>, meta:MAP<STRING,BIGINT>>

    Args:
        type_str: The SQL type string to parse.
        _depth: Internal recursion depth counter — do not pass from call sites.
            Incremented each time a complex-type boundary is crossed so that
            pathological inputs are rejected before the Python call stack
            overflows (see ``_MAX_TYPE_NESTING_DEPTH``).

    Raises:
        ValueError: descriptive message for invalid, unsupported, or
            excessively nested input.
    """
    type_str = type_str.strip()
    if _depth > _MAX_TYPE_NESTING_DEPTH:
        raise ValueError(
            f"Type nesting depth ({_depth}) exceeds the maximum allowed "
            f"({_MAX_TYPE_NESTING_DEPTH}). The inner type {type_str!r} is "
            f"nested too deeply — simplify the type definition."
        )
    if not type_str:
        raise ValueError("Type string cannot be empty.")
    upper = type_str.upper()
    lt = upper.find("<")
    lp = upper.find("(")
    has_angle = lt != -1
    has_paren = lp != -1

    if not has_angle and not has_paren:
        if upper not in _SIMPLE_TYPE_NAMES:
            raise ValueError(
                f"Unknown type {upper!r}. "
                f"For parameterised types use DECIMAL(p,s)/CHAR(n)/VARCHAR(n). "
                f"For complex types use ARRAY<T>, MAP<K,V>, STRUCT<n:T>. "
                f"Simple types: {', '.join(sorted(_SIMPLE_TYPE_NAMES))}."
            )
        f = catalog_models.TableFieldSchema()
        f.type_category = upper
        f.sql_type_definition = upper
        return f

    if has_angle and (not has_paren or lt < lp):
        base = upper[:lt].strip()
        close = _find_close_bracket(upper, lt, "<", ">")
        inner = type_str[lt + 1 : close].strip()
        f = _parse_complex_type(base, inner, _depth=_depth + 1)
        f.sql_type_definition = upper
        return f

    # Parameterised: DECIMAL(…), CHAR(…), VARCHAR(…)
    base = upper[:lp].strip()
    close = _find_close_bracket(upper, lp, "(", ")")
    inner = type_str[lp + 1 : close].strip()
    f = _parse_param_type(base, inner)
    f.sql_type_definition = upper
    return f


def _parse_param_type(base: str, inner: str) -> catalog_models.TableFieldSchema:
    """Parse a parameterised simple type: DECIMAL(p,s), CHAR(n), VARCHAR(n)."""
    f = catalog_models.TableFieldSchema()
    if base == "DECIMAL":
        parts = [p.strip() for p in inner.split(",")]
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(
                f"DECIMAL requires (precision, scale) — e.g. DECIMAL(10,2); got: ({inner!r})."
            )
        f.type_category = "DECIMAL"
        f.precision = parts[0]
        f.scale = parts[1]
    elif base in ("CHAR", "VARCHAR"):
        n = inner.strip()
        if not n:
            raise ValueError(f"{base} requires a length parameter, e.g. {base}(255).")
        f.type_category = base
        f.max_length = n
    else:
        raise ValueError(
            f"Type {base!r} does not accept parameters. "
            f"For complex nested types use angle brackets: {base}<\u2026>."
        )
    return f


def _parse_complex_type(base: str, inner: str, _depth: int = 0) -> catalog_models.TableFieldSchema:
    """Parse ARRAY<…>, MAP<…>, or STRUCT<…>. ``_depth``: see :func:`_parse_sql_type`.

    Note: callers already increment _depth by 1 before passing it here, so
    recursive calls back to :func:`_parse_sql_type` pass ``_depth`` as-is
    (each _parse_sql_type <-> _parse_complex_type cycle counts as one level).
    """
    f = catalog_models.TableFieldSchema()
    if base == "ARRAY":
        elem = _parse_sql_type(inner, _depth=_depth)
        elem.field_name = "element"
        # Catalog API: "Field mode can only be specified for top level fields"
        # — nested element fields must NOT carry a mode attribute.
        elem.mode = None
        f.type_category = "ARRAY"
        f.fields = [elem]
    elif base == "MAP":
        parts = _split_top_level(inner, ",")
        if len(parts) != 2:
            raise ValueError(
                f"MAP requires exactly two type arguments <key, value>; "
                f"got {len(parts)} in: {inner!r}."
            )
        key = _parse_sql_type(parts[0], _depth=_depth)
        key.field_name = "key"
        # Catalog API: mode is only allowed on top-level fields
        key.mode = None
        val = _parse_sql_type(parts[1], _depth=_depth)
        val.field_name = "value"
        val.mode = None
        f.type_category = "MAP"
        f.fields = [key, val]
    elif base == "STRUCT":
        col_strs = _split_top_level(inner, ",")
        if not col_strs:
            raise ValueError("STRUCT requires at least one field: STRUCT<name:type, \u2026>.")
        sub_fields: List[catalog_models.TableFieldSchema] = []
        for part in col_strs:
            colon = _find_colon_top_level(part)
            if colon == -1:
                raise ValueError(
                    f"Each STRUCT field must be 'name:type'; missing ':' in {part!r}."
                )
            field_name = part[:colon].strip().lower()
            type_part = part[colon + 1 :].strip()
            if not field_name:
                raise ValueError(f"STRUCT field name is empty in: {part!r}.")
            sub = _parse_sql_type(type_part, _depth=_depth)
            sub.field_name = field_name
            # Catalog API: mode is only allowed on top-level fields
            sub.mode = None
            sub_fields.append(sub)
        f.type_category = "STRUCT"
        f.fields = sub_fields
    else:
        raise ValueError(
            f"Unknown complex type {base!r}. Expected ARRAY, MAP, or STRUCT."
        )
    return f


def _iter_partition_definition_columns(partition_definition: Any) -> List[Any]:
    """Partition columns from SDK ``PartitionDefinition`` (`partitioned_columns` / JSON ``partitionedColumns``)."""
    if partition_definition is None:
        return []
    return list(getattr(partition_definition, "partitioned_columns", None) or [])


def serialize_table_meta(t: catalog_models.Table) -> Dict[str, Any]:
    """Flatten a Table model into a compact, business-semantic dict.

    Exposes only the fields that are meaningful to business users and mutable
    via ``update_table``: description, labels, columns (recursive, with mode +
    nested children), expiration, plus bookkeeping (type/etag/timestamps).

    Cold SDK fields (clustering, tableConstraints, tableFormatDefinition,
    typeCategory/precision/scale/maxLength/defaultValueExpression) are
    deliberately left out to keep responses focused and token-cheap.
    """
    columns_out: List[Dict[str, Any]] = []
    if t.table_schema and t.table_schema.fields:
        for f in t.table_schema.fields:
            columns_out.append(_field_to_dict(f))

    expiration: Dict[str, Any] = {}
    if t.expiration_options is not None:
        days = t.expiration_options.expiration_days
        part_days = t.expiration_options.partition_expiration_days
        if days is not None:
            expiration["days"] = days
        if part_days is not None:
            expiration["partitionDays"] = part_days

    partition_keys: list[str] = []
    if t.partition_definition is not None:
        for pc in _iter_partition_definition_columns(t.partition_definition):
            field = getattr(pc, "field", None)
            if field:
                partition_keys.append(field)

    return {
        "etag": t.etag,
        "type": t.type,
        "description": t.description or "",
        "labels": dict(t.labels or {}),
        "columns": columns_out,
        "partitionKeys": list(dict.fromkeys(partition_keys)),
        "expiration": expiration,
        "createTime": t.create_time,
        "lastModifiedTime": t.last_modified_time,
    }


class TableMetaMixin:
    """Mixin for updating table-level metadata.

    ``get_table_schema`` (in :class:`CatalogMixin`) is the canonical reader —
    it returns both the SQL-oriented view and the business-semantic-layer
    fields that this mixin mutates. ``update_table`` maps directly to the
    underlying ``sdk.client.update_table`` call.

    Expects the host class to provide: ``sdk``, ``default_project``.
    """

    def update_table(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a structured patch to an existing table.

        Input shape (all patch groups optional; at least one must be present)::

            {
              "project", "schema", "table",               # identification
              "description": str,                         # "" clears the comment
              "labels":     {"set": {k: v}, "mode": "merge"|"replace"|"delete"},
              "expiration": {"days": int, "partitionDays": int},
              "columns":    {
                  "setComments": {col_or_path: desc},     # dotted path for nested
                  "setNullable": [col, ...],              # REQUIRED → NULLABLE (top-level only)
                  "add":         [{name, type, description?}, ...]
              },
              "etag": str                                 # optional manual override
            }

        Concurrency: always starts with a ``get_table`` to fetch the current
        etag, so normal callers never have to pass one. Supply ``etag`` only
        when you've already held a snapshot and want strict OCC.
        """
        project = opt_arg(args, "project", self.default_project) or self.default_project
        schema = opt_arg(args, "schema", "default") or "default"
        table = require_arg(args, "table", "Table name cannot be empty")

        try:
            plan = self._normalize_patch(args)
        except ValueError as e:
            return mcp_text_result({"success": False, "error": str(e)})

        if not plan:
            return mcp_text_result(
                {
                    "success": False,
                    "error": (
                        "No updatable fields provided. Supply at least one of: "
                        "description, labels, expiration, columns."
                    ),
                }
            )

        try:
            current = self.sdk.client.get_table(
                catalog_models.Table(
                    project_id=project, schema_name=schema, table_name=table
                )
            )
        except Exception as e:
            logger.exception("update_table: get_table failed")
            return mcp_text_result(
                {"success": False, "error": f"Failed to fetch current table state: {e}"}
            )

        try:
            touched = self._apply_plan(current, plan)
        except ValueError as e:
            return mcp_text_result({"success": False, "error": str(e)})

        override_etag = opt_arg(args, "etag")
        if override_etag:
            current.etag = override_etag

        try:
            resp: catalog_models.Table = self.sdk.client.update_table(current)
        except Exception as e:
            logger.exception("update_table: sdk.client.update_table failed")
            return mcp_text_result({"success": False, "error": str(e)})

        payload = serialize_table_meta(resp)
        payload["project"] = project
        payload["schema"] = schema
        payload["table"] = table
        payload["updatedFields"] = touched
        return mcp_text_result(
            {
                "success": True,
                "data": payload,
                "summary": f"Updated {len(touched)} field(s): {', '.join(touched)}",
            }
        )

    # ---- internals ----

    @staticmethod
    def _normalize_patch(args: Dict[str, Any]) -> Dict[str, Any]:
        """Validate input shape and return a normalized plan dict.

        Unknown/empty patch groups are dropped, so ``if not plan`` lets the
        caller return a clean "no changes" error.
        """
        plan: Dict[str, Any] = {}

        if "description" in args:
            desc = args.get("description")
            if desc is None:
                raise ValueError(
                    "description cannot be null; use empty string \"\" to clear, "
                    "or omit the key to skip."
                )
            if not isinstance(desc, str):
                raise ValueError("description must be a string.")
            plan["description"] = desc

        labels = args.get("labels")
        if labels is not None:
            if not isinstance(labels, dict):
                raise ValueError(
                    "labels must be an object {set: {k: v}, mode: 'merge'|'replace'|'delete'}."
                )
            lset = labels.get("set")
            lmode = labels.get("mode", "merge")
            if lset is None or not isinstance(lset, dict):
                raise ValueError("labels.set must be an object of string key/value pairs.")
            if lmode not in _LABELS_MODES:
                raise ValueError(
                    f"labels.mode must be one of {sorted(_LABELS_MODES)}; got {lmode!r}."
                )
            plan["labels"] = {
                "set": {str(k): str(v) for k, v in lset.items()},
                "mode": lmode,
            }

        expiration = args.get("expiration")
        if expiration is not None:
            if not isinstance(expiration, dict):
                raise ValueError("expiration must be an object {days?, partitionDays?}.")
            norm_exp: Dict[str, int] = {}
            for key, dst in (("days", "days"), ("partitionDays", "partitionDays")):
                if key not in expiration:
                    continue
                try:
                    iv = int(expiration[key])
                except (TypeError, ValueError):
                    raise ValueError(f"expiration.{key} must be an integer.")
                if iv < 0:
                    raise ValueError(
                        f"expiration.{key} must be >= 0 (use 0 to disable expiration)."
                    )
                norm_exp[dst] = iv
            if norm_exp:
                plan["expiration"] = norm_exp

        columns = args.get("columns")
        if columns is not None:
            if not isinstance(columns, dict):
                raise ValueError(
                    "columns must be an object {setComments?, setNullable?, add?}."
                )
            norm_cols: Dict[str, Any] = {}

            set_comments = columns.get("setComments")
            if set_comments is not None:
                if not isinstance(set_comments, dict):
                    raise ValueError("columns.setComments must be an object of column → comment.")
                norm_cols["setComments"] = {str(k): str(v or "") for k, v in set_comments.items()}

            set_nullable = columns.get("setNullable")
            if set_nullable is not None:
                if not isinstance(set_nullable, list) or any(
                    not isinstance(x, str) for x in set_nullable
                ):
                    raise ValueError("columns.setNullable must be an array of column name strings.")
                for col in set_nullable:
                    if "." in col:
                        raise ValueError(
                            f"columns.setNullable: nested column {col!r} cannot be changed; "
                            "MaxCompute only allows mode changes on top-level columns."
                        )
                norm_cols["setNullable"] = list(set_nullable)

            add = columns.get("add")
            if add is not None:
                if not isinstance(add, list):
                    raise ValueError("columns.add must be an array of column objects.")
                norm_add = []
                for i, c in enumerate(add):
                    if not isinstance(c, dict):
                        raise ValueError(f"columns.add[{i}] must be an object.")
                    if not c.get("name"):
                        raise ValueError(f"columns.add[{i}] missing 'name'.")
                    if not c.get("type"):
                        raise ValueError(f"columns.add[{i}] missing 'type'.")
                    # mode is implicit NULLABLE; REQUIRED appends are rejected by the service.
                    # Intentionally converts empty description to None so the SDK omits
                    # the field during serialization (unset comment), unlike the top-level
                    # description where "" means "clear the comment".
                    raw_desc = c.get("description")
                    description = raw_desc if raw_desc else None
                    norm_add.append({
                        "name": str(c["name"]),
                        "type": str(c["type"]),
                        "description": description,
                    })
                if norm_add:
                    norm_cols["add"] = norm_add

            if norm_cols:
                plan["columns"] = norm_cols

        return plan

    def _apply_plan(
        self,
        current: catalog_models.Table,
        plan: Dict[str, Any],
    ) -> List[str]:
        """Mutate ``current`` per a normalized plan; return touched field keys."""
        touched: List[str] = []

        if "description" in plan:
            current.description = plan["description"]
            touched.append("description")

        if "labels" in plan:
            new_labels = plan["labels"]["set"]
            mode = plan["labels"]["mode"]
            existing = dict(current.labels or {})
            if mode == "replace":
                current.labels = dict(new_labels)
            elif mode == "delete":
                for k in new_labels.keys():
                    existing.pop(k, None)
                current.labels = existing
            else:  # merge
                existing.update(new_labels)
                current.labels = existing
            touched.append(f"labels({mode})")

        cols_plan = plan.get("columns") or {}
        if cols_plan:
            fields = self._ensure_fields(current)

            # Process `add` first so that `setComments` / `setNullable` can
            # target newly-added columns in the same request.
            if cols_plan.get("add"):
                existing_names = {f.field_name for f in fields if f.field_name}
                for c in cols_plan["add"]:
                    name = c["name"]
                    if name in existing_names:
                        raise ValueError(f"columns.add: column {name!r} already exists.")
                    try:
                        new_field = _parse_sql_type(c["type"])
                    except ValueError as exc:
                        raise ValueError(
                            f"columns.add: invalid type for column {name!r}: {exc}"
                        ) from exc
                    new_field.field_name = name
                    new_field.mode = "NULLABLE"
                    if c.get("description"):
                        new_field.description = c["description"]
                    fields.append(new_field)
                    existing_names.add(name)
                touched.append("columns.add")

            for path, desc in (cols_plan.get("setComments") or {}).items():
                target = _find_field_by_path(fields, path.split("."))
                if target is None:
                    raise ValueError(f"columns.setComments: column path {path!r} not found.")
                target.description = desc
            if cols_plan.get("setComments"):
                touched.append("columns.setComments")

            for col in cols_plan.get("setNullable", []):
                target = _find_field_by_path(fields, [col])
                if target is None:
                    raise ValueError(f"columns.setNullable: column {col!r} not found.")
                target.mode = "NULLABLE"
            if cols_plan.get("setNullable"):
                touched.append("columns.setNullable")

        if "expiration" in plan:
            opts = current.expiration_options or catalog_models.ExpirationOptions()
            if "days" in plan["expiration"]:
                opts.expiration_days = plan["expiration"]["days"]
            if "partitionDays" in plan["expiration"]:
                opts.partition_expiration_days = plan["expiration"]["partitionDays"]
            current.expiration_options = opts
            touched.append("expiration")

        return touched

    @staticmethod
    def _ensure_fields(
        current: catalog_models.Table,
    ) -> List[catalog_models.TableFieldSchema]:
        """Return the table's top-level field list, creating the schema wrapper if missing."""
        if current.table_schema is None:
            current.table_schema = catalog_models.TableFieldSchema()
        if current.table_schema.fields is None:
            current.table_schema.fields = []
        return current.table_schema.fields

