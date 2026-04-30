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
        for pc in t.partition_definition.partitioned_column or []:
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

            if cols_plan.get("add"):
                existing_names = {f.field_name for f in fields if f.field_name}
                for c in cols_plan["add"]:
                    name = c["name"]
                    if name in existing_names:
                        raise ValueError(f"columns.add: column {name!r} already exists.")
                    new_field = catalog_models.TableFieldSchema(
                        field_name=name,
                        sql_type_definition=c["type"],
                        mode="NULLABLE",
                        description=c.get("description"),
                    )
                    fields.append(new_field)
                    existing_names.add(name)
                touched.append("columns.add")

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

