"""Table creation and data insertion tool handlers.

Provides DesignerMixin with handlers for creating tables and inserting values.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List

from .mcp_protocol import mcp_text_result
from .tools_common import (
    _build_timeout_response,
    _env,
    _escape_identifier,
    _quote_partition_literal,
    _quote_sql_value,
    _unsupported,
    opt_arg,
    opt_int,
    parse_bool,
    parse_timeout,
    require_arg,
)
from .tools_compute import _run_dml

logger = logging.getLogger(__name__)


def _get_max_partition_batches() -> int:
    """Read MAX_PARTITION_BATCHES from environment with fallback to 100 on invalid input."""
    val = _env("MAX_PARTITION_BATCHES", "100")
    try:
        parsed = int(val)
        if parsed < 1:
            raise ValueError("must be >= 1")
        return parsed
    except ValueError:
        logger.warning(
            "Invalid MAX_PARTITION_BATCHES=%r, using default 100", val
        )
        return 100


# Maximum number of distinct partition batches allowed in a single insert_values call.
# Can be overridden via the MAX_PARTITION_BATCHES environment variable.
_MAX_PARTITION_BATCHES = _get_max_partition_batches()


class DesignerMixin:
    """Mixin providing table design handlers (create_table, insert_values).

    Expects the host class to provide: maxcompute_client, default_project,
    _get_compute_client_for_project().
    """

    def create_table(self, args: Dict[str, Any]) -> Dict[str, Any]:
        project = opt_arg(args, "project", self.default_project) or self.default_project
        schema = opt_arg(args, "schema", "default") or "default"
        table = require_arg(args, "table", "Table name cannot be empty")
        columns = args.get("columns") or []
        partition_columns = args.get("partitionColumns") or []
        lifecycle = opt_int(args, "lifecycle", 0)
        comment = opt_arg(args, "description")
        storage_tier = opt_arg(args, "storageTier")
        primary_key = args.get("primaryKey")
        # Normalize `primaryKey: []` to None so it's treated as "unset" (pyodps
        # would otherwise emit invalid DDL). Kept as an explicit check rather
        # than `or None` for consistency with tableProperties/hints, which must
        # NOT coerce empty dicts this way.
        if primary_key == []:
            primary_key = None
        table_properties = args.get("tableProperties")
        hints = args.get("hints")

        try:
            if_not_exists = parse_bool(args, "ifNotExists", False)
            transactional = parse_bool(args, "transactional", False)
        except TypeError as e:
            return mcp_text_result({"success": False, "error": str(e)})

        if table_properties is not None and not isinstance(table_properties, dict):
            return mcp_text_result({"success": False, "error": "tableProperties must be an object of string key/value pairs."})
        if hints is not None and not isinstance(hints, dict):
            return mcp_text_result({"success": False, "error": "hints must be an object of string key/value pairs."})
        if primary_key is not None and not isinstance(primary_key, list):
            return mcp_text_result({"success": False, "error": "primaryKey must be an array of column names."})

        if not self.maxcompute_client:
            return _unsupported(
                "Creating tables requires MaxCompute compute engine (default_project + sdk_endpoint)."
            )
        if "columns" not in args:
            return mcp_text_result({"success": False, "error": "columns is required."})
        if not columns:
            return mcp_text_result({"success": False, "error": "columns must not be empty."})
        try:
            from odps.models import TableSchema
            from odps.types import Column as OdpsColumn
            compute = self.maxcompute_client
            table_name = f"{schema}.{table}" if schema and schema != "default" else table

            def _build_column(c: Any, *, is_partition: bool) -> OdpsColumn:
                if isinstance(c, dict):
                    name = c.get("name") or c.get("columnName") or ""
                    col_type = c.get("type") or c.get("dataType") or "STRING"
                    col_comment = c.get("description")
                    not_null = bool(c.get("notNull", False))
                    gen_expr = c.get("generateExpression")
                else:
                    name = str(c)
                    col_type = "STRING"
                    col_comment = None
                    not_null = False
                    gen_expr = None
                if not name:
                    field = "partitionColumns" if is_partition else "columns"
                    raise ValueError(f"{field} entry missing 'name'")
                kwargs: Dict[str, Any] = {"name": name, "type": col_type, "comment": col_comment}
                if not is_partition:
                    # nullable is meaningful only on data columns; partition columns
                    # don't carry a NOT NULL constraint in MaxCompute.
                    # notNull=True ↔ nullable=False (and vice versa).
                    kwargs["nullable"] = not not_null
                if gen_expr:
                    # generate_expression drives AUTO PARTITIONED BY on partition columns
                    # (TRUNC_TIME(...) etc.); also supported on data columns for generated
                    # columns where applicable.
                    kwargs["generate_expression"] = gen_expr
                return OdpsColumn(**kwargs)

            col_objs = [_build_column(c, is_partition=False) for c in columns]
            part_objs = [_build_column(pc, is_partition=True) for pc in partition_columns]
            table_schema = TableSchema(columns=col_objs, partitions=part_objs or None)

            create_kwargs: Dict[str, Any] = {
                "project": project or None,
                "comment": comment,
                "lifecycle": lifecycle or None,
                "if_not_exists": if_not_exists,
                "transactional": transactional,
            }
            if primary_key:
                create_kwargs["primary_key"] = primary_key
            if storage_tier:
                create_kwargs["storage_tier"] = storage_tier
            if table_properties:
                create_kwargs["table_properties"] = {
                    str(k): str(v) for k, v in table_properties.items()
                }
            if hints:
                create_kwargs["hints"] = {str(k): str(v) for k, v in hints.items()}

            compute.create_table(table_name, table_schema, **create_kwargs)
            return mcp_text_result({
                "success": True,
                "project": project,
                "schema": schema,
                "table": table,
                "message": "Table created via MaxCompute.",
            })
        except Exception as e:
            logger.exception("create_table failed")
            return mcp_text_result({"success": False, "error": str(e)})

    def insert_values(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if not self.maxcompute_client:
            return _unsupported("INSERT INTO ... VALUES requires MaxCompute compute engine (default_project + sdk_endpoint).")
        project = opt_arg(args, "project", self.default_project) or self.default_project
        schema = opt_arg(args, "schema", "default") or "default"
        table = require_arg(args, "table", "Table name cannot be empty")
        columns = args.get("columns") or []
        values = args.get("values") or []
        partition_cols = args.get("partitionColumns") or []
        try:
            async_mode = parse_bool(args, "async", False)
        except TypeError as e:
            return mcp_text_result({"success": False, "error": str(e)})
        timeout_secs = 60  # only parsed and used in sync mode
        if not async_mode:
            try:
                timeout_secs = parse_timeout(args, 60)
            except ValueError as e:
                return mcp_text_result({"success": False, "error": str(e)})
        if not columns or not values:
            return mcp_text_result({"success": False, "error": "columns and values cannot be empty"})
        try:
            compute = self._get_compute_client_for_project(project)
            if compute is None:
                return mcp_text_result({"success": False, "error": "Failed to create compute client; check configuration."})
            # Escape identifiers with backticks to prevent SQL injection
            escaped_schema = _escape_identifier(schema)
            escaped_table = _escape_identifier(table)
            full_name = f"{escaped_schema}.{escaped_table}" if schema and schema != "default" else escaped_table

            def _col_name(c: Any) -> str:
                return (c if isinstance(c, str) else c.get("name", c.get("columnName", ""))) or ""

            column_names = [_col_name(c) for c in columns]
            if len(column_names) != len(columns) or any(not n for n in column_names):
                return mcp_text_result({"success": False, "error": "Empty column name found in columns"})

            hints = {"odps.sql.type.system.odps2": "true"}

            if partition_cols:
                return self._insert_values_partitioned(
                    compute, full_name, column_names, partition_cols, values, hints,
                    project, schema, table, timeout_secs, async_mode
                )

            col_list = ", ".join(_escape_identifier(n) for n in column_names)
            rows_sql = []
            for row in values:
                row_vals = [_quote_sql_value(row[i]) if i < len(row) else "NULL" for i in range(len(column_names))]
                rows_sql.append("(" + ", ".join(row_vals) + ")")
            values_sql = ", ".join(rows_sql)
            sql = f"INSERT INTO {full_name} ({col_list}) VALUES {values_sql};"

            if async_mode:
                inst = compute.run_sql(sql, hints=hints)
                return mcp_text_result({
                    "success": True,
                    "instanceId": inst.id,
                    "project": project,
                    "status": "submitted",
                    "message": (
                        "Insert submitted. Use get_instance_status or get_instance "
                        "with instanceId to poll for results."
                    ),
                })

            inst, timed_out = _run_dml(compute, sql, hints, timeout_secs)
            if timed_out:
                return _build_timeout_response(inst, project, timeout_secs, "Insert")
            return mcp_text_result({
                "success": True,
                "project": project,
                "schema": schema,
                "table": table,
                "rowsInserted": len(values),
            })
        except Exception as e:
            logger.exception("insert_values failed")
            return mcp_text_result({"success": False, "error": str(e)})

    def _insert_values_partitioned(
        self,
        compute: Any,
        full_name: str,
        column_names: List[str],
        partition_cols: List[str],
        values: List[List[Any]],
        hints: Dict[str, str],
        project: str,
        schema: str,
        table: str,
        timeout_secs: int,
        async_mode: bool,
    ) -> Dict[str, Any]:
        """Insert values into a partitioned table, grouping by partition key.

        Args:
            compute: MaxCompute client for SQL execution
            full_name: Fully qualified table name (escaped)
            column_names: List of all column names
            partition_cols: List of partition column names
            values: 2D array of row values
            hints: SQL hints dict
            project, schema, table: For result reporting

        Returns:
            MCP result dict with success status and row counts
        """
        pc_set = set(partition_cols)
        for pc in partition_cols:
            if pc not in column_names:
                return mcp_text_result(
                    {"success": False, "error": f"partitionColumns '{pc}' not found in columns"}
                )
        data_col_names = [n for n in column_names if n not in pc_set]
        if not data_col_names:
            return mcp_text_result({"success": False, "error": "At least one data column required besides partition columns"})

        # Build index map for O(1) lookups instead of O(n) index() calls
        col_idx = {name: i for i, name in enumerate(column_names)}
        pc_indices = [col_idx[pc] for pc in partition_cols]
        dc_indices = [col_idx[dc] for dc in data_col_names]

        # Group rows by partition key
        groups: Dict[tuple, list] = defaultdict(list)
        for row in values:
            if len(row) < len(column_names):
                return mcp_text_result(
                    {"success": False, "error": f"Row length {len(row)} does not match columns count {len(column_names)}"}
                )
            pk = tuple(row[idx] for idx in pc_indices)
            groups[pk].append(row)

        if len(groups) > _MAX_PARTITION_BATCHES:
            return mcp_text_result({
                "success": False,
                "error": (
                    f"Too many partition batches: {len(groups)} exceeds the limit of {_MAX_PARTITION_BATCHES}. "
                    "Split the request into smaller batches."
                ),
            })

        total_inserted = 0
        batches: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        for pk, rows in groups.items():
            part_clause = ", ".join(
                f"{_escape_identifier(partition_cols[j])}={_quote_partition_literal(pk[j])}"
                for j in range(len(partition_cols))
            )
            col_list = ", ".join(_escape_identifier(n) for n in data_col_names)
            rows_sql = []
            for row in rows:
                row_vals = [
                    _quote_sql_value(row[idx]) if idx < len(row) else "NULL"
                    for idx in dc_indices
                ]
                rows_sql.append("(" + ", ".join(row_vals) + ")")
            values_sql = ", ".join(rows_sql)
            sql = f"INSERT INTO {full_name} PARTITION ({part_clause}) ({col_list}) VALUES {values_sql};"

            if async_mode:
                try:
                    inst = compute.run_sql(sql, hints=hints)
                    batches.append({"partitionKey": list(pk), "instanceId": inst.id})
                    total_inserted += len(rows)
                except Exception as e:
                    errors.append({"partitionKey": list(pk), "error": str(e)})
                    logger.error("Failed to submit partition batch %s: %s", list(pk), e)
                continue

            inst, timed_out = _run_dml(compute, sql, hints, timeout_secs)
            if timed_out:
                return _build_timeout_response(inst, project, timeout_secs, "Insert", " on partition batch")
            total_inserted += len(rows)

        if async_mode:
            if errors:
                return mcp_text_result({
                    "success": False,
                    "batches": batches,
                    "errors": errors,
                    "project": project,
                    "message": (
                        f"{len(errors)} partition batch(es) failed to submit. "
                        "Successfully submitted batches can still be tracked via instanceId."
                    ),
                })
            return mcp_text_result({
                "success": True,
                "batches": batches,
                "project": project,
                "status": "submitted",
                "message": (
                    "All partition batches submitted. Use get_instance_status or get_instance "
                    "with each instanceId to poll for results."
                ),
            })

        return mcp_text_result({
            "success": True,
            "project": project,
            "schema": schema,
            "table": table,
            "rowsInserted": total_inserted,
            "partitionBatches": len(groups),
        })
