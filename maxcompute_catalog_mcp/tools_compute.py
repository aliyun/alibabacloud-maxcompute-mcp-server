"""SQL execution and instance management tool handlers.

Provides ComputeMixin with handlers for SQL cost estimation, execution,
and instance status/result retrieval.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional
from urllib.parse import urlparse

try:
    from odps.errors import WaitTimeoutError as _OdpsWaitTimeoutError
except ImportError:
    _OdpsWaitTimeoutError = None  # type: ignore[assignment,misc]
    logging.getLogger(__name__).warning(
        "odps.errors.WaitTimeoutError not available; SQL timeout detection disabled"
    )

from .mcp_protocol import mcp_text_result
from .tools_common import (
    _build_timeout_response,
    _is_read_only_sql,
    _unsupported,
    opt_arg,
    parse_bool,
    parse_timeout,
    require_arg,
)

logger = logging.getLogger(__name__)


def _max_inline_rows() -> int:
    """Upper bound on rows kept in-memory when no output_uri is given."""
    raw = os.environ.get("MAXC_RESULT_ROW_CAP", "1000").strip()
    try:
        val = int(raw)
        return val if val > 0 else 1000
    except ValueError:
        return 1000


_PREVIEW_ROWS = 20


class _ReadResult(NamedTuple):
    rows: List[Dict[str, Any]]
    total: int
    truncated: bool
    bytes_written: Optional[int]


def _serialize_record(record: Any, cols: List[str]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    for col in cols:
        val = record[col]
        row[col] = val if isinstance(val, (int, float, bool, type(None))) else str(val)
    return row


def _resolve_output_uri(uri: str, *, create_dir: bool = True) -> Path:
    """Validate *uri* and return absolute Path.

    When ``create_dir=True`` (default): ensure parent dir exists.
    When ``create_dir=False``: pure validation only, no filesystem side effects —
    use this when only format/scheme checking is needed (e.g. validating input
    early in an async submit path where the file will be written in a later call).

    Only file:// and bare paths are accepted. Raises ValueError on unsupported
    schemes, empty paths, or paths targeting sensitive system directories.
    """
    if not uri or not uri.strip():
        raise ValueError("output_uri must not be empty")
    parsed = urlparse(uri)
    if parsed.scheme and parsed.scheme != "file":
        raise ValueError(
            f"Unsupported output_uri scheme {parsed.scheme!r}; only 'file://' is supported."
        )
    raw_path = parsed.path if parsed.scheme == "file" else uri
    if not raw_path.strip():
        raise ValueError("output_uri path is empty")
    path = Path(raw_path).expanduser().resolve()

    # Defense-in-depth: reject paths under sensitive system directories to prevent
    # an LLM agent from being tricked into overwriting critical system files.
    _DENIED_PREFIXES = ("/etc", "/bin", "/sbin", "/usr/bin", "/usr/sbin",
                        "/boot", "/proc", "/sys", "/dev")
    path_str = str(path)
    for prefix in _DENIED_PREFIXES:
        if path_str == prefix or path_str.startswith(prefix + "/"):
            raise ValueError(
                f"output_uri resolves to a restricted system path: {path_str!r}"
            )

    if create_dir:
        path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _decorate_output_path(
    base: Path, instance_id: str, task_name: Optional[str] = None
) -> Path:
    """Insert instanceId (and optional task_name) into the filename stem.

    Ensures that (a) repeated calls with the same output_uri but different
    instances never overwrite each other and (b) multi-task results within one
    instance stay distinguishable.
    """
    parts = [base.stem, instance_id]
    if task_name:
        parts.append(task_name)
    return base.with_name(".".join(parts) + base.suffix)


def _read_rows(
    reader: Any,
    cols: List[str],
    *,
    output_path: Optional[Path],
) -> _ReadResult:
    """Consume *reader* once.

    When *output_path* is given: stream every row to JSONL. Writes go to
    ``<output_path>.partial`` first and are atomically renamed to
    ``output_path`` on success; on exception the partial file is removed so
    the caller never sees a misleadingly-complete file at ``output_path``.
    Returns the first PREVIEW_ROWS as an in-response preview; ``truncated`` is
    always False (file has the full dataset).

    When *output_path* is None: keep up to ``_max_inline_rows()`` rows in
    memory; the remainder is counted but discarded. ``truncated`` is True iff
    more rows existed beyond the cap.
    """
    max_inline = _max_inline_rows()
    rows_kept: List[Dict[str, Any]] = []
    total = 0
    truncated = False

    if output_path is None:
        for record in reader:
            total += 1
            row = _serialize_record(record, cols)
            if len(rows_kept) < max_inline:
                rows_kept.append(row)
            else:
                truncated = True
        return _ReadResult(rows_kept, total, truncated, None)

    # Streaming mode: write to .partial, atomically rename on success so the
    # final path is all-or-nothing. Errors leave no trace at output_path.
    tmp_path = output_path.with_suffix(output_path.suffix + ".partial")
    bytes_written = 0
    completed = False
    fh = tmp_path.open("w", encoding="utf-8")
    try:
        for record in reader:
            total += 1
            row = _serialize_record(record, cols)
            line = json.dumps(row, ensure_ascii=False) + "\n"
            fh.write(line)
            bytes_written += len(line.encode("utf-8"))
            if len(rows_kept) < _PREVIEW_ROWS:
                rows_kept.append(row)
        completed = True
    finally:
        fh.close()
        if not completed:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError as unlink_exc:
                logger.warning("Failed to remove partial file %s: %s", tmp_path, unlink_exc)
    # os.replace semantics: atomic on POSIX, replaces dest if it exists
    tmp_path.replace(output_path)
    return _ReadResult(rows_kept, total, truncated, bytes_written)


def _run_dml(compute: Any, sql: str, hints: Dict[str, str], timeout_secs: int):
    """Submit a DML SQL statement and wait for completion with timeout.

    Returns (inst, timed_out: bool). Raises on non-timeout errors.
    """
    inst = compute.run_sql(sql, hints=hints)
    try:
        inst.wait_for_success(timeout=timeout_secs)
        return inst, False
    except Exception as exc:
        # When WaitTimeoutError is unavailable (old SDK), we cannot detect timeouts,
        # so all exceptions are re-raised as-is. When available, only timeout errors
        # return (inst, True); all other exceptions are still re-raised.
        if _OdpsWaitTimeoutError is not None and isinstance(exc, _OdpsWaitTimeoutError):
            return inst, True
        raise


class ComputeMixin:
    """Mixin providing SQL execution and instance management handlers.

    Expects the host class to provide: maxcompute_client, default_project,
    _get_compute_client_for_project().
    """

    @staticmethod
    def _get_instance_logview(inst: Any) -> Optional[str]:
        """Return instance log view URL via pyodps Instance.get_logview_address()."""
        try:
            return inst.get_logview_address()
        except Exception as e:
            logger.debug("Failed to get logview address: %s", e)
            return None

    def _estimate_sql_cost(
        self,
        project: str,
        sql: str,
        hints: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Estimate SQL cost before execution (MaxCompute compute client only).

        hints: runtime parameters forwarded to pyodps execute_sql_cost. Required
        for 3-level (schema-enabled) projects to resolve schema.table references
        (e.g. odps.namespace.schema=true); otherwise the underlying ODPS call
        raises ODPS-0130131 Table not found and the stub fallback kicks in.
        """
        if self.maxcompute_client:
            try:
                compute = self._get_compute_client_for_project(project)
                if compute is None:
                    return {
                        "estimatedCU": 0,
                        "inputBytes": 0,
                        "complexity": 0.0,
                        "udfCount": 0,
                        "stub": True,
                        "message": "Failed to create compute client; check configuration.",
                    }
                cost = compute.execute_sql_cost(sql, hints=hints)
                input_size = getattr(cost, "input_size", 0) or 0
                complexity = getattr(cost, "complexity", 0.0) or 0.0
                udf_num = getattr(cost, "udf_num", 0) or 0
                return {
                    "estimatedCU": int(complexity * (input_size / (1024 ** 3)) * 10) or 1,
                    "inputBytes": input_size,
                    "complexity": complexity,
                    "udfCount": udf_num,
                }
            except Exception as e:
                logger.debug("SQL cost estimation failed (non-fatal): %s", e)
                return {
                    "estimatedCU": 0,
                    "inputBytes": 0,
                    "complexity": 0.0,
                    "udfCount": 0,
                    "stub": True,
                    "message": f"Cost estimation failed: {e}",
                }
        return {
            "estimatedCU": 0,
            "inputBytes": 0,
            "complexity": 0.0,
            "udfCount": 0,
            "stub": True,
            "message": "Configure MaxCompute compute engine (default_project + sdk_endpoint) to enable cost estimation.",
        }

    def cost_sql(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Estimate SQL cost without executing the SQL."""
        project = opt_arg(args, "project", self.default_project) or self.default_project
        sql = require_arg(args, "sql", "SQL statement cannot be empty")
        extra_hints = args.get("hints")
        hints = extra_hints if isinstance(extra_hints, dict) else None
        estimate = self._estimate_sql_cost(project, sql, hints=hints)
        result: Dict[str, Any] = {"project": project, "sql": sql[:200], "costEstimate": estimate}
        if len(sql) > 200:
            result["sqlTruncated"] = True
        return mcp_text_result(result)

    def execute_sql(self, args: Dict[str, Any]) -> Dict[str, Any]:
        project = opt_arg(args, "project", self.default_project) or self.default_project
        sql = require_arg(args, "sql", "SQL statement cannot be empty")
        max_cu = args.get("maxCU")
        if max_cu is not None:
            try:
                max_cu = int(max_cu)
            except (TypeError, ValueError):
                max_cu = None

        if max_cu is not None and max_cu >= 0:
            extra_hints = args.get("hints")
            estimate_hints = extra_hints if isinstance(extra_hints, dict) else None
            estimate = self._estimate_sql_cost(project, sql, hints=estimate_hints)
            estimated_cu = estimate.get("estimatedCU") or 0
            if estimated_cu > max_cu:
                suggested = max(estimated_cu, int(estimated_cu * 1.2))
                return mcp_text_result({
                    "success": False,
                    "overLimit": True,
                    "message": "Estimated resource usage exceeds the limit; increase maxCU and retry.",
                    "estimatedCU": estimated_cu,
                    "maxCU": max_cu,
                    "suggestedMaxCU": suggested,
                    "costEstimate": estimate,
                })

        if self.maxcompute_client:
            is_safe, err_msg = _is_read_only_sql(sql)
            if not is_safe:
                return mcp_text_result({
                    "success": False,
                    "error": err_msg or "Only SELECT queries are allowed.",
                })
            try:
                compute = self._get_compute_client_for_project(project)
                if compute is None:
                    return mcp_text_result({
                        "success": False,
                        "error": "Failed to create compute client; check configuration.",
                    })
                hints = {"odps.sql.submit.mode": "script"}
                extra_hints = args.get("hints")
                if isinstance(extra_hints, dict):
                    hints = {**hints, **extra_hints}
                # Server-side read-only enforcement: applied AFTER user hints so
                # callers cannot override it.  Even if the client-side keyword
                # guard (_is_read_only_sql) is bypassed, the MC server will
                # reject any DML/DDL when this flag is set.
                hints["odps.sql.read.only"] = "true"

                try:
                    async_mode = parse_bool(args, "async", True)
                except TypeError as e:
                    return mcp_text_result({"success": False, "error": str(e)})

                # Validate output_uri early to avoid submitting an orphan job on bad input.
                # Async mode does format/scheme validation only — the file is written by a
                # later get_instance call, so mkdir here would be an unnecessary side effect
                # (and contradict the ToolSpec note that output_uri only takes effect in sync mode).
                output_path: Optional[Path] = None
                output_uri_raw = args.get("output_uri")
                if output_uri_raw:
                    try:
                        output_path = _resolve_output_uri(
                            str(output_uri_raw), create_dir=not async_mode,
                        )
                    except ValueError as e:
                        return mcp_text_result({"success": False, "error": str(e)})

                # Parse timeout before submitting to avoid orphaned jobs on invalid input
                timeout_secs = 30
                if not async_mode:
                    try:
                        timeout_secs = parse_timeout(args, 30)
                    except ValueError as e:
                        return mcp_text_result({"success": False, "error": str(e)})

                inst = compute.run_sql(sql, hints=hints)

                # Async mode (default): return instanceId immediately without waiting
                if async_mode:
                    resp: Dict[str, Any] = {
                        "success": True,
                        "instanceId": inst.id,
                        "project": project,
                        "status": "submitted",
                        "message": (
                            "Query submitted. Use get_instance_status or get_instance "
                            "with instanceId to poll for results."
                        ),
                    }
                    if output_path is not None:
                        resp["outputUriHint"] = (
                            "output_uri was supplied at submit time but is only honored "
                            "by get_instance; pass it again when you fetch the result."
                        )
                    return mcp_text_result(resp)

                # Sync mode: wait for completion with timeout
                try:
                    inst.wait_for_success(timeout=timeout_secs)
                except Exception as timeout_exc:
                    # Same fallback pattern as _run_dml: re-raise non-timeout errors
                    # unconditionally; timeout errors are only caught when the SDK
                    # exposes WaitTimeoutError.
                    if _OdpsWaitTimeoutError is not None and isinstance(timeout_exc, _OdpsWaitTimeoutError):
                        return _build_timeout_response(inst, project, timeout_secs, "Query")
                    raise

                # Try structured reader (SELECT / WITH queries). Only open_reader()
                # failure or a schema-less reader falls back to raw-output; exceptions
                # from _read_rows must propagate so the caller sees the real error
                # (e.g. mid-stream network failure) instead of a misleading empty result.
                structured_committed = False
                structured_resp: Optional[Dict[str, Any]] = None
                try:
                    with inst.open_reader() as reader:
                        schema = getattr(reader, "_schema", None)
                        if schema is not None and getattr(schema, "columns", None):
                            columns = [col.name for col in schema.columns]
                            structured_committed = True
                            final_output_path = (
                                _decorate_output_path(output_path, inst.id)
                                if output_path is not None
                                else None
                            )
                            rows, total, truncated, bytes_written = _read_rows(
                                reader, columns, output_path=final_output_path,
                            )
                            structured_resp = {
                                "success": True,
                                "instanceId": inst.id,
                                "columns": columns,
                                "rowCount": total,
                                "truncated": truncated,
                            }
                            if final_output_path is not None:
                                structured_resp["outputPath"] = str(final_output_path)
                                structured_resp["bytesWritten"] = bytes_written
                                structured_resp["preview"] = rows
                                structured_resp["previewRows"] = len(rows)
                            else:
                                structured_resp["data"] = rows
                                structured_resp["rowsReturned"] = len(rows)
                                if truncated:
                                    structured_resp["message"] = (
                                        f"Result truncated to {len(rows)} rows (of {total} total). "
                                        "Pass output_uri='file:///path.jsonl' to stream the full result to disk."
                                    )
                except Exception as reader_exc:
                    if structured_committed:
                        # We were past schema check; this is a real error, not a fallback signal
                        raise
                    logger.debug(
                        "open_reader() unavailable (non-SELECT or schema-less result); "
                        "falling back to raw task output. Reason: %s", reader_exc,
                    )

                if structured_resp is not None:
                    return mcp_text_result(structured_resp)

                # Fallback for SHOW / DESC / EXPLAIN: read raw task results
                task_results = inst.get_task_results()
                raw_output = "\n".join(str(v) for v in task_results.values())
                lines = [line for line in raw_output.strip().split("\n") if line.strip()]
                return mcp_text_result({
                    "success": True,
                    "instanceId": inst.id,
                    "data": lines,
                    "rawOutput": raw_output,
                    "rowCount": len(lines),
                })
            except Exception as e:
                logger.exception("execute_sql failed")
                return mcp_text_result({"success": False, "error": str(e)})

        return _unsupported("SQL execution requires MaxCompute compute engine (default_project + sdk_endpoint).")

    def get_instance_status(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if not self.maxcompute_client:
            return _unsupported("Querying instance status requires MaxCompute compute engine (default_project + sdk_endpoint).")
        project = opt_arg(args, "project", self.default_project) or self.default_project
        instance_id = require_arg(args, "instanceId", "instanceId cannot be empty")
        try:
            inst = self.maxcompute_client.get_instance(instance_id, project=project or None)
            status = getattr(inst, "status", None)
            if status is not None and hasattr(status, "name"):
                status = status.name
            return mcp_text_result({
                "instanceId": instance_id,
                "project": project,
                "status": str(status) if status else None,
                "isTerminated": getattr(inst, "is_terminated", lambda: False)(),
                "isSuccessful": getattr(inst, "is_successful", lambda: False)(),
                "logView": self._get_instance_logview(inst),
            })
        except Exception as e:
            logger.exception("get_instance_status failed for %r", instance_id)
            return mcp_text_result({"success": False, "error": str(e)})

    def get_instance(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if not self.maxcompute_client:
            return _unsupported("Retrieving instance results requires MaxCompute compute engine (default_project + sdk_endpoint).")
        project = opt_arg(args, "project", self.default_project) or self.default_project
        instance_id = require_arg(args, "instanceId", "instanceId cannot be empty")

        # Validate output_uri before touching ODPS to avoid wasted round-trips on bad input
        output_path: Optional[Path] = None
        output_uri_raw = args.get("output_uri")
        if output_uri_raw:
            try:
                output_path = _resolve_output_uri(str(output_uri_raw))
            except ValueError as e:
                return mcp_text_result({"success": False, "error": str(e)})

        try:
            inst = self.maxcompute_client.get_instance(instance_id, project=project or None)
            if not getattr(inst, "is_terminated", lambda: False)():
                return mcp_text_result({
                    "instanceId": instance_id,
                    "message": "Instance not terminated yet; wait and retry or use get_instance_status.",
                })
            results = getattr(inst, "get_task_results", lambda: {})()
            if not results:
                return mcp_text_result({"instanceId": instance_id, "results": {}, "message": "No task results."})

            # Decorate filenames with instanceId (always) and task_name (only when multi-task)
            # so repeated calls never overwrite each other.
            multi_task = len(results) > 1
            out: Dict[str, Any] = {}
            for task_name, task_result in results.items():
                try:
                    if hasattr(task_result, "open_reader"):
                        with task_result.open_reader() as reader:
                            # Same dual guard as execute_sql: reject both missing
                            # _schema and empty columns, otherwise _read_rows would
                            # produce rowCount=N with data=[{}, ...] — misleading the
                            # agent into thinking there are rows when every row is empty.
                            schema = getattr(reader, "_schema", None)
                            if schema is None or not getattr(schema, "columns", None):
                                out[task_name] = {
                                    "error": "Task result has no column schema; "
                                             "cannot decode rows. This usually means the task "
                                             "is not a SELECT/WITH query."
                                }
                                continue
                            cols = [c.name for c in schema.columns]
                            task_output_path: Optional[Path] = None
                            if output_path is not None:
                                task_output_path = _decorate_output_path(
                                    output_path,
                                    instance_id,
                                    task_name if multi_task else None,
                                )
                            rows, total, truncated, bytes_written = _read_rows(
                                reader, cols, output_path=task_output_path,
                            )
                            task_entry: Dict[str, Any] = {
                                "columns": cols,
                                "rowCount": total,
                                "truncated": truncated,
                            }
                            if task_output_path is not None:
                                task_entry["outputPath"] = str(task_output_path)
                                task_entry["bytesWritten"] = bytes_written
                                task_entry["preview"] = rows
                                task_entry["previewRows"] = len(rows)
                            else:
                                task_entry["data"] = rows
                                task_entry["rowsReturned"] = len(rows)
                                if truncated:
                                    task_entry["message"] = (
                                        f"Result truncated to {len(rows)} rows (of {total} total). "
                                        "Pass output_uri='file:///path.jsonl' to stream the full result to disk."
                                    )
                            out[task_name] = task_entry
                    else:
                        out[task_name] = str(task_result)
                except Exception as e:
                    out[task_name] = {"error": str(e)}
            return mcp_text_result({"instanceId": instance_id, "results": out})
        except Exception as e:
            logger.exception("get_instance failed for %r", instance_id)
            return mcp_text_result({"success": False, "error": str(e)})
