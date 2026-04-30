"""Catalog explorer and metadata search tool handlers.

Provides CatalogMixin with handlers for browsing projects, schemas, tables,
partitions, and searching metadata via the Catalog API.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from pyodps_catalog import models as catalog_models

from .mcp_protocol import mcp_ok_result, mcp_text_result
from .tools_common import _unsupported, opt_arg, opt_int, require_arg
from .tools_table_meta import serialize_table_meta

logger = logging.getLogger(__name__)


class CatalogMixin:
    """Mixin providing catalog explorer and metadata search handlers.

    Expects the host class to provide: sdk, default_project, namespace_id,
    _is_schema_enabled().
    """

    def list_projects(self, args: Dict[str, Any]) -> Dict[str, Any]:
        page_size = opt_int(args, "pageSize", 100)
        token = opt_arg(args, "token")
        resp = self.sdk.client.list_projects(page_size=page_size, page_token=token)
        m = resp.to_map() if hasattr(resp, "to_map") else resp
        items = m.get("projects") or m.get("entries") or []
        summary = f"{len(items)} project(s)" if items else "0 projects"
        return mcp_ok_result(m, summary=summary)

    def get_project(self, args: Dict[str, Any]) -> Dict[str, Any]:
        project = require_arg(args, "project", "Project name cannot be empty")
        resp = self.sdk.client.get_project(project_id=project)
        m = resp.to_map() if hasattr(resp, "to_map") else resp
        return mcp_ok_result(m)

    def list_schemas(self, args: Dict[str, Any]) -> Dict[str, Any]:
        project = opt_arg(args, "project", self.default_project) or self.default_project
        if not self._is_schema_enabled(project):
            logger.info("Project %r is 2-level (schemaEnabled=false); returning synthetic default schema.", project)
            return mcp_ok_result(
                {"schemas": [{"name": "default"}]},
                summary="2-level project, returning synthetic default schema",
            )
        page_size = opt_int(args, "pageSize", 100)
        token = opt_arg(args, "token")
        resp = self.sdk.client.list_schemas(project_id=project, page_size=page_size, page_token=token)
        m = resp.to_map() if hasattr(resp, "to_map") else resp
        items = m.get("schemas") or m.get("entries") or []
        summary = f"{len(items)} schema(s)" if items else "0 schemas"
        return mcp_ok_result(m, summary=summary)

    def get_schema(self, args: Dict[str, Any]) -> Dict[str, Any]:
        project = opt_arg(args, "project", self.default_project) or self.default_project
        schema = opt_arg(args, "schema", "default") or "default"
        if not self._is_schema_enabled(project):
            logger.info("Project %r is 2-level (schemaEnabled=false); returning synthetic schema.", project)
            return mcp_ok_result(
                {"name": schema, "project": project, "description": "2-level project (no schema support)"},
            )
        resp = self.sdk.client.get_schema(project_id=project, schema_name=schema)
        m = resp.to_map() if hasattr(resp, "to_map") else resp
        return mcp_ok_result(m)

    def list_tables(self, args: Dict[str, Any]) -> Dict[str, Any]:
        project = opt_arg(args, "project", self.default_project) or self.default_project
        schema = opt_arg(args, "schema", "default") or "default"
        page_size = opt_int(args, "pageSize", 100)
        token = opt_arg(args, "token")
        table_filter = opt_arg(args, "filter")

        list_kwargs: Dict[str, Any] = {
            "project_id": project,
            "schema_name": schema,
            "page_size": page_size,
            "page_token": token,
        }
        if table_filter:
            list_kwargs["table_name_prefix"] = table_filter
        resp = self.sdk.client.list_tables(**list_kwargs)
        m = resp.to_map() if hasattr(resp, "to_map") else resp
        items = m.get("tables") or m.get("entries") or []
        summary = f"{len(items)} table(s)" if items else "0 tables"

        schema_enabled = self._is_schema_enabled(project)
        naming_model = "3-level" if schema_enabled else "2-level"
        return mcp_ok_result({**m, "namingModel": naming_model}, summary=summary)

    def get_table_schema(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Return column names, types, comments and partition keys (like DESC).

        Handles 2-level projects by omitting schema from the Catalog API call.
        """
        project = opt_arg(args, "project", self.default_project) or self.default_project
        schema = opt_arg(args, "schema", "default") or "default"
        table = require_arg(args, "table", "Table name cannot be empty")

        return self._get_table_via_catalog(project, schema, table)

    def _get_table_via_catalog(self, project: str, schema: str, table: str) -> Dict[str, Any]:
        """Call Catalog API get_table and return a unified schema + metadata view.

        The payload exposes only business-facing fields:
        - SQL authoring: ``columns`` (name/type/comment plus optional ``mode``
          and nested ``fields`` for struct types), ``partitionKeys``,
          ``namingModel``, ``sqlTableRef``, ``sqlExample``.
        - Semantic layer (mutable via ``update_table``): ``description``,
          ``labels``, ``expiration``, ``type``, ``etag``, ``createTime``,
          ``lastModifiedTime``.
        """
        t = catalog_models.Table(project_id=project, schema_name=schema, table_name=table)
        resp = self.sdk.client.get_table(t)

        # Fallback path: SDK returned an unexpected dict-like (older mocks / raw map)
        if not isinstance(resp, catalog_models.Table):
            m = resp.to_map() if hasattr(resp, "to_map") else resp
            return mcp_ok_result(
                {
                    "project": project,
                    "schema": schema,
                    "table": table,
                    "columns": [],
                    "partitionKeys": [],
                    "raw": m,
                }
            )

        meta = serialize_table_meta(resp)

        schema_enabled = self._is_schema_enabled(project)
        naming_model = "3-level" if schema_enabled else "2-level"
        sql_table_ref = f"{schema}.{table}" if schema_enabled else table
        columns = meta.get("columns", [])
        col_names = ", ".join(c["name"] for c in columns[:5] if c.get("name")) or "*"
        sql_example = f"SELECT {col_names} FROM {sql_table_ref} LIMIT 10"

        return mcp_ok_result(
            {
                "project": project,
                "schema": schema,
                "table": table,
                "namingModel": naming_model,
                "sqlTableRef": sql_table_ref,
                "sqlExample": sql_example,
                "etag": meta.get("etag"),
                "type": meta.get("type"),
                "description": meta.get("description", ""),
                "labels": meta.get("labels", {}),
                "expiration": meta.get("expiration", {}),
                "createTime": meta.get("createTime"),
                "lastModifiedTime": meta.get("lastModifiedTime"),
                "partitionKeys": meta.get("partitionKeys", []),
                "columns": columns,
            }
        )

    def get_partition_info(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List partitions for a partitioned table (Catalog API only).

        Handles 2-level projects by omitting schema_name from the API call.
        """
        project = opt_arg(args, "project", self.default_project) or self.default_project
        schema = opt_arg(args, "schema", "default") or "default"
        table = require_arg(args, "table", "Table name cannot be empty")
        page_size = opt_int(args, "pageSize", 100)
        token = opt_arg(args, "token")

        # 2-level projects use 'default' schema; 3-level projects use the provided schema
        schema_arg = schema if self._is_schema_enabled(project) else "default"

        try:
            resp = self.sdk.client.list_partitions(
                project_id=project,
                schema_name=schema_arg,
                table_name=table,
                page_size=page_size,
                page_token=token,
            )
        except AttributeError:
            return _unsupported("Current Catalog SDK does not support list_partitions.")
        except Exception as e:
            logger.exception("get_partition_info failed")
            return mcp_text_result({"success": False, "error": str(e)})

        m = resp.to_map() if hasattr(resp, "to_map") else resp
        return mcp_text_result(m)

    # ---- metadata search ----

    def search_meta_data(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Search metadata by keyword. Uses Catalog API namespaces/:search via SDK Client.search."""
        query = require_arg(args, "query", "Search query cannot be empty")
        page_size = opt_int(args, "pageSize", 100)
        token = opt_arg(args, "token")
        order_by = opt_arg(args, "orderBy")

        catalog_client = getattr(self.sdk, "client", None)
        if catalog_client is None:
            return _unsupported("Metadata search requires Catalog SDK (pyodps_catalog) support, which is not configured.")

        namespace_id = self.namespace_id or ""
        if not namespace_id:
            return _unsupported(
                "search_meta_data requires namespace_id (main account UID). Please set namespaceId in config or the MAXCOMPUTE_NAMESPACE_ID environment variable."
            )

        try:
            resp = catalog_client.search(
                namespace_id=namespace_id,
                query=query,
                page_size=page_size,
                page_token=token or "",
                order_by=order_by or "",
            )
            m = resp.to_map() if hasattr(resp, "to_map") else {
                "entries": getattr(resp, "entries", []),
                "next_page_token": getattr(resp, "next_page_token", None),
            }
            return mcp_text_result(m)
        except Exception as e:
            logger.exception("search_meta_data failed")
            return mcp_text_result({"success": False, "error": str(e)})
