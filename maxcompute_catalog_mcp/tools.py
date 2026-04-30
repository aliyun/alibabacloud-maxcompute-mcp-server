"""MCP tools entry point — Tools class with spec definitions and call dispatcher.

Handler implementations are split across mixin modules:
- tools_catalog.py  — catalog explorer and metadata search
- tools_compute.py  — SQL execution and instance management
- tools_security.py — identity and permission checks
- tools_designer.py — table creation and data insertion

Shared utilities live in tools_common.py.
"""
from __future__ import annotations

import logging
from collections import OrderedDict
from threading import Lock
from typing import Any, Dict, Optional, Protocol, runtime_checkable

from odps import ODPS

from .maxcompute_client import MaxComputeCatalogSdk, MaxComputeClient, _FULL_USER_AGENT
from .mcp_protocol import JsonRpcError
from .tools_catalog import CatalogMixin
from .tools_common import ToolSpec, input_schema, int_prop, string_prop
from .tools_compute import ComputeMixin
from .tools_designer import DesignerMixin
from .tools_security import SecurityMixin
from .tools_table_meta import TableMetaMixin

# NOTE: _FULL_USER_AGENT is explicitly set on ODPS clients. The pyodps SDK appends
# its own version info, so this acts as a prefix rather than a full override.

logger = logging.getLogger(__name__)


@runtime_checkable
class CredentialClient(Protocol):
    """Protocol for credential clients (alibabacloud-credentials compatible)."""

    def get_credential(self) -> Any: ...


class Tools(CatalogMixin, ComputeMixin, SecurityMixin, DesignerMixin, TableMetaMixin):
    """MCP tools implementation.

    - MCP payload: {content:[{type:"text", text:"<json>"}]}; JSON convention for model extraction:
      - Success: success=true, data=<actual data>, optional summary=<one-line summary>
      - Failure: success=false, error=<error message>
      - List tools (list_*) data contains entries/schemas/tables etc. plus next_page_token; summary is a count/overview
    - sdk: Catalog API client (pyodps_catalog)
    - maxcompute_client: optional MaxCompute compute client (pyodps) for SQL/partitions/instances/create_table
    """

    def __init__(
        self,
        *,
        sdk: MaxComputeCatalogSdk,
        default_project: str = "",
        namespace_id: str = "",
        maxcompute_client: Optional[MaxComputeClient] = None,
        credential_client: Optional[CredentialClient] = None,
    ):
        self.sdk = sdk
        self.default_project = default_project or ""
        self.namespace_id = namespace_id or ""
        self.maxcompute_client = maxcompute_client
        self._credential_client = credential_client
        # Cache of per-project schemaEnabled flag (True = 3-level, False = 2-level).
        # Populated lazily on first schema-related call via get_project.
        self._schema_enabled_cache: dict[str, bool] = {}
        # Cache of per-project compute clients for cross-project SQL execution.
        # Using OrderedDict with max size to prevent unbounded growth.
        self._compute_client_cache: OrderedDict[str, MaxComputeClient] = OrderedDict()
        self._max_compute_client_cache_size = 100
        # Lock for thread-safe cache operations
        self._compute_client_cache_lock = Lock()
        # Enable ODPS2 type extensions (DATE, etc.) once at init, not per-request
        from odps import options as odps_options
        odps_options.sql.use_odps2_extension = True

    # ---- credential / client management ----

    def _create_odps_client_with_credentials(
        self,
        underlying: Any,
        project: str,
        endpoint: str
    ) -> ODPS:
        """Create ODPS client by copying credentials from existing client."""
        # Prefer reusing the parent client's account object (CredentialProviderAccount holds a Credentials Client reference and auto-refreshes)
        if hasattr(underlying, "account") and underlying.account is not None:
            return ODPS(
                account=underlying.account,
                project=project,
                endpoint=endpoint,
                rest_client_kwargs={"user_agent": _FULL_USER_AGENT},
            )

        # Fallback: use the held Credentials Client to create a new CredentialProviderAccount
        if self._credential_client is not None:
            try:
                from odps.accounts import CredentialProviderAccount
            except ImportError:
                raise RuntimeError(
                    "CredentialProviderAccount not available (requires pyodps >= 0.12.0). "
                    "Please upgrade pyodps to enable STS token auto-refresh."
                )
            account = CredentialProviderAccount(self._credential_client)
            return ODPS(
                account=account,
                project=project,
                endpoint=endpoint,
                rest_client_kwargs={"user_agent": _FULL_USER_AGENT},
            )

        # Final fallback: get credentials from default chain (static snapshot, only used when no credential_client)
        try:
            from .credentials import get_credentials_from_default_chain
            from odps.accounts import StsAccount
            creds = get_credentials_from_default_chain()
        except Exception as cred_err:
            logger.exception("Failed to get credentials from default chain: %s", cred_err)
            raise RuntimeError("No valid credentials found in default chain") from cred_err

        if creds.security_token:
            account = StsAccount(creds.access_key_id, creds.access_key_secret, creds.security_token)
            return ODPS(
                account=account,
                project=project,
                endpoint=endpoint,
                rest_client_kwargs={"user_agent": _FULL_USER_AGENT},
            )

        return ODPS(
            access_id=creds.access_key_id,
            secret_access_key=creds.access_key_secret,
            project=project,
            endpoint=endpoint,
            rest_client_kwargs={"user_agent": _FULL_USER_AGENT},
        )

    def _get_compute_client_for_project(self, project: str) -> Optional[MaxComputeClient]:
        """Get or create a compute client for the specified project.

        If project matches the default_project, returns the default maxcompute_client.
        Otherwise, creates a new ODPS client by copying config from the existing client
        and only changing the project.
        """
        if not project or project == self.default_project:
            return self.maxcompute_client

        # Fast path: check cache without lock first (TOCTOU-safe for reads in CPython)
        if project in self._compute_client_cache:
            return self._compute_client_cache[project]

        # Create a new client for the specified project
        if self.maxcompute_client is None:
            return None

        try:
            # Get the underlying ODPS client (outside lock to avoid holding lock during I/O)
            underlying = self.maxcompute_client.odps_client
            endpoint = underlying.endpoint
            client = self._create_odps_client_with_credentials(underlying, project, endpoint)
            new_client = MaxComputeClient(_client=client)

            # Hold lock only for the cache write to ensure atomicity
            with self._compute_client_cache_lock:
                # Re-check after acquiring lock to avoid duplicate creation
                if project in self._compute_client_cache:
                    logger.debug(
                        "Concurrent client creation detected for project %r, "
                        "using cached client instead of newly created one",
                        project
                    )
                    return self._compute_client_cache[project]
                # Add to cache with LRU eviction
                self._compute_client_cache[project] = new_client
                while len(self._compute_client_cache) > self._max_compute_client_cache_size:
                    oldest_key = next(iter(self._compute_client_cache))
                    del self._compute_client_cache[oldest_key]
                    logger.debug("Evicted oldest compute client from cache: %s", oldest_key)
            return new_client
        except Exception as e:
            logger.exception("Failed to create compute client for project %r: %s", project, e)
            raise RuntimeError(
                f"Cannot create compute client for project '{project}'. "
                f"Check credentials and endpoint configuration. Original error: {e}"
            ) from e

    # ---- 2-level / 3-level project detection via get_project ----

    def _is_schema_enabled(self, project: str) -> bool:
        """Return True if the project supports schemas (3-level model).

        Calls get_project once per project and caches the schemaEnabled field.
        Falls back to True (assume 3-level) when get_project is unavailable,
        so that callers still attempt the real API and surface the real error.
        """
        if project not in self._schema_enabled_cache:
            try:
                resp = self.sdk.client.get_project(project_id=project)
                m = resp.to_map() if hasattr(resp, "to_map") else resp
                enabled = m.get("schemaEnabled")
                # schemaEnabled absent on older API versions → assume 3-level
                self._schema_enabled_cache[project] = True if enabled is None else bool(enabled)
                logger.debug("Project %r schemaEnabled=%s", project, self._schema_enabled_cache[project])
            except Exception as e:
                logger.warning("get_project(%r) failed, assuming schema-enabled: %s", project, e)
                return True  # don't cache transient errors — allow retry on next call
        return self._schema_enabled_cache[project]

    # ---- tool specs ----

    def specs(self) -> list[ToolSpec]:
        project_prop = string_prop("MaxCompute project name", self.default_project if self.default_project else None)
        schema_prop = string_prop("Schema name (database name)", "default")
        table_prop = string_prop("Table name", None)
        page_size_prop = int_prop("Page size", 100)

        # maxcompute_explorer
        explorer = [
            ToolSpec(
                name="list_projects",
                description="List all Project IDs (Catalogs) accessible to the current account. Returns JSON: success, data (containing projects, next_page_token), summary (count overview)",
                input_schema=input_schema({"pageSize": page_size_prop, "token": string_prop("Pagination token (optional)")}),
            ),
            ToolSpec(
                name="get_project",
                description="Get detailed information about the specified project",
                input_schema=input_schema({"project": project_prop}),
            ),
            ToolSpec(
                name="list_schemas",
                description="List schemas under a project. Returns JSON: success, data (containing schemas, next_page_token), summary (count overview)",
                input_schema=input_schema({
                    "project": project_prop,
                    "pageSize": page_size_prop,
                    "token": string_prop("Pagination token (optional)"),
                }),
            ),
            ToolSpec(
                name="get_schema",
                description="Get detailed information about the specified schema, including metadata and properties",
                input_schema=input_schema({"project": project_prop, "schema": schema_prop}),
            ),
            ToolSpec(
                name="list_tables",
                description=(
                    "List all tables under the specified Project (including internal tables, external tables, views). "
                    "Returns JSON: success, data (containing namingModel, tables, next_page_token), summary (count overview). "
                    "data.namingModel is '3-level' (use schema.table) or '2-level' (use table only), "
                    "which determines the table reference format in subsequent SQL queries."
                ),
                input_schema=input_schema(
                    {
                        "project": project_prop,
                        "schema": schema_prop,
                        "pageSize": page_size_prop,
                        "filter": string_prop("Table name filter (optional)"),
                        "token": string_prop("Pagination token (optional)"),
                    }
                ),
            ),
            ToolSpec(
                name="get_table_schema",
                description=(
                    "Get the schema and business-semantic view of the specified table (excluding physical cold fields).\n"
                    "[SQL Usage] columns (name/type/description, STRUCT types include nested fields), "
                    "partitionKeys, namingModel ('3-level' or '2-level'), "
                    "sqlTableRef (table reference for direct use in SQL, handles 3-level/2-level difference: 3-level is schema.table, 2-level is table), "
                    "sqlExample (sample SELECT statement). Use sqlTableRef directly when generating SQL.\n"
                    "[Business Semantics (modifiable via update_table)] description (table comment), labels (table labels), "
                    "expiration ({days, partitionDays}, see update_table).\n"
                    "[Read-only Attributes] type (table type, e.g. MANAGED_TABLE/VIEW/EXTERNAL_TABLE), "
                    "etag (concurrency control token), createTime, lastModifiedTime — automatically maintained by the server, not modifiable.\n"
                    "It is recommended to call this tool first to get the current snapshot before calling update_table."
                ),
                input_schema=input_schema({"project": project_prop, "schema": schema_prop, "table": table_prop}),
            ),
            ToolSpec(
                name="get_partition_info",
                description="Query the partition list of a partitioned table, including data volume and time metrics such as latest access time (LAT) for each partition",
                input_schema=input_schema({
                    "project": project_prop,
                    "schema": schema_prop,
                    "table": table_prop,
                    "pageSize": page_size_prop,
                    "token": string_prop("Pagination token (optional)"),
                }),
            ),
        ]

        # maxcompute_data_query
        data_query = [
            ToolSpec(
                name="cost_sql",
                description=(
                    "Estimate SQL execution cost without actually running it. Returns estimatedCU, inputBytes (input scan bytes), complexity, udfCount. "
                    "Used together with execute_sql's maxCU for cost control.\n"
                    "project specifies the project for submitting the job (for billing), not the project where the table resides in SQL; if omitted, default_project is used.\n"
                    "SQL table reference format is the same as execute_sql."
                ),
                input_schema=input_schema({
                    "project": string_prop(
                        "Project for submitting the ODPS job (for billing). "
                        "If not specified, defaults to default_project in the configuration. "
                        "Note: this is the job submission project, not the project where the table resides in SQL"
                    ),
                    "sql": string_prop("MaxCompute SQL statement (SELECT queries only)"),
                    "hints": {
                        "type": "object",
                        "description": (
                            "ODPS runtime parameters (optional). 3-level (schema-enabled) projects must pass "
                            "{\"odps.namespace.schema\": \"true\"} during estimation, otherwise schema.table references will fail "
                            "to parse during cost estimation, resulting in a stub (estimatedCU=0) and making execute_sql's maxCU protection ineffective. "
                            "Should be consistent with execute_sql's hints."
                        ),
                        "additionalProperties": {"type": "string"},
                    },
                }),
            ),
            ToolSpec(
                name="execute_sql",
                description=(
                    "Execute MaxCompute SQL (only SELECT and other DQL supported; DML is prohibited). Can set maxCU resource limit; cost is estimated before execution, and if exceeded, prompts to increase maxCU and retry.\n"
                    "[Safety Protection]\n"
                    "Dual-layer read-only protection:\n"
                    "1. Client-side keyword check: validates SQL statement type before submission, rejects INSERT/UPDATE/DELETE/CREATE/DROP and other DML/DDL; "
                    "also rejects SET statements (to prevent disabling read-only protection via SET); use the hints parameter for runtime settings.\n"
                    "2. Server-side forced read-only: automatically injects odps.sql.read.only=true hint; the MaxCompute server will reject any write operations, "
                    "even if the client-side check is bypassed. This hint cannot be overridden by the caller.\n"
                    "[Execution Mode] Default is async (async=true): returns instanceId immediately, use get_instance_status / get_instance to poll and retrieve results. "
                    "Sync (async=false): waits for completion and returns results directly; on timeout (default 30s), returns instanceId for async tracking.\n"
                    "[Large Result Handling] Without output_uri, results are returned inline; exceeding MAXC_RESULT_ROW_CAP (default 1000) rows will be truncated (truncated=true), "
                    "rowCount shows the actual total rows. For large expected results (e.g. SELECT * without LIMIT, dump semantics, or previous truncated=true), pass "
                    "output_uri=\"file:///tmp/maxc/<name>.jsonl\" for streaming to disk; response only returns preview (first 20 rows) + file path.\n"
                    "[project] Specifies the project for submitting the job (for billing and permissions), not the project where the table resides in SQL; if omitted, default_project is used.\n"
                    "[SQL Table Reference Format - Important] Before writing SQL, you must call get_table_schema to get the sqlTableRef field, then reference the table in SQL using that format:\n"
                    "- Same project: 3-level uses schema.table (e.g. default.orders); 2-level uses table directly\n"
                    "- Cross-project: 3-level uses project.schema.table; 2-level uses project.table"
                ),
                input_schema=input_schema({
                    "project": string_prop(
                        "Project for submitting the ODPS job (for billing and access control). "
                        "If not specified, defaults to default_project in the configuration. "
                        "Note: this is the job submission project, not the project where the table resides in SQL"
                    ),
                    "sql": string_prop("MaxCompute SQL statement (SELECT queries only)"),
                    "async": {
                        "type": "boolean",
                        "description": (
                            "Whether to execute asynchronously (default true). "
                            "true: returns instanceId immediately after submission, use get_instance_status / get_instance to retrieve results asynchronously; "
                            "false: waits synchronously for completion and returns results directly; on timeout (see timeout parameter), returns instanceId for async tracking."
                        ),
                    },
                    "maxCU": int_prop("Resource usage limit (CU). Cost is checked before execution; if exceeded, user needs to set a higher limit to proceed"),
                    "timeout": int_prop("Timeout in seconds for sync mode (async=false, optional, default 30s). On timeout, returns instanceId; use get_instance_status / get_instance to query results asynchronously"),
                    "output_uri": string_prop(
                        "Optional. Path for streaming large results to disk, e.g. \"file:///tmp/maxc/result.jsonl\". The server automatically inserts instanceId into the filename "
                        "(e.g. result.<instanceId>.jsonl, see response outputPath for actual path) to prevent overwrites from repeated calls. "
                        "Only effective in sync mode (async=false) during execute_sql; for async submissions, pass output_uri when calling get_instance. "
                        "Only file:// scheme is supported; parent directories are created automatically."
                    ),
                    "hints": {
                        "type": "object",
                        "description": (
                            "ODPS runtime parameters (optional). Passed as key-value pairs, merged with default hints (caller takes precedence). "
                            "Common examples: {\"odps.sql.decimal.odps2\": \"true\", "
                            "\"odps.sql.hive.compatible\": \"true\", "
                            "\"odps.sql.type.system.odps2\": \"true\"}"
                        ),
                        "additionalProperties": {"type": "string"},
                    },
                }),
            ),
            ToolSpec(
                name="get_instance_status",
                description="Query job running status, resource consumption (CU), and progress by Instance ID",
                input_schema=input_schema({
                    "project": project_prop,
                    "instanceId": string_prop("Job Instance ID"),
                }),
            ),
            ToolSpec(
                name="get_instance",
                description=(
                    "Retrieve data analysis results. Without output_uri, results are returned inline and truncated (truncated=true) "
                    "when exceeding MAXC_RESULT_ROW_CAP (default 1000 rows); rowCount shows the actual total. "
                    "For large results, pass output_uri for streaming to disk; response returns preview + outputPath + bytesWritten."
                ),
                input_schema=input_schema({
                    "project": project_prop,
                    "instanceId": string_prop("Job Instance ID"),
                    "output_uri": string_prop(
                        "Optional. Path for streaming large results to disk, e.g. \"file:///tmp/maxc/result.jsonl\". The server inserts instanceId into the filename; "
                        "for multi-task results, task_name is also appended (e.g. result.<instanceId>.<task_name>.jsonl). See response outputPath for actual path. "
                        "Only file:// scheme is supported; parent directories are created automatically."
                    ),
                }),
            ),
        ]

        # maxcompute_data_insights (Catalog SDK search parameter docs: see pyodps_catalog.client.Client.search)
        _search_query_desc = (
            "Search query string, multiple conditions separated by commas. "
            "Syntax examples: name:foo,type=TABLE or description:bar,type=TABLE,project=proj. "
            "Conditions: name:xxx name substring; description:xxx description substring; type=TABLE|RESOURCE|SCHEMA (required); "
            "project=proj or project=(proj1|proj2) to scope project; region=region_id to scope region. "
            "Constraints: type is required; project single-value and multi-value cannot appear together; region and project cannot appear together."
        )
        _search_order_desc = (
            "Sort order: default, create_time asc, create_time desc, "
            "last_modified_time asc, last_modified_time desc."
        )
        data_insights = [
            ToolSpec(
                name="search_meta_data",
                description="Search Catalog entities (tables/resources/schemas) under the configured namespace (primary account). "
                "Calls Catalog API namespaces/:search; query uses the syntax defined by the SDK (see query parameter description). "
                "namespace_id is provided via configuration or the MAXCOMPUTE_NAMESPACE_ID environment variable.",
                input_schema=input_schema({
                    "query": string_prop(_search_query_desc),
                    "pageSize": int_prop("Number of items per page, greater than 0, max 100", 100),
                    "token": string_prop("Pagination token, obtain next page from the previous response's next_page_token (optional)"),
                    "orderBy": string_prop(_search_order_desc + " Optional."),
                }),
            ),
        ]

        # maxcompute_security - identity and permission queries
        security_tools = [
            ToolSpec(
                name="check_access",
                description=(
                    "Verify the identity and permissions of the current MCP MaxCompute access, "
                    "equivalent to running whoami + SHOW GRANTS in the ODPS Console. "
                    "Always returns identity information (account type, masked AK ID, default project, endpoint, "
                    "and the server-side whoami result showing the authenticated account name). "
                    "When include_grants=true, also executes SHOW GRANTS to return the current user's "
                    "permissions in the target project. "
                    "Note: only supports querying the current user's own permissions; "
                    "querying other users or roles is not supported."
                ),
                input_schema=input_schema({
                    "project": string_prop(
                        "Target project for the SHOW GRANTS query (only used when include_grants=true). "
                        "Defaults to default_project if not specified."
                    ),
                    "include_grants": {
                        "type": "boolean",
                        "description": (
                            "Whether to execute SHOW GRANTS to query the current user's permissions. "
                            "Defaults to true. Set to false if you only need identity information "
                            "and want to avoid the extra network call."
                        ),
                    },
                }),
            ),
        ]

        # maxcompute_table_designer
        table_designer = [
            ToolSpec(
                name="create_table",
                description=(
                    "Create a new table with the provided schema (columns, types, comments). "
                    "Supports setting Lifecycle, partition keys, table comment, "
                    "ifNotExists (no error when table exists), transactional (transactional table) + primaryKey (primary key, transactional tables only), "
                    "storageTier (storage tier, e.g. standard/lowfrequency/longterm), "
                    "tableProperties (table creation properties, key-value pairs), hints (SQL hints)."
                ),
                input_schema=input_schema({
                    "project": project_prop,
                    "schema": schema_prop,
                    "table": table_prop,
                    "columns": {
                        "type": "array",
                        "description": (
                            "Column definitions [{name, type, description?, notNull?, generateExpression?}]. "
                            "notNull=true means the column is NOT NULL (required for primary key columns)."
                        ),
                        "items": {"type": "object"},
                    },
                    "partitionColumns": {
                        "type": "array",
                        "description": (
                            "Partition column definitions (optional). Elements can be strings (default STRING type) "
                            "or {name, type?, description?, generateExpression?} (type defaults to STRING; "
                            "generateExpression is for AUTO PARTITIONED BY, e.g. "
                            "\"TRUNC_TIME(sale_date, 'month')\")"
                        ),
                        "items": {},
                    },
                    "lifecycle": int_prop("Table lifecycle in days (optional)"),
                    "description": string_prop("Table comment (optional)"),
                    "ifNotExists": {
                        "type": "boolean",
                        "description": "Whether to skip creation without error when table already exists (optional, default false)",
                    },
                    "transactional": {
                        "type": "boolean",
                        "description": "Whether to create a transactional table (optional, default false). Transactional tables can use primaryKey",
                    },
                    "primaryKey": {
                        "type": "array",
                        "description": "Primary key column name list (optional), only effective for transactional tables, e.g. ['id']",
                        "items": {"type": "string"},
                    },
                    "storageTier": string_prop(
                        "Storage tier (optional), e.g. 'standard' / 'lowfrequency' / 'longterm'"
                    ),
                    "tableProperties": {
                        "type": "object",
                        "description": (
                            "Table creation properties (optional). Values are converted to strings before being passed to MaxCompute, "
                            "e.g. {\"transactional\":\"true\"} or {\"transactional\":true}"
                        ),
                    },
                    "hints": {
                        "type": "object",
                        "description": (
                            "SQL hints (optional). Values are converted to strings before being passed to MaxCompute, "
                            "e.g. {\"odps.sql.type.system.odps2\":\"true\"}"
                        ),
                    },
                }),
            ),
            ToolSpec(
                name="insert_values",
                description=(
                    "Insert specified single or multiple records into a table (INSERT INTO ... VALUES). "
                    "The element order in each values row must strictly match the columns list order. "
                    "For partitioned tables, set partitionColumns (list of partition column names, must be a subset of columns); "
                    "records will be grouped by partition values and generate INSERT INTO ... PARTITION (...) (data columns) VALUES ..., "
                    "to avoid ODPS partition column count mismatch errors."
                ),
                input_schema=input_schema({
                    "project": project_prop,
                    "schema": schema_prop,
                    "table": table_prop,
                    "columns": {
                        "type": "array",
                        "description": "Column name list, e.g. ['id', 'name', 'age']",
                        "items": {"type": "string"},
                    },
                    "partitionColumns": {
                        "type": "array",
                        "description": "Partition column name list (optional). When non-empty, writes using static partitions, e.g. ['dt'].",
                        "items": {"type": "string"},
                    },
                    "values": {
                        "type": "array",
                        "description": "Multiple rows of data in a 2D array format. e.g. [[1, 'Alice', 20], [2, 'Bob', 25]]",
                        "items": {
                            "type": "array",
                            "description": "Single row data value list",
                            "items": {},
                        },
                    },
                    "timeout": int_prop("Timeout in seconds (optional, default 60s). On timeout, returns instanceId; use get_instance_status / get_instance to query results asynchronously"),
                    "async": {
                        "type": "boolean",
                        "description": (
                            "Whether to execute asynchronously (default false). "
                            "false: waits for completion and returns results; on timeout (see timeout parameter), returns instanceId for async tracking; "
                            "true: returns instanceId immediately after submission (partitioned tables return instanceIds list); use get_instance_status / get_instance to retrieve results asynchronously."
                        ),
                    },
                }),
            ),
        ]

        # maxcompute_table_meta — update table schema + business-semantic attributes.
        # Reading is handled by get_table_schema, which returns both the SQL view
        # and the full mutable metadata (description/labels/expiration/...).
        table_meta = [
            ToolSpec(
                name="update_table",
                description=(
                    "Update business-semantic metadata of a table. Uses internal read-modify-write with automatic etag handling; "
                    "it is recommended to call get_table_schema first to view the current snapshot.\n"
                    "Parameters are grouped by business object; provide at least one of description/labels/expiration/columns:\n"
                    "- description: table comment (string); pass empty string \"\" to clear the comment; null is not allowed (omit the field if you don't want to modify).\n"
                    "- labels: table label key-value patch.\n"
                    "    set: label key-value pairs to write (required);\n"
                    "    mode: merge strategy, default merge.\n"
                    "      merge   — keys in set override/add, other keys preserved\n"
                    "      replace — entire label set is replaced by set (unlisted labels will be deleted, use with caution)\n"
                    "      delete  — delete keys listed in set (value is ignored)\n"
                    "- expiration: expiration policy, all units are in days; 0 means disable expiration.\n"
                    "    days:          table expiration days; the table itself will be automatically deleted after this many days since last access/modification\n"
                    "    partitionDays: partition expiration days (only meaningful for partitioned tables); individual partitions will be automatically deleted after this many days\n"
                    "                   Both take effect independently; you can set only one\n"
                    "- columns: column-level operations.\n"
                    "    setComments: {column name or dot-path: new comment}, e.g. {\"id\": \"primary key\", \"addr.city\": \"city\"}, "
                    "supports STRUCT nested sub-columns\n"
                    "    setNullable: [top-level column names], changes these columns from REQUIRED to NULLABLE (allow null); nested columns not supported\n"
                    "    add:         [{name, type, description?}], appends new columns; new columns are forced to NULLABLE\n"
                    "Server limitations (MaxCompute does not support): deleting columns / changing column types / reordering columns / inserting columns in the middle / "
                    "NULLABLE→REQUIRED / appending REQUIRED columns / modifying nested column mode."
                ),
                input_schema=input_schema({
                    "project": project_prop,
                    "schema": schema_prop,
                    "table": table_prop,
                    "description": string_prop("Table comment (optional); pass empty string \"\" to clear existing comment, omit this field if you don't want to modify"),
                    "labels": {
                        "type": "object",
                        "description": "Table label patch (optional), see the tool's top-level description for semantics",
                        "required": ["set"],
                        "properties": {
                            "set": {
                                "type": "object",
                                "additionalProperties": {"type": "string"},
                            },
                            "mode": {
                                "type": "string",
                                "enum": ["merge", "replace", "delete"],
                            },
                        },
                    },
                    "expiration": {
                        "type": "object",
                        "description": "Expiration policy (optional), unit is days; 0 means disable",
                        "properties": {
                            "days": {"type": "integer", "minimum": 0},
                            "partitionDays": {"type": "integer", "minimum": 0},
                        },
                    },
                    "columns": {
                        "type": "object",
                        "description": "Column-level operations (optional)",
                        "properties": {
                            "setComments": {
                                "type": "object",
                                "additionalProperties": {"type": "string"},
                            },
                            "setNullable": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "add": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "type": {"type": "string"},
                                        "description": {"type": "string"},
                                    },
                                    "required": ["name", "type"],
                                },
                            },
                        },
                    },
                    "etag": string_prop(
                        "Optional: explicitly pass etag to enforce optimistic concurrency control (defaults to automatically fetching the latest etag from get_table)"
                    ),
                }, required=["table"]),
            ),
        ]

        # resource_mgr (ops) tools not implemented
        return (
            explorer
            + data_query
            + data_insights
            + security_tools
            + table_designer
            + table_meta
        )

    # ---- tool call dispatcher ----

    def call(self, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        handlers = {
            "list_projects": self.list_projects,
            "get_project": self.get_project,
            "list_schemas": self.list_schemas,
            "get_schema": self.get_schema,
            "list_tables": self.list_tables,
            "get_table_schema": self.get_table_schema,
            "get_partition_info": self.get_partition_info,
            "cost_sql": self.cost_sql,
            "execute_sql": self.execute_sql,
            "get_instance_status": self.get_instance_status,
            "get_instance": self.get_instance,
            "search_meta_data": self.search_meta_data,
            "check_access": self.check_access,
            "create_table": self.create_table,
            "insert_values": self.insert_values,
            "update_table": self.update_table,
        }
        fn = handlers.get(name)
        if fn is None:
            raise JsonRpcError(-32602, "Invalid params", f"Unknown tool: {name}")
        return fn(args)
