# Alibaba Cloud MaxCompute MCP Server

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-green.svg)](https://www.python.org/downloads/)

[中文文档](README_ZH.md)

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server for Alibaba Cloud [MaxCompute](https://www.alibabacloud.com/product/maxcompute). It exposes Catalog API and compute capabilities as MCP tools so that AI assistants such as Cursor and Claude Code can list projects / schemas / tables, search metadata, estimate and execute SQL, and manage MaxCompute instances over stdio or Streamable HTTP.

> [!IMPORTANT]
> MaxCompute Remote MCP Server is the recommended way to use MaxCompute MCP.
> Start with the hosted Remote MCP service documentation:
> [MaxCompute MCP service (Remote MCP Server)](https://www.alibabacloud.com/help/en/maxcompute/getting-started/mcmcp-service-remote-mcp-server).
>
> This repository continues to host the local MCP server code for self-hosted
> and development scenarios. During the Remote MCP rollout, public,
> non-sensitive Remote MCP feedback is tracked through this repository's
> [Remote MCP issue template](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/issues/new?template=remote-mcp-service-feedback.md).

## Features

- **Catalog**: list projects / schemas / tables; get project, schema, table and partition details.
- **Search**: metadata search via `search_meta_data` (requires `namespaceId`).
- **Compute**: SQL cost estimation, read-only SQL execution, instance status and result retrieval.
- **Table management**: create table, insert values (via PyODPS).
- **Table metadata**: update comment, labels, lifecycle and column descriptions (`update_table`).
- **Identity & access**: `check_access` combines identity discovery and grant inspection.
- **Transports**: stdio (default, for IDE integration) and Streamable HTTP (built-in, no `mcp-proxy` needed).

## Remote MCP Server (Recommended)

Use the hosted MaxCompute Remote MCP Server first unless you specifically need
a local `stdio` or self-hosted setup. The remote service removes local runtime
and credential setup from the MCP server process, uses Streamable HTTP, and
follows the official Alibaba Cloud onboarding flow.

For setup instructions, supported endpoints, OAuth login flow, tool
capabilities, and safety notes, see:

- [MaxCompute MCP service (Remote MCP Server)](https://www.alibabacloud.com/help/en/maxcompute/getting-started/mcmcp-service-remote-mcp-server)

### Remote MCP feedback

Use this repository's issues for public, non-sensitive Remote MCP feedback:

- [Report Remote MCP feedback](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/issues/new?template=remote-mcp-service-feedback.md)
- [View existing issues](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/issues)

Include the MCP client name/version, endpoint type, tool name, request ID,
time window with timezone, region, sanitized error code/message, expected
behavior, actual behavior, and reproduction steps when available.

Do not include access tokens, refresh tokens, authorization codes, cookies,
AccessKey IDs or secrets, OAuth callback URLs with query strings, sensitive SQL,
customer data, or sensitive Logview content. Use official Alibaba Cloud support
or security channels for account-specific permissions, billing, SLA-bound
incidents, production outages, vulnerabilities, or confidential data cases.

## Local MCP Server (Optional)

The sections below describe the local MCP server in this repository. Use the
local server when you need self-hosting, stdio integration, local development,
or direct credential control. For the hosted service, follow the Remote MCP
documentation above instead.

### Requirements

The local MCP server needs:

- Python 3.10 or newer.
- [`uv`](https://docs.astral.sh/uv/) for dependency management (recommended).
- MaxCompute access with an Access Key / STS credentials / credentials URI.

### Installation

This first public release is distributed as a source repository only. PyPI and standalone tarballs are not available in this phase.

```bash
git clone https://github.com/aliyun/alibabacloud-maxcompute-mcp-server.git
cd alibabacloud-maxcompute-mcp-server
uv sync
```

Verify the entry point:

```bash
uv run alibabacloud-maxcompute-mcp-server --help
```

### Configuration

Copy the public example and fill in real values locally:

```bash
cp config.example.json config.json
# edit config.json with real endpoint / project / credentials
```

`config.json` is git-ignored by default and must not be committed.

#### Configuration fields

| Field | Required | Description |
| --- | --- | --- |
| `maxcompute.maxcompute_endpoint` | yes | MaxCompute service endpoint, e.g. `https://service.cn-hangzhou.maxcompute.aliyun.com/api`. |
| `maxcompute.catalogapi_endpoint` | optional | Catalog API endpoint. When absent it is derived from `maxcompute_endpoint`. |
| `maxcompute.defaultProject` | optional | Default project name used as the execution context. |
| `maxcompute.namespaceId` | optional | Main account UID required by `search_meta_data`. |
| `maxcompute.protocol` | optional | `https` (default) or `http`. |
| `maxcompute.accessKeyId` / `accessKeySecret` | optional | Static credentials for development. Prefer `ALIBABA_CLOUD_CREDENTIALS_URI` in production. |

#### Credential precedence

1. `ALIBABA_CLOUD_ACCESS_KEY_ID` / `ALIBABA_CLOUD_ACCESS_KEY_SECRET` environment variables (optionally with `ALIBABA_CLOUD_SECURITY_TOKEN`).
2. `ALIBABA_CLOUD_CREDENTIALS_URI` pointing to a local credential provider.
3. The Alibaba Cloud default credential chain (environment, file, ECS RAM role, etc.).
4. Static `accessKeyId` / `accessKeySecret` inside `config.json` (lowest priority, development only).

#### Environment-variable-only mode

You can skip the JSON file entirely and configure the server through environment variables:

| Variable | Purpose |
| --- | --- |
| `MAXCOMPUTE_ENDPOINT` | MaxCompute service endpoint. |
| `MAXCOMPUTE_CATALOG_API_ENDPOINT` | Optional Catalog API endpoint override. |
| `MAXCOMPUTE_DEFAULT_PROJECT` | Default project name. |
| `MAXCOMPUTE_NAMESPACE_ID` | Namespace ID for `search_meta_data`. |
| `ALIBABA_CLOUD_ACCESS_KEY_ID` / `ALIBABA_CLOUD_ACCESS_KEY_SECRET` | Static credentials. |
| `ALIBABA_CLOUD_SECURITY_TOKEN` | Optional STS token. |
| `ALIBABA_CLOUD_CREDENTIALS_URI` | Credential provider URI. |

#### Named configs (runtime switching)

To switch between regions, endpoints, projects, or identities without restarting the MCP server, create a local multi-config file such as `config.multi.json` and point `MAXCOMPUTE_CATALOG_CONFIG` to it:

```json
{
  "default": "beijing",
  "configs": {
    "beijing": {
      "region": "cn-beijing",
      "description": "Beijing production",
      "maxcompute_endpoint": "https://service.cn-beijing.maxcompute.aliyun.com/api",
      "accessKeyId": "<ALIBABA_CLOUD_ACCESS_KEY_ID>",
      "accessKeySecret": "<ALIBABA_CLOUD_ACCESS_KEY_SECRET>",
      "defaultProject": "<DEFAULT_PROJECT_NAME>",
      "namespaceId": "<ALIBABACLOUD_ACCOUNT_UID>"
    },
    "singapore": {
      "region": "ap-southeast-1",
      "description": "Singapore production",
      "maxcompute_endpoint": "https://service.ap-southeast-1.maxcompute.aliyun.com/api",
      "catalogapi_endpoint": "https://catalogapi.ap-southeast-1.maxcompute.aliyun.com",
      "protocol": "https",
      "accessKeyId": "<ALIBABA_CLOUD_ACCESS_KEY_ID>",
      "accessKeySecret": "<ALIBABA_CLOUD_ACCESS_KEY_SECRET>",
      "defaultProject": "<DEFAULT_PROJECT_NAME>",
      "namespaceId": "<ALIBABACLOUD_ACCOUNT_UID>"
    },
    "intl-readonly": {
      "region": "ap-southeast-1",
      "description": "Singapore readonly identity",
      "maxcompute_endpoint": "https://service.ap-southeast-1.maxcompute.aliyun.com/api",
      "catalogapi_endpoint": "https://catalogapi.ap-southeast-1.maxcompute.aliyun.com",
      "protocol": "https",
      "accessKeyId": "<READONLY_ALIBABA_CLOUD_ACCESS_KEY_ID>",
      "accessKeySecret": "<READONLY_ALIBABA_CLOUD_ACCESS_KEY_SECRET>",
      "defaultProject": "<READONLY_DEFAULT_PROJECT_NAME>",
      "namespaceId": "<ALIBABACLOUD_ACCOUNT_UID>"
    }
  }
}
```

The server starts with `default` (or the first config when `default` is omitted). Use the session tools `list_configs`, `get_current_config`, and `use_config` to inspect and switch the active config at runtime. These tools never return AccessKey IDs, AccessKey secrets, or STS tokens.

Each named config must provide `maxcompute_endpoint`. If `catalogapi_endpoint` is omitted, also provide `defaultProject` so the server can resolve the Catalog API endpoint through MaxCompute.

The active config is process-global. Runtime switching is best suited to stdio / single-client usage. In shared Streamable HTTP mode, all connected clients share the same active config, so a `use_config` call from one client affects the others.

### Running

#### stdio (default)

```bash
uv run alibabacloud-maxcompute-mcp-server
```

#### Streamable HTTP

```bash
uv run alibabacloud-maxcompute-mcp-server --transport http --host 127.0.0.1 --port 8000
```

### MCP tools

All tools return JSON in an MCP text response. Check `success` first, then read `data`, `summary`, or `error`.

| Category | Tools | Purpose |
| --- | --- | --- |
| Catalog discovery | `list_projects`, `get_project`, `list_schemas`, `get_schema`, `list_tables`, `get_table_schema`, `get_partition_info` | Browse projects, schemas, tables, table schemas, table metadata, and partitions. |
| SQL and instances | `cost_sql`, `execute_sql`, `get_instance_status`, `get_instance` | Estimate query cost, run read-only SQL, poll instances, and retrieve results. |
| Search and access | `search_meta_data`, `check_access` | Search Catalog metadata under a namespace and inspect the current identity / grants. |
| Table management | `create_table`, `insert_values`, `update_table` | Create tables, insert rows, and update table comments, labels, lifecycle, and column metadata. |
| Session config | `list_configs`, `get_current_config`, `use_config` | List named configs, inspect the active config, and switch region / identity / project at runtime. |

Notes:

- `execute_sql` is read-only by design. The server validates SQL client-side and also submits jobs with the MaxCompute read-only hint.
- For SQL table references, call `get_table_schema` first and use the returned `sqlTableRef`; this handles two-level and three-level project naming.
- `search_meta_data` requires `namespaceId` / `MAXCOMPUTE_NAMESPACE_ID`.
- Large query results can be streamed to a local `file://` `output_uri`; otherwise responses are returned inline and may be truncated.

### MCP client setup

#### Cursor / Claude Code (stdio, config file)

```json
{
  "mcpServers": {
    "alibabacloud-maxcompute-mcp-server": {
      "command": "uv",
      "args": [
        "--directory",
        "/absolute/path/to/alibabacloud-maxcompute-mcp-server",
        "run",
        "alibabacloud-maxcompute-mcp-server"
      ],
      "env": {
        "MAXCOMPUTE_CATALOG_CONFIG": "/absolute/path/to/alibabacloud-maxcompute-mcp-server/config.json"
      }
    }
  }
}
```

#### Cursor / Claude Code (stdio, environment variables only)

```json
{
  "mcpServers": {
    "alibabacloud-maxcompute-mcp-server": {
      "command": "uv",
      "args": [
        "--directory",
        "/absolute/path/to/alibabacloud-maxcompute-mcp-server",
        "run",
        "alibabacloud-maxcompute-mcp-server"
      ],
      "env": {
        "MAXCOMPUTE_ENDPOINT": "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
        "MAXCOMPUTE_DEFAULT_PROJECT": "<DEFAULT_PROJECT_NAME>",
        "MAXCOMPUTE_NAMESPACE_ID": "<ALIBABACLOUD_ACCOUNT_UID>",
        "ALIBABA_CLOUD_ACCESS_KEY_ID": "<ALIBABA_CLOUD_ACCESS_KEY_ID>",
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET": "<ALIBABA_CLOUD_ACCESS_KEY_SECRET>"
      }
    }
  }
}
```

#### Streamable HTTP

Start the server (see above), then point your MCP client at `http://127.0.0.1:8000/mcp`.

### Development

```bash
uv sync --all-extras
uv run pytest tests/ -q
uv build
```

#### Package naming

| Name | Context |
| --- | --- |
| `alibabacloud-maxcompute-mcp-server` | pip package name, CLI entry point, repository name |
| `maxcompute_catalog_mcp` | Python import path (`from maxcompute_catalog_mcp import ...`) |

The import module name predates the public package name and is kept for backward compatibility.

## Contributing

- This is the first public source release. PyPI packages and GitHub Release artifacts are **not** available in this phase.
- Pull requests and issues are welcome. For Remote MCP service feedback, use
  the Remote MCP issue template. For local server code changes, please open an
  issue before starting large changes.

## License

Apache License 2.0. See [LICENSE](LICENSE).
