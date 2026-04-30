# Alibaba Cloud MaxCompute MCP Server

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-green.svg)](https://www.python.org/downloads/)

[中文文档](README_ZH.md)

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server for Alibaba Cloud [MaxCompute](https://www.alibabacloud.com/product/maxcompute). It exposes Catalog API and compute capabilities as MCP tools so that AI assistants such as Cursor and Claude Code can list projects / schemas / tables, search metadata, estimate and execute SQL, and manage MaxCompute instances over stdio or Streamable HTTP.

## Features

- **Catalog**: list projects / schemas / tables; get project, schema, table and partition details.
- **Search**: metadata search via `search_meta_data` (requires `namespaceId`).
- **Compute**: SQL cost estimation, read-only SQL execution, instance status and result retrieval.
- **Table management**: create table, insert values (via PyODPS).
- **Table metadata**: update comment, labels, lifecycle and column descriptions (`update_table`).
- **Identity & access**: `check_access` combines identity discovery and grant inspection.
- **Transports**: stdio (default, for IDE integration) and Streamable HTTP (built-in, no `mcp-proxy` needed).

## Requirements

- Python 3.10 or newer.
- [`uv`](https://docs.astral.sh/uv/) for dependency management (recommended).
- MaxCompute access with an Access Key / STS credentials / credentials URI.

## Installation (source checkout)

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

## Configuration

Copy the public example and fill in real values locally:

```bash
cp config.example.json config.json
# edit config.json with real endpoint / project / credentials
```

`config.json` is git-ignored by default and must not be committed.

### Configuration fields

| Field | Required | Description |
| --- | --- | --- |
| `maxcompute.maxcompute_endpoint` | yes | MaxCompute service endpoint, e.g. `https://service.cn-hangzhou.maxcompute.aliyun.com/api`. |
| `maxcompute.catalogapi_endpoint` | optional | Catalog API endpoint. When absent it is derived from `maxcompute_endpoint`. |
| `maxcompute.defaultProject` | optional | Default project name used as the execution context. |
| `maxcompute.namespaceId` | optional | Main account UID required by `search_meta_data`. |
| `maxcompute.protocol` | optional | `https` (default) or `http`. |
| `maxcompute.accessKeyId` / `accessKeySecret` | optional | Static credentials for development. Prefer `ALIBABA_CLOUD_CREDENTIALS_URI` in production. |

### Credential precedence

1. `ALIBABA_CLOUD_ACCESS_KEY_ID` / `ALIBABA_CLOUD_ACCESS_KEY_SECRET` environment variables (optionally with `ALIBABA_CLOUD_SECURITY_TOKEN`).
2. `ALIBABA_CLOUD_CREDENTIALS_URI` pointing to a local credential provider.
3. The Alibaba Cloud default credential chain (environment, file, ECS RAM role, etc.).
4. Static `accessKeyId` / `accessKeySecret` inside `config.json` (lowest priority, development only).

### Environment-variable-only mode

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

## Running

### stdio (default)

```bash
uv run alibabacloud-maxcompute-mcp-server
```

### Streamable HTTP

```bash
uv run alibabacloud-maxcompute-mcp-server --transport http --host 127.0.0.1 --port 8000
```

## MCP client setup

### Cursor / Claude Code (stdio, config file)

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

### Cursor / Claude Code (stdio, environment variables only)

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

### Streamable HTTP

Start the server (see above), then point your MCP client at `http://127.0.0.1:8000/mcp`.

## Development

```bash
uv sync --all-extras
uv run pytest tests/ -q
uv build
```

### Package naming

| Name | Context |
| --- | --- |
| `alibabacloud-maxcompute-mcp-server` | pip package name, CLI entry point, repository name |
| `maxcompute_catalog_mcp` | Python import path (`from maxcompute_catalog_mcp import ...`) |

The import module name predates the public package name and is kept for backward compatibility.

## Contributing

- This is the first public source release. PyPI packages and GitHub Release artifacts are **not** available in this phase.
- Pull requests and issues are welcome. Please open an issue before starting large changes.

## License

Apache License 2.0. See [LICENSE](LICENSE).
