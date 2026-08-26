# Alibaba Cloud MaxCompute MCP Server

[![PyPI](https://img.shields.io/pypi/v/alibabacloud-maxcompute-mcp-server)](https://pypi.org/project/alibabacloud-maxcompute-mcp-server/)
[![Python](https://img.shields.io/pypi/pyversions/alibabacloud-maxcompute-mcp-server)](https://pypi.org/project/alibabacloud-maxcompute-mcp-server/)
[![License](https://img.shields.io/github/license/aliyun/alibabacloud-maxcompute-mcp-server)](LICENSE)
[![CI](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/actions/workflows/ci.yml)
[![English](https://img.shields.io/badge/lang-English-blue)](README.md)
[![中文](https://img.shields.io/badge/lang-中文-red)](README_ZH.md)

A local [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) launcher for Alibaba Cloud [MaxCompute](https://www.alibabacloud.com/product/maxcompute). It can run the original SDK-backed server (`local` mode) or act as a transparent stdio proxy to the hosted MaxCompute MCP service (`remote` mode).

> [!IMPORTANT]
> MaxCompute Remote MCP Server is the recommended way to use MaxCompute MCP.
> Start with the hosted Remote MCP service documentation:
> [MaxCompute MCP documentation](https://help.aliyun.com/en/maxcompute/getting-started/mcp-overview-and-access).
>
> This repository continues to host the local MCP server code for self-hosted
> and development scenarios. During the Remote MCP rollout, public,
> non-sensitive Remote MCP feedback is tracked through this repository's
> [Remote MCP issue template](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/issues/new?template=remote-mcp-service-feedback.md).

## Features

- **Backward-compatible configuration**: existing FE/Catalog endpoints determine Region and public/VPC network; simple `region` + `network` config derives FE, Catalog, and MCP endpoints without a Region registry.
- **Transparent remote mode**: stdio MCP messages are relayed to the hosted Streamable HTTP endpoint without rebuilding or renaming the remote tool surface.
- **Current protocol support**: MCP Python SDK 2.x preserves modern `2026-07-28` request metadata and routing headers while retaining legacy initialize compatibility.
- **Standard CatalogAPI authentication**: the remote proxy signs the bodyless `mcpAccessToken` operation with the existing credential provider and resolves current AK/STS/ECS RAM Role credentials again on renewal.
- **Credential isolation**: only a short-lived `mcpc_` bearer is sent to the Gateway; AccessKey secrets and STS credentials never enter MCP messages or URLs, and a selected remote session never falls back to the local SDK.

## Remote MCP Server (Recommended)

Use the hosted MaxCompute Remote MCP Server first unless you specifically need
a local `stdio` or self-hosted setup. The remote service removes local runtime
and credential setup from the MCP server process, uses Streamable HTTP, and
follows the official Alibaba Cloud onboarding flow.

For setup instructions, supported endpoints, OAuth login flow, tool
capabilities, and safety notes, see:

- [MaxCompute MCP documentation](https://help.aliyun.com/en/maxcompute/getting-started/mcp-overview-and-access)

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

## Local launcher (optional)

The launcher defaults to `default` for upgrades from old configurations. It
derives the Region and public/VPC network from the selected legacy FE and
Catalog endpoints, or uses an explicit `region` plus `network` simple config.
When this process uses stdio and Region/network can be determined, it obtains a
CatalogAPI token and authenticates the one regional MCP endpoint with
`initialize`. Token issuance or remote initialization failure falls back to the
original local implementation. Use `--mode local` to pin that implementation.

| Mode | MCP path | Backend | Transport exposed by this process |
| --- | --- | --- | --- |
| `default` | Probe remote at startup, then select one path | Remote MCP first; local SDK on initialization failure | stdio; non-stdio stays local |
| `local` | MCP client → this process → local SDK | MaxCompute SDK / CatalogAPI | stdio or Streamable HTTP |
| `remote` | MCP client → this process → hosted Gateway | Remote MCP Streamable HTTP | stdio only |

Explicit `remote` mode is fail-closed: CatalogAPI issuance, MCP initialize,
token renewal, or Gateway transport failures terminate the remote flow.
`default` falls back only before selection. Once its authenticated initialize
has selected remote, a later remote-session failure never invokes local SDK
tools dynamically.

### Requirements

The launcher needs:

- Python 3.10 or newer.
- [`uv`](https://docs.astral.sh/uv/) for dependency management (recommended).
- The base installation supports the default remote proxy without PyODPS or
  PyArrow. Local SDK mode uses the optional `local` dependency set; its secure
  PyArrow wheel requires glibc 2.28+ or musl 1.2+ on Linux.
- Remote and local modes both need MaxCompute access through an
  AccessKey, STS credentials, a credentials URI, ECS RAM Role, or another
  supported default-credential-chain source.
- The remote proxy obtains a 300-second `mcpc_` token through the standard
  signed CatalogAPI operation and stores no refresh token.
- MCP Python SDK 2.x is installed from the lockfile; modern `2026-07-28` and
  negotiated legacy stdio clients are both supported.

### Installation

Install the released package from PyPI with either `pip` or `uv`:

```bash
python -m pip install alibabacloud-maxcompute-mcp-server
# or install the command in an isolated environment
uv tool install alibabacloud-maxcompute-mcp-server
```

The base package is intentionally remote-first and does not install PyODPS or
PyArrow. Install the local SDK implementation only when it is needed:

```bash
python -m pip install "alibabacloud-maxcompute-mcp-server[local]"
# or
uv tool install "alibabacloud-maxcompute-mcp-server[local]"
```

With the base package, `default` still tries remote first. If remote
initialization fails, the local fallback reports the command above instead of
silently running without its SDK dependencies.

Verify the installed entry point:

```bash
alibabacloud-maxcompute-mcp-server --help
```

For source development, clone the repository and synchronize all development
dependencies:

```bash
git clone https://github.com/aliyun/alibabacloud-maxcompute-mcp-server.git
cd alibabacloud-maxcompute-mcp-server
uv sync --all-extras
```

Run the development entry point:

```bash
uv run alibabacloud-maxcompute-mcp-server --help
```

### Configuration

Runtime values use this precedence: command line, environment variables, JSON,
then defaults. The mode is `default` when no source selects one.

| Purpose | CLI | Environment | JSON |
| --- | --- | --- | --- |
| Runtime mode | `--mode default\|remote\|local` | `MAXCOMPUTE_MCP_MODE` | `mode` |
| Remote MCP URL | `--remote-url` | `MAXCOMPUTE_REMOTE_MCP_URL` | `remote.url` |
| Startup profile | `--profile` | `MAXCOMPUTE_MCP_PROFILE` | `profile` |
| Config file | `--config` | `MAXCOMPUTE_CATALOG_CONFIG` | n/a |

The profile selects the endpoint, CatalogAPI client, and credentials used for
token issuance in remote mode as well as the local SDK setup in local mode.

Copy the public example and fill in real values locally:

```bash
cp config.example.json config.json
# edit config.json with the real Region / network / project / credentials
```

`config.json` is git-ignored by default and must not be committed.

#### MaxCompute configuration fields

| Field | Required | Description |
| --- | --- | --- |
| `maxcompute.region` | with `network` | Region used for simple endpoint synthesis and remote selection, e.g. `cn-hangzhou`. |
| `maxcompute.network` | with `region` | `public` or `vpc`. Together with `region`, this replaces explicit FE/Catalog endpoint configuration. |
| `maxcompute.maxcompute_endpoint` | unless `region` + `network` | MaxCompute FE endpoint, e.g. `https://service.cn-hangzhou.maxcompute.aliyun.com/api`. |
| `maxcompute.catalogapi_endpoint` | optional | Catalog API endpoint. Simple config synthesizes it; legacy config otherwise resolves it from `maxcompute_endpoint` when absent. |
| `maxcompute.defaultProject` | optional | Default project name used as the execution context. |
| `maxcompute.namespaceId` | optional | Main account UID required by `search_meta_data`. |
| `maxcompute.protocol` | optional | `https` (default) or `http`. |
| `maxcompute.accessKeyId` / `accessKeySecret` | optional | Static credentials for development. Prefer `ALIBABA_CLOUD_CREDENTIALS_URI` in production. |

The simplest endpoint configuration is:

```json
{
  "maxcompute": {
    "region": "cn-hangzhou",
    "network": "public",
    "defaultProject": "<DEFAULT_PROJECT_NAME>"
  }
}
```

`region` and `network` synthesize FE, Catalog, and MCP endpoints by rule.
`network: "vpc"` uses intranet FE/Catalog endpoints and a VPC MCP endpoint.

#### Local-mode credential precedence

1. `ALIBABA_CLOUD_ACCESS_KEY_ID` / `ALIBABA_CLOUD_ACCESS_KEY_SECRET` environment variables (optionally with `ALIBABA_CLOUD_SECURITY_TOKEN`).
2. `ALIBABA_CLOUD_CREDENTIALS_URI` pointing to a local credential provider.
3. The Alibaba Cloud default credential chain (environment, file, ECS RAM role, etc.).
4. Static `accessKeyId` / `accessKeySecret` inside `config.json` (lowest priority, development only).

#### Environment-variable-only mode

You can skip the JSON file entirely and configure the server through environment variables:

| Variable | Purpose |
| --- | --- |
| `MAXCOMPUTE_MCP_MODE` | `default`, `remote`, or `local`; omitted means `default`. |
| `MAXCOMPUTE_REMOTE_MCP_URL` | Optional override. It must match the MaxCompute endpoint network; VPC must also use the same Region. |
| `MAXCOMPUTE_MCP_PROFILE` | Named profile selected for local or remote startup. |
| `MAXCOMPUTE_ENDPOINT` | MaxCompute service endpoint. |
| `MAXCOMPUTE_CATALOG_API_ENDPOINT` | Optional Catalog API endpoint override. |
| `MAXCOMPUTE_REGION` | Region for simple endpoint configuration or validation. |
| `MAXCOMPUTE_NETWORK` | `public` or `vpc`; requires `MAXCOMPUTE_REGION`. |
| `MAXCOMPUTE_DEFAULT_PROJECT` | Default project name. |
| `MAXCOMPUTE_NAMESPACE_ID` | Namespace ID for `search_meta_data`. |
| `ALIBABA_CLOUD_ACCESS_KEY_ID` / `ALIBABA_CLOUD_ACCESS_KEY_SECRET` | Static credentials. |
| `ALIBABA_CLOUD_SECURITY_TOKEN` | Optional STS token. |
| `ALIBABA_CLOUD_CREDENTIALS_URI` | Credential provider URI. |

#### Named local configs (runtime switching)

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

The server starts with `default` (or the first config when `default` is omitted).
In local mode, use the session tools `list_configs`, `get_current_config`, and
`use_config` to inspect and switch the active config at runtime. These tools
never return AccessKey IDs, AccessKey secrets, or STS tokens. Use
`--profile <name>` or `MAXCOMPUTE_MCP_PROFILE=<name>` to select a config at
startup in either mode.

Each named config must provide either `maxcompute_endpoint` or `region` plus
`network`. If a legacy config omits `catalogapi_endpoint`, also provide
`defaultProject` so the server can resolve the Catalog API endpoint through
MaxCompute.

The active local config is process-global. Runtime switching is best suited to
stdio / single-client usage. In shared Streamable HTTP mode, all connected
clients share the same active config, so a `use_config` call from one client
affects the others. Remote mode fixes one profile at startup and does not expose
local session-switching tools.

#### Default selection and remote overrides

Released top-level `maxcompute`, top-level `odps`, named `configs`, and
environment-only configurations all continue to load. Endpoint derivation has
no Region list:

| Service | public rule | VPC rule |
| --- | --- | --- |
| FE | `https://service.<region>.maxcompute.aliyun.com/api` | `https://service.<region>-intranet.maxcompute.aliyun-inc.com/api` |
| CatalogAPI | `https://catalogapi.<region>.maxcompute.aliyun.com` | `https://catalogapi.<region>-intranet.maxcompute.aliyun-inc.com` |
| MCP for Mainland China Region (`cn-*`, except `cn-hongkong`) | `https://mcp.<region>.maxcompute.aliyun.com/mcp` | `https://mcp.<region>-vpc.maxcompute.aliyun-inc.com/mcp` |
| MCP for overseas Region (including `cn-hongkong`) | `https://mcp-intl.<region>.maxcompute.aliyun.com/mcp` | `https://mcp-intl.<region>-vpc.maxcompute.aliyun-inc.com/mcp` |

For legacy configuration, recognized FE and Catalog endpoints both contribute
Region/network evidence. Public endpoints select public MCP; recognized VPC or
intranet endpoints select VPC MCP. When both endpoints are recognized, they
must identify the same Region and network. Conflicting endpoints never select
remote, and explicit `region`/`network` values that contradict a recognized
endpoint are rejected as invalid configuration.

A VPC configuration never selects public MCP or crosses Regions. Custom or
historical endpoints with no verifiable Region/network, conflicting FE/Catalog
evidence, or a local HTTP transport stay on legacy local behavior under
`default`; explicit `remote` mode fails closed.

Endpoint naming is determined only by Region: Mainland China Regions (`cn-*`,
except `cn-hongkong`) use `mcp`, while `cn-hongkong` and every other overseas
Region use `mcp-intl`. This does not detect or distinguish account sites: the
CatalogAPI-issued token path does not enter RAM OAuth. The derived endpoint is
authenticated once during process startup and remains fixed for the process
lifetime. Later CatalogAPI token renewal does not repeat initialization. When
`remote.url` is explicitly configured, only that URL is used; VPC must still
use the same Region.

A minimal remote override reuses the existing MaxCompute configuration and
credentials:

```json
{
  "mode": "remote",
  "remote": {
    "url": "https://mcp.cn-hangzhou.maxcompute.aliyun.com/mcp"
  },
  "maxcompute": {
    "region": "cn-hangzhou",
    "network": "public",
    "accessKeyId": "<ALIBABA_CLOUD_ACCESS_KEY_ID>",
    "accessKeySecret": "<ALIBABA_CLOUD_ACCESS_KEY_SECRET>"
  }
}
```

The remote URL must be an exact `/mcp` resource without userinfo, query, or
fragment. The proxy rejects redirects, ignores ambient HTTP proxy variables,
and renews the short-lived bearer through CatalogAPI single-flight. The
CatalogAPI operation has no business body; scope and TTL are server-owned.

### Running

#### Force local mode, stdio

```bash
uv run alibabacloud-maxcompute-mcp-server --mode local
```

#### Local mode, Streamable HTTP

```bash
uv run alibabacloud-maxcompute-mcp-server --transport http --host 127.0.0.1 --port 8000
```

#### Remote mode, stdio proxy

```bash
uv run alibabacloud-maxcompute-mcp-server --config /path/to/config.json
```

Derived regional endpoints need no site field under `default`. Add
`--mode remote --remote-url https://<REMOTE_MCP_HOST>/mcp` only to override the
default entry.

Remote mode rejects `--transport http` and `--transport streamable-http`.

### MCP tools

Remote mode transparently exposes the tool list and contracts advertised by the
selected Gateway. Local mode preserves the released SDK-backed tool surface.
Local tools return JSON in an MCP text response; check `success` first, then
read `data`, `summary`, or `error`.

| Category | Tools | Purpose |
| --- | --- | --- |
| Catalog discovery | `list_projects`, `get_project`, `list_schemas`, `get_schema`, `list_tables`, `get_table_schema`, `get_partition_info` | Browse projects, schemas, tables, table schemas, table metadata, and partitions. |
| SQL and instances | `cost_sql`, `execute_sql`, `get_instance_status`, `get_instance` | Estimate query cost, run read-only SQL, poll instances, and retrieve results. |
| Search and access | `search_meta_data`, `check_access` | Search Catalog metadata under a namespace and inspect the current identity / grants. |
| Table management | `create_table`, `insert_values`, `update_table` | Create tables, insert rows, and update table comments, labels, lifecycle, and column metadata. |
| Session config | `list_configs`, `get_current_config`, `use_config` | List named configs, inspect the active config, and switch region / identity / project at runtime. |

Notes:

- `execute_sql` is read-only by design. The server validates SQL client-side
  and also submits jobs with the MaxCompute read-only hint.
- For SQL table references, call `get_table_schema` first and use the returned
  `sqlTableRef`; this handles two-level and three-level project naming.
- `search_meta_data` requires `namespaceId` / `MAXCOMPUTE_NAMESPACE_ID`.
- Large query results can be streamed to a local `file://` `output_uri`;
  otherwise responses are returned inline and may be truncated.

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
        "MAXCOMPUTE_REGION": "cn-hangzhou",
        "MAXCOMPUTE_NETWORK": "public",
        "MAXCOMPUTE_DEFAULT_PROJECT": "<DEFAULT_PROJECT_NAME>",
        "ALIBABA_CLOUD_ACCESS_KEY_ID": "<ALIBABA_CLOUD_ACCESS_KEY_ID>",
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET": "<ALIBABA_CLOUD_ACCESS_KEY_SECRET>"
      }
    }
  }
}
```

#### Cursor / Claude Code (remote mode through local stdio)

```json
{
  "mcpServers": {
    "alibabacloud-maxcompute-mcp-server": {
      "command": "uv",
      "args": [
        "--directory",
        "/absolute/path/to/alibabacloud-maxcompute-mcp-server",
        "run",
        "alibabacloud-maxcompute-mcp-server",
        "--mode",
        "remote",
        "--remote-url",
        "https://<REMOTE_MCP_HOST>/mcp"
      ],
      "env": {
        "MAXCOMPUTE_CATALOG_CONFIG": "/absolute/path/to/alibabacloud-maxcompute-mcp-server/config.json"
      }
    }
  }
}
```

For a rule-derived same-network regional endpoint, omit `--mode` and
`--remote-url` and let `default` probe it. Do not put AccessKey IDs, secrets, STS
tokens, or bearer tokens in MCP client arguments.

#### Streamable HTTP

This listener is available only in local mode. Start the server (see above),
then point your MCP client at `http://127.0.0.1:8000/mcp`.

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

- A release tag publishes the PyPI package through Trusted Publishing and then
  creates the corresponding GitHub Release; maintainers can follow the
  [publishing runbook](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/blob/master/docs/publishing.md).
- Pull requests and issues are welcome. For Remote MCP service feedback, use
  the Remote MCP issue template. For local server code changes, please open an
  issue before starting large changes.

## License

Apache License 2.0. See the
[LICENSE](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/blob/master/LICENSE).
