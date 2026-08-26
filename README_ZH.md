# 阿里云 MaxCompute MCP Server

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-green.svg)](https://www.python.org/downloads/)

[English](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/blob/master/README.md)

阿里云 [MaxCompute](https://www.aliyun.com/product/odps) 的本地 [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) 启动器。它既可以用 `local` 模式运行原有 SDK 实现，也可以用 `remote` 模式作为托管版 MaxCompute MCP 服务的透明 stdio 代理。

> [!IMPORTANT]
> 推荐优先使用 MaxCompute Remote MCP Server。请先阅读托管版 Remote MCP
> 服务文档：[MaxCompute MCP 服务使用文档](https://help.aliyun.com/zh/maxcompute/getting-started/mcp-overview-and-access)。
>
> 本仓库继续保留 local MCP server 代码，适用于自托管、开发调试和特殊本地
> stdio 场景。Remote MCP 推进期间，公开且不含敏感信息的反馈可以通过本仓库的
> [Remote MCP issue 模板](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/issues/new?template=remote-mcp-service-feedback.md)
> 提交。

## 能力概览

- **兼容原有配置**：通过已有 FE/Catalog endpoint 判断 Region 和 public/VPC 网络；简单 `region` + `network` 配置无需 Region 表即可生成 FE、Catalog 和 MCP endpoint。
- **透明 remote 模式**：将 stdio MCP 消息原样转发到托管版 Streamable HTTP endpoint，不重建或重命名远端工具。
- **当前协议兼容**：使用 MCP Python SDK 2.x，保留现代 `2026-07-28` 请求的 metadata 与路由 Header，同时兼容 legacy initialize。
- **标准 CatalogAPI 鉴权**：remote 代理使用现有凭证 Provider 对无 body 的 `mcpAccessToken` 请求签名，续期时重新取得当前 AK/STS/ECS RAM Role 凭证。
- **凭证隔离**：只把短期 `mcpc_` bearer 发给 Gateway，不在 MCP 消息或 URL 中发送 AK Secret / STS 凭证；选定 remote 后失败也绝不 fallback 到本地 SDK。

## Remote MCP Server（推荐）

除非你明确需要 local `stdio` 或自托管方式，否则建议优先使用托管版
MaxCompute Remote MCP Server。Remote MCP 服务减少本地运行环境和 MCP
服务端凭证配置成本，使用 Streamable HTTP，并按阿里云官方接入流程完成授权。

接入步骤、支持地域、OAuth 登录流程、工具能力和安全注意事项见：

- [MaxCompute MCP 服务使用文档](https://help.aliyun.com/zh/maxcompute/getting-started/mcp-overview-and-access)

### Remote MCP 反馈

公开且不含敏感信息的 Remote MCP 反馈可以通过本仓库 issues 提交：

- [提交 Remote MCP 反馈](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/issues/new?template=remote-mcp-service-feedback.md)
- [查看已有 issues](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/issues)

建议包含 MCP Client 名称和版本、Endpoint 类型、工具名、request ID、带时区的时间窗口、
地域、脱敏后的错误码和错误信息、期望行为、实际行为以及复现步骤。

不要在公开 issue 中包含 access token、refresh token、授权码、Cookie、AccessKey ID
或 Secret、带 query 参数的 OAuth callback URL、敏感 SQL、客户数据或敏感 Logview
内容。账号级权限、账单、SLA、生产故障、安全漏洞或包含机密数据的问题，请使用阿里云
官方支持或安全渠道。

## 本地启动器（可选）

为保持老配置升级兼容，启动器默认使用 `default`。它从所选老配置的 FE 和 Catalog
endpoint 判断 Region 和 public/VPC 网络，也支持显式 `region` + `network` 简单配置。
本进程使用 stdio 且能够确定 Region/网络时，先通过 CatalogAPI 签发 token，再对唯一的
地域 MCP endpoint 完成经过认证的 `initialize`；token 签发或 remote 初始化失败时
fallback 到原有 local 实现。`--mode local` 可以显式固定原实现。

| 模式 | MCP 路径 | 后端 | 本进程对外传输 |
| --- | --- | --- | --- |
| `default` | 启动时先探测 remote，再选择一种路径 | remote 优先；初始化失败使用本地 SDK | stdio；非 stdio 保持 local |
| `local` | MCP Client → 本进程 → 本地 SDK | MaxCompute SDK / CatalogAPI | stdio 或 Streamable HTTP |
| `remote` | MCP Client → 本进程 → 托管 Gateway | Remote MCP Streamable HTTP | 仅 stdio |

显式 `remote` 模式 fail closed：CatalogAPI 签发、MCP initialize、token 续期或
Gateway 传输失败都会终止远端流程。`default` 只在选型前 fallback；一旦认证初始化
成功并选择 remote，之后的远端会话失败绝不动态调用本地 SDK tools。

### 运行要求

本地启动器需要：

- Python 3.10+
- [`uv`](https://docs.astral.sh/uv/)（推荐的依赖管理工具）
- `local` 和本地 remote 代理都需要 AK、STS、凭证服务 URI、ECS RAM Role 或默认
  凭证链中的其他受支持凭证来源
- remote 代理调用 CatalogAPI 的标准签名接口取得 300 秒 `mcpc_` token；不保存
  refresh token
- lockfile 固定 MCP Python SDK 2.x，同时支持现代 `2026-07-28` 与协商后的 legacy
  stdio client

### 安装

可以使用 `pip` 或 `uv` 从 PyPI 安装发布版：

```bash
python -m pip install alibabacloud-maxcompute-mcp-server
# 或安装到独立环境
uv tool install alibabacloud-maxcompute-mcp-server
```

验证安装后的命令行入口：

```bash
alibabacloud-maxcompute-mcp-server --help
```

如果需要从源码开发，克隆仓库并同步全部开发依赖：

```bash
git clone https://github.com/aliyun/alibabacloud-maxcompute-mcp-server.git
cd alibabacloud-maxcompute-mcp-server
uv sync --all-extras
```

运行开发环境中的入口脚本：

```bash
uv run alibabacloud-maxcompute-mcp-server --help
```

### 配置

运行时配置优先级为：命令行、环境变量、JSON、默认值。所有来源都未选择模式时为
`default`。

| 用途 | CLI | 环境变量 | JSON |
| --- | --- | --- | --- |
| 运行模式 | `--mode default\|remote\|local` | `MAXCOMPUTE_MCP_MODE` | `mode` |
| Remote MCP 地址 | `--remote-url` | `MAXCOMPUTE_REMOTE_MCP_URL` | `remote.url` |
| 启动命名 profile | `--profile` | `MAXCOMPUTE_MCP_PROFILE` | `profile` |
| 配置文件 | `--config` | `MAXCOMPUTE_CATALOG_CONFIG` | 不适用 |

`profile` 同时决定 endpoint、CatalogAPI 客户端和签发 token 使用的凭证；remote 不会
同时加载另一个 profile 的本地 SDK tools。

复制公共示例到本地，并填入实际值：

```bash
cp config.example.json config.json
# 编辑 config.json，填写实际 Region / 网络 / project / 凭证
```

`config.json` 默认被 `.gitignore` 忽略，切勿提交。

#### MaxCompute 配置字段

| 字段 | 是否必填 | 说明 |
| --- | --- | --- |
| `maxcompute.region` | 与 `network` 同时配置 | 简单 endpoint 配置和 remote 选型使用的 Region，如 `cn-hangzhou` |
| `maxcompute.network` | 与 `region` 同时配置 | `public` 或 `vpc`；与 `region` 一起使用时无需显式配置 FE/Catalog endpoint |
| `maxcompute.maxcompute_endpoint` | 未配置 `region` + `network` 时必填 | MaxCompute FE 端点，如 `https://service.cn-hangzhou.maxcompute.aliyun.com/api` |
| `maxcompute.catalogapi_endpoint` | 可选 | 简单配置会自动生成；老配置省略时从 `maxcompute_endpoint` 推导 |
| `maxcompute.defaultProject` | 可选 | 默认项目名，作为执行上下文 |
| `maxcompute.namespaceId` | 可选 | `search_meta_data` 所需主账号 UID |
| `maxcompute.protocol` | 可选 | `https`（默认）或 `http` |
| `maxcompute.accessKeyId` / `accessKeySecret` | 可选 | 静态凭证，仅供开发调试；生产环境建议使用凭证服务 URI |

最简单的 endpoint 配置为：

```json
{
  "maxcompute": {
    "region": "cn-hangzhou",
    "network": "public",
    "defaultProject": "<DEFAULT_PROJECT_NAME>"
  }
}
```

`region` 和 `network` 会按规则生成 FE、Catalog 和 MCP endpoint。`network: "vpc"`
使用内网 FE/Catalog endpoint 和 VPC MCP endpoint。

#### local 模式凭证优先级

1. 环境变量 `ALIBABA_CLOUD_ACCESS_KEY_ID` / `ALIBABA_CLOUD_ACCESS_KEY_SECRET`（可选携带 `ALIBABA_CLOUD_SECURITY_TOKEN`）
2. `ALIBABA_CLOUD_CREDENTIALS_URI` 指向的本地凭证服务
3. 阿里云默认凭证链（环境变量 / 配置文件 / ECS RAM Role 等）
4. `config.json` 中的静态 `accessKeyId` / `accessKeySecret`（优先级最低，仅供开发）

#### 仅使用环境变量

也可以完全不写 JSON 配置，通过环境变量驱动：

| 变量 | 用途 |
| --- | --- |
| `MAXCOMPUTE_MCP_MODE` | `default`、`remote` 或 `local`；省略时为 `default` |
| `MAXCOMPUTE_REMOTE_MCP_URL` | 可选覆盖；必须与 MaxCompute endpoint 网络一致，VPC 还必须同 Region |
| `MAXCOMPUTE_MCP_PROFILE` | local/remote 启动时选择的命名 profile |
| `MAXCOMPUTE_ENDPOINT` | MaxCompute 服务端点 |
| `MAXCOMPUTE_CATALOG_API_ENDPOINT` | 可选的 Catalog API 端点覆盖 |
| `MAXCOMPUTE_REGION` | 简单 endpoint 配置或校验使用的 Region |
| `MAXCOMPUTE_NETWORK` | `public` 或 `vpc`；必须同时配置 `MAXCOMPUTE_REGION` |
| `MAXCOMPUTE_DEFAULT_PROJECT` | 默认项目名 |
| `MAXCOMPUTE_NAMESPACE_ID` | `search_meta_data` 所需 namespace ID |
| `ALIBABA_CLOUD_ACCESS_KEY_ID` / `ALIBABA_CLOUD_ACCESS_KEY_SECRET` | 静态凭证 |
| `ALIBABA_CLOUD_SECURITY_TOKEN` | 可选的 STS token |
| `ALIBABA_CLOUD_CREDENTIALS_URI` | 凭证服务 URI |

#### local 命名配置（运行时切换）

如果需要在不重启 MCP Server 的情况下切换地域、endpoint、项目或身份，可以创建本地多配置文件，例如 `config.multi.json`，并通过 `MAXCOMPUTE_CATALOG_CONFIG` 指向它：

```json
{
  "default": "beijing",
  "configs": {
    "beijing": {
      "region": "cn-beijing",
      "description": "北京生产环境",
      "maxcompute_endpoint": "https://service.cn-beijing.maxcompute.aliyun.com/api",
      "accessKeyId": "<ALIBABA_CLOUD_ACCESS_KEY_ID>",
      "accessKeySecret": "<ALIBABA_CLOUD_ACCESS_KEY_SECRET>",
      "defaultProject": "<DEFAULT_PROJECT_NAME>",
      "namespaceId": "<ALIBABACLOUD_ACCOUNT_UID>"
    },
    "singapore": {
      "region": "ap-southeast-1",
      "description": "新加坡生产环境",
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
      "description": "新加坡只读身份",
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

服务启动时使用 `default` 指定的配置；未指定 `default` 时使用第一个配置。local 模式可
通过 session 工具 `list_configs`、`get_current_config`、`use_config` 查看和切换当前
配置；这些工具不会返回 AccessKey ID、AccessKey Secret 或 STS token。两种模式都可
通过 `--profile <name>` 或 `MAXCOMPUTE_MCP_PROFILE=<name>` 在启动时选择命名配置。

每个命名配置必须提供 `maxcompute_endpoint`，或同时提供 `region` 和 `network`。老配置
省略 `catalogapi_endpoint` 时，还需要提供 `defaultProject`，以便服务通过 MaxCompute
自动解析 Catalog API endpoint。

local 当前配置是进程级状态。运行时切换更适合 stdio / 单客户端场景。在共享的
Streamable HTTP 模式下，所有客户端共享同一个当前配置，一个客户端调用 `use_config`
会影响其他客户端。remote 模式在启动时固定一个 profile，不发布 local session 切换工具。

#### default 选型与 remote 覆盖

老版本的顶层 `maxcompute`、顶层 `odps`、`configs` 命名配置以及纯环境变量配置都继续
加载。Endpoint 通过统一规则生成，不维护 Region 列表：

| 服务 | public 规则 | VPC 规则 |
| --- | --- | --- |
| FE | `https://service.<region>.maxcompute.aliyun.com/api` | `https://service.<region>-intranet.maxcompute.aliyun-inc.com/api` |
| CatalogAPI | `https://catalogapi.<region>.maxcompute.aliyun.com` | `https://catalogapi.<region>-intranet.maxcompute.aliyun-inc.com` |
| 中国内地 Region MCP（`cn-*`，不含 `cn-hongkong`） | `https://mcp.<region>.maxcompute.aliyun.com/mcp` | `https://mcp.<region>-vpc.maxcompute.aliyun-inc.com/mcp` |
| 海外 Region MCP（含 `cn-hongkong`） | `https://mcp-intl.<region>.maxcompute.aliyun.com/mcp` | `https://mcp-intl.<region>-vpc.maxcompute.aliyun-inc.com/mcp` |

对于老配置，能够识别的 FE 和 Catalog endpoint 都会提供 Region/网络证据：公网
endpoint 选择公网 MCP，已识别的 VPC/内网 endpoint 选择 VPC MCP。两者都能识别时，
必须指向相同 Region 和网络；二者冲突时绝不选择 remote。显式 `region`/`network`
与已识别 endpoint 冲突则视为配置错误。

VPC 配置绝不选择公网 MCP，也不跨 Region。只有自定义/历史 endpoint 且无法验证
Region/网络、FE/Catalog 证据冲突，或进程使用本地 HTTP transport 时，`default` 保持
原 local 实现；显式 `remote` 模式 fail closed。

MCP 域名只按 Region 决定：中国内地 Region（`cn-*`，但不包括 `cn-hongkong`）使用
`mcp`，`cn-hongkong` 和其他海外 Region 使用 `mcp-intl`。这不是账号站点探测；
CatalogAPI 签发 token 的链路不进入 RAM OAuth。生成的 endpoint 只在进程启动时完成
一次认证初始化，之后固定到进程结束；CatalogAPI token 续期不会重新初始化。显式配置
`remote.url` 时只使用该 URL；VPC 仍必须同 Region。

最小 remote 覆盖 JSON 仍复用原 MaxCompute 配置和凭证：

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

remote URL 必须是无 userinfo、query、fragment 且路径严格为 `/mcp` 的 resource。
代理拒绝 redirect、忽略环境 HTTP proxy，并以 single-flight 方式通过 CatalogAPI
续期短期 bearer token。CatalogAPI 接口无业务 body，scope 和 TTL 由服务端固定。

### 运行

#### 强制 local 模式，stdio

```bash
uv run alibabacloud-maxcompute-mcp-server --mode local
```

#### local 模式，Streamable HTTP

```bash
uv run alibabacloud-maxcompute-mcp-server --transport http --host 127.0.0.1 --port 8000
```

#### remote 模式，stdio 代理

```bash
uv run alibabacloud-maxcompute-mcp-server --config /path/to/config.json
```

按规则生成的地域 endpoint 在 `default` 下无需增加站点字段。仅在需要覆盖默认入口时增加
`--mode remote --remote-url https://<REMOTE_MCP_HOST>/mcp`。

remote 模式会拒绝 `--transport http` 和 `--transport streamable-http`。

### MCP 工具

remote 模式透明发布所选 Gateway 返回的 tool list 和合同。local 模式保留已发布的
SDK 工具面。local 工具通过 MCP text 响应返回 JSON；调用方应先检查 `success`，再读取
`data`、`summary` 或 `error`。

| 分类 | 工具 | 用途 |
| --- | --- | --- |
| Catalog 发现 | `list_projects`, `get_project`, `list_schemas`, `get_schema`, `list_tables`, `get_table_schema`, `get_partition_info` | 浏览项目、schema、表、表结构、表元数据与分区 |
| SQL 与实例 | `cost_sql`, `execute_sql`, `get_instance_status`, `get_instance` | 预估查询成本、执行只读 SQL、轮询实例状态、获取结果 |
| 搜索与权限 | `search_meta_data`, `check_access` | 在 namespace 下搜索 Catalog 元数据，并查看当前身份 / 授权 |
| 表管理 | `create_table`, `insert_values`, `update_table` | 建表、插入数据、更新表注释、标签、生命周期和列元数据 |
| Session 配置 | `list_configs`, `get_current_config`, `use_config` | 列出命名配置、查看当前配置、运行时切换地域 / 身份 / 项目 |

注意事项：

- `execute_sql` 只允许只读查询。服务端会先做 SQL 类型校验，并在提交 MaxCompute 作业时
  强制带上只读 hint
- 生成 SQL 前建议先调用 `get_table_schema`，直接使用返回的 `sqlTableRef`；它会处理
  二级 / 三级模型下表名引用差异
- `search_meta_data` 依赖 `namespaceId` / `MAXCOMPUTE_NAMESPACE_ID`
- 大结果集可通过本地 `file://` `output_uri` 流式写盘；不传时结果以内联方式返回，超过
  上限会被截断

### MCP 客户端接入

#### Cursor / Claude Code（stdio，使用配置文件）

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

#### Cursor / Claude Code（stdio，仅环境变量）

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

#### Cursor / Claude Code（通过本地 stdio 使用 remote 模式）

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

按规则生成的同网络 Region endpoint 可以省略 `--mode` 和 `--remote-url`，由 `default`
探测选择。不要把 AccessKey ID、Secret、STS token 或 bearer token 放在 MCP Client
args 中。

#### Streamable HTTP

该 listener 仅在 local 模式可用。按上文启动后，将 MCP Client 地址指向
`http://127.0.0.1:8000/mcp`。

### 开发

```bash
uv sync --all-extras
uv run pytest tests/ -q
uv build
```

#### 包名与模块名

| 名称 | 上下文 |
| --- | --- |
| `alibabacloud-maxcompute-mcp-server` | pip 包名、CLI 入口、仓库名 |
| `maxcompute_catalog_mcp` | Python 导入路径（`from maxcompute_catalog_mcp import ...`） |

导入模块名早于公开包名产生，为保持向后兼容而保留。

## 参与贡献

- PyPI 包会通过 release tag 和 Trusted Publishing 发布，成功后自动创建
  GitHub Release；维护者请按
  [发布手册](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/blob/master/docs/publishing.md)
  操作
- 欢迎提交 Pull Request 和 Issue。Remote MCP 服务反馈请使用 Remote MCP issue 模板；
  local server 代码较大改动请先开 Issue 讨论

## 开源协议

Apache License 2.0。详见
[LICENSE](https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/blob/master/LICENSE)。
