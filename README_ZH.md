# 阿里云 MaxCompute MCP Server

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-green.svg)](https://www.python.org/downloads/)

[English](README.md)

基于 [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) 的阿里云 [MaxCompute](https://www.aliyun.com/product/odps) 服务端。将 Catalog API 与计算能力封装为 MCP 工具，供 Cursor、Claude Code 等 AI 助手通过 stdio 或 Streamable HTTP 列出项目/schema/表、搜索元数据、预估与执行 SQL、管理 MaxCompute 实例。

## 能力概览

- **Catalog**：列出项目 / schema / 表；获取项目、schema、表及分区详情
- **搜索**：`search_meta_data` 元数据搜索（依赖 `namespaceId`）
- **计算**：SQL 成本预估、只读 SQL 执行、实例状态与结果查询
- **表管理**：建表、按行插入（via PyODPS）
- **表元数据**：更新注释、标签、生命周期、列描述（`update_table`）
- **身份与权限**：`check_access` 合并身份发现与权限查询
- **传输层**：stdio（默认，适合 IDE 集成）与内置 Streamable HTTP（无需 mcp-proxy）

## 运行要求

- Python 3.10+
- [`uv`](https://docs.astral.sh/uv/)（推荐的依赖管理工具）
- 可访问的 MaxCompute 项目以及 AK / STS 凭证 / 凭证服务 URI

## 源码安装

首个开源版本仅以源码仓形式发布。本阶段不提供 PyPI 与 standalone 独立发行版。

```bash
git clone https://github.com/aliyun/alibabacloud-maxcompute-mcp-server.git
cd alibabacloud-maxcompute-mcp-server
uv sync
```

验证入口脚本：

```bash
uv run alibabacloud-maxcompute-mcp-server --help
```

## 配置

复制公共示例到本地，并填入实际值：

```bash
cp config.example.json config.json
# 编辑 config.json，填写 endpoint / project / 凭证
```

`config.json` 默认被 `.gitignore` 忽略，切勿提交。

### 配置字段

| 字段 | 是否必填 | 说明 |
| --- | --- | --- |
| `maxcompute.maxcompute_endpoint` | 是 | MaxCompute 服务端点，如 `https://service.cn-hangzhou.maxcompute.aliyun.com/api` |
| `maxcompute.catalogapi_endpoint` | 可选 | Catalog API 端点。未配置时从 `maxcompute_endpoint` 推导 |
| `maxcompute.defaultProject` | 可选 | 默认项目名，作为执行上下文 |
| `maxcompute.namespaceId` | 可选 | `search_meta_data` 所需主账号 UID |
| `maxcompute.protocol` | 可选 | `https`（默认）或 `http` |
| `maxcompute.accessKeyId` / `accessKeySecret` | 可选 | 静态凭证，仅供开发调试；生产环境建议使用凭证服务 URI |

### 凭证优先级

1. 环境变量 `ALIBABA_CLOUD_ACCESS_KEY_ID` / `ALIBABA_CLOUD_ACCESS_KEY_SECRET`（可选携带 `ALIBABA_CLOUD_SECURITY_TOKEN`）
2. `ALIBABA_CLOUD_CREDENTIALS_URI` 指向的本地凭证服务
3. 阿里云默认凭证链（环境变量 / 配置文件 / ECS RAM Role 等）
4. `config.json` 中的静态 `accessKeyId` / `accessKeySecret`（优先级最低，仅供开发）

### 仅使用环境变量

也可以完全不写 JSON 配置，通过环境变量驱动：

| 变量 | 用途 |
| --- | --- |
| `MAXCOMPUTE_ENDPOINT` | MaxCompute 服务端点 |
| `MAXCOMPUTE_CATALOG_API_ENDPOINT` | 可选的 Catalog API 端点覆盖 |
| `MAXCOMPUTE_DEFAULT_PROJECT` | 默认项目名 |
| `MAXCOMPUTE_NAMESPACE_ID` | `search_meta_data` 所需 namespace ID |
| `ALIBABA_CLOUD_ACCESS_KEY_ID` / `ALIBABA_CLOUD_ACCESS_KEY_SECRET` | 静态凭证 |
| `ALIBABA_CLOUD_SECURITY_TOKEN` | 可选的 STS token |
| `ALIBABA_CLOUD_CREDENTIALS_URI` | 凭证服务 URI |

## 运行

### stdio（默认）

```bash
uv run alibabacloud-maxcompute-mcp-server
```

### Streamable HTTP

```bash
uv run alibabacloud-maxcompute-mcp-server --transport http --host 127.0.0.1 --port 8000
```

## MCP 客户端接入

### Cursor / Claude Code（stdio，使用配置文件）

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

### Cursor / Claude Code（stdio，仅环境变量）

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

先按上文启动服务端，再将 MCP 客户端地址指向 `http://127.0.0.1:8000/mcp`。

## 开发

```bash
uv sync --all-extras
uv run pytest tests/ -q
uv build
```

### 包名与模块名

| 名称 | 上下文 |
| --- | --- |
| `alibabacloud-maxcompute-mcp-server` | pip 包名、CLI 入口、仓库名 |
| `maxcompute_catalog_mcp` | Python 导入路径（`from maxcompute_catalog_mcp import ...`） |

导入模块名早于公开包名产生，为保持向后兼容而保留。

## 参与贡献

- 这是首个开源源码版本。本阶段**不**提供 PyPI 包和 GitHub Release 构件
- 欢迎提交 Pull Request 和 Issue。进行较大改动前请先开 Issue 讨论

## 开源协议

Apache License 2.0。详见 [LICENSE](LICENSE)。
