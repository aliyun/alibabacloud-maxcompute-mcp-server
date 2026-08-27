# Changelog

All notable changes to `alibabacloud-maxcompute-mcp-server` will be documented
here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.7] - 2026-08-27

### Added

- `MAXCOMPUTE_ALLOW_UNVERIFIED_REMOTE_URL=1` opt-in lets explicit `remote`
  mode proceed for a remote MCP hostname outside the known endpoint
  registry; the default stays fail-closed and a warning is printed when
  the opt-in is used.
- The stdio relay now runs a watchdog over the first exchange: if the
  first forwarded request receives no remote response within 30 seconds,
  the relay sends a JSON-RPC error for the stalled request, logs the
  stall, and exits non-zero instead of hanging silently.

### Fixed

- The remote stdio relay built its HTTP client without an explicit
  timeout, so the httpx five-second default applied to every request
  phase and cut off any gateway call slower than five seconds (SQL
  synchronous waits, KB/LLM answers). Only the connect phase is bounded
  now (30 seconds); reads stay open, matching the Streamable HTTP
  reverse proxy client.

### Changed

- Dependencies: `mcp` 2.0.0 -> 2.1.1, `cryptography` -> 50.0.1, `anthropic`
  (dev) 0.122.0 -> 1.1.0; `astral-sh/setup-uv` v9.0.0 -> v10.0.1 and
  `zizmorcore/zizmor-action` v0.6.1 -> v0.6.2.

## [0.1.6] - 2026-08-26

### Added

- `default` and `remote` modes now expose a transparent Streamable HTTP proxy
  for `POST`, `GET`, and `DELETE` with JSON, SSE, session resumption, and
  per-request token lookup and single-flight CatalogAPI renewal.
- Remote forwarding preserves MRTR state and input fields, progress
  notifications, unknown future MCP payload fields, and `Mcp-*` extension
  headers. Non-error Streamable HTTP messages remain byte-for-byte unchanged.

## [0.1.5] - 2026-08-26

### Added

- Remote proxy failures now preserve safe CatalogAPI and Gateway Request IDs in
  stderr diagnostics and MCP error metadata while leaving successful responses
  unchanged.

### Fixed

- Local SDK mode now requests the public CatalogAPI project view required by
  `pyodps-catalog` 0.4, restoring `get_project` and schema-model detection.

## [0.1.4] - 2026-08-26

### Added

- First PyPI distribution, published from release tags through PyPI Trusted
  Publishing without a long-lived upload token.

### Changed

- Package metadata now uses the Apache-2.0 SPDX license expression and includes
  explicit runtime compatibility and security dependency floors.
- The default remote-proxy installation no longer installs PyODPS or PyArrow.
  Local SDK mode is available through the `local` extra, where PyArrow requires
  `>=23.0.1,<26` to close `GHSA-rgxp-2hwp-jwgg`.
- CI now blocks Ruff, formatting, mypy, lockfile drift, dependency audit,
  high-severity dependency changes, workflow security findings, line coverage
  below 80%, and branch coverage below 90%.
- A pushed release tag now publishes to PyPI and then creates a GitHub Release
  from this changelog section, matching the `maxcompute-semantic` release flow.

## [0.1.3] - 2026-08-26

### Added

- `default`, `remote`, and `local` launcher modes, including the transparent
  stdio proxy to the hosted MaxCompute MCP service.
- CatalogAPI-issued short-lived MCP tokens with AK, STS, credentials URI, and
  default-credential-chain support.
- Rule-based public and VPC endpoint derivation for Mainland China and
  international Regions without a fixed Region registry.

[Unreleased]: https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/compare/v0.1.6...HEAD
[0.1.6]: https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/compare/v0.1.2...v0.1.3
