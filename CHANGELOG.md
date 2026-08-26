# Changelog

All notable changes to `alibabacloud-maxcompute-mcp-server` will be documented
here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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

[Unreleased]: https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/compare/v0.1.4...HEAD
[0.1.4]: https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/aliyun/alibabacloud-maxcompute-mcp-server/compare/v0.1.2...v0.1.3
