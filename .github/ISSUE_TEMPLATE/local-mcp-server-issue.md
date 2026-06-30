---
name: Local MCP server issue
about: Report an issue with the local/self-hosted MaxCompute MCP server in this repository
title: "[LOCAL] "
labels: local, needs-triage
assignees: ""
---

This template is for the local or self-hosted MaxCompute MCP server code in
this repository. For hosted MaxCompute Remote MCP service feedback, use the
Remote MCP service feedback template instead.

Do not include AccessKey IDs, AccessKey secrets, STS tokens, bearer tokens,
customer data, sensitive SQL, or sensitive Logview content.

## Issue type

- [ ] Bug in local MCP server
- [ ] Local setup or configuration issue
- [ ] Local server tool or feature request
- [ ] Documentation feedback for local setup

## Environment

- OS:
- Python version:
- `uv` version:
- Repository commit:
- Transport: stdio / Streamable HTTP
- MCP client name and version:

## Configuration shape

Describe the local configuration shape without secrets.

- Uses `config.json`: yes / no
- Uses environment variables only: yes / no
- Uses `ALIBABA_CLOUD_CREDENTIALS_URI`: yes / no
- MaxCompute endpoint region:

## Tool or workflow

- MCP tool name, if related to an existing tool:
- Sanitized error code/message:

## Description

Describe what happened.

## Expected behavior

Describe what you expected to happen.

## Actual behavior

Describe the actual behavior. Include only sanitized output.

## Reproduction steps

1.
2.
3.

## Redaction checklist

- [ ] I removed AccessKey IDs, AccessKey secrets, STS tokens, and bearer tokens.
- [ ] I removed customer data, sensitive SQL, and sensitive Logview content.
- [ ] I did not include local credential files or private configuration.
