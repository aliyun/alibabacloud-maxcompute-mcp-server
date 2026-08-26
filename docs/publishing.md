# Publishing to PyPI

This repository publishes `alibabacloud-maxcompute-mcp-server` when a release
tag is pushed, then creates the corresponding GitHub Release from
`CHANGELOG.md`. It uses PyPI Trusted Publishing and does not use a PyPI API
token, username, or password. This sequence is aligned with the
`maxcompute-semantic` release workflow.

## One-time setup

The normalized project name was not registered on PyPI when this runbook was
prepared. A PyPI owner must create a pending Trusted Publisher with these exact
values before the first release:

| Field | Value |
| --- | --- |
| PyPI project name | `alibabacloud-maxcompute-mcp-server` |
| GitHub owner | `aliyun` |
| GitHub repository | `alibabacloud-maxcompute-mcp-server` |
| Workflow filename | `publish.yml` |
| Environment name | `pypi` |

The pending publisher creates the PyPI project when the first matching workflow
publishes successfully. It does not reserve the project name, so perform the
live name check again immediately before configuring the publisher.

In GitHub repository settings, create the `pypi` environment. Configure its
deployment protection rules so that only release maintainers can approve a
production publication. The workflow grants `id-token: write` only to the
environment-protected publish job. All actions use reviewed immutable commit
SHAs, and dependency caching is disabled for the release job.

## Version policy

PyPI distribution versions are immutable. Do not rebuild an existing version
or publish a historical tag whose `pyproject.toml` version does not match the
tag. In particular, the repository's `v0.1.3` tag declares package version
`0.1.1`, so the first PyPI release must not be built from that tag. Version
`0.1.4` is the first release prepared for PyPI.

## Release procedure

1. Set `[project].version` in `pyproject.toml` to the intended release version
   and update `uv.lock`.
2. Build and verify the package locally:

   ```bash
   uv run pytest tests/ -q
   uv run coverage json -o coverage.json
   uv run python scripts/check_coverage.py coverage.json \
     --line-fail-under 80 --branch-fail-under 90
   uv run ruff check maxcompute_catalog_mcp tests scripts
   uv run ruff format --check maxcompute_catalog_mcp tests scripts
   uv run mypy maxcompute_catalog_mcp
   uv build
   uvx --from twine twine check --strict dist/*
   uvx check-wheel-contents dist/*.whl
   ```

3. Merge the release preparation to `master` and require the repository CI to
   pass on the exact merge commit.
4. Recheck that the intended version does not exist on PyPI and that the
   Trusted Publisher fields still match this repository and workflow.
5. Create and push tag `vX.Y.Z` at the exact green `master` commit. The tag
   triggers `.github/workflows/publish.yml`, matching the release flow used by
   `maxcompute-semantic`.
6. Approve the `pypi` environment deployment after checking the source commit,
   tag/version equality, changelog entry, and expected package name. Wait for
   the publish job to succeed.
7. Wait for the workflow to create the GitHub Release from `CHANGELOG.md`, then
   verify the PyPI project page and install the wheel in a new environment:

   ```bash
   python -m venv /tmp/maxcompute-mcp-release-check
   /tmp/maxcompute-mcp-release-check/bin/python -m pip install \
     alibabacloud-maxcompute-mcp-server==X.Y.Z
   /tmp/maxcompute-mcp-release-check/bin/alibabacloud-maxcompute-mcp-server --help
   ```

Do not publish from a local workstation and do not add a long-lived PyPI token
to GitHub Actions secrets.

## Failed or incorrect releases

PyPI does not allow a released filename or version to be replaced. If a
published release is unusable, yank it on PyPI and publish a new patch version;
do not delete and recreate the same version. A yanked release remains available
for exact version pins but is ignored by normal dependency resolution.

## References

- [PyPI: Using a Trusted Publisher](https://docs.pypi.org/trusted-publishers/using-a-publisher/)
- [PyPI: Creating a project through OIDC](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/)
- [Python Packaging User Guide: Packaging Python projects](https://packaging.python.org/en/latest/tutorials/packaging-projects/)
- [PyPA publish action](https://github.com/pypa/gh-action-pypi-publish)
