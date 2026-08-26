# PyPI publishing readiness

Date: 2026-08-26 (Asia/Shanghai)
Repository baseline reviewed: `95e01ed861e273b4f42214ea4906ed5b8337222e`
(`v0.1.3`, then `origin/master`)

## Conclusion

The repository is technically publishable to PyPI, and its intended distribution
name has no publicly visible owner at the time of this check. It is not safe to
publish the existing `v0.1.3` tag: that tag builds package version `0.1.1`, not
`0.1.3`, and the released README explicitly says that no PyPI package is
available.

The recommended first PyPI release is `v0.1.4`, prepared on a new commit with
matching package metadata, updated installation documentation, and a dedicated
Trusted Publishing workflow. Do not move or rebuild the already published
`v0.1.3` Git tag.

This is a technical readiness conclusion. The repository's Apache-2.0 license
permits redistribution but does not grant trademark rights; an authorized
Alibaba Cloud/Aliyun release owner should confirm the branded distribution name
and own the PyPI project.

## Name availability and collision boundary

[PyPA name normalization](https://packaging.python.org/en/latest/specifications/name-normalization/)
lowercases names and replaces every run of `.`, `_`, or `-` with one `-`.
Consequently, all of these spellings are the same PyPI project:

- `alibabacloud-maxcompute-mcp-server`
- `alibabacloud_maxcompute_mcp_server`
- `alibabacloud.maxcompute.mcp.server`
- `AlibabaCloud-MaxCompute-MCP-Server`

Read-only checks on 2026-08-26 returned HTTP 404 for every spelling from both
the production [PyPI JSON endpoint](https://pypi.org/pypi/alibabacloud-maxcompute-mcp-server/json)
and [Simple API endpoint](https://pypi.org/simple/alibabacloud-maxcompute-mcp-server/).
The production JSON and Simple endpoints for the underscore, dotted, and
mixed-case variants also returned 404. The corresponding
[TestPyPI JSON endpoint](https://test.pypi.org/pypi/alibabacloud-maxcompute-mcp-server/json)
and [Simple endpoint](https://test.pypi.org/simple/alibabacloud-maxcompute-mcp-server/)
returned 404 as well.

Therefore, no existing public project or third-party owner is visible. This is
not a reservation or a guarantee that the first upload will succeed. PyPI can
reject an apparently unused name because it is already registered without a
release, is confusable with another project, is prohibited, or conflicts with a
standard-library name; see the
[PyPI project-name FAQ](https://pypi.org/help/#project-name). A pending Trusted
Publisher also does not reserve the name, and becomes invalid if someone else
registers it first. Recheck the JSON and Simple endpoints immediately before the
first publication and minimize the delay between publisher setup and upload.

## Repository findings at the reviewed baseline

### Blockers

1. **Release and package versions disagree.** `v0.1.0` contains package version
   `0.1.0`, and `v0.1.1` contains `0.1.1`; both `v0.1.2` and `v0.1.3` still
   contain `version = "0.1.1"`. A Git tag does not override the core metadata in
   the built files. Building `v0.1.3` produced
   `alibabacloud_maxcompute_mcp_server-0.1.1.tar.gz` and
   `alibabacloud_maxcompute_mcp_server-0.1.1-py3-none-any.whl`.

2. **The public README contradicts a PyPI release.** It says that PyPI and
   standalone tarballs are unavailable, provides only a source checkout install,
   and repeats the no-PyPI statement under Contributing. These statements must
   be replaced with `pip`/`uv tool` installation and a separate source-development
   path. Its relative `README_ZH.md` and `LICENSE` links should become absolute
   repository links because PyPI renders the README outside GitHub.

3. **There is no release publishing path.** The only workflow at the baseline is
   `.github/workflows/ci.yml`, triggered for `master` pushes and pull requests.
   It tests Python 3.10/3.11/3.12 and runs `uv build`, but does not retain a
   canonical artifact, validate the tag-to-version relationship, or publish.

### Packaging metadata

The existing project name, summary, Python requirement, entry point, classifiers,
URLs, package selection, and Hatchling backend are structurally valid. All ten
runtime dependency requirements resolved from production PyPI in an isolated
Python 3.10 environment on 2026-08-26; this includes the less common
`pyodps-catalog`, `maxcompute-tea-openapi`, `maxcompute-tea-util`, and `httpx2`
distributions.

The README is already selected by `readme = "README.md"`; Hatchling generated
`Description-Content-Type: text/markdown`, and strict Twine rendering validation
passed. PyPA recommends `twine check` for this purpose; see
[Making a PyPI-friendly README](https://packaging.python.org/en/latest/guides/making-a-pypi-friendly-readme/).

The full `LICENSE` file is already present in both the sdist and wheel, but the
source metadata uses the deprecated `license = { text = "Apache-2.0" }` form.
Before the first release, use the PEP 639 form:

```toml
[project]
license = "Apache-2.0"
license-files = ["LICENSE"]

[build-system]
requires = ["hatchling>=1.27"]
```

Remove the legacy `License :: OSI Approved :: Apache Software License`
classifier when opting into `License-Expression`; PyPA describes the legacy
table/classifier forms as deprecated and records Hatchling 1.27.0 as the first
version supporting the new fields in
[Writing `pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/#license-and-license-files).
The present `hatchling>=1.24` lower bound is too low for this migration even if a
current isolated build happens to select a newer backend.

At the reviewed baseline, the compatibility and runtime security floors under
`[tool.uv].constraint-dependencies` were project-sync constraints rather than
`Requires-Dist` metadata, so a normal `pip install` from PyPI would not enforce
them. The release preparation moves the `pyarrow` compatibility range and the
runtime security floors into `[project].dependencies`; only the development-only
`pygments` floor remains under `[tool.uv]`.

## Recommended Trusted Publishing setup

Use PyPI Trusted Publishing instead of a long-lived upload token. For a new
project, an eligible PyPI user with a verified email and 2FA should create a
[pending publisher](https://pypi.org/manage/account/publishing/) with these exact
values:

| Field | Value |
| --- | --- |
| PyPI project name | `alibabacloud-maxcompute-mcp-server` |
| GitHub owner | `aliyun` |
| GitHub repository | `alibabacloud-maxcompute-mcp-server` |
| Workflow filename | `publish.yml` |
| Environment | `pypi` |

The project name, repository owner/name, top-level workflow filename, and
environment claim must match exactly. The first successful upload creates the
project and converts the pending publisher into a normal publisher; see
[Creating a project through OIDC](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/)
and [Trusted Publisher troubleshooting](https://docs.pypi.org/trusted-publishers/troubleshooting/).
PyPI requires a verified email to register a project or upload files, as stated
in its [account FAQ](https://pypi.org/help/#verified-email).

Create a GitHub `pypi` environment with:

- required release-owner reviewers;
- prevent-self-review enabled;
- deployments restricted to the intended `v*` release tags;
- administrator bypass disabled if the repository policy permits it.

GitHub documents these controls under
[Deployments and environments](https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments).
The PyPA publishing guide specifically requires manual approval for the
production `pypi` environment.

Use a dedicated, non-reusable top-level workflow. To align with the established
`maxcompute-semantic` release contract, the prepared workflow uses one protected
publish job: a pushed `v*` tag builds and checks the distributions, publishes
those exact files through Trusted Publishing, and then a second job creates the
GitHub Release from `CHANGELOG.md`. The publish job checks that the tag commit
belongs to `master`, verifies `vX.Y.Z` equals `[project].version`, runs
`twine check --strict`, and smoke-tests the wheel before upload.

A separate read-only build job would isolate OIDC permission more strictly, but
would diverge from the requested shared CI convention. The aligned workflow
instead protects the entire publish job with the `pypi` environment, disables
dependency caching, and pins every action to a reviewed commit SHA.

The essential permission boundary is:

```yaml
publish:
  environment: pypi
  permissions:
    contents: read
    id-token: write
```

`id-token: write` allows the job to request an OIDC identity; it does not grant
repository write access. Keep it at job scope. The official flow is documented in
[Publishing with a Trusted Publisher](https://docs.pypi.org/trusted-publishers/using-a-publisher/)
and the
[PyPA GitHub Actions publishing guide](https://packaging.python.org/en/latest/guides/publishing-package-distribution-releases-using-github-actions-ci-cd-workflows/).

`pypa/gh-action-pypi-publish` generates and uploads PEP 740 attestations by
default when using Trusted Publishing, so this flow does not need a separate
long-lived signing key or GitHub `attestations: write` permission. Keep
`skip-existing` at its default `false` so an accidental duplicate fails loudly.
The [official action](https://github.com/pypa/gh-action-pypi-publish) recommends
pinning actions to immutable commit SHAs for stronger supply-chain
reproducibility; review and update the pins deliberately.

## API-token fallback

Use an API token only if Trusted Publishing is unavailable after its exact
claims have been debugged. PyPI requires username `__token__` and the complete
`pypi-...` token as the password. A CI token should have the narrowest possible
project scope and be stored as a protected GitHub environment secret, never in
the repository. See the [PyPI API-token FAQ](https://pypi.org/help/#apitoken).

For a truly new project, a project-scoped token cannot be created until the
project exists. The fallback is therefore a temporary account-wide token for
the first upload, followed immediately by revocation and replacement with a
project-scoped token. PyPI and TestPyPI need separate accounts/tokens. This
fallback has greater secret exposure and is not recommended for this GitHub
repository while Trusted Publishing is supported.

## TestPyPI boundary

TestPyPI is useful for exercising rendering and upload mechanics, but it is not
a staging namespace for production PyPI:

- it has a separate account and project database;
- it needs its own pending publisher and preferably a distinct `testpypi`
  GitHub environment;
- its database is periodically pruned;
- its project name does not reserve the production name;
- its dependency population differs from production PyPI.

These limitations are documented in
[Using TestPyPI](https://packaging.python.org/en/latest/guides/using-testpypi/).
If installation from TestPyPI needs production dependencies, use a deliberately
controlled two-index test or install the dependencies from production first and
then install the exact TestPyPI artifact with `--no-deps`. Do not treat a
successful TestPyPI upload as evidence of production ownership or availability.
Use separate publish jobs; the official publish action does not support invoking
it twice in one job.

## Release and verification sequence

1. Prepare `v0.1.4`: set `[project].version = "0.1.4"`, update `uv.lock`, adopt
   PEP 639 license metadata/backend floor, update both READMEs, and add the
   release workflow/runbook.
2. Build from a clean checkout of the exact candidate commit. Run the complete
   test matrix, `uv build`, and `twine check --strict dist/*`.
3. Install both the wheel and the sdist into empty environments from production
   PyPI dependencies; verify import metadata and
   `alibabacloud-maxcompute-mcp-server --help` on supported Python versions.
4. Record SHA-256 for the sdist and wheel. The release job must check and publish
   the same files without rebuilding between those steps.
5. Merge to `master`, require green CI on that exact commit, then create and push
   `v0.1.4` at that commit. Approve the protected `pypi` environment only after
   checking the source SHA, tag/version equality, changelog, and expected package
   name. The workflow publishes PyPI first and creates the GitHub Release only
   after publication succeeds.
6. After upload, read
   `https://pypi.org/pypi/alibabacloud-maxcompute-mcp-server/0.1.4/json` and
   verify version, filenames, `requires_python`, dependency metadata, hashes,
   project URLs, and attestations. Finally install
   `alibabacloud-maxcompute-mcp-server==0.1.4` from production PyPI in a clean
   environment and run the CLI smoke test.

PyPI does not permit a distribution filename to be reused even after deletion;
fixes require a new version and newly built files. See the
[PyPI filename-reuse FAQ](https://pypi.org/help/#file-name-reuse) and
[JSON API](https://docs.pypi.org/api/json/) for post-upload metadata and hash
verification.

## Local read-only verification performed

Against baseline `95e01ed`:

- `uv build` produced a 350 KiB sdist and a 78 KiB universal wheel, both marked
  `0.1.1`.
- `twine check --strict` passed for both files.
- Two consecutive builds on the same host were byte-identical:
  - wheel SHA-256:
    `6347da3b23831474f6f9807b815cadb7a18faff51a16fee2065e819fba1d8b26`
  - sdist SHA-256:
    `ebe758909a52b3446ac642780de0a8cb75abe44806f94d8736c134cc88c49f1a`
- Both the wheel and sdist installed from a clean Python 3.10 environment using
  production PyPI dependencies; isolated import, installed metadata version,
  and CLI `--help` succeeded.
- The wheel contained the full Python package, console-script entry point,
  metadata, and `LICENSE`. The sdist could build the wheel and contained the
  required source/metadata/license files.

The matching hashes demonstrate repeatability for this source and local build
environment, not universal reproducibility across arbitrary future backend and
runner versions. The release workflow should pin actions, require the correct
Hatchling capability floor, and publish the same distributions it checks rather
than rebuilding them.
