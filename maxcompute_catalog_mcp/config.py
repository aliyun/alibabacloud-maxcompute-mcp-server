"""MaxCompute Catalog MCP config.

Configuration methods (recommended for MCP):

1. **Config file** (path from env or CLI)
   - Env: MAXCOMPUTE_CATALOG_CONFIG → path to JSON file.
   - CLI: alibabacloud-maxcompute-mcp-server --config /path/to/config.json
   - File format: see config.example.json (key "maxcompute").

2. **Environment variables** (override file, or use without file)
   - Canonical names only (no alternate spellings):
     MAXCOMPUTE_CATALOG_CONFIG           - config file path
     MAXCOMPUTE_CATALOG_API_ENDPOINT      - Catalog API URL (optional, auto-resolved if not set)
     MAXCOMPUTE_ENDPOINT                  - maxcompute (pyodps) URL
     MAXCOMPUTE_PROTOCOL                  - global transport protocol: "https" | "http" | ""
     ALIBABA_CLOUD_ACCESS_KEY_ID         - access key ID
     ALIBABA_CLOUD_ACCESS_KEY_SECRET     - access key secret
     ALIBABA_CLOUD_SECURITY_TOKEN        - STS security token (optional)
     ALIBABA_CLOUD_CREDENTIALS_URI       - one option in the default credential chain; used when AK/SK is not set
     MAXCOMPUTE_DEFAULT_PROJECT          - default project name
     MAXCOMPUTE_NAMESPACE_ID             - optional; main account UID for Catalog search (namespaces/:search)
   - Credentials: AK/SK from config/env takes priority; otherwise Alibaba Cloud default credential chain is used.
   - catalogapi_endpoint: auto-resolved via the existing ODPS client in build_tools() if not explicitly set.
   - protocol: explicit override of transport scheme for both planes. When empty,
     scheme is inferred per-client from the embedded scheme of each endpoint
     (catalogapi falls back to maxcompute scheme), defaulting to "https".
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .tools_common import _env


_ALLOWED_PROTOCOLS = ("", "http", "https")


@dataclass(frozen=True)
class MaxComputeCatalogConfig:
    """Config fields match config.example.json under key \"maxcompute\"."""

    catalogapi_endpoint: str
    maxcompute_endpoint: str
    access_key_id: str
    access_key_secret: str
    security_token: str = ""  # STS temporary token; empty for non-STS auth
    default_project: str = ""
    namespace_id: str = ""  # main account UID for Catalog API namespaces/:search
    protocol: str = ""  # "" | "http" | "https"; empty means infer per-client


@dataclass(frozen=True)
class ResolvedEndpoints:
    """Per-client protocol + normalised endpoint values derived from config."""

    maxcompute_protocol: str   # "http" | "https"
    maxcompute_url: str        # full URL including scheme
    catalogapi_protocol: str   # "http" | "https"
    catalogapi_host: str       # bare host (no scheme)


def split_scheme(endpoint: str) -> Tuple[Optional[str], str]:
    """Split an endpoint into (scheme, host).

    Returns (scheme_lower, host_without_scheme) when a case-insensitive
    https:// or http:// prefix is present; otherwise (None, endpoint_stripped).
    Whitespace is trimmed.
    """
    s = (endpoint or "").strip()
    low = s.lower()
    for prefix in ("https://", "http://"):
        if low.startswith(prefix):
            return prefix[:-3], s[len(prefix):].strip()
    return None, s


def resolve_protocol_and_endpoints(cfg: MaxComputeCatalogConfig) -> ResolvedEndpoints:
    """Pure resolver: derive per-client protocol and normalised endpoints.

    Priority chains (see openspec/changes/unify-endpoint-ssl-protocol):
      maxcompute_protocol:
        1. cfg.protocol (if non-empty)
        2. maxcompute_endpoint embedded scheme
        3. "https"
      catalogapi_protocol:
        1. cfg.protocol (if non-empty)
        2. catalogapi_endpoint embedded scheme
        3. maxcompute_endpoint embedded scheme (follow maxcompute)
        4. "https"
    """
    explicit = (cfg.protocol or "").strip().lower()
    mc_scheme, mc_host = split_scheme(cfg.maxcompute_endpoint)
    cat_scheme, cat_host = split_scheme(cfg.catalogapi_endpoint)

    if explicit in ("http", "https"):
        mc_proto = explicit
        cat_proto = explicit
    else:
        mc_proto = mc_scheme or "https"
        cat_proto = cat_scheme or mc_scheme or "https"

    # maxcompute always needs a schemed full URL for pyodps
    maxcompute_url = f"{mc_proto}://{mc_host}" if mc_host else ""
    return ResolvedEndpoints(
        maxcompute_protocol=mc_proto,
        maxcompute_url=maxcompute_url,
        catalogapi_protocol=cat_proto,
        catalogapi_host=cat_host,
    )


def resolve_catalogapi_endpoint_with_client(
    odps_client: Any,
    maxcompute_url: str,
) -> str:
    """Resolve Catalog API endpoint via a signed GET request using an existing ODPS client.

    Raises ValueError on failure.
    """
    base = (maxcompute_url or "").strip().rstrip("/")
    if not base:
        raise ValueError("maxcompute_url is empty")
    url = f"{base}/catalogapi"
    try:
        resp = odps_client.rest.get(url, timeout=10.0)
    except Exception as e:
        raise ValueError(f"Can't get catalog api server address: {e}") from e

    if not getattr(resp, "ok", True):
        status = getattr(resp, "status_code", getattr(resp, "status", None))
        raise ValueError(f"Can't get catalog api server address: HTTP {status}")

    body = (resp.text if hasattr(resp, "text") else resp.content.decode("utf-8")).strip()
    if not body:
        raise ValueError("Can't get catalog api server address: empty response body")
    return body


def load_config(config_path: str | None = None) -> MaxComputeCatalogConfig:
    """Load config from file and environment variables.

    Does not auto-resolve catalogapi_endpoint; if unset, it is resolved in build_tools()
    after the ODPS client is created.
    """
    path = Path(config_path or _env("MAXCOMPUTE_CATALOG_CONFIG") or "config.json")
    conn: Dict[str, Any] = {}
    if path.exists():
        cfg_json = json.loads(path.read_text(encoding="utf-8")) or {}
        conn = (cfg_json.get("maxcompute") or cfg_json.get("odps")) or {}

    def pick(*keys: str, from_env: str = "") -> str:
        v = _env(from_env) if from_env else ""
        if not v:
            for k in keys:
                v = conn.get(k) or ""
                if v:
                    break
        return (v or "").strip() if isinstance(v, str) else str(v or "")

    catalogapi_endpoint = pick(
        "catalogapi_endpoint", "catalogapiEndpoint", "endpoint",
        from_env="MAXCOMPUTE_CATALOG_API_ENDPOINT",
    )
    maxcompute_endpoint = pick(
        "maxcompute_endpoint", "maxcomputeEndpoint", "sdkEndpoint",
        from_env="MAXCOMPUTE_ENDPOINT",
    )
    default_project = pick("defaultProject", "default_project", from_env="MAXCOMPUTE_DEFAULT_PROJECT")
    namespace_id = pick("namespaceId", "namespace_id", "account_uid", from_env="MAXCOMPUTE_NAMESPACE_ID")
    protocol_raw = pick("protocol", from_env="MAXCOMPUTE_PROTOCOL")
    protocol = protocol_raw.lower()
    if protocol not in _ALLOWED_PROTOCOLS:
        raise ValueError(
            f"Invalid protocol value {protocol_raw!r}; allowed: "
            f"{{'', 'http', 'https'}}"
        )

    # resolve credentials
    access_key_id = pick("accessKeyId", "access_key_id", from_env="ALIBABA_CLOUD_ACCESS_KEY_ID")
    access_key_secret = pick("accessKeySecret", "access_key_secret", from_env="ALIBABA_CLOUD_ACCESS_KEY_SECRET")
    security_token = pick("securityToken", "security_token", from_env="ALIBABA_CLOUD_SECURITY_TOKEN")
    # Credential resolution is intentionally deferred to get_credentials_client() in build_tools().
    # Resolving here would snapshot dynamic credentials (credentials_uri / ECS RAM Role) at startup,
    # preventing the SDK from auto-refreshing tokens.
    # catalogapi_endpoint may be empty; resolved later in build_tools()

    missing = []
    if not maxcompute_endpoint:
        missing.append("maxcompute_endpoint (or env MAXCOMPUTE_ENDPOINT)")
    if missing:
        raise ValueError(
            "Missing required MaxCompute config: " + ", ".join(missing) + f". Config file: {path.resolve()}"
        )

    return MaxComputeCatalogConfig(
        catalogapi_endpoint=catalogapi_endpoint,
        maxcompute_endpoint=maxcompute_endpoint,
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
        security_token=security_token or "",
        default_project=default_project,
        namespace_id=namespace_id,
        protocol=protocol,
    )
