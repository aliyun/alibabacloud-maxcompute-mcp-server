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
     MAXCOMPUTE_REGION                   - Region for endpoint synthesis or validation
     MAXCOMPUTE_NETWORK                  - "public" | "vpc" for endpoint synthesis or validation
   - Credentials: AK/SK from config/env takes priority; otherwise Alibaba Cloud default credential chain is used.
   - catalogapi_endpoint: synthesized by simple region/network config; otherwise
     auto-resolved via the existing ODPS client in build_tools() if not set.
   - protocol: explicit override of transport scheme for both planes. When empty,
     scheme is inferred per-client from the embedded scheme of each endpoint
     (catalogapi falls back to maxcompute scheme), defaulting to "https".
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .tools_common import _env

_ALLOWED_PROTOCOLS = ("", "http", "https")
_ALLOWED_NETWORKS = ("", "public", "vpc")
_REGION_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$")


@dataclass(frozen=True)
class MaxComputeCatalogConfig:
    """Config fields match config.example.json under key \"maxcompute\"."""

    catalogapi_endpoint: str
    maxcompute_endpoint: str
    access_key_id: str = field(repr=False)
    access_key_secret: str = field(repr=False)
    security_token: str = field(default="", repr=False)  # STS token; empty for non-STS auth
    default_project: str = ""
    namespace_id: str = ""  # main account UID for Catalog API namespaces/:search
    protocol: str = ""  # "" | "http" | "https"; empty means infer per-client
    region: str = ""  # Region for simple config or endpoint validation
    network: str = ""  # "" | "public" | "vpc"; simple-config network
    description: str = ""  # display-only human note for named configs; not used for connection


@dataclass(frozen=True)
class ResolvedEndpoints:
    """Per-client protocol + normalised endpoint values derived from config."""

    maxcompute_protocol: str   # "http" | "https"
    maxcompute_url: str        # full URL including scheme
    catalogapi_protocol: str   # "http" | "https"
    catalogapi_host: str       # bare host (no scheme)


def _resolve_simple_endpoints(
    maxcompute_endpoint: str,
    catalogapi_endpoint: str,
    region: str,
    network: str,
) -> tuple[str, str]:
    """Synthesize standard FE and Catalog endpoints from region + network."""

    if network not in _ALLOWED_NETWORKS:
        raise ValueError(
            f"invalid network value {network!r}; allowed: {{'', 'public', 'vpc'}}"
        )
    if network and not region:
        raise ValueError("region is required when network is configured")
    if network and _REGION_PATTERN.fullmatch(region) is None:
        raise ValueError("region is invalid for endpoint synthesis")
    if not maxcompute_endpoint and region and network:
        if network == "public":
            maxcompute_endpoint = f"https://service.{region}.maxcompute.aliyun.com/api"
            catalogapi_endpoint = catalogapi_endpoint or (
                f"https://catalogapi.{region}.maxcompute.aliyun.com"
            )
        else:
            maxcompute_endpoint = (
                f"https://service.{region}-intranet.maxcompute.aliyun-inc.com/api"
            )
            catalogapi_endpoint = catalogapi_endpoint or (
                f"https://catalogapi.{region}-intranet.maxcompute.aliyun-inc.com"
            )
    return maxcompute_endpoint, catalogapi_endpoint


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

    Does not discover catalogapi_endpoint over the network; if simple config
    cannot synthesize it, discovery happens in build_tools() after the ODPS
    client is created.
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
    region = pick("region", from_env="MAXCOMPUTE_REGION")
    network = pick("network", from_env="MAXCOMPUTE_NETWORK").lower()
    maxcompute_endpoint, catalogapi_endpoint = _resolve_simple_endpoints(
        maxcompute_endpoint,
        catalogapi_endpoint,
        region,
        network,
    )
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
        region=region,
        network=network,
    )


def _config_from_bundle(conn: Dict[str, Any]) -> MaxComputeCatalogConfig:
    """Parse one named-config bundle (dict-only, NO environment overrides).

    Used by load_configs() for each entry under the top-level "configs" object.
    Field-name variants mirror load_config(). description remains display-only.
    """
    def pick(*keys: str) -> str:
        for k in keys:
            v = conn.get(k)
            if v:
                return v.strip() if isinstance(v, str) else str(v)
        return ""

    catalogapi_endpoint = pick("catalogapi_endpoint", "catalogapiEndpoint", "endpoint")
    maxcompute_endpoint = pick("maxcompute_endpoint", "maxcomputeEndpoint", "sdkEndpoint")
    region = pick("region")
    network = pick("network").lower()
    maxcompute_endpoint, catalogapi_endpoint = _resolve_simple_endpoints(
        maxcompute_endpoint,
        catalogapi_endpoint,
        region,
        network,
    )
    if not maxcompute_endpoint:
        raise ValueError("missing required 'maxcompute_endpoint'")
    protocol = pick("protocol").lower()
    if protocol not in _ALLOWED_PROTOCOLS:
        raise ValueError(f"invalid protocol value {protocol!r}; allowed: {{'', 'http', 'https'}}")

    return MaxComputeCatalogConfig(
        catalogapi_endpoint=catalogapi_endpoint,
        maxcompute_endpoint=maxcompute_endpoint,
        access_key_id=pick("accessKeyId", "access_key_id"),
        access_key_secret=pick("accessKeySecret", "access_key_secret"),
        security_token=pick("securityToken", "security_token"),
        default_project=pick("defaultProject", "default_project"),
        namespace_id=pick("namespaceId", "namespace_id", "account_uid"),
        protocol=protocol,
        region=region,
        network=network,
        description=pick("description", "desc"),
    )


def load_configs(
    config_path: str | None = None,
) -> Tuple[Dict[str, MaxComputeCatalogConfig], str]:
    """Load one-or-many named configs. Returns (configs_by_name, default_name).

    - If the JSON file has a top-level "configs" object: parse each named bundle
      (no env overrides per bundle); default = json["default"] or first key.
    - Otherwise (legacy single "maxcompute"/"odps", or env-only): delegate to
      load_config() and return it under the name "default".

    This keeps full backward compatibility: existing single-config files and
    env-only setups behave exactly as before, surfaced as a single config
    named "default".
    """
    path = Path(config_path or _env("MAXCOMPUTE_CATALOG_CONFIG") or "config.json")
    raw: Dict[str, Any] = {}
    if path.exists():
        raw = json.loads(path.read_text(encoding="utf-8")) or {}

    configs_obj = raw.get("configs")
    if isinstance(configs_obj, dict) and configs_obj:
        configs: Dict[str, MaxComputeCatalogConfig] = {}
        for name, bundle in configs_obj.items():
            if not isinstance(bundle, dict):
                raise ValueError(f"config {name!r} must be an object")
            try:
                configs[name] = _config_from_bundle(bundle)
            except ValueError as e:
                raise ValueError(f"invalid named config {name!r}: {e}") from e
        default_name = (raw.get("default") or "").strip() or next(iter(configs))
        if default_name not in configs:
            raise ValueError(
                f"default config {default_name!r} not found in 'configs'; "
                f"available: {sorted(configs)}"
            )
        return configs, default_name

    # Legacy single-config / env-only path → one config named "default".
    single = load_config(config_path)
    return {"default": single}, "default"
