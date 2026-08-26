"""Deterministic startup selection for legacy local and remote proxy modes."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .config import MaxComputeCatalogConfig, load_configs


class RuntimeMode(str, Enum):
    """Configured process runtime modes."""

    DEFAULT = "default"
    REMOTE = "remote"
    LOCAL = "local"


class EndpointNetwork(str, Enum):
    """Network boundary represented by a recognized service endpoint."""

    PUBLIC = "public"
    VPC = "vpc"


@dataclass(frozen=True)
class EndpointIdentity:
    """Region and network identity extracted from a service endpoint."""

    region: str
    network: EndpointNetwork


@dataclass(frozen=True)
class RemoteRuntimeConfig:
    """Validated remote MCP proxy settings."""

    url: str


@dataclass(frozen=True)
class RuntimeConfig:
    """Requested mode plus a validated remote endpoint, when available."""

    mode: RuntimeMode
    profile: str = ""
    remote: RemoteRuntimeConfig | None = None


_REGION_PATTERN = r"[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?"
_PUBLIC_MAXCOMPUTE_HOST = re.compile(
    rf"^service\.(?P<region>{_REGION_PATTERN})\.maxcompute\.aliyun\.com$"
)
_VPC_MAXCOMPUTE_HOST = re.compile(
    rf"^service\.(?P<region>{_REGION_PATTERN})"
    r"(?:-intranet|-vpc)"
    r"\.maxcompute\.aliyun-inc\.com$"
)
_PUBLIC_CATALOGAPI_HOST = re.compile(
    rf"^catalogapi\.(?P<region>{_REGION_PATTERN})\.maxcompute\.aliyun\.com$"
)
_VPC_CATALOGAPI_HOST = re.compile(
    rf"^catalogapi\.(?P<region>{_REGION_PATTERN})"
    r"(?:-intranet|-vpc)"
    r"\.maxcompute\.aliyun-inc\.com$"
)
_PUBLIC_MCP_HOST = re.compile(
    rf"^mcp(?:-intl)?\.(?P<region>{_REGION_PATTERN})\.maxcompute\.aliyun\.com$"
)
_VPC_MCP_HOST = re.compile(
    rf"^mcp(?:-intl)?\.(?P<region>{_REGION_PATTERN})-vpc\.maxcompute\.aliyun-inc\.com$"
)
_GLOBAL_PUBLIC_MCP_HOSTS = frozenset(
    {"mcp.maxcompute.aliyun.com", "mcp-intl.maxcompute.aliyun.com"}
)
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})
_REMOTE_FIELDS = frozenset({"url"})


def _first_non_empty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _endpoint_host(value: str) -> str:
    text = (value or "").strip()
    parsed = urlsplit(text if "://" in text else f"//{text}")
    return (parsed.hostname or "").lower()


def _match_endpoint(
    host: str,
    *,
    public_pattern: re.Pattern[str],
    vpc_pattern: re.Pattern[str],
) -> EndpointIdentity | None:
    public_match = public_pattern.fullmatch(host)
    if public_match is not None:
        return EndpointIdentity(
            region=public_match.group("region"),
            network=EndpointNetwork.PUBLIC,
        )
    vpc_match = vpc_pattern.fullmatch(host)
    if vpc_match is not None:
        return EndpointIdentity(
            region=vpc_match.group("region"),
            network=EndpointNetwork.VPC,
        )
    return None


def _maxcompute_endpoint_identity(
    config: MaxComputeCatalogConfig,
) -> EndpointIdentity | None:
    return _match_endpoint(
        _endpoint_host(config.maxcompute_endpoint),
        public_pattern=_PUBLIC_MAXCOMPUTE_HOST,
        vpc_pattern=_VPC_MAXCOMPUTE_HOST,
    )


def _catalogapi_endpoint_identity(
    config: MaxComputeCatalogConfig,
) -> EndpointIdentity | None:
    return _match_endpoint(
        _endpoint_host(config.catalogapi_endpoint),
        public_pattern=_PUBLIC_CATALOGAPI_HOST,
        vpc_pattern=_VPC_CATALOGAPI_HOST,
    )


def _configured_endpoint_identity(
    config: MaxComputeCatalogConfig,
) -> EndpointIdentity | None:
    identities = [
        identity
        for identity in (
            _maxcompute_endpoint_identity(config),
            _catalogapi_endpoint_identity(config),
        )
        if identity is not None
    ]
    explicit_region = (config.region or "").strip().lower()
    explicit_network_text = (config.network or "").strip().lower()
    explicit_network = (
        EndpointNetwork(explicit_network_text)
        if explicit_network_text
        else None
    )
    for identity in identities:
        region_conflicts = bool(explicit_region) and explicit_region != identity.region
        network_conflicts = (
            explicit_network is not None and explicit_network is not identity.network
        )
        if region_conflicts or network_conflicts:
            raise ValueError(
                "explicit region/network conflicts with configured endpoints"
            )
    if identities and any(identity != identities[0] for identity in identities[1:]):
        return None
    if identities:
        return identities[0]
    if explicit_region and explicit_network is not None:
        return EndpointIdentity(
            region=explicit_region,
            network=explicit_network,
        )
    return None


def _mcp_endpoint_identity(url: str) -> EndpointIdentity | None:
    host = _endpoint_host(url)
    if host in _GLOBAL_PUBLIC_MCP_HOSTS:
        return EndpointIdentity(region="", network=EndpointNetwork.PUBLIC)
    return _match_endpoint(
        host,
        public_pattern=_PUBLIC_MCP_HOST,
        vpc_pattern=_VPC_MCP_HOST,
    )


def _is_mainland_china_region(region: str) -> bool:
    """Return whether a Region uses the Mainland China MCP hostname."""

    return region.startswith("cn-") and region != "cn-hongkong"


def _regional_mcp_url(identity: EndpointIdentity) -> str:
    """Synthesize one MCP endpoint from Region and network."""

    prefix = "mcp" if _is_mainland_china_region(identity.region) else "mcp-intl"
    if identity.network is EndpointNetwork.PUBLIC:
        return f"https://{prefix}.{identity.region}.maxcompute.aliyun.com/mcp"
    return (
        f"https://{prefix}.{identity.region}-vpc.maxcompute.aliyun-inc.com/mcp"
    )


def _is_loopback_url(url: str) -> bool:
    host = _endpoint_host(url)
    return host in _LOOPBACK_HOSTS or host.endswith(".localhost")


def _validate_remote_url(value: str) -> str:
    if not value:
        raise ValueError("remote MCP URL is required")
    parsed = urlsplit(value)
    if not parsed.hostname:
        raise ValueError("remote MCP URL must include a host")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("remote MCP URL must not contain userinfo")
    if parsed.query:
        raise ValueError("remote MCP URL must not contain a query")
    if parsed.fragment:
        raise ValueError("remote MCP URL must not contain a fragment")
    if parsed.path != "/mcp":
        raise ValueError("remote MCP URL path must be /mcp")
    is_loopback_http = parsed.scheme == "http" and _is_loopback_url(value)
    if parsed.scheme != "https" and not is_loopback_http:
        raise ValueError("remote MCP URL must use HTTPS")
    try:
        _ = parsed.port
    except ValueError as error:
        raise ValueError("remote MCP URL has an invalid port") from error
    return value


def _validate_network_compatibility(
    maxcompute: EndpointIdentity,
    remote_url: str,
) -> None:
    if _is_loopback_url(remote_url):
        return
    remote = _mcp_endpoint_identity(remote_url)
    if remote is None:
        raise ValueError("remote MCP URL network type cannot be verified")
    if remote.network is not maxcompute.network:
        raise ValueError(
            "remote MCP URL network type must match the MaxCompute endpoint"
        )
    if (
        maxcompute.network is EndpointNetwork.VPC
        and remote.region != maxcompute.region
    ):
        raise ValueError(
            "a VPC MaxCompute endpoint requires a same Region VPC MCP URL"
        )


def _load_document(config_path: str | None) -> dict[str, Any]:
    resolved_path = _first_non_empty(
        config_path,
        os.getenv("MAXCOMPUTE_CATALOG_CONFIG"),
    )
    if not resolved_path:
        return {}
    path = Path(resolved_path)
    if not path.exists():
        return {}
    loaded = json.loads(path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise TypeError("runtime config must be a JSON object")
    return loaded


def load_runtime_config(
    config_path: str | None,
    *,
    mode: str | None = None,
    remote_url: str | None = None,
    profile: str | None = None,
    allow_remote: bool = True,
) -> RuntimeConfig:
    """Resolve a safe remote candidate for the requested startup mode.

    No configured mode means ``default``. FE/Catalog endpoints or explicit
    simple config identify the Region and network used to derive one regional
    MCP endpoint. Custom or conflicting endpoints leave no remote
    candidate, so the server starts the original local implementation.
    """

    document = _load_document(config_path)
    raw_mode = _first_non_empty(
        mode,
        os.getenv("MAXCOMPUTE_MCP_MODE"),
        document.get("mode"),
        RuntimeMode.DEFAULT.value,
    ).lower()
    try:
        requested_mode = RuntimeMode(raw_mode)
    except ValueError as error:
        raise ValueError("mode must be 'default', 'remote', or 'local'") from error

    resolved_profile = _first_non_empty(
        profile,
        os.getenv("MAXCOMPUTE_MCP_PROFILE"),
        document.get("profile"),
    )
    if requested_mode is RuntimeMode.LOCAL:
        return RuntimeConfig(mode=RuntimeMode.LOCAL, profile=resolved_profile)
    if not allow_remote:
        if requested_mode is RuntimeMode.REMOTE:
            raise ValueError("remote proxy mode is only available with stdio")
        return RuntimeConfig(mode=requested_mode, profile=resolved_profile)

    if (
        requested_mode is RuntimeMode.DEFAULT
        and not document
        and not os.getenv("MAXCOMPUTE_ENDPOINT")
        and not os.getenv("MAXCOMPUTE_REGION")
        and not os.getenv("MAXCOMPUTE_NETWORK")
    ):
        return RuntimeConfig(mode=RuntimeMode.DEFAULT, profile=resolved_profile)

    raw_remote = document.get("remote") or {}
    if not isinstance(raw_remote, dict):
        raise TypeError("remote config must be an object")
    unsupported = sorted(set(raw_remote).difference(_REMOTE_FIELDS))
    if unsupported:
        raise ValueError(f"remote config contains unsupported field: {unsupported[0]}")

    configs, default_name = load_configs(config_path)
    selected_name = resolved_profile or default_name
    if selected_name not in configs:
        raise ValueError(f"unknown MaxCompute profile: {selected_name!r}")
    endpoint_identity = _configured_endpoint_identity(configs[selected_name])
    configured_remote_url = _first_non_empty(
        remote_url,
        os.getenv("MAXCOMPUTE_REMOTE_MCP_URL"),
        raw_remote.get("url"),
    )

    if configured_remote_url:
        configured_remote_url = _validate_remote_url(configured_remote_url)
        if endpoint_identity is None and not _is_loopback_url(configured_remote_url):
            raise ValueError(
                "MaxCompute endpoint network type and Region cannot be verified"
            )
        if endpoint_identity is not None:
            _validate_network_compatibility(
                endpoint_identity,
                configured_remote_url,
            )
        return RuntimeConfig(
            mode=requested_mode,
            profile=selected_name,
            remote=RemoteRuntimeConfig(url=configured_remote_url),
        )

    if endpoint_identity is None:
        if requested_mode is RuntimeMode.REMOTE:
            raise ValueError(
                "remote MCP endpoint cannot be derived from the MaxCompute config"
            )
        return RuntimeConfig(mode=RuntimeMode.DEFAULT, profile=selected_name)
    return RuntimeConfig(
        mode=requested_mode,
        profile=selected_name,
        remote=RemoteRuntimeConfig(url=_regional_mcp_url(endpoint_identity)),
    )
