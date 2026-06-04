"""Build a full runtime client set from one MaxCompute config.

Extracted from server.build_tools() so the same wiring can be reused at runtime
when switching named configs (see Tools.use_config / Tools._activate_config).

A ClientSet bundles everything that is bound to a single named config:
credentials client, ODPS compute client, Catalog SDK client, and the
default_project / namespace_id that travel with that identity.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Optional

from .config import (
    MaxComputeCatalogConfig,
    resolve_catalogapi_endpoint_with_client,
    resolve_protocol_and_endpoints,
)
from .credentials import get_credentials_client
from .maxcompute_client import MaxComputeCatalogSdk, MaxComputeClient


@dataclass
class ClientSet:
    """All runtime clients/handles bound to one named config."""

    sdk: MaxComputeCatalogSdk
    maxcompute_client: Optional[MaxComputeClient]
    credential_client: Any
    default_project: str
    namespace_id: str


def build_client_set(cfg: MaxComputeCatalogConfig) -> ClientSet:
    """Build credentials + ODPS compute client + Catalog SDK for one config.

    Mirrors the original build_tools() initialization order:
      1. credentials client (static AK/SK > default credential chain)
      2. ODPS compute client (also used to resolve catalogapi_endpoint)
      3. resolve catalogapi_endpoint if not configured
      4. Catalog SDK client

    Raises ValueError (credentials) / RuntimeError (endpoint/SDK) on failure;
    the caller decides whether to sys.exit (startup) or surface it as a tool
    error (runtime switch via use_config).
    """
    credential_client = get_credentials_client(
        access_key_id=cfg.access_key_id,
        access_key_secret=cfg.access_key_secret,
        security_token=cfg.security_token,
    )

    resolved = resolve_protocol_and_endpoints(cfg)
    maxcompute_client = MaxComputeClient.create(
        cfg, credential_client=credential_client, resolved=resolved,
    )

    catalogapi_endpoint = cfg.catalogapi_endpoint
    if not catalogapi_endpoint:
        if maxcompute_client is None:
            raise RuntimeError(
                "Cannot resolve catalogapi_endpoint without a valid ODPS client. "
                "Set maxcompute_endpoint/default_project, or explicitly set catalogapi_endpoint."
            )
        try:
            catalogapi_endpoint = resolve_catalogapi_endpoint_with_client(
                maxcompute_client.odps_client, resolved.maxcompute_url,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to resolve catalogapi endpoint: {e}") from e

    # update config with resolved endpoint, then re-resolve so catalogapi
    # protocol/host reflect any scheme embedded in the probed value.
    cfg = replace(cfg, catalogapi_endpoint=catalogapi_endpoint)
    resolved = resolve_protocol_and_endpoints(cfg)

    sdk = MaxComputeCatalogSdk.create(
        cfg, credential_client=credential_client, resolved=resolved,
    )

    return ClientSet(
        sdk=sdk,
        maxcompute_client=maxcompute_client,
        credential_client=credential_client,
        default_project=cfg.default_project,
        namespace_id=cfg.namespace_id,
    )
