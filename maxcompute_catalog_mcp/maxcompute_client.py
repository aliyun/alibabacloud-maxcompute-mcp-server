from __future__ import annotations

import importlib.metadata
import logging
from dataclasses import dataclass
from typing import Any, Optional

from maxcompute_tea_openapi import models as open_api_models
from pyodps_catalog.client import Client as CatalogClient

try:
    from odps import ODPS
    from odps.accounts import CredentialProviderAccount
    from odps.rest import default_user_agent as _get_default_user_agent
except ImportError:
    ODPS = None  # type: ignore[misc,assignment]
    CredentialProviderAccount = None  # type: ignore[misc,assignment]
    _get_default_user_agent = None  # type: ignore[misc,assignment]

from .config import (
    MaxComputeCatalogConfig,
    ResolvedEndpoints,
    resolve_protocol_and_endpoints,
)

logger = logging.getLogger(__name__)

try:
    _VERSION = importlib.metadata.version("alibabacloud-maxcompute-mcp-server")
except importlib.metadata.PackageNotFoundError:
    _VERSION = "unknown"

_USER_AGENT = f"alibabacloud-maxcompute-mcp-server/{_VERSION}"

# Full UA: PyODPS default prefix + MCP suffix, computed once at module load
try:
    _base_ua = _get_default_user_agent() if _get_default_user_agent else ""
except Exception:
    _base_ua = ""
_FULL_USER_AGENT = f"{_base_ua} {_USER_AGENT}".strip() if _base_ua else _USER_AGENT


def _build_catalog_client(
    endpoint: str,
    *,
    credential_client: Any = None,
    access_key_id: str = "",
    access_key_secret: str = "",
    security_token: str = "",
    protocol: str = "",
) -> CatalogClient:
    """Build a CatalogClient.

    Uses credential_client when provided (Tea SDK calls get_credential() per request for
    transparent token refresh); falls back to static AK/SK for backward compatibility.

    ``protocol`` is the Tea OpenAPI transport scheme; must be "HTTP" or "HTTPS" when
    supplied. It is emitted only when non-empty so callers that omit it preserve the
    SDK's default behaviour.
    """
    config_kw: dict = {"endpoint": endpoint, "user_agent": _USER_AGENT}
    if protocol:
        config_kw["protocol"] = protocol
    if credential_client is not None:
        config_kw["credential"] = credential_client
    else:
        config_kw["access_key_id"] = access_key_id
        config_kw["access_key_secret"] = access_key_secret
        if security_token:
            config_kw["security_token"] = security_token
    try:
        config = open_api_models.Config(**config_kw)
    except TypeError as exc:
        logger.warning(
            "open_api_models.Config rejected credential fields (%s); "
            "falling back to static AK/SK. This may indicate an SDK version mismatch.",
            exc,
        )
        config_kw.pop("security_token", None)
        config_kw.pop("credential", None)
        config = open_api_models.Config(**config_kw)
    return CatalogClient(config)


class MaxComputeCatalogSdk:
    """Catalog API client (pyodps_catalog) with automatic credential refresh.

    Passes credential_client to Tea OpenAPI Config; Tea SDK calls get_credential() on
    each request, so token refresh is transparent without rebuilding the client.
    """

    def __init__(self, catalog_client: CatalogClient) -> None:
        self._catalog_client = catalog_client

    @property
    def client(self) -> CatalogClient:
        return self._catalog_client

    @staticmethod
    def create(
        cfg: MaxComputeCatalogConfig,
        credential_client: Any,
        resolved: Optional[ResolvedEndpoints] = None,
    ) -> "MaxComputeCatalogSdk":
        if credential_client is None:
            raise RuntimeError(
                "credential_client is required for MaxComputeCatalogSdk. "
                "Pass a valid alibabacloud-credentials Client instance."
            )
        if resolved is None:
            resolved = resolve_protocol_and_endpoints(cfg)
        # Tea OpenAPI expects a bare host endpoint plus a separate protocol ("HTTP"/"HTTPS").
        catalog_client = _build_catalog_client(
            resolved.catalogapi_host,
            credential_client=credential_client,
            protocol=resolved.catalogapi_protocol.upper(),
        )
        return MaxComputeCatalogSdk(catalog_client=catalog_client)


@dataclass
class MaxComputeClient:
    """Compute client via pyodps (SQL, partitions, instances, etc.). Delegates to ODPS instance."""

    _client: Any  # odps.ODPS

    @staticmethod
    def create(
        cfg: MaxComputeCatalogConfig,
        credential_client: Any,
        resolved: Optional[ResolvedEndpoints] = None,
    ) -> Optional["MaxComputeClient"]:
        """Return a compute client, or None if default_project is unset or pyodps unavailable.

        Requires credential_client (alibabacloud-credentials Client);
        CredentialProviderAccount calls get_credential() on each request to refresh STS tokens.
        """
        if ODPS is None:
            return None

        if not cfg.default_project:
            return None

        if resolved is None:
            resolved = resolve_protocol_and_endpoints(cfg)
        endpoint = resolved.maxcompute_url
        if not endpoint:
            return None

        if credential_client is None:
            raise RuntimeError(
                "credential_client is required for MaxComputeClient. "
                "Pass a valid alibabacloud-credentials Client instance."
            )

        if CredentialProviderAccount is None:
            raise RuntimeError(
                "CredentialProviderAccount not available (requires pyodps >= 0.12.0). "
                "Please upgrade pyodps to enable STS token auto-refresh."
            )
        account = CredentialProviderAccount(credential_client)
        client = ODPS(account=account, project=cfg.default_project, endpoint=endpoint,
                      rest_client_kwargs={"user_agent": _FULL_USER_AGENT})
        return MaxComputeClient(_client=client)

    @property
    def odps_client(self) -> Any:
        """Public accessor for the underlying ODPS instance."""
        return self._client

    def __getattr__(self, name: str) -> Any:
        """Delegate to the underlying ODPS instance so compute.execute_sql etc. work unchanged."""
        return getattr(self._client, name)
