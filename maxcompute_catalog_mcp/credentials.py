"""User credential resolution (using Alibaba Cloud Credentials SDK).

Uses the default credential chain from the alibabacloud-credentials SDK, supporting:
- Environment variables ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET / ALIBABA_CLOUD_SECURITY_TOKEN
- Environment variable ALIBABA_CLOUD_CREDENTIALS_URI (remote credential service, natively supported by the SDK)
- Configuration file ~/.aliyun/config.json
- ECS RAM Role, OIDC, etc.

See: https://help.aliyun.com/zh/sdk/developer-reference/v2-manage-python-access-credentials
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResolvedCredentials:
    """Resolved credentials (supports STS)."""

    access_key_id: str
    access_key_secret: str
    security_token: str = ""


def get_credentials_from_default_chain() -> ResolvedCredentials:
    """Obtain credentials using the Alibaba Cloud Credentials SDK default chain (consistent with dataworks-mcp-server behavior).

    Default chain order: environment variables AK/SK -> OIDC -> config file ~/.aliyun/config.json -> ECS RAM Role
    -> ALIBABA_CLOUD_CREDENTIALS_URI. See:
    https://help.aliyun.com/zh/sdk/developer-reference/v2-manage-python-access-credentials

    Returns:
        ResolvedCredentials

    Raises:
        ValueError: SDK not installed or no valid credentials obtained from the chain.
    """
    try:
        from alibabacloud_credentials.client import Client as CredentialClient

        client = CredentialClient()
        cred = client.get_credential()
        access_key_id = (cred.get_access_key_id() or "").strip()
        access_key_secret = (cred.get_access_key_secret() or "").strip()
        if not access_key_id or not access_key_secret:
            raise ValueError(
                "Default credential chain did not return valid access_key_id/access_key_secret. "
                "Check ALIBABA_CLOUD_ACCESS_KEY_ID/SECRET or ALIBABA_CLOUD_CREDENTIALS_URI."
            )
        return ResolvedCredentials(
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
            security_token=(cred.get_security_token() or "").strip(),
        )
    except ImportError as e:
        raise ValueError(
            "alibabacloud-credentials is required for credential resolution. "
            "Install it with: pip install alibabacloud-credentials"
        ) from e
    except Exception as e:
        raise ValueError(f"Failed to get credential from default chain: {e}") from e


def get_credentials_client(
    access_key_id: str = "",
    access_key_secret: str = "",
    security_token: str = "",
) -> CredentialClient:
    """Return an alibabacloud-credentials Client instance (supports auto-refresh).

    Credential source priority:
    1. If access_key_id/access_key_secret parameters are provided, use static credential configuration
    2. Otherwise use the default credential chain (environment variables -> OIDC -> config file -> ECS RAM Role -> credentials_uri)

    Static credentials (AK/SK) do not expire and need no refresh;
    STS/credentials_uri from the default credential chain are automatically refreshed by the SDK.

    Args:
        access_key_id: Optional, static AccessKey ID (from config.json)
        access_key_secret: Optional, static AccessKey Secret
        security_token: Optional, STS temporary credential (not auto-refreshed when statically configured)

    Returns:
        alibabacloud_credentials.client.Client

    Raises:
        ValueError: alibabacloud-credentials SDK not installed or unable to obtain credentials.
    """
    try:
        from alibabacloud_credentials.client import Client as CredentialClient
        from alibabacloud_credentials.models import Config

        # If static AK/SK are provided, use static credential configuration
        if access_key_id and access_key_secret:
            if security_token:
                # STS credentials: use type="sts" so security_token is returned by every get_credential() call.
                # Token is a static snapshot; use ALIBABA_CLOUD_CREDENTIALS_URI for auto-refresh.
                config = Config(
                    type="sts",
                    access_key_id=access_key_id,
                    access_key_secret=access_key_secret,
                    security_token=security_token,
                )
            else:
                config = Config(
                    type="access_key",
                    access_key_id=access_key_id,
                    access_key_secret=access_key_secret,
                )
            return CredentialClient(config=config)

        # Otherwise use the default credential chain.
        # Probe once to fail-fast at startup if no credentials are available;
        # the returned client still calls get_credential() dynamically per request,
        # so STS auto-refresh (credentials_uri / ECS RAM Role) is unaffected.
        client = CredentialClient()
        try:
            client.get_credential()
        except Exception as probe_err:
            raise ValueError(
                f"Default credential chain returned no valid credentials: {probe_err}. "
                "Provide credentials via config.json, environment variables "
                "(ALIBABA_CLOUD_ACCESS_KEY_ID/SECRET), ALIBABA_CLOUD_CREDENTIALS_URI, "
                "or an ECS RAM role."
            ) from probe_err
        return client
    except ImportError as e:
        raise ValueError(
            "alibabacloud-credentials is required for credential resolution. "
            "Install it with: pip install alibabacloud-credentials"
        ) from e
