"""Safe handling for untrusted provider and gateway request IDs."""

from __future__ import annotations

from typing import Any

_MAX_REQUEST_ID_CHARS = 256
_MISSING_REQUEST_ID_VALUES = frozenset({"none", "null"})
_REQUEST_ID_DATA_KEYS = ("request_id", "requestId", "RequestId")


def sanitize_request_id(value: object) -> str | None:
    """Return one log-safe printable ASCII request ID, if present."""

    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if (
        not candidate
        or len(candidate) > _MAX_REQUEST_ID_CHARS
        or candidate.casefold() in _MISSING_REQUEST_ID_VALUES
        or any(character < " " or character > "~" for character in candidate)
    ):
        return None
    return candidate


def request_id_from_exception(error: BaseException) -> str | None:
    """Extract only an explicit request ID from a provider exception."""

    try:
        request_id = sanitize_request_id(getattr(error, "request_id", None))
        data: Any = getattr(error, "data", None)
    except Exception:  # noqa: BLE001 -- exception objects are an external boundary.
        return None
    if request_id is not None:
        return request_id
    if not isinstance(data, dict):
        return None
    for key in _REQUEST_ID_DATA_KEYS:
        request_id = sanitize_request_id(data.get(key))
        if request_id is not None:
            return request_id
    return None
