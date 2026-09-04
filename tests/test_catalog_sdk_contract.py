"""Signature-contract tests against the installed Catalog SDK.

pyodps-catalog 0.4.0 turned ``view`` and ``query`` into required arguments of
``Client.list_tables`` and ``Client.list_partitions``. The shared unit-test
fake client is a ``MagicMock``, which accepts any keyword arguments, so a
launcher that omits a required argument passes every mocked test and only
fails at runtime in local mode. These tests bind the arguments the launcher
actually passed to the signature of the installed SDK client, so signature
drift fails in CI instead of in a customer's local session.
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

from pyodps_catalog.client import Client as CatalogClient

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import data as _data
from tests.conftest import text_payload as _text_payload


def _assert_call_matches_sdk_signature(client: MagicMock, method_name: str) -> None:
    call = getattr(client, method_name).call_args
    assert call is not None, f"{method_name} was never called"
    signature = inspect.signature(getattr(CatalogClient, method_name))
    # bind() raises TypeError when the launcher omitted a required argument.
    signature.bind(None, *call.args, **call.kwargs)


def test_list_tables_passes_every_required_sdk_argument(tools: Tools) -> None:
    r = tools.call("list_tables", {"project": "p1", "schema": "default"})
    payload = _text_payload(r)
    assert "error" not in payload, payload.get("error")

    _assert_call_matches_sdk_signature(tools.sdk.client, "list_tables")


def test_get_partition_info_passes_every_required_sdk_argument(
    tools: Tools,
) -> None:
    r = tools.call(
        "get_partition_info",
        {"project": "p1", "schema": "default", "table": "t1"},
    )
    payload = _text_payload(r)
    assert "error" not in payload, payload.get("error")
    assert "partitions" in _data(payload)

    _assert_call_matches_sdk_signature(tools.sdk.client, "list_partitions")
