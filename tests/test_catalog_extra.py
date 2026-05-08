"""Additional boundary tests for tools_catalog.py."""
from __future__ import annotations

import types
from unittest.mock import MagicMock

import pytest

from maxcompute_catalog_mcp.tools import Tools
from tests.conftest import data as _data, text_payload as _text_payload


def _make_tools(sdk: MagicMock | None = None, **kwargs) -> Tools:
    if sdk is None:
        sdk = MagicMock()
        sdk.client.get_project.return_value = MagicMock(
            to_map=lambda: {"projectId": "p1", "schemaEnabled": True}
        )
    return Tools(
        sdk=sdk,
        default_project=kwargs.get("default_project", "p1"),
        namespace_id=kwargs.get("namespace_id", "ns1"),
        maxcompute_client=kwargs.get("maxcompute_client"),
    )


def test_get_partition_info_sdk_no_list_partitions() -> None:
    """SDK has no list_partitions → unsupported."""
    sdk = MagicMock()
    sdk.client.get_project.return_value = MagicMock(
        to_map=lambda: {"projectId": "p1", "schemaEnabled": True}
    )
    sdk.client.list_partitions.side_effect = AttributeError("no such method")
    t = _make_tools(sdk=sdk)
    r = t.call("get_partition_info", {"project": "p1", "schema": "default", "table": "t1"})
    payload = _text_payload(r)
    assert payload.get("error") == "unsupported"


def test_get_partition_info_exception() -> None:
    """list_partitions raises generic exception → error."""
    sdk = MagicMock()
    sdk.client.get_project.return_value = MagicMock(
        to_map=lambda: {"projectId": "p1", "schemaEnabled": True}
    )
    sdk.client.list_partitions.side_effect = RuntimeError("api error")
    t = _make_tools(sdk=sdk)
    r = t.call("get_partition_info", {"project": "p1", "schema": "default", "table": "t1"})
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "api error" in payload.get("error", "")


def test_search_meta_data_sdk_no_client() -> None:
    """catalog_client is None → unsupported."""
    sdk = MagicMock()
    sdk.client = None
    t = _make_tools(sdk=sdk)
    # Need to bypass _is_schema_enabled since it uses sdk.client too
    t._schema_enabled_cache["p1"] = True
    r = t.call("search_meta_data", {"query": "test"})
    payload = _text_payload(r)
    assert payload.get("error") == "unsupported"


def test_search_meta_data_exception() -> None:
    """search raises → success=False + error."""
    sdk = MagicMock()
    sdk.client.get_project.return_value = MagicMock(
        to_map=lambda: {"projectId": "p1", "schemaEnabled": True}
    )
    sdk.client.search.side_effect = RuntimeError("search error")
    t = _make_tools(sdk=sdk)
    r = t.call("search_meta_data", {"query": "test"})
    payload = _text_payload(r)
    assert payload.get("success") is False
    assert "search error" in payload.get("error", "")


def test_list_projects_plain_dict_response() -> None:
    """Response is a plain dict without to_map → hasattr branch is False, dict used directly."""
    sdk = MagicMock()
    # Plain dict response: no to_map attribute → exercises the else branch
    sdk.client.list_projects.return_value = {
        "projects": [{"projectId": "p1"}, {"projectId": "p2"}],
        "next_page_token": None,
    }
    sdk.client.get_project.return_value = MagicMock(
        to_map=lambda: {"projectId": "p1", "schemaEnabled": True}
    )
    t = _make_tools(sdk=sdk)
    r = t.call("list_projects", {"pageSize": 10})
    payload = _text_payload(r)
    d = _data(payload)
    assert d.get("projects") == [{"projectId": "p1"}, {"projectId": "p2"}]


def test_list_tables_with_filter() -> None:
    """list_tables with filter parameter performs client-side prefix filtering."""
    sdk = MagicMock()
    sdk.client.get_project.return_value = MagicMock(
        to_map=lambda: {"projectId": "p1", "schemaEnabled": True}
    )
    sdk.client.list_tables.return_value = MagicMock(
        to_map=lambda: {
            "tables": [
                {"tableName": "my_prefix_table"},
                {"tableName": "other_table"},
            ],
            "next_page_token": None,
        }
    )
    t = _make_tools(sdk=sdk)
    r = t.call("list_tables", {"project": "p1", "schema": "default", "filter": "my_prefix"})
    call_kwargs = sdk.client.list_tables.call_args.kwargs
    # filter is NOT passed to SDK as table_name_prefix (client-side filtering)
    assert "table_name_prefix" not in call_kwargs, (
        f"table_name_prefix should not be passed to SDK; got: {call_kwargs}"
    )
    p = _text_payload(r)
    d = _data(p)
    tables = d.get("tables") or []
    # Only the table matching the prefix should remain
    assert len(tables) == 1, f"Expected 1 filtered table, got: {tables}"
    assert tables[0]["tableName"] == "my_prefix_table"


def test_get_table_schema_non_dict_response() -> None:
    """get_table response is non-dict → raw wrapper."""
    sdk = MagicMock()
    sdk.client.get_project.return_value = MagicMock(
        to_map=lambda: {"projectId": "p1", "schemaEnabled": True}
    )
    sdk.client.get_table.return_value = MagicMock(
        to_map=lambda: "not a dict"
    )
    t = _make_tools(sdk=sdk)
    r = t.call("get_table_schema", {"project": "p1", "schema": "default", "table": "t1"})
    payload = _text_payload(r)
    d = _data(payload)
    assert "raw" in d


def test_get_table_schema_with_partition_definition() -> None:
    """partitionDefinition.partitionedColumns parsing via real Table model."""
    from pyodps_catalog import models as catalog_models

    sdk = MagicMock()
    sdk.client.get_project.return_value = MagicMock(
        to_map=lambda: {"projectId": "p1", "schemaEnabled": True}
    )
    t = catalog_models.Table(
        project_id="p1", schema_name="default", table_name="t1",
    )
    schema = catalog_models.TableFieldSchema()
    col = catalog_models.TableFieldSchema(field_name="id", sql_type_definition="BIGINT")
    schema.fields = [col]
    t.table_schema = schema
    pd = catalog_models.PartitionDefinition()
    pc = catalog_models.PartitionedColumn()
    pc.field = "ds"
    pd.partitioned_columns = [pc]
    t.partition_definition = pd

    sdk.client.get_table.return_value = t
    tools = _make_tools(sdk=sdk)
    r = tools.call("get_table_schema", {"project": "p1", "schema": "default", "table": "t1"})
    payload = _text_payload(r)
    d = _data(payload)
    assert "ds" in d.get("partitionKeys", [])


def test_get_table_schema_schema_as_list() -> None:
    """Schema fields parsed from real Table model objects."""
    from pyodps_catalog import models as catalog_models

    sdk = MagicMock()
    sdk.client.get_project.return_value = MagicMock(
        to_map=lambda: {"projectId": "p1", "schemaEnabled": True}
    )
    t = catalog_models.Table(
        project_id="p1", schema_name="default", table_name="t1",
    )
    schema = catalog_models.TableFieldSchema()
    col1 = catalog_models.TableFieldSchema(field_name="col1", sql_type_definition="STRING")
    col2 = catalog_models.TableFieldSchema(field_name="col2", sql_type_definition="BIGINT")
    schema.fields = [col1, col2]
    t.table_schema = schema

    sdk.client.get_table.return_value = t
    tools = _make_tools(sdk=sdk)
    r = tools.call("get_table_schema", {"project": "p1", "schema": "default", "table": "t1"})
    payload = _text_payload(r)
    d = _data(payload)
    assert len(d.get("columns", [])) == 2
    assert d["columns"][0]["name"] == "col1"
    assert d["columns"][1]["name"] == "col2"


def test_get_table_schema_partition_keys_in_schema_dict() -> None:
    """Multiple partition keys parsed from real Table model."""
    from pyodps_catalog import models as catalog_models

    sdk = MagicMock()
    sdk.client.get_project.return_value = MagicMock(
        to_map=lambda: {"projectId": "p1", "schemaEnabled": True}
    )
    t = catalog_models.Table(
        project_id="p1", schema_name="default", table_name="t_part",
    )
    schema = catalog_models.TableFieldSchema()
    col = catalog_models.TableFieldSchema(field_name="id", sql_type_definition="BIGINT")
    schema.fields = [col]
    t.table_schema = schema
    pd = catalog_models.PartitionDefinition()
    pc1 = catalog_models.PartitionedColumn()
    pc1.field = "ds"
    pc2 = catalog_models.PartitionedColumn()
    pc2.field = "region"
    pd.partitioned_columns = [pc1, pc2]
    t.partition_definition = pd

    sdk.client.get_table.return_value = t
    tools = _make_tools(sdk=sdk)
    r = tools.call("get_table_schema", {"project": "p1", "schema": "default", "table": "t_part"})
    payload = _text_payload(r)
    d = _data(payload)
    pkeys = d.get("partitionKeys", [])
    assert "ds" in pkeys
    assert "region" in pkeys


def test_iter_partition_definition_columns_helper() -> None:
    """Direct coverage for ``_iter_partition_definition_columns`` (none / missing attr / empty / normal)."""

    from pyodps_catalog import models as catalog_models

    from maxcompute_catalog_mcp.tools_table_meta import _iter_partition_definition_columns

    assert _iter_partition_definition_columns(None) == []

    missing_attr = types.SimpleNamespace()
    assert _iter_partition_definition_columns(missing_attr) == []

    pd_empty = catalog_models.PartitionDefinition()
    assert _iter_partition_definition_columns(pd_empty) == []

    pd_nonempty = catalog_models.PartitionDefinition()
    pc_ds = catalog_models.PartitionedColumn()
    pc_ds.field = "ds"
    pc_region = catalog_models.PartitionedColumn()
    pc_region.field = "region"
    pd_nonempty.partitioned_columns = [pc_ds, pc_region]

    cols = _iter_partition_definition_columns(pd_nonempty)
    assert cols == [pc_ds, pc_region]
    assert [getattr(c, "field", None) for c in cols] == ["ds", "region"]

    pd_nonempty.partitioned_columns = []
    assert _iter_partition_definition_columns(pd_nonempty) == []
