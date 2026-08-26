from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class JsonRpcError(Exception):
    code: int
    message: str
    data: str | None = None


def mcp_text_result(data: dict[str, Any]) -> dict[str, Any]:
    """Format tool result as MCP content: { content: [ { type: 'text', text: '<json>' } ] }."""
    return {"content": [{"type": "text", "text": json.dumps(data, ensure_ascii=False)}]}


def mcp_ok_result(data: dict[str, Any], summary: str | None = None) -> dict[str, Any]:
    """Return a consistent success envelope for model extraction: success=true, data=payload, optional summary."""
    out: dict[str, Any] = {"success": True, "data": data}
    if summary is not None:
        out["summary"] = summary
    return mcp_text_result(out)
