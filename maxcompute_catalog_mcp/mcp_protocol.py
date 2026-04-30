from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class JsonRpcError(Exception):
    code: int
    message: str
    data: Optional[str] = None


def mcp_text_result(data: Dict[str, Any]) -> Dict[str, Any]:
    """Format tool result as MCP content: { content: [ { type: 'text', text: '<json>' } ] }."""
    return {"content": [{"type": "text", "text": json.dumps(data, ensure_ascii=False)}]}


def mcp_ok_result(data: Dict[str, Any], summary: Optional[str] = None) -> Dict[str, Any]:
    """Return a consistent success envelope for model extraction: success=true, data=payload, optional summary."""
    out: Dict[str, Any] = {"success": True, "data": data}
    if summary is not None:
        out["summary"] = summary
    return mcp_text_result(out)
