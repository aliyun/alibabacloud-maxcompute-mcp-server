"""Session management tools: list / switch / inspect named runtime configs.

A "named config" bundles endpoint + AccessKey + project under a name. At runtime
the user picks which named config is active; subsequent tool calls use that
config's clients. This lets one MCP server reach multiple regions/identities
(switch via prompt) instead of being bound to a single config at startup.

SECURITY: these tools NEVER return AccessKey id/secret or STS token — only
name / region / description / endpoint / default_project are exposed.

Requires the host Tools to provide:
  self._configs:      dict[name, MaxComputeCatalogConfig]
  self._current_name: str   (active config name)
  self._default_name: str   (startup default config name)
  self._activate_config(name): swap active clients to the named config
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from .mcp_protocol import mcp_text_result
from .tools_common import require_arg

logger = logging.getLogger(__name__)


class SessionMixin:
    """list_configs / use_config / get_current_config handlers."""

    def _config_view(self, name: str) -> Dict[str, Any]:
        """Non-secret view of one named config."""
        cfg = self._configs[name]
        return {
            "name": name,
            "region": getattr(cfg, "region", "") or "",
            "description": getattr(cfg, "description", "") or "",
            "maxcompute_endpoint": cfg.maxcompute_endpoint,
            "default_project": cfg.default_project,
            "is_default": name == self._default_name,
            "is_current": name == self._current_name,
        }

    def list_configs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        configs = [self._config_view(n) for n in self._configs.keys()]
        return mcp_text_result({
            "success": True,
            "data": {
                "current": self._current_name,
                "default": self._default_name,
                "configs": configs,
            },
            "summary": (
                f"{len(configs)} config(s); current={self._current_name!r}, "
                f"default={self._default_name!r}"
            ),
        })

    def get_current_config(self, args: Dict[str, Any]) -> Dict[str, Any]:
        if self._current_name not in self._configs:
            return mcp_text_result({"success": False, "error": "no current config is set"})
        return mcp_text_result({"success": True, "data": self._config_view(self._current_name)})

    def use_config(self, args: Dict[str, Any]) -> Dict[str, Any]:
        name = require_arg(args, "name", "Missing required parameter: name")
        if name not in self._configs:
            return mcp_text_result({
                "success": False,
                "error": f"Unknown config {name!r}. Available: {sorted(self._configs)}",
            })
        if name == self._current_name:
            return mcp_text_result({
                "success": True,
                "data": self._config_view(name),
                "summary": f"Already using config {name!r}",
            })
        try:
            self._activate_config(name)
        except Exception as e:  # build/connect failure → keep current, report
            logger.exception("Failed to switch to config %r: %s", name, e)
            return mcp_text_result({
                "success": False,
                "error": (
                    f"Failed to switch to config {name!r}: {e}. "
                    f"Current config unchanged ({self._current_name!r})."
                ),
            })
        return mcp_text_result({
            "success": True,
            "data": self._config_view(name),
            "summary": f"Switched current config to {name!r}",
        })
