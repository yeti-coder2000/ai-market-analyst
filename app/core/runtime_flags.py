"""Reversible environment gates for the retired legacy TPO runtime."""

from __future__ import annotations

import os


TRUE_VALUES = frozenset({"1", "true", "yes", "y", "on"})


def env_flag(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in TRUE_VALUES


def tpo_runtime_enabled() -> bool:
    """Return whether any automatic legacy TPO worker may run."""
    return env_flag("TPO_RUNTIME_ENABLED", default=False)


def tpo_telegram_enabled() -> bool:
    """Return whether the legacy analyst may contact Telegram."""
    return env_flag("TPO_TELEGRAM_ENABLED", default=False)
