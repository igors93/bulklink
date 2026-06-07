"""Deterministic configuration validation."""

from __future__ import annotations

import math


def require_label(value: str) -> str:
    """Return a normalized non-empty label."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("label must be a non-empty string")
    return value.strip()


def require_positive_integer(name: str, value: int) -> int:
    """Return a positive integer."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def require_non_negative_integer(name: str, value: int) -> int:
    """Return a non-negative integer."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def require_optional_positive_number(name: str, value: float | None) -> float | None:
    """Return ``None`` or a positive finite float."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a positive finite number or None")
    converted = float(value)
    if converted <= 0 or not math.isfinite(converted):
        raise ValueError(f"{name} must be a positive finite number or None")
    return converted
