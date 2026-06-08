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


def require_finite_number(name: str, value: float) -> float:
    """Return a finite float."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number")
    converted = float(value)
    if not math.isfinite(converted):
        raise ValueError(f"{name} must be a finite number")
    return converted


def require_positive_number(name: str, value: float) -> float:
    """Return a positive finite float."""
    converted = require_finite_number(name, value)
    if converted <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return converted


def require_optional_positive_number(name: str, value: float | None) -> float | None:
    """Return ``None`` or a positive finite float."""
    if value is None:
        return None
    return require_positive_number(name, value)


def resolve_wait_limit(default: float | None, requested: float) -> float:
    """Return the shortest valid wait limit for one admission."""
    per_call = require_positive_number("wait_limit", requested)
    if default is None:
        return per_call
    return min(default, per_call)
