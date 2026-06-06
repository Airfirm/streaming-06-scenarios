"""src/streaming/data_validation/data_validation_femi.py.

Project-specific validation extensions.

Technical Modification:
- Adds money validation.
- Adds optional allowed-value validation helper.
- Keeps reusable validation helpers available for other files.

Author: O S
Date: 2026-06-06
"""

# === DECLARE IMPORTS ===

from datafun_streaming.data_validation.reference import (
    make_lookup_set,
    validate_reference_records,
)
from datafun_streaming.data_validation.validation_utils import add_validation_errors

# === DECLARE EXPORTS ===

__all__ = [
    "add_validation_errors",
    "make_lookup_set",
    "validate_allowed_optional",
    "validate_money_amount",
    "validate_quantity",
    "validate_reference_records",
]


def validate_quantity(value: str) -> list[str]:
    """Return errors for an invalid quantity value."""
    try:
        quantity = int(value)
    except ValueError:
        return [f"Quantity must be an integer: {value}"]

    if quantity < 1:
        return [f"Quantity must be at least 1: {value}"]

    return []


def validate_money_amount(value: str, *, field_name: str) -> list[str]:
    """Return errors for an invalid money amount."""
    try:
        amount = float(value)
    except ValueError:
        return [f"{field_name} must be numeric: {value}"]

    if amount < 0:
        return [f"{field_name} must be greater than or equal to 0: {value}"]

    return []


def validate_allowed_optional(
    record: dict[str, str],
    *,
    field_name: str,
    allowed_values: set[str],
) -> list[str]:
    """Validate an optional field only when it has a value."""
    value = str(record.get(field_name, "")).strip()

    if not value:
        return []

    if value not in allowed_values:
        return [f"Invalid {field_name}: {value!r}"]

    return []
