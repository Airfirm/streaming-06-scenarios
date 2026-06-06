"""src/streaming/data_validation/data_contract_femi.py.

Defines what a valid message looks like for this project.

Technical Modification:
- Adds stronger validation for quantity and unit_price.
- Safely validates optional fields only when values are provided.
- Adds new output fields used for sales performance monitoring.

New Problem:
- Real-time sales performance and customer/channel monitoring.

Author: O S
Date: 2026-06-06
"""

# === DECLARE IMPORTS ===

from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.data_validation.types import ValidationResult
from datafun_streaming.data_validation.validation_utils import (
    validate_boolean_text,
    validate_datetime,
    validate_required_fields,
)

from streaming.data_validation.data_validation_femi import (
    validate_allowed_optional,
    validate_money_amount,
    validate_quantity,
)

# === EVENT TABLE FIELDS ===

SALES_REQUIRED_FIELDS: Final[list[str]] = [
    "order_id",
    "datetime",
    "region_id",
    "currency_code",
    "product_id",
    "unit_price",
    "quantity",
    "is_online",
    "customer_id",
    "payment_method",
]

SALES_OPTIONAL_FIELDS: Final[list[str]] = [
    "is_new_customer",
    "device_type",
    "referral_source",
    "discount_code",
    "customer_note",
]

VALID_SALES_FIELDNAMES: Final[list[str]] = [
    *SALES_REQUIRED_FIELDS,
    *SALES_OPTIONAL_FIELDS,
]

# === REFERENCE TABLE FIELDS ===

REGIONS_REQUIRED_FIELDS: Final[list[str]] = [
    "region_id",
    "region_name",
    "country_code",
    "country_name",
    "currency_code",
    "tax_rate_pct",
    "timezone",
]

PRODUCTS_REQUIRED_FIELDS: Final[list[str]] = [
    "product_id",
    "product_name",
    "category",
    "level",
    "price_usd",
    "instructor",
]

CURRENCIES_REQUIRED_FIELDS: Final[list[str]] = [
    "currency_code",
    "currency_name",
    "symbol",
    "exchange_rate_to_usd",
    "rate_date",
]

DISCOUNT_CODES_REQUIRED_FIELDS: Final[list[str]] = [
    "discount_code",
    "discount_pct",
    "valid_from",
    "valid_to",
    "description",
]

# === ALLOWED VALUES ===

ALLOWED_DEVICE_TYPES: Final[set[str]] = {"mobile", "desktop", "tablet"}

ALLOWED_PAYMENT_METHODS: Final[set[str]] = {
    "credit_card",
    "paypal",
    "apple_pay",
    "gift_card",
}

ALLOWED_REFERRAL_SOURCES: Final[set[str]] = {
    "organic",
    "paid_search",
    "email",
    "social",
}

ALLOWED_CURRENCY_CODES: Final[set[str]] = {"USD", "CAD", "MXN"}

# === OUTPUT FIELD ORDER ===

CONSUMED_FIELDNAMES: Final[list[str]] = [
    *VALID_SALES_FIELDNAMES,
    "subtotal",
    "tax_amount",
    "total",
    "product_category",
    "sales_channel",
    "order_size_band",
    "high_value_order",
    "region_running_total",
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]

REJECTED_SALES_FIELDNAMES: Final[list[str]] = [
    *VALID_SALES_FIELDNAMES,
    "validation_errors",
]


def validate_sale_record(
    *,
    record: DataRecordDict,
    valid_region_ids: set[str],
    valid_product_ids: set[str],
) -> ValidationResult:
    """Validate one sale record against this project's data contract."""
    errors: list[str] = []

    errors.extend(
        validate_required_fields(record=record, required_fields=SALES_REQUIRED_FIELDS)
    )

    if errors:
        return ValidationResult(is_valid=False, errors=errors)

    if record["region_id"] not in valid_region_ids:
        errors.append(f"Unknown region_id: {record['region_id']!r}")

    if record["product_id"] not in valid_product_ids:
        errors.append(f"Unknown product_id: {record['product_id']!r}")

    if record["currency_code"] not in ALLOWED_CURRENCY_CODES:
        errors.append(f"Invalid currency_code: {record['currency_code']!r}")

    if record["payment_method"] not in ALLOWED_PAYMENT_METHODS:
        errors.append(f"Invalid payment_method: {record['payment_method']!r}")

    errors.extend(validate_datetime(record["datetime"]))
    errors.extend(validate_quantity(record["quantity"]))
    errors.extend(validate_money_amount(record["unit_price"], field_name="unit_price"))
    errors.extend(validate_boolean_text(record["is_online"], field_name="is_online"))

    is_new_customer = str(record.get("is_new_customer", "")).strip()
    if is_new_customer:
        errors.extend(
            validate_boolean_text(is_new_customer, field_name="is_new_customer")
        )

    errors.extend(
        validate_allowed_optional(
            record=record,
            field_name="device_type",
            allowed_values=ALLOWED_DEVICE_TYPES,
        )
    )

    errors.extend(
        validate_allowed_optional(
            record=record,
            field_name="referral_source",
            allowed_values=ALLOWED_REFERRAL_SOURCES,
        )
    )

    return ValidationResult(is_valid=not bool(errors), errors=errors)


def keep_sales_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Return only required sales fields in standard order."""
    return {field: row.get(field, "") for field in SALES_REQUIRED_FIELDS}
