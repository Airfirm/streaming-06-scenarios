"""src/streaming/data_engineering/derived_fields_femi.py.

Derived field calculations for sales messages.

Technical Modification:
- Adds sales channel classification.
- Adds high-value order flag.
- Adds order size band.
- Adds product category enrichment.
- Adds running regional sales support through the consumer.

New Problem:
- Real-time sales performance and customer/channel monitoring.

Author: O S
Date: 2026-06-06
"""

# === DECLARE IMPORTS ===

import logging
from typing import Any, Final

# === DECLARE EXPORTS ===

__all__ = [
    "HIGH_VALUE_TOTAL",
    "TAX_RATE_DEFAULT",
    "classify_order_size",
    "classify_sales_channel",
    "compute_tax_amount",
    "compute_total_price",
    "enrich_message",
    "get_tax_rate",
    "is_high_value_order",
]

# === DECLARE CONSTANTS ===

TAX_RATE_DEFAULT: Final[float] = 0.08
HIGH_VALUE_TOTAL: Final[float] = 250.0

# === CONFIGURE LOGGER ===

LOG = logging.getLogger(__name__)


def compute_total_price(quantity: int, unit_price: float) -> float:
    """Compute the total price before tax."""
    return round(quantity * unit_price, 2)


def compute_tax_amount(total_price: float, tax_rate: float) -> float:
    """Compute the tax amount for an order."""
    return round(total_price * tax_rate, 2)


def get_tax_rate(region_id: str, region_lookup: dict[str, float]) -> float:
    """Look up the tax rate for a region."""
    if region_id in region_lookup:
        return region_lookup[region_id] / 100.0

    LOG.warning(
        f"Region {region_id!r} not in lookup table. "
        f"Using default tax rate {TAX_RATE_DEFAULT}."
    )
    return TAX_RATE_DEFAULT


def classify_sales_channel(is_online: str, device_type: str) -> str:
    """Classify the sales channel from online status and device type."""
    online_text = str(is_online).strip().lower()
    device_text = str(device_type).strip().lower()

    if online_text == "true" and device_text:
        return f"online_{device_text}"

    if online_text == "true":
        return "online_unknown"

    return "offline"


def classify_order_size(total: float) -> str:
    """Classify an order as low, medium, or high value."""
    if total >= 250:
        return "high"

    if total >= 100:
        return "medium"

    return "low"


def is_high_value_order(total: float) -> str:
    """Return yes/no for high-value order status."""
    if total >= HIGH_VALUE_TOTAL:
        return "yes"

    return "no"


def enrich_message(
    row: dict[str, Any],
    region_lookup: dict[str, float],
    product_category_lookup: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Add derived fields to a raw message row.

    Args:
        row: A validated raw message row.
        region_lookup: A dict mapping region_id to tax_rate_pct.
        product_category_lookup: Optional dict mapping product_id to product category.

    Returns:
        A new dict containing original fields plus derived fields.
    """
    quantity = int(row.get("quantity", 0))
    unit_price = float(row.get("unit_price", 0.0))
    region_id = str(row.get("region_id", ""))
    product_id = str(row.get("product_id", ""))

    tax_rate = get_tax_rate(region_id, region_lookup)
    subtotal = compute_total_price(quantity, unit_price)
    tax_amount = compute_tax_amount(subtotal, tax_rate)
    total = round(subtotal + tax_amount, 2)

    product_category = "unknown"
    if product_category_lookup is not None:
        product_category = product_category_lookup.get(product_id, "unknown")

    sales_channel = classify_sales_channel(
        is_online=str(row.get("is_online", "")),
        device_type=str(row.get("device_type", "")),
    )

    order_size_band = classify_order_size(total)
    high_value_order = is_high_value_order(total)

    return {
        **row,
        "subtotal": subtotal,
        "tax_amount": tax_amount,
        "total": total,
        "product_category": product_category,
        "sales_channel": sales_channel,
        "order_size_band": order_size_band,
        "high_value_order": high_value_order,
    }
