"""src/streaming/storage/storage_femi.py.

Project-specific DuckDB storage functions used by the Kafka consumer.

Technical Modification:
- Stores accepted and rejected messages.
- Uses the custom consumed field order from data_contract_femi.py.
- Logs summaries by region, product category, sales channel, and high-value status.

New Problem:
- Real-time sales performance and customer/channel monitoring.

Author: O S
Date: 2026-06-06
"""

# === DECLARE IMPORTS ===

from pathlib import Path
from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.storage.duckdb_sql import (
    build_clear_table_sql,
    build_create_table_sql,
    build_insert_sql,
)
from datafun_toolkit.logger import get_logger
import duckdb

from streaming.data_validation.data_contract_femi import (
    CONSUMED_FIELDNAMES,
    REJECTED_SALES_FIELDNAMES,
)
from streaming.data_validation.data_validation_femi import add_validation_errors

# === DECLARE EXPORTS ===

__all__ = [
    "clear_storage_tables",
    "create_storage_tables",
    "init_db",
    "log_storage_summary",
    "write_rejected_record",
    "write_valid_record",
]

# === CONFIGURE LOGGER ===

LOG = get_logger("C06-STORAGE", level="DEBUG")

# === DECLARE GLOBAL CONSTANTS ===

VALID_TABLE_NAME: Final[str] = "consumed_valid_sales"
REJECTED_TABLE_NAME: Final[str] = "consumed_rejected_sales"

CONSUMED_VALID_FIELDNAMES: Final[list[str]] = CONSUMED_FIELDNAMES

CONSUMED_REJECTED_FIELDNAMES: Final[list[str]] = [
    *REJECTED_SALES_FIELDNAMES,
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]


def clean_valid_consumed_record(record: dict[str, Any]) -> dict[str, Any]:
    """Keep only the fields written to the valid consumed table."""
    return {field: record.get(field, "") for field in CONSUMED_VALID_FIELDNAMES}


def clean_rejected_consumed_record(record: dict[str, Any]) -> dict[str, Any]:
    """Keep only the fields written to the rejected consumed table."""
    return {field: record.get(field, "") for field in CONSUMED_REJECTED_FIELDNAMES}


def create_storage_tables(db_path: Path) -> None:
    """Create consumed message tables if they do not exist."""
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            build_create_table_sql(VALID_TABLE_NAME, CONSUMED_VALID_FIELDNAMES)
        )
        conn.execute(
            build_create_table_sql(REJECTED_TABLE_NAME, CONSUMED_REJECTED_FIELDNAMES)
        )


def clear_storage_tables(db_path: Path) -> None:
    """Clear prior consumed message rows for a fresh run."""
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(build_clear_table_sql(VALID_TABLE_NAME))
        conn.execute(build_clear_table_sql(REJECTED_TABLE_NAME))


def init_db(db_path: Path) -> None:
    """Initialize the DuckDB database for this project."""
    create_storage_tables(db_path)
    clear_storage_tables(db_path)


def write_valid_record(db_path: Path, record: DataRecordDict) -> None:
    """Write one valid consumed sales record to DuckDB."""
    clean_record = clean_valid_consumed_record(record)
    insert_sql = build_insert_sql(VALID_TABLE_NAME, CONSUMED_VALID_FIELDNAMES)
    insert_values = [clean_record[field] for field in CONSUMED_VALID_FIELDNAMES]

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(insert_sql, insert_values)


def write_rejected_record(
    db_path: Path,
    record: DataRecordDict,
    errors: list[str],
) -> None:
    """Write one rejected consumed sales record to DuckDB."""
    rejected_record = add_validation_errors(record=record, errors=errors)
    clean_record = clean_rejected_consumed_record(rejected_record)
    insert_sql = build_insert_sql(REJECTED_TABLE_NAME, CONSUMED_REJECTED_FIELDNAMES)
    insert_values = [clean_record[field] for field in CONSUMED_REJECTED_FIELDNAMES]

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(insert_sql, insert_values)


def log_storage_summary(db_path: Path) -> None:
    """Log DuckDB query results after consuming messages."""
    sql_valid_count = f"SELECT COUNT(*) FROM {VALID_TABLE_NAME}"  # noqa: S608
    sql_rejected_count = f"SELECT COUNT(*) FROM {REJECTED_TABLE_NAME}"  # noqa: S608

    sql_by_region = f"""
        SELECT
            region_id,
            COUNT(*) AS sale_count,
            ROUND(SUM(CAST(total AS DOUBLE)), 2) AS total_sales
        FROM {VALID_TABLE_NAME}
        GROUP BY region_id
        ORDER BY total_sales DESC
        """  # noqa: S608

    sql_by_category = f"""
        SELECT
            product_category,
            COUNT(*) AS sale_count,
            ROUND(SUM(CAST(total AS DOUBLE)), 2) AS total_sales
        FROM {VALID_TABLE_NAME}
        GROUP BY product_category
        ORDER BY total_sales DESC
        """  # noqa: S608

    sql_by_channel = f"""
        SELECT
            sales_channel,
            COUNT(*) AS sale_count,
            ROUND(SUM(CAST(total AS DOUBLE)), 2) AS total_sales
        FROM {VALID_TABLE_NAME}
        GROUP BY sales_channel
        ORDER BY total_sales DESC
        """  # noqa: S608

    sql_high_value = f"""
        SELECT
            high_value_order,
            COUNT(*) AS order_count,
            ROUND(SUM(CAST(total AS DOUBLE)), 2) AS total_sales
        FROM {VALID_TABLE_NAME}
        GROUP BY high_value_order
        ORDER BY total_sales DESC
        """  # noqa: S608

    with duckdb.connect(str(db_path)) as conn:
        valid_result = conn.execute(sql_valid_count).fetchone()
        rejected_result = conn.execute(sql_rejected_count).fetchone()

        valid_count = valid_result[0] if valid_result else 0
        rejected_count = rejected_result[0] if rejected_result else 0

        region_rows = conn.execute(sql_by_region).fetchall()
        category_rows = conn.execute(sql_by_category).fetchall()
        channel_rows = conn.execute(sql_by_channel).fetchall()
        high_value_rows = conn.execute(sql_high_value).fetchall()

    LOG.info(f"DuckDB valid row(s): {valid_count}")
    LOG.info(f"DuckDB rejected row(s): {rejected_count}")

    LOG.info("DuckDB sales by region:")
    for region_id, sale_count, total_sales in region_rows:
        LOG.info(f"  {region_id}: {sale_count} sale(s), total=${total_sales}")

    LOG.info("DuckDB sales by product category:")
    for product_category, sale_count, total_sales in category_rows:
        LOG.info(f"  {product_category}: {sale_count} sale(s), total=${total_sales}")

    LOG.info("DuckDB sales by channel:")
    for sales_channel, sale_count, total_sales in channel_rows:
        LOG.info(f"  {sales_channel}: {sale_count} sale(s), total=${total_sales}")

    LOG.info("DuckDB high-value order summary:")
    for high_value_order, order_count, total_sales in high_value_rows:
        LOG.info(f"  {high_value_order}: {order_count} order(s), total=${total_sales}")
