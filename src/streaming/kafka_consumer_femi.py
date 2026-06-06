"""src/streaming/kafka_consumer_femi.py.

Kafka consumer: full pipeline example.

Reads sales messages from a Kafka topic and runs the full pipeline:
  - Validates each message against the data contract
  - Computes derived fields
  - Applies custom analytics for a new business problem
  - Updates a live chart
  - Stores accepted and rejected messages in DuckDB
  - Writes accepted messages to a CSV file

Technical Modification:
- Adds product category enrichment from products.csv.
- Adds sales channel classification.
- Adds order size band.
- Adds high-value order flag.
- Adds running sales total by region.
- Stores rejected consumed messages in DuckDB.
- Uses _femi output file names.

New Problem:
- Real-time sales performance and customer/channel monitoring.

Author: O S
Date: 2026-06-06

Run from the root project folder:

    uv run python -m streaming.kafka_consumer_femi
"""

# === DECLARE IMPORTS ===

import os
from pathlib import Path
from typing import Any, Final

from confluent_kafka.cimpl import OFFSET_BEGINNING, TopicPartition
from datafun_streaming.io.io_utils import append_csv_row, read_csv_as_lookup
from datafun_streaming.kafka.kafka_admin_utils import (
    create_admin_client,
    get_topic_message_count,
    topic_exists,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_consumer_utils import (
    consume_kafka_message,
    create_consumer,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_streaming.stats.stats_utils import RunningStats
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars
from streaming.data_engineering.derived_fields_femi import enrich_message
from streaming.data_validation.data_contract_femi import (
    CONSUMED_FIELDNAMES,
    SALES_REQUIRED_FIELDS,
    validate_required_fields,
)
from streaming.storage.storage_femi import (
    init_db,
    log_storage_summary,
    write_rejected_record,
    write_valid_record,
)
from streaming.visualizations.live_visualizations_femi import (
    close_live_chart,
    init_live_chart,
    save_live_chart,
    update_live_chart,
)

# === CONFIGURE LOGGER ===

LOG = get_logger("C06", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

COURSE_NAME: Final[str] = "Streaming Data"
TIMEOUT_SECONDS: Final[float] = float(os.getenv("CONSUMER_TIMEOUT_SECONDS", "10.0"))
MAX_MESSAGES: Final[int] = int(os.getenv("CONSUMER_MAX_MESSAGES", "1000"))

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

OUTPUT_CSV: Final[Path] = OUTPUT_DIR / "consumed_sales_femi.csv"
OUTPUT_DB: Final[Path] = OUTPUT_DIR / "sales_femi.duckdb"
OUTPUT_CHART: Final[Path] = OUTPUT_DIR / "sales_chart_femi.png"

REGIONS_CSV: Final[Path] = DATA_DIR / "regions.csv"
PRODUCTS_CSV: Final[Path] = DATA_DIR / "products.csv"
CURRENCIES_CSV: Final[Path] = DATA_DIR / "currencies.csv"
DISCOUNT_CODES_CSV: Final[Path] = DATA_DIR / "discount_codes.csv"


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "C06")
    LOG.info("========================")
    LOG.info("START consumer main()")
    LOG.info("========================")
    LOG.info("Project: %s", COURSE_NAME)
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "OUTPUT_DB", OUTPUT_DB)
    log_path(LOG, "OUTPUT_CHART", OUTPUT_CHART)
    log_path(LOG, "REGIONS_CSV", REGIONS_CSV)
    log_path(LOG, "PRODUCTS_CSV", PRODUCTS_CSV)
    log_path(LOG, "CURRENCIES_CSV", CURRENCIES_CSV)
    log_path(LOG, "DISCOUNT_CODES_CSV", DISCOUNT_CODES_CSV)


def load_settings() -> KafkaSettings:
    """Load settings from .env and log them."""
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS  = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC              = {settings.topic}")
    LOG.info(f"KAFKA_GROUP_ID           = {settings.group_id}")
    LOG.info(f"CONSUMER_TIMEOUT_SECONDS = {TIMEOUT_SECONDS}")
    LOG.info(f"CONSUMER_MAX_MESSAGES    = {MAX_MESSAGES}")
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify Kafka is reachable before doing anything else."""
    LOG.info("Verifying Kafka connection...")

    try:
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(str(error))
        raise SystemExit(1) from error


def verify_topic(settings: KafkaSettings) -> None:
    """Verify the topic exists and has messages."""
    LOG.info("Verifying Kafka topic...")
    admin = create_admin_client(settings)

    if not topic_exists(admin, settings.topic):
        LOG.error(f"Topic {settings.topic!r} does not exist.")
        LOG.error("Run the producer first.")
        raise SystemExit(1)

    message_count = get_topic_message_count(admin, settings.topic, settings)
    LOG.info(f"Topic {settings.topic!r} exists.")
    LOG.info(f"Found {message_count} message(s) available.")

    if message_count == 0:
        LOG.error("Topic is empty. Run the producer first.")
        raise SystemExit(1)


def get_kafka_consumer(settings: KafkaSettings) -> Any:
    """Create a Kafka consumer subscribed to the topic."""
    LOG.info("Creating Kafka consumer...")
    consumer = create_consumer(settings)

    consumer.subscribe(
        [settings.topic],
        on_assign=lambda c, partitions: c.assign(
            [
                TopicPartition(
                    partition.topic,
                    partition.partition,
                    OFFSET_BEGINNING,
                )
                for partition in partitions
            ]
        ),
    )

    LOG.info(f"Subscribed to topic: {settings.topic!r} (reading from beginning)")
    return consumer


def initialize_output() -> tuple[Any, Any, list[int], list[float], RunningStats]:
    """Initialize output resources."""
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()

    LOG.info(f"Output CSV cleared: {OUTPUT_CSV.name}")

    init_db(OUTPUT_DB)
    LOG.info(f"Database initialized: {OUTPUT_DB.name}")

    figure, axis, x_values, y_values = init_live_chart()
    LOG.info("Live chart initialized.")

    stats = RunningStats()

    return figure, axis, x_values, y_values, stats


def load_reference_data() -> tuple[dict[str, float], dict[str, str]]:
    """Load reference data used for message enrichment."""
    LOG.info("Loading enrichment reference data...")

    region_rows = read_csv_as_lookup(
        REGIONS_CSV,
        key_field="region_id",
        value_field="tax_rate_pct",
    )

    region_lookup: dict[str, float] = {}
    for region_id, tax_rate_pct in region_rows.items():
        region_lookup[str(region_id)] = float(tax_rate_pct)

    product_rows = read_csv_as_lookup(
        PRODUCTS_CSV,
        key_field="product_id",
        value_field="category",
    )

    product_category_lookup: dict[str, str] = {}
    for product_id, category in product_rows.items():
        product_category_lookup[str(product_id)] = str(category)

    LOG.info(f"Found {len(region_lookup)} region tax rates.")
    LOG.info(f"Found {len(product_category_lookup)} product categories.")

    return region_lookup, product_category_lookup


def process_message(
    row: dict[str, Any],
    *,
    region_lookup: dict[str, float],
    product_category_lookup: dict[str, str],
    stats: RunningStats,
    region_sales_totals: dict[str, float],
    figure: Any,
    axis: Any,
    x_values: list[int],
    y_values: list[float],
) -> tuple[dict[str, Any] | None, list[str]]:
    """Process one consumed message."""
    errors = validate_required_fields(record=row, required_fields=SALES_REQUIRED_FIELDS)

    if errors:
        LOG.warning(f"Validation failed for order {row.get('order_id', '?')}")
        LOG.warning(f"errors={errors}")
        return None, errors

    try:
        enriched = enrich_message(
            row,
            region_lookup,
            product_category_lookup,
        )

        total = float(enriched["total"])
        region_id = str(enriched["region_id"])

        previous_region_total = region_sales_totals.get(region_id, 0.0)
        new_region_total = previous_region_total + total
        region_sales_totals[region_id] = new_region_total

        enriched["region_running_total"] = round(new_region_total, 2)

    except (KeyError, TypeError, ValueError) as error:
        error_message = f"Processing failed: {error}"
        LOG.warning(error_message)
        return None, [error_message]

    LOG.info(
        f"subtotal={enriched['subtotal']}  "
        f"tax={enriched['tax_amount']}  "
        f"total={enriched['total']}  "
        f"category={enriched['product_category']}  "
        f"channel={enriched['sales_channel']}  "
        f"band={enriched['order_size_band']}  "
        f"high_value={enriched['high_value_order']}  "
        f"region_running_total={enriched['region_running_total']}  "
        f"running_total={stats.total + float(enriched['total']):.2f}"
    )

    stats.update(float(enriched["total"]))

    update_live_chart(
        figure=figure,
        axis=axis,
        x_values=x_values,
        y_values=y_values,
        message=enriched,
    )

    return enriched, []


def consume_messages(
    consumer: Any,
    *,
    region_lookup: dict[str, float],
    product_category_lookup: dict[str, str],
    stats: RunningStats,
    figure: Any,
    axis: Any,
    x_values: list[int],
    y_values: list[float],
) -> tuple[int, int]:
    """Consume and process messages from the Kafka topic."""
    LOG.info("Consuming messages...")
    LOG.info(f"Waiting for up to {MAX_MESSAGES} message(s).")
    LOG.info("Press CTRL+C to stop early.\n")

    consumed_count = 0
    skipped_count = 0
    region_sales_totals: dict[str, float] = {}

    while consumed_count + skipped_count < MAX_MESSAGES:
        row = consume_kafka_message(
            consumer=consumer,
            timeout_seconds=TIMEOUT_SECONDS,
        )

        if row is None:
            LOG.info(f"No message received within {TIMEOUT_SECONDS}s timeout.")
            LOG.info("Producer finished or paused. Stopping consumer.")
            break

        LOG.info(row)

        enriched, errors = process_message(
            row,
            region_lookup=region_lookup,
            product_category_lookup=product_category_lookup,
            stats=stats,
            region_sales_totals=region_sales_totals,
            figure=figure,
            axis=axis,
            x_values=x_values,
            y_values=y_values,
        )

        if enriched is None:
            skipped_count += 1

            write_rejected_record(
                db_path=OUTPUT_DB,
                record=row,
                errors=errors,
            )

            LOG.warning("MESSAGE REJECTED")
            LOG.warning(f"order={row.get('order_id', '?')}")
            LOG.warning(f"skipped={skipped_count}")
            continue

        write_valid_record(OUTPUT_DB, enriched)

        LOG.info("Wrote valid record to DuckDB:")
        LOG.info(f"  order={enriched['order_id']}")

        append_csv_row(
            path=OUTPUT_CSV,
            row={field: enriched.get(field, "") for field in CONSUMED_FIELDNAMES},
            fieldnames=CONSUMED_FIELDNAMES,
        )

        consumed_count += 1

        LOG.info("MESSAGE ACCEPTED")
        LOG.info(f"order={enriched['order_id']}")
        LOG.info(f"total=${float(enriched['total']):.2f}")
        LOG.info(f"category={enriched['product_category']}")
        LOG.info(f"channel={enriched['sales_channel']}")
        LOG.info(f"band={enriched['order_size_band']}")
        LOG.info(f"high_value={enriched['high_value_order']}")
        LOG.info(f"region_running_total={enriched['region_running_total']}")
        LOG.info(f"consumed={consumed_count}")

        LOG.info("RUNNING STATS")
        LOG.info(f"total_sales=${stats.total:,.2f}")
        LOG.info(f"average=${stats.mean:,.2f}")
        LOG.info(f"min=${stats.minimum:,.2f}")
        LOG.info(f"max=${stats.maximum:,.2f}")

    return consumed_count, skipped_count


def save_artifacts(figure: Any) -> None:
    """Save output artifacts and log DuckDB summary results."""
    LOG.info("Saving artifacts...")

    save_live_chart(figure=figure, chart_path=OUTPUT_CHART)

    log_path(LOG, "WROTE OUTPUT_CHART", OUTPUT_CHART)
    log_path(LOG, "WROTE OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "WROTE OUTPUT_DB", OUTPUT_DB)

    log_storage_summary(OUTPUT_DB)


def log_summary(
    consumed_count: int,
    skipped_count: int,
    stats: RunningStats,
    settings: KafkaSettings,
) -> None:
    """Log final summary statistics."""
    LOG.info("Summary:")
    LOG.info(f"Consumed {consumed_count} message(s) from topic {settings.topic!r}.")
    LOG.info(f"Skipped  {skipped_count} message(s).")
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "OUTPUT_DB", OUTPUT_DB)
    log_path(LOG, "OUTPUT_CHART", OUTPUT_CHART)

    if stats.count > 0:
        LOG.info(f"  Total sales:  ${stats.total:,.2f}")
        LOG.info(f"  Average sale: ${stats.mean:,.2f}")
        LOG.info(f"  Minimum sale: ${stats.minimum:,.2f}")
        LOG.info(f"  Maximum sale: ${stats.maximum:,.2f}")

    LOG.info("========================")
    LOG.info("Consumer executed successfully!")
    LOG.info("========================")


def main() -> None:
    """Main entry point for the Kafka consumer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    verify_topic(settings)
    consumer = get_kafka_consumer(settings)

    LOG.info("========================")
    LOG.info("SECTION C. Consume and Process Messages")
    LOG.info("========================")

    figure, axis, x_values, y_values, stats = initialize_output()
    region_lookup, product_category_lookup = load_reference_data()

    consumed_count = 0
    skipped_count = 0

    try:
        try:
            consumed_count, skipped_count = consume_messages(
                consumer,
                region_lookup=region_lookup,
                product_category_lookup=product_category_lookup,
                stats=stats,
                figure=figure,
                axis=axis,
                x_values=x_values,
                y_values=y_values,
            )
        finally:
            consumer.close()
            LOG.info("Kafka consumer closed.")

        save_artifacts(figure)

    finally:
        close_live_chart()
        LOG.info("Live chart closed.")

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    log_summary(consumed_count, skipped_count, stats, settings)


if __name__ == "__main__":
    main()
