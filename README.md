# Streaming Data Pipeline Project - streaming-06-scenarios

## Project Overview

This project demonstrates a Python-based streaming data pipeline using Kafka. The project shows how
data can move from a CSV file into a Kafka topic through a producer, then be consumed, processed,
logged, stored, and reviewed through output artifacts.

The purpose of this project is to show how a streaming workflow works from beginning to end.

It includes a Kafka admin file, a producer, a consumer, validation logic, derived field calculations,
DuckDB storage, CSV output, and a saved visualization.

## Custom Project Focus

The custom focus of this project is:
**Real-Time Sales Performance and Customer/Channel Monitoring**

The pipeline streams sales transaction data and turns it into useful business intelligence.
The consumer enriches each message with calculated fields and custom analytics fields that help
monitor sales by region, product category, sales channel, order size, and high-value order status.

## Repository Contents

Important project files include:

```text
src/streaming/kafka_admin_femi.py
src/streaming/kafka_producer_femi.py
src/streaming/kafka_consumer_femi.py
src/streaming/data_engineering/derived_fields_femi.py
src/streaming/data_validation/data_contract_femi.py
src/streaming/data_validation/data_validation_femi.py
src/streaming/storage/storage_femi.py
src/streaming/visualizations/live_visualizations_femi.py
```

Important data files include:

```text
data/sales.csv
data/regions.csv
data/products.csv
data/currencies.csv
data/discount_codes.csv
```

Expected output files include:

```text
data/output/producer_rejected_sales_femi.csv
data/output/consumed_sales_femi.csv
data/output/sales_femi.duckdb
data/output/sales_chart_femi.png
```

## Dataset

The Kafka producer streams records from:

```text
data/sales.csv
```

This dataset contains sales transaction records. Each row represents one sales order.

The sales records include fields such as:

```text
order_id
datetime
region_id
currency_code
product_id
unit_price
quantity
is_online
customer_id
is_new_customer
device_type
payment_method
referral_source
discount_code
customer_note
```

The project also uses static reference tables:

```text
data/regions.csv
data/products.csv
data/currencies.csv
data/discount_codes.csv
```

These reference tables are not streamed directly. They are used for validation and enrichment.
For example, `regions.csv` provides tax rates, and `products.csv` provides product category information.

## Kafka Topic

The Kafka topic used for this project is:

```text
streaming-06-scenarios-femi
```

The producer sends valid sales records to this topic.

The Kafka message key is:

```text
region_id
```

Using `region_id` as the message key helps group sales messages by region.

## Producer

The producer file is:

```text
src/streaming/kafka_producer_femi.py
```

The producer reads sales records from `data/sales.csv`, validates them, and sends valid records to Kafka.

If a record is invalid, the producer does not send it to Kafka. Instead, it writes the rejected record
to:

```text
data/output/producer_rejected_sales_femi.csv
```

## Consumer

The consumer file is:

```text
src/streaming/kafka_consumer_femi.py
```

The consumer reads messages from Kafka and processes each sales record.

For each valid consumed message, the consumer:

* validates required fields
* calculates subtotal
* calculates tax amount
* calculates total
* adds product category
* classifies the sales channel
* creates an order size band
* flags high-value orders
* tracks running sales total by region
* updates running statistics
* updates a live chart
* writes accepted records to CSV
* stores accepted records in DuckDB
* stores rejected records in DuckDB

## Technical Modifications

Several technical modifications were made for this project.

The output files were customized with `_femi` in the file names.

The consumer was modified to add new analytics fields, including:

```text
subtotal
tax_amount
total
product_category
sales_channel
order_size_band
high_value_order
region_running_total
```

The storage logic was updated so DuckDB stores accepted and rejected consumed records.
The DuckDB summary logs also report sales by region, product category, sales channel,
and high-value order status.

The live visualization was customized to show real-time sales totals by Kafka message.

## New Problem Applied

For the custom application, I applied the streaming pipeline to a real-time sales performance
and customer/channel monitoring problem.

This helps answer questions such as:

* Which regions are generating sales?
* Which product categories are being purchased?
* Which sales channels are being used?
* Which orders are high-value?
* How much revenue is accumulating by region?
* How many records were accepted or rejected?

## Setup Instructions

### 1. Clone the repository

```powershell
git clone https://github.com/Airfirm/streaming-06-scenarios
cd streaming-06-scenarios
```

### 2. Install dependencies

```powershell
uv sync
```

### 3. Start Kafka

Kafka should be running before the producer or consumer files are executed.

In WSL, start Kafka from the Kafka folder:

```bash
bin/kafka-server-start.sh config/server.properties
```

Leave this terminal open while running the project.

### 4. Verify the project

From the project root folder in PowerShell, run:

```powershell
uv run python -m streaming.kafka_admin_femi
```

This verifies that Kafka is reachable and that the topic exists.

## How to Run the Project

Run the files from the project root folder in this order.

### Step 1: Run the Kafka admin file

```powershell
uv run python -m streaming.kafka_admin_femi
```

### Step 2: Run the Kafka producer

```powershell
uv run python -m streaming.kafka_producer_femi
```

### Step 3: Run the Kafka consumer

```powershell
uv run python -m streaming.kafka_consumer_femi
```

## Expected Results

When the project runs successfully, the admin file verifies the Kafka topic.

The producer sends valid sales messages to Kafka and writes rejected records to a rejected records
CSV file if any records fail validation.

The consumer reads the Kafka messages, enriches them, writes accepted records to CSV, stores records
in DuckDB, updates a live chart, and saves the final chart image.

The consumer logs running statistics such as:

```text
total sales
average sale
minimum sale
maximum sale
accepted message count
skipped message count
```

The DuckDB summary logs provide additional information about:

```text
sales by region
sales by product category
sales by sales channel
high-value order status
valid row count
rejected row count
```

## Output Artifacts

The output artifacts are saved in:

```text
data/output/
```

Expected output files include:

```text
producer_rejected_sales_femi.csv
consumed_sales_femi.csv
sales_femi.duckdb
sales_chart_femi.png
```

The CSV files can be opened directly.

The DuckDB file is a database file and should be reviewed using DuckDB queries or through the terminal
summary logs.

The PNG file shows the saved chart from the streaming pipeline.

## Troubleshooting

If the producer or consumer cannot connect to Kafka, make sure Kafka is running and that the project
is using:

```text
localhost:9092
```

If the consumer says the topic is empty, run the producer first.

If DuckDB gives a type error when calculating totals, check that numeric fields are cast correctly
in SQL queries, such as:

```sql
SUM(CAST(total AS DOUBLE))
```

If output files do not appear, make sure the project was run from the repository root folder and
that the `data/output/` folder exists.

## Peer Review Notes

For peer review, reviewers should check whether:

* the repository is organized clearly
* the README explains how to set up and run the project
* Kafka can be started successfully
* the admin file verifies the topic
* the producer sends data
* the consumer processes data
* output artifacts are created
* there are no unexpected errors
* the custom modifications are explained clearly

## Interpretation

This project shows how a streaming data pipeline moves data from a source file into Kafka and then
into a consumer for processing and storage.

The original example focused on basic Kafka message movement. My custom version adds validation,
enrichment, DuckDB storage, CSV output, and visualization.

The stream could help a business monitor sales activity in real time. Instead of only seeing raw
records, the business can understand revenue, product category activity, customer channel activity,
high-value orders, and regional sales performance.

The business intelligence gained from the consumed messages includes:

* total revenue
* average order value
* minimum and maximum order values
* sales by region
* sales by product category
* sales by sales channel
* high-value order counts
* rejected message tracking

Overall, this project helped me understand how streaming data and stored data work together.
Kafka handles the movement of data, while CSV files, DuckDB, logs, and charts make the data
useful for review and analysis.

## AI Disclosure

AI assistance was used to help review, debug, and improve parts of this project. I reviewed the
suggested changes, tested the project, and remain responsible for the submitted work.
