# Module 6: Database Integration with Streaming Pipelines - streaming-06-scenarios

## Overview

This project demonstrates how to build a Kafka streaming pipeline that reads
sales data from a CSV file, sends valid records to a Kafka topic, consumes
those records, enriches the messages, stores the results in DuckDB, writes
output files, and creates a live sales chart.

The project shows how streaming data can be validated, processed, enriched,
stored, and analyzed after consumption.

## Project Scenario

The custom problem for this module is:
**Real-Time Sales Performance and Customer/Channel Monitoring**

The goal is to monitor sales transactions as they move through a Kafka
streaming pipeline. Each message represents one sales order. The consumer
enriches each message with business intelligence fields that help identify
sales performance by region, product category, sales channel, and order value.

## Technologies Used

This project uses:

- Python
- Kafka
- DuckDB
- CSV files
- Matplotlib
- PowerShell
- WSL Ubuntu
- VS Code
- uv

## Project Files

The main custom files for this project are:

```text
src/streaming/kafka_admin_femi.py
src/streaming/kafka_producer_femi.py
src/streaming/kafka_consumer_femi.py
src/streaming/data_engineering/derived_fields_femi.py
src/streaming/data_validation/data_contract_femi.py
src/streaming/data_validation/data_validation_femi.py
src/streaming/storage/storage_femi.py
src/streaming/visualizations/live_visualizations_femi.py


Dataset

The producer streams data from:

data/sales.csv

This file contains sales transaction records. Each row represents one sales order.

The sales records include fields such as:

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

The project also uses static reference tables:

data/regions.csv
data/products.csv
data/currencies.csv
data/discount_codes.csv

These files are not streamed directly. They are used for validation and enrichment. For example, regions.csv provides tax rates, and products.csv provides product category information.

Kafka Topic

The Kafka topic used for this project is:

streaming-06-scenarios-case

The producer sends valid sales records to this topic.

The Kafka message key is:

region_id

Using region_id as the key helps group sales messages by region.

Producer

The producer file is:

src/streaming/kafka_producer_femi.py

The producer reads records from data/sales.csv, validates each record, and sends valid records to Kafka.

If a record is invalid, it is not sent to Kafka. Instead, it is written to:

data/output/producer_rejected_sales_femi.csv

The producer validates records using the custom data contract and reference tables.

Consumer

The consumer file is:

src/streaming/kafka_consumer_femi.py

The consumer reads messages from the Kafka topic and processes each sales record.

For each valid consumed message, the consumer:

validates required fields
calculates subtotal
calculates tax amount
calculates total
adds product category
classifies the sales channel
creates an order size band
flags high-value orders
tracks running sales total by region
updates running statistics
updates a live chart
writes accepted records to CSV
stores accepted records in DuckDB
stores rejected records in DuckDB
Technical Modifications

Several technical modifications were made for this project.

Modified Output File Names

The output files were customized with _femi in the file names.

Expected output files include:

data/output/producer_rejected_sales_femi.csv
data/output/consumed_sales_femi.csv
data/output/sales_femi.duckdb
data/output/sales_chart_femi.png
Added Derived Fields

The consumer now adds new analytics fields to each accepted sales message:

subtotal
tax_amount
total
product_category
sales_channel
order_size_band
high_value_order
region_running_total
Added Product Category Enrichment

The consumer uses data/products.csv to add product category information to each consumed sales message.

Added Sales Channel Classification

The consumer classifies each sale based on whether it was online and which device type was used.

Examples include:

online_mobile
online_desktop
online_tablet
offline
Added High-Value Order Flag

The consumer flags orders as high-value if they meet or exceed the high-value threshold.

The field is:

high_value_order
Added Order Size Band

The consumer classifies each order as:

low
medium
high
Added Running Regional Sales Total

The consumer tracks how much revenue is accumulating by region while messages are consumed.

The field is:

region_running_total
Added DuckDB Storage Summary

The storage file summarizes consumed data by:

valid row count
rejected row count
sales by region
sales by product category
sales by channel
high-value order status
New Problem Applied

For Phase 5, I applied the streaming skills to a new business problem:

Real-time sales performance and customer/channel monitoring

This new problem uses the Kafka pipeline to help answer questions such as:

Which regions are generating sales?
Which product categories are being purchased?
Which sales channels are being used?
Which orders are high-value?
How much revenue is accumulating by region?
How many records were accepted or rejected?
How to Run the Project

Before running the Python files, make sure Kafka is running in WSL.

Then run the files from the project root folder in this order.

1. Run the Kafka admin file
uv run python -m streaming.kafka_admin_femi

This verifies or creates the Kafka topic.

2. Run the Kafka producer
uv run python -m streaming.kafka_producer_femi

This reads sales records from data/sales.csv, validates them, and sends valid messages to Kafka.

3. Run the Kafka consumer
uv run python -m streaming.kafka_consumer_femi

This consumes messages from Kafka, enriches them, stores them, writes output files, and saves the chart.

Output Artifacts

The project creates output artifacts in:

data/output/

Expected output artifacts are:

producer_rejected_sales_femi.csv
consumed_sales_femi.csv
sales_femi.duckdb
sales_chart_femi.png

The CSV file can be opened and reviewed directly.

The DuckDB file is a database file and should be reviewed using DuckDB queries or through the terminal summary logs.

The PNG chart shows the sales total by Kafka message.

Results

When the project runs successfully, the admin file verifies that the Kafka topic exists.

The producer sends valid sales messages to Kafka and writes any rejected records to the rejected producer CSV file.

The consumer reads the Kafka messages and enriches each valid record with new analytics fields. It writes accepted records to a CSV file, stores records in DuckDB, updates a live chart, and saves the final chart image.

The consumer also logs running statistics, including:

total sales
average sale
minimum sale
maximum sale
accepted message count
skipped message count

The DuckDB summary logs provide additional insights about sales by region, product category, sales channel, and high-value order status.

Interpretation

This project shows how Kafka can be used to move data through a streaming pipeline. The producer sends sales messages to Kafka, Kafka stores the messages in a topic, and the consumer reads and processes those messages.

The original example focused on basic message streaming. My custom version adds stronger validation, enrichment, charting, DuckDB storage, and business intelligence fields.

The stream could help a business monitor sales activity in real time. Instead of only seeing raw sales records, the business can understand sales totals, regional activity, product category performance, channel performance, and high-value order activity.

The business intelligence gained from the consumed messages includes:

total revenue
average order value
minimum and maximum order values
sales by region
sales by product category
sales by sales channel
high-value order counts
rejected message tracking

Overall, this module helped me understand how streaming data and stored data work together. Kafka handles the movement of data, while CSV files, DuckDB, and charts make the data useful for review and analysis.

What I Learned

In this module, I learned how to connect a Kafka producer and consumer to a database-backed streaming pipeline. I learned how to validate records before sending them, enrich records after consuming them, and save the processed results for later analysis.

I also learned that field names and data types must match across the producer, consumer, data contract, storage file, and output files. Small mismatches can cause errors, so testing each step is important.

This project helped me better understand how real-time data pipelines can support business decisions.
