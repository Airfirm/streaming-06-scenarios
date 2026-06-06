# Database Integration with Streaming Pipelines Scenarios

## Custom Project

### Dataset

For this custom project, I used the original sales dataset located in:

`data/sales.csv`

This dataset contains online sales transaction records. Each row represents
one sales order that can be sent through Kafka as a streaming message.

Each sales record includes fields such as:

- `order_id`
- `datetime`
- `region_id`
- `currency_code`
- `product_id`
- `unit_price`
- `quantity`
- `is_online`
- `customer_id`
- `is_new_customer`
- `device_type`
- `payment_method`
- `referral_source`
- `discount_code`
- `customer_note`

I used the original sales dataset, but I modified the processing logic to
create additional business intelligence fields after the messages were consumed.

The project also uses static reference files, including:

- `data/regions.csv`
- `data/products.csv`
- `data/currencies.csv`
- `data/discount_codes.csv`

These reference files are not streamed directly. They are used to validate and enrich the sales messages.

### Kafka Messages

The Kafka producer sends sales records from `data/sales.csv` to the Kafka topic.

The Kafka topic used for this project is:

`streaming-06-scenarios-case`

Each message sent through Kafka represents one sales order. The producer
validates each sales record before sending it. If a record is valid, it is
sent to Kafka. If a record is invalid, it is written to a rejected records CSV file.

The message key used by the producer is:

`region_id`

I used `region_id` as the Kafka message key because it helps group sales
messages by region. This is useful for tracking regional sales activity.

I did not change the raw message fields sent by the producer. Instead, I
changed the consumer processing so the consumer adds new fields after receiving each message.

### Consumer Processing

The Kafka consumer receives sales messages from the Kafka topic and processes
each message one at a time.

For each consumed message, the consumer:

- validates required fields
- enriches the message with calculated fields
- adds product category information from `products.csv`
- calculates subtotal, tax amount, and total
- classifies the sales channel
- creates an order size band
- flags high-value orders
- tracks running sales total by region
- writes accepted records to a CSV file
- stores accepted and rejected records in DuckDB
- updates and saves a live chart

The consumer writes accepted records to:

`data/output/consumed_sales_femi.csv`

The consumer stores database output in:

`data/output/sales_femi.duckdb`

The consumer saves the chart output as:

`data/output/sales_chart_femi.png`

The producer writes rejected records to:

`data/output/producer_rejected_sales_femi.csv`

The consumer processes selected fields from the original message and adds new fields, including:

- `subtotal`
- `tax_amount`
- `total`
- `product_category`
- `sales_channel`
- `order_size_band`
- `high_value_order`
- `region_running_total`

These fields make the stream more useful for business analysis.

### Experiments

For my Phase 4 technical modification, I updated the project so the output
files include `_femi` in their file names. This makes my output artifacts easier to identify.

I also modified the derived fields and consumer logic. The original example
calculated basic sales totals, but I expanded the consumer to create more
business-focused analytics fields.

Specific changes included:

- adding product category enrichment from `products.csv`
- adding sales channel classification
- adding high-value order status
- adding order size bands
- adding running sales totals by region
- saving accepted records to CSV
- storing accepted and rejected records in DuckDB
- saving a live sales chart image

For my Phase 5 application, I applied the skills to a new problem: real-time
sales performance and customer/channel monitoring.

Instead of only streaming and consuming sales messages, my updated project
helps answer business questions such as:

- Which region is generating sales?
- Which product categories are being purchased?
- Which orders are high-value?
- Which sales channels are being used?
- How much revenue is accumulating by region?
- How many messages were accepted or rejected?

### Results

When I ran the project, the Kafka admin file verified that the Kafka topic existed.

Then the producer read sales records from `data/sales.csv`, validated them,
and sent valid records to the Kafka topic. Rejected records were written to
the rejected producer output file if any validation errors were found.

After that, the consumer read the messages from Kafka. It processed each
message, added derived fields, updated running statistics, wrote accepted
records to CSV, stored results in DuckDB, and saved a live chart.

The consumer output showed running sales statistics such as:

- total sales
- average sale
- minimum sale
- maximum sale
- accepted message count
- skipped message count

The DuckDB storage summary also provided information about sales by region,
sales by product category, sales by channel, and high-value order status.

### Interpretation

This Kafka streaming workflow showed how data can move from a CSV file into a
Kafka topic and then into a consumer for processing, enrichment, storage, and analysis.

The original example focused mainly on sending and receiving sales messages.
My modification changed the project into a more complete streaming analytics
pipeline by adding validation, enrichment, database storage, and business intelligence fields.

Watching the messages move through Kafka helped me understand the role of each
part of the pipeline. The producer prepares and sends messages. Kafka holds
the messages in a topic. The consumer reads the messages, processes them, and saves the results.

This type of stream could tell a business which sales are happening, where
they are coming from, how much revenue is being generated, and which channels
or product categories are most active.

The business intelligence gained from the consumed messages includes:

- total revenue from consumed sales
- average order value
- minimum and maximum order values
- sales by region
- sales by product category
- sales by customer channel
- high-value order activity
- rejected message tracking

Overall, this project showed that streaming data becomes more valuable when it
is validated, enriched, stored, and summarized. The final pipeline supports
both real-time monitoring and later analysis using CSV, DuckDB, and saved
chart outputs.
