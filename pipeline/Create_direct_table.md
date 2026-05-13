# Databricks Structured Streaming — Single Pipeline Method

> Architecture: Azure Data Lake Storage → Bronze → Silver → Gold  
> Pattern: Single Notebook Pipeline using Structured Streaming

---

# Overview

This implementation demonstrates:

- Sample Data Generation
- Auto Loader
- Structured Streaming
- Bronze → Silver → Gold Architecture
- Single Notebook Pipeline
- Delta Lake Tables
- Incremental Processing

---

# Architecture Flow

```text
Sample JSON Data
        ↓
Bronze Layer
(Raw Streaming Data)
        ↓
Silver Layer
(Cleaned & Enriched Data)
        ↓
Gold Layer
(Aggregated Business Metrics)
```

---

# 1. Create Storage Directory

## Description
Creates a storage directory in Azure Data Lake Storage for storing sample JSON files.

This folder acts as the streaming source location.

## Command

```python
dbutils.fs.mkdirs(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/orders"
)
```

---

# 2. Generate Sample Streaming Data

## Description
Creates 250 random order records and stores them as JSON files.

These records simulate incoming streaming transactions.

## Command

```python
import json
import random

# ------------------------------------------------------------
# Sample Master Data
# ------------------------------------------------------------

customers = [
    "Ravi", "Arjun", "Kiran", "Meena", "John",
    "Priya", "Vijay", "Anu", "Rahul", "Sneha",
    "Aakash", "Divya", "Manoj", "Keerthi", "Surya"
]

cities = [
    "Chennai", "Bangalore", "Hyderabad",
    "Mumbai", "Delhi", "Pune",
    "Kolkata", "Coimbatore"
]

products = [
    "Laptop", "Mobile", "Tablet", "Watch",
    "Headphones", "Camera", "TV", "Keyboard"
]

payment_modes = [
    "UPI", "Card", "Cash", "NetBanking"
]

status_list = [
    "SUCCESS", "PENDING", "FAILED"
]

# ------------------------------------------------------------
# Generate Records
# ------------------------------------------------------------

records = []

for i in range(1, 251):

    order = {
        "order_id": i,
        "customer": random.choice(customers),
        "city": random.choice(cities),
        "product": random.choice(products),
        "amount": random.randint(500, 15000),
        "quantity": random.randint(1, 5),
        "payment_mode": random.choice(payment_modes),
        "order_status": random.choice(status_list)
    }

    records.append(json.dumps(order))

# ------------------------------------------------------------
# Convert to Multiline JSON
# ------------------------------------------------------------

data = "\n".join(records)

# ------------------------------------------------------------
# Write JSON File into ADLS
# ------------------------------------------------------------

dbutils.fs.put(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/orders/orders_bulk_250.json",
    data,
    True
)

print("250 streaming JSON records created successfully")
```

---

# 3. Bronze Layer — Raw Streaming Ingestion

## Description
Reads JSON files continuously using Auto Loader and stores raw records into the Bronze Delta table.

Bronze layer stores raw unprocessed data.

## Command

```python
bronze_df = (

    spark.readStream

         .format("cloudFiles")

         .option(
             "cloudFiles.format",
             "json"
         )

         .option(
             "cloudFiles.schemaLocation",
             "abfss://raw-data@databricktesdat.dfs.core.windows.net/checkpoints/schema/orders"
         )

         .load(
             "abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/orders"
         )
)

(
    bronze_df.writeStream

        .format("delta")

        .option(
            "checkpointLocation",
            "abfss://raw-data@databricktesdat.dfs.core.windows.net/checkpoints/bronze"
        )

        .option(
            "mergeSchema",
            "true"
        )

        .outputMode("append")

        .table("orders_schema.bronze_orders")
)
```

---

# 4. Silver Layer — Cleaned and Enriched Data

## Description
Reads streaming data from the Bronze table and applies filtering, validation, and enrichment logic.

Silver layer contains clean business-ready records.

## Command

```python
from pyspark.sql.functions import (
    col,
    current_timestamp
)

silver_df = (

    spark.readStream

         .table("orders_schema.bronze_orders")

         # ----------------------------------------------------
         # Data Quality Filters
         # ----------------------------------------------------

         .filter(
             col("amount") > 1000
         )

         .filter(
             col("customer").isNotNull()
         )

         .filter(
             col("order_status") == "SUCCESS"
         )

         # ----------------------------------------------------
         # Select Required Columns
         # ----------------------------------------------------

         .select(
             "order_id",
             "customer",
             "city",
             "product",
             "quantity",
             "payment_mode",
             "amount",
             "order_status"
         )

         # ----------------------------------------------------
         # Derived Column
         # ----------------------------------------------------

         .withColumn(
             "total_price",
             col("amount") * col("quantity")
         )

         # ----------------------------------------------------
         # Processing Timestamp
         # ----------------------------------------------------

         .withColumn(
             "processed_time",
             current_timestamp()
         )
)

(
    silver_df.writeStream

             .format("delta")

             .option(
                 "checkpointLocation",
                 "abfss://raw-data@databricktesdat.dfs.core.windows.net/checkpoints/silver"
             )

             .outputMode("append")

             .table("orders_schema.silver_orders")
)
```

---

# 5. Gold Layer — Aggregated Metrics

## Description
Reads streaming data from the Silver table and creates aggregated business metrics.

Gold layer is optimized for dashboards and analytics.

## Command

```python
from pyspark.sql.functions import (
    sum,
    count,
    avg,
    round
)

gold_df = (

    spark.readStream

         .table("orders_schema.silver_orders")

         .groupBy(
             "city",
             "product"
         )

         .agg(

             count("order_id")
                 .alias("total_orders"),

             sum("total_price")
                 .alias("total_revenue"),

             round(
                 avg("total_price"),
                 2
             ).alias("avg_order_value")
         )
)

(
    gold_df.writeStream

           .format("delta")

           .outputMode("complete")

           .option(
               "checkpointLocation",
               "abfss://raw-data@databricktesdat.dfs.core.windows.net/checkpoints/gold"
           )

           .table("orders_schema.gold_orders")
)
```

---

# 6. Validate Bronze Output

## Query

```sql
SELECT *
FROM orders_schema.bronze_orders
```

## Sample Output

| order_id | customer | city | product | amount | quantity | payment_mode | order_status |
|---|---|---|---|---|---|---|---|
| 1 | Ravi | Chennai | Laptop | 5686 | 2 | UPI | SUCCESS |
| 2 | Meena | Delhi | Mobile | 5483 | 1 | Card | SUCCESS |
| 3 | Vijay | Ahmedabad | TV | 8087 | 3 | Cash | PENDING |
| 4 | Divya | Hyderabad | Camera | 4400 | 1 | NetBanking | SUCCESS |

---

# 7. Validate Silver Output

## Query

```sql
SELECT *
FROM orders_schema.silver_orders
```

## Sample Output

| order_id | customer | city | product | amount | quantity | total_price |
|---|---|---|---|---|---|---|
| 1 | Ravi | Chennai | Laptop | 5686 | 2 | 11372 |
| 2 | Meena | Delhi | Mobile | 5483 | 1 | 5483 |
| 4 | Divya | Hyderabad | Camera | 4400 | 1 | 4400 |
| 6 | Rahul | Mumbai | Watch | 7540 | 3 | 22620 |

---

# 8. Validate Gold Output

## Query

```sql
SELECT *
FROM orders_schema.gold_orders
```

## Sample Output

| city | product | total_orders | total_revenue | avg_order_value |
|---|---|---|---|---|
| Chennai | Laptop | 4 | 45280 | 11320 |
| Mumbai | Watch | 7 | 67860 | 9694.29 |
| Pune | Tablet | 5 | 51200 | 10240 |
| Delhi | Mobile | 6 | 38900 | 6483.33 |

---

# Complete Pipeline Flow

```text
orders_bulk_250.json
        ↓

Bronze Layer
(Raw Streaming Data)
        ↓

Silver Layer
(Cleaned & Enriched Data)
        ↓

Gold Layer
(Aggregated Metrics)
```

---

# Important Concepts

| Concept | Description |
|---|---|
| Structured Streaming | Real-time stream processing |
| Auto Loader | Incremental file ingestion |
| Bronze Layer | Raw ingestion layer |
| Silver Layer | Cleaned & enriched layer |
| Gold Layer | Aggregated reporting layer |
| Delta Lake | Transactional storage layer |
| Checkpointing | Fault tolerance and recovery |
| Append Mode | Adds new records incrementally |
| Complete Mode | Rewrites full aggregation output |

---

# Pipeline Statistics

| Layer | Approx Rows | Description |
|---|---|---|
| Bronze | 250 | Raw streaming records |
| Silver | ~90 | Filtered SUCCESS records |
| Gold | ~40 | Aggregated business metrics |

---
