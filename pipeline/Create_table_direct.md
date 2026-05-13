# Databricks Structured Streaming Pipeline — Direct Table Creation Method

> Architecture: Azure Data Lake Storage → Bronze → Silver → Gold  
> Pattern: Structured Streaming with Direct Delta Table Creation

---

# Overview

This implementation demonstrates:

- Structured Streaming
- Auto Loader
- Direct Table Creation
- Bronze → Silver → Gold Architecture
- Delta Lake
- Incremental Streaming Processing
- Aggregation Pipelines

---

# Architecture Flow

```text
ADLS JSON Files
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

# 1. Generate Sample Streaming Data

## Description
Creates 250 random order records and stores them as JSON files in Azure Data Lake Storage.

These files act as incoming streaming data for the pipeline.

## Command

```python
%python

import json
import random

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

# Convert to multiline JSON string
data = "\n".join(records)

# Write JSON file into ADLS
dbutils.fs.put(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/orders/orders_bulk_250.json",
    data,
    True
)

print("250 streaming JSON records created successfully")
```

## Purpose
- Simulates streaming order transactions
- Creates source files for streaming ingestion

---

# 2. Create Bronze Layer

## Description
Reads JSON files continuously using Auto Loader and stores raw records into the Bronze Delta table.

Bronze layer contains unprocessed raw streaming data.

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

        .table("ext_schema.bronze_orders")
)
```

## Purpose
- Ingests raw streaming files
- Stores data into Bronze Delta table

---

# 3. Create Silver Layer

## Description
Reads streaming data from the Bronze table and applies filtering, cleansing, and enrichment logic.

Silver layer contains validated and business-ready records.

## Command

```python
from pyspark.sql.functions import (
    col,
    current_timestamp
)

silver_df = (

    spark.readStream

         .table("ext_schema.bronze_orders")

         # Remove invalid records
         .filter(col("amount") > 1000)

         .filter(col("customer").isNotNull())

         .filter(
             col("order_status") == "SUCCESS"
         )

         # Select required columns
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

         # Add calculated column
         .withColumn(
             "total_price",
             col("amount") * col("quantity")
         )

         # Add processing timestamp
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

             .table("ext_schema.silver_orders")
)
```

## Purpose
- Cleans invalid records
- Applies business transformations
- Creates enriched streaming dataset

---

# 4. Create Gold Layer

## Description
Reads streaming data from the Silver table and performs aggregations.

Gold layer stores summarized business metrics for analytics and reporting.

## Command

```python
from pyspark.sql.functions import (
    sum,
    count
)

gold_df = (

    spark.readStream

         .table("ext_schema.silver_orders")

         .groupBy(
             "city",
             "product"
         )

         .agg(

             sum("total_price")
                 .alias("total_revenue"),

             count("order_id")
                 .alias("total_orders")
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

           .table("ext_schema.gold_sales")
)
```

## Purpose
- Aggregates business metrics
- Generates reporting-ready datasets

---

# 5. Validate Bronze Output

## Query

```sql
SELECT *
FROM ext_schema.bronze_orders
```

## Sample Output

| order_id | customer | city | product | amount | quantity | payment_mode | order_status |
|---|---|---|---|---|---|---|---|
| 1 | Ravi | Chennai | Laptop | 5686 | 2 | UPI | SUCCESS |
| 2 | Meena | Delhi | Mobile | 5483 | 1 | Card | SUCCESS |
| 3 | Vijay | Ahmedabad | TV | 8087 | 3 | Cash | PENDING |
| 4 | Divya | Hyderabad | Camera | 4400 | 1 | NetBanking | SUCCESS |
| 5 | Manoj | Pune | Keyboard | 6290 | 2 | UPI | FAILED |

## Characteristics

- Stores raw streaming records
- No transformations applied
- Source ingestion layer

---

# 6. Validate Silver Output

## Query

```sql
SELECT *
FROM ext_schema.silver_orders
```

## Sample Output

| order_id | customer | city | product | amount | quantity | total_price | order_status |
|---|---|---|---|---|---|---|---|
| 1 | Ravi | Chennai | Laptop | 5686 | 2 | 11372 | SUCCESS |
| 2 | Meena | Delhi | Mobile | 5483 | 1 | 5483 | SUCCESS |
| 4 | Divya | Hyderabad | Camera | 4400 | 1 | 4400 | SUCCESS |
| 6 | Rahul | Mumbai | Watch | 7540 | 3 | 22620 | SUCCESS |
| 7 | Priya | Pune | Tablet | 4173 | 2 | 8346 | SUCCESS |

## Characteristics

- Invalid records removed
- Business transformations applied
- Includes calculated columns

---

# 7. Validate Gold Output

## Query

```sql
SELECT *
FROM ext_schema.gold_sales
```

## Sample Output

| city | product | total_revenue | total_orders |
|---|---|---|---|
| Chennai | Laptop | 45280 | 4 |
| Mumbai | Watch | 67860 | 7 |
| Pune | Tablet | 51200 | 5 |
| Delhi | Mobile | 38900 | 6 |
| Hyderabad | Camera | 27450 | 3 |

## Characteristics

- Aggregated business metrics
- Optimized for reporting
- Supports dashboards and analytics

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
(Aggregated Business Metrics)
```

---

# Important Structured Streaming Concepts

| Concept | Description |
|---|---|
| Auto Loader | Incremental file ingestion |
| Structured Streaming | Real-time data processing |
| Bronze Layer | Raw ingestion layer |
| Silver Layer | Cleaned and validated layer |
| Gold Layer | Aggregated analytics layer |
| Delta Lake | Reliable transactional storage |
| Checkpointing | Fault tolerance and recovery |
| Output Mode Append | Appends new records only |
| Output Mode Complete | Rewrites full aggregation output |

---

# Pipeline Statistics

| Layer | Approx Rows | Description |
|---|---|---|
| Bronze | 250 | Raw streaming records |
| Silver | ~90 | Filtered SUCCESS records |
| Gold | ~40 | Aggregated business metrics |

---



---
