# Databricks Structured Streaming — Bronze → Silver → Gold Pipeline

> **Architecture:** Azure Data Lake Storage → Bronze → Silver → Gold
> **Pattern:** Created a streaming pipeline using PySpark with the direct table creation method.

---

## Overview

This notebook demonstrates a complete end-to-end streaming pipeline:

- Sample JSON Data Generation
- Auto Loader (incremental file ingestion)
- Bronze → Silver → Gold Architecture
- Delta Lake Tables
- Structured Streaming with checkpointing

---

## Architecture Flow

```
Sample JSON Data
      ↓
Bronze Layer  →  Raw Streaming Ingestion
      ↓
Silver Layer  →  Cleaned & Enriched Data
      ↓
Gold Layer    →  Aggregated Business Metrics
```

---

## Step 1 — Create Storage Directory

Creates the ADLS directory that will serve as the streaming source location.

```python
dbutils.fs.mkdirs(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/orders"
)
```

---

## Step 2 — Generate Sample Streaming Data

Generates 250 random order records and writes them as a JSON file to ADLS.

```python
import json
import random

# ── Master Data ────────────────────────────────────────────────────────────────

customers     = ["Ravi", "Arjun", "Kiran", "Meena", "John",
                 "Priya", "Vijay", "Anu", "Rahul", "Sneha",
                 "Aakash", "Divya", "Manoj", "Keerthi", "Surya"]

cities        = ["Chennai", "Bangalore", "Hyderabad",
                 "Mumbai", "Delhi", "Pune",
                 "Kolkata", "Coimbatore"]

products      = ["Laptop", "Mobile", "Tablet", "Watch",
                 "Headphones", "Camera", "TV", "Keyboard"]

payment_modes = ["UPI", "Card", "Cash", "NetBanking"]

status_list   = ["SUCCESS", "PENDING", "FAILED"]

# ── Generate 250 Records ───────────────────────────────────────────────────────

records = []

for i in range(1, 251):
    order = {
        "order_id":     i,
        "customer":     random.choice(customers),
        "city":         random.choice(cities),
        "product":      random.choice(products),
        "amount":       random.randint(500, 15000),
        "quantity":     random.randint(1, 5),
        "payment_mode": random.choice(payment_modes),
        "order_status": random.choice(status_list)
    }
    records.append(json.dumps(order))

# ── Write to ADLS ──────────────────────────────────────────────────────────────

data = "\n".join(records)

dbutils.fs.put(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/orders/orders_bulk_250.json",
    data,
    True
)

print("250 streaming JSON records created successfully")
```

---

## Step 3 — Configure Pipeline (Bronze + Silver + Gold — Single Script)

All three layers are combined into one Python script. Each `writeStream` starts in the background; `awaitAnyTermination()` keeps all streams running together.

```python
from pyspark.sql.functions import col, current_timestamp, sum, count, avg, round

# ── BRONZE — Raw Streaming Ingestion ──────────────────────────────────────────

bronze_df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option(
             "cloudFiles.schemaLocation",
             "abfss://raw-data@databricktesdat.dfs.core.windows.net/checkpoints/schema/orders"
         )
         .load("abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/orders")
)

bronze_query = (
    bronze_df.writeStream
        .format("delta")
        .option("checkpointLocation", "abfss://raw-data@databricktesdat.dfs.core.windows.net/checkpoints/bronze")
        .option("mergeSchema", "true")
        .outputMode("append")
        .table("orders_schema.bronze_orders")
)

# ── SILVER — Cleaned & Enriched Data ──────────────────────────────────────────

silver_df = (
    spark.readStream
         .table("orders_schema.bronze_orders")

         # ── Data Quality Filters ───────────────────────────────────────────
         .filter(col("amount") > 1000)
         .filter(col("customer").isNotNull())
         .filter(col("order_status") == "SUCCESS")

         # ── Select Required Columns ────────────────────────────────────────
         .select(
             "order_id", "customer", "city", "product",
             "quantity", "payment_mode", "amount", "order_status"
         )

         # ── Derived Columns ────────────────────────────────────────────────
         .withColumn("total_price",    col("amount") * col("quantity"))
         .withColumn("processed_time", current_timestamp())
)

silver_query = (
    silver_df.writeStream
             .format("delta")
             .option("checkpointLocation", "abfss://raw-data@databricktesdat.dfs.core.windows.net/checkpoints/silver")
             .outputMode("append")
             .table("orders_schema.silver_orders")
)

# ── GOLD — Aggregated Business Metrics ────────────────────────────────────────

gold_df = (
    spark.readStream
         .table("orders_schema.silver_orders")
         .groupBy("city", "product")
         .agg(
             count("order_id")            .alias("total_orders"),
             sum("total_price")           .alias("total_revenue"),
             round(avg("total_price"), 2) .alias("avg_order_value")
         )
)

gold_query = (
    gold_df.writeStream
           .format("delta")
           .outputMode("complete")
           .option("checkpointLocation", "abfss://raw-data@databricktesdat.dfs.core.windows.net/checkpoints/gold")
           .table("orders_schema.gold_orders")
)

# ── WAIT FOR ALL STREAMS ───────────────────────────────────────────────────────

spark.streams.awaitAnyTermination()
```

> **Bronze** → raw ingestion | **Silver** → filtered & enriched | **Gold** → aggregated metrics
> All three streams run concurrently. `awaitAnyTermination()` keeps the notebook alive until any stream stops or fails.

---

## Step 4 — Validate Outputs

### Bronze

```sql
SELECT * FROM orders_schema.bronze_orders;
```

| order_id | customer | city      | product | amount | quantity | payment_mode | order_status |
|----------|----------|-----------|---------|--------|----------|--------------|--------------|
| 1        | Ravi     | Chennai   | Laptop  | 5686   | 2        | UPI          | SUCCESS      |
| 2        | Meena    | Delhi     | Mobile  | 5483   | 1        | Card         | SUCCESS      |
| 3        | Vijay    | Ahmedabad | TV      | 8087   | 3        | Cash         | PENDING      |
| 4        | Divya    | Hyderabad | Camera  | 4400   | 1        | NetBanking   | SUCCESS      |

### Silver

```sql
SELECT * FROM orders_schema.silver_orders;
```

| order_id | customer | city      | product | amount | quantity | total_price |
|----------|----------|-----------|---------|--------|----------|-------------|
| 1        | Ravi     | Chennai   | Laptop  | 5686   | 2        | 11372       |
| 2        | Meena    | Delhi     | Mobile  | 5483   | 1        | 5483        |
| 4        | Divya    | Hyderabad | Camera  | 4400   | 1        | 4400        |
| 6        | Rahul    | Mumbai    | Watch   | 7540   | 3        | 22620       |

### Gold

```sql
SELECT * FROM orders_schema.gold_orders;
```

| city    | product | total_orders | total_revenue | avg_order_value |
|---------|---------|--------------|---------------|-----------------|
| Chennai | Laptop  | 4            | 45280         | 11320.00        |
| Mumbai  | Watch   | 7            | 67860         | 9694.29         |
| Pune    | Tablet  | 5            | 51200         | 10240.00        |
| Delhi   | Mobile  | 6            | 38900         | 6483.33         |

---

## Key Concepts

| Concept              | Description                                      |
|----------------------|--------------------------------------------------|
| Structured Streaming | Real-time stream processing engine in Spark      |
| Auto Loader          | Incremental file ingestion using `cloudFiles`    |
| Bronze Layer         | Raw ingestion — stores data as-is                |
| Silver Layer         | Cleaned & enriched — filtered business records   |
| Gold Layer           | Aggregated reporting layer for dashboards        |
| Delta Lake           | Transactional storage with ACID guarantees       |
| Checkpointing        | Ensures fault tolerance and exactly-once delivery|
| Append Mode          | Adds new records without modifying existing ones |
| Complete Mode        | Rewrites the full aggregated output each trigger |

---

## Pipeline Row Statistics

| Layer  | Approx. Rows | Notes                        |
|--------|--------------|------------------------------|
| Bronze | 250          | All raw incoming records     |
| Silver | ~90          | SUCCESS records, amount >1000|
| Gold   | ~40          | Aggregated city-product rows |

---

