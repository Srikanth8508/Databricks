# Databricks Structured Streaming — Bronze → Silver → Gold Pipeline

> **Architecture:** Azure Data Lake Storage → Bronze → Silver → Gold
> **Pattern:** Single Notebook Pipeline using Structured Streaming (PySpark & SQL)

---

## Overview

This notebook demonstrates a complete end-to-end streaming pipeline using both **PySpark** and **SQL** approaches:

- Sample JSON Data Generation
- Auto Loader (incremental file ingestion)
- Bronze → Silver → Gold Architecture
- Delta Lake Tables
- Structured Streaming with checkpointing
- Delta Live Tables (DLT) via SQL

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

All three layers are combined into a single Python script using direct SQL-based table creation. Each layer runs as part of the same pipeline execution flow, and `awaitAnyTermination()` keeps the pipeline active throughout the streaming process.

### Full SQL Pipeline (Single Script)

```sql
-- ============================================================
-- STEP 1 — CREATE SCHEMA
-- ============================================================

CREATE SCHEMA IF NOT EXISTS orders_schema;

-- ============================================================
-- STEP 2 — BRONZE LAYER: Raw Streaming Ingestion
-- ============================================================

CREATE OR REFRESH STREAMING TABLE orders_schema.bronze_orders
COMMENT "Raw streaming orders ingested from ADLS via Auto Loader"
AS
SELECT *
FROM cloud_files(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/orders",
    "json",
    map(
        "cloudFiles.schemaLocation",
        "abfss://raw-data@databricktesdat.dfs.core.windows.net/checkpoints/schema/orders"
    )
);

-- ============================================================
-- STEP 3 — SILVER LAYER: Cleaned & Enriched Data
-- ============================================================

CREATE OR REFRESH STREAMING TABLE orders_schema.silver_orders
COMMENT "Filtered and enriched orders — SUCCESS only, amount > 1000"
AS
SELECT
    order_id,
    customer,
    city,
    product,
    quantity,
    payment_mode,
    amount,
    order_status,
    amount * quantity         AS total_price,
    current_timestamp()       AS processed_time
FROM STREAM(orders_schema.bronze_orders)
WHERE
    amount           > 1000
    AND customer     IS NOT NULL
    AND order_status = "SUCCESS";

-- ============================================================
-- STEP 4 — GOLD LAYER: Aggregated Business Metrics
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW orders_schema.gold_orders
COMMENT "City and product level aggregations for dashboards"
AS
SELECT
    city,
    product,
    COUNT(order_id)            AS total_orders,
    SUM(total_price)           AS total_revenue,
    ROUND(AVG(total_price), 2) AS avg_order_value
FROM orders_schema.silver_orders
GROUP BY
    city,
    product;
```

> **Bronze & Silver** use `CREATE OR REFRESH STREAMING TABLE` for incremental append.
> **Gold** uses `CREATE OR REFRESH MATERIALIZED VIEW` — matches PySpark's `outputMode("complete")` for full recompute on every trigger.

### Why Gold uses `MATERIALIZED VIEW` instead of `STREAMING TABLE`

| | `STREAMING TABLE` | `MATERIALIZED VIEW` |
|---|---|---|
| Used for | Append-only ingestion | Aggregations & transformations |
| Re-processes | Only new records | Full recompute when upstream changes |
| Output mode | Append | Complete |
| Best for | Bronze, Silver | Gold (GROUP BY, SUM, COUNT) |

### How to Run the SQL Pipeline in Databricks

**Option A — Delta Live Tables (DLT)** *(Recommended)*
- Go to **Workflows → Delta Live Tables → Create Pipeline**
- Paste the SQL script above as the notebook source
- DLT manages all stream orchestration, checkpointing, and retries automatically

**Option B — SQL Notebook with `STREAM()`**
- Create a new SQL notebook in Databricks
- Run each `CREATE OR REFRESH STREAMING TABLE` cell individually
- Checkpoints are managed per-table automatically

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

| Concept                 | Description                                            |
|-------------------------|--------------------------------------------------------|
| Structured Streaming    | Real-time stream processing engine in Spark            |
| Auto Loader             | Incremental file ingestion using `cloudFiles`          |
| Bronze Layer            | Raw ingestion — stores data as-is                      |
| Silver Layer            | Cleaned & enriched — filtered business records         |
| Gold Layer              | Aggregated reporting layer for dashboards              |
| Delta Lake              | Transactional storage with ACID guarantees             |
| Checkpointing           | Ensures fault tolerance and exactly-once delivery      |
| Append Mode             | Adds new records without modifying existing ones       |
| Complete Mode           | Rewrites the full aggregated output each trigger       |
| Delta Live Tables (DLT) | SQL-native declarative pipeline framework in Databricks|
| STREAMING TABLE         | SQL equivalent of PySpark `writeStream` append mode    |
| MATERIALIZED VIEW       | SQL equivalent of PySpark `outputMode("complete")`     |

---

## Pipeline Row Statistics

| Layer  | Approx. Rows | Notes                        |
|--------|--------------|------------------------------|
| Bronze | 250          | All raw incoming records     |
| Silver | ~90          | SUCCESS records, amount >1000|
| Gold   | ~40          | Aggregated city-product rows |

---

