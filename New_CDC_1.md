# Databricks Delta Live Tables (DLT) with CDC — Complete Setup Guide

> **Architecture:** Azure Data Lake Storage → Bronze (CDC) → Silver (SCD Type 1) → Gold  
> **Pattern:** Multi-hop Medallion Architecture with Change Data Capture (CDC)

---

# Overview

This guide demonstrates:

- Delta Live Tables (DLT)
- Multi-Hop Architecture
- Auto Loader
- CDC Processing
- `dlt.apply_changes()`
- SCD Type 1 Processing
- Bronze → Silver → Gold Flow
- Incremental Streaming Pipelines

---

# CDC Architecture Flow

```text
ADLS JSON Files
        ↓
Bronze Layer
(Raw CDC Events)
        ↓
Silver Layer
(SCD Type 1 Latest State)
        ↓
Gold Layer
(Business Aggregations)
```

---

# 1. Create Storage Directory

## Description
Creates a storage directory in Azure Data Lake Storage for storing CDC JSON files.

This directory acts as the landing zone for incoming INSERT, UPDATE, and DELETE events.

## Command

```python
dbutils.fs.mkdirs(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/dlt/orders/"
)
```

## Purpose
- Creates source CDC storage location
- Stores incoming CDC files

---

# 2. Generate Initial CDC INSERT Records

## Description
Generates 100 initial INSERT records with CDC metadata columns.

Each event contains operation type and sequence number required for CDC processing.

## Enhanced CDC Schema

| Field | Type | Description |
|---|---|---|
| `order_id` | Integer | Unique order identifier |
| `customer` | String | Customer name |
| `city` | String | Customer city |
| `amount` | Integer | Order amount |
| `order_time` | Timestamp | Order timestamp |
| `_operation` | String | CDC operation type |
| `_sequence_num` | Long | Event ordering sequence |

---

## Command

```python
import json
import random
from datetime import datetime, timedelta

customers = [
    "Ravi", "Arjun", "Kiran", "Meena", "John",
    "Priya", "Vijay", "Anu", "Rahul", "Sneha",
    "Aakash", "Divya", "Manoj", "Keerthi", "Suresh"
]

cities = [
    "Chennai", "Bangalore", "Hyderabad",
    "Mumbai", "Delhi", "Pune",
    "Kolkata", "Ahmedabad"
]

records = []

base_time = datetime(2025, 5, 12, 10, 0, 0)

sequence_num = 1

# ------------------------------------------------------------
# Generate Initial INSERT Events
# ------------------------------------------------------------

for i in range(1, 101):

    record = {
        "order_id": i,
        "customer": random.choice(customers),
        "city": random.choice(cities),
        "amount": random.randint(500, 10000),
        "order_time": (
            base_time + timedelta(minutes=i)
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "_operation": "INSERT",
        "_sequence_num": sequence_num
    }

    records.append(json.dumps(record))

    sequence_num += 1

data = "\n".join(records)

dbutils.fs.put(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/dlt/orders/orders1.json",
    data,
    True
)

print(f"Created orders1.json with {len(records)} INSERT operations")
```

## Purpose
- Simulates initial CDC INSERT events
- Creates source CDC streaming data

---

# 3. Generate UPDATE and DELETE CDC Events

## Description
Creates UPDATE and DELETE events for existing orders.

This simulates real-world CDC changes arriving after the initial load.

## Command

```python
import json
import random
from datetime import datetime

cdc_events = []

sequence_num = 101

# ------------------------------------------------------------
# Generate UPDATE Events
# ------------------------------------------------------------

for _ in range(10):

    order_id = random.randint(1, 100)

    cdc_events.append({
        "order_id": order_id,
        "customer": random.choice([
            "Ravi", "Priya", "Arjun", "Meena",
            "John", "Vijay", "Anu"
        ]),
        "city": random.choice([
            "Chennai", "Mumbai", "Delhi"
        ]),
        "amount": random.randint(1500, 12000),
        "order_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "_operation": "UPDATE",
        "_sequence_num": sequence_num
    })

    sequence_num += 1

# ------------------------------------------------------------
# Generate DELETE Events
# ------------------------------------------------------------

for _ in range(5):

    order_id = random.randint(1, 100)

    cdc_events.append({
        "order_id": order_id,
        "customer": None,
        "city": None,
        "amount": None,
        "order_time": None,
        "_operation": "DELETE",
        "_sequence_num": sequence_num
    })

    sequence_num += 1

data = "\n".join([
    json.dumps(event)
    for event in cdc_events
])

dbutils.fs.put(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/dlt/orders/orders2.json",
    data,
    True
)

print(
    f"Created orders2.json with {len(cdc_events)} CDC events"
)
```

## Purpose
- Simulates CDC UPDATE operations
- Simulates CDC DELETE operations

---

# 4. Create DLT Pipeline

## Description
Creates a Delta Live Tables pipeline from the Databricks UI.

DLT automatically manages dependencies, orchestration, retries, monitoring, and execution.

## Steps

```text
Jobs & Pipelines
        ↓
Create
        ↓
ETL Pipeline
```

## Purpose
- Creates managed DLT workflow
- Automates CDC processing pipeline

---

# 5. Configure Pipeline Settings

## Description
Configure pipeline mode, default schema, and Azure storage authentication.

These configurations allow DLT to securely access Azure Data Lake Storage.

## Steps

```text
Settings
   ↓
Pipeline Mode
(Triggered / Continuous)

   ↓
Select Default Schema

   ↓
Add Configuration Values
```

## Purpose
- Configures pipeline execution behavior
- Enables secure storage connectivity

---

# 6. Add Azure OAuth Configuration

## Description
Adds Azure OAuth Service Principal configurations for ADLS access.

These settings authenticate DLT pipelines securely.

## Configuration

```properties
spark.hadoop.fs.azure.account.auth.type.databricktesdat.dfs.core.windows.net=OAuth

spark.hadoop.fs.azure.account.oauth.provider.type.databricktesdat.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider

spark.hadoop.fs.azure.account.oauth2.client.id.databricktesdat.dfs.core.windows.net=add client id

spark.hadoop.fs.azure.account.oauth2.client.secret.databricktesdat.dfs.core.windows.net=add client secret

spark.hadoop.fs.azure.account.oauth2.client.endpoint.databricktesdat.dfs.core.windows.net=https://login.microsoftonline.com/6d5176bef049/oauth2/token
```

## Purpose
- Authenticates Azure storage access
- Allows DLT to read/write ADLS files

---

# 7. Complete DLT CDC Pipeline Code

## Description
Processes CDC events using `dlt.apply_changes()`.

The Silver table automatically handles UPSERT and DELETE operations using SCD Type 1 logic.

## Command

```python
import dlt

from pyspark.sql.functions import (
    col,
    sum,
    count,
    avg,
    round,
    to_date,
    expr
)

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

STORAGE_ACCOUNT = "databricktesdat"

CONTAINER = "raw-data"

ORDERS_PATH = (
    f"abfss://{CONTAINER}@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net"
    "/dlt/orders/"
)

# ------------------------------------------------------------
# Bronze Layer
# ------------------------------------------------------------

@dlt.table(
    name="bronze_orders",
    comment="Raw CDC event stream"
)

def bronze_orders():

    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option(
                 "cloudFiles.schemaLocation",
                 f"{ORDERS_PATH}_schema"
             )
             .load(ORDERS_PATH)
    )

# ------------------------------------------------------------
# Silver Layer — CDC Merge
# ------------------------------------------------------------

dlt.create_streaming_table(
    name="silver_orders",
    comment="Current state of orders"
)

dlt.apply_changes(
    target="silver_orders",
    source="bronze_orders",
    keys=["order_id"],
    sequence_by=col("_sequence_num"),
    apply_as_deletes=expr("_operation = 'DELETE'"),
    except_column_list=[
        "_operation",
        "_sequence_num"
    ],
    stored_as_scd_type=1
)

# ------------------------------------------------------------
# Silver Enriched Layer
# ------------------------------------------------------------

@dlt.table(
    name="silver_orders_enriched",
    comment="Validated and enriched orders"
)

def silver_orders_enriched():

    return (
        dlt.read("silver_orders")

           .filter(col("amount") > 1000)

           .withColumn(
               "order_date",
               to_date(col("order_time"))
           )
    )

# ------------------------------------------------------------
# Gold Layer
# ------------------------------------------------------------

@dlt.table(
    name="gold_revenue",
    comment="Revenue summary by customer and city"
)

def gold_revenue():

    return (
        dlt.read("silver_orders_enriched")

           .groupBy("customer", "city")

           .agg(
               count("order_id")
                   .alias("total_orders"),

               sum("amount")
                   .alias("total_revenue"),

               round(avg("amount"), 2)
                   .alias("avg_order_value")
           )

           .orderBy(
               "total_revenue",
               ascending=False
           )
    )
```

## Purpose
- Handles CDC UPSERT operations
- Handles CDC DELETE operations
- Maintains latest SCD Type 1 table state

---

# 8. Perform Dry Run

## Description
Validates the DLT CDC pipeline before execution.

Checks dependencies, permissions, syntax, and table flow.

## Steps

```text
Click Dry Run
```

## Purpose
- Detects configuration issues
- Validates CDC pipeline setup

---

# 9. Run the Pipeline

## Description
Starts the DLT CDC pipeline.

The pipeline automatically processes CDC events incrementally.

## Steps

```text
Click Start / Run Pipeline
```

## Purpose
- Executes CDC workflow
- Starts incremental CDC processing

---

# CDC Processing Flow

```text
orders1.json
(100 INSERT Events)
        ↓

orders2.json
(10 UPDATE + 5 DELETE Events)
        ↓

Bronze Layer
(Raw CDC Event Stream)
        ↓

dlt.apply_changes()
        ↓

Silver Layer
(SCD Type 1 Current State)
        ↓

Silver Enriched Layer
(Validated Business Data)
        ↓

Gold Layer
(Business Aggregations)
```

---

# Expected Bronze Output

| order_id | customer | amount | _operation | _sequence_num |
|---|---|---|---|---|
| 1 | Ravi | 3200 | INSERT | 1 |
| 5 | Vijay | 8200 | UPDATE | 101 |
| 12 | NULL | NULL | DELETE | 111 |

---

# Expected Silver Output

| order_id | customer | city | amount |
|---|---|---|---|
| 1 | Ravi | Chennai | 3200 |
| 5 | Vijay | Delhi | 8200 |

Characteristics:

- Latest state maintained
- DELETE records removed
- CDC metadata columns excluded
- Old values overwritten using SCD Type 1

---

# Expected Gold Output

| customer | city | total_orders | total_revenue | avg_order_value |
|---|---|---|---|---|
| Vijay | Delhi | 5 | 32400 | 6480.00 |
| Priya | Mumbai | 3 | 21350 | 7116.67 |

---

# Important CDC Concepts

| Concept | Description |
|---|---|
| `dlt.apply_changes()` | Handles CDC merges automatically |
| `keys` | Primary key used for merge |
| `sequence_by` | Maintains latest event ordering |
| `apply_as_deletes` | Deletes matching records |
| `stored_as_scd_type=1` | Maintains latest row only |
| Bronze Layer | Raw CDC event storage |
| Silver Layer | Latest clean state |
| Gold Layer | Aggregated analytics layer |
| SCD Type 1 | Overwrites old values |
| CDC | Tracks INSERT/UPDATE/DELETE operations |

---

# CDC Architecture Summary

```text
ADLS
 ├── orders1.json (INSERT events)
 └── orders2.json (UPDATE + DELETE events)
            ↓

Bronze Layer
(Raw CDC Events)
            ↓

apply_changes()
(keys + sequence handling)
            ↓

Silver Layer
(Current State Table)
            ↓

Business Transformations
            ↓

Gold Layer
(Analytics & Reporting)
```

---

# Testing CDC Flow

## Step 1 — Initial Load

```python
# Run pipeline after creating orders1.json

# Bronze:
100 INSERT events

# Silver:
Current active records

# Gold:
Revenue aggregations
```

---

## Step 2 — Process CDC Changes

```python
# Add orders2.json

# Bronze:
INSERT + UPDATE + DELETE events

# Silver:
Updated latest state

# Gold:
Updated business metrics
```

---

## Step 3 — Validate CDC Merge

```sql
-- Check updated order

SELECT *
FROM silver_orders
WHERE order_id = 5;

-- Verify deleted order

SELECT *
FROM silver_orders
WHERE order_id = 12;

-- Validate Gold aggregation

SELECT *
FROM gold_revenue;
```

---

# Summary

This implementation demonstrates:

- Delta Live Tables
- CDC pipelines
- Bronze → Silver → Gold architecture
- Auto Loader ingestion
- `dlt.apply_changes()`
- SCD Type 1 processing
- Incremental streaming
- Business aggregations

---

Reference source from uploaded markdown file:
:contentReference[oaicite:0]{index=0}
