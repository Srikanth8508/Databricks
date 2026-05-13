# Databricks Delta Live Tables (DLT) — Complete Guide

This document explains:

- Basic DLT Pipeline Setup
- Bronze → Silver → Gold Architecture
- Auto Loader Integration
- Data Quality Rules
- CDC (Change Data Capture) Processing
- `apply_changes()` Implementation
- Incremental Streaming Flow
- Aggregation Layer

---

# Part 1 — Basic DLT Pipeline Setup

---

# 1. Create Storage Directory

## Description
Creates a directory in Azure Data Lake Storage for storing raw JSON order files.

This folder acts as the landing zone where incoming files are continuously processed by the DLT pipeline.

## Command

```python
dbutils.fs.mkdirs(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/dlt/orders/"
)
```

## Purpose
- Creates source storage location
- Stores raw ingestion files

---

# 2. Create Sample JSON File

## Description
Creates sample order records and writes them into ADLS as a JSON file.

This file becomes the input source for the Bronze layer.

## Command

```python
data = """
{"order_id":1,"customer":"Ravi","amount":1200}
{"order_id":2,"customer":"Arjun","amount":900}
{"order_id":3,"customer":"Meena","amount":2500}
{"order_id":4,"customer":"Kiran","amount":3000}
"""

dbutils.fs.put(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/dlt/orders/orders.json",
    data,
    True
)
```

## Purpose
- Generates sample streaming data
- Creates source records for DLT ingestion

---

# 3. Create DLT Pipeline

## Description
Creates a Delta Live Tables pipeline from the Databricks UI.

DLT automatically manages orchestration, dependency tracking, monitoring, retries, and execution.

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
- Automates streaming execution

---

# 4. Configure Pipeline Settings

## Description
Configure pipeline mode, default schema, and storage authentication settings.

These configurations allow the pipeline to securely access Azure Data Lake Storage.

## Steps

```text
Settings
   ↓
Change Pipeline Mode
(Triggered / Continuous)

   ↓
Select Default Schema

   ↓
Add Configuration Values
```

## Purpose
- Controls pipeline behavior
- Enables secure ADLS connectivity

---

# 5. Add Azure OAuth Configuration

## Description
Adds Azure OAuth Service Principal configurations required for ADLS access.

DLT uses these credentials to securely read and write storage files.

## Configuration

```properties
spark.hadoop.fs.azure.account.auth.type.databricktesdat.dfs.core.windows.net=OAuth

spark.hadoop.fs.azure.account.oauth.provider.type.databricktesdat.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider

spark.hadoop.fs.azure.account.oauth2.client.id.databricktesdat.dfs.core.windows.net=add client id

spark.hadoop.fs.azure.account.oauth2.client.secret.databricktesdat.dfs.core.windows.net=add client secret

spark.hadoop.fs.azure.account.oauth2.client.endpoint.databricktesdat.dfs.core.windows.net=https://login.microsoftonline.com/6d5176bef049/oauth2/token
```

## Purpose
- Authenticates storage access
- Enables DLT to interact with ADLS

---

# 6. Basic Bronze → Silver → Gold DLT Pipeline

## Description
Creates a multi-hop DLT architecture using Auto Loader and Delta Live Tables.

The pipeline processes raw data through Bronze, Silver, and Gold layers incrementally.

## Command

```python
import dlt
from pyspark.sql.functions import *

# ============================================================
# Bronze Layer
# ============================================================

@dlt.table(
    name="bronze_orders",
    comment="Raw ingestion from Azure landing zone"
)

def bronze_orders():

    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .load(
                 "abfss://raw-data@databricktesdat.dfs.core.windows.net/dlt/orders/"
             )
    )

# ============================================================
# Silver Layer
# ============================================================

@dlt.table(
    name="silver_orders",
    comment="Cleaned data with quality validation"
)

@dlt.expect_or_drop(
    "positive_amount",
    "amount > 1000"
)

def silver_orders():

    return dlt.read_stream("bronze_orders")

# ============================================================
# Gold Layer
# ============================================================

@dlt.table(
    name="gold_revenue",
    comment="Aggregate revenue by customer"
)

def gold_revenue():

    return (
        dlt.read("silver_orders")

           .groupBy("customer")

           .agg(
               count("order_id").alias("total_orders"),
               sum("amount").alias("total_revenue")
           )
    )
```

## Purpose
- Creates Bronze, Silver, and Gold layers
- Processes data incrementally using DLT

---

# 7. Perform Dry Run

## Description
Dry Run validates the pipeline before actual execution.

It checks dependencies, syntax, permissions, and table flow.

## Steps

```text
Click Dry Run
```

## Purpose
- Detects configuration errors
- Validates pipeline setup

---

# 8. Run the Pipeline

## Description
Starts the DLT pipeline and begins incremental processing.

DLT automatically creates and manages all dependent tables.

## Steps

```text
Click Start / Run Pipeline
```

## Purpose
- Executes the DLT workflow
- Starts streaming ingestion

---

# Basic DLT Architecture

```text
ADLS JSON Files
        ↓
Bronze Layer
(Raw Data)
        ↓
Silver Layer
(Cleaned Data)
        ↓
Gold Layer
(Aggregated Metrics)
```

---

# Part 2 — DLT CDC (Change Data Capture) Pipeline

---

# CDC Scenario

## Description
An e-commerce application continuously sends INSERT, UPDATE, and DELETE events into Databricks.

The objective is to maintain the latest state of every order using DLT CDC processing.

---

# 1. Sample CDC Feed Data

## Description
Raw CDC events arrive from systems such as Kafka, Debezium, SQL CDC, or Event Hubs.

Each event contains operation type and sequence timestamp information.

## Sample Data

```json
{ "order_id": 1001, "customer": "Alice", "amount": 500, "status": "PLACED", "operation": "INSERT", "seq_ts": "2024-01-01 10:00:00" }

{ "order_id": 1002, "customer": "Bob", "amount": 200, "status": "PLACED", "operation": "INSERT", "seq_ts": "2024-01-01 10:05:00" }

{ "order_id": 1003, "customer": "Carol", "amount": 750, "status": "PLACED", "operation": "INSERT", "seq_ts": "2024-01-01 10:10:00" }

{ "order_id": 1001, "customer": "Alice", "amount": 500, "status": "SHIPPED", "operation": "UPDATE", "seq_ts": "2024-01-01 11:00:00" }

{ "order_id": 1002, "customer": "Bob", "amount": 200, "status": "CANCELLED", "operation": "DELETE", "seq_ts": "2024-01-01 12:00:00" }

{ "order_id": 1003, "customer": "Carol", "amount": 900, "status": "DELIVERED", "operation": "UPDATE", "seq_ts": "2024-01-01 13:00:00" }
```

## Purpose
- Simulates real CDC events
- Demonstrates INSERT, UPDATE, DELETE flow

---

# 2. Full DLT CDC Pipeline

## Description
Processes CDC events using `dlt.apply_changes()` to maintain the latest state table.

The Silver layer automatically handles UPSERT and DELETE operations.

## Command

```python
import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import *

# ============================================================
# Configuration
# ============================================================

STORAGE_ACCOUNT = "databricktesdat"
CONTAINER = "raw-data"

BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

CDC_RAW_PATH = f"{BASE_PATH}/tmp/orders_cdc/"

SCHEMA_CHECKPOINT = (
    f"{BASE_PATH}/autoloader-checkpoints/schema/orders_cdc"
)

# ============================================================
# CDC Schema
# ============================================================

cdc_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer", StringType(), True),
    StructField("amount", LongType(), True),
    StructField("status", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("seq_ts", TimestampType(), True)
])

# ============================================================
# Bronze Layer
# ============================================================

@dlt.table(
    name="bronze_orders_cdc",
    comment="Raw CDC events"
)

def bronze_orders_cdc():

    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option(
                 "cloudFiles.schemaLocation",
                 SCHEMA_CHECKPOINT
             )
             .schema(cdc_schema)
             .load(CDC_RAW_PATH)
    )

# ============================================================
# Silver Layer — APPLY CHANGES
# ============================================================

dlt.create_streaming_table(
    name="silver_orders_cdc",
    comment="Latest clean order state"
)

dlt.apply_changes(
    target="silver_orders_cdc",
    source="bronze_orders_cdc",
    keys=["order_id"],
    sequence_by=col("seq_ts"),
    apply_as_deletes=col("operation") == "DELETE",
    except_column_list=["operation", "seq_ts"]
)

# ============================================================
# Gold Layer
# ============================================================

@dlt.table(
    name="gold_order_summary",
    comment="Revenue summary by status"
)

def gold_order_summary():

    return (
        dlt.read("silver_orders_cdc")

           .groupBy("status")

           .agg(
               sum("amount").alias("total_revenue"),
               count("order_id").alias("total_orders")
           )
    )
```

## Purpose
- Handles CDC UPSERT and DELETE operations
- Maintains latest table state automatically

---

# CDC Processing Flow

```text
CDC Events
(INSERT / UPDATE / DELETE)
        ↓
Bronze CDC Table
(Raw Change Events)
        ↓
apply_changes()
        ↓
Silver CDC Table
(Latest Clean State)
        ↓
Gold Aggregation Table
(Business Metrics)
```

---

# How `apply_changes()` Works

| Configuration | Description |
|---|---|
| `target` | Destination Silver table |
| `source` | Bronze CDC source stream |
| `keys` | Primary key column |
| `sequence_by` | Handles latest event ordering |
| `apply_as_deletes` | Deletes matching records |
| `except_column_list` | Removes CDC metadata columns |

---

# Example CDC Processing

## INSERT Event

```text
INSERT order_id = 1001
        ↓
Row inserted into Silver table
```

---

## UPDATE Event

```text
UPDATE order_id = 1001
        ↓
Existing row updated automatically
```

---

## DELETE Event

```text
DELETE order_id = 1002
        ↓
Matching row removed from Silver table
```

---

# Final Silver Table Example

| order_id | customer | amount | status |
|---|---|---|---|
| 1001 | Alice | 500 | SHIPPED |
| 1003 | Carol | 900 | DELIVERED |

---

# Final Gold Output Example

| status | total_revenue | total_orders |
|---|---|---|
| SHIPPED | 500 | 1 |
| DELIVERED | 900 | 1 |

---

# Key DLT Concepts

| Concept | Description |
|---|---|
| `@dlt.table` | Creates managed DLT tables |
| `dlt.read_stream()` | Reads streaming tables incrementally |
| `dlt.read()` | Reads batch/static tables |
| `@dlt.expect_or_drop()` | Applies data quality validation |
| `dlt.apply_changes()` | Handles CDC merges automatically |
| Bronze Layer | Raw ingestion layer |
| Silver Layer | Cleaned/latest state layer |
| Gold Layer | Aggregated analytics layer |
| Auto Loader | Incremental file ingestion |
| CDC | Tracks INSERT/UPDATE/DELETE changes |
| Sequence Column | Maintains latest event ordering |

---

# Summary

This guide demonstrated:

- DLT pipeline setup
- Multi-hop architecture
- Auto Loader ingestion
- Data quality enforcement
- CDC processing
- Incremental streaming
- `apply_changes()` implementation
- Aggregated Gold layer creation

---

Source reference from uploaded CDC workflow file: :contentReference[oaicite:0]{index=0}
