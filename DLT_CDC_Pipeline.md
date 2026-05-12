## DLT CDC Pipeline — Complete Sample Workflow

---

### Scenario
An e-commerce platform streams order changes (inserts, updates, deletes) from a source database into Databricks via CDC. We need to maintain the **latest state** of every order in a clean Silver table.

---

### Step 1 — Sample Raw CDC Feed Data

This is what arrives in your bronze/raw layer from the CDC source (Kafka, Debezium, etc.):

```json
// orders_cdc_feed — raw events arriving over time
{ "order_id": 1001, "customer": "Alice", "amount": 500,  "status": "PLACED",    "operation": "INSERT", "seq_ts": "2024-01-01 10:00:00" }
{ "order_id": 1002, "customer": "Bob",   "amount": 200,  "status": "PLACED",    "operation": "INSERT", "seq_ts": "2024-01-01 10:05:00" }
{ "order_id": 1003, "customer": "Carol", "amount": 750,  "status": "PLACED",    "operation": "INSERT", "seq_ts": "2024-01-01 10:10:00" }
{ "order_id": 1001, "customer": "Alice", "amount": 500,  "status": "SHIPPED",   "operation": "UPDATE", "seq_ts": "2024-01-01 11:00:00" }
{ "order_id": 1002, "customer": "Bob",   "amount": 200,  "status": "CANCELLED", "operation": "DELETE", "seq_ts": "2024-01-01 12:00:00" }
{ "order_id": 1003, "customer": "Carol", "amount": 900,  "status": "DELIVERED", "operation": "UPDATE", "seq_ts": "2024-01-01 13:00:00" }
```

---

### Step 2 — Full DLT Pipeline Code

```python
import dlt
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType
)

# ============================================================
# CONFIGURATION
# ============================================================

STORAGE_ACCOUNT   = "databricktesdat"
CONTAINER         = "raw-data"
BASE_PATH         = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

CDC_RAW_PATH      = f"{BASE_PATH}/tmp/orders_cdc/"
SCHEMA_CHECKPOINT = f"{BASE_PATH}/autoloader-checkpoints/schema/orders_cdc"


# ============================================================
# SCHEMA — CDC feed columns including operation + sequence
# ============================================================

cdc_schema = StructType([
    StructField("order_id",  LongType(),      True),   # primary key
    StructField("customer",  StringType(),    True),
    StructField("amount",    LongType(),      True),
    StructField("status",    StringType(),    True),
    StructField("operation", StringType(),    True),   # INSERT / UPDATE / DELETE
    StructField("seq_ts",    TimestampType(), True),   # sequence — ordering field
])


# ============================================================
# BRONZE LAYER — raw CDC events, nothing filtered or changed
# Captures every INSERT / UPDATE / DELETE as-is
# ============================================================

@dlt.table(
    name="bronze_orders_cdc",
    comment="Raw CDC events from source — all operations captured as-is"
)
def bronze_orders_cdc():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",              "json")
            .option("cloudFiles.schemaLocation",      SCHEMA_CHECKPOINT)
            .option("cloudFiles.schemaEvolutionMode", "none")
            .option("cloudFiles.includeExistingFiles","true")
            .schema(cdc_schema)
            .load(CDC_RAW_PATH)
    )


# ============================================================
# SILVER LAYER — APPLY CHANGES INTO (CDC merge target)
#
# DLT reads the bronze stream and:
#   - UPSERTS rows where operation = INSERT or UPDATE
#   - DELETES rows where operation = DELETE
#   - Uses seq_ts to handle out-of-order events correctly
#   - Excludes CDC metadata columns (operation, seq_ts)
#     from the final clean target table
# ============================================================

dlt.create_streaming_table(
    name="silver_orders",
    comment="Latest state of all orders — CDC applied, clean and deduplicated"
)

dlt.apply_changes(
    target          = "silver_orders",                  # destination table
    source          = "bronze_orders_cdc",              # bronze CDC stream
    keys            = ["order_id"],                     # primary key
    sequence_by     = col("seq_ts"),                    # order events by timestamp
    apply_as_deletes= col("operation") == "DELETE",     # delete condition
    except_column_list = ["operation", "seq_ts"]        # exclude CDC metadata from target
)


# ============================================================
# GOLD LAYER — business aggregation on top of clean silver
# ============================================================

@dlt.table(
    name="gold_order_summary",
    comment="Live order summary — total revenue and count by status"
)
def gold_order_summary():
    return (
        dlt.read("silver_orders")           # batch read — silver is not a stream after CDC
            .groupBy("status")
            .agg(
                {"amount": "sum",           # total revenue per status
                 "order_id": "count"}       # order count per status
            )
            .withColumnRenamed("sum(amount)",    "total_revenue")
            .withColumnRenamed("count(order_id)","total_orders")
    )
```

---

### Step 3 — Event-by-Event Walkthrough

#### Batch 1 — Three INSERTs arrive
```
bronze_orders_cdc receives:
┌──────────┬──────────┬────────┬──────────┬───────────┬─────────────────────┐
│ order_id │ customer │ amount │ status   │ operation │ seq_ts              │
├──────────┼──────────┼────────┼──────────┼───────────┼─────────────────────┤
│ 1001     │ Alice    │ 500    │ PLACED   │ INSERT    │ 2024-01-01 10:00:00 │
│ 1002     │ Bob      │ 200    │ PLACED   │ INSERT    │ 2024-01-01 10:05:00 │
│ 1003     │ Carol    │ 750    │ PLACED   │ INSERT    │ 2024-01-01 10:10:00 │
└──────────┴──────────┴────────┴──────────┴───────────┴─────────────────────┘

silver_orders after APPLY CHANGES:
┌──────────┬──────────┬────────┬─────────┐
│ order_id │ customer │ amount │ status  │
├──────────┼──────────┼────────┼─────────┤
│ 1001     │ Alice    │ 500    │ PLACED  │  ← inserted
│ 1002     │ Bob      │ 200    │ PLACED  │  ← inserted
│ 1003     │ Carol    │ 750    │ PLACED  │  ← inserted
└──────────┴──────────┴────────┴─────────┘
```

#### Batch 2 — UPDATE and DELETE arrive
```
bronze_orders_cdc receives:
┌──────────┬──────────┬────────┬───────────┬───────────┬─────────────────────┐
│ order_id │ customer │ amount │ status    │ operation │ seq_ts              │
├──────────┼──────────┼────────┼───────────┼───────────┼─────────────────────┤
│ 1001     │ Alice    │ 500    │ SHIPPED   │ UPDATE    │ 2024-01-01 11:00:00 │
│ 1002     │ Bob      │ 200    │ CANCELLED │ DELETE    │ 2024-01-01 12:00:00 │
└──────────┴──────────┴────────┴───────────┴───────────┴─────────────────────┘

silver_orders after APPLY CHANGES:
┌──────────┬──────────┬────────┬──────────┐
│ order_id │ customer │ amount │ status   │
├──────────┼──────────┼────────┼──────────┤
│ 1001     │ Alice    │ 500    │ SHIPPED  │  ← updated (PLACED → SHIPPED)
│ 1003     │ Carol    │ 750    │ PLACED   │  ← untouched
│  1002 removed ───────────────────────────  ← deleted (Bob's order gone)
└──────────┴──────────┴────────┴──────────┘
```

#### Batch 3 — Late arriving UPDATE for 1003
```
bronze_orders_cdc receives (out of order — arrived late):
┌──────────┬──────────┬────────┬───────────┬───────────┬─────────────────────┐
│ order_id │ customer │ amount │ status    │ operation │ seq_ts              │
├──────────┼──────────┼────────┼───────────┼───────────┼─────────────────────┤
│ 1003     │ Carol    │ 900    │ DELIVERED │ UPDATE    │ 2024-01-01 13:00:00 │
└──────────┴──────────┴────────┴───────────┴───────────┴─────────────────────┘

SEQUENCE BY seq_ts confirms 13:00 > 10:10 → safely applied

silver_orders (final state):
┌──────────┬──────────┬────────┬───────────┐
│ order_id │ customer │ amount │ status    │
├──────────┼──────────┼────────┼───────────┤
│ 1001     │ Alice    │ 500    │ SHIPPED   │
│ 1003     │ Carol    │ 900    │ DELIVERED │  ← updated correctly
└──────────┴──────────┴────────┴───────────┘
```

---

### Step 4 — Gold Output (final aggregation)

```
gold_order_summary:
┌───────────┬───────────────┬──────────────┐
│ status    │ total_revenue │ total_orders │
├───────────┼───────────────┼──────────────┤
│ SHIPPED   │ 500           │ 1            │
│ DELIVERED │ 900           │ 1            │
└───────────┴───────────────┴──────────────┘
```

---

### Key Concepts Recap

| Concept | What it does |
|---|---|
| `bronze_orders_cdc` | Captures raw CDC events — never filtered or modified |
| `dlt.create_streaming_table` | Pre-declares silver target before `apply_changes` writes to it |
| `apply_changes()` | DLT's CDC engine — handles upserts and deletes automatically |
| `keys=["order_id"]` | Matches events to existing rows by primary key |
| `sequence_by=col("seq_ts")` | Ensures latest event wins even if events arrive out of order |
| `apply_as_deletes` | Rows where `operation == DELETE` are removed from silver |
| `except_column_list` | Strips CDC metadata (`operation`, `seq_ts`) from the clean target |
| `dlt.read("silver_orders")` | Gold reads silver as **batch** (not stream) since CDC target isn't streamable |
