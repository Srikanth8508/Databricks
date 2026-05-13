# Databricks Delta Live Tables (DLT) CDC Pipeline 

> Architecture: Azure Data Lake Storage → Bronze → Silver → Silver Enriched → Gold  
> Pattern: Multi-Hop CDC Pipeline using Delta Live Tables and SCD Type 1

---

# 1. Create ADLS Directory

## Description
Creates the source directory in Azure Data Lake Storage for storing CDC JSON files.

This folder acts as the landing zone for INSERT, UPDATE, and DELETE events.

## Command

```python
dbutils.fs.mkdirs(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/dlt/orders_1/"
)
```

---

# 2. Generate Initial CDC INSERT Records

## Description
Creates 100 initial INSERT events with CDC metadata columns.

Each record contains `_operation` and `_sequence_num` required for CDC processing.

## Command

```python
import json
import random
from datetime import datetime, timedelta

# ------------------------------------------------------------
# Sample Master Data
# ------------------------------------------------------------

customers = [
    "Ravi", "Arjun", "Kiran", "Meena", "John",
    "Priya", "Vijay", "Anu", "Rahul", "Sneha",
    "Aakash", "Divya", "Manoj", "Keerthi"
]

cities = [
    "Chennai",
    "Bangalore",
    "Hyderabad",
    "Mumbai",
    "Delhi",
    "Pune",
    "Kolkata",
    "Ahmedabad"
]

# ------------------------------------------------------------
# Generate CDC INSERT Records
# ------------------------------------------------------------

records = []

base_time = datetime(2025, 5, 12, 10, 0, 0)

sequence_num = 1

for i in range(1, 101):

    order = {
        "order_id": i,
        "customer": random.choice(customers),
        "city": random.choice(cities),
        "amount": random.randint(1000, 10000),
        "order_time": (
            base_time + timedelta(minutes=i)
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "_operation": "INSERT",
        "_sequence_num": sequence_num
    }

    records.append(json.dumps(order))

    sequence_num += 1

# ------------------------------------------------------------
# Convert to Multiline JSON
# ------------------------------------------------------------

data = "\n".join(records)

# ------------------------------------------------------------
# Write File into ADLS
# ------------------------------------------------------------

dbutils.fs.put(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/dlt/orders_1/orders1.json",
    data,
    True
)

print(f"Created orders1.json with {len(records)} INSERT events")
```

---

# 3. Generate CDC UPDATE and DELETE Events

## Description
Creates additional CDC events for UPDATE and DELETE operations.

These events simulate real-time changes after the initial load.

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
            "Ravi", "Priya", "Arjun",
            "Meena", "Vijay", "Rahul"
        ]),
        "city": random.choice([
            "Chennai",
            "Mumbai",
            "Delhi",
            "Pune"
        ]),
        "amount": random.randint(2000, 12000),
        "order_time": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
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

# ------------------------------------------------------------
# Convert to JSON String
# ------------------------------------------------------------

data = "\n".join([
    json.dumps(event)
    for event in cdc_events
])

# ------------------------------------------------------------
# Write CDC Events File
# ------------------------------------------------------------

dbutils.fs.put(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/dlt/orders_1/orders2.json",
    data,
    True
)

print(
    f"Created orders2.json with "
    f"{len(cdc_events)} CDC events"
)
```

---

# 4. Create DLT Pipeline

## Steps

```text
Jobs & Pipelines
        ↓
Create
        ↓
ETL Pipeline
```

---

# 5. Configure Pipeline Settings

## Steps

```text
Settings
   ↓
Pipeline Mode
(Triggered / Continuous)

   ↓
Select Default Schema
orders_schema

   ↓
Add Configuration Values
```

---

# 6. Add Azure OAuth Configuration

## Configuration

```properties
spark.hadoop.fs.azure.account.auth.type.databricktesdat.dfs.core.windows.net=OAuth

spark.hadoop.fs.azure.account.oauth.provider.type.databricktesdat.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider

spark.hadoop.fs.azure.account.oauth2.client.id.databricktesdat.dfs.core.windows.net=add client id

spark.hadoop.fs.azure.account.oauth2.client.secret.databricktesdat.dfs.core.windows.net=add client secret

spark.hadoop.fs.azure.account.oauth2.client.endpoint.databricktesdat.dfs.core.windows.net=https://login.microsoftonline.com/6d5176bef049/oauth2/token
```

---

# 7. Complete DLT CDC Pipeline Code

## Description
Processes CDC events using `dlt.apply_changes()` and maintains the latest SCD Type 1 table state.

---

## Command

```python
import dlt

from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    round,
    expr,
    to_date
)

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

STORAGE_ACCOUNT = "databricktesdat"

CONTAINER = "raw-data"

BASE_PATH = (
    f"abfss://{CONTAINER}@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net"
)

ORDERS_PATH = f"{BASE_PATH}/dlt/orders_1/"

SCHEMA_PATH = f"{BASE_PATH}/dlt/schema/orders_1"

# ------------------------------------------------------------
# Bronze Layer
# ------------------------------------------------------------

@dlt.table(
    name="bronze_orders_1",
    comment="Raw CDC event stream from ADLS"
)

def bronze_orders_1():

    return (

        spark.readStream

             .format("cloudFiles")

             .option("cloudFiles.format", "json")

             .option(
                 "cloudFiles.schemaLocation",
                 SCHEMA_PATH
             )

             .load(ORDERS_PATH)
    )

# ------------------------------------------------------------
# Silver Layer — CDC Merge
# ------------------------------------------------------------

dlt.create_streaming_table(
    name="silver_orders_1",
    comment="Current active order state using SCD Type 1"
)

dlt.apply_changes(

    target="silver_orders_1",

    source="bronze_orders_1",

    keys=["order_id"],

    sequence_by=col("_sequence_num"),

    apply_as_deletes=expr(
        "_operation = 'DELETE'"
    ),

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
    comment="Validated and enriched order dataset"
)

def silver_orders_enriched():

    return (

        dlt.read("silver_orders_1")

           .filter(
               col("amount") > 1000
           )

           .withColumn(
               "order_date",
               to_date(col("order_time"))
           )
    )

# ------------------------------------------------------------
# Gold Layer
# ------------------------------------------------------------

@dlt.table(
    name="gold_orders_1",
    comment="Customer and city level revenue summary"
)

def gold_orders_1():

    return (

        dlt.read("silver_orders_enriched")

           .groupBy(
               "customer",
               "city"
           )

           .agg(

               count("order_id")
                   .alias("total_orders"),

               sum("amount")
                   .alias("total_revenue"),

               round(
                   avg("amount"),
                   2
               ).alias("avg_order_value")
           )

           .orderBy(
               col("total_revenue").desc()
           )
    )
```

---

# 8. Perform Dry Run

## Steps

```text
Click Dry Run
```

## Purpose
- Validates pipeline syntax
- Checks dependencies and permissions

---

# 9. Run Pipeline

## Steps

```text
Click Start / Run Pipeline
```

## Purpose
- Starts CDC processing
- Creates Bronze, Silver, and Gold tables

---

# 10. Validate Bronze Output

## Query

```sql
SELECT *
FROM orders_schema.bronze_orders_1
```

## Sample Output

| _operation | _sequence_num | amount | city | customer | order_id |
|---|---|---|---|---|---|
| INSERT | 1 | 5686 | Chennai | Kiran | 1 |
| INSERT | 2 | 5483 | Delhi | Meena | 2 |
| INSERT | 3 | 8087 | Ahmedabad | Vijay | 3 |

---

# 11. Validate Silver Output

## Query

```sql
SELECT *
FROM orders_schema.silver_orders_1
```

## Sample Output

| amount | city | customer | order_id |
|---|---|---|---|
| 5686 | Chennai | Kiran | 1 |
| 3104 | Pune | Ravi | 10 |
| 7540 | Mumbai | Rahul | 100 |

---

# 12. Validate Silver Enriched Output

## Query

```sql
SELECT *
FROM orders_schema.silver_orders_enriched
```

## Sample Output

| amount | city | customer | order_date |
|---|---|---|---|
| 5686 | Chennai | Kiran | 2025-05-12 |
| 3104 | Pune | Ravi | 2025-05-12 |
| 7540 | Mumbai | Rahul | 2025-05-12 |

---

# 13. Validate Gold Output

## Query

```sql
SELECT *
FROM orders_schema.gold_orders_1
```

## Sample Output

| customer | city | total_orders | total_revenue | avg_order_value |
|---|---|---|---|---|
| Vijay | Ahmedabad | 1 | 8087 | 8087 |
| Manoj | Bangalore | 2 | 11710 | 5855 |
| Anu | Delhi | 2 | 10434 | 5217 |

---

# Complete CDC Flow

```text
orders1.json
(100 INSERT Events)
        ↓

orders2.json
(UPDATE + DELETE Events)
        ↓

Bronze Layer
(Raw CDC Event Stream)
        ↓

dlt.apply_changes()
(SCD Type 1 Merge)
        ↓

Silver Layer
(Current Latest State)
        ↓

Silver Enriched Layer
(Business Transformations)
        ↓

Gold Layer
(Aggregated Business Metrics)
```

---

# Important CDC Concepts

| Concept | Description |
|---|---|
| `dlt.apply_changes()` | Handles CDC merge automatically |
| `keys` | Primary key for merge |
| `sequence_by` | Maintains latest event order |
| `apply_as_deletes` | Deletes matching rows |
| `stored_as_scd_type=1` | Maintains latest record only |
| Bronze Layer | Raw CDC event storage |
| Silver Layer | Latest merged state |
| Gold Layer | Aggregated reporting layer |

---

# Summary

This implementation demonstrates:

- Delta Live Tables
- CDC Pipelines
- Bronze → Silver → Gold Architecture
- Auto Loader
- SCD Type 1 Processing
- Incremental Streaming
- CDC Merge Processing
- Business Aggregation Pipelines

---
