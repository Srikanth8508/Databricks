# Delta Live Tables (DLT)

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

