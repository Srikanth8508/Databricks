````md id="s7pl2x"
# Databricks Delta Live Tables (DLT) Pipeline — Complete Setup Guide

---

# 1. Create Storage Directory

## Description
Creates a directory in Azure Data Lake Storage for storing raw order JSON files.

This folder acts as the landing zone for incoming files that will be processed by the DLT pipeline.

## Command
```python
dbutils.fs.mkdirs(
  "abfss://raw-data@databricktesdat.dfs.core.windows.net/dlt/orders/"
)
```

## Purpose
- Creates the source folder for DLT ingestion
- Stores raw files for pipeline processing

---

# 2. Create Sample JSON File

## Description
Creates sample order data and writes it into the ADLS folder as a JSON file.

This data will be used as the input source for the Bronze table in the DLT pipeline.

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
- Generates sample streaming input data
- Provides source records for DLT processing

---

# 3. Create DLT Pipeline

## Description
Creates a Delta Live Tables pipeline from the Databricks UI.

The pipeline manages orchestration, dependency handling, monitoring, and execution automatically.

## Steps
```text
Click Jobs & Pipelines
        ↓
Click Create
        ↓
Select ETL Pipeline
```

## Purpose
- Creates a managed DLT workflow
- Automates pipeline execution and monitoring

---

# 4. Configure Pipeline Settings

## Description
Configure the pipeline mode, catalog/schema, and authentication settings.

These settings allow the DLT pipeline to securely access Azure Data Lake Storage.

## Configuration Steps
```text
Settings
   ↓
Change Pipeline Mode (Triggered / Continuous)
   ↓
Select Default Schema
   ↓
Add Configuration Values
```

## Purpose
- Defines pipeline execution behavior
- Enables secure storage access

---

# 5. Add Azure OAuth Configuration

## Description
Adds OAuth authentication details required for accessing the ADLS storage account.

These configurations authenticate the DLT pipeline using Azure Service Principal credentials.

## Configuration
```properties
spark.hadoop.fs.azure.account.auth.type.databricktesdat.dfs.core.windows.net=OAuth

spark.hadoop.fs.azure.account.oauth.provider.type.databricktesdat.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider

spark.hadoop.fs.azure.account.oauth2.client.id.databricktesdat.dfs.core.windows.net=add client id

spark.hadoop.fs.azure.account.oauth2.client.secret.databricktesdat.dfs.core.windows.net=add client secret

spark.hadoop.fs.azure.account.oauth2.client.endpoint.databricktesdat.dfs.core.windows.net=https://login.microsoftonline.com/6d5176bef049/oauth2/token
```

## Purpose
- Authenticates Azure storage access securely
- Allows DLT to read/write ADLS files

---

# 6. Create Bronze Layer

## Description
Reads raw JSON files from ADLS using Auto Loader and stores them in the Bronze table.

Bronze layer captures raw incoming data without transformations.

## Command
```python
import dlt
from pyspark.sql.functions import *

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
```

## Purpose
- Ingests raw streaming files
- Creates the Bronze ingestion layer

---

# 7. Create Silver Layer

## Description
Reads streaming data from the Bronze table and applies data quality validation.

Records where `amount <= 1000` are automatically dropped using DLT expectations.

## Command
```python
@dlt.table(
    name="silver_orders",
    comment="Cleaned data with quality gates"
)

@dlt.expect_or_drop(
    "positive_amount",
    "amount > 1000"
)

def silver_orders():

    return dlt.read_stream("bronze_orders")
```

## Purpose
- Cleans invalid records
- Applies data quality rules automatically

---

# 8. Create Gold Layer

## Description
Aggregates cleaned Silver data to generate customer-level revenue metrics.

Gold layer contains summarized analytical data for reporting and dashboards.

## Command
```python
@dlt.table(
    name="gold_revenue",
    comment="Aggregate sales by customer"
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
- Generates business-level aggregated metrics
- Supports reporting and analytics

---

# 9. Perform Dry Run

## Description
Dry Run validates the pipeline configuration before actual execution.

It checks dependencies, syntax, table flow, and configuration settings.

## Steps
```text
Click Dry Run
```

## Purpose
- Validates pipeline configuration
- Detects setup or syntax errors early

---

# 10. Run the Pipeline

## Description
Executes the DLT pipeline and starts processing the data flow.

The pipeline automatically creates and manages Bronze, Silver, and Gold tables.

## Steps
```text
Click Start / Run Pipeline
```

## Purpose
- Executes the complete DLT workflow
- Processes streaming data incrementally

---

# DLT Multi-Hop Architecture Flow

```text
ADLS JSON Files
        ↓
Bronze Layer
(Raw Data)
        ↓
Silver Layer
(Cleaned & Validated Data)
        ↓
Gold Layer
(Aggregated Business Metrics)
```

---

# Expected Output

## Bronze Table
| order_id | customer | amount |
|---|---|---|
| 1 | Ravi | 1200 |
| 2 | Arjun | 900 |
| 3 | Meena | 2500 |
| 4 | Kiran | 3000 |

---

## Silver Table
(Amount > 1000 only)

| order_id | customer | amount |
|---|---|---|
| 1 | Ravi | 1200 |
| 3 | Meena | 2500 |
| 4 | Kiran | 3000 |

---

## Gold Table
| customer | total_orders | total_revenue |
|---|---|---|
| Ravi | 1 | 1200 |
| Meena | 1 | 2500 |
| Kiran | 1 | 3000 |

---

# Key DLT Concepts

| Concept | Description |
|---|---|
| `@dlt.table` | Creates managed DLT tables |
| `dlt.read_stream()` | Reads streaming data incrementally |
| `dlt.read()` | Reads batch/static table data |
| `@dlt.expect_or_drop()` | Applies data quality rules |
| Bronze Layer | Raw ingestion layer |
| Silver Layer | Cleaned and validated data |
| Gold Layer | Aggregated business data |
| Auto Loader | Incremental file ingestion |
| Dry Run | Validates pipeline configuration |

---
````
