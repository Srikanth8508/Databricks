# Databricks Asset Bundle (DAB) — Multi-Task Workflow Project

> Architecture: CSV File → Bronze → Silver → Gold  
> Pattern: Databricks Asset Bundle with Multi-Task Job Workflow

---

# Overview

This project demonstrates:

- Databricks Asset Bundles (DAB)
- Multi-Task Workflow Jobs
- Bronze → Silver → Gold Architecture
- Delta Lake Tables
- Declarative Deployment
- Environment-Based Deployment
- Workflow Orchestration
- Automated Validation

---

# Project Structure

```text
Demo_1/
│
├── databricks.yml
│
├── resources/
│   └── job.yml
│
├── src/
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   └── validation_notebook.py
│
└── data/
    └── taxi_sample.csv
```

---

# Architecture Flow

```text
CSV File
   ↓
Bronze Layer
(Raw Data)
   ↓
Silver Layer
(Cleaned Data)
   ↓
Gold Layer
(Aggregated Metrics)
   ↓
Validation Notebook
```

---

# 1. Configure Asset Bundle — `databricks.yml`

## Description
Defines the Databricks Asset Bundle configuration and deployment targets.

This file controls environments, workspace configuration, deployment mode, and bundle settings.

## Command

```yaml
bundle:
  name: "Demo_1"

include:
  - resources/*.yml
  - resources/*/*.yml

targets:

  dev:
    mode: development
    default: true

    workspace:
      host: https://adb-7405618463147819.19.azuredatabricks.net

  prod:
    mode: production

    workspace:
      host: https://adb-7405618463147819.19.azuredatabricks.net

      root_path: /Users/john@gmail.com/.bundle/${bundle.name}/${bundle.target}

    run_as:
      user_name: john@gmail.com
```

## Purpose
- Defines bundle configuration
- Configures dev and prod environments

---

# 2. Configure Multi-Task Workflow — `resources/job.yml`

## Description
Defines a multi-task workflow job with task dependencies.

Each task executes sequentially from Bronze → Silver → Gold → Validation.

## Command

```yaml
resources:

  jobs:

    taxi_workflow:

      name: taxi_workflow

      tasks:

        # ----------------------------------------------------
        # Bronze Task
        # ----------------------------------------------------

        - task_key: bronze_task

          existing_cluster_id: "0512-083236-5qsrxfoi"

          spark_python_task:
            python_file: ../src/bronze.py

        # ----------------------------------------------------
        # Silver Task
        # ----------------------------------------------------

        - task_key: silver_task

          existing_cluster_id: "0512-083236-5qsrxfoi"

          depends_on:
            - task_key: bronze_task

          spark_python_task:
            python_file: ../src/silver.py

        # ----------------------------------------------------
        # Gold Task
        # ----------------------------------------------------

        - task_key: gold_task

          existing_cluster_id: "0512-083236-5qsrxfoi"

          depends_on:
            - task_key: silver_task

          spark_python_task:
            python_file: ../src/gold.py

        # ----------------------------------------------------
        # Validation Task
        # ----------------------------------------------------

        - task_key: validation_task

          existing_cluster_id: "0512-083236-5qsrxfoi"

          depends_on:
            - task_key: gold_task

          spark_python_task:
            python_file: ../src/validation_notebook.py
```

## Purpose
- Creates workflow orchestration
- Defines task dependencies

---

# 3. Bronze Layer — `src/bronze.py`

## Description
Reads raw CSV data and stores it into the Bronze Delta table.

Bronze layer stores raw ingestion data without transformations.

## Command

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ------------------------------------------------------------
# Read CSV File
# ------------------------------------------------------------

file_path = (
    "/Workspace/Users/john@gmail.com/"
    "Databricks_1/Demo_1/data/taxi_sample.csv"
)

df = (

    spark.read

         .option("header", True)

         .csv(file_path)
)

# ------------------------------------------------------------
# Write Bronze Delta Table
# ------------------------------------------------------------

(
    df.write

      .format("delta")

      .mode("overwrite")

      .saveAsTable("demo.taxi.taxi_bronze")
)

print("Bronze table created successfully")
```

## Purpose
- Reads raw CSV file
- Creates Bronze Delta table

---

# 4. Silver Layer — `src/silver.py`

## Description
Reads Bronze data and applies cleansing and validation logic.

Invalid `fare_amount` values are converted safely and filtered out.

## Command

```python
from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    expr
)

spark = SparkSession.builder.getOrCreate()

# ------------------------------------------------------------
# Read Bronze Table
# ------------------------------------------------------------

df = spark.table(
    "demo.taxi.taxi_bronze"
)

# ------------------------------------------------------------
# Data Cleansing
# ------------------------------------------------------------

cleaned_df = (

    df

      # Safely cast fare_amount
      .withColumn(
          "fare_amount",
          expr(
              "try_cast(fare_amount as double)"
          )
      )

      # Remove invalid/null values
      .filter(
          col("fare_amount").isNotNull()
      )
)

# ------------------------------------------------------------
# Write Silver Table
# ------------------------------------------------------------

(
    cleaned_df.write

              .format("delta")

              .mode("overwrite")

              .option(
                  "overwriteSchema",
                  "true"
              )

              .saveAsTable(
                  "demo.taxi.taxi_silver"
              )
)

print(
    f"Silver table written: "
    f"{cleaned_df.count()} rows"
)
```

## Purpose
- Cleans invalid records
- Creates validated Silver table

---

# 5. Gold Layer — `src/gold.py`

## Description
Reads Silver data and creates aggregated business metrics.

Gold layer stores city-level total fare summaries.

## Command

```python
from pyspark.sql import SparkSession

from pyspark.sql.functions import sum

spark = SparkSession.builder.getOrCreate()

# ------------------------------------------------------------
# Read Silver Table
# ------------------------------------------------------------

df = spark.table(
    "demo.taxi.taxi_silver"
)

# ------------------------------------------------------------
# Aggregate Data
# ------------------------------------------------------------

gold_df = (

    df.groupBy("city")

      .agg(
          sum("fare_amount")
              .alias("total_fare")
      )
)

# ------------------------------------------------------------
# Write Gold Table
# ------------------------------------------------------------

(
    gold_df.write

           .format("delta")

           .mode("overwrite")

           .saveAsTable(
               "demo.taxi.city_fare_summary"
           )
)

print("Gold aggregation completed")
```

## Purpose
- Aggregates fare metrics
- Creates Gold reporting table

---

# 6. Validation Notebook — `src/validation_notebook.py`

## Description
Displays the final Gold layer output table.

Used to validate pipeline execution and aggregation results.

## Command

```python
display(
    spark.table(
        "demo.taxi.city_fare_summary"
    )
)
```

## Purpose
- Validates final output
- Displays aggregated metrics

---

# 7. Validate Asset Bundle

## Description
Validates Asset Bundle configuration before deployment.

Checks syntax, references, and workflow structure.

## Command

```bash
databricks bundle validate
```

## Purpose
- Detects configuration issues
- Validates deployment files

---

# 8. Deploy Asset Bundle

## Description
Deploys the Asset Bundle into the Databricks workspace.

Creates workflow jobs, tasks, and configurations.

## Command

```bash
databricks bundle deploy -t dev
```

## Purpose
- Deploys workflow resources
- Creates job configurations

---

# 9. Run Workflow Job

## Description
Executes the workflow job sequentially.

Tasks run in dependency order: Bronze → Silver → Gold → Validation.

## Command

```bash
databricks bundle run taxi_workflow -t dev
```

## Purpose
- Executes complete workflow
- Creates all Delta tables

---

# Workflow Execution Flow

```text
bronze_task
      ↓
silver_task
      ↓
gold_task
      ↓
validation_task
```

---

# Expected Bronze Output

## Query

```sql
SELECT *
FROM demo.taxi.taxi_bronze
```

## Sample Output

| trip_id | city | fare_amount |
|---|---|---|
| 1 | Chennai | 250 |
| 2 | Bangalore | 320 |
| 3 | Hyderabad | 150t |
| 4 | Mumbai | 480 |

## Characteristics

- Raw ingestion layer
- Invalid values still present
- No transformations applied

---

# Expected Silver Output

## Query

```sql
SELECT *
FROM demo.taxi.taxi_silver
```

## Sample Output

| trip_id | city | fare_amount |
|---|---|---|
| 1 | Chennai | 250 |
| 2 | Bangalore | 320 |
| 4 | Mumbai | 480 |

## Characteristics

- Invalid values removed
- `fare_amount` converted to double
- Clean analytical dataset

---

# Expected Gold Output

## Query

```sql
SELECT *
FROM demo.taxi.city_fare_summary
```

## Sample Output

| city | total_fare |
|---|---|
| Chennai | 250 |
| Bangalore | 320 |
| Mumbai | 480 |

## Characteristics

- Aggregated business metrics
- Reporting-ready dataset
- Used for dashboards

---

