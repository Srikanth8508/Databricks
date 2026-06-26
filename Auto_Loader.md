# Databricks Auto Loader – Complete Production Guide

## 1. Complete Auto Loader Example (All Commonly Used Options)

```python
from pyspark.sql.types import *

df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("cloudFiles.schemaLocation", "/Volumes/main/checkpoints/schema")
         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
         .option("cloudFiles.inferColumnTypes", "true")
         .option("header", "true")
         .load("/Volumes/main/raw/employees/")
)

(
    df.writeStream
      .option("checkpointLocation", "/Volumes/main/checkpoints/employee")
      .option("mergeSchema", "true")
      .trigger(availableNow=True)
      .toTable("employee_data")
)
```

---

# 3. Production Scenario

Assume new employee files arrive every day.

```
/Volumes/main/raw/employees/

Day 1
 ├── emp1.csv
 └── emp2.csv

Day 2
 ├── emp1.csv
 ├── emp2.csv
 └── emp3.csv

Day 3
 ├── emp1.csv
 ├── emp2.csv
 ├── emp3.csv
 ├── emp4.csv
 └── emp5.csv
```

Auto Loader processes **only newly arrived files**.

---

# 4. How Auto Loader Handles New Files

## First Run

```
emp1.csv
emp2.csv
```

| File     | Action   |
| -------- | -------- |
| emp1.csv | ✅ Loaded |
| emp2.csv | ✅ Loaded |

Checkpoint remembers both files.

---

## Second Run

```
emp1.csv
emp2.csv
emp3.csv
```

| File     | Action   |
| -------- | -------- |
| emp1.csv | ⏭️ Skip  |
| emp2.csv | ⏭️ Skip  |
| emp3.csv | ✅ Loaded |

---

## Third Run

```
emp1.csv
emp2.csv
emp3.csv
emp4.csv
emp5.csv
```

| File     | Action   |
| -------- | -------- |
| emp1.csv | ⏭️ Skip  |
| emp2.csv | ⏭️ Skip  |
| emp3.csv | ⏭️ Skip  |
| emp4.csv | ✅ Loaded |
| emp5.csv | ✅ Loaded |

---

# 5. `cloudFiles.schemaLocation`

```python
.option(
    "cloudFiles.schemaLocation",
    "/Volumes/main/checkpoints/schema"
)
```

## Why do we need it?

Suppose Day 1 contains:

```csv
id,name,salary
1,Alice,50000
```

Auto Loader infers

| Column | Type   |
| ------ | ------ |
| id     | INT    |
| name   | STRING |
| salary | INT    |

The inferred schema is stored in:

```
/Volumes/main/checkpoints/schema
```

Next time another file arrives:

```csv
id,name,salary
2,Bob,70000
```

Auto Loader reuses the stored schema instead of inferring it again.

### Benefits

* Better performance
* Consistent schema
* Enables schema evolution
* Supports restart after failures

---

# 6. `cloudFiles.schemaEvolutionMode`

## a) addNewColumns (Recommended)

```
Day 1
```

```csv
id,name
1,Alice
```

```
Day 2
```

```csv
id,name,department
2,Bob,IT
```

Result:

| id | name  | department |
| -- | ----- | ---------- |
| 1  | Alice | NULL       |
| 2  | Bob   | IT         |

Automatically adds `department`.

---

## b) rescue

Unknown columns are moved into `_rescued_data`.

Input

```csv
id,name,new_col
1,Alice,XYZ
```

Output

| id | name  | _rescued_data     |
| -- | ----- | ----------------- |
| 1  | Alice | {"new_col":"XYZ"} |

---

## c) failOnNewColumns

New column detected:

```csv
id,name,department
```

Result

```
❌ Stream fails immediately.
```

Useful for strict governance.

---

## d) none

Ignores unknown columns completely.

```
department
```

is simply ignored.

---

# 7. `cloudFiles.inferColumnTypes`

```python
.option(
    "cloudFiles.inferColumnTypes",
    "true"
)
```

Without inference

```csv
id,salary
1,50000
```

Spark may infer:

| id     | salary |
| ------ | ------ |
| STRING | STRING |

With inference enabled:

| id      | salary  |
| ------- | ------- |
| INTEGER | INTEGER |

No manual casting required.

---

# 8. `mergeSchema` in `writeStream`

```python
.writeStream
.option(
    "mergeSchema",
    "true"
)
```

Existing Delta table:

| id | name |
| -- | ---- |

Incoming data:

| id | name | department |
| -- | ---- | ---------- |

Without mergeSchema

```
❌ Schema mismatch error
```

With mergeSchema

Delta table automatically becomes

| id | name | department |
| -- | ---- | ---------- |

---

# 9. Trigger Options

## availableNow=True

```
Folder

emp1.csv
emp2.csv
emp3.csv
```

Processes

```
✅ emp1
✅ emp2
✅ emp3
```

Then stops.

If `emp4.csv` arrives later, rerun the job.

---

## once=True

Runs only **one micro-batch**.

Suppose:

```
10:00
emp1.csv
emp2.csv
```

First micro-batch picks:

```
✅ emp1.csv
```

Job exits.

`emp2.csv` waits until next execution.

---

## processingTime="1 minute"

Job keeps running.

```
10:00 → emp1
10:02 → emp2
10:05 → emp3
```

Every minute Auto Loader checks for new files and processes them.

Best for continuous streaming pipelines.

---

# 10. StructType (Explicit Schema)

Instead of inferring schema automatically, we can define it ourselves.

```python
from pyspark.sql.types import *

employee_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True)
])
```

## Reading using StructType

```python
df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .schema(employee_schema)
         .load("/Volumes/main/raw/employees/")
)
```

## Writing using StructType

```python
(
    df.writeStream
      .option(
          "checkpointLocation",
          "/Volumes/main/checkpoints/employee"
      )
      .trigger(availableNow=True)
      .toTable("employee_data")
)
```

---

## Why use StructType?

Suppose input file:

```csv
employee_id,salary
001,50000
002,60000
```

With inference:

| employee_id |
| ----------- |
| 1           |
| 2           |

Leading zeros may be lost.

Using

```python
StructField(
    "employee_id",
    StringType(),
    True
)
```

preserves

| employee_id |
| ----------- |
| 001         |
| 002         |

StructType also:

* Improves startup performance.
* Prevents incorrect type inference.
* Ensures consistent schema across all files.
* Is recommended for production when the schema is known.

---

# 11. Nested StructType

Useful for nested JSON data.

Input JSON

```json
{
  "id":1,
  "name":"Alice",
  "address":{
      "city":"Chennai",
      "state":"Tamil Nadu",
      "pincode":600001
  }
}
```

Schema

```python
address_schema = StructType([
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("pincode", IntegerType())
])

employee_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("address", address_schema)
])
```

Output

| id | name  | address.city | address.state | address.pincode |
| -- | ----- | ------------ | ------------- | --------------- |
| 1  | Alice | Chennai      | Tamil Nadu    | 600001          |

Nested `StructType` allows Spark to represent complex objects inside a single column.

---

# 12. Schema Inference vs StructType

| Feature             | Schema Inference                                | StructType                 |
| ------------------- | ----------------------------------------------- | -------------------------- |
| Who defines schema? | Spark automatically                             | Developer explicitly       |
| Startup performance | Slower (needs inference)                        | Faster                     |
| Data type accuracy  | May infer incorrectly                           | Fully controlled           |
| Consistency         | May vary across files                           | Always consistent          |
| Leading zeros       | May be removed                                  | Can be preserved           |
| Best for            | Exploration or unknown data                     | Production pipelines       |
| Example             | `.option("cloudFiles.inferColumnTypes","true")` | `.schema(employee_schema)` |

---

# 13. Parameter Summary

| Parameter                            | Purpose                                  |
| ------------------------------------ | ---------------------------------------- |
| `format("cloudFiles")`               | Enables Auto Loader                      |
| `cloudFiles.format`                  | Specifies source file type               |
| `cloudFiles.schemaLocation`          | Stores inferred schema metadata          |
| `cloudFiles.schemaEvolutionMode`     | Handles schema changes                   |
| `cloudFiles.inferColumnTypes`        | Infers column data types                 |
| `schema()`                           | Uses explicit `StructType` schema        |
| `checkpointLocation`                 | Tracks processed files                   |
| `mergeSchema`                        | Evolves Delta table schema               |
| `trigger(availableNow=True)`         | Processes all available files then stops |
| `trigger(once=True)`                 | Executes one micro-batch and stops       |
| `trigger(processingTime="1 minute")` | Continuously checks for new files        |
| `toTable()`                          | Writes the stream into a Delta table     |


