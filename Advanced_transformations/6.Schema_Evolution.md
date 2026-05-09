# Schema Evolution in Delta Lake — Complete Practice Guide

# What is Schema Evolution?

Schema Evolution means:

## Automatically adapting table schema when new columns arrive

without manually recreating the table.

---

# Why Schema Evolution is Important

Real-world data changes frequently.

## Example

### Today

Customer table:

| id | name |
|---|---|
| 1 | Ravi |

---

### Tomorrow

Source system sends:

| id | name | city |
|---|---|---|
| 1 | Ravi | Chennai |

New column added:

```text
city
```

---

# Without Schema Evolution

Problems:

- Write operation fails
- Schema mismatch error occurs
- Manual table recreation required
- ETL pipelines break

---

# With Schema Evolution

Delta Lake automatically:

- Detects new columns
- Updates table schema
- Safely appends data
- Prevents pipeline failures

---

# Real-Time Use Cases

| Industry | Example |
|---|---|
| E-Commerce | New product attributes |
| Banking | Additional transaction fields |
| IoT | New sensor columns |
| APIs | Dynamic JSON structures |
| Streaming | Evolving event schema |

---

# Types of Schema Handling in Delta Lake

| Feature | Purpose |
|---|---|
| Schema Enforcement | Prevent invalid schema |
| Schema Evolution | Allow controlled schema changes |
| mergeSchema | Add new columns automatically |
| overwriteSchema | Replace schema completely |

---

# Step 1 — Create Initial Delta Table

```sql
CREATE TABLE customers (
    id INT,
    name STRING
)
USING DELTA;
```

---

# Step 2 — Insert Initial Data

```sql
INSERT INTO customers VALUES
(1, 'Ravi'),
(2, 'Arjun');
```

---

# Step 3 — View Existing Table

```sql
SELECT * FROM customers;
```

---

# Output

| id | name |
|---|---|
| 1 | Ravi |
| 2 | Arjun |

---

# Step 4 — New Data Arrives with Extra Column

## New Dataset

| id | name | city |
|---|---|---|
| 3 | Kiran | Mumbai |

New column:

```text
city
```

---

# Without Schema Evolution

If you try normal append:

```python
df.write \
  .format("delta") \
  .mode("append") \
  .save("/delta/customers")
```

---

# Error

```text
Schema mismatch detected
```

Reason:

- Existing table schema differs from incoming schema

---

# Step 5 — Enable Schema Evolution

Use:

```python
df.write \
  .format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \
  .save("/delta/customers")
```

---

# What Happens Internally?

Delta Lake automatically:

1. Detects new column
2. Updates Delta schema
3. Preserves old data
4. Safely appends new records

---

# Step 6 — View Updated Table

```sql
SELECT * FROM customers;
```

---

# Final Output

| id | name | city |
|---|---|---|
| 1 | Ravi | NULL |
| 2 | Arjun | NULL |
| 3 | Kiran | Mumbai |

---

# Important Observation

Old rows contain:

```text
city = NULL
```

because historical data did not contain that column.

---

# Auto Schema Evolution in MERGE

Delta Lake also supports schema evolution during:

## MERGE INTO

---

# Enable Auto Merge

```sql
SET spark.databricks.delta.schema.autoMerge.enabled = true;
```

---

# MERGE Example

```sql
MERGE INTO customers AS target
USING updates AS source
ON target.id = source.id

WHEN MATCHED THEN
UPDATE SET *

WHEN NOT MATCHED THEN
INSERT *;
```

---

# Real-Time MERGE Scenario

## Existing Table

| id | name |
|---|---|
| 1 | Ravi |

---

## Incoming Source

| id | name | city |
|---|---|---|
| 1 | Ravi | Chennai |

---

# What Happens?

MERGE automatically:

- Adds new column `city`
- Updates matching records
- Preserves existing schema safely

---

# Schema Enforcement vs Schema Evolution

| Feature | Behavior |
|---|---|
| Schema Enforcement | Rejects invalid schema |
| Schema Evolution | Allows controlled schema changes |

---

# overwriteSchema in Delta Lake — Detailed Explanation

# What is overwriteSchema?

`overwriteSchema` is used when you want to:

## Completely replace the existing table schema

with a new schema.

---

# Important Concept

This is NOT just adding new columns.

It replaces:

- Column structure
- Data types
- Column names
- Table metadata

---

# Syntax

```python
df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("employees")
```

---

# Real-Time Scenario

# Existing Table

| id | name |
|---|---|
| 1 | Ravi |
| 2 | Arjun |

---

# Existing Schema

```text
id INT
name STRING
```

---

# New Business Requirement

Business wants:

| emp_id | full_name | city |
|---|---|---|
| 1 | Ravi Kumar | Chennai |

---

# New Schema

```text
emp_id INT
full_name STRING
city STRING
```

---

# Problem

This schema is completely different.

Normal append:

```text
Fails
```

`mergeSchema`:

```text
Not enough
```

Because:

- Column names changed
- Structure changed completely

---

# Solution → overwriteSchema

---

# Step 1 — Create Existing Table

```sql
CREATE TABLE employees (
    id INT,
    name STRING
)
USING DELTA;
```

---

# Step 2 — Insert Initial Data

```sql
INSERT INTO employees VALUES
(1, 'Ravi'),
(2, 'Arjun');
```

---

# Step 3 — Create New DataFrame

```python
data = [
    (1, 'Ravi Kumar', 'Chennai'),
    (2, 'Arjun Kumar', 'Hyderabad')
]

columns = ['emp_id', 'full_name', 'city']
```

---

# Step 4 — Overwrite Schema

```python
df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("employees")
```

---

# Final Table Schema

| emp_id | full_name | city |
|---|---|---|
| 1 | Ravi Kumar | Chennai |
| 2 | Arjun Kumar | Hyderabad |

---

# What Happens Internally?

Delta Lake:

1. Removes old schema metadata
2. Creates new schema
3. Rewrites table data
4. Creates new Delta table version

---

# Key Difference

| Feature | Behavior |
|---|---|
| mergeSchema | Adds new columns |
| overwriteSchema | Replaces complete schema |

---

# Visual Comparison

# mergeSchema

## Before

| id | name |
|---|---|

## After

| id | name | city |
|---|---|---|

Old columns remain.

---

# overwriteSchema

## Before

| id | name |
|---|---|

## After

| emp_id | full_name | city |
|---|---|---|

Old structure removed completely.

---

# Important Requirement

Must use:

```python
.mode("overwrite")
```

Otherwise:

```text
overwriteSchema does not work properly
```

---

# Common Errors

| Error | Cause | Fix |
|---|---|---|
| Schema mismatch detected | Incoming schema differs | Enable mergeSchema |
| Cannot merge incompatible data types | Different column types | Cast columns properly |
| overwriteSchema ignored | Missing overwrite mode | Use .mode(\"overwrite\") |
| Missing required column | Invalid source schema | Validate input data |

---

# Performance Considerations

Schema evolution is powerful but should be used carefully.

Best practices:

- Avoid unnecessary schema changes
- Keep schema stable when possible
- Monitor evolving columns
- Use governance for production tables

---



