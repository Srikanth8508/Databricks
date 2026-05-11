# Change Data Capture (CDC) in Delta Lake — Complete Practice Guide

# What is CDC?

CDC stands for:

## Change Data Capture

CDC tracks changes happening in a table such as:

- INSERTS
- UPDATES
- DELETES

Instead of scanning the full table repeatedly, CDC processes only changed rows.

---

# Why CDC is Important

## Without CDC

Problems:

- Entire table scanned repeatedly
- Expensive processing
- Slow ETL pipelines
- High compute cost

---

## With CDC

Benefits:

- Only changed rows processed
- Faster incremental pipelines
- Reduced compute usage
- Efficient real-time processing
- Better scalability

---

# Real-Time Example

## Existing Customer Table

| id | name | city |
|---|---|---|
| 1 | Ravi | Chennai |
| 2 | Arjun | Bangalore |

---

## Later Changes

- Arjun city updated
- New customer inserted
- Existing customer deleted

CDC captures only these changes instead of reading the complete table.

---

# Delta Lake CDC Feature

Delta Lake provides:

## Change Data Feed (CDF)

CDF automatically tracks row-level changes in Delta tables.

---

# Step 1 — Create Delta Table

```sql
CREATE TABLE customers (
    id INT,
    name STRING,
    city STRING
)
USING DELTA;
```

---

# Step 2 — Enable Change Data Feed

```sql
ALTER TABLE customers
SET TBLPROPERTIES (
    delta.enableChangeDataFeed = true
);
```

---

# Step 3 — Insert Initial Data

```sql
INSERT INTO customers VALUES
(1, 'Ravi', 'Chennai'),
(2, 'Arjun', 'Bangalore');
```

---

# Step 4 — Check Table Data

```sql
SELECT * FROM customers;
```

## Output

| id | name | city |
|---|---|---|
| 1 | Ravi | Chennai |
| 2 | Arjun | Bangalore |

---

# Step 5 — Update Existing Row

```sql
UPDATE customers
SET city = 'Hyderabad'
WHERE id = 2;
```

---

# Step 6 — Insert New Row

```sql
INSERT INTO customers VALUES
(3, 'Kiran', 'Mumbai');
```

---

# Step 7 — Delete Existing Row

```sql
DELETE FROM customers
WHERE id = 1;
```

---

# Step 8 — Read CDC Changes

```sql
SELECT *
FROM table_changes('customers', 0);
```

---

# CDC Output Explanation

## Sample CDC Output

| id | name | city | _change_type | _commit_version |
|---|---|---|---|---|
| 1 | Ravi | Chennai | insert | 1 |
| 2 | Arjun | Bangalore | insert | 1 |
| 2 | Arjun | Bangalore | update_preimage | 2 |
| 2 | Arjun | Hyderabad | update_postimage | 2 |
| 3 | Kiran | Mumbai | insert | 3 |
| 1 | Ravi | Chennai | delete | 4 |

---

# Understanding CDC Metadata Columns

# 1. _change_type

Shows the type of operation performed.

## Possible Values

| Value | Meaning |
|---|---|
| insert | New row inserted |
| delete | Existing row deleted |
| update_preimage | Old row before update |
| update_postimage | New row after update |

---

# 2. _commit_version

Represents the Delta table version number.

Every operation creates a new table version.

---

# 3. _commit_timestamp

Stores the timestamp of the operation.

Useful for:

- Auditing
- Incremental processing
- Debugging

---

# Read CDC Between Specific Versions

## Read Changes from Version 1 to 3

```sql
SELECT *
FROM table_changes('customers', 1, 3);
```

## Meaning

| Parameter | Description |
|---|---|
| 1 | Start Version |
| 3 | End Version |

Only changes between these versions are returned.

---

# Check Delta Table History

```sql
DESCRIBE HISTORY customers;
```

You can view operations like:

- INSERT
- UPDATE
- DELETE
- MERGE
- OPTIMIZE
- VACUUM

---

