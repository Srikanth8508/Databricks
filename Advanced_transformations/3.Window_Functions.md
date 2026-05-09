# Window Functions in Delta Lake — Complete Practice Guide

# What are Window Functions?

Window functions perform calculations across a group of rows while still returning individual rows.

Unlike `GROUP BY`:

- rows are NOT collapsed
- original records remain visible

---

# Real-Time Example

Employee salary table:

| emp_id | dept | salary |
|---|---|---|
| 1 | IT | 50000 |
| 2 | IT | 70000 |
| 3 | HR | 40000 |

You may want to find:

- Highest salary in each department
- Employee ranking
- Running total
- Latest record
- Deduplication

This is where window functions are used.

---

# Basic Syntax

```sql
FUNCTION() OVER (
    PARTITION BY column
    ORDER BY column
)
```

---

# Important Keywords

| Keyword | Purpose |
|---|---|
| PARTITION BY | Creates logical groups |
| ORDER BY | Defines row order |
| OVER() | Defines the window |

---

# Step 1 — Create Sample Table

```sql
CREATE TABLE employees (
    emp_id INT,
    name STRING,
    dept STRING,
    salary INT,
    joining_date DATE
)
USING DELTA;
```

---

# Step 2 — Insert Sample Data

```sql
INSERT INTO employees VALUES
(1, 'Ravi', 'IT', 50000, '2024-01-10'),
(2, 'Arjun', 'IT', 70000, '2024-03-15'),
(3, 'Kiran', 'IT', 65000, '2024-04-01'),
(4, 'John', 'HR', 40000, '2024-02-12'),
(5, 'David', 'HR', 45000, '2024-05-01');
```

---

# Step 3 — View Data

```sql
SELECT * FROM employees;
```

---

# 1. ROW_NUMBER()

Assigns a unique sequence number to each row.

---

# Example

```sql
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY dept
           ORDER BY salary DESC
       ) AS rn
FROM employees;
```

---

# Output

| dept | salary | rn |
|---|---|---|
| IT | 70000 | 1 |
| IT | 65000 | 2 |
| IT | 50000 | 3 |
| HR | 45000 | 1 |
| HR | 40000 | 2 |

---

# Real-Time Use Case

Most common usage:

- Deduplication
- Latest record extraction

---

# Deduplication Example

```sql
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY emp_id
               ORDER BY joining_date DESC
           ) AS rn
    FROM employees
)

SELECT *
FROM ranked
WHERE rn = 1;
```

---

# 2. RANK()

Assigns rank with gaps.

---

# Example

```sql
SELECT *,
       RANK() OVER (
           PARTITION BY dept
           ORDER BY salary DESC
       ) AS rank_num
FROM employees;
```

---

# Example Output

| salary | rank |
|---|---|
| 70000 | 1 |
| 65000 | 2 |
| 65000 | 2 |
| 50000 | 4 |

Notice:

- Rank 3 is skipped

---

# 3. DENSE_RANK()

Same as RANK but without gaps.

---

# Example

```sql
SELECT *,
       DENSE_RANK() OVER (
           PARTITION BY dept
           ORDER BY salary DESC
       ) AS dense_rank_num
FROM employees;
```

---

# Output

| salary | dense_rank |
|---|---|
| 70000 | 1 |
| 65000 | 2 |
| 65000 | 2 |
| 50000 | 3 |

---

# Difference Between ROW_NUMBER vs RANK vs DENSE_RANK

| Function | Duplicate Values | Gaps |
|---|---|---|
| ROW_NUMBER | Unique number | No |
| RANK | Same rank | Yes |
| DENSE_RANK | Same rank | No |

---

# 4. LEAD()

Gets next row value.

---

# Example

```sql
SELECT *,
       LEAD(salary) OVER (
           PARTITION BY dept
           ORDER BY salary
       ) AS next_salary
FROM employees;
```

---

# Use Cases

- Compare current vs next row
- Trend analysis

---

# 5. LAG()

Gets previous row value.

---

# Example

```sql
SELECT *,
       LAG(salary) OVER (
           PARTITION BY dept
           ORDER BY salary
       ) AS previous_salary
FROM employees;
```

---

# Use Cases

- Historical comparisons
- Growth calculations

---

# 6. Running Total

---

# Example

```sql
SELECT *,
       SUM(salary) OVER (
           PARTITION BY dept
           ORDER BY joining_date
       ) AS running_total
FROM employees;
```

---

# Output

| salary | running_total |
|---|---|
| 50000 | 50000 |
| 70000 | 120000 |
| 65000 | 185000 |

---

# 7. AVG() Over Window

Calculates department-wise average salary.

---

# Example

```sql
SELECT *,
       AVG(salary) OVER (
           PARTITION BY dept
       ) AS avg_salary
FROM employees;
```

---

# 8. FIRST_VALUE()

Gets the first value in the partition.

---

# Example

```sql
SELECT *,
       FIRST_VALUE(salary) OVER (
           PARTITION BY dept
           ORDER BY salary DESC
       ) AS highest_salary
FROM employees;
```

---

# 9. LAST_VALUE()

Gets the last value in the partition.

---

# Example

```sql
SELECT *,
       LAST_VALUE(salary) OVER (
           PARTITION BY dept
           ORDER BY salary
       ) AS last_salary
FROM employees;
```

---

