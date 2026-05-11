# Higher Order Functions in Delta Lake / Apache Spark — Complete Guide

# Sample Dataset

# Step 1 — Create Sample Table

```sql
CREATE TABLE orders (
    order_id INT,
    items ARRAY<INT>
)
USING DELTA;
```

---

# Step 2 — Insert Sample Data

```sql
INSERT INTO orders VALUES
(1, array(10,20,30)),
(2, array(5,15,25));
```

---

# Step 3 — View Data

```sql
SELECT * FROM orders;
```

---

# 1. transform()

Transforms each array element.

---

# Example — Multiply Every Value by 2

```sql
SELECT
    order_id,
    transform(
        items,
        x -> x * 2
    ) AS doubled_items
FROM orders;
```

---

# Output

| order_id | doubled_items |
|---|---|
| 1 | [20,40,60] |
| 2 | [10,30,50] |

---

# Understanding Syntax

```text
x -> x * 2
```

Meaning:

- Take each array element
- Apply transformation logic
- Return modified array


---

# 2. filter()

Filters array values based on condition.

---

# Example — Keep Values Greater Than 15

```sql
SELECT
    order_id,
    filter(
        items,
        x -> x > 15
    ) AS filtered_items
FROM orders;
```

---

# Output

| order_id | filtered_items |
|---|---|
| 1 | [20,30] |
| 2 | [25] |

---

# 3. exists()

Checks if ANY element satisfies a condition.

---

# Example

```sql
SELECT
    order_id,
    exists(
        items,
        x -> x > 25
    ) AS has_large_value
FROM orders;
```

---

# Output

| order_id | has_large_value |
|---|---|
| 1 | true |
| 2 | false |

---

# Meaning

```text
Does the array contain any value > 25?
```

---

# 4. forall()

Checks if ALL elements satisfy a condition.

---

# Example

```sql
SELECT
    order_id,
    forall(
        items,
        x -> x > 0
    ) AS all_positive
FROM orders;
```

---

# Output

| order_id | all_positive |
|---|---|
| 1 | true |
| 2 | true |

---


# 5. aggregate()

Reduces array into a single value.

---

# Example — Sum All Values

```sql
SELECT
    order_id,
    aggregate(
        items,
        0,
        (acc, x) -> acc + x
    ) AS total
FROM orders;
```

---

# Output

| order_id | total |
|---|---|
| 1 | 60 |
| 2 | 45 |

---

# Understanding aggregate()

| Parameter | Meaning |
|---|---|
| items | Input array |
| 0 | Initial accumulator |
| (acc, x) | Aggregation logic |

---


# 6. zip_with()

Combines two arrays element by element.

---

# Example

```sql
SELECT
    zip_with(
        array(1,2,3),
        array(10,20,30),
        (x,y) -> x + y
    ) AS result;
```

---

# Output

```text
[11,22,33]
```

---

# 7. sort_array()

Sorts array values.

---

# Ascending Sort

```sql
SELECT
    sort_array(items)
FROM orders;
```

---

# Descending Sort

```sql
SELECT
    sort_array(items, false)
FROM orders;
```

---

# 8. array_distinct()

Removes duplicate array elements.

---

# Example

```sql
SELECT
    array_distinct(array(1,2,2,3));
```

---

# Output

```text
[1,2,3]
```

---

# 9. array_contains()

Checks if array contains a specific value.

---

# Example

```sql
SELECT
    array_contains(items, 20)
FROM orders;
```

---

# 10. flatten()

Flattens nested arrays into single array.

---

# Example

```sql
SELECT
    flatten(array(
        array(1,2),
        array(3,4)
    ));
```

---

# Output

```text
[1,2,3,4]
```

---
