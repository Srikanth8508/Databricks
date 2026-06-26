# Databricks `COPY INTO` – End-to-End Example

## Scenario

Assume you receive employee data files every day in the following location:

```text
/Volumes/main/raw/employees/
│
├── emp1.csv
├── emp2.csv
├── emp3.csv
├── customers.json
└── readme.txt
```

You want to load only `emp1.csv` and `emp2.csv` into a Delta table.

---

# Step 1: Create the Target Table

```sql
CREATE TABLE employee_data (
    id INT,
    name STRING,
    department STRING,
    salary DOUBLE
);
```

Initially, the table is empty.

| id          | name | department | salary |
| ----------- | ---- | ---------- | ------ |
| *(no rows)* |      |            |        |

---

# Step 2: Input Files

## emp1.csv

```csv
id,name,department,salary
1,Alice,HR,50000
2,Bob,IT,65000
```

## emp2.csv

```csv
id,name,department,salary
3,Charlie,Finance,70000
4,David,Sales,55000
```

## emp3.csv

```csv
id,name,department,salary
5,Eve,Marketing,60000
```

---

# Step 3: Run `COPY INTO`

```sql
COPY INTO employee_data
FROM '/Volumes/main/raw/employees/'
FILEFORMAT = CSV

FORMAT_OPTIONS (
  'header' = 'true',
  'delimiter' = ',',
  'quote' = '"',
  'escape' = '\\'
)

COPY_OPTIONS (
  'mergeSchema' = 'true'
)

PATTERN = '.*\.csv'

FILES = ('emp1.csv', 'emp2.csv');
```

---

# How Databricks Processes the Command

## 1. `FROM`

```sql
FROM '/Volumes/main/raw/employees/'
```

Databricks looks inside this folder for source files.

It finds:

```text
emp1.csv
emp2.csv
emp3.csv
customers.json
readme.txt
```

---

## 2. `FILEFORMAT = CSV`

Databricks expects the selected files to be CSV files and parses them accordingly.

---

## 3. `PATTERN = '.*\.csv'`

This regular expression keeps only files ending in `.csv`.

Files considered after applying the pattern:

```text
✓ emp1.csv
✓ emp2.csv
✓ emp3.csv

✗ customers.json
✗ readme.txt
```

---

## 4. `FILES = ('emp1.csv', 'emp2.csv')`

Even though `emp3.csv` matches the pattern, only these two files are loaded:

```text
✓ emp1.csv
✓ emp2.csv

✗ emp3.csv (ignored because it is not listed)
```

---

## 5. `FORMAT_OPTIONS`

```sql
'header' = 'true'
```

The first line is treated as column names:

```text
id,name,department,salary
```

and is **not** loaded as data.

```sql
'delimiter' = ','
```

Commas separate individual columns.

```text
1,Alice,HR,50000
```

becomes

| id | name  | department | salary |
| -- | ----- | ---------- | ------ |
| 1  | Alice | HR         | 50000  |

```sql
'quote' = '"'
```

Allows commas inside quoted values.

Example:

```csv
1,"John, Smith",IT,60000
```

`"John, Smith"` is treated as a single field.

```sql
'escape' = '\\'
```

Allows escaped quotes inside quoted text.

Example:

```csv
1,"He said \"Hello\"",IT,60000
```

The resulting value stored is:

```text
He said "Hello"
```

---

## 6. `COPY_OPTIONS ('mergeSchema' = 'true')`

If a future file contains an extra column such as `location`:

```csv
id,name,department,salary,location
6,Ravi,IT,70000,Chennai
```

Databricks can evolve the target schema to include the new column instead of failing.

---

# Final Output Table

After processing `emp1.csv` and `emp2.csv`, the `employee_data` table contains:

| id | name    | department | salary |
| -- | ------- | ---------- | ------ |
| 1  | Alice   | HR         | 50000  |
| 2  | Bob     | IT         | 65000  |
| 3  | Charlie | Finance    | 70000  |
| 4  | David   | Sales      | 55000  |

Notice that:

* `emp3.csv` was **not loaded** because it was not listed in `FILES`.
* `customers.json` was **ignored** because it did not match the `PATTERN`.
* `readme.txt` was **ignored** because it was neither a CSV file nor matched the pattern.

---

# What Happens if You Run the Same Command Again?

`COPY INTO` tracks which source files have already been successfully ingested.

If you rerun the same command without changing the files:

```text
emp1.csv  → Already loaded → Skipped
emp2.csv  → Already loaded → Skipped
```

No duplicate rows are inserted.

If a new file `emp4.csv` appears in the folder and matches your filters, only that new file will be loaded.
