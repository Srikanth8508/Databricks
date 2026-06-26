
## What is `COPY INTO`?

`COPY INTO` is used to incrementally load files from cloud storage or volumes into a Delta table.

**Key features:**

* ✅ Automatically tracks previously loaded files.
* ✅ Loads only **new files** by default.
* ✅ Supports multiple file formats such as **CSV, JSON, PARQUET, AVRO, ORC, and TEXT**.

---

# 1. Handling Multiple Files Automatically (Production Scenario)

In real projects, you **don't know the names of future files**. New files arrive continuously, so you simply point `COPY INTO` to a folder.

## Initial Folder Structure

```text
/Volumes/main/raw/employees/

Day 1
├── emp1.csv
├── emp2.csv
```

## COPY INTO Command

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

PATTERN = '.*\.csv';
```

Notice that **`FILES` is not specified**.

---

## First Execution

Databricks scans the folder.

| File     | Already Loaded? | Action |
| -------- | --------------- | ------ |
| emp1.csv | No              | ✅ Load |
| emp2.csv | No              | ✅ Load |

Databricks internally records:

```text
Loaded Files
-------------
emp1.csv
emp2.csv
```

---

## Day 2 - New File Arrives

```text
/Volumes/main/raw/employees/

emp1.csv
emp2.csv
emp3.csv
```

Run the **same `COPY INTO` command**.

| File     | Already Loaded? | Action  |
| -------- | --------------- | ------- |
| emp1.csv | Yes             | ⏭️ Skip |
| emp2.csv | Yes             | ⏭️ Skip |
| emp3.csv | No              | ✅ Load  |

Only **`emp3.csv`** is processed.

---

## Day 3 - More Files Arrive

```text
/Volumes/main/raw/employees/

emp1.csv
emp2.csv
emp3.csv
emp4.csv
emp5.csv
```

Run the **same command again**.

| File     | Already Loaded? | Action  |
| -------- | --------------- | ------- |
| emp1.csv | Yes             | ⏭️ Skip |
| emp2.csv | Yes             | ⏭️ Skip |
| emp3.csv | Yes             | ⏭️ Skip |
| emp4.csv | No              | ✅ Load  |
| emp5.csv | No              | ✅ Load  |

Only the newly arrived files are loaded.

---

# 2. Forcing Reload of Previously Loaded Files

Normally, Databricks skips files it has already processed.

If you want to **reload all files**, use:

```sql
COPY INTO employee_data
FROM '/Volumes/main/raw/employees/'
FILEFORMAT = CSV

COPY_OPTIONS (
  'force' = 'true'
);
```

## Example

Current folder:

```text
emp1.csv
emp2.csv
emp3.csv
```

Result:

| File     | Action         |
| -------- | -------------- |
| emp1.csv | ✅ Loaded Again |
| emp2.csv | ✅ Loaded Again |
| emp3.csv | ✅ Loaded Again |

⚠️ This may create duplicate records because `COPY INTO` appends data.

---

# 3. Understanding `FORMAT_OPTIONS`

## `header = 'true'`

Treats the first row as column names.

### Input

```csv
id,name,department,salary
1,Alice,HR,50000
2,Bob,IT,65000
```

### Loaded Data

| id | name  | department | salary |
| -- | ----- | ---------- | ------ |
| 1  | Alice | HR         | 50000  |
| 2  | Bob   | IT         | 65000  |

---

## `delimiter = ','`

Defines the column separator.

### Input

```text
1,Alice,HR,50000
```

If your file uses `|` instead:

```text
1|Alice|HR|50000
```

Use:

```sql
FORMAT_OPTIONS (
  'delimiter' = '|'
)
```

---

## `quote = '"'`

Allows commas inside quoted values.

### Input

```csv
1,"John, Smith",IT
```

Without `quote`, `John, Smith` would be split into two columns.

With `quote`, it is stored correctly as:

| id | name        | department |
| -- | ----------- | ---------- |
| 1  | John, Smith | IT         |

---

## `escape = '\\'`

Handles escaped quotes inside strings.

### Input

```csv
1,"He said \"Hello\""
```

Stored value:

```text
He said "Hello"
```

---

# 4. When to Use `FILES`

Normally, **do not use `FILES`** in production because you don't know future filenames.

However, if you want to load only selected files:

```sql
COPY INTO employee_data
FROM '/Volumes/main/raw/employees/'
FILEFORMAT = CSV

FILES = (
  'emp10.csv',
  'emp25.csv'
);
```

Only these files are processed.

Use cases:

* Testing
* Reprocessing specific files
* One-time manual loads

---

# 5. Summary Table

| Feature         | Purpose                                | Example                                 |            |
| --------------- | -------------------------------------- | --------------------------------------- | ---------- |
| `FROM`          | Source folder                          | `/Volumes/main/raw/employees/`          |            |
| `FILEFORMAT`    | Input file type                        | `CSV`, `JSON`, `PARQUET`, `AVRO`, `ORC` |            |
| `header`        | Ignore header row                      | `'header'='true'`                       |            |
| `delimiter`     | Column separator                       | `','`, `'                               | '`, `'\t'` |
| `quote`         | Handle delimiters inside quoted values | `"John, Smith"`                         |            |
| `escape`        | Handle escaped quotes                  | `He said \"Hello\"`                     |            |
| `PATTERN`       | Filter matching files                  | `'.*\.csv'`                             |            |
| `FILES`         | Load only named files                  | `('emp1.csv','emp2.csv')`               |            |
| `force='false'` | Skip already loaded files              | Default behavior                        |            |
| `force='true'`  | Reload previously loaded files         | May create duplicates                   |            |

---

