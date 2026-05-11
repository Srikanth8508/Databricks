# Create and Read CSV File in Azure Storage using Databricks

# Step 1 — Create CSV File Using Python

```python
%python

# Define storage path
test_path = "abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/dataset_bookstore/books-csv/"

# Create directory if not exists
dbutils.fs.mkdirs(test_path)

# Sample CSV content
csv_content = """id;title;author;price
1;The Great Gatsby;F. Scott Fitzgerald;10.99
2;1984;George Orwell;8.99
3;The Hobbit;J.R.R. Tolkien;15.00"""

# Create CSV file
dbutils.fs.put(
    f"{test_path}export_test_01.csv",
    csv_content,
    overwrite=True
)

print(f"Test file created at: {test_path}export_test_01.csv")
```

---

# What This Code Does

| Operation | Purpose |
|---|---|
| dbutils.fs.mkdirs() | Creates directory |
| dbutils.fs.put() | Writes file into storage |
| overwrite=True | Replaces file if already exists |

---

# Step 2 — Read CSV File Using SQL

```sql
SELECT *
FROM read_files(
    'abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/dataset_bookstore/books-csv/export_*.csv',
    format => 'csv',
    header => 'true',
    delimiter => ';'
);
```

---

# Explanation of read_files()

| Parameter | Purpose |
|---|---|
| format => 'csv' | Reads CSV files |
| header => 'true' | First row treated as column names |
| delimiter => ';' | Uses semicolon separator |
| export_*.csv | Reads multiple matching files |

---

# Expected Output

| id | title | author | price |
|---|---|---|---|
| 1 | The Great Gatsby | F. Scott Fitzgerald | 10.99 |
| 2 | 1984 | George Orwell | 8.99 |
| 3 | The Hobbit | J.R.R. Tolkien | 15.00 |

---
