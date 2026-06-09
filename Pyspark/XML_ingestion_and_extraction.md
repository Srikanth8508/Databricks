# XML Processing Workflow in PySpark (Patent Dataset)

## Step 1: Load XML File

### Command

```python
df_xml = spark.read \
    .format("xml") \
    .option("rowTag", "us-patent-grant") \
    .load("/data/patent.xml")
```

### Purpose

* Reads the XML file into a Spark DataFrame.
* `rowTag` specifies which XML tag represents a single record.
* Creates a DataFrame containing nested structures.

### Sample Output

```python
df_xml.printSchema()
```

```text
root
 |-- _country: string
 |-- _date-publ: string
 |-- abstract: struct
 |-- claims: struct
 |-- description: struct
 |-- us-bibliographic-data-grant: struct
```

---

## Step 2: View Top-Level Columns

### Command

```python
df_xml.columns
```

### Purpose

Displays all available columns at the root level.

### Sample Output

```python
[
 '_country',
 '_date-produced',
 '_date-publ',
 '_id',
 'abstract',
 'claims',
 'description',
 'us-bibliographic-data-grant'
]
```

### Why?

Before extracting data, you need to know which columns exist.

---

## Step 3: Inspect Nested Structure

### Command

```python
df_xml.select("us-bibliographic-data-grant").printSchema()
```

### Purpose

Displays the schema of a nested column.

### Sample Output

```text
root
 |-- us-bibliographic-data-grant: struct
 |    |-- publication-reference: struct
 |    |    |-- document-id: struct
 |    |    |    |-- doc-number: string
 |    |    |    |-- date: string
 |    |
 |    |-- invention-title: struct
 |    |    |-- _VALUE: string
```

### Why?

XML data is hierarchical. Exploring the schema helps identify field paths.

---

## Step 4: Find the Required Field Path

### Schema Hierarchy

```text
us-bibliographic-data-grant
 └── publication-reference
      └── document-id
           └── doc-number
```

### Full Path

```python
us-bibliographic-data-grant.publication-reference.document-id.doc-number
```

### Another Example

```python
us-bibliographic-data-grant.invention-title._VALUE
```

### Purpose

Determine the complete path to the nested field that needs to be extracted.

---

## Step 5: Extract Required Fields

### Command

```python
patents = df_xml.select(
    "_country",
    "_date-publ",
    "us-bibliographic-data-grant.publication-reference.document-id.doc-number",
    "us-bibliographic-data-grant.invention-title._VALUE"
)
```

### Purpose

Flattens nested XML fields into DataFrame columns.

### Sample Output Schema

```python
patents.printSchema()
```

```text
root
 |-- _country: string
 |-- _date-publ: string
 |-- doc-number: string
 |-- _VALUE: string
```

---

## Step 6: Display Extracted Data

### Command

```python
patents.show(5, truncate=False)
```

### Sample Output

```text
+--------+----------+----------+--------------------------+
|_country|_date-publ|doc-number|_VALUE                    |
+--------+----------+----------+--------------------------+
|US      |20240604  |11999999  |Wireless Charging System  |
|US      |20240604  |12000000  |AI-Based Monitoring Device|
|US      |20240604  |12000001  |Smart Battery Management  |
+--------+----------+----------+--------------------------+
```

### Why?

Validate that the required data has been extracted correctly.

---

## Step 7: Rename Columns (Recommended)

### Command

```python
from pyspark.sql.functions import col

patents = patents.select(
    "_country",
    "_date-publ",
    col("doc-number").alias("patent_number"),
    col("_VALUE").alias("invention_title")
)
```

### Sample Output

```text
+--------+----------+-------------+--------------------------+
|_country|_date-publ|patent_number|invention_title           |
+--------+----------+-------------+--------------------------+
|US      |20240604  |11999999     |Wireless Charging System  |
+--------+----------+-------------+--------------------------+
```

### Why?

Business-friendly column names improve readability and usability.

---

## Step 8: Save the Data

### Save as Delta

```python
patents.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/patents_delta")
```

### Save as CSV

```python
patents.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/mnt/patents_csv")
```

### Purpose

Persist the processed dataset for analytics and reporting.

---

# Quick Reference Workflow

```text
Load XML
    ↓
Check Root Columns (.columns)
    ↓
Inspect Nested Schema (.printSchema())
    ↓
Find Required Field Path
    ↓
Extract Fields (.select())
    ↓
Flatten & Rename Columns
    ↓
Validate Data (.show())
    ↓
Save as Delta / CSV
```

This is the standard workflow followed by Spark and Databricks engineers when processing XML, JSON, and other nested data formats.
