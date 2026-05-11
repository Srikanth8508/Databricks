# Databricks Structured Streaming Commands — Complete Flow Explanation

---

# 1. Create Streaming Input Folder

## Description
Creates a directory in Azure Data Lake Storage for storing streaming JSON files.

This folder acts as the source location where new incoming files will be continuously monitored by Structured Streaming.

## Command
```python
%python
dbutils.fs.mkdirs("abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/streaming/orders")
```

## Purpose
- Creates the input directory for streaming
- Helps Structured Streaming monitor new files automatically

---

# 2. Create Initial JSON Streaming Data

## Description
Creates sample order data and writes it as a JSON file into the streaming source folder.

Structured Streaming treats every newly added file as incoming real-time data.

## Command
```python
%python
data = """
{"order_id":1,"customer":"Ravi","amount":1000}
{"order_id":2,"customer":"Arjun","amount":2500}
{"order_id":3,"customer":"Kiran","amount":500}
"""

dbutils.fs.put(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/streaming/orders/orders1.json",
    data,
    True
)
```

## Purpose
- Simulates real-time incoming order data
- Provides initial files for streaming processing

---

# 3. Define Schema for Streaming Data

## Description
Defines the structure of incoming JSON data using PySpark schema.

Providing schema manually improves streaming performance and avoids automatic schema inference.

## Command
```python
%python
from pyspark.sql.types import *

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer", StringType(), True),
    StructField("amount", IntegerType(), True)
])

print(schema)
```

## Purpose
- Defines column names and data types
- Improves performance and data consistency

---

# 4. Read Streaming Data

## Description
Reads JSON files continuously from the streaming folder using Structured Streaming.

Whenever a new file arrives in the folder, Spark automatically processes it.

## Command
```python
stream_df = spark.readStream \
    .schema(schema) \
    .json("abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/streaming/orders")
```

## Purpose
- Starts reading streaming data continuously
- Creates a streaming DataFrame for processing

---

# 5. Filter Streaming Data

## Description
Filters orders where the amount is greater than 1000.

This transformation processes only high-value orders from the incoming stream.

## Command
```python
filtered_df = stream_df.filter("amount > 1000")
```

## Purpose
- Removes unwanted records
- Processes only required streaming data

---

# 6. Add Tax Column

## Description
Creates a new column called `tax` by calculating 18% of the order amount.

This is a transformation step applied on streaming data before writing output.

## Command
```python
from pyspark.sql.functions import col

final_df = filtered_df.withColumn(
    "tax",
    col("amount") * 0.18
)
```

## Purpose
- Enriches streaming data with calculated values
- Applies business logic during streaming

---

# 7. Write Streaming Data to Delta Table

## Description
Writes the processed streaming data into a Delta table continuously.

Checkpointing is used to track processed files and recover from failures safely.

## Command
```python
query = final_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/checkpoints/orders") \
    .table("stream_orders")
```

## Purpose
- Stores streaming output into Delta Lake
- Enables fault tolerance using checkpoints

---

# 8. Add New Streaming File

## Description
Adds another JSON file into the streaming folder.

Structured Streaming automatically detects this new file and processes it.

## Command
```python
new_data = '''
{"order_id":7,"customer":"Jod","amount":3300}
'''

dbutils.fs.put(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/streaming/orders/orders7.json",
    new_data,
    True
)
```

## Purpose
- Simulates real-time new incoming data
- Tests continuous streaming ingestion

---

# 9. View Streaming Output Table

## Description
Reads data from the Delta table where streaming output is stored.

Used to verify whether streaming data is processed successfully.

## Command
```sql
%sql
SELECT * FROM stream_orders
```

## Purpose
- Displays processed streaming records
- Validates streaming pipeline output

---

# 10. Add Additional Streaming JSON File

## Description
Adds another batch of JSON records into the streaming source folder.

Spark automatically picks up and processes the newly added file.

## Command
```python
new_data = """
{"order_id":4,"customer":"John","amount":3000}
{"order_id":5,"customer":"David","amount":700}
"""

dbutils.fs.put(
    "abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/streaming/orders/orders2.json",
    new_data,
    True
)
```

## Purpose
- Simulates continuous batch arrivals
- Helps test ongoing streaming execution

---

# 11. Read Raw JSON Files Directly

## Description
Reads all JSON files directly from the storage location without streaming.

Used for debugging and verifying source file contents.

## Command
```sql
%sql
SELECT *
FROM json.`abfss://raw-data@databricktesdat.dfs.core.windows.net/tmp/streaming/orders/*.json`
```

## Purpose
- Displays raw source JSON data
- Helps validate input files

---

# 12. Check Active Streaming Queries

## Description
Displays all currently running streaming queries in the Spark session.

Useful for monitoring active Structured Streaming jobs.

## Command
```python
spark.streams.active
```

## Purpose
- Monitors active streaming jobs
- Helps debug streaming execution

---

# 13. Stop All Active Streams

## Description
Stops all currently running streaming queries.

Used to safely terminate streaming pipelines after testing or development.

## Command
```python
for s in spark.streams.active:
    s.stop()
```

## Purpose
- Stops running streaming jobs safely
- Prevents unnecessary resource usage

---
