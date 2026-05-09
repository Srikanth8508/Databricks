````md
# End-to-End Setup: Azure Databricks + External Blob/ADLS Storage + Delta Tables

## Goal

Create a complete Azure Databricks environment that:

- Creates a Databricks Workspace and Compute Cluster
- Configures external storage using Azure Blob Storage / ADLS Gen2
- Connects Databricks securely to external storage
- Creates Delta tables in external storage
- Stores logs and table data in external storage
- Configures required access permissions and authentication
- Verifies connectivity and storage access

---

# Architecture Overview

## Components Used

| Component | Purpose |
|---|---|
| Azure Storage Account (ADLS Gen2) | External storage layer |
| Blob Containers | Store raw data and Delta tables |
| Microsoft Entra ID App Registration | Authentication |
| Service Principal | Secure Databricks access |
| Azure Databricks Workspace | Data engineering platform |
| Databricks Cluster | Compute engine |
| Unity Catalog External Location | Governed external storage access |
| Delta Lake | Storage format for tables |

---

# Phase 1: Azure Resource Setup

# Step 1: Create Resource Group

1. Go to Azure Portal
2. Search for **Resource Groups**
3. Click **Create**
4. Enter:

| Field | Example |
|---|---|
| Resource Group Name | databricks-rg |
| Region | East US / Central India |

5. Click **Review + Create**
6. Click **Create**

---

# Step 2: Create Storage Account (ADLS Gen2)

1. Go to Azure Portal
2. Search for **Storage Accounts**
3. Click **Create**

## Basic Configuration

| Field | Example |
|---|---|
| Subscription | Your Azure Subscription |
| Resource Group | databricks-rg |
| Storage Account Name | databricksdat |
| Region | Same as Databricks |
| Performance | Standard |
| Redundancy | LRS |

## Important Configuration

Go to the **Advanced** tab.

Enable:

✅ Enable hierarchical namespace

This converts Blob Storage into:

## Azure Data Lake Storage Gen2 (ADLS Gen2)

This is mandatory for:

- Delta Lake
- Unity Catalog
- Efficient Spark reads/writes
- ACID transactions

4. Click **Review + Create**
5. Click **Create**

---

# Step 3: Create Containers

Once the storage account is created:

1. Open the Storage Account
2. Go to:

## Data Storage → Containers

3. Create the following containers:

| Container | Purpose |
|---|---|
| raw-data | Source/raw files |
| delta-lake | Delta tables |
| logs | Spark and application logs |
| checkpoints | Streaming checkpoints |

---

# Step 4: Upload Sample Data

1. Open the `raw-data` container
2. Click **Upload**
3. Upload a sample CSV file

Example:

```csv
id,product,price
1,Laptop,50000
2,Mouse,1000
3,Keyboard,2500
````

---

# Phase 2: Security & Authentication

# Why Authentication is Required

Databricks must securely access Azure Storage.

Recommended methods:

| Method            | Recommended For        |
| ----------------- | ---------------------- |
| Service Principal | Learning & Development |
| Managed Identity  | Production             |

---

# Option 1: Service Principal Authentication

# Step 1: Create App Registration

1. Go to Azure Portal
2. Search:

## Microsoft Entra ID

3. Go to:

## App Registrations → New Registration

4. Enter:

| Field | Example                   |
| ----- | ------------------------- |
| Name  | databricks-storage-access |

5. Click **Register**

---

# Step 2: Copy Important Values

After registration, copy:

| Value                   | Usage     |
| ----------------------- | --------- |
| Application (client) ID | Client ID |
| Directory (tenant) ID   | Tenant ID |

Save them securely.

---

# Step 3: Create Client Secret

1. Open your App Registration
2. Go to:

## Certificates & Secrets

3. Click:

## New Client Secret

4. Add description and expiration
5. Click **Add**
6. Copy the **Secret Value** immediately

⚠️ Important:

You cannot view the secret again after leaving the page.

---

# Step 4: Grant Storage Permissions

Go to:

## Storage Account → Access Control (IAM)

1. Click:

## Add → Add Role Assignment

2. Select Role:

## Storage Blob Data Contributor

3. Click **Next**

4. Assign access to:

## User, group, or service principal

5. Select your App Registration
6. Click:

## Review + Assign

---

# Additional Recommended Access Roles

| Role                          | Purpose                         |
| ----------------------------- | ------------------------------- |
| Storage Blob Data Contributor | Read/write storage data         |
| Reader                        | Read storage metadata           |
| Contributor                   | Optional full management access |

---

# Phase 3: Azure Databricks Setup

# Step 1: Create Databricks Workspace

1. Go to Azure Portal
2. Search:

## Azure Databricks

3. Click **Create**

## Configure Workspace

| Field          | Example                 |
| -------------- | ----------------------- |
| Workspace Name | databricks-demo         |
| Resource Group | databricks-rg           |
| Region         | Same as Storage Account |
| Pricing Tier   | Standard or Premium     |

4. Click:

## Review + Create

5. Click **Create**

---

# Step 2: Launch Workspace

1. Open Databricks Workspace
2. Click:

## Launch Workspace

---

# Step 3: Create Compute Cluster

1. In Databricks:

## Compute → Create Compute

2. Configure:

| Field            | Example         |
| ---------------- | --------------- |
| Cluster Name     | ETL-Cluster     |
| Runtime          | 14.3 LTS        |
| Node Type        | Standard_DS3_v2 |
| Auto Termination | 20 Minutes      |

3. Click:

## Create Compute

---

# Common Cluster Permission Issues

## Error: Insufficient CPU Quota

Possible fixes:

* Increase Azure VM quota
* Use smaller VM sizes
* Change Azure region
* Stop unused VMs

---

# Phase 4: Configure Storage Access in Databricks

# Step 1: Create Notebook

1. Go to:

## Workspace → New → Notebook

2. Attach notebook to cluster
3. Select Python language

---

# Step 2: Configure OAuth Authentication

```python
storage_account_name = "databricksdat"
client_id = "<your-client-id>"
tenant_id = "<your-tenant-id>"
client_secret = "<your-client-secret>"

# Remove conflicting configs
spark.conf.unset(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net"
)

# OAuth Authentication
spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net",
    "OAuth"
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net",
    client_id
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net",
    client_secret
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

print("Authentication Configured Successfully")
```

---

# Step 3: Verify Storage Connectivity

```python
dbutils.fs.ls(
    "abfss://raw-data@databricksdat.dfs.core.windows.net/"
)
```

Expected Result:

You should see uploaded files.

---

# Phase 5: Create External Delta Table

# Step 1: Read CSV File

```python
sales_df = spark.read.format("csv") \
    .option("header", "true") \
    .load("abfss://raw-data@databricksdat.dfs.core.windows.net/sales.csv")

sales_df.show()
```

---

# Step 2: Write Data as Delta Table

```python
sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://delta-lake@databricksdat.dfs.core.windows.net/sales_delta")
```

---

# Step 3: Create External Table in Databricks

```sql
CREATE TABLE sales_external
USING DELTA
LOCATION 'abfss://delta-lake@databricksdat.dfs.core.windows.net/sales_delta';
```

---

# Step 4: Query the Table

```sql
SELECT * FROM sales_external;
```

---

# Phase 6: Configure Spark Logs in External Storage

# Why Store Logs Externally?

Benefits:

* Centralized monitoring
* Persistent logs after cluster termination
* Easier debugging
* Compliance and auditing

---

# Configure Cluster Log Delivery

1. Go to:

## Compute → Your Cluster → Edit

2. Open:

## Advanced Options → Logging

3. Enable:

## Cluster Log Delivery

4. Provide external storage path:

```text
abfss://logs@databricksdat.dfs.core.windows.net/cluster-logs/
```

5. Save cluster configuration
6. Restart cluster

---

# Phase 7: Unity Catalog External Location Setup

# Step 1: Create Access Connector

In Azure Portal:

1. Search:

## Access Connector for Azure Databricks

2. Click **Create**

3. Configure:

| Field          | Example                     |
| -------------- | --------------------------- |
| Name           | databricks-access-connector |
| Resource Group | databricks-rg               |
| Region         | Same as Databricks          |

4. Click:

## Review + Create

5. Click **Create**

---

# Step 2: Copy Access Connector Resource ID

Go to:

## Access Connector → Properties

Copy:

## Resource ID

Example:

```text
/subscriptions/xxxx/resourceGroups/xxxx/providers/Microsoft.Databricks/accessConnectors/xxxx
```

---

# Step 3: Grant Access Connector Permissions

Go to:

## Storage Account → Access Control (IAM)

Assign Role:

## Storage Blob Data Contributor

Assign access to:

## Managed Identity

Select:

## Your Access Connector

---

# Step 4: Create Storage Credential in Databricks

In Databricks:

1. Go to:

## Catalog → External Data → Storage Credentials

2. Click:

## Add Credential

3. Configure:

| Field               | Example                       |
| ------------------- | ----------------------------- |
| Credential Name     | databricks-storage-credential |
| Authentication Type | Azure Managed Identity        |
| Access Connector ID | Paste Resource ID             |

4. Click **Create**

---

# Step 5: Create External Location

Go to:

## Catalog → External Data → External Locations

Click:

## Create External Location

Configure:

| Field                  | Example                                                                                                  |
| ---------------------- | -------------------------------------------------------------------------------------------------------- |
| External Location Name | external-delta                                                                                           |
| URL                    | abfss://delta-[lake@databricksdat.dfs.core.windows.net](mailto:lake@databricksdat.dfs.core.windows.net)/ |
| Storage Credential     | databricks-storage-credential                                                                            |

Click:

## Create

Then:

## Test Connection

---


# Common Errors & Fixes

| Error                                | Cause                            | Fix                                  |
| ------------------------------------ | -------------------------------- | ------------------------------------ |
| AuthorizationPermissionMismatch      | Missing IAM role                 | Assign Storage Blob Data Contributor |
| Invalid configuration value detected | Wrong OAuth config               | Verify tenant/client/secret          |
| Cluster won't start                  | CPU quota issue                  | Increase Azure quota                 |
| Path does not exist                  | Wrong container path             | Verify abfss URL                     |
| Permission denied                    | Service principal missing access | Reassign IAM role                    |

---

# Important Storage URL Format

```text
abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
```

Example:

```text
abfss://delta-lake@databricksdat.dfs.core.windows.net/sales_delta
```

---

