# End-to-End Setup: Azure Databricks + External Blob Storage + Delta Lake

## Goal

The goal of this setup is to:

- Create an Azure Databricks Workspace and Compute Cluster
- Configure external storage using Azure Blob Storage / ADLS Gen2
- Connect Databricks securely with external storage
- Create Delta tables in external storage
- Store logs in external storage
- Configure IAM access and authentication
- Verify storage connectivity from Databricks

---

# Architecture Overview

| Component | Purpose |
|---|---|
| Azure Storage Account (ADLS Gen2) | External Storage |
| Blob Containers | Store raw data and Delta tables |
| Microsoft Entra ID | Authentication |
| Service Principal | Secure access to storage |
| Azure Databricks | Data Engineering Platform |
| Databricks Cluster | Compute Engine |
| Delta Lake | ACID Storage Format |
| Unity Catalog | Governance and External Locations |

---

# Phase 1: Azure Resource Setup

## Step 1: Create Resource Group

1. Open Azure Portal
2. Search for **Resource Groups**
3. Click **Create**
4. Enter the below details

| Field | Value |
|---|---|
| Resource Group | databricks-rg |
| Region | Central India |

5. Click **Review + Create**
6. Click **Create**

---

## Step 2: Create Storage Account (ADLS Gen2)

1. Search for **Storage Accounts**
2. Click **Create**

### Basic Configuration

| Field | Example |
|---|---|
| Resource Group | databricks-rg |
| Storage Account Name | databricksdat |
| Region | Central India |
| Performance | Standard |
| Redundancy | LRS |

---

### Important Configuration

Go to the **Advanced** tab.

Enable:

✅ **Enable Hierarchical Namespace**

This converts the storage account into:

## Azure Data Lake Storage Gen2 (ADLS Gen2)

This is required for:

- Delta Lake
- Unity Catalog
- Spark Optimizations
- ACID Transactions

Click:

- Review + Create
- Create

---

## Step 3: Create Containers

Once the storage account is created:

1. Open the Storage Account
2. Navigate to:

```text
Data Storage → Containers
```

Create the following containers:

| Container Name | Purpose |
|---|---|
| raw-data | Store raw files |
| delta-lake | Store Delta tables |
| logs | Store cluster logs |
| checkpoints | Streaming checkpoints |

---

## Step 4: Upload Sample CSV File

1. Open the `raw-data` container
2. Click **Upload**
3. Upload a sample CSV file

Example:

```csv
id,product,price
1,Laptop,50000
2,Mouse,1000
3,Keyboard,2500
```

---

# Phase 2: Configure Authentication & Access

## Why Authentication is Required

Databricks requires secure access to Azure Storage.

Recommended authentication methods:

| Method | Usage |
|---|---|
| Service Principal | Learning / Development |
| Managed Identity | Production |

---

## Step 1: Create App Registration

1. Open Azure Portal
2. Search for:

```text
Microsoft Entra ID
```

3. Navigate to:

```text
App Registrations → New Registration
```

4. Enter:

| Field | Value |
|---|---|
| Name | databricks-storage-access |

5. Click **Register**

---

## Step 2: Copy Required Values

After registration, copy:

| Value | Purpose |
|---|---|
| Application (Client) ID | Client ID |
| Directory (Tenant) ID | Tenant ID |

Save them securely.

---

## Step 3: Create Client Secret

1. Open the App Registration
2. Navigate to:

```text
Certificates & Secrets
```

3. Click:

```text
New Client Secret
```

4. Enter description and expiration
5. Click **Add**
6. Copy the secret value immediately

⚠️ Important:

Once you leave the page, the secret value cannot be viewed again.

---

## Step 4: Assign Storage Permissions

Navigate to:

```text
Storage Account → Access Control (IAM)
```

### Add Role Assignment

| Setting | Value |
|---|---|
| Role | Storage Blob Data Contributor |
| Assign Access To | User, Group, or Service Principal |
| Member | databricks-storage-access |

Click:

```text
Review + Assign
```

---

# Phase 3: Create Azure Databricks Workspace

## Step 1: Create Workspace

1. Search for:

```text
Azure Databricks
```

2. Click **Create**

### Workspace Configuration

| Field | Example |
|---|---|
| Workspace Name | databricks-demo |
| Resource Group | databricks-rg |
| Region | Central India |
| Pricing Tier | Standard |

Click:

- Review + Create
- Create

---

## Step 2: Launch Workspace

After deployment:

1. Open the Databricks Workspace
2. Click:

```text
Launch Workspace
```

---

# Phase 4: Create Databricks Cluster

## Step 1: Create Compute

Navigate to:

```text
Compute → Create Compute
```

### Cluster Configuration

| Field | Example |
|---|---|
| Cluster Name | ETL-Cluster |
| Runtime Version | 14.3 LTS |
| Node Type | Standard_DS3_v2 |
| Auto Termination | 20 Minutes |

Click:

```text
Create Compute
```

---

## Common Cluster Issue

### Error

```text
This account may not have enough CPU cores to start a cluster
```

### Fixes

- Increase Azure VM quota
- Use smaller VM size
- Change region
- Stop unused VMs

---

# Phase 5: Configure Storage Access in Databricks

## Step 1: Create Notebook

Navigate to:

```text
Workspace → New → Notebook
```

Attach the notebook to your cluster.

---

## Step 2: Configure OAuth Authentication

```python
storage_account_name = "databricksdat"

client_id = "<client-id>"
tenant_id = "<tenant-id>"
client_secret = "<client-secret>"

# Remove conflicting config
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

## Step 3: Verify Storage Connectivity

```python
dbutils.fs.ls(
    "abfss://raw-data@databricksdat.dfs.core.windows.net/"
)
```

Expected Result:

You should see uploaded files.

---

# Phase 6: Create Delta Table in External Storage

## Step 1: Read CSV File

```python
sales_df = spark.read.format("csv") \
    .option("header", "true") \
    .load(
        "abfss://raw-data@databricksdat.dfs.core.windows.net/sales.csv"
    )

sales_df.show()
```

---

## Step 2: Write Data as Delta Table

```python
sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://delta-lake@databricksdat.dfs.core.windows.net/sales_delta"
    )
```

---

## Step 3: Create External Delta Table

```sql
CREATE TABLE sales_external
USING DELTA
LOCATION 'abfss://delta-lake@databricksdat.dfs.core.windows.net/sales_delta';
```

---

## Step 4: Query the Table

```sql
SELECT * FROM sales_external;
```

---

# Phase 7: Store Cluster Logs in External Storage

## Why External Logs?

Benefits:

- Centralized monitoring
- Persistent logs
- Easier debugging
- Compliance tracking

---

## Configure Cluster Log Delivery

Navigate to:

```text
Compute → Cluster → Edit → Advanced Options → Logging
```

Enable:

```text
Cluster Log Delivery
```

Provide log path:

```text
abfss://logs@databricksdat.dfs.core.windows.net/cluster-logs/
```

Save and restart the cluster.

---

# Phase 8: Unity Catalog External Location Setup

## Step 1: Create Access Connector

Search in Azure Portal:

```text
Access Connector for Azure Databricks
```

Create connector with:

| Field | Example |
|---|---|
| Name | databricks-access-connector |
| Resource Group | databricks-rg |
| Region | Central India |

---

## Step 2: Copy Resource ID

Navigate to:

```text
Access Connector → Properties
```

Copy:

```text
Resource ID
```

Example:

```text
/subscriptions/xxxx/resourceGroups/xxxx/providers/Microsoft.Databricks/accessConnectors/xxxx
```

---

## Step 3: Assign Access Connector Permission

Navigate to:

```text
Storage Account → Access Control (IAM)
```

Assign role:

```text
Storage Blob Data Contributor
```

Assign access to:

```text
Managed Identity
```

Select the Access Connector.

---

## Step 4: Create Storage Credential

In Databricks:

```text
Catalog → External Data → Storage Credentials
```

Click:

```text
Add Credential
```

Configuration:

| Field | Value |
|---|---|
| Credential Name | databricks-storage-credential |
| Authentication Type | Azure Managed Identity |
| Access Connector ID | Paste Resource ID |

Click **Create**

---

## Step 5: Create External Location

Navigate to:

```text
Catalog → External Data → External Locations
```

Configuration:

| Field | Value |
|---|---|
| External Location Name | external-delta |
| URL | abfss://delta-lake@databricksdat.dfs.core.windows.net/ |
| Storage Credential | databricks-storage-credential |

Click:

```text
Create
```

Then:

```text
Test Connection
```

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

# Common Errors & Fixes

| Error | Cause | Fix |
|---|---|---|
| AuthorizationPermissionMismatch | Missing IAM Role | Assign Storage Blob Data Contributor |
| Invalid configuration value | Wrong OAuth config | Verify credentials |
| Cluster not starting | CPU quota issue | Increase Azure quota |
| Path does not exist | Wrong container path | Verify ABFSS path |
| Permission denied | Missing storage access | Reassign IAM role |

---



