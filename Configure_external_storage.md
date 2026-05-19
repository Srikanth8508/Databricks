# Azure Databricks + External Blob Storage + Delta Lake

# Phase 1: Azure Resource Setup

---

# Step 1: Create Resource Group

## Steps

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

# Step 2: Create Storage Account (ADLS Gen2)

## Steps

1. Search for **Storage Accounts**
2. Click **Create**

---

## Basic Configuration

| Field | Example |
|---|---|
| Resource Group | databricks-rg |
| Storage Account Name | databricksdat |
| Region | Central India |
| Performance | Standard |
| Redundancy | LRS |

---

## Important Configuration

Go to the **Advanced** tab.

Enable:

```text
Enable Hierarchical Namespace
```

This converts the storage account into:

## Azure Data Lake Storage Gen2 (ADLS Gen2)

Required for:

- Delta Lake
- Unity Catalog
- Spark Optimizations
- ACID Transactions

Click:

- Review + Create
- Create

---

# Step 3: Create Containers

## Navigation

```text
Storage Account
      ↓
Data Storage
      ↓
Containers
```

---

## Create Containers

| Container Name | Purpose |
|---|---|
| raw-data | Store raw files |
| delta-lake | Store Delta tables |
| logs | Store cluster logs |
| checkpoints | Streaming checkpoints |

---

# Step 4: Upload Sample CSV File

## Steps

1. Open the `raw-data` container
2. Click **Upload**
3. Upload sample CSV file

---

## Example CSV

```csv
id,product,price
1,Laptop,50000
2,Mouse,1000
3,Keyboard,2500
```

---

# Phase 2: Configure Authentication & IAM Access

---

# Why Authentication is Required

Databricks requires secure access to Azure Storage.

Recommended authentication methods:

| Method | Usage |
|---|---|
| Service Principal | Learning / Development |
| Managed Identity | Production |

---

# Step 1: Create App Registration

## Navigation

```text
Azure Portal
      ↓
Microsoft Entra ID
      ↓
App Registrations
      ↓
New Registration
```

---

## Configuration

| Field | Value |
|---|---|
| Name | databricks-storage-access |

Click:

```text
Register
```

---

# Step 2: Copy Required Values

## Copy the Below Values

| Value | Purpose |
|---|---|
| Application (Client) ID | Client ID |
| Directory (Tenant) ID | Tenant ID |

Save them securely.

---

# Step 3: Create Client Secret

## Navigation

```text
Certificates & Secrets
      ↓
New Client Secret
```

---

## Steps

1. Enter description
2. Choose expiration
3. Click **Add**
4. Copy secret immediately

⚠️ Important:

Once you leave the page, the secret value cannot be viewed again.

---

# Step 4: Assign Storage Permissions

## Navigation

```text
Storage Account
      ↓
Access Control (IAM)
      ↓
Add Role Assignment
```

---

# Required IAM Permissions

## 1. Storage Blob Data Contributor

### Purpose
Allows Databricks to read and write data inside ADLS containers.

| Setting | Value |
|---|---|
| Role | Storage Blob Data Contributor |
| Assign Access To | User, Group, or Service Principal |
| Member | databricks-storage-access |

---

## 2. EventGrid EventSubscription Contributor

### Purpose
Required for Auto Loader file notification mode and Event Grid integrations.

Allows Databricks to create and manage Event Grid subscriptions automatically.

| Setting | Value |
|---|---|
| Role | EventGrid EventSubscription Contributor |
| Assign Access To | User, Group, or Service Principal |
| Member | databricks-storage-access |

---

## 3. Storage Queue Data Contributor

### Purpose
Required for queue-based file notifications and streaming event processing.

Allows Databricks to read and write Azure Queue messages.

| Setting | Value |
|---|---|
| Role | Storage Queue Data Contributor |
| Assign Access To | User, Group, or Service Principal |
| Member | databricks-storage-access |

---

# Recommended IAM Role Combination

| Role | Required |
|---|---|
| Storage Blob Data Contributor | ✅ Yes |
| EventGrid EventSubscription Contributor | ✅ Recommended |
| Storage Queue Data Contributor | ✅ Recommended |

---

# Benefits of Additional Permissions

| Permission | Usage |
|---|---|
| EventGrid EventSubscription Contributor | Auto Loader notification setup |
| Storage Queue Data Contributor | Queue event processing |
| Storage Blob Data Contributor | Read/write storage access |

---

# Phase 3: Create Azure Databricks Workspace

---

# Step 1: Create Workspace

## Navigation

```text
Azure Portal
      ↓
Azure Databricks
      ↓
Create
```

---

## Workspace Configuration

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

# Step 2: Launch Workspace

## Steps

1. Open Databricks Workspace
2. Click:

```text
Launch Workspace
```

---

# Phase 4: Create Databricks Cluster

---

# Step 1: Create Compute

## Navigation

```text
Compute
      ↓
Create Compute
```

---

## Cluster Configuration

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

# Common Cluster Issue

## Error

```text
This account may not have enough CPU cores to start a cluster
```

---

## Fixes

- Increase Azure VM quota
- Use smaller VM size
- Change region
- Stop unused VMs

---

# Phase 5: Configure Storage Access in Databricks

---

# Step 1: Create Notebook

## Navigation

```text
Workspace
      ↓
New
      ↓
Notebook
```

Attach notebook to cluster.

---

# Step 2: Configure OAuth Authentication

## Command

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

# Step 3: Verify Storage Connectivity

## Command

```python
dbutils.fs.ls(
    "abfss://raw-data@databricksdat.dfs.core.windows.net/"
)
```

---

## Expected Result

Uploaded files should be visible.

---

# Phase 6: Create Delta Table in External Storage

---

# Step 1: Read CSV File

## Command

```python
sales_df = spark.read.format("csv") \
    .option("header", "true") \
    .load(
        "abfss://raw-data@databricksdat.dfs.core.windows.net/sales.csv"
    )

sales_df.show()
```

---

# Step 2: Write Data as Delta Table

## Command

```python
sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://delta-lake@databricksdat.dfs.core.windows.net/sales_delta"
    )
```

---

# Step 3: Create External Delta Table

## Command

```sql
CREATE TABLE sales_external
USING DELTA
LOCATION 'abfss://delta-lake@databricksdat.dfs.core.windows.net/sales_delta';
```

---

# Step 4: Query External Table

## Command

```sql
SELECT * FROM sales_external;
```

---

# Phase 7: Configure Cluster Log Delivery

---

# Why External Logs?

Benefits:

- Centralized monitoring
- Persistent logs
- Easier debugging
- Compliance tracking

---

# Configure Cluster Log Delivery

## Navigation

```text
Compute
      ↓
Cluster
      ↓
Edit
      ↓
Advanced Options
      ↓
Logging
```

Enable:

```text
Cluster Log Delivery
```

---

## Log Path

```text
abfss://logs@databricksdat.dfs.core.windows.net/cluster-logs/
```

Save and restart cluster.

---

# Phase 8: Unity Catalog External Location Setup

---

# Step 1: Create Access Connector

## Navigation

```text
Azure Portal
      ↓
Access Connector for Azure Databricks
```

---

## Configuration

| Field | Example |
|---|---|
| Name | databricks-access-connector |
| Resource Group | databricks-rg |
| Region | Central India |

---

# Step 2: Copy Resource ID

## Navigation

```text
Access Connector
      ↓
Properties
```

Copy:

```text
Resource ID
```

---

# Step 3: Assign Access Connector Permission

## Navigation

```text
Storage Account
      ↓
Access Control (IAM)
```

---

## Assign Role

```text
Storage Blob Data Contributor
```

Assign access to:

```text
Managed Identity
```

Select Access Connector.

---

# Step 4: Create Storage Credential

## Navigation

```text
Catalog
      ↓
External Data
      ↓
Storage Credentials
```

Click:

```text
Add Credential
```

---

## Configuration

| Field | Value |
|---|---|
| Credential Name | databricks-storage-credential |
| Authentication Type | Azure Managed Identity |
| Access Connector ID | Paste Resource ID |

---

# Step 5: Create External Location

## Navigation

```text
Catalog
      ↓
External Data
      ↓
External Locations
```

---

## Configuration

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

---

## Example

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
