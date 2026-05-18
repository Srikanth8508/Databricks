# Databricks Delta Sharing — Complete Practice Guide

> Architecture: Unity Catalog → Share → Recipient → Secure Data Access  
> Pattern: Cross Workspace / External Secure Data Sharing

---

# Delta Sharing Architecture

```text
Provider Workspace
(Data Owner)
        ↓

Create Share
        ↓

Add Tables / Views
        ↓

Create Recipient
        ↓

Grant Access
        ↓

Recipient Consumes Shared Data
```

---

# Important Requirement

## Description
Before starting Delta Sharing practice, ensure that:

- Unity Catalog is enabled
- Metastore is attached
- External Delta Sharing is enabled

Without enabling external Delta Sharing, recipients outside the organization cannot access shared data.

---

# 1. Verify Unity Catalog

## Description
Check whether Unity Catalog is enabled in the workspace.

## Command

```sql
SHOW CATALOGS;
```

---

## Example Output

| catalog_name |
|---|
| main |
| system |
| demo |

---

## Interpretation

If catalogs such as:

- `main`
- `system`
- `demo`

exist → Unity Catalog is likely enabled.

---

# 2. Enable External Delta Sharing

## Description
External Delta Sharing must be enabled at the metastore level before creating external recipients.

If disabled, external sharing will not work.

---

## Steps

```text
Open Browser
      ↓
Login to:
https://accounts.azuredatabricks.net/

      ↓
Click Catalog
      ↓
Choose Metastore
      ↓
Enable Checkbox:
"Allow Delta Sharing with parties outside your organization"

      ↓
Choose Expiration Days
      ↓
Choose Token Name
      ↓
Save Changes
```

---

## Portal Navigation

```text
Accounts Console
      ↓
Catalog
      ↓
Metastore
      ↓
Delta Sharing Settings
```

---

## Important Settings

| Setting | Purpose |
|---|---|
| Allow Delta Sharing with parties outside your organization | Enables external sharing |
| Expiration Days | Token validity duration |
| Token Name | Recipient authentication token |

---

## Purpose
- Enables external recipient sharing
- Allows secure cross-organization access

---

# Delta Sharing Components

| Component | Purpose |
|---|---|
| Share | Container for shared data |
| Recipient | External consumer |
| Provider | Data owner |
| Shared Table | Table added into share |

---

# Delta Sharing Practice Flow

```text
Enable External Delta Sharing
            ↓
Create Table
            ↓
Create Share
            ↓
Add Table to Share
            ↓
Create Recipient
            ↓
Grant Share Access
            ↓
Generate Activation Link
            ↓
Recipient Accesses Shared Data
```

---

# 3. Create Sample Table

## Description
Creates a sample Delta table for Delta Sharing practice.

This table will later be shared with recipients.

## Command

```sql
CREATE TABLE demo.default.employee_share (

    id INT,
    name STRING,
    city STRING
);

INSERT INTO demo.default.employee_share
VALUES
(1, 'Sri', 'Chennai'),
(2, 'Ravi', 'Bangalore');
```

---

## Purpose
- Creates practice dataset
- Provides shareable table

---

# 4. Create Share

## Description
Creates a Share object in Unity Catalog.

A Share acts as a secure container for shared objects.

## Command

```sql
CREATE SHARE employee_share_obj;
```

---

## Purpose
- Creates share container
- Enables secure data sharing

---

# 5. Add Table to Share

## Description
Adds a Delta table into the Share object.

Recipients can access only objects added to the Share.

## Command

```sql
ALTER SHARE employee_share_obj
ADD TABLE demo.default.employee_share;
```

---

## Purpose
- Adds table into share
- Makes table available externally

---

# 6. Create Recipient

## Description
Creates a Recipient object representing the external consumer.

Recipients can be users, external organizations, or Databricks workspaces.

---

# Option 1 — Simple Recipient

## Command

```sql
CREATE RECIPIENT practice_user;
```

---

# Option 2 — Recipient Using Identity

## Command

```sql
CREATE RECIPIENT practice_user
USING IDENTITY 'lumieresaloonbacklink@gmail.com';
```

---

## Purpose
- Creates recipient configuration
- Enables secure authentication

---

# 7. Grant Share Access

## Description
Grants recipient access to the Share object.

Without this step, recipients cannot query shared data.

## Command

```sql
GRANT SELECT
ON SHARE employee_share_obj
TO RECIPIENT practice_user;
```

---

## Purpose
- Grants data access
- Enables recipient queries

---

# 8. Generate Activation Link

## Description
Generates recipient activation details such as activation URL and token.

Recipients use this information to activate secure sharing access.

## Command

```sql
DESCRIBE RECIPIENT practice_user;
```

---

## Example Output

| Field | Description |
|---|---|
| activation_url | Recipient activation URL |
| token | Authentication token |
| sharing_identifier | Unique sharing identity |

---

## Purpose
- Generates secure activation details
- Enables recipient onboarding

---

# 9. Consumer Side Access

## Description
Recipients open the activation link and access shared data securely.

No direct Databricks workspace access is required.

---

## Consumer Workflow

```text
Open Activation Link
        ↓
Authenticate
        ↓
Access Shared Data
        ↓
Run Queries
```

---

# Important Delta Sharing Commands

---

# Show Shares

## Command

```sql
SHOW SHARES;
```

---

# Show Recipients

## Command

```sql
SHOW RECIPIENTS;
```

---

# Show Shared Objects

## Command

```sql
SHOW ALL IN SHARE employee_share_obj;
```

---

# Remove Table from Share

## Command

```sql
ALTER SHARE employee_share_obj
REMOVE TABLE demo.default.employee_share;
```

---

# Drop Share

## Command

```sql
DROP SHARE employee_share_obj;
```

---

# Drop Recipient

## Command

```sql
DROP RECIPIENT practice_user;
```

---

# Important Concepts

| Concept | Description |
|---|---|
| Share | Container for shared data |
| Recipient | External consumer |
| Provider | Data owner |
| Activation URL | Access onboarding link |
| Token | Authentication credential |
| Unity Catalog | Governance layer |
| External Sharing | Cross-organization sharing |

---

# End-to-End Flow

```text
Enable External Delta Sharing
            ↓
Create Shareable Table
            ↓
Create Share
            ↓
Add Table to Share
            ↓
Create Recipient
            ↓
Grant Share Access
            ↓
Generate Activation URL
            ↓
Recipient Accesses Shared Data
```



---
