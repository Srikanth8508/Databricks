# Databricks Delta Sharing — Complete Practice Guide

> Architecture: Unity Catalog → Share → Recipient → Secure Data Access  
> Pattern: Cross Workspace / External Secure Data Sharing

---

# Overview

This guide explains:

- What is Delta Sharing
- Delta Sharing Architecture
- Share Creation
- Recipient Creation
- Granting Access
- Activation Links
- Consumer Access
- Important Commands
- Advanced Topics
- Interview Questions

---

# What is Delta Sharing?

## Description
Delta Sharing is an open protocol developed by Databricks for securely sharing data across organizations, workspaces, clouds, and platforms.

It allows sharing tables, views, and volumes without giving direct workspace access.

---

# What Can Be Shared?

| Shareable Object | Description |
|---|---|
| Tables | Delta tables |
| Views | Secure SQL views |
| Volumes | Unity Catalog files |
| Streaming Data | Shared incremental data |

---

# Delta Sharing Benefits

| Benefit | Description |
|---|---|
| Secure Sharing | No direct workspace access required |
| Cross Platform | Works across clouds and tools |
| Open Protocol | Supports external consumers |
| Real-Time Access | Always latest Delta data |
| Governance | Controlled through Unity Catalog |

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

# Delta Sharing Components

| Component | Purpose |
|---|---|
| Share | Container for shared data |
| Recipient | External consumer |
| Provider | Data owner |
| Shared Table | Table added into share |

---

# Important Requirement

## Description
Delta Sharing requires Unity Catalog to be enabled in the Databricks workspace.

The workspace must be attached to a Unity Catalog metastore.

---

## Verify Unity Catalog

### Command

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

If catalogs like:

- `main`
- `system`
- `demo`

exist → Unity Catalog is likely enabled.

---

# Delta Sharing Practice Flow

```text
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

# 1. Create Sample Table

## Description
Creates a sample Delta table for sharing practice.

This table will later be added into the Share object.

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
- Provides sample shareable table

---

# 2. Create Share

## Description
Creates a Share object in Unity Catalog.

A Share acts as a container for tables, views, and volumes.

## Command

```sql
CREATE SHARE employee_share_obj;
```

---

## Purpose
- Creates secure share container
- Used for sharing objects externally

---

# 3. Add Table to Share

## Description
Adds a Delta table into the Share object.

Recipients can access tables added to the Share.

## Command

```sql
ALTER SHARE employee_share_obj
ADD TABLE demo.default.employee_share;
```

---

## Purpose
- Adds table into share
- Makes table available for recipients

---

# 4. Create Recipient

## Description
Creates a Recipient object representing the external consumer.

Recipients can be Databricks users, workspaces, or external systems.

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
- Defines external consumer
- Enables secure authentication

---

# 5. Grant Share Access to Recipient

## Description
Grants recipient access to the Share object.

Without this step, recipients cannot access shared data.

## Command

```sql
GRANT SELECT
ON SHARE employee_share_obj
TO RECIPIENT practice_user;
```

---

## Purpose
- Grants access to shared tables
- Enables recipient data consumption

---

# 6. Generate Activation Link

## Description
Generates recipient activation details including activation URL and access token.

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
| sharing_identifier | Unique recipient identity |

---

## Purpose
- Generates secure activation link
- Enables recipient onboarding

---

# 7. Consumer Side Access

## Description
Recipients use the activation link to access shared data securely.

No direct workspace access is required.

---

## Consumer Workflow

```text
Open Activation Link
        ↓
Authenticate
        ↓
Access Shared Tables
        ↓
Query Shared Data
```

---

## Purpose
- Enables secure external data access
- Supports cross-workspace sharing

---

# Important Delta Sharing Commands

---

# Show Shares

## Description
Displays all Share objects available in the workspace.

## Command

```sql
SHOW SHARES;
```

---

# Show Recipients

## Description
Displays all configured recipients.

## Command

```sql
SHOW RECIPIENTS;
```

---

# Show Shared Objects

## Description
Displays all tables/views inside a Share.

## Command

```sql
SHOW ALL IN SHARE employee_share_obj;
```

---

# Remove Table from Share

## Description
Removes a table from the Share object.

## Command

```sql
ALTER SHARE employee_share_obj
REMOVE TABLE demo.default.employee_share;
```

---

# Drop Share

## Description
Deletes the Share object.

## Command

```sql
DROP SHARE employee_share_obj;
```

---

# Drop Recipient

## Description
Deletes the recipient configuration.

## Command

```sql
DROP RECIPIENT practice_user;
```

---

# Example Shared Data Query

## Description
Recipient queries shared data without direct workspace access.

## Example

```sql
SELECT *
FROM employee_share;
```

---

# Delta Sharing Security Model

```text
Unity Catalog
       ↓
Share Object
       ↓
Recipient Authentication
       ↓
Controlled Secure Access
```

---

# Types of Delta Sharing

| Type | Description |
|---|---|
| Databricks-to-Databricks | Sharing between workspaces |
| Open Sharing | Sharing with external systems |
| Cross Cloud Sharing | Multi-cloud sharing |
| Cross Region Sharing | Regional data sharing |

---

# Advanced Practice Topics

| Topic | Description |
|---|---|
| Sharing Views | Share secure SQL views |
| Sharing Volumes | Share Unity Catalog files |
| Open Sharing | External consumers |
| Recipient Tokens | Authentication management |
| Cross-region Sharing | Multi-region access |
| Clean Rooms | Privacy-safe collaboration |
| Governance | Unity Catalog security |

---

# Common Interview Questions

| Question | Focus Area |
|---|---|
| Difference between Delta Sharing and DBFS sharing | Architecture |
| Open Sharing vs Databricks Sharing | Security |
| How recipient authentication works | Authentication |
| Share vs Recipient vs Provider | Components |
| Benefits of Delta Sharing | Governance |
| Why Unity Catalog is required | Security Model |

---

# Important Delta Sharing Concepts

| Concept | Description |
|---|---|
| Share | Container for shared data |
| Recipient | External consumer |
| Provider | Data owner |
| Activation URL | Access onboarding link |
| Token | Authentication credential |
| Unity Catalog | Governance layer |
| Open Sharing | External platform access |

---

# End-to-End Flow

```text
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
Generate Activation URL
      ↓
Recipient Queries Shared Data
```

---

# Summary

This guide demonstrated:

- Delta Sharing fundamentals
- Share creation
- Recipient management
- Granting share access
- Activation links
- Consumer-side access
- Unity Catalog integration
- Security model
- Advanced sharing topics
- Interview preparation topics

---
