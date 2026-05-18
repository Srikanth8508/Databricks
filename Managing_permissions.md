# Databricks Unity Catalog — Permissions & User Access Management Guide

> Architecture: Azure AD → Databricks Workspace → Unity Catalog Permissions  
> Scope: User Creation → Workspace Access → Catalog → Schema → Table → View → Function → Volume

---

# User Access Flow

```text
Azure Portal
    ↓
Create Azure AD User
    ↓
Add User into Databricks Workspace
    ↓
Grant Unity Catalog Permissions
```

---

# 1. Create User in Azure Portal

## Description
Create a new Azure Active Directory user before granting Databricks access.

This user will later be added into the Databricks workspace.

---

## Steps

```text
Open Azure Portal
        ↓
Go to Users
        ↓
Click Create New User
```

---

## Required Details

| Field | Description |
|---|---|
| User Principal Name | Login email / username |
| Display Name | User display name |

---

## Example

| Field | Value |
|---|---|
| User Principal Name | lumieresaloonbacklink@gmail.com |
| Display Name | Demo User |

---

## Purpose
- Creates Azure AD identity
- Enables authentication into Databricks

---

# 2. Add User into Databricks Workspace

## Description
After creating the Azure AD user, add the same user into the Databricks workspace.

This allows the user to access Databricks resources.

---

## Steps

```text
Login to Databricks
        ↓
Go to Settings
        ↓
Identity and Access
        ↓
Users (Manage)
        ↓
Add User
        ↓
Enter Previously Created User Name
```

---

## Example

```text
lumieresaloonbacklink@gmail.com
```

---

## Purpose
- Grants workspace access
- Enables user authentication into Databricks

---

# Unity Catalog Permission Hierarchy

```text
Catalog
   ↓
Schema
   ↓
Table / View / Function / Volume
```

---

# 3. Catalog Level Permissions

## Description
Catalog-level permissions control access to the catalog itself.

Users must have `USE CATALOG` permission before accessing schemas or tables.

---

## Grant Catalog Access

### Command

```sql
GRANT USE CATALOG
ON CATALOG demo
TO `lumieresaloonbacklink@gmail.com`;
```

---

## Common Catalog Privileges

| Privilege | Description |
|---|---|
| `USE CATALOG` | Access catalog |
| `CREATE SCHEMA` | Create schemas |
| `BROWSE` | Browse metadata |
| `ALL PRIVILEGES` | Full catalog access |

---

# 4. Schema Level Permissions

## Description
Schema-level permissions control access within a schema/database.

Users need `USE SCHEMA` permission before accessing schema objects.

---

## Grant Schema Access

### Command

```sql
GRANT USE SCHEMA
ON SCHEMA demo.default
TO `lumieresaloonbacklink@gmail.com`;
```

---

## Common Schema Privileges

| Privilege | Description |
|---|---|
| `USE SCHEMA` | Access schema |
| `CREATE TABLE` | Create tables |
| `CREATE VIEW` | Create views |
| `CREATE FUNCTION` | Create functions |
| `MODIFY` | Modify objects |
| `ALL PRIVILEGES` | Full schema access |

---

# Example — Data Engineer Access

```sql
GRANT CREATE TABLE, MODIFY
ON SCHEMA demo.default
TO `lumieresaloonbacklink@gmail.com`;
```

---

# 5. Table Level Permissions

## Description
Table-level permissions are most commonly used in Unity Catalog.

These permissions control read and write operations on tables.

---

## Grant Table Access

### Command

```sql
GRANT SELECT
ON TABLE demo.default.employee
TO `lumieresaloonbacklink@gmail.com`;
```

---

## Common Table Privileges

| Privilege | Description |
|---|---|
| `SELECT` | Read table data |
| `MODIFY` | Modify table |
| `INSERT` | Insert records |
| `UPDATE` | Update records |
| `DELETE` | Delete records |
| `TRUNCATE` | Remove all rows |
| `READ_METADATA` | View metadata |
| `ALL PRIVILEGES` | Full table access |

---

# Example — Full Table Access

```sql
GRANT ALL PRIVILEGES
ON TABLE demo.default.employee
TO `lumieresaloonbacklink@gmail.com`;
```

---

# Example — Read-Only Access

```sql
GRANT SELECT
ON TABLE demo.default.employee
TO `lumieresaloonbacklink@gmail.com`;
```

---

# 6. View Level Permissions

## Description
View-level permissions control access to SQL views.

Users need `SELECT` permission to query views.

---

## Grant View Access

### Command

```sql
GRANT SELECT
ON VIEW demo.default.sales_view
TO `lumieresaloonbacklink@gmail.com`;
```

---

# 7. Function Level Permissions

## Description
Function-level permissions control execution access for SQL functions.

Users require `EXECUTE` permission to run functions.

---

## Grant Function Access

### Command

```sql
GRANT EXECUTE
ON FUNCTION demo.default.calculate_tax
TO `lumieresaloonbacklink@gmail.com`;
```

---

# 8. Volume Level Permissions

## Description
Volume-level permissions control access to Unity Catalog volumes and files.

Used for managing storage-level access.

---

## Grant Volume Access

### Command

```sql
GRANT READ FILES
ON VOLUME demo.default.raw_files
TO `lumieresaloonbacklink@gmail.com`;
```

---

## Common Volume Privileges

| Privilege | Description |
|---|---|
| `READ FILES` | Read files |
| `WRITE FILES` | Write files |

---

# Common Grant Patterns

---

# Read-Only User Access

## Commands

```sql
GRANT USE CATALOG
ON CATALOG demo
TO `user`;

GRANT USE SCHEMA
ON SCHEMA demo.default
TO `user`;

GRANT SELECT
ON ALL TABLES IN SCHEMA demo.default
TO `user`;
```

---

# Full Table Access

## Command

```sql
GRANT ALL PRIVILEGES
ON TABLE demo.default.employee
TO `user`;
```

---

# Data Engineer Access

## Command

```sql
GRANT CREATE TABLE, MODIFY
ON SCHEMA demo.default
TO `user`;
```

---

# Useful Permission Commands

---

# Show User Grants

## Command

```sql
SHOW GRANTS TO `lumieresaloonbacklink@gmail.com`;
```

---

# Show Table Grants

## Command

```sql
SHOW GRANTS
ON TABLE demo.default.employee_d;
```

---

# Revoke Permissions

## Command

```sql
REVOKE SELECT
ON TABLE demo.default.employee_d
FROM `lumieresaloonbacklink@gmail.com`;
```

---

# Complete Access Flow

```text
Azure Portal
    ↓
Create Azure AD User
    ↓
Add User into Databricks Workspace
    ↓
Grant Catalog Permission
    ↓
Grant Schema Permission
    ↓
Grant Table/View/Function Access
```

---

# Summary

This guide demonstrated:

- Azure AD user creation
- Adding users into Databricks
- Unity Catalog permission hierarchy
- Catalog-level permissions
- Schema-level permissions
- Table-level permissions
- View permissions
- Function permissions
- Volume permissions
- GRANT / REVOKE commands
- Security best practices

---
