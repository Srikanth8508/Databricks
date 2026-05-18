````md id="p8vk3x"
# Databricks Unity Catalog — Permissions & GRANT Commands Guide

> Architecture: Unity Catalog Security Model  
> Scope: Catalog → Schema → Table → View → Function → Volume

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

# 1. Catalog Level Permissions

## Description
Catalog-level permissions control access to the catalog itself.

Users must have `USE CATALOG` permission before accessing schemas or tables inside the catalog.

---

## Grant Access to Catalog

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
| `USE CATALOG` | Allows access to the catalog |
| `CREATE SCHEMA` | Allows creating schemas |
| `BROWSE` | Allows browsing metadata |
| `ALL PRIVILEGES` | Grants all catalog permissions |

---

## Example — Full Catalog Access

```sql
GRANT ALL PRIVILEGES
ON CATALOG demo
TO `lumieresaloonbacklink@gmail.com`;
```

---

# 2. Schema Level Permissions

## Description
Schema-level permissions control access within a schema/database.

Users need `USE SCHEMA` permission before accessing objects inside the schema.

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
| `MODIFY` | Modify schema objects |
| `ALL PRIVILEGES` | Full schema access |

---

## Example — Data Engineer Access

```sql
GRANT CREATE TABLE, MODIFY
ON SCHEMA demo.default
TO `lumieresaloonbacklink@gmail.com`;
```

---

# 3. Table Level Permissions

## Description
Table-level permissions are the most commonly used permissions in Unity Catalog.

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
| `MODIFY` | Modify table structure |
| `INSERT` | Insert records |
| `UPDATE` | Update records |
| `DELETE` | Delete records |
| `TRUNCATE` | Remove all table rows |
| `READ_METADATA` | View table metadata |
| `ALL PRIVILEGES` | Full table access |

---

## Example — Full Table Access

```sql
GRANT ALL PRIVILEGES
ON TABLE demo.default.employee
TO `lumieresaloonbacklink@gmail.com`;
```

---

## Example — Read-Only Access

```sql
GRANT SELECT
ON TABLE demo.default.employee
TO `lumieresaloonbacklink@gmail.com`;
```

---

## Example — Insert and Update Access

```sql
GRANT INSERT, UPDATE
ON TABLE demo.default.employee
TO `lumieresaloonbacklink@gmail.com`;
```

---

# 4. View Level Permissions

## Description
View-level permissions control access to SQL views.

Users typically need `SELECT` permission to query a view.

---

## Grant View Access

### Command

```sql
GRANT SELECT
ON VIEW demo.default.sales_view
TO `lumieresaloonbacklink@gmail.com`;
```

---

## Common View Privileges

| Privilege | Description |
|---|---|
| `SELECT` | Read view data |
| `ALL PRIVILEGES` | Full access to view |

---

# 5. Function Level Permissions

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

## Common Function Privileges

| Privilege | Description |
|---|---|
| `EXECUTE` | Execute SQL function |
| `ALL PRIVILEGES` | Full function access |

---

# 6. Volume Level Permissions

## Description
Volume-level permissions control access to Unity Catalog volumes and files.

Used for managing file-based storage access.

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
| `READ FILES` | Read files from volume |
| `WRITE FILES` | Write files into volume |

---

## Example — Read & Write Access

```sql
GRANT READ FILES, WRITE FILES
ON VOLUME demo.default.raw_files
TO `lumieresaloonbacklink@gmail.com`;
```

---

# Common Grant Patterns

---

# Read-Only User Access

## Description
Provides read-only access to all tables inside a schema.

Useful for analysts and reporting users.

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

## Description
Provides complete control over a specific table.

Useful for table owners or administrators.

## Command

```sql
GRANT ALL PRIVILEGES
ON TABLE demo.default.employee
TO `user`;
```

---

# Data Engineer Access

## Description
Allows engineers to create and modify schema objects.

Useful for ETL and pipeline development.

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

## Description
Displays all privileges granted to a user.

## Command

```sql
SHOW GRANTS TO `lumieresaloonbacklink@gmail.com`;
```

---

# Show Table Grants

## Description
Displays permissions configured on a table.

## Command

```sql
SHOW GRANTS
ON TABLE demo.default.employee_d;
```

---

# Revoke Permissions

## Description
Removes privileges from a user.

## Command

```sql
REVOKE SELECT
ON TABLE demo.default.employee_d
FROM `lumieresaloonbacklink@gmail.com`;
```

---

# Example Permission Flow

```text
Catalog Access
        ↓
Schema Access
        ↓
Table / View / Function / Volume Access
```

---

# Important Unity Catalog Concepts

| Concept | Description |
|---|---|
| Catalog | Top-level container |
| Schema | Database inside catalog |
| Table | Structured data object |
| View | Virtual query object |
| Function | Reusable SQL logic |
| Volume | File storage object |
| GRANT | Assign permissions |
| REVOKE | Remove permissions |
| SHOW GRANTS | Display configured access |

---

````
