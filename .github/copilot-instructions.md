# MySQL Recursive Dump - AI Coding Guidelines

## Overview
This is a Python script that performs recursive MySQL table dumps based on foreign key relationships. It resolves dependencies to ensure data can be imported without foreign key constraint violations.

## Architecture
- **Single-file script**: All logic in `recursive_dump.py`
- **Configuration-driven**: Database connection via `config.json`
- **Dependency resolution**: Uses DFS to traverse foreign key relationships from a starting table
- **Output**: Generates SQL INSERT statements in dependency order

## Key Patterns

### Database Metadata Queries
Use `information_schema` for dynamic schema inspection:
```python
# Get foreign key parents
SELECT REFERENCED_TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND REFERENCED_TABLE_NAME IS NOT NULL
```

### SQL Generation
- **Batch INSERTs**: Group 1000 rows per INSERT for efficiency
- **Primary key ordering**: Always ORDER BY primary key columns when fetching
- **Proper escaping**: Custom `sql_escape()` handles NULL, numbers, and strings with single quotes

### Dependency Resolution
Recursive DFS ensures parent tables are dumped before children:
```python
def resolve_recursive_dependencies(conn, start_table):
    visited = set()
    order = []
    def dfs(table):
        if table in visited: return
        visited.add(table)
        parents = get_foreign_key_parents(conn, table)
        for p in parents: dfs(p)
        order.append(table)
    dfs(start_table)
    return order
```

## Workflows

### Running the Script
```bash
python recursive_dump.py
```
Prompts for table name, outputs `{table}_recursive_dump.sql`

### Configuration
Edit `config.json` with MySQL credentials:
```json
{
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "your_db"
}
```

## Dependencies
- `mysql-connector-python` for database connection
- Standard library only (json, collections)

## Conventions
- **File naming**: Output files use `{table}_recursive_dump.sql` pattern
- **Error handling**: Minimal - assumes valid connections and permissions
- **Encoding**: UTF-8 for output files
- **Table quoting**: Always use backticks for MySQL identifiers</content>
<parameter name="filePath">d:\Users\gaurahari\Tools\mysql-recursive-dump\.github\copilot-instructions.md