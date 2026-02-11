
import json
import mysql.connector
from collections import deque, defaultdict


def load_config(path="config.json"):
    with open(path) as f:
        return json.load(f)


def connect_db(cfg):
    return mysql.connector.connect(
        host=cfg["host"],
        port=cfg.get("port", 3306),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"]
    )


def get_foreign_key_parents(conn, table):
    """
    Returns a list of tuples (referenced_table, fk_column) for foreign keys from given table.
    """
    q = """
        SELECT
            REFERENCED_TABLE_NAME, COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND REFERENCED_TABLE_NAME IS NOT NULL
    """
    cur = conn.cursor()
    cur.execute(q, (table,))
    return [(row[0], row[1]) for row in cur.fetchall()]


def get_foreign_key_children(conn, table):
    """
    Returns a list of tuples (child_table, fk_column) for foreign keys pointing to the given table.
    """
    q = """
        SELECT
            TABLE_NAME, COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
          AND REFERENCED_TABLE_NAME = %s
          AND COLUMN_NAME IS NOT NULL
    """
    cur = conn.cursor()
    cur.execute(q, (table,))
    return [(row[0], row[1]) for row in cur.fetchall()]


def has_non_null_fk_values(conn, table, fk_column):
    """
    Checks if the foreign key column has any non-null values in the table.
    """
    q = f"SELECT 1 FROM `{table}` WHERE `{fk_column}` IS NOT NULL LIMIT 1"
    cur = conn.cursor()
    cur.execute(q)
    return cur.fetchone() is not None


def get_primary_key(conn, table):
    q = """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND COLUMN_KEY = 'PRI'
        ORDER BY ORDINAL_POSITION
    """
    cur = conn.cursor()
    cur.execute(q, (table,))
    pk_cols = [row[0] for row in cur.fetchall()]
    return pk_cols


def fetch_all_rows(conn, table, pk_cols):
    cur = conn.cursor(dictionary=True)
    if pk_cols:
        order = ", ".join(pk_cols)
        cur.execute(f"SELECT * FROM `{table}` ORDER BY {order}")
    else:
        cur.execute(f"SELECT * FROM `{table}`")
    return cur.fetchall()


def sql_escape(value):
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def dump_table(conn, table):
    pk_cols = get_primary_key(conn, table)
    rows = fetch_all_rows(conn, table, pk_cols)
    if not rows:
        return ""

    columns = rows[0].keys()
    col_list = ", ".join(f"`{c}`" for c in columns)

    sql = [f"-- Dump of table `{table}`"]

    # Multi-row INSERTs (no DELETE, TRUNCATE will be at the top)
    batch = []
    for row in rows:
        row_values = ", ".join(sql_escape(row[c]) for c in columns)
        batch.append(f"({row_values})")

        if len(batch) == 1000:  # commit insert every 1000 rows
            sql.append(
                f"INSERT INTO `{table}` ({col_list}) VALUES\n" +
                ",\n".join(batch) + ";"
            )
            batch = []

    if batch:
        sql.append(
            f"INSERT INTO `{table}` ({col_list}) VALUES\n" +
            ",\n".join(batch) + ";"
        )

    sql.append("")  # newline
    return "\n".join(sql)


def resolve_recursive_dependencies(conn, start_table):
    """
    Discover all related tables (parents and descendants), then run a
    multi-pass ordering algorithm that prompts the user before dumping each
    table. Returns the ordered list of table names that the user confirmed
    to dump (skipped tables are treated as 'dumped' for dependency resolution
    but are not included in the returned list).
    """
    visited = set()
    traversal = []

    def dfs(table):
        if table in visited:
            return
        visited.add(table)

        parents = get_foreign_key_parents(conn, table)
        for p, fk_col in parents:
            if has_non_null_fk_values(conn, table, fk_col):
                dfs(p)

        traversal.append(table)

        # Recursively get children
        children = get_foreign_key_children(conn, table)
        for c, fk_col in children:
            if has_non_null_fk_values(conn, c, fk_col):
                dfs(c)

    dfs(start_table)

    if not traversal:
        return []

    # Use a set/dict of tables for multi-pass logic; traversal order is not required.
    tables = set(traversal)

    print("Tables discovered (order not preserved):")
    for t in sorted(tables):
        print(" -", t)

    # Build entries from the set (deterministic order)
    entries = []
    for t in sorted(tables):
        parents = []
        for p, fk_col in get_foreign_key_parents(conn, t):
            if p in tables and has_non_null_fk_values(conn, t, fk_col):
                parents.append(p)

        children = []
        for c, fk_col in get_foreign_key_children(conn, t):
            if c in tables and has_non_null_fk_values(conn, c, fk_col):
                children.append(c)

        entries.append({
            "name": t,
            "childOf": parents,
            "parentOf": children,
            "dumped": False,
        })

    # Multi-pass: repeatedly select tables with empty childOf
    ordered_to_dump = []
    while True:
        made_progress = False

        for entry in entries:
            if entry["dumped"]:
                continue

            if not entry["childOf"]:
                name = entry["name"]
                confirm = input(f"Dump table `{name}`? (y/n) [y]: ").strip().lower()
                if confirm == "" or confirm == "y":
                    ordered_to_dump.append(name)
                    entry["dumped"] = True

                    # remove this table from others' childOf
                    for other in entries:
                        if name in other["childOf"]:
                            other["childOf"] = [x for x in other["childOf"] if x != name]

                    made_progress = True
                else:
                    # treat skipped as dumped so dependents proceed
                    print(f" - Skipped `{name}` (marked as dumped)")
                    entry["dumped"] = True
                    for other in entries:
                        if name in other["childOf"]:
                            other["childOf"] = [x for x in other["childOf"] if x != name]
                    made_progress = True

        if not made_progress:
            remaining = [e["name"] for e in entries if not e["dumped"]]
            if not remaining:
                break
            print("\nCannot proceed further: the following tables are still blocked:")
            for r in remaining:
                print(" -", r)
            print("If you skipped a parent table, rerun and allow it, or inspect circular FKs.")
            break

    return ordered_to_dump


def main():
    cfg = load_config()
    conn = connect_db(cfg)

    start_table = input("Enter table name to dump: ").strip()

    print("Resolving dependencies...")
    tables = resolve_recursive_dependencies(conn, start_table)

    print("\nGenerating SQL dump...\n")

    # Build TRUNCATE section in reverse order (children to parents)
    truncate_sql = ["-- TRUNCATE tables (children to parents)"]
    for t in reversed(tables):
        truncate_sql.append(f"TRUNCATE TABLE `{t}`;")

    # Build INSERT section (normal order)
    insert_parts = []
    for t in tables:
        dump = dump_table(conn, t)
        if dump:
            insert_parts.append(dump)

    insert_sql = "\n".join(insert_parts)

    # Combine: TRUNCATE section + INSERT section
    dump_sql = "\n".join(truncate_sql) + "\n\n" + insert_sql

    outfile = f"{start_table}_recursive_dump.sql"
    with open(outfile, "w", encoding="utf-8") as f:
        f.write(dump_sql)

    print(f"\nDump written to: {outfile}")


if __name__ == "__main__":
    main()
