
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
    Returns a list of tables referenced by given table (i.e., parent tables).
    """
    q = """
        SELECT
            REFERENCED_TABLE_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND REFERENCED_TABLE_NAME IS NOT NULL
    """
    cur = conn.cursor()
    cur.execute(q, (table,))
    return [row[0] for row in cur.fetchall()]


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
        return f"-- No rows in `{table}`\n"

    columns = rows[0].keys()
    col_list = ", ".join(f"`{c}`" for c in columns)

    sql = [f"-- Dump of table `{table}`"]
    sql.append(f"DELETE FROM `{table}`;")

    # Multi-row INSERTs
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
    Returns a list of all tables referenced by start_table recursively,
    including the start table itself. Order is ensured so parents come first.
    """
    visited = set()
    order = []

    def dfs(table):
        if table in visited:
            return
        visited.add(table)

        parents = get_foreign_key_parents(conn, table)
        for p in parents:
            dfs(p)

        order.append(table)

    dfs(start_table)
    return order


def main():
    cfg = load_config()
    conn = connect_db(cfg)

    start_table = input("Enter table name to dump: ").strip()

    print("Resolving dependencies...")
    tables = resolve_recursive_dependencies(conn, start_table)

    print("Tables included in dump (parent tables first):")
    for t in tables:
        print(" -", t)

    print("\nGenerating SQL dump...\n")

    dump_parts = []
    for t in tables:
        dump_parts.append(dump_table(conn, t))

    dump_sql = "\n".join(dump_parts)

    outfile = f"{start_table}_recursive_dump.sql"
    with open(outfile, "w", encoding="utf-8") as f:
        f.write(dump_sql)

    print(f"\nDump written to: {outfile}")


if __name__ == "__main__":
    main()
