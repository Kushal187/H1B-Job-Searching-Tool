#!/usr/bin/env python3
"""Migrate local SQLite data into Supabase/Postgres preserving primary keys.

Usage:
  python scripts/migrate_sqlite_to_supabase.py \
    --sqlite-path data/h1b_jobs.db \
    --postgres-url "$SUPABASE_DB_URL"
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path

try:
    import psycopg
    from psycopg import sql
except ImportError:  # pragma: no cover - runtime dependency
    psycopg = None
    sql = None


TABLE_COLUMNS: dict[str, list[str]] = {
    "sec_formd_companies": [
        "id",
        "company_name",
        "cik_number",
        "state",
        "industry_group",
        "total_amount_sold",
        "filing_date",
        "normalized_name",
    ],
    "h1b_sponsors": [
        "id",
        "employer_name",
        "city",
        "state",
        "naics_code",
        "visa_class",
        "initial_approvals",
        "continuing_approvals",
        "initial_denials",
        "fiscal_year",
        "normalized_name",
    ],
    "matched_companies": [
        "id",
        "company_name",
        "normalized_name",
        "source",
        "h1b_approval_count",
        "sec_amount_raised",
        "priority_score",
    ],
    "company_ats_status": [
        "id",
        "company_id",
        "normalized_name",
        "ats_system",
        "last_checked",
        "has_jobs",
    ],
    "workday_boards": [
        "id",
        "company_id",
        "normalized_name",
        "tenant",
        "subdomain",
        "board",
        "url",
        "job_count",
        "last_scraped",
    ],
    "job_listings": [
        "id",
        "company_id",
        "company_name",
        "ats_system",
        "job_title",
        "job_location",
        "job_url",
        "department",
        "scraped_at",
        "first_seen_at",
        "last_seen_at",
        "posted_at",
        "is_active",
        "raw_json",
    ],
}

LOAD_ORDER = [
    "sec_formd_companies",
    "h1b_sponsors",
    "matched_companies",
    "company_ats_status",
    "workday_boards",
    "job_listings",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate SQLite DB into Supabase/Postgres."
    )
    parser.add_argument(
        "--sqlite-path",
        default="data/h1b_jobs.db",
        help="Path to source SQLite DB (default: data/h1b_jobs.db)",
    )
    parser.add_argument(
        "--postgres-url",
        default=os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL"),
        help="Destination Postgres URL (or set DATABASE_URL/SUPABASE_DB_URL)",
    )
    parser.add_argument(
        "--project-ref",
        default=os.environ.get("SUPABASE_PROJECT_REF"),
        help="Supabase project ref (used with --db-password if --postgres-url is not provided)",
    )
    parser.add_argument(
        "--db-password",
        default=os.environ.get("SUPABASE_DB_PASSWORD"),
        help="Supabase DB password (used with --project-ref if --postgres-url is not provided)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Insert batch size (default: 5000)",
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Do not truncate destination tables before migration",
    )
    parser.add_argument(
        "--skip-schema-init",
        action="store_true",
        help="Skip creating/updating destination schema",
    )
    return parser.parse_args()


def build_postgres_url(args: argparse.Namespace) -> str | None:
    if args.postgres_url:
        return args.postgres_url
    if args.project_ref and args.db_password:
        return (
            "postgresql://postgres:"
            f"{args.db_password}"
            f"@db.{args.project_ref}.supabase.co:5432/postgres?sslmode=require"
        )
    return None


def split_sql_statements(script: str) -> list[str]:
    statements: list[str] = []
    buff: list[str] = []
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False

    i = 0
    n = len(script)
    while i < n:
        ch = script[i]
        nxt = script[i + 1] if i + 1 < n else ""

        if in_line_comment:
            buff.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            buff.append(ch)
            if ch == "*" and nxt == "/":
                buff.append(nxt)
                i += 2
                in_block_comment = False
                continue
            i += 1
            continue

        if in_single:
            buff.append(ch)
            if ch == "'" and nxt == "'":
                buff.append(nxt)
                i += 2
                continue
            if ch == "'":
                in_single = False
            i += 1
            continue

        if in_double:
            buff.append(ch)
            if ch == '"' and nxt == '"':
                buff.append(nxt)
                i += 2
                continue
            if ch == '"':
                in_double = False
            i += 1
            continue

        if ch == "-" and nxt == "-":
            buff.append(ch)
            buff.append(nxt)
            i += 2
            in_line_comment = True
            continue

        if ch == "/" and nxt == "*":
            buff.append(ch)
            buff.append(nxt)
            i += 2
            in_block_comment = True
            continue

        if ch == "'":
            buff.append(ch)
            in_single = True
            i += 1
            continue

        if ch == '"':
            buff.append(ch)
            in_double = True
            i += 1
            continue

        if ch == ";":
            statement = "".join(buff).strip()
            if statement:
                statements.append(statement)
            buff = []
            i += 1
            continue

        buff.append(ch)
        i += 1

    tail = "".join(buff).strip()
    if tail:
        statements.append(tail)
    return statements


def sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def sqlite_row_count(conn: sqlite3.Connection, table_name: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row else 0


def init_postgres_schema(pg_conn):
    schema_path = Path(__file__).resolve().parents[1] / "db" / "schema_postgres.sql"
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    statements = split_sql_statements(schema_sql)
    with pg_conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
    pg_conn.commit()


def truncate_destination(pg_conn):
    # Ordered from deepest dependencies to roots.
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            TRUNCATE TABLE
                job_listings,
                company_ats_status,
                workday_boards,
                matched_companies,
                h1b_sponsors,
                sec_formd_companies
            RESTART IDENTITY CASCADE
            """
        )
    pg_conn.commit()


def migrate_table(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    table_name: str,
    columns: list[str],
    batch_size: int,
) -> int:
    if not sqlite_table_exists(sqlite_conn, table_name):
        print(f"[skip] {table_name}: table does not exist in SQLite source.")
        return 0

    total = sqlite_row_count(sqlite_conn, table_name)
    if total == 0:
        print(f"[ok] {table_name}: 0 rows")
        return 0

    cols_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = (
        f"INSERT INTO {table_name} ({cols_sql}) "
        f"OVERRIDING SYSTEM VALUE VALUES ({placeholders})"
    )
    select_sql = f"SELECT {cols_sql} FROM {table_name}"

    transferred = 0
    src_cur = sqlite_conn.execute(select_sql)
    with pg_conn.cursor() as dst_cur:
        while True:
            rows = src_cur.fetchmany(batch_size)
            if not rows:
                break
            dst_cur.executemany(insert_sql, rows)
            transferred += len(rows)
            if transferred % (batch_size * 10) == 0 or transferred == total:
                print(f"[progress] {table_name}: {transferred:,}/{total:,}")

    pg_conn.commit()
    print(f"[ok] {table_name}: {transferred:,} rows")
    return transferred


def reseed_sequences(pg_conn):
    with pg_conn.cursor() as cur:
        for table in TABLE_COLUMNS.keys():
            cur.execute(
                sql.SQL(
                    "SELECT setval("
                    "pg_get_serial_sequence({}, 'id'), "
                    "COALESCE((SELECT MAX(id) FROM {}), 1), true)"
                ).format(
                    sql.Literal(table),
                    sql.Identifier(table),
                )
            )
    pg_conn.commit()


def main():
    args = parse_args()

    if psycopg is None or sql is None:
        raise SystemExit(
            "Missing dependency 'psycopg'. Install requirements.txt first."
        )

    postgres_url = build_postgres_url(args)

    if not postgres_url:
        raise SystemExit(
            "Missing destination URL. Pass --postgres-url, or pass --project-ref and "
            "--db-password (or set SUPABASE_PROJECT_REF/SUPABASE_DB_PASSWORD)."
        )

    sqlite_path = Path(args.sqlite_path).expanduser().resolve()
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite DB not found: {sqlite_path}")

    print(f"Source SQLite: {sqlite_path}")
    print("Connecting to destination Postgres...")

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    try:
        pg_conn = psycopg.connect(postgres_url)
        try:
            with pg_conn.cursor() as cur:
                cur.execute("SET TIME ZONE 'UTC'")
            pg_conn.commit()

            if not args.skip_schema_init:
                print("Initializing destination schema...")
                init_postgres_schema(pg_conn)

            if not args.no_truncate:
                print("Truncating destination tables...")
                truncate_destination(pg_conn)

            grand_total = 0
            for table in LOAD_ORDER:
                moved = migrate_table(
                    sqlite_conn=sqlite_conn,
                    pg_conn=pg_conn,
                    table_name=table,
                    columns=TABLE_COLUMNS[table],
                    batch_size=args.batch_size,
                )
                grand_total += moved

            print("Reseeding identity sequences...")
            reseed_sequences(pg_conn)

            print(f"Done. Total migrated rows: {grand_total:,}")
        finally:
            pg_conn.close()
    finally:
        sqlite_conn.close()


if __name__ == "__main__":
    main()
