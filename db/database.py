"""Database helpers with dual-backend support (SQLite or Postgres/Supabase)."""

from __future__ import annotations

import os
import re
import sqlite3
from contextlib import contextmanager
from typing import Any

import config

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - optional dependency for SQLite-only users
    psycopg = None
    dict_row = None


DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL")


def using_postgres() -> bool:
    """Return True when DATABASE_URL/SUPABASE_DB_URL is configured."""
    return bool(DATABASE_URL)


def _require_psycopg():
    if psycopg is None:
        raise RuntimeError(
            "Postgres backend selected but psycopg is not installed. "
            "Install requirements.txt (includes psycopg[binary])."
        )


def _connect_postgres():
    _require_psycopg()
    conninfo = DATABASE_URL
    if conninfo and "sslmode=" not in conninfo:
        sep = "&" if "?" in conninfo else "?"
        conninfo = f"{conninfo}{sep}sslmode=require"

    # Disable prepared statements by default for serverless/pooler compatibility.
    conn = psycopg.connect(
        conninfo,
        row_factory=dict_row,
        prepare_threshold=None,
        connect_timeout=10,
    )
    conn.execute("SET TIME ZONE 'UTC'")
    return conn


def get_connection():
    """Return a DB connection for the active backend."""
    if using_postgres():
        return _connect_postgres()

    os.makedirs(os.path.dirname(config.DB_PATH), exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")  # wait up to 10s for write locks
    return conn


@contextmanager
def get_db():
    """Context manager that yields a connection and auto-commits/closes."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _split_sql_statements(script: str) -> list[str]:
    """Split a SQL script into statements, respecting quoted strings/comments."""
    statements: list[str] = []
    buff: list[str] = []
    i = 0
    n = len(script)
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False

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


def _normalize_params(params: tuple | list | Any) -> tuple:
    if params is None:
        return ()
    if isinstance(params, tuple):
        return params
    if isinstance(params, list):
        return tuple(params)
    return (params,)


def _replace_qmark_placeholders(sql: str) -> str:
    """Convert SQLite-style '?' placeholders to psycopg '%s' placeholders.

    Also escapes literal '%' characters as '%%' for psycopg's placeholder parser.
    """
    out: list[str] = []
    i = 0
    n = len(sql)
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False

    def _append_source_char(ch: str):
        # psycopg placeholder parser requires literal '%' to be escaped.
        if ch == "%":
            out.append("%%")
        else:
            out.append(ch)

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""

        if in_line_comment:
            _append_source_char(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            _append_source_char(ch)
            if ch == "*" and nxt == "/":
                _append_source_char(nxt)
                i += 2
                in_block_comment = False
                continue
            i += 1
            continue

        if in_single:
            _append_source_char(ch)
            if ch == "'" and nxt == "'":
                _append_source_char(nxt)
                i += 2
                continue
            if ch == "'":
                in_single = False
            i += 1
            continue

        if in_double:
            _append_source_char(ch)
            if ch == '"' and nxt == '"':
                _append_source_char(nxt)
                i += 2
                continue
            if ch == '"':
                in_double = False
            i += 1
            continue

        if ch == "-" and nxt == "-":
            _append_source_char(ch)
            _append_source_char(nxt)
            i += 2
            in_line_comment = True
            continue

        if ch == "/" and nxt == "*":
            _append_source_char(ch)
            _append_source_char(nxt)
            i += 2
            in_block_comment = True
            continue

        if ch == "'":
            _append_source_char(ch)
            in_single = True
            i += 1
            continue

        if ch == '"':
            _append_source_char(ch)
            in_double = True
            i += 1
            continue

        if ch == "?":
            out.append("%s")
        else:
            _append_source_char(ch)
        i += 1

    return "".join(out)


_NOW_OFFSET_RE = re.compile(
    r"datetime\(\s*'now'\s*,\s*'([+-]?\d+)\s*(day|days|hour|hours|minute|minutes)'\s*\)",
    flags=re.IGNORECASE,
)
_NOW_DYNAMIC_OFFSET_RE = re.compile(
    r"datetime\(\s*'now'\s*,\s*%s\s*\|\|\s*'\s*(day|days|hour|hours|minute|minutes)'\s*\)",
    flags=re.IGNORECASE,
)
_NOW_RE = re.compile(r"datetime\(\s*'now'\s*\)", flags=re.IGNORECASE)
_DATE_NOW_OFFSET_RE = re.compile(
    r"date\(\s*'now'\s*,\s*'([+-]?\d+)\s*(day|days)'\s*\)",
    flags=re.IGNORECASE,
)
_DATE_NOW_RE = re.compile(r"date\(\s*'now'\s*\)", flags=re.IGNORECASE)


def _rewrite_sqlite_datetime_for_postgres(sql: str) -> str:
    """Translate common SQLite datetime expressions to Postgres equivalents."""

    def _now_expr_with_interval(delta_sql: str) -> str:
        # Match SQLite datetime('now', ...) output shape: YYYY-MM-DD HH:MM:SS
        return (
            "to_char(("
            f"{delta_sql}"
            ") AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')"
        )

    def _now_offset(match: re.Match) -> str:
        raw_amount = match.group(1)
        unit = match.group(2).lower()
        amount = int(raw_amount)
        if amount >= 0:
            return _now_expr_with_interval(f"NOW() + INTERVAL '{amount} {unit}'")
        return _now_expr_with_interval(f"NOW() - INTERVAL '{abs(amount)} {unit}'")

    def _now_dynamic_offset(match: re.Match) -> str:
        unit = match.group(1).lower()
        return _now_expr_with_interval(f"NOW() + (%s || ' {unit}')::interval")

    def _date_now_offset(match: re.Match) -> str:
        raw_amount = match.group(1)
        unit = match.group(2).lower()
        amount = int(raw_amount)
        if amount >= 0:
            return f"DATE(NOW() + INTERVAL '{amount} {unit}')"
        return f"DATE(NOW() - INTERVAL '{abs(amount)} {unit}')"

    sql = _NOW_OFFSET_RE.sub(_now_offset, sql)
    sql = _NOW_DYNAMIC_OFFSET_RE.sub(_now_dynamic_offset, sql)
    sql = _NOW_RE.sub(_now_expr_with_interval("NOW()"), sql)
    sql = _DATE_NOW_OFFSET_RE.sub(_date_now_offset, sql)
    sql = _DATE_NOW_RE.sub("CURRENT_DATE", sql)
    return sql


def _adapt_sql(sql: str) -> str:
    """Adapt SQL for the active backend while keeping call sites backend-agnostic."""
    if not using_postgres():
        return sql
    sql = _replace_qmark_placeholders(sql)
    sql = _rewrite_sqlite_datetime_for_postgres(sql)
    return sql


def adapt_sql(sql: str) -> str:
    """Public adapter for call sites executing SQL on raw connections."""
    return _adapt_sql(sql)


def _exec_script(conn, script: str):
    if using_postgres():
        for statement in _split_sql_statements(script):
            conn.execute(_adapt_sql(statement))
        return
    conn.executescript(script)


def init_db():
    """Initialize the database by running schema and migrations."""
    schema_name = "schema_postgres.sql" if using_postgres() else "schema.sql"
    schema_path = os.path.join(os.path.dirname(__file__), schema_name)
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    with get_db() as conn:
        _exec_script(conn, schema_sql)
    _migrate()
    if using_postgres():
        print("Database initialized with Postgres backend")
    else:
        print(f"Database initialized at {config.DB_PATH}")


def _sqlite_migrations() -> list[str]:
    return [
        # Add first_seen_at column to job_listings
        "ALTER TABLE job_listings ADD COLUMN first_seen_at TEXT",
        # Create unique index on job_url (for upsert support)
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_url_unique ON job_listings(job_url)",
        # Index for querying new jobs
        "CREATE INDEX IF NOT EXISTS idx_jobs_first_seen ON job_listings(first_seen_at)",
        # Add posted_at — the date the company originally published the job
        "ALTER TABLE job_listings ADD COLUMN posted_at TEXT",
        "CREATE INDEX IF NOT EXISTS idx_jobs_posted_at ON job_listings(posted_at)",
        # Deduplicate matched_companies before adding unique constraint
        # (keeps the row with the highest id for each normalized_name)
        """DELETE FROM matched_companies WHERE id NOT IN (
            SELECT MAX(id) FROM matched_companies GROUP BY normalized_name
        )""",
        # Add unique index on normalized_name to support upsert and prevent duplicates
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_matched_name_unique ON matched_companies(normalized_name)",
        # Job lifecycle tracking columns
        "ALTER TABLE job_listings ADD COLUMN last_seen_at TEXT",
        "ALTER TABLE job_listings ADD COLUMN is_active INTEGER DEFAULT 1",
        "CREATE INDEX IF NOT EXISTS idx_jobs_is_active ON job_listings(is_active)",
        # Backfill last_seen_at from scraped_at for existing rows
        "UPDATE job_listings SET last_seen_at = scraped_at WHERE last_seen_at IS NULL",
        # Workday boards table (tenant/subdomain/board config for Workday scraper)
        """CREATE TABLE IF NOT EXISTS workday_boards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER REFERENCES matched_companies(id),
            normalized_name TEXT NOT NULL,
            tenant TEXT NOT NULL,
            subdomain TEXT NOT NULL,
            board TEXT NOT NULL,
            url TEXT NOT NULL,
            job_count INTEGER DEFAULT 0,
            last_scraped TEXT,
            UNIQUE(tenant, board)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_workday_tenant ON workday_boards(tenant)",
        "CREATE INDEX IF NOT EXISTS idx_workday_normalized ON workday_boards(normalized_name)",
        """CREATE TABLE IF NOT EXISTS user_profile (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            location TEXT,
            headline TEXT,
            constraints_json TEXT,
            updated_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS profile_fact (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER NOT NULL REFERENCES user_profile(id) ON DELETE CASCADE,
            fact_type TEXT NOT NULL,
            source_section TEXT,
            raw_text TEXT NOT NULL,
            normalized_keywords TEXT,
            priority INTEGER DEFAULT 50,
            active INTEGER DEFAULT 1,
            updated_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS generation_event (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trace_id TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL,
            latency_ms INTEGER DEFAULT 0,
            model_route TEXT,
            token_in INTEGER DEFAULT 0,
            token_out INTEGER DEFAULT 0,
            error_code TEXT
        )""",
        "CREATE INDEX IF NOT EXISTS idx_profile_fact_profile ON profile_fact(profile_id)",
        "CREATE INDEX IF NOT EXISTS idx_profile_fact_priority ON profile_fact(priority DESC)",
        "CREATE INDEX IF NOT EXISTS idx_generation_event_created ON generation_event(created_at)",
    ]


def _postgres_migrations() -> list[str]:
    return [
        "ALTER TABLE job_listings ADD COLUMN IF NOT EXISTS first_seen_at TEXT",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_url_unique ON job_listings(job_url)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_first_seen ON job_listings(first_seen_at)",
        "ALTER TABLE job_listings ADD COLUMN IF NOT EXISTS posted_at TEXT",
        "CREATE INDEX IF NOT EXISTS idx_jobs_posted_at ON job_listings(posted_at)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_matched_name_unique ON matched_companies(normalized_name)",
        "ALTER TABLE job_listings ADD COLUMN IF NOT EXISTS last_seen_at TEXT",
        "ALTER TABLE job_listings ADD COLUMN IF NOT EXISTS is_active INTEGER DEFAULT 1",
        "CREATE INDEX IF NOT EXISTS idx_jobs_is_active ON job_listings(is_active)",
        "UPDATE job_listings SET last_seen_at = scraped_at WHERE last_seen_at IS NULL",
        """CREATE TABLE IF NOT EXISTS workday_boards (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            company_id BIGINT REFERENCES matched_companies(id),
            normalized_name TEXT NOT NULL,
            tenant TEXT NOT NULL,
            subdomain TEXT NOT NULL,
            board TEXT NOT NULL,
            url TEXT NOT NULL,
            job_count INTEGER DEFAULT 0,
            last_scraped TEXT,
            UNIQUE(tenant, board)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_workday_tenant ON workday_boards(tenant)",
        "CREATE INDEX IF NOT EXISTS idx_workday_normalized ON workday_boards(normalized_name)",
        """CREATE TABLE IF NOT EXISTS user_profile (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            location TEXT,
            headline TEXT,
            constraints_json TEXT,
            updated_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS profile_fact (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            profile_id BIGINT NOT NULL REFERENCES user_profile(id) ON DELETE CASCADE,
            fact_type TEXT NOT NULL,
            source_section TEXT,
            raw_text TEXT NOT NULL,
            normalized_keywords TEXT,
            priority INTEGER DEFAULT 50,
            active INTEGER DEFAULT 1,
            updated_at TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS generation_event (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            trace_id TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL,
            latency_ms INTEGER DEFAULT 0,
            model_route TEXT,
            token_in INTEGER DEFAULT 0,
            token_out INTEGER DEFAULT 0,
            error_code TEXT
        )""",
        "CREATE INDEX IF NOT EXISTS idx_profile_fact_profile ON profile_fact(profile_id)",
        "CREATE INDEX IF NOT EXISTS idx_profile_fact_priority ON profile_fact(priority DESC)",
        "CREATE INDEX IF NOT EXISTS idx_generation_event_created ON generation_event(created_at)",
    ]


def _migrate():
    """Apply incremental schema migrations to an existing database."""
    migrations = _postgres_migrations() if using_postgres() else _sqlite_migrations()

    with get_db() as conn:
        for sql in migrations:
            try:
                conn.execute(_adapt_sql(sql))
            except sqlite3.OperationalError:
                # SQLite migration may re-run on existing columns/indexes.
                pass


def insert_many(table: str, rows: list[dict], conn=None):
    """Bulk insert a list of dicts into a table."""
    if not rows:
        return

    columns = list(rows[0].keys())
    placeholders = ", ".join(["?"] * len(columns))
    col_names = ", ".join(columns)
    sql = _adapt_sql(f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})")

    values = [tuple(row.get(c) for c in columns) for row in rows]

    def _execute(c):
        c.executemany(sql, values)

    if conn is not None:
        _execute(conn)
    else:
        with get_db() as c:
            _execute(c)


def query(sql: str, params: tuple = (), conn=None) -> list[dict]:
    """Execute a SELECT query and return results as a list of dicts."""
    sql = _adapt_sql(sql)
    params = _normalize_params(params)

    def _execute(c) -> list[dict]:
        cursor = c.execute(sql, params)
        rows = cursor.fetchall()
        if using_postgres():
            return [dict(row) for row in rows]
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    if conn is not None:
        return _execute(conn)
    with get_db() as c:
        return _execute(c)


def execute(sql: str, params: tuple = (), conn=None):
    """Execute a non-SELECT SQL statement (UPDATE, DELETE, etc.)."""
    sql = _adapt_sql(sql)
    params = _normalize_params(params)
    if conn is not None:
        conn.execute(sql, params)
    else:
        with get_db() as c:
            c.execute(sql, params)


def clear_table(table: str, conn=None):
    """Delete all rows from a table."""
    execute(f"DELETE FROM {table}", conn=conn)


def get_db_size_mb() -> float | None:
    """Return DB size in MB (None when unavailable)."""
    if using_postgres():
        rows = query("SELECT pg_database_size(current_database()) as bytes")
        if not rows:
            return None
        return round(rows[0]["bytes"] / (1024 * 1024), 2)

    if os.path.exists(config.DB_PATH):
        return round(os.path.getsize(config.DB_PATH) / (1024 * 1024), 2)
    return 0.0


def vacuum():
    """Run VACUUM (SQLite only)."""
    if using_postgres():
        raise RuntimeError("VACUUM endpoint is SQLite-only in this project.")
    conn = get_connection()
    try:
        conn.execute("VACUUM")
    finally:
        conn.close()
