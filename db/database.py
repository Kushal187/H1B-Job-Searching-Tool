"""SQLite database connection helpers."""

import os
import sqlite3
from contextlib import contextmanager

import config


def get_connection() -> sqlite3.Connection:
    """Return a new SQLite connection with row factory enabled."""
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


def init_db():
    """Initialize the database by running schema.sql, then apply migrations."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        schema_sql = f.read()
    with get_db() as conn:
        conn.executescript(schema_sql)
    _migrate()
    print(f"Database initialized at {config.DB_PATH}")


def _migrate():
    """Apply incremental schema migrations to an existing database."""
    migrations = [
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
    ]
    with get_db() as conn:
        for sql in migrations:
            try:
                conn.execute(sql)
            except sqlite3.OperationalError:
                pass  # column/index already exists — safe to ignore


def insert_many(table: str, rows: list[dict], conn: sqlite3.Connection | None = None):
    """Bulk insert a list of dicts into a table.

    Args:
        table: Name of the target table.
        rows: List of dicts where keys are column names.
        conn: Optional existing connection. If None, creates a new one.
    """
    if not rows:
        return

    columns = list(rows[0].keys())
    placeholders = ", ".join(["?"] * len(columns))
    col_names = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"

    values = [tuple(row.get(c) for c in columns) for row in rows]

    def _execute(c: sqlite3.Connection):
        c.executemany(sql, values)

    if conn is not None:
        _execute(conn)
    else:
        with get_db() as c:
            _execute(c)


def query(
    sql: str, params: tuple = (), conn: sqlite3.Connection | None = None
) -> list[dict]:
    """Execute a SELECT query and return results as a list of dicts.

    Args:
        sql: SQL query string.
        params: Query parameters.
        conn: Optional existing connection.

    Returns:
        List of dicts, one per row.
    """

    def _execute(c: sqlite3.Connection) -> list[dict]:
        cursor = c.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    if conn is not None:
        return _execute(conn)
    else:
        with get_db() as c:
            return _execute(c)


def execute(sql: str, params: tuple = (), conn: sqlite3.Connection | None = None):
    """Execute a non-SELECT SQL statement (UPDATE, DELETE, etc.).

    Args:
        sql: SQL statement.
        params: Query parameters.
        conn: Optional existing connection.
    """
    if conn is not None:
        conn.execute(sql, params)
    else:
        with get_db() as c:
            c.execute(sql, params)


def clear_table(table: str, conn: sqlite3.Connection | None = None):
    """Delete all rows from a table."""
    execute(f"DELETE FROM {table}", conn=conn)


def vacuum():
    """Run VACUUM on the database. Must run outside a transaction."""
    conn = get_connection()
    try:
        conn.execute("VACUUM")
    finally:
        conn.close()
