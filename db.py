
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

ENV_FILE = Path(__file__).with_name(".env")

_pool: ThreadedConnectionPool | None = None


def load_env(path: Path = ENV_FILE) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def dsn_params() -> dict[str, str | int]:
    load_env()
    password = os.environ.get("PGPASSWORD")
    if not password:
        raise RuntimeError(
            "PGPASSWORD is not set. Put it in prediction_markets/.env "
            "(see .env.example) or export it in your shell."
        )
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", 5432)),
        "dbname": os.environ.get("PGDATABASE", "prediction_markets"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": password,
        # Timestamps are stored UTC; display them in IST regardless of what
        # timezone the machine running this happens to be set to. PGSCHEMA
        # puts every table in a schema of its own -- how the tests run beside
        # the real data without touching it.
        "options": f"-c timezone={os.environ.get('PGTIMEZONE', 'Asia/Kolkata')}"
                   + (f" -c search_path={os.environ['PGSCHEMA']},public"
                      if os.environ.get("PGSCHEMA") else ""),
    }


def get_pool(minconn: int = 1, maxconn: int = 10) -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(minconn, maxconn, **dsn_params())
    return _pool


@contextmanager
def connection():
    """Borrow a pooled connection; commits on success, rolls back on error.

    A connection broken by a database restart is discarded rather than handed
    back to the pool, so a long-running service recovers by itself.
    """
    pool = get_pool()
    conn = pool.getconn()
    broken = False
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except psycopg2.Error:
            broken = True
        raise
    finally:
        pool.putconn(conn, close=broken or conn.closed != 0)


@contextmanager
def cursor(dict_rows: bool = True):
    """Borrow a cursor. Rows come back as dicts unless dict_rows=False."""
    with connection() as conn:
        factory = RealDictCursor if dict_rows else None
        with conn.cursor(cursor_factory=factory) as cur:
            yield cur


def query(sql: str, params: tuple | dict | None = None) -> list:
    with cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def execute(sql: str, params: tuple | dict | None = None) -> int:
    """Run a statement that returns no rows. Returns affected row count."""
    with cursor(dict_rows=False) as cur:
        cur.execute(sql, params)
        return cur.rowcount


def ping() -> bool:
    """True if the server answers. Raises on config/auth problems."""
    with cursor(dict_rows=False) as cur:
        cur.execute("select 1")
        return cur.fetchone()[0] == 1


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


if __name__ == "__main__":
    params = dsn_params()
    print(f"connecting to {params['user']}@{params['host']}:{params['port']}/{params['dbname']}")
    rows = query("select version() as version, current_database() as db")
    print(rows[0]["version"])
    print("database:", rows[0]["db"])
    close_pool()
