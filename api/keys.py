"""API keys: made, listed and revoked on the server, checked on every request.

    python -m api.keys create NAME [--read-only] [--rate N]
    python -m api.keys list
    python -m api.keys revoke NAME

A key is shown once, when it is made; only its SHA-256 is stored. A read-only
key can read everything but cannot ask for backfills.
"""

import argparse
import hashlib
import secrets
import threading
import time

import db
import store

CACHE_SECONDS = 60              # a revoked key keeps working this long at most
_cache = {}                     # hash -> (row or None, looked up at)
_lock = threading.Lock()


def digest(key):
    return hashlib.sha256(key.encode()).hexdigest()


def lookup(key):
    """The key's row if it is valid and not revoked, else None. Cached, and
    last_used_at is written at most once per CACHE_SECONDS."""
    h = digest(key)
    now = time.time()
    with _lock:
        hit = _cache.get(h)
    if hit and now - hit[1] < CACHE_SECONDS:
        return hit[0]
    rows = db.query("""
        UPDATE api_keys SET last_used_at = now()
        WHERE key_hash = %s AND revoked_at IS NULL
        RETURNING key_id, name, scope, rate_per_min""", (h,))
    row = dict(rows[0]) if rows else None
    with _lock:
        if len(_cache) > 10_000:
            _cache.clear()
        _cache[h] = (row, now)
    return row


def create(name, scope="full", rate=None):
    key = "pm_" + secrets.token_urlsafe(32)
    db.execute("INSERT INTO api_keys (name, key_hash, key_prefix, scope, rate_per_min)"
               " VALUES (%s, %s, %s, %s, %s)", (name, digest(key), key[:10], scope, rate))
    return key


def revoke(name):
    return db.execute("UPDATE api_keys SET revoked_at = now()"
                      " WHERE name = %s AND revoked_at IS NULL", (name,))


def main():
    parser = argparse.ArgumentParser(description="Manage API keys.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("create", help="make a key and print it (shown only this once)")
    c.add_argument("name")
    c.add_argument("--read-only", action="store_true", help="no backfill requests")
    c.add_argument("--rate", type=int, help="requests per minute, instead of the default")
    sub.add_parser("list", help="every key, with when it was last used")
    r = sub.add_parser("revoke", help="stop a key working (within a minute)")
    r.add_argument("name")
    args = parser.parse_args()

    store.ensure_schema()
    if args.cmd == "create":
        print(create(args.name, "read" if args.read_only else "full", args.rate))
    elif args.cmd == "list":
        rows = db.query("SELECT name, key_prefix, scope, rate_per_min, created_at,"
                        " last_used_at, revoked_at FROM api_keys ORDER BY key_id")
        for r in rows:
            state = f"revoked {r['revoked_at']:%Y-%m-%d}" if r["revoked_at"] else "active"
            used = f"{r['last_used_at']:%Y-%m-%d %H:%M}" if r["last_used_at"] else "never"
            print(f"{r['name']:<20} {r['key_prefix']}...  {r['scope']:<5}"
                  f" {r['rate_per_min'] or 'default':>8}/min  last used {used:<16}  {state}")
        if not rows:
            print("no keys yet: python -m api.keys create NAME")
    else:
        print("revoked" if revoke(args.name) else f"no active key named {args.name!r}")


if __name__ == "__main__":
    main()
