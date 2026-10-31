"""What every endpoint shares: errors, parameters, output formats, keys and
request limits."""

import base64
import csv
import io
import json
import re
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal

from fastapi import Request
from fastapi.responses import Response

import db
import ratelimit
import settings
from api import config, keys


# ---------------------------------------------------------------------------
# Errors: an HTTP status and {"error": {"code": ..., "message": ...}}
# ---------------------------------------------------------------------------

class ApiError(Exception):
    def __init__(self, status, code, message, headers=None):
        super().__init__(message)
        self.status, self.code, self.message, self.headers = status, code, message, headers


def error_response(status, code, message, headers=None):
    return json_response({"error": {"code": code, "message": message}}, status, headers)


def not_found(what, name):
    raise ApiError(404, "not_found", f"no {what} {name!r} in the store")


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

def parse_time(value, name):
    """ISO 8601 (a date, or a date and time; UTC unless it says otherwise) or
    Unix seconds (milliseconds are recognised too). Returns Unix seconds."""
    if value is None or str(value).strip() == "":
        return None
    text = str(value).strip()
    # An unencoded "+05:30" in a URL arrives as " 05:30".
    text = re.sub(r" (\d\d:?\d\d)$", r"+\1", text)
    try:
        if re.fullmatch(r"\d+(\.\d+)?", text):
            ts = float(text)
            return ts / 1000 if ts > 1e11 else ts
        stamp = datetime.fromisoformat(text)
    except ValueError:
        raise ApiError(400, "invalid_parameter",
                       f"{name}: expected a time like 2026-10-31, 2026-10-31T14:00:00Z"
                       f" or Unix seconds, not {text!r}") from None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.timestamp()


def split_list(value, name, most):
    """'A,B,C' -> ['A', 'B', 'C'], without repeats."""
    items = list(dict.fromkeys(p.strip() for p in (value or "").split(",") if p.strip()))
    if not items:
        raise ApiError(400, "invalid_parameter", f"{name}: give at least one")
    if len(items) > most:
        raise ApiError(400, "invalid_parameter", f"{name}: at most {most} per request")
    return items


def make_token(**state):
    return base64.urlsafe_b64encode(json.dumps(state).encode()).decode().rstrip("=")


def read_token(text):
    if not text:
        return {}
    try:
        state = json.loads(base64.urlsafe_b64decode(text + "=" * (-len(text) % 4)))
        assert isinstance(state, dict)
        return state
    except Exception:
        raise ApiError(400, "invalid_page_token", "page_token is not one this API gave out") from None


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

@contextmanager
def read():
    """A cursor whose queries are stopped after STATEMENT_TIMEOUT seconds."""
    with db.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = {int(config.STATEMENT_TIMEOUT * 1000)}")
        yield cur


# ---------------------------------------------------------------------------
# Output: json (the default), csv or parquet, all of which pandas reads as is
# ---------------------------------------------------------------------------

def utc(dt):
    return dt.astimezone(timezone.utc) if dt is not None else None


def iso(value):
    if value is None:
        return None
    if not isinstance(value, datetime):
        value = datetime.fromtimestamp(value, timezone.utc)
    return utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean(row):
    """A database row made ready to send: numbers as floats, times in UTC."""
    out = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            v = float(v)
        elif isinstance(v, datetime):
            v = utc(v)
        out[k] = v
    return out


def _default(value):
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"cannot send a {type(value).__name__}")


def json_response(body, status=200, headers=None):
    return Response(json.dumps(body, default=_default, separators=(",", ":")),
                    status_code=status, media_type="application/json", headers=headers)


def _arrow_column(values):
    import pyarrow as pa
    sample = next((v for v in values if v is not None), None)
    if isinstance(sample, datetime):
        return pa.array([int(v.timestamp()) if v is not None else None for v in values],
                        type=pa.timestamp("s", tz="UTC"))
    if isinstance(sample, bool):
        return pa.array(values, type=pa.bool_())
    if isinstance(sample, (int, float)):
        floats = any(isinstance(v, float) for v in values)
        return pa.array(values, type=pa.float64() if floats else pa.int64())
    return pa.array([None if v is None else str(v) for v in values], type=pa.string())


def table(rows, columns, fmt, key="data", meta=None, next_token=None, headers=None):
    """A list of rows in the asked-for format. For json, `meta` goes beside
    the rows; csv and parquet carry only the table, and the next page's token
    in the X-Next-Page-Token header."""
    headers = dict(headers or {})
    if fmt == "json":
        body = dict(meta or {})
        body[key] = rows
        body["next_page_token"] = next_token
        return json_response(body, headers=headers)
    if next_token:
        headers["X-Next-Page-Token"] = next_token
    if fmt == "csv":
        buf = io.StringIO()
        out = csv.writer(buf, lineterminator="\n")
        out.writerow(columns)
        for r in rows:
            out.writerow(["" if r.get(c) is None else
                          iso(r[c]) if isinstance(r[c], datetime) else
                          str(r[c]).lower() if isinstance(r[c], bool) else r[c]
                          for c in columns])
        return Response(buf.getvalue(), media_type="text/csv", headers=headers)
    import pyarrow as pa
    import pyarrow.parquet as pq
    arrays = [_arrow_column([r.get(c) for r in rows]) for c in columns]
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_arrays(arrays, names=columns), buf, compression="zstd")
    return Response(buf.getvalue(), media_type="application/vnd.apache.parquet", headers=headers)


# ---------------------------------------------------------------------------
# Keys and request limits
# ---------------------------------------------------------------------------

def _count(name, limit_window=60):
    """Add one to a per-minute counter in Redis. Returns (count, seconds until
    it resets). If Redis is out the request is let through: the key check
    itself does not depend on it."""
    now = time.time()
    window = int(now // limit_window)
    reset = int(limit_window - now % limit_window) + 1
    try:
        r = ratelimit.client()
        key = f"{settings.REDIS_PREFIX}api:{name}:{window}"
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, limit_window * 2)
        return pipe.execute()[0], reset
    except Exception:
        return 0, reset


def _refused():
    count, reset = _count("bad_keys")
    if count > config.BAD_KEYS_PER_MIN:
        raise ApiError(429, "rate_limited", f"too many requests without a valid key; try again"
                       f" in {reset}s", {"Retry-After": str(reset)})


def require_key(request: Request):
    """Every /v1 request: a valid key in X-API-Key (or Authorization: Bearer),
    within its requests-per-minute limit."""
    key = request.headers.get("x-api-key")
    if not key:
        auth = request.headers.get("authorization", "")
        key = auth[7:].strip() if auth.lower().startswith("bearer ") else None
    if not key:
        _refused()
        raise ApiError(401, "missing_key", "send your API key in the X-API-Key header")
    row = keys.lookup(key) if key.startswith("pm_") and len(key) < 100 else None
    if row is None:
        _refused()
        raise ApiError(401, "invalid_key", "this API key is not valid, or has been revoked")
    request.state.key = row
    limit = row["rate_per_min"] or config.RATE_PER_MIN
    count, reset = _count(f"rate:{row['key_id']}")
    request.state.ratelimit = (limit, max(0, limit - count), reset)
    if count > limit:
        raise ApiError(429, "rate_limited", f"more than {limit} requests a minute; try again"
                       f" in {reset}s", {"Retry-After": str(reset)})
    return row


def require_full(request: Request):
    """Backfill requests: a full (not read-only) key, at most BACKFILL_PER_MIN."""
    row = request.state.key
    if row["scope"] != "full":
        raise ApiError(403, "read_only_key", "this key can read data but not ask for backfills")
    count, reset = _count(f"backfill:{row['key_id']}")
    if count > config.BACKFILL_PER_MIN:
        raise ApiError(429, "rate_limited", f"more than {config.BACKFILL_PER_MIN} backfill"
                       f" requests a minute; try again in {reset}s", {"Retry-After": str(reset)})
    return row
