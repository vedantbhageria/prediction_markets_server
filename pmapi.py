"""Python client for the prediction_markets API. One file: copy it anywhere.

Needs requests and pandas; with pyarrow installed, candles and lists travel as
parquet (smaller, and the column types arrive intact).

    from pmapi import Client
    pm = Client("https://theonealpha.tail947e08.ts.net:8443", key="pm_...")
    # or set PM_API_URL and PM_API_KEY and just: pm = Client()

    pm.markets(q="bitcoin", status="open")            # DataFrame of markets
    pm.market("KXBTCD-26NOV0117-T70000")             # one market, as a dict
    pm.candles("KXBTCD-26NOV0117-T70000", "5m", start="2026-10-25")
                                                      # DataFrame indexed by ts
    pm.candles(["A", "B"], "1h", fill="raw")          # long: a ticker column
    pm.closes(["A", "B"], "1h")                       # wide: one close column each
    job = pm.backfill("KXBTCD-26NOV0117-T70000")      # download it again, now
    pm.wait(job)                                      # ...and wait until done

Every list is fetched page by page until it ends (or `max_rows`), so a call
returns everything it asked for. Times can be strings, datetimes, pandas
Timestamps or Unix seconds; naive ones are taken as UTC.
"""

import io
import os
import socket
import time
from datetime import datetime, timezone

import pandas as pd
import requests

try:
    import pyarrow  # noqa: F401
    _WIRE = "parquet"
except ImportError:
    _WIRE = "json"

CANDLE_COLUMNS = ["ts", "open", "high", "low", "close", "volume", "open_interest", "filled"]
TIME_COLUMNS = {"ts", "open_time", "close_time", "last_trade_ts", "candles_to"}


class APIError(Exception):
    """The API said no. `status` is the HTTP status, `code` a short name
    (not_found, invalid_parameter, rate_limited, ...)."""

    def __init__(self, status, code, message):
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code, self.message = status, code, message


def _time(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        stamp = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return stamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(value)


def _tickers(value):
    return [value] if isinstance(value, str) else list(value)


class Client:
    def __init__(self, url=None, key=None, timeout=120, retries=3):
        self.url = (url or os.environ.get("PM_API_URL", "")).rstrip("/")
        key = key or os.environ.get("PM_API_KEY")
        if not self.url or not key:
            raise ValueError("give url and key, or set PM_API_URL and PM_API_KEY")
        self.timeout, self.retries = timeout, retries
        self.session = requests.Session()
        self.session.headers["X-API-Key"] = key
        # The machine's name, so the server's usage page can tell callers apart.
        self.session.headers["User-Agent"] = f"pmapi/1.0 ({socket.gethostname()})"

    # -- plumbing -----------------------------------------------------------

    def _request(self, method, path, **kwargs):
        """One call, waiting out rate limits and brief outages."""
        for attempt in range(self.retries + 1):
            try:
                r = self.session.request(method, self.url + path, timeout=self.timeout, **kwargs)
            except requests.ConnectionError:
                if attempt == self.retries:
                    raise
                time.sleep(2 ** attempt)
                continue
            if r.status_code in (429, 503) and attempt < self.retries:
                time.sleep(float(r.headers.get("Retry-After", 2 ** attempt)))
                continue
            if r.status_code >= 400:
                try:
                    err = r.json()["error"]
                except Exception:
                    err = {"code": "http_error", "message": r.text[:300]}
                raise APIError(r.status_code, err["code"], err["message"])
            return r

    def _get(self, path, **params):
        params = {k: v for k, v in params.items() if v is not None}
        return self._request("GET", path, params=params)

    def _frame(self, response, key):
        if response.headers.get("content-type", "").startswith("application/vnd.apache.parquet"):
            return pd.read_parquet(io.BytesIO(response.content))
        body = response.json()
        df = pd.DataFrame(body[key])
        for c in df.columns:
            if c in TIME_COLUMNS:
                df[c] = pd.to_datetime(df[c], utc=True)
        return df

    def _pages(self, path, key, max_rows=None, **params):
        """Every page of a list, joined. Returns (DataFrame, the first response)."""
        frames, token, rows, first = [], None, 0, None
        while True:
            r = self._get(path, format=_WIRE, page_token=token, **params)
            first = first or r
            frames.append(self._frame(r, key))
            rows += len(frames[-1])
            token = (r.headers.get("X-Next-Page-Token") if _WIRE == "parquet"
                     else r.json().get("next_page_token"))
            if not token or (max_rows is not None and rows >= max_rows):
                break
        df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        return (df.head(max_rows) if max_rows is not None else df), first

    # -- markets ------------------------------------------------------------

    def markets(self, max_rows=None, **filters):
        """Markets as a DataFrame. Filters: status (open/closed/active/...),
        series, event, category, q, tickers, closes_after, closes_before,
        min_volume, sort (e.g. "-volume", "close_time")."""
        for k in ("closes_after", "closes_before"):
            filters[k] = _time(filters.get(k))
        for k in ("series", "event", "category", "tickers"):
            if filters.get(k) is not None and not isinstance(filters[k], str):
                filters[k] = ",".join(filters[k])
        limit = min(10_000, max_rows) if max_rows else 1000
        return self._pages("/v1/markets", "markets", max_rows, limit=limit, **filters)[0]

    def market(self, ticker, include_raw=False):
        return self._get(f"/v1/markets/{ticker}", include_raw=include_raw or None).json()

    def events(self, max_rows=None, **filters):
        """Events as a DataFrame. Filters: series, category, q, status."""
        return self._pages("/v1/events", "events", max_rows, limit=1000, **filters)[0]

    def event(self, event_ticker):
        """An event as a dict; its markets under "markets" as a DataFrame."""
        body = self._get(f"/v1/events/{event_ticker}").json()
        body["markets"] = pd.DataFrame(body["markets"])
        return body

    def series_list(self, max_rows=None, **filters):
        """Series as a DataFrame. Filters: category, frequency, q."""
        return self._pages("/v1/series", "series", max_rows, limit=1000, **filters)[0]

    def series(self, series_ticker):
        body = self._get(f"/v1/series/{series_ticker}").json()
        body["events"] = pd.DataFrame(body["events"])
        return body

    def categories(self):
        return self._pages("/v1/categories", "categories")[0]

    # -- candles ------------------------------------------------------------

    def candles(self, tickers, interval="1m", start=None, end=None, fill="forward"):
        """Candles as a DataFrame.

        One ticker: indexed by ts. Several (a list): a long table with a
        ticker column. interval: 1m 5m 15m 1h 4h 1d. fill: "forward" (a row
        for every bucket; quiet ones repeat the last close, filled=True) or
        "raw" (only buckets with a trade). Default window: the last day.

        For one ticker, df.attrs["coverage"] says what stretch the store
        holds and whether it covers the window asked for (pm.coverage()
        asks for any ticker).
        """
        params = dict(interval=interval, start=_time(start), end=_time(end), fill=fill,
                      limit=10_000)
        if isinstance(tickers, str):
            df, first = self._pages(f"/v1/markets/{tickers}/candles", "candles", **params)
            df = self._typed(df, CANDLE_COLUMNS).set_index("ts")
            h = first.headers
            df.attrs["coverage"] = (first.json()["coverage"] if _WIRE == "json" else {
                "from": h.get("X-Coverage-From") or None, "to": h.get("X-Coverage-To") or None,
                "complete": h.get("X-Coverage-Complete") == "true"})
            return df
        names, frames = list(tickers), []
        for i in range(0, len(names), 100):
            frames.append(self._pages("/v1/candles", "candles",
                                      tickers=",".join(names[i:i + 100]), **params)[0])
        df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        return self._typed(df, ["ticker"] + CANDLE_COLUMNS)

    @staticmethod
    def _typed(df, columns):
        if df.empty:
            df = pd.DataFrame(columns=columns)
            df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df["filled"] = df["filled"].astype(bool)
        return df

    def coverage(self, ticker, interval="1m", start=None, end=None):
        """{"from", "to", "complete"} for this window."""
        return self._get(f"/v1/markets/{ticker}/candles", interval=interval, start=_time(start),
                         end=_time(end), limit=1).json()["coverage"]

    def closes(self, tickers, interval="1m", start=None, end=None, fill="forward",
               field="close"):
        """One column per ticker, indexed by ts: ready to compare or correlate."""
        df = self.candles(list(_tickers(tickers)), interval, start, end, fill)
        return df.pivot(index="ts", columns="ticker", values=field)

    def latest(self, tickers):
        """One ticker: a dict with its last trade. A list: a DataFrame."""
        if isinstance(tickers, str):
            return self._get(f"/v1/markets/{tickers}/latest").json()
        return self._pages("/v1/latest", "latest", tickers=",".join(tickers))[0]

    # -- collection ---------------------------------------------------------

    def hot(self):
        """The markets live collection fetches every minute, busiest first."""
        return self._pages("/v1/hot", "markets")[0]

    def status(self):
        return self._get("/v1/status").json()

    def health(self):
        return requests.get(self.url + "/health", timeout=self.timeout).json()

    # -- the push command ---------------------------------------------------

    def backfill(self, ticker, start=None, end=None):
        """Ask the server to download this market's candles again, now. By
        default its whole life inside the store window. Returns the job."""
        body = {"ticker": ticker}
        if start is not None:
            body["from"] = _time(start)
        if end is not None:
            body["to"] = _time(end)
        return self._request("POST", "/v1/backfill", json=body).json()

    def job(self, job):
        job_id = job["job_id"] if isinstance(job, dict) else job
        return self._get(f"/v1/backfill/{job_id}").json()

    def wait(self, job, timeout=900, every=2):
        """Block until the job is done; returns it. Raises if it failed."""
        deadline = time.time() + timeout
        while True:
            j = self.job(job)
            if j["status"] == "done":
                return j
            if j["status"] == "failed":
                raise APIError(500, "backfill_failed", j["error"] or "the job failed")
            if time.time() > deadline:
                raise TimeoutError(f"job {j['job_id']} still {j['status']} after {timeout}s")
            time.sleep(every)
