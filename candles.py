"""Turn a stream of price samples into fixed-width candles.

Neither venue publishes anything finer than a minute, so 5-second candles are
built here from polled quotes. No I/O: feed samples in with add(), collect
finished candles with flush().
"""

from datetime import datetime, timezone


class _State:
    __slots__ = ("bucket", "open", "high", "low", "close", "volume", "cum",
                 "oi", "last", "sent_close", "sent_oi", "sent_at")

    def __init__(self):
        self.bucket = None      # index of the candle being built, None between candles
        self.open = self.high = self.low = self.close = None
        self.volume = 0.0
        self.cum = None         # venue's running volume total at the last sample
        self.oi = None
        self.last = None        # close of the previous candle, emitted or not
        self.sent_close = self.sent_oi = None
        self.sent_at = None     # end time of the last candle actually emitted


class CandleBuilder:
    """Candles of `seconds` width, one series per key.

    A candle where nothing changed is dropped unless `heartbeat` seconds have
    passed since the last one emitted for that key. `stamp` is which edge of
    the candle its timestamp names: Kalshi's own history uses the end,
    Polymarket's the start, and live rows have to sit alongside both.
    """

    def __init__(self, seconds=5, heartbeat=60, stamp="end"):
        self.seconds = seconds
        self.heartbeat = heartbeat
        self.stamp = stamp
        self._states = {}
        self._out = []

    def add(self, key, ts, quote=None, trade=None, cum_volume=None, open_interest=None):
        """One sample taken at unix time `ts`.

        `quote` is the book mid, `trade` the last traded price. The trade price
        is used only when the running volume moved since the previous sample --
        otherwise it may be hours stale and the quote is the better number. This
        is the same rule the batch scraper applies to Kalshi's minute candles.
        """
        state = self._states.get(key)
        if state is None:
            state = self._states[key] = _State()

        traded = (cum_volume is not None and state.cum is not None
                  and cum_volume > state.cum)
        price = trade if (traded and trade is not None) else (
            quote if quote is not None else trade)
        if price is None:
            if state.cum is None:
                state.cum = cum_volume      # baseline only; later volume still counts
            return
        price = round(price, 6)

        bucket = int(ts // self.seconds)
        if state.bucket is not None and bucket > state.bucket:
            self._close(key, state)
        if state.bucket is None:
            state.bucket = bucket
            state.open = state.last if state.last is not None else price
            state.high = state.low = state.open
            state.volume = 0.0

        state.high = max(state.high, price)
        state.low = min(state.low, price)
        state.close = price
        if cum_volume is not None:
            if state.cum is not None and cum_volume > state.cum:
                state.volume += cum_volume - state.cum
            state.cum = cum_volume
        if open_interest is not None:
            state.oi = open_interest

    def flush(self, now):
        """Finished candles as (key, ts, open, high, low, close, volume, oi)."""
        current = int(now // self.seconds)
        for key, state in self._states.items():
            if state.bucket is not None and state.bucket < current:
                self._close(key, state)
        out, self._out = self._out, []
        return out

    def forget(self, key):
        self._states.pop(key, None)

    def _close(self, key, state):
        start = state.bucket * self.seconds
        end = start + self.seconds
        moved = (state.sent_close is None or state.close != state.sent_close
                 or state.high != state.low or state.volume > 0
                 or state.oi != state.sent_oi)
        if moved or end - state.sent_at >= self.heartbeat:
            when = datetime.fromtimestamp(end if self.stamp == "end" else start,
                                          timezone.utc)
            # No running total from the venue means volume is unknown, not zero.
            volume = round(state.volume, 4) if state.cum is not None else None
            self._out.append((key, when, state.open, state.high, state.low,
                              state.close, volume, state.oi))
            state.sent_close, state.sent_oi, state.sent_at = state.close, state.oi, end
        state.last = state.close
        state.bucket = None
