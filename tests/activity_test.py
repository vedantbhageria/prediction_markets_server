"""The activity rules, case by case, in a schema of their own.

    .venv/bin/python tests/activity_test.py
"""

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

os.environ["PGSCHEMA"] = "pm_unit_test"
os.environ["REDIS_PREFIX"] = "pmunit:"
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import activity               # noqa: E402
import db                     # noqa: E402
import store                  # noqa: E402

NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)


def ago(minutes):
    return NOW - timedelta(minutes=minutes)


def market(ticker, opened, closes, volume, status="active"):
    db.execute("INSERT INTO kalshi_markets (ticker, event_ticker, status, open_time, close_time,"
               " volume, raw) VALUES (%s, 'E', %s, %s, %s, %s, '{}')",
               (ticker, status, opened, closes, volume))


def scan(readings, finished=True):
    """A scan with these readings: [(ticker, volume, asked, answered)]."""
    sid = db.query("INSERT INTO kalshi_scans (finished_at) VALUES (%s) RETURNING scan_id",
                   (NOW if finished else None,))[0]["scan_id"]
    for ticker, volume, asked, answered in readings:
        db.execute("INSERT INTO kalshi_observations VALUES (%s, %s, %s, %s, %s)",
                   (sid, ticker, volume, asked, answered))
    return sid


def row(ticker):
    found = db.query("SELECT * FROM kalshi_tracking WHERE ticker = %s", (ticker,))
    return found[0] if found else None


class Activity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db.execute("DROP SCHEMA IF EXISTS pm_unit_test CASCADE")
        db.execute("CREATE SCHEMA pm_unit_test")
        store.ensure_schema()

    def setUp(self):
        db.execute("TRUNCATE kalshi_tracking, kalshi_observations, kalshi_scans,"
                   " kalshi_markets CASCADE")
        db.execute("INSERT INTO kalshi_series VALUES ('S') ON CONFLICT DO NOTHING")
        db.execute("INSERT INTO kalshi_events (event_ticker, series_ticker, raw)"
                   " VALUES ('E', 'S', '{}') ON CONFLICT DO NOTHING")

    def score_all(self):
        for sid in activity.waiting_scans():
            activity.score_scan(sid)

    def test_new_markets_start_right(self):
        market("NEVER", ago(600), ago(-600), 0)
        market("OLD", ago(6000), ago(-600), 1200)
        market("YOUNG", ago(30), ago(-600), 90)
        scan([("NEVER", 0, ago(12), ago(9)), ("OLD", 1200, ago(12), ago(9)),
              ("YOUNG", 90, ago(12), ago(9))])
        self.score_all()

        never = row("NEVER")
        self.assertEqual(never["candles_fetched_from"], ago(600))       # complete from the open
        self.assertEqual(never["candles_fetched_to"], ago(12))
        self.assertEqual(never["volume_measured_at"], ago(12))
        self.assertEqual(never["activity_score"], 0)

        old = row("OLD")                                               # live from here
        self.assertEqual(old["candles_fetched_from"], ago(12))
        self.assertEqual(old["candles_fetched_to"], ago(12))
        self.assertEqual(old["volume_measured_at"], ago(9))             # so it is warm
        self.assertAlmostEqual(old["activity_score"], 1200 / (6000 - 9))  # volume / time open

        young = row("YOUNG")                                           # live fetches it all
        self.assertEqual(young["candles_fetched_from"], ago(30))
        self.assertEqual(young["candles_fetched_to"], ago(30))
        self.assertAlmostEqual(young["activity_score"], 90 / 21)

    def test_closed_markets_need_nothing_after_close(self):
        market("SETTLED", ago(3000), ago(1000), 500, "settled")        # never seen in a scan
        market("SEEN", ago(3000), ago(1000), 700, "finalized")         # seen by the closed import
        market("UNTRADED", ago(3000), ago(1000), 0, "settled")
        scan([("SEEN", 700, ago(12), ago(9))])
        self.score_all()
        for t in ("SETTLED", "SEEN"):
            r = row(t)
            self.assertEqual(r["candles_fetched_from"], ago(1000), t)
            self.assertEqual(r["candles_fetched_to"], ago(1000), t)
            self.assertEqual(r["volume_measured_at"], ago(1000), t)    # not warm
        self.assertEqual(row("UNTRADED")["candles_fetched_from"], ago(3000))

    def test_quiet_markets_advance_scan_after_scan(self):
        market("Q", ago(600), ago(-600), 0)
        scan([("Q", 0, ago(42), ago(40))])
        scan([("Q", 0, ago(27), ago(25))])
        scan([("Q", 0, ago(12), ago(10))])
        self.score_all()
        r = row("Q")
        self.assertEqual(r["candles_fetched_to"], ago(12))             # the chain held
        self.assertEqual(r["volume_measured_at"], ago(12))

    def test_unchanged_volume_is_not_enough_without_coverage(self):
        # Seen with trades, never fetched: the trades between its ask and its
        # answer may be missing, so no change must not move coverage.
        market("W", ago(6000), ago(-600), 50)
        scan([("W", 50, ago(27), ago(25))])
        scan([("W", 50, ago(12), ago(10))])
        self.score_all()
        r = row("W")
        self.assertEqual(r["candles_fetched_to"], ago(27))
        self.assertEqual(r["volume_measured_at"], ago(10))             # still warm

    def test_fetched_past_the_answer_then_quiet(self):
        market("F", ago(6000), ago(-600), 50)
        scan([("F", 50, ago(27), ago(25))])
        self.score_all()
        db.execute("UPDATE kalshi_tracking SET candles_fetched_to = %s WHERE ticker = 'F'",
                   (ago(20),))                                          # live fetched past it
        scan([("F", 50, ago(12), ago(10))])
        self.score_all()
        r = row("F")
        self.assertEqual(r["candles_fetched_to"], ago(12))
        self.assertEqual(r["volume_measured_at"], ago(12))

    def test_fetched_inside_the_reading_is_not_enough(self):
        # Coverage that stops between a reading's ask and its answer does not
        # prove the reading's trades are stored.
        market("G", ago(6000), ago(-600), 50)
        scan([("G", 50, ago(27), ago(25))])
        self.score_all()
        db.execute("UPDATE kalshi_tracking SET candles_fetched_to = %s WHERE ticker = 'G'",
                   (ago(26),))
        scan([("G", 50, ago(12), ago(10))])
        self.score_all()
        self.assertEqual(row("G")["candles_fetched_to"], ago(26))

    def test_score_is_a_one_hour_average(self):
        market("S", ago(6000), ago(-600), 0)
        scan([("S", 0, ago(32), ago(30))])
        scan([("S", 300, ago(17), ago(15))])                           # 300 in 15 minutes
        self.score_all()
        # dt runs from the last measurement (the first reading's ask, since it
        # had no volume) to this answer: 17 minutes.
        w = 1 - 2 ** (-17 / 60)
        self.assertAlmostEqual(row("S")["activity_score"], w * 300 / 17, places=6)
        self.assertEqual(row("S")["candles_fetched_to"], ago(32))      # moved: no advance

    def test_volume_going_down_counts_as_a_change(self):
        market("D", ago(600), ago(-600), 10)
        scan([("D", 10, ago(32), ago(30))])
        self.score_all()
        db.execute("UPDATE kalshi_tracking SET candles_fetched_to = %s WHERE ticker = 'D'",
                   (ago(29),))
        scan([("D", 7, ago(17), ago(15))])
        self.score_all()
        r = row("D")
        self.assertEqual(r["candles_fetched_to"], ago(29))
        self.assertEqual(r["volume_measured_at"], ago(15))
        self.assertGreaterEqual(r["activity_score"], 0)

    def test_two_readings_in_one_scan_take_the_later(self):
        market("T", ago(600), ago(-600), 5)
        scan([("T", 3, ago(14), ago(13)), ("T", 5, ago(12), ago(11))])
        self.score_all()
        self.assertEqual(row("T")["volume"], 5)

    def test_abandoned_scans_are_scored_but_a_running_one_waits(self):
        market("A", ago(600), ago(-600), 0)
        first = scan([("A", 0, ago(30), ago(29))], finished=False)     # discover died
        scan([("A", 0, ago(15), ago(14))], finished=False)             # still running
        self.assertEqual(activity.waiting_scans(), [first])

    def test_hot_list_respects_cap_and_hysteresis(self):
        for i, score in enumerate([5, 3, 2, 0.8, 0.6, 0.2]):
            market(f"H{i}", ago(600), ago(-600), 0)
            db.execute("INSERT INTO kalshi_tracking VALUES (%s, 0, %s, %s, %s, %s)",
                       (f"H{i}", NOW, score, NOW, NOW))
        self.assertEqual(activity.pick_hot(2), ["H0", "H1"])           # the cap
        self.assertEqual(activity.pick_hot(10), ["H0", "H1", "H2"])    # enter at 1
        self.assertEqual(activity.pick_hot(10, ["H3", "H5"]), ["H0", "H1", "H2", "H3"])  # stay at 0.5


if __name__ == "__main__":
    outcome = unittest.main(verbosity=2, exit=False).result
    if outcome.wasSuccessful():
        db.execute("DROP SCHEMA IF EXISTS pm_unit_test CASCADE")   # kept only when a test fails
    sys.exit(0 if outcome.wasSuccessful() else 1)
