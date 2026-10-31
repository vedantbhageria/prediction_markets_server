"""python -m unittest test_candles"""

import unittest
from datetime import datetime, timezone

from candles import CandleBuilder


def at(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc)


class CandleBuilderTest(unittest.TestCase):

    def test_samples_in_one_bucket_make_one_candle(self):
        b = CandleBuilder(5, 60)
        b.add("A", 100.5, quote=0.40)
        b.add("A", 102.0, quote=0.46)
        b.add("A", 104.0, quote=0.42)
        self.assertEqual(b.flush(104.9), [])          # bucket still open
        self.assertEqual(b.flush(105.0), [("A", at(105), 0.40, 0.46, 0.40, 0.42, None, None)])

    def test_open_is_the_previous_close(self):
        b = CandleBuilder(5, 60)
        b.add("A", 100, quote=0.40)
        b.add("A", 105, quote=0.50)
        first, second = b.flush(110)
        self.assertEqual(first[2:6], (0.40, 0.40, 0.40, 0.40))
        self.assertEqual(second[2:6], (0.40, 0.50, 0.40, 0.50))

    def test_unchanged_candles_are_skipped_until_the_heartbeat(self):
        b = CandleBuilder(5, 60)
        out = []
        for t in range(100, 235, 5):
            b.add("A", t, quote=0.40)
            out += b.flush(t)
        self.assertEqual([c[1] for c in out], [at(105), at(165), at(225)])

    def test_heartbeat_equal_to_width_keeps_every_candle(self):
        b = CandleBuilder(5, 5)
        out = []
        for t in range(100, 130, 5):
            b.add("A", t, quote=0.40)
            out += b.flush(t)
        self.assertEqual(len(out), 5)

    def test_volume_is_the_change_in_the_running_total(self):
        b = CandleBuilder(5, 60)
        b.add("A", 100, quote=0.40, trade=0.30, cum_volume=1000)   # baseline, not a trade
        b.add("A", 105, quote=0.40, trade=0.44, cum_volume=1025)   # 25 traded at 0.44
        b.add("A", 110, quote=0.41, trade=0.44, cum_volume=1025)   # quote only
        first, second, third = b.flush(115)
        self.assertEqual((first[5], first[6]), (0.40, 0.0))
        self.assertEqual((second[5], second[6]), (0.44, 25.0))
        self.assertEqual((third[5], third[6]), (0.41, 0.0))

    def test_trade_price_stands_in_when_there_is_no_quote(self):
        b = CandleBuilder(5, 60)
        b.add("A", 100, quote=None, trade=0.97, cum_volume=10)
        self.assertEqual(b.flush(105)[0][5], 0.97)

    def test_no_price_means_no_candle(self):
        b = CandleBuilder(5, 60)
        b.add("A", 100, quote=None, trade=None, cum_volume=10)
        self.assertEqual(b.flush(200), [])

    def test_open_interest_change_alone_emits(self):
        b = CandleBuilder(5, 60)
        b.add("A", 100, quote=0.40, cum_volume=5, open_interest=10)
        b.add("A", 105, quote=0.40, cum_volume=5, open_interest=12)
        self.assertEqual([c[7] for c in b.flush(110)], [10, 12])

    def test_start_stamp_and_separate_keys(self):
        b = CandleBuilder(5, 60, stamp="start")
        b.add("A", 101, quote=0.2)
        b.add("B", 102, quote=0.8)
        self.assertEqual(sorted((c[0], c[1]) for c in b.flush(105)),
                         [("A", at(100)), ("B", at(100))])

    def test_gap_between_samples_does_not_invent_candles(self):
        b = CandleBuilder(5, 60)
        b.add("A", 100, quote=0.40)
        b.add("A", 400, quote=0.45)
        out = b.flush(405)
        self.assertEqual([c[1] for c in out], [at(105), at(405)])
        self.assertEqual(out[1][2], 0.40)


if __name__ == "__main__":
    unittest.main()
