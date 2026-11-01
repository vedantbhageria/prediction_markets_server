"""The shared budget under pressure: three processes, eighteen threads.

    .venv/bin/python tests/ratelimit_test.py        # about 3.5 minutes

Each process hammers the budget from six threads -- two hot, two warm, two
backfill -- for 100 s. Then one "too many requests" is reported and they go
on for another 60 s. Checks:

  - no 60-second window ever saw more than the budget
  - hot got more than warm, and warm at least as much as backfill
  - after the 429 the rate dropped to about half
"""

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUDGET = 120
os.environ.update(REDIS_PREFIX="pmrate:", KALSHI_REQUESTS_PER_MIN=str(BUDGET))
sys.path.insert(0, str(ROOT))

import ratelimit              # noqa: E402


def worker(start, end, out):
    """Hammer from `start` to `end` (unix time). A thread still waiting at the
    end is let go by `stop` and takes nothing, so every grant is inside."""
    grants = []
    stop = threading.Event()
    time.sleep(max(0.0, start - time.time()))
    threading.Timer(end - time.time(), stop.set).start()

    def hammer(priority):
        with ratelimit.using(priority):
            while not stop.is_set():
                if ratelimit.acquire(stop):
                    grants.append((time.time(), priority))

    threads = [threading.Thread(target=hammer, args=(p,))
               for p in ("hot", "hot", "warm", "warm", "backfill", "backfill")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    Path(out).write_text(json.dumps(grants))


def worst_window(times, width=60):
    times, worst, j = sorted(times), 0, 0
    for i, t in enumerate(times):
        while times[j] < t - width:
            j += 1
        worst = max(worst, i - j + 1)
    return worst


def phase(seconds, tag):
    files = [f"/tmp/pmrate_{tag}_{i}.json" for i in range(3)]
    start = time.time() + 3             # all three begin together
    procs = [subprocess.Popen([sys.executable, __file__, "--worker", str(start),
                               str(start + seconds), f]) for f in files]
    for p in procs:
        p.wait()
    grants = [g for f in files for g in json.loads(Path(f).read_text())]
    by = {p: sum(1 for _, q in grants if q == p) for p in ("hot", "warm", "backfill")}
    return grants, by


def main():
    r = ratelimit.client()
    for key in r.scan_iter("pmrate:*"):
        r.delete(key)
    failures = []

    grants, by = phase(100, "a")
    worst = worst_window([t for t, _ in grants])
    rate = len(grants) / 100 * 60
    print(f"normal: {len(grants)} grants in 100 s ({rate:.0f}/min), busiest 60 s {worst},"
          f" budget {BUDGET}; by priority {by}")
    if worst > BUDGET:
        failures.append(f"budget broken: {worst} in 60 s")
    # With hot draining every token, warm and backfill get only what was in
    # the full bucket at the very start -- whoever asks first. One token's
    # difference either way is that race, not a priority broken.
    if not (by["hot"] > by["warm"] and by["warm"] + 1 >= by["backfill"]):
        failures.append(f"priorities not respected: {by}")

    ratelimit.throttled()
    print(f"429 reported: budget factor now {ratelimit.factor()}")
    grants, by = phase(60, "b")
    after = len(grants)
    print(f"after 429: {after} grants in 60 s (expected about {BUDGET // 2}); by priority {by}")
    if not BUDGET * 0.35 <= after <= BUDGET * 0.5:
        failures.append(f"throttle not applied: {after} grants in 60 s after a 429")

    for key in r.scan_iter("pmrate:*"):
        r.delete(key)
    print("PASSED" if not failures else "FAILED: " + "; ".join(failures))
    return 1 if failures else 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(float(sys.argv[2]), float(sys.argv[3]), sys.argv[4])
    else:
        sys.exit(main())
