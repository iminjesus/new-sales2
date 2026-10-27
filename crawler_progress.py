"""
crawler_progress.py — shared 10%-step progress reporter for all
*_crawling*.py scripts.

Usage:
    from crawler_progress import Progress
    prog = Progress("tempe_crawling", total_steps=len(BRANDS))
    for i, brand in enumerate(BRANDS, 1):
        prog.step(i)
        ...                     # existing scrape work
    prog.finish()

What it prints (sample):
    [progress] tempe_crawling: started (last 5 runs averaged ~318 s)
    [progress] tempe_crawling: 10% (1/8)  elapsed 42 s  est. remaining ~276 s
    [progress] tempe_crawling: 20% (2/8)  elapsed 73 s  est. remaining ~245 s
    ...
    [progress] tempe_crawling: 100% done in 311 s

Progress buckets fire at 10 % increments so a crawler that iterates
hundreds of items doesn't spam one line per item — just a milestone
every ~10 % of the way through.

Historical durations: every finish() call appends the actual run
duration into `.crawler_progress.json` beside this file.  The next
run reads the last 5 entries, averages them, and uses that as an
ETA baseline — overriding the straight-line extrapolation from
`current / total` which is noisy during the start-up phase.
"""
import os
import json
import time

_HIST_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          ".crawler_progress.json")
_HIST_WINDOW = 5    # average the last N runs for the ETA baseline
_HIST_KEEP   = 10   # trim history to the last N entries so the file stays small


def _load_history():
    try:
        with open(_HIST_FILE, "r") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save_history(data):
    try:
        with open(_HIST_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[progress] history save failed: {e}")


class Progress:
    """One-shot progress tracker for a single crawler run."""

    def __init__(self, name, total_steps=None, *, label=None):
        self.name = name
        self.label = label or name
        self.total = total_steps      # may be None at start if unknown
        self.start = time.time()
        self.last_bucket = -1

        hist = _load_history().get(name, [])
        self.hist_avg = (sum(hist[-_HIST_WINDOW:]) / len(hist[-_HIST_WINDOW:])
                         if hist else None)

        if self.hist_avg:
            print(f"[progress] {self.label}: started "
                  f"(last {len(hist[-_HIST_WINDOW:])} run(s) averaged "
                  f"~{self.hist_avg:.0f} s)")
        else:
            print(f"[progress] {self.label}: started "
                  f"(no history yet — ETA will warm up after the first bucket)")

    def set_total(self, total):
        """Call this when the total only becomes known after a first
        scrape (e.g. page count of a brand catalog)."""
        self.total = total

    def step(self, current, total=None):
        """Report "current" items processed out of "total" (or the
        stored total).  Prints at most once per 10 % bucket."""
        if total is not None:
            self.total = total
        if not self.total:
            return
        try:
            pct = int(current / self.total * 100)
        except Exception:
            return
        bucket = max(0, min(100, (pct // 10) * 10))
        if bucket <= self.last_bucket:
            return
        self.last_bucket = bucket
        elapsed = time.time() - self.start

        # ETA: prefer the historical average when we have one AND the
        # current run is still broadly on track with it (within ±60 %);
        # otherwise fall back to straight-line extrapolation.
        est_remain = None
        if self.hist_avg and 0 < bucket < 100:
            hist_remain = max(0.0, self.hist_avg - elapsed)
            extrapolated = (elapsed / (pct / 100)) - elapsed if pct > 0 else None
            if extrapolated is not None:
                ratio = extrapolated / self.hist_avg if self.hist_avg else 1
                est_remain = hist_remain if 0.4 <= ratio <= 1.6 else extrapolated
            else:
                est_remain = hist_remain
        elif 0 < bucket < 100 and pct > 0:
            est_remain = max(0.0, (elapsed / (pct / 100)) - elapsed)

        _rem = f"  est. remaining ~{est_remain:.0f} s" if est_remain is not None else ""
        print(f"[progress] {self.label}: {bucket}% ({current}/{self.total})  "
              f"elapsed {elapsed:.0f} s{_rem}")

    def tick(self, done_fraction):
        """Alternative API: pass a float 0..1 directly when the caller
        wants to blend work from multiple nested loops into one number
        (e.g. (brand_idx + page_frac) / brand_count)."""
        if not (0 <= done_fraction <= 1):
            return
        self.step(int(done_fraction * 1000), total=1000)

    def finish(self):
        duration = time.time() - self.start
        print(f"[progress] {self.label}: 100% done in {duration:.0f} s")
        data = _load_history()
        hist = data.get(self.name, [])
        hist.append(round(duration, 1))
        data[self.name] = hist[-_HIST_KEEP:]
        _save_history(data)
