#!/usr/bin/env python3
"""
build_price_dashboard_cache.py — pre-compute the /price dashboard's
data payload and dump it to the on-disk cache so the next HTTP
request serves instantly.

Why: the /price endpoint has to walk every source CSV (Tempe /
Tempeweborder / BobJane / JAX / NewbeeWeborder) and build a
~2,000-combo matrix with per-month, per-brand, per-store, per-
pattern slots every time its cache misses.  Before this pre-build
step, that cache miss happened on every Flask restart and every
first HTTP request after a crawler drop -- long enough that the
user's browser occasionally timed out.

How: load the same data pipeline price_compare.py's /price endpoint
uses (load_all_months + build_data), then serialise to the
.price_dashboard_cache.pkl.gz file alongside the source CSVs.
Keyed by a tuple of (basename, mtime) for every source CSV, so a
fresh crawl automatically invalidates the stale cache.

Usage (the crawler should call this on its way out):

    python build_price_dashboard_cache.py

Add it as the last line of the batch crawler wrapper, or run it
manually after any CSV edit.  Idempotent + safe to re-run.
"""
from __future__ import annotations

import sys
import time


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.strip() if __doc__ else "")
    ap.add_argument("--force", action="store_true",
                    help="Rebuild the cache even when the on-disk fingerprint "
                         "already matches the current source CSVs.  Default "
                         "behaviour is to no-op in that case -- the "
                         "orchestrator hook calls this on every pipeline "
                         "success and the price CSVs don't change unless a "
                         "crawler actually ran.")
    args = ap.parse_args(argv)

    t0 = time.monotonic()
    # Import deferred so --help works even if openpyxl / Flask aren't
    # installed in this environment.
    from price_compare import (
        _price_cache_key,
        load_all_months,
        build_data,
        save_price_cache_to_disk,
        load_price_cache_from_disk,
    )
    key = _price_cache_key()
    if not key:
        print("[build_cache] no source CSVs found; nothing to do.",
              file=sys.stderr)
        return 2

    # Nov-04 user ask: skip when the disk cache's stored fingerprint
    # already matches the current source CSVs.  The orchestrator
    # hook calls this on every pipeline tick, but the price CSVs
    # only change on manual crawler runs, so the vast majority of
    # invocations no-op in ~5 ms instead of paying the full ~15 s
    # build cost.  `--force` bypasses the skip for manual rebuilds.
    if not args.force:
        _dkey, _ = load_price_cache_from_disk()
        if _dkey == key:
            print(f"[build_cache] disk cache already matches "
                  f"({len(key)} csv fingerprints) — skip "
                  f"(--force to rebuild anyway)")
            return 0

    monthly = load_all_months()
    if not monthly:
        print("[build_cache] no source CSVs found; nothing to do.",
              file=sys.stderr)
        return 2
    print(f"[build_cache] {len(key)} csv fingerprints -> building data...",
          flush=True)
    data = build_data(monthly)
    print(f"[build_cache] build_data done in {time.monotonic() - t0:.1f}s; "
          f"writing disk cache...", flush=True)
    ok = save_price_cache_to_disk(key, data)
    # Nov-04: drop large in-process references before returning so
    # the orchestrator hook (which imports + calls main() in-
    # process) sees the ~100-200 MB transient build footprint get
    # garbage-collected instead of lingering in the long-lived
    # orchestrator RSS.
    del data, monthly
    import gc as _gc
    _gc.collect()
    if ok:
        print(f"[build_cache] done in {time.monotonic() - t0:.1f}s total.")
        return 0
    print("[build_cache] FAILED to write disk cache", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
