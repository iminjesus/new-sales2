#!/usr/bin/env python3
"""
postcode_size_demand.py — predict tyre demand per postcode by FULL
tyre size (e.g. 215/45R17) instead of just rim family (R17).

Extends postcode_rim_demand.py one level deeper:
  1. Read out/rego/vehicle_postcode_make_model.csv (BITRE fleet).
  2. Read out/rego/vehicle_tyre_fitment.csv (wheel-size.com crawl).
  3. For each (make, model), look up its BASE-OE size — the first
     entry in the sorted fitment list, which by wheel-size.com's
     alphanumeric ordering is the narrowest / smallest-rim fitment
     (matches the entry-trim OE spec for the vast majority of models).
  4. Aggregate to (state, postcode, size, fleet_units).
  5. Write out/rego/postcode_size_demand.csv:
       state, postcode, size, fleet_units, annual_tyres_est,
       coverage_pct

Why "base-OE only" (same philosophy as postcode_rim_demand.py):
  BITRE doesn't ship a trim breakdown, so we can't split fleet
  proportionally across a model's multiple factory fitments.  Picking
  the base fitment for every unit is a deliberate simplification —
  covers the largest share of the fleet, and even up-sized owners
  usually replace within one width step so the demand signal survives.

Handling of the fitment CSV:
  - Deduped in-memory on (make, model, size) — the wheel-size crawler
    is append-mode and a crash + rerun without --resume can double-
    write the prefix.  Deduping here keeps this script correct
    regardless of the crawler's state.
  - Empty size rows (crawler couldn't find fitment for a model) are
    ignored — those (make, model) pairs still count toward the fleet
    total under the "UNKNOWN" size bucket, so a low-coverage area is
    visible in the output.

Extending:
  Run the script — the tail prints the top 20 uncovered (make, model)
  pairs by fleet volume.  Rerun the wheel-size crawler after adding
  those MODEL_ALIASES / MAKE_ALIASES entries and coverage climbs.
"""
from __future__ import annotations
import csv
import re
from collections import defaultdict
from pathlib import Path

BASE_DIR  = Path(__file__).resolve().parent
BITRE_CSV = BASE_DIR / "out" / "rego" / "vehicle_postcode_make_model.csv"
FIT_CSV   = BASE_DIR / "out" / "rego" / "vehicle_tyre_fitment.csv"
OUTPUT    = BASE_DIR / "out" / "rego" / "postcode_size_demand.csv"


def normalize_key(make: str, model: str) -> tuple[str, str]:
    """Same normalisation as postcode_rim_demand.py so the two lookup
    maps line up when we cross-reference.  Uppercase; collapse
    consecutive spaces; treat '-', '_' and '/' as spaces so
    MERCEDES-BENZ / MERCEDES BENZ / MERCEDES_BENZ all match."""
    def n(s: str) -> str:
        s = (s or "").strip().upper()
        s = re.sub(r"[_/]", " ", s)
        s = re.sub(r"\s+", " ", s)
        return s
    return (n(make), n(model))


def load_fitment_base_oe() -> dict[tuple[str, str], str]:
    """Return {(make, model) → base-OE size string}.

    Base-OE = the first entry when the model's sizes are sorted as
    strings.  For "215/70R16, 255/55R20, 255/65R17, …" that pulls
    the narrowest width first (215) which almost always corresponds
    to the entry-trim OE spec (Ranger XL = R16, Wildtrak = R18/R20)."""
    if not FIT_CSV.exists():
        print(f"[fatal] fitment CSV not found: {FIT_CSV}")
        print("        run vehicle_tyre_fitment.py first to produce it.")
        return {}

    # Collect the full size set per (make, model) so we can sort and
    # take the first one deterministically — the CSV is append-mode
    # written and may have rows out of order after resume-crashes.
    sizes_by_key: dict[tuple[str, str], set[str]] = defaultdict(set)
    with open(FIT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            mk = (r.get("make")  or "").strip()
            md = (r.get("model") or "").strip()
            sz = (r.get("size")  or "").strip()
            if not mk or not md or not sz:
                continue
            key = normalize_key(mk, md)
            sizes_by_key[key].add(sz)

    base_oe: dict[tuple[str, str], str] = {}
    for key, sizes in sizes_by_key.items():
        base_oe[key] = sorted(sizes)[0]
    return base_oe


def main():
    if not BITRE_CSV.exists():
        print(f"[fatal] BITRE fleet CSV not found: {BITRE_CSV}")
        return

    fit_map = load_fitment_base_oe()
    if not fit_map:
        return
    print(f"[info] loaded fitment map: {len(fit_map):,} (make, model) pairs")

    agg: dict[tuple[str, str, str], int] = defaultdict(int)
    per_postcode_total:   dict[tuple[str, str], int] = defaultdict(int)
    per_postcode_matched: dict[tuple[str, str], int] = defaultdict(int)
    total_fleet   = 0
    matched_fleet = 0
    unmatched_top_qty: dict[tuple[str, str], int] = defaultdict(int)

    with open(BITRE_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            mk    = (r.get("make")     or "").strip()
            md    = (r.get("model")    or "").strip()
            state = (r.get("state")    or "").strip().upper()
            pc    = (r.get("postcode") or "").strip()
            try:
                qty = int(r.get("qty") or 0)
            except ValueError:
                qty = 0
            if qty <= 0 or not pc:
                continue

            total_fleet += qty
            per_postcode_total[(state, pc)] += qty

            size = fit_map.get(normalize_key(mk, md))
            if not size:
                # No fitment for this (make, model) — parked in a
                # single UNKNOWN bucket per postcode so a low-coverage
                # area is still visible in the output.
                agg[(state, pc, "UNKNOWN")] += qty
                unmatched_top_qty[(mk, md)] += qty
                continue

            agg[(state, pc, size)] += qty
            per_postcode_matched[(state, pc)] += qty
            matched_fleet += qty

    cov = (matched_fleet / total_fleet * 100.0) if total_fleet else 0.0
    print(f"[info] total fleet loaded : {total_fleet:,}")
    print(f"[info] matched to fitment : {matched_fleet:,} ({cov:.1f} %)")
    print(f"[info] unmatched          : {total_fleet - matched_fleet:,}")

    if unmatched_top_qty:
        print("\n[extend fitment] top 20 uncovered (make, model) by fleet:")
        top = sorted(unmatched_top_qty.items(), key=lambda kv: kv[1], reverse=True)[:20]
        for (mk, md), q in top:
            print(f"    {q:>12,}   {mk} · {md}")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["state", "postcode", "size", "fleet_units",
                    "annual_tyres_est", "coverage_pct"])
        for (state, pc, size), fleet in sorted(agg.items()):
            total   = per_postcode_total[(state, pc)]
            matched = per_postcode_matched[(state, pc)]
            pc_cov  = (matched / total * 100.0) if total else 0.0
            # Same coarse annual-replacement assumption as the rim
            # aggregator — every vehicle runs one full 4-tyre set
            # replaced every 4 years → annual tyre-units = fleet_units.
            w.writerow([state, pc, size, fleet, fleet, f"{pc_cov:.1f}"])

    print(f"\n[done] wrote {OUTPUT}  ({len(agg):,} rows)")

    # National size distribution for the covered fleet — top 20 sizes
    # so you can eyeball whether the shape matches the Australian
    # market (215/60R16, 235/60R18, 265/65R17 should be near the top).
    dist: dict[str, int] = defaultdict(int)
    for (_, _, size), q in agg.items():
        if size == "UNKNOWN":
            continue
        dist[size] += q
    total = sum(dist.values()) or 1
    print("\n[national top 20 sizes]")
    top = sorted(dist.items(), key=lambda kv: kv[1], reverse=True)[:20]
    for size, q in top:
        pct = q / total * 100.0
        print(f"    {size:>16s}  {q:>12,}  ({pct:5.2f} %)")


if __name__ == "__main__":
    main()
