#!/usr/bin/env python3
"""
postcode_size_demand.py — predict tyre demand per postcode by FULL
tyre size (e.g. 215/45R17) with generation-aware matching.

Extends postcode_rim_demand.py two levels deeper:

  1. Read BITRE fleet — prefer the year-aware derived file
       out/rego/vehicle_postcode_make_model_year_estimate.csv
     (from vehicle_rego.py's proportional allocation of
      postcode_make_model × make_model_year), and fall back to
       out/rego/vehicle_postcode_make_model.csv
     when year isn't available.
  2. Read vehicle_tyre_fitment.csv (wheel-size.com crawl).
     Each fitment row now carries the generation start year of the
     latest-gen page it was scraped from (e.g. Ford Ranger T6.2 →
     gen_start_year=2022).
  3. Split fleet into TWO tiers per (make, model):
       - CURRENT-GEN   : year_of_manufacture >= gen_start_year - 1
                         → matched to the wheel-size fitment list
                           (BASE-OE = sorted[0]).
       - LEGACY        : older years → parked in a separate row so
                         the size prediction doesn't over-claim
                         accuracy for pre-generation vehicles.
     Without gen_start_year (older CSV or crawl miss), everything
     falls back to a global GEN_CUTOFF_YEAR heuristic.
  4. Aggregate to (state, postcode, size, gen, fleet_units).
  5. Write out/rego/postcode_size_demand.csv:
       state, postcode, size, gen, fleet_units, annual_tyres_est,
       coverage_pct

Why this matters for Ford Ranger:
  - PJ/PK  (2006-2011) : R16 mostly
  - PX/T6  (2011-2022) : R16 / R17 / R18
  - T6.2   (2022-now)  : R17 / R18 / R20 / R21
  The wheel-size crawler picks T6.2's fitments (latest gen), so a
  BITRE fleet row with year_of_manufacture=2015 shouldn't be matched
  to those — it belongs to a different generation.  Year filtering
  routes each vehicle to the right fitment list (or LEGACY bucket
  when we don't have older-gen fitments crawled).

"Base-OE only" policy inherited from postcode_rim_demand.py: pick
sorted[0] of the fitment list — first entry by string sort ends up
being the narrowest / smallest-rim fitment, which matches the entry-
trim OE spec for the vast majority of models.  BITRE doesn't ship
a trim breakdown so we can't split fleet across a model's multiple
factory fitments; upgrading trim mix awaits either a manual
per-model split table or a sales-weighted policy.
"""
from __future__ import annotations
import csv
import re
from collections import defaultdict
from pathlib import Path

BASE_DIR      = Path(__file__).resolve().parent
BITRE_YEAR    = BASE_DIR / "out" / "rego" / "vehicle_postcode_make_model_year_estimate.csv"
BITRE_NOYEAR  = BASE_DIR / "out" / "rego" / "vehicle_postcode_make_model.csv"
FIT_CSV       = BASE_DIR / "out" / "rego" / "vehicle_tyre_fitment.csv"
# Hand-curated OE fitments for ~150 top Australian models — used as
# the primary source when present, with the wheel-size crawl output
# filling in anything not covered.  Committed to git alongside the
# script; safe to hand-edit.  Same schema (make, model,
# gen_start_year, size) so both files pipe into the same parser.
FIT_MANUAL    = BASE_DIR / "out" / "rego" / "vehicle_tyre_fitment_manual.csv"
OUTPUT        = BASE_DIR / "out" / "rego" / "postcode_size_demand.csv"

# Fallback cutoff when a fitment row doesn't carry gen_start_year
# (older crawl output, or a model whose gen slug had no year token).
# Rough proxy for "mainstream Australian model's most-recent generation"
# — most 2019+ vehicles are still on the same body/trim family as
# what wheel-size.com serves as the "latest" gen today.
GEN_CUTOFF_YEAR = 2019
# Small buffer applied to the per-model gen_start_year — a vehicle
# built in the gen's launch year is definitely current-gen, but
# some overlap exists in the calendar year just before as ANCAP
# approvals / stock arrives.
GEN_START_BUFFER = 1


def normalize_key(make: str, model: str) -> tuple[str, str]:
    """Same normalisation as postcode_rim_demand.py."""
    def n(s: str) -> str:
        s = (s or "").strip().upper()
        s = re.sub(r"[_/]", " ", s)
        s = re.sub(r"\s+", " ", s)
        return s
    return (n(make), n(model))


def _read_fitment_file(path: Path,
                       sizes_by_key: dict[tuple[str, str], set[str]],
                       gen_by_key:   dict[tuple[str, str], int]):
    """Absorb one fitment CSV into the two accumulator dicts.  Later
    calls layer on top of earlier ones, so priority = call order."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            mk = (r.get("make")  or "").strip()
            md = (r.get("model") or "").strip()
            sz = (r.get("size")  or "").strip()
            gy = (r.get("gen_start_year") or "").strip()
            if not mk or not md:
                continue
            key = normalize_key(mk, md)
            if sz:
                sizes_by_key[key].add(sz)
            if gy and key not in gen_by_key:
                try:
                    gen_by_key[key] = int(gy)
                except ValueError:
                    pass


def load_fitment_base_oe() -> dict[tuple[str, str], tuple[str, int | None]]:
    """Return {(make, model) → (base_oe_size, gen_start_year_or_None)}.

    Sources — MANUAL first (definitive; wins on collision), then the
    wheel-size crawl.  Base-OE = the first entry when the model's
    non-empty sizes are sorted as strings — narrowest width first,
    which matches entry-trim OE spec for most models.  For MANUAL
    rows we curate one row per model so sorted[0] is exactly what
    we picked; for wheel-size rows we get whatever the crawler saw."""
    sizes_by_key: dict[tuple[str, str], set[str]] = defaultdict(set)
    gen_by_key:   dict[tuple[str, str], int]     = {}
    # MANUAL wins on collision because a bare model key already
    # tagged in gen_by_key won't be overwritten by the crawl.
    manual_loaded = False
    if FIT_MANUAL.exists():
        _read_fitment_file(FIT_MANUAL, sizes_by_key, gen_by_key)
        manual_loaded = True

    # For crawl entries: only fill sizes where MANUAL didn't already
    # cover them — otherwise the crawl's wider fitment list would
    # displace our curated base-OE via sorted[0].
    crawl_loaded = False
    if FIT_CSV.exists():
        manual_keys = set(sizes_by_key.keys())
        with open(FIT_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                mk = (r.get("make")  or "").strip()
                md = (r.get("model") or "").strip()
                sz = (r.get("size")  or "").strip()
                gy = (r.get("gen_start_year") or "").strip()
                if not mk or not md:
                    continue
                key = normalize_key(mk, md)
                if key in manual_keys:
                    continue  # curated already — skip crawl noise
                if sz:
                    sizes_by_key[key].add(sz)
                if gy and key not in gen_by_key:
                    try:
                        gen_by_key[key] = int(gy)
                    except ValueError:
                        pass
        crawl_loaded = True

    if not (manual_loaded or crawl_loaded):
        print(f"[fatal] neither fitment CSV found:\n"
              f"        {FIT_MANUAL}\n        {FIT_CSV}")
        return {}
    print(f"[info] fitment sources: "
          f"manual={'yes' if manual_loaded else 'no'} · "
          f"crawl={'yes' if crawl_loaded else 'no'}")

    out: dict[tuple[str, str], tuple[str, int | None]] = {}
    for key, sizes in sizes_by_key.items():
        out[key] = (sorted(sizes)[0], gen_by_key.get(key))
    return out


def pick_input_csv() -> tuple[Path, bool]:
    """Prefer the year-aware BITRE file if present; fall back to the
    year-less one.  Returns (path, has_year)."""
    if BITRE_YEAR.exists():
        return BITRE_YEAR, True
    if BITRE_NOYEAR.exists():
        return BITRE_NOYEAR, False
    return BITRE_NOYEAR, False   # caller checks exists()


def _gen_tier(year: int | None, gen_start: int | None) -> str:
    """Classify a fleet row as 'current' or 'legacy' relative to the
    fitment we have on file.  Missing year → 'unknown' (treated as
    current so we don't strand rows in a coverage limbo when the
    input CSV isn't year-aware)."""
    if year is None:
        return "unknown"
    cutoff = (gen_start - GEN_START_BUFFER) if gen_start else GEN_CUTOFF_YEAR
    return "current" if year >= cutoff else "legacy"


def main():
    src, has_year = pick_input_csv()
    if not src.exists():
        print(f"[fatal] BITRE fleet CSV not found under {src.parent}/")
        print(f"        expected {BITRE_YEAR.name} or {BITRE_NOYEAR.name}")
        return
    print(f"[info] input : {src.name} (year-aware={'yes' if has_year else 'no'})")

    fit_map = load_fitment_base_oe()
    if not fit_map:
        return
    n_with_gen = sum(1 for _, g in fit_map.values() if g is not None)
    print(f"[info] fitment: {len(fit_map):,} models  "
          f"({n_with_gen:,} with gen_start_year, "
          f"{len(fit_map) - n_with_gen:,} using cutoff={GEN_CUTOFF_YEAR})")

    # Aggregation keyed by (state, postcode, size, gen).
    agg: dict[tuple[str, str, str, str], int] = defaultdict(int)
    per_postcode_total:   dict[tuple[str, str], int] = defaultdict(int)
    per_postcode_matched: dict[tuple[str, str], int] = defaultdict(int)
    total_fleet   = 0
    matched_fleet = 0
    legacy_fleet  = 0
    unmatched_top_qty: dict[tuple[str, str], int] = defaultdict(int)

    with open(src, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            mk    = (r.get("make")     or "").strip()
            md    = (r.get("model")    or "").strip()
            state = (r.get("state")    or "").strip().upper()
            pc    = (r.get("postcode") or "").strip()
            year_raw = (r.get("year_of_manufacture") or "").strip()
            try:
                year = int(year_raw) if year_raw else None
            except ValueError:
                year = None
            try:
                qty = int(r.get("qty") or 0)
            except ValueError:
                qty = 0
            if qty <= 0 or not pc:
                continue

            total_fleet += qty
            per_postcode_total[(state, pc)] += qty

            key = normalize_key(mk, md)
            hit = fit_map.get(key)
            if not hit:
                agg[(state, pc, "UNKNOWN", "unknown")] += qty
                unmatched_top_qty[(mk, md)] += qty
                continue

            size, gen_start = hit
            tier = _gen_tier(year, gen_start)
            if tier == "legacy":
                # We only crawled the latest gen — matching an older
                # vehicle to it would over-claim.  Keep it as a
                # LEGACY bucket at the model level so the fleet total
                # still balances and users can see the size of the
                # pre-gen tail per postcode.
                agg[(state, pc, "LEGACY", "legacy")] += qty
                legacy_fleet += qty
                continue

            agg[(state, pc, size, tier)] += qty
            per_postcode_matched[(state, pc)] += qty
            matched_fleet += qty

    cov = (matched_fleet / total_fleet * 100.0) if total_fleet else 0.0
    print(f"[info] total fleet loaded  : {total_fleet:,}")
    print(f"[info] matched (current-gen): {matched_fleet:,} ({cov:.1f} %)")
    print(f"[info] legacy (pre-gen)    : {legacy_fleet:,}")
    print(f"[info] unmatched (no map)  : {total_fleet - matched_fleet - legacy_fleet:,}")

    if unmatched_top_qty:
        print("\n[extend fitment] top 20 uncovered (make, model) by fleet:")
        top = sorted(unmatched_top_qty.items(), key=lambda kv: kv[1], reverse=True)[:20]
        for (mk, md), q in top:
            print(f"    {q:>12,}   {mk} · {md}")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["state", "postcode", "size", "gen", "fleet_units",
                    "annual_tyres_est", "coverage_pct"])
        for (state, pc, size, gen), fleet in sorted(agg.items()):
            total   = per_postcode_total[(state, pc)]
            matched = per_postcode_matched[(state, pc)]
            pc_cov  = (matched / total * 100.0) if total else 0.0
            # Same coarse annual-replacement assumption as the rim
            # aggregator — one full 4-tyre set replaced every 4 years
            # → annual tyre-units = fleet_units.
            w.writerow([state, pc, size, gen, fleet, fleet, f"{pc_cov:.1f}"])

    print(f"\n[done] wrote {OUTPUT}  ({len(agg):,} rows)")

    # National top-20 sizes — current-gen only, so the shape reflects
    # what wheel-size actually mapped.  Compares directly against what
    # Hankook sells today.
    dist: dict[str, int] = defaultdict(int)
    for (_, _, size, gen), q in agg.items():
        if gen != "current" or size in ("UNKNOWN", "LEGACY"):
            continue
        dist[size] += q
    total = sum(dist.values()) or 1
    print("\n[national top 20 current-gen sizes]")
    top = sorted(dist.items(), key=lambda kv: kv[1], reverse=True)[:20]
    for size, q in top:
        pct = q / total * 100.0
        print(f"    {size:>16s}  {q:>12,}  ({pct:5.2f} %)")


if __name__ == "__main__":
    main()
