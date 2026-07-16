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


def _read_gen_rows(path: Path,
                   per_pair: dict[tuple[str, str], dict[int | None, list[str]]]):
    """Load one fitment CSV into a {(make, model): {gen_start_year: [size, ...]}}
    accumulator.  Multiple rows for the same (make, model, gen_start_year)
    stack their sizes in the list; the per-gen base-OE is then sorted[0]
    of that list (narrowest width first = entry-trim base)."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            mk = (r.get("make")  or "").strip()
            md = (r.get("model") or "").strip()
            sz = (r.get("size")  or "").strip()
            gy = (r.get("gen_start_year") or "").strip()
            if not mk or not md or not sz:
                continue
            key = normalize_key(mk, md)
            gy_int: int | None = None
            if gy:
                try: gy_int = int(gy)
                except ValueError: pass
            per_pair.setdefault(key, {}).setdefault(gy_int, []).append(sz)


def load_fitment_gens() -> dict[tuple[str, str], list[tuple[int | None, str]]]:
    """Return {(make, model) → [(gen_start_year, base_oe_size), ...]}
    sorted DESCENDING by gen_start_year (newest gen first).

    Sources — MANUAL first (definitive; wins on collision), then the
    wheel-size crawl.  Manual rows can carry one row per generation
    for models whose base-OE changed between gens (Ford Ranger:
    R15 → R16 → R17 as PJ → PX → T6.2), and each gen's base is
    picked as sorted[0] of that gen's sizes (narrowest width first,
    matching entry-trim spec).  Models with a single unchanged base
    across gens just get one row with the OLDEST relevant year."""
    per_pair: dict[tuple[str, str], dict[int | None, list[str]]] = {}
    manual_loaded = False
    if FIT_MANUAL.exists():
        _read_gen_rows(FIT_MANUAL, per_pair)
        manual_loaded = True

    crawl_loaded = False
    if FIT_CSV.exists():
        manual_keys = set(per_pair.keys())
        # Sniff crawl into a scratch dict, then only fold in pairs
        # the manual CSV didn't cover — a curated (make, model) shouldn't
        # be diluted by the crawler's wider list.
        scratch: dict[tuple[str, str], dict[int | None, list[str]]] = {}
        _read_gen_rows(FIT_CSV, scratch)
        for key, gen_map in scratch.items():
            if key in manual_keys:
                continue
            per_pair[key] = gen_map
        crawl_loaded = True

    if not (manual_loaded or crawl_loaded):
        print(f"[fatal] neither fitment CSV found:\n"
              f"        {FIT_MANUAL}\n        {FIT_CSV}")
        return {}
    print(f"[info] fitment sources: "
          f"manual={'yes' if manual_loaded else 'no'} · "
          f"crawl={'yes' if crawl_loaded else 'no'}")

    out: dict[tuple[str, str], list[tuple[int | None, str]]] = {}
    for key, gen_map in per_pair.items():
        gens = [(gy, sorted(sizes)[0]) for gy, sizes in gen_map.items() if sizes]
        # Sort newest-first so _pick_gen can walk it top-down.  None
        # gens go last so we prefer year-tagged rows when available.
        gens.sort(key=lambda g: (g[0] if g[0] is not None else -1), reverse=True)
        out[key] = gens
    return out


def pick_input_csv() -> tuple[Path, bool]:
    """Prefer the year-aware BITRE file if present; fall back to the
    year-less one.  Returns (path, has_year)."""
    if BITRE_YEAR.exists():
        return BITRE_YEAR, True
    if BITRE_NOYEAR.exists():
        return BITRE_NOYEAR, False
    return BITRE_NOYEAR, False   # caller checks exists()


def _pick_gen(year: int | None,
              gens: list[tuple[int | None, str]]
              ) -> tuple[str | None, str]:
    """Pick the best matching fitment for a fleet row.

    Returns (size, tier):
      * ("215/70R16", "current") — matched to newest gen the vehicle
        qualifies for (year >= gen_start - buffer)
      * ("215/70R16", "prev-gen") — matched to an older-than-newest
        gen (year fell into an older gen's window)
      * (size,       "unknown")  — no year info; falls back to newest
        gen so the fleet unit isn't stranded
      * (None,       "legacy")   — vehicle year is older than the
        oldest gen we've curated → LEGACY bucket, no size predicted
      * (None,       "unknown")  — no gens on file at all

    gens is sorted DESC by gen_start_year (newest first), None-year
    entries last.  For a single-gen model, gens has one entry — a
    vehicle year >= its gen_start_year matches, older → legacy."""
    if not gens:
        return (None, "unknown")
    if year is None:
        # No MY info → default to the newest gen (best guess for
        # unlabelled fleet rows in year-less BITRE releases).
        return (gens[0][1], "unknown")
    newest_gy = gens[0][0]
    for gy, sz in gens:
        if gy is None:
            # None-year entries act as an "any year" catch-all.
            return (sz, "unknown")
        if year >= gy - GEN_START_BUFFER:
            tier = "current" if gy == newest_gy else "prev-gen"
            return (sz, tier)
    # Fleet year is older than any curated gen — dump into LEGACY.
    return (None, "legacy")


def main():
    src, has_year = pick_input_csv()
    if not src.exists():
        print(f"[fatal] BITRE fleet CSV not found under {src.parent}/")
        print(f"        expected {BITRE_YEAR.name} or {BITRE_NOYEAR.name}")
        return
    print(f"[info] input : {src.name} (year-aware={'yes' if has_year else 'no'})")

    fit_map = load_fitment_gens()
    if not fit_map:
        return
    multi_gen = sum(1 for gs in fit_map.values() if len(gs) > 1)
    total_rows = sum(len(gs) for gs in fit_map.values())
    print(f"[info] fitment: {len(fit_map):,} models "
          f"({multi_gen:,} multi-gen, {total_rows:,} total gen entries)")

    # Aggregation keyed by (state, postcode, size, gen).  `gen` values:
    #   "current" — matched to newest gen for that model
    #   "prev-gen" — matched to an older-than-newest gen
    #   "legacy"  — vehicle older than any curated gen (no size predicted)
    #   "unknown" — no year info, defaulted to newest gen
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
            gens = fit_map.get(key)
            if not gens:
                agg[(state, pc, "UNKNOWN", "unknown")] += qty
                unmatched_top_qty[(mk, md)] += qty
                continue

            size, tier = _pick_gen(year, gens)
            if size is None:
                # Vehicle year is older than the oldest curated gen —
                # LEGACY.  Fleet total still balances via the bucket.
                agg[(state, pc, "LEGACY", "legacy")] += qty
                legacy_fleet += qty
                continue

            agg[(state, pc, size, tier)] += qty
            per_postcode_matched[(state, pc)] += qty
            matched_fleet += qty

    cov = (matched_fleet / total_fleet * 100.0) if total_fleet else 0.0
    print(f"[info] total fleet loaded  : {total_fleet:,}")
    print(f"[info] matched to fitment  : {matched_fleet:,} ({cov:.1f} %)")
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

    # National top-20 sizes — every matched row (current, prev-gen,
    # unknown-year) contributes; UNKNOWN / LEGACY buckets excluded so
    # the distribution reflects tyre sizes we can actually name.
    dist: dict[str, int] = defaultdict(int)
    for (_, _, size, gen), q in agg.items():
        if size in ("UNKNOWN", "LEGACY"):
            continue
        dist[size] += q
    total = sum(dist.values()) or 1
    print("\n[national top 20 sizes (matched fleet, all gens)]")
    top = sorted(dist.items(), key=lambda kv: kv[1], reverse=True)[:20]
    for size, q in top:
        pct = q / total * 100.0
        print(f"    {size:>16s}  {q:>12,}  ({pct:5.2f} %)")


if __name__ == "__main__":
    main()
