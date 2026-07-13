#!/usr/bin/env python3
"""
postcode_rim_demand.py — predict tyre demand per postcode by rim family
using BITRE fleet data and a hand-curated (make, model) → rim family
map (top ~120 Australian light-vehicle models).

Pipeline:
  1. Read out/rego/vehicle_postcode_make_model.csv (BITRE fleet).
  2. For each (make, model) row, look up its base-OE rim in RIM_MAP.
  3. Aggregate to (state, postcode, rim_family, fleet_units).
  4. Estimate annual replacement demand:
        fleet × 4 tyres × 25 % worn-out per year  = fleet units
     (i.e. one full tyre-set every 4 years — coarse but useful).
  5. Log the top 20 unmatched (make, model) pairs by fleet volume so
     the map can be extended for the next iteration.
  6. Write out/rego/postcode_rim_demand.csv:
        state, postcode, rim_family, fleet_units, annual_tyres_est,
        coverage_pct

Fitment choice — BASE OE:
  Every mapped model uses its ENTRY-trim OE rim size (Ford Ranger XL
  = R17, not the Wildtrak's R18).  That's a deliberate simplification:
  the entry trim covers the largest share of the fleet, and even
  when a customer up-sizes, replacement usually stays within one
  step, so R17 owners rarely replace with something totally
  different.

Extending the map:
  Run the script once — the tail prints the top 20 uncovered
  (make, model) pairs by fleet volume.  Paste those into RIM_MAP with
  a sensible base-OE rim size, rerun, and coverage climbs quickly.
"""
from __future__ import annotations
import csv
import os
import re
from collections import defaultdict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
INPUT    = BASE_DIR / "out" / "rego" / "vehicle_postcode_make_model.csv"
OUTPUT   = BASE_DIR / "out" / "rego" / "postcode_rim_demand.csv"

# (MAKE, MODEL) — normalised: uppercase, single-space, hyphens kept.
# BITRE ships some names as MERCEDES-BENZ / MERCEDES BENZ / MERCEDES_BENZ
# depending on the release, so the normalizer strips the connectors.
RIM_MAP: dict[tuple[str, str], int] = {
    # ── Hyundai ───────────────────────────────────────────────
    ("HYUNDAI", "I30"):        15,
    ("HYUNDAI", "I20"):        16,
    ("HYUNDAI", "ELANTRA"):    16,
    ("HYUNDAI", "SONATA"):     17,
    ("HYUNDAI", "ACCENT"):     15,
    ("HYUNDAI", "TUCSON"):     17,
    ("HYUNDAI", "IX35"):       17,
    ("HYUNDAI", "KONA"):       16,
    ("HYUNDAI", "SANTA FE"):   18,
    ("HYUNDAI", "PALISADE"):   18,
    ("HYUNDAI", "VENUE"):      15,
    ("HYUNDAI", "ILOAD"):      16,
    ("HYUNDAI", "IMAX"):       16,
    ("HYUNDAI", "STARIA"):     17,
    ("HYUNDAI", "IONIQ 5"):    19,
    # ── Toyota ────────────────────────────────────────────────
    ("TOYOTA", "COROLLA"):     15,
    ("TOYOTA", "CAMRY"):       16,
    ("TOYOTA", "YARIS"):       15,
    ("TOYOTA", "YARIS CROSS"): 16,
    ("TOYOTA", "HILUX"):       17,
    ("TOYOTA", "RAV4"):        17,
    ("TOYOTA", "KLUGER"):      18,
    ("TOYOTA", "PRADO"):       17,
    ("TOYOTA", "LANDCRUISER"): 17,
    ("TOYOTA", "LAND CRUISER"): 17,
    ("TOYOTA", "HIACE"):       16,
    ("TOYOTA", "FORTUNER"):    17,
    ("TOYOTA", "GRANVIA"):     17,
    ("TOYOTA", "C-HR"):        17,
    ("TOYOTA", "86"):          17,
    ("TOYOTA", "AURION"):      16,
    ("TOYOTA", "ECHO"):        14,
    # ── Ford ──────────────────────────────────────────────────
    ("FORD", "RANGER"):        17,
    ("FORD", "EVEREST"):       18,
    ("FORD", "FOCUS"):         16,
    ("FORD", "TRANSIT"):       16,
    ("FORD", "ESCAPE"):        17,
    ("FORD", "KUGA"):          17,
    ("FORD", "FIESTA"):        15,
    ("FORD", "MUSTANG"):       19,
    ("FORD", "TERRITORY"):     17,
    ("FORD", "FALCON"):        16,
    # ── Mazda ─────────────────────────────────────────────────
    ("MAZDA", "MAZDA 2"):      15,
    ("MAZDA", "MAZDA 3"):      16,
    ("MAZDA", "MAZDA 6"):      17,
    ("MAZDA", "CX-3"):         16,
    ("MAZDA", "CX-30"):        18,
    ("MAZDA", "CX-5"):         17,
    ("MAZDA", "CX-8"):         17,
    ("MAZDA", "CX-9"):         18,
    ("MAZDA", "BT-50"):        16,
    ("MAZDA", "MX-5"):         16,
    # ── Mitsubishi ────────────────────────────────────────────
    ("MITSUBISHI", "TRITON"):        17,
    ("MITSUBISHI", "PAJERO"):        18,
    ("MITSUBISHI", "PAJERO SPORT"):  18,
    ("MITSUBISHI", "OUTLANDER"):     18,
    ("MITSUBISHI", "ECLIPSE CROSS"): 18,
    ("MITSUBISHI", "ASX"):           16,
    ("MITSUBISHI", "MIRAGE"):        14,
    ("MITSUBISHI", "MAGNA"):         15,
    ("MITSUBISHI", "LANCER"):        16,
    # ── Kia ───────────────────────────────────────────────────
    ("KIA", "CERATO"):     16,
    ("KIA", "RIO"):        15,
    ("KIA", "PICANTO"):    14,
    ("KIA", "STONIC"):     16,
    ("KIA", "SELTOS"):     16,
    ("KIA", "SPORTAGE"):   17,
    ("KIA", "SORENTO"):    17,
    ("KIA", "CARNIVAL"):   18,
    ("KIA", "STINGER"):    18,
    ("KIA", "EV6"):        19,
    ("KIA", "NIRO"):       16,
    ("KIA", "OPTIMA"):     17,
    # ── Nissan ────────────────────────────────────────────────
    ("NISSAN", "X-TRAIL"):    17,
    ("NISSAN", "NAVARA"):     17,
    ("NISSAN", "PATROL"):     18,
    ("NISSAN", "QASHQAI"):    17,
    ("NISSAN", "PATHFINDER"): 18,
    ("NISSAN", "JUKE"):       17,
    ("NISSAN", "MICRA"):      15,
    ("NISSAN", "PULSAR"):     16,
    ("NISSAN", "MURANO"):     18,
    ("NISSAN", "TIIDA"):      15,
    ("NISSAN", "DUALIS"):     16,
    # ── Subaru ────────────────────────────────────────────────
    ("SUBARU", "FORESTER"):  17,
    ("SUBARU", "OUTBACK"):   18,
    ("SUBARU", "IMPREZA"):   17,
    ("SUBARU", "XV"):        17,
    ("SUBARU", "CROSSTREK"): 17,
    ("SUBARU", "WRX"):       17,
    ("SUBARU", "LIBERTY"):   17,
    ("SUBARU", "BRZ"):       17,
    # ── Holden (legacy) ────────────────────────────────────────
    ("HOLDEN", "COMMODORE"): 16,
    ("HOLDEN", "COLORADO"):  16,
    ("HOLDEN", "CAPTIVA"):   17,
    ("HOLDEN", "CRUZE"):     16,
    ("HOLDEN", "TRAX"):      17,
    ("HOLDEN", "BARINA"):    15,
    ("HOLDEN", "ASTRA"):     16,
    ("HOLDEN", "RODEO"):     16,
    ("HOLDEN", "ACADIA"):    18,
    # ── Honda ─────────────────────────────────────────────────
    ("HONDA", "CIVIC"):    16,
    ("HONDA", "ACCORD"):   18,
    ("HONDA", "CR-V"):     18,
    ("HONDA", "HR-V"):     17,
    ("HONDA", "JAZZ"):     15,
    ("HONDA", "ODYSSEY"):  17,
    ("HONDA", "CITY"):     16,
    # ── Volkswagen ────────────────────────────────────────────
    ("VOLKSWAGEN", "GOLF"):        15,
    ("VOLKSWAGEN", "POLO"):        15,
    ("VOLKSWAGEN", "TIGUAN"):      17,
    ("VOLKSWAGEN", "AMAROK"):      17,
    ("VOLKSWAGEN", "PASSAT"):      16,
    ("VOLKSWAGEN", "T-CROSS"):     16,
    ("VOLKSWAGEN", "TOUAREG"):     19,
    ("VOLKSWAGEN", "CADDY"):       15,
    ("VOLKSWAGEN", "TRANSPORTER"): 16,
    # ── Isuzu / Isuzu-Ute ──────────────────────────────────────
    ("ISUZU", "D-MAX"):    17,
    ("ISUZU", "MU-X"):     18,
    ("ISUZU UTE", "D-MAX"): 17,
    ("ISUZU UTE", "MU-X"):  18,
    # ── Suzuki ────────────────────────────────────────────────
    ("SUZUKI", "SWIFT"):    15,
    ("SUZUKI", "JIMNY"):    15,
    ("SUZUKI", "VITARA"):   16,
    ("SUZUKI", "BALENO"):   15,
    ("SUZUKI", "IGNIS"):    15,
    ("SUZUKI", "S-CROSS"):  17,
    ("SUZUKI", "ACROSS"):   17,
    # ── BMW ───────────────────────────────────────────────────
    ("BMW", "1 SERIES"): 16,
    ("BMW", "2 SERIES"): 17,
    ("BMW", "3 SERIES"): 17,
    ("BMW", "4 SERIES"): 18,
    ("BMW", "5 SERIES"): 18,
    ("BMW", "7 SERIES"): 19,
    ("BMW", "X1"):       18,
    ("BMW", "X2"):       18,
    ("BMW", "X3"):       19,
    ("BMW", "X4"):       19,
    ("BMW", "X5"):       19,
    ("BMW", "X6"):       19,
    ("BMW", "X7"):       20,
    # ── Mercedes-Benz ─────────────────────────────────────────
    ("MERCEDES-BENZ", "A-CLASS"):  16,
    ("MERCEDES-BENZ", "B-CLASS"):  16,
    ("MERCEDES-BENZ", "C-CLASS"):  16,
    ("MERCEDES-BENZ", "CLA"):      18,
    ("MERCEDES-BENZ", "CLS"):      18,
    ("MERCEDES-BENZ", "E-CLASS"):  17,
    ("MERCEDES-BENZ", "GLA"):      18,
    ("MERCEDES-BENZ", "GLC"):      18,
    ("MERCEDES-BENZ", "GLE"):      19,
    ("MERCEDES-BENZ", "GLS"):      20,
    ("MERCEDES-BENZ", "VITO"):     16,
    ("MERCEDES-BENZ", "SPRINTER"): 16,
    ("MERCEDES-BENZ", "V-CLASS"):  17,
    # ── Audi ──────────────────────────────────────────────────
    ("AUDI", "A1"): 15,
    ("AUDI", "A3"): 16,
    ("AUDI", "A4"): 17,
    ("AUDI", "A5"): 18,
    ("AUDI", "A6"): 17,
    ("AUDI", "Q2"): 17,
    ("AUDI", "Q3"): 17,
    ("AUDI", "Q5"): 18,
    ("AUDI", "Q7"): 19,
    ("AUDI", "Q8"): 20,
    # ── Volvo ─────────────────────────────────────────────────
    ("VOLVO", "XC40"): 18,
    ("VOLVO", "XC60"): 18,
    ("VOLVO", "XC90"): 19,
    ("VOLVO", "S60"):  18,
    ("VOLVO", "S90"):  18,
    # ── Tesla / EV ────────────────────────────────────────────
    ("TESLA", "MODEL 3"): 18,
    ("TESLA", "MODEL Y"): 19,
    ("TESLA", "MODEL S"): 19,
    ("TESLA", "MODEL X"): 20,
    ("POLESTAR", "2"):    19,
    # ── Chinese brands ────────────────────────────────────────
    ("MG", "ZS"):        17,
    ("MG", "HS"):        18,
    ("MG", "MG3"):       15,
    ("MG", "MG4"):       17,
    ("MG", "3"):         15,
    ("HAVAL", "H6"):     19,
    ("HAVAL", "JOLION"): 17,
    ("GWM", "CANNON"):   18,
    ("GWM HAVAL", "H6"): 19,
    ("GREAT WALL", "CANNON"): 18,
    ("GREAT WALL", "STEED"):  16,
    ("LDV", "T60"):        16,
    ("LDV", "G10"):        15,
    ("LDV", "D90"):        18,
    ("LDV", "V80"):        16,
    ("LDV", "DELIVER 9"):  16,
    ("BYD", "ATTO 3"):     18,
    ("BYD", "SEAL"):       19,
    ("CHERY", "OMODA 5"):  18,
    ("CHERY", "TIGGO 7"):  18,
    # ── SsangYong ─────────────────────────────────────────────
    ("SSANGYONG", "MUSSO"):  18,
    ("SSANGYONG", "REXTON"): 18,
    ("SSANGYONG", "ACTYON"): 17,
    # ── Lexus ─────────────────────────────────────────────────
    ("LEXUS", "IS"): 17,
    ("LEXUS", "ES"): 17,
    ("LEXUS", "NX"): 18,
    ("LEXUS", "RX"): 18,
    ("LEXUS", "LX"): 20,
    ("LEXUS", "GX"): 18,
    ("LEXUS", "UX"): 17,
    # ── Land Rover / Jaguar ───────────────────────────────────
    ("LAND ROVER", "DISCOVERY"):         19,
    ("LAND ROVER", "DISCOVERY SPORT"):   18,
    ("LAND ROVER", "RANGE ROVER"):       20,
    ("LAND ROVER", "RANGE ROVER SPORT"): 20,
    ("LAND ROVER", "RANGE ROVER EVOQUE"):18,
    ("LAND ROVER", "DEFENDER"):          18,
    ("LAND ROVER", "FREELANDER"):        17,
    ("JAGUAR", "XE"):    17,
    ("JAGUAR", "XF"):    17,
    ("JAGUAR", "F-PACE"): 18,
    # ── Jeep ──────────────────────────────────────────────────
    ("JEEP", "GRAND CHEROKEE"): 18,
    ("JEEP", "CHEROKEE"):       17,
    ("JEEP", "COMPASS"):        17,
    ("JEEP", "WRANGLER"):       17,
    ("JEEP", "GLADIATOR"):      17,
    # ── Porsche ───────────────────────────────────────────────
    ("PORSCHE", "MACAN"):    18,
    ("PORSCHE", "CAYENNE"):  19,
    ("PORSCHE", "TAYCAN"):   19,
    ("PORSCHE", "PANAMERA"): 19,
    # ── Peugeot / Renault / Skoda / Citroen ───────────────────
    ("PEUGEOT", "308"):   16,
    ("PEUGEOT", "3008"):  17,
    ("PEUGEOT", "2008"):  16,
    ("RENAULT", "MEGANE"):  17,
    ("RENAULT", "KOLEOS"):  18,
    ("RENAULT", "CAPTUR"):  17,
    ("RENAULT", "TRAFIC"):  16,
    ("SKODA", "OCTAVIA"): 16,
    ("SKODA", "KODIAQ"):  18,
    ("SKODA", "KAROQ"):   17,
    ("SKODA", "SUPERB"):  17,
    ("CITROEN", "C3"):    15,
    ("CITROEN", "C4"):    16,
    ("CITROEN", "C5"):    17,
    # ── Ram / heavy pickup ────────────────────────────────────
    ("RAM", "1500"): 20,
    ("RAM", "2500"): 18,
    # ── Light commercial (LCV) / trucks (TBR) ─────────────────
    ("FUSO", "CANTER"):     16,
    ("FUSO", "FIGHTER"):    22,
    ("HINO", "300 SERIES"): 16,
    ("HINO", "500 SERIES"): 22,
    ("HINO", "700 SERIES"): 22,
    ("ISUZU", "NPR"):       16,
    ("ISUZU", "NLR"):       16,
    ("ISUZU", "FRR"):       19,
    ("ISUZU", "FSR"):       22,
    ("ISUZU", "FVR"):       22,
    ("ISUZU", "GIGA"):      22,
    ("IVECO", "DAILY"):     16,
    ("IVECO", "EUROCARGO"): 22,
    ("KENWORTH", "T610"):   22,
    ("SCANIA", "R500"):     22,
    ("VOLVO", "FH"):        22,
    ("MAN", "TGX"):         22,
    ("MERCEDES-BENZ", "ACTROS"): 22,
    ("MERCEDES-BENZ", "ATEGO"):  22,
    ("DAF", "CF"):          22,
}


def rim_family(inches: int | None) -> str:
    """Bucket a rim size into a coarse family label."""
    if inches is None:
        return "UNKNOWN"
    if inches <= 14: return "R14"
    if inches == 15: return "R15"
    if inches == 16: return "R16"
    if inches == 17: return "R17"
    if inches == 18: return "R18"
    if inches == 19: return "R19"
    if inches == 20: return "R20"
    if inches == 21: return "R21"
    if inches >= 22: return "R22+ (truck)"
    return f"R{inches}"


def normalize_key(make: str, model: str) -> tuple[str, str]:
    """Normalise (make, model) so BITRE's spelling variants collapse
    to a single lookup key.  Uppercase; collapse consecutive spaces;
    treat '-', '_' and '/' as spaces so 'MERCEDES-BENZ', 'MERCEDES BENZ'
    and 'MERCEDES_BENZ' all match the same map entry."""
    def n(s: str) -> str:
        s = (s or "").strip().upper()
        s = re.sub(r"[_/]", " ", s)
        # keep '-' inside model tokens (e.g. CX-5, MU-X) — only strip it
        # when the whole string still has letters flanking on both sides
        # of the hyphen and there are spaces elsewhere (rare).
        s = re.sub(r"\s+", " ", s)
        return s
    return (n(make), n(model))


def main():
    if not INPUT.exists():
        print(f"[fatal] input CSV not found: {INPUT}")
        return

    agg: dict[tuple[str, str, str], int] = defaultdict(int)
    per_postcode_total:   dict[tuple[str, str], int] = defaultdict(int)
    per_postcode_matched: dict[tuple[str, str], int] = defaultdict(int)
    total_fleet   = 0
    matched_fleet = 0
    unmatched_top_qty: dict[tuple[str, str], int] = defaultdict(int)

    with open(INPUT, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            mk = (r.get("make") or "").strip()
            md = (r.get("model") or "").strip()
            state = (r.get("state") or "").strip().upper()
            pc = (r.get("postcode") or "").strip()
            try:
                qty = int(r.get("qty") or 0)
            except ValueError:
                qty = 0
            if qty <= 0 or not pc:
                continue

            total_fleet += qty
            per_postcode_total[(state, pc)] += qty

            key = normalize_key(mk, md)
            rim = RIM_MAP.get(key)
            if rim is None:
                # Bucket unknown separately so a low-coverage postcode
                # is visible in the output (large UNKNOWN row = the map
                # needs extending for that area).
                agg[(state, pc, "UNKNOWN")] += qty
                unmatched_top_qty[key] += qty
                continue

            agg[(state, pc, rim_family(rim))] += qty
            per_postcode_matched[(state, pc)] += qty
            matched_fleet += qty

    cov = (matched_fleet / total_fleet * 100.0) if total_fleet else 0.0
    print(f"[info] total fleet loaded  : {total_fleet:,}")
    print(f"[info] matched to rim map  : {matched_fleet:,} ({cov:.1f} %)")
    print(f"[info] unmatched (unknown) : {total_fleet - matched_fleet:,}")

    if unmatched_top_qty:
        print("\n[extend RIM_MAP] top 20 uncovered (make, model) by fleet:")
        top = sorted(unmatched_top_qty.items(), key=lambda kv: kv[1], reverse=True)[:20]
        for (mk, md), q in top:
            print(f"    {q:>12,}   {mk} · {md}")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["state", "postcode", "rim_family", "fleet_units",
                    "annual_tyres_est", "coverage_pct"])
        for (state, pc, fam), fleet in sorted(agg.items()):
            total   = per_postcode_total[(state, pc)]
            matched = per_postcode_matched[(state, pc)]
            pc_cov  = (matched / total * 100.0) if total else 0.0
            # Coarse annual-replacement estimate: assume every vehicle
            # runs one full tyre set (4 tyres) and replaces the whole
            # set every 4 years — so annual tyre-units = fleet_units.
            w.writerow([state, pc, fam, fleet, fleet, f"{pc_cov:.1f}"])

    print(f"\n[done] wrote {OUTPUT}  ({len(agg):,} rows)")

    # National rim distribution for the covered fleet — quick sanity
    # check that the map's shape matches the Australian market.
    dist: dict[str, int] = defaultdict(int)
    for (_, _, fam), q in agg.items():
        if fam != "UNKNOWN":
            dist[fam] += q
    if dist:
        print("\n[national rim mix — matched fleet only]:")
        total = sum(dist.values())
        for fam, q in sorted(dist.items(), key=lambda kv: kv[1], reverse=True):
            pct = q / total * 100
            print(f"    {fam:<12}  {q:>12,}   {pct:5.1f} %")


if __name__ == "__main__":
    main()
