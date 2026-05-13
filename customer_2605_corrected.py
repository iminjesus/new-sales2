"""
Manual review of the 47 BDE-state-vs-coord mismatches.

For each row I cross-checked the Address + City against my knowledge of
Australian suburbs / well-known businesses and assigned a corrected
latitude / longitude.  Each row also gets:

  coord_status   WRONG_COORD     – original lat/lng is in the wrong region
                                   (geocoder error); use correct_*.
                 COORD_OK        – original lat/lng matches the address;
                                   the mismatch is the BDE State assignment
                                   (regional grouping, or genuinely wrong).
  confidence     street          – I'm fairly sure of the exact spot
                                   (well-known business / unique street).
                 suburb          – correct suburb, exact street unverified
                                   (treat as ~few hundred metres rough).

The "expected_state" column shows which AU state the corrected coords
fall in, so the user can decide whether to fix the BDE State row too.
"""
import csv

# Keyed on Ship-to code.
# (correct_lat, correct_lng, coord_status, confidence, note)
CORRECTIONS = {
    "A029737": (-33.282,  151.420,  "WRONG_COORD", "suburb",
                "Wyong NSW (Central Coast) — geocoder hit Toowoomba QLD"),
    "A029675": (-33.426,  151.394,  "WRONG_COORD", "suburb",
                "Wyoming NSW (Central Coast) — geocoder hit Maryborough QLD"),
    "A018891": (-33.6404, 150.9162, "WRONG_COORD", "street",
                "Mercedes-Benz Aust HQ, Mulgrave NSW 2756 — geocoder picked Mulgrave VIC"),
    "A031010": (-33.913,  151.158,  "WRONG_COORD", "suburb",
                "Marrickville NSW (inner-west Sydney) — geocoder hit Gold Coast QLD"),
    "A029671": (-33.298,  151.421,  "WRONG_COORD", "suburb",
                "Tuggerah NSW (Central Coast) — geocoder hit Gold Coast QLD"),
    "A029907": (-33.847,  150.890,  "WRONG_COORD", "suburb",
                "Wetherill Park NSW (W Sydney) — geocoder hit Maryborough QLD"),
    "A029953": (-32.969,  151.671,  "WRONG_COORD", "suburb",
                "Cardiff NSW (Newcastle); 'Munibunga' likely typo for 'Munibung Rd'"),
    "A030039": (-33.853,  150.901,  "WRONG_COORD", "suburb",
                "Wetherill Park NSW (W Sydney) — geocoder hit Brisbane QLD"),
    "A030073": (-27.971,  153.367,  "WRONG_COORD", "suburb",
                "Molendinar QLD (Gold Coast); shop name 'Olendinar' is missing leading M"),
    "A030986": (-33.218,  151.535,  "WRONG_COORD", "suburb",
                "Doyalson NSW (Central Coast) — geocoder hit Adelaide SA"),
    "A031024": (-34.288,  146.042,  "WRONG_COORD", "suburb",
                "Griffith NSW (Riverina) — not Griffith QLD/ACT; geocoder hit Maryborough QLD"),
    "A021362": (-34.176,  140.747,  "COORD_OK",    "street",
                "Renmark SA — coords are correct; BDE=QLD is the error"),
    "A030081": (-27.582,  152.945,  "WRONG_COORD", "suburb",
                "Sumner Park QLD (Brisbane SW industrial)"),
    "A022295": (-33.749,  150.694,  "COORD_OK",    "street",
                "Penrith NSW — coords are correct; BDE=QLD is the error"),
    "A031209": (-17.932,  145.928,  "WRONG_COORD", "suburb",
                "Tully QLD (Far North) — geocoder hit Newcastle NSW"),
    "A029641": (-27.456,  153.010,  "WRONG_COORD", "street",
                "Red Hill QLD (Brisbane inner-north); Keith McKay's Tyres"),
    "A030488": (-27.529,  153.265,  "WRONG_COORD", "suburb",
                "Cleveland QLD (Redlands)"),
    "A030369": (-27.768,  153.210,  "WRONG_COORD", "suburb",
                "Stapylton QLD (between Brisbane and Gold Coast)"),
    "A029690": (-25.373,  151.121,  "WRONG_COORD", "suburb",
                "Eidsvold QLD (North Burnett)"),
    "A029640": (-37.967,  145.054,  "WRONG_COORD", "street",
                "Moorabbin VIC (Melbourne SE); Warrigal Rd is a main road"),
    "A030361": (-34.182,  139.984,  "WRONG_COORD", "suburb",
                "Waikerie SA (Riverland)"),
    "731634":  (-34.213,  142.166,  "COORD_OK",    "street",
                "Mildura VIC — coords correct; BDE=SA is regional grouping (Riverland covers Mildura)"),
    "735533":  (-34.207,  142.140,  "COORD_OK",    "street",
                "Mildura VIC — coords correct; same regional grouping"),
    "A022148": (-20.722,  139.490,  "COORD_OK",    "street",
                "Mount Isa QLD — coords correct; BDE=SA is the error"),
    "A023018": (-31.926,  116.005,  "COORD_OK",    "street",
                "Perth Airport WA — coords correct; BDE=SA is the error"),
    "A004325": (-34.207,  142.157,  "COORD_OK",    "street",
                "Mildura VIC — coords correct; regional grouping"),
    "A007127": (-34.187,  142.162,  "COORD_OK",    "street",
                "Mildura VIC — coords correct; regional grouping"),
    "A028865": (-34.187,  142.162,  "COORD_OK",    "street",
                "Mildura VIC — coords correct; regional grouping"),
    "A018839": (-34.303,  142.183,  "COORD_OK",    "street",
                "Red Cliffs VIC (near Mildura) — coords correct"),
    "A029689": (-36.959,  140.741,  "WRONG_COORD", "suburb",
                "Naracoorte SA (SE region) — geocoder hit Newcastle NSW"),
    "A029906": (-35.143,  138.487,  "WRONG_COORD", "suburb",
                "Noarlunga Centre SA (S Adelaide) — geocoder hit Gold Coast QLD"),
    "A029908": (-34.967,  138.587,  "WRONG_COORD", "suburb",
                "Clarence Gardens SA (S Adelaide) — geocoder hit Newcastle NSW"),
    "A029756": (-37.771,  144.836,  "WRONG_COORD", "suburb",
                "Sunshine North VIC (Melbourne NW)"),
    "A029948": (-38.011,  145.214,  "WRONG_COORD", "suburb",
                "Dandenong South VIC (Melbourne SE)"),
    "A031061": (-36.553,  145.978,  "WRONG_COORD", "suburb",
                "Benalla VIC (NE regional)"),
    "A031273": (-42.881,  147.328,  "WRONG_COORD", "street",
                "Hobart TAS — shop name 'BOB JANE HOBART' confirms TAS, not VIC"),
    "A028332": (-35.018,  138.621,  "COORD_OK",    "street",
                "Blackwood SA (Adelaide hills) — coords correct; BDE=VIC is the error"),
    "A030532": (-38.098,  145.281,  "WRONG_COORD", "suburb",
                "Cranbourne VIC (Melbourne SE) — geocoder hit Geraldton WA"),
    "A022505": (-34.931,  138.594,  "COORD_OK",    "street",
                "Adelaide CBD SA — coords correct; BDE=VIC is the error"),
    "A023434": (-34.858,  138.572,  "COORD_OK",    "street",
                "Regency Park SA — coords correct; BDE=VIC is the error"),
    "A030866": (-37.987,  145.215,  "WRONG_COORD", "suburb",
                "Dandenong VIC (Melbourne SE)"),
    "A029806": (-37.742,  145.001,  "WRONG_COORD", "street",
                "Preston VIC (Melbourne north)"),
    "A029985": (-36.434,  145.230,  "WRONG_COORD", "suburb",
                "Tatura VIC (Goulburn Valley)"),
    "A028518": (-34.886,  138.602,  "COORD_OK",    "street",
                "Nailsworth SA (Adelaide) — coords correct; BDE=VIC is the error"),
    "A031195": (-32.122,  115.928,  "WRONG_COORD", "suburb",
                "Harrisdale WA (Perth SE) — geocoder hit Glen Iris VIC"),
    "A029642": (-31.952,  115.799,  "WRONG_COORD", "suburb",
                "Shenton Park WA (Perth W) — geocoder hit Brisbane QLD"),
    "A030036": (-31.989,  115.948,  "WRONG_COORD", "street",
                "Welshpool WA (Perth SE) — geocoder hit Brisbane QLD"),
}

# Same crude bbox set used earlier to label which state the corrected
# coords actually fall in.
BBOX = {
    "NSW": (140.9, 153.8, -37.6, -28.1),
    "VIC": (140.9, 150.1, -39.3, -33.7),
    "QLD": (137.9, 153.8, -29.3, -10.5),
    "SA":  (128.9, 141.1, -38.2, -25.9),
    "WA":  (112.9, 129.1, -35.3, -13.5),
    "NT":  (129.0, 138.1, -25.9, -10.8),
    "TAS": (143.5, 148.6, -43.7, -39.5),
    "ACT": (148.7, 149.5, -35.96, -35.1),
}
def state_of(lng, lat):
    for s, (a, b, c, d) in BBOX.items():
        if a <= lng <= b and c <= lat <= d:
            return s
    return ""

src = "/root/.claude/uploads/15c1b26d-2122-4e43-8cd8-9dee5d2c694f/a40e92d6-customer_2605_state_mismatch.csv"
out = "/home/user/new-sales2/customer_2605_corrected.csv"

with open(src, newline='', encoding='utf-8-sig') as f:
    rdr = csv.DictReader(f, delimiter='\t')
    cols = rdr.fieldnames
    rows = list(rdr)

out_cols = cols + ["correct_lat", "correct_lng",
                   "coord_status", "confidence",
                   "expected_state", "note"]

missing = []
with open(out, "w", newline='', encoding='utf-8-sig') as f:
    w = csv.DictWriter(f, fieldnames=out_cols, delimiter='\t')
    w.writeheader()
    for r in rows:
        st = r.get("Ship-to") or ""
        c = CORRECTIONS.get(st)
        if c is None:
            missing.append(st)
            continue
        lat, lng, status, conf, note = c
        r["correct_lat"]     = f"{lat:.6f}"
        r["correct_lng"]     = f"{lng:.6f}"
        r["coord_status"]    = status
        r["confidence"]      = conf
        r["expected_state"]  = state_of(lng, lat)
        r["note"]            = note
        w.writerow(r)

if missing:
    print(f"WARNING: no manual correction for ship_to: {missing}")
print(f"Wrote: {out} ({len(rows) - len(missing)} rows)")

# Quick summary
from collections import Counter
status_counts = Counter(CORRECTIONS[r["Ship-to"]][2] for r in rows
                        if r["Ship-to"] in CORRECTIONS)
print("Status counts:", dict(status_counts))
