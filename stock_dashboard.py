"""
stock_dashboard.py — experimental Stock Balance dashboard
=========================================================

Reads the latest ``Stock_report_*.xlsm`` in the project directory and
serves an interactive web UI at ``/stock_lab``.

The idea: quickly show *where our stock is short and where it's
surplus* — nationally, by state, and sliced by every product
characteristic Hankook actually cares about (group SP/HP/UHP/TBR,
size, rim inch, load index, speed rating, brand).

If the user likes what they see this becomes a J-Force feature, so
the UI is deliberately clean, keyboard-driven, and reuses the
Tyre-Price-Dashboard visual language (dark blue header, monospace
figures, coloured brand pills).

Data model per row of "Stock Status Worksheet":
  - Merge Code  → maps to one or more M CODE via the MM sheet
  - Group       → SP / HP / UHP / TBR / TBR-M / M-T etc.
  - Classif.    → 1UHP&PCR, 2PCR, … (Hankook-internal cluster)
  - State stock : N.STOCK / Q.STOCK / V.STOCK / W.STOCK  + PORT /
                  WATER / FAC pipeline per state
  - Overall     : STOCK / PORT / WATER / FAC / TOTAL
  - Averages    : 3M Average, Max 12M Ave, Max 12M — five 3-column
                  groups, one per state and one overall (NSW, QLD,
                  VIC, WA, TOTAL)

Status buckets (default thresholds — see STATUS_THRESHOLDS):
  - Shortage : MOH < 1 month  AND 3M demand > 0
  - Balanced : 1 ≤ MOH ≤ 4
  - Surplus  : MOH > 4 months
  - No move  : 3M demand == 0 AND stock > 0    (dead stock)
  - Empty    : stock == 0 AND 3M demand == 0   (skipped from UI)

Registration:
  from stock_dashboard import stock_dashboard, load_stock_data
  app.add_url_rule('/stock_lab', 'stock_dashboard', stock_dashboard)

Requires: openpyxl (pip install openpyxl).
"""
import csv, glob, json, os, re, time
from datetime import datetime
from flask import render_template_string, request

_BASE = os.path.dirname(os.path.abspath(__file__))


# ── Data loading ────────────────────────────────────────────────────

STATES = ["NSW", "QLD", "VIC", "WA"]

# Status thresholds in months of inventory (MOI = stock / 3M avg demand).
# The bucket boundaries match Hankook Australia's own definitions:
#   MOI ≤ 1      → Shortage
#   1  < MOI ≤ 3 → Balance
#   3  < MOI ≤ 6 → Surplus
#   MOI > 6      → Serious Surplus
# (Stock > 0 AND demand == 0 stays its own "No-move" bucket — dead
# stock is different from over-stock and needs a different response.)
STATUS_SHORTAGE_MOI  = 1.0
STATUS_BALANCE_MOI   = 3.0
STATUS_SURPLUS_MOI   = 6.0

# Which sheet columns hold which value.  These indices are 1-based to
# match openpyxl's cell().  Verified against the sample workbook.
COL_MERGE_CODE = 1
COL_GROUP      = 2
COL_CLASSIF    = 3

# Per-state current stock (own stock in warehouse, excludes pipeline)
COL_STATE_STOCK = {"NSW": 86, "QLD": 93, "VIC": 100, "WA": 107}
# Pipeline (in transit) per state — PORT + WATER + FAC.  Total is
# reported in the sheet's "TOTAL" column right after each state block.
COL_STATE_PIPELINE = {
    "NSW": (87, 88, 89),   # PORT, WATER, FAC
    "QLD": (94, 95, 96),
    "VIC": (101, 102, 103),
    "WA":  (108, 109, 110),
}
COL_TOTAL_STOCK    = 112   # Overall on-hand stock
COL_TOTAL_TOTAL    = 116   # Overall stock + pipeline

# 3-month monthly-average demand (sum of last 3 months ÷ 3).  Layout
# per row: [NSW, QLD, VIC, WA, TOTAL] right after the 12-month sales
# history block, verified against the raw workbook (col 67 = WA
# 3M_A, col 68 = national 3M_A).  Cols 117-131 look similar but are
# a separate "3M Avg / Max 12M Ave / Max 12M" statistics block —
# those are for downstream reports and not the true monthly demand
# used for MOH.
COL_STATE_3M = {"NSW": 64, "QLD": 65, "VIC": 66, "WA": 67}
COL_TOTAL_3M = 68

# 12-month monthly-average demand — same layout as 3M_A but averaged
# over 12 months.  Used for the Max-12M-vs-3M comparison in the UI.
COL_STATE_12M = {"NSW": 74, "QLD": 75, "VIC": 76, "WA": 77}
COL_TOTAL_12M = 78

# 12-month sales history: 12 blocks of [NSW, QLD, VIC, WA, TOTAL] at
# cols 4-63.  Used by the drill-down modal to draw a per-state
# monthly line chart when the user clicks a problem SKU.
HIST_MONTHS   = 12
HIST_COL_START = 4      # -12M NSW column (default — overridden by header scan)
HIST_BLOCK_LEN = 5      # NSW, QLD, VIC, WA, TOTAL per month


def _detect_header_row(ws, max_scan=10):
    """Find the row that carries the column headers.  Walks the first
    `max_scan` rows and picks whichever row has the strongest match
    against the expected label set (Merge Code + NSW + QLD + VIC + WA
    + one of the state stock markers).  Returns a 1-based row index,
    defaulting to 2 (the historical location) if nothing scores well.

    This makes the loader resilient to blank leader rows being added
    or removed above the header — a common Excel edit that would
    otherwise break every column lookup below.  Labels are normalised
    (upper + punctuation stripped) so 'Merge Code', 'Merge_Code',
    'Merge-Code' all score the same."""
    KEY_LABELS = {"MERGECODE", "MERGECD", "NSW", "QLD", "VIC", "WA"}
    STATE_STOCK_MARKERS = {"NSTOCK", "QSTOCK", "VSTOCK", "WSTOCK", "STOCK"}
    def _n(v):
        s = str(v or "").strip().upper()
        for ch in (" ", "\t", "\n", "\r", "\xa0", "​", "‌",
                  "‍", "﻿", ".", "_", "-", "/", "\\", "(", ")"):
            s = s.replace(ch, "")
        return s
    best_row, best_score = 2, 0
    for r in range(1, max_scan + 1):
        vals = [_n(ws.cell(row=r, column=c).value) for c in range(1, ws.max_column + 1)]
        vset = set(vals)
        score = sum(1 for k in KEY_LABELS if k in vset)
        score += sum(1 for k in STATE_STOCK_MARKERS if k in vset)
        if score > best_score:
            best_row, best_score = r, score
    return best_row


def _scan_stock_header(ws, header_row=None):
    """Walk the Stock Status Worksheet header (auto-detected row) and
    return a column-index map keyed by semantic role rather than by
    ordinal position.  When the workbook grows a column or a section
    shifts, this scan keeps the loader working — the hard-coded COL_*
    values below become fallbacks only.

    The header alternates repeating labels — "NSW / QLD / VIC / WA"
    appears 12 times for monthly history, 4 more times for the
    3M_A/6M_A/12M_A/12M TOT block, and 5 more times for the
    3M Avg/Max 12M/Max 12M Ave block — so we track a section
    marker (the label that closes each 4-cell state group) and
    interpret each state cell in that section's context.

    Returns a dict with keys:
      MERGE_CODE / GROUP / CLASSIFICATION
      HIST[m]           -> {NSW, QLD, VIC, WA, TOTAL}     m ∈ 1..12  (month back)
      PERIOD_3M / 6M / 12M / 12M_TOT   -> {NSW, QLD, VIC, WA, TOTAL}
      STATE_STOCK       -> {NSW, QLD, VIC, WA}
      STATE_PORT        -> {NSW, QLD, VIC, WA}
      STATE_WATER       -> {NSW, QLD, VIC, WA}
      STATE_FAC         -> {NSW, QLD, VIC, WA}
      TOTAL_STOCK / TOTAL_PORT / TOTAL_WATER / TOTAL_FAC / TOTAL_ALL
    Columns are 1-based to match openpyxl's cell(row, column) API.
    """
    if header_row is None:
        header_row = _detect_header_row(ws)
    header = [ws.cell(row=header_row, column=c).value for c in range(1, ws.max_column + 1)]

    # Merged-cell header fallback: when the header row's cell for a
    # column reads as None (or an empty string), walk up to two rows
    # above and adopt whichever ancestor cell carries a non-empty label
    # — Excel-authored stock reports frequently use a stacked header
    # (row 4: section banner, row 5: column names) where a section
    # like "Product Name" is written into a merged range spanning both
    # rows.  openpyxl reports the label only on the top-left cell of a
    # merge, so C5 comes back as None while C4 has "Product Name".
    for c in range(1, ws.max_column + 1):
        v = header[c - 1]
        if v is None or (isinstance(v, str) and not v.strip()):
            for above in range(1, 3):
                if header_row - above < 1:
                    break
                anc = ws.cell(row=header_row - above, column=c).value
                if anc is not None and (not isinstance(anc, str) or anc.strip()):
                    header[c - 1] = anc
                    break

    def norm(v):
        return str(v).strip() if v is not None else ""

    # Aggressively normalised form of the header (upper-case with
    # whitespace / dots / underscores / hyphens / slashes stripped)
    # so "Merge Code" / "Merge_Code" / "Merge-Code" / "MergeCode" all
    # match the same alias.  Used by the identity-column detection
    # below AND by the label-based section walker further down.  We
    # also strip non-breaking space (\xa0), zero-width space, tab and
    # newline — Excel-authored headers frequently carry those hidden
    # characters and they would otherwise defeat the alias match.
    def snorm(v):
        s = str(v or "").strip().upper()
        for ch in (" ", "\t", "\n", "\r", "\xa0", "​", "‌",
                  "‍", "﻿", ".", "_", "-", "/", "\\", "(", ")"):
            s = s.replace(ch, "")
        return s

    cmap = {"HIST": {}}

    # Identity columns on the LEFT of the stock sheet.  These are the
    # per-M-CODE product columns the newer workbook layout added on
    # top of Merge Code + Group.  Alias sets are matched against the
    # aggressively-normalised header so a typo or a hyphenated variant
    # still maps to the right slot.
    IDENTITY_ALIASES = {
        "MERGE_CODE":   ("MERGECODE", "MERGECD"),
        "GROUP":        ("NEWGROUP", "GROUP", "SEG", "SEGMENT"),
        "CLASSIFICATION": ("CLASSIFICATION", "CLASS"),
        "MCODE":        ("CODE", "MCODE", "MATERIAL", "MATERIALCODE"),
        # Product Name is the material's variant name.  Broad alias set
        # so Excel-authored headers like "Product Name", "PROD NAME",
        # "Product_Name", "ProductName", "Model Name", "PROD" all map to
        # the same slot.  A pure "NAME" header would be ambiguous so we
        # keep it out unless a "PRODUCT" prefix appears.
        "PRODUCT_NAME": ("PRODUCTNAME", "PRODNAME", "MODEL", "PRODUCT",
                         "MODELNAME", "PRODUCTMODEL", "MODELPRODUCT",
                         "ITEMNAME", "PROD", "PRODUCTNM", "PRDNAME"),
        "DESCRIPTION":  ("DESCRIPTION", "DESC", "PRODUCTDESCRIPTION",
                         "MATERIALDESCRIPTION"),
        "SIZE":         ("SIZE", "TYRESIZE", "TIRESIZE"),
        "INCH":         ("INCH", "RIM", "RIMINCH", "RIMDIAM"),
        "PATTERN":      ("PTTN", "PATTERN", "PATTERNCODE"),
        "OPE":          ("OPE", "OPESTATUS", "AU", "AUSTATUS", "STATUS"),
    }
    for c, h in enumerate(header, start=1):
        n = snorm(h)
        for slot, aliases in IDENTITY_ALIASES.items():
            if n in aliases:
                cmap.setdefault(slot, c)
                break

    # Walk left-to-right in a small state machine.  Every time we see
    # NSW/QLD/VIC/WA in row 2 we buffer the four indices, then look at
    # the NEXT header cell to figure out which section this state group
    # closes into.
    STATES4 = ["NSW", "QLD", "VIC", "WA"]
    def _label_maps_to(section, state_cols):
        d = cmap.setdefault(section, {})   # return the dict *inside* cmap
        d.update(dict(zip(STATES4, state_cols)))
        return d

    buf = []      # list of (col, header) for the current 4-state block
    i = 0
    while i < len(header):
        h = norm(header[i]).upper()
        col = i + 1
        # If the current cell is a state name AND the next 3 are also
        # state names, this is the start of a 4-state group.
        if (h in STATES4
                and i + 3 < len(header)
                and norm(header[i+1]).upper() == "QLD"
                and norm(header[i+2]).upper() == "VIC"
                and norm(header[i+3]).upper() == "WA"):
            state_cols = [col, col+1, col+2, col+3]
            # The label immediately AFTER the 4 state cells identifies
            # the section this group belongs to.
            label = norm(header[i+4]).upper() if i + 4 < len(header) else ""
            if label.startswith("-") and label.endswith("M"):
                try:
                    m = int(label[1:-1])
                    d = _label_maps_to(f"HIST_{m}", state_cols)
                    d["TOTAL"] = i + 5   # the label cell itself carries the monthly total
                    cmap["HIST"][m] = d
                except ValueError:
                    pass
            elif "3M_A" in label or "3M A" in label:
                d = _label_maps_to("PERIOD_3M", state_cols); d["TOTAL"] = i + 5
            elif "6M_A" in label or "6M A" in label:
                d = _label_maps_to("PERIOD_6M", state_cols); d["TOTAL"] = i + 5
            elif "12M_A" in label or "12M A" in label:
                d = _label_maps_to("PERIOD_12M", state_cols); d["TOTAL"] = i + 5
            elif "12M TOT" in label:
                d = _label_maps_to("PERIOD_12M_TOT", state_cols); d["TOTAL"] = i + 5
            i += 5
            continue

        # State-stock blocks: pattern is "After Sto | Σ 3M Ave |
        # <STATE>.STOCK | PORT | WATER | FAC | TOTAL" (7 cols per
        # state, then a 5-col national block after WA).
        if h in ("N.STOCK", "Q.STOCK", "V.STOCK", "W.STOCK"):
            st = {"N.STOCK": "NSW", "Q.STOCK": "QLD",
                  "V.STOCK": "VIC", "W.STOCK": "WA"}[h]
            cmap.setdefault("STATE_STOCK", {})[st] = col
            # PORT/WATER/FAC follow in the next three cells
            if norm(header[i+1]).upper() == "PORT":
                cmap.setdefault("STATE_PORT",  {})[st] = i + 2
            if norm(header[i+2]).upper() == "WATER":
                cmap.setdefault("STATE_WATER", {})[st] = i + 3
            if norm(header[i+3]).upper() == "FAC":
                cmap.setdefault("STATE_FAC",   {})[st] = i + 4
            i += 5   # jump past After Sto / Σ 3M Ave / STOCK / PORT / WATER / FAC / TOTAL
            continue

        # National total block: STOCK / PORT / WATER / FAC / TOTAL
        if h == "STOCK" and "TOTAL_STOCK" not in cmap:
            cmap["TOTAL_STOCK"] = col
            if norm(header[i+1]).upper() == "PORT":
                cmap["TOTAL_PORT"]  = i + 2
            if norm(header[i+2]).upper() == "WATER":
                cmap["TOTAL_WATER"] = i + 3
            if norm(header[i+3]).upper() == "FAC":
                cmap["TOTAL_FAC"]   = i + 4
            if norm(header[i+4]).upper() == "TOTAL":
                cmap["TOTAL_ALL"]   = i + 5
            i += 5
            continue

        i += 1

    cmap["_HEADER_ROW"] = header_row
    cmap["_HEADER_CELLS"] = header
    return cmap


# ── Marketing line (Group in the user's vernacular) ──────────────
# Derived from the pattern code.  Hankook / Laufenn use a stable
# letter-prefix system that maps to a marketing line:
#   K7xx / K4xx / K3xx / KH..     → Kinergy    (touring / comfort)
#   K1xx (K107/K115/K117/K120…)   → Ventus     (UHP / performance)
#   RA / RF / RH / RT             → Dynapro    (SUV / LT / MT)
#   LG / LC / LH (Laufenn)        → Laufenn G Fit
#   LK / LP / LV (Laufenn)        → Laufenn X Fit
#   LS (Laufenn)                  → Laufenn S Fit
#   LI / LW (Laufenn)             → Laufenn I Fit  (winter)
#   W (Winter)                    → Winter i*cept
#   AH / AL / AM / DH / DL / TH   → TBR / Truck (Smart / e-cube …)
#   Z / older H4xx / RH0x         → Optimo / legacy
# This is intentionally coarse — good enough to slice the shortage
# vs surplus board.  Falls back to "Other" so the column never blanks.
_LINE_RULES = [
    # Laufenn — each 2-letter pattern prefix maps to a SPECIFIC
    # sub-line, not the generic "G/S/X/I Fit" umbrella.  Based on
    # actual pattern codes seen in the AU workbook: LH41 = G FIT AS,
    # LK41 = X FIT AT, LS = S Fit EQ, LI = I Fit ICE, LV01 = X FIT
    # VAN.  Users kept asking why an LK-series pattern reads as the
    # umbrella — this row-by-row split settles it.
    (r"^LG|^LC|^LH",             "Laufenn G Fit"),
    (r"^LK|^LP|^LV",             "Laufenn X Fit"),
    (r"^LS",                     "Laufenn S Fit"),
    (r"^LI|^LW",                 "Laufenn I Fit"),
    # Hankook — pattern prefix identifies the marketing line.  Specific
    # variant (Kinergy GT / Ventus S1 EVO3 / Dynapro AT2) is added
    # separately by _marketing_line_from_desc() when the description
    # carries it.
    (r"^RA|^RF|^RH0|^RH1|^RT",   "Dynapro"),
    (r"^K1\d\d",                 "Ventus"),
    (r"^K7\d\d|^K4\d\d|^K3\d\d|^KH", "Kinergy"),
    (r"^H4\d\d",                 "Kinergy"),
    (r"^W\d\d\d",                "Winter i*cept"),
    (r"^AH|^AL|^AM|^DH|^DL|^TH|^SM|^TL", "Truck / TBR"),
    (r"^Z\d\d\d",                "Optimo (legacy)"),
]

def _marketing_line(pattern):
    """Map a raw pattern code (K425, RA33, LH41…) to a marketing line
    name suitable for grouping and filtering."""
    if not pattern:
        return ""
    p = str(pattern).strip().upper()
    for regex, name in _LINE_RULES:
        if re.match(regex, p):
            return name
    return "Other"


def _marketing_line_from_desc(desc, pattern):
    """Read the marketing line from the description's 4th comma field
    when it carries a recognisable line name (X FIT / G FIT / KINERGY
    / VENTUS / DYNAPRO / …).  The 4th field is the human-readable
    variant so it's the source of truth — the pattern-based mapping
    is only a fallback for merges whose description is missing.

    Example inputs:
      '265/70R17, LF, LK41, X FIT AT, 121S'   → 'Laufenn X Fit'
      '245/45R18, HK, K127, VENTUS S1 EVO3'   → 'Ventus'
      '205/55R16, HK, K425, KINERGY GT, 91V'  → 'Kinergy'
    """
    if desc:
        parts = str(desc).split(",")
        if len(parts) >= 4:
            variant = parts[3].strip().upper()
            # Laufenn sub-lines — check "X FIT" / "G FIT" / "S FIT" /
            # "I FIT" literals from the description.
            if "X FIT"    in variant: return "Laufenn X Fit"
            if "G FIT"    in variant: return "Laufenn G Fit"
            if "S FIT"    in variant: return "Laufenn S Fit"
            if "I FIT"    in variant: return "Laufenn I Fit"
            # Hankook marketing lines
            if "VENTUS"   in variant: return "Ventus"
            if "KINERGY"  in variant: return "Kinergy"
            if "DYNAPRO"  in variant: return "Dynapro"
            if "WINTER"   in variant or "ICEPT" in variant: return "Winter i*cept"
            if "OPTIMO"   in variant: return "Optimo"
            if "SMART"    in variant or "E-CUBE" in variant or "ECUBE" in variant:
                return "Truck / TBR"
    # Fall back to the pattern-prefix rules for merges with no desc
    return _marketing_line(pattern)


def _extract_pattern(desc):
    """Pull the pattern code out of a JAX / Sheet2 description.  Format
    is comma-separated fields where the 3rd field is the pattern code
    (K425, RA33, Z222…).  Returns "" on unusual formats so downstream
    code doesn't need a null guard."""
    if not desc:
        return ""
    parts = str(desc).split(",")
    if len(parts) < 3:
        return ""
    p = parts[2].strip()
    return p if p and not p.startswith("#") else ""


# Brand-code shorthand → canonical 2-letter code.  Hankook (HK),
# Laufenn (LF), Kingstar (KS), Aurora (AU), Kumho (KL), Michelin (MI),
# Bridgestone (BS), Yokohama (YH), Continental (CN), Goodyear (GY),
# Dunlop (DL), Pirelli (PI).  User asked for the short code in the
# Brand column instead of the full name, so BOTH the raw code AND
# the full name resolve to the short form here.  Any unknown token
# passes through uppercased so the column never blanks.
_BRAND_CODE_MAP = {
    "HK": "HK", "HANKOOK":     "HK",
    "LF": "LF", "LAUFENN":     "LF",
    "KS": "KS", "KINGSTAR":    "KS",
    "AU": "AU", "AURORA":      "AU",
    "KL": "KL", "KUMHO":       "KL",
    "MI": "MI", "MICHELIN":    "MI",
    "BS": "BS", "BRIDGESTONE": "BS",
    "YH": "YH", "YOKOHAMA":    "YH",
    "CN": "CN", "CONTINENTAL": "CN",
    "GY": "GY", "GOODYEAR":    "GY",
    "DL": "DL", "DUNLOP":      "DL",
    "PI": "PI", "PIRELLI":     "PI",
}

def _extract_brand(desc):
    """Pull the brand from a JAX / stock-sheet description.

    Two known formats co-exist:
      • Old JAX ("265/70R17, HK, VENTUS AT ATL RF11, 121S")
        — brand is the 2nd comma field.
      • New stock sheet ("205/55R16V,04,LH41,L,B,-,LF")
        — brand is the LAST comma field.

    We look at the last field first — if it maps to a known brand
    code (HK / LF / KS / AU / KL / MI / …), we use that.  Otherwise
    we fall back to the 2nd field.  Never returns a numeric "04"
    or a single-letter code.
    """
    if not desc:
        return ""
    parts = [p.strip() for p in str(desc).split(",")]
    if len(parts) < 2:
        return ""
    def _pick(token):
        if not token or token.startswith("#") or token.isdigit() or len(token) < 2:
            return ""
        u = token.upper()
        # Reject single letters or clearly non-brand tokens.
        if len(u) == 1:
            return ""
        # Known-code fast path
        if u in _BRAND_CODE_MAP:
            return _BRAND_CODE_MAP[u]
        # Alpha-only tokens 2-8 chars are plausible brand strings
        # Alpha-only tokens 2-8 chars are plausible brand codes.
        # We uppercase them so downstream consumers see a compact
        # canonical short form (HK / LF / KS / AU / …) rather than
        # a mix of "Hankook" / "HK".
        if u.isalpha() and 2 <= len(u) <= 12:
            return u
        return ""
    last = _pick(parts[-1])
    if last:
        return last
    return _pick(parts[1])


def _extract_size(desc):
    """Pull the size out of the 1st comma field (185R14C, 205/55R16…)."""
    if not desc:
        return ""
    parts = str(desc).split(",")
    first = parts[0].strip()
    return first if first and not first.startswith("#") else ""


# Groups that fall under the PCLT bucket (Passenger Car / Light Truck).
# Anything else with a truck pattern rolls into TBR.
_PCLT_GROUPS = {"SP", "HP", "UHP", "LS", "LV", "RUNFLAT", "RACING"}
_TBR_GROUPS  = {"TBR", "TBR/M", "M-T", "M/T", "MT"}

def _product_category(group, line):
    """Return 'PCLT' or 'TBR' or 'Other' for the row.  The two big
    charts on the dashboard are split along this axis so the user
    can see truck-tyre health without passenger-car noise (and vice
    versa)."""
    if not group:
        group = ""
    g = str(group).strip().upper()
    if g in _PCLT_GROUPS:
        return "PCLT"
    if g in _TBR_GROUPS:
        return "TBR"
    if line in ("Truck / TBR",):
        return "TBR"
    if line in ("Kinergy", "Ventus", "Dynapro", "Winter i*cept",
                "Optimo (legacy)",
                "Laufenn G Fit", "Laufenn X Fit",
                "Laufenn S Fit", "Laufenn I Fit"):
        return "PCLT"
    return "Other"


def _is_suv(line, pattern):
    """SUV chip.  Hankook's Dynapro line is the SUV / LT tyre family.
    Pattern-prefix fallback catches SKUs whose group label is missing
    but whose pattern code is unambiguous."""
    if line == "Dynapro":
        return True
    if pattern and re.match(r"^(RA|RF|RH)", str(pattern).upper()):
        return True
    return False


def _normalize_inch(v):
    """Normalise the rim-inch column.

    The workbook stores some inch cells as the natural human value
    ("17", "22.5") but others as that value × 100 ("1700", "2250").
    We accept both.  Anything numerically >= 100 is treated as
    x100 and divided down; the result is rendered as an integer
    when the fractional part is zero and one decimal otherwise
    (so "1700" → "17" and "2250" → "22.5")."""
    if v is None or v == "":
        return ""
    try:
        n = float(v)
    except (TypeError, ValueError):
        return str(v).strip()
    if n >= 100:
        n = n / 100.0
    if abs(n - round(n)) < 0.05:
        return str(int(round(n)))
    return f"{n:.1f}"


def _num_or_none(v):
    """Best-effort numeric coercion for LI / SR / Inch cells.  Returns
    None on '#N/A' / blank / string junk."""
    if v is None:
        return None
    if isinstance(v, str):
        if v.startswith("#"):
            return None
        try:
            return float(v.split("/")[0])   # LI can be "149/146" — take first
        except ValueError:
            return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

# Sheet2 headers we care about for enrichment
_SHEET2_COLS = ("M CODE", "Description", "Group", "Brand",
                "LI", "SS", "PLY", "S.W", "SR", "Inch",
                "Factory", "Origin", "AU")


# Preferred location for the source file on Hankook AU's Windows box.
# Set via env STOCK_DIR to override; falls back to the project dir when
# the preferred dir doesn't exist (e.g. running on the Linux server).
_DEFAULT_STOCK_DIR = os.environ.get(
    "STOCK_DIR",
    r"E:\01. work\2025\Data_Anal_Website",
)

def _latest_stock_xlsm():
    """Find the newest Stock_report file by mtime.

    Looks in _DEFAULT_STOCK_DIR first, then falls back to the project
    directory.  Filename pattern is loose so 'Stock report updated
    <date>.xlsm', 'Stock_report_updated_<date>.xlsm' and legacy
    'Stock_report_*.xlsm' variants all match."""
    candidates = []
    for base in (_DEFAULT_STOCK_DIR, _BASE):
        if not base or not os.path.isdir(base):
            continue
        for pat in ("Stock report updated*.xls[mx]",
                    "Stock_report_updated*.xls[mx]",
                    "Stock*report*updated*.xls[mx]",
                    "Stock_report_*.xls[mx]"):
            candidates.extend(glob.glob(os.path.join(base, pat)))
    candidates = sorted(set(candidates), key=os.path.getmtime, reverse=True)
    return candidates[0] if candidates else None


def _parse_data_date(path):
    """Extract the "as-of" date from the filename.

    Australian workbook naming uses DDMMYYYY (e.g. 'Stock report
    updated 15092026.xlsm' = 15 September 2026).  Some earlier files
    used MMDDYYYY, so we try DDMMYYYY first and fall back to
    MMDDYYYY if that ordering isn't a valid calendar date.  Returns
    a datetime or None."""
    if not path:
        return None
    name = os.path.basename(path)
    m = re.search(r'(\d{8})', name)
    if not m:
        return None
    s = m.group(1)
    a, b, yyyy = int(s[:2]), int(s[2:4]), int(s[4:])
    # DDMMYYYY (preferred) — succeeds when a is a valid day.
    if 1 <= b <= 12 and 1 <= a <= 31:
        try:
            return datetime(yyyy, b, a)
        except (ValueError, TypeError):
            pass
    # Fallback: MMDDYYYY.
    if 1 <= a <= 12 and 1 <= b <= 31:
        try:
            return datetime(yyyy, a, b)
        except (ValueError, TypeError):
            pass
    return None


_cache = {"path": None, "mtime": 0, "rows": None, "meta": None}

# Populated fresh during load_stock_data(); exposed on the meta object
# so the dashboard can surface "why is my product info blank?" without
# users needing to poke at the .xlsm.
_stock_load_debug = {}


def load_stock_data():
    """Read (and cache) the latest stock XLSM into a list of row dicts.

    Re-parses only when the file's mtime changes so the dashboard
    stays snappy across reloads.  Returns (rows, meta) where meta
    carries the file path, load time, and row-count breakdown."""
    path = _latest_stock_xlsm()
    if not path:
        return [], {"error": "No Stock_report_*.xlsm found in "
                             f"{_BASE!r}. Drop the latest report there."}
    mtime = os.path.getmtime(path)
    if _cache["path"] == path and _cache["mtime"] == mtime and _cache["rows"] is not None:
        return _cache["rows"], _cache["meta"]

    import openpyxl
    t0 = time.time()
    wb = openpyxl.load_workbook(path, data_only=True, keep_vba=False, read_only=True)

    # ── MM sheet: locate CODE + Merge Code columns by header name ──
    # so a re-ordered MM sheet still parses.  Also survives when
    # someone adds a header row above the data.
    merge_to_mcodes = {}
    mc_to_mcode = {}
    mm_order = []
    if "MM" in wb.sheetnames:
        ws = wb["MM"]
        code_col = merge_col = None
        header_row_mm = 1
        for r in range(1, min(6, ws.max_row + 1)):
            found = {"CODE": None, "MERGE CODE": None, "MERGE_CODE": None}
            for c in range(1, ws.max_column + 1):
                v = str(ws.cell(row=r, column=c).value or "").strip().upper()
                if v in found:
                    found[v] = c
            if found["CODE"] and (found["MERGE CODE"] or found["MERGE_CODE"]):
                code_col     = found["CODE"]
                merge_col    = found["MERGE CODE"] or found["MERGE_CODE"]
                header_row_mm = r
                break
        if code_col is None:
            code_col, merge_col, header_row_mm = 1, 2, 1
        for r in ws.iter_rows(min_row=header_row_mm + 1, values_only=True):
            if not r:
                continue
            code = r[code_col  - 1] if len(r) >= code_col  else None
            merg = r[merge_col - 1] if len(r) >= merge_col else None
            if code is None or merg is None:
                continue
            if not isinstance(merg, (int, float)):
                continue
            try:
                code_i  = int(code) if isinstance(code, (int, float)) else code
                merge_i = int(merg)
            except (TypeError, ValueError):
                continue
            merge_to_mcodes.setdefault(merge_i, []).append(code_i)
            mm_order.append((code_i, merge_i))
            mc_to_mcode.setdefault(merge_i, code_i)

    # ── Sheet2 SKU master ─────────────────────────────────────────
    # Locate the header row by looking for the "M CODE" label (very
    # forgiving — whitespace / punctuation / case are all normalised
    # away).  Column-name lookup is likewise case-insensitive so
    # "AU" / " au " / "A/U" / "OLD Stock" all resolve to the same
    # bucket.  This survives a lot of accidental workbook edits.
    m_master = {}
    def _norm_hdr(v):
        """Aggressively normalise a header label so lookups tolerate
        case, spaces, hyphens, dots and slashes."""
        s = str(v or "").strip().upper()
        for ch in (" ", ".", "_", "-", "/", "\\"):
            s = s.replace(ch, "")
        return s
    # Any of these normalised forms means "M CODE".  Sheet2 in the
    # Sep-15 workbook labels this column simply "CODE" (matching the
    # stock sheet), so "CODE" is included as a top-level alias too.
    MCODE_ALIASES = {"CODE", "MCODE", "MATERIAL", "MATERIALCODE"}
    if "Sheet2" in wb.sheetnames:
        ws = wb["Sheet2"]
        header = None
        header_row_s2 = 1
        for r in range(1, min(8, ws.max_row + 1)):
            vals = [ws.cell(row=r, column=c).value for c in range(1, ws.max_column + 1)]
            up = [_norm_hdr(v) for v in vals]
            if any(m in up for m in MCODE_ALIASES):
                header = vals
                header_row_s2 = r
                break
        if header is None:
            header = [ws.cell(row=1, column=c).value for c in range(1, ws.max_column + 1)]
            header_row_s2 = 1
        # Build a normalised-name → index map so lookups tolerate
        # whitespace / punctuation / case differences in the header.
        idx = {}
        for i, name in enumerate(header):
            key = _norm_hdr(name)
            if key and key not in idx:
                idx[key] = i
        # Locate the M CODE column dynamically — Sheet2 doesn't always
        # put M CODE in column 1 (users routinely add a leading "no."
        # column), and hard-coding r[0] silently mis-keys the whole
        # master.
        mcode_col = None
        for alias in MCODE_ALIASES:
            if alias in idx:
                mcode_col = idx[alias]; break
        if mcode_col is None:
            mcode_col = 0    # last-ditch fallback
        # Aliases per semantic field.  Multiple keys map to the same
        # slot so renamed columns still get picked up.
        FIELD_ALIASES = {
            "description": ("DESCRIPTION", "DESC", "PRODUCTDESCRIPTION",
                            "PRODUCTNAME", "NAME", "MATERIALDESCRIPTION"),
            "group":       ("GROUP", "NEWGROUP", "SEGMENT", "CATEGORY"),
            "brand":       ("BRAND", "MAKE"),
            "li":          ("LI", "LOADINDEX"),
            "ss":          ("SS", "SPEEDSYMBOL", "SPEEDRATING"),
            "ply":         ("PLY", "PR"),
            "sw":          ("SW", "SECTIONWIDTH", "WIDTH"),
            "sr":          ("SR", "SECTIONRATIO", "ASPECT", "ASPECTRATIO"),
            "inch":        ("INCH", "RIM", "RIMDIAMETER", "DIAM"),
            "factory":     ("FACTORY", "PLANT", "FACTORYORIGIN"),
            "origin":      ("ORIGIN", "COUNTRY", "MADEIN"),
            "au":          ("AU", "AUSTATUS", "STATUS", "AUFLAG",
                            "USAGE", "USE"),
            "old_stock":   ("OLDSTOCK", "OLDST", "FADEOUT", "F/O"),
        }
        def _pick(row, keys):
            for k in keys:
                if k in idx:
                    ci = idx[k]
                    if ci < len(row) and row[ci] not in (None, ""):
                        return row[ci]
            return ""
        n_master_rows = 0
        for r in ws.iter_rows(min_row=header_row_s2 + 1, values_only=True):
            if not r:
                continue
            mval = r[mcode_col] if mcode_col < len(r) else None
            if mval is None:
                continue
            try:
                m = int(mval)
            except Exception:
                continue
            m_master[m] = {name: _pick(r, keys) for name, keys in FIELD_ALIASES.items()}
            n_master_rows += 1
        _stock_load_debug["sheet2_rows"] = n_master_rows
        _stock_load_debug["sheet2_header_row"] = header_row_s2
        _stock_load_debug["sheet2_columns_found"] = sorted(
            f for f, keys in FIELD_ALIASES.items() if any(k in idx for k in keys)
        )

    def _num(v):
        if v is None:
            return 0.0
        if isinstance(v, str):
            if v.startswith('#'):
                return 0.0
            try:
                return float(v)
            except ValueError:
                return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    # ── Pass 1: read Stock Status Worksheet, keyed by Merge Code ──
    # Each Merge Code carries the stock/demand aggregates; the pass
    # that emits per-M-CODE rows will look up these values by merge.
    stock_by_merge = {}
    ws = wb["Stock Status Worksheet"]
    # Auto-detect the header row and build a semantic column map so
    # inserted/moved columns don't break the loader.
    cmap = _scan_stock_header(ws)
    header_row_ssw = cmap.pop("_HEADER_ROW", 2)
    header_cells_ssw = cmap.pop("_HEADER_CELLS", [])
    data_start_row = header_row_ssw + 1

    # Convenience lookups, with fallback to the hard-coded defaults
    # if the header scan didn't manage to identify a section (older
    # workbook variants).
    def _c(key, fallback):
        v = cmap.get(key)
        return v if v is not None else fallback
    c_merge   = _c("MERGE_CODE",     COL_MERGE_CODE)
    c_group   = _c("GROUP",          COL_GROUP)
    c_classif = _c("CLASSIFICATION", COL_CLASSIF)
    c_desc    = cmap.get("DESCRIPTION")   # may be None — used as fallback only
    # New (per-M-CODE workbook layout) columns: each stock row now
    # carries the specific material's identity as well as its sales
    # numbers, so we key by (merge, m_code) instead of merge alone.
    c_mcode        = cmap.get("MCODE")
    c_prod_name    = cmap.get("PRODUCT_NAME")
    c_size         = cmap.get("SIZE")
    c_inch         = cmap.get("INCH")
    c_pattern      = cmap.get("PATTERN")
    c_ope          = cmap.get("OPE")
    _stock_load_debug["stock_columns_found"] = {
        "merge_code":   bool(c_merge),
        "group":        bool(c_group),
        "m_code":       bool(c_mcode),
        "product_name": bool(c_prod_name),
        "description":  bool(c_desc),
        "size":         bool(c_size),
        "inch":         bool(c_inch),
        "pattern":      bool(c_pattern),
        "ope":          bool(c_ope),
    }
    # Diagnostic: record the raw header cells for the first 12 columns
    # + a compact pointer to whichever column each identity slot mapped
    # onto.  Surfaced in the UI meta line so the user can see instantly
    # when the detector attaches to the wrong row / mis-labels a slot.
    _stock_load_debug["header_row"] = header_row_ssw
    _stock_load_debug["header_first_cells"] = [
        str(h) if h is not None else "" for h in header_cells_ssw[:12]
    ]
    _stock_load_debug["identity_col_map"] = {
        "MERGE_CODE":   c_merge,
        "GROUP":        c_group,
        "MCODE":        c_mcode,
        "PRODUCT_NAME": c_prod_name,
        "DESCRIPTION":  c_desc,
        "SIZE":         c_size,
        "INCH":         c_inch,
        "PATTERN":      c_pattern,
        "OPE":          c_ope,
    }

    state_stock_cols = cmap.get("STATE_STOCK") or COL_STATE_STOCK
    state_port_cols  = cmap.get("STATE_PORT")  or {s: COL_STATE_PIPELINE[s][0] for s in STATES}
    state_water_cols = cmap.get("STATE_WATER") or {s: COL_STATE_PIPELINE[s][1] for s in STATES}
    state_fac_cols   = cmap.get("STATE_FAC")   or {s: COL_STATE_PIPELINE[s][2] for s in STATES}

    state_3m_cols    = (cmap.get("PERIOD_3M")  or {}).get if isinstance(cmap.get("PERIOD_3M"), dict) else None
    if cmap.get("PERIOD_3M"):
        state_3m_map = {s: cmap["PERIOD_3M"][s] for s in STATES if s in cmap["PERIOD_3M"]}
        total_3m_col = cmap["PERIOD_3M"].get("TOTAL", COL_TOTAL_3M)
    else:
        state_3m_map = COL_STATE_3M
        total_3m_col = COL_TOTAL_3M

    if cmap.get("PERIOD_12M"):
        total_12m_col = cmap["PERIOD_12M"].get("TOTAL", COL_TOTAL_12M)
    else:
        total_12m_col = COL_TOTAL_12M

    total_stock_col = _c("TOTAL_STOCK", COL_TOTAL_STOCK)
    total_all_col   = _c("TOTAL_ALL",   COL_TOTAL_TOTAL)

    # History start column (the -12M NSW cell).  Prefer the scanned
    # HIST[12] map; fall back to the historical default (col 4).
    hist_map = cmap.get("HIST") or {}
    if 12 in hist_map:
        hist_start = hist_map[12]["NSW"]
    else:
        hist_start = HIST_COL_START

    # Whether each stock-sheet row is per-M-CODE (newer workbook) or
    # per-Merge (older).  When c_mcode is present we treat every
    # (merge, m_code) row as its own SKU with its OWN sales / stock
    # figures; when it isn't we fall back to the old merge-only path.
    per_mcode_rows = c_mcode is not None
    _stock_load_debug["per_mcode_rows"] = per_mcode_rows
    # stock_rows collects (merge_code, m_code, detail) triples in
    # workbook order so Pass 2 can iterate them directly.  merge-level
    # aggregation (for Sub Total rows + KPIs) is derived from these.
    stock_rows = []
    stock_by_merge = {}     # kept for merge-level lookups (Sub Total etc.)

    for r in ws.iter_rows(min_row=data_start_row, values_only=True):
        if not r:
            continue
        mc = r[c_merge - 1] if len(r) >= c_merge else None
        if mc is None:
            continue
        try:
            mc = int(mc)
        except Exception:
            continue
        # In per-M-CODE mode, pull THIS row's M CODE.  Rows without a
        # valid M CODE are skipped so junk lines don't pollute the
        # dataset.  In merge-only mode we synthesise m_code = merge.
        m_code_row = None
        if per_mcode_rows:
            v = r[c_mcode - 1] if len(r) >= c_mcode else None
            try:
                m_code_row = int(v) if v is not None else None
            except (TypeError, ValueError):
                m_code_row = None
            if m_code_row is None:
                continue
        else:
            m_code_row = mc

        group   = r[c_group   - 1] if len(r) >= c_group   else None
        classif = r[c_classif - 1] if len(r) >= c_classif else None
        # Row-level product info from the stock sheet.  When present
        # this is the SOURCE OF TRUTH — description / size / inch /
        # pattern / OPE are read directly from the row and become
        # merge_details fallback for Pass 2.
        row_prod_name = ""
        row_desc      = ""
        row_size      = ""
        row_inch      = ""
        row_pattern   = ""
        row_ope       = ""
        if c_prod_name:
            v = r[c_prod_name - 1] if len(r) >= c_prod_name else None
            if v is not None: row_prod_name = str(v).strip()
        if c_desc:
            v = r[c_desc - 1] if len(r) >= c_desc else None
            if v is not None: row_desc = str(v).strip()
        if c_size:
            v = r[c_size - 1] if len(r) >= c_size else None
            if v is not None: row_size = str(v).strip()
        if c_inch:
            v = r[c_inch - 1] if len(r) >= c_inch else None
            if v is not None: row_inch = str(v).strip()
        if c_pattern:
            v = r[c_pattern - 1] if len(r) >= c_pattern else None
            if v is not None: row_pattern = str(v).strip()
        if c_ope:
            v = r[c_ope - 1] if len(r) >= c_ope else None
            if v is not None: row_ope = str(v).strip()

        # ssw_desc keeps its old meaning (merge-level fallback text)
        # so downstream code doesn't need to change.  Prefer the
        # row's own Description if the workbook has one, else the
        # Product Name.
        ssw_desc = row_desc or row_prod_name

        def _cell(col):
            return r[col - 1] if col and len(r) >= col else None
        state_stock = {s: _num(_cell(state_stock_cols[s])) for s in STATES}
        state_pipe_parts = {
            s: {
                "port":  _num(_cell(state_port_cols[s])),
                "water": _num(_cell(state_water_cols[s])),
                "fac":   _num(_cell(state_fac_cols[s])),
            } for s in STATES
        }
        state_pipe = {s: sum(state_pipe_parts[s].values()) for s in STATES}
        state_3m   = {s: _num(_cell(state_3m_map[s])) for s in STATES}
        total_stock = _num(_cell(total_stock_col))
        total_all   = _num(_cell(total_all_col))
        # Fallback: if the workbook doesn't populate a national
        # TOTAL column (Stock + Port + Water + Factory), compute it
        # ourselves from the per-state pipeline parts so
        # Merge_MOI(PPL) never comes out identically zero.
        if total_all == 0 and total_stock > 0:
            total_all = total_stock + sum(state_pipe.values())
        total_3m    = _num(_cell(total_3m_col))

        # 12-month sales history per state.  Prefer the scanned per-
        # month HIST map when present; otherwise fall back to the
        # historical 5-column-per-month stride from hist_start.
        history = {"NSW": [], "QLD": [], "VIC": [], "WA": [], "TOTAL": []}
        for month in range(HIST_MONTHS):
            m_back = 12 - month   # -12M … -1M
            if m_back in hist_map:
                d = hist_map[m_back]
                history["NSW"].append(_num(_cell(d["NSW"])))
                history["QLD"].append(_num(_cell(d["QLD"])))
                history["VIC"].append(_num(_cell(d["VIC"])))
                history["WA"].append(_num(_cell(d["WA"])))
                history["TOTAL"].append(_num(_cell(d.get("TOTAL", d["WA"] + 1))))
            else:
                base = hist_start + month * HIST_BLOCK_LEN
                history["NSW"].append(_num(_cell(base)))
                history["QLD"].append(_num(_cell(base + 1)))
                history["VIC"].append(_num(_cell(base + 2)))
                history["WA"].append(_num(_cell(base + 3)))
                history["TOTAL"].append(_num(_cell(base + 4)))

        # 12M average
        total_12m = _num(_cell(total_12m_col))

        # ── Period-average demand ─────────────────────────────────
        # Every period average is computed the SAME way — sum three
        # consecutive months from history["TOTAL"] and divide by 3 —
        # so the four "3M / 4-6M / 7-9M / 10-12M" columns are truly
        # comparable across the row.  history["TOTAL"] is indexed
        # -12M(0), -11M(1), -10M(2), -9M(3), -8M(4), -7M(5), -6M(6),
        # -5M(7), -4M(8), -3M(9), -2M(10), -1M(11).
        h_tot = history["TOTAL"]
        if len(h_tot) >= 12:
            p_3m       = (h_tot[9]  + h_tot[10] + h_tot[11]) / 3.0   # months -1 .. -3
            avg_6m_old = (h_tot[6]  + h_tot[7]  + h_tot[8])  / 3.0   # months -4 .. -6
            avg_7_9m   = (h_tot[3]  + h_tot[4]  + h_tot[5])  / 3.0   # months -7 .. -9
            avg_10_12m = (h_tot[0]  + h_tot[1]  + h_tot[2])  / 3.0   # months -10 .. -12
        else:
            p_3m = avg_6m_old = avg_7_9m = avg_10_12m = 0.0

        # Max-demand basis: the LARGEST of the four period averages
        # (3M · 4-6M · 7-9M · 10-12M), per user request.  Using the
        # biggest recent baseline protects against under-stocking
        # when the most recent 3M avg has temporarily dipped.
        max_demand = max(p_3m, avg_6m_old, avg_7_9m, avg_10_12m,
                         0.0) if any([p_3m, avg_6m_old, avg_7_9m, avg_10_12m]) else 0.0

        row_bundle = {
            "group_raw":       group,
            "classification":  classif or "",
            "ssw_desc":        ssw_desc,
            "row_prod_name":   row_prod_name,
            "row_desc":        row_desc,
            "row_size":        row_size,
            "row_inch":        row_inch,
            "row_pattern":     row_pattern,
            "row_ope":         row_ope,
            "state_stock":     state_stock,
            "state_pipe_parts":state_pipe_parts,
            "state_pipe":      state_pipe,
            "state_3m":        state_3m,
            "total_stock":     total_stock,
            "total_all":       total_all,
            "total_3m":        total_3m,
            "total_12m":       total_12m,
            "p_3m":            p_3m,
            "avg_6m_old":      avg_6m_old,
            "avg_7_9m":        avg_7_9m,
            "avg_10_12m":      avg_10_12m,
            "max_demand":      max_demand,
            "history":         history,
        }
        stock_rows.append((mc, m_code_row, row_bundle))
        # Merge-level aggregate — used for Sub Total rows and the
        # KPI cards.  When multiple rows share a merge (the per-M-
        # CODE workbook), sums accumulate; when they don't, the
        # single row becomes the merge total (identical values on
        # both).  history is stored as a per-state list of monthly
        # SUMS across all M CODEs of the merge.
        agg = stock_by_merge.get(mc)
        if agg is None:
            agg = {
                "group_raw":       group,
                "classification":  classif or "",
                "ssw_desc":        ssw_desc,
                # Row-level product info harvested from the FIRST
                # non-empty stock-sheet cell across all rows sharing
                # this merge.  Merge-shared M CODEs (siblings from
                # the MM sheet without their own stock row) inherit
                # these so they don't render as blank rows.
                "row_prod_name":   row_prod_name,
                "row_desc":        row_desc,
                "row_size":        row_size,
                "row_inch":        row_inch,
                "row_pattern":     row_pattern,
                "row_ope":         row_ope,
                "state_stock":     {s: 0.0 for s in STATES},
                "state_pipe_parts":{s: {"port":0.0,"water":0.0,"fac":0.0} for s in STATES},
                "state_pipe":      {s: 0.0 for s in STATES},
                "state_3m":        {s: 0.0 for s in STATES},
                "total_stock":     0.0,
                "total_all":       0.0,
                "total_3m":        0.0,
                "total_12m":       0.0,
                "p_3m":            0.0,
                "avg_6m_old":      0.0,
                "avg_7_9m":        0.0,
                "avg_10_12m":      0.0,
                "max_demand":      0.0,
                "history":         {k: [0.0]*HIST_MONTHS for k in ("NSW","QLD","VIC","WA","TOTAL")},
            }
            stock_by_merge[mc] = agg
        else:
            # First-non-empty fill for the merge-level product-info
            # fallback fields — later stock-sheet rows for the same
            # merge only patch in what the first row was missing.
            for k, v in (("row_prod_name", row_prod_name),
                         ("row_desc",      row_desc),
                         ("row_size",      row_size),
                         ("row_inch",      row_inch),
                         ("row_pattern",   row_pattern),
                         ("row_ope",       row_ope),
                         ("ssw_desc",      ssw_desc)):
                if not agg.get(k) and v:
                    agg[k] = v
        for s in STATES:
            agg["state_stock"][s] += state_stock[s]
            agg["state_pipe"][s]  += state_pipe[s]
            agg["state_3m"][s]    += state_3m[s]
            for leg in ("port","water","fac"):
                agg["state_pipe_parts"][s][leg] += state_pipe_parts[s][leg]
        agg["total_stock"] += total_stock
        agg["total_all"]   += total_all
        agg["total_3m"]    += total_3m
        agg["total_12m"]   += total_12m
        agg["p_3m"]        += p_3m
        agg["avg_6m_old"]  += avg_6m_old
        agg["avg_7_9m"]    += avg_7_9m
        agg["avg_10_12m"]  += avg_10_12m
        for k, arr in history.items():
            for i, v in enumerate(arr):
                agg["history"][k][i] += v
        # max_demand recomputed at read-time from the FOUR summed
        # period averages so it stays a true "biggest baseline".
        agg["max_demand"] = max(agg["p_3m"], agg["avg_6m_old"],
                                agg["avg_7_9m"], agg["avg_10_12m"], 0.0)

    # ── Pass 1.5: build a per-merge "best product info" cache.
    # Walk every merge that has at least one M CODE in the MM sheet
    # and aggregate the first-seen non-empty value for each field
    # across all its siblings' Sheet2 records.  Pass 2 then uses this
    # as the FIRST fallback source before touching individual M CODE
    # records — so a merge like 1156 with ten empty M CODEs and one
    # populated sibling has that one populate ALL ten rows.
    NEED_FIELDS = ("description", "brand", "sw", "sr", "inch",
                   "li", "ss", "group", "factory", "origin", "au",
                   "old_stock", "ply")
    merge_details = {}
    for merge_code, siblings in merge_to_mcodes.items():
        aggregate = {}
        for sib in siblings:
            rec = m_master.get(sib)
            if not rec:
                continue
            for f in NEED_FIELDS:
                v = rec.get(f)
                if v and f not in aggregate:
                    aggregate[f] = v
            if len(aggregate) == len(NEED_FIELDS):
                break
        if aggregate:
            merge_details[merge_code] = aggregate

    # ── Pass 2: build the iteration plan.
    # We want a dashboard row for every M CODE the workbook knows
    # about, from ANY source:
    #   • stock_rows (per-M-CODE granularity in the stock sheet)
    #   • MM sheet (M CODE ↔ Merge Code mapping)
    #   • stock_by_merge (merges with a stock row but no MM entry)
    # For each (merge, m_code) pair, we prefer per-M-CODE data when
    # the stock sheet carries it; otherwise the row inherits the
    # merge aggregate and is tagged `merge_shared` so downstream
    # aggregation can dedupe by merge instead of summing (which
    # would multi-count siblings).
    rows_out = []
    seen_pairs = set()
    # stock_by_pair only carries GENUINE per-M-CODE bundles.  In
    # per-merge workbook layout the m_code_row was synthesised to
    # equal the merge code, so those entries don't count as
    # per-M-CODE data — everything from MM sheet inherits the merge
    # aggregate instead.
    stock_by_pair = ({(mc, m): stk for (mc, m, stk) in stock_rows}
                     if per_mcode_rows else {})

    # Collect every (merge, m_code) pair we've seen, preserving order:
    # per-M-CODE stock rows first (workbook order) then MM entries
    # not already covered.  A merge with NO M CODE anywhere gets one
    # synthetic row (merge as its own M CODE) at the end.
    pair_order = []
    seen_plan = set()
    if per_mcode_rows:
        for (mc_sr, m_sr) in stock_by_pair.keys():
            if (mc_sr, m_sr) not in seen_plan:
                pair_order.append((mc_sr, m_sr))
                seen_plan.add((mc_sr, m_sr))
    for m_code_mm, merge_code_mm in mm_order:
        if merge_code_mm not in stock_by_merge:
            continue        # merge has no stock row anywhere — skip
        key = (merge_code_mm, m_code_mm)
        if key not in seen_plan:
            pair_order.append(key)
            seen_plan.add(key)
    # Merges with stock rows but no MM entry → one synthetic row.
    merges_covered = {mc for (mc, _) in seen_plan}
    for merge_code in stock_by_merge:
        if merge_code in merges_covered:
            continue
        pair_order.append((merge_code, merge_code))
        seen_plan.add((merge_code, merge_code))

    iter_plan = []
    for (merge_code, m_code) in pair_order:
        stk = stock_by_merge.get(merge_code)
        if stk is None:
            continue
        pair_stk = stock_by_pair.get((merge_code, m_code))
        if pair_stk is not None:
            # Per-M-CODE bundle exists — this row carries its own
            # unique stock/demand figures.
            iter_plan.append((merge_code, m_code, pair_stk, False))
        else:
            # No per-M-CODE row in the stock sheet — inherit the
            # merge aggregate (all siblings share the same numbers).
            iter_plan.append((merge_code, m_code, stk, True))

    for merge_code, m_code, stk, merge_shared in iter_plan:
        if (m_code, merge_code) in seen_pairs:
            continue
        seen_pairs.add((m_code, merge_code))
        mc = merge_code
        # Take THIS M CODE's Sheet2 record as the starting point, then
        # patch any blank field from the merge-wide aggregate built in
        # Pass 1.5 — that already scanned every sibling once, so we
        # don't need to re-walk them here.  Siblings within a Merge
        # share size / brand / pattern, so borrowing is safe.
        detail = dict(m_master.get(m_code, {}))
        merge_agg = merge_details.get(merge_code, {})
        for f in NEED_FIELDS:
            if not detail.get(f) and merge_agg.get(f):
                detail[f] = merge_agg[f]

        # Row-level product info from the stock sheet takes precedence
        # over Sheet2 fallbacks whenever the stock-sheet column is
        # populated (this is the source of truth for the new
        # per-M-CODE workbook layout).  Per-M-CODE rows can have blank
        # cells (e.g. only the first sibling has an OPE flag), so we
        # fall back to the merge-wide first-non-empty aggregate stored
        # on stock_by_merge — this is the same aggregate that already
        # feeds merge-shared rows in Pass 1.
        merge_stk = stock_by_merge.get(merge_code, {}) if not merge_shared else stk
        def _pick(field):
            v = stk.get(field, "") or ""
            if v:
                return v
            return merge_stk.get(field, "") or ""
        row_pn      = _pick("row_prod_name")
        row_desc    = _pick("row_desc")
        row_size_raw= _pick("row_size")
        row_inch_raw= _pick("row_inch")
        row_pat_raw = _pick("row_pattern")
        row_ope_raw = _pick("row_ope")
        # Detail fallback chain: Sheet2 → merge aggregate → stock row
        if not detail.get("description") and row_desc:
            detail["description"] = row_desc
        if not detail.get("au") and row_ope_raw:
            detail["au"] = row_ope_raw
        if not detail.get("inch") and row_inch_raw:
            detail["inch"] = row_inch_raw

        raw_desc = detail.get("description", "")
        # Ultimate description fallback: Product Name column, then the
        # merge-level ssw_desc.  Newly-created merges often have their
        # name typed on the stock sheet even though the master record
        # hasn't been entered into Sheet2 yet.
        if not raw_desc and row_pn:
            raw_desc = row_pn
        if not raw_desc and stk.get("ssw_desc"):
            raw_desc = stk["ssw_desc"]
        pattern  = row_pat_raw or _extract_pattern(raw_desc)
        # Marketing line uses the description's variant field first
        # (so "X FIT AT" → "Laufenn X Fit" rather than the generic
        # "G/S/X/I Fit" umbrella).  Falls back to pattern-prefix
        # rules when the description has no useable variant field.
        # For the new workbook layout, Product Name (e.g. "Ventus TD",
        # "G FIT AS", "X FIT Van") is often the cleanest source of
        # sub-line info, so parse it first.
        line = ""
        if row_pn:
            up = row_pn.upper()
            if   "X FIT"    in up: line = "Laufenn X Fit"
            elif "G FIT"    in up: line = "Laufenn G Fit"
            elif "S FIT"    in up: line = "Laufenn S Fit"
            elif "I FIT"    in up: line = "Laufenn I Fit"
            elif "Z FIT"    in up: line = "Laufenn Z Fit"
            elif "VENTUS"   in up: line = "Ventus"
            elif "KINERGY"  in up: line = "Kinergy"
            elif "DYNAPRO"  in up: line = "Dynapro"
            elif "OPTIMO"   in up: line = "Optimo"
            elif "SMART"    in up or "E-CUBE" in up or "ECUBE" in up: line = "Truck / TBR"
            elif "WINTER"   in up or "ICEPT" in up: line = "Winter i*cept"
            elif "ION"      in up or "IONEV" in up: line = "iON"
        if not line:
            line = _marketing_line_from_desc(raw_desc, pattern)
        # eff_group prefers the row's own group; falls back to Sheet2
        # group.  Leading "1UHP&PCR", "2SUV&LTR" style codes have their
        # rank prefix stripped for display.
        raw_group = str(stk.get("group_raw") or detail.get("group") or "").strip()
        eff_group = re.sub(r"^\d+\s*", "", raw_group)

        # Brand: prefer the Sheet2 Brand column, fall back to whatever
        # the description's 2nd comma field carries (HK→Hankook, KS→
        # Kingstar…) so a merge with only a Description still shows the
        # brand.  Sheet2's Brand column often uses the same 2-letter
        # shorthand (LF, HK, KS, AU) so we run the whole thing through
        # the brand-code map — a stray "LF" that survives now becomes
        # "Laufenn" like everything else.
        raw_brand = str(detail.get("brand", "") or "").strip()
        if raw_brand:
            brand = _BRAND_CODE_MAP.get(raw_brand.upper(), raw_brand)
        else:
            brand = _extract_brand(raw_desc)

        # Size: prefer the stock sheet's SIZE column (source of truth
        # in the per-M-CODE layout), fall back to the description's
        # 1st comma-field, then to SW/SR/Inch composition.
        size = row_size_raw or _extract_size(raw_desc)
        if not size and detail.get("sw") and detail.get("sr") and detail.get("inch"):
            try:
                sw = int(detail["sw"]); sr = int(detail["sr"]); inch = detail["inch"]
                inch_s = str(inch).rstrip('0').rstrip('.') if isinstance(inch, float) else str(inch)
                size = f"{sw}/{sr}R{inch_s}"
            except Exception:
                pass

        # Fetch merge-level metrics
        state_stock = stk["state_stock"]
        state_pipe_parts = stk["state_pipe_parts"]
        state_pipe  = stk["state_pipe"]
        state_3m    = stk["state_3m"]
        total_stock = stk["total_stock"]
        total_all   = stk["total_all"]
        total_3m    = stk["total_3m"]
        total_12m   = stk["total_12m"]
        p_3m        = stk["p_3m"]
        avg_6m_old  = stk["avg_6m_old"]
        avg_7_9m    = stk["avg_7_9m"]
        avg_10_12m  = stk["avg_10_12m"]
        max_demand  = stk["max_demand"]
        history     = stk["history"]
        classif     = stk["classification"]

        moh = (total_stock / total_3m) if total_3m > 0 else None

        # MOI including entire Factory pipeline (incoming KR/JP/HU) —
        # a longer-horizon planning metric than the current MOI.
        moh_plus = (total_all / total_3m) if total_3m > 0 else None

        # Merge_MOI(PPL): (Stock + Port + Water + Factory) ÷
        # MAX(3M Avg, 4-6M Avg, 7-9M Avg, 10-12M Avg).  Uses the
        # full available inventory (on-hand + all incoming) as the
        # numerator and the biggest recent monthly draw as the
        # denominator, so planners see coverage against the busiest
        # of the last four periods.
        moh_plus_max = (total_all / max_demand) if max_demand > 0 else None

        # Status classification uses the MERGE-level MOI so every
        # sibling M CODE in a merge lands in the same tab.  The row's
        # own moh (which can differ per M CODE) drives the numeric
        # colour on the row, but the filter bucket comes from the
        # merge total — otherwise one Balance-shaped M CODE would
        # split off from its Surplus-shaped siblings and confuse the
        # merge grouping the user reads by.
        merge_stock = stock_by_merge[merge_code]["total_stock"]
        merge_3m    = stock_by_merge[merge_code]["total_3m"]
        merge_moh   = (merge_stock / merge_3m) if merge_3m > 0 else None
        if merge_3m == 0 and merge_stock == 0:
            status = "empty"
        elif merge_3m == 0 and merge_stock > 0:
            status = "no_move"
        elif merge_moh is not None and merge_moh <= STATUS_SHORTAGE_MOI:
            status = "shortage"
        elif merge_moh is not None and merge_moh <= STATUS_BALANCE_MOI:
            status = "balanced"
        elif merge_moh is not None and merge_moh <= STATUS_SURPLUS_MOI:
            status = "surplus"
        else:
            status = "serious_surplus"

        # Product characteristics for the quick-filter chips
        category = _product_category(eff_group, line)
        inch_num = _num_or_none(detail.get("inch"))
        sr_num   = _num_or_none(detail.get("sr"))
        is_18plus     = inch_num is not None and inch_num >= 18
        is_low_prof   = sr_num   is not None and sr_num   < 50
        is_suv_flag   = _is_suv(line, pattern)

        # F/O · OPE indicator derived from the stock sheet's OPE
        # column (source of truth in the new layout) with Sheet2's AU
        # column as fallback.  The value may be a free-form tag
        # (multiple tokens joined by "/" or space) so we walk the
        # string looking for the seven recognised buckets, most
        # specific first.  The result is a compact "+"-joined string
        # so a filter dropdown can key on any single bucket.
        # Recognised buckets:
        #   F/O · OPE · M/S · Testing · Transfer · OE A/S · Price
        au = str(row_ope_raw or detail.get("au", "") or "").upper().strip()
        old_stock = str(detail.get("old_stock", "") or "").upper().strip()
        sku_flags = []
        if "FADE OUT" in au or old_stock == "YES":
            sku_flags.append("F/O")
        # "OE A/S" is more specific than plain "OPE" — check it first
        # so "OPE / OE A/S" doesn't double-fire.
        if "OE A/S" in au or "OE_AS" in au or "OEA/S" in au:
            sku_flags.append("OE A/S")
        if "OPE" in au and "OE A/S" not in au and "OE_AS" not in au and "OEA/S" not in au:
            sku_flags.append("OPE")
        if "M/S" in au or " MS " in " " + au + " " or au.startswith("MS ") or au.endswith(" MS") or au == "MS":
            sku_flags.append("M/S")
        if "TESTING" in au or "TEST" in au:
            sku_flags.append("Testing")
        if "TRANSFER" in au or "TRF" in au:
            sku_flags.append("Transfer")
        if "PRICE" in au:
            sku_flags.append("Price")
        sku_status = " + ".join(sku_flags) if sku_flags else "Active"

        rows_out.append({
            "merge_code":     mc,
            "m_code":         m_code,
            # `merge_shared` = true when the row inherits the merge
            # total (per-merge workbook layout) instead of carrying
            # its own per-M-CODE numbers.  The frontend uses this
            # flag so KPI / state-card aggregation dedupes shared
            # rows by merge instead of summing them (which would
            # multi-count).
            "merge_shared":   merge_shared,
            "group":          eff_group,
            "classification": classif or "",
            "brand":          brand,
            "line":           line,           # Marketing line umbrella (Kinergy / Dynapro / Ventus / Laufenn X Fit…)
            # Prefer the stock sheet's Product Name column; fall back
            # to the marketing line so the column is never blank when
            # the production workbook doesn't carry a Product Name
            # column at all.
            "product_name":   row_pn or line,
            "pattern":        pattern,        # Pattern code (K425, RA33…)
            "description":    raw_desc,
            "size":           size,
            "inch":           _normalize_inch(detail.get("inch") or row_inch_raw or ""),
            "sr":             detail.get("sr", ""),
            "li":             detail.get("li", ""),
            "ss":             detail.get("ss", ""),
            "factory":        detail.get("factory", ""),
            "origin":         detail.get("origin", ""),
            # F/O · OPE indicator
            "sku_status":     sku_status,     # 'F/O' / 'OPE' / 'F/O + OPE' / 'OE A/S' / 'Active'
            # Quick-filter flags used by the top chip bar
            "category":       category,        # "PCLT" | "TBR" | "Other"
            "is_18plus":      is_18plus,
            "is_low_profile": is_low_prof,
            "is_suv":         is_suv_flag,
            "state_stock":       state_stock,
            "state_pipeline":    state_pipe,
            "state_pipe_parts":  state_pipe_parts,
            "state_3m":          state_3m,
            "history":           history,    # 12 months by state
            "total_stock":       total_stock,
            "total_all":         total_all,
            "total_3m":          total_3m,           # sheet's pre-computed 3M_A (MOI basis)
            "p_3m":              round(p_3m, 2),     # history-derived 3M avg (period column)
            "avg_6m_old":        round(avg_6m_old, 2),
            "avg_7_9m":          round(avg_7_9m, 2),
            "avg_10_12m":        round(avg_10_12m, 2),
            "total_12m":         round(total_12m, 2),
            "max_demand":        round(max_demand, 2),
            "moh":               round(moh, 2)          if moh          is not None else None,
            "moh_plus":          round(moh_plus, 2)     if moh_plus     is not None else None,
            "moh_plus_max":      round(moh_plus_max, 2) if moh_plus_max is not None else None,
            "status":            status,
        })

    dd = _parse_data_date(path)
    if dd:
        # Cross-platform "9 November 2026": strip leading zero from day.
        data_date_str = dd.strftime("%d %B %Y").lstrip("0")
    else:
        data_date_str = "unknown"
    # Diagnostics — which merges still ended up with no product info?
    # Count them per-merge (not per-row).  A merge counts as "no info"
    # only when EVERY M CODE row in it lacks product identifiers
    # (brand / line / size / product name / pattern) — a partially-
    # filled merge doesn't get flagged.
    merge_any_info = {}
    for row in rows_out:
        mc = row["merge_code"]
        has_info = bool(row.get("brand") or row.get("line") or row.get("size")
                        or row.get("product_name") or row.get("pattern"))
        merge_any_info[mc] = merge_any_info.get(mc, False) or has_info
    no_info_merges = [mc for mc, has in merge_any_info.items() if not has]
    seen_merges = merge_any_info
    meta = {
        "path":       os.path.basename(path),
        "path_dir":   os.path.dirname(path),
        "mtime":      datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M"),
        "data_date":  data_date_str,
        "rows":       len(rows_out),
        "load_s":     round(time.time() - t0, 2),
        "sheet2_rows":            _stock_load_debug.get("sheet2_rows", 0),
        "sheet2_header_row":      _stock_load_debug.get("sheet2_header_row", 1),
        "sheet2_columns_found":   _stock_load_debug.get("sheet2_columns_found", []),
        "stock_columns_found":    sorted(
            k for k, v in (_stock_load_debug.get("stock_columns_found") or {}).items() if v
        ),
        "stock_header_row":       _stock_load_debug.get("header_row", 0),
        "stock_header_cells":     _stock_load_debug.get("header_first_cells", []),
        "stock_identity_cols":    _stock_load_debug.get("identity_col_map", {}),
        "per_mcode_rows":         _stock_load_debug.get("per_mcode_rows", False),
        "no_info_merges":         no_info_merges[:100],
        "no_info_merge_count":    len(no_info_merges),
        "total_merges":           len(seen_merges),
    }
    _cache.update({"path": path, "mtime": mtime, "rows": rows_out, "meta": meta})
    return rows_out, meta


# ── Aggregation ────────────────────────────────────────────────────

def _aggregate(rows):
    """Build the summary payload sent to the front-end."""
    STATUSES = ["shortage", "balanced", "surplus", "serious_surplus", "no_move"]

    def _empty_bucket():
        return {s: 0 for s in STATUSES + ["empty"]}

    kpi_status = _empty_bucket()
    total_stock = 0.0
    total_3m    = 0.0
    total_pipe  = 0.0
    state_totals = {s: {"stock": 0.0, "pipeline": 0.0, "demand_3m": 0.0,
                        "shortage": 0, "surplus": 0, "serious_surplus": 0,
                        "balanced": 0, "no_move": 0} for s in STATES}
    by_group    = {}
    by_inch     = {}
    by_brand    = {}
    by_classif  = {}
    by_line     = {}    # marketing line: Kinergy / Dynapro / Ventus / …
    by_pclt     = {}    # marketing line within PCLT only  → main chart 1
    by_tbr      = {}    # marketing line within TBR only   → main chart 2

    shortage_rows, balanced_rows, surplus_rows, serious_rows, no_move_rows = [], [], [], [], []
    all_rows_flat = []   # every non-empty row, for the drill-down index

    # Dedupe stock/demand accumulation for rows that share their
    # merge total (per-merge workbook layout — every sibling M CODE
    # carries the same numbers so summing them would multi-count).
    _shared_stock_seen = set()

    for r in rows:
        st = r["status"]
        kpi_status[st] += 1
        if st == "empty":
            continue

        # Skip stock-total accumulation for shared rows we've already
        # counted at the merge level.  Status counts still increment
        # per row so the tab labels match what the table shows.
        skip_stock = False
        if r.get("merge_shared"):
            if r["merge_code"] in _shared_stock_seen:
                skip_stock = True
            else:
                _shared_stock_seen.add(r["merge_code"])

        if not skip_stock:
            total_stock += r["total_stock"]
            total_3m    += r["total_3m"]
            total_pipe  += sum(r["state_pipeline"].values())

            for s in STATES:
                state_totals[s]["stock"]     += r["state_stock"][s]
                state_totals[s]["pipeline"]  += r["state_pipeline"][s]
                state_totals[s]["demand_3m"] += r["state_3m"][s]

        for s in STATES:
            # Per-state status: local MOH
            sd = r["state_3m"][s]
            ss = r["state_stock"][s]
            # State counts credit the SKU's MERGE-CODE status wherever
            # the SKU has activity — either non-zero stock or non-zero
            # 3M demand in that state.  Skip on shared duplicates so a
            # merge's status only counts once per state.
            if not skip_stock and (sd > 0 or ss > 0) and r["status"] in state_totals[s]:
                state_totals[s][r["status"]] += 1

        # Group breakdowns
        g = r["group"] or "—"
        by_group.setdefault(g, _empty_bucket())[st] += 1
        if not skip_stock:
            by_group[g].setdefault("stock", 0)
            by_group[g]["stock"] += r["total_stock"]
            by_group[g].setdefault("demand_3m", 0)
            by_group[g]["demand_3m"] += r["total_3m"]

        inch = str(r["inch"] or "—")
        by_inch.setdefault(inch, _empty_bucket())[st] += 1
        if not skip_stock:
            by_inch[inch].setdefault("stock", 0); by_inch[inch]["stock"] += r["total_stock"]

        br = r["brand"] or "—"
        by_brand.setdefault(br, _empty_bucket())[st] += 1
        if not skip_stock:
            by_brand[br].setdefault("stock", 0); by_brand[br]["stock"] += r["total_stock"]

        cl = r["classification"] or "—"
        by_classif.setdefault(cl, _empty_bucket())[st] += 1
        if not skip_stock:
            by_classif[cl].setdefault("stock", 0); by_classif[cl]["stock"] += r["total_stock"]

        ln = r["line"] or "Other"
        by_line.setdefault(ln, _empty_bucket())[st] += 1
        if not skip_stock:
            by_line[ln].setdefault("stock", 0); by_line[ln]["stock"] += r["total_stock"]

        # Chart split — PCLT and TBR each get their own by-line
        # breakdown so the two hero charts show the axis the user
        # actually reads (never mix passenger and truck lines on
        # the same bar).
        if r["category"] == "PCLT":
            by_pclt.setdefault(ln, _empty_bucket())[st] += 1
            if not skip_stock:
                by_pclt[ln].setdefault("stock", 0); by_pclt[ln]["stock"] += r["total_stock"]
        elif r["category"] == "TBR":
            by_tbr.setdefault(ln, _empty_bucket())[st] += 1
            if not skip_stock:
                by_tbr[ln].setdefault("stock", 0); by_tbr[ln]["stock"] += r["total_stock"]

        # SKU entry (unified — every table + the drill-down index uses
        # the same shape so the front-end can look up a full row from
        # its Merge Code without a second lookup structure).
        entry = {
            "merge_code":     r["merge_code"],
            "m_code":         r["m_code"],
            "merge_shared":   r.get("merge_shared", False),
            "description":    r["description"] or f"MC {r['merge_code']}",
            "group":          r["group"],
            "line":           r["line"],
            "product_name":   r.get("product_name", ""),
            "pattern":        r["pattern"],
            "brand":          r["brand"],
            "size":           r["size"],
            "inch":           str(r["inch"]) if r["inch"] not in (None, "") else "",
            "sr":             str(r["sr"])   if r["sr"]   not in (None, "") else "",
            "li":             str(r["li"])   if r["li"]   not in (None, "") else "",
            "ss":             str(r["ss"])   if r["ss"]   not in (None, "") else "",
            "sku_status":     r.get("sku_status", "Active"),
            "category":       r["category"],
            "is_18plus":      r["is_18plus"],
            "is_low_profile": r["is_low_profile"],
            "is_suv":         r["is_suv"],
            "total_stock":    r["total_stock"],
            "total_all":      r["total_all"],
            "total_3m":       round(r["total_3m"], 2),
            "p_3m":           r["p_3m"],
            "avg_6m_old":     r["avg_6m_old"],
            "avg_7_9m":       r["avg_7_9m"],
            "avg_10_12m":     r["avg_10_12m"],
            "total_12m":      r["total_12m"],
            "max_demand":     r["max_demand"],
            "moh":            r["moh"],
            "moh_plus":       r["moh_plus"],
            "moh_plus_max":   r["moh_plus_max"],
            "status":         r["status"],
            "state_stock":    r["state_stock"],
            "state_3m":       r["state_3m"],
            "state_pipeline": r["state_pipeline"],
            "state_pipe_parts": r["state_pipe_parts"],
            "history":        r["history"],
        }
        all_rows_flat.append(entry)
        if st == "shortage":
            shortage_rows.append(entry)
        elif st == "balanced":
            balanced_rows.append(entry)
        elif st == "surplus":
            surplus_rows.append(entry)
        elif st == "serious_surplus":
            serious_rows.append(entry)
        elif st == "no_move":
            no_move_rows.append(entry)

    # Sort tables — shortage by MOI ascending (most urgent first)
    shortage_rows.sort(key=lambda e: (e["moh"] if e["moh"] is not None else 0, -e["total_3m"]))
    # Balance by 3M demand descending — biggest movers first
    balanced_rows.sort(key=lambda e: -e["total_3m"])
    # Surplus / Serious Surplus by MOI descending (biggest overstock first)
    surplus_rows.sort(key=lambda e: -(e["moh"] if e["moh"] is not None else 0))
    serious_rows.sort(key=lambda e: -(e["moh"] if e["moh"] is not None else 0))
    # No-move by stock size descending
    no_move_rows.sort(key=lambda e: -e["total_stock"])

    # National MOH weighted-average = total stock / total 3M demand
    nat_moh = round(total_stock / total_3m, 2) if total_3m > 0 else None
    for s in STATES:
        d = state_totals[s]
        d["moh"] = round(d["stock"] / d["demand_3m"], 2) if d["demand_3m"] > 0 else None

    return {
        "kpi": {
            "sku_total":      sum(kpi_status[k] for k in ["shortage","balanced","surplus","serious_surplus","no_move"]),
            "sku_shortage":        kpi_status["shortage"],
            "sku_balanced":        kpi_status["balanced"],
            "sku_surplus":         kpi_status["surplus"],
            "sku_serious_surplus": kpi_status["serious_surplus"],
            "sku_no_move":         kpi_status["no_move"],
            "total_stock":         int(total_stock),
            "total_pipeline":      int(total_pipe),
            "total_demand_3m":     int(round(total_3m)),
            "national_moh":        nat_moh,
        },
        "thresholds": {
            "shortage": STATUS_SHORTAGE_MOI,
            "balance":  STATUS_BALANCE_MOI,
            "surplus":  STATUS_SURPLUS_MOI,
        },
        "state":         state_totals,
        "by_group":      by_group,
        "by_line":       by_line,
        "by_pclt":       by_pclt,
        "by_tbr":        by_tbr,
        "by_inch":       by_inch,
        "by_brand":      by_brand,
        "by_classif":    by_classif,
        "shortage_rows":        shortage_rows,
        "balanced_rows":        balanced_rows,
        "surplus_rows":         surplus_rows,
        "serious_surplus_rows": serious_rows,
        "no_move_rows":         no_move_rows,
        "all_rows":             all_rows_flat,
    }


# ── Flask route ────────────────────────────────────────────────────

def stock_dashboard():
    rows, meta = load_stock_data()
    if meta.get("error"):
        return (f"<pre style='padding:24px;font-family:sans-serif;color:#c00'>"
                f"{meta['error']}</pre>")
    payload = _aggregate(rows)
    return render_template_string(
        _HTML,
        data_json = json.dumps(payload, default=str),
        meta      = meta,
    )


# ── HTML template ──────────────────────────────────────────────────

_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stock Balance Lab</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
<!-- SheetJS Community Edition — CSV export writes .xlsx directly. -->
<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap">
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
:root {
    --ground:#F4F6F9; --card:#FFFFFF; --border:#E1E5EB;
    --ink:#263238; --muted:#607D8B;
    --hdr1:#0E3F5F; --hdr2:#1F4E79;
    /* Pure lemon-yellow so Shortage stays visually far from Surplus
       (dark orange).  Text uses a deep mustard for readability on
       white; chip background is a pale lemon; chart bars use the
       vivid mid-yellow. */
    --short:#854D0E;   --short-fg:#FEF3C7;
    --short-line:#FACC15;
    --bal:#2E7D32;     --bal-fg:#E8F5E9;
    --sur:#EF6C00;     --sur-fg:#FFF3E0;
    --ser:#B71C1C;     --ser-fg:#FDE0E0;
    --nom:#6A1B9A;     --nom-fg:#F3E5F5;
    --hover:#F5F7FA;
}
body { font-family:'IBM Plex Sans','Segoe UI',system-ui,sans-serif;
       background:var(--ground); color:var(--ink); height:100vh;
       display:flex; flex-direction:column; overflow:hidden;
       font-variant-numeric:tabular-nums; }

/* ── Header ── */
.hdr { background:linear-gradient(135deg,var(--hdr1),var(--hdr2));
       color:#fff; padding:12px 22px; display:flex; align-items:center;
       gap:14px; flex-shrink:0; box-shadow:0 2px 8px rgba(0,0,0,0.12); }
.hdr h1 { font-size:18px; letter-spacing:.3px; font-weight:600; }
.hdr .subtitle { font-size:11px; opacity:.75; line-height:1.4; }
.hdr .nav { margin-left:auto; display:flex; gap:6px; align-items:center; }
.hdr .nav a { font-size:12px; padding:5px 13px; border-radius:5px; cursor:pointer;
              border:1px solid rgba(255,255,255,0.45); color:#fff; text-decoration:none; }
.hdr .nav a:hover { background:rgba(255,255,255,0.15); }
.hdr .nav a.active { background:#fff; color:var(--hdr1); font-weight:600; }

/* ── Icon button (email / expand / % toggle) ── */
.icon-btn { background:rgba(255,255,255,0.12); border:1px solid rgba(255,255,255,0.4);
            color:#fff; padding:4px 9px; border-radius:5px; cursor:pointer;
            font:600 12px/1 'IBM Plex Sans',system-ui,sans-serif;
            display:inline-flex; align-items:center; gap:4px; }
.icon-btn:hover { background:rgba(255,255,255,0.24); }
.card .icon-btn { background:#EEF2F7; color:var(--hdr1); border:1px solid #CFD8DC; }
.card .icon-btn:hover { background:#DDE4EE; }
.card .icon-btn.active { background:var(--hdr1); color:#fff; border-color:var(--hdr1); }

/* ── Top persistent filter bar (multi-select checkbox dropdowns) ── */
.topfilters { background:#263238; color:#ECEFF1; padding:8px 16px;
              display:flex; align-items:center; gap:8px; flex-wrap:wrap;
              flex-shrink:0; border-bottom:1px solid #1a2225; }
.topfilters .lbl { font-size:10.5px; text-transform:uppercase;
                   letter-spacing:.08em; color:#90A4AE; font-weight:600;
                   margin-right:4px; }
.ms { position:relative; display:inline-block; }
.ms-btn { background:#37474F; border:1px solid #455A64; color:#ECEFF1;
          font:500 12px 'IBM Plex Sans',system-ui,sans-serif;
          padding:5px 24px 5px 11px; border-radius:4px; cursor:pointer;
          min-width:100px; text-align:left; position:relative; }
.ms-btn::after { content:'▾'; position:absolute; right:8px; top:50%;
                 transform:translateY(-50%); font-size:9px; opacity:.7; }
.ms-btn:hover { background:#455A64; }
.ms-btn.active { background:var(--hdr2); border-color:var(--hdr2); color:#fff; }
.ms-btn .n { background:rgba(255,255,255,0.22); border-radius:8px;
             padding:0 5px; margin-left:4px; font-size:10px; font-weight:600; }
.ms-panel { display:none; position:absolute; top:100%; left:0; z-index:60;
            background:#fff; color:var(--ink); border:1px solid #B0BEC5;
            border-radius:5px; margin-top:3px; min-width:200px;
            max-height:360px; overflow-y:auto;
            box-shadow:0 6px 20px rgba(0,0,0,0.2); padding:4px 0; }
.ms.open .ms-panel { display:block; }
.ms-panel label { display:flex; align-items:center; gap:7px;
                  padding:5px 12px; font-size:12px; cursor:pointer;
                  user-select:none; }
.ms-panel label:hover { background:var(--hover); }
.ms-panel input[type=checkbox] { margin:0; }
.ms-panel .ms-actions { display:flex; justify-content:space-between;
                        padding:6px 12px 8px; border-top:1px solid #ECEFF1;
                        margin-top:4px; }
.ms-panel .ms-actions button { font-size:11px; padding:3px 10px; border-radius:3px;
                               border:1px solid #CFD8DC; background:#fff;
                               cursor:pointer; color:var(--hdr1); }
.ms-panel .ms-actions button:hover { background:var(--hover); }
.topfilters .search { margin-left:auto; }
.topfilters .search input { background:#37474F; border:1px solid #455A64;
                             color:#ECEFF1; font:500 12px 'IBM Plex Sans',sans-serif;
                             padding:5px 11px; border-radius:4px; width:240px; }
.topfilters .search input::placeholder { color:#78909C; }
.topfilters .reset { background:none; border:1px solid #607D8B;
                     color:#B0BEC5; padding:4px 11px; border-radius:4px;
                     cursor:pointer; font:500 11px 'IBM Plex Sans',sans-serif; }
.topfilters .reset:hover { background:#37474F; color:#fff; }

/* ── KPI strip ── */
.kpi-strip { display:grid; grid-template-columns:repeat(7,1fr);
             gap:10px; padding:10px 16px 4px; }
.kpi { background:var(--card); border-radius:8px; padding:10px 14px;
       border:1px solid var(--border); position:relative; overflow:hidden; }
.kpi h4 { font-size:10px; color:var(--muted); text-transform:uppercase;
          letter-spacing:.1em; margin-bottom:4px; font-weight:600; }
.kpi .v { font-size:22px; font-weight:700; color:var(--hdr1);
          font-family:'IBM Plex Mono',ui-monospace,monospace;
          font-variant-numeric:tabular-nums; }
.kpi .u { font-size:10.5px; color:#78909C; margin-left:4px; }
.kpi.short { border-left:4px solid var(--short); }
.kpi.short  .v { color:var(--short); }
.kpi.bal   { border-left:4px solid var(--bal); }
.kpi.bal    .v { color:var(--bal); }
.kpi.sur   { border-left:4px solid var(--sur); }
.kpi.sur    .v { color:var(--sur); }
.kpi.ser   { border-left:4px solid var(--ser); }
.kpi.ser    .v { color:var(--ser); }
.kpi.nom   { border-left:4px solid var(--nom); }
.kpi.nom    .v { color:var(--nom); }

/* ── Threshold footnote strip ── */
.thresh-note { font-size:11px; color:var(--muted);
               padding:4px 18px 6px; line-height:1.6; }
.thresh-note b { color:var(--ink); font-weight:600; }
.thresh-note .sw { display:inline-block; width:10px; height:10px;
                   border-radius:2px; vertical-align:-1px; margin-right:4px; }
.thresh-note .sw.short { background:var(--short); }
.thresh-note .sw.bal   { background:var(--bal); }
.thresh-note .sw.sur   { background:var(--sur); }
.thresh-note .sw.ser   { background:var(--ser); }
.thresh-note .sw.nom   { background:var(--nom); }

/* ── main body grid ── */
.wrap { flex:1; overflow-y:auto; padding:6px 16px 20px; }
.grid { display:grid; grid-template-columns:320px 1fr; gap:12px; }

.state-col { display:flex; flex-direction:column; gap:10px; }
.state-card { background:var(--card); border-radius:8px; border:1px solid var(--border);
    padding:12px 14px; cursor:pointer;
    transition:box-shadow .12s, border-color .12s; }
.state-card:hover { border-color:#94A3B8; box-shadow:0 2px 6px rgba(15,23,42,0.08); }
.state-card-active { border-color:#1976D2; box-shadow:0 0 0 2px #DBEAFE inset; }
.state-card-active h3 { color:#0E3F5F; }
.state-card h3 { font-size:13px; display:flex; justify-content:space-between;
                 align-items:baseline; margin-bottom:6px; font-weight:600; }
.state-card h3 .m { font-family:'IBM Plex Mono',monospace;
                    color:#546E7A; font-size:11px; font-variant-numeric:tabular-nums; }
.state-row { display:flex; justify-content:space-between; align-items:baseline;
             font-size:11.5px; padding:3px 0; border-top:1px solid #F1F3F5; }
.state-row:first-of-type { border-top:none; }
.state-row .lbl { color:var(--muted); }
.state-row .v   { font-family:'IBM Plex Mono',monospace; font-weight:600;
                  color:var(--ink); font-variant-numeric:tabular-nums; }
.chip { display:inline-block; padding:1px 7px; border-radius:8px;
        font-size:10px; font-weight:600; margin-left:4px;
        font-family:'IBM Plex Mono',monospace; }
.chip.short { background:var(--short-fg); color:var(--short); }
.chip.sur   { background:var(--sur-fg);   color:var(--sur); }
.chip.ser   { background:var(--ser-fg);   color:var(--ser); }
.chip.bal   { background:var(--bal-fg);   color:var(--bal); }
.chip.nom   { background:var(--nom-fg);   color:var(--nom); }

.right { display:flex; flex-direction:column; gap:12px; min-width:0; }
.card { background:var(--card); border-radius:8px; border:1px solid var(--border);
        padding:12px 14px 14px; position:relative; }
.card h3 { font-size:12.5px; color:var(--hdr1);
           border-bottom:1px solid #ECEFF1; padding-bottom:6px; margin-bottom:8px;
           display:flex; justify-content:space-between; align-items:baseline;
           font-weight:600; }
.card h3 .hint { font-size:10.5px; color:#78909C; font-weight:400; margin-left:auto; }
.card h3 .icons { display:flex; gap:4px; margin-left:8px; }
.chartbox { position:relative; height:230px; }
.chartbox.tall { height:260px; }

.tabs { display:flex; gap:4px; margin-bottom:6px; }
.tab {  font-size:11.5px; padding:5px 12px; border-radius:5px; cursor:pointer;
        background:#ECEFF1; color:#37474F; border:1px solid #CFD8DC; }
.tab.active { background:var(--hdr1); color:#fff; border-color:var(--hdr1); }
.tab:hover:not(.active) { background:#CFD8DC; }
.tab .n { display:inline-block; margin-left:6px; padding:0 6px;
          border-radius:8px; background:rgba(0,0,0,0.08); font-size:10px;
          font-family:'IBM Plex Mono',monospace; }
.tab.active .n { background:rgba(255,255,255,0.24); }

/* ── SKU table ──
   border-collapse: separate + border-spacing 0 keeps the visual of a
   collapsed table but lets `position: sticky` work on <th>/<td>
   in every browser (with `collapse`, Chromium drops the sticky
   layer under the pipeline-detail two-row header, so the state
   banner would slide out of view when the user scrolled — that's
   what the user was hitting). */
table.dt { width:100%; border-collapse:separate; border-spacing:0;
           font-size:11.5px; table-layout:fixed; }
table.dt thead th { background:#ECEFF1; color:#37474F; padding:6px 8px;
                    text-align:left; position:sticky; top:0; z-index:2;
                    border-bottom:1px solid #CFD8DC; font-size:10.5px;
                    text-transform:uppercase; letter-spacing:.04em;
                    cursor:pointer; user-select:none; white-space:nowrap; }
/* Numeric column headers (state stock, MOI, MOI(PPL), 3M/4-6M/7-9M/
   10-12M, pipeline STK/PRT/WTR/FAC) center their title so the label
   reads over the middle of the data below.  Body cells still use
   right alignment for the numbers via `.r` on <td>. */
table.dt thead th.r { text-align:center; }
table.dt thead th:hover { background:#DDE4EE; }
/* Column-resize drag handle on the right edge of every header
   cell.  Grab and drag to widen a column when content clips. */
table.dt thead th { position:sticky; position:-webkit-sticky; }
.col-resize-handle {
    position:absolute; top:0; right:0; bottom:0; width:6px;
    cursor:col-resize; z-index:8; user-select:none;
}
.col-resize-handle:hover { background:#94A3B8; opacity:.5; }
table.dt thead th { padding-right:12px; }   /* room for the handle */
/* Pipeline-detail mode has a two-row header:
     Row 1 = state banner (NSW/QLD/VIC/WA/TOTAL) — sticks at top:0
     Row 2 = sub-column labels (Stock/Port/Water/Factory + MOI…)
             sticks at top: <banner-height> so the banner stays
             visible above the labels during vertical scroll.
   The `pipe-mode` class on <thead> switches this on. */
table.dt thead.pipe-mode tr.state-band th {
    top:0; z-index:3; padding:4px 6px; font-size:11px;
    border-bottom:2px solid #0F172A; cursor:default;
}
table.dt thead.pipe-mode tr.state-band th:hover { filter:brightness(1.05); }
table.dt thead.pipe-mode tr.col-labels th { top:26px; }

/* ── Frozen identity columns (Merge → LI/SS) ──
   The first 10 cells of every row (Merge Code, M CODE, Brand,
   Marketing Line, Product Name, Pattern, F/O·OPE, Size, Inch,
   LI/SS) stick to the left of the scroll container so they stay
   visible when the user pans the state / MOI / period-avg block
   horizontally.  Each column has a fixed width and a pre-computed
   `left` offset (accumulated width of preceding frozen columns).
   `nth-child(N)` matches by ordinal position which is stable
   regardless of pipeline mode. */
/* Identity column widths — packed tight per user request so the
   freeze block reserves as little horizontal room as possible.
   Sum ≈ 630px versus the old 830px, freeing 200px for the
   elastic data columns to the right. */
table.dt th:nth-child(1),  table.dt td:nth-child(1)  { min-width:46px;  width:46px;  }
table.dt th:nth-child(2),  table.dt td:nth-child(2)  { min-width:68px;  width:68px;  }
table.dt th:nth-child(3),  table.dt td:nth-child(3)  { min-width:36px;  width:36px;  }
table.dt th:nth-child(4),  table.dt td:nth-child(4)  { min-width:92px;  width:92px;  }
table.dt th:nth-child(5),  table.dt td:nth-child(5)  { min-width:92px;  width:92px;  }
table.dt th:nth-child(6),  table.dt td:nth-child(6)  { min-width:52px;  width:52px;  }
table.dt th:nth-child(7),  table.dt td:nth-child(7)  { min-width:68px;  width:68px;  }
table.dt th:nth-child(8),  table.dt td:nth-child(8)  { min-width:80px;  width:80px;  }
table.dt th:nth-child(9),  table.dt td:nth-child(9)  { min-width:40px;  width:40px;  }
table.dt th:nth-child(10), table.dt td:nth-child(10) { min-width:56px;  width:56px;  }

/* ── Data columns (nth-child 11+) — elastic uniform width ──
   User asked that numeric columns share the remaining viewport
   evenly instead of one column ("10-12M AVG") ballooning while
   its neighbours stay tight.  `table-layout: fixed` on `<table>`
   plus a `<colgroup>` injected by the JS forces every data
   column to the same width, computed as
     (viewport − identity block width) ÷ (# data columns).
   Cells beyond that width just clip or wrap — which is fine for
   the small integers the numeric columns carry.  Identity columns
   still get their fixed widths above via the same `<colgroup>`. */
table.dt.pipe-mode th:nth-child(n+11),
table.dt.pipe-mode td:nth-child(n+11) {
    min-width:52px; padding:5px 4px;
    overflow:hidden; text-overflow:ellipsis;
}
table.dt:not(.pipe-mode) th:nth-child(n+11),
table.dt:not(.pipe-mode) td:nth-child(n+11) {
    min-width:64px; padding:5px 6px;
    overflow:hidden; text-overflow:ellipsis;
}

/* z-index ordering:
     • state-band th          → z-index: 3  (vertical sticky only)
     • col-labels th          → z-index: 2  (vertical sticky only)
     • Frozen tbody cells     → z-index: 5  (occlude state banner
                                             sliding into the freeze
                                             area during horizontal
                                             pan)
     • Frozen thead cells     → z-index: 7  (over frozen tbody)
     • Frozen total-row cells → z-index: 6  (between the above two) */
table.dt tbody td:nth-child(-n+10) { position:sticky; background:#fff; z-index:5; }
table.dt thead th:nth-child(-n+10) { position:sticky; z-index:7; }
table.dt tbody tr:hover td:nth-child(-n+10) { background:var(--hover); }
table.dt tbody tr.selected td:nth-child(-n+10) { background:#DBEAFE; }
table.dt tbody tr.selected:hover td:nth-child(-n+10) { background:#BFDBFE; }
table.dt tbody tr.sub-total td:nth-child(-n+10) { background:#FFF8E1; }
table.dt tbody tr.total-row td:nth-child(-n+10) { background:#EEF3F8; z-index:6; }
table.dt tbody tr.merge-cohover td:nth-child(-n+10) { background:#EEF2FF; }
/* Rows that use colspan (state-band + Sub Total + Total-in-View)
   don't line their nth-child positions up with data columns.
   Their FIRST cell is one wide label spanning the 10 frozen
   identity columns; nth-child(2) onwards are numeric cells that
   belong at data columns 11+ and must SCROLL horizontally.
   Without this override, those numeric cells inherit the freeze
   offsets (left:56px, 136px, 188px, …) from the generic nth-child
   rules and lock into the frozen area — the user reported this as
   Sub Total numbers "밀려서" landing under Size / Inch / LI/SS
   instead of NSW / QLD / VIC.  We cancel left/right offsets on
   those cells and push their z-index below the frozen block so
   they disappear behind it as they scroll in.
   The FIRST cell of each row gets z-index high enough to cover
   the scrolling siblings while it stays sticky at left:0. */
table.dt thead.pipe-mode tr.state-band th:first-child {
    left: 0 !important; z-index: 7 !important;
}
table.dt thead.pipe-mode tr.state-band th:nth-child(n+2) {
    left: auto !important; right: auto !important; z-index: 3 !important;
}
table.dt tbody tr.sub-total td:first-child,
table.dt tbody tr.total-row td:first-child {
    left: 0 !important; z-index: 6 !important;
}
table.dt tbody tr.sub-total td:nth-child(n+2),
table.dt tbody tr.total-row td:nth-child(n+2) {
    left: auto !important; right: auto !important; z-index: 1 !important;
}
/* Cumulative left offsets — running sum of the widths above.
   Total = 46+68+36+92+92+52+68+80+40+56 = 630 px */
table.dt th:nth-child(1),  table.dt td:nth-child(1)  { left:0; }
table.dt th:nth-child(2),  table.dt td:nth-child(2)  { left:46px; }
table.dt th:nth-child(3),  table.dt td:nth-child(3)  { left:114px; }
table.dt th:nth-child(4),  table.dt td:nth-child(4)  { left:150px; }
table.dt th:nth-child(5),  table.dt td:nth-child(5)  { left:242px; }
table.dt th:nth-child(6),  table.dt td:nth-child(6)  { left:334px; }
table.dt th:nth-child(7),  table.dt td:nth-child(7)  { left:386px; }
table.dt th:nth-child(8),  table.dt td:nth-child(8)  { left:454px; }
table.dt th:nth-child(9),  table.dt td:nth-child(9)  { left:534px; }
table.dt th:nth-child(10), table.dt td:nth-child(10) { left:574px; }
/* Right edge marker on the last frozen column — thin (1px) per
   user request so it doesn't dominate visually. */
table.dt th:nth-child(10), table.dt td:nth-child(10) { border-right:1px solid #CBD5E1; }

table.dt thead th .sort { display:inline-block; margin-left:3px; opacity:.35;
                          font-size:9px; }
table.dt thead th.sort-asc  .sort::after { content:'▲'; opacity:1; }
table.dt thead th.sort-desc .sort::after { content:'▼'; opacity:1; }
table.dt tbody td { padding:5px 8px; border-bottom:1px solid #F1F3F5;
                    white-space:nowrap; font-size:11.5px; }
table.dt tbody tr { cursor:pointer; }
/* Zebra stripes on even rows (soft grey) so the wide table stays
   scannable across many columns. */
/* Zebra stripes removed per user request — every row keeps the
   default white background so gold Sub Total, indigo cohover, and
   the M CODE freeze band stay the only horizontal cues. */
/* table.dt tbody tr:nth-child(even) td { background:#F5F7FA; } */
table.dt tbody tr:hover td { background:var(--hover); }
table.dt tbody tr.selected td { background:#DBEAFE; }
table.dt tbody tr.selected:hover td { background:#BFDBFE; }
/* Merge-group cohover: when the mouse is over any row of a Merge,
   every other row of the same Merge (its M CODE siblings + Sub
   Total) gets a subtle indigo tint so the reader can trace the
   group across the very wide table without scrolling around. */
table.dt tbody tr.merge-cohover td { background:#EEF2FF !important; }
table.dt tbody tr.merge-cohover.selected td { background:#C7D2FE !important; }
table.dt tbody tr.merge-cohover.sub-total td { background:#E0E7FF !important; }
/* Merge-code separator: first row of each new Merge Code carries
   `merge-break` and draws a heavy top border so the merge groups
   read cleanly. */
table.dt tbody tr.merge-break td { border-top:2px solid #37474F; }
/* Sub Total row per merge — bold, quiet gold ground, tighter
   border above so it visually clings to its M CODE siblings. */
table.dt tbody tr.sub-total td { background:#FFF8E1 !important;
    font-weight:700; border-top:1px dashed #94A3B8;
    border-bottom:2px solid #94A3B8; }
table.dt tbody tr.sub-total:hover td { background:#FFECB3 !important; }
/* M CODE (non-total) rows keep a subtle band on the leftmost cell
   so the reader's eye traces the merge group top-to-bottom. */
table.dt tbody tr.mc-row td:first-child { font-family:'IBM Plex Mono',monospace;
    font-weight:600; color:#37474F; }
/* Numeric cells on an M CODE (non-total) row render in a distinct
   blue so the reader instantly sees they are the merge's roll-up
   value applied to this M CODE, not this SKU's own sold-per-unit
   history.  Sub Total rows keep the default black. */
table.dt tbody tr.mc-row td.r { color:#1D4ED8; }
table.dt tbody tr.mc-row td.r.short { color:var(--short) !important; }
table.dt tbody tr.mc-row td.r.sur   { color:var(--sur)   !important; }
table.dt tbody tr.mc-row td.r.ser   { color:var(--ser)   !important; }
/* When a merge genuinely has no product info in Sheet2, the M CODE
   row shows a soft banner in the (very wide) description cell so
   users can tell missing text from missing numbers. */
table.dt tbody tr.mc-row td.no-info {
    background:#FEF3C7 !important; color:#92400E; font-style:italic;
    font-weight:600; font-size:10.5px; }
/* MOI-column magnifier button — opens the drill-down modal.  Row
   clicks handle selection instead, so the button carries its own
   affordance (subtle blue chip) to say "click me for detail". */
.moi-detail-btn {
    display:inline-block; margin-left:6px; padding:0 6px;
    background:#E0F2FE; color:#0369A1; border:1px solid #7DD3FC;
    border-radius:3px; font-size:11px; line-height:16px; cursor:pointer;
    vertical-align:middle;
}
.moi-detail-btn:hover { background:#BAE6FD; }
/* Sticky Total row at the top of the tbody, showing sums over the
   current filtered view.  Its top offset differs by header depth:
   compact mode -> 24px (below the single header row),
   pipeline mode -> 52px (below state banner + sub-column labels). */
table.dt tbody tr.total-row td { background:#EEF3F8 !important;
    font-weight:700; color:var(--hdr1); border-top:2px solid var(--hdr1);
    border-bottom:2px solid var(--hdr1); position:sticky; top:24px; z-index:1; }
table.dt.pipe-mode tbody tr.total-row td { top:52px; }
/* Darker/blue treatment for the (3M) demand parenthesis so the
   sales figure reads as a distinct piece of data next to stock. */
.dem-parens { color:#1976D2 !important; font-weight:600; font-size:10.5px; }
table.dt .r { text-align:right; font-family:'IBM Plex Mono',monospace;
              font-variant-numeric:tabular-nums; }
/* Vertical group dividers: draw a solid left border on the FIRST
   cell of each column group.  In the compact view the boundaries
   sit between state columns; in Pipeline-detail mode they sit
   between state groups + before the national TOTAL group. */
table.dt .grp-start { border-left:2px solid #90A4AE; }
table.dt thead th.grp-start { border-left:2px solid #90A4AE; }
/* Suppressed divider — kills any inherited border between the four
   period-avg columns so 4-6M / 7-9M / 10-12M read as one block. */
table.dt .no-div { border-left:none !important; border-right:none !important; }
table.dt thead th.no-div { border-left:none !important; border-right:none !important; }
table.dt .r.short { color:var(--short); font-weight:700; }
table.dt .r.sur   { color:var(--sur);   font-weight:700; }
table.dt .r.ser   { color:var(--ser);   font-weight:700; }
.tbl-wrap { max-height:480px; overflow-y:auto; overflow-x:auto; }

/* ── Active-filter summary strip ── */
.filter-text { padding:6px 10px; background:#EEF3F8; border-radius:5px;
               font-size:11.5px; color:var(--muted); margin-bottom:6px;
               border:1px solid #DBE4EE; display:flex; gap:6px; flex-wrap:wrap;
               align-items:center; line-height:1.5; }
.filter-text .lbl { color:var(--hdr1); font-weight:700; text-transform:uppercase;
                    letter-spacing:.06em; font-size:10.5px; }
.filter-text .chip-txt { display:inline-block; background:#fff; color:var(--ink);
                         padding:2px 8px; border-radius:12px; font-weight:600;
                         border:1px solid #CFD8DC; font-size:11px; }
.filter-text .chip-txt .k { color:var(--muted); font-weight:500; margin-right:4px; }
.filter-text em { color:var(--muted); font-style:italic; }

/* ── Selection summary strip ── */
.sel-summary { display:flex; gap:14px; align-items:center; padding:6px 4px;
               font-size:11.5px; color:var(--ink); flex-wrap:wrap;
               border-bottom:1px solid #ECEFF1; margin-bottom:6px; }
.sel-summary .lbl { color:var(--muted); font-weight:600; text-transform:uppercase;
                    letter-spacing:.06em; font-size:10px; }
.sel-summary .v { font-family:'IBM Plex Mono',monospace;
                  font-variant-numeric:tabular-nums; font-weight:600; }
.sel-summary .clear-sel { margin-left:auto; background:#FFF1E0; color:#B45309;
                          border:1px solid #FDBA74; padding:3px 10px;
                          border-radius:4px; cursor:pointer; font-size:11px; }
.sel-summary .clear-sel:hover { background:#FDE4B2; }
.sel-summary .none-selected { color:var(--muted); font-style:italic; }

.pill { display:inline-block; padding:2px 7px; border-radius:10px;
        font-size:10.5px; font-weight:600; }
.pill.g-SP  { background:#E3F2FD; color:#1565C0; }
.pill.g-HP  { background:#FFF3E0; color:#E65100; }
.pill.g-UHP { background:#FCE4EC; color:#AD1457; }
.pill.g-TBR { background:#E8F5E9; color:#2E7D32; }
.pill.g-LS  { background:#EDE7F6; color:#4527A0; }
.pill.g-LV  { background:#E0F2F1; color:#00695C; }
.pill.g-RUNFLAT { background:#FCE4EC; color:#880E4F; }
.pill.g-RACING  { background:#FBE9E7; color:#BF360C; }
.pill.g-other { background:#ECEFF1; color:#455A64; }
.pill.sm { font-size:9.5px; padding:1px 6px; }

.legend { display:flex; gap:12px; font-size:11px; margin-top:6px; }
.legend .dot { display:inline-block; width:9px; height:9px;
               border-radius:50%; margin-right:4px; vertical-align:-1px; }
.legend .dot.short { background:var(--short); }
.legend .dot.bal   { background:var(--bal); }
.legend .dot.sur   { background:var(--sur); }
.legend .dot.ser   { background:var(--ser); }
.legend .dot.nom   { background:var(--nom); }

/* ── Fullscreen expansion of the SKU table ── */
body.expand-table .expand-target { position:fixed !important; top:0; left:0;
    right:0; bottom:0; width:100vw !important; height:100vh !important;
    z-index:9998; margin:0 !important; border-radius:0; overflow:auto;
    box-shadow:0 0 0 9999px rgba(0,0,0,0.4); }
body.expand-table .expand-target .tbl-wrap { max-height:calc(100vh - 160px); }

/* ── Modal ── */
.modal-bg { display:none; position:fixed; inset:0; z-index:200;
            background:rgba(15,25,35,0.55); }
.modal-bg.open { display:flex; align-items:center; justify-content:center; }
.modal { background:var(--card); width:min(1080px,96vw); max-height:92vh;
         border-radius:10px; overflow:hidden; display:flex;
         flex-direction:column; box-shadow:0 20px 60px rgba(0,0,0,0.35); }
.modal-hdr { background:linear-gradient(135deg,var(--hdr1),var(--hdr2));
             color:#fff; padding:14px 20px; display:flex; align-items:center;
             gap:12px; }
.modal-hdr .mtitle { font-size:15px; font-weight:600; }
.modal-hdr .msub { font-size:11.5px; opacity:.75; margin-top:2px; }
.modal-hdr .actions { margin-left:auto; display:flex; gap:6px; }
.modal-body { padding:18px 20px 22px; overflow-y:auto; display:grid;
              grid-template-columns:320px 1fr; gap:18px; }
.modal-body .stat { font-size:11px; color:var(--muted);
                    text-transform:uppercase; letter-spacing:.06em;
                    margin-bottom:2px; font-weight:600; }
.modal-body .figv { font-size:20px; font-weight:700;
                    font-family:'IBM Plex Mono',monospace;
                    color:var(--hdr1); font-variant-numeric:tabular-nums;
                    margin-bottom:10px; }
.modal-body .figv .u { font-size:11px; color:var(--muted);
                       font-family:'IBM Plex Sans',sans-serif; margin-left:3px; }
.modal-body .figv.short { color:var(--short); }
.modal-body .figv.sur   { color:var(--sur); }
.modal-body .figv.ser   { color:var(--ser); }
.modal-body .figv.bal   { color:var(--bal); }

.pipe-tbl { width:100%; border-collapse:collapse; font-size:11.5px; margin-top:6px; }
.pipe-tbl th { text-align:left; color:var(--muted); padding:5px 6px;
               font-size:10px; text-transform:uppercase; letter-spacing:.06em; }
.pipe-tbl td { padding:5px 6px; border-top:1px solid #ECEFF1;
               font-family:'IBM Plex Mono',monospace;
               font-variant-numeric:tabular-nums; text-align:right; }
.pipe-tbl td.st { text-align:left; font-family:'IBM Plex Sans',sans-serif;
                  font-weight:600; color:var(--ink); }
.pipe-tbl tr.tot td { border-top:2px solid #CFD8DC; font-weight:700; }
.pipe-tbl .short { color:var(--short); }
.pipe-tbl .sur   { color:var(--sur); }
.pipe-tbl .ser   { color:var(--ser); }

/* Toast (temporary email-download confirmation) */
.toast { position:fixed; bottom:24px; right:24px; z-index:9999;
         background:#0F3B5C; color:#fff; padding:12px 16px; border-radius:6px;
         font:500 12.5px 'IBM Plex Sans',sans-serif; box-shadow:0 4px 16px rgba(0,0,0,0.3);
         opacity:0; transform:translateY(6px); transition:opacity .2s, transform .2s; }
.toast.show { opacity:1; transform:translateY(0); }
</style>
</head>
<body>

<div class="hdr">
  <h1>📦 Stock Balance Lab</h1>
  <span class="subtitle">Where we're short · where we're surplus · by state · by product<br>
    <b style="color:#FFD54F">Data as of {{ meta.data_date }}</b> &nbsp;·&nbsp; source: {{ meta.path }}
    &nbsp;·&nbsp; loaded {{ meta.mtime }} &nbsp;·&nbsp; {{ meta.rows }} rows in {{ meta.load_s }} s
    {% if meta.no_info_merge_count and meta.no_info_merge_count > 0 %}
      <br><span style="color:#FDE68A;font-size:11.5px" title="These merges have neither a Sheet2 record nor a Description on the Stock Status Worksheet — CS needs to enter their master data.">
        ⚠ {{ meta.no_info_merge_count }} of {{ meta.total_merges }} merges have no product info in the workbook
        {% if meta.no_info_merges %}(e.g. {{ meta.no_info_merges[:8]|join(', ') }}{% if meta.no_info_merges|length > 8 %}, …{% endif %}){% endif %}
      </span>
    {% endif %}
    {% if meta.sheet2_rows is defined %}
      <br><span style="color:#94A3B8;font-size:11px">Sheet2 master: {{ meta.sheet2_rows }} rows · columns detected: {{ meta.sheet2_columns_found|join(', ') }}</span>
    {% endif %}
    {% if meta.stock_columns_found is defined %}
      <br><span style="color:#94A3B8;font-size:11px">Stock sheet columns detected: {{ meta.stock_columns_found|join(', ') }}{% if meta.per_mcode_rows %} · per-M-CODE layout{% endif %}{% if meta.stock_header_row %} · header row {{ meta.stock_header_row }}{% endif %}</span>
    {% endif %}
    {% if meta.stock_header_cells is defined and meta.stock_header_cells %}
      <br><span style="color:#94A3B8;font-size:11px" title="Raw header cells the loader read for columns 1-12">Header cells: {% for h in meta.stock_header_cells %}{% if loop.index0 > 0 %} · {% endif %}<code style="background:#F1F5F9;padding:0 4px;border-radius:3px">{{ loop.index }}={{ h or '∅' }}</code>{% endfor %}</span>
    {% endif %}
    </span>
  <nav class="nav">
    <button class="icon-btn" onclick="location.reload()"
            title="Reload the page — the server picks up the newest Stock_report file automatically">🔄 Refresh</button>
    <button class="icon-btn" onclick="emailScreen('page','Full dashboard')"
            title="Capture the whole screen and start an Outlook mail">✉ Email screen</button>
    <a href="/">Dashboard</a>
    <a href="/stock_balance" class="active">Stock Balance</a>
  </nav>
</div>

<!-- Top persistent filter bar (multi-select checkbox dropdowns) -->
<div class="topfilters" id="topfilters">
  <span class="lbl">Filter</span>
  <div class="ms" data-key="status"><button class="ms-btn">Status</button><div class="ms-panel"></div></div>
  <div class="ms" data-key="state"><button class="ms-btn">State</button><div class="ms-panel"></div></div>
  <div class="ms" data-key="brand"><button class="ms-btn">Brand</button><div class="ms-panel"></div></div>
  <div class="ms" data-key="product"><button class="ms-btn">Product</button><div class="ms-panel"></div></div>
  <div class="ms" data-key="sku_status"><button class="ms-btn">F/O · OPE</button><div class="ms-panel"></div></div>
  <div class="ms" data-key="line"><button class="ms-btn">Marketing Line</button><div class="ms-panel"></div></div>
  <div class="ms" data-key="inch"><button class="ms-btn">Rim (inch)</button><div class="ms-panel"></div></div>
  <div class="ms" data-key="pattern"><button class="ms-btn">Pattern</button><div class="ms-panel"></div></div>
  <button class="reset" onclick="resetFilters()">Reset</button>
  <div class="search">
    <input id="fltr-search" type="text" placeholder="Search SKU / M-code / description / size…">
  </div>
</div>

<!-- KPI strip -->
<div class="kpi-strip" id="kpi-strip">
  <div class="kpi"><h4>Active SKUs</h4><span class="v" id="kpi-sku">—</span></div>
  <div class="kpi short"><h4>Shortage</h4><span class="v" id="kpi-short">—</span><span class="u">SKUs</span></div>
  <div class="kpi bal"><h4>Balance</h4><span class="v" id="kpi-bal">—</span><span class="u">SKUs</span></div>
  <div class="kpi sur"><h4>Surplus</h4><span class="v" id="kpi-sur">—</span><span class="u">SKUs</span></div>
  <div class="kpi ser"><h4>Serious Surplus</h4><span class="v" id="kpi-ser">—</span><span class="u">SKUs</span></div>
  <div class="kpi nom"><h4>No move</h4><span class="v" id="kpi-nom">—</span><span class="u">SKUs</span></div>
  <div class="kpi"><h4>National MOI</h4><span class="v" id="kpi-moi">—</span><span class="u">months</span></div>
</div>

<div class="thresh-note">
  <b>MOI</b> = Months Of Inventory (Stock on hand ÷ 3-month avg monthly demand) &nbsp;·&nbsp;
  <span class="sw short"></span><b>Shortage</b> MOI ≤ 1 &nbsp;·&nbsp;
  <span class="sw bal"></span><b>Balance</b> 1 &lt; MOI ≤ 3 &nbsp;·&nbsp;
  <span class="sw sur"></span><b>Surplus</b> 3 &lt; MOI ≤ 6 &nbsp;·&nbsp;
  <span class="sw ser"></span><b>Serious Surplus</b> MOI &gt; 6 &nbsp;·&nbsp;
  <span class="sw nom"></span><b>No move</b> stock present but zero 3M demand
</div>

<div class="wrap">
<div class="grid">

  <div class="state-col">
    <div class="card">
      <h3>Stock across states
        <span class="hint">as of {{ meta.data_date }}</span>
        <span class="icons">
          <button class="icon-btn" onclick="emailScreen('state-card','State overview')" title="Email this panel">✉</button>
        </span>
      </h3>
      <div id="state-cards"></div>
    </div>
  </div>

  <div class="right">
    <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px;">
      <div class="card" id="chart-card-line" data-chart="line">
        <h3>By Marketing Line
          <span class="hint" id="chart-line-hint">SKU count · stacked by status</span>
          <span class="icons">
            <button class="icon-btn" onclick="toggleChartMode('line')" title="Switch between # count and % mix" id="btn-line-pct">%</button>
            <button class="icon-btn" onclick="emailScreen('chart-card-line','Marketing Line chart')" title="Email this chart">✉</button>
          </span>
        </h3>
        <div class="chartbox"><canvas id="chart-line"></canvas></div>
        <div class="legend">
          <span><span class="dot short"></span>Shortage</span>
          <span><span class="dot bal"></span>Balance</span>
          <span><span class="dot sur"></span>Surplus</span>
          <span><span class="dot ser"></span>Serious Surplus</span>
          <span><span class="dot nom"></span>No move</span>
        </div>
      </div>
      <div class="card" id="chart-card-inch" data-chart="inch">
        <h3>By Rim size (inch)
          <span class="hint" id="chart-inch-hint">SKU count · stacked by status</span>
          <span class="icons">
            <button class="icon-btn" onclick="toggleChartMode('inch')" title="Switch between # count and % mix" id="btn-inch-pct">%</button>
            <button class="icon-btn" onclick="emailScreen('chart-card-inch','Rim inch chart')" title="Email this chart">✉</button>
          </span>
        </h3>
        <div class="chartbox"><canvas id="chart-inch"></canvas></div>
      </div>
    </div>

    <div class="card expand-target" id="sku-card">
      <h3>SKU drill-down
        <span class="hint">click any row for the monthly-by-state view</span>
        <span class="icons">
          <button id="btn-dl-xlsx" class="icon-btn" onclick="downloadXLSX()" title="Download the current view as XLSX (recommended — opens directly in Excel with formatted headers). Select rows first to download just those.">⬇ XLSX</button>
          <button id="btn-dl-csv"  class="icon-btn" onclick="downloadCSV()" title="Download the current view as CSV (one value per cell). Select rows first to download just those.">⬇ CSV</button>
          <button class="icon-btn" onclick="toggleExpandTable()" title="Expand table full-screen" id="btn-expand-tbl">⛶</button>
          <button class="icon-btn" onclick="emailScreen('sku-card','SKU drill-down')" title="Email this table">✉</button>
        </span>
      </h3>

      <!-- Active-filter summary — mirrors the top filter bar as plain text
           so a captured screenshot / printed page tells you what the
           values are filtered to without having to see the filter bar. -->
      <div class="filter-text" id="filter-text">
        <span class="lbl">Filter</span>
        <em>all SKUs</em>
      </div>

      <div class="tabs">
        <div class="tab active" data-tab="shortage">🟡 Shortage <span class="n" id="n-short">0</span></div>
        <div class="tab" data-tab="balanced">🟢 Balance <span class="n" id="n-baltab">0</span></div>
        <div class="tab" data-tab="surplus">🟠 Surplus <span class="n" id="n-sur">0</span></div>
        <div class="tab" data-tab="serious_surplus">🟥 Serious Surplus <span class="n" id="n-ser">0</span></div>
        <div class="tab" data-tab="no_move">🟣 No move <span class="n" id="n-nom">0</span></div>
        <div class="tab" data-tab="total">🔵 Total <span class="n" id="n-tot">0</span></div>
        <button class="icon-btn" style="margin-left:auto" onclick="togglePipeline()"
                id="btn-pipeline" title="Show / hide per-state Stock · Port · Water · Factory (plus National Total)">
          ▶ Pipeline detail
        </button>
      </div>

      <div class="sel-summary" id="sel-summary">
        <span class="lbl">Total in view</span>
        <span><span class="lbl">Rows</span> <span class="v" id="sum-rows">—</span></span>
        <span><span class="lbl">Stock</span> <span class="v" id="sum-stock">—</span></span>
        <span><span class="lbl">3M Avg / mo</span> <span class="v" id="sum-3m">—</span></span>
        <span><span class="lbl">Aggregate MOI</span> <span class="v" id="sum-moi">—</span></span>
        <span id="sel-info" class="none-selected">no rows selected — click rows to build a subset</span>
        <button class="clear-sel" id="clear-sel" style="display:none" onclick="clearSelection()">Clear selection</button>
      </div>

      <div class="tbl-wrap">
        <table class="dt" id="sku-tbl">
          <colgroup id="sku-colgroup"></colgroup>
          <thead id="sku-thead"></thead>
          <tbody id="tbl-body"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>
</div>

<!-- ── Drill-down modal ── -->
<div class="modal-bg" id="modal-bg">
  <div class="modal" id="modal-panel">
    <div class="modal-hdr">
      <div>
        <div class="mtitle" id="m-title">—</div>
        <div class="msub" id="m-sub">—</div>
      </div>
      <div class="actions">
        <button class="icon-btn" onclick="emailScreen('modal-panel','SKU detail')" title="Email this SKU view">✉</button>
        <button class="icon-btn" onclick="closeModal()">✕ Close</button>
      </div>
    </div>
    <div class="modal-body">
      <div>
        <div class="stat">Stock on hand</div>
        <div class="figv" id="m-stock">—</div>
        <div class="stat">Pipeline incoming (Port + Water + Factory)</div>
        <div class="figv" id="m-pipe">—</div>
        <div class="stat">3-month avg demand / month</div>
        <div class="figv" id="m-3m">—</div>
        <div class="stat">MOI — stock on hand only</div>
        <div class="figv" id="m-moi">—</div>
        <div class="stat" title="(Stock + Port + Water + Factory) ÷ MAX(3M Avg, 4-6M Avg, 7-9M Avg, 10-12M Avg)">Merge_MOI(PPL) — (Stock+Pipeline) ÷ max of four period averages</div>
        <div class="figv" id="m-moiplus">—</div>
      </div>
      <div>
        <div class="stat">12-month sales by state <span style="text-transform:none;font-weight:400;color:#B0BEC5">— state-stacked bar</span></div>
        <div class="chartbox tall"><canvas id="m-chart"></canvas></div>
        <div class="stat" style="margin-top:14px">State breakdown</div>
        <table class="pipe-tbl">
          <thead><tr><th>State</th><th>Stock</th><th>Port</th><th>Water</th>
              <th>Factory</th><th>Pipeline</th><th>3M Avg</th><th>MOI</th><th>MOI +PPL</th></tr></thead>
          <tbody id="m-pipe-tbl"></tbody>
        </table>

        <div class="stat" style="margin-top:16px">M CODE breakdown <span style="text-transform:none;font-weight:400;color:#B0BEC5">— every material inside this Merge</span></div>
        <table class="pipe-tbl">
          <thead><tr><th>M CODE</th><th style="text-align:left">Product</th>
              <th>Stock</th><th>3M Avg</th><th>4-6M</th><th>7-9M</th><th>10-12M</th><th>MOI</th></tr></thead>
          <tbody id="m-mcode-tbl"></tbody>
        </table>

        <div class="stat" style="margin-top:16px">Top 10 Ship-to customers <span style="text-transform:none;font-weight:400;color:#B0BEC5">— last month · this month</span></div>
        <div id="m-top10" style="padding:10px 12px;background:#F8FAFC;border:1px dashed #CBD5E1;
             border-radius:6px;color:#607D8B;font-size:11.5px;line-height:1.55;margin-top:4px">
          <b style="color:#334155">Data source needed.</b>
          The current Stock_report XLSM doesn't include per-customer (Ship-to)
          shipment history — only aggregated per-state monthly sales.
          Once a Ship-to-level file (e.g. SAP VBRK/VBRP extract or a monthly
          Ship-to CSV) is wired in, this panel will show the top-10 buyers
          for last month and this month with the % each takes of the
          relevant state's demand.
        </div>
      </div>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
const DATA = {{ data_json | safe }};
/* Server-side workbook metadata (file, date, load stats) — exposed
   globally so the XLSX and .eml flows can label their outputs with
   the source workbook + data-as-of date. */
const META = {{ meta | tojson | safe }};
window.META = META;
const STATES = ["NSW","QLD","VIC","WA"];

/* ── Formatting ── */
const FMT_INT = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
const FMT_1   = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1, minimumFractionDigits: 1 });
const FMT_2   = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2, minimumFractionDigits: 2 });
function fmtI(n) { if (n == null || n === '') return '—';
    const v = +n; if (isNaN(v)) return '—'; return FMT_INT.format(v); }
function fmtF(n, d) { if (n == null || n === '') return '—';
    const v = +n; if (isNaN(v)) return '—';
    return (d === 2 ? FMT_2 : FMT_1).format(v); }
function pill(gr) {
    const cls = 'pill g-' + (gr || 'other').toString().replace(/[^A-Za-z]/g,'') + ' sm';
    return '<span class="' + cls + '">' + (gr || '—') + '</span>';
}
/* Small grey "(3M avg)" suffix rendered next to state stock cells so
   each cell reads e.g. "129 (0.5)" — the second figure is that
   state's 3-month monthly demand.  Skipped when the demand is zero
   so the cell doesn't look busy for dead SKUs. */
function demSuffix(demand) {
    if (demand == null || demand === 0) return '';
    /* Sales figures render as integers on screen (rounded from the
       raw monthly average); the exported XLSX still carries the
       precise decimal for downstream analysis. */
    return ' <span class="dem-parens">(' + FMT_INT.format(Math.round(demand)) + ')</span>';
}

/* ── Multi-select filter state ──
   Kept as Sets for O(1) membership tests, and persisted to
   localStorage so a page refresh restores the same view.  The
   storage key is versioned so a data-model change won't crash the
   restore path — it just resets the affected keys. */
const FILTER_LS_KEY = 'hkau_stock_filters_v1';
const filterState = {
    status:     new Set(),
    state:      new Set(),
    brand:      new Set(),
    product:    new Set(),      // PCLT / TBR / Other  (r.category)
    sku_status: new Set(),      // Active / F/O / OPE / OE A/S
    line:       new Set(),
    inch:       new Set(),
    pattern:    new Set(),
};
function saveFilterState() {
    try {
        const snap = {};
        for (const k in filterState) snap[k] = [...filterState[k]];
        localStorage.setItem(FILTER_LS_KEY, JSON.stringify(snap));
    } catch (_) { /* private mode / disabled — silently no-op */ }
}
function loadFilterState() {
    try {
        const raw = localStorage.getItem(FILTER_LS_KEY);
        if (!raw) return;
        const snap = JSON.parse(raw);
        for (const k in filterState) {
            if (!Array.isArray(snap[k])) continue;
            filterState[k] = new Set(snap[k]);
        }
    } catch (_) { /* corrupted / disabled — start fresh */ }
}
function clearAllFilters() {
    for (const k in filterState) filterState[k].clear();
    saveFilterState();
    /* Refresh every visible multi-select panel + button label,
       then re-render the page. */
    Object.keys(filterState).forEach(k => {
        renderMsPanel(k);
        updateMsBtn(k);
    });
    refresh();
    showToast('All filters cleared.');
}
const KEY_LBL = { status:'Status', state:'State', brand:'Brand',
                  product:'Product', sku_status:'F/O · OPE',
                  line:'Marketing Line',
                  inch:'Rim (inch)', pattern:'Pattern' };

function buildFilterOptions() {
    const values = { status: ['shortage','balanced','surplus','serious_surplus','no_move'],
                     state: STATES.slice(),
                     brand: new Set(), product: new Set(), sku_status: new Set(),
                     line: new Set(), inch: new Set(), pattern: new Set() };
    DATA.all_rows.forEach(r => {
        if (r.brand)      values.brand.add(r.brand);
        if (r.category)   values.product.add(r.category);
        if (r.sku_status) values.sku_status.add(r.sku_status);
        if (r.line)       values.line.add(r.line);
        if (r.inch)       values.inch.add(r.inch);
        if (r.pattern)    values.pattern.add(r.pattern);
    });
    /* Product order: PCLT first (biggest segment), TBR next, Other last */
    const prodOrder = ['PCLT','TBR','Other'];
    /* F/O · OPE order: Active first, then the seven flagged buckets
       (plus any composite "A + B" values that showed up in the data) */
    const skuBase   = ['Active','F/O','OPE','M/S','Testing','Transfer','OE A/S','Price'];
    const seenSku   = [...values.sku_status];
    const skuKnown  = skuBase.filter(s => values.sku_status.has(s));
    const skuMixed  = seenSku.filter(s => !skuBase.includes(s)).sort();
    const skuOrder  = [...skuKnown, ...skuMixed];
    return {
        status:     values.status,
        state:      values.state,
        brand:      [...values.brand].sort(),
        product:    prodOrder.filter(p => values.product.has(p)),
        sku_status: skuOrder,
        line:       [...values.line].sort(),
        inch:       [...values.inch].sort((a,b) => parseFloat(a) - parseFloat(b) || a.localeCompare(b)),
        pattern:    [...values.pattern].sort(),
    };
}
const OPT = buildFilterOptions();

const STATUS_PRETTY = { shortage:'Shortage', balanced:'Balance', surplus:'Surplus',
                        serious_surplus:'Serious Surplus', no_move:'No move' };

function renderMsPanel(key) {
    const opts = OPT[key];
    const chosen = filterState[key];
    const panel = document.querySelector('.ms[data-key="'+key+'"] .ms-panel');
    let h = '';
    opts.forEach(v => {
        const shown = key === 'status' ? STATUS_PRETTY[v] : v;
        const checked = chosen.has(v) ? ' checked' : '';
        h += '<label><input type="checkbox" value="' + v + '"' + checked + '> ' + shown + '</label>';
    });
    h += '<div class="ms-actions"><button data-act="all">All</button><button data-act="none">None</button></div>';
    panel.innerHTML = h;
    panel.querySelectorAll('input[type=checkbox]').forEach(cb => {
        cb.addEventListener('change', () => {
            if (cb.checked) filterState[key].add(cb.value);
            else            filterState[key].delete(cb.value);
            saveFilterState();
            updateMsBtn(key); refresh();
        });
    });
    panel.querySelector('[data-act="all"]').addEventListener('click', (e) => {
        e.stopPropagation();
        opts.forEach(v => filterState[key].add(v));
        saveFilterState();
        renderMsPanel(key); updateMsBtn(key); refresh();
    });
    panel.querySelector('[data-act="none"]').addEventListener('click', (e) => {
        e.stopPropagation();
        filterState[key].clear();
        saveFilterState();
        renderMsPanel(key); updateMsBtn(key); refresh();
    });
}
function updateMsBtn(key) {
    const btn = document.querySelector('.ms[data-key="'+key+'"] .ms-btn');
    const chosen = filterState[key];
    if (chosen.size === 0) { btn.classList.remove('active'); btn.innerHTML = KEY_LBL[key]; }
    else { btn.classList.add('active'); btn.innerHTML = KEY_LBL[key] + '<span class="n">' + chosen.size + '</span>'; }
}
/* Restore any saved filter state BEFORE we paint the dropdowns
   for the first time — otherwise the checkboxes render unchecked
   and users lose their view every refresh. */
loadFilterState();
Object.keys(filterState).forEach(key => {
    renderMsPanel(key); updateMsBtn(key);
    const holder = document.querySelector('.ms[data-key="'+key+'"]');
    holder.querySelector('.ms-btn').addEventListener('click', (e) => {
        e.stopPropagation();
        document.querySelectorAll('.ms.open').forEach(o => { if (o !== holder) o.classList.remove('open'); });
        holder.classList.toggle('open');
    });
});
document.addEventListener('click', (e) => {
    if (!e.target.closest('.ms')) {
        document.querySelectorAll('.ms.open').forEach(o => o.classList.remove('open'));
    }
});
function resetFilters() {
    Object.keys(filterState).forEach(key => {
        filterState[key].clear(); renderMsPanel(key); updateMsBtn(key);
    });
    document.getElementById('fltr-search').value = '';
    saveFilterState();
    try { localStorage.removeItem(SEARCH_LS_KEY); } catch (_) {}
    refresh();
}

/* ── Row filter (used by every derived section) ── */
function rowPasses(r) {
    if (filterState.status.size     && !filterState.status.has(r.status))         return false;
    if (filterState.brand.size      && !filterState.brand.has(r.brand))           return false;
    if (filterState.product.size    && !filterState.product.has(r.category))      return false;
    if (filterState.sku_status.size && !filterState.sku_status.has(r.sku_status)) return false;
    if (filterState.line.size    && !filterState.line.has(r.line))       return false;
    if (filterState.inch.size    && !filterState.inch.has(r.inch))       return false;
    if (filterState.pattern.size && !filterState.pattern.has(r.pattern)) return false;
    if (filterState.state.size) {
        let hit = false;
        filterState.state.forEach(s => {
            if (r.state_stock[s] > 0 || r.state_3m[s] > 0) hit = true;
        });
        if (!hit) return false;
    }
    const fq = (document.getElementById('fltr-search').value || '').trim().toLowerCase();
    if (fq) {
        const hay = ((r.description||'') + ' ' + (r.size||'') + ' '
                   + (r.m_code||'') + ' ' + (r.merge_code||'') + ' '
                   + (r.pattern||'') + ' ' + (r.line||'') + ' '
                   + (r.product_name||'') + ' ' + (r.brand||'')).toLowerCase();
        if (!hay.includes(fq)) return false;
    }
    return true;
}
/* Persist the search box too, so a page refresh keeps the whole
   filter picture — chips AND free text.  Debounced write matches the
   render debounce so we don't hammer localStorage on every keystroke. */
const SEARCH_LS_KEY = 'hkau_stock_search_v1';
try {
    const savedSearch = localStorage.getItem(SEARCH_LS_KEY);
    if (savedSearch) document.getElementById('fltr-search').value = savedSearch;
} catch (_) {}
document.getElementById('fltr-search').addEventListener('input',
    (function() { let t; return function(e) {
        clearTimeout(t);
        t = setTimeout(() => {
            try { localStorage.setItem(SEARCH_LS_KEY, e.target.value || ''); } catch (_) {}
            refresh();
        }, 150);
    }; })());

function currentSetOfRows() { return DATA.all_rows.filter(rowPasses); }
/* Aggregate stats (KPI tiles, state cards, charts, Total row) work
   at the MERGE CODE level.  In the per-M-CODE workbook layout each
   M CODE row carries only its own stock / demand, so a plain
   dedupe would UNDER-count the merge total.  We sum the M CODE
   rows into one synthetic merge-total row instead. */
function currentSetDedupedByMerge() {
    /* Merge-level roll-up.  Per-M CODE rows sum; merge-shared rows
       (workbook only had per-merge granularity, so all siblings
       inherit the same merge total) contribute exactly once so we
       don't multi-count.  `_seen_shared` tracks which merges already
       consumed their shared numbers. */
    const bucket = new Map();
    const seenShared = new Set();
    for (const r of DATA.all_rows) {
        if (!rowPasses(r)) continue;
        let agg = bucket.get(r.merge_code);
        if (!agg) {
            agg = {
                merge_code: r.merge_code,
                brand: r.brand, line: r.line, pattern: r.pattern,
                product_name: r.product_name, size: r.size,
                inch: r.inch, group: r.group, category: r.category,
                is_18plus: r.is_18plus, is_low_profile: r.is_low_profile,
                is_suv: r.is_suv, sku_status: r.sku_status,
                state_stock:      {NSW:0,QLD:0,VIC:0,WA:0},
                state_pipeline:   {NSW:0,QLD:0,VIC:0,WA:0},
                state_pipe_parts: {NSW:{port:0,water:0,fac:0},QLD:{port:0,water:0,fac:0},VIC:{port:0,water:0,fac:0},WA:{port:0,water:0,fac:0}},
                state_3m:         {NSW:0,QLD:0,VIC:0,WA:0},
                history:          {NSW:new Array(12).fill(0), QLD:new Array(12).fill(0),
                                   VIC:new Array(12).fill(0), WA:new Array(12).fill(0),
                                   TOTAL:new Array(12).fill(0)},
                total_stock:0, total_all:0, total_3m:0, total_12m:0,
                p_3m:0, avg_6m_old:0, avg_7_9m:0, avg_10_12m:0, max_demand:0,
            };
            bucket.set(r.merge_code, agg);
        }
        /* Shared rows: every sibling carries the SAME merge total, so
           we take the first one and skip the rest. */
        if (r.merge_shared) {
            if (seenShared.has(r.merge_code)) continue;
            seenShared.add(r.merge_code);
        }
        STATES.forEach(s => {
            agg.state_stock[s]    += r.state_stock[s]    || 0;
            agg.state_pipeline[s] += r.state_pipeline[s] || 0;
            agg.state_3m[s]       += r.state_3m[s]       || 0;
            const pp = r.state_pipe_parts?.[s] || {port:0,water:0,fac:0};
            agg.state_pipe_parts[s].port  += pp.port  || 0;
            agg.state_pipe_parts[s].water += pp.water || 0;
            agg.state_pipe_parts[s].fac   += pp.fac   || 0;
            for (let i = 0; i < 12; i++) agg.history[s][i] += r.history[s]?.[i] || 0;
        });
        for (let i = 0; i < 12; i++) agg.history.TOTAL[i] += r.history.TOTAL?.[i] || 0;
        agg.total_stock += r.total_stock || 0;
        agg.total_all   += r.total_all   || 0;
        agg.total_3m    += r.total_3m    || 0;
        agg.total_12m   += r.total_12m   || 0;
        agg.p_3m        += r.p_3m        || 0;
        agg.avg_6m_old  += r.avg_6m_old  || 0;
        agg.avg_7_9m    += r.avg_7_9m    || 0;
        agg.avg_10_12m  += r.avg_10_12m  || 0;
        agg.max_demand   = Math.max(agg.max_demand, r.max_demand || 0);
    }
    /* Recompute the merge-level MOI and status from the summed
       stock / demand.  This is the ONLY correct place to derive
       these; per-M CODE status doesn't apply to the merge total. */
    const out = [];
    for (const agg of bucket.values()) {
        agg.moh          = agg.total_3m > 0 ? agg.total_stock / agg.total_3m : null;
        agg.moh_plus     = agg.total_3m > 0 ? agg.total_all   / agg.total_3m : null;
        agg.moh_plus_max = agg.max_demand > 0 ? agg.total_all / agg.max_demand : null;
        if      (agg.total_3m === 0 && agg.total_stock === 0) agg.status = 'empty';
        else if (agg.total_3m === 0 && agg.total_stock  >  0) agg.status = 'no_move';
        else if (agg.moh <= 1)  agg.status = 'shortage';
        else if (agg.moh <= 3)  agg.status = 'balanced';
        else if (agg.moh <= 6)  agg.status = 'surplus';
        else                    agg.status = 'serious_surplus';
        out.push(agg);
    }
    return out;
}

/* ── KPI + state cards ── */
function recomputeKPI() {
    /* KPI tiles show MERGE-level counts (sum-by-merge — a merge with
       10 M CODEs still counts as 1 merge in each bucket).  Tab labels
       show the ROW-level M CODE counts that match what the table
       actually renders, so users see consistent numbers.  Both come
       out of one pass so the two never disagree on shared filters. */
    const mergeRows = currentSetDedupedByMerge();
    let sh=0, bl=0, su=0, se=0, nm=0, totStk=0, tot3m=0;
    mergeRows.forEach(r => {
        if      (r.status === 'shortage')        sh++;
        else if (r.status === 'balanced')        bl++;
        else if (r.status === 'surplus')         su++;
        else if (r.status === 'serious_surplus') se++;
        else if (r.status === 'no_move')         nm++;
        totStk += r.total_stock; tot3m += r.total_3m;
    });
    document.getElementById('kpi-sku').textContent   = fmtI(sh+bl+su+se+nm);
    document.getElementById('kpi-short').textContent = fmtI(sh);
    document.getElementById('kpi-bal').textContent   = fmtI(bl);
    document.getElementById('kpi-sur').textContent   = fmtI(su);
    document.getElementById('kpi-ser').textContent   = fmtI(se);
    document.getElementById('kpi-nom').textContent   = fmtI(nm);
    document.getElementById('kpi-moi').textContent   = tot3m > 0 ? FMT_1.format(totStk / tot3m) : '—';
    /* Tab counters: per-M-CODE (matches table row count).  A merge
       whose per-M-CODE status is spread across buckets contributes
       to each one — the tab table itself would show the same. */
    const perRow = currentSetOfRows();
    let rsh=0, rbl=0, rsu=0, rse=0, rnm=0;
    perRow.forEach(r => {
        if      (r.status === 'shortage')        rsh++;
        else if (r.status === 'balanced')        rbl++;
        else if (r.status === 'surplus')         rsu++;
        else if (r.status === 'serious_surplus') rse++;
        else if (r.status === 'no_move')         rnm++;
    });
    document.getElementById('n-short').textContent   = fmtI(rsh);
    document.getElementById('n-baltab').textContent  = fmtI(rbl);
    document.getElementById('n-sur').textContent     = fmtI(rsu);
    document.getElementById('n-ser').textContent     = fmtI(rse);
    document.getElementById('n-nom').textContent     = fmtI(rnm);
    document.getElementById('n-tot').textContent     = fmtI(rsh + rbl + rsu + rse + rnm);
}
function renderStateCards() {
    const rows = currentSetDedupedByMerge();
    const host = document.getElementById('state-cards');
    const acc = {};
    STATES.forEach(s => acc[s] = {stock:0, pipeline:0, demand_3m:0,
        shortage:0, balanced:0, surplus:0, serious_surplus:0, no_move:0});
    rows.forEach(r => {
        STATES.forEach(s => {
            acc[s].stock     += r.state_stock[s]    || 0;
            acc[s].pipeline  += r.state_pipeline[s] || 0;
            acc[s].demand_3m += r.state_3m[s]       || 0;
            /* Status chip counts now use the SKU's MERGE-CODE status
               (from Merge_MOI(PPL)) rather than a re-computed
               per-state MOI.  A SKU is credited to a state only if it
               has stock or 3M demand there — matches how planners
               think about "the shortage list in NSW". */
            const sd = r.state_3m[s] || 0, ss = r.state_stock[s] || 0;
            if (sd > 0 || ss > 0) {
                if (r.status && acc[s][r.status] !== undefined) acc[s][r.status]++;
            }
        });
    });
    let html = '';
    STATES.forEach(s => {
        const st = acc[s];
        const moi = st.demand_3m > 0 ? (st.stock / st.demand_3m) : null;
        const activeCls = filterState.state.has(s) ? ' state-card-active' : '';
        html += '<div class="state-card' + activeCls + '" data-state="' + s + '" '
             +      'title="Click to toggle the ' + s + ' filter">'
             + '<h3>' + s + ' <span class="m">MOI ' + (moi != null ? FMT_1.format(moi) : '—') + '</span></h3>'
             + '<div class="state-row"><span class="lbl">Stock on hand</span>'
             + '<span class="v">' + fmtI(st.stock) + '</span></div>'
             + '<div class="state-row"><span class="lbl">In pipeline</span>'
             + '<span class="v">' + fmtI(st.pipeline) + '</span></div>'
             + '<div class="state-row"><span class="lbl">3M avg demand / mo</span>'
             + '<span class="v">' + fmtI(st.demand_3m) + '</span></div>'
             + '<div class="state-row"><span class="lbl">SKU status</span>'
             + '<span class="v">'
             + '<span class="chip short">' + fmtI(st.shortage) + '</span>'
             + '<span class="chip bal">'   + fmtI(st.balanced) + '</span>'
             + '<span class="chip sur">'   + fmtI(st.surplus)  + '</span>'
             + '<span class="chip ser">'   + fmtI(st.serious_surplus) + '</span>'
             + '<span class="chip nom">'   + fmtI(st.no_move)  + '</span>'
             + '</span></div>'
             + '</div>';
    });
    host.innerHTML = html;
    /* Card click → toggle the State multi-select filter.  Solo-select
       makes the most common flow ("show only NSW") one click, and
       clicking the same card again turns it off.  Multi-state is
       still available via the top filter dropdown for power users. */
    host.querySelectorAll('.state-card').forEach(card => {
        card.addEventListener('click', () => {
            const s = card.dataset.state;
            if (filterState.state.has(s) && filterState.state.size === 1) {
                filterState.state.clear();
            } else {
                filterState.state.clear();
                filterState.state.add(s);
            }
            saveFilterState();
            renderMsPanel('state'); updateMsBtn('state');
            refresh();
        });
    });
}

/* ── Charts (Marketing Line + Rim inch) with # / % toggle ── */
let _lineChart = null, _inchChart = null;
const chartMode = { line: 'count', inch: 'count' };   // 'count' | 'pct'
function computeBy(rows, keyFn) {
    const by = {};
    rows.forEach(r => {
        const k = keyFn(r);
        if (!k) return;
        if (!by[k]) by[k] = {shortage:0, balanced:0, surplus:0, serious_surplus:0, no_move:0};
        if (by[k][r.status] !== undefined) by[k][r.status]++;
    });
    return by;
}
function drawStackedBar(canvas_id, by, order, mode) {
    const labels = order;
    let short = labels.map(k => by[k].shortage);
    let bal   = labels.map(k => by[k].balanced);
    let sur   = labels.map(k => by[k].surplus);
    let ser   = labels.map(k => by[k].serious_surplus);
    let nom   = labels.map(k => by[k].no_move);
    if (mode === 'pct') {
        const tot = labels.map((_,i) => short[i]+bal[i]+sur[i]+ser[i]+nom[i]);
        const pct = arr => arr.map((v,i) => tot[i] > 0 ? (v / tot[i] * 100) : 0);
        short = pct(short); bal = pct(bal); sur = pct(sur); ser = pct(ser); nom = pct(nom);
    }
    return new Chart(document.getElementById(canvas_id), {
        type: 'bar',
        data: { labels, datasets: [
            { label: 'Shortage', data: short, backgroundColor: '#FACC15' },
            { label: 'Balance',  data: bal,   backgroundColor: '#66BB6A' },
            { label: 'Surplus',  data: sur,   backgroundColor: '#FB8C00' },
            { label: 'Serious',  data: ser,   backgroundColor: '#B71C1C' },
            { label: 'No move',  data: nom,   backgroundColor: '#8E24AA' },
        ]},
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: { callbacks: {
                    label: ctx => {
                        const v = ctx.raw;
                        return ctx.dataset.label + ': ' + (mode === 'pct'
                            ? FMT_1.format(v) + '%'
                            : FMT_INT.format(v));
                    }
                }}
            },
            scales: {
                x: { stacked: true, ticks: { font:{size:10}, autoSkip:false, maxRotation:45, minRotation:25 } },
                y: { stacked: true, ticks: { font:{size:10},
                        callback: v => mode === 'pct' ? v + '%' : v },
                     max: mode === 'pct' ? 100 : undefined,
                }
            }
        }
    });
}
function renderCharts() {
    /* Charts count SKUs — Merge Code is the SKU, so dedupe.
       Guarded so a missing Chart.js library doesn't kill the whole
       refresh() chain (the tabs / table / KPI would go dark). */
    if (typeof Chart === 'undefined') {
        return;
    }
    try {
        const rows = currentSetDedupedByMerge();
        if (_lineChart) _lineChart.destroy();
        if (_inchChart) _inchChart.destroy();
        const byLine = computeBy(rows, r => r.line || 'Other');
        const lineOrder = Object.keys(byLine).sort((a,b) => (byLine[b].shortage||0) - (byLine[a].shortage||0));
        _lineChart = drawStackedBar('chart-line', byLine, lineOrder, chartMode.line);

        const byInch = computeBy(rows, r => r.inch || '—');
        const inchOrder = Object.keys(byInch).sort((a,b) => {
            const na = parseFloat(a), nb = parseFloat(b);
            if (!isNaN(na) && !isNaN(nb)) return na - nb;
            return a.localeCompare(b);
        });
        _inchChart = drawStackedBar('chart-inch', byInch, inchOrder, chartMode.inch);
    } catch (err) {
        console.warn('main charts skipped:', err);
        return;
    }

    document.getElementById('chart-line-hint').textContent =
        chartMode.line === 'pct' ? 'SKU % mix · each bar sums to 100' : 'SKU count · stacked by status';
    document.getElementById('chart-inch-hint').textContent =
        chartMode.inch === 'pct' ? 'SKU % mix · each bar sums to 100' : 'SKU count · stacked by status';
    document.getElementById('btn-line-pct').classList.toggle('active', chartMode.line === 'pct');
    document.getElementById('btn-inch-pct').classList.toggle('active', chartMode.inch === 'pct');
    document.getElementById('btn-line-pct').textContent = chartMode.line === 'pct' ? '#' : '%';
    document.getElementById('btn-inch-pct').textContent = chartMode.inch === 'pct' ? '#' : '%';
}
function toggleChartMode(which) {
    chartMode[which] = chartMode[which] === 'count' ? 'pct' : 'count';
    renderCharts();
}

/* ── SKU table (sorting + row selection + summary + tabs + pipeline expansion) ── */
let curTab = 'shortage';
let sortCol = null, sortDir = 0;     // 0 = none, 1 = asc, -1 = desc
let showPipeline = false;            // toggled by "▶ Pipeline detail"
const selected = new Set();

function togglePipeline() {
    showPipeline = !showPipeline;
    const btn = document.getElementById('btn-pipeline');
    btn.innerHTML = showPipeline ? '◀ Hide pipeline' : '▶ Pipeline detail';
    btn.classList.toggle('active', showPipeline);
    renderTable();
}

/* Build the <thead> row.  When showPipeline is on, each state
   expands into a 5-cell group: Stock / Port / Water / CY / Factory.
   A slim colored header band above the sub-cells identifies which
   state the group belongs to. */
function buildTableHead() {
    const nonState = [
        ['merge_code','Merge'], ['m_code','M CODE'], ['brand','Brand'],
        ['line','Marketing Line'], ['product_name','Product Name'],
        ['pattern','Pattern'],
        ['sku_status','F/O·OPE'],
        ['size','Size'], ['inch','Inch'], ['li_ss','LI/SS'],
    ];
    const stateColour = { NSW:'#1976D2', QLD:'#EF6C00', VIC:'#8E24AA', WA:'#00897B' };
    /* Tag both <table> and <thead> with a `pipe-mode` class so the CSS
       knows the header is a two-row banner and needs sticky offsets
       (state banner top:0, sub-column labels top:26px, total-row
       top:52px) instead of the compact single-row layout. */
    document.getElementById('sku-thead').className = showPipeline ? 'pipe-mode' : '';
    document.getElementById('sku-tbl').classList.toggle('pipe-mode', showPipeline);
    /* Rebuild <colgroup> so `table-layout: fixed` has explicit
       widths per column.  The 10 identity columns use the fixed
       pixel widths declared in CSS, and the data columns share
       the remaining viewport evenly via `width: 1fr`-style
       distribution (each gets `width: *px` where the browser
       ignores nothing — with table-layout:fixed each column with
       no width gets an equal share of the remainder). */
    /* Widths MUST match the CSS nth-child(N) rules above so the
       colgroup and the sticky-left offsets stay in sync. */
    const IDENTITY_WIDTHS = [46, 68, 36, 92, 92, 52, 68, 80, 40, 56];
    /* Give each data column an EXPLICIT width so `table-layout: fixed`
       renders headers wide enough to read ("STK / PRT / MOI / 3M /
       4-6M …") without truncating.  If the viewport isn't wide enough
       the table overflows horizontally — the freeze pane keeps the
       identity columns visible, so a scroll bar is fine. */
    const DATA_COL_WIDTH = showPipeline ? 60 : 72;
    const nDataCols = showPipeline
        ? (STATES.length * 4 + 4 + 2 + 4)  // 4 states × 4 sub + total × 4 + MOI×2 + period×4 = 26
        : (STATES.length + 1 + 2 + 4);      // state stock + STOCK + MOI×2 + period×4 = 11
    const colgroup = document.getElementById('sku-colgroup');
    colgroup.innerHTML = IDENTITY_WIDTHS.map(w => '<col style="width:' + w + 'px">').join('')
        + Array(nDataCols).fill('<col style="width:' + DATA_COL_WIDTH + 'px">').join('');
    /* Table needs to grow past 100% width when data cols need more
       than the viewport provides — force width to the summed total
       so horizontal scroll kicks in cleanly. */
    const totalWidth = IDENTITY_WIDTHS.reduce((a,b)=>a+b, 0) + nDataCols * DATA_COL_WIDTH;
    document.getElementById('sku-tbl').style.width = totalWidth + 'px';
    let h = '';
    if (showPipeline) {
        /* Two-row header: state band + sub-column labels.  Each state
           has 4 sub-columns (Stock/Port/Water/Factory) and a final
           TOTAL group sits at the far right so the reader can compare
           per-state pipeline vs. the national roll-up in one glance.
           The state-band row is marked `state-band`; the sub-label
           row is marked `col-labels` — CSS above sticks them at 0 and
           26px so the banner stays anchored during vertical scroll. */
        h = '<tr class="state-band"><th colspan="' + nonState.length + '" style="background:#F1F5F9"></th>';
        STATES.forEach(s => {
            h += '<th colspan="4" class="grp-start" style="text-align:center;background:' + stateColour[s]
              + ';color:#fff;font-weight:700">' + s + '</th>';
        });
        h += '<th colspan="4" class="grp-start" style="text-align:center;background:#0E3F5F;color:#fff;font-weight:700">TOTAL</th>';
        h += '<th colspan="6" class="grp-start" style="background:#F1F5F9"></th></tr><tr class="col-labels">';
    } else {
        h = '<tr>';
    }
    nonState.forEach(([col, label]) => {
        h += '<th data-col="' + col + '">' + label + '<span class="sort"></span></th>';
    });
    if (showPipeline) {
        STATES.forEach(s => {
            const dc = s.toLowerCase();
            /* Short 3-char sub-column labels — with 27 data columns
               the viewport can't fit "Stock/Port/Water/Factory" in
               every state group without truncating.  STK/PRT/WTR/FAC
               reads clearly at 52 px each. */
            h += '<th class="r grp-start" data-col="' + dc + '" title="' + s + ' Stock">STK<span class="sort"></span></th>'
              +  '<th class="r" data-col="' + dc + '_port" title="' + s + ' Port">PRT<span class="sort"></span></th>'
              +  '<th class="r" data-col="' + dc + '_water" title="' + s + ' Water">WTR<span class="sort"></span></th>'
              +  '<th class="r" data-col="' + dc + '_fac" title="' + s + ' Factory">FAC<span class="sort"></span></th>';
        });
        /* National Total group divider */
        h += '<th class="r grp-start" data-col="total_stock" title="Total Stock">STK<span class="sort"></span></th>'
          +  '<th class="r" data-col="total_port" title="Total Port">PRT<span class="sort"></span></th>'
          +  '<th class="r" data-col="total_water" title="Total Water">WTR<span class="sort"></span></th>'
          +  '<th class="r" data-col="total_fac" title="Total Factory">FAC<span class="sort"></span></th>';
    } else {
        STATES.forEach((s, i) => {
            const dc = s.toLowerCase();
            const cls = (i === 0) ? 'r grp-start' : 'r';   /* divider before NSW */
            h += '<th class="' + cls + '" data-col="' + dc + '">' + s + ' <span style="opacity:.55;font-weight:400">(3M)</span><span class="sort"></span></th>';
        });
        /* Divider before Stock total */
        h += '<th class="r grp-start" data-col="total_stock">Stock <span style="opacity:.55;font-weight:400">(3M)</span><span class="sort"></span></th>';
    }
    /* MOI + MOI(PPL) — the earlier MERGE_MOI column was identical
       to MOI (both = Stock ÷ 3M), so we consolidate to two columns
       and use terse labels so they fit at 56 px. */
    h += '<th class="r grp-start" data-col="moh" title="Stock ÷ 3M Avg">MOI<span class="sort"></span></th>'
      +  '<th class="r grp-start" data-col="merge_moi_ppl" '
      +      'title="(Stock + Port + Water + Factory) ÷ MAX(3M Avg, 4-6M Avg, 7-9M Avg, 10-12M Avg).">'
      +      'MOI(PPL)<span class="sort"></span></th>';
    /* Period-average demand break-down — short "3M / 4-6M / 7-9M /
       10-12M" labels (no "Avg" suffix) so the four columns fit in
       ~56 px each without the browser truncating them.  The 3M column
       gets a leading divider so the period-avg block is visually
       separate from MOI(PPL); the other three stay borderless so the
       four periods read as one continuous group. */
    h += '<th class="r grp-start" data-col="p_3m"  title="Monthly avg over months −1 · −2 · −3 (the most recent 3 months)">3M<span class="sort"></span></th>'
      +  '<th class="r no-div" data-col="p_4_6m"   title="Monthly avg over months −4 · −5 · −6">4-6M<span class="sort"></span></th>'
      +  '<th class="r no-div" data-col="p_7_9m"   title="Monthly avg over months −7 · −8 · −9">7-9M<span class="sort"></span></th>'
      +  '<th class="r no-div" data-col="p_10_12m" title="Monthly avg over months −10 · −11 · −12">10-12M<span class="sort"></span></th>';
    h += '</tr>';
    document.getElementById('sku-thead').innerHTML = h;
    /* Re-wire sort handlers on the freshly built headers */
    document.querySelectorAll('#sku-tbl thead th[data-col]').forEach(th => {
        th.addEventListener('click', (e) => {
            /* Ignore clicks that landed on the drag handle — those
               are for column resize, not sort. */
            if (e.target.classList && e.target.classList.contains('col-resize-handle')) return;
            const col = th.dataset.col;
            if (sortCol === col) sortDir = sortDir === 1 ? -1 : (sortDir === -1 ? 0 : 1);
            else { sortCol = col; sortDir = 1; }
            renderTable();
        });
    });
    /* Attach draggable resize handles to every header cell so users
       can widen a column when its content is clipped.  Each handle
       tracks the <col> element at the SAME index inside <colgroup>
       and rewrites its `style.width` as the pointer moves. */
    const allTh = Array.from(document.querySelectorAll('#sku-tbl thead tr:last-child th'));
    const cols  = Array.from(document.querySelectorAll('#sku-colgroup col'));
    allTh.forEach((th, idx) => {
        const handle = document.createElement('div');
        handle.className = 'col-resize-handle';
        th.appendChild(handle);
        handle.addEventListener('mousedown', (e) => {
            e.preventDefault(); e.stopPropagation();
            const startX = e.clientX;
            const col = cols[idx];
            const startW = col ? parseInt(col.style.width, 10) || col.offsetWidth || 60 : 60;
            const onMove = (ev) => {
                const newW = Math.max(28, startW + (ev.clientX - startX));
                if (col) col.style.width = newW + 'px';
                /* Recompute the sticky-left offsets for the frozen
                   identity block if a frozen col was resized (idx<10).
                   Nudges other frozen cols leftward/rightward so they
                   stay contiguous.  For data cols (idx>=10) sticky
                   offsets don't apply, so the column just widens. */
                if (idx < 10) {
                    let acc = 0;
                    for (let i = 0; i < 10; i++) {
                        const w = cols[i] ? parseInt(cols[i].style.width, 10) : 60;
                        document.querySelectorAll(
                            '#sku-tbl thead th:nth-child(' + (i+1) + '),'
                          + '#sku-tbl tbody td:nth-child(' + (i+1) + ')'
                        ).forEach(el => { el.style.left = acc + 'px'; });
                        acc += w;
                    }
                }
            };
            const onUp = () => {
                document.removeEventListener('mousemove', onMove);
                document.removeEventListener('mouseup',   onUp);
                document.body.style.cursor = '';
            };
            document.addEventListener('mousemove', onMove);
            document.addEventListener('mouseup',   onUp);
            document.body.style.cursor = 'col-resize';
        });
    });
}

function totalPipe(r, leg) {
    if (!r.state_pipe_parts) return 0;
    return STATES.reduce((s, k) => s + (r.state_pipe_parts[k]?.[leg] || 0), 0);
}
function sortKey(r, col) {
    /* Pipeline sub-columns: nsw_port / qld_water / wa_fac … */
    const m = /^(nsw|qld|vic|wa)_(port|water|fac)$/.exec(col);
    if (m) {
        const state = m[1].toUpperCase(), leg = m[2];
        const pp = r.state_pipe_parts?.[state];
        return pp ? (pp[leg] || 0) : 0;
    }
    /* National-Total pipeline sub-columns */
    const t = /^total_(port|water|fac)$/.exec(col);
    if (t) return totalPipe(r, t[1]);
    switch (col) {
        case 'nsw': return r.state_stock.NSW || 0;
        case 'qld': return r.state_stock.QLD || 0;
        case 'vic': return r.state_stock.VIC || 0;
        case 'wa':  return r.state_stock.WA  || 0;
        case 'li_ss': return (parseFloat(r.li) || 0);
        case 'inch':  return (parseFloat(r.inch) || 0);
        case 'sku_status':     return r.sku_status || 'Active';
        case 'moh':            return r.moh          == null ? -1 : r.moh;
        case 'merge_moi_ppl':  return r.moh_plus_max == null ? -1 : r.moh_plus_max;
        case 'p_3m':           return r.p_3m        || 0;
        case 'p_4_6m':         return r.avg_6m_old  || 0;
        case 'p_7_9m':         return r.avg_7_9m    || 0;
        case 'p_10_12m':       return r.avg_10_12m  || 0;
        case 'total_3m':  return r.total_3m || 0;
        case 'total_stock': return r.total_stock || 0;
        default:      return r[col] == null ? '' : r[col];
    }
}
/* (Sort click handlers are wired inside buildTableHead so they attach
   to the freshly rebuilt <th> nodes every time the pipeline toggle
   changes.) */

document.querySelectorAll('.tab').forEach(t => {
    t.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
        t.classList.add('active');
        curTab = t.dataset.tab;
        renderTable();
    });
});

function renderTable() {
    buildTableHead();
    /* Total tab = every non-empty SKU (Shortage + Balance + Surplus +
       Serious Surplus + No move rolled into one). */
    const rawSrc = curTab === 'total' ? DATA.all_rows : DATA[curTab + '_rows'];
    let src = rawSrc.filter(rowPasses);

    /* ── MOI data-bar scale — INDEPENDENT per column ──
       Each of the three MOI columns computes its own reference max
       so a small value in Merge_MOI(PPL) still gets a visible
       bar when the plain MOI column has an outlier and vice-versa.
       Each scale is capped at 12 months so one giant SKU can't
       compress the rest into invisible slivers. */
    let mohMax = 0, mppMax = 0;
    src.forEach(r => {
        if (r.moh          != null && r.moh          > mohMax) mohMax = r.moh;
        if (r.moh_plus_max != null && r.moh_plus_max > mppMax) mppMax = r.moh_plus_max;
    });
    mohMax = Math.min(Math.max(mohMax, 1), 12);
    mppMax = Math.min(Math.max(mppMax, 1), 12);
    const moiBarFor = (v, scale) => {
        if (v == null || v <= 0) return '';
        const pct = Math.min(v / scale, 1) * 100;
        return 'background:linear-gradient(to right,#C8E6C9 ' + pct + '%, transparent ' + pct + '%);';
    };
    const moiBar    = v => moiBarFor(v, mohMax);
    const moiBarPPL = v => moiBarFor(v, mppMax);

    /* ── Aggregate row (rendered at the top) ──
       Per-M CODE rows sum; merge-shared rows (where every sibling
       carries the same merge total) contribute exactly once so we
       don't multi-count.  `mergeMax` tracks per-merge max_demand so
       ttlMax is a sum of merge maxes, not per-row maxes. */
    let ttlStock=0, ttlAll=0, ttl3M=0, ttl6=0, ttl79=0, ttl1012=0, ttlMax=0;
    const ttlStateStock = {NSW:0,QLD:0,VIC:0,WA:0}, ttlState3M = {NSW:0,QLD:0,VIC:0,WA:0};
    const ttlPipe = {NSW:{port:0,water:0,fac:0}, QLD:{port:0,water:0,fac:0},
                     VIC:{port:0,water:0,fac:0}, WA:{port:0,water:0,fac:0}};
    const mergeMax = new Map();
    const seenSharedTotals = new Set();
    src.forEach(r => {
        if (r.merge_shared) {
            if (seenSharedTotals.has(r.merge_code)) return;
            seenSharedTotals.add(r.merge_code);
        }
        ttlStock += r.total_stock || 0;
        ttlAll   += r.total_all   || 0;
        ttl3M    += r.total_3m    || 0;
        ttl6     += r.avg_6m_old  || 0;
        ttl79    += r.avg_7_9m    || 0;
        ttl1012  += r.avg_10_12m  || 0;
        const cur = mergeMax.get(r.merge_code) || 0;
        if ((r.max_demand || 0) > cur) mergeMax.set(r.merge_code, r.max_demand || 0);
        STATES.forEach(s => {
            ttlStateStock[s] += r.state_stock[s]  || 0;
            ttlState3M[s]    += r.state_3m[s]     || 0;
            const pp = r.state_pipe_parts?.[s] || {port:0,water:0,fac:0};
            ttlPipe[s].port  += pp.port  || 0;
            ttlPipe[s].water += pp.water || 0;
            ttlPipe[s].fac   += pp.fac   || 0;
        });
    });
    for (const v of mergeMax.values()) ttlMax += v;
    const ttlMOI  = ttl3M   > 0 ? (ttlStock / ttl3M)  : null;
    const ttlPPL  = ttlMax  > 0 ? (ttlAll   / ttlMax) : null;

    /* Cosmetic left-column band — cycles through 8 quiet hues so
       adjacent merge groups are visually distinct without loud
       colour bombing.  Same merge → same band on both M CODE rows
       and its Sub Total. */
    const BAND_COLOURS = ['#4A90E2','#F5A623','#7ED321','#BD10E0',
                          '#50E3C2','#B8E986','#F8A5C2','#9013FE'];
    const mergeBandColor = mc => BAND_COLOURS[Math.abs(mc) % BAND_COLOURS.length];
    /* ── Merge-group aware sort ──
       Sorting must never break up a merge group — M CODE rows for the
       same merge always stay side-by-side.  We rank each merge by the
       value shown on its SUB TOTAL row so the visible order matches
       what the user sees in that row (stock/pipe columns sum siblings;
       MOI columns use the true merge-level ratio; string columns use
       the first non-empty across siblings). */
    const mergeGroups = new Map();
    src.forEach(r => {
        if (!mergeGroups.has(r.merge_code)) mergeGroups.set(r.merge_code, []);
        mergeGroups.get(r.merge_code).push(r);
    });
    /* Build a summary object (mirrors what mergeSumRow does further
       down) for every merge group.  We sum stock / pipe / period
       demand fields respecting the merge_shared flag, then derive
       moh and moh_plus_max from the summed pieces so the ratio the
       Sub Total row displays IS the sort key. */
    const buildSummary = (rows) => {
        const sum = {
            merge_code: rows[0].merge_code,
            state_stock: {NSW:0,QLD:0,VIC:0,WA:0},
            state_pipe_parts: {NSW:{port:0,water:0,fac:0},QLD:{port:0,water:0,fac:0},VIC:{port:0,water:0,fac:0},WA:{port:0,water:0,fac:0}},
            state_3m: {NSW:0,QLD:0,VIC:0,WA:0},
            total_stock:0, total_all:0, total_3m:0,
            p_3m:0, avg_6m_old:0, avg_7_9m:0, avg_10_12m:0,
        };
        const sharedSeen = new Set();
        rows.forEach(r => {
            if (r.merge_shared) {
                if (sharedSeen.has(r.merge_code)) return;
                sharedSeen.add(r.merge_code);
            }
            STATES.forEach(s => {
                sum.state_stock[s] += r.state_stock[s] || 0;
                sum.state_3m[s]    += r.state_3m[s]    || 0;
                const pp = r.state_pipe_parts?.[s] || {port:0,water:0,fac:0};
                sum.state_pipe_parts[s].port  += pp.port  || 0;
                sum.state_pipe_parts[s].water += pp.water || 0;
                sum.state_pipe_parts[s].fac   += pp.fac   || 0;
            });
            sum.total_stock += r.total_stock || 0;
            sum.total_all   += r.total_all   || 0;
            sum.total_3m    += r.total_3m    || 0;
            sum.p_3m        += r.p_3m        || 0;
            sum.avg_6m_old  += r.avg_6m_old  || 0;
            sum.avg_7_9m    += r.avg_7_9m    || 0;
            sum.avg_10_12m  += r.avg_10_12m  || 0;
        });
        sum.moh = sum.total_3m > 0 ? sum.total_stock / sum.total_3m : null;
        const mergeMax = Math.max(sum.p_3m, sum.avg_6m_old, sum.avg_7_9m, sum.avg_10_12m, 0);
        sum.moh_plus_max = mergeMax > 0 ? sum.total_all / mergeMax : null;
        return sum;
    };
    if (sortCol && sortDir !== 0) {
        const dir = sortDir;
        /* Rank each merge by the SORT COLUMN's value on the merge's
           SUB TOTAL row (via sortKey against the summary object).
           String columns fall back to first non-empty across
           siblings since the summary has no string identity fields. */
        const groupKey = (rows) => {
            const summary = buildSummary(rows);
            const v = sortKey(summary, sortCol);
            if (typeof v === 'number' && !isNaN(v)) return v;
            /* String column — use the first non-empty across siblings. */
            const vals = rows.map(r => sortKey(r, sortCol));
            const nonEmpty = vals.find(x => x !== '' && x != null);
            return nonEmpty == null ? '' : nonEmpty;
        };
        const buckets = Array.from(mergeGroups.entries()).map(([mc, rows]) => ({
            mc, rows, key: groupKey(rows)
        }));
        buckets.sort((a, b) => {
            if (a.key < b.key) return -1 * dir;
            if (a.key > b.key) return  1 * dir;
            return 0;
        });
        mergeGroups.clear();
        buckets.forEach(b => mergeGroups.set(b.mc, b.rows));
        src = [].concat.apply([], buckets.map(b => b.rows));
    }
    /* Update sort arrows */
    document.querySelectorAll('#sku-tbl thead th').forEach(th => {
        th.classList.remove('sort-asc','sort-desc');
        if (th.dataset.col === sortCol) th.classList.add(sortDir === 1 ? 'sort-asc' : 'sort-desc');
    });

    /* State-cell factory — compact form (Stock + inline 3M) or the
       expanded 4-cell form (Stock / Port / Water / Factory).  A
       national TOTAL group is emitted separately below.  The FIRST
       cell of each state group gets `grp-start` so a vertical
       divider stripes the table. */
    const stateCells = r => {
        if (!showPipeline) {
            return STATES.map((s, i) =>
                '<td class="r' + (i === 0 ? ' grp-start' : '') + '">'
                + fmtI(r.state_stock[s]) + demSuffix(r.state_3m[s]) + '</td>'
            ).join('');
        }
        return STATES.map(s => {
            const pp = r.state_pipe_parts?.[s] || { port:0, water:0, fac:0 };
            return '<td class="r grp-start">' + fmtI(r.state_stock[s]) + '</td>'
                 + '<td class="r">'          + fmtI(pp.port)          + '</td>'
                 + '<td class="r">'          + fmtI(pp.water)         + '</td>'
                 + '<td class="r">'          + fmtI(pp.fac)           + '</td>';
        }).join('');
    };

    const body = document.getElementById('tbl-body');

    /* ── Total row (rendered at the top) ── */
    const nonStateCount = 10;   /* Merge / M CODE / Brand / Line /
                                   Product Name / Pattern / F/O·OPE
                                   / Size / Inch / LI·SS
                                   = 10 non-numeric cols */
    let totalRow = '<tr class="total-row"><td colspan="' + nonStateCount + '">TOTAL IN VIEW · '
                 + fmtI(src.length) + ' rows</td>';
    /* State cells for the Total row */
    if (showPipeline) {
        STATES.forEach((s, i) => {
            /* Every state's STK cell in the Total row gets grp-start
               too so the TOTAL IN VIEW summary shows the same
               state-group dividers as the rows below it. */
            totalRow += '<td class="r grp-start">' + fmtI(ttlStateStock[s]) + '</td>'
                     +  '<td class="r">' + fmtI(ttlPipe[s].port)  + '</td>'
                     +  '<td class="r">' + fmtI(ttlPipe[s].water) + '</td>'
                     +  '<td class="r">' + fmtI(ttlPipe[s].fac)   + '</td>';
        });
        const tp = STATES.reduce((s,k)=>s+ttlPipe[k].port,0);
        const tw = STATES.reduce((s,k)=>s+ttlPipe[k].water,0);
        const tf = STATES.reduce((s,k)=>s+ttlPipe[k].fac,0);
        totalRow += '<td class="r grp-start">' + fmtI(ttlStock) + '</td>'
                 +  '<td class="r">' + fmtI(tp) + '</td>'
                 +  '<td class="r">' + fmtI(tw) + '</td>'
                 +  '<td class="r">' + fmtI(tf) + '</td>';
    } else {
        STATES.forEach((s, i) => {
            totalRow += '<td class="r' + (i === 0 ? ' grp-start' : '') + '">'
                     + fmtI(ttlStateStock[s]) + demSuffix(ttlState3M[s]) + '</td>';
        });
        totalRow += '<td class="r grp-start">' + fmtI(ttlStock) + demSuffix(ttl3M) + '</td>';
    }
    totalRow += '<td class="r grp-start" style="' + moiBar(ttlMOI)    + '">' + (ttlMOI != null ? fmtF(ttlMOI, 1) : '—') + '</td>'
             +  '<td class="r grp-start" style="' + moiBarPPL(ttlPPL) + '">' + (ttlPPL != null ? fmtF(ttlPPL, 1) : '—') + '</td>'
             +  '<td class="r grp-start">' + fmtF(ttl3M,   1) + '</td>'
             +  '<td class="r no-div">'   + fmtF(ttl6,    1) + '</td>'
             +  '<td class="r no-div">'   + fmtF(ttl79,   1) + '</td>'
             +  '<td class="r no-div">'   + fmtF(ttl1012, 1) + '</td>'
             +  '</tr>';

    /* Sub Total row visibility rule: sorting now sorts merge GROUPS
       (rows within a merge stay together), so Sub Total rows stay
       useful in every sort orientation.  Always show them. */
    const showSubTotal = true;

    /* Palette for the F/O · OPE pill (per token).  Each bucket has
       its own soft ground + strong foreground; unrecognised tokens
       fall back to a neutral blue-grey.  OPE uses fresh green per
       user request — it's the most common non-Active tag and greens
       read as "planned outbound" rather than "problem". */
    const SKU_PILL = {
        'F/O':      { bg:'#E5E7EB', fg:'#4B5563' },
        'OPE':      { bg:'#DCFCE7', fg:'#15803D' },
        'OE A/S':   { bg:'#E3F2FD', fg:'#1565C0' },
        'M/S':      { bg:'#F3E5F5', fg:'#6A1B9A' },
        'Testing':  { bg:'#FFF9C4', fg:'#827717' },
        'Transfer': { bg:'#E0F2F1', fg:'#00695C' },
        'Price':    { bg:'#EDE7F6', fg:'#4527A0' }
    };
    const renderSkuPill = (skuStatus) => {
        const s = skuStatus || 'Active';
        if (s === 'Active') return '<span style="color:#78909C;font-size:10.5px">—</span>';
        return s.split(/\s*\+\s*/).map(t => {
            const c = SKU_PILL[t] || { bg:'#ECEFF1', fg:'#455A64' };
            return '<span style="display:inline-block;padding:1px 6px;'
                 + 'border-radius:8px;font-size:10px;font-weight:600;'
                 + 'white-space:nowrap;margin-right:2px;'
                 + 'background:' + c.bg + ';color:' + c.fg + '">' + t + '</span>';
        }).join('');
    };

    /* Numeric-cell factories — take a row (M CODE or Sub Total) and
       return the state / total / MOI / period-avg TD strings.  Both
       row types share the SAME merge-level figures; the visual
       difference is cell colour (blue for M CODE, black for Sub
       Total) which comes from tr.mc-row td.r vs tr.sub-total td. */
    const stateCellsFor = (r) => {
        if (showPipeline) {
            /* Pipeline mode: every state's FIRST sub-cell (STK) gets
               `grp-start` so a vertical divider draws BETWEEN state
               groups — NSW | QLD | VIC | WA — and the other three
               sub-cells (PRT / WTR / FAC) stay borderless inside the
               group.  Fixes the earlier bug where only NSW STK had
               the divider so the four state groups looked like one
               continuous stripe of numbers. */
            return STATES.map((s, i) => {
                const pp = r.state_pipe_parts?.[s] || { port:0, water:0, fac:0 };
                return '<td class="r grp-start">' + fmtI(r.state_stock[s]) + '</td>'
                     + '<td class="r">' + fmtI(pp.port)  + '</td>'
                     + '<td class="r">' + fmtI(pp.water) + '</td>'
                     + '<td class="r">' + fmtI(pp.fac)   + '</td>';
            }).join('');
        }
        return STATES.map((s, i) =>
            '<td class="r' + (i===0 ? ' grp-start' : '') + '">'
            + fmtI(r.state_stock[s]) + demSuffix(r.state_3m[s]) + '</td>'
        ).join('');
    };
    const totalGrpFor = (r) => {
        if (showPipeline) {
            const tp = totalPipe(r,'port'), tw = totalPipe(r,'water'), tf = totalPipe(r,'fac');
            return '<td class="r grp-start">' + fmtI(r.total_stock) + '</td>'
                 + '<td class="r">' + fmtI(tp) + '</td>'
                 + '<td class="r">' + fmtI(tw) + '</td>'
                 + '<td class="r">' + fmtI(tf) + '</td>';
        }
        return '<td class="r grp-start">' + fmtI(r.total_stock) + demSuffix(r.total_3m) + '</td>';
    };
    const moiCellsFor = (r, showBtn) => {
        const cls_mo = r.status === 'shortage' ? 'short'
                     : r.status === 'surplus'  ? 'sur'
                     : r.status === 'serious_surplus' ? 'ser' : '';
        // Small magnifier button that opens the drill-down modal.
        // Sits inside the MOI cell so row clicks can be reserved for
        // toggle-select (the user asked for that split).
        const detailBtn = showBtn
            ? '<button class="moi-detail-btn" data-open-mc="' + r.merge_code + '" '
              + 'title="Show 12-month per-state stack + per-M CODE breakdown">🔍</button>'
            : '';
        // Period averages render as ROUNDED INTEGERS on M CODE rows —
        // the user asked for individual per-material figures with
        // decimals rounded away.  Sub Total rows keep the full merge
        // sum with the same integer rounding.
        return '<td class="r grp-start ' + cls_mo + '" style="' + moiBar(r.moh)             + '">' + (r.moh          != null ? fmtF(r.moh, 1)          : '—') + detailBtn + '</td>'
             + '<td class="r grp-start ' + cls_mo + '" style="' + moiBarPPL(r.moh_plus_max) + '">' + (r.moh_plus_max != null ? fmtF(r.moh_plus_max, 1) : '—') + '</td>'
             + '<td class="r grp-start">' + fmtI(Math.round(r.p_3m       || 0)) + '</td>'
             + '<td class="r no-div">'   + fmtI(Math.round(r.avg_6m_old || 0)) + '</td>'
             + '<td class="r no-div">'   + fmtI(Math.round(r.avg_7_9m   || 0)) + '</td>'
             + '<td class="r no-div">'   + fmtI(Math.round(r.avg_10_12m || 0)) + '</td>';
    };
    /* Build a synthetic "merge total" row from a group of M CODE
       rows by summing the per-row figures.  Used to render the Sub
       Total row so its numbers are the actual merge sum, not just
       the first M CODE's individual figure. */
    const mergeSumRow = (groupRows) => {
        const sum = {
            merge_code: groupRows[0].merge_code,
            state_stock: {NSW:0,QLD:0,VIC:0,WA:0},
            state_pipe_parts: {NSW:{port:0,water:0,fac:0},QLD:{port:0,water:0,fac:0},VIC:{port:0,water:0,fac:0},WA:{port:0,water:0,fac:0}},
            state_3m: {NSW:0,QLD:0,VIC:0,WA:0},
            total_stock:0, total_all:0, total_3m:0,
            p_3m:0, avg_6m_old:0, avg_7_9m:0, avg_10_12m:0,
        };
        /* Merge-shared rows all carry the same total; take the first
           one and skip the rest.  Per-M CODE rows sum normally. */
        const sharedSeen = new Set();
        groupRows.forEach(r => {
            if (r.merge_shared) {
                if (sharedSeen.has(r.merge_code)) return;
                sharedSeen.add(r.merge_code);
            }
            STATES.forEach(s => {
                sum.state_stock[s] += r.state_stock[s] || 0;
                sum.state_3m[s]    += r.state_3m[s]    || 0;
                const pp = r.state_pipe_parts?.[s] || {port:0,water:0,fac:0};
                sum.state_pipe_parts[s].port  += pp.port  || 0;
                sum.state_pipe_parts[s].water += pp.water || 0;
                sum.state_pipe_parts[s].fac   += pp.fac   || 0;
            });
            sum.total_stock += r.total_stock || 0;
            sum.total_all   += r.total_all   || 0;
            sum.total_3m    += r.total_3m    || 0;
            sum.p_3m        += r.p_3m        || 0;
            sum.avg_6m_old  += r.avg_6m_old  || 0;
            sum.avg_7_9m    += r.avg_7_9m    || 0;
            sum.avg_10_12m  += r.avg_10_12m  || 0;
        });
        sum.moh          = sum.total_3m > 0 ? sum.total_stock / sum.total_3m : null;
        const mergeMax = Math.max(sum.p_3m, sum.avg_6m_old,
                                  sum.avg_7_9m, sum.avg_10_12m, 0);
        sum.moh_plus_max = mergeMax > 0 ? sum.total_all / mergeMax : null;
        // Sub Total's row-level status matches its own merge MOI —
        // used to colour the MOI cells.
        sum.status = (sum.total_3m === 0 && sum.total_stock === 0) ? 'empty'
                   : (sum.total_3m === 0 && sum.total_stock >  0) ? 'no_move'
                   : (sum.moh <= 1) ? 'shortage'
                   : (sum.moh <= 3) ? 'balanced'
                   : (sum.moh <= 6) ? 'surplus'
                   : 'serious_surplus';
        return sum;
    };

    /* Info-availability checks — used to render the ⚠ badge that
       tells the user which merges/rows the workbook itself is
       missing.  Product Name counts as info on its own since it's
       the human-readable identifier straight from the stock sheet.
       Per-row check triggers even when siblings in the same merge
       DO have data, so a partially-filled merge still flags its
       truly-empty M CODEs. */
    const rowHasInfo   = (r)   => !!(r.brand || r.line || r.size || r.pattern || r.product_name);
    const mergeHasInfo = (grp) => grp.some(rowHasInfo);
    /* Small compact badge for M CODEs whose own record is empty —
       makes each empty row instantly identifiable in a mixed merge. */
    const noDataBadge =
        '<span title="Sheet2에 이 M CODE에 대한 제품 정보가 없습니다" '
        + 'style="display:inline-block;margin-left:6px;padding:1px 6px;'
        + 'border-radius:8px;font-size:10px;font-weight:600;'
        + 'background:#FEF3C7;color:#92400E;white-space:nowrap">⚠ no data</span>';

    const rowsHtml = [];
    let mergesRendered = 0;
    const ROW_CAP = 800;

    /* Emission order depends on the sort mode.  In merge-natural
       order (or explicit merge_code sort) we walk merge groups and
       emit N M CODE rows + 1 Sub Total per merge.  In any other
       sort we simply emit the sorted `src` row-by-row (no sub
       totals). */
    if (showSubTotal) {
        for (const [mc, groupRows] of mergeGroups) {
            if (mergesRendered >= ROW_CAP) break;
            mergesRendered += groupRows.length;
            const band  = mergeBandColor(mc);
            const rep   = groupRows[0];
            const hasInfo = mergeHasInfo(groupRows);

            groupRows.forEach((r, idxInGroup) => {
                const isFirst = idxInGroup === 0;
                const classes = ['mc-row'];
                if (selected.has(r.merge_code)) classes.push('selected');
                if (isFirst)                     classes.push('merge-break');
                const bandStyle = ' style="border-left:4px solid ' + band + '"';
                /* Three cases:
                   1. Whole merge lacks info (`hasInfo` false) — one
                      wide banner span replaces the product block.
                   2. This row lacks info but siblings have it — we
                      still render 8 product cells so the alignment
                      stays stable; the M CODE gets a small ⚠ badge
                      so the user can see WHICH rows are missing.
                   3. Row has info — normal render. */
                const thisRowHasInfo = rowHasInfo(r);
                const mcodeCell = '<td>' + (r.m_code || '—')
                                + (thisRowHasInfo ? '' : noDataBadge) + '</td>';
                const productCells = hasInfo
                    ? ( '<td>' + (r.brand        || '—') + '</td>'
                      + '<td>' + (r.line         || '—') + '</td>'
                      + '<td>' + (r.product_name || '—') + '</td>'
                      + '<td>' + (r.pattern      || '—') + '</td>'
                      + '<td>' + renderSkuPill(r.sku_status) + '</td>'
                      + '<td>' + (r.size         || '—') + '</td>'
                      + '<td>' + (r.inch         || '—') + '</td>'
                      + '<td>' + (r.li ? r.li : '—') + (r.ss ? '/' + r.ss : '') + '</td>' )
                    : ( '<td class="no-info" colspan="8">'
                      + '⚠ 정보 없음 — Sheet2/Stock Sheet 어디에도 이 Merge Code의 제품 정보가 없습니다. '
                      + 'CS가 Sheet2에 Merge ' + r.merge_code + ' 마스터 레코드를 등록해야 채워집니다.'
                      + '</td>' );
                rowsHtml.push(
                    '<tr class="' + classes.join(' ') + '" data-mc="' + r.merge_code + '">'
                    + '<td' + bandStyle + '>' + r.merge_code + '</td>'
                    + mcodeCell
                    + productCells
                    + stateCellsFor(r) + totalGrpFor(r) + moiCellsFor(r, true)
                    + '</tr>');
            });

            /* Sub Total row — dedicated aggregate line under each
               merge group.  Uses `mergeSumRow` to sum the M CODE
               figures so the total reflects the actual merge (not
               just the first M CODE's individual numbers). */
            const subTotal = mergeSumRow(groupRows);
            rowsHtml.push(
                '<tr class="sub-total" data-mc="' + subTotal.merge_code + '">'
                + '<td style="border-left:4px solid ' + band
                +      ';font-weight:700;color:' + band + '" colspan="10">'
                + 'SUB TOTAL · Merge ' + subTotal.merge_code + '</td>'
                + stateCellsFor(subTotal) + totalGrpFor(subTotal) + moiCellsFor(subTotal, true)
                + '</tr>');
        }
    } else {
        /* Flat per-M-CODE listing (no sub totals).  Each row still
           carries the mc-row class so the numeric cells render blue.
           A `merge-break` accent draws a divider when the merge
           changes between adjacent rows so groups are still visible
           after a material-attribute sort. */
        let prev = null;
        for (const r of src) {
            if (mergesRendered >= ROW_CAP) break;
            mergesRendered++;
            const classes = ['mc-row'];
            if (selected.has(r.merge_code)) classes.push('selected');
            if (r.merge_code !== prev)      classes.push('merge-break');
            prev = r.merge_code;
            const band = mergeBandColor(r.merge_code);
            /* Per-row no-data badge — same rule as the merge-grouped
               path: if this specific M CODE has no product info, tag
               it inline so the empty row is instantly identifiable. */
            const thisRowHasInfo = rowHasInfo(r);
            const mcodeCell = '<td>' + (r.m_code || '—')
                            + (thisRowHasInfo ? '' : noDataBadge) + '</td>';
            rowsHtml.push(
                '<tr class="' + classes.join(' ') + '" data-mc="' + r.merge_code + '">'
                + '<td style="border-left:4px solid ' + band + '">' + r.merge_code + '</td>'
                + mcodeCell
                + '<td>' + (r.brand        || '—') + '</td>'
                + '<td>' + (r.line         || '—') + '</td>'
                + '<td>' + (r.product_name || '—') + '</td>'
                + '<td>' + (r.pattern      || '—') + '</td>'
                + '<td>' + renderSkuPill(r.sku_status) + '</td>'
                + '<td>' + (r.size         || '—') + '</td>'
                + '<td>' + (r.inch         || '—') + '</td>'
                + '<td>' + (r.li ? r.li : '—') + (r.ss ? '/' + r.ss : '') + '</td>'
                + stateCellsFor(r) + totalGrpFor(r) + moiCellsFor(r, true)
                + '</tr>');
        }
    }

    body.innerHTML = totalRow + rowsHtml.join('');
    const suffix = mergesRendered >= ROW_CAP
        ? ' — showing first ' + ROW_CAP + ' rows'
        : '';
    /* row-count element is optional — some layouts drop it.  Guard
       so a missing target doesn't throw and abort the render
       (which would leave the SKU table body wired without the
       magnifier click handlers attached below). */
    const rowCountEl = document.getElementById('row-count');
    if (rowCountEl) {
        rowCountEl.textContent =
            fmtI(src.length) + ' M-CODE rows across '
            + fmtI(mergeGroups.size) + ' merges' + suffix;
    }

    /* Row click behaviour split into two:
         • Anywhere on the row → toggle-select the merge.
         • The 🔍 button in the MOI cell → open the drill-down modal.
       The button uses `data-open-mc` and stops-propagation so a click
       on it doesn't also flip the row's selection state. */
    body.querySelectorAll('tr').forEach(tr => {
        tr.addEventListener('click', (e) => {
            /* Ignore clicks that originated on the detail button —
               those are handled by the delegated listener below. */
            if (e.target.closest('.moi-detail-btn')) return;
            const mc = +tr.dataset.mc;
            if (selected.has(mc)) { selected.delete(mc); tr.classList.remove('selected'); }
            else                  { selected.add(mc);    tr.classList.add('selected'); }
            updateSelectionSummary(src);
        });
    });
    body.querySelectorAll('.moi-detail-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const mc = +btn.dataset.openMc;
            if (!Number.isNaN(mc)) openModal(mc);
        });
    });
    /* Merge-group cohover — pointing at any row of a Merge tints
       every other row that carries the same data-mc so the wide
       table lets the reader trace groups visually.  We index rows
       by merge_code once here so each mouseenter is O(1). */
    const rowsByMerge = new Map();
    body.querySelectorAll('tr[data-mc]').forEach(tr => {
        const mc = tr.dataset.mc;
        if (!rowsByMerge.has(mc)) rowsByMerge.set(mc, []);
        rowsByMerge.get(mc).push(tr);
    });
    body.querySelectorAll('tr[data-mc]').forEach(tr => {
        tr.addEventListener('mouseenter', () => {
            const siblings = rowsByMerge.get(tr.dataset.mc) || [];
            siblings.forEach(x => { if (x !== tr) x.classList.add('merge-cohover'); });
        });
        tr.addEventListener('mouseleave', () => {
            const siblings = rowsByMerge.get(tr.dataset.mc) || [];
            siblings.forEach(x => x.classList.remove('merge-cohover'));
        });
    });
    updateSelectionSummary(src);
}

function updateSelectionSummary(currentList) {
    /* If nothing selected, show totals over the ENTIRE filtered set.
       If some rows are selected, show totals over the SELECTED subset. */
    const useSelected = selected.size > 0;
    const rows = useSelected
        ? currentList.filter(r => selected.has(r.merge_code))
        : currentList;
    let totStk = 0, tot3m = 0;
    rows.forEach(r => { totStk += r.total_stock; tot3m += r.total_3m; });
    document.getElementById('sum-rows').textContent  = fmtI(rows.length);
    document.getElementById('sum-stock').textContent = fmtI(totStk);
    document.getElementById('sum-3m').textContent    = fmtF(tot3m, 1);
    document.getElementById('sum-moi').textContent   = tot3m > 0 ? FMT_1.format(totStk / tot3m) : '—';
    document.getElementById('sel-info').textContent = useSelected
        ? (fmtI(rows.length) + ' rows selected — figures above cover this subset (click a row again to remove)')
        : 'no rows selected — click a row to add it to the subset · use the 🔍 button in the MOI column to open the detail modal';
    document.getElementById('sel-info').classList.toggle('none-selected', !useSelected);
    document.getElementById('clear-sel').style.display = useSelected ? 'inline-block' : 'none';
    /* Reflect the selection state on the download buttons so users
       know at a glance whether the export will contain everything
       filtered or just their picked subset. */
    const xlsxBtn = document.getElementById('btn-dl-xlsx');
    const csvBtn  = document.getElementById('btn-dl-csv');
    if (xlsxBtn) xlsxBtn.textContent = useSelected ? ('⬇ XLSX (' + selected.size + ')') : '⬇ XLSX';
    if (csvBtn)  csvBtn.textContent  = useSelected ? ('⬇ CSV ('  + selected.size + ')') : '⬇ CSV';
}
function clearSelection() {
    selected.clear();
    document.querySelectorAll('#sku-tbl tbody tr.selected').forEach(tr => tr.classList.remove('selected'));
    updateSelectionSummary(DATA[curTab + '_rows'].filter(rowPasses));
}

/* ── Expand SKU table ── */
function toggleExpandTable() {
    const on = document.body.classList.toggle('expand-table');
    const btn = document.getElementById('btn-expand-tbl');
    btn.innerHTML = on ? '✕' : '⛶';
    btn.title = on ? 'Return to split view' : 'Expand table full-screen';
}
document.addEventListener('keydown', (e) => {
    /* Escape hierarchy: modal wins first (its own handler further
       down), then expanded-table view, then selection.  Only clear
       selection when nothing "modal-ish" is on screen so users can
       chain Esc without wiping their subset. */
    if (e.key === 'Escape') {
        if (document.getElementById('modal-bg').classList.contains('open')) return;
        if (document.body.classList.contains('expand-table')) { toggleExpandTable(); return; }
        /* Only clear selection if focus isn't inside a text input —
           Escape should still cancel edits in the free-text search. */
        const inField = document.activeElement && /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement.tagName);
        if (!inField && selected.size > 0) { clearSelection(); return; }
    }
    /* Ctrl / Cmd + F → focus the free-text search box.  Prevents the
       browser's find-in-page from stealing the keystroke since our
       search covers merge / M CODE / brand / line / product / size /
       description / pattern in one filter. */
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'f') {
        const search = document.getElementById('fltr-search');
        if (search) {
            e.preventDefault();
            search.focus(); search.select();
        }
    }
});

/* ── Active-filter text row (mirrors the top filter bar as text) ── */
function renderFilterText() {
    const el = document.getElementById('filter-text');
    if (!el) return;
    const chips = [];
    Object.keys(filterState).forEach(key => {
        const set = filterState[key];
        if (set.size === 0) return;
        const label = KEY_LBL[key];
        const values = [...set].map(v => key === 'status' ? STATUS_PRETTY[v] : v);
        /* Cap really long lists so the chip stays readable — e.g.
           "Pattern: 12 selected" once you tick more than 5. */
        const shown = values.length <= 5
            ? values.join(', ')
            : (values.slice(0, 3).join(', ') + ' + ' + (values.length - 3) + ' more');
        chips.push('<span class="chip-txt"><span class="k">' + label + ':</span> ' + shown + '</span>');
    });
    const sq = (document.getElementById('fltr-search').value || '').trim();
    if (sq) chips.push('<span class="chip-txt"><span class="k">Search:</span> "' + sq + '"</span>');
    if (chips.length === 0) {
        el.innerHTML = '<span class="lbl">Filter</span><em>all SKUs</em>';
    } else {
        el.innerHTML = '<span class="lbl">Filter</span>' + chips.join(' ');
    }
}

/* Return the source rows the current view is drawing from, sorted +
   optionally narrowed to the current selection.  Both XLSX and CSV
   downloads use this so an exported file always matches what the
   user can see on screen (or the subset they explicitly picked). */
function exportSource() {
    const rawSrc = curTab === 'total' ? DATA.all_rows : DATA[curTab + '_rows'];
    let src = rawSrc.filter(rowPasses);
    if (selected.size > 0) {
        src = src.filter(r => selected.has(r.merge_code));
    }
    if (sortCol && sortDir !== 0) {
        /* Same merge-group aware, sub-total-anchored sort as the
           on-screen view — rows for the same merge always export
           together and buckets rank by the sortCol's value on the
           merge's SUB TOTAL row. */
        const dir = sortDir;
        const groups = new Map();
        src.forEach(r => {
            if (!groups.has(r.merge_code)) groups.set(r.merge_code, []);
            groups.get(r.merge_code).push(r);
        });
        const summaryOf = (rows) => {
            const sum = {
                merge_code: rows[0].merge_code,
                state_stock: {NSW:0,QLD:0,VIC:0,WA:0},
                state_pipe_parts: {NSW:{port:0,water:0,fac:0},QLD:{port:0,water:0,fac:0},VIC:{port:0,water:0,fac:0},WA:{port:0,water:0,fac:0}},
                state_3m: {NSW:0,QLD:0,VIC:0,WA:0},
                total_stock:0, total_all:0, total_3m:0,
                p_3m:0, avg_6m_old:0, avg_7_9m:0, avg_10_12m:0,
            };
            const sharedSeen = new Set();
            rows.forEach(r => {
                if (r.merge_shared) {
                    if (sharedSeen.has(r.merge_code)) return;
                    sharedSeen.add(r.merge_code);
                }
                ['NSW','QLD','VIC','WA'].forEach(s => {
                    sum.state_stock[s] += r.state_stock[s] || 0;
                    sum.state_3m[s]    += r.state_3m[s]    || 0;
                    const pp = r.state_pipe_parts?.[s] || {port:0,water:0,fac:0};
                    sum.state_pipe_parts[s].port  += pp.port  || 0;
                    sum.state_pipe_parts[s].water += pp.water || 0;
                    sum.state_pipe_parts[s].fac   += pp.fac   || 0;
                });
                sum.total_stock += r.total_stock || 0;
                sum.total_all   += r.total_all   || 0;
                sum.total_3m    += r.total_3m    || 0;
                sum.p_3m        += r.p_3m        || 0;
                sum.avg_6m_old  += r.avg_6m_old  || 0;
                sum.avg_7_9m    += r.avg_7_9m    || 0;
                sum.avg_10_12m  += r.avg_10_12m  || 0;
            });
            sum.moh = sum.total_3m > 0 ? sum.total_stock / sum.total_3m : null;
            const mm = Math.max(sum.p_3m, sum.avg_6m_old, sum.avg_7_9m, sum.avg_10_12m, 0);
            sum.moh_plus_max = mm > 0 ? sum.total_all / mm : null;
            return sum;
        };
        const groupKey = (rows) => {
            const v = sortKey(summaryOf(rows), sortCol);
            if (typeof v === 'number' && !isNaN(v)) return v;
            const vals = rows.map(r => sortKey(r, sortCol));
            const nonEmpty = vals.find(x => x !== '' && x != null);
            return nonEmpty == null ? '' : nonEmpty;
        };
        const buckets = Array.from(groups.entries()).map(([mc, rows]) => ({
            mc, rows, key: groupKey(rows)
        }));
        buckets.sort((a, b) => {
            if (a.key < b.key) return -1 * dir;
            if (a.key > b.key) return  1 * dir;
            return 0;
        });
        src = [].concat.apply([], buckets.map(b => b.rows));
    }
    return src;
}

/* ── CSV download (current filtered view) ── */
function downloadCSV() {
    /* Same source the visible table draws from — status tab + top
       filters + free-text search + current sort.  When rows are
       selected the export narrows to just those.  Values go into
       separate cells so Excel opens it cleanly. */
    const src = exportSource();
    const cols = [
        ['Merge',           r => r.merge_code],
        ['M CODE',          r => r.m_code || ''],
        ['Brand',           r => r.brand || ''],
        ['Marketing Line',  r => r.line || ''],
        ['Product Name',    r => r.product_name || ''],
        ['Pattern',         r => r.pattern || ''],
        ['Size',            r => r.size || ''],
        ['Inch',            r => r.inch || ''],
        ['LI',              r => r.li || ''],
        ['SS',              r => r.ss || ''],
        ['NSW Stock',       r => r.state_stock.NSW || 0],
        ['NSW 3M Avg',      r => (r.state_3m.NSW ?? 0).toFixed(2)],
        ['QLD Stock',       r => r.state_stock.QLD || 0],
        ['QLD 3M Avg',      r => (r.state_3m.QLD ?? 0).toFixed(2)],
        ['VIC Stock',       r => r.state_stock.VIC || 0],
        ['VIC 3M Avg',      r => (r.state_3m.VIC ?? 0).toFixed(2)],
        ['WA Stock',        r => r.state_stock.WA  || 0],
        ['WA 3M Avg',       r => (r.state_3m.WA  ?? 0).toFixed(2)],
        ['Total Stock',     r => r.total_stock || 0],
        ['Total 3M Avg',    r => (r.total_3m ?? 0).toFixed(2)],
        ['NSW Port',        r => r.state_pipe_parts?.NSW?.port  || 0],
        ['NSW Water',       r => r.state_pipe_parts?.NSW?.water || 0],
        ['NSW Factory',     r => r.state_pipe_parts?.NSW?.fac   || 0],
        ['QLD Port',        r => r.state_pipe_parts?.QLD?.port  || 0],
        ['QLD Water',       r => r.state_pipe_parts?.QLD?.water || 0],
        ['QLD Factory',     r => r.state_pipe_parts?.QLD?.fac   || 0],
        ['VIC Port',        r => r.state_pipe_parts?.VIC?.port  || 0],
        ['VIC Water',       r => r.state_pipe_parts?.VIC?.water || 0],
        ['VIC Factory',     r => r.state_pipe_parts?.VIC?.fac   || 0],
        ['WA Port',         r => r.state_pipe_parts?.WA?.port   || 0],
        ['WA Water',        r => r.state_pipe_parts?.WA?.water  || 0],
        ['WA Factory',      r => r.state_pipe_parts?.WA?.fac    || 0],
        ['MOI',                r => r.moh          != null ? r.moh.toFixed(2)          : ''],
        ['MOI(PPL)',           r => r.moh_plus_max != null ? r.moh_plus_max.toFixed(2) : ''],
        ['3M Avg (m -1..-3)',  r => (r.p_3m       ?? 0).toFixed(2)],
        ['4-6M Avg (m -4..-6)',r => (r.avg_6m_old ?? 0).toFixed(2)],
        ['7-9M Avg (m -7..-9)',r => (r.avg_7_9m   ?? 0).toFixed(2)],
        ['10-12M Avg (m -10..-12)', r => (r.avg_10_12m ?? 0).toFixed(2)],
        ['12M Avg (basis)',    r => (r.total_12m ?? 0).toFixed(2)],
        ['Max demand (basis)', r => (r.max_demand ?? 0).toFixed(2)],
        ['Status',          r => STATUS_PRETTY[r.status] || r.status || ''],
        ['Description',     r => r.description || ''],
    ];
    /* Proper CSV escaping so a description with a comma or quote
       doesn't split cells. */
    const esc = v => {
        const s = v == null ? '' : String(v);
        return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
    };
    const lines = [];
    lines.push(cols.map(c => esc(c[0])).join(','));
    src.forEach(r => lines.push(cols.map(c => esc(c[1](r))).join(',')));

    /* Include a first-line comment showing the filter state so the
       downloaded file has provenance — some SIEM tools object to a
       leading BOM, but Excel opens UTF-8-BOM cleanly with tildes and
       Korean characters. */
    const selSuffix = selected.size > 0 ? ' · selection only (' + selected.size + ' merges)' : '';
    const provenance = '# Stock Balance Lab · ' + curTab.replace('_',' ')
                     + ' · ' + new Date().toISOString().slice(0, 10)
                     + ' · Filter: ' + (filterSummary() || 'all SKUs')
                     + selSuffix;
    const csv = '﻿' + provenance + '\n' + lines.join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url;
    a.download = 'stock_balance_' + curTab
               + (selected.size > 0 ? '_selection' : '')
               + '_' + todayStr() + '.csv';
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(url), 5000);
    showToast('Downloaded ' + a.download + ' (' + fmtI(src.length) + ' rows)');
}

/* ── XLSX download (current filtered view) ──
   Uses the same column list as downloadCSV so downstream users see
   identical fields regardless of format.  Numbers are written as
   Excel-native numeric cells (not strings) so pivot tables and
   filters just work, and the header row gets a bold navy fill so
   the file looks report-ready without extra formatting. */
function downloadXLSX() {
    if (typeof XLSX === 'undefined') {
        showToast('XLSX library still loading — try again in a second');
        return;
    }
    const src = exportSource();
    /* Column list mirrors downloadCSV but returns NUMBERS as numbers
       (not toFixed strings) so Excel treats them as numeric cells. */
    const cols = [
        ['Merge',                  r => r.merge_code],
        ['M CODE',                 r => r.m_code || ''],
        ['Brand',                  r => r.brand || ''],
        ['Marketing Line',         r => r.line || ''],
        ['Product Name',           r => r.product_name || ''],
        ['Pattern',                r => r.pattern || ''],
        ['Size',                   r => r.size || ''],
        ['Inch',                   r => r.inch || ''],
        ['LI',                     r => r.li || ''],
        ['SS',                     r => r.ss || ''],
        ['F/O · OPE',              r => r.sku_status || 'Active'],
        ['NSW Stock',              r => r.state_stock.NSW || 0],
        ['NSW 3M Avg',             r => r.state_3m.NSW || 0],
        ['QLD Stock',              r => r.state_stock.QLD || 0],
        ['QLD 3M Avg',             r => r.state_3m.QLD || 0],
        ['VIC Stock',              r => r.state_stock.VIC || 0],
        ['VIC 3M Avg',             r => r.state_3m.VIC || 0],
        ['WA Stock',               r => r.state_stock.WA  || 0],
        ['WA 3M Avg',              r => r.state_3m.WA  || 0],
        ['Total Stock',            r => r.total_stock || 0],
        ['Total 3M Avg',           r => r.total_3m || 0],
        ['NSW Port',               r => r.state_pipe_parts?.NSW?.port  || 0],
        ['NSW Water',              r => r.state_pipe_parts?.NSW?.water || 0],
        ['NSW Factory',            r => r.state_pipe_parts?.NSW?.fac   || 0],
        ['QLD Port',               r => r.state_pipe_parts?.QLD?.port  || 0],
        ['QLD Water',              r => r.state_pipe_parts?.QLD?.water || 0],
        ['QLD Factory',            r => r.state_pipe_parts?.QLD?.fac   || 0],
        ['VIC Port',               r => r.state_pipe_parts?.VIC?.port  || 0],
        ['VIC Water',              r => r.state_pipe_parts?.VIC?.water || 0],
        ['VIC Factory',            r => r.state_pipe_parts?.VIC?.fac   || 0],
        ['WA Port',                r => r.state_pipe_parts?.WA?.port   || 0],
        ['WA Water',               r => r.state_pipe_parts?.WA?.water  || 0],
        ['WA Factory',             r => r.state_pipe_parts?.WA?.fac    || 0],
        ['MOI',                    r => r.moh          == null ? null : r.moh],
        ['MOI(PPL)',               r => r.moh_plus_max == null ? null : r.moh_plus_max],
        ['3M Avg (m -1..-3)',      r => r.p_3m       || 0],
        ['4-6M Avg (m -4..-6)',    r => r.avg_6m_old || 0],
        ['7-9M Avg (m -7..-9)',    r => r.avg_7_9m   || 0],
        ['10-12M Avg (m -10..-12)',r => r.avg_10_12m || 0],
        ['12M Avg (basis)',        r => r.total_12m  || 0],
        ['Max demand (basis)',     r => r.max_demand || 0],
        ['Status',                 r => STATUS_PRETTY[r.status] || r.status || ''],
        ['Description',            r => r.description || ''],
    ];
    /* Build the sheet as an array-of-arrays; row 1 is the header. */
    const aoa = [cols.map(c => c[0])];
    src.forEach(r => aoa.push(cols.map(c => c[1](r))));
    const ws = XLSX.utils.aoa_to_sheet(aoa);
    /* Style header row bold + freeze it. */
    const range = XLSX.utils.decode_range(ws['!ref']);
    for (let c = range.s.c; c <= range.e.c; c++) {
        const addr = XLSX.utils.encode_cell({r: 0, c});
        if (ws[addr]) ws[addr].s = { font: { bold: true } };
    }
    ws['!freeze'] = { xSplit: 0, ySplit: 1 };
    /* Reasonable column widths — 12 for numeric, 26 for description. */
    ws['!cols'] = cols.map(([name]) =>
        ({ wch: name === 'Description' ? 26 : name.length > 12 ? 16 : 12 }));
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, curTab === 'total' ? 'All' : curTab);
    /* Data-provenance sheet: filter, tab, timestamp — a downstream
       viewer can trace what generated the file. */
    const provWs = XLSX.utils.aoa_to_sheet([
        ['Property',        'Value'],
        ['Generated',       new Date().toISOString()],
        ['Tab',             curTab],
        ['Filter',          filterSummary() || 'all SKUs'],
        ['Selection',       selected.size > 0 ? (selected.size + ' merges — selection only') : 'all filtered rows'],
        ['Rows',            src.length],
        ['Data as of',      (window.META && META.data_date) || ''],
        ['Source workbook', (window.META && META.path) || ''],
    ]);
    provWs['!cols'] = [{ wch: 20 }, { wch: 60 }];
    XLSX.utils.book_append_sheet(wb, provWs, 'Meta');
    const fname = 'stock_balance_' + curTab
               + (selected.size > 0 ? '_selection' : '')
               + '_' + todayStr() + '.xlsx';
    XLSX.writeFile(wb, fname);
    showToast('Downloaded ' + fname + ' (' + fmtI(src.length) + ' rows)');
}

/* ── Central refresh ── */
function refresh() {
    recomputeKPI();
    renderStateCards();
    renderCharts();
    renderFilterText();
    renderTable();
}
refresh();

/* ── Drill-down modal ── */
let _modalChart = null;
const MONTH_LABELS = ['-12M','-11M','-10M','-9M','-8M','-7M','-6M','-5M','-4M','-3M','-2M','-1M'];
/* Every M CODE row in this merge (used for the per-M-CODE breakdown
   table below the main aggregate). */
function siblingRows(mc) {
    return DATA.all_rows.filter(r => r.merge_code === mc);
}
function findRow(mc) { return DATA.all_rows.find(r => r.merge_code === mc); }

/* Sum a set of M CODE rows into a synthetic "merge aggregate" row so
   the modal's headline stats reflect the whole Merge (not just the
   first M CODE that matches).  Keeps the modal source of truth in
   sync with the Sub Total line rendered on the main table. */
function aggregateMerge(mcRows) {
    if (!mcRows || !mcRows.length) return null;
    /* Pick the first M CODE that actually has product info as the
       "rep" for identity fields, not just mcRows[0].  A merge where
       the first sibling is bare but the second is populated used
       to show a mostly-empty modal header. */
    const rep = mcRows.find(x => x.brand || x.line || x.size || x.pattern || x.product_name) || mcRows[0];
    const sum = {
        merge_code: rep.merge_code,
        brand: rep.brand, line: rep.line, product_name: rep.product_name,
        pattern: rep.pattern, size: rep.size, li: rep.li, ss: rep.ss,
        group: rep.group,
        state_stock: {NSW:0,QLD:0,VIC:0,WA:0},
        state_pipeline: {NSW:0,QLD:0,VIC:0,WA:0},
        state_pipe_parts: {NSW:{port:0,water:0,fac:0},QLD:{port:0,water:0,fac:0},VIC:{port:0,water:0,fac:0},WA:{port:0,water:0,fac:0}},
        state_3m: {NSW:0,QLD:0,VIC:0,WA:0},
        history: {NSW: new Array(12).fill(0), QLD: new Array(12).fill(0),
                  VIC: new Array(12).fill(0), WA:  new Array(12).fill(0),
                  TOTAL: new Array(12).fill(0)},
        total_stock:0, total_all:0, total_3m:0, max_demand:0,
    };
    mcRows.forEach(r => {
        STATES.forEach(s => {
            sum.state_stock[s]    += r.state_stock[s]    || 0;
            sum.state_pipeline[s] += r.state_pipeline[s] || 0;
            sum.state_3m[s]       += r.state_3m[s]       || 0;
            const pp = r.state_pipe_parts?.[s] || {port:0,water:0,fac:0};
            sum.state_pipe_parts[s].port  += pp.port  || 0;
            sum.state_pipe_parts[s].water += pp.water || 0;
            sum.state_pipe_parts[s].fac   += pp.fac   || 0;
            for (let i = 0; i < 12; i++) sum.history[s][i] += r.history[s]?.[i] || 0;
        });
        for (let i = 0; i < 12; i++) sum.history.TOTAL[i] += r.history.TOTAL?.[i] || 0;
        sum.total_stock += r.total_stock || 0;
        sum.total_all   += r.total_all   || 0;
        sum.total_3m    += r.total_3m    || 0;
        sum.max_demand   = Math.max(sum.max_demand, r.max_demand || 0);
    });
    sum.moh          = sum.total_3m > 0 ? sum.total_stock / sum.total_3m : null;
    sum.moh_plus     = sum.total_3m > 0 ? sum.total_all   / sum.total_3m : null;
    sum.moh_plus_max = sum.max_demand > 0 ? sum.total_all / sum.max_demand : null;
    return sum;
}

function openModal(mergeCode) {
    const mcRows = siblingRows(mergeCode);
    const r = aggregateMerge(mcRows) || findRow(mergeCode);
    if (!r) return;
    /* Prefer the specific Product Name (e.g. "Ventus TD") over the
       umbrella Marketing Line for the modal title.  When both are
       present we show line + product name; otherwise just whichever
       one exists. */
    const pnRep = mcRows.map(x => x.product_name).find(x => x) || r.product_name || '';
    const lineRep = r.line || 'Other';
    const idParts = pnRep && pnRep !== lineRep ? [lineRep, pnRep] : [lineRep];
    document.getElementById('m-title').textContent =
        (r.brand || '—') + ' · ' + idParts.join(' · ') + ' · ' + (r.pattern || '—')
        + '  ·  ' + (r.size || '') + '  ·  LI/SS ' + (r.li||'—') + '/' + (r.ss||'—');
    document.getElementById('m-sub').textContent =
        'Merge ' + r.merge_code + ' · ' + mcRows.length + ' M CODE'
        + (mcRows.length === 1 ? '' : 's') + ' inside';
    document.getElementById('m-stock').innerHTML = fmtI(r.total_stock) + '<span class="u">units</span>';
    const pipe = STATES.reduce((s, k) => s + (r.state_pipeline[k] || 0), 0);
    document.getElementById('m-pipe').innerHTML  = fmtI(pipe) + '<span class="u">units on the way</span>';
    document.getElementById('m-3m').innerHTML    = fmtF(r.total_3m, 1) + '<span class="u">units / mo</span>';
    const mohClass = r.moh == null ? '' : r.moh <= 1 ? 'short' : r.moh <= 3 ? 'bal' : r.moh <= 6 ? 'sur' : 'ser';
    document.getElementById('m-moi').className   = 'figv ' + mohClass;
    document.getElementById('m-moi').innerHTML   = (r.moh != null ? fmtF(r.moh, 1) : '—') + '<span class="u">months</span>';
    document.getElementById('m-moiplus').innerHTML = (r.moh_plus_max != null ? fmtF(r.moh_plus_max, 1) : '—') + '<span class="u">months  ·  demand basis = ' + fmtF(r.max_demand, 1) + ' / mo</span>';

    /* 12-month per-state STACKED BAR (per user request) — one bar per
       month, with the four states stacked so the reader can see the
       state mix at a glance instead of four overlapping lines. */
    if (_modalChart) { _modalChart.destroy(); _modalChart = null; }
    const stateColour = { NSW: '#1976D2', QLD: '#EF6C00', VIC: '#8E24AA', WA: '#00897B' };
    const datasets = STATES.map(s => ({
        label: s, data: r.history[s],
        backgroundColor: stateColour[s], borderColor: stateColour[s],
        borderWidth: 0, stack: 'monthly',
    }));
    /* Chart.js may fail to load on restricted networks / air-gapped
       installs.  Wrap the constructor so a missing library never
       kills the modal — the numeric tables below still render and
       the user gets a graceful fallback message on the canvas. */
    try {
        if (typeof Chart === 'undefined') throw new Error('Chart.js not loaded');
        _modalChart = new Chart(document.getElementById('m-chart'), {
            type: 'bar',
            data: { labels: MONTH_LABELS, datasets },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'top', labels: { boxWidth: 14, font: { size: 11 } } },
                    tooltip: {
                        callbacks: {
                            footer: (items) => {
                                const sum = items.reduce((s, it) => s + it.parsed.y, 0);
                                return 'Total: ' + FMT_INT.format(Math.round(sum));
                            }
                        }
                    }
                },
                scales: {
                    y: { beginAtZero: true, stacked: true, ticks: { font: { size: 10 } } },
                    x: { stacked: true, ticks: { font: { size: 10 } } }
                }
            }
        });
    } catch (err) {
        console.warn('modal chart skipped:', err);
        const cvs = document.getElementById('m-chart');
        if (cvs && cvs.parentElement) {
            cvs.parentElement.innerHTML =
                '<div style="padding:24px;color:#94A3B8;font-size:12px;text-align:center">'
              + 'Chart library unavailable — numeric breakdown below.</div>';
        }
    }

    const rows = STATES.map(s => {
        const pp = r.state_pipe_parts[s], stock = r.state_stock[s];
        const p  = pp.port + pp.water + pp.fac, dem = r.state_3m[s];
        const smoh = dem > 0 ? (stock / dem) : null;
        const spmoh = dem > 0 ? ((stock + p) / dem) : null;
        const cls = smoh == null ? '' : smoh <= 1 ? 'short' : smoh <= 3 ? '' : smoh <= 6 ? 'sur' : 'ser';
        return '<tr>'
            + '<td class="st">' + s + '</td>'
            + '<td>' + fmtI(stock) + '</td>'
            + '<td>' + fmtI(pp.port) + '</td>'
            + '<td>' + fmtI(pp.water) + '</td>'
            + '<td>' + fmtI(pp.fac) + '</td>'
            + '<td>' + fmtI(p) + '</td>'
            + '<td>' + fmtF(dem, 1) + '</td>'
            + '<td class="' + cls + '">' + (smoh != null ? fmtF(smoh, 1) : '—') + '</td>'
            + '<td>' + (spmoh != null ? fmtF(spmoh, 1) : '—') + '</td>'
            + '</tr>';
    }).join('');
    const totStock = STATES.reduce((s,k) => s + r.state_stock[k], 0);
    const totPort  = STATES.reduce((s,k) => s + r.state_pipe_parts[k].port, 0);
    const totWater = STATES.reduce((s,k) => s + r.state_pipe_parts[k].water, 0);
    const totFac   = STATES.reduce((s,k) => s + r.state_pipe_parts[k].fac, 0);
    const totPipe  = totPort + totWater + totFac;
    document.getElementById('m-pipe-tbl').innerHTML = rows
      + '<tr class="tot"><td class="st">Total</td><td>' + fmtI(totStock)
      + '</td><td>' + fmtI(totPort) + '</td><td>' + fmtI(totWater)
      + '</td><td>' + fmtI(totFac)  + '</td><td>' + fmtI(totPipe)
      + '</td><td>' + fmtF(r.total_3m, 1) + '</td>'
      + '<td>' + (r.moh != null ? fmtF(r.moh, 1) : '—') + '</td>'
      + '<td>' + (r.moh_plus != null ? fmtF(r.moh_plus, 1) : '—') + '</td></tr>';

    /* Per-M-CODE breakdown — one row per material inside this Merge.
       Product-info columns come from the row itself; sales figures
       round to integer to match the main-table treatment. */
    const mcRowsHtml = mcRows.map(mc => {
        const mohCls = mc.moh == null ? '' :
            mc.moh <= 1 ? 'short' : mc.moh <= 3 ? '' : mc.moh <= 6 ? 'sur' : 'ser';
        const productBits = [mc.brand, mc.product_name || mc.line, mc.pattern, mc.size]
            .filter(x => x).join(' · ') || '—';
        return '<tr>'
            + '<td>' + (mc.m_code || '—') + '</td>'
            + '<td style="text-align:left;font-size:11px">' + escapeHtml(productBits) + '</td>'
            + '<td>' + fmtI(mc.total_stock)                + '</td>'
            + '<td>' + fmtI(Math.round(mc.p_3m       || 0))+ '</td>'
            + '<td>' + fmtI(Math.round(mc.avg_6m_old || 0))+ '</td>'
            + '<td>' + fmtI(Math.round(mc.avg_7_9m   || 0))+ '</td>'
            + '<td>' + fmtI(Math.round(mc.avg_10_12m || 0))+ '</td>'
            + '<td class="' + mohCls + '">' + (mc.moh != null ? fmtF(mc.moh, 1) : '—') + '</td>'
            + '</tr>';
    }).join('');
    document.getElementById('m-mcode-tbl').innerHTML = mcRowsHtml
      + '<tr class="tot"><td>—</td><td style="text-align:left;font-weight:700">Merge total</td>'
      + '<td>' + fmtI(r.total_stock)          + '</td>'
      + '<td>' + fmtI(Math.round(mcRows.reduce((s,x)=>s+(x.p_3m       ||0),0))) + '</td>'
      + '<td>' + fmtI(Math.round(mcRows.reduce((s,x)=>s+(x.avg_6m_old ||0),0))) + '</td>'
      + '<td>' + fmtI(Math.round(mcRows.reduce((s,x)=>s+(x.avg_7_9m   ||0),0))) + '</td>'
      + '<td>' + fmtI(Math.round(mcRows.reduce((s,x)=>s+(x.avg_10_12m ||0),0))) + '</td>'
      + '<td>' + (r.moh != null ? fmtF(r.moh, 1) : '—') + '</td></tr>';

    document.getElementById('modal-bg').classList.add('open');
}

function escapeHtml(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g,
        ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
}
function closeModal() {
    document.getElementById('modal-bg').classList.remove('open');
    if (_modalChart) { _modalChart.destroy(); _modalChart = null; }
}
document.getElementById('modal-bg').addEventListener('click', (e) => {
    if (e.target.id === 'modal-bg') closeModal();
});
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && document.getElementById('modal-bg').classList.contains('open')) closeModal();
});

/* ── Email-capture flow ── */
function todayStr() {
    const d = new Date();
    return d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');
}
function filterSummary() {
    const parts = [];
    Object.keys(filterState).forEach(key => {
        if (filterState[key].size) {
            const label = KEY_LBL[key];
            const vals = [...filterState[key]].map(v => key === 'status' ? STATUS_PRETTY[v] : v).join(', ');
            parts.push(label + ': ' + vals);
        }
    });
    const sq = (document.getElementById('fltr-search').value || '').trim();
    if (sq) parts.push('Search: "' + sq + '"');
    return parts.length ? parts.join(' · ') : 'all SKUs';
}
function showToast(msg) {
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.classList.add('show');
    setTimeout(() => t.classList.remove('show'), 2600);
}

/* Standard Hankook AU signature block, rendered inside the HTML
   body of the outgoing message.  No drag-and-drop text — the PNG is
   already attached to the .eml file the user opens. */
const HKAU_SIGNATURE = ''
  + '<div style="margin-top:24px;padding-top:12px;border-top:1px solid #E2E8F0;'
  +      'font-family:Arial,sans-serif;font-size:12px;color:#334155">'
  + '<div style="font-weight:600;color:#0E3F5F">Hankook Australia · Sales Strategy & Analytics</div>'
  + '<div style="color:#64748B;margin-top:2px">'
  +     'Stock Balance Lab · auto-generated snapshot'
  + '</div>'
  + '</div>';

/* Convert an ArrayBuffer to base64 in ~64-char lines (RFC 2045). */
function _b64Lines(u8) {
    let b = '';
    for (let i = 0; i < u8.length; i++) b += String.fromCharCode(u8[i]);
    const b64 = btoa(b);
    return b64.match(/.{1,76}/g).join('\r\n');
}

async function emailScreen(target, panelName) {
    /* Determine which DOM node to capture. */
    const node = target === 'page'
        ? document.body
        : target === 'state-card'
            ? document.querySelector('.state-col .card')
            : document.getElementById(target);
    if (!node) { showToast('Could not find that panel to capture.'); return; }
    showToast('Capturing screen…');
    try {
        const canvas = await html2canvas(node, {
            scale: 1.4, backgroundColor: '#F4F6F9', useCORS: true, logging: false,
        });
        /* Build a .eml file — MIME multipart/mixed with the PNG
           already attached and the signature embedded in the body.
           Outlook / Apple Mail / Thunderbird open .eml as a new
           draft with attachments intact, so the user just double-
           clicks the file and their compose window opens ready to
           send.  No drag-and-drop needed. */
        const pngBlob = await new Promise(res => canvas.toBlob(res, 'image/png'));
        const pngBuf  = new Uint8Array(await pngBlob.arrayBuffer());
        const pngB64  = _b64Lines(pngBuf);
        const stem    = panelName.replace(/[^A-Za-z0-9]+/g, '_');
        const pngName = 'stock_balance_' + stem + '_' + todayStr() + '.png';
        const emlName = 'stock_balance_' + stem + '_' + todayStr() + '.eml';
        const subject = '[Hankook AU · Stock Balance Lab] '
                      + panelName + ' — ' + todayStr()
                      + ' — ' + filterSummary();
        const meta = (window.META || {});
        const htmlBody = ''
            + '<html><body style="font-family:Arial,sans-serif;font-size:13px;color:#111827">'
            + '<p>Snapshot of the Stock Balance Lab — <b>' + panelName + '</b>.</p>'
            + '<p><b>Filter view:</b> ' + filterSummary() + '<br>'
            + '<b>Data as of:</b> ' + (meta.data_date || '')
            + ' &nbsp;·&nbsp; <b>Source workbook:</b> ' + (meta.path || '') + '</p>'
            + '<p>The dashboard snapshot is attached as <b>' + pngName + '</b>.</p>'
            + HKAU_SIGNATURE
            + '</body></html>';
        const boundary = '=_HKAU_' + Math.random().toString(36).slice(2, 12);
        /* RFC 5322 date-time, e.g. Mon, 15 Sep 2026 09:41:07 +1000 */
        const now = new Date();
        const rfcDate = now.toUTCString();
        const eml = ''
            + 'From: "Stock Balance Lab" <no-reply@localhost>\r\n'
            + 'To: \r\n'
            + 'Subject: ' + subject + '\r\n'
            + 'Date: ' + rfcDate + '\r\n'
            + 'MIME-Version: 1.0\r\n'
            + 'X-Unsent: 1\r\n'
            + 'Content-Type: multipart/mixed; boundary="' + boundary + '"\r\n'
            + '\r\n'
            + '--' + boundary + '\r\n'
            + 'Content-Type: text/html; charset="UTF-8"\r\n'
            + 'Content-Transfer-Encoding: 7bit\r\n'
            + '\r\n'
            + htmlBody + '\r\n'
            + '\r\n'
            + '--' + boundary + '\r\n'
            + 'Content-Type: image/png; name="' + pngName + '"\r\n'
            + 'Content-Transfer-Encoding: base64\r\n'
            + 'Content-Disposition: attachment; filename="' + pngName + '"\r\n'
            + '\r\n'
            + pngB64 + '\r\n'
            + '\r\n'
            + '--' + boundary + '--\r\n';
        const blob = new Blob([eml], { type: 'message/rfc822' });
        const url  = URL.createObjectURL(blob);
        const a    = document.createElement('a');
        a.href = url; a.download = emlName;
        document.body.appendChild(a); a.click(); document.body.removeChild(a);
        setTimeout(() => URL.revokeObjectURL(url), 5000);
        showToast('Downloaded ' + emlName + ' — double-click to open in Outlook');
    } catch (err) {
        console.error(err);
        showToast('Could not capture the screen: ' + err.message);
    }
}
</script>
</body>
</html>
"""
