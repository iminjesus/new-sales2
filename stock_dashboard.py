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
HIST_COL_START = 4      # -12M NSW column
HIST_BLOCK_LEN = 5      # NSW, QLD, VIC, WA, TOTAL per month


# ── Marketing line (Group in the user's vernacular) ──────────────
# Derived from the pattern code.  Hankook / Laufenn use a stable
# letter-prefix system that maps to a marketing line:
#   K7xx / K4xx / K3xx / KH..     → Kinergy    (touring / comfort)
#   K1xx (K107/K115/K117/K120…)   → Ventus     (UHP / performance)
#   RA / RF / RH / RT             → Dynapro    (SUV / LT / MT)
#   LH / LK / LS / LI (Laufenn)   → Laufenn G/S/X/I Fit
#   W (Winter)                    → Winter i*cept
#   AH / AL / AM / DH / DL / TH   → TBR / Truck (Smart / e-cube …)
#   Z / older H4xx / RH0x         → Optimo / legacy
# This is intentionally coarse — good enough to slice the shortage
# vs surplus board.  Falls back to "Other" so the column never blanks.
_LINE_RULES = [
    (r"^LH|^LK|^LS|^LI",         "Laufenn G/S/X/I Fit"),
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
                "Laufenn G/S/X/I Fit", "Optimo (legacy)"):
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


def _latest_stock_xlsm():
    """Glob the project directory for Stock_report_*.xlsm and return the
    newest by mtime.  Falls back to any *.xlsm containing 'stock' in
    the name so a user-renamed copy still works."""
    hits = sorted(glob.glob(os.path.join(_BASE, "Stock_report_*.xlsm")),
                  key=os.path.getmtime, reverse=True)
    if hits:
        return hits[0]
    fallback = sorted(glob.glob(os.path.join(_BASE, "*[Ss]tock*.xls[mx]")),
                      key=os.path.getmtime, reverse=True)
    return fallback[0] if fallback else None


_cache = {"path": None, "mtime": 0, "rows": None, "meta": None}


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

    # MM sheet: CODE → Merge Code map
    mc_to_mcode = {}
    if "MM" in wb.sheetnames:
        ws = wb["MM"]
        for i, r in enumerate(ws.iter_rows(min_row=2, values_only=True)):
            if not r or r[0] is None or r[1] is None:
                continue
            code, merge = r[0], r[1]
            if isinstance(merge, (int, float)):
                mc_to_mcode.setdefault(int(merge), int(code) if isinstance(code, (int, float)) else code)

    # Sheet2 SKU master: M CODE → detail dict
    m_master = {}
    if "Sheet2" in wb.sheetnames:
        ws = wb["Sheet2"]
        rows = ws.iter_rows(min_row=1, values_only=True)
        header = next(rows, None) or ()
        idx = {name: i for i, name in enumerate(header) if isinstance(name, str)}
        for r in rows:
            if not r or r[0] is None:
                continue
            m = r[0]
            try:
                m = int(m)
            except Exception:
                continue
            m_master[m] = {
                "description": r[idx["Description"]] if "Description" in idx else "",
                "group":       r[idx["Group"]]       if "Group"       in idx else "",
                "brand":       r[idx["Brand"]]       if "Brand"       in idx else "",
                "li":          r[idx["LI"]]          if "LI"          in idx else "",
                "ss":          r[idx["SS"]]          if "SS"          in idx else "",
                "ply":         r[idx["PLY"]]         if "PLY"         in idx else "",
                "sw":          r[idx["S.W"]]         if "S.W"         in idx else "",
                "sr":          r[idx["SR"]]          if "SR"          in idx else "",
                "inch":        r[idx["Inch"]]        if "Inch"        in idx else "",
                "factory":     r[idx["Factory"]]     if "Factory"     in idx else "",
                "origin":      r[idx["Origin"]]      if "Origin"      in idx else "",
                "au":          r[idx["AU"]]          if "AU"          in idx else "",
            }

    # Stock Status Worksheet — main data
    ws = wb["Stock Status Worksheet"]
    rows_out = []
    for r in ws.iter_rows(min_row=3, values_only=True):
        if not r:
            continue
        mc = r[COL_MERGE_CODE - 1]
        if mc is None:
            continue
        try:
            mc = int(mc)
        except Exception:
            continue
        group   = r[COL_GROUP - 1]
        classif = r[COL_CLASSIF - 1]

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

        state_stock = {s: _num(r[COL_STATE_STOCK[s] - 1]) for s in STATES}
        # Pipeline decomposed so the drill-down can show Port/Water/Factory
        # separately.  Order = (PORT, WATER, FAC) per COL_STATE_PIPELINE.
        state_pipe_parts = {
            s: {
                "port":  _num(r[COL_STATE_PIPELINE[s][0] - 1]),
                "water": _num(r[COL_STATE_PIPELINE[s][1] - 1]),
                "fac":   _num(r[COL_STATE_PIPELINE[s][2] - 1]),
            } for s in STATES
        }
        state_pipe = {s: sum(state_pipe_parts[s].values()) for s in STATES}
        state_3m   = {s: _num(r[COL_STATE_3M[s] - 1]) for s in STATES}
        total_stock = _num(r[COL_TOTAL_STOCK - 1])
        total_all   = _num(r[COL_TOTAL_TOTAL - 1])
        total_3m    = _num(r[COL_TOTAL_3M - 1])

        # 12-month sales history per state — used by the drill-down modal
        # to draw a per-state line chart.  Stored as [-12M … -1M] so the
        # front-end can plot in chronological order.
        history = {"NSW": [], "QLD": [], "VIC": [], "WA": [], "TOTAL": []}
        for m in range(HIST_MONTHS):
            base = HIST_COL_START + m * HIST_BLOCK_LEN
            history["NSW"].append(_num(r[base - 1]))
            history["QLD"].append(_num(r[base + 0]))
            history["VIC"].append(_num(r[base + 1]))
            history["WA"].append(_num(r[base + 2]))
            history["TOTAL"].append(_num(r[base + 3]))

        # Enrich via MM → Sheet2
        mcode = mc_to_mcode.get(mc)
        detail = m_master.get(mcode, {}) if mcode else {}

        # Size string: try "SW/SR R Inch" (205/55R16)
        size = ""
        if detail.get("sw") and detail.get("sr") and detail.get("inch"):
            try:
                sw = int(detail["sw"])
                sr = int(detail["sr"])
                inch = detail["inch"]
                # Inch may be float like 24.5 (truck)
                inch_s = str(inch).rstrip('0').rstrip('.') if isinstance(inch, float) else str(inch)
                size = f"{sw}/{sr}R{inch_s}"
            except Exception:
                pass

        # Status bucket (MOI = months of inventory)
        moh = (total_stock / total_3m) if total_3m > 0 else None
        if total_3m == 0 and total_stock == 0:
            status = "empty"
        elif total_3m == 0 and total_stock > 0:
            status = "no_move"
        elif moh is not None and moh <= STATUS_SHORTAGE_MOI:
            status = "shortage"
        elif moh is not None and moh <= STATUS_BALANCE_MOI:
            status = "balanced"
        elif moh is not None and moh <= STATUS_SURPLUS_MOI:
            status = "surplus"
        else:
            status = "serious_surplus"

        raw_desc = detail.get("description", "")
        pattern  = _extract_pattern(raw_desc)
        line     = _marketing_line(pattern)
        eff_group = (group or detail.get("group") or "").strip()

        # MOI including entire Factory pipeline (incoming KR/JP/HU) —
        # a longer-horizon planning metric than the current MOI.
        moh_plus = (total_all / total_3m) if total_3m > 0 else None

        # Product characteristics for the quick-filter chips
        category = _product_category(eff_group, line)
        inch_num = _num_or_none(detail.get("inch"))
        sr_num   = _num_or_none(detail.get("sr"))
        is_18plus     = inch_num is not None and inch_num >= 18
        is_low_prof   = sr_num   is not None and sr_num   < 50
        is_suv_flag   = _is_suv(line, pattern)

        rows_out.append({
            "merge_code":     mc,
            "m_code":         mcode,
            "group":          eff_group,
            "classification": classif or "",
            "brand":          detail.get("brand", ""),
            "line":           line,           # Marketing line (Kinergy / Dynapro / Ventus…)
            "pattern":        pattern,        # Pattern code (K425, RA33…)
            "description":    raw_desc,
            "size":           size,
            "inch":           detail.get("inch", ""),
            "sr":             detail.get("sr", ""),
            "li":             detail.get("li", ""),
            "ss":             detail.get("ss", ""),
            "factory":        detail.get("factory", ""),
            "origin":         detail.get("origin", ""),
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
            "total_3m":          total_3m,
            "moh":               round(moh, 2)      if moh      is not None else None,
            "moh_plus":          round(moh_plus, 2) if moh_plus is not None else None,
            "status":            status,
        })

    meta = {
        "path":   os.path.basename(path),
        "mtime":  datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M"),
        "rows":   len(rows_out),
        "load_s": round(time.time() - t0, 2),
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

    shortage_rows, surplus_rows, serious_rows, no_move_rows = [], [], [], []
    all_rows_flat = []   # every non-empty row, for the drill-down index

    for r in rows:
        st = r["status"]
        kpi_status[st] += 1
        if st == "empty":
            continue

        total_stock += r["total_stock"]
        total_3m    += r["total_3m"]
        total_pipe  += sum(r["state_pipeline"].values())

        for s in STATES:
            state_totals[s]["stock"]     += r["state_stock"][s]
            state_totals[s]["pipeline"]  += r["state_pipeline"][s]
            state_totals[s]["demand_3m"] += r["state_3m"][s]
            # Per-state status: local MOH
            sd = r["state_3m"][s]
            ss = r["state_stock"][s]
            if sd == 0 and ss == 0:
                pass
            elif sd == 0 and ss > 0:
                state_totals[s]["no_move"] += 1
            else:
                s_moh = ss / sd if sd > 0 else None
                if   s_moh is None:                        pass
                elif s_moh <= STATUS_SHORTAGE_MOI:         state_totals[s]["shortage"] += 1
                elif s_moh <= STATUS_BALANCE_MOI:          state_totals[s]["balanced"] += 1
                elif s_moh <= STATUS_SURPLUS_MOI:          state_totals[s]["surplus"] += 1
                else:                                      state_totals[s]["serious_surplus"] += 1

        # Group breakdowns
        g = r["group"] or "—"
        by_group.setdefault(g, _empty_bucket())[st] += 1
        by_group[g].setdefault("stock", 0)
        by_group[g]["stock"] += r["total_stock"]
        by_group[g].setdefault("demand_3m", 0)
        by_group[g]["demand_3m"] += r["total_3m"]

        inch = str(r["inch"] or "—")
        by_inch.setdefault(inch, _empty_bucket())[st] += 1
        by_inch[inch].setdefault("stock", 0); by_inch[inch]["stock"] += r["total_stock"]

        br = r["brand"] or "—"
        by_brand.setdefault(br, _empty_bucket())[st] += 1
        by_brand[br].setdefault("stock", 0); by_brand[br]["stock"] += r["total_stock"]

        cl = r["classification"] or "—"
        by_classif.setdefault(cl, _empty_bucket())[st] += 1
        by_classif[cl].setdefault("stock", 0); by_classif[cl]["stock"] += r["total_stock"]

        ln = r["line"] or "Other"
        by_line.setdefault(ln, _empty_bucket())[st] += 1
        by_line[ln].setdefault("stock", 0); by_line[ln]["stock"] += r["total_stock"]

        # Chart split — PCLT and TBR each get their own by-line
        # breakdown so the two hero charts show the axis the user
        # actually reads (never mix passenger and truck lines on
        # the same bar).
        if r["category"] == "PCLT":
            by_pclt.setdefault(ln, _empty_bucket())[st] += 1
            by_pclt[ln].setdefault("stock", 0); by_pclt[ln]["stock"] += r["total_stock"]
        elif r["category"] == "TBR":
            by_tbr.setdefault(ln, _empty_bucket())[st] += 1
            by_tbr[ln].setdefault("stock", 0); by_tbr[ln]["stock"] += r["total_stock"]

        # SKU entry (unified — every table + the drill-down index uses
        # the same shape so the front-end can look up a full row from
        # its Merge Code without a second lookup structure).
        entry = {
            "merge_code":     r["merge_code"],
            "m_code":         r["m_code"],
            "description":    r["description"] or f"MC {r['merge_code']}",
            "group":          r["group"],
            "line":           r["line"],
            "pattern":        r["pattern"],
            "brand":          r["brand"],
            "size":           r["size"],
            "inch":           str(r["inch"]) if r["inch"] not in (None, "") else "",
            "sr":             str(r["sr"])   if r["sr"]   not in (None, "") else "",
            "li":             str(r["li"])   if r["li"]   not in (None, "") else "",
            "ss":             str(r["ss"])   if r["ss"]   not in (None, "") else "",
            "category":       r["category"],
            "is_18plus":      r["is_18plus"],
            "is_low_profile": r["is_low_profile"],
            "is_suv":         r["is_suv"],
            "total_stock":    r["total_stock"],
            "total_all":      r["total_all"],
            "total_3m":       round(r["total_3m"], 2),
            "moh":            r["moh"],
            "moh_plus":       r["moh_plus"],
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
        elif st == "surplus":
            surplus_rows.append(entry)
        elif st == "serious_surplus":
            serious_rows.append(entry)
        elif st == "no_move":
            no_move_rows.append(entry)

    # Sort tables — shortage by MOI ascending (most urgent first)
    shortage_rows.sort(key=lambda e: (e["moh"] if e["moh"] is not None else 0, -e["total_3m"]))
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
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap">
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
:root {
    --ground: #F4F6F9;
    --card:   #FFFFFF;
    --border: #E1E5EB;
    --ink:    #263238;
    --muted:  #607D8B;
    --hdr1:   #0E3F5F;
    --hdr2:   #1F4E79;
    --short:  #C62828;
    --short-fg:#FFEBEE;
    --bal:    #2E7D32;
    --bal-fg: #E8F5E9;
    --sur:    #EF6C00;
    --sur-fg: #FFF3E0;
    --ser:    #B71C1C;   /* Serious surplus — darker warning red */
    --ser-fg: #FDE0E0;
    --nom:    #6A1B9A;
    --nom-fg: #F3E5F5;
    --hover:  #F5F7FA;
}
body { font-family: 'IBM Plex Sans', 'Segoe UI', system-ui, sans-serif;
       background: var(--ground); color: var(--ink); height: 100vh;
       display: flex; flex-direction: column; overflow: hidden;
       font-variant-numeric: tabular-nums; }

/* ── Header ── */
.hdr { background: linear-gradient(135deg,var(--hdr1),var(--hdr2));
       color:#fff; padding: 12px 22px; display:flex; align-items:center;
       gap:14px; flex-shrink:0; box-shadow: 0 2px 8px rgba(0,0,0,0.12); }
.hdr h1 { font-size: 18px; letter-spacing:.3px; font-weight: 600; }
.hdr .subtitle { font-size: 11px; opacity:.75; line-height: 1.4; }
.hdr .nav { margin-left:auto; display:flex; gap:6px; }
.hdr .nav a { font-size: 12px; padding: 5px 13px; border-radius: 5px; cursor: pointer;
              border: 1px solid rgba(255,255,255,0.45); color: #fff; text-decoration: none; }
.hdr .nav a:hover { background: rgba(255,255,255,0.15); }
.hdr .nav a.active { background: #fff; color: var(--hdr1); font-weight: 600; }

/* ── Top filter (two rows: PRODUCT / REGION-BDE) ── */
.topfilters { background: #fff; border-bottom: 1px solid var(--border);
              flex-shrink: 0; }
.filter-row { display: flex; align-items: center; gap: 6px; padding: 8px 16px;
              flex-wrap: wrap; border-bottom: 1px solid #F1F3F5; }
.filter-row:last-child { border-bottom: none; }
.filter-row .flabel { font-size: 10.5px; text-transform: uppercase;
                      letter-spacing: .08em; color: var(--muted); font-weight: 700;
                      min-width: 80px; }
.chip-btn { background: #F1F5F9; border: 1px solid #CBD5E1; color: #334155;
            font: 500 12px/1 'IBM Plex Sans', system-ui, sans-serif;
            padding: 6px 12px; border-radius: 6px; cursor: pointer;
            transition: background .12s, color .12s, border-color .12s; }
.chip-btn:hover:not(.active) { background: #E2E8F0; }
.chip-btn.active { background: #1D4ED8; border-color: #1D4ED8; color: #fff;
                   font-weight: 600; }
.chip-btn.disabled { opacity: .45; cursor: not-allowed; text-decoration: line-through;
                     font-style: italic; }
.chip-btn.disabled:hover { background: #F1F5F9; }

.combo { display: flex; align-items: center; gap: 5px; }
.combo label { font-size: 12px; color: #334155; font-weight: 600; }
.combo select, .combo input {
    font: 500 12px 'IBM Plex Sans', system-ui, sans-serif;
    padding: 5px 10px; border: 1px solid #CBD5E1;
    border-radius: 5px; background: #fff; color: var(--ink);
    min-width: 130px; }
.combo select:focus, .combo input:focus { outline: none; border-color: #1D4ED8; }
.combo .clear { background: none; border: none; color: #94A3B8; font-size: 13px;
                cursor: pointer; padding: 0 3px; }
.combo .clear:hover { color: var(--short); }
.reset-btn { margin-left: auto; background: none; border: 1px solid #94A3B8;
             color: #475569; font: 500 11px 'IBM Plex Sans', sans-serif;
             padding: 4px 12px; border-radius: 5px; cursor: pointer; }
.reset-btn:hover { background: #E2E8F0; }

/* ── KPI strip ── */
.kpi-strip { display: grid; grid-template-columns: repeat(7, 1fr);
             gap: 10px; padding: 10px 16px 4px; }
.kpi { background: var(--card); border-radius: 8px; padding: 10px 14px;
       border: 1px solid var(--border); position: relative; overflow: hidden; }
.kpi h4 { font-size: 10px; color:var(--muted); text-transform: uppercase;
          letter-spacing:.1em; margin-bottom: 4px; font-weight: 600; }
.kpi .v { font-size: 22px; font-weight: 700; color:var(--hdr1);
          font-family: 'IBM Plex Mono', ui-monospace, monospace;
          font-variant-numeric: tabular-nums; }
.kpi .u { font-size: 10.5px; color:#78909C; margin-left: 4px; }
.kpi.short { border-left: 4px solid var(--short); }
.kpi.short  .v { color:var(--short); }
.kpi.bal   { border-left: 4px solid var(--bal); }
.kpi.bal    .v { color:var(--bal); }
.kpi.sur   { border-left: 4px solid var(--sur); }
.kpi.sur    .v { color:var(--sur); }
.kpi.ser   { border-left: 4px solid var(--ser); }
.kpi.ser    .v { color:var(--ser); }
.kpi.nom   { border-left: 4px solid var(--nom); }
.kpi.nom    .v { color:var(--nom); }

/* ── Threshold footnote strip ── */
.thresh-note { font-size: 11px; color: var(--muted);
               padding: 4px 18px 6px; line-height: 1.6; }
.thresh-note b { color: var(--ink); font-weight: 600; }
.thresh-note .sw { display: inline-block; width: 10px; height: 10px;
                   border-radius: 2px; vertical-align: -1px; margin-right: 4px; }
.thresh-note .sw.short { background: var(--short); }
.thresh-note .sw.bal   { background: var(--bal); }
.thresh-note .sw.sur   { background: var(--sur); }
.thresh-note .sw.ser   { background: var(--ser); }
.thresh-note .sw.nom   { background: var(--nom); }

/* ── main body grid ── */
.wrap { flex:1; overflow-y: auto; padding: 6px 16px 20px; }
.grid { display: grid; grid-template-columns: 320px 1fr; gap: 12px; }

.state-col { display: flex; flex-direction: column; gap: 10px; }
.state-card { background:var(--card); border-radius:8px; border:1px solid var(--border);
              padding: 12px 14px; }
.state-card h3 { font-size: 13px; display:flex; justify-content:space-between;
                 align-items:baseline; margin-bottom: 6px; font-weight: 600; }
.state-card h3 .m { font-family:'IBM Plex Mono', monospace;
                    color:#546E7A; font-size:11px; font-variant-numeric: tabular-nums; }
.state-row { display:flex; justify-content:space-between; align-items:baseline;
             font-size: 11.5px; padding: 3px 0; border-top: 1px solid #F1F3F5; }
.state-row:first-of-type { border-top: none; }
.state-row .lbl { color:var(--muted); }
.state-row .v   { font-family: 'IBM Plex Mono', monospace; font-weight: 600;
                  color:var(--ink); font-variant-numeric: tabular-nums; }
.chip { display:inline-block; padding: 1px 7px; border-radius: 8px;
        font-size: 10px; font-weight: 600; margin-left: 4px;
        font-family: 'IBM Plex Mono', monospace; }
.chip.short { background:var(--short-fg); color:var(--short); }
.chip.sur   { background:var(--sur-fg);   color:var(--sur); }
.chip.ser   { background:var(--ser-fg);   color:var(--ser); }
.chip.bal   { background:var(--bal-fg);   color:var(--bal); }
.chip.nom   { background:var(--nom-fg);   color:var(--nom); }

.right { display: flex; flex-direction: column; gap: 12px; min-width: 0; }
.card { background:var(--card); border-radius:8px; border:1px solid var(--border);
        padding: 12px 14px 14px; }
.card h3 { font-size: 12.5px; color:var(--hdr1);
           border-bottom: 1px solid #ECEFF1; padding-bottom: 6px; margin-bottom: 8px;
           display: flex; justify-content: space-between; align-items: baseline;
           font-weight: 600; }
.card h3 .hint { font-size: 10.5px; color:#78909C; font-weight: 400; }
.card h3 .cat-pill { display: inline-block; padding: 1px 8px; border-radius: 10px;
                     background: #E2E8F0; color: #334155; font-size: 10px;
                     font-weight: 700; letter-spacing: .06em; margin-left: 6px; }
.chartbox { position: relative; height: 240px; }
.chartbox.tall { height: 260px; }

.tabs { display:flex; gap: 4px; margin-bottom: 6px; }
.tab {  font-size: 11.5px; padding: 5px 12px; border-radius: 5px; cursor: pointer;
        background: #ECEFF1; color: #37474F; border: 1px solid #CFD8DC; }
.tab.active { background: var(--hdr1); color: #fff; border-color: var(--hdr1); }
.tab:hover:not(.active) { background: #CFD8DC; }
.tab .n { display:inline-block; margin-left: 6px; padding: 0 6px;
          border-radius: 8px; background: rgba(0,0,0,0.08); font-size: 10px;
          font-family: 'IBM Plex Mono', monospace; }
.tab.active .n { background: rgba(255,255,255,0.24); }

table.dt { width: 100%; border-collapse: collapse; font-size: 11.5px; }
table.dt thead th { background: #ECEFF1; color:#37474F; padding: 6px 8px;
                    text-align: left; position: sticky; top: 0; z-index: 2;
                    border-bottom: 1px solid #CFD8DC; font-size: 10.5px;
                    text-transform: uppercase; letter-spacing: .04em; }
table.dt tbody td { padding: 5px 8px; border-bottom: 1px solid #F1F3F5;
                    white-space: nowrap; font-size: 11.5px; }
table.dt tbody tr { cursor: pointer; }
table.dt tbody tr:hover td { background: var(--hover); }
table.dt .r { text-align: right; font-family: 'IBM Plex Mono', monospace;
              font-variant-numeric: tabular-nums; }
table.dt .r.short { color:var(--short); font-weight: 700; }
table.dt .r.sur   { color:var(--sur);   font-weight: 700; }
table.dt .r.ser   { color:var(--ser);   font-weight: 700; }
.tbl-wrap { max-height: 480px; overflow-y: auto; overflow-x: auto; }

.pill { display: inline-block; padding: 2px 7px; border-radius: 10px;
        font-size: 10.5px; font-weight: 600; }
.pill.g-SP  { background: #E3F2FD; color: #1565C0; }
.pill.g-HP  { background: #FFF3E0; color: #E65100; }
.pill.g-UHP { background: #FCE4EC; color: #AD1457; }
.pill.g-TBR { background: #E8F5E9; color: #2E7D32; }
.pill.g-LS  { background: #EDE7F6; color: #4527A0; }
.pill.g-LV  { background: #E0F2F1; color: #00695C; }
.pill.g-RUNFLAT { background: #FCE4EC; color: #880E4F; }
.pill.g-RACING  { background: #FBE9E7; color: #BF360C; }
.pill.g-other { background: #ECEFF1; color: #455A64; }
.pill.sm { font-size: 9.5px; padding: 1px 6px; }

.legend { display: flex; gap: 12px; font-size: 11px; margin-top: 6px; }
.legend .dot { display: inline-block; width: 9px; height: 9px;
               border-radius: 50%; margin-right: 4px; vertical-align: -1px; }
.legend .dot.short { background: var(--short); }
.legend .dot.bal   { background: var(--bal); }
.legend .dot.sur   { background: var(--sur); }
.legend .dot.ser   { background: var(--ser); }
.legend .dot.nom   { background: var(--nom); }

.row-count { margin-left: auto; align-self: center; color: var(--muted);
             font-size: 11px; font-family: 'IBM Plex Mono', monospace; }
.filter-row-inner { display: flex; gap: 8px; margin-bottom: 10px; align-items: center; }

/* ── Modal ── */
.modal-bg { display: none; position: fixed; inset: 0; z-index: 200;
            background: rgba(15,25,35,0.55); }
.modal-bg.open { display: flex; align-items: center; justify-content: center; }
.modal { background: var(--card); width: min(1080px, 96vw); max-height: 92vh;
         border-radius: 10px; overflow: hidden; display: flex;
         flex-direction: column; box-shadow: 0 20px 60px rgba(0,0,0,0.35); }
.modal-hdr { background: linear-gradient(135deg,var(--hdr1),var(--hdr2));
             color: #fff; padding: 14px 20px; display: flex; align-items: center;
             gap: 12px; }
.modal-hdr .mtitle { font-size: 15px; font-weight: 600; }
.modal-hdr .msub { font-size: 11.5px; opacity: .75; margin-top: 2px; }
.modal-hdr .close { margin-left: auto; background: rgba(255,255,255,0.15);
                    color: #fff; border: 1px solid rgba(255,255,255,0.35);
                    border-radius: 5px; padding: 4px 12px; cursor: pointer;
                    font: 500 12px 'IBM Plex Sans', sans-serif; }
.modal-hdr .close:hover { background: rgba(255,255,255,0.25); }
.modal-body { padding: 18px 20px 22px; overflow-y: auto; display: grid;
              grid-template-columns: 320px 1fr; gap: 18px; }
.modal-body .stat { font-size: 11px; color: var(--muted);
                    text-transform: uppercase; letter-spacing: .06em;
                    margin-bottom: 2px; font-weight: 600; }
.modal-body .figv { font-size: 20px; font-weight: 700;
                    font-family: 'IBM Plex Mono', monospace;
                    color: var(--hdr1); font-variant-numeric: tabular-nums;
                    margin-bottom: 10px; }
.modal-body .figv .u { font-size: 11px; color: var(--muted);
                       font-family: 'IBM Plex Sans', sans-serif; margin-left: 3px; }
.modal-body .figv.short { color: var(--short); }
.modal-body .figv.sur   { color: var(--sur); }
.modal-body .figv.ser   { color: var(--ser); }
.modal-body .figv.bal   { color: var(--bal); }

.pipe-tbl { width: 100%; border-collapse: collapse;
            font-size: 11.5px; margin-top: 6px; }
.pipe-tbl th { text-align: left; color: var(--muted); padding: 5px 6px;
               font-size: 10px; text-transform: uppercase; letter-spacing: .06em; }
.pipe-tbl td { padding: 5px 6px; border-top: 1px solid #ECEFF1;
               font-family: 'IBM Plex Mono', monospace;
               font-variant-numeric: tabular-nums; text-align: right; }
.pipe-tbl td.st { text-align: left; font-family: 'IBM Plex Sans', sans-serif;
                  font-weight: 600; color: var(--ink); }
.pipe-tbl tr.tot td { border-top: 2px solid #CFD8DC; font-weight: 700; }
.pipe-tbl .short { color: var(--short); }
.pipe-tbl .sur   { color: var(--sur); }
.pipe-tbl .ser   { color: var(--ser); }
</style>
</head>
<body>

<div class="hdr">
  <h1>📦 Stock Balance Lab</h1>
  <span class="subtitle">Where we're short · where we're surplus · by state · by product<br>
    Source: {{ meta.path }} &nbsp;·&nbsp; loaded {{ meta.mtime }} &nbsp;·&nbsp; {{ meta.rows }} rows in {{ meta.load_s }} s</span>
  <nav class="nav">
    <a href="/">Dashboard</a>
    <a href="/price">Price</a>
    <a href="/stock_lab" class="active">Stock Lab</a>
  </nav>
</div>

<!-- Top filter — matches the attached mock (Salesman + Promotion + EV/iSeg
     are not driven by data in the current XLSM so they render disabled). -->
<div class="topfilters" id="topfilters">
  <div class="filter-row">
    <span class="flabel">Product</span>
    <button class="chip-btn active" data-prod="all">All</button>
    <button class="chip-btn"        data-prod="pclt">PCLT</button>
    <button class="chip-btn"        data-prod="18plus">18+ Inch</button>
    <button class="chip-btn disabled" title="No EV/iSeg tag in source data">EV/iSeg</button>
    <button class="chip-btn"        data-prod="suv">SUV</button>
    <button class="chip-btn"        data-prod="tbr">TBR</button>
    <button class="chip-btn"        data-prod="lowprofile">Low profile</button>
    <button class="chip-btn"        data-brand="HK">HK</button>
    <button class="chip-btn"        data-brand="LF">LF</button>
    <button class="chip-btn disabled" title="No promotion column in source data">Promotion</button>
    <div class="combo">
      <label>Product Group</label>
      <select id="f-group"><option value="">All Groups</option></select>
      <button class="clear" onclick="clearSelect('f-group')">×</button>
    </div>
    <div class="combo">
      <label>Pattern</label>
      <input type="text" id="f-pattern" placeholder="Search Pattern">
      <button class="clear" onclick="clearInput('f-pattern')">×</button>
    </div>
    <div class="combo">
      <label>Size</label>
      <select id="f-size"><option value="">Search Size</option></select>
      <button class="clear" onclick="clearSelect('f-size')">×</button>
    </div>
    <div class="combo">
      <label>Code</label>
      <input type="text" id="f-code" placeholder="Search Code">
      <button class="clear" onclick="clearInput('f-code')">×</button>
    </div>
    <button class="reset-btn" onclick="resetAllFilters()">Reset</button>
  </div>
  <div class="filter-row">
    <span class="flabel">Region /<br>BDE</span>
    <button class="chip-btn active" data-state="all">All</button>
    <button class="chip-btn"        data-state="NSW">NSW</button>
    <button class="chip-btn"        data-state="QLD">QLD</button>
    <button class="chip-btn"        data-state="VIC">VIC</button>
    <button class="chip-btn"        data-state="WA">WA</button>
    <div class="combo" style="margin-left:14px">
      <label>Salesman:</label>
      <select disabled title="No salesman data in current source"><option>ALL</option></select>
    </div>
  </div>
</div>

<!-- KPI strip -->
<div class="kpi-strip">
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
    <div class="card"><h3>Stock across states <span class="hint">as of {{ meta.mtime }}</span></h3>
      <div id="state-cards"></div>
    </div>
  </div>

  <div class="right">
    <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 12px;">
      <div class="card">
        <h3>Marketing Lines <span class="cat-pill">PCLT</span>
            <span class="hint">SKU count · stacked by status</span></h3>
        <div class="chartbox"><canvas id="chart-pclt"></canvas></div>
        <div class="legend">
          <span><span class="dot short"></span>Shortage</span>
          <span><span class="dot bal"></span>Balance</span>
          <span><span class="dot sur"></span>Surplus</span>
          <span><span class="dot ser"></span>Serious Surplus</span>
          <span><span class="dot nom"></span>No move</span>
        </div>
      </div>
      <div class="card">
        <h3>Marketing Lines <span class="cat-pill">TBR</span>
            <span class="hint">SKU count · stacked by status</span></h3>
        <div class="chartbox"><canvas id="chart-tbr"></canvas></div>
      </div>
    </div>

    <div class="card">
      <h3>SKU drill-down <span class="hint">click any row for the monthly-by-state view</span></h3>
      <div class="tabs">
        <div class="tab active" data-tab="shortage">🔴 Shortage <span class="n" id="n-short">0</span></div>
        <div class="tab" data-tab="surplus">🟠 Surplus <span class="n" id="n-sur">0</span></div>
        <div class="tab" data-tab="serious_surplus">🟥 Serious Surplus <span class="n" id="n-ser">0</span></div>
        <div class="tab" data-tab="no_move">🟣 No move <span class="n" id="n-nom">0</span></div>
      </div>

      <div class="filter-row-inner">
        <span class="row-count" id="row-count">—</span>
      </div>

      <div class="tbl-wrap">
        <table class="dt">
          <thead>
            <tr>
              <th>Merge</th><th>M CODE</th><th>Brand</th>
              <th>Marketing Line</th><th>Pattern</th>
              <th>Group</th><th>Size</th><th>LI/SS</th>
              <th class="r">NSW</th><th class="r">QLD</th><th class="r">VIC</th><th class="r">WA</th>
              <th class="r">Stock</th><th class="r">3M Avg</th><th class="r">MOI</th>
            </tr>
          </thead>
          <tbody id="tbl-body"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>
</div>

<!-- ── Drill-down modal ── -->
<div class="modal-bg" id="modal-bg">
  <div class="modal">
    <div class="modal-hdr">
      <div>
        <div class="mtitle" id="m-title">—</div>
        <div class="msub" id="m-sub">—</div>
      </div>
      <button class="close" onclick="closeModal()">✕ Close</button>
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
        <div class="stat">MOI — including Factory pipeline</div>
        <div class="figv" id="m-moiplus">—</div>
      </div>
      <div>
        <div class="stat">12-month sales by state</div>
        <div class="chartbox tall"><canvas id="m-chart"></canvas></div>

        <div class="stat" style="margin-top:14px">State breakdown</div>
        <table class="pipe-tbl">
          <thead><tr><th>State</th><th>Stock</th><th>Port</th><th>Water</th>
              <th>Factory</th><th>Pipeline</th><th>3M Avg</th><th>MOI</th><th>MOI +Pipe</th></tr></thead>
          <tbody id="m-pipe-tbl"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<script>
const DATA = {{ data_json | safe }};
const STATES = ["NSW","QLD","VIC","WA"];

/* ── Formatting helpers ── */
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

/* ── Filter state (matches the attached mock) ── */
const F = {
    /* PRODUCT row chips */
    prod: 'all',       /* 'all' | 'pclt' | '18plus' | 'suv' | 'tbr' | 'lowprofile' */
    brand: null,       /* null | 'HK' | 'LF' */
    /* PRODUCT row combos */
    group: '',
    pattern: '',
    size: '',
    code: '',
    /* REGION row */
    state: 'all',      /* 'all' | 'NSW' | 'QLD' | 'VIC' | 'WA' */
};

/* Row passes all top filters */
function rowPassesTopFilters(r) {
    switch (F.prod) {
        case 'pclt':       if (r.category !== 'PCLT') return false; break;
        case 'tbr':        if (r.category !== 'TBR') return false; break;
        case '18plus':     if (!r.is_18plus) return false; break;
        case 'suv':        if (!r.is_suv) return false; break;
        case 'lowprofile': if (!r.is_low_profile) return false; break;
        case 'all':        default: break;
    }
    if (F.brand && r.brand !== F.brand) return false;
    if (F.group && r.group !== F.group) return false;
    if (F.pattern) {
        const p = F.pattern.toLowerCase();
        if (!(r.pattern || '').toLowerCase().includes(p)) return false;
    }
    if (F.size && r.size !== F.size) return false;
    if (F.code) {
        const c = F.code.toLowerCase();
        const hay = ((r.m_code || '') + ' ' + (r.merge_code || '')).toLowerCase();
        if (!hay.includes(c)) return false;
    }
    if (F.state !== 'all') {
        if ((r.state_stock[F.state] || 0) === 0 && (r.state_3m[F.state] || 0) === 0) return false;
    }
    return true;
}

/* ── KPI tiles (fed from the current filter selection) ── */
function currentSetOfRows() {
    return DATA.all_rows.filter(rowPassesTopFilters);
}
function recomputeKPI() {
    const rows = currentSetOfRows();
    let sh=0, bl=0, su=0, se=0, nm=0;
    rows.forEach(r => {
        if (r.status === 'shortage')        sh++;
        else if (r.status === 'balanced')   bl++;
        else if (r.status === 'surplus')    su++;
        else if (r.status === 'serious_surplus') se++;
        else if (r.status === 'no_move')    nm++;
    });
    const totStk = rows.reduce((s,r) => s + r.total_stock, 0);
    const tot3m  = rows.reduce((s,r) => s + r.total_3m, 0);
    document.getElementById('kpi-sku').textContent   = fmtI(sh+bl+su+se+nm);
    document.getElementById('kpi-short').textContent = fmtI(sh);
    document.getElementById('kpi-bal').textContent   = fmtI(bl);
    document.getElementById('kpi-sur').textContent   = fmtI(su);
    document.getElementById('kpi-ser').textContent   = fmtI(se);
    document.getElementById('kpi-nom').textContent   = fmtI(nm);
    document.getElementById('kpi-moi').textContent   = tot3m > 0 ? FMT_2.format(totStk / tot3m) : '—';
    document.getElementById('n-short').textContent   = fmtI(sh);
    document.getElementById('n-sur').textContent     = fmtI(su);
    document.getElementById('n-ser').textContent     = fmtI(se);
    document.getElementById('n-nom').textContent     = fmtI(nm);
}

/* ── State cards (also react to the top filters) ── */
function renderStateCards() {
    const rows = currentSetOfRows();
    const host = document.getElementById('state-cards');
    /* Aggregate per state from the filtered rows */
    const acc = {};
    STATES.forEach(s => acc[s] = {stock:0, pipeline:0, demand_3m:0,
        shortage:0, balanced:0, surplus:0, serious_surplus:0, no_move:0});
    rows.forEach(r => {
        STATES.forEach(s => {
            acc[s].stock     += r.state_stock[s]    || 0;
            acc[s].pipeline  += r.state_pipeline[s] || 0;
            acc[s].demand_3m += r.state_3m[s]       || 0;
            const sd = r.state_3m[s] || 0, ss = r.state_stock[s] || 0;
            if      (sd === 0 && ss > 0)   acc[s].no_move++;
            else if (sd  >  0) {
                const m = ss / sd;
                if      (m <= 1) acc[s].shortage++;
                else if (m <= 3) acc[s].balanced++;
                else if (m <= 6) acc[s].surplus++;
                else             acc[s].serious_surplus++;
            }
        });
    });
    let html = '';
    STATES.forEach(s => {
        const st = acc[s];
        const moi = st.demand_3m > 0 ? (st.stock / st.demand_3m) : null;
        html += '<div class="state-card">'
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
}

/* ── PCLT / TBR stacked-bar charts (built from filtered rows) ── */
let _pcltChart = null, _tbrChart = null;
function computeByLine(rows, category) {
    const by = {};
    rows.forEach(r => {
        if (r.category !== category) return;
        const ln = r.line || 'Other';
        if (!by[ln]) by[ln] = {shortage:0, balanced:0, surplus:0, serious_surplus:0, no_move:0};
        if (by[ln][r.status] !== undefined) by[ln][r.status]++;
    });
    return by;
}
function drawStackedBar(canvas_id, by) {
    const labels = Object.keys(by).sort((a,b) => (by[b].shortage||0) - (by[a].shortage||0));
    const short = labels.map(k => by[k].shortage);
    const bal   = labels.map(k => by[k].balanced);
    const sur   = labels.map(k => by[k].surplus);
    const ser   = labels.map(k => by[k].serious_surplus);
    const nom   = labels.map(k => by[k].no_move);
    const ctx = document.getElementById(canvas_id);
    return new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets: [
            { label: 'Shortage', data: short, backgroundColor: '#C62828' },
            { label: 'Balance',  data: bal,   backgroundColor: '#66BB6A' },
            { label: 'Surplus',  data: sur,   backgroundColor: '#FB8C00' },
            { label: 'Serious',  data: ser,   backgroundColor: '#B71C1C' },
            { label: 'No move',  data: nom,   backgroundColor: '#8E24AA' },
        ]},
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false },
                       tooltip: { callbacks: {
                           footer: items => {
                               let sum = 0; items.forEach(i => sum += i.raw);
                               return 'Total: ' + FMT_INT.format(sum);
                           }} } },
            scales: {
                x: { stacked: true, ticks: { font: { size: 10 }, autoSkip: false, maxRotation: 45, minRotation: 25 } },
                y: { stacked: true, ticks: { font: { size: 10 } } },
            }
        }
    });
}
function renderCharts() {
    const rows = currentSetOfRows();
    if (_pcltChart) _pcltChart.destroy();
    if (_tbrChart)  _tbrChart.destroy();
    _pcltChart = drawStackedBar('chart-pclt', computeByLine(rows, 'PCLT'));
    _tbrChart  = drawStackedBar('chart-tbr',  computeByLine(rows, 'TBR'));
}

/* ── Combo (Product Group, Size) options + wiring ── */
function populateCombos() {
    const groups = new Set(), sizes = new Set();
    DATA.all_rows.forEach(r => {
        if (r.group) groups.add(r.group);
        if (r.size)  sizes.add(r.size);
    });
    const gSel = document.getElementById('f-group');
    [...groups].sort().forEach(g => {
        const o = document.createElement('option'); o.value = g; o.textContent = g;
        gSel.appendChild(o);
    });
    const sSel = document.getElementById('f-size');
    [...sizes].sort().forEach(s => {
        const o = document.createElement('option'); o.value = s; o.textContent = s;
        sSel.appendChild(o);
    });
}
populateCombos();

document.getElementById('f-group').addEventListener('change',   e => { F.group = e.target.value; refresh(); });
document.getElementById('f-size').addEventListener('change',    e => { F.size  = e.target.value; refresh(); });
document.getElementById('f-pattern').addEventListener('input',
    (function(){ let t; return function(e) { clearTimeout(t); t = setTimeout(()=>{ F.pattern = e.target.value; refresh(); }, 150); }; })());
document.getElementById('f-code').addEventListener('input',
    (function(){ let t; return function(e) { clearTimeout(t); t = setTimeout(()=>{ F.code = e.target.value; refresh(); }, 150); }; })());

/* Chip buttons — PRODUCT row */
document.querySelectorAll('[data-prod]').forEach(btn => {
    btn.addEventListener('click', () => {
        if (btn.classList.contains('disabled')) return;
        F.prod = btn.dataset.prod;
        document.querySelectorAll('[data-prod]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        refresh();
    });
});
document.querySelectorAll('[data-brand]').forEach(btn => {
    btn.addEventListener('click', () => {
        if (btn.classList.contains('disabled')) return;
        const br = btn.dataset.brand;
        if (F.brand === br) { F.brand = null; btn.classList.remove('active'); }
        else {
            F.brand = br;
            document.querySelectorAll('[data-brand]').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
        }
        refresh();
    });
});
/* Chip buttons — REGION row */
document.querySelectorAll('[data-state]').forEach(btn => {
    btn.addEventListener('click', () => {
        F.state = btn.dataset.state;
        document.querySelectorAll('[data-state]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        refresh();
    });
});

function clearSelect(id) { document.getElementById(id).value = '';
    if (id === 'f-group') F.group = ''; else if (id === 'f-size') F.size = '';
    refresh(); }
function clearInput(id) { document.getElementById(id).value = '';
    if (id === 'f-pattern') F.pattern = ''; else if (id === 'f-code') F.code = '';
    refresh(); }

function resetAllFilters() {
    F.prod = 'all'; F.brand = null; F.state = 'all';
    F.group = ''; F.pattern = ''; F.size = ''; F.code = '';
    document.querySelectorAll('[data-prod]').forEach(b => b.classList.toggle('active', b.dataset.prod === 'all'));
    document.querySelectorAll('[data-brand]').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('[data-state]').forEach(b => b.classList.toggle('active', b.dataset.state === 'all'));
    ['f-group','f-size','f-pattern','f-code'].forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
    refresh();
}

/* ── SKU drill-down table + tabs ── */
let curTab = 'shortage';
document.querySelectorAll('.tab').forEach(t => {
    t.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
        t.classList.add('active');
        curTab = t.dataset.tab;
        renderTable();
    });
});

function renderTable() {
    const src = DATA[curTab + '_rows'].filter(rowPassesTopFilters);
    const body = document.getElementById('tbl-body');
    body.innerHTML = src.slice(0, 500).map(r => {
        const cls_mo = r.status === 'shortage'        ? 'short'
                     : r.status === 'surplus'         ? 'sur'
                     : r.status === 'serious_surplus' ? 'ser' : '';
        return '<tr onclick="openModal(' + r.merge_code + ')">'
            + '<td>' + r.merge_code + '</td>'
            + '<td>' + (r.m_code || '—') + '</td>'
            + '<td>' + (r.brand || '—') + '</td>'
            + '<td>' + (r.line || '—') + '</td>'
            + '<td>' + (r.pattern || '—') + '</td>'
            + '<td>' + pill(r.group) + '</td>'
            + '<td>' + (r.size || '—') + '</td>'
            + '<td>' + (r.li ? r.li : '—') + (r.ss ? '/' + r.ss : '') + '</td>'
            + '<td class="r">' + fmtI(r.state_stock.NSW) + '</td>'
            + '<td class="r">' + fmtI(r.state_stock.QLD) + '</td>'
            + '<td class="r">' + fmtI(r.state_stock.VIC) + '</td>'
            + '<td class="r">' + fmtI(r.state_stock.WA)  + '</td>'
            + '<td class="r">' + fmtI(r.total_stock) + '</td>'
            + '<td class="r">' + fmtF(r.total_3m, 1) + '</td>'
            + '<td class="r ' + cls_mo + '">' + (r.moh != null ? fmtF(r.moh, 1) : '—') + '</td>'
            + '</tr>';
    }).join('');
    const suffix = src.length > 500 ? ' — showing first 500' : '';
    document.getElementById('row-count').textContent = fmtI(src.length) + ' rows' + suffix;
}

/* Central refresh — re-runs everything that depends on filter state */
function refresh() {
    recomputeKPI();
    renderStateCards();
    renderCharts();
    renderTable();
}
refresh();

/* ── Drill-down modal ── */
let _modalChart = null;
const MONTH_LABELS = ['-12M','-11M','-10M','-9M','-8M','-7M','-6M','-5M','-4M','-3M','-2M','-1M'];
function findRow(mc) { return DATA.all_rows.find(r => r.merge_code === mc); }

function openModal(mergeCode) {
    const r = findRow(mergeCode);
    if (!r) return;
    document.getElementById('m-title').textContent =
        (r.brand || '—') + ' · ' + (r.line || 'Other') + ' · ' + (r.pattern || '—')
        + '  ·  ' + (r.size || '') + '  ·  LI/SS ' + (r.li||'—') + '/' + (r.ss||'—');
    document.getElementById('m-sub').textContent =
        'Merge ' + r.merge_code + ' · M-code ' + (r.m_code || '—')
        + ' · ' + (r.description || '');
    document.getElementById('m-stock').innerHTML   = fmtI(r.total_stock) + '<span class="u">units</span>';
    const pipe = STATES.reduce((s, k) => s + (r.state_pipeline[k] || 0), 0);
    document.getElementById('m-pipe').innerHTML    = fmtI(pipe) + '<span class="u">units on the way</span>';
    document.getElementById('m-3m').innerHTML      = fmtF(r.total_3m, 1) + '<span class="u">units / mo</span>';
    const mohClass = r.moh == null ? ''
                   : r.moh <= 1 ? 'short'
                   : r.moh <= 3 ? 'bal'
                   : r.moh <= 6 ? 'sur' : 'ser';
    document.getElementById('m-moi').className     = 'figv ' + mohClass;
    document.getElementById('m-moi').innerHTML     = (r.moh != null ? fmtF(r.moh, 1) : '—') + '<span class="u">months</span>';
    document.getElementById('m-moiplus').innerHTML = (r.moh_plus != null ? fmtF(r.moh_plus, 1) : '—') + '<span class="u">months (Stock + Factory pipeline)</span>';

    if (_modalChart) { _modalChart.destroy(); _modalChart = null; }
    const stateColour = { NSW: '#1976D2', QLD: '#EF6C00', VIC: '#8E24AA', WA: '#00897B' };
    const datasets = STATES.map(s => ({
        label: s, data: r.history[s], borderColor: stateColour[s],
        backgroundColor: stateColour[s] + '22', borderWidth: 2,
        pointRadius: 3, tension: .25, fill: false,
    }));
    datasets.push({
        label: 'Total', data: r.history.TOTAL,
        borderColor: '#37474F', backgroundColor: '#37474F22',
        borderDash: [5,4], borderWidth: 1.5, pointRadius: 2, tension: .25, fill: false,
    });
    _modalChart = new Chart(document.getElementById('m-chart'), {
        type: 'line',
        data: { labels: MONTH_LABELS, datasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { position: 'top', labels: { boxWidth: 14, font: { size: 11 } } } },
            scales: { y: { beginAtZero: true, ticks: { font: { size: 10 } } },
                       x: { ticks: { font: { size: 10 } } } }
        }
    });

    const rows = STATES.map(s => {
        const pp = r.state_pipe_parts[s];
        const stock = r.state_stock[s];
        const pipe = pp.port + pp.water + pp.fac;
        const dem = r.state_3m[s];
        const smoh = dem > 0 ? (stock / dem) : null;
        const spmoh = dem > 0 ? ((stock + pipe) / dem) : null;
        const cls = smoh == null ? '' : smoh <= 1 ? 'short'
                  : smoh <= 3 ? '' : smoh <= 6 ? 'sur' : 'ser';
        return '<tr>'
            + '<td class="st">' + s + '</td>'
            + '<td>' + fmtI(stock) + '</td>'
            + '<td>' + fmtI(pp.port) + '</td>'
            + '<td>' + fmtI(pp.water) + '</td>'
            + '<td>' + fmtI(pp.fac) + '</td>'
            + '<td>' + fmtI(pipe) + '</td>'
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
      + '<tr class="tot">'
      + '<td class="st">Total</td>'
      + '<td>' + fmtI(totStock) + '</td>'
      + '<td>' + fmtI(totPort) + '</td>'
      + '<td>' + fmtI(totWater) + '</td>'
      + '<td>' + fmtI(totFac) + '</td>'
      + '<td>' + fmtI(totPipe) + '</td>'
      + '<td>' + fmtF(r.total_3m, 1) + '</td>'
      + '<td>' + (r.moh != null ? fmtF(r.moh, 1) : '—') + '</td>'
      + '<td>' + (r.moh_plus != null ? fmtF(r.moh_plus, 1) : '—') + '</td>'
      + '</tr>';

    document.getElementById('modal-bg').classList.add('open');
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
</script>
</body>
</html>
"""
