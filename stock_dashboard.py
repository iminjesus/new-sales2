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

STATUS_SHORTAGE_MOH = 1.0      # < 1 month → shortage
STATUS_SURPLUS_MOH  = 4.0      # > 4 months → surplus

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

        # Status bucket
        moh = (total_stock / total_3m) if total_3m > 0 else None
        if total_3m == 0 and total_stock == 0:
            status = "empty"
        elif total_3m == 0 and total_stock > 0:
            status = "no_move"
        elif moh is not None and moh < STATUS_SHORTAGE_MOH:
            status = "shortage"
        elif moh is not None and moh > STATUS_SURPLUS_MOH:
            status = "surplus"
        else:
            status = "balanced"

        raw_desc = detail.get("description", "")
        pattern  = _extract_pattern(raw_desc)
        line     = _marketing_line(pattern)

        # Second MOH: Stock On Hand + entire Factory pipeline (incoming
        # from KR/JP/HU plants) — a longer-horizon planning metric.
        moh_plus = ((total_stock + total_all - total_stock) / total_3m) if total_3m > 0 else None
        # total_all already includes stock+pipeline, so:
        moh_plus = (total_all / total_3m) if total_3m > 0 else None

        rows_out.append({
            "merge_code":     mc,
            "m_code":         mcode,
            "group":          group or detail.get("group") or "",
            "classification": classif or "",
            "brand":          detail.get("brand", ""),
            "line":           line,           # Marketing line (Kinergy / Dynapro / Ventus…)
            "pattern":        pattern,        # Pattern code (K425, RA33…)
            "description":    raw_desc,
            "size":           size,
            "inch":           detail.get("inch", ""),
            "li":             detail.get("li", ""),
            "ss":             detail.get("ss", ""),
            "factory":        detail.get("factory", ""),
            "origin":         detail.get("origin", ""),
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
    STATUSES = ["shortage", "balanced", "surplus", "no_move"]

    def _empty_bucket():
        return {s: 0 for s in STATUSES + ["empty"]}

    kpi_status = _empty_bucket()
    total_stock = 0.0
    total_3m    = 0.0
    total_pipe  = 0.0
    state_totals = {s: {"stock": 0.0, "pipeline": 0.0, "demand_3m": 0.0,
                        "shortage": 0, "surplus": 0, "balanced": 0,
                        "no_move": 0} for s in STATES}
    by_group    = {}
    by_inch     = {}
    by_brand    = {}
    by_classif  = {}
    by_line     = {}    # marketing line: Kinergy / Dynapro / Ventus / …

    shortage_rows, surplus_rows, no_move_rows = [], [], []
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
                if s_moh is not None and s_moh < STATUS_SHORTAGE_MOH:
                    state_totals[s]["shortage"] += 1
                elif s_moh is not None and s_moh > STATUS_SURPLUS_MOH:
                    state_totals[s]["surplus"] += 1
                else:
                    state_totals[s]["balanced"] += 1

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
            "li":             str(r["li"])   if r["li"]   not in (None, "") else "",
            "ss":             str(r["ss"])   if r["ss"]   not in (None, "") else "",
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
        elif st == "no_move":
            no_move_rows.append(entry)

    # Sort tables — shortage by MOH ascending (most urgent first)
    shortage_rows.sort(key=lambda e: (e["moh"] if e["moh"] is not None else 0, -e["total_3m"]))
    # Surplus by MOH descending (biggest overstock first)
    surplus_rows.sort(key=lambda e: -(e["moh"] if e["moh"] is not None else 0))
    # No-move by stock size descending
    no_move_rows.sort(key=lambda e: -e["total_stock"])

    # National MOH weighted-average = total stock / total 3M demand
    nat_moh = round(total_stock / total_3m, 2) if total_3m > 0 else None
    for s in STATES:
        d = state_totals[s]
        d["moh"] = round(d["stock"] / d["demand_3m"], 2) if d["demand_3m"] > 0 else None

    return {
        "kpi": {
            "sku_total":      sum(kpi_status[k] for k in ["shortage", "balanced", "surplus", "no_move"]),
            "sku_shortage":   kpi_status["shortage"],
            "sku_balanced":   kpi_status["balanced"],
            "sku_surplus":    kpi_status["surplus"],
            "sku_no_move":    kpi_status["no_move"],
            "total_stock":    int(total_stock),
            "total_pipeline": int(total_pipe),
            "total_demand_3m": int(round(total_3m)),   # integer with thousand sep
            "national_moh":   nat_moh,
        },
        "state":         state_totals,
        "by_group":      by_group,
        "by_line":       by_line,
        "by_inch":       by_inch,
        "by_brand":      by_brand,
        "by_classif":    by_classif,
        "shortage_rows": shortage_rows,   # all — front-end filters/paginates
        "surplus_rows":  surplus_rows,
        "no_move_rows":  no_move_rows,
        "all_rows":      all_rows_flat,   # complete SKU index for drill-down lookups
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

/* ── Top persistent filter bar ── */
.topfilters { background: #263238; color: #ECEFF1; padding: 8px 16px;
              display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
              flex-shrink: 0; border-bottom: 1px solid #1a2225; }
.topfilters .lbl { font-size: 10.5px; text-transform: uppercase;
                   letter-spacing: .08em; color: #90A4AE; font-weight: 600;
                   margin-right: 4px; }

/* Multi-select checkbox dropdown widget */
.ms { position: relative; display: inline-block; }
.ms-btn { background: #37474F; border: 1px solid #455A64; color: #ECEFF1;
          font: 500 12px 'IBM Plex Sans', system-ui, sans-serif;
          padding: 5px 24px 5px 11px; border-radius: 4px; cursor: pointer;
          min-width: 92px; text-align: left; position: relative; }
.ms-btn::after { content: '▾'; position: absolute; right: 8px; top: 50%;
                 transform: translateY(-50%); font-size: 9px; opacity: .7; }
.ms-btn:hover { background: #455A64; }
.ms-btn.active { background: var(--hdr2); border-color: var(--hdr2); color: #fff; }
.ms-btn .n { background: rgba(255,255,255,0.22); border-radius: 8px;
             padding: 0 5px; margin-left: 4px; font-size: 10px; font-weight: 600; }
.ms-panel { display: none; position: absolute; top: 100%; left: 0; z-index: 60;
            background: #fff; color: var(--ink);
            border: 1px solid #B0BEC5; border-radius: 5px; margin-top: 3px;
            min-width: 180px; max-height: 320px; overflow-y: auto;
            box-shadow: 0 6px 20px rgba(0,0,0,0.2); padding: 4px 0; }
.ms.open .ms-panel { display: block; }
.ms-panel label { display: flex; align-items: center; gap: 7px;
                  padding: 5px 12px; font-size: 12px; cursor: pointer;
                  user-select: none; }
.ms-panel label:hover { background: var(--hover); }
.ms-panel input[type=checkbox] { margin: 0; }
.ms-panel .ms-actions { display: flex; justify-content: space-between;
                        padding: 6px 12px 8px; border-top: 1px solid #ECEFF1;
                        margin-top: 4px; }
.ms-panel .ms-actions button { font-size: 11px; padding: 3px 10px; border-radius: 3px;
                               border: 1px solid #CFD8DC; background: #fff;
                               cursor: pointer; color: var(--hdr1); }
.ms-panel .ms-actions button:hover { background: var(--hover); }

.topfilters .search { margin-left: auto; }
.topfilters .search input { background: #37474F; border: 1px solid #455A64;
                             color: #ECEFF1; font: 500 12px 'IBM Plex Sans', sans-serif;
                             padding: 5px 11px; border-radius: 4px; width: 240px; }
.topfilters .search input::placeholder { color: #78909C; }
.topfilters .reset { background: none; border: 1px solid #607D8B;
                     color: #B0BEC5; padding: 4px 11px; border-radius: 4px;
                     cursor: pointer; font: 500 11px 'IBM Plex Sans', sans-serif; }
.topfilters .reset:hover { background: #37474F; color: #fff; }

/* ── KPI strip ── */
.kpi-strip { display: grid; grid-template-columns: repeat(6, 1fr);
             gap: 10px; padding: 10px 16px 4px; }
.kpi { background: var(--card); border-radius: 8px; padding: 10px 14px;
       border: 1px solid var(--border); position: relative; overflow: hidden; }
.kpi h4 { font-size: 10px; color:var(--muted); text-transform: uppercase;
          letter-spacing:.1em; margin-bottom: 4px; font-weight: 600; }
.kpi .v { font-size: 24px; font-weight: 700; color:var(--hdr1);
          font-family: 'IBM Plex Mono', ui-monospace, monospace;
          font-variant-numeric: tabular-nums; }
.kpi .u { font-size: 10.5px; color:#78909C; margin-left: 4px; }
.kpi.short { border-left: 4px solid var(--short); }
.kpi.short  .v { color:var(--short); }
.kpi.bal   { border-left: 4px solid var(--bal); }
.kpi.bal    .v { color:var(--bal); }
.kpi.sur   { border-left: 4px solid var(--sur); }
.kpi.sur    .v { color:var(--sur); }
.kpi.nom   { border-left: 4px solid var(--nom); }
.kpi.nom    .v { color:var(--nom); }

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
.chartbox { position: relative; height: 210px; }
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
table.dt .r.sur   { color:var(--sur); font-weight: 700; }
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
.legend .dot.nom   { background: var(--nom); }

.row-count { margin-left: auto; align-self: center; color: var(--muted);
             font-size: 11px; font-family: 'IBM Plex Mono', monospace; }
.filter-row { display: flex; gap: 8px; margin-bottom: 10px; align-items: center; }

/* ── Modal (drill-down) ── */
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

<!-- Top persistent filter bar -->
<div class="topfilters" id="topfilters">
  <span class="lbl">Filter</span>
  <div class="ms" data-key="status"><button class="ms-btn">Status</button><div class="ms-panel"></div></div>
  <div class="ms" data-key="state"><button class="ms-btn">State</button><div class="ms-panel"></div></div>
  <div class="ms" data-key="brand"><button class="ms-btn">Brand</button><div class="ms-panel"></div></div>
  <div class="ms" data-key="group"><button class="ms-btn">Group</button><div class="ms-panel"></div></div>
  <div class="ms" data-key="line"><button class="ms-btn">Marketing Line</button><div class="ms-panel"></div></div>
  <div class="ms" data-key="inch"><button class="ms-btn">Rim (inch)</button><div class="ms-panel"></div></div>
  <div class="ms" data-key="pattern"><button class="ms-btn">Pattern</button><div class="ms-panel"></div></div>
  <button class="reset" onclick="resetFilters()">Reset</button>
  <div class="search">
    <input id="fltr-search" type="text" placeholder="Search SKU / M-code / description / size…">
  </div>
</div>

<!-- KPI strip -->
<div class="kpi-strip">
  <div class="kpi"><h4>Active SKUs</h4><span class="v" id="kpi-sku">—</span></div>
  <div class="kpi short"><h4>Shortage</h4><span class="v" id="kpi-short">—</span><span class="u">SKUs</span></div>
  <div class="kpi bal"><h4>Balanced</h4><span class="v" id="kpi-bal">—</span><span class="u">SKUs</span></div>
  <div class="kpi sur"><h4>Surplus</h4><span class="v" id="kpi-sur">—</span><span class="u">SKUs</span></div>
  <div class="kpi nom"><h4>No move</h4><span class="v" id="kpi-nom">—</span><span class="u">SKUs</span></div>
  <div class="kpi"><h4>National MOH</h4><span class="v" id="kpi-moh">—</span><span class="u">months</span></div>
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
        <h3>By Marketing Line <span class="hint">SKU count · stacked by status</span></h3>
        <div class="chartbox"><canvas id="chart-line"></canvas></div>
        <div class="legend">
          <span><span class="dot short"></span>Shortage</span>
          <span><span class="dot bal"></span>Balanced</span>
          <span><span class="dot sur"></span>Surplus</span>
          <span><span class="dot nom"></span>No move</span>
        </div>
      </div>
      <div class="card">
        <h3>By Rim size (inch) <span class="hint">SKU count · stacked</span></h3>
        <div class="chartbox"><canvas id="chart-inch"></canvas></div>
      </div>
    </div>

    <div class="card">
      <h3>SKU drill-down <span class="hint">click any row for the monthly-by-state view</span></h3>
      <div class="tabs">
        <div class="tab active" data-tab="shortage">🔴 Shortage <span class="n" id="n-short">0</span></div>
        <div class="tab" data-tab="surplus">🟠 Surplus <span class="n" id="n-sur">0</span></div>
        <div class="tab" data-tab="no_move">🟣 No move <span class="n" id="n-nom">0</span></div>
      </div>

      <div class="filter-row">
        <span class="row-count" id="row-count">—</span>
      </div>

      <div class="tbl-wrap">
        <table class="dt">
          <thead>
            <tr>
              <th>Merge</th><th>M CODE</th><th>Brand</th>
              <th>Marketing Line</th><th>Pattern</th>
              <th>Group</th><th>Size</th><th>Inch</th><th>LI/SS</th>
              <th class="r">NSW</th><th class="r">QLD</th><th class="r">VIC</th><th class="r">WA</th>
              <th class="r">Stock</th><th class="r">3M/mo</th><th class="r">MOH</th>
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
        <div class="stat">MOH — on hand only</div>
        <div class="figv" id="m-moh">—</div>
        <div class="stat">MOH — including Factory pipeline</div>
        <div class="figv" id="m-mohplus">—</div>
      </div>
      <div>
        <div class="stat">12-month sales by state</div>
        <div class="chartbox tall"><canvas id="m-chart"></canvas></div>

        <div class="stat" style="margin-top:14px">State breakdown</div>
        <table class="pipe-tbl">
          <thead><tr><th>State</th><th>Stock</th><th>Port</th><th>Water</th>
              <th>Factory</th><th>Pipeline</th><th>3M/mo</th><th>MOH</th><th>MOH +Pipe</th></tr></thead>
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

/* ── KPI tiles ── */
document.getElementById('kpi-sku').textContent   = fmtI(DATA.kpi.sku_total);
document.getElementById('kpi-short').textContent = fmtI(DATA.kpi.sku_shortage);
document.getElementById('kpi-bal').textContent   = fmtI(DATA.kpi.sku_balanced);
document.getElementById('kpi-sur').textContent   = fmtI(DATA.kpi.sku_surplus);
document.getElementById('kpi-nom').textContent   = fmtI(DATA.kpi.sku_no_move);
document.getElementById('kpi-moh').textContent   = DATA.kpi.national_moh != null
    ? FMT_2.format(DATA.kpi.national_moh) : '—';

/* ── State cards ── */
(function renderState() {
    const host = document.getElementById('state-cards');
    let html = '';
    STATES.forEach(s => {
        const st = DATA.state[s];
        html += '<div class="state-card">'
             + '<h3>' + s + ' <span class="m">MOH ' + (st.moh != null ? FMT_2.format(st.moh) : '—') + '</span></h3>'
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
             + '<span class="chip nom">'   + fmtI(st.no_move)  + '</span>'
             + '</span></div>'
             + '</div>';
    });
    host.innerHTML = html;
})();

/* ── Stacked bar charts (marketing line / inch) ── */
function stackedBar(canvas_id, srcObj, order) {
    const labels = order || Object.keys(srcObj);
    const short = labels.map(k => srcObj[k].shortage || 0);
    const bal   = labels.map(k => srcObj[k].balanced || 0);
    const sur   = labels.map(k => srcObj[k].surplus  || 0);
    const nom   = labels.map(k => srcObj[k].no_move  || 0);
    new Chart(document.getElementById(canvas_id), {
        type: 'bar',
        data: { labels, datasets: [
            { label: 'Shortage', data: short, backgroundColor: '#C62828' },
            { label: 'Balanced', data: bal,   backgroundColor: '#66BB6A' },
            { label: 'Surplus',  data: sur,   backgroundColor: '#FB8C00' },
            { label: 'No move',  data: nom,   backgroundColor: '#8E24AA' },
        ]},
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { stacked: true, ticks: { font: { size: 10 }, autoSkip: false, maxRotation: 55, minRotation: 30 } },
                y: { stacked: true, ticks: { font: { size: 10 } } },
            }
        }
    });
}
/* Marketing line ordered by shortage descending — most urgent first */
{
    const src = DATA.by_line;
    const order = Object.keys(src).sort((a,b) => (src[b].shortage||0) - (src[a].shortage||0));
    stackedBar('chart-line', src, order);
}
/* Inch ordered numerically */
{
    const src = DATA.by_inch;
    const order = Object.keys(src).sort((a,b) => {
        const na = parseFloat(a), nb = parseFloat(b);
        if (!isNaN(na) && !isNaN(nb)) return na - nb;
        return a.localeCompare(b);
    });
    stackedBar('chart-inch', src, order);
}

/* ── Multi-select checkbox dropdowns ── */
const filterState = {
    status:  new Set(),
    state:   new Set(),
    brand:   new Set(),
    group:   new Set(),
    line:    new Set(),
    inch:    new Set(),
    pattern: new Set(),
};

function buildFilterOptions() {
    /* Collect option values from every SKU in all_rows so filters
       include every possible value, not just the current tab. */
    const values = { status: [], state: STATES.slice(),
        brand: new Set(), group: new Set(), line: new Set(),
        inch: new Set(), pattern: new Set() };
    values.status = ['shortage','balanced','surplus','no_move'];
    DATA.all_rows.forEach(r => {
        if (r.brand)   values.brand.add(r.brand);
        if (r.group)   values.group.add(r.group);
        if (r.line)    values.line.add(r.line);
        if (r.inch)    values.inch.add(r.inch);
        if (r.pattern) values.pattern.add(r.pattern);
    });
    return {
        status:  values.status,
        state:   values.state,
        brand:   [...values.brand].sort(),
        group:   [...values.group].sort(),
        line:    [...values.line].sort(),
        inch:    [...values.inch].sort((a,b) => parseFloat(a) - parseFloat(b) || a.localeCompare(b)),
        pattern: [...values.pattern].sort(),
    };
}
const OPT = buildFilterOptions();

function renderMsPanel(key) {
    const opts = OPT[key];
    const chosen = filterState[key];
    const panel = document.querySelector('.ms[data-key="'+key+'"] .ms-panel');
    let h = '';
    opts.forEach(v => {
        const id = 'ms-' + key + '-' + v.toString().replace(/[^A-Za-z0-9]/g,'_');
        const checked = chosen.has(v) ? ' checked' : '';
        h += '<label><input type="checkbox" value="' + v + '"' + checked + '> ' + v + '</label>';
    });
    h += '<div class="ms-actions">'
       + '<button data-act="all">All</button>'
       + '<button data-act="none">None</button></div>';
    panel.innerHTML = h;
    panel.querySelectorAll('input[type=checkbox]').forEach(cb => {
        cb.addEventListener('change', () => {
            if (cb.checked) filterState[key].add(cb.value);
            else            filterState[key].delete(cb.value);
            updateMsBtn(key);
            renderTable();
        });
    });
    panel.querySelector('[data-act="all"]').addEventListener('click', (e) => {
        e.stopPropagation();
        opts.forEach(v => filterState[key].add(v));
        renderMsPanel(key); updateMsBtn(key); renderTable();
    });
    panel.querySelector('[data-act="none"]').addEventListener('click', (e) => {
        e.stopPropagation();
        filterState[key].clear();
        renderMsPanel(key); updateMsBtn(key); renderTable();
    });
}

function updateMsBtn(key) {
    const btn = document.querySelector('.ms[data-key="'+key+'"] .ms-btn');
    const chosen = filterState[key];
    const total = OPT[key].length;
    const KEY_LBL = { status:'Status', state:'State', brand:'Brand',
                      group:'Group', line:'Marketing Line', inch:'Rim (inch)',
                      pattern:'Pattern' };
    if (chosen.size === 0) {
        btn.classList.remove('active');
        btn.innerHTML = KEY_LBL[key];
    } else {
        btn.classList.add('active');
        btn.innerHTML = KEY_LBL[key] + '<span class="n">' + chosen.size + '</span>';
    }
}

/* Wire dropdowns */
Object.keys(filterState).forEach(key => {
    renderMsPanel(key);
    updateMsBtn(key);
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
        filterState[key].clear();
        renderMsPanel(key); updateMsBtn(key);
    });
    document.getElementById('fltr-search').value = '';
    renderTable();
}

/* ── SKU table (tabs) ── */
let curTab = 'shortage';

document.getElementById('n-short').textContent = fmtI(DATA.shortage_rows.length);
document.getElementById('n-sur').textContent   = fmtI(DATA.surplus_rows.length);
document.getElementById('n-nom').textContent   = fmtI(DATA.no_move_rows.length);

function activeSource() {
    /* When no status filter is set, honour the tab.  When ANY status
       is chosen in the top filter, source from all_rows and let the
       status filter narrow it — this lets the user combine e.g.
       "Shortage + Surplus" in one view. */
    if (filterState.status.size === 0) return DATA[curTab + '_rows'];
    return DATA.all_rows;
}

function renderTable() {
    const src = activeSource();
    const fq  = (document.getElementById('fltr-search').value || '').trim().toLowerCase();
    const body = document.getElementById('tbl-body');
    const filtered = src.filter(r => {
        if (filterState.status.size  && !filterState.status.has(r.status))   return false;
        if (filterState.brand.size   && !filterState.brand.has(r.brand))     return false;
        if (filterState.group.size   && !filterState.group.has(r.group))     return false;
        if (filterState.line.size    && !filterState.line.has(r.line))       return false;
        if (filterState.inch.size    && !filterState.inch.has(r.inch))       return false;
        if (filterState.pattern.size && !filterState.pattern.has(r.pattern)) return false;
        if (filterState.state.size) {
            /* Show only rows that have stock or demand in AT LEAST ONE
               of the chosen states — helps focus on regional issues. */
            let hit = false;
            filterState.state.forEach(s => {
                if (r.state_stock[s] > 0 || r.state_3m[s] > 0) hit = true;
            });
            if (!hit) return false;
        }
        if (fq) {
            const hay = ((r.description||'') + ' ' + (r.size||'') + ' '
                       + (r.m_code||'') + ' ' + (r.merge_code||'') + ' '
                       + (r.pattern||'') + ' ' + (r.line||'')).toLowerCase();
            if (!hay.includes(fq)) return false;
        }
        return true;
    });

    const shorts = curTab === 'shortage', surplus = curTab === 'surplus';
    body.innerHTML = filtered.slice(0, 500).map(r => {
        const cls_mo = r.status === 'shortage' ? 'short'
                     : (r.status === 'surplus' ? 'sur' : '');
        return '<tr onclick="openModal(' + r.merge_code + ')">'
            + '<td>' + r.merge_code + '</td>'
            + '<td>' + (r.m_code || '—') + '</td>'
            + '<td>' + (r.brand || '—') + '</td>'
            + '<td>' + (r.line || '—') + '</td>'
            + '<td>' + (r.pattern || '—') + '</td>'
            + '<td>' + pill(r.group) + '</td>'
            + '<td>' + (r.size || '—') + '</td>'
            + '<td>' + (r.inch || '—') + '</td>'
            + '<td>' + (r.li ? r.li : '—') + (r.ss ? '/' + r.ss : '') + '</td>'
            + '<td class="r">' + fmtI(r.state_stock.NSW) + '</td>'
            + '<td class="r">' + fmtI(r.state_stock.QLD) + '</td>'
            + '<td class="r">' + fmtI(r.state_stock.VIC) + '</td>'
            + '<td class="r">' + fmtI(r.state_stock.WA)  + '</td>'
            + '<td class="r">' + fmtI(r.total_stock) + '</td>'
            + '<td class="r">' + fmtF(r.total_3m, 1) + '</td>'
            + '<td class="r ' + cls_mo + '">' + (r.moh != null ? fmtF(r.moh, 2) : '—') + '</td>'
            + '</tr>';
    }).join('');
    const suffix = filtered.length > 500 ? ' — showing first 500' : '';
    document.getElementById('row-count').textContent = fmtI(filtered.length) + ' rows' + suffix;
}
document.querySelectorAll('.tab').forEach(t => {
    t.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
        t.classList.add('active');
        curTab = t.dataset.tab;
        renderTable();
    });
});
document.getElementById('fltr-search').addEventListener('input',
    (function() { let t; return function() { clearTimeout(t); t = setTimeout(renderTable, 150); }; })());
renderTable();

/* ── Drill-down modal ── */
let _modalChart = null;
const MONTH_LABELS = ['-12M','-11M','-10M','-9M','-8M','-7M','-6M','-5M','-4M','-3M','-2M','-1M'];

function findRow(merge_code) {
    return DATA.all_rows.find(r => r.merge_code === merge_code);
}

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
    const mohClass = r.moh != null && r.moh < 1 ? 'short'
                   : r.moh != null && r.moh > 4 ? 'sur' : 'bal';
    document.getElementById('m-moh').className     = 'figv ' + mohClass;
    document.getElementById('m-moh').innerHTML     = (r.moh != null ? fmtF(r.moh, 2) : '—') + '<span class="u">months</span>';
    document.getElementById('m-mohplus').innerHTML = (r.moh_plus != null ? fmtF(r.moh_plus, 2) : '—') + '<span class="u">months (Stock + Factory pipeline)</span>';

    /* Monthly chart per state */
    if (_modalChart) { _modalChart.destroy(); _modalChart = null; }
    const stateColour = { NSW: '#1976D2', QLD: '#EF6C00', VIC: '#8E24AA', WA: '#00897B' };
    const datasets = STATES.map(s => ({
        label: s, data: r.history[s], borderColor: stateColour[s],
        backgroundColor: stateColour[s] + '22', borderWidth: 2,
        pointRadius: 3, tension: .25, fill: false,
    }));
    /* National total as a dashed grey line */
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
            scales: {
                y: { beginAtZero: true, ticks: { font: { size: 10 } } },
                x: { ticks: { font: { size: 10 } } },
            }
        }
    });

    /* Per-state pipeline table */
    const rows = STATES.map(s => {
        const pp = r.state_pipe_parts[s];
        const stock = r.state_stock[s];
        const pipe = pp.port + pp.water + pp.fac;
        const dem = r.state_3m[s];
        const smoh = dem > 0 ? (stock / dem) : null;
        const spmoh = dem > 0 ? ((stock + pipe) / dem) : null;
        const cls = smoh != null && smoh < 1 ? 'short'
                  : smoh != null && smoh > 4 ? 'sur' : '';
        return '<tr>'
            + '<td class="st">' + s + '</td>'
            + '<td>' + fmtI(stock) + '</td>'
            + '<td>' + fmtI(pp.port) + '</td>'
            + '<td>' + fmtI(pp.water) + '</td>'
            + '<td>' + fmtI(pp.fac) + '</td>'
            + '<td>' + fmtI(pipe) + '</td>'
            + '<td>' + fmtF(dem, 1) + '</td>'
            + '<td class="' + cls + '">' + (smoh != null ? fmtF(smoh, 2) : '—') + '</td>'
            + '<td>' + (spmoh != null ? fmtF(spmoh, 2) : '—') + '</td>'
            + '</tr>';
    }).join('');
    /* Totals */
    const totStock = STATES.reduce((s,k) => s + r.state_stock[k], 0);
    const totPort  = STATES.reduce((s,k) => s + r.state_pipe_parts[k].port, 0);
    const totWater = STATES.reduce((s,k) => s + r.state_pipe_parts[k].water, 0);
    const totFac   = STATES.reduce((s,k) => s + r.state_pipe_parts[k].fac, 0);
    const totPipe  = totPort + totWater + totFac;
    const totDem   = r.total_3m;
    document.getElementById('m-pipe-tbl').innerHTML = rows
      + '<tr class="tot">'
      + '<td class="st">Total</td>'
      + '<td>' + fmtI(totStock) + '</td>'
      + '<td>' + fmtI(totPort) + '</td>'
      + '<td>' + fmtI(totWater) + '</td>'
      + '<td>' + fmtI(totFac) + '</td>'
      + '<td>' + fmtI(totPipe) + '</td>'
      + '<td>' + fmtF(totDem, 1) + '</td>'
      + '<td>' + (r.moh != null ? fmtF(r.moh, 2) : '—') + '</td>'
      + '<td>' + (r.moh_plus != null ? fmtF(r.moh_plus, 2) : '—') + '</td>'
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
