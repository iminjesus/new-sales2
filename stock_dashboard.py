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
        state_pipe  = {s: sum(_num(r[c - 1]) for c in COL_STATE_PIPELINE[s]) for s in STATES}
        state_3m    = {s: _num(r[COL_STATE_3M[s] - 1]) for s in STATES}
        total_stock = _num(r[COL_TOTAL_STOCK - 1])
        total_all   = _num(r[COL_TOTAL_TOTAL - 1])
        total_3m    = _num(r[COL_TOTAL_3M - 1])

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

        rows_out.append({
            "merge_code":  mc,
            "m_code":      mcode,
            "group":       group or detail.get("group") or "",
            "classification": classif or "",
            "brand":       detail.get("brand", ""),
            "description": detail.get("description", ""),
            "size":        size,
            "inch":        detail.get("inch", ""),
            "li":          detail.get("li", ""),
            "ss":          detail.get("ss", ""),
            "factory":     detail.get("factory", ""),
            "origin":      detail.get("origin", ""),
            "state_stock":    state_stock,
            "state_pipeline": state_pipe,
            "state_3m":       state_3m,
            "total_stock":    total_stock,
            "total_all":      total_all,
            "total_3m":       total_3m,
            "moh":            round(moh, 2) if moh is not None else None,
            "status":         status,
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

    shortage_rows, surplus_rows, no_move_rows = [], [], []

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

        # SKU tables (top offenders)
        entry = {
            "merge_code":  r["merge_code"],
            "m_code":      r["m_code"],
            "description": r["description"] or f"MC {r['merge_code']}",
            "group":       r["group"],
            "brand":       r["brand"],
            "size":        r["size"],
            "total_stock": r["total_stock"],
            "total_3m":    round(r["total_3m"], 2),
            "moh":         r["moh"],
            "state_stock": r["state_stock"],
            "state_3m":    r["state_3m"],
        }
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
            "sku_total":    sum(kpi_status[k] for k in ["shortage", "balanced", "surplus", "no_move"]),
            "sku_shortage": kpi_status["shortage"],
            "sku_balanced": kpi_status["balanced"],
            "sku_surplus":  kpi_status["surplus"],
            "sku_no_move":  kpi_status["no_move"],
            "total_stock":  int(total_stock),
            "total_pipeline": int(total_pipe),
            "total_demand_3m": round(total_3m, 1),
            "national_moh": nat_moh,
        },
        "state":       state_totals,
        "by_group":    by_group,
        "by_inch":     by_inch,
        "by_brand":    by_brand,
        "by_classif":  by_classif,
        "shortage_rows": shortage_rows[:200],   # first 200 — enough for a scan
        "surplus_rows":  surplus_rows[:200],
        "no_move_rows":  no_move_rows[:200],
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
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI', system-ui, sans-serif;
       background: #F4F6F9; color: #263238;
       height: 100vh; display: flex; flex-direction: column; overflow: hidden; }

.hdr { background: linear-gradient(135deg,#0E3F5F,#1F4E79); color:#fff;
       padding: 12px 22px; display:flex; align-items:center; gap:14px; flex-shrink:0;
       box-shadow: 0 2px 8px rgba(0,0,0,0.12); }
.hdr h1 { font-size: 17px; letter-spacing:.4px; }
.hdr .subtitle { font-size: 11px; opacity:.72; }
.hdr .nav { margin-left:auto; display:flex; gap:6px; }
.hdr .nav a { font-size: 12px; padding: 5px 13px; border-radius: 5px; cursor: pointer;
              border: 1px solid rgba(255,255,255,0.45); color: #fff; text-decoration: none; }
.hdr .nav a:hover { background: rgba(255,255,255,0.15); }
.hdr .nav a.active { background: #fff; color: #0E3F5F; font-weight: 600; }

.body { flex:1; display:flex; overflow:hidden; }

/* ── KPI strip ───────────────────────────────────────────── */
.kpi-strip { display: grid; grid-template-columns: repeat(6, 1fr);
             gap: 10px; padding: 12px 16px 4px; }
.kpi { background: #fff; border-radius: 8px; padding: 10px 14px;
       border: 1px solid #E1E5EB; position: relative; overflow: hidden; }
.kpi h4 { font-size: 10.5px; color:#546E7A; text-transform: uppercase;
          letter-spacing:.06em; margin-bottom: 4px; font-weight: 600; }
.kpi .v { font-size: 24px; font-weight: 700; color:#0E3F5F; font-family:'Segoe UI Semibold', monospace; }
.kpi .u { font-size: 10.5px; color:#78909C; margin-left: 4px; }
.kpi.short { border-left: 4px solid #C62828; }
.kpi.short  .v { color:#C62828; }
.kpi.bal   { border-left: 4px solid #2E7D32; }
.kpi.bal    .v { color:#2E7D32; }
.kpi.sur   { border-left: 4px solid #EF6C00; }
.kpi.sur    .v { color:#EF6C00; }
.kpi.nom   { border-left: 4px solid #6A1B9A; }
.kpi.nom    .v { color:#6A1B9A; }

/* ── main grid ───────────────────────────────────────────── */
.wrap { flex:1; overflow-y: auto; padding: 6px 16px 20px; }
.grid { display: grid; grid-template-columns: 340px 1fr; gap: 14px; }

/* State cards on the left */
.state-col { display: flex; flex-direction: column; gap: 10px; }
.state-card { background:#fff; border-radius:8px; border:1px solid #E1E5EB;
              padding: 12px 14px; }
.state-card h3 { font-size: 13px; display:flex; justify-content:space-between;
                 align-items:baseline; margin-bottom: 6px; }
.state-card h3 .m { font-family:monospace; color:#546E7A; font-size:11px; }
.state-row { display:flex; justify-content:space-between; align-items:baseline;
             font-size: 11.5px; padding: 3px 0; border-top: 1px solid #F1F3F5; }
.state-row:first-of-type { border-top: none; }
.state-row .lbl { color:#607D8B; }
.state-row .v   { font-family: monospace; font-weight: 600; color:#263238; }
.chip { display:inline-block; padding: 1px 7px; border-radius: 8px;
        font-size: 10px; font-weight: 600; margin-left: 4px; }
.chip.short { background:#FFEBEE; color:#C62828; }
.chip.sur   { background:#FFF3E0; color:#EF6C00; }
.chip.bal   { background:#E8F5E9; color:#2E7D32; }
.chip.nom   { background:#F3E5F5; color:#6A1B9A; }

.right { display: flex; flex-direction: column; gap: 12px; min-width: 0; }
.card { background:#fff; border-radius:8px; border:1px solid #E1E5EB;
        padding: 12px 14px 14px; }
.card h3 { font-size: 12.5px; color:#0E3F5F;
           border-bottom: 1px solid #ECEFF1; padding-bottom: 6px; margin-bottom: 8px;
           display: flex; justify-content: space-between; align-items: baseline; }
.card h3 .hint { font-size: 10.5px; color:#78909C; font-weight: 400; }
.chartbox { position: relative; height: 210px; }

/* Bar chart legend text */
.tabs { display:flex; gap: 4px; margin-bottom: 6px; }
.tab {  font-size: 11.5px; padding: 5px 12px; border-radius: 5px; cursor: pointer;
        background: #ECEFF1; color: #37474F; border: 1px solid #CFD8DC; }
.tab.active { background: #0E3F5F; color: #fff; border-color: #0E3F5F; }
.tab:hover:not(.active) { background: #CFD8DC; }
.tab .n { display:inline-block; margin-left: 6px; padding: 0 6px;
          border-radius: 8px; background: rgba(255,255,255,0.25); font-size: 10px; }
.tab.active .n { background: rgba(255,255,255,0.3); }
.tab:not(.active) .n { background: rgba(0,0,0,0.08); }

table.dt { width: 100%; border-collapse: collapse; font-size: 11.5px; }
table.dt thead th { background: #ECEFF1; color:#37474F; padding: 6px 8px;
                    text-align: left; position: sticky; top: 0; z-index: 2;
                    border-bottom: 1px solid #CFD8DC; font-size: 11px; }
table.dt tbody td { padding: 5px 8px; border-bottom: 1px solid #F1F3F5;
                    white-space: nowrap; font-size: 11.5px; }
table.dt tbody tr:hover td { background: #F5F7FA; }
table.dt .r { text-align: right; font-family: monospace; }
table.dt .r.short { color:#C62828; font-weight: 700; }
table.dt .r.sur   { color:#EF6C00; font-weight: 700; }
.tbl-wrap { max-height: 340px; overflow-y: auto; }

.pill { display: inline-block; padding: 2px 7px; border-radius: 10px;
        font-size: 10.5px; font-weight: 600; }
.pill.g-SP  { background: #E3F2FD; color: #1565C0; }
.pill.g-HP  { background: #FFF3E0; color: #E65100; }
.pill.g-UHP { background: #FCE4EC; color: #AD1457; }
.pill.g-TBR { background: #E8F5E9; color: #2E7D32; }
.pill.g-other { background: #ECEFF1; color: #455A64; }

.filter-row { display: flex; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; }
.filter-row select, .filter-row input {
    font-size: 12px; padding: 4px 8px; border: 1px solid #CFD8DC;
    border-radius: 4px; background: #fff; }
.filter-row label { font-size: 11px; color: #607D8B;
                    display: flex; align-items: center; gap: 4px; }

/* Legend inline (dot markers) */
.legend { display: flex; gap: 12px; font-size: 11px; margin-top: 6px; }
.legend .dot { display: inline-block; width: 9px; height: 9px;
               border-radius: 50%; margin-right: 4px; vertical-align: -1px; }
.legend .dot.short { background: #C62828; }
.legend .dot.bal   { background: #2E7D32; }
.legend .dot.sur   { background: #EF6C00; }
.legend .dot.nom   { background: #6A1B9A; }
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

<!-- KPI strip -->
<div class="kpi-strip">
  <div class="kpi"><h4>Active SKUs</h4><span class="v" id="kpi-sku">—</span></div>
  <div class="kpi short"><h4>Shortage</h4><span class="v" id="kpi-short">—</span><span class="u">SKUs</span></div>
  <div class="kpi bal"><h4>Balanced</h4><span class="v" id="kpi-bal">—</span><span class="u">SKUs</span></div>
  <div class="kpi sur"><h4>Surplus</h4><span class="v" id="kpi-sur">—</span><span class="u">SKUs</span></div>
  <div class="kpi nom"><h4>No move</h4><span class="v" id="kpi-nom">—</span><span class="u">SKUs</span></div>
  <div class="kpi"><h4>National MOH</h4><span class="v" id="kpi-moh">—</span><span class="u">months</span></div>
</div>

<!-- main body -->
<div class="wrap">
<div class="grid">

  <!-- LEFT: state cards -->
  <div class="state-col">
    <div class="card"><h3>Stock across states <span class="hint">as of {{ meta.mtime }}</span></h3>
      <div id="state-cards"></div>
    </div>
  </div>

  <!-- RIGHT: analytics -->
  <div class="right">

    <!-- Charts row -->
    <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 12px;">
      <div class="card">
        <h3>By product Group <span class="hint">stacked count of SKUs</span></h3>
        <div class="chartbox"><canvas id="chart-group"></canvas></div>
        <div class="legend">
          <span><span class="dot short"></span>Shortage</span>
          <span><span class="dot bal"></span>Balanced</span>
          <span><span class="dot sur"></span>Surplus</span>
          <span><span class="dot nom"></span>No move</span>
        </div>
      </div>
      <div class="card">
        <h3>By Rim size (inch) <span class="hint">stacked count of SKUs</span></h3>
        <div class="chartbox"><canvas id="chart-inch"></canvas></div>
      </div>
    </div>

    <!-- SKU tables -->
    <div class="card">
      <h3>SKU drill-down <span class="hint">click a tab to switch list</span></h3>
      <div class="tabs">
        <div class="tab active" data-tab="shortage">🔴 Shortage <span class="n" id="n-short">0</span></div>
        <div class="tab" data-tab="surplus">🟠 Surplus <span class="n" id="n-sur">0</span></div>
        <div class="tab" data-tab="no_move">🟣 No move <span class="n" id="n-nom">0</span></div>
      </div>

      <div class="filter-row">
        <label>State
          <select id="fltr-state">
            <option value="">All</option>
            <option>NSW</option><option>QLD</option><option>VIC</option><option>WA</option>
          </select>
        </label>
        <label>Group
          <select id="fltr-group">
            <option value="">All</option>
          </select>
        </label>
        <label>Search
          <input id="fltr-search" type="text" placeholder="SKU / description / size…" style="width: 220px">
        </label>
        <span id="row-count" style="margin-left:auto; align-self:center; color:#78909C; font-size:11px"></span>
      </div>

      <div class="tbl-wrap">
        <table class="dt">
          <thead>
            <tr>
              <th>Merge</th><th>M CODE</th><th>Group</th><th>Size</th>
              <th>Description</th>
              <th class="r">NSW</th><th class="r">QLD</th><th class="r">VIC</th><th class="r">WA</th>
              <th class="r">Stock</th><th class="r">3M dem/mo</th><th class="r">MOH</th>
            </tr>
          </thead>
          <tbody id="tbl-body"></tbody>
        </table>
      </div>
    </div>

  </div>
</div>
</div>

<script>
const DATA = {{ data_json | safe }};
const STATES = ["NSW","QLD","VIC","WA"];

function fmtN(n) {
    if (n == null || n === '') return '—';
    if (typeof n !== 'number') n = +n;
    if (isNaN(n)) return '—';
    return n.toLocaleString('en-US', { maximumFractionDigits: 0 });
}
function fmtF(n, d) {
    if (n == null || n === '') return '—';
    if (typeof n !== 'number') n = +n;
    if (isNaN(n)) return '—';
    return n.toFixed(d != null ? d : 1);
}

/* ── KPI tiles ─────────────────────────────── */
document.getElementById('kpi-sku').textContent   = fmtN(DATA.kpi.sku_total);
document.getElementById('kpi-short').textContent = fmtN(DATA.kpi.sku_shortage);
document.getElementById('kpi-bal').textContent   = fmtN(DATA.kpi.sku_balanced);
document.getElementById('kpi-sur').textContent   = fmtN(DATA.kpi.sku_surplus);
document.getElementById('kpi-nom').textContent   = fmtN(DATA.kpi.sku_no_move);
document.getElementById('kpi-moh').textContent   = DATA.kpi.national_moh != null
    ? DATA.kpi.national_moh.toFixed(2) : '—';

/* ── State cards ────────────────────────────── */
(function renderState() {
    const host = document.getElementById('state-cards');
    let html = '';
    STATES.forEach(s => {
        const st = DATA.state[s];
        html += '<div class="state-card">'
             + '<h3>' + s + ' <span class="m">MOH ' + (st.moh != null ? st.moh.toFixed(2) : '—') + '</span></h3>'
             + '<div class="state-row"><span class="lbl">Stock on hand</span>'
             + '<span class="v">' + fmtN(st.stock) + '</span></div>'
             + '<div class="state-row"><span class="lbl">In pipeline</span>'
             + '<span class="v">' + fmtN(st.pipeline) + '</span></div>'
             + '<div class="state-row"><span class="lbl">3M avg demand / mo</span>'
             + '<span class="v">' + fmtF(st.demand_3m, 1) + '</span></div>'
             + '<div class="state-row"><span class="lbl">SKU status</span>'
             + '<span class="v">'
             + '<span class="chip short">' + fmtN(st.shortage) + '</span>'
             + '<span class="chip bal">'   + fmtN(st.balanced) + '</span>'
             + '<span class="chip sur">'   + fmtN(st.surplus)  + '</span>'
             + '<span class="chip nom">'   + fmtN(st.no_move)  + '</span>'
             + '</span></div>'
             + '</div>';
    });
    host.innerHTML = html;
})();

/* ── Group / Inch stacked bar charts ────────── */
function stackedBar(canvas_id, srcObj, sortKey) {
    const labels = Object.keys(srcObj).sort((a, b) => {
        const va = srcObj[a][sortKey] || 0;
        const vb = srcObj[b][sortKey] || 0;
        return vb - va;
    });
    const short = labels.map(k => srcObj[k].shortage || 0);
    const bal   = labels.map(k => srcObj[k].balanced || 0);
    const sur   = labels.map(k => srcObj[k].surplus  || 0);
    const nom   = labels.map(k => srcObj[k].no_move  || 0);
    new Chart(document.getElementById(canvas_id), {
        type: 'bar',
        data: {
            labels,
            datasets: [
                { label: 'Shortage', data: short, backgroundColor: '#C62828' },
                { label: 'Balanced', data: bal,   backgroundColor: '#66BB6A' },
                { label: 'Surplus',  data: sur,   backgroundColor: '#FB8C00' },
                { label: 'No move',  data: nom,   backgroundColor: '#8E24AA' },
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { stacked: true, ticks: { font: { size: 10 } } },
                y: { stacked: true, ticks: { font: { size: 10 } } },
            }
        }
    });
}
stackedBar('chart-group', DATA.by_group, 'shortage');
// Order inch numerically so 12 → 14 → 16 → 18 not alphabetical
(function(){
    const sd = DATA.by_inch;
    const labels = Object.keys(sd).sort((a, b) => {
        const na = parseFloat(a), nb = parseFloat(b);
        if (!isNaN(na) && !isNaN(nb)) return na - nb;
        return a.localeCompare(b);
    });
    const src = {};
    labels.forEach(k => src[k] = sd[k]);
    stackedBar('chart-inch', src, '__none__');   // preserve inch order
    // Rebuild inch chart with pre-ordered dict — quick monkey by
    // relying on stackedBar reading Object.keys in insertion order:
    // labels above respect insertion order in modern JS engines.
})();

/* ── SKU tables (tabs) ─────────────────────── */
let curTab = 'shortage';

/* Populate the Group filter dropdown from every table's rows */
(function populateGroupFilter() {
    const groups = new Set();
    ['shortage_rows','surplus_rows','no_move_rows'].forEach(k => {
        DATA[k].forEach(r => { if (r.group) groups.add(r.group); });
    });
    const sel = document.getElementById('fltr-group');
    [...groups].sort().forEach(g => {
        const o = document.createElement('option');
        o.value = g; o.textContent = g;
        sel.appendChild(o);
    });
})();

document.getElementById('n-short').textContent = DATA.shortage_rows.length;
document.getElementById('n-sur').textContent   = DATA.surplus_rows.length;
document.getElementById('n-nom').textContent   = DATA.no_move_rows.length;

function renderTable() {
    const src = DATA[curTab + '_rows'];
    const fs  = document.getElementById('fltr-state').value;
    const fg  = document.getElementById('fltr-group').value;
    const fq  = (document.getElementById('fltr-search').value || '').trim().toLowerCase();
    const body = document.getElementById('tbl-body');
    const shorts = curTab === 'shortage', surplus = curTab === 'surplus';
    let n = 0;
    const html = src.filter(r => {
        if (fs && r.state_stock[fs] == null) return false;
        if (fg && r.group !== fg) return false;
        if (fq) {
            const hay = ((r.description||'') + ' ' + (r.size||'') + ' '
                       + (r.m_code||'') + ' ' + (r.merge_code||'')).toLowerCase();
            if (!hay.includes(fq)) return false;
        }
        return true;
    }).map(r => {
        n++;
        const cls_mo = shorts ? 'short' : (surplus ? 'sur' : '');
        return '<tr>'
            + '<td>' + r.merge_code + '</td>'
            + '<td>' + (r.m_code || '—') + '</td>'
            + '<td><span class="pill g-' + (r.group||'other').replace(/[^A-Za-z]/g,'') + '">' + (r.group||'—') + '</span></td>'
            + '<td>' + (r.size || '—') + '</td>'
            + '<td>' + (r.description || '—') + '</td>'
            + '<td class="r">' + fmtN(r.state_stock.NSW) + '</td>'
            + '<td class="r">' + fmtN(r.state_stock.QLD) + '</td>'
            + '<td class="r">' + fmtN(r.state_stock.VIC) + '</td>'
            + '<td class="r">' + fmtN(r.state_stock.WA)  + '</td>'
            + '<td class="r">' + fmtN(r.total_stock) + '</td>'
            + '<td class="r">' + fmtF(r.total_3m, 1) + '</td>'
            + '<td class="r ' + cls_mo + '">' + (r.moh != null ? r.moh.toFixed(2) : '—') + '</td>'
            + '</tr>';
    }).join('');
    body.innerHTML = html;
    document.getElementById('row-count').textContent = n + ' rows';
}
document.querySelectorAll('.tab').forEach(t => {
    t.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
        t.classList.add('active');
        curTab = t.dataset.tab;
        renderTable();
    });
});
document.getElementById('fltr-state').addEventListener('change', renderTable);
document.getElementById('fltr-group').addEventListener('change', renderTable);
document.getElementById('fltr-search').addEventListener('input',
    (function() { let t; return function() { clearTimeout(t); t = setTimeout(renderTable, 150); }; })());
renderTable();
</script>
</body>
</html>
"""
