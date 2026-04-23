"""
price_compare.py — Tyre Price Comparison Dashboard
http://127.0.0.1:5000/price

Left  : summary table (latest month)
Right :
  [Size buttons]   ← global filter, applies to both views
  [By Store]  → brand sub-buttons → 3 store lines (Tempe/BJ/JAX) for selected brand + size
  [By Brand]  → store sub-buttons → multiple brand lines for selected store + size
  [chart area]
"""
import glob, json, os, re, sys
from datetime import datetime
from flask import Flask, render_template_string

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_BASE = os.path.dirname(os.path.abspath(__file__))

from price_compare_jax import (
    load_csv,
    build_tempe_lookup, build_bj_lookup, build_jax_lookup, build_tw_lookup,
    best_tempe, best_bj, best_jax, best_tw, best_twi,
    BRANDS, SIZE_CATEGORY, BRAND_COLOURS, ROW_FILLS,
)

app = Flask(__name__)


# ── helpers ───────────────────────────────────────────────────────────────────

_MONTH_ABBR = {
    'JAN':'01','JANUARY':'01','FEB':'02','FEBRUARY':'02','MAR':'03','MARCH':'03',
    'APR':'04','APRIL':'04','MAY':'05','JUN':'06','JUNE':'06',
    'JUL':'07','JULY':'07','AUG':'08','AUGUST':'08','SEP':'09','SEPTEMBER':'09',
    'OCT':'10','OCTOBER':'10','NOV':'11','NOVEMBER':'11','DEC':'12','DECEMBER':'12',
}

def _month_key(fp):
    name = os.path.basename(fp).upper()
    # Format 1: YYYYMMDD anywhere in name  (e.g. Tempe_20260401_1439.csv)
    m = re.search(r'(\d{4})(\d{2})\d{2}', name)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    # Format 2: MonthName_YYYY  (e.g. Tempe_Apr_2026.csv, BobJane_March_2026.csv)
    yr = re.search(r'(\d{4})', name)
    if yr:
        year = yr.group(1)
        # find longest matching month name
        best_len, best_num = 0, None
        for key, num in _MONTH_ABBR.items():
            if key in name and len(key) > best_len:
                best_len, best_num = len(key), num
        if best_num:
            return f"{year}{best_num}"
    return "000000"

def _month_label(yyyymm):
    try:
        return datetime.strptime(str(yyyymm), "%Y%m").strftime("%b %Y")
    except Exception:
        return str(yyyymm)

def _latest_per_month(pattern):
    by_month = {}
    for f in sorted(glob.glob(os.path.join(_BASE, pattern))):
        by_month[_month_key(f)] = f
    return by_month


# ── data loading ──────────────────────────────────────────────────────────────

def load_all_months():
    t_f  = _latest_per_month("Tempe_*.csv")
    bj_f = _latest_per_month("BobJane_*.csv")
    jx_f = _latest_per_month("JAX_*.csv")
    tw_f = _latest_per_month("[Tt]empeweborder_*.csv")
    all_months = sorted(set(list(t_f) + list(bj_f) + list(jx_f) + list(tw_f)))
    return {
        mk: {
            "t_rows":  load_csv(t_f[mk])  if mk in t_f  else [],
            "bj_rows": load_csv(bj_f[mk]) if mk in bj_f else [],
            "jx_rows": load_csv(jx_f[mk]) if mk in jx_f else [],
            "tw_rows": load_csv(tw_f[mk]) if mk in tw_f else [],
        }
        for mk in all_months
    }


def build_data(monthly):
    months       = sorted(monthly.keys())
    month_labels = [_month_label(m) for m in months]
    sizes        = list(SIZE_CATEGORY.keys())
    abbrs        = list(BRANDS.keys())

    store_avg = {"tempe": [], "bj": [], "jax": [], "tw": [], "twi": []}
    # size_data[size][abbr][store] = [price_per_month, ...]
    size_data = {s: {a: {"tempe": [], "bj": [], "jax": [], "tw": [], "twi": []} for a in abbrs} for s in sizes}

    latest       = months[-1] if months else None
    summary_rows = []

    for mk in months:
        md     = monthly[mk]
        t_lk   = build_tempe_lookup(md["t_rows"])
        bj_lk  = build_bj_lookup(md["bj_rows"])
        jx_lk  = build_jax_lookup(md["jx_rows"])
        tw_lk  = build_tw_lookup(md["tw_rows"])
        tp, bp, jp, twp, twip2 = [], [], [], [], []

        for size in sizes:
            for abbr in abbrs:
                t_desc, t_cost, t_price      = best_tempe(size, abbr, t_lk)
                bj_desc, bj_price, *_        = best_bj(size, abbr, bj_lk, t_desc)
                jx_desc, jx_price, *_        = best_jax(size, abbr, jx_lk, t_desc)
                _tw_desc, _tw_cost, tw_price  = best_tw(size, abbr, tw_lk)
                _twi_desc, _, twi_price          = best_twi(size, abbr, tw_lk)

                size_data[size][abbr]["tempe"].append(t_price)
                size_data[size][abbr]["bj"].append(bj_price)
                size_data[size][abbr]["jax"].append(jx_price)
                size_data[size][abbr]["tw"].append(tw_price)
                size_data[size][abbr]["twi"].append(twi_price)

                if t_price:   tp.append(t_price)
                if bj_price:  bp.append(bj_price)
                if jx_price:  jp.append(jx_price)
                if tw_price:  twp.append(tw_price)
                if twi_price: twip2.append(twi_price)

                if mk == latest and (t_price or bj_price or jx_price):
                    cat = SIZE_CATEGORY.get(size, ("?", "?", "?"))
                    summary_rows.append({
                        "size":      size,
                        "brand":     f"{abbr} {BRANDS[abbr]}",
                        "abbr":      abbr,
                        "t_price":   t_price,
                        "bj_price":  bj_price,
                        "jax_price": jx_price,
                        "category":  f"{cat[0]}/{cat[1]}",
                    })

        store_avg["tempe"].append(round(sum(tp)/len(tp),   2) if tp  else None)
        store_avg["bj"].append(round(sum(bp)/len(bp),     2) if bp  else None)
        store_avg["jax"].append(round(sum(jp)/len(jp),    2) if jp  else None)
        store_avg["tw"].append(round(sum(twp)/len(twp),   2) if twp   else None)
        store_avg["twi"].append(round(sum(twip2)/len(twip2), 2) if twip2 else None)

    # brand_size_data: only combos that have at least one real price
    brand_size_data = {}
    for abbr in abbrs:
        for size in sizes:
            d = size_data[size][abbr]
            if any(p is not None for p in d["tempe"] + d["bj"] + d["jax"] + d["tw"] + d["twi"]):
                brand_size_data.setdefault(abbr, {})[size] = d

    return {
        "months":          month_labels,
        "store_avg":       store_avg,
        "size_data":       size_data,
        "brand_size_data": brand_size_data,
        "summary_rows":    summary_rows,
        "sizes_with_data": sorted({
            size for size in sizes
            for abbr in abbrs
            if any(p is not None for p in
                   size_data[size][abbr]["tempe"] +
                   size_data[size][abbr]["bj"] +
                   size_data[size][abbr]["jax"] +
                   size_data[size][abbr]["tw"] +
                   size_data[size][abbr]["twi"])
        }, key=lambda s: sizes.index(s)),
    }


# ── HTML template ─────────────────────────────────────────────────────────────

_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Tyre Price Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: system-ui, sans-serif; background: #f0f2f5;
       height: 100vh; display: flex; flex-direction: column; overflow: hidden; }

.hdr { background: #1F4E79; color: #fff; padding: 11px 18px; flex-shrink: 0;
       display: flex; align-items: center; gap: 14px; }
.hdr h1 { font-size: 16px; }
.hdr span { font-size: 11px; opacity: .65; }
.hdr .nav { margin-left: auto; display: flex; gap: 6px; }
.hdr .nav a { font-size: 12px; padding: 4px 11px; border-radius: 4px; cursor: pointer;
              border: 1px solid rgba(255,255,255,0.5); color: #fff; text-decoration: none;
              background: transparent; }
.hdr .nav a:hover { background: rgba(255,255,255,0.15); }
.hdr .nav a.active { background: #fff; color: #1F4E79; font-weight: 600; }

.body { display: flex; flex: 1; overflow: hidden; }

/* ── LEFT panel — comparison tables ── */
.left { width: 40%; border-right: 1px solid #d0d5dd;
        display: flex; flex-direction: column; overflow: hidden; background: #fff; }
.left-title { background: #1F4E79; color: #fff; padding: 7px 14px;
              font-size: 11.5px; font-weight: 700; flex-shrink: 0; letter-spacing: .02em; }
#chart-table-wrap { flex: 1; overflow-y: auto; padding: 6px 0; }

/* ── RIGHT panel ── */
.right { width: 60%; display: flex; flex-direction: column; overflow: hidden; }

/* Button panel */
.btn-panel { flex-shrink: 0; padding: 10px 14px 8px; background: #f5f7fa;
             border-bottom: 1px solid #dde1e7; }

.btn-row { display: flex; align-items: flex-start; gap: 8px; margin-bottom: 7px; }
.btn-row:last-child { margin-bottom: 0; }

.row-label { font-size: 10px; font-weight: 700; color: #1F4E79; letter-spacing: .03em;
             min-width: 70px; padding-top: 5px; flex-shrink: 0; text-transform: uppercase; }

.btns { display: flex; flex-wrap: wrap; gap: 4px; }

/* buttons */
.btn {
    font-size: 11px; padding: 4px 9px; border-radius: 4px; cursor: pointer;
    border: 1.5px solid #c5ccd8; background: #fff; color: #444;
    transition: background .12s, color .12s, border-color .12s;
    white-space: nowrap; line-height: 1.4;
}
.btn:hover  { background: #eef1f8; border-color: #1F4E79; color: #1F4E79; }
.btn.active { background: #1F4E79 !important; color: #fff !important;
              border-color: #1F4E79 !important; }

/* mode buttons are slightly bigger */
.btn.mode { font-size: 12px; padding: 5px 14px; font-weight: 600; }

/* divider between size row and mode rows */
.divider { border: none; border-top: 1px solid #dde1e7; margin: 6px 0; }

/* sub-buttons row animation */
#sub-row { display: none; }

/* ── Chart area ── */
.chart-panel { flex: 1; padding: 12px 14px; display: flex;
               flex-direction: column; min-height: 0; overflow: hidden; }
.chart-title { font-size: 12.5px; font-weight: 600; color: #223;
               margin-bottom: 8px; }
.no-data { color: #e53; font-size: 13px; padding-top: 20px; text-align: center; }
.chart-wrap { flex: 1; min-height: 220px; position: relative; }
.chart-wrap canvas { position: absolute; inset: 0; width:100%!important; height:100%!important; }

.ctable { width: 100%; border-collapse: collapse; font-size: 13.5px; }
.ctable thead th { padding: 7px 12px; position: sticky; top: 0;
                   z-index: 3; text-align: center; white-space: nowrap; font-weight: 700; }
.ctable thead th.th-month { background: #1F4E79; color: #fff; }
.ctable thead th:not(.th-month) { background: #f0f4f9; border-bottom: 2px solid #c5ccd8; }
.ctable tbody td { padding: 7px 12px; border-bottom: 1px solid #eee; white-space: nowrap; font-size: 13.5px; }
.ctable tbody tr:hover td { background: #f0f4fa; }
.ctable .r { text-align: right; font-family: monospace; }
.ctable.idxtbl thead th.th-month { background: #37474F; }
.idx-title { font-size: 12px; font-weight: 700; color: #37474F;
             padding: 12px 12px 4px 12px; letter-spacing: .03em; text-transform: uppercase; }
</style>
</head>
<body>

<div class="hdr">
  <h1>Tyre Price Comparison Dashboard</h1>
  <span>Tempe &nbsp;|&nbsp; Bob Jane &nbsp;|&nbsp; JAX &nbsp;&middot;&nbsp;
        {{ month_count }} month{{ 's' if month_count != 1 else '' }} of data</span>
  <nav class="nav">
    <a href="/">Graph View</a>
    <a href="/map">Map View</a>
    <a href="/stock">Stock</a>
    <a href="/rebate">Rebate</a>
    <a href="/price" class="active">Price</a>
  </nav>
</div>

<div class="body">

  <!-- ── LEFT: comparison tables ── -->
  <div class="left">
    <div class="left-title">Comparison Base table for popular size</div>
    <div id="chart-table-wrap"></div>
  </div>

  <!-- ── RIGHT: controls + chart ── -->
  <div class="right">

    <div class="btn-panel">

      <!-- Row 1: Size filter (global) -->
      <div class="btn-row">
        <span class="row-label">Size</span>
        <div class="btns" id="size-btns"></div>
      </div>

      <hr class="divider">

      <!-- Row 2: View mode -->
      <div class="btn-row">
        <span class="row-label">View</span>
        <div class="btns">
          <button class="btn mode" id="btn-store" onclick="onModeStore(this)">By Store</button>
          <button class="btn mode" id="btn-brand" onclick="onModeBrand(this)">By Brand</button>
        </div>
      </div>

      <!-- Row 3: Sub-buttons (brand list or store list) -->
      <div class="btn-row" id="sub-row">
        <span class="row-label" id="sub-label"></span>
        <div class="btns" id="sub-btns"></div>
      </div>

    </div>

    <!-- Chart -->
    <div class="chart-panel">
      <div class="chart-title" id="chart-title">Select a size and view mode above</div>
      <div class="chart-wrap" id="chart-wrap">
        <canvas id="main-chart"></canvas>
      </div>
    </div>

  </div>
</div>

<script>
const D      = {{ chart_json | safe }};
const BRANDS = {{ brands_json | safe }};
const BCOLORS= {{ bcolors_json | safe }};
const SIZES  = {{ sizes_json | safe }};
const months = D.months;

const SC = { tempe:'#2196F3', bj:'#4CAF50', jax:'#FF9800', tw:'#9C27B0', twi:'#E040FB' };
const SL = { tempe:'Tempe',   bj:'Bob Jane', jax:'JAX',   tw:'TempeWOS Sell Out', twi:'TempeWOS Sell In' };
const SD = { tempe:[], bj:[5,5], jax:[2,3], tw:[8,3], twi:[3,3] };
const PALETTE = ['#C8102E','#E31837','#FFA500','#003087','#00539B',
                 '#E8001A','#FF6600','#E4002B','#003DA5','#555555'];

const baseOpts = {
    responsive:true, maintainAspectRatio:false,
    clip: false,
    plugins:{
        legend:{ position:'bottom', labels:{ boxWidth:11, font:{size:10}, padding:8 } },
        tooltip:{ callbacks:{ label: ctx => ' $' + (ctx.raw ?? '—') } }
    },
    scales:{
        x:{ offset:true, ticks:{ font:{size:10}, padding:8 } },
        y:{
            beginAtZero: false,
            ticks:{ font:{size:10}, callback: v => '$'+v },
            grid:{ color:'#f0f0f0' },
        }
    },
    elements:{
        point:{
            radius: 7,
            hoverRadius: 9,
            borderWidth: 2,
            borderColor: '#fff',
            hitRadius: 12,
        }
    }
};

/* Compute Y min/max from datasets before chart creation */
function _yRange(datasets) {
    const vals = datasets.flatMap(ds => ds.data).filter(v => v !== null && v !== undefined);
    if (!vals.length) return {};
    const lo = Math.min(...vals), hi = Math.max(...vals);
    const pad = Math.max((hi - lo) * 0.2, 20);   // 20% padding, min $20
    return { min: Math.floor(lo - pad), max: Math.ceil(hi + pad) };
}

/* Draw price labels above each point after chart renders */
const _labelPlugin = {
    id: 'pointLabels',
    afterDatasetsDraw(chart) {
        const ctx = chart.ctx;
        ctx.save();
        ctx.font = 'bold 10px system-ui, sans-serif';
        ctx.textAlign = 'center';
        chart.data.datasets.forEach((ds, di) => {
            const meta = chart.getDatasetMeta(di);
            meta.data.forEach((pt, i) => {
                const val = ds.data[i];
                if (val === null || val === undefined) return;
                const color = Array.isArray(ds.borderColor) ? ds.borderColor[i] : ds.borderColor;
                ctx.fillStyle = color || '#333';
                ctx.fillText('$' + val, pt.x, pt.y - 12);
            });
        });
        ctx.restore();
    }
};
Chart.register(_labelPlugin);


/* ── Table below chart ───────────────────────────────── */
function _renderTable() {
    const wrap = document.getElementById('chart-table-wrap');
    if (!wrap || !selectedMode || !selectedSubKey) return;
    if (selectedMode === 'store') _renderStoreTable(wrap);
    else                          _renderBrandTable(wrap);
}

function _renderStoreTable(wrap) {
    const abbr = selectedSubKey;
    const stores = [
        { key:'tempe', label:'Tempe',              c: SC.tempe },
        { key:'bj',    label:'Bob Jane',           c: SC.bj   },
        { key:'jax',   label:'JAX',                c: SC.jax  },
        { key:'tw',    label:'TempeWOS Sell Out',   c: SC.tw   },
        { key:'twi',   label:'TempeWOS Sell In',    c: SC.twi  },
    ];
    const storePrices = stores.map(function(s) {
        if (abbr === 'ALL') return _avgAllBrands(selectedSize, s.key);
        if (selectedSize === 'ALL') return _avgAllSizes(abbr, s.key);
        const bsd = (D.brand_size_data[abbr] || {})[selectedSize];
        return bsd ? (bsd[s.key] || []) : [];
    });

    // ── Price table ──
    let html = '<table class="ctable"><thead><tr><th class="th-month">Store</th>';
    months.forEach(function(m) { html += '<th class="th-month">' + m + '</th>'; });
    html += '</tr></thead><tbody>';
    stores.forEach(function(s, si) {
        const hasData = storePrices[si].some(function(p){ return p !== null && p !== undefined; });
        if (!hasData) return;
        html += '<tr><td style="color:' + s.c + ';font-weight:700">' + s.label + '</td>';
        storePrices[si].forEach(function(p) {
            html += '<td class="r">' + (p !== null && p !== undefined ? '$' + p : '—') + '</td>';
        });
        html += '</tr>';
    });
    html += '</tbody></table>';

    // ── JAX Index table (JAX = 100) ──
    const jaxIdx = stores.findIndex(function(s) { return s.key === 'jax'; });
    const jaxPrices = jaxIdx >= 0 ? storePrices[jaxIdx] : [];
    html += '<div class="idx-title">JAX Index &nbsp;(JAX = 100)</div>';
    html += '<table class="ctable idxtbl"><thead><tr><th class="th-month">Store</th>';
    months.forEach(function(m) { html += '<th class="th-month">' + m + '</th>'; });
    html += '</tr></thead><tbody>';
    stores.forEach(function(s, si) {
        const hasData = storePrices[si].some(function(p){ return p !== null && p !== undefined; });
        if (!hasData) return;
        html += '<tr><td style="color:' + s.c + ';font-weight:700">' + s.label + '</td>';
        storePrices[si].forEach(function(p, i) {
            const base = jaxPrices[i];
            let cell;
            if (base == null) {
                cell = '—';                          // JAX blank → all blank
            } else if (p == null) {
                cell = '—';                          // store blank → blank
            } else {
                const idx = Math.round(p / base * 100);
                const clr = idx < 98 ? '#388E3C' : (idx > 102 ? '#C62828' : '#37474F');
                cell = '<b style="color:' + clr + '">' + idx + '</b>';
            }
            html += '<td class="r">' + cell + '</td>';
        });
        html += '</tr>';
    });
    html += '</tbody></table>';
    wrap.innerHTML = html;
}

function _renderBrandTable(wrap) {
    const store = selectedSubKey;
    const entries = Object.keys(BRANDS).map(function(abbr) {
        const prices = selectedSize === 'ALL'
            ? _avgAllSizes(abbr, store)
            : ((D.size_data[selectedSize] || {})[abbr] || {})[store];
        return { abbr: abbr, prices: prices || [] };
    }).filter(function(e) {
        return e.prices.some(function(p){ return p !== null && p !== undefined; });
    });

    // ── Price table ──
    let html = '<table class="ctable"><thead><tr><th class="th-month">Brand</th>';
    months.forEach(function(m) { html += '<th class="th-month">' + m + '</th>'; });
    html += '</tr></thead><tbody>';
    entries.forEach(function(e) {
        const c = '#' + (BCOLORS[e.abbr] || '555555');
        html += '<tr><td style="color:' + c + ';font-weight:700">' + e.abbr + ' ' + (BRANDS[e.abbr] || '') + '</td>';
        e.prices.forEach(function(p) {
            html += '<td class="r">' + (p !== null && p !== undefined ? '$' + p : '—') + '</td>';
        });
        html += '</tr>';
    });
    html += '</tbody></table>';

    // ── HK Index table (HK = 100) ──
    const hkEntry = entries.find(function(e) { return e.abbr === 'HK'; });
    const hkPrices = hkEntry ? hkEntry.prices : [];
    html += '<div class="idx-title">HK Index &nbsp;(Hankook = 100)</div>';
    html += '<table class="ctable idxtbl"><thead><tr><th class="th-month">Brand</th>';
    months.forEach(function(m) { html += '<th class="th-month">' + m + '</th>'; });
    html += '</tr></thead><tbody>';
    entries.forEach(function(e) {
        const c = '#' + (BCOLORS[e.abbr] || '555555');
        html += '<tr><td style="color:' + c + ';font-weight:700">' + e.abbr + ' ' + (BRANDS[e.abbr] || '') + '</td>';
        e.prices.forEach(function(p, i) {
            const base = hkPrices[i];
            let cell;
            if (base == null) {
                cell = '—';                          // HK blank → all blank
            } else if (p == null) {
                cell = '—';                          // brand blank → blank
            } else {
                const idx = Math.round(p / base * 100);
                const clr = idx < 98 ? '#388E3C' : (idx > 102 ? '#C62828' : '#37474F');
                cell = '<b style="color:' + clr + '">' + idx + '</b>';
            }
            html += '<td class="r">' + cell + '</td>';
        });
        html += '</tr>';
    });
    html += '</tbody></table>';
    wrap.innerHTML = html;
}

/* ── state ────────────────────────────────────────────── */
let selectedSize    = null;
let selectedMode    = null;   // 'store' | 'brand'
let selectedSubKey  = null;   // abbr (store mode) | 'tempe'/'bj'/'jax' (brand mode)
let _chart          = null;
let _activeSizeBtn  = null;
let _activeModeBtn  = null;
let _activeSubBtn   = null;

/* ── chart helpers ────────────────────────────────────── */
function _render(title, datasets) {
    document.getElementById('chart-title').textContent = title;
    if (_chart) { _chart.destroy(); _chart = null; }

    const yr   = _yRange(datasets);
    const opts = JSON.parse(JSON.stringify(baseOpts));  // deep-clone so baseOpts is not mutated
    if (yr.min !== undefined) {
        opts.scales.y.min = yr.min;
        opts.scales.y.max = yr.max;
    }

    _chart = new Chart(document.getElementById('main-chart'), {
        type: 'line', data: { labels: months, datasets }, options: opts
    });
}

function _noData(msg) {
    document.getElementById('chart-title').textContent = msg;
    if (_chart) { _chart.destroy(); _chart = null; }
    const wrap = document.getElementById('chart-table-wrap');
    if (wrap) wrap.innerHTML = '';
}

/* ── ALL sizes: average across every size ──────────────── */
function _avgAllSizes(abbr, store) {
    const nM = months.length;
    const sums = new Array(nM).fill(0), counts = new Array(nM).fill(0);
    const storeKey = store;  // 'tempe','bj','jax','tw'
    Object.values(D.size_data).forEach(function(sizeAbbrs) {
        const prices = ((sizeAbbrs[abbr] || {})[storeKey]) || [];
        prices.forEach(function(p, i) {
            if (p !== null && p !== undefined) { sums[i] += p; counts[i]++; }
        });
    });
    return sums.map(function(s, i) { return counts[i] > 0 ? Math.round(s / counts[i]) : null; });
}

/* average across every brand for a store */
function _avgAllBrands(size, store) {
    if (size === 'ALL') return D.store_avg[store] || [];
    const nM = months.length;
    const sums = new Array(nM).fill(0), counts = new Array(nM).fill(0);
    Object.keys(BRANDS).forEach(function(abbr) {
        const prices = ((D.size_data[size] || {})[abbr] || {})[store] || [];
        prices.forEach(function(p, i) {
            if (p !== null && p !== undefined) { sums[i] += p; counts[i]++; }
        });
    });
    return sums.map(function(s, i) { return counts[i] > 0 ? Math.round(s / counts[i]) : null; });
}

function _tryRender() {
    if (!selectedSize || !selectedMode || !selectedSubKey) return;
    if (selectedMode === 'store') _renderStore();
    else                          _renderBrand();
}

/* ── By Store: selected brand → 3 store lines ───────── */
function _renderStore() {
    const abbr = selectedSubKey;
    const sizeLabel  = selectedSize === 'ALL' ? 'All Sizes (avg)' : selectedSize;
    const brandLabel = abbr === 'ALL' ? 'All Brands (avg)' : (BRANDS[abbr] || abbr);
    const ds = ['tempe','bj','jax','tw','twi'].reduce((arr, s) => {
        let prices;
        if (abbr === 'ALL') {
            prices = _avgAllBrands(selectedSize, s);
        } else if (selectedSize === 'ALL') {
            prices = _avgAllSizes(abbr, s);
        } else {
            const bsd = (D.brand_size_data[abbr] || {})[selectedSize];
            prices = bsd ? bsd[s] : null;
        }
        if (prices && !prices.every(p => p === null))
            arr.push({ label:SL[s], data:prices, borderColor:SC[s],
                       backgroundColor:SC[s]+'33', borderDash:SD[s],
                       spanGaps:true, tension:.3, pointRadius:5, fill:false });
        return arr;
    }, []);
    if (!ds.length) { _noData(brandLabel + ' — ' + sizeLabel + ': no data'); return; }
    _render((BRANDS[abbr]||abbr) + ' \u2014 ' + sizeLabel
            + ' \u2014 Tempe vs Bob Jane vs JAX', ds);
    _renderTable();
}

/* ── By Brand: selected store → brand lines ─────────── */
function _renderBrand() {
    const store = selectedSubKey;
    const sizeLabel = selectedSize === 'ALL' ? 'All Sizes (avg)' : selectedSize;
    const ds = [];
    let ci = 0;
    Object.keys(BRANDS).forEach(function(abbr) {
        const prices = selectedSize === 'ALL'
            ? _avgAllSizes(abbr, store)
            : ((D.size_data[selectedSize] || {})[abbr] || {})[store];
        if (!prices || prices.every(p => p === null)) return;
        const c = '#' + (BCOLORS[abbr] || PALETTE[ci%PALETTE.length].slice(1));
        const solid = (abbr === 'HK' || abbr === 'LF');
        ci++;
        ds.push({ label:abbr + ' ' + (BRANDS[abbr]||''), data:prices,
                  borderColor:c, backgroundColor:c+'33',
                  borderDash: solid ? [] : [6,4],
                  borderWidth: solid ? 3 : 1.5,
                  pointRadius: solid ? 7 : 5,
                  spanGaps:true, tension:.3, fill:false });
    });
    if (!ds.length) { _noData(SL[store] + ' — ' + sizeLabel + ': no data'); return; }
    _render(SL[store] + ' \u2014 ' + sizeLabel + ' \u2014 Brand Comparison', ds);
    _renderTable();
}

/* ── Size buttons ─────────────────────────────────────── */
function _setSize(size, btn) {
    if (_activeSizeBtn) _activeSizeBtn.classList.remove('active');
    _activeSizeBtn = btn; btn.classList.add('active');
    selectedSize = size;
    _tryRender();
}

/* ── Mode: By Store ───────────────────────────────────── */
function onModeStore(btn) {
    if (_activeModeBtn) _activeModeBtn.classList.remove('active');
    _activeModeBtn = btn; btn.classList.add('active');
    selectedMode = 'store'; selectedSubKey = null;
    if (_activeSubBtn) { _activeSubBtn.classList.remove('active'); _activeSubBtn = null; }

    document.getElementById('sub-label').textContent = 'Brand';
    const subBtns = document.getElementById('sub-btns');
    subBtns.innerHTML = '';

    // ALL button first
    (function() {
        const b = document.createElement('button');
        b.className = 'btn';
        b.textContent = 'ALL';
        b.style.fontWeight = '700';
        b.onclick = function() {
            if (_activeSubBtn) _activeSubBtn.classList.remove('active');
            _activeSubBtn = b; b.classList.add('active');
            selectedSubKey = 'ALL'; _tryRender();
        };
        subBtns.appendChild(b);
    }());

    // one button per brand that has data in any size
    Object.keys(D.brand_size_data).forEach(function(abbr) {
        const c = '#' + (BCOLORS[abbr] || '555555');
        const b = document.createElement('button');
        b.className = 'btn';
        b.textContent = BRANDS[abbr] || abbr;
        b.style.borderColor = c; b.style.color = c;
        b.onclick = function() {
            if (_activeSubBtn) _activeSubBtn.classList.remove('active');
            _activeSubBtn = b; b.classList.add('active');
            selectedSubKey = abbr; _tryRender();
        };
        subBtns.appendChild(b);
    });
    document.getElementById('sub-row').style.display = 'flex';
    if (subBtns.firstChild) subBtns.firstChild.click();
}

/* ── Mode: By Brand ───────────────────────────────────── */
function onModeBrand(btn) {
    if (_activeModeBtn) _activeModeBtn.classList.remove('active');
    _activeModeBtn = btn; btn.classList.add('active');
    selectedMode = 'brand'; selectedSubKey = null;
    if (_activeSubBtn) { _activeSubBtn.classList.remove('active'); _activeSubBtn = null; }

    document.getElementById('sub-label').textContent = 'Store';
    const subBtns = document.getElementById('sub-btns');
    subBtns.innerHTML = '';

    [{key:'tempe', label:'Tempe',              c:'#2196F3'},
     {key:'bj',    label:'Bob Jane',          c:'#4CAF50'},
     {key:'jax',   label:'JAX',               c:'#FF9800'},
     {key:'tw',    label:'TempeWOS Sell Out',  c:'#9C27B0'},
     {key:'twi',   label:'TempeWOS Sell In',   c:'#E040FB'}].forEach(function(s) {
        const b = document.createElement('button');
        b.className = 'btn';
        b.textContent = s.label;
        b.style.borderColor = s.c; b.style.color = s.c;
        b.onclick = function() {
            if (_activeSubBtn) _activeSubBtn.classList.remove('active');
            _activeSubBtn = b; b.classList.add('active');
            selectedSubKey = s.key; _tryRender();
        };
        subBtns.appendChild(b);
    });
    document.getElementById('sub-row').style.display = 'flex';
    if (subBtns.firstChild) subBtns.firstChild.click();
}

/* ── init ─────────────────────────────────────────────── */
(function init() {
    const szContainer = document.getElementById('size-btns');
    // ALL button first
    const allBtn = document.createElement('button');
    allBtn.className = 'btn';
    allBtn.textContent = 'ALL';
    allBtn.style.fontWeight = '700';
    allBtn.onclick = function() { _setSize('ALL', allBtn); };
    szContainer.appendChild(allBtn);
    SIZES.forEach(function(size) {
        const b = document.createElement('button');
        b.className = 'btn';
        b.textContent = size;
        b.onclick = function() { _setSize(size, b); };
        szContainer.appendChild(b);
    });
    // default: ALL
    allBtn.click();
    // auto-select By Store mode
    document.getElementById('btn-store').click();
}());
</script>
</body>
</html>
"""


# ── Flask route ───────────────────────────────────────────────────────────────

@app.route('/price')
def price_dashboard():
    monthly = load_all_months()
    if not monthly:
        return ("<h2 style='padding:40px;font-family:sans-serif;color:#c00'>"
                "No data files found. Need Tempe_*.csv, BobJane_*.csv, JAX_*.csv.</h2>")

    data = build_data(monthly)
    chart_json = json.dumps({
        "months":          data["months"],
        "store_avg":       data["store_avg"],
        "size_data":       data["size_data"],
        "brand_size_data": data["brand_size_data"],
    }, default=str)

    return render_template_string(
        _HTML,
        month_count  = len(data["months"]),
        chart_json   = chart_json,
        brands_json  = json.dumps(BRANDS),
        bcolors_json = json.dumps(BRAND_COLOURS),
        sizes_json   = json.dumps(data["sizes_with_data"]),
    )


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
