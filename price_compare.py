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
    build_tempe_lookup, build_bj_lookup, build_jax_lookup,
    best_tempe, best_bj, best_jax,
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
    all_months = sorted(set(list(t_f) + list(bj_f) + list(jx_f)))
    return {
        mk: {
            "t_rows":  load_csv(t_f[mk])  if mk in t_f  else [],
            "bj_rows": load_csv(bj_f[mk]) if mk in bj_f else [],
            "jx_rows": load_csv(jx_f[mk]) if mk in jx_f else [],
        }
        for mk in all_months
    }


def build_data(monthly):
    months       = sorted(monthly.keys())
    month_labels = [_month_label(m) for m in months]
    sizes        = list(SIZE_CATEGORY.keys())
    abbrs        = list(BRANDS.keys())

    store_avg = {"tempe": [], "bj": [], "jax": []}
    # size_data[size][abbr][store] = [price_per_month, ...]
    size_data = {s: {a: {"tempe": [], "bj": [], "jax": []} for a in abbrs} for s in sizes}

    latest       = months[-1] if months else None
    summary_rows = []

    for mk in months:
        md     = monthly[mk]
        t_lk   = build_tempe_lookup(md["t_rows"])
        bj_lk  = build_bj_lookup(md["bj_rows"])
        jx_lk  = build_jax_lookup(md["jx_rows"])
        tp, bp, jp = [], [], []

        for size in sizes:
            for abbr in abbrs:
                t_desc, t_cost, t_price  = best_tempe(size, abbr, t_lk)
                bj_desc, bj_price, *_    = best_bj(size, abbr, bj_lk, t_desc)
                jx_desc, jx_price, *_    = best_jax(size, abbr, jx_lk, t_desc)

                size_data[size][abbr]["tempe"].append(t_price)
                size_data[size][abbr]["bj"].append(bj_price)
                size_data[size][abbr]["jax"].append(jx_price)

                if t_price:  tp.append(t_price)
                if bj_price: bp.append(bj_price)
                if jx_price: jp.append(jx_price)

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

        store_avg["tempe"].append(round(sum(tp)/len(tp), 2) if tp else None)
        store_avg["bj"].append(round(sum(bp)/len(bp), 2)   if bp else None)
        store_avg["jax"].append(round(sum(jp)/len(jp), 2)  if jp else None)

    # brand_size_data: only combos that have at least one real price
    brand_size_data = {}
    for abbr in abbrs:
        for size in sizes:
            d = size_data[size][abbr]
            if any(p is not None for p in d["tempe"] + d["bj"] + d["jax"]):
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
                   size_data[size][abbr]["jax"])
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
       display: flex; align-items: baseline; gap: 14px; }
.hdr h1 { font-size: 16px; }
.hdr span { font-size: 11px; opacity: .65; }

.body { display: flex; flex: 1; overflow: hidden; }

/* ── LEFT table ── */
.left { width: 38%; border-right: 1px solid #d0d5dd;
        overflow-y: auto; background: #fff; }
table { width: 100%; border-collapse: collapse; font-size: 11.5px; }
thead th { background: #1F4E79; color: #fff; padding: 7px 5px;
           position: sticky; top: 0; z-index: 5; text-align: center; }
tbody td { padding: 4px 5px; border-bottom: 1px solid #eee; white-space: nowrap; }
tbody tr:hover td { filter: brightness(.94); }
.r { text-align: right; font-family: monospace; }
.dim { color: #888; font-size: 10.5px; }
.sep td { background: #e4e8ef !important; height: 4px; }

/* ── RIGHT panel ── */
.right { width: 62%; display: flex; flex-direction: column; overflow: hidden; }

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
.chart-wrap { height: 360px; max-height: 100%; position: relative; }
.chart-wrap canvas { position: absolute; inset: 0; width:100%!important; height:100%!important; }
</style>
</head>
<body>

<div class="hdr">
  <h1>Tyre Price Comparison Dashboard</h1>
  <span>Tempe &nbsp;|&nbsp; Bob Jane &nbsp;|&nbsp; JAX &nbsp;&middot;&nbsp;
        {{ month_count }} month{{ 's' if month_count != 1 else '' }} of data</span>
</div>

<div class="body">

  <!-- ── LEFT: summary table ── -->
  <div class="left">
    <table>
      <thead>
        <tr>
          <th>Size</th><th>Brand</th><th>Cat.</th>
          <th>Tempe</th><th>BJ</th><th>JAX</th>
        </tr>
      </thead>
      <tbody>
        {% set prev = namespace(size='') %}
        {% for r in summary_rows %}
          {% if r.size != prev.size and not loop.first %}
            <tr class="sep"><td colspan="6"></td></tr>
          {% endif %}
          {% set prev.size = r.size %}
          <tr style="background:#{{ fills.get(r.abbr,'ffffff') }}">
            <td>{{ r.size }}</td>
            <td>{{ r.brand }}</td>
            <td class="dim">{{ r.category }}</td>
            <td class="r">{{ '$%d'|format(r.t_price|int)   if r.t_price   else '—' }}</td>
            <td class="r">{{ '$%d'|format(r.bj_price|int)  if r.bj_price  else '—' }}</td>
            <td class="r">{{ '$%d'|format(r.jax_price|int) if r.jax_price else '—' }}</td>
          </tr>
        {% endfor %}
      </tbody>
    </table>
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

const SC = { tempe:'#2196F3', bj:'#4CAF50', jax:'#FF9800' };
const SL = { tempe:'Tempe',   bj:'Bob Jane', jax:'JAX' };
const SD = { tempe:[], bj:[5,5], jax:[2,3] };
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
    const pad = Math.max((hi - lo) * 0.5, 30);   // 50% padding, min $30
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
}

function _tryRender() {
    if (!selectedSize || !selectedMode || !selectedSubKey) return;
    if (selectedMode === 'store') _renderStore();
    else                          _renderBrand();
}

/* ── By Store: selected brand → 3 store lines ───────── */
function _renderStore() {
    const abbr = selectedSubKey;
    const bsd  = (D.brand_size_data[abbr] || {})[selectedSize];
    if (!bsd) {
        _noData((BRANDS[abbr]||abbr) + ' — ' + selectedSize + ': no data');
        return;
    }
    const ds = ['tempe','bj','jax'].reduce((arr, s) => {
        const prices = bsd[s];
        if (prices && !prices.every(p => p === null))
            arr.push({ label:SL[s], data:prices, borderColor:SC[s],
                       backgroundColor:SC[s]+'33', borderDash:SD[s],
                       tension:.3, pointRadius:5, fill:false });
        return arr;
    }, []);
    _render((BRANDS[abbr]||abbr) + ' \u2014 ' + selectedSize
            + ' \u2014 Tempe vs Bob Jane vs JAX', ds);
}

/* ── By Brand: selected store → brand lines ─────────── */
function _renderBrand() {
    const store = selectedSubKey;
    const sizeMap = D.size_data[selectedSize];
    if (!sizeMap) { _noData('No data for ' + selectedSize); return; }
    const ds = [];
    let ci = 0;
    Object.keys(sizeMap).forEach(function(abbr) {
        const prices = sizeMap[abbr][store];
        if (!prices || prices.every(p => p === null)) return;
        const c = '#' + (BCOLORS[abbr] || PALETTE[ci%PALETTE.length].slice(1));
        const solid = (abbr === 'HK' || abbr === 'LF');
        ci++;
        ds.push({ label:abbr + ' ' + (BRANDS[abbr]||''), data:prices,
                  borderColor:c, backgroundColor:c+'33',
                  borderDash: solid ? [] : [6,4],
                  borderWidth: solid ? 3 : 1.5,
                  pointRadius: solid ? 7 : 5,
                  tension:.3, fill:false });
    });
    if (!ds.length) { _noData(SL[store] + ' — ' + selectedSize + ': no data'); return; }
    _render(SL[store] + ' \u2014 ' + selectedSize + ' \u2014 Brand Comparison', ds);
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

    [{key:'tempe', label:'Tempe',    c:'#2196F3'},
     {key:'bj',    label:'Bob Jane', c:'#4CAF50'},
     {key:'jax',   label:'JAX',      c:'#FF9800'}].forEach(function(s) {
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
    SIZES.forEach(function(size) {
        const b = document.createElement('button');
        b.className = 'btn';
        b.textContent = size;
        b.onclick = function() { _setSize(size, b); };
        szContainer.appendChild(b);
    });
    // auto-select first size
    if (szContainer.firstChild) szContainer.firstChild.click();
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
        summary_rows = data["summary_rows"],
        fills        = ROW_FILLS,
        month_count  = len(data["months"]),
        chart_json   = chart_json,
        brands_json  = json.dumps(BRANDS),
        bcolors_json = json.dumps(BRAND_COLOURS),
        sizes_json   = json.dumps(data["sizes_with_data"]),
    )


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
