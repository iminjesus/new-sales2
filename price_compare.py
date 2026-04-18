"""
price_compare.py — Tyre Price Comparison Dashboard
http://127.0.0.1:5000/price

Left  : summary table (latest month)
Right : line charts (X = month, Y = price)
  Section A — Store Overview  : Tempe / BJ / JAX average price trend
  Section B — By Size         : one card per size, lines = brands (Tempe price)
  Section C — By Brand × Size : one card per (brand, size), lines = stores
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

def _month_key(fp):
    m = re.search(r'(\d{4})(\d{2})\d{2}', os.path.basename(fp))
    return f"{m.group(1)}{m.group(2)}" if m else "000000"

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

.hdr { background: #1F4E79; color: #fff; padding: 12px 20px; flex-shrink: 0;
       display: flex; align-items: baseline; gap: 16px; }
.hdr h1 { font-size: 17px; }
.hdr span { font-size: 12px; opacity: .65; }

.body { display: flex; flex: 1; overflow: hidden; }

.left { width: 41%; border-right: 1px solid #d0d5dd;
        overflow-y: auto; background: #fff; }
table { width: 100%; border-collapse: collapse; font-size: 11.5px; }
thead th { background: #1F4E79; color: #fff; padding: 7px 5px;
           position: sticky; top: 0; z-index: 5; text-align: center;
           white-space: nowrap; }
tbody td { padding: 4px 5px; border-bottom: 1px solid #eee; white-space: nowrap; }
tbody tr:hover td { filter: brightness(.94); }
.r { text-align: right; font-family: monospace; }
.dim { color: #888; font-size: 10.5px; }
.sep td { background: #e8edf2 !important; height: 5px; }

.right { width: 59%; overflow-y: auto; padding: 14px 16px; }

.sec { font-size: 13px; font-weight: 700; color: #1F4E79;
       border-bottom: 2px solid #1F4E79; padding-bottom: 4px;
       margin: 22px 0 10px; }
.sec:first-of-type { margin-top: 0; }

.grid { display: flex; flex-wrap: wrap; gap: 10px; }

.card { background: #fff; border-radius: 7px;
        box-shadow: 0 1px 3px rgba(0,0,0,.13); padding: 10px; }
.card.w100 { width: 100%; }
.card.w50  { width: calc(50% - 5px); }
.card.w33  { width: calc(33.33% - 7px); }
.ct { font-size: 11px; font-weight: 600; text-align: center;
      color: #444; margin-bottom: 6px; }
</style>
</head>
<body>

<div class="hdr">
  <h1>Tyre Price Comparison Dashboard</h1>
  <span>Tempe &nbsp;|&nbsp; Bob Jane &nbsp;|&nbsp; JAX &nbsp;&middot;&nbsp;
        {{ month_count }} month{{ 's' if month_count != 1 else '' }} of data</span>
</div>

<div class="body">

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
            <td class="r">{{ '$%d'|format(r.t_price|int)   if r.t_price   else '&mdash;' }}</td>
            <td class="r">{{ '$%d'|format(r.bj_price|int)  if r.bj_price  else '&mdash;' }}</td>
            <td class="r">{{ '$%d'|format(r.jax_price|int) if r.jax_price else '&mdash;' }}</td>
          </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>

  <div class="right">

    <div class="sec">Store Overview &mdash; Average Price Trend</div>
    <div class="grid">
      <div class="card w100">
        <div class="ct">Tempe vs Bob Jane vs JAX &mdash; Average Tyre Price (all sizes &amp; brands)</div>
        <canvas id="c_overview" height="85"></canvas>
      </div>
    </div>

    <div class="sec">By Size &mdash; Brand Price Comparison (Tempe Price)</div>
    <div class="grid" id="grid_size"></div>

    <div class="sec">By Brand &amp; Size &mdash; Store Comparison (Tempe / BJ / JAX)</div>
    <div class="grid" id="grid_brand"></div>

  </div>
</div>

<script>
const D       = {{ chart_json | safe }};
const BRANDS  = {{ brands_json | safe }};
const BCOLORS = {{ bcolors_json | safe }};
const months  = D.months;

const SC = { tempe:'#2196F3', bj:'#4CAF50', jax:'#FF9800' };
const SL = { tempe:'Tempe', bj:'Bob Jane', jax:'JAX' };
const SD = { tempe:[], bj:[5,5], jax:[2,3] };
const PALETTE = ['#e74c3c','#3498db','#2ecc71','#f39c12','#9b59b6',
                 '#1abc9c','#e67e22','#34495e','#e91e63','#607d8b'];

const baseOpts = {
    responsive: true,
    plugins: {
        legend: { position:'bottom', labels:{ boxWidth:10, font:{size:9}, padding:5 } },
        tooltip: { callbacks:{ label: ctx => '$' + ctx.raw } }
    },
    scales: {
        x: { ticks:{ font:{size:9} } },
        y: { ticks:{ font:{size:9}, callback: v => '$'+v }, grid:{ color:'#f0f0f0' } }
    }
};

function mkCard(gridId, title, cid, w) {
    const g = document.getElementById(gridId);
    const d = document.createElement('div');
    d.className = 'card ' + (w || 'w33');
    d.innerHTML = '<div class="ct">' + title + '</div>'
                + '<canvas id="' + cid + '" height="160"></canvas>';
    g.appendChild(d);
}

function mkChart(id, datasets) {
    const el = document.getElementById(id);
    if (!el) return;
    new Chart(el, { type:'line', data:{ labels:months, datasets }, options:baseOpts });
}

/* A: Store Overview */
new Chart(document.getElementById('c_overview'), {
    type:'line',
    data:{
        labels: months,
        datasets: ['tempe','bj','jax'].map(s => ({
            label: SL[s], data: D.store_avg[s],
            borderColor: SC[s], backgroundColor: SC[s]+'33',
            tension:.35, pointRadius:5, fill:false,
        }))
    },
    options: baseOpts
});

/* B: By Size — brand lines (Tempe price) */
Object.keys(D.size_data).forEach(function(size) {
    const brandMap = D.size_data[size];
    const ds = [];
    let ci = 0;
    Object.keys(brandMap).forEach(function(abbr) {
        const prices = brandMap[abbr].tempe;
        if (prices.every(p => p === null)) return;
        const c = '#' + (BCOLORS[abbr] || PALETTE[ci % PALETTE.length].slice(1));
        ci++;
        ds.push({ label:abbr, data:prices, borderColor:c,
                  backgroundColor:c+'33', tension:.3, pointRadius:3 });
    });
    if (!ds.length) return;
    const cid = 'c_sz_' + size.replace(/[\/\s]/g,'_');
    mkCard('grid_size', size, cid, 'w33');
    mkChart(cid, ds);
});

/* C: By Brand x Size — store comparison */
Object.keys(D.brand_size_data).forEach(function(abbr) {
    Object.keys(D.brand_size_data[abbr]).forEach(function(size) {
        const sd = D.brand_size_data[abbr][size];
        const ds = [];
        ['tempe','bj','jax'].forEach(function(store) {
            const prices = sd[store];
            if (prices.every(p => p === null)) return;
            ds.push({
                label: SL[store], data: prices,
                borderColor: SC[store], backgroundColor: SC[store]+'33',
                borderDash: SD[store], tension:.3, pointRadius:3,
            });
        });
        if (!ds.length) return;
        const cid = 'c_bs_' + abbr + '_' + size.replace(/[\/\s]/g,'_');
        mkCard('grid_brand', (BRANDS[abbr]||abbr) + ' &mdash; ' + size, cid, 'w33');
        mkChart(cid, ds);
    });
});
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
    )


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
