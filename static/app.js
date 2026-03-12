// --- fast defaults ---

// --- fast defaults (safe) ---
// Put this AFTER Chart.js is loaded (and AFTER ChartDataLabels if you use it).

Chart.defaults.animation = false;                 // Disable animations for speed
Chart.defaults.responsiveAnimationDuration = 0;
Chart.defaults.normalized = true;                 // Faster parsing for large datasets
Chart.defaults.elements.bar.borderWidth = 0;

// --- Optional: custom value-drawing plugin (expensive) ---
const SHOW_BAR_VALUES = false;
if (SHOW_BAR_VALUES && typeof showDataValuesPlugin !== "undefined") {
  // Register only once
  if (!Chart.registry.plugins.get("showDataValues")) {
    Chart.register(showDataValuesPlugin);
  }
}

// --- DataLabels: show ONLY for line datasets ---
// This requires chartjs-plugin-datalabels to be loaded and registered.
(function enableLineOnlyDataLabels(){
  // If ChartDataLabels is not available, just skip safely.
  if (typeof ChartDataLabels === "undefined") return;

  // Register only once
  if (!Chart.registry.plugins.get("datalabels")) {
    Chart.register(ChartDataLabels);
  }

  // IMPORTANT:
  // Do NOT overwrite Chart.defaults.plugins.
  // Only set the datalabels sub-config.
  Chart.defaults.plugins.datalabels = {
    display: (ctx) => {
      // Dataset type wins; otherwise chart type.
      const dsType = ctx.dataset?.type;
      const chartType = ctx.chart?.config?.type;
      const type = dsType || chartType;
      return type === "line";                     // True for lines, false for bars
    }
  };
})();

const REGION_STACK_ORDER = ["NSW", "QLD", "VIC", "SA", "WA", "COMMON"];
  
/* -------------------------- state & helpers -------------------------- */
const COLORS=["#374388ff","#90359cff","#3e9150ff","#93b14dff","#d6c635ff","#95E4E5","#275ccfff","#175d96ff","#366592ff","#667c91ff","#FF968A","#FFAEA6"];
const REGION_SALESMEN={
  NSW:["LUTTRELL STEVE","Borghese Alessio","NSW SM"],
  QLD:["Maclure Adam","Spires Steven","Sampson Kieren","Marsh Aaron"],
  VIC:["Bellotto Nicola","Bilston Kelley","Gultjaeff Jason","Hobkirk Calvin"],
  WA:["DAIS Jim","WA SM"]
};
const fmt = (n) => (+n || 0).toLocaleString();

// Normalise names so monthly & daily labels match exactly
const norm = (s) => (s ?? "")
  .toString()
  .replace(/\s+/g, " ")
  .trim()
  .toUpperCase();

let topCustomerChartInst = null;
const filters={
  metric:"qty",
  group_by:"region",
  region:"ALL",
  salesman:"ALL",
  sold_to_group:"ALL",
  sold_to:"ALL",
  ship_to:"ALL",          
  product_group:"ALL",
  pattern:"ALL",    
  material:"ALL",      
  category:"ALL",
  category_target:"ALL",
  top_limit: 0   
};

const mapFilters = {
  metric:"qty",
  group_by:"region",
  region:"ALL",
  salesman:"ALL",
  sold_to_group:"ALL",
  sold_to:"ALL",
  ship_to:"ALL",          
  product_group:"ALL",
  pattern:"ALL",          
  material:"ALL",
  category:"ALL",
  category_target:"ALL",
  top_limit: 0   
};

let dailyInst,dailyCumInst,monthlyInst,monthlyCumInst,yearlyInst,monthlyTargetInst,
    stackedDailyInst,stackedDailyCumInst, stackedDailyPctInst, stackedDailyCumPctInst, stackedYearlyInst, stackedYearlyPctInst,
    stackedMonthlyInst, stackedMonthlyCumInst, stackedMonthlyPctInst, stackedMonthlyCumPctInst,
    stackedDailyTargetInst, stackedDailyTargetCumInst, stackedDailyTargetPctInst, stackedDailyTargetCumPctInst,
    stackedMonthlyTargetInst, stackedMonthlyTargetCumInst, stackedMonthlyTargetPctInst, stackedMonthlyTargetCumPctInst;

const $=s=>document.querySelector(s);
function showError(msg){
  const el = document.getElementById('errbar');
  if (!el) return;
  el.textContent = msg;
  el.hidden = false;
}
const fetchJSON = async (u) => {
  try {
    const r = await fetch(u, {credentials:'same-origin'});
    if(!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return await r.json();
  } catch (e) {
    console.error('Fetch fail:', u, e);
    showError(`Failed: ${u} — ${e.message}`);
    return [];
  }
};
const fetchJSON_DIRECT = async (u) => {
  // Bypass any v2 shim/caching (used for KPI table accuracy)
  try {
    const r = await fetch(u, {credentials:'same-origin'});
    if(!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return await r.json();
  } catch (e) {
    console.error('Fetch fail (DIRECT):', u, e);
    showError(`Failed: ${u} — ${e.message}`);
    return [];
  }
};

const setActive = (wrap, attr, val) => {
  if (!wrap) return;
  wrap.querySelectorAll(".btn").forEach(b => {
    b.classList.toggle("active", b.dataset[attr] === val);
  });
};
function populateSelect(el,arr,includeAll=true){ el.innerHTML=""; if(includeAll){const o=document.createElement("option");o.value="ALL";o.textContent="ALL";el.appendChild(o);} arr.forEach(v=>{const o=document.createElement("option");o.value=v;o.textContent=v;el.appendChild(o);}); }
function makeStacked(id,labels,datasets,title,max){ return new Chart(document.getElementById(id),{type:"bar",data:{labels,datasets},options:getCommonOptions(true, max, title)}); }
const monthsLabels=()=>["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const daysLabels=()=>[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31];
const yearsLabels=()=>[2021,2022,2023,2024,2025];
function toCumulative(arr){const out=[];let run=0;for(const v of arr){run+=(+v||0);out.push(run);}return out;}
function cumPerGroup(map){ const out={}; for(const k in map){out[k]=toCumulative(map[k]);} return out;}

// The API already returns r.group_label for any grouping.
// Keep labels exactly as the API gives them.
const labelForRow = (r) => r.group_label || 'UNKNOWN';

function populateDatalist(listId, items){
  const list = document.getElementById(listId);
  if (!list) return;
  list.innerHTML = '';
  (items||[]).forEach(v=>{
    const o = document.createElement('option');
    o.value = v; list.appendChild(o);
  });
}

async function refreshSoldToList() {
  // NOTE:
  // We intentionally DO NOT send current sold_to/ship_to/pattern/material as filters here.
  // If we do, the list can "lock" to the current value and you can't re-open other options.
  await refreshSoldToCustom();
  await refreshShipToCustom();
}
async function ensureTopSet(){
  const k = keyForTopSet();
  if (TOP_SET && TOP_SET_KEY === k) return TOP_SET;
  TOP_SET = await fetchTopSet2025();
  TOP_SET_KEY = k;
  return TOP_SET;
}

/*
 * Reduce to Top-10 + “Other” smartly:
 * 1) Try TOP_SET intersection with current groups (normalized).
 * 2) If intersection is empty, fall back to top 10 present in the current map (by totals).
 */
function reduceToTopSmart(groups, map, topSet){
  if (!groups || !groups.length) return { groups, map };

  const firstKey = groups.find(g => Array.isArray(map[g]));
  if (!firstKey) return { groups, map };
  const len = map[firstKey].length;

  // Totals per group for fallback
  const totals = new Map(groups.map(g => [g, (map[g]||[]).reduce((a,b)=>a + (+b||0), 0)]));

  // Preferred keep set: intersection with TOP_SET (by normalized name)
  let keep = [];
  if (topSet && topSet.size) keep = groups.filter(g => topSet.has(norm(g)));

  // If no intersection, use top 10 by totals present in this dataset
  if (keep.length === 0) {
    keep = [...totals.entries()]
      .sort((a,b)=> b[1]-a[1])
      .slice(0,10)
      .map(([g])=>g);
  }

  const newMap = {};
  const other = Array(len).fill(0);

  groups.forEach(g=>{
    if (keep.includes(g)) {
      newMap[g] = map[g] || Array(len).fill(0);
    } else {
      const arr = map[g] || [];
      for (let i=0;i<len;i++) other[i] += (+arr[i]||0);
    }
  });

  const outGroups = [...keep];
  if (other.some(v=>v!==0)) {
    newMap['Other'] = other;
    outGroups.push('Other');
  }

  return { groups: outGroups, map: newMap };
}

/* ---------- Common options with dd-mm x labels & tooltip title ---------- */
function xAxisDdMm(stacked=false){
  return {
    stacked,
    grid: { color: "rgba(0,0,0,0.05)" },
    ticks: {
      maxRotation: 0,
      autoSkip: true,
      callback: function(value){
        const full = this.getLabelForValue(value);
        return typeof full === 'string' ? full.slice(0,2) : full; // dd
      }
    }
  };
}
// Plugin to display actual data values centered inside bars
const showDataValuesPlugin = {
  id: "showDataValues",
  afterDatasetsDraw(chart, _args, pluginOpts = {}) {
    const ctx = chart.ctx;
    const area = chart.chartArea;

    // global defaults; can be overridden per chart via options.plugins.showDataValues
    const color     = pluginOpts.color ?? "#232191ff";
    const font      = pluginOpts.font  ?? "10px Arial";
    const offset    = pluginOpts.offset ?? 6;                 // for line points
    const include   = pluginOpts.include ?? ["bar", "line"];  // which types to draw
    const formatter = pluginOpts.formatter ?? (v =>
      (typeof v === "number" ? v.toLocaleString() : String(v)));

    ctx.save();
    chart.data.datasets.forEach((ds, i) => {
      const meta = chart.getDatasetMeta(i);
      if (!meta || meta.hidden) return;

      const type = meta.type || ds.type || chart.config.type;
      if (!include.includes(type)) return;

      (meta.data || []).forEach((elem, idx) => {
        if (!elem) return;

        const raw = ds.data[idx];
        const val = Number(raw);
        if (raw == null || !Number.isFinite(val) || val === 0) return; // skip 0s

        let x, y;
        if (type === "bar") {
          // center the text vertically in the bar
          const { x: bx, y: by, base } = elem.getProps(["x", "y", "base"], true);
          x = bx;
          y = by + (base - by) / 2;
        } else { // line
          // draw slightly above the point
          const p = elem.getProps(["x", "y"], true);
          x = p.x;
          y = p.y - offset;
        }

        // keep labels inside the chart area
        y = Math.max(area.top + 8, Math.min(y, area.bottom - 8));

        ctx.fillStyle = (ds.datalabels && ds.datalabels.color) || color;
        ctx.font = font;
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(formatter(val), x, y);
      });
    });
    ctx.restore();
  }
};

function getCommonOptions(stacked=false, yMax, yTitle){
  return {
    responsive:true,
    plugins:{
      legend:{position:"right"},
      tooltip:{
        callbacks:{
          title: function(items){
            const lbl = items && items[0] ? items[0].label : '';
            return typeof lbl === 'string' ? lbl.slice(0,5) : lbl; // dd-mm in tooltip title
          }
        }
      }
    },
    scales:{
      x: xAxisDdMm(stacked),
      y:{ beginAtZero:true, max: yMax ?? undefined, title:{ display: !!yTitle, text: yTitle } }
    }
  };
}
// Collect the exact params your APIs expect
function getFilterParams() {
  // If you already have a global getFilterParams(), keep using it.
  // This fallback matches your APIs: metric, category, region, salesman, sold_to_group, sold_to, product_group, ship_to, pattern, material
  const $ = (sel) => document.querySelector(sel);

  // Adjust selectors only if yours differ.
  const metric        = (document.querySelector('[name="metric"]:checked')?.value || 'qty').toLowerCase();
  const category      = (document.querySelector('.cat-btn.active')?.dataset?.cat || 'ALL').toUpperCase();
  const region        = ($('#regionTabs .active')?.dataset?.region || 'ALL').toUpperCase();
  const salesman      = ($('#salesmanSelect')?.value || 'ALL');
  const sold_to_group = ($('#soldToGroup')?.value || 'ALL');
  const sold_to       = ($('#soldTo')?.value || 'ALL');
  const product_group = ($('#productGroup')?.value || 'ALL');
  const ship_to       = ($('#shipTo')?.value || 'ALL');
  const pattern       = ($('#patternInput')?.value || 'ALL');
  const material      = ($('#material')?.value || 'ALL');
  return { metric, category, region, salesman, sold_to_group, sold_to, product_group, ship_to, pattern, material };
}


/* -------------------------- UI wiring -------------------------- */

document.getElementById('catBtns').addEventListener("click",e=>{
  if(!e.target.classList.contains("btn"))return;
  filters.category=e.target.dataset.val;
  [...document.querySelectorAll("#catBtns .btn")].forEach(b=>b.classList.toggle("active",b.dataset.val===filters.category));
  refreshAllDebounced();
});
document.getElementById('metricBtns').addEventListener("click",e=>{
  if(!e.target.classList.contains("btn"))return;
  filters.metric=e.target.dataset.metric;
  setActive(document.getElementById('metricBtns'),"metric",filters.metric);
  const dt = document.getElementById('dailyTitle');
  if (dt) dt.textContent = filters.metric==="amount"?"Daily Amount":"Daily Sales";
  const ct = document.getElementById('cumTitle');
  if (ct) ct.textContent = filters.metric==="amount"?"Cumulative Amount":"Cumulative Sales";
  refreshAllDebounced();
});
document.getElementById('group_by').addEventListener("change",()=>{
  filters.group_by=document.getElementById('group_by').value;
  refreshAllDebounced();
});
document.getElementById('regionBtns').addEventListener("click",e=>{
  if(!e.target.classList.contains("btn"))return;
  filters.region=e.target.dataset.val; setActive(document.getElementById('regionBtns'),"val",filters.region);
  const all=Object.values(REGION_SALESMEN).flat();
  const list=filters.region==="ALL"?all:(REGION_SALESMEN[filters.region]||[]);
  populateSelect(document.getElementById('salesman_name'),[...new Set(list)].sort());
  filters.salesman = 'ALL';
  document.getElementById('salesman_name').value = 'ALL';
  refreshAllDebounced();
});

document.getElementById('salesman_name').addEventListener('change', (e)=>{
  filters.salesman = e.target.value || 'ALL';
  e.target.classList.toggle('sel-active', e.target.value !== 'ALL');
  refreshAllDebounced();
});

document.getElementById('sold_to_group').addEventListener('change', async ()=>{
  const el = document.getElementById('sold_to_group');
  filters.sold_to_group = el.value || 'ALL';
  el.classList.toggle('sel-active', el.value !== 'ALL');

  // reset dependent filters
  const soldEl = document.getElementById('sold_to');
  const shipEl = document.getElementById('ship_to');
  if (soldEl) { soldEl.value = ""; ddUpdateActive(soldEl); }
  if (shipEl) { shipEl.value = ""; ddUpdateActive(shipEl); }
  filters.sold_to = "ALL";
  filters.ship_to = "ALL";

  // refresh dropdown option caches
  await refreshSoldToCustom();
  await refreshShipToCustom();

  refreshAllDebounced();
});


// When SOLD_TO_GROUP changes you already reload sold_to options — keep as is.

// SOLD-TO -> fetch Ship-to names under that Sold-to, enable input


/* removed old handler: document.getElementById('sold_to').addEventListener('input' */



// Ship-to input -> mirror into filters

/* removed old handler: document.getElementById('ship_to').addEventListener('input' */


// PRODUCT GROUP -> existing code… plus refresh patterns

// product_group handled via bindDropdown (see window load listener below)



// PATTERN input -> mirror into filters

/* removed old handler: document.getElementById('pattern').addEventListener('input' */



/* removed old handler: const __mat = document.getElementById('material'); */


document.getElementById("topCustomerControls").addEventListener("click", e => {
  if (!e.target.classList.contains("btn")) return;

  const limit = Number(e.target.dataset.limit || 10);
  filters.top_limit = limit;   // store as number

  // highlight the active button
  setActive(
    document.getElementById("topCustomerControls"),
    "limit",                  // matches data-limit
    String(limit)             // dataset values are strings
  );

  
  refreshSoldToList();

  // redraw charts / KPI
  refreshAllDebounced();
});

async function refreshShipTo(){
  // kept for backward-compat with existing init flows
  await refreshShipToCustom();
}

// Load patterns for current product group
async function refreshPatterns(){ await refreshPatternsCustom(); }

async function refreshMaterials(){ await refreshMaterialsCustom(); }

function cutoffIdxFromBreakdown(rows, N){
  let cutoffIdx = -1;
  for (const r of (rows || [])){
    const d = parseInt(r.day, 10);
    if (d >= 1 && d <= N) cutoffIdx = Math.max(cutoffIdx, d - 1);
  }
  return cutoffIdx;
}




/* -------------------------- daily (Oct) – same structure as monthly -------------------------- */

async function fetchDailySales(){
  const qs = new URLSearchParams({
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, material:filters.material, top_limit:filters.top_limit ||0
  }).toString();
  return fetchJSON_DIRECT(`/api/daily_sales?${qs}`);
}

async function fetchDailyKPIActual(region,BDE){
  const qs=new URLSearchParams({
    metric:filters.metric, category:filters.category, region:region, salesman:BDE,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, material:filters.material, top_limit:filters.top_limit ||0
  }).toString();
  return fetchJSON_DIRECT(`/api/daily_sales?${qs}`);
}

async function fetchDailyKPITarget(region,BDE){
  const qs=new URLSearchParams({
    metric:filters.metric, category:filters.category, region:region, salesman:BDE,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, material:filters.material, top_limit:filters.top_limit ||0
  }).toString();
  return fetchJSON_DIRECT(`/api/daily_target?${qs}`);
}

async function fetchDailyBreakdownWithGroup(groupBy){
  const qs = new URLSearchParams({
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, material:filters.material, group_by: groupBy, top_limit:filters.top_limit ||0
  }).toString();
  return fetchJSON(`/api/daily_breakdown?${qs}`);
}


// totals (bar + cumulative), same shape as drawDailyTotals (no target for daily)
async function drawDailyTotals(){
  const [salesRows, targetRows, cutRows] = await Promise.all([
    fetchDailySales(),
    fetchJSON(`/api/daily_target?${new URLSearchParams({
      metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
      sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
      product_group:filters.product_group, pattern:filters.pattern, material:filters.material, top_limit:filters.top_limit ||0
    }).toString()}`),
    // Use breakdown as the cutoff source (same reason stacked stops correctly)
    fetchDailyBreakdownWithGroup("region")
  ]);

  const labels = daysLabels();
  const N = labels.length;

  // cutoffIdx from breakdown: max day present (1-based) -> 0-based index
  let cutoffIdx = cutoffIdxFromBreakdown(cutRows, N);
  // If breakdown is empty for some reason, fallback to last non-zero in salesRows
  // (still simple; avoids "31 rows with future zeros" problem)
  if (cutoffIdx < 0){
    const tmp = salesRows.map(r => (+r.value || 0));
    for (let i = 0; i < Math.min(tmp.length, N); i++){
      if (tmp[i] !== 0) cutoffIdx = i;
    }
    if (cutoffIdx < 0) cutoffIdx = Math.min(salesRows.length, N) - 1;
  }

  // Sales: fill up to cutoffIdx, then null
  const sales = new Array(N).fill(null);
  for (let i = 0; i <= cutoffIdx; i++){
    sales[i] = (+salesRows[i]?.value || 0);
  }

  // Targets: if you want the chart to visually stop, also null after cutoff
  const targets = new Array(N).fill(null);
  for (let i = 0; i <= cutoffIdx; i++){
    targets[i] = (+targetRows[i]?.value || 0);
  }

  // Cumulative: stop at cutoffIdx
  const salesCum  = new Array(N).fill(null);
  const targetCum = new Array(N).fill(null);

  let sRun = 0, tRun = 0;
  for (let i = 0; i <= cutoffIdx; i++){
    sRun += (+sales[i] || 0);
    tRun += (+targets[i] || 0);
    salesCum[i]  = sRun;
    targetCum[i] = tRun;
  }

  // Achievement: stop at cutoffIdx
  const achievement    = new Array(N).fill(null);
  const cumAchievement = new Array(N).fill(null);

  for (let i = 0; i <= cutoffIdx; i++){
    const s  = (+sales[i] || 0);
    const t  = (+targets[i] || 0);
    const sc = (+salesCum[i] || 0);
    const tc = (+targetCum[i] || 0);

    if (t  > 0) achievement[i]    = +((s  / t ) * 100).toFixed(1);
    if (tc > 0) cumAchievement[i] = +((sc / tc) * 100).toFixed(1);
  }

  [dailyInst, dailyCumInst].forEach(c => c && c.destroy());

  dailyInst = new Chart(document.getElementById("dailyChart"), {
    type: "bar",
    data: { labels, datasets: [
      { label: "Ach(%)", type: "line", data: achievement, yAxisID: "y1",
        borderWidth: 2, pointRadius: 0, borderColor: "#ef4444",
        datalabels: {
          display: (ctx) => {
            const i = ctx.dataIndex;
            return (i % 2 === 0) && (sales[i] !== null) && (sales[i] > 0);
          },
          align: "top", anchor: "end",
          formatter: v => v == null ? "" : v.toFixed(1) + "%"
        }
      },
      { label: filters.metric === "amount" ? "Sales Amount" : "SalesQty",
        type: "bar", data: sales, backgroundColor: "#ABDEE6",
        categoryPercentage: 0.9, barPercentage: 0.9, datalabels: { display: false }
      },
      { label: "Target", type: "bar", data: targets,
        borderWidth: 2, borderColor: "#ABDEE6", datalabels: { display: false }
      }
    ]},
    options: getCommonOptions(false)
  });

  dailyCumInst = new Chart(document.getElementById("dailyCumChart"), {
    type: "bar",
    data: { labels, datasets: [
      { label: "Ach(%)", type: "line", data: cumAchievement, yAxisID: "y1",
        borderWidth: 2, pointRadius: 0, borderColor: "#ef4444",
        datalabels: {
          display: (ctx) => {
            const i = ctx.dataIndex;
            return (i % 2 === 0) && (sales[i] !== null) && (sales[i] > 0);
          },
          align: "top", anchor: "end",
          formatter: v => v == null ? "" : v.toFixed(1) + "%"
        }
      },
      { label: filters.metric === "amount" ? "Cumulative Amount" : "Cumulative Qty",
        type: "bar", data: salesCum, backgroundColor: "#ABDEE6",
        categoryPercentage: 0.9, barPercentage: 0.9, datalabels: { display: false }
      },
      { label: "Cumulative Target", type: "bar", data: targetCum,
        borderWidth: 2, borderColor: "#ABDEE6", datalabels: { display: false }
      }
    ]},
    options: getCommonOptions(false)
  });
}


function buildDailyStacks(rows){
  const labels = daysLabels();

  // ignore null / empty groups
  let groups = [...new Set(
    rows.map(r => r.group_label).filter(g => g != null && g !== "")
  )];

  // force region stack order NSW, QLD, VIC, SA, WA, COMMON
  const ordered = [];
  REGION_STACK_ORDER.forEach(name => { if (groups.includes(name)) ordered.push(name); });
  groups.forEach(g => { if (!REGION_STACK_ORDER.includes(g)) ordered.push(g); });
  groups = ordered;

  // Determine last day where actual exists (based on rows)
  // - If a day appears in rows (even value 0), treat it as "actual exists"
  let lastActualIdx = -1;
  rows.forEach(r => {
    const d = parseInt(r.day, 10);
    if (d >= 1 && d <= labels.length) {
      lastActualIdx = Math.max(lastActualIdx, d - 1);
    }
  });

  const N = labels.length;

  const byGroup = {};
  groups.forEach(g => byGroup[g] = Array(N).fill(null));

  // Initialize days up to lastActualIdx as 0 so stacks can sum properly (including 0-actual weekends)
  groups.forEach(g => {
    for (let i = 0; i <= lastActualIdx; i++) byGroup[g][i] = 0;
  });

  // Fill values
  rows.forEach(r => {
    const g = r.group_label;
    if (g == null || g === "") return;
    const d = parseInt(r.day, 10);
    if (d >= 1 && d <= N && (d - 1) <= lastActualIdx) {
      byGroup[g][d - 1] += (+r.value || 0);
    }
  });

  const datasets = groups.map((g,i)=>({
    label: g,
    data: byGroup[g],
    backgroundColor: COLORS[i%COLORS.length],
    stack: "S",
    categoryPercentage: 0.9,
    barPercentage: 0.9
  }));

  return { labels, groups, byGroup, datasets, lastActualIdx };
}


function toPercentStacksN(byKey, N, lastActualIdx){
  const keys = Object.keys(byKey);
  const pct = {}; keys.forEach(k => pct[k] = Array(N).fill(null));

  for (let i = 0; i < N; i++){
    if (lastActualIdx != null && i > lastActualIdx) {
      keys.forEach(k => pct[k][i] = null);
      continue;
    }

    const tot = keys.reduce((a,k)=> a + (+byKey[k][i] || 0), 0);

    if (!tot) {
      // If total is 0, show 0% for each (still a valid actual day)
      keys.forEach(k => pct[k][i] = 0);
    } else {
      keys.forEach(k => pct[k][i] = +(( (+byKey[k][i] || 0) / tot) * 100).toFixed(2));
    }
  }
  return pct;
}


// stacked (value / cumulative / % / cumulative %) — identical to monthly version
async function drawDailyStacked(){
  const effectiveGroup = filters.group_by;
  const rows = await fetchDailyBreakdownWithGroup(effectiveGroup);
  
    if (!rows || !rows.length){

    const totals = await fetchDailySales();
    const labels = daysLabels();

    // Build sales aligned to labels; future days = null, 0 is valid
    const data = new Array(labels.length).fill(null);
    for (let i = 0; i < Math.min(totals.length, labels.length); i++){
      data[i] = (+totals[i].value || 0);
    }

    // Find last actual index (0 is valid; only null means future)
    let lastActualIdx = -1;
    for (let i = 0; i < data.length; i++){
      if (data[i] !== null) lastActualIdx = i;
    }

    // Cumulative data: stop drawing after lastActualIdx
    const cum = new Array(labels.length).fill(null);
    let run = 0;
    for (let i = 0; i < labels.length; i++){
      if (i <= lastActualIdx){
        run += (+data[i] || 0);
        cum[i] = run;
      } else {
        cum[i] = null;
      }
    }

    stackedDailyInst = makeStacked("stackedDailyChart", labels, [
      { label:"Total", data, backgroundColor:"#a78bfa", stack:"S", categoryPercentage:0.9, barPercentage:0.9 }
    ], "Daily");

    // Percent: only up to lastActualIdx, then null
    const pct100 = labels.map((_,i)=> (i <= lastActualIdx ? 100 : null));
    stackedDailyPctInst = makeStacked("stackedDailyPercentChart", labels, [
      { label:"Total %", data: pct100, backgroundColor:"#a78bfa", stack:"S", categoryPercentage:0.9, barPercentage:0.9 }
    ], "Daily %", 100);

    stackedDailyCumInst = makeStacked("stackedDailyCumChart", labels, [
      { label:"Total", data: cum, backgroundColor:"#10b981", stack:"S", categoryPercentage:0.9, barPercentage:0.9 }
    ], "Cumulative by Day");

    stackedDailyCumPctInst = makeStacked("stackedDailyCumPercentChart", labels, [
      { label:"Total %", data: pct100, backgroundColor:"#10b981", stack:"S", categoryPercentage:0.9, barPercentage:0.9 }
    ], "Cumulative %", 100);

    return;
  }


  // Build stacks
   let { labels, groups, byGroup, datasets, lastActualIdx } = buildDailyStacks(rows);

  const byGroupCum   = cumPerGroup(byGroup);

  // After lastActualIdx, force cumulative stacks to null so they don't draw
  groups.forEach(g => {
    for (let i = lastActualIdx + 1; i < labels.length; i++){
      byGroupCum[g][i] = null;
    }
  });

  const datasetsCum  = groups.map((g,i)=>({
    label:g, data:byGroupCum[g],
    backgroundColor:COLORS[i%COLORS.length],
    stack:"S", categoryPercentage:0.9, barPercentage:0.9
  }));

  const pct          = toPercentStacksN(byGroup,    labels.length, lastActualIdx);
  const pctCum       = toPercentStacksN(byGroupCum, labels.length, lastActualIdx);

  const datasetsPct  = groups.map((g,i)=>({
    label:g, data:pct[g],
    backgroundColor:COLORS[i%COLORS.length],
    stack:"S", categoryPercentage:0.9, barPercentage:0.9
  }));

  const datasetsPctC = groups.map((g,i)=>({
    label:g, data:pctCum[g],
    backgroundColor:COLORS[i%COLORS.length],
    stack:"S", categoryPercentage:0.9, barPercentage:0.9
  }));


  [stackedDailyInst, stackedDailyCumInst, stackedDailyPctInst, stackedDailyCumPctInst]
    .forEach(c=>c&&c.destroy());

  stackedDailyInst = new Chart(document.getElementById("stackedDailyChart"), {
    type:"bar", data:{ labels, datasets }, options:getCommonOptions(true, undefined, "Daily")
  });
  // IMPORTANT: match the IDs in your HTML (second box in row 1)
  stackedDailyCumInst = new Chart(document.getElementById("stackedDailyCumChart"), {
    type:"bar", data:{ labels, datasets:datasetsCum }, options:getCommonOptions(true, undefined, "Cumulative by Day")
  });
  stackedDailyPctInst = new Chart(document.getElementById("stackedDailyPercentChart"), {
    type:"bar", data:{ labels, datasets:datasetsPct }, options:getCommonOptions(true, 100, "Daily %")
  });
  stackedDailyCumPctInst = new Chart(document.getElementById("stackedDailyCumPercentChart"), {
    type:"bar", data:{ labels, datasets:datasetsPctC }, options:getCommonOptions(true, 100, "Cumulative %")
  });
}

/* -------------------------- monthly charts -------------------------- */
async function fetchMonthlySales(year=2025){
  const qs = new URLSearchParams({
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, material:filters.material, top_limit:filters.top_limit ||0,
    year: year
  }).toString();
  return fetchJSON_DIRECT(`/api/monthly_sales?${qs}`);
}

// helpers
const quarterOf = m => Math.floor(m/3); // 0..3 for Jan..Dec

// KPI by quarter, calculated only up to the previous month
// KPI by quarter, calculated up to the current month (YTD within the year)
function kpiByQuarter(sales, targets){
  const s = [0,0,0,0];
  const t = [0,0,0,0];

  // Find last month index where actual exists (use non-zero as "exists")
  // If your actual can be 0 but still valid, replace this rule with a better cutoff source.
  let lastIdx = -1;
  for (let i = 0; i < 12; i++){
    if ((+sales[i] || 0) !== 0) lastIdx = i;
  }
  if (lastIdx < 0) lastIdx = 0;

  // Sum only up to lastIdx (YTD by data, not by calendar)
  for (let m = 0; m <= lastIdx && m < 12; m++){
    const q = Math.floor(m / 3);
    s[q] += (+sales[m]   || 0);
    t[q] += (+targets[m] || 0);
  }

  const currentQ = Math.floor(lastIdx / 3);
  return t.map((tv, i) => {
    if (i > currentQ) return null;
    return tv > 0 ? +(s[i] / tv * 100).toFixed(1) : null;
  });
}


// Daily KPI = cumulative achievement (%) up to the last date where Actual exists
// - 0 is considered a valid actual (e.g., weekends)
// - null / undefined / "" means "no data yet" (future date) and should be excluded
function dailyKPIFromSeries(sales, targets, cutoffIdx){
  if (cutoffIdx == null || cutoffIdx < 0) return null;

  const N = Math.min(sales.length, targets.length, cutoffIdx + 1);
  if (N <= 0) return null;

  let sCum = 0;
  let tCum = 0;

  for (let i = 0; i < N; i++){
    sCum += (+sales[i]   || 0);
    tCum += (+targets[i] || 0);
  }

  if (tCum <= 0) return null;

  return +((sCum / tCum) * 100).toFixed(1);
}


async function fetchMonthlyKPIActual(region,BDE, year=2026){
  const qs=new URLSearchParams({
    metric:filters.metric, category:filters.category, region:region, salesman:BDE,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, material:filters.material, top_limit:filters.top_limit ||0, year: year
  }).toString();
  return fetchJSON_DIRECT(`/api/monthly_sales?${qs}`);
}
async function fetchMonthlyKPITarget(region,BDE, year=2026){
  const qs=new URLSearchParams({
    metric:filters.metric, category:filters.category, region:region, salesman:BDE,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, material:filters.material, top_limit:filters.top_limit ||0, year: year 
  }).toString();
  return fetchJSON_DIRECT(`/api/monthly_target?${qs}`);
}

// build & render table
async function drawMonthlyKPI(){
  const rows = [];

  // All row (no region/salesman filter)
  {
    const qsSales = new URLSearchParams({
      metric: filters.metric, category: filters.category, region: "ALL", salesman:"ALL",
      sold_to_group: filters.sold_to_group, sold_to: filters.sold_to, ship_to: filters.ship_to,
      product_group: filters.product_group, pattern: filters.pattern, material:filters.material, top_limit:filters.top_limit ||0, year:2026
    }).toString();
    const qsTarget = new URLSearchParams({
      metric: filters.metric,
      category: filters.category, region: "ALL", salesman:"ALL",
      sold_to_group: filters.sold_to_group, sold_to: filters.sold_to, ship_to: filters.ship_to,
      product_group: filters.product_group, pattern: filters.pattern, material:filters.material, top_limit:filters.top_limit ||0, year:2026
    }).toString();

     // monthly + daily in parallel
    const [
      salesRows,
      targetRows,
      dailySalesRows,
      dailyTargetRows,
      cutRows
    ] = await Promise.all([
      fetchJSON(`/api/monthly_sales?${qsSales}`),
      fetchJSON(`/api/monthly_target?${qsTarget}`),
      fetchJSON(`/api/daily_sales?${qsSales}`),
      fetchJSON(`/api/daily_target?${qsTarget}`),
      fetchDailyBreakdownWithGroup("region")
    ]);

    const sales    = salesRows.map(r => +r.value || 0);
    const targets  = targetRows.map(r => +r.value || 0);
    const q        = kpiByQuarter(sales, targets);
    const N = daysLabels().length;
    let cutoffIdx = cutoffIdxFromBreakdown(cutRows, N);
    const dailySales   = dailySalesRows.map(r => +r.value || 0);
    const dailyTargets = dailyTargetRows.map(r => +r.value || 0);
    const dailyKPI     = dailyKPIFromSeries(dailySales, dailyTargets, cutoffIdx);

    rows.push({
      region: "All",
      bde: "All",
      Q1: q[0], Q2: q[1], Q3: q[2], Q4: q[3],
      dailyKPI: dailyKPI
    });
  }

  // Region / BDE rows
  for (const region of ["NSW","QLD","VIC","WA"]) {
    {
    const yearKpi = 2026;

    const [
      salesRows,
      targetRows,
      dailySalesRows,
      dailyTargetRows,
      cutRows
    ] = await Promise.all([
      fetchMonthlyKPIActual(region, "ALL", yearKpi),
      fetchMonthlyKPITarget(region, "ALL", yearKpi),
      fetchDailyKPIActual(region, "ALL"),
      fetchDailyKPITarget(region, "ALL"),
      fetchDailyBreakdownWithGroup("region") // used only for cutoffIdx
    ]);

    const sales   = salesRows.map(r => +r.value || 0);
    const targets = targetRows.map(r => +r.value || 0);
    const q       = kpiByQuarter(sales, targets);

    const N = daysLabels().length;
    const cutoffIdx = cutoffIdxFromBreakdown(cutRows, N);

    const dSales   = dailySalesRows.map(r => +r.value || 0);
    const dTargets = dailyTargetRows.map(r => +r.value || 0);
    const dailyKPI = dailyKPIFromSeries(dSales, dTargets, cutoffIdx);

    rows.push({
      region: region,
      bde: "All",          // Region total row label
      Q1: q[0], Q2: q[1], Q3: q[2], Q4: q[3],
      dailyKPI: dailyKPI
    });
  }
    const bdes = REGION_SALESMEN[region] || []; // e.g. ["BDE2","BDE3"] or ids
    // fetch each BDE pair in parallel, then append in order
    const yearKpi=2026;
    const perBDE = await Promise.all(bdes.map(async (bde) => {
      const [
        salesRows,
        targetRows,
        dailySalesRows,
        dailyTargetRows
      ] = await Promise.all([
        fetchMonthlyKPIActual(region, bde, yearKpi),
        fetchMonthlyKPITarget(region, bde, yearKpi),
        fetchDailyKPIActual(region, bde),
        fetchDailyKPITarget(region, bde)
      ]);

      const sales    = salesRows.map(r => +r.value || 0);
      const targets  = targetRows.map(r => +r.value || 0);
      const q        = kpiByQuarter(sales, targets);
      const cutRows = await fetchDailyBreakdownWithGroup("region");
      const N = daysLabels().length;
      let cutoffIdx = cutoffIdxFromBreakdown(cutRows, N);
      const dailySales   = dailySalesRows.map(r => +r.value || 0);
      const dailyTargets = dailyTargetRows.map(r => +r.value || 0);
      const dailyKPI     = dailyKPIFromSeries(dailySales, dailyTargets, cutoffIdx);
      return {
        region: `${region}`,
        bde: `${bde}`,
        Q1: q[0], Q2: q[1], Q3: q[2], Q4: q[3],
        dailyKPI: dailyKPI
      };
    }));
    rows.push(...perBDE);
  }

  // render table
  const el = document.getElementById("kpiByRegion");
  if (!el) return;

  const fmt = v => v == null ? "-" : v.toFixed(1) + "%";

  // light background per region
  const REGION_BG = {
    ALL: "#f3f4f6",  // light grey
    NSW: "#eff6ff",  // light blue
    QLD: "#fff7ed",  // light orange
    VIC: "#ecfdf3",  // light green
    WA:  "#fef2f2"   // light red
  };
  const rowBg = region => REGION_BG[region] || "#ffffff";

  // font colour based on achievement %
  const cellHtml = (v) => {
    if (v == null) {
      return `<td style="text-align:right;color:#9ca3af">-</td>`;
    }
    const val = Number(v);
    let color;
    if (val >= 100)      color = "#16a34a"; // green
    else if (val >= 90)  color = "#f97316"; // orange
    else                 color = "#dc2626"; // red
    return `<td style="text-align:right;color:${color}">${fmt(val)}</td>`;
  };

  el.innerHTML = `
  <thead>
  <tr>
    <th style="text-align:center; font-weight:bold;">Region</th>
    <th style="text-align:center; font-weight:bold;">BDE</th>
    <th style="text-align:right; font-weight:bold;">This Month</th>
    <th style="text-align:right; font-weight:bold;">Q1</th>
    <th style="text-align:right; font-weight:bold;">Q2</th>
    <th style="text-align:right; font-weight:bold;">Q3</th>
    <th style="text-align:right; font-weight:bold;">Q4</th>
  </tr></thead>
  <tbody>
    ${rows.map(r => `
      <tr style="background-color:${rowBg(r.region)}">
        <td style="text-align:center">${r.region}</td>
        <td style="text-align:center">${r.bde}</td>
        ${cellHtml(r.dailyKPI)}
        ${cellHtml(r.Q1)}
        ${cellHtml(r.Q2)}
        ${cellHtml(r.Q3)}
        ${cellHtml(r.Q4)}
      </tr>`).join("")}
  </tbody>`;
}
async function fetchMonthlyBreakdownWithGroup(groupBy, year=2025){
  const params = {
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, material:filters.material, group_by: groupBy, top_limit:filters.top_limit ||0,
    year: year
  };
  const qs=new URLSearchParams(params).toString();
  return fetchJSON(`/api/monthly_breakdown?${qs}`);
}

async function fetchMonthlyTargetBreakdownWithGroup(groupBy, year=2026){
  const qs = new URLSearchParams({
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, material:filters.material,
    group_by: groupBy, top_limit:filters.top_limit || 0,
    year: year
  }).toString();

  return fetchJSON(`/api/monthly_target_breakdown?${qs}`); // <-- you need this endpoint
}

async function drawMonthlyTotals(){
  const [
    sales25Rows,
    sales26Rows,
    target26Rows
  ] = await Promise.all([
    fetchMonthlySales(2025),
    fetchMonthlySales(2026),
    fetchJSON(`/api/monthly_target?${new URLSearchParams({
      metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
      sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
      product_group:filters.product_group, pattern:filters.pattern, material:filters.material,
      top_limit:filters.top_limit ||0,
      year: 2026
    }).toString()}`)
  ]);

  const labels = monthsLabels();

  const sales25   = labels.map((_,i)=> +sales25Rows[i]?.value || 0);

  // 2026 actual/target
  let sales26     = labels.map((_,i)=> +sales26Rows[i]?.value || 0);
  let target26    = labels.map((_,i)=> +target26Rows[i]?.value || 0);

  // 2026 YTD cut (null after last non-zero actual)
  let last26 = -1;
  for (let i=0;i<sales26.length;i++){
    if ((+sales26[i]||0) !== 0) last26 = i;
  }
  for (let i=last26+1;i<12;i++){
    sales26[i]  = null;
    target26[i] = null;
  }

  // cumulative (2025 actual cum always full year)
  const salesCum25 = toCumulative(sales25);

  // cumulative 2026 actual/target stop at last26
  const salesCum26 = Array(12).fill(null);
  const targetCum26= Array(12).fill(null);
  let sRun=0, tRun=0;
  for (let i=0;i<12;i++){
    if (sales26[i] == null || target26[i] == null) break;
    sRun += (+sales26[i]||0);
    tRun += (+target26[i]||0);
    salesCum26[i]  = sRun;
    targetCum26[i] = tRun;
  }

  [monthlyInst, monthlyCumInst].forEach(c => c && c.destroy());

  monthlyInst = new Chart(document.getElementById("monthlyChart"),{
    type:"bar",
    data:{ labels, datasets:[
      // 2025 Actual only
      {
        label: (filters.metric==="amount" ? "Sales Amount (2025)" : "SalesQty (2025)"),
        type:"bar",
        data:sales25,
        backgroundColor:"#ABDEE6",
        categoryPercentage:0.8,
        barPercentage:0.9,
        datalabels:{ display:false }
      },
      // 2026 Actual
      {
        label: (filters.metric==="amount" ? "Sales Amount (2026)" : "SalesQty (2026)"),
        type:"bar",
        data:sales26,
        backgroundColor:"#93c5fd",
        categoryPercentage:0.8,
        barPercentage:0.9,
        datalabels:{ display:false }
      },
      // 2026 Target
      {
        label:"Target (2026)",
        type:"bar",
        data:target26,
        borderWidth:2,
        borderColor:"#93c5fd",
        datalabels:{ display:false }
      }
    ]},
    options:getCommonOptions(false)
  });

  monthlyCumInst = new Chart(document.getElementById("monthlyCumChart"),{
    type:"bar",
    data:{ labels, datasets:[
      {
        label: (filters.metric==="amount" ? "Cumulative Amount (2025)" : "Cumulative Qty (2025)"),
        type:"bar",
        data:salesCum25,
        backgroundColor:"#ABDEE6",
        categoryPercentage:0.8,
        barPercentage:0.9,
        datalabels:{ display:false }
      },
      {
        label: (filters.metric==="amount" ? "Cumulative Amount (2026)" : "Cumulative Qty (2026)"),
        type:"bar",
        data:salesCum26,
        backgroundColor:"#93c5fd",
        categoryPercentage:0.8,
        barPercentage:0.9,
        datalabels:{ display:false }
      },
      {
        label:"Cumulative Target (2026)",
        type:"bar",
        data:targetCum26,
        borderWidth:2,
        borderColor:"#93c5fd",
        datalabels:{ display:false }
      }
    ]},
    options:getCommonOptions(false)
  });
}

function buildMonthlyStacks(rows){
  const labels=monthsLabels();
  let groups = [...new Set(
    rows.map(r => r.group_label).filter(g => g != null && g !== "")
  )];

  const ordered = [];
  REGION_STACK_ORDER.forEach(name => {
    if (groups.includes(name)) ordered.push(name);
  });
  groups.forEach(g => {
    if (!REGION_STACK_ORDER.includes(g)) ordered.push(g);
  });
  groups = ordered;

  const byGroup = {};
  groups.forEach(g => byGroup[g] = Array(12).fill(0));
  // ...
  rows.forEach(r=>{ 
    const g = r.group_label;
    if (g == null || g === "") return;
    const m=parseInt(r.month,10); if(m>=1 && m<=12){ byGroup[r.group_label][m-1]+= (+r.value||0); } });
  const datasets=groups.map((g,i)=>({label:g,data:byGroup[g],backgroundColor:COLORS[i%COLORS.length],stack:"S", categoryPercentage:0.9, barPercentage:0.9, datalabels: {
          display: false}}));
  return {labels,groups,byGroup,datasets};
}

function toPercentStacks(byKey){
  const n=12, keys=Object.keys(byKey);
  const pct={}; keys.forEach(k=>pct[k]=Array(n).fill(0));
  for(let i=0;i<n;i++){
    const tot=keys.reduce((a,k)=>a+(+byKey[k][i]||0),0)||1;
    keys.forEach(k=>{ pct[k][i]= +(((byKey[k][i]/tot)*100)).toFixed(2); });
  }
  return pct;
}


async function drawMonthlyStacked(){
  const effectiveGroup = filters.group_by;

  // Fetch:
  // - 2025 actual breakdown (for comparison stack)
  // - 2026 actual breakdown
  // - 2026 target breakdown (NEW endpoint)
  // - 2026 actual totals (used for cutoff + consistency)
  const [rows25, rows26, rowsT26, sales26TotalRows] = await Promise.all([
    fetchMonthlyBreakdownWithGroup(effectiveGroup, 2025),
    fetchMonthlyBreakdownWithGroup(effectiveGroup, 2026),
    fetchMonthlyTargetBreakdownWithGroup(effectiveGroup, 2026),
    fetchMonthlySales(2026)
  ]);
  // Legend: show only the stack key (group label), not year/actual/target suffixes
  function withStackKeyLegend(baseOpts){
    return {
      ...baseOpts,
      plugins: {
        ...(baseOpts.plugins || {}),
        legend: {
          ...(baseOpts.plugins?.legend || {}),
          labels: {
            ...(baseOpts.plugins?.legend?.labels || {}),
            generateLabels(chart){
              const seen = new Set();
              const out = [];

              // Keep the first occurrence of each group key (NSW/QLD/... or Salesman name, etc.)
              for (let i = 0; i < chart.data.datasets.length; i++){
                const ds = chart.data.datasets[i];
                if (!ds) continue;
                if (ds.type === "line") continue; // ignore Ach line

                // Your labels are like: "NSW (2025)" or "NSW (2026 Target)"
                // Stack key is the part before " ("
                const key = String(ds.label || "").split(" (")[0].trim();
                if (!key || seen.has(key)) continue;

                seen.add(key);
                out.push({
                  text: key,
                  fillStyle: ds.backgroundColor,
                  strokeStyle: ds.borderColor || ds.backgroundColor,
                  lineWidth: 0,
                  hidden: false,
                  // Toggle visibility of the first dataset that represents this key
                  datasetIndex: i
                });
              }

              return out;
            }
          }
        }
      }
    };
  }


  const labels = monthsLabels();

  // Build {groups, byGroup} for month=1..12
  function buildMonthlyStacksForRows(rows){
    let groups = [...new Set(
      (rows||[]).map(r => r.group_label).filter(g => g != null && g !== "")
    )];

    // region ordering
    const ordered = [];
    REGION_STACK_ORDER.forEach(name => { if (groups.includes(name)) ordered.push(name); });
    groups.forEach(g => { if (!REGION_STACK_ORDER.includes(g)) ordered.push(g); });
    groups = ordered;

    const byGroup = {};
    groups.forEach(g => byGroup[g] = Array(12).fill(0));

    (rows||[]).forEach(r => {
      const g = r.group_label;
      if (!g) return;
      const m = parseInt(r.month, 10);
      if (m >= 1 && m <= 12) byGroup[g][m-1] += (+r.value || 0);
    });

    return { groups, byGroup };
  }

  // Actual stacks
  const { groups: groups25, byGroup: by25raw } = buildMonthlyStacksForRows(rows25);
  const { groups: groups26, byGroup: by26raw } = buildMonthlyStacksForRows(rows26);

  // Target stacks (2026)
  const { groups: groupsT26, byGroup: byT26raw } = buildMonthlyStacksForRows(rowsT26);

  // Union groups across 2025 actual, 2026 actual, 2026 target
  const groups = [
    ...groups25,
    ...groups26.filter(g => !groups25.includes(g)),
    ...groupsT26.filter(g => !groups25.includes(g) && !groups26.includes(g))
  ];

  // Normalize maps to the union groups
  const by25  = Object.fromEntries(groups.map(g => [g, (by25raw[g]  || Array(12).fill(0))]));
  const by26  = Object.fromEntries(groups.map(g => [g, (by26raw[g]  || Array(12).fill(0))]));
  const byT26 = Object.fromEntries(groups.map(g => [g, (byT26raw[g] || Array(12).fill(0))]));

  // 2026 actual totals (for cutoff)
  let sales26Total = labels.map((_,i)=> +sales26TotalRows[i]?.value || 0);

  // Cutoff: last non-zero month in 2026 actual totals
  let last26 = -1;
  for (let i=0;i<12;i++){
    if ((+sales26Total[i]||0) !== 0) last26 = i;
  }
  if (last26 < 0) last26 = 0;

  // Null out future months for 2026 series only (actual + target + totals)
  function nullAfterLastActual(arr, lastIdx){
    const out = arr.slice();
    for (let i=lastIdx+1;i<out.length;i++) out[i] = null;
    return out;
  }

  groups.forEach(g=>{
    by26[g]  = nullAfterLastActual(by26[g],  last26);
    byT26[g] = nullAfterLastActual(byT26[g], last26);
  });
  sales26Total = nullAfterLastActual(sales26Total, last26);

  // Build 2026 target total from target stacks (sum across groups)
  let target26Total = labels.map((_,i)=>{
    if (i > last26) return null;
    let t = 0;
    for (const g of groups) t += (+byT26[g][i] || 0);
    return t;
  });

  // Cumulative maps
  const by25Cum = cumPerGroup(by25);

  const by26Cum = cumPerGroup(Object.fromEntries(groups.map(g => [
    g,
    (by26[g] || Array(12).fill(null)).map(v => (v==null ? 0 : +v||0))
  ])));

  const byT26Cum = cumPerGroup(Object.fromEntries(groups.map(g => [
    g,
    (byT26[g] || Array(12).fill(null)).map(v => (v==null ? 0 : +v||0))
  ])));

  // Null out cumulative after cutoff for 2026 series
  groups.forEach(g=>{
    by26Cum[g]  = nullAfterLastActual(by26Cum[g]  || Array(12).fill(0), last26);
    byT26Cum[g] = nullAfterLastActual(byT26Cum[g] || Array(12).fill(0), last26);
  });

  // Cumulative totals for 2026 actual + 2026 target
  const sales26CumTotal  = Array(12).fill(null);
  const target26CumTotal = Array(12).fill(null);

  let sRun=0, tRun=0;
  for (let i=0;i<12;i++){
    if (i > last26) break;
    sRun += (+sales26Total[i]  || 0);
    tRun += (+target26Total[i] || 0);
    sales26CumTotal[i]  = sRun;
    target26CumTotal[i] = tRun;
  }


  // Percent helpers (for % charts: keep your original meaning = composition comparison)
  function toPercentStacksYear(byKey, lastIdxOrNull){
    const keys = Object.keys(byKey);
    const pct = {}; keys.forEach(k => pct[k] = Array(12).fill(null));

    for (let i=0;i<12;i++){
      if (lastIdxOrNull != null && i > lastIdxOrNull){
        keys.forEach(k => pct[k][i] = null);
        continue;
      }
      const tot = keys.reduce((a,k)=> a + (+byKey[k][i] || 0), 0);
      if (!tot){
        keys.forEach(k => pct[k][i] = 0);
      } else {
        keys.forEach(k => pct[k][i] = +(((+byKey[k][i]||0)/tot)*100).toFixed(2));
      }
    }
    return pct;
  }

  const pct25    = toPercentStacksYear(by25, null);
  const pct26    = toPercentStacksYear(
    Object.fromEntries(groups.map(g => [g, (by26[g]||Array(12).fill(null)).map(v => v==null ? 0 : +v||0)])),
    last26
  );

  const pct25Cum = toPercentStacksYear(by25Cum, null);
  const pct26Cum = toPercentStacksYear(
    Object.fromEntries(groups.map(g => [g, (by26Cum[g]||Array(12).fill(null)).map(v => v==null ? 0 : +v||0)])),
    last26
  );

  // Datasets:
  // - 2025 actual stack: Y2025
  // - 2026 actual stack: Y2026
  // - 2026 target stack: T2026 (stacked by group like actual)
  const ds25 = groups.map((g,i)=>({
    label: `${g} (2025)`,
    data: by25[g] || Array(12).fill(0),
    backgroundColor: COLORS[i%COLORS.length],
    stack: "Y2025",
    categoryPercentage: 0.9,
    barPercentage: 0.9,
    datalabels: { display:false }
  }));

  const ds26 = groups.map((g,i)=>({
    label: `${g} (2026 Actual)`,
    data: by26[g] || Array(12).fill(null),
    backgroundColor: COLORS[i%COLORS.length],
    stack: "Y2026",
    categoryPercentage: 0.9,
    barPercentage: 0.9,
    datalabels: { display:false }
  }));

  // Target stack uses the same group colors; it will appear as another stacked bar group
  const dsT26 = groups.map((g,i)=>({
    label: `${g} (2026 Target)`,
    data: byT26[g] || Array(12).fill(null),
    // same color as actual but lighter (alpha)
    backgroundColor: COLORS[i%COLORS.length].replace('ff', '80'),
    stack: "T2026",
    categoryPercentage: 0.9,
    barPercentage: 0.9,
    datalabels: { display:false }
  }));

  const ds25Cum = groups.map((g,i)=>({
    label: `${g} (2025)`,
    data: by25Cum[g] || Array(12).fill(0),
    backgroundColor: COLORS[i%COLORS.length],
    stack: "Y2025",
    categoryPercentage: 0.9,
    barPercentage: 0.9,
    datalabels: { display:false }
  }));

  const ds26Cum = groups.map((g,i)=>({
    label: `${g} (2026 Actual)`,
    data: by26Cum[g] || Array(12).fill(null),
    backgroundColor: COLORS[i%COLORS.length],
    stack: "Y2026",
    categoryPercentage: 0.9,
    barPercentage: 0.9,
    datalabels: { display:false }
  }));

  const dsT26Cum = groups.map((g,i)=>({
    label: `${g} (2026 Target)`,
    data: byT26Cum[g] || Array(12).fill(null),
    backgroundColor: COLORS[i%COLORS.length],
    // same color as actual but lighter (alpha)
    backgroundColor: COLORS[i%COLORS.length].replace('ff', '80'),
    stack: "T2026",
    categoryPercentage: 0.9,
    barPercentage: 0.9,
    datalabels: { display:false }
  }));

  const ds25Pct = groups.map((g,i)=>({
    label: `${g} (2025)`,
    data: pct25[g] || Array(12).fill(0),
    backgroundColor: COLORS[i%COLORS.length],
    stack: "Y2025",
    categoryPercentage: 0.9,
    barPercentage: 0.9,
    datalabels: { display:false }
  }));

  const ds26Pct = groups.map((g,i)=>({
    label: `${g} (2026)`,
    data: pct26[g] || Array(12).fill(null),
    backgroundColor: COLORS[i%COLORS.length],
    stack: "Y2026",
    categoryPercentage: 0.9,
    barPercentage: 0.9,
    datalabels: { display:false }
  }));

  const ds25PctCum = groups.map((g,i)=>({
    label: `${g} (2025)`,
    data: pct25Cum[g] || Array(12).fill(0),
    backgroundColor: COLORS[i%COLORS.length],
    stack: "Y2025",
    categoryPercentage: 0.9,
    barPercentage: 0.9,
    datalabels: { display:false }
  }));

  const ds26PctCum = groups.map((g,i)=>({
    label: `${g} (2026)`,
    data: pct26Cum[g] || Array(12).fill(null),
    backgroundColor: COLORS[i%COLORS.length],
    stack: "Y2026",
    categoryPercentage: 0.9,
    barPercentage: 0.9,
    datalabels: { display:false }
  }));

  // Destroy old
  [stackedMonthlyInst, stackedMonthlyCumInst, stackedMonthlyPctInst, stackedMonthlyCumPctInst]
    .forEach(c=>c && c.destroy());

  stackedMonthlyInst = new Chart(document.getElementById("stackedMonthlyChart"), {
    type:"bar",
    data:{ labels, datasets:[...ds25, ...ds26, ...dsT26] },
    options: withStackKeyLegend(
      getCommonOptions(true, undefined, "Monthly (2025 Actual / 2026 Actual+Target)")
    )
  });

  stackedMonthlyCumInst = new Chart(document.getElementById("stackedMonthlyCumChart"), {
    type:"bar",
    data:{ labels, datasets:[...ds25Cum, ...ds26Cum, ...dsT26Cum] },
    options: withStackKeyLegend(
      getCommonOptions(true, undefined, "Monthly (2025 Actual / 2026 Actual+Target)")
    )
  });

  stackedMonthlyPctInst = new Chart(document.getElementById("stackedMonthlyPercentChart"), {
    type:"bar",
    data:{ labels, datasets:[...ds25Pct, ...ds26Pct] },
    options: withStackKeyLegend(
      getCommonOptions(true, undefined, "Monthly (2025 Actual / 2026 Actual+Target)")
    )
  });

  stackedMonthlyCumPctInst = new Chart(document.getElementById("stackedMonthlyCumPercentChart"), {
    type:"bar",
    data:{ labels, datasets:[...ds25PctCum, ...ds26PctCum] },
    options: withStackKeyLegend(
      getCommonOptions(true, undefined, "Monthly (2025 Actual / 2026 Actual+Target)")
    )
  });
}



/* -------------------------- yearly charts -------------------------- */

// we already have: let yearlyInst, stackedYearlyInst, stackedYearlyPctInst;
// DON’T redeclare them again.

async function fetchYearlySales() {
  const qs = new URLSearchParams({
    metric:        filters.metric,
    category:      filters.category,
    region:        filters.region,
    salesman:      filters.salesman,
    sold_to_group: filters.sold_to_group,
    sold_to:       filters.sold_to,
    ship_to:       filters.ship_to,
    product_group: filters.product_group,
    pattern:       filters.pattern,
    material:      filters.material,
    material:      filters.material,
    top_limit:filters.top_limit ||0
  }).toString();
  return fetchJSON(`/api/yearly_sales?${qs}`);
}

async function fetchYearlyBreakdownWithGroup(groupBy) {
  const qs = new URLSearchParams({
    metric:        filters.metric,
    category:      filters.category,
    region:        filters.region,
    salesman:      filters.salesman,
    sold_to_group: filters.sold_to_group,
    sold_to:       filters.sold_to,
    ship_to:       filters.ship_to,
    product_group: filters.product_group,
    pattern:       filters.pattern,
    material:      filters.material,
    top_limit:filters.top_limit ||0,
    group_by:      groupBy
  }).toString();
  return fetchJSON(`/api/yearly_breakdown?${qs}`);
}


// simple yearly bar (no cumulative)
async function drawYearlyTotals() {
  const rows = await fetchYearlySales();   // [{year:2024, value:123}, ...]
  const labels = yearsLabels();
  // map 4 years -> values
  const data = labels.map(y => {
    const r = rows.find(row => +row.year === y);
    return r ? +r.value || 0 : 0;
  });

  if (yearlyInst) {
    yearlyInst.destroy();
  }

  yearlyInst = new Chart(document.getElementById("yearlyChart"), {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: filters.metric === "amount" ? "Yearly Amount" : "Yearly Qty",
          data,
          backgroundColor: "#ABDEE6",
          categoryPercentage: 0.9,
          barPercentage: 0.9
        }
      ]
    },
    options: getCommonOptions(false, undefined, "Yearly")
  });
}

// build stacks for yearly
function buildYearlyStacks(rows) {
  const labels = yearsLabels();
  let groups = [...new Set(
    rows.map(r => r.group_label).filter(g => g != null && g !== "")
  )];

  const ordered = [];
  REGION_STACK_ORDER.forEach(name => {
    if (groups.includes(name)) ordered.push(name);
  });
  groups.forEach(g => {
    if (!REGION_STACK_ORDER.includes(g)) ordered.push(g);
  });
  groups = ordered;

  const byGroup = {};
  groups.forEach(g => (byGroup[g] = Array(labels.length).fill(0)));
  rows.forEach(r => {
  const g = r.group_label;
  if (g == null || g === "") return;       // skip null / empty groups

  const y = parseInt(r.year, 10);
  const idx = labels.indexOf(y);
  if (idx !== -1) {
    byGroup[g][idx] += (+r.value || 0);
  }
  });

  const datasets = groups.map((g, i) => ({
    label: g,
    data: byGroup[g],
    backgroundColor: COLORS[i % COLORS.length],
    stack: "S",
    categoryPercentage: 0.9,
    barPercentage: 0.9
  }));

  return { labels, groups, byGroup, datasets };
}

// % helper for N=number of years
function toPercentStacksNYears(byKey, labelsLen) {
  const keys = Object.keys(byKey);
  const pct = {};
  keys.forEach(k => (pct[k] = Array(labelsLen).fill(0)));
  for (let i = 0; i < labelsLen; i++) {
    const tot = keys.reduce((a, k) => a + (+byKey[k][i] || 0), 0) || 1;
    keys.forEach(k => {
      pct[k][i] = +(((byKey[k][i] || 0) / tot) * 100).toFixed(2);
    });
  }
  return pct;
}

async function drawYearlyStacked() {
  const effectiveGroup = filters.group_by;
  const rows = await fetchYearlyBreakdownWithGroup(effectiveGroup);

  // no data -> show single total per year
  if (!rows || !rows.length) {
    const totals = await fetchYearlySales();
    const labels = yearsLabels();
    const data = labels.map(y => {
      const r = totals.find(t => +t.year === y);
      return r ? +r.value || 0 : 0;
    });

    if (stackedYearlyInst) stackedYearlyInst.destroy();
    if (stackedYearlyPctInst) stackedYearlyPctInst.destroy();

    stackedYearlyInst = makeStacked(
      "stackedYearlyChart",
      labels,
      [
        {
          label: "Total",
          data,
          backgroundColor: "#a78bfa",
          stack: "S",
          categoryPercentage: 0.9,
          barPercentage: 0.9
        }
      ],
      "Yearly"
    );

    stackedYearlyPctInst = makeStacked(
      "stackedYearlyPercentChart",
      labels,
      [
        {
          label: "Total %",
          data: labels.map(() => 100),
          backgroundColor: "#a78bfa",
          stack: "S",
          categoryPercentage: 0.9,
          barPercentage: 0.9
        }
      ],
      "Yearly %",
      100
    );
    return;
  }

  // build stacks from real rows
  let { labels, groups, byGroup, datasets } = buildYearlyStacks(rows);

  const pct = toPercentStacksNYears(byGroup, labels.length);
  const datasetsPct = groups.map((g, i) => ({
    label: g,
    data: pct[g],
    backgroundColor: COLORS[i % COLORS.length],
    stack: "S",
    categoryPercentage: 0.9,
    barPercentage: 0.9
  }));

  if (stackedYearlyInst) stackedYearlyInst.destroy();
  if (stackedYearlyPctInst) stackedYearlyPctInst.destroy();

  stackedYearlyInst = new Chart(document.getElementById("stackedYearlyChart"), {
    type: "bar",
    data: { labels, datasets },
    options: getCommonOptions(true, undefined, "Yearly")
  });

  stackedYearlyPctInst = new Chart(
    document.getElementById("stackedYearlyPercentChart"),
    {
      type: "bar",
      data: { labels, datasets: datasetsPct },
      options: getCommonOptions(true, 100, "Yearly %")
    }
  );
}

/* -------------------------- init & orchestrator -------------------------- */
async function initControls(){
  setActive(document.getElementById('metricBtns'),"metric",filters.metric);
  setActive(document.getElementById('regionBtns'),"val",filters.region);
  populateSelect(document.getElementById('salesman_name'),[...new Set(Object.values(REGION_SALESMEN).flat())].sort());

  const groups = await fetchJSON("/api/product_group");
  __PRODUCT_GROUP_OPTIONS = Array.isArray(groups) ? groups : (groups?.rows || []);

  const stg3 = await fetchJSON("/api/sold_to_groups");
  populateSelect(document.getElementById('sold_to_group'), stg3, true);
  document.getElementById('sold_to_group').value = "ALL";

  await refreshSoldToList();
  await refreshShipTo();
}

/* ===================== PROFIT (COMBINED) ===================== */

let profitComboInst = null;

const PROFIT_MONTH_LABELS = [
  "Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"
];

function renderProfitCombined(rows) {
  const el = document.getElementById("profitComboChart");
  if (!el) return;

  if (profitComboInst) {
    profitComboInst.destroy();
    profitComboInst = null;
  }

  // Ensure we always have 12 months
  const byMonth = Array.from({ length: 12 }, (_, i) =>
    rows.find(r => +r.month === i + 1) || { month: i + 1, gross: 0, sd: 0, cogs: 0, op_cost: 0 }
  );

  const gross = byMonth.map(r => +r.gross || 0);
  const sd    = byMonth.map(r => +r.sd || 0);
  const cogs  = byMonth.map(r => +r.cogs || 0);
  const op    = byMonth.map(r => +r.op_cost || 0);

  const totalCost = sd.map((v, i) => sd[i] + cogs[i] + op[i]);
  const profitPct = gross.map((g, i) => (g > 0 ? ((g - totalCost[i]) / g) * 100 : 0));

  profitComboInst = new Chart(el, {
    type: "bar",
    data: {
      labels: PROFIT_MONTH_LABELS,
      datasets: [
        // Line: Profit %
        {
          type: "line",
          label: "Profit %",
          data: profitPct,
          yAxisID: "y1",   // right axis
          tension: 0.25,
          borderWidth: 2,
          pointRadius: 2,
          fill: false,
          borderColor: "#10b981",
          pointBackgroundColor: "#10b981",
          datalabels: {
            align: "top",
            formatter: (v) => v == null ? "" : v.toFixed(2) + "%",
            font: {
            weight: "bold",
            size : 20
            }
          }
        },
        // Bar group 1: Gross
        {
          type: "bar",
          label: "Gross",
          data: gross,
          yAxisID: "y",
          stack: "G",
          backgroundColor: "#93c5fd",
          borderWidth: 1,
          categoryPercentage: 0.9,
          barPercentage: 0.9
        },
        // Bar group 2: stacked Costs (beside Gross)
       
        {
          type: "bar",
          label: "COGS",
          data: cogs,
          yAxisID: "y",
          stack: "C",
          backgroundColor: "#f87171",
          categoryPercentage: 0.9,
          barPercentage: 0.9
        },
        {
          type: "bar",
          label: "Op Cost",
          data: op,
          yAxisID: "y",
          stack: "C",
          backgroundColor: "#fbbf24",
          categoryPercentage: 0.9,
          barPercentage: 0.9
        },
        {
          type: "bar",
          label: "Sales Deduction",
          data: sd,
          yAxisID: "y",
          stack: "C",
          backgroundColor: "#d55fc3ff",
          categoryPercentage: 0.9,
          barPercentage: 0.9
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      scales: {
        x: { stacked: false },
        y: {
          beginAtZero: true,
          title: { display: true, text: "Amount" },
          ticks: { callback: v => Number(v).toLocaleString() }
        },
        y1: {
          position: "right",
          beginAtZero: true,
          suggestedMax: 100,
          title: { display: true, text: "Profit %" },
          grid: { drawOnChartArea: false },
          ticks: { callback: v => `${Math.round(v)}%` }
        }
      },
      plugins: {
        datalabels: { display: false },
        legend: { display: true },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              if (ctx.dataset.yAxisID === "y1") {
                return `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(1)}%`;
              }
              return `${ctx.dataset.label}: ${Number(ctx.parsed.y || 0).toLocaleString()}`;
            }
          }
        }
      }
    }
  });
}

async function loadProfit() {
  const qs = new URLSearchParams({
    metric:        filters.metric,
    category:      filters.category,
    region:        filters.region,
    salesman:      filters.salesman,
    sold_to_group: filters.sold_to_group,
    sold_to:       filters.sold_to,
    ship_to:       filters.ship_to,
    product_group: filters.product_group,
    pattern:       filters.pattern,
    material:      filters.material,
    top_limit:filters.top_limit ||0
  }).toString();

  const rows = await fetchJSON(`/api/profit_monthly?${qs}`);
  renderProfitCombined(Array.isArray(rows) ? rows : []);
}

/* =================== END PROFIT (COMBINED) =================== */


async function refreshAllWithKpi(){
  // save current filters so map.html can read them
   // save current filters so other views (e.g. map) can read them
  localStorage.setItem("salesFilters", JSON.stringify(filters));

  const hasMap       = document.getElementById('salesMap');
  const hasDashboard = document.getElementById('dailyChart');

  // If we are on the map page and map.js has defined loadSalesMap,
  // delegate to that instead of drawing all the graphs.
  if (hasMap) {
    if (typeof loadSalesMap === "function") {
      await loadSalesMap();
    }
    // on map view we don't need to run the big dashboard stack
    return;
  }

  // If we don't even have the main dashboard canvases, do nothing.
  if (!hasDashboard) {
    return;
  }
  // Run all chart sections in parallel
  await Promise.all([
    loadProfit(),
    drawDailyTotals(),
    drawDailyStacked(),
    drawMonthlyKPI(),
    drawMonthlyTotals(),
    drawMonthlyStacked(),
    drawYearlyTotals(),
    drawYearlyStacked(),
  ]);
  
}
let refreshTimer = null;
function refreshAllDebounced(){
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(() => refreshAllWithKpi(), 250);
}

(async function start(){
  await initControls();
  
  [...document.querySelectorAll("#catBtns .btn")].forEach(b=>b.classList.toggle("active",b.dataset.val===filters.category));
  await refreshAllWithKpi();
  
  await refreshPatterns();                 // make pattern list available on load
  document.getElementById('ship_to').disabled = false; // locked until Sold-to is chosen
})();
// Global capture-based select-all for datalist/text inputs (most reliable)
// This overrides browser caret placement even when the input is already focused.
(function enableGlobalSelectAllForFilters(){
  const IDS = new Set(['sold_to', 'ship_to', 'pattern', 'material']);

  const isTarget = (el) => el && el.id && IDS.has(el.id);

  const selectAll = (el) => {
    if (!el) return;
    const v = el.value || "";
    if (!v.length) return;
    try {
      el.setSelectionRange(0, v.length);
    } catch (_) {
      el.select();
    }
  };

  // Capture mousedown before the browser places the caret
  document.addEventListener('mousedown', (e) => {
    const el = e.target;
    if (!isTarget(el)) return;
    if ((el.value || "").length === 0) return;
    if (e.button === 2) return; // allow right-click context menu

    e.preventDefault();          // stop caret placement
    el.focus();
    // Re-apply selection after focus settles
    setTimeout(() => selectAll(el), 0);
  }, true);

  // Some browsers collapse selection on mouseup; force it again
  document.addEventListener('mouseup', (e) => {
    const el = e.target;
    if (!isTarget(el)) return;
    if ((el.value || "").length === 0) return;

    e.preventDefault();
    setTimeout(() => selectAll(el), 0);
  }, true);

  // Keyboard focus (Tab) should also select all
  document.addEventListener('focusin', (e) => {
    const el = e.target;
    if (!isTarget(el)) return;
    setTimeout(() => selectAll(el), 0);
  }, true);
})();
// ------------------------------
// Pattern / Material custom dropdown redesign (NO datalist)
// ------------------------------
let __PRODUCT_GROUP_OPTIONS = [];
let __PATTERN_OPTIONS = [];
let __MATERIAL_OPTIONS = [];


let __SOLD_TO_OPTIONS = [];
let __SHIP_TO_OPTIONS = [];

async function refreshSoldToCustom(){
  const stg = document.getElementById("sold_to_group")?.value || "ALL";
  const res = await fetchJSON(`/api/sold_to_names?sold_to_group=${encodeURIComponent(stg)}`);
  const rows = Array.isArray(res) ? res : (res?.rows || []);
  __SOLD_TO_OPTIONS = rows.map(x => String(x)).filter(Boolean);
}

async function refreshShipToCustom(){
  const stg = document.getElementById("sold_to_group")?.value || "ALL";
  const soldTo = (document.getElementById("sold_to")?.value || "").trim();
  const qs = new URLSearchParams({
    sold_to_group: stg,
    sold_to: soldTo ? soldTo : "ALL"
  }).toString();

  const res = await fetchJSON(`/api/ship_to_names?${qs}`);
  const rows = Array.isArray(res) ? res : (res?.rows || []);
  __SHIP_TO_OPTIONS = rows.map(x => String(x)).filter(Boolean);
}

function ddOpen(menuEl){ if (menuEl) menuEl.style.display = "block"; }
function ddClose(menuEl){ if (menuEl) menuEl.style.display = "none"; }

function ddRender(menuEl, items, onPick){
  if (!menuEl) return;
  menuEl.innerHTML = "";
  if (!items || items.length === 0){
    const div = document.createElement("div");
    div.className = "dd-empty";
    div.textContent = "No results";
    menuEl.appendChild(div);
    return;
  }
  items.forEach(v => {
    const div = document.createElement("div");
    div.className = "dd-item";
    div.textContent = v;
    div.addEventListener("mousedown", (e) => {
      e.preventDefault(); // keep focus
      onPick(v);
    });
    menuEl.appendChild(div);
  });
}

function ddFilter(options, q){
  const s = (q || "").trim().toLowerCase();
  if (!s) return options.slice(0, 500);
  return options.filter(x => String(x).toLowerCase().includes(s)).slice(0, 500);
}

function ddUpdateActive(inp) {
  const dd = inp.closest('.dd');
  if (dd) dd.classList.toggle('dd-active', inp.value.trim() !== '');
}

function bindDropdown({ inputId, btnId, clearId, menuId, getOptions, onPick }){
  const inp = document.getElementById(inputId);
  const btn = document.getElementById(btnId);
  const clr = clearId ? document.getElementById(clearId) : null;
  const menu = document.getElementById(menuId);
  if (!inp || !btn || !menu) return;

  function openWithCurrent(){
    const opts = getOptions();
    ddRender(menu, ddFilter(opts, inp.value), onPick);
    ddOpen(menu);
  }

  // button always opens full list (no need to clear)
  btn.addEventListener("click", () => openWithCurrent());

  // focus also opens list
  inp.addEventListener("focus", () => openWithCurrent());

  // typing filters list (still allows reselect without clearing via ▼)
  inp.addEventListener("input", () => openWithCurrent());

  // update active state whenever input value changes
  inp.addEventListener("input", () => ddUpdateActive(inp));

  // clear (optional)
  if (clr) {
    clr.addEventListener("click", () => {
      inp.value = "";
      ddUpdateActive(inp);
      onPick("ALL");
      ddClose(menu);
      inp.focus();
    });
  }

  // close when clicking outside
  document.addEventListener("mousedown", (e) => {
    if (!menu.contains(e.target) && e.target !== inp && e.target !== btn && e.target !== clr){
      ddClose(menu);
    }
  });
}

// fetch helpers you already have: fetchJSON(url)

async function refreshPatternsCustom(){
  const pg = document.getElementById("product_group")?.value || "ALL";
  const res = await fetchJSON(`/api/patterns?product_group=${encodeURIComponent(pg)}`);
  // your backend sometimes returns {rows:[...]} or [...]
  const rows = Array.isArray(res) ? res : (res?.rows || []);
  __PATTERN_OPTIONS = rows.map(x => String(x)).filter(Boolean);
}

async function refreshMaterialsCustom(){
  const pg = document.getElementById("product_group")?.value || "ALL";
  const pat = (document.getElementById("pattern")?.value || "").trim();
  const qs = new URLSearchParams({
    product_group: pg,
    // pattern이 있으면 material을 더 좁히고, 없으면 전체 material
    ...(pat ? { pattern: pat } : {})
  }).toString();

  const res = await fetchJSON(`/api/materials?${qs}`);
  const rows = Array.isArray(res) ? res : (res?.rows || []);
  __MATERIAL_OPTIONS = rows.map(x => String(x)).filter(Boolean);
}

// bind once on load
window.addEventListener("load", async () => {
  // initial load
  await refreshPatternsCustom();
  await refreshMaterialsCustom();

  bindDropdown({
    inputId: "product_group",
    btnId: "pgBtn",
    menuId: "pgMenu",
    getOptions: () => __PRODUCT_GROUP_OPTIONS,
    onPick: async (val) => {
      const v = (val === "ALL") ? "" : val;
      const pgEl = document.getElementById("product_group");
      if (pgEl) { pgEl.value = v; ddUpdateActive(pgEl); }
      filters.product_group = v || "ALL";
      const patEl = document.getElementById('pattern');
      const matEl = document.getElementById('material');
      if (patEl) { patEl.value = ""; ddUpdateActive(patEl); }
      if (matEl) { matEl.value = ""; ddUpdateActive(matEl); }
      filters.pattern = "ALL";
      filters.material = "ALL";
      await refreshPatternsCustom();
      await refreshMaterialsCustom();
      await refreshSoldToCustom();
      await refreshShipToCustom();
      refreshAllDebounced();
    }
  });

  bindDropdown({
    inputId: "sold_to",
    btnId: "soldToBtn",
    clearId: "soldToClear",
    menuId: "soldToMenu",
    getOptions: () => __SOLD_TO_OPTIONS,
    onPick: async (val) => {
      const v = (val === "ALL") ? "" : val;
      const soldEl = document.getElementById("sold_to");
      const shipEl = document.getElementById("ship_to");
      if (soldEl) { soldEl.value = v; ddUpdateActive(soldEl); }
      filters.sold_to = v || "ALL";

      // sold-to changes -> reset ship-to + reload ship-to list
      if (shipEl) { shipEl.value = ""; ddUpdateActive(shipEl); }
      filters.ship_to = "ALL";
      await refreshShipToCustom();

      refreshAllDebounced();
    }
  });

  bindDropdown({
    inputId: "ship_to",
    btnId: "shipToBtn",
    clearId: "shipToClear",
    menuId: "shipToMenu",
    getOptions: () => __SHIP_TO_OPTIONS,
    onPick: (val) => {
      const v = (val === "ALL") ? "" : val;
      const shipEl = document.getElementById("ship_to");
      if (shipEl) { shipEl.value = v; ddUpdateActive(shipEl); }
      filters.ship_to = v || "ALL";
      refreshAllDebounced();
    }
  });

  bindDropdown({
    inputId: "pattern",
    btnId: "patternBtn",
    clearId: "patternClear",
    menuId: "patternMenu",
    getOptions: () => __PATTERN_OPTIONS,
    onPick: async (val) => {
      const v = (val === "ALL") ? "" : val;
      const patEl = document.getElementById("pattern");
      patEl.value = v; ddUpdateActive(patEl);
      filters.pattern = v || "ALL";
      await refreshMaterialsCustom();
      refreshAllDebounced();
    }
  });

  bindDropdown({
    inputId: "material",
    btnId: "materialBtn",
    clearId: "materialClear",
    menuId: "materialMenu",
    getOptions: () => __MATERIAL_OPTIONS,
    onPick: (val) => {
      const v = (val === "ALL") ? "" : val;
      const matEl = document.getElementById("material");
      matEl.value = v; ddUpdateActive(matEl);
      filters.material = v || "ALL";
      refreshAllDebounced();
    }
  });
});

