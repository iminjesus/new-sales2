// --- fast defaults ---

Chart.defaults.animation = false;               // no animations
Chart.defaults.responsiveAnimationDuration = 0;
Chart.defaults.normalized = true;               // faster parsing
Chart.defaults.elements.bar.borderWidth = 0;

// turn value labels OFF (they’re expensive to draw)
const SHOW_BAR_VALUES = false;
if (SHOW_BAR_VALUES && !Chart.registry.plugins.get("showDataValues")) {
  Chart.register(showDataValuesPlugin);
}
// DataLabels: show only for line datasets (no labels on bars)
Chart.defaults.plugins = Chart.defaults.plugins || {};
Chart.defaults.plugins.datalabels = {
  display: (ctx) => {
    const type = ctx.dataset.type || ctx.chart.config.type;
    return type === "line";          // true for lines, false for bars
  }
};
const REGION_STACK_ORDER = ["NSW", "QLD", "VIC", "SA", "WA", "COMMON"];
/* -------------------------- state & helpers -------------------------- */
const COLORS=["#374388ff","#90359cff","#3e9150ff","#93b14dff","#d6c635ff","#95E4E5","#275ccfff","#175d96ff","#366592ff","#667c91ff","#FF968A","#FFAEA6"];
const REGION_SALESMEN={
  NSW:["Hamid Jallis","LUTTRELL STEVE","Cummings Mark","Lee Don"],
  QLD:["Maclure Adam","Spires Steven","Sampson Kieren","Marsh Aaron"],
  VIC:["Bellotto Nicola","Bilston Kelley","Gultjaeff Jason","Hobkirk Calvin"],
  WA:["Fruci Davide","Gilbert Michael"]
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
  list.innerHTML = '';
  (items||[]).forEach(v=>{
    const o = document.createElement('option');
    o.value = v; list.appendChild(o);
  });
}

async function refreshSoldToList() {
  const qs = new URLSearchParams({
    sold_to_group: filters.sold_to_group || "ALL",
    metric:        filters.metric,
    category:      filters.category,
    region:        filters.region,
    salesman:      filters.salesman,
    sold_to:       filters.sold_to,
    ship_to:       filters.ship_to,
    product_group: filters.product_group,
    pattern:       filters.pattern,
    top_limit:     filters.top_limit || 0   // 0 = show all
  }).toString();

  const soldTo = await fetchJSON("/api/sold_to_names?" + qs);
  const list   = document.getElementById("sold_to_list");
  if (!list) return;

  list.innerHTML = "";
  soldTo.forEach(n => {
    const o = document.createElement("option");
    o.value = n;
    list.appendChild(o);
  });
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
  // This fallback matches your APIs: metric, category, region, salesman, sold_to_group, sold_to, product_group, ship_to, pattern
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

  return { metric, category, region, salesman, sold_to_group, sold_to, product_group, ship_to, pattern };
}


/* -------------------------- UI wiring -------------------------- */
document.getElementById('catBtns').addEventListener("click",e=>{
  if(!e.target.classList.contains("btn"))return;
  filters.category=e.target.dataset.val;
  [...document.querySelectorAll("#catBtns .btn")].forEach(b=>b.classList.toggle("active",b.dataset.val===filters.category));
  refreshAllWithKpi();
});
document.getElementById('metricBtns').addEventListener("click",e=>{
  if(!e.target.classList.contains("btn"))return;
  filters.metric=e.target.dataset.metric;
  setActive(document.getElementById('metricBtns'),"metric",filters.metric);
  document.getElementById('dailyTitle').textContent = filters.metric==="amount"?"Daily Amount":"Daily Sales";
  document.getElementById('cumTitle').textContent   = filters.metric==="amount"?"Cumulative Amount":"Cumulative Sales";
  refreshAllWithKpi();
});
document.getElementById('group_by').addEventListener("change",()=>{
  filters.group_by=document.getElementById('group_by').value;
  refreshAllWithKpi();
});
document.getElementById('regionBtns').addEventListener("click",e=>{
  if(!e.target.classList.contains("btn"))return;
  filters.region=e.target.dataset.val; setActive(document.getElementById('regionBtns'),"val",filters.region);
  const all=Object.values(REGION_SALESMEN).flat();
  const list=filters.region==="ALL"?all:(REGION_SALESMEN[filters.region]||[]);
  populateSelect(document.getElementById('salesman_name'),[...new Set(list)].sort());
  filters.salesman = 'ALL';
  document.getElementById('salesman_name').value = 'ALL';
  refreshAllWithKpi();
});

document.getElementById('salesman_name').addEventListener('change', (e)=>{
  filters.salesman = e.target.value || 'ALL';
  refreshAllWithKpi();
});
document.getElementById('sold_to_group').addEventListener('change', async ()=>{  filters.sold_to_group = document.getElementById('sold_to_group').value || 'ALL';

  const names = await fetchJSON(`/api/sold_to_names?sold_to_group=${document.getElementById('sold_to_group').value}`);
  populateDatalist('sold_to_list', names);
  await refreshShipTo(); // NEW: keep ship-to list in sync with group
  refreshAllWithKpi();
});

// When SOLD_TO_GROUP changes you already reload sold_to options — keep as is.

// SOLD-TO -> fetch Ship-to names under that Sold-to, enable input
document.getElementById('sold_to').addEventListener('input', async (e) => {
  filters.sold_to = e.target.value || 'ALL';
   await refreshShipTo();
  const shipInput = document.getElementById('ship_to');
  const listId = 'ship_to_list';

  if (filters.sold_to && filters.sold_to !== 'ALL') {
    const qs = new URLSearchParams({ sold_to: filters.sold_to }).toString();
    const names = await fetchJSON(`/api/ship_to_names?${qs}`);
    populateDatalist(listId, names);
    shipInput.disabled = false;
  } else {
    populateDatalist(listId, []);
    shipInput.value = '';
    shipInput.disabled = true;
    filters.ship_to = 'ALL';
  }
  refreshAllWithKpi();
});

// Ship-to input -> mirror into filters
document.getElementById('ship_to').addEventListener('input', (e)=>{
  filters.ship_to = document.getElementById('ship_to').value || 'ALL';
  refreshAllWithKpi();
});

// PRODUCT GROUP -> existing code… plus refresh patterns
document.getElementById('product_group').addEventListener('change', async ()=>{
  filters.product_group = document.getElementById('product_group').value || 'ALL';
  await refreshPatterns();     // NEW
  refreshAllWithKpi();
});

// PATTERN input -> mirror into filters
document.getElementById('pattern').addEventListener('input', (e)=>{
  filters.pattern = e.target.value || 'ALL';
  refreshAllWithKpi();
});

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
  refreshAllWithKpi();
});

async function refreshShipTo(){
  const stg3 = document.getElementById('sold_to_group').value || 'ALL';
  const sold = document.getElementById('sold_to').value || 'ALL';
  const qs = new URLSearchParams({ sold_to_group: stg3, sold_to: sold }).toString();
  const names = await fetchJSON(`/api/ship_to_names?${qs}`);
  populateDatalist('ship_to_list', names);
  refreshAllWithKpi();
}

// Load patterns for current product group
async function refreshPatterns(){
  const pg = document.getElementById('product_group').value || 'ALL';
  const names = await fetchJSON(`/api/patterns?product_group=${encodeURIComponent(pg)}`);
  populateDatalist('pattern_list', names);
  refreshAllWithKpi();
}






/* -------------------------- daily (Oct) – same structure as monthly -------------------------- */

async function fetchDailySales(){
  const qs = new URLSearchParams({
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, top_limit:filters.top_limit ||0
  }).toString();
  return fetchJSON(`/api/daily_sales?${qs}`);
}

async function fetchDailyKPIActual(region,BDE){
  const qs=new URLSearchParams({
    metric:filters.metric, category:filters.category, region:region, salesman:BDE,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, top_limit:filters.top_limit ||0
  }).toString();
  return fetchJSON(`/api/daily_sales?${qs}`);
}

async function fetchDailyKPITarget(region,BDE){
  const qs=new URLSearchParams({
    metric:filters.metric, category:filters.category, region:region, salesman:BDE,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, top_limit:filters.top_limit ||0
  }).toString();
  return fetchJSON(`/api/daily_target?${qs}`);
}

async function fetchDailyBreakdownWithGroup(groupBy){
  const qs = new URLSearchParams({
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, group_by: groupBy, top_limit:filters.top_limit ||0
  }).toString();
  return fetchJSON(`/api/daily_breakdown?${qs}`);
}


// totals (bar + cumulative), same shape as drawDailyTotals (no target for daily)
async function drawDailyTotals(){
  const [salesRows,targetRows]=await Promise.all([
    fetchDailySales(),
    fetchJSON(`/api/daily_target?${new URLSearchParams({
      metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
      sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
      product_group:filters.product_group, pattern:filters.pattern, top_limit:filters.top_limit ||0
    }).toString()}`)
  ]);
    const labels = daysLabels();
  const sales  = salesRows.map(r => +r.value || 0);
  const targets = targetRows.map(r => +r.value || 0);

  const salesCum  = toCumulative(sales);
  const targetCum = toCumulative(targets);

  const achievement = [];
  const cumAchievement = [];

  let lastAch = null;
  let lastCumAch = null;

  for (let i = 0; i < labels.length; i++) {
    const s  = sales[i]   || 0;
    const t  = targets[i] || 0;
    const sc = salesCum[i]   || 0;
    const tc = targetCum[i]  || 0;

    // daily %
    if (t > 0 && s > 0) {
      lastAch = (s / t) * 100;
      achievement.push(lastAch);
    } else if (t > 0 && s === 0 && lastAch != null) {
      // no sales today -> keep previous value
      achievement.push(lastAch);
    } else {
      achievement.push(null);
    }

    // cumulative %
    if (tc > 0 && sc > 0) {
      lastCumAch = (sc / tc) * 100;
      cumAchievement.push(lastCumAch);
    } else if (tc > 0 && sc === 0 && lastCumAch != null) {
      // no additional sales -> keep previous cumulative %
      cumAchievement.push(lastCumAch);
    } else {
      cumAchievement.push(null);
    }
  }


  [dailyInst,dailyCumInst].forEach(c=>c&&c.destroy());
  //if (!Chart.registry.plugins.get("showDataValues")) {
  //  Chart.register(showDataValuesPlugin);
  //}
  
  
  dailyInst=new Chart(document.getElementById("dailyChart"),{
      type:"bar",
      data:{labels,datasets:[
        {label:"Achievement(%)",type:"line", data: achievement,  yAxisID: "y1", borderWidth:2,pointRadius:0,borderColor:"#ef4444",  datalabels: {
            display: (ctx) => {
            const i = ctx.dataIndex;
            return (i % 2 === 0) && (sales[i] > 0);
          },
          align: "top",
          anchor: "end",
          formatter: v => v == null ? "" : v.toFixed(1) + "%"
          }
        },
        {label:filters.metric==="amount"?"Sales Amount":"SalesQty",data:sales, backgroundColor:"#ABDEE6", categoryPercentage:0.9, barPercentage:0.9, z:0, datalabels: {
            display: false}},
        {label:"Target",type:"bar",data:targets, borderWidth:2, borderColor:"#ABDEE6",datalabels: {
            display: false}}
        
      ]},
      options:getCommonOptions(false)
    });
    
  dailyCumInst=new Chart(document.getElementById("dailyCumChart"),{
    type:"bar",
    data:{labels,datasets:[
      {label:"Achievement(%)",type:"line", data: cumAchievement,  yAxisID: "y1", borderWidth:2,pointRadius:0,borderColor:"#ef4444", datalabels: {
        display: (ctx) => {
          const i = ctx.dataIndex;
          return (i % 2 === 0) && (sales[i] > 0);
        },
        align: "top",
        anchor: "end",
        formatter: v => v == null ? "" : v.toFixed(1) + "%"
      }},
      {label:filters.metric==="amount"?"Cumulative Amount":"Cumulative Qty",data:salesCum,backgroundColor:"#ABDEE6", categoryPercentage:0.9, barPercentage:0.9, datalabels: {
          display: false}},
      {label:"Cumulative Target",type:"bar",data:targetCum,borderWidth:2,borderWidth:2, borderColor:"#ABDEE6", datalabels: {
          display: false}}      
    ]},
    options:getCommonOptions(false)
  });
}

function buildDailyStacks(rows){
  const labels = daysLabels();;
  // ignore null / empty groups
  // ignore null / empty groups
  let groups = [...new Set(
    rows.map(r => r.group_label).filter(g => g != null && g !== "")
  )];

  // force region stack order NSW, QLD, VIC, SA, WA, COMMON
  const ordered = [];
  REGION_STACK_ORDER.forEach(name => {
    if (groups.includes(name)) ordered.push(name);
  });
  groups.forEach(g => {
    if (!REGION_STACK_ORDER.includes(g)) ordered.push(g);
  });
  groups = ordered;

  const byGroup = {};
  groups.forEach(g => byGroup[g] = Array(31).fill(0));
  rows.forEach(r => {
    const g = r.group_label;
    if (g == null || g === "") return;          // skip null group
    const d = parseInt(r.day, 10);
    if (d>=1 && d<=31) byGroup[r.group_label][d-1] += (+r.value || 0);
  });
  const datasets = groups.map((g,i)=>({
    label:g,
    data:byGroup[g],
    backgroundColor:COLORS[i%COLORS.length],
    stack:"S", categoryPercentage:0.9, barPercentage:0.9
  }));
  return { labels, groups, byGroup, datasets };
}

function toPercentStacksN(byKey, N){
  const keys = Object.keys(byKey);
  const pct = {}; keys.forEach(k => pct[k] = Array(N).fill(0));
  for (let i=0; i<N; i++){
    const tot = keys.reduce((a,k)=> a + (+byKey[k][i]||0), 0) || 1;
    keys.forEach(k => pct[k][i] = +((byKey[k][i] / tot) * 100).toFixed(2));
  }
  return pct;
}


// stacked (value / cumulative / % / cumulative %) — identical to monthly version
async function drawDailyStacked(){
  const effectiveGroup = filters.group_by;
  const rows = await fetchDailyBreakdownWithGroup(effectiveGroup);
  
  if (!rows || !rows.length){
    
    const totals = await fetchDailySales();
    const labels = daysLabels();;
    const data=totals.map(r=>+r.value||0);
    const cum = toCumulative(data);
    stackedDailyInst = makeStacked("stackedDailyChart", labels, [
      { label:"Total", data, backgroundColor:"#a78bfa", stack:"S", categoryPercentage:0.9, barPercentage:0.9 }
    ], "Daily");

    stackedDailyPctInst = makeStacked("stackedDailyPercentChart", labels, [
      { label:"Total %", data: labels.map(()=>100), backgroundColor:"#a78bfa", stack:"S", categoryPercentage:0.9, barPercentage:0.9 }
    ], "Daily %", 100);

    
    // IMPORTANT: write to the same IDs you use in HTML
    stackedDailyCumInst = makeStacked("stackedDailyCumChart", labels, [
      { label:"Total", data:cum, backgroundColor:"#10b981", stack:"S", categoryPercentage:0.9, barPercentage:0.9 }
    ], "Cumulative by Day");

    stackedDailyCumPctInst = makeStacked("stackedDailyCumPercentChart", labels, [
      { label:"Total %", data: labels.map(()=>100), backgroundColor:"#10b981", stack:"S", categoryPercentage:0.9, barPercentage:0.9 }
    ], "Cumulative %", 100);

    return;
  }

  // Build stacks
  let { labels, groups, byGroup, datasets } = buildDailyStacks(rows);



  const byGroupCum   = cumPerGroup(byGroup);
  const datasetsCum  = groups.map((g,i)=>({ label:g, data:byGroupCum[g], backgroundColor:COLORS[i%COLORS.length], stack:"S", categoryPercentage:0.9, barPercentage:0.9 }));
  const pct          = toPercentStacksN(byGroup, 31);
  const pctCum       = toPercentStacksN(byGroupCum, 31);
  const datasetsPct  = groups.map((g,i)=>({ label:g, data:pct[g],    backgroundColor:COLORS[i%COLORS.length], stack:"S", categoryPercentage:0.9, barPercentage:0.9 }));
  const datasetsPctC = groups.map((g,i)=>({ label:g, data:pctCum[g], backgroundColor:COLORS[i%COLORS.length], stack:"S", categoryPercentage:0.9, barPercentage:0.9 }));

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
async function fetchMonthlySales(){
  const qs = new URLSearchParams({
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, top_limit:filters.top_limit ||0
  }).toString();
  return fetchJSON(`/api/monthly_sales?${qs}`);
}

// helpers
const quarterOf = m => Math.floor(m/3); // 0..3 for Jan..Dec

// KPI by quarter, calculated only up to the previous month
function kpiByQuarter(sales, targets){
  const s = [0,0,0,0];
  const t = [0,0,0,0];

  // previous month index (0=Jan, 11=Dec)
  const now = new Date();
  let lastMonth = now.getMonth() - 1;   // previous month
  if (lastMonth < 0) lastMonth = 0;     // guard for January

  for (let m = 0; m <= lastMonth && m < 12; m++){
    const q = quarterOf(m);
    s[q] += +sales[m]   || 0;
    t[q] += +targets[m] || 0;
  }

  return t.map((tv, i) => tv > 0 ? +(s[i] / tv * 100).toFixed(1) : null);
}

// last cumulative achievement (%) from daily series
function dailyKPIFromSeries(sales, targets){
  const n = Math.max(sales.length, targets.length);
  let sCum = 0, tCum = 0;
  let last = null;
  for (let i = 0; i < n; i++){
    const s = +sales[i]   || 0;
    const t = +targets[i] || 0;
    sCum += s;
    tCum += t;
    if (tCum > 0){
      last = sCum / tCum * 100;
    }
  }
  return last != null ? +last.toFixed(1) : null;
}

async function fetchMonthlyKPIActual(region,BDE){
  const qs=new URLSearchParams({
    metric:filters.metric, category:filters.category, region:region, salesman:BDE,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, top_limit:filters.top_limit ||0
  }).toString();
  return fetchJSON(`/api/monthly_sales?${qs}`);
}
async function fetchMonthlyKPITarget(region,BDE){
  const qs=new URLSearchParams({
    metric:filters.metric, category:filters.category, region:region, salesman:BDE,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, top_limit:filters.top_limit ||0
  }).toString();
  return fetchJSON(`/api/monthly_target?${qs}`);
}

// build & render table
async function drawMonthlyKPI(){
  const rows = [];

  // All row (no region/salesman filter)
  {
    const qsSales = new URLSearchParams({
      metric: filters.metric, category: filters.category, region: "ALL", salesman:"ALL",
      sold_to_group: filters.sold_to_group, sold_to: filters.sold_to, ship_to: filters.ship_to,
      product_group: filters.product_group, pattern: filters.pattern, top_limit:filters.top_limit ||0
    }).toString();
    const qsTarget = new URLSearchParams({
      metric: filters.metric,
      category: filters.category, region: "ALL", salesman:"ALL",
      sold_to_group: filters.sold_to_group, sold_to: filters.sold_to, ship_to: filters.ship_to,
      product_group: filters.product_group, pattern: filters.pattern, top_limit:filters.top_limit ||0
    }).toString();

     // monthly + daily in parallel
    const [
      salesRows,
      targetRows,
      dailySalesRows,
      dailyTargetRows
    ] = await Promise.all([
      fetchJSON(`/api/monthly_sales?${qsSales}`),
      fetchJSON(`/api/monthly_target?${qsTarget}`),
      fetchJSON(`/api/daily_sales?${qsSales}`),
      fetchJSON(`/api/daily_target?${qsTarget}`)
    ]);

    const sales    = salesRows.map(r => +r.value || 0);
    const targets  = targetRows.map(r => +r.value || 0);
    const q        = kpiByQuarter(sales, targets);

    const dailySales   = dailySalesRows.map(r => +r.value || 0);
    const dailyTargets = dailyTargetRows.map(r => +r.value || 0);
    const dailyKPI     = dailyKPIFromSeries(dailySales, dailyTargets);

    rows.push({
      region: "All",
      bde: "All",
      Q1: q[0], Q2: q[1], Q3: q[2], Q4: q[3],
      dailyKPI: dailyKPI
    });
  }

  // Region / BDE rows
  for (const region of ["NSW","QLD","VIC","WA"]) {
    const bdes = REGION_SALESMEN[region] || []; // e.g. ["BDE2","BDE3"] or ids
    // fetch each BDE pair in parallel, then append in order
    const perBDE = await Promise.all(bdes.map(async (bde) => {
      const [
        salesRows,
        targetRows,
        dailySalesRows,
        dailyTargetRows
      ] = await Promise.all([
        fetchMonthlyKPIActual(region, bde),
        fetchMonthlyKPITarget(region, bde),
        fetchDailyKPIActual(region, bde),
        fetchDailyKPITarget(region, bde)
      ]);

      const sales    = salesRows.map(r => +r.value || 0);
      const targets  = targetRows.map(r => +r.value || 0);
      const q        = kpiByQuarter(sales, targets);

      const dailySales   = dailySalesRows.map(r => +r.value || 0);
      const dailyTargets = dailyTargetRows.map(r => +r.value || 0);
      const dailyKPI     = dailyKPIFromSeries(dailySales, dailyTargets);

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
async function fetchMonthlyBreakdownWithGroup(groupBy){
  const params = {
    metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
    sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
    product_group:filters.product_group, pattern:filters.pattern, group_by: groupBy, top_limit:filters.top_limit ||0
  };
  const qs=new URLSearchParams(params).toString();
  return fetchJSON(`/api/monthly_breakdown?${qs}`);
}

async function drawMonthlyTotals(){
  const [salesRows,targetRows]=await Promise.all([
    fetchMonthlySales(),
    fetchJSON(`/api/monthly_target?${new URLSearchParams({
      metric:filters.metric, category:filters.category, region:filters.region, salesman:filters.salesman,
      sold_to_group:filters.sold_to_group, sold_to:filters.sold_to, ship_to:filters.ship_to,
      product_group:filters.product_group, pattern:filters.pattern, top_limit:filters.top_limit ||0
    }).toString()}`)
  ]);
  const labels=monthsLabels();
  const sales = salesRows.map(r=>+r.value||0);
  const targets= targetRows.map(r=>+r.value||0);
  
   // % achievement per month
  const achievement = labels.map((_, i) =>
    targets[i] > 0 ? (sales[i] / targets[i]) * 100 : null
  );
  const salesCum=toCumulative(sales), targetCum=toCumulative(targets);
  const cumAchievement = labels.map((_, i) =>
    targets[i] > 0 ? (salesCum[i] / targetCum[i]) * 100 : null
  );

  [monthlyInst,monthlyCumInst].forEach(c=>c&&c.destroy());
  //if (!Chart.registry.plugins.get("showDataValues")) {
  //  Chart.register(showDataValuesPlugin);
  //}
  monthlyInst=new Chart(document.getElementById("monthlyChart"),{
    type:"bar",
    data:{labels,datasets:[
      {label:"Achievement(%)",type:"line", data: achievement,  yAxisID: "y1", borderWidth:2,pointRadius:0,borderColor:"#ef4444",  datalabels: {
          display: true,
          align: "top",
          anchor: "end",
          formatter: v => v == null ? "" : v.toFixed(1) + "%"
        }
      },
      {label:filters.metric==="amount"?"Sales Amount":"SalesQty",data:sales, backgroundColor:"#ABDEE6", categoryPercentage:0.9, barPercentage:0.9, z:0, datalabels: {
          display: false}},
      {label:"Target",type:"bar",data:targets, borderWidth:2, borderColor:"#ABDEE6",datalabels: {
          display: false}}
    ]},
    options:getCommonOptions(false)
  });
  monthlyCumInst=new Chart(document.getElementById("monthlyCumChart"),{
    type:"bar",
    data:{labels,datasets:[
      {label:"Achievement(%)",type:"line", data: cumAchievement,  yAxisID: "y1", borderWidth:2,pointRadius:0,borderColor:"#ef4444", datalabels: {
          display: true,
          align: "top",
          anchor: "end",
          formatter: v => v == null ? "" : v.toFixed(1) + "%"
        }},
      {label:filters.metric==="amount"?"Cumulative Amount":"Cumulative Qty",data:salesCum,backgroundColor:"#ABDEE6", categoryPercentage:0.9, barPercentage:0.9, datalabels: {
          display: false}},
      {label:"Cumulative Target",type:"bar",data:targetCum,borderWidth:2,borderWidth:2, borderColor:"#ABDEE6", datalabels: {
          display: false}} 
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
  // Respect user’s Group By; only reduce when Group By = sold_to and Top10 active
  const effectiveGroup = filters.group_by;
  const rows = await fetchMonthlyBreakdownWithGroup(effectiveGroup);

  if(!rows || !rows.length){
    [stackedMonthlyInst, stackedMonthlyCumInst, stackedMonthlyPctInst, stackedMonthlyCumPctInst]
      .forEach(c=>c&&c.destroy());
    const totals=await fetchMonthlySales();
    const labels=monthsLabels();
    const data=totals.map(r=>+r.value||0);
    stackedMonthlyInst=makeStacked("stackedMonthlyChart",labels,[{label:"Total",data,backgroundColor:"#a78bfa",stack:"S", categoryPercentage:0.9, barPercentage:0.9}],"Monthly");
    stackedMonthlyPctInst=makeStacked("stackedMonthlyPercentChart",labels,[{label:"Total %",data:labels.map(()=>100),backgroundColor:"#a78bfa",stack:"S", categoryPercentage:0.9, barPercentage:0.9}],"Monthly %",100);
    const cum=toCumulative(data);
    stackedMonthlyCumInst=makeStacked("stackedMonthlyCumChart",labels,[{label:"Total",data:cum,backgroundColor:"#10b981",stack:"S", categoryPercentage:0.9, barPercentage:0.9}],"Cumulative by Month");
    stackedMonthlyCumPctInst=makeStacked("stackedMonthlyCumPercentChart",labels,[{label:"Total %",data:labels.map(()=>100),backgroundColor:"#10b981",stack:"S", categoryPercentage:0.9, barPercentage:0.9}],"Cumulative %",100);
    return;
  }

  let {labels,groups,byGroup,datasets}=buildMonthlyStacks(rows);


  const byGroupCum=cumPerGroup(byGroup);
  const datasetsCum=groups.map((g,i)=>({label:g,data:byGroupCum[g],backgroundColor:COLORS[i%COLORS.length],stack:"S", categoryPercentage:0.9, barPercentage:0.9}));
  const pct=toPercentStacks(byGroup);
  const datasetsPct=groups.map((g,i)=>({label:g,data:pct[g],backgroundColor:COLORS[i%COLORS.length],stack:"S", categoryPercentage:0.9, barPercentage:0.9}));
  const pctCum=toPercentStacks(byGroupCum);
  const datasetsPctCum=groups.map((g,i)=>({label:g,data:pctCum[g],backgroundColor:COLORS[i%COLORS.length],stack:"S", categoryPercentage:0.9, barPercentage:0.9}));

  [stackedMonthlyInst, stackedMonthlyCumInst, stackedMonthlyPctInst, stackedMonthlyCumPctInst]
    .forEach(c=>c&&c.destroy());

  stackedMonthlyInst       = new Chart(document.getElementById("stackedMonthlyChart"), { type:"bar", data:{ labels, datasets }, options:getCommonOptions(true, undefined, "Monthly") });
  stackedMonthlyCumInst    = new Chart(document.getElementById("stackedMonthlyCumChart"), { type:"bar", data:{ labels, datasets: datasetsCum }, options:getCommonOptions(true, undefined, "Cumulative by Month") });
  stackedMonthlyPctInst    = new Chart(document.getElementById("stackedMonthlyPercentChart"), { type:"bar", data:{ labels, datasets: datasetsPct }, options:getCommonOptions(true, 100, "Monthly %") });
  stackedMonthlyCumPctInst = new Chart(document.getElementById("stackedMonthlyCumPercentChart"), { type:"bar", data:{ labels, datasets: datasetsPctCum }, options:getCommonOptions(true, 100, "Cumulative %") });
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

  const groups=await fetchJSON("/api/product_group"); populateSelect(document.getElementById('product_group'),groups);

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

  const totalCost = sd.map((v, i) => v + cogs[i] + op[i]);
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
          borderColor: "#60a5fa",
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
        datalabels: true,
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
    top_limit:filters.top_limit ||0
  }).toString();

  const rows = await fetchJSON(`/api/profit_monthly?${qs}`);
  renderProfitCombined(Array.isArray(rows) ? rows : []);
}

/* =================== END PROFIT (COMBINED) =================== */


async function refreshAllWithKpi(){
  // save current filters so map.html can read them
  localStorage.setItem("salesFilters", JSON.stringify(filters));
  await drawDailyTotals(),          // now uses October data internally
  await drawDailyStacked(),
  await drawMonthlyKPI();
  await drawMonthlyTotals();
  await drawMonthlyStacked();
  await loadProfit();
  await drawYearlyTotals();
  await drawYearlyStacked();
  
}

(async function start(){
  await initControls();
  
  [...document.querySelectorAll("#catBtns .btn")].forEach(b=>b.classList.toggle("active",b.dataset.val===filters.category));
  await refreshAllWithKpi();
  
  await refreshPatterns();                 // make pattern list available on load
  document.getElementById('ship_to').disabled = false; // locked until Sold-to is chosen
})();
