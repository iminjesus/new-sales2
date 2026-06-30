// stock.js
// Stock map page logic + right-side Sales charts (Monthly/Yearly) like map view.
// You don't need a separate map.js; this file stays minimal.
// Requires in stock.html:
//   - Chart.js loaded (same CDN you use in index.html)
//   - A right panel with 2 canvases:
//       <canvas id="salesMonthlyChart"></canvas>
//       <canvas id="salesYearlyChart"></canvas>

(() => {
  const API_STOCK    = "/api/stock";
  const API_ORDERS   = "/api/orders";
  const API_INCOMING = "/api/incoming";

  const $ = (s) => document.querySelector(s);

  // custom dropdown option arrays
  let __PRODUCT_GROUP_OPTIONS = [];
  let __PATTERN_OPTIONS  = [];
  let __MATERIAL_OPTIONS = [];

  // custom dropdown helpers
  function ddOpen(menuEl){ if (menuEl) menuEl.style.display = "block"; }
  function ddClose(menuEl){ if (menuEl) menuEl.style.display = "none"; }
  function ddUpdateActive(inp){
    const dd = inp?.closest('.dd');
    if (dd) dd.classList.toggle('dd-active', (inp.value || '').trim() !== '');
  }

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
        e.preventDefault();
        onPick(v);
        ddClose(menuEl);
      });
      menuEl.appendChild(div);
    });
  }

  function ddFilter(options, q){
    const s = (q || "").trim().toLowerCase();
    if (!s) return options.slice(0, 500);
    return options.filter(x => String(x).toLowerCase().includes(s)).slice(0, 500);
  }

  function bindDropdown({ inputId, btnId, clearId, menuId, getOptions, onPick }){
    const inp  = document.getElementById(inputId);
    const btn  = document.getElementById(btnId);
    const clr  = clearId ? document.getElementById(clearId) : null;
    const menu = document.getElementById(menuId);
    if (!inp || !btn || !menu) return;

    function openWithCurrent(){
      ddRender(menu, ddFilter(getOptions(), inp.value), onPick);
      ddOpen(menu);
    }

    btn.addEventListener("click", () => openWithCurrent());
    inp.addEventListener("focus", () => openWithCurrent());
    inp.addEventListener("input", () => { openWithCurrent(); ddUpdateActive(inp); });
    if (clr) {
      clr.addEventListener("click", () => {
        inp.value = "";
        ddUpdateActive(inp);
        onPick("ALL");
        ddClose(menu);
        inp.focus();
      });
    }
    document.addEventListener("mousedown", (e) => {
      if (!menu.contains(e.target) && e.target !== inp && e.target !== btn && e.target !== clr){
        ddClose(menu);
      }
    });

    // Keyboard navigation: ↑/↓ move highlight, Enter picks, Esc closes.
    function activeItems(){ return menu.querySelectorAll(".dd-item"); }
    function setActiveIdx(idx){
      const items = activeItems();
      if (!items.length) return;
      items.forEach(el => el.classList.remove("dd-item-active"));
      const i = ((idx % items.length) + items.length) % items.length;
      const target = items[i];
      target.classList.add("dd-item-active");
      target.scrollIntoView({ block: "nearest" });
    }
    function currentIdx(){
      const items = activeItems();
      for (let i = 0; i < items.length; i++) {
        if (items[i].classList.contains("dd-item-active")) return i;
      }
      return -1;
    }
    inp.addEventListener("keydown", (e) => {
      const open = menu.style.display === "block";
      if (e.key === "ArrowDown") {
        if (!open) { openWithCurrent(); setActiveIdx(0); }
        else { setActiveIdx(currentIdx() + 1); }
        e.preventDefault();
      } else if (e.key === "ArrowUp") {
        if (!open) { openWithCurrent(); setActiveIdx(-1); }
        else { setActiveIdx(currentIdx() - 1); }
        e.preventDefault();
      } else if (e.key === "Enter") {
        const items = activeItems();
        const i = currentIdx();
        if (open && i >= 0 && items[i]) {
          onPick(items[i].textContent);
          ddClose(menu);
          e.preventDefault();
        } else if (open && items.length === 1) {
          onPick(items[0].textContent);
          ddClose(menu);
          e.preventDefault();
        }
      } else if (e.key === "Escape") {
        ddClose(menu);
      }
    });
  }

  const state = {
    category: "ALL",
    product_group: "ALL",
    pattern: "ALL",
    material: "ALL",
    orders_metric: "po" // "po" | "confirm"
  };

  // ---------------------- helpers ----------------------
  function setActiveButtons(wrapId, attr, value){
    const wrap = document.getElementById(wrapId);
    if (!wrap) return;
    wrap.querySelectorAll(".btn").forEach(b => {
      b.classList.toggle("active", b.dataset[attr] === value);
    });
  }

  function populateSelect(el, arr, includeAll = true){
    if (!el) return;
    el.innerHTML = "";
    if (includeAll){
      const o = document.createElement("option");
      o.value = "ALL"; o.textContent = "ALL";
      el.appendChild(o);
    }
    (arr || []).forEach(v => {
      if (v == null || String(v).trim() === "") return;
      const o = document.createElement("option");
      o.value = v; o.textContent = v;
      el.appendChild(o);
    });
  }

  function populateDatalist(listId, items){
    const list = document.getElementById(listId);
    if (!list) return;
    list.innerHTML = "";
    (items || []).forEach(v => {
      if (v == null || String(v).trim() === "") return;
      const o = document.createElement("option");
      o.value = v;
      list.appendChild(o);
    });
  }

  function showError(msg){ /* silenced */ }

  async function fetchJSON(url){
    try{
      const r = await fetch(url, { credentials: "same-origin", cache: "no-store" });
      if (r.ok) return await r.json();
      if (r.status >= 500) {
        await new Promise(res => setTimeout(res, 800));
        const r2 = await fetch(url, { credentials: "same-origin", cache: "no-store" });
        if (r2.ok) return await r2.json();
      }
      throw new Error(`${r.status} ${r.statusText}`);
    }catch(e){
      console.error("fetch fail:", url, e.message);
      return null;
    }
  }

  function buildQueryParams(extra = {}){
    const p = new URLSearchParams();
    p.set("category", state.category);
    p.set("product_group", state.product_group);
    if (state.pattern && state.pattern !== "ALL")  p.set("pattern", state.pattern);
    if (state.material && state.material !== "ALL") p.set("material", state.material);
    Object.entries(extra).forEach(([k,v]) => { if (v != null) p.set(k, String(v)); });
    return p.toString();
  }

  function bubbleRadius(v){
  const n = Math.max(0, Number(v) || 0);
  return Math.max(10, Math.min(50, Math.sqrt(n) / 3 + 8));
}
  function fmt(n){ return Math.round(Number(n)||0).toLocaleString(); }

  // ---------------- Leaflet ----------------
  const map = L.map("stockMap", { zoomSnap: 0.1 });
  map.setView([5, 120], 4); // Asia default; fitBounds after data loads
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", { maxZoom: 19 }).addTo(map);

  const layerStock         = L.layerGroup().addTo(map);
  const layerOrders        = L.layerGroup().addTo(map);
  const layerIncoming      = L.layerGroup().addTo(map);
  const layerIncomingLines = L.layerGroup().addTo(map);

  function setStatus(msg){
    const el = document.getElementById("statusLine");
    if (el) el.textContent = msg;
  }

  function setFiltersDisabled(disabled){
    // Disable/enable all filter buttons so rapid repeated clicks are prevented
    document.querySelectorAll("#catBtns .btn, #ordersMetricBtns .btn").forEach(b => {
      b.disabled = disabled;
    });
    // Dropdown inputs
    ["product_group","pattern","material"].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.disabled = disabled;
    });
    // Dropdown toggle/clear buttons
    ["pgBtn","pgClear","patternBtn","patternClear","materialBtn","materialClear"].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.disabled = disabled;
    });
    // Surface progress via body[data-loading] → reveals the floating
    // #loadingBadge + flips the cursor so a filter click doesn't look
    // like a frozen page.
    if (disabled) document.body.setAttribute("data-loading", "1");
    else          document.body.removeAttribute("data-loading");
  }

  const ORIGIN_COLOR = {
    "CHN": "#e53935",
    "KOR": "#1e88e5",
    "HUN": "#43a047",
    "IDN": "#fb8c00"
  };

  function drawStock(rows){
    layerStock.clearLayers();
    (rows || []).forEach(r => {
      const lat = Number(r.lat), lon = Number(r.lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
      const plant = r.plant ?? "-";
      const val = r.stock_value ?? 0;

      const m = L.circleMarker([lat, lon], {
        radius: bubbleRadius(val),
        weight: 1,
        fillOpacity: 0.55,
        color: "#1e78ff"
      });
      m.bindPopup(`Plant: ${plant}<br>Unrestricted stock: ${fmt(val)}`);
      m.addTo(layerStock);
    });
  }

  function drawOrders(rows){
    layerOrders.clearLayers();
    (rows || []).forEach(r => {
      const lat = Number(r.lat), lon = Number(r.lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;

      const originCode = String(r.origin ?? r.origin_code ?? r.originCd ?? "").trim().toUpperCase();
      const color = ORIGIN_COLOR[originCode] || "#999";

      const originLabel = r.origin_name ?? r.origin ?? "-";
      const val = r.order_value ?? 0;

      const m = L.circleMarker([lat, lon], {
        radius: bubbleRadius(val),
        weight: 1,
        fillOpacity: 0.55,
        color
      });
      m.bindPopup(`Origin: ${originLabel}<br>${state.orders_metric.toUpperCase()} Qty: ${fmt(val)}`);
      m.addTo(layerOrders);
    });
  }

  function drawIncoming(rows){
    layerIncoming.clearLayers();
    layerIncomingLines.clearLayers();

    (rows || []).forEach(r => {
      const lat = Number(r.lat), lon = Number(r.lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;

      const plant = r.plant ?? "-";
      const origin = r.origin_name ?? r.origin ?? "-";
      const eta = r.eta_date ? String(r.eta_date).slice(0,10) : "-";
      const val = r.incoming_value ?? 0;

      const m = L.circleMarker([lat, lon], {
        radius: bubbleRadius(val),
        weight: 1,
        fillOpacity: 0.35,
        color: "#8b5cf6"
      });
      m.bindPopup(`Incoming<br>Origin: ${origin}<br>Plant: ${plant}<br>ETA: ${eta}<br>Qty: ${fmt(val)}`);
      m.addTo(layerIncoming);

      const line = r.line;
      if (Array.isArray(line) && line.length >= 2){
        const pts = line.map(p => [Number(p.lat), Number(p.lon)]).filter(p => Number.isFinite(p[0]) && Number.isFinite(p[1]));
        if (pts.length >= 2){
          L.polyline(pts, { weight: 2, opacity: 0.35, color: "#8b5cf6" }).addTo(layerIncomingLines);
        }
      }
    });
  }

  // ---------------- Sales charts (right panel) ----------------
  let monthlyChart = null;
  let yearlyChart  = null;

  function monthsLabels(){
    return ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  }

  // Charts float over the world map.  The map itself is dense blue
  // (coastlines + cluster bubbles), so the bars use warm tones — amber
  // for prior year, deeper orange for current — to read cleanly
  // against the blue map.  Fill alpha is high enough (0.75) to look
  // saturated but still lets faint coastline show through.
  const _OVERLAY_BAR_25 = "rgba(251,191,36,0.75)";   // amber-400
  const _OVERLAY_BAR_26 = "rgba(234,88,12,0.75)";    // orange-600

  function drawMonthlySales(rows25, rows26){
    const canvas = document.getElementById("salesMonthlyChart");
    if (!canvas || typeof Chart === "undefined") return;

    const labels = monthsLabels();
    const data25 = Array(12).fill(0);
    const data26 = Array(12).fill(0);

    function fillData(rows, arr){
      (rows || []).forEach(r => {
        const mi = (r.month ?? r.m ?? 0) - 1;
        if (mi >= 0 && mi < 12) arr[mi] += Number(r.value || r.qty || 0);
      });
    }
    fillData(rows25, data25);
    fillData(rows26, data26);

    if (monthlyChart) { monthlyChart.destroy(); monthlyChart = null; }
    monthlyChart = new Chart(canvas.getContext("2d"), {
      type: "bar",
      data: {
        labels,
        datasets: [
          { label: "Sales (2025)", data: data25, backgroundColor: _OVERLAY_BAR_25, categoryPercentage: 0.8, barPercentage: 0.9 },
          { label: "Sales (2026)", data: data26, backgroundColor: _OVERLAY_BAR_26, categoryPercentage: 0.8, barPercentage: 0.9 }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: true, position: "bottom", labels: { boxWidth: 10, font: { size: 9 } } },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmt(c.parsed.y)}` } }
        },
        scales: {
          x: { ticks: { font: { size: 9 } }, grid: { display: false } },
          y: { beginAtZero: true,
               ticks: { callback: (v) => fmt(v), font: { size: 9 } },
               grid: { color: "rgba(0,0,0,0.05)" } }
        }
      }
    });
  }

  function drawYearlySales(yearRows){
    const canvas = document.getElementById("salesYearlyChart");
    if (!canvas || typeof Chart === "undefined") return;

    const byYear = {};
    (yearRows || []).forEach(r => {
      const y = String(r.year ?? r.y ?? "");
      if (!y) return;
      byYear[y] = (byYear[y] || 0) + Number(r.value || r.qty || 0);
    });

    const labels = Object.keys(byYear).sort();
    const data = labels.map(k => byYear[k]);

    if (yearlyChart) { yearlyChart.destroy(); yearlyChart = null; }
    yearlyChart = new Chart(canvas.getContext("2d"), {
      type: "bar",
      data: {
        labels,
        datasets: [{ label: "Yearly Sales", data, backgroundColor: _OVERLAY_BAR_25, categoryPercentage: 0.8, barPercentage: 0.9 }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: (c) => fmt(c.parsed.y) } }
        },
        scales: {
          x: { ticks: { font: { size: 9 } }, grid: { display: false } },
          y: { beginAtZero: true,
               ticks: { callback: (v) => fmt(v), font: { size: 9 } },
               grid: { color: "rgba(0,0,0,0.05)" } }
        }
      }
    });
  }

  function fmtQty(n){ return Number.isFinite(n) ? n.toLocaleString() : "—"; }
  function fmtPipe(n){ return Number.isFinite(n) && n > 0 ? n.toFixed(1) + " mo" : "—"; }
  function fmtPrice(n){ return (n != null && Number.isFinite(n)) ? "$" + n.toFixed(2) : "—"; }

  async function fetchAndRenderPrice(){
    const qs = buildQueryParams();
    const d  = await fetchJSON(`/api/carrying_price?${qs}`);
    const listEl = document.getElementById("priceList");
    if (listEl) listEl.textContent = d ? fmtPrice(d.list_price) : "—";
  }

  // last fetched base_sales for pipeline rendering
  let _baseSales = 0;

  async function fetchAndRenderSalesStats(){
    const qs = buildQueryParams({ metric: "qty" });
    const d  = await fetchJSON(`/api/sales_stats?${qs}`);
    if (!d) return;
    _baseSales = d.base_sales || 0;
    return d.base_sales || 0;
  }

  function renderPipeline(stockTotal, waterTotal, factoryTotal, baseSales){
    _baseSales = baseSales || _baseSales;
  }

  // ── Cascade table (Line → Product Group → Pattern → Size) ───────
  // Mirrors graph-view's click-to-drill: clicking a Line row narrows
  // to that line's Product Groups, clicking a Product Group narrows
  // to its Patterns, clicking a Pattern narrows to its Sizes.  The
  // first-column header carries the breadcrumb so the user always
  // sees where they are; ← Back chip pops one level.
  const CASCADE_LEVELS = ["line", "product_group", "pattern", "size"];
  const CASCADE_LABEL  = {
    line:          "Line",
    product_group: "Product Group",
    pattern:       "Pattern",
    size:          "Size",
  };
  let cascadeState = { level: "line", line: "", pg: "", pat: "" };

  function _renderCascadeCrumb(){
    const crumb = document.getElementById("cascadeCrumb");
    const back  = document.getElementById("cascadeBack");
    if (!crumb || !back) return;
    const parts = ["All"];
    if (cascadeState.line) parts.push(cascadeState.line);
    if (cascadeState.pg)   parts.push(cascadeState.pg);
    if (cascadeState.pat)  parts.push(cascadeState.pat);
    parts.push(CASCADE_LABEL[cascadeState.level]);
    crumb.textContent = parts.join(" › ");
    back.style.display = (cascadeState.level === "line") ? "none" : "";
    const th = document.getElementById("cascadeBucketTh");
    if (th) th.textContent = CASCADE_LABEL[cascadeState.level];
  }

  async function fetchAndRenderCascadeTable(){
    _renderCascadeCrumb();
    const tbody = document.getElementById("cascadeTableBody");
    const tfoot = document.getElementById("cascadeTableFoot");
    if (!tbody || !tfoot) return;
    const qs = buildQueryParams({
      metric:             "qty",
      level:              cascadeState.level,
      line:               cascadeState.line,
      product_group_anc:  cascadeState.pg,
      pattern_anc:        cascadeState.pat,
    });
    const data = await fetchJSON(`/api/sales_stats_by_product_level?${qs}`);
    if (!data || !data.rows || data.rows.length === 0) {
      tbody.innerHTML = `<tr><td colspan="13" class="st-loading">No data</td></tr>`;
      tfoot.innerHTML = "";
      return;
    }
    // Above-size rows drill the cascade.  Size rows are still
    // clickable but they don't drill — they set state.material so the
    // map + charts narrow to that exact size.  Both paths share the
    // .cascade-clickable hook; the visual chevron (`›`) is suppressed
    // on size rows via .cas-terminal.
    const drillable = true;
    const terminalCls = (cascadeState.level === "size") ? " cas-terminal" : "";
    let totQ3=0, totQ6=0, totQ12=0,
        totStock=0, totWater=0, totCy=0, totFactory=0;

    // Each bucket row emits 4 NSW/QLD/VIC/WA sub-rows followed by a
    // per-bucket subtotal row.  The bucket label sits in a rowSpan'd
    // first cell on the first state row so the product name lines up
    // visually with its state group.
    const html = [];
    data.rows.forEach(r => {
      const by = r.by_state || [];
      const cls   = "cascade-clickable" + terminalCls;
      const dataB = ` data-bucket="${(r.bucket || "").replace(/"/g, "&quot;")}"`;
      const bucketLabel = r.bucket || "—";

      const span = by.length + 1; // 4 states + 1 subtotal row
      by.forEach((s, idx) => {
        const bs       = s.base_sales;
        const sq       = s.stock_qty   || 0;
        const wq       = s.water_qty   || 0;
        const cy       = s.cy_qty      || 0;
        const fqRaw    = s.factory_qty || 0;
        const fqMinus  = Math.max(0, fqRaw - cy);
        const pipeS    = bs ? (sq                       / bs) : 0;
        const pipeSW   = bs ? ((sq + wq)                / bs) : 0;
        const pipeSWF  = bs ? ((sq + wq + cy + fqMinus) / bs) : 0;
        const bucketCell = (idx === 0)
          ? `<td class="cas-bucket" rowspan="${span}">${bucketLabel}</td>`
          : "";
        html.push(`<tr class="${cls} cas-state-row"${dataB}>
          ${bucketCell}
          <td class="cas-state">${s.state}</td>
          <td>${fmtQty(s.qty_3m)}</td>
          <td>${fmtQty(s.qty_6m)}</td>
          <td>${fmtQty(s.qty_12m)}</td>
          <td class="st-base">${fmtQty(bs)}</td>
          <td class="st-qty">${fmtQty(sq)}</td>
          <td class="st-qty">${fmtQty(wq)}</td>
          <td class="st-qty">${fmtQty(cy)}</td>
          <td class="st-qty">${fmtQty(fqMinus)}</td>
          <td class="st-pipe">${bs ? fmtPipe(pipeS)   : "—"}</td>
          <td class="st-pipe">${bs ? fmtPipe(pipeSW)  : "—"}</td>
          <td class="st-pipe">${bs ? fmtPipe(pipeSWF) : "—"}</td>
        </tr>`);
      });

      // Per-bucket subtotal — same metric set as the State table TOTAL
      // row, scoped to this bucket only.
      const bs = r.base_sales;
      const sq = r.stock_qty   || 0;
      const wq = r.water_qty   || 0;
      const cy = r.cy_qty      || 0;
      const fqRaw   = r.factory_qty || 0;
      const fqMinus = Math.max(0, fqRaw - cy);
      totQ3      += r.qty_3m;
      totQ6      += r.qty_6m;
      totQ12     += r.qty_12m;
      totStock   += sq; totWater += wq; totCy += cy; totFactory += fqMinus;
      const pipeS   = bs ? (sq                       / bs) : 0;
      const pipeSW  = bs ? ((sq + wq)                / bs) : 0;
      const pipeSWF = bs ? ((sq + wq + cy + fqMinus) / bs) : 0;
      // If we had no states for this bucket, the bucket cell wasn't
      // emitted above — include it on the subtotal row instead.
      const bucketSubCell = (by.length === 0)
        ? `<td class="cas-bucket">${bucketLabel}</td>`
        : "";
      html.push(`<tr class="cas-subtotal ${cls}"${dataB}>
        ${bucketSubCell}
        <td class="cas-state">Sub</td>
        <td>${fmtQty(r.qty_3m)}</td>
        <td>${fmtQty(r.qty_6m)}</td>
        <td>${fmtQty(r.qty_12m)}</td>
        <td class="st-base">${fmtQty(bs)}</td>
        <td class="st-qty">${fmtQty(sq)}</td>
        <td class="st-qty">${fmtQty(wq)}</td>
        <td class="st-qty">${fmtQty(cy)}</td>
        <td class="st-qty">${fmtQty(fqMinus)}</td>
        <td class="st-pipe">${bs ? fmtPipe(pipeS)   : "—"}</td>
        <td class="st-pipe">${bs ? fmtPipe(pipeSW)  : "—"}</td>
        <td class="st-pipe">${bs ? fmtPipe(pipeSWF) : "—"}</td>
      </tr>`);
    });
    tbody.innerHTML = html.join("");

    const totBase = Math.round((totQ3 + totQ6 + totQ12) / 3);
    const tpipeS   = totBase ? fmtPipe(totStock / totBase) : "—";
    const tpipeSW  = totBase ? fmtPipe((totStock+totWater) / totBase) : "—";
    const tpipeSWF = totBase
      ? fmtPipe((totStock+totWater+totCy+totFactory) / totBase)
      : "—";
    tfoot.innerHTML = `<tr class="st-total">
      <td>TOTAL</td>
      <td></td>
      <td>${fmtQty(totQ3)}</td>
      <td>${fmtQty(totQ6)}</td>
      <td>${fmtQty(totQ12)}</td>
      <td class="st-base">${fmtQty(totBase)}</td>
      <td class="st-qty">${fmtQty(totStock)}</td>
      <td class="st-qty">${fmtQty(totWater)}</td>
      <td class="st-qty">${fmtQty(totCy)}</td>
      <td class="st-qty">${fmtQty(totFactory)}</td>
      <td class="st-pipe">${tpipeS}</td>
      <td class="st-pipe">${tpipeSW}</td>
      <td class="st-pipe">${tpipeSWF}</td>
    </tr>`;
  }

  // Reverse direction of _syncPageFiltersFromCascade: when the user
  // narrows via the search/dropdowns (product_group / pattern /
  // material), jump the cascade to the matching level so the table
  // bottoms out at exactly what they searched for.  /api/cascade_
  // ancestors resolves the most-specific filter back to its line /
  // product_group / pattern chain.  Returns true if cascadeState
  // actually moved.
  async function _syncCascadeFromFilters(){
    const params = new URLSearchParams();
    if (state.material && state.material !== "ALL")
      params.set("material", state.material);
    if (state.pattern && state.pattern !== "ALL")
      params.set("pattern", state.pattern);
    if (state.product_group && state.product_group !== "ALL")
      params.set("product_group", state.product_group);

    // No narrowing filter? Reset cascade back to the line root so the
    // user sees PCLT / TBR again instead of being stranded mid-drill.
    if (![...params].length) {
      cascadeState = { level: "line", line: "", pg: "", pat: "" };
      return true;
    }

    const d = await fetchJSON(`/api/cascade_ancestors?${params.toString()}`);
    if (!d || !d.level) return false;
    if (d.level === "size") {
      cascadeState = { level: "size",
                       line: d.line || "", pg: d.product_group || "",
                       pat: d.pattern || "" };
    } else if (d.level === "pattern") {
      cascadeState = { level: "pattern",
                       line: d.line || "", pg: d.product_group || "",
                       pat: "" };
    } else if (d.level === "product_group") {
      cascadeState = { level: "product_group",
                       line: d.line || "", pg: "", pat: "" };
    } else {
      cascadeState = { level: "line", line: "", pg: "", pat: "" };
    }
    return true;
  }

  // Mirror cascade ancestor state into the top-bar filters so the map
  // + sales charts narrow alongside the table.  Material is handled
  // separately (size-row click sets it directly; back/advance clears
  // it via the caller).
  function _syncPageFiltersFromCascade(){
    const catVal = cascadeState.line || "ALL";
    state.category = (catVal === "PCLT" || catVal === "TBR") ? catVal : "ALL";
    setActiveButtons("catBtns", "val", state.category);

    state.product_group = cascadeState.pg || "ALL";
    const pgEl = document.getElementById("product_group");
    if (pgEl) {
      pgEl.value = state.product_group === "ALL" ? "" : state.product_group;
      ddUpdateActive(pgEl);
    }

    state.pattern = cascadeState.pat || "ALL";
    const patEl = document.getElementById("pattern");
    if (patEl) {
      patEl.value = state.pattern === "ALL" ? "" : state.pattern;
      ddUpdateActive(patEl);
    }
  }

  function _clearMaterialFilter(){
    state.material = "ALL";
    const matEl = document.getElementById("material");
    if (matEl) { matEl.value = ""; ddUpdateActive(matEl); }
  }

  async function _cascadeAdvance(bucket){
    if (!bucket) return;
    const lvl = cascadeState.level;
    if (lvl === "line") {
      cascadeState = { level: "product_group", line: bucket, pg: "", pat: "" };
      _clearMaterialFilter();
    } else if (lvl === "product_group") {
      cascadeState = { level: "pattern", line: cascadeState.line, pg: bucket, pat: "" };
      _clearMaterialFilter();
    } else if (lvl === "pattern") {
      cascadeState = { level: "size", line: cascadeState.line, pg: cascadeState.pg, pat: bucket };
      _clearMaterialFilter();
    } else if (lvl === "size") {
      // Size is terminal — clicking a size row sets the material
      // filter so the map + charts narrow to that exact size.  No
      // further cascade drilling.
      state.material = bucket;
      const matEl = document.getElementById("material");
      if (matEl) { matEl.value = bucket; ddUpdateActive(matEl); }
    } else {
      return;
    }
    _syncPageFiltersFromCascade();
    // Refresh dropdown options so the secondary lists reflect the new
    // ancestor (pattern list = patterns within selected product_group,
    // material list = sizes within selected pattern, etc.).
    await refreshPatterns();
    await refreshMaterials();
    // Full re-render: map dots + monthly/yearly charts + price box +
    // cascade table itself.
    fetchAndRender();
  }

  async function _cascadeBack(){
    const lvl = cascadeState.level;
    if (lvl === "product_group") {
      cascadeState = { level: "line", line: "", pg: "", pat: "" };
    } else if (lvl === "pattern") {
      cascadeState = { level: "product_group", line: cascadeState.line, pg: "", pat: "" };
    } else if (lvl === "size") {
      cascadeState = { level: "pattern", line: cascadeState.line, pg: cascadeState.pg, pat: "" };
    } else {
      return;
    }
    // Going back also drops any size-specific material filter that
    // was set by a size-row click on the way down.
    _clearMaterialFilter();
    _syncPageFiltersFromCascade();
    await refreshPatterns();
    await refreshMaterials();
    fetchAndRender();
  }

  document.addEventListener("click", function(e){
    const row = e.target.closest && e.target.closest("#cascadeTable tbody tr.cascade-clickable");
    if (row && row.dataset.bucket != null) {
      _cascadeAdvance(row.dataset.bucket);
      return;
    }
    if (e.target && e.target.id === "cascadeBack") _cascadeBack();
  });

  async function fetchAndRenderSales(){
    const qs = buildQueryParams({ metric: "qty" });
    const [rows25, rows26, yearRows] = await Promise.all([
      fetchJSON(`/api/monthly_sales?${qs}&year=2025`),
      fetchJSON(`/api/monthly_sales?${qs}&year=2026`),
      fetchJSON(`/api/yearly_sales?${qs}`)
    ]);

    drawMonthlySales(rows25 || [], rows26 || []);
    drawYearlySales(yearRows || []);
  }

  // ---------------------- main fetch ----------------------
  async function fetchAndRender(){
    setStatus("Loading…");
    setFiltersDisabled(true);
    try {
      const qsStock  = buildQueryParams();
      const qsOrders = buildQueryParams({ metric: state.orders_metric });

      const [stockRes, ordersRes, incomingRes] = await Promise.all([
        fetchJSON(`${API_STOCK}?${qsStock}`),
        fetchJSON(`${API_ORDERS}?${qsOrders}`),
        fetchJSON(`${API_INCOMING}?${qsStock}`),
      ]);

      drawStock(stockRes?.rows || []);
      drawOrders(ordersRes?.rows || []);
      drawIncoming(incomingRes?.rows || []);

      // Compute pipeline totals from map data
      const stockTotal   = (stockRes?.rows    || []).reduce((s,r) => s + (r.stock_value    ?? 0), 0);
      const waterTotal   = (incomingRes?.rows || []).reduce((s,r) => s + (r.incoming_value ?? 0), 0);
      const factoryTotal = (ordersRes?.rows   || []).reduce((s,r) => s + (r.order_value    ?? 0), 0);
      _stockTotal = stockTotal; _waterTotal = waterTotal; _factoryTotal = factoryTotal;

      // Fit map to all visible data points
      const allRows = [
        ...(stockRes?.rows || []),
        ...(ordersRes?.rows || []),
        ...(incomingRes?.rows || [])
      ];
      const validPts = allRows
        .map(r => [Number(r.lat), Number(r.lon)])
        .filter(([la, lo]) => Number.isFinite(la) && Number.isFinite(lo));
      if (validPts.length > 0) {
        const bounds = L.latLngBounds(validPts);
        // Always include full Australia (VIC at SW corner) in view
        bounds.extend([-39.5, 114.0]);
        bounds.extend([-10.6, 153.6]);
        map.fitBounds(bounds, { padding: [30, 30], maxZoom: 4 });
      }

      const [, baseSales] = await Promise.all([
        fetchAndRenderSales(),
        fetchAndRenderSalesStats(),
        fetchAndRenderPrice(),
      ]);
      renderPipeline(stockTotal, waterTotal, factoryTotal, baseSales);
      // Cascade table (Line → Product Group → Pattern → Size) is the
      // only on-page table now — each product row drops 4 state sub-rows
      // (NSW / QLD / VIC / WA) underneath itself so the separate State
      // table on top is no longer needed.
      await fetchAndRenderCascadeTable();

      setStatus(
        `Done. Stock: ${(stockRes?.rows||[]).length}, ` +
        `Orders: ${(ordersRes?.rows||[]).length}, ` +
        `Incoming: ${(incomingRes?.rows||[]).length}`
      );
    } finally {
      setFiltersDisabled(false);
    }
  }

  // ---------------------- dropdown/datalist loaders ----------------------
  async function refreshProductGroups(){
    const d = await fetchJSON("/api/v2/dimensions");
    __PRODUCT_GROUP_OPTIONS = d?.product_groups || [];
  }

  async function refreshPatterns(){
    const pg  = state.product_group;
    const res = await fetchJSON(`/api/patterns?product_group=${encodeURIComponent(pg)}`);
    const rows = Array.isArray(res) ? res : (res?.rows || []);
    __PATTERN_OPTIONS = rows.map(x => String(x)).filter(Boolean);
  }

  async function refreshMaterials(){
    const pg  = state.product_group;
    const pat = ($("#pattern")?.value || "").trim();
    const qs  = new URLSearchParams({
      product_group: pg,
      ...(pat ? { pattern: pat } : {})
    }).toString();

    const res = await fetchJSON(`/api/materials?${qs}`);
    const rows = Array.isArray(res) ? res : (res?.rows || []);
    __MATERIAL_OPTIONS = rows.map(x => String(x)).filter(Boolean);
  }

  function readUIToState(){
    const pgVal = ($("#product_group")?.value || "").trim();
    state.product_group = pgVal || "ALL";
    const pat = ($("#pattern")?.value || "").trim();
    const mat = ($("#material")?.value || "").trim();
    state.pattern = pat ? pat : "ALL";
    state.material = mat ? mat : "ALL";
  }

  function clearUI(){
    const pg  = document.getElementById("product_group");
    const pat = document.getElementById("pattern");
    const mat = document.getElementById("material");
    if (pg)  { pg.value  = ""; ddUpdateActive(pg); }
    if (pat) { pat.value = ""; ddUpdateActive(pat); }
    if (mat) { mat.value = ""; ddUpdateActive(mat); }
    state.category     = "ALL";
    state.product_group = "ALL";
    state.pattern      = "ALL";
    state.material     = "ALL";
    state.orders_metric = "po";

    setActiveButtons("catBtns", "val", "ALL");
    setActiveButtons("ordersMetricBtns", "metric", "po");
  }

  // ---------------------- events ----------------------
  document.getElementById("catBtns")?.addEventListener("click", async (e) => {
    const btn = e.target;
    if (!btn.classList.contains("btn")) return;
    state.category = btn.dataset.val || "ALL";
    setActiveButtons("catBtns", "val", state.category);
    await fetchAndRender();
  });

  document.getElementById("ordersMetricBtns")?.addEventListener("click", async (e) => {
    const btn = e.target;
    if (!btn.classList.contains("btn")) return;
    state.orders_metric = btn.dataset.metric || "po";
    setActiveButtons("ordersMetricBtns", "metric", state.orders_metric);
    await fetchAndRender();
  });

  // ---------------------- init ----------------------
  window.addEventListener("load", async () => {
    setActiveButtons("catBtns", "val", state.category);
    setActiveButtons("ordersMetricBtns", "metric", state.orders_metric);

    await refreshProductGroups();
    await refreshPatterns();
    await refreshMaterials();

    bindDropdown({
      inputId: "product_group",
      btnId: "pgBtn",
      clearId: "pgClear",
      menuId: "pgMenu",
      getOptions: () => __PRODUCT_GROUP_OPTIONS,
      onPick: async (val) => {
        const v = (val === "ALL") ? "" : val;
        const pgEl = document.getElementById("product_group");
        if (pgEl) { pgEl.value = v; ddUpdateActive(pgEl); }
        state.product_group = v || "ALL";
        const patEl = document.getElementById("pattern");
        const matEl = document.getElementById("material");
        if (patEl) { patEl.value = ""; ddUpdateActive(patEl); }
        if (matEl) { matEl.value = ""; ddUpdateActive(matEl); }
        state.pattern = "ALL";
        state.material = "ALL";
        await refreshPatterns();
        await refreshMaterials();
        // Auto-jump the cascade to product_group level so the user
        // sees the picked group's patterns instead of starting back at
        // PCLT / TBR.
        await _syncCascadeFromFilters();
        await fetchAndRender();
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
        state.pattern = v || "ALL";
        await refreshMaterials();
        // Auto-jump cascade to pattern level (resolves the ancestor
        // line / product_group from carrying_26 even when the user
        // skipped picking those filters).
        await _syncCascadeFromFilters();
        await fetchAndRender();
      }
    });

    bindDropdown({
      inputId: "material",
      btnId: "materialBtn",
      clearId: "materialClear",
      menuId: "materialMenu",
      getOptions: () => __MATERIAL_OPTIONS,
      onPick: async (val) => {
        const v = (val === "ALL") ? "" : val;
        const matEl = document.getElementById("material");
        matEl.value = v; ddUpdateActive(matEl);
        state.material = v || "ALL";
        // Skip the product-group / pattern dropdowns? — that's the
        // common workflow when a customer asks about a specific size.
        // Look up its ancestors and jump the cascade straight to size
        // level so the user sees which category / pattern that size
        // belongs to alongside the per-state stock.
        await _syncCascadeFromFilters();
        await fetchAndRender();
      }
    });

    // Let the browser paint the flex layout before Leaflet measures the map div
    await new Promise(r => setTimeout(r, 100));
    try { map.invalidateSize(); } catch(_e) {}

    await fetchAndRender();
  });
})();