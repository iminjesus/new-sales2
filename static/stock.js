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
    ["pgBtn","patternBtn","patternClear","materialBtn","materialClear"].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.disabled = disabled;
    });
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
          { label: "Sales (2025)", data: data25, backgroundColor: "#ABDEE6", categoryPercentage: 0.8, barPercentage: 0.9 },
          { label: "Sales (2026)", data: data26, backgroundColor: "#93c5fd", categoryPercentage: 0.8, barPercentage: 0.9 }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: true, position: "bottom", labels: { boxWidth: 12, font: { size: 11 } } },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmt(c.parsed.y)}` } }
        },
        scales: { y: { beginAtZero: true, ticks: { callback: (v) => fmt(v) } } }
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
        datasets: [{ label: "Yearly Sales", data, backgroundColor: "#ABDEE6", categoryPercentage: 0.8, barPercentage: 0.9 }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: (c) => fmt(c.parsed.y) } }
        },
        scales: { y: { beginAtZero: true, ticks: { callback: (v) => fmt(v) } } }
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

  // ---------------------- state table ----------------------
  // stockTotal / waterTotal / factoryTotal are the global totals (pipeline numerators)
  let _stockTotal = 0, _waterTotal = 0, _factoryTotal = 0;

  async function fetchAndRenderStateTable(){
    const qs   = buildQueryParams({ metric: "qty" });
    const data = await fetchJSON(`/api/sales_stats_by_state?${qs}`);
    const tbody = document.getElementById("stateTableBody");
    const tfoot = document.getElementById("stateTableFoot");
    if (!tbody || !tfoot) return;
    if (!data || !data.rows || data.rows.length === 0) {
      tbody.innerHTML = `<tr><td colspan="8" class="st-loading">No data</td></tr>`;
      return;
    }

    let totQ3 = 0, totQ6 = 0, totQ12 = 0, totStock = 0, totWater = 0, totFactory = 0;
    const rows = data.rows;   // backend already excludes COMMON and zero rows

    tbody.innerHTML = rows.map(r => {
      totQ3      += r.qty_3m;
      totQ6      += r.qty_6m;
      totQ12     += r.qty_12m;
      totStock   += r.stock_qty   || 0;
      totWater   += r.water_qty   || 0;
      totFactory += r.factory_qty || 0;
      const bs  = r.base_sales;
      const sq  = r.stock_qty   || 0;
      const wq  = r.water_qty   || 0;
      const fq  = r.factory_qty || 0;
      const pipeS   = bs ? (sq          / bs) : 0;
      const pipeSW  = bs ? ((sq+wq)     / bs) : 0;
      const pipeSWF = bs ? ((sq+wq+fq)  / bs) : 0;
      return `<tr>
        <td class="st-state">${r.state}</td>
        <td>${fmtQty(r.qty_3m)}</td>
        <td>${fmtQty(r.qty_6m)}</td>
        <td>${fmtQty(r.qty_12m)}</td>
        <td class="st-base">${fmtQty(bs)}</td>
        <td class="st-qty">${fmtQty(sq)}</td>
        <td class="st-qty">${fmtQty(wq)}</td>
        <td class="st-qty">${fmtQty(fq)}</td>
        <td class="st-pipe">${bs ? fmtPipe(pipeS)   : "—"}</td>
        <td class="st-pipe">${bs ? fmtPipe(pipeSW)  : "—"}</td>
        <td class="st-pipe">${bs ? fmtPipe(pipeSWF) : "—"}</td>
      </tr>`;
    }).join("");

    // Total footer row
    const totBase = Math.round((totQ3 + totQ6 + totQ12) / 3);
    const tpipeS   = totBase ? fmtPipe(totStock / totBase) : "—";
    const tpipeSW  = totBase ? fmtPipe((totStock+totWater) / totBase) : "—";
    const tpipeSWF = totBase ? fmtPipe((totStock+totWater+totFactory) / totBase) : "—";
    tfoot.innerHTML = `<tr class="st-total">
      <td>TOTAL</td>
      <td>${fmtQty(totQ3)}</td>
      <td>${fmtQty(totQ6)}</td>
      <td>${fmtQty(totQ12)}</td>
      <td class="st-base">${fmtQty(totBase)}</td>
      <td class="st-qty">${fmtQty(totStock)}</td>
      <td class="st-qty">${fmtQty(totWater)}</td>
      <td class="st-qty">${fmtQty(totFactory)}</td>
      <td class="st-pipe">${tpipeS}</td>
      <td class="st-pipe">${tpipeSW}</td>
      <td class="st-pipe">${tpipeSWF}</td>
    </tr>`;
  }

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
      await fetchAndRenderStateTable();

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
        await fetchAndRender();
      }
    });

    // Let the browser paint the flex layout before Leaflet measures the map div
    await new Promise(r => setTimeout(r, 100));
    try { map.invalidateSize(); } catch(_e) {}

    await fetchAndRender();
  });
})();