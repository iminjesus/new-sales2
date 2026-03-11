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
  const API_DASH     = "/api/v2/dashboard";

  const $ = (s) => document.querySelector(s);

  // custom dropdown option arrays
  let __PATTERN_OPTIONS  = [];
  let __MATERIAL_OPTIONS = [];

  // custom dropdown helpers
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
        e.preventDefault();
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

  function bindDropdown({ inputId, btnId, clearId, menuId, getOptions, onPick }){
    const inp  = document.getElementById(inputId);
    const btn  = document.getElementById(btnId);
    const clr  = document.getElementById(clearId);
    const menu = document.getElementById(menuId);
    if (!inp || !btn || !clr || !menu) return;

    function openWithCurrent(){
      ddRender(menu, ddFilter(getOptions(), inp.value), onPick);
      ddOpen(menu);
    }

    btn.addEventListener("click", () => openWithCurrent());
    inp.addEventListener("focus", () => openWithCurrent());
    inp.addEventListener("input", () => openWithCurrent());
    clr.addEventListener("click", () => {
      inp.value = "";
      onPick("ALL");
      ddClose(menu);
      inp.focus();
    });
    document.addEventListener("mousedown", (e) => {
      if (!menu.contains(e.target) && e.target !== inp && e.target !== btn && e.target !== clr){
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

  function showError(msg){
    const el = document.getElementById("errbar");
    if (!el) return;
    el.textContent = msg;
    el.hidden = false;
  }

  async function fetchJSON(url){
    try{
      const r = await fetch(url, { credentials: "same-origin", cache: "no-store" });
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      return await r.json();
    }catch(e){
      console.error("fetch fail:", url, e);
      showError(`Failed: ${url} — ${e.message}`);
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
  map.setView([-5.0, 180.0], 4.2); // fixed view
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", { maxZoom: 19 }).addTo(map);

  const layerStock         = L.layerGroup().addTo(map);
  const layerOrders        = L.layerGroup().addTo(map);
  const layerIncoming      = L.layerGroup().addTo(map);
  const layerIncomingLines = L.layerGroup().addTo(map);

  function setStatus(msg){
    const el = document.getElementById("statusLine");
    if (el) el.textContent = msg;
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

  function destroyCharts(){
    try{ monthlyChart?.destroy(); }catch(_e){}
    try{ yearlyChart?.destroy(); }catch(_e){}
    monthlyChart = null;
    yearlyChart = null;
  }

  function monthsLabels(){
    return ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  }

  function drawMonthlySales(monthRows){
    const canvas = document.getElementById("salesMonthlyChart");
    if (!canvas || typeof Chart === "undefined") return;

    const labels = monthsLabels();
    const data = Array(12).fill(0);

    (monthRows || []).forEach(r => {
      // accept "month" int 1-12 or "billing_month" like "2026-01"
      let m = r.month ?? r.billing_month ?? r.m;
      let mi = -1;
      if (typeof m === "number") mi = m - 1;
      else if (typeof m === "string"){
        const mm = m.slice(-2);
        const n = parseInt(mm, 10);
        if (!isNaN(n)) mi = n - 1;
      }
      if (mi >= 0 && mi < 12){
        const v = r.value ?? r.qty ?? r.amt ?? r.total_value ?? r.total ?? 0;
        data[mi] += Number(v) || 0;
      }
    });

    const ctx = canvas.getContext("2d");
    monthlyChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels,
        datasets: [{
          label: "Sales (Monthly)",
          data
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: (c) => fmt(c.parsed.y) } }
        },
        scales: {
          y: { beginAtZero: true, ticks: { callback: (v) => fmt(v) } }
        }
      }
    });
  }

  function drawYearlySales(yearRows){
    const canvas = document.getElementById("salesYearlyChart");
    if (!canvas || typeof Chart === "undefined") return;

    // expect rows like: [{year:2025,value:...},{year:2026,value:...}] or similar
    const byYear = {};
    (yearRows || []).forEach(r => {
      const y = r.year ?? r.y ?? r.billing_year;
      if (y == null) return;
      const v = r.value ?? r.qty ?? r.amt ?? r.total_value ?? r.total ?? 0;
      byYear[String(y)] = (byYear[String(y)] || 0) + (Number(v)||0);
    });

    const labels = Object.keys(byYear).sort();
    const data = labels.map(k => byYear[k]);

    const ctx = canvas.getContext("2d");
    yearlyChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels,
        datasets: [{
          label: "Sales (Yearly)",
          data
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: (c) => fmt(c.parsed.y) } }
        },
        scales: {
          y: { beginAtZero: true, ticks: { callback: (v) => fmt(v) } }
        }
      }
    });
  }

  async function fetchAndRenderSales(){
    // Pull only sales summaries from the existing dashboard endpoint.
    // We keep it light: monthly + yearly for the selected filters.
    // metric: qty by default; change to amt if you want.
    const qs = buildQueryParams({ metric: "qty" });

    const dash = await fetchJSON(`${API_DASH}?${qs}`);
    if (!dash) return;

    // Try common field names (depends on your backend implementation)
    const monthRows =
      dash.sales_monthly ||
      dash.monthly_sales ||
      dash.monthly ||
      dash.monthly_rows ||
      [];

    const yearRows =
      dash.sales_yearly ||
      dash.yearly_sales ||
      dash.yearly ||
      dash.yearly_rows ||
      [];

    destroyCharts();
    drawMonthlySales(monthRows);
    drawYearlySales(yearRows);

    const title = document.getElementById("salesTitle");
    if (title){
      title.textContent = `Sales (qty) — ${state.category} / ${state.product_group}` +
        (state.pattern !== "ALL" ? ` / ${state.pattern}` : "") +
        (state.material !== "ALL" ? ` / ${state.material}` : "");
    }
  }

  // ---------------------- main fetch ----------------------
  async function fetchAndRender(){
    setStatus("Loading…");

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

    await fetchAndRenderSales();

    setStatus(
      `Done. Stock: ${(stockRes?.rows||[]).length}, ` +
      `Orders: ${(ordersRes?.rows||[]).length}, ` +
      `Incoming: ${(incomingRes?.rows||[]).length}`
    );
  }

  // ---------------------- dropdown/datalist loaders ----------------------
  async function refreshProductGroups(){
    const d = await fetchJSON("/api/v2/dimensions");
    const pgs = d?.product_groups || [];
    populateSelect($("#product_group"), pgs, true);
  }

  async function refreshPatterns(){
    const pg  = $("#product_group")?.value || "ALL";
    const res = await fetchJSON(`/api/patterns?product_group=${encodeURIComponent(pg)}`);
    const rows = Array.isArray(res) ? res : (res?.rows || []);
    __PATTERN_OPTIONS = rows.map(x => String(x)).filter(Boolean);
  }

  async function refreshMaterials(){
    const pg  = $("#product_group")?.value || "ALL";
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
    state.product_group = $("#product_group")?.value || "ALL";
    const pat = ($("#pattern")?.value || "").trim();
    const mat = ($("#material")?.value || "").trim();
    state.pattern = pat ? pat : "ALL";
    state.material = mat ? mat : "ALL";
  }

  function clearUI(){
    const pg  = document.getElementById("product_group");
    const pat = document.getElementById("pattern");
    const mat = document.getElementById("material");
    if (pg)  pg.value  = "ALL";
    if (pat) pat.value = "";
    if (mat) mat.value = "";
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

  $("#product_group")?.addEventListener("change", async () => {
    readUIToState();
    await refreshPatterns();
    await refreshMaterials();
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
      inputId: "pattern",
      btnId: "patternBtn",
      clearId: "patternClear",
      menuId: "patternMenu",
      getOptions: () => __PATTERN_OPTIONS,
      onPick: async (val) => {
        const v = (val === "ALL") ? "" : val;
        document.getElementById("pattern").value = v;
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
        document.getElementById("material").value = v;
        state.material = v || "ALL";
        await fetchAndRender();
      }
    });

    setTimeout(() => { try{ map.invalidateSize(); }catch(_e){} }, 50);

    await fetchAndRender();
  });
})();