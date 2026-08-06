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
  let __CODE_OPTIONS     = [];

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
    code: "ALL",   // carrying_26.m_code
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
    if (state.code && state.code !== "ALL")         p.set("code", state.code);
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
      // Click → open aging popup (loading state first, then filled in
      // once /api/stock_aging returns).  Kept lazy so the /stock page
      // load doesn't fire 4 aging queries up-front.
      m.on("click", () => openStockAgingPopup(m, plant, val));
      m.addTo(layerStock);
    });
  }

  // Stock aging popup — shows per-bucket totals for the clicked plant
  // and the top 30 aged materials.  Bucket cells are colour-coded from
  // green (fresh ≤12M) to deep red (37M+) so a glance already tells
  // the operator "there's a lot of red here" without reading numbers.
  const _AGING_COLORS = {
    "≤12M":    "#22c55e",
    "13-18M":  "#84cc16",
    "19-24M":  "#eab308",
    "25-36M":  "#f97316",
    "37M+":    "#dc2626",
    "unknown": "#94a3b8",
  };
  // Same ordered list the backend sends back in row.aging keys.
  const _AGING_BUCKETS = ["≤12M", "13-18M", "19-24M", "25-36M", "37M+"];
  async function openStockAgingPopup(marker, plant, headlineTotal){
    const container = document.createElement("div");
    container.style.minWidth = "420px";
    container.style.maxWidth = "540px";
    container.innerHTML =
      `<div style="font-weight:700;font-size:13px;">${plant} — stock aging</div>
       <div style="font-size:11.5px;color:#666;margin-top:2px;" id="_agTotal">
         Total: ${fmt(headlineTotal)}
       </div>
       <div id="_agBody" style="margin-top:6px;font-size:12px;color:#666;">
         Loading aging breakdown…
       </div>`;
    marker.unbindPopup();
    marker.bindPopup(container, {maxWidth: 560, minWidth: 420}).openPopup();

    // Send the same filters as the cascade table.  Without this the
    // aging bars would show the full-plant DOT population even when
    // the map circle has already been narrowed to a size / pattern.
    const qs = buildQueryParams({plant: plant});
    let data;
    try {
      const r = await fetch(`/api/stock_aging?${qs}`,
                             {credentials: "same-origin"});
      data = await r.json();
    } catch(e){
      container.querySelector("#_agBody").textContent = "Load failed: " + e;
      return;
    }
    if (data.error){
      container.querySelector("#_agBody").textContent = "Error: " + data.error;
      return;
    }

    const buckets = data.buckets || {};
    const order   = (data.bucket_order || []).filter(b =>
       (buckets[b] || 0) > 0 || b !== "unknown");   // hide empty 'unknown'
    const total   = data.total || 1;
    // Use the filtered total from the API so it matches the aging bars.
    const totalEl = container.querySelector("#_agTotal");
    if (totalEl) totalEl.textContent = `Total: ${fmt(data.total || 0)}`;

    // Bucket summary bar
    const summary = order.map(b => {
      const v = buckets[b] || 0;
      const pct = (v / total * 100).toFixed(1);
      const col = _AGING_COLORS[b] || "#94a3b8";
      return `
        <div style="display:flex;align-items:center;gap:6px;margin:2px 0;font-size:11.5px;">
          <div style="width:64px;">${b}</div>
          <div style="flex:1;background:#e5e7eb;height:10px;border-radius:3px;overflow:hidden;">
            <div style="width:${pct}%;background:${col};height:100%;"></div>
          </div>
          <div style="width:70px;text-align:right;font-variant-numeric:tabular-nums;">
            ${fmt(v)}
          </div>
          <div style="width:44px;text-align:right;color:#666;">
            ${pct}%
          </div>
        </div>`;
    }).join("");

    // Top-materials table — only rendered when no filter is active.
    // With a filter (size / pattern / etc.) the per-material breakdown
    // collapses to essentially one row and adds nothing over the
    // bucket bars, so we skip it entirely.
    let matSection = "";
    if (!data.has_filter) {
      const mats = data.materials || [];
      const matRows = mats.slice(0, 15).map(m => {
        const cells = order.map(b => {
          const v = m.buckets[b] || 0;
          const col = _AGING_COLORS[b] || "#94a3b8";
          return `<td style="padding:2px 4px;text-align:right;background:${v ? col + "22" : "transparent"};">
                    ${v ? fmt(v) : ""}
                  </td>`;
        }).join("");
        return `
          <tr>
            <td style="padding:2px 4px;">${_esc(m.material)}</td>
            <td style="padding:2px 4px;">${_esc(m.size)}</td>
            <td style="padding:2px 4px;">${_esc(m.pattern)}</td>
            ${cells}
            <td style="padding:2px 4px;text-align:right;font-weight:600;">${fmt(m.total)}</td>
          </tr>`;
      }).join("");
      matSection = `
        <div style="font-size:11.5px;font-weight:600;margin:8px 0 2px;">
          Top aged materials (biggest 37M+ / 25-36M first)
        </div>
        <div style="max-height:260px;overflow:auto;border:1px solid #e5e7eb;border-radius:4px;">
          <table style="width:100%;border-collapse:collapse;font-size:11px;">
            <thead>
              <tr style="background:#f1f5f9;position:sticky;top:0;">
                <th style="padding:4px;text-align:left;">Material</th>
                <th style="padding:4px;text-align:left;">Size</th>
                <th style="padding:4px;text-align:left;">Pattern</th>
                ${order.map(b => `<th style="padding:4px;text-align:right;">${b}</th>`).join("")}
                <th style="padding:4px;text-align:right;">Total</th>
              </tr>
            </thead>
            <tbody>${matRows}</tbody>
          </table>
        </div>`;
    }

    container.querySelector("#_agBody").innerHTML =
      `<div style="margin-bottom:8px;">${summary}</div>${matSection}`;
  }

  function _esc(s){
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
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
  // When true, the Stock column is expanded into 5 aging-bucket
  // sub-columns.  Toggled by the ▸ chip in the Stock header cell.
  let _agingExpanded = false;

  function _renderCascadeHeader(){
    const thead = document.querySelector("#cascadeTable thead");
    if (!thead) return;
    const bucketLbl = CASCADE_LABEL[cascadeState.level];
    const stockCells = _agingExpanded
      ? (
          _AGING_BUCKETS.map((b, i) => {
            const col = _AGING_COLORS[b];
            const toggle = (i === 0)
              ? `<button type="button" class="ag-toggle" data-ag-toggle="1" title="Collapse aging">◂</button> `
              : "";
            return `<th class="st-qty-hdr aging-hdr" data-ag="${b}"
                         style="background:${col}22;">
                      ${toggle}${b}
                    </th>`;
          }).join("")
          // Keep the aggregate Stock total visible even when the column
          // is expanded — so the user doesn't have to eyeball-sum five
          // aging cells to see how much stock a state actually holds.
          + `<th class="st-qty-hdr aging-hdr" title="Sum of the 5 aging buckets"
                  style="background:#e0e7ff;">Total</th>`
        )
      : `<th class="st-qty-hdr">
           Stock
           <button type="button" class="ag-toggle" data-ag-toggle="1"
                   title="Expand by DOT age">▸</button>
         </th>`;
    thead.innerHTML = `<tr>
      <th id="cascadeBucketTh">${bucketLbl}</th>
      <th>State</th>
      <th>3M</th>
      <th>6M</th>
      <th>12M</th>
      <th>Base Sales</th>
      ${stockCells}
      <th class="st-qty-hdr">Water</th>
      <th class="st-qty-hdr">CY</th>
      <th class="st-qty-hdr">Factory</th>
      <th>Stock.idx</th>
      <th>+Water.idx</th>
      <th>+Factory.idx</th>
    </tr>`;
  }

  function _cascadeColspan(){
    // 13 base columns + 5 extra when Stock is expanded to 5 aging cells
    // plus a Total cell.
    return _agingExpanded ? 18 : 13;
  }

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

  // Low-stock-warning mode: when on, the cascade body is replaced
  // with just the (line › pg › pattern › size × state) rows whose
  // Stock.idx ≤ 1.5 mo.  Same table, same columns, filtered content.
  let _lowStockOn = false;

  async function fetchAndRenderCascadeTable(){
    if (_lowStockOn) { await renderLowStockRows(); return; }
    _renderCascadeCrumb();
    _renderCascadeHeader();
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
    const _colspan = _cascadeColspan();
    if (!data || !data.rows || data.rows.length === 0) {
      tbody.innerHTML = `<tr><td colspan="${_colspan}" class="st-loading">No data</td></tr>`;
      tfoot.innerHTML = "";
      return;
    }
    // Helper — one Stock cell OR (five aging bucket cells + one Total).
    // The Total cell always shows the aggregate stock so the user can
    // still see it at a glance when the column is expanded.
    const stockCells = (aging, plainQty) => (
      _agingExpanded
        ? (
            _AGING_BUCKETS.map(b => {
              const v = (aging || {})[b] || 0;
              const col = _AGING_COLORS[b];
              return `<td class="st-qty aging-cell" style="background:${v ? col + "22" : "transparent"};">${v ? fmtQty(v) : ""}</td>`;
            }).join("")
            + `<td class="st-qty aging-cell" style="background:#e0e7ff;font-weight:600;">${fmtQty(plainQty)}</td>`
          )
        : `<td class="st-qty">${fmtQty(plainQty)}</td>`
    );
    // Above-size rows drill the cascade.  Size rows are still
    // clickable but they don't drill — they set state.material so the
    // map + charts narrow to that exact size.  Both paths share the
    // .cascade-clickable hook; the visual chevron (`›`) is suppressed
    // on size rows via .cas-terminal.
    const drillable = true;
    const terminalCls = (cascadeState.level === "size") ? " cas-terminal" : "";
    let totQ3=0, totQ6=0, totQ12=0,
        totStock=0, totWater=0, totCy=0, totFactory=0;
    const totAging = _AGING_BUCKETS.reduce((acc, b) => (acc[b] = 0, acc), {});

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
          ${stockCells(s.aging, sq)}
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
      const rAg = r.aging || {};
      _AGING_BUCKETS.forEach(bk => { totAging[bk] += (rAg[bk] || 0); });
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
        ${stockCells(r.aging, sq)}
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
      ${stockCells(totAging, totStock)}
      <td class="st-qty">${fmtQty(totWater)}</td>
      <td class="st-qty">${fmtQty(totCy)}</td>
      <td class="st-qty">${fmtQty(totFactory)}</td>
      <td class="st-pipe">${tpipeS}</td>
      <td class="st-pipe">${tpipeSW}</td>
      <td class="st-pipe">${tpipeSWF}</td>
    </tr>`;
  }

  // ── Low-stock warning body ──────────────────────────────────────
  // Same cascade table, filtered content: one row per
  // (line › pg › pattern › size × state) whose Stock.idx ≤ 1.5.
  // Water / CY / Factory columns show '—' since the warning
  // endpoint doesn't compute those (we only need stock + base +
  // Stock.idx to make the warn/no-warn call).
  async function renderLowStockRows(){
    const tbody = document.getElementById("cascadeTableBody");
    const tfoot = document.getElementById("cascadeTableFoot");
    const crumb = document.getElementById("cascadeCrumb");
    const back  = document.getElementById("cascadeBack");
    const bkTh  = document.getElementById("cascadeBucketTh");
    if (!tbody || !tfoot) return;
    if (crumb) crumb.textContent = "⚠ Low stock warning";
    if (back)  back.style.display = "none";
    if (bkTh)  bkTh.textContent = "Line › PG › Pattern › Size";
    const _colspan = _cascadeColspan();
    tbody.innerHTML = `<tr><td colspan="${_colspan}" class="st-loading">Loading…</td></tr>`;
    tfoot.innerHTML = "";

    const qs = buildQueryParams({ threshold: 1.5 });
    const d  = await fetchJSON(`/api/stock_warnings?${qs}`);
    const rows = (d && d.rows) || [];
    const countEl = document.getElementById("lowStockCount");
    if (countEl) countEl.textContent = rows.length
      ? `— ${rows.length} row${rows.length===1?"":"s"}`
      : "— none";
    if (!rows.length) {
      tbody.innerHTML = `<tr><td colspan="${_colspan}" class="st-loading">
        No sizes with Stock.idx ≤ 1.5 for the current filter.
      </td></tr>`;
      return;
    }
    // Colour scale by urgency.
    const warnCls = idx => idx <= 0.5 ? "warn-crit"
                        :  idx <= 1.0 ? "warn-high"
                        :  "warn-med";
    const dash = `<td class="st-qty">—</td>`;
    const html = rows.map(r => {
      const label = [r.line, r.product_group, r.pattern, r.size]
                    .filter(x => x && String(x).trim()).join(" › ");
      // Empty Stock aging cells when the ▸ Stock header is expanded
      // — keep the layout aligned but visually blank.
      const stkPart = _agingExpanded
        ? (_AGING_BUCKETS.map(() => `<td class="st-qty aging-cell"></td>`).join("")
           + `<td class="st-qty aging-cell" style="background:#e0e7ff;font-weight:600;">${fmtQty(r.stock_qty)}</td>`)
        : `<td class="st-qty">${fmtQty(r.stock_qty)}</td>`;
      // Data-* attributes so a click can drill the cascade to that
      // exact size (same interaction the normal size-row click has).
      return `
        <tr class="cascade-clickable ${warnCls(r.stock_idx)}"
            data-warn-line="${escAttr(r.line)}"
            data-warn-pg="${escAttr(r.product_group)}"
            data-warn-pattern="${escAttr(r.pattern)}"
            data-warn-size="${escAttr(r.size)}">
          <td class="cas-bucket">${escHtml(label)}</td>
          <td class="cas-state">${r.state}</td>
          <td>—</td><td>—</td><td>—</td>
          <td class="st-base">${fmtQty(r.base_sales)}</td>
          ${stkPart}
          ${dash}${dash}${dash}
          <td class="st-pipe">${r.stock_idx.toFixed(1)} mo</td>
          <td class="st-pipe">—</td>
          <td class="st-pipe">—</td>
        </tr>
      `;
    }).join("");
    tbody.innerHTML = html;
  }
  function escHtml(s){ return String(s == null ? "" : s)
    .replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
  function escAttr(s){ return escHtml(s).replace(/"/g,"&quot;"); }

  // Reverse direction of _syncPageFiltersFromCascade: when the user
  // narrows via the search/dropdowns (product_group / pattern /
  // material), jump the cascade to the matching level so the table
  // bottoms out at exactly what they searched for.  /api/cascade_
  // ancestors resolves the most-specific filter back to its line /
  // product_group / pattern chain.  Returns true if cascadeState
  // actually moved.
  async function _syncCascadeFromFilters(){
    const params = new URLSearchParams();
    // Code is the most specific — resolves to a single m_code, so the
    // backend can hand back the full line/pg/pattern/size chain in one
    // shot.  Send it first (and alone if set) so upstream filters get
    // reset to the code's real ancestors instead of whatever stale
    // combination was in place.
    if (state.code && state.code !== "ALL")
      params.set("code", state.code);
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
      // When resolved via `code`, also lock the sibling dropdowns to
      // the code's ancestors so the whole filter row visually agrees
      // with the picked code.
      if (state.code && state.code !== "ALL" && d.size) {
        state.material = d.size;
        const matEl = document.getElementById("material");
        if (matEl) { matEl.value = d.size; ddUpdateActive(matEl); }
      }
      if (state.code && state.code !== "ALL" && d.pattern) {
        state.pattern = d.pattern;
        const patEl = document.getElementById("pattern");
        if (patEl) { patEl.value = d.pattern; ddUpdateActive(patEl); }
      }
      if (state.code && state.code !== "ALL" && d.product_group) {
        state.product_group = d.product_group;
        const pgEl = document.getElementById("product_group");
        if (pgEl) { pgEl.value = d.product_group; ddUpdateActive(pgEl); }
      }
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
    state.code     = "ALL";
    const matEl = document.getElementById("material");
    if (matEl) { matEl.value = ""; ddUpdateActive(matEl); }
    const cdEl  = document.getElementById("code");
    if (cdEl)  { cdEl.value  = ""; ddUpdateActive(cdEl); }
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
      // If pg was empty (size pick path), step all the way back to
      // line so the user isn't stranded at an emptier intermediate.
      if (!cascadeState.pg) {
        cascadeState = { level: "line", line: "", pg: "", pat: "" };
      } else {
        cascadeState = { level: "product_group", line: cascadeState.line, pg: "", pat: "" };
      }
    } else if (lvl === "size") {
      // size → pattern, but skip levels we never pinned.
      if (cascadeState.pg && cascadeState.pat) {
        cascadeState = { level: "pattern", line: cascadeState.line, pg: cascadeState.pg, pat: "" };
      } else if (cascadeState.line) {
        cascadeState = { level: "product_group", line: cascadeState.line, pg: "", pat: "" };
      } else {
        cascadeState = { level: "line", line: "", pg: "", pat: "" };
      }
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
    // Stock header ▸/◂ chip → toggle 5-bucket aging expansion.
    const agBtn = e.target.closest && e.target.closest('[data-ag-toggle="1"]');
    if (agBtn) {
      _agingExpanded = !_agingExpanded;
      fetchAndRenderCascadeTable();
      e.stopPropagation();
      return;
    }
    // ⚠ Low stock warning chip → swap the cascade body for the
    // warning list (or back).
    if (e.target && e.target.id === "lowStockBtn") {
      _lowStockOn = !_lowStockOn;
      e.target.classList.toggle("active", _lowStockOn);
      if (!_lowStockOn) {
        const c = document.getElementById("lowStockCount");
        if (c) c.textContent = "";
      }
      fetchAndRenderCascadeTable();
      return;
    }
    const row = e.target.closest && e.target.closest("#cascadeTable tbody tr.cascade-clickable");
    if (row) {
      // Warning-mode row: drill the cascade to that exact size and
      // exit warning mode so the drill lands on the normal Size view.
      if (row.dataset.warnSize != null) {
        _lowStockOn = false;
        const btn = document.getElementById("lowStockBtn");
        if (btn) btn.classList.remove("active");
        const countEl = document.getElementById("lowStockCount");
        if (countEl) countEl.textContent = "";
        // Set the size filter directly — same effect as clicking a
        // size row in the normal cascade — then let
        // _syncCascadeFromFilters snap the cascade to the size's
        // ancestors before we re-render.
        state.material = row.dataset.warnSize || "ALL";
        const matEl = document.getElementById("material");
        if (matEl) { matEl.value = state.material === "ALL" ? "" : state.material;
                     ddUpdateActive(matEl); }
        (async () => {
          await _syncCascadeFromFilters();
          await fetchAndRender();
        })();
        return;
      }
      if (row.dataset.bucket != null) {
        _cascadeAdvance(row.dataset.bucket);
        return;
      }
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

  // ── Monthly stock-vs-sales history chart ───────────────────────
  // Reads /api/stock_history (backed by the `stock_history` table
  // that sapcrawling_history_26.py populates + sales_2526 for the
  // sell-out leg). One canvas, two datasets, twin y-axes because
  // month-start stock and single-month sales sit on very
  // different scales.
  let _stockHistoryChart      = null;
  let _stockAgingHistoryChart = null;
  // The ▸ Aging toggle just flips this; the fetch/render is kicked
  // from wherever we already fetch the stock history so the two
  // views stay in sync with the current filter set.
  let _agingHistoryOn         = false;
  // Region chip in the chart header — 'ALL' or one of NSW/QLD/VIC/WA.
  // Applies to both the line chart and the aging bars.
  let _historyState           = "ALL";
  // Same palette the /stock aging popover uses so the two views
  // read consistently (green fresh → deep red 37M+).
  const _AGING_BAR_COLORS = {
    "≤12M":    "#22c55e",
    "13-18M":  "#84cc16",
    "19-24M":  "#eab308",
    "25-36M":  "#f97316",
    "37M+":    "#dc2626",
    "unknown": "#94a3b8",
  };
  const _AGING_BAR_ORDER = ["≤12M","13-18M","19-24M","25-36M","37M+","unknown"];
  async function fetchAndRenderStockHistory(){
    const canvas = document.getElementById("stockHistoryChart");
    if (!canvas || typeof Chart === "undefined") return;
    const hint = document.getElementById("stockHistoryHint");
    if (hint) hint.textContent = "loading…";
    const parts = [buildQueryParams()];
    if (_agingHistoryOn)              parts.push("with_aging=1");
    if (_historyState && _historyState !== "ALL")
                                      parts.push("state=" + encodeURIComponent(_historyState));
    const qs = parts.join("&");
    const d  = await fetchJSON(`/api/stock_history?${qs}`);
    if (!d) { if (hint) hint.textContent = "load failed"; return; }
    const months = d.months || [];
    if (hint) {
      hint.textContent = d.warning
        ? d.warning
        : (months.length + " month" + (months.length === 1 ? "" : "s"));
    }
    if (_stockHistoryChart) { _stockHistoryChart.destroy(); _stockHistoryChart = null; }
    if (!months.length) {
      // Nothing to plot — leave the canvas blank so the panel
      // doesn't look broken.
      const ctx = canvas.getContext && canvas.getContext("2d");
      if (ctx) ctx.clearRect(0, 0, canvas.width, canvas.height);
      return;
    }
    _stockHistoryChart = new Chart(canvas, {
      type: "line",
      data: {
        labels: months.map(m => m.label || m.snapshot_date),
        datasets: [
          {
            label: "Stock (month start)",
            data:  months.map(m => m.stock_qty),
            borderColor: "#3b82f6",
            backgroundColor: "rgba(59,130,246,0.15)",
            fill: true, tension: 0.25,
            pointRadius: 3, pointHoverRadius: 5,
          },
          {
            label: "Sales (that month)",
            data:  months.map(m => m.sales_qty),
            borderColor: "#f97316",
            backgroundColor: "rgba(249,115,22,0.10)",
            fill: false, tension: 0.25,
            pointRadius: 3, pointHoverRadius: 5,
          },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { position: "bottom",
                    labels: { boxWidth: 12, font: { size: 11 } } },
          tooltip: {
            callbacks: {
              label: (ctx) =>
                `${ctx.dataset.label}: ${Math.round(ctx.parsed.y).toLocaleString()}`,
            },
          },
        },
        scales: {
          x: { ticks: { font: { size: 10 } } },
          // Shared y so Stock vs Sales sit on the same magnitude —
          // the visual gap between the two lines IS the story.
          y: { type: "linear", beginAtZero: true,
               ticks: { font: { size: 10 },
                        callback: v => Number(v).toLocaleString() } },
        },
      },
    });

    // Aging companion: same x labels, stacked bars per bucket.
    renderStockAgingHistory(d.aging || []);
  }

  function renderStockAgingHistory(aging){
    const wrap   = document.getElementById("stockAgingHistoryWrap");
    const canvas = document.getElementById("stockAgingHistoryChart");
    if (!wrap || !canvas) return;
    // Hidden unless the ▸ Aging chip is on.
    wrap.hidden = !_agingHistoryOn;
    if (_stockAgingHistoryChart) {
      _stockAgingHistoryChart.destroy();
      _stockAgingHistoryChart = null;
    }
    if (!_agingHistoryOn || !aging.length) return;

    // One dataset per bucket; stacked on the same y so the bar
    // height is the total stock and the coloured segments show
    // how the mix ages over time.
    const datasets = _AGING_BAR_ORDER
      .filter(b => aging.some(m => (m.buckets || {})[b] > 0))
      .map(b => ({
        label: b,
        data:  aging.map(m => (m.buckets || {})[b] || 0),
        backgroundColor: _AGING_BAR_COLORS[b],
        borderColor:     _AGING_BAR_COLORS[b],
        borderWidth: 1,
        stack: "stock",
      }));
    _stockAgingHistoryChart = new Chart(canvas, {
      type: "bar",
      data: { labels: aging.map(m => m.label || m.snapshot_date), datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { position: "bottom",
                    labels: { boxWidth: 12, font: { size: 11 } } },
          tooltip: {
            callbacks: {
              label: (ctx) =>
                `${ctx.dataset.label}: ${Math.round(ctx.parsed.y).toLocaleString()}`,
              footer: (items) => {
                const tot = items.reduce((s, it) => s + (it.parsed.y || 0), 0);
                return `Total: ${Math.round(tot).toLocaleString()}`;
              },
            },
          },
        },
        scales: {
          x: { stacked: true, ticks: { font: { size: 10 } } },
          y: { stacked: true, beginAtZero: true,
               ticks: { font: { size: 10 },
                        callback: v => Number(v).toLocaleString() } },
        },
      },
    });
  }

  // Wire the ▸ Aging toggle chip.  Deferred so the DOM is
  // guaranteed to be there — this file loads with `defer`.
  {
    const btn = document.getElementById("stockAgingToggle");
    if (btn) {
      btn.addEventListener("click", () => {
        _agingHistoryOn = !_agingHistoryOn;
        btn.classList.toggle("active", _agingHistoryOn);
        btn.textContent = (_agingHistoryOn ? "◂ Aging" : "▸ Aging");
        // Refresh — the fetch adds ?with_aging=1 when the flag
        // is on so we don't pay the extra grouping cost otherwise.
        fetchAndRenderStockHistory();
      });
    }
  }

  // Wire the region-chip row (All / NSW / QLD / VIC / WA) in the
  // history header.  A click just flips _historyState and re-fetches
  // — both the line chart and the aging bars pick up the same filter
  // via the shared fetchAndRenderStockHistory path.
  {
    const seg = document.getElementById("stockHistoryStateSeg");
    if (seg) {
      seg.addEventListener("click", (e) => {
        const btn = e.target.closest(".hist-st-btn");
        if (!btn) return;
        const val = btn.dataset.st || "ALL";
        if (val === _historyState) return;   // no-op click on active chip
        _historyState = val;
        seg.querySelectorAll(".hist-st-btn").forEach(b =>
          b.classList.toggle("active", b === btn));
        fetchAndRenderStockHistory();
      });
    }
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

      // Monthly history line chart under the cascade — same filter
      // chips so the two views move together.  Non-blocking: it
      // takes a few hundred ms so we don't await it in the critical
      // path.
      fetchAndRenderStockHistory();

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

  async function refreshCodes(){
    const pg  = state.product_group;
    const pat = ($("#pattern")?.value  || "").trim();
    const mat = ($("#material")?.value || "").trim();
    const qs  = new URLSearchParams({
      product_group: pg,
      ...(pat ? { pattern:  pat } : {}),
      ...(mat ? { material: mat } : {}),
    }).toString();
    const res = await fetchJSON(`/api/codes?${qs}`);
    const rows = Array.isArray(res) ? res : (res?.rows || []);
    __CODE_OPTIONS = rows.map(x => String(x)).filter(Boolean);
  }

  function readUIToState(){
    const pgVal = ($("#product_group")?.value || "").trim();
    state.product_group = pgVal || "ALL";
    const pat  = ($("#pattern")?.value  || "").trim();
    const mat  = ($("#material")?.value || "").trim();
    const cd   = ($("#code")?.value     || "").trim();
    state.pattern  = pat ? pat : "ALL";
    state.material = mat ? mat : "ALL";
    state.code     = cd  ? cd  : "ALL";
  }

  function clearUI(){
    const pg  = document.getElementById("product_group");
    const pat = document.getElementById("pattern");
    const mat = document.getElementById("material");
    const cd  = document.getElementById("code");
    if (pg)  { pg.value  = ""; ddUpdateActive(pg); }
    if (pat) { pat.value = ""; ddUpdateActive(pat); }
    if (mat) { mat.value = ""; ddUpdateActive(mat); }
    if (cd)  { cd.value  = ""; ddUpdateActive(cd); }
    state.category     = "ALL";
    state.product_group = "ALL";
    state.pattern      = "ALL";
    state.material     = "ALL";
    state.code         = "ALL";
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
    await refreshCodes();

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
        const cdEl  = document.getElementById("code");
        if (patEl) { patEl.value = ""; ddUpdateActive(patEl); }
        if (matEl) { matEl.value = ""; ddUpdateActive(matEl); }
        if (cdEl)  { cdEl.value  = ""; ddUpdateActive(cdEl); }
        state.pattern  = "ALL";
        state.material = "ALL";
        state.code     = "ALL";
        await refreshPatterns();
        await refreshMaterials();
        await refreshCodes();
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
        await refreshCodes();
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
        await refreshCodes();
        await _syncCascadeFromFilters();
        await fetchAndRender();
      }
    });

    bindDropdown({
      inputId: "code",
      btnId: "codeBtn",
      clearId: "codeClear",
      menuId: "codeMenu",
      getOptions: () => __CODE_OPTIONS,
      onPick: async (val) => {
        const v = (val === "ALL") ? "" : val;
        const cdEl = document.getElementById("code");
        if (cdEl) { cdEl.value = v; ddUpdateActive(cdEl); }
        state.code = v || "ALL";
        // Code is the leaf dimension — resolve its full ancestor chain
        // (line / pg / pattern / size) so the sibling dropdowns show
        // what this code actually belongs to, and drop the cascade
        // table straight onto the corresponding size row.
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