// ============== MAP VIEW (uses global filters from app.js) ==============

// Simple palette per BDE
const BDE_COLOR_PALETTE = [
  "#4c6fff", "#ff4d6a", "#00b894", "#fdcb6e",
  "#6c5ce7", "#0984e3", "#d63031", "#00cec9",
  "#e84393", "#2d3436"
];

const bdeColorMap = {};
let bdeColorIndex = 0;

function getBdeColor(bdeName) {
  if (!bdeName) return "#999999";
  const key = String(bdeName).trim();
  if (!bdeColorMap[key]) {
    const c = BDE_COLOR_PALETTE[bdeColorIndex % BDE_COLOR_PALETTE.length];
    bdeColorMap[key] = c;
    bdeColorIndex += 1;
  }
  return bdeColorMap[key];
}

let salesMap = null;
let salesMapLayer = null;
let shopMonthlyInst = null;
let shopYearlyInst  = null;

function initSalesMap() {
  if (salesMap) return;

  const el = document.getElementById("salesMap");
  if (!el) return;  // not on the map page

  salesMap = L.map("salesMap", {
    minZoom: 4
  }).setView([-27.0, 134.0], 5);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 18,
    attribution: "&copy; OpenStreetMap contributors"
  }).addTo(salesMap);

  salesMapLayer = L.layerGroup().addTo(salesMap);
}

// This is called from refreshAllWithKpi() when we are on the map page
async function loadSalesMap() {
  initSalesMap();
  if (!salesMapLayer) return;
  salesMapLayer.clearLayers();

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
    top_limit:     filters.top_limit || 0 
  }).toString();

  const [data, summary] = await Promise.all([
    fetchJSON(`/api/sales_map?${qs}`),
    fetchJSON(`/api/visit_summary?year=2026`),
  ]);
  if (!Array.isArray(data) || data.length === 0) {
    salesMap.setView([-25.0, 133.0], 4);
    renderBdeVisitTable(summary);
    return;
  }

  const visitsByShipTo = (summary && summary.visits_by_ship_to) || {};
  const visitedSet = new Set(Object.keys(visitsByShipTo));
  const visitedShops = (summary && Array.isArray(summary.visited_shops))
                       ? summary.visited_shops : [];
  const loggedSet  = new Set((summary && summary.logged_ship_tos) || []);
  // Per-purpose ship-to sets — drives the Purpose filter row.
  const loggedByPurpose = (summary && summary.logged_ship_tos_by_purpose) || {};
  const purposeSets = {};
  for (const k of Object.keys(loggedByPurpose)) {
    purposeSets[k] = new Set(loggedByPurpose[k] || []);
  }
  const purposeMode = (window._purposeFilter || "ALL");
  const passesPurpose = (shipTo) => {
    // "All" in the Purpose row = every shop with at least one logged
    // visit (any purpose).  No filter is applied to non-logged shops —
    // the count and the marker set both restrict to loggedSet.
    if (purposeMode === "ALL") return loggedSet.has(shipTo);
    const set = purposeSets[purposeMode];
    return set ? set.has(shipTo) : false;
  };

  // Status filter — All / Logs / Visit / No-visit — composes with the
  // other filters (Region, Salesman, Sold-to, Top N, etc.) by simply
  // skipping markers that don't match.
  const statusMode = (window._shopStatusFilter || "ALL");
  const passesStatus = (shipTo) => {
    if (statusMode === "ALL")     return true;
    if (statusMode === "LOGS")    return loggedSet.has(shipTo);
    if (statusMode === "VISIT")   return visitedSet.has(shipTo);
    if (statusMode === "NOVISIT") return !visitedSet.has(shipTo);
    return true;
  };

  // Status-chip badge counts.  Computed over the full filtered dataset
  // (other filters already applied server-side) BEFORE applying the
  // status filter itself, so the user sees how many shops fall into
  // each bucket regardless of which chip is currently active.
  let cAll = 0, cLogs = 0, cVisit = 0, cNoVisit = 0;
  const _seen = new Set();
  data.forEach(row => {
    const sh = row.ship_to ?? row.Ship_To ?? "";
    if (!sh || _seen.has(sh)) return;
    _seen.add(sh);
    cAll += 1;
    if (loggedSet.has(sh))       cLogs    += 1;
    if (visitedSet.has(sh))      cVisit   += 1;
    else                         cNoVisit += 1;
  });
  document.querySelectorAll(".ss-count").forEach(el => {
    const k = el.dataset.c;
    el.textContent = (k === "ALL"     ? cAll
                    : k === "LOGS"    ? cLogs
                    : k === "VISIT"   ? cVisit
                    : k === "NOVISIT" ? cNoVisit : 0).toLocaleString();
  });
  // Purpose-row counts — intersect each purpose's logged set with the
  // shops currently visible in `data` (i.e. respecting other filters).
  const visibleShipTos = new Set(data.map(r => r.ship_to ?? r.Ship_To ?? "").filter(Boolean));
  const cByPurpose = {};
  // Purpose=All counts ANY logged shop in the visible set (union of
  // every purpose).  Matches Shop-status Logs.
  let cPurposeAll = 0;
  visibleShipTos.forEach(s => { if (loggedSet.has(s)) cPurposeAll += 1; });
  for (const k of Object.keys(purposeSets)) {
    let n = 0;
    purposeSets[k].forEach(s => { if (visibleShipTos.has(s)) n += 1; });
    cByPurpose[k] = n;
  }
  document.querySelectorAll(".pf-count").forEach(el => {
    const k = el.dataset.p;
    el.textContent = (k === "ALL" ? cPurposeAll : (cByPurpose[k] || 0)).toLocaleString();
  });

  // BDE-overlay mode: when a single salesman is filter-selected, colour
  // markers semantically (green / red / orange) instead of by BDE palette
  // so own-territory vs other-territory visits are obvious at a glance.
  const selectedBde   = (filters.salesman && filters.salesman !== "ALL")
                        ? String(filters.salesman) : "";
  const selectedNorm  = selectedBde.trim().toUpperCase();
  const bdeMode       = !!selectedBde;
  const legendEl      = document.getElementById("mapLegend");
  if (legendEl) legendEl.style.display = bdeMode ? "block" : "none";
  const COLOR_OWN_VISITED   = "#16a34a";  // green  — own + visited
  const COLOR_OWN_NOVISIT   = "#dc2626";  // red    — own + not visited
  const COLOR_OTHER_VISITED = "#f59e0b";  // amber  — other territory + visited

  const points = [];
  const ownShipTos = new Set();

  data.forEach(row => {
  if (!window.__loggedSalesMapRow) {
    console.log("sales_map row:", row);
    window.__loggedSalesMapRow = true;
  }

  const lat = row.lat ?? row.latitude ?? row.Latitude;
  const lng = row.lng ?? row.longitude ?? row.Longitude;
  if (lat == null || lng == null) return;

  const total  = Number(row.total_value || 0);
  const target = Number(row.target_value || 0);
  const ach    = target > 0 ? (total / target) * 100 : 0;

  const radius = 4 + Math.log10(total + 1) * 3;

  const regionVal = row.region ?? row.Region;
  const shipTo = row.ship_to ?? row.Ship_To ?? "";
  const shipNm = row.ship_to_name ?? row.Ship_To_Name ?? "";
  const bde    = row.bde ?? row.BDE ?? row.BDE_Name ?? "";

  ownShipTos.add(shipTo);

  // Shop-status filter — hide markers that don't match.
  if (!passesStatus(shipTo)) return;
  if (!passesPurpose(shipTo)) return;

  const latNum = +lat;
  const lngNum = +lng;
  const isVisited = visitedSet.has(shipTo);

  let fillColor, strokeColor, fillOp, strokeOp, weight;
  if (bdeMode) {
    // sales_map is already filtered to the selected BDE's territory,
    // so every row here is own-territory by definition.
    fillColor   = isVisited ? COLOR_OWN_VISITED : COLOR_OWN_NOVISIT;
    strokeColor = fillColor;
    fillOp      = isVisited ? 0.85 : 0.35;
    strokeOp    = isVisited ? 1.0  : 0.85;
    weight      = isVisited ? 1.5  : 1.0;
  } else {
    const c = getBdeColor(bde);
    fillColor   = c;
    strokeColor = c;
    fillOp      = isVisited ? 0.85 : 0.4;
    strokeOp    = isVisited ? 1.0  : 0.85;
    weight      = isVisited ? 1.5  : 1.0;
  }

  const marker = L.circleMarker([latNum, lngNum], {
    radius,
    color: strokeColor,
    fillColor,
    fillOpacity: fillOp,
    opacity:     strokeOp,
    weight
  });

  const visitCount = visitsByShipTo[shipTo] || 0;
  marker.bindPopup(
  `${shipTo} - ${shipNm}<br>` +
  `Region: ${regionVal || "-"}<br>` +
  `BDE: ${bde || "-"}<br>` +
  `Total Visits (2026): ${visitCount}<br>` +
  `<span id="popup-total">Total (2026): …</span><br>` +
  `<button type="button" onclick="openVisitLogModal('${shipTo}','${(shipNm || '').replace(/'/g,"\\'")}')"`
  + ` style="margin-top:6px;font-size:11px;padding:3px 8px;cursor:pointer;`
  + `border:1px solid #2563eb;background:#eff6ff;color:#1d4ed8;border-radius:4px;">`
  + `📝 View visit logs</button>`
  );

  marker.on("click", () => {
    const titleEl = document.getElementById("shopTitle");
    if (titleEl) {
      titleEl.textContent = (shipNm || shipTo) + " – Monthly / Yearly";
    }
    drawShopCharts(shipTo, latNum, lngNum);
  });

  marker.addTo(salesMapLayer);
  points.push([latNum, lngNum]);
});

  // BDE-overlay extra layer: shops the selected BDE physically visited
  // that are NOT part of their own territory (orange).  Skipped in
  // "ALL" mode where every shop already shows in its own colour.
  if (bdeMode && selectedNorm) {
    visitedShops.forEach(vs => {
      const vb = Array.isArray(vs.visited_by) ? vs.visited_by : [];
      if (!vb.includes(selectedNorm)) return;
      if (ownShipTos.has(vs.ship_to)) return;
      // BDE-overlay markers are by definition GPS-visited.  Apply the
      // same status filter so "No-visit" hides them and "Logs" keeps
      // only those that also have a meeting_log entry.
      if (!passesStatus(vs.ship_to)) return;
      if (!passesPurpose(vs.ship_to)) return;
      const latNum = +vs.latitude;
      const lngNum = +vs.longitude;
      if (!isFinite(latNum) || !isFinite(lngNum)) return;
      const radius = 5;
      const marker = L.circleMarker([latNum, lngNum], {
        radius,
        color: COLOR_OTHER_VISITED,
        fillColor: COLOR_OTHER_VISITED,
        fillOpacity: 0.85,
        opacity: 1.0,
        weight: 1.5
      });
      marker.bindPopup(
        `${vs.ship_to} - ${vs.ship_to_name || ""}<br>` +
        `Assigned BDE: ${vs.salesman_name || "-"}<br>` +
        `Visited by: ${selectedBde}<br>` +
        `Total Visits (2026): ${vs.visit_days || 0}<br>` +
        `<button type="button" onclick="openVisitLogModal('${vs.ship_to}','${(vs.ship_to_name || '').replace(/'/g,"\\'")}')"`
        + ` style="margin-top:6px;font-size:11px;padding:3px 8px;cursor:pointer;`
        + `border:1px solid #2563eb;background:#eff6ff;color:#1d4ed8;border-radius:4px;">`
        + `📝 View visit logs</button>`
      );
      marker.on("click", () => {
        const titleEl = document.getElementById("shopTitle");
        if (titleEl) {
          titleEl.textContent = (vs.ship_to_name || vs.ship_to) + " – Monthly / Yearly";
        }
        drawShopCharts(vs.ship_to, latNum, lngNum);
      });
      marker.addTo(salesMapLayer);
      points.push([latNum, lngNum]);
    });
  }


  if (points.length === 1) {
    salesMap.setView(points[0], 10);
  } else if (points.length > 1) {
    const bounds = L.latLngBounds(points);
    salesMap.fitBounds(bounds.pad(0.1));
  } else {
    salesMap.setView([-25.0, 133.0], 4);
  }

  renderBdeVisitTable(summary);
  // Show default charts on the right panel so it isn't empty before a
  // marker is clicked. Scope follows the current filter (salesman > region > all).
  drawDefaultShopCharts();
}

function defaultShopTitle() {
  if (filters.salesman && filters.salesman !== "ALL") {
    return filters.salesman + " – Monthly / Yearly";
  }
  if (filters.region && filters.region !== "ALL") {
    return filters.region + " – Monthly / Yearly";
  }
  if (filters.sold_to_group && filters.sold_to_group !== "ALL") {
    return filters.sold_to_group + " – Monthly / Yearly";
  }
  return "All Regions – Monthly / Yearly";
}

function drawDefaultShopCharts() {
  const titleEl = document.getElementById("shopTitle");
  if (titleEl) titleEl.textContent = defaultShopTitle();
  // ship_to=ALL → API returns aggregate for the current filter scope.
  // No lat/lng so the visit overlay line is skipped (only meaningful per shop).
  drawShopCharts("ALL", null, null);
}

const BDE_MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun",
                          "Jul","Aug","Sep","Oct","Nov","Dec"];

// Metric → (data key on each BDE row, label, base RGB for heat colour).
const BDE_METRICS = {
  shops:   { key: "shops_by_month",         label: "Visited Shop",   rgb: [22, 163, 74]  },
  visits:  { key: "visit_days_by_month",    label: "Visits",         rgb: [37, 99, 235]  },
  novisit: { key: "no_visit_days_by_month", label: "No-Visit Days",  rgb: [220, 38, 38]  },
  logs:    { key: "logs_by_month",          label: "Logs",           rgb: [124, 58, 237] },  // violet
};

// Element-wise sum of 12-month arrays (used for subtotal / total rows).
function sumMonthly(rows, key) {
  const out = new Array(12).fill(0);
  for (const r of rows) {
    const a = r[key];
    if (!Array.isArray(a)) continue;
    for (let i = 0; i < 12; i++) out[i] += +a[i] || 0;
  }
  return out;
}

// Cached so the metric toggle can re-render without a fresh API call.
let _lastVisitSummary = null;
let _bdeMetric        = "shops";

// rgba string scaled to value/max → light shade for small values,
// near-opaque for the maximum.
function heatBg(rgb, v, max) {
  if (!v || max <= 0) return "transparent";
  const a = Math.min(1, 0.12 + 0.78 * (v / max));
  return `rgba(${rgb[0]},${rgb[1]},${rgb[2]},${a.toFixed(3)})`;
}

function renderBdeVisitTable(data) {
  // Allow the metric-toggle buttons to call renderBdeVisitTable() with no
  // args and re-render from the cached payload.
  if (data) _lastVisitSummary = data;
  else      data = _lastVisitSummary;

  const tbody  = document.querySelector("#bdeVisitTable tbody");
  const thead  = document.getElementById("bdeVisitHead");
  const totalsEl = document.getElementById("bdeVisitTotals");
  if (!tbody) return;

  const rawRows = (data && Array.isArray(data.by_bde)) ? data.by_bde : [];
  const rows = rawRows.filter(r => {
    const st  = (r.state || "").trim();
    const bde = (r.bde   || "").trim();
    if (!st  || st  === "-") return false;
    if (!bde || bde === "#N/A" || bde === "(unassigned)") return false;
    return true;
  });

  const metric    = BDE_METRICS[_bdeMetric] || BDE_METRICS.shops;
  const dataKey   = metric.key;
  const baseRGB   = metric.rgb;

  // Header: State | BDE | Shops | Total | Jan..Dec
  if (thead) {
    let h = `<tr style="background:#f0f0f0;">
      <th style="text-align:left;padding:4px 6px;border-bottom:1px solid #ccc;">State</th>
      <th style="text-align:left;padding:4px 6px;border-bottom:1px solid #ccc;">BDE</th>
      <th style="text-align:right;padding:4px 6px;border-bottom:1px solid #ccc;">Shops</th>
      <th style="text-align:right;padding:4px 4px;border-bottom:1px solid #ccc;">Total</th>`;
    for (const m of BDE_MONTH_LABELS) {
      h += `<th style="text-align:right;padding:4px 4px;border-bottom:1px solid #ccc;font-weight:500;">${m}</th>`;
    }
    h += "</tr>";
    thead.innerHTML = h;
  }

  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="16" style="padding:6px;color:#999;">No visit data</td></tr>';
    if (totalsEl) totalsEl.textContent = "";
    return;
  }

  // Group by state, preserving backend order (NSW→QLD→VIC→SA→WA→…).
  const buckets = [];
  const seen = new Map();
  for (const r of rows) {
    if (!seen.has(r.state)) {
      const b = { state: r.state, rows: [] };
      seen.set(r.state, b); buckets.push(b);
    }
    seen.get(r.state).rows.push(r);
  }
  const stateMonthly = buckets.map(b => sumMonthly(b.rows, dataKey));
  const grandMonthly = sumMonthly(rows, dataKey);

  // Max-scale across every visible monthly value (BDE rows + subtotals
  // + total) so heat shades are consistent within the table.
  const candidates = [
    ...rows.flatMap(r => (Array.isArray(r[dataKey]) ? r[dataKey] : [])),
    ...stateMonthly.flat(),
    ...grandMonthly,
  ];
  const maxVal = Math.max(1, ...candidates);

  const monthCell = (v, extra = "") => {
    const bg = heatBg(baseRGB, +v || 0, maxVal);
    return `<td style="text-align:right;padding:3px 4px;border-bottom:1px solid #eee;background:${bg};${extra}">`
         + `${v || ""}</td>`;
  };
  const td      = (v, extra = "") => `<td style="text-align:right;padding:3px 6px;border-bottom:1px solid #eee;${extra}">${v}</td>`;
  const tdLeft  = (v, extra = "") => `<td style="padding:3px 6px;border-bottom:1px solid #eee;${extra}">${v}</td>`;

  const subStyle = "background:#cbd5e1;font-weight:600;";
  const totStyle = "background:#94a3b8;font-weight:700;color:#fff;";
  const subTd     = (v, extra = "") => `<td style="text-align:right;padding:3px 6px;${subStyle}${extra}">${v}</td>`;
  const subTdLeft = (v, extra = "") => `<td style="padding:3px 6px;${subStyle}${extra}">${v}</td>`;
  const totTd     = (v, extra = "") => `<td style="text-align:right;padding:3px 6px;${totStyle}${extra}">${v}</td>`;
  const totTdLeft = (v, extra = "") => `<td style="padding:3px 6px;${totStyle}${extra}">${v}</td>`;

  // Total column = the single metric currently displayed (YTD).
  const totalKey = _bdeMetric === "shops"  ? "shops_visited"
                 : _bdeMetric === "visits" ? "visit_days"
                 : _bdeMetric === "logs"   ? "logs_total"
                 :                            "no_visit_days";

  let html = "";
  let gShopsAssigned = 0;
  let gTotal         = 0;
  buckets.forEach((b, bi) => {
    let sShopsAssigned = 0;
    let sTotal         = 0;
    for (const r of b.rows) {
      sShopsAssigned += +r.total_shops || 0;
      sTotal         += +r[totalKey]   || 0;
      const arr = Array.isArray(r[dataKey]) ? r[dataKey] : new Array(12).fill(0);
      html += `<tr>
        ${tdLeft(r.state || "-", "color:#666;")}
        ${tdLeft(r.bde)}
        ${td(r.total_shops)}
        ${td(r[totalKey] ?? 0)}
        ${arr.map(v => monthCell(v)).join("")}
      </tr>`;
    }
    html += `<tr>
      ${subTdLeft(b.state)}
      ${subTdLeft("")}
      ${subTd(sShopsAssigned)}
      ${subTd(sTotal)}
      ${stateMonthly[bi].map(v => {
        const bg = heatBg(baseRGB, v, maxVal);
        return `<td style="text-align:right;padding:3px 4px;${subStyle}background:${bg};">${v || ""}</td>`;
      }).join("")}
    </tr>`;
    gShopsAssigned += sShopsAssigned;
    gTotal         += sTotal;
  });
  html += `<tr>
    ${totTdLeft("All")}
    ${totTdLeft("Total")}
    ${totTd(gShopsAssigned)}
    ${totTd(gTotal)}
    ${grandMonthly.map(v => {
      const bg = heatBg(baseRGB, v, maxVal);
      return `<td style="text-align:right;padding:3px 4px;${totStyle}background:${bg};">${v || ""}</td>`;
    }).join("")}
  </tr>`;

  tbody.innerHTML = html;
  if (totalsEl) totalsEl.textContent = "";
}

// Wire up the metric-toggle buttons once the DOM is ready.
document.addEventListener("DOMContentLoaded", () => {
  const grp = document.getElementById("bdeMetricBtns");
  if (grp){
    grp.addEventListener("click", (e) => {
      const b = e.target.closest("button[data-m]");
      if (!b) return;
      _bdeMetric = b.dataset.m;
      grp.querySelectorAll("button").forEach(x => x.classList.toggle("active", x === b));
      renderBdeVisitTable();
    });
  }

  // Shop-status chip row (All / Logs / Visit / No-visit) — picks one
  // mutually-exclusive filter that re-renders the map without going
  // back to the API (filters cached data in loadSalesMap).
  const stat = document.getElementById("shopStatusBtns");
  if (stat){
    stat.addEventListener("click", (e) => {
      const b = e.target.closest("button[data-status]");
      if (!b) return;
      window._shopStatusFilter = b.dataset.status || "ALL";
      stat.querySelectorAll("button").forEach(x =>
        x.classList.toggle("active", x === b));
      if (typeof loadSalesMap === "function") loadSalesMap();
    });
  }

  // Purpose filter — same toggle pattern, drives passesPurpose().
  const purp = document.getElementById("purposeFilterBtns");
  if (purp){
    purp.addEventListener("click", (e) => {
      const b = e.target.closest("button[data-purpose]");
      if (!b) return;
      window._purposeFilter = b.dataset.purpose || "ALL";
      purp.querySelectorAll("button").forEach(x =>
        x.classList.toggle("active", x === b));
      if (typeof loadSalesMap === "function") loadSalesMap();
    });
  }
});

function monthlyMapOptions() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    plugins: {
      legend: { position: "right", align: "start",
                labels: { boxWidth: 12, boxHeight: 12, padding: 6, font: { size: 11 } },
                maxWidth: 140 },
      tooltip: { mode: "index", intersect: false }
    },
    scales: {
      x: { stacked: false },
      y: {
        position: "left",
        beginAtZero: true
      },
      y1: {
        position: "right",
        beginAtZero: true,
        grid: { drawOnChartArea: false },
        title: { display: true, text: "Visits", color: "#E91E63" },
        ticks: { color: "#E91E63", precision: 0 }
      }
    }
  };
}

function yearlyMapOptions() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { position: "right", align: "start",
                labels: { boxWidth: 12, boxHeight: 12, padding: 6, font: { size: 11 } },
                maxWidth: 140 },
      tooltip: { mode: "index", intersect: false }
    },
    scales: {
      x: { stacked: false },
      y: { beginAtZero: true }
    }
  };
}

async function drawShopCharts(shipToCode, shopLat, shopLng) {
  const params = new URLSearchParams({
    metric:        filters.metric,
    category:      filters.category,
    region:        filters.region,
    salesman:      filters.salesman,
    sold_to_group: filters.sold_to_group,
    sold_to:       filters.sold_to,
    ship_to:       shipToCode,
    product_group: filters.product_group,
    pattern:       filters.pattern
  });

  const params25 = new URLSearchParams(params); params25.set("year", "2025");
  const params26 = new URLSearchParams(params); params26.set("year", "2026");

  // visit params — only if we have a valid location
  const hasLoc = shopLat != null && shopLng != null;
  const visitParams26 = hasLoc
    ? new URLSearchParams({ lat: shopLat, lng: shopLng, year: "2026" })
    : null;

  const [sales25Rows, sales26Rows, target26Rows, yearlyRows,
         visit26Rows] = await Promise.all([
    fetchJSON("/api/monthly_sales?" + params25.toString()),
    fetchJSON("/api/monthly_sales?" + params26.toString()),
    fetchJSON("/api/monthly_target?" + params26.toString()),
    fetchJSON("/api/yearly_sales?" + params.toString()),
    visitParams26 ? fetchJSON("/api/monthly_visits?" + visitParams26.toString()) : Promise.resolve([]),
  ]);

  const monthLabels = ["Ja","Fe","Ma","Ap","Ma","Ju","Ju","Au","Se","Oc","No","De"];
  const sales25   = monthLabels.map((_, i) => Number((sales25Rows[i]?.value) || 0));
  const sales26   = monthLabels.map((_, i) => Number((sales26Rows[i]?.value) || 0));
  const targets26 = monthLabels.map((_, i) => Number((target26Rows[i]?.value) || 0));

  // Build visit arrays indexed by month (1-12 → index 0-11)
  const toVisitArr = rows => {
    const arr = new Array(12).fill(null);
    (rows || []).forEach(r => { if (r.m >= 1 && r.m <= 12) arr[r.m - 1] = r.visits; });
    return arr;
  };
  const visits26 = toVisitArr(visit26Rows);
  const hasVisits = visits26.some(v => v !== null);

  // Update popup total
  const total26 = sales26.reduce((a, b) => a + b, 0);
  const popupTotalEl = document.getElementById("popup-total");
  if (popupTotalEl) popupTotalEl.textContent = `Total (2026): ${total26.toLocaleString()}`;

  // monthly side chart
  if (shopMonthlyInst) shopMonthlyInst.destroy();
  const mCtx = document.getElementById("monthlyChart");
  if (mCtx) {
    const datasets = [
      {
        label: filters.metric === "amount" ? "Sales Amount (2025)" : "SalesQty (2025)",
        type: "bar",
        data: sales25,
        backgroundColor: "#7dc4a3",
        datalabels: { display: false }
      },
      {
        label: filters.metric === "amount" ? "Sales Amount (2026)" : "SalesQty (2026)",
        type: "bar",
        data: sales26,
        backgroundColor: "#5fbcd3",
        datalabels: { display: false }
      },
      {
        label: "Target (2026)",
        type: "bar",
        data: targets26,
        backgroundColor: "#4d8897",
        datalabels: { display: false }
      }
    ];

    if (hasVisits) {
      datasets.push({
        label: "Visit (2026)",
        type: "line",
        data: visits26,
        borderColor: "#E91E63",
        backgroundColor: "transparent",
        borderWidth: 2,
        pointRadius: 4,
        spanGaps: true,
        yAxisID: "y1",
        order: -1,
        datalabels: { display: false }
      });
    }

    shopMonthlyInst = new Chart(mCtx, {
      type: "bar",
      data: { labels: monthLabels, datasets },
      options: monthlyMapOptions()
    });
  }

  // yearly side chart
  if (shopYearlyInst) shopYearlyInst.destroy();
  const yCtx = document.getElementById("yearlyChart");
  if (yCtx) {
    const yLabels = yearlyRows.map(r => r.year);
    const yVals   = yearlyRows.map(r => Number(r.value || 0));

    shopYearlyInst = new Chart(yCtx, {
      type: "bar",
      data: {
        labels: yLabels,
        datasets: [
          {
            label: "Yearly Qty",
            data: yVals,
            backgroundColor: "#ABDEE6",
            categoryPercentage: 0.9,
            barPercentage: 0.9,
            datalabels: {
              display: true,
              align: "center",
              anchor: "center",
              formatter: v => v.toLocaleString()
            }
          }
        ]
      },
      options: yearlyMapOptions()
    });
  }
}

// On map page, initialise from saved filters and draw
document.addEventListener("DOMContentLoaded", async () => {
  const mapEl = document.getElementById("salesMap");
  if (!mapEl) return;  // not on /map

  // If there are saved filters from the graph view, merge them into filters
  try {
    const saved = JSON.parse(localStorage.getItem("salesFilters") || "{}");
    if (saved && typeof saved === "object") {
      Object.assign(filters, saved);
    }
  } catch(e) {
    // ignore parse error
  }

  // Wait for role-based locks before the first map fetch.  applyRoleScope
  // is idempotent — if app.js already ran it, this is a no-op.
  if (typeof applyRoleScope === "function") {
    try { await applyRoleScope(); } catch(e) {}
  }

  // Sync visible UI controls with the restored filter state so the user
  // can see what's currently active (chips/highlights/dropdown inputs).
  // Without this, filters carry over invisibly and the inputs look empty.
  const setInput = (id, val) => {
    const el = document.getElementById(id);
    if (!el) return;
    const v = (val == null || val === "ALL") ? "" : String(val);
    el.value = v;
    if (typeof ddUpdateActive === "function") ddUpdateActive(el);
  };
  setInput("sold_to",       filters.sold_to);
  setInput("ship_to",       filters.ship_to);
  setInput("product_group", filters.product_group);
  setInput("pattern",       filters.pattern);
  setInput("material",      filters.material);

  // Native <select> dropdowns (sold_to_group, salesman_name)
  const stgSel = document.getElementById("sold_to_group");
  if (stgSel) stgSel.value = filters.sold_to_group || "ALL";
  const smSel = document.getElementById("salesman_name");
  if (smSel) smSel.value = filters.salesman || "ALL";

  // Toggle-button groups (category / region / metric / top_limit)
  if (typeof setActive === "function") {
    setActive(document.getElementById("catBtns"),    "val",   filters.category || "ALL");
    setActive(document.getElementById("regionBtns"), "val",   filters.region   || "ALL");
    setActive(document.getElementById("metricBtns"), "val",   filters.metric   || "qty");
  }

  // Make top-customer buttons reflect current filters.top_limit
  const topCtl = document.getElementById("topCustomerControls");
  if (topCtl) {
    setActive(topCtl, "limit", String(filters.top_limit || 0));
  }

  // Category, metric, region buttons will already have listeners from app.js.
  // Just trigger one initial refresh.
  await loadSalesMap();
  if (salesMap) setTimeout(() => salesMap.invalidateSize(), 0);
});

