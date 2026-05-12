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

  const points = [];

  data.forEach(row => {
  // DEBUG: log first row once
  // (you'll see target_value and total_value in the console)
  if (!window.__loggedSalesMapRow) {
    console.log("sales_map row:", row);
    window.__loggedSalesMapRow = true;
  }

  const lat = row.lat ?? row.latitude ?? row.Latitude;
  const lng = row.lng ?? row.longitude ?? row.Longitude;
  if (lat == null || lng == null) return;

  // we KNOW from /api/sales_map that keys are total_value and target_value
  const total  = Number(row.total_value || 0);
  const target = Number(row.target_value || 0);
  const ach    = target > 0 ? (total / target) * 100 : 0;

  const radius = 4 + Math.log10(total + 1) * 3;

  const regionVal = row.region ?? row.Region;
  const shipTo = row.ship_to ?? row.Ship_To ?? "";
  const shipNm = row.ship_to_name ?? row.Ship_To_Name ?? "";
  const bde    = row.bde ?? row.BDE ?? row.BDE_Name ?? "";

  const color = getBdeColor(bde);

  const latNum = +lat;
  const lngNum = +lng;

  const isVisited = visitedSet.has(shipTo);
  const marker = L.circleMarker([latNum, lngNum], {
    radius,
    color,
    fillColor: color,
    fillOpacity: isVisited ? 0.85 : 0.4,
    opacity:    isVisited ? 1.0  : 0.85,
    weight:     isVisited ? 1.5  : 1.0
  });

  const visitCount = visitsByShipTo[shipTo] || 0;
  marker.bindPopup(
  `${shipTo} - ${shipNm}<br>` +
  `Region: ${regionVal || "-"}<br>` +
  `BDE: ${bde || "-"}<br>` +
  `Total Visits (2026): ${visitCount}<br>` +
  `<span id="popup-total">Total (2026): …</span>`
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


  if (points.length === 1) {
    salesMap.setView(points[0], 10);
  } else if (points.length > 1) {
    const bounds = L.latLngBounds(points);
    salesMap.fitBounds(bounds.pad(0.1));
  } else {
    salesMap.setView([-25.0, 133.0], 4);
  }

  renderBdeVisitTable(summary);
}

function renderBdeVisitTable(data) {
  const tbody = document.querySelector("#bdeVisitTable tbody");
  const totalsEl = document.getElementById("bdeVisitTotals");
  if (!tbody) return;
  const rows = (data && Array.isArray(data.by_bde)) ? data.by_bde : [];
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="6" style="padding:6px;color:#999;">No visit data</td></tr>';
    if (totalsEl) totalsEl.textContent = "";
    return;
  }

  // Group rows by state, in the same order the rows arrive (backend
  // already sorts NSW → QLD → VIC → SA → WA → others), so the table
  // visually keeps that order while we inject per-state subtotal rows.
  const buckets = [];   // [{state, rows: [...]}, ...]
  const seen = new Map();
  for (const r of rows) {
    const st = r.state || "-";
    if (!seen.has(st)) {
      const b = { state: st, rows: [] };
      seen.set(st, b);
      buckets.push(b);
    }
    seen.get(st).rows.push(r);
  }

  const td       = (v, extra = "") => `<td style="text-align:right;padding:3px 6px;border-bottom:1px solid #eee;${extra}">${v}</td>`;
  const tdLeft   = (v, extra = "") => `<td style="padding:3px 6px;border-bottom:1px solid #eee;${extra}">${v}</td>`;
  const tdRedRight = (v) => td(v, "color:#c00;");

  // Per-BDE row vs subtotal/total row use the same column layout but
  // subtotals/total get a darker background for visual separation.
  const subStyle  = "background:#cbd5e1;font-weight:600;";   // slate-300
  const totStyle  = "background:#94a3b8;font-weight:700;color:#fff;"; // slate-400
  const subTd     = (v, extra = "") => `<td style="text-align:right;padding:3px 6px;${subStyle}${extra}">${v}</td>`;
  const subTdLeft = (v, extra = "") => `<td style="padding:3px 6px;${subStyle}${extra}">${v}</td>`;
  const totTd     = (v, extra = "") => `<td style="text-align:right;padding:3px 6px;${totStyle}${extra}">${v}</td>`;
  const totTdLeft = (v, extra = "") => `<td style="padding:3px 6px;${totStyle}${extra}">${v}</td>`;

  let html = "";
  let gShops = 0, gVisitedShops = 0, gVisits = 0;
  for (const b of buckets) {
    let sShops = 0, sVisitedShops = 0, sVisits = 0;
    for (const r of b.rows) {
      sShops        += +r.total_shops   || 0;
      sVisitedShops += +r.shops_visited || 0;
      sVisits       += +r.visit_days    || 0;
      html += `<tr>
        ${tdLeft(r.state || "-", "color:#666;")}
        ${tdLeft(r.bde)}
        ${td(r.total_shops)}
        ${td(r.shops_visited)}
        ${td(r.visit_days)}
        ${tdRedRight(r.no_visit_days ?? "-")}
      </tr>`;
    }
    // No-Visit Days isn't summable across BDEs (each BDE has their own
    // denominator of active days), so the subtotal/total row leaves it
    // blank rather than show a misleading number.
    html += `<tr>
      ${subTdLeft(b.state)}
      ${subTdLeft("")}
      ${subTd(sShops)}
      ${subTd(sVisitedShops)}
      ${subTd(sVisits)}
      ${subTd("-")}
    </tr>`;
    gShops        += sShops;
    gVisitedShops += sVisitedShops;
    gVisits       += sVisits;
  }
  html += `<tr>
    ${totTdLeft("All")}
    ${totTdLeft("Total")}
    ${totTd(gShops)}
    ${totTd(gVisitedShops)}
    ${totTd(gVisits)}
    ${totTd("-")}
  </tr>`;

  tbody.innerHTML = html;
  if (totalsEl) totalsEl.textContent = "";
}

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

