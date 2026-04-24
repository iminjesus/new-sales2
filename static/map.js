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

  const data = await fetchJSON(`/api/sales_map?${qs}`);
  if (!Array.isArray(data) || data.length === 0) {
    salesMap.setView([-25.0, 133.0], 4);
    return;
  }

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

  const marker = L.circleMarker([latNum, lngNum], {
    radius,
    color,
    fillColor: color,
    fillOpacity: 0.7,
    weight: 1
  });

  marker.bindPopup(
  `${shipTo} - ${shipNm}<br>` +
  `Region: ${regionVal || "-"}<br>` +
  `BDE: ${bde || "-"}<br>` +
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
}

function monthlyMapOptions() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    plugins: {
      legend: { position: "right" },
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
        ticks: { color: "#E91E63", callback: v => v + "회" }
      }
    }
  };
}

function yearlyMapOptions() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { position: "right" },
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
  const visitParams25 = hasLoc
    ? new URLSearchParams({ lat: shopLat, lng: shopLng, year: "2025" })
    : null;

  const [sales25Rows, sales26Rows, target26Rows, yearlyRows,
         visit26Rows, visit25Rows] = await Promise.all([
    fetchJSON("/api/monthly_sales?" + params25.toString()),
    fetchJSON("/api/monthly_sales?" + params26.toString()),
    fetchJSON("/api/monthly_target?" + params26.toString()),
    fetchJSON("/api/yearly_sales?" + params.toString()),
    visitParams26 ? fetchJSON("/api/monthly_visits?" + visitParams26.toString()) : Promise.resolve([]),
    visitParams25 ? fetchJSON("/api/monthly_visits?" + visitParams25.toString()) : Promise.resolve([]),
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
  const visits25 = toVisitArr(visit25Rows);
  const hasVisits = visits26.some(v => v !== null) || visits25.some(v => v !== null);

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
        label: "Visit (2025)",
        type: "line",
        data: visits25,
        borderColor: "#FF9800",
        backgroundColor: "transparent",
        borderWidth: 1.5,
        borderDash: [4, 3],
        pointRadius: 3,
        spanGaps: true,
        yAxisID: "y1",
        datalabels: { display: false }
      });
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

