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
    top_limit:     filters.top_limit || 0
  }).toString();

  const data = await fetchJSON(`/api/sales_map?${qs}`);
  if (!Array.isArray(data) || data.length === 0) {
    salesMap.setView([-25.0, 133.0], 4);
    return;
  }

  const points = [];

  data.forEach(row => {
    const lat = row.lat ?? row.latitude ?? row.Latitude;
    const lng = row.lng ?? row.longitude ?? row.Longitude;
    if (lat == null || lng == null) return;

    const total  = row.total_value ?? row.total ?? 0;
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
      `Total: ${Number(total || 0).toLocaleString()}`
    );

    marker.on("click", () => {
      const titleEl = document.getElementById("shopTitle");
      if (titleEl) {
        titleEl.textContent = (shipNm || shipTo) + " – Monthly / Yearly";
      }
      drawShopCharts(shipTo);
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
        ticks: { callback: v => v + "%" }
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

async function drawShopCharts(shipToCode) {
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

  const [salesRows, targetRows, yearlyRows] = await Promise.all([
    fetchJSON("/api/monthly_sales?" + params.toString()),
    fetchJSON("/api/monthly_target?" + params.toString()),
    fetchJSON("/api/yearly_sales?" + params.toString())
  ]);

  const monthLabels  = ["Ja","Fe","Ma","Ap","Ma","Ju","Ju","Au","Se","Oc","No","De"];
  const sales   = monthLabels.map((_, i) => Number((salesRows[i]?.value)  || 0));
  const targets = monthLabels.map((_, i) => Number((targetRows[i]?.value) || 0));
  const achieve = monthLabels.map((_, i) =>
    targets[i] > 0 ? (sales[i] / targets[i]) * 100 : 0
  );

  // monthly side chart
  if (shopMonthlyInst) shopMonthlyInst.destroy();
  const mCtx = document.getElementById("monthlyChart");
  if (mCtx) {
    shopMonthlyInst = new Chart(mCtx, {
      type: "bar",
      data: {
        labels: monthLabels,
        datasets: [
          {
            label: "Achievement(%)",
            type: "line",
            data: achieve,
            yAxisID: "y1",
            borderWidth: 2,
            pointRadius: 0,
            borderColor: "#ef4444",
            order: 99,
            datalabels: {
              display: true,
              align: "top",
              anchor: "end",
              formatter: v => (v == null ? "" : v.toFixed(1) + "%")
            }
          },
          {
            label: filters.metric === "amount" ? "Sales Amount" : "SalesQty",
            data: sales,
            backgroundColor: "#ABDEE6",
            categoryPercentage: 0.9,
            barPercentage: 0.9,
            datalabels: { display: false }
          },
          {
            label: "Target",
            type: "bar",
            data: targets,
            borderWidth: 2,
            borderColor: "#ABDEE6",
            datalabels: { display: false }
          }
        ]
      },
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
});
