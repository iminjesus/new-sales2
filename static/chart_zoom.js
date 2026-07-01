/* Chart enlarge-on-click
 *
 * Adds a small zoom button (top-right of every .box that contains a
 * <canvas>) — clicking it clones the Chart.js instance into a modal
 * overlay so dense stacked / monthly charts can be inspected at full
 * size.  Backdrop click + ESC close the modal.
 *
 * Robust to chart re-renders: a MutationObserver re-attaches the
 * button whenever a new canvas appears, and the existing
 * Chart.getChart() call always resolves the latest live instance.
 */
(function () {
  "use strict";

  const MODAL_ID         = "chartZoomModal";
  const MODAL_CANVAS_ID  = "chartZoomCanvas";
  let modalChartInst     = null;
  // Track which canvas the modal is mirroring so we can re-clone from
  // its updated state after the dashboard refreshes from a drill.
  let modalSourceCanvasId = null;
  // Enlarged view defaults to labels ON — the reader is here to see
  // exact values.  Click the Labels chip to toggle.  Sticky across
  // reopens so the user's last preference wins.
  let modalLabelsOn      = true;

  // ── helpers ────────────────────────────────────────────────────────
  function deepCloneSafe(value) {
    // Drop functions / non-serialisable bits.  We only need data +
    // visual options to re-render, not the live event handlers.
    try {
      return JSON.parse(JSON.stringify(value));
    } catch (_) {
      return undefined;
    }
  }

  function pickBoxTitle(boxEl) {
    // Use the <h3> sitting at the top of each .box as the modal title
    // when present (every chart box on the dashboard has one).
    const h = boxEl && boxEl.querySelector("h3");
    return h ? h.textContent.trim() : "";
  }

  // Build the datalabels plugin config we splice into every modal
  // chart.  Toggle chip in the header flips modalLabelsOn; when off
  // the plugin is inert (display: false), when on we show a compact
  // number above each data point regardless of the source chart's
  // own display rule.
  function _modalDatalabelsConfig() {
    if (!modalLabelsOn) return { display: false };
    return {
      display: (ctx) => {
        const v = ctx.dataset && ctx.dataset.data
          ? ctx.dataset.data[ctx.dataIndex] : null;
        if (v == null) return false;
        const n = typeof v === "object" ? (v.y ?? v.value ?? 0) : v;
        return Number(n) !== 0;
      },
      anchor: "end",
      align:  "top",
      offset: 2,
      color:  "#111827",
      font:   { size: 10, weight: 600 },
      formatter: (val) => {
        const n = typeof val === "object" ? (val.y ?? val.value ?? 0) : val;
        const num = Number(n);
        if (!isFinite(num) || num === 0) return "";
        if (Math.abs(num) >= 1000) return Math.round(num).toLocaleString();
        return (Math.round(num * 10) / 10).toString();
      },
    };
  }

  // ── modal open / close ─────────────────────────────────────────────
  function _renderModalFromChart(srcChart) {
    if (!srcChart) return;
    if (modalChartInst) {
      try { modalChartInst.destroy(); } catch (_) {}
      modalChartInst = null;
    }
    const ctx = document.getElementById(MODAL_CANVAS_ID);
    if (!ctx) return;

    // Deep-clone data so the modal chart doesn't share live references
    // with the underlying chart.  Options keep their function-typed
    // callbacks (onClick / onHover for drill, datalabel formatters,
    // …) so we reuse the source options object directly instead of
    // JSON-cloning it.
    const data = deepCloneSafe(srcChart.config.data) || srcChart.data;
    const baseOpts = srcChart.config.options || {};
    const opts = Object.assign({}, baseOpts, {
      responsive: true,
      maintainAspectRatio: false,
    });
    // The cross-chart legend onClick in app.js only makes sense on the
    // dashboard's monthly bars, not on this modal copy — drop it so
    // clicking a legend in the modal just toggles locally.
    // At the same time, override the datalabels plugin config so ALL
    // datasets (bars + lines) show their values by default when
    // enlarged — the enlarged view's whole point is legibility.  The
    // toggle chip in the header flips this on/off per session.
    opts.plugins = Object.assign({}, opts.plugins || {});
    if (opts.plugins.legend) {
      opts.plugins.legend = Object.assign({}, opts.plugins.legend, { onClick: undefined });
    }
    opts.plugins.datalabels = _modalDatalabelsConfig();
    // Some source charts also set per-dataset `datalabels` (either
    // { display:false } for cumulative overlays or a bespoke formatter
    // for lines).  Per-dataset config wins over chart-level in the
    // datalabels plugin, so we must override each dataset too or the
    // labels stay hidden on bars.  Preserve line-dataset styling
    // (they already look right) — only stomp on bar datasets.
    if (data && Array.isArray(data.datasets)) {
      data.datasets.forEach((ds) => {
        const dsType = ds.type || (srcChart.config.type);
        if (dsType === "bar") {
          ds.datalabels = _modalDatalabelsConfig();
        } else if (!modalLabelsOn) {
          // Labels chip switched off — hide line labels too so the
          // toggle affects the whole chart, not just bars.
          ds.datalabels = { display: false };
        }
      });
    }
    try {
      modalChartInst = new Chart(ctx, {
        type: srcChart.config.type,
        data: data,
        options: opts,
      });
    } catch (e) {
      console.warn("[chart-zoom] failed to clone chart:", e);
      closeModal();
    }
  }

  function openModal(srcChart, title) {
    const modal = document.getElementById(MODAL_ID);
    if (!modal || !srcChart) return;
    modal.style.display = "flex";

    const titleEl = modal.querySelector(".czm-title");
    if (titleEl) titleEl.textContent = title || "";
    // Reflect the current labels-on state on the toggle chip.
    const labelsBtn = modal.querySelector(".czm-labels");
    if (labelsBtn) labelsBtn.classList.toggle("active", modalLabelsOn);

    // Remember which canvas we cloned from so a drill firing inside
    // the modal can re-pull from the same source after the dashboard
    // refreshes.
    modalSourceCanvasId = srcChart.canvas && srcChart.canvas.id;
    _renderModalFromChart(srcChart);
  }

  function closeModal() {
    const modal = document.getElementById(MODAL_ID);
    if (modal) modal.style.display = "none";
    if (modalChartInst) {
      try { modalChartInst.destroy(); } catch (_) {}
      modalChartInst = null;
    }
    modalSourceCanvasId = null;
  }

  // Called by app.js after a drill / back so the modal mirrors the
  // dashboard's new state instead of getting stuck on stale data.
  // Retries a few times because chart redraw lags behind the SQL
  // round-trip (refreshAllDebounced fires ~250 ms after the action,
  // then each draw() takes a few ms more).
  function refreshModalFromSource(attempt) {
    if (!modalSourceCanvasId) return;
    const modal = document.getElementById(MODAL_ID);
    if (!modal || modal.style.display === "none") return;
    const canvas = document.getElementById(modalSourceCanvasId);
    const src = (canvas && typeof Chart !== "undefined") ? Chart.getChart(canvas) : null;
    if (!src) {
      // Source chart not yet re-created — try again shortly, up to 8x.
      if ((attempt || 0) < 8) {
        setTimeout(function(){ refreshModalFromSource((attempt || 0) + 1); }, 150);
      }
      return;
    }
    _renderModalFromChart(src);
  }
  window._refreshModalFromSource = refreshModalFromSource;

  // ── zoom-button attachment ─────────────────────────────────────────
  function attachZoomButton(boxEl) {
    if (!boxEl || boxEl.dataset.zoomBound) return;
    const canvas = boxEl.querySelector("canvas");
    if (!canvas) return;
    boxEl.dataset.zoomBound = "1";

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "chart-zoom-btn";
    btn.title = "Enlarge chart";
    btn.setAttribute("aria-label", "Enlarge chart");
    btn.textContent = "⤢";
    btn.addEventListener("click", function (e) {
      e.preventDefault();
      e.stopPropagation();
      const ch = (typeof Chart !== "undefined") ? Chart.getChart(canvas) : null;
      if (ch) openModal(ch, pickBoxTitle(boxEl));
    });
    boxEl.appendChild(btn);
  }

  // Drill-back button next to the zoom button.  CSS hides it unless
  // <body> carries the .drilled class — see app.js which toggles
  // that class whenever the drill stack changes.  Click delegates
  // to the global window._drillBack so any chart's back button does
  // the same "pop one level" action across the whole dashboard.
  function attachBackButton(boxEl) {
    if (!boxEl || boxEl.dataset.backBound) return;
    const canvas = boxEl.querySelector("canvas");
    if (!canvas) return;
    boxEl.dataset.backBound = "1";

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "chart-back-btn";
    btn.title = "Step back one level";
    btn.textContent = "← Back";
    btn.addEventListener("click", function (e) {
      e.preventDefault();
      e.stopPropagation();
      if (typeof window._drillBack === "function") window._drillBack();
    });
    boxEl.appendChild(btn);
  }

  function attachAll() {
    document.querySelectorAll(".box").forEach(function(boxEl){
      attachZoomButton(boxEl);
      attachBackButton(boxEl);
    });
  }

  // ── boot ───────────────────────────────────────────────────────────
  function boot() {
    attachAll();

    // Watch for new .box / canvas nodes added by later JS (KPI panels,
    // shop briefing slide-in, etc.) and bind them automatically.
    const obs = new MutationObserver(() => attachAll());
    obs.observe(document.body, { childList: true, subtree: true });

    // Backdrop click + close button + ESC dismiss the modal.
    // Inside-modal Back chip calls the same window._drillBack the
    // dashboard's chips use.
    const modal = document.getElementById(MODAL_ID);
    if (modal) {
      const backdrop = modal.querySelector(".czm-backdrop");
      const closeBtn = modal.querySelector(".czm-close");
      const backBtn  = modal.querySelector(".czm-back");
      const labelsBtn = modal.querySelector(".czm-labels");
      if (backdrop) backdrop.addEventListener("click", closeModal);
      if (closeBtn) closeBtn.addEventListener("click", closeModal);
      if (backBtn) backBtn.addEventListener("click", function(e){
        e.preventDefault();
        e.stopPropagation();
        if (typeof window._drillBack === "function") window._drillBack();
      });
      if (labelsBtn) labelsBtn.addEventListener("click", function(e){
        e.preventDefault();
        e.stopPropagation();
        modalLabelsOn = !modalLabelsOn;
        labelsBtn.classList.toggle("active", modalLabelsOn);
        // Live-update the current chart's plugin config AND each
        // dataset's own datalabels config (dataset-level wins over
        // chart-level in the plugin, so we can't skip this step).
        if (modalChartInst) {
          modalChartInst.options.plugins = modalChartInst.options.plugins || {};
          modalChartInst.options.plugins.datalabels = _modalDatalabelsConfig();
          const dsList = (modalChartInst.data && modalChartInst.data.datasets) || [];
          dsList.forEach((ds) => {
            const dsType = ds.type || modalChartInst.config.type;
            if (dsType === "bar") {
              ds.datalabels = _modalDatalabelsConfig();
            } else if (!modalLabelsOn) {
              ds.datalabels = { display: false };
            } else {
              // Turning labels back on: drop any explicit dataset
              // config so the chart-level plugin takes over again.
              delete ds.datalabels;
            }
          });
          modalChartInst.update();
        }
      });
    }
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") closeModal();
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
