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
    if (opts.plugins && opts.plugins.legend) {
      opts.plugins = Object.assign({}, opts.plugins, {
        legend: Object.assign({}, opts.plugins.legend, { onClick: undefined }),
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
      if (backdrop) backdrop.addEventListener("click", closeModal);
      if (closeBtn) closeBtn.addEventListener("click", closeModal);
      if (backBtn) backBtn.addEventListener("click", function(e){
        e.preventDefault();
        e.stopPropagation();
        if (typeof window._drillBack === "function") window._drillBack();
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
