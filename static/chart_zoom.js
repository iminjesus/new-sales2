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
  function openModal(srcChart, title) {
    const modal = document.getElementById(MODAL_ID);
    if (!modal || !srcChart) return;
    modal.style.display = "flex";

    // Title — picked from the source box's <h3> when present.
    const titleEl = modal.querySelector(".czm-title");
    if (titleEl) titleEl.textContent = title || "";

    // Destroy any previous modal chart before re-rendering.
    if (modalChartInst) {
      try { modalChartInst.destroy(); } catch (_) {}
      modalChartInst = null;
    }

    const ctx = document.getElementById(MODAL_CANVAS_ID);
    if (!ctx) return;

    // Deep-clone data + options so the modal chart doesn't share live
    // references with the underlying chart (a click on the modal's
    // legend shouldn't mutate the dashboard's hidden flags).  We do
    // re-use the chart `type` directly since it's a string.
    const data = deepCloneSafe(srcChart.config.data) || srcChart.data;
    const opts = deepCloneSafe(srcChart.config.options) || {};

    // Force responsive + non-aspect-ratio so the chart fills the modal.
    opts.responsive = true;
    opts.maintainAspectRatio = false;
    if (opts.plugins && opts.plugins.legend) {
      // The cross-chart legend onClick in app.js loses meaning here
      // (modal chart isn't on the dashboard), so reset to Chart.js
      // default which just toggles the local dataset.
      delete opts.plugins.legend.onClick;
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

  function closeModal() {
    const modal = document.getElementById(MODAL_ID);
    if (modal) modal.style.display = "none";
    if (modalChartInst) {
      try { modalChartInst.destroy(); } catch (_) {}
      modalChartInst = null;
    }
  }

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
      // Late-resolved so the latest instance always opens, even when
      // refreshAllWithKpi() destroyed and re-created the chart.
      const ch = (typeof Chart !== "undefined") ? Chart.getChart(canvas) : null;
      if (ch) openModal(ch, pickBoxTitle(boxEl));
    });
    boxEl.appendChild(btn);
  }

  function attachAll() {
    document.querySelectorAll(".box").forEach(attachZoomButton);
  }

  // ── boot ───────────────────────────────────────────────────────────
  function boot() {
    attachAll();

    // Watch for new .box / canvas nodes added by later JS (KPI panels,
    // shop briefing slide-in, etc.) and bind them automatically.
    const obs = new MutationObserver(() => attachAll());
    obs.observe(document.body, { childList: true, subtree: true });

    // Backdrop click + close button + ESC dismiss the modal.
    const modal = document.getElementById(MODAL_ID);
    if (modal) {
      const backdrop = modal.querySelector(".czm-backdrop");
      const closeBtn = modal.querySelector(".czm-close");
      if (backdrop) backdrop.addEventListener("click", closeModal);
      if (closeBtn) closeBtn.addEventListener("click", closeModal);
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
