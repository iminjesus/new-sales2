/* Column resizer shared by the SPRF pages (Submitted Orders list and
   the SPRF form).

   makeTableResizable(table, opts)
     - Adds a drag handle to the right edge of every header cell.
     - Drag to widen / narrow.  The table grows wider than its parent
       when needed, so the parent should have overflow-x:auto.
     - Double-click a handle: auto-fit (input cells) or reset that
       column to its default width.
     - Widths are saved per browser (localStorage) and restored on
       the next visit.  resetTableColumnWidths(table) clears them.

   opts:
     storageKey  localStorage key suffix (default: path + table id)
     minWidth    smallest width a column can be dragged to (px, default 36)
     filler      data-col / header text of a column that soaks up any
                 spare width so the table always reaches its parent's
                 right edge (optional)

   Widths are written into a generated <style> under @media screen, so
   print stylesheets keep full control of the printed layout. */
(function () {
  "use strict";

  const BASE_CSS = `
@media screen {
  .crz-th { position: relative; }
  .crz-handle {
    position: absolute; top: 0; right: 0; width: 8px; height: 100%;
    cursor: col-resize; user-select: none; touch-action: none; z-index: 3;
  }
  .crz-handle::after {
    content: ""; position: absolute; top: 20%; bottom: 20%; right: 0;
    width: 2px; border-radius: 1px; background: rgba(255,255,255,0.28);
  }
  .crz-handle:hover::after, .crz-handle.dragging::after {
    top: 0; bottom: 0; background: #38bdf8;
  }
  body.crz-dragging, body.crz-dragging * { cursor: col-resize !important; user-select: none !important; }
}
@media print { .crz-handle { display: none !important; } }`;

  function injectBaseCss() {
    if (document.getElementById("crz-base-css")) return;
    const s = document.createElement("style");
    s.id = "crz-base-css";
    s.textContent = BASE_CSS;
    document.head.appendChild(s);
  }

  function colKey(th, i) {
    return th.dataset.col
      || (th.textContent || "").replace(/\s+/g, " ").trim()
      || ("col" + i);
  }

  function readStore(key) {
    try { return JSON.parse(localStorage.getItem(key) || "{}") || {}; }
    catch (_) { return {}; }
  }
  function writeStore(key, obj) {
    try { localStorage.setItem(key, JSON.stringify(obj)); } catch (_) {}
  }

  let _canvas = null;
  function textWidth(text, font) {
    _canvas = _canvas || document.createElement("canvas");
    const ctx = _canvas.getContext("2d");
    ctx.font = font;
    return ctx.measureText(text || "").width;
  }

  function makeTableResizable(table, opts) {
    opts = opts || {};
    if (!table || !table.tHead || table.__crz) return;
    injectBaseCss();

    if (!table.id) table.id = "crz_" + Math.random().toString(36).slice(2, 9);
    const storeKey = "colw:" + (opts.storageKey || (location.pathname + "#" + table.id));
    const minW = opts.minWidth || 36;
    const ths = Array.from(table.tHead.rows[0].cells);
    const keys = ths.map(colKey);
    const fillerIdx = opts.filler ? keys.indexOf(opts.filler) : -1;

    // Default width = inline px width if declared, else the rendered width.
    const defaults = ths.map(th => {
      const inline = parseFloat(th.style.width);
      if (th.style.width.endsWith("px") && inline > 0) return inline;
      const r = th.getBoundingClientRect().width;
      if (r > 0) return Math.round(r);
      const cs = parseFloat(getComputedStyle(th).width);
      return cs > 0 ? cs : 80;
    });
    if (fillerIdx >= 0) {
      const fMin = parseFloat(getComputedStyle(ths[fillerIdx]).minWidth);
      defaults[fillerIdx] = fMin > 0 ? fMin : 140;
    }

    const saved = readStore(storeKey);
    const widths = ths.map((th, i) => {
      const v = Number(saved[keys[i]]);
      return v >= minW ? v : defaults[i];
    });

    const styleEl = document.createElement("style");
    styleEl.dataset.crzFor = table.id;
    document.head.appendChild(styleEl);

    function render() {
      const sel = "#" + CSS.escape(table.id);
      // Hidden columns (display:none) don't take space.
      const total = widths.reduce((s, w, i) =>
        s + (getComputedStyle(ths[i]).display === "none" ? 0 : w), 0);
      let css = "@media screen {\n";
      css += `${sel}{table-layout:fixed !important;width:${Math.ceil(total)}px !important;max-width:none !important;`
           + (fillerIdx >= 0 ? "min-width:100% !important;" : "min-width:0 !important;") + "}\n";
      widths.forEach((w, i) => {
        const cell = `${sel} > thead > tr:first-child > th:nth-child(${i + 1})`;
        if (i === fillerIdx) {
          css += `${cell}{width:auto !important;min-width:${w}px !important;max-width:none !important;}\n`;
        } else {
          css += `${cell}{width:${w}px !important;min-width:${w}px !important;max-width:${w}px !important;}\n`;
        }
      });
      css += "}";
      styleEl.textContent = css;
    }

    function save() {
      const out = {};
      widths.forEach((w, i) => { if (w !== defaults[i]) out[keys[i]] = Math.round(w); });
      writeStore(storeKey, out);
    }

    function autoFit(i) {
      // Only meaningful for input-based cells (SPRF form).  Everything
      // else resets to the column's default width.
      let widest = 0, sawInput = false;
      Array.from(table.tBodies).forEach(tb => Array.from(tb.rows).forEach(tr => {
        if (tr.cells.length !== ths.length) return;
        const inp = tr.cells[i] && tr.cells[i].querySelector("input");
        if (!inp) return;
        sawInput = true;
        const cs = getComputedStyle(inp);
        const pad = parseFloat(cs.paddingLeft) + parseFloat(cs.paddingRight);
        widest = Math.max(widest, textWidth(inp.value, cs.font) + pad + 6);
      }));
      if (!sawInput) return defaults[i];
      return Math.max(defaults[i] * 0.6, minW, Math.min(400, Math.ceil(widest)));
    }

    ths.forEach((th, i) => {
      th.classList.add("crz-th");
      th.querySelectorAll(".col-resizer, .crz-handle").forEach(n => n.remove());
      const h = document.createElement("div");
      h.className = "crz-handle";
      h.title = "Drag to resize · double-click to fit / reset";
      th.appendChild(h);

      h.addEventListener("click", e => e.stopPropagation());
      h.addEventListener("pointerdown", e => {
        if (e.button !== 0) return;
        e.preventDefault(); e.stopPropagation();
        const startX = e.clientX;
        const startW = i === fillerIdx ? th.getBoundingClientRect().width : widths[i];
        h.setPointerCapture(e.pointerId);
        h.classList.add("dragging");
        document.body.classList.add("crz-dragging");
        const onMove = mv => {
          widths[i] = Math.max(minW, Math.round(startW + (mv.clientX - startX)));
          render();
        };
        const onUp = () => {
          h.releasePointerCapture(e.pointerId);
          h.classList.remove("dragging");
          document.body.classList.remove("crz-dragging");
          h.removeEventListener("pointermove", onMove);
          h.removeEventListener("pointerup", onUp);
          h.removeEventListener("pointercancel", onUp);
          save();
        };
        h.addEventListener("pointermove", onMove);
        h.addEventListener("pointerup", onUp);
        h.addEventListener("pointercancel", onUp);
      });
      h.addEventListener("dblclick", e => {
        e.preventDefault(); e.stopPropagation();
        widths[i] = autoFit(i);
        render(); save();
      });
    });

    table.__crz = {
      reset() {
        defaults.forEach((w, i) => { widths[i] = w; });
        try { localStorage.removeItem(storeKey); } catch (_) {}
        render();
      },
      refresh: render,
    };
    render();
  }

  function resetTableColumnWidths(table) {
    if (table && table.__crz) table.__crz.reset();
  }

  window.makeTableResizable = makeTableResizable;
  window.resetTableColumnWidths = resetTableColumnWidths;
})();
