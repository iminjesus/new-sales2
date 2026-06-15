// Shop Briefing slide-in popup — opens /shop/<ship_to>?embed=1 in an
// iframe panel docked to the right of the page so a user can keep
// their meeting log / rebate context visible while inspecting one
// shop's briefing card.  Reuses the standalone /shop/ page wholesale
// (no duplicate render code).
(function(){
  // Build the panel DOM lazily on first call so pages that never
  // open a briefing don't pay for the styles or markup.
  let _panel, _frame, _backdrop;

  function _build(){
    if (_panel) return;

    const css = document.createElement("style");
    css.textContent = `
      .sbp-backdrop{position:fixed;inset:0;background:rgba(15,23,42,.35);
        z-index:99998;opacity:0;transition:opacity .18s ease;
        pointer-events:none;backdrop-filter:blur(1px)}
      .sbp-backdrop.open{opacity:1;pointer-events:auto}
      .sbp-panel{position:fixed;top:0;right:0;height:100vh;width:460px;
        max-width:96vw;background:#fff;z-index:99999;
        box-shadow:-4px 0 20px rgba(15,23,42,.18);
        transform:translateX(100%);transition:transform .22s ease;
        display:flex;flex-direction:column}
      .sbp-panel.open{transform:translateX(0)}
      .sbp-head{padding:9px 14px;background:#1e3a5f;color:#fff;
        display:flex;align-items:center;gap:10px;
        font-size:13px;font-weight:700}
      .sbp-head .sbp-title{flex:1;overflow:hidden;text-overflow:ellipsis;
        white-space:nowrap;font-size:12px;font-weight:600}
      .sbp-head .sbp-open-tab{color:#cbd5e1;text-decoration:none;font-size:11px;
        font-weight:500}
      .sbp-head .sbp-open-tab:hover{color:#fff}
      .sbp-head .sbp-close{background:transparent;color:#fff;border:none;
        font-size:20px;line-height:1;padding:2px 6px;cursor:pointer;
        border-radius:4px}
      .sbp-head .sbp-close:hover{background:rgba(255,255,255,.15)}
      .sbp-body{flex:1;border:none;width:100%;background:#f3f4f6}
      @media (max-width:640px){
        .sbp-panel{width:100vw;max-width:100vw}
      }
    `;
    document.head.appendChild(css);

    _backdrop = document.createElement("div");
    _backdrop.className = "sbp-backdrop";
    _backdrop.addEventListener("click", closeShopBriefing);
    document.body.appendChild(_backdrop);

    _panel = document.createElement("div");
    _panel.className = "sbp-panel";
    _panel.innerHTML = `
      <div class="sbp-head">
        <span class="sbp-title" id="sbpTitle">Shop briefing</span>
        <a class="sbp-open-tab" id="sbpOpenTab" target="_blank"
           title="Open in new tab">↗ tab</a>
        <button class="sbp-close" type="button"
                aria-label="Close" title="Close (Esc)">×</button>
      </div>
      <iframe class="sbp-body" id="sbpFrame"
              loading="lazy" referrerpolicy="same-origin"></iframe>
    `;
    document.body.appendChild(_panel);
    _frame = _panel.querySelector("#sbpFrame");
    _panel.querySelector(".sbp-close").addEventListener("click", closeShopBriefing);

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && _panel.classList.contains("open")) {
        closeShopBriefing();
      }
    });
  }

  window.openShopBriefing = function(shipTo, shipName){
    if (!shipTo) return;
    _build();
    const title = document.getElementById("sbpTitle");
    title.textContent = shipName
      ? `${shipName}  ·  #${shipTo}`
      : `Shop briefing  ·  #${shipTo}`;
    const tabLink = document.getElementById("sbpOpenTab");
    tabLink.href = "/shop/" + encodeURIComponent(shipTo);
    // ?embed=1 → briefing page hides its toolbar so it sits flush
    // inside the panel.
    _frame.src = "/shop/" + encodeURIComponent(shipTo) + "?embed=1";
    requestAnimationFrame(() => {
      _panel.classList.add("open");
      _backdrop.classList.add("open");
    });
  };

  window.closeShopBriefing = function(){
    if (!_panel) return;
    _panel.classList.remove("open");
    _backdrop.classList.remove("open");
    // Clear iframe so background fetches stop and a fresh load
    // happens on next open (don't leak prior shop's state).
    setTimeout(() => { if (_frame) _frame.src = "about:blank"; }, 240);
  };
})();
