import time
import pandas as pd
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright

URL = "https://app.hellosign.com/home/manage?status=bulk"

STATUS_WORDS = ["Pending", "Completed", "Declined", "Signed"]


def scrape_rows(page) -> List[Dict[str, str]]:
    # DOM 구조 의존 최소화: replied(3 / 6) + status가 함께 있는 작은 블록 파싱
    js = r"""
    () => {
      const statusWords = new Set(["Pending","Completed","Declined","Signed"]);
      const repliedRe = /\b\d+\s*\/\s*\d+\b/;

      const root = document.querySelector("main") || document.body;

      const candidates = Array.from(root.querySelectorAll("div, li, tr"))
        .filter(el => {
          const t = (el.innerText || "").trim();
          if (!t) return false;
          const lines = t.split("\n");
          if (lines.length > 25) return false;
          if (!repliedRe.test(t)) return false;
          for (const w of statusWords) if (t.includes(w)) return true;
          return false;
        });

      function parse(t) {
        const lines = t.split("\n").map(x => x.trim()).filter(Boolean);
        if (lines.length > 25) return null;

        let replied = "";
        let status = "";
        for (const l of lines) {
          if (!replied && repliedRe.test(l)) replied = l.match(repliedRe)[0];
          if (!status) {
            for (const w of statusWords) {
              if (l === w || l.includes(w)) { status = w; break; }
            }
          }
        }

        // name: replied/status/버튼 아닌 것 중 가장 긴 줄
        let best = "";
        for (const l of lines) {
          if (repliedRe.test(l)) continue;
          if (statusWords.has(l)) continue;
          if (l.toLowerCase() === "view progress") continue;
          if (l.length > best.length) best = l;
        }
        const name = best || (lines[0] || "");

        if (!name || !replied || !status) return null;
        return { Name: name, Replied: replied, Status: status };
      }

      const out = [];
      const seen = new Set();

      for (const el of candidates) {
        const row = parse((el.innerText || "").trim());
        if (!row) continue;
        const k = row.Name + "|" + row.Replied + "|" + row.Status;
        if (seen.has(k)) continue;
        seen.add(k);
        out.push(row);
      }

      return out;
    }
    """
    return page.evaluate(js)


def get_pagination_info(page):
    # pagination 컨테이너, active 페이지, 다음 클릭 대상 등을 브라우저에서 계산
    js = r"""
    () => {
      function visible(el){
        const r = el.getBoundingClientRect();
        return r.width > 0 && r.height > 0;
      }
      function isDisabled(el){
        if (!el) return true;
        const d = el.getAttribute("disabled");
        const ad = (el.getAttribute("aria-disabled")||"").toLowerCase();
        const cls = (el.getAttribute("class")||"").toLowerCase();
        return d !== null || ad === "true" || cls.includes("disabled");
      }

      const root = document.querySelector("main") || document.body;

      const clickables = Array.from(root.querySelectorAll("a,button")).filter(visible);

      const numBtns = clickables.filter(el => /^\d+$/.test((el.innerText||"").trim()));
      // 부모 후보 점수화: 숫자 버튼이 많이 모인 영역 = pagination일 확률 높음
      const score = new Map();
      for (const el of numBtns) {
        let p = el.parentElement;
        for (let i=0;i<6 && p;i++){
          score.set(p, (score.get(p)||0)+1);
          p = p.parentElement;
        }
      }

      let pager = null, best = 0;
      for (const [k,v] of score.entries()){
        if (v > best){ best = v; pager = k; }
      }

      // fallback: role navigation/nav/ul 중 화면 아래쪽
      if (!pager){
        const navs = Array.from(root.querySelectorAll("nav, ul, ol, [role='navigation']")).filter(visible);
        navs.sort((a,b) => b.getBoundingClientRect().top - a.getBoundingClientRect().top);
        pager = navs[0] || null;
      }

      if (!pager){
        return { ok:false, reason:"no pager container" };
      }

      const items = Array.from(pager.querySelectorAll("a,button")).filter(visible);
      const texts = items.map(el => (el.innerText||"").trim()).filter(t => t);

      // active 찾기
      let activeEl = null;
      for (const el of items){
        const t = (el.innerText||"").trim();
        if (!/^\d+$/.test(t)) continue;

        const aria = (el.getAttribute("aria-current")||"").trim();
        const cls = (el.getAttribute("class")||"").toLowerCase();
        if (aria === "page" || cls.includes("active") || cls.includes("selected") || cls.includes("current")){
          activeEl = el; break;
        }
      }

      // active가 안 잡히면: 숫자 중에서 스타일이 다른게 없을 수 있으니 1번을 active로 가정
      if (!activeEl){
        activeEl = items.find(el => /^\d+$/.test((el.innerText||"").trim())) || null;
      }

      const activeText = activeEl ? (activeEl.innerText||"").trim() : "";
      const activeNum = activeText && /^\d+$/.test(activeText) ? parseInt(activeText,10) : null;

      // 다음 숫자 버튼
      let nextNumEl = null;
      if (activeNum !== null){
        nextNumEl = items.find(el => (el.innerText||"").trim() === String(activeNum+1)) || null;
      }

      // 다음 화살표(텍스트 없는 svg일 수 있어 맨 마지막 컨트롤도 후보)
      const nextAria = items.find(el => ((el.getAttribute("aria-label")||"").toLowerCase().includes("next"))) || null;
      const nextText = items.find(el => {
        const t = (el.innerText||"").trim();
        return t === ">" || t === "›" || t === "→" || t.toLowerCase() === "next";
      }) || null;
      const lastCtrl = items.length ? items[items.length-1] : null;

      return {
        ok:true,
        activeNum,
        texts,
        hasNextNum: !!nextNumEl,
        hasNextAria: !!nextAria,
        hasNextText: !!nextText,
        lastCtrlText: lastCtrl ? ((lastCtrl.innerText||"").trim()) : "",
        nextNumDisabled: nextNumEl ? isDisabled(nextNumEl) : null,
        nextAriaDisabled: nextAria ? isDisabled(nextAria) : null,
        nextTextDisabled: nextText ? isDisabled(nextText) : null,
        lastCtrlDisabled: lastCtrl ? isDisabled(lastCtrl) : null
      };
    }
    """
    return page.evaluate(js)


def click_next(page) -> bool:
    # 1) 아래로 내려 pagination 노출
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(400)

    before = get_pagination_info(page)
    if not before.get("ok"):
        return False

    active_before = before.get("activeNum")

    # 2) 브라우저 안에서 “active+1 숫자 클릭” 우선, 그 다음 next aria/text, 마지막 컨트롤
    js_click = r"""
    () => {
      function visible(el){
        const r = el.getBoundingClientRect();
        return r.width > 0 && r.height > 0;
      }
      function isDisabled(el){
        if (!el) return true;
        const d = el.getAttribute("disabled");
        const ad = (el.getAttribute("aria-disabled")||"").toLowerCase();
        const cls = (el.getAttribute("class")||"").toLowerCase();
        return d !== null || ad === "true" || cls.includes("disabled");
      }

      const root = document.querySelector("main") || document.body;

      // pager 찾기 (get_pagination_info와 유사)
      const clickables = Array.from(root.querySelectorAll("a,button")).filter(visible);
      const numBtns = clickables.filter(el => /^\d+$/.test((el.innerText||"").trim()));
      const score = new Map();
      for (const el of numBtns) {
        let p = el.parentElement;
        for (let i=0;i<6 && p;i++){
          score.set(p, (score.get(p)||0)+1);
          p = p.parentElement;
        }
      }
      let pager = null, best = 0;
      for (const [k,v] of score.entries()){
        if (v > best){ best = v; pager = k; }
      }
      if (!pager){
        const navs = Array.from(root.querySelectorAll("nav, ul, ol, [role='navigation']")).filter(visible);
        navs.sort((a,b) => b.getBoundingClientRect().top - a.getBoundingClientRect().top);
        pager = navs[0] || null;
      }
      if (!pager) return {ok:false, why:"no pager"};

      const items = Array.from(pager.querySelectorAll("a,button")).filter(visible);

      // active 찾기
      let activeEl = null;
      for (const el of items){
        const t = (el.innerText||"").trim();
        if (!/^\d+$/.test(t)) continue;
        const aria = (el.getAttribute("aria-current")||"").trim();
        const cls = (el.getAttribute("class")||"").toLowerCase();
        if (aria === "page" || cls.includes("active") || cls.includes("selected") || cls.includes("current")){
          activeEl = el; break;
        }
      }
      if (!activeEl){
        activeEl = items.find(el => /^\d+$/.test((el.innerText||"").trim())) || null;
      }
      const activeText = activeEl ? (activeEl.innerText||"").trim() : "";
      const activeNum = (activeText && /^\d+$/.test(activeText)) ? parseInt(activeText,10) : null;

      // 1) active+1 숫자
      if (activeNum !== null){
        const target = items.find(el => (el.innerText||"").trim() === String(activeNum+1));
        if (target && !isDisabled(target)) { target.click(); return {ok:true, why:"clicked next number"}; }
      }

      // 2) aria-label next
      const nextAria = items.find(el => ((el.getAttribute("aria-label")||"").toLowerCase().includes("next")));
      if (nextAria && !isDisabled(nextAria)) { nextAria.click(); return {ok:true, why:"clicked aria next"}; }

      // 3) text next
      const nextText = items.find(el => {
        const t = (el.innerText||"").trim();
        return t === ">" || t === "›" || t === "→" || t.toLowerCase() === "next";
      });
      if (nextText && !isDisabled(nextText)) { nextText.click(); return {ok:true, why:"clicked text next"}; }

      // 4) last control (svg 아이콘일 수 있음)
      const last = items.length ? items[items.length-1] : null;
      if (last && !isDisabled(last)) { last.click(); return {ok:true, why:"clicked last control"}; }

      return {ok:false, why:"next disabled or not found"};
    }
    """

    res = page.evaluate(js_click)
    if not res or not res.get("ok"):
        return False

    # 3) 실제로 active 페이지가 바뀔 때까지 대기
    end = time.time() + 10
    while time.time() < end:
        page.wait_for_timeout(250)
        after = get_pagination_info(page)
        if after.get("ok"):
            active_after = after.get("activeNum")
            if active_before is not None and active_after is not None and active_after != active_before:
                return True

    # active를 못 찾는 UI라면, URL/텍스트 변화로 보조 판단
    # 그래도 실패면 False 반환 (디버그 저장하도록)
    return False


def save_debug(page, prefix="debug"):
    ts = int(time.time())
    png = f"{prefix}_{ts}.png"
    html = f"{prefix}_{ts}.html"
    try:
        page.screenshot(path=png, full_page=True)
    except:
        pass
    try:
        with open(html, "w", encoding="utf-8") as f:
            f.write(page.content())
    except:
        pass
    return png, html


def main():
    all_rows: List[Dict[str, str]] = []

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir="C:/temp/pw_dropboxsign_profile",
            headless=False
        )
        page = context.new_page()
        page.goto(URL, wait_until="domcontentloaded", timeout=60000)

        print("브라우저 열림.")
        print("로그인 후 Bulk send 목록이 보이면 이 터미널로 돌아와 Enter.")
        input()

        # page 1 수집
        page_no = 1
        while True:
            rows = scrape_rows(page)
            if rows:
                all_rows.extend(rows)
                print(f"page {page_no}: +{len(rows)} rows (total {len(all_rows)})")
            else:
                print(f"page {page_no}: 0 rows")

            info = get_pagination_info(page)
            if info.get("ok"):
                print("pagination texts:", info.get("texts")[:30], "active:", info.get("activeNum"))
            else:
                print("pagination info:", info)

            moved = click_next(page)
            if not moved:
                print("끝: 다음 페이지 이동 실패")
                png, html = save_debug(page, prefix="dropboxsign_next_fail")
                print("debug saved:", png, html)
                break

            page_no += 1
            # 로딩 안정화
            page.wait_for_timeout(800)

        df = pd.DataFrame(all_rows).drop_duplicates()
        df.to_csv("bulk.csv", index=False, encoding="utf-8-sig")
        print(f"완료: bulk.csv 생성됨 (총 {len(df)} rows)")
        context.close()


if __name__ == "__main__":
    main()
