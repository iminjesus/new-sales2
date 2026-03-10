import re
import time
import pandas as pd
from typing import List, Dict
from playwright.sync_api import sync_playwright

URL_ALL = "https://app.hellosign.com/home/manage?status=all"

STATUS_PHRASES = [
    "Pending signature",
    "Completed",
    "Declined",
    "Canceled",
    "Expired",
    "Draft",
]

STATUS_RE = re.compile("|".join(re.escape(s) for s in STATUS_PHRASES), re.IGNORECASE)

NOISE_LINES = {
    "name", "status", "pending", "updated",   # 헤더
    "email reminder", "view progress", "view details",
}

def force_all(page):
    # 로그인 후 어디로 튕기든, all로 강제 진입 + Your documents 탭 클릭 시도
    page.goto(URL_ALL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1200)

    # 탭이 있으면 눌러주기 (Bulk send 탭에 머무는 경우 방지)
    for _ in range(2):
        try:
            page.get_by_text("Your documents", exact=False).click(timeout=1500)
            page.wait_for_timeout(900)
            break
        except:
            page.wait_for_timeout(400)

def scrape_name_status_by_role_rows(page) -> List[Dict[str, str]]:
    # 화면에 status 문구가 하나라도 보일 때까지 대기 (로딩 느리면 여기서 기다림)
    try:
        page.wait_for_selector(
            "text=/Pending signature|Completed|Declined|Canceled|Expired|Draft/i",
            timeout=20000
        )
    except:
        # 못 찾으면 그냥 빈 리스트 반환 (디버그는 main에서 저장)
        return []

    out: List[Dict[str, str]] = []
    seen = set()

    # 표든 div-grid든, 보통 row role이 잡힘
    rows = page.get_by_role("row").all()

    for r in rows:
        try:
            t = r.inner_text(timeout=1000).strip()
        except:
            continue

        if not t:
            continue

        # 헤더 row 스킵
        tl = t.lower()
        if "name" in tl and "status" in tl:
            continue

        if not STATUS_RE.search(t):
            continue

        lines = [x.strip() for x in t.splitlines() if x.strip()]
        # status 추출
        m = STATUS_RE.search(t)
        status = m.group(0) if m else ""

        # name 추출: status/헤더/버튼류 제외 후 가장 긴 라인
        name = ""
        best_len = 0
        for line in lines:
            ll = line.lower()
            if ll in NOISE_LINES:
                continue
            if STATUS_RE.search(line):
                continue
            if len(line) > best_len:
                best_len = len(line)
                name = line

        if not name or not status:
            continue

        k = (name, status.lower())
        if k in seen:
            continue
        seen.add(k)
        out.append({"Name": name, "Status": status})

    return out

def click_next_simple(page) -> bool:
    # 페이지네이션이 있으면 Next를 클릭 (없으면 False)
    # 1) aria-label / role name으로 Next
    try:
        page.get_by_role("button", name=re.compile(r"next", re.I)).click(timeout=1500)
        page.wait_for_timeout(1200)
        return True
    except:
        pass

    # 2) ">" 텍스트 버튼
    try:
        page.get_by_role("button", name=">").click(timeout=1500)
        page.wait_for_timeout(1200)
        return True
    except:
        pass

    # 3) aria-label에 next 포함된 버튼 아무거나
    try:
        page.locator("button[aria-label*='Next' i], a[aria-label*='Next' i]").first.click(timeout=1500)
        page.wait_for_timeout(1200)
        return True
    except:
        return False

def save_debug(page, prefix="debug_all"):
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
    print("debug saved:", png, html)

def main():
    all_rows: List[Dict[str, str]] = []

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir="C:/temp/pw_dropboxsign_profile",
            headless=False
        )
        page = context.new_page()
        page.goto(URL_ALL, wait_until="domcontentloaded", timeout=60000)

        print("로그인 후 Enter")
        input()

        force_all(page)
        print("current url:", page.url)

        for page_no in [1, 2]:  # 2페이지까지만
            rows = scrape_name_status_by_role_rows(page)
            print(f"page {page_no}: {len(rows)} rows")
            all_rows.extend(rows)

            if page_no == 2:
                break

            moved = click_next_simple(page)
            if not moved:
                print("Next 못찾음 → 2페이지 이동 실패")
                break

        if not all_rows:
            print("rows=0 이라서 debug 저장")
            save_debug(page)

        df = pd.DataFrame(all_rows).drop_duplicates()
        df.to_csv("all.csv", index=False, encoding="utf-8-sig")
        print("완료: all.csv", len(df), "rows")

        context.close()

if __name__ == "__main__":
    main()