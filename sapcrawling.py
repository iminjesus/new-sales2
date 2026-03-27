import os
import time
import glob
import csv
import win32com.client
from datetime import datetime
from openpyxl import load_workbook

"""
MB52 Auto Export -> detects SAP-created EXPORT_*.xlsx -> converts to CSV.

Fix for your current error:
- Your SAP GUI does NOT have the menu id: wnd[0]/mbar/menu[0]/menu[3]/menu[1]
  (List -> Export -> Spreadsheet)
- This version uses multiple fallback strategies:
  1) Try ALV Grid toolbar export (most stable across layouts)
  2) Try menu selection by text (List/Export/Spreadsheet), not by index
  3) Try a few common menu index paths

Pre-req:
- SAP GUI must be OPEN and LOGGED IN
- Packages: pywin32, openpyxl
"""

# ================= CONFIG =================
PLANT_LOW  = "42R0"
PLANT_HIGH = "42R4"

SAP_EXPORT_DIR  = r"C:\temp"         # where EXPORT_*.xlsx appears
SAP_EXPORT_GLOB = "EXPORT_*.xlsx"

OUT_CSV = r"D:\Data-Anal website\rawdata\unlock\mb52_42.csv"

DELETE_XLSX_AFTER_CONVERT = False
MIN_CSV_SIZE = 200  # bytes


# ================= HELPERS =================
def wait(sec=0.3):
    time.sleep(sec)

def ensure_out_dir(path):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def exists(session, wid):
    try:
        session.findById(wid)
        return True
    except:
        return False

def close_popups(session, max_steps=6):
    for _ in range(max_steps):
        if not exists(session, "wnd[1]"):
            return
        try:
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            wait(0.25)
            continue
        except:
            pass
        try:
            session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()
            wait(0.25)
            continue
        except:
            pass
        try:
            session.findById("wnd[1]").sendVKey(0)
            wait(0.25)
            continue
        except:
            pass
        return

def start_mb52_fresh(session):
    session.findById("wnd[0]/tbar[0]/okcd").Text = "/nMB52"
    session.findById("wnd[0]").sendVKey(0)
    wait(1.0)
    close_popups(session, 4)

def set_plant(session):
    # LOW = 42R0
    try:
        session.findById("wnd[0]/usr/ctxtWERKS-LOW").Text = PLANT_LOW
    except:
        session.findById("wnd[0]/usr/ctxtWERKS").Text = PLANT_LOW

    # HIGH = 42R4
    try:
        session.findById("wnd[0]/usr/ctxtWERKS-HIGH").Text = PLANT_HIGH
    except:
        pass

    session.findById("wnd[0]").sendVKey(0)
    wait(0.5)
    close_popups(session, 3)

def newest_export_xlsx(after_ts: float) -> str:
    pattern = os.path.join(SAP_EXPORT_DIR, SAP_EXPORT_GLOB)
    candidates = []
    for p in glob.glob(pattern):
        try:
            mtime = os.path.getmtime(p)
            if mtime >= after_ts:
                candidates.append((mtime, p))
        except:
            pass
    if not candidates:
        return ""
    candidates.sort(reverse=True)
    return candidates[0][1]

def xlsx_to_csv(xlsx_path: str, csv_path: str):
    wb = load_workbook(xlsx_path, data_only=True)
    ws = wb.active
    ensure_out_dir(csv_path)

    def norm(v):
        if v is None:
            return ""
        s = str(v).strip()
        return s

    def is_number_like(s: str) -> bool:
        # "234,252" / "1234.56" / "-10" 같은 걸 숫자로 판단
        if not s:
            return False
        t = s.replace(",", "")
        try:
            float(t)
            return True
        except:
            return False

    def is_trailing_summary_row(values) -> bool:
        # 규칙:
        # - 비어있지 않은 셀이 1~2개 정도로 매우 적고
        # - 그 값들이 숫자처럼 보이면 "요약 줄"로 간주
        non_empty = [v for v in values if v != ""]
        if len(non_empty) == 0:
            return True  # 완전 빈 줄은 제거
        if len(non_empty) <= 2 and all(is_number_like(v) for v in non_empty):
            return True
        return False

    # 1) 엑셀 전체를 리스트로 읽기
    rows = []
    for row in ws.iter_rows(values_only=True):
        rows.append([norm(v) for v in row])

    # 2) 뒤쪽의 빈 줄 제거
    while rows and all(v == "" for v in rows[-1]):
        rows.pop()

    # 3) 마지막 줄이 "요약 줄" 패턴이면 제거 (필요하면 연속으로 여러 줄도 제거)
    while rows and is_trailing_summary_row(rows[-1]):
        rows.pop()

    # 4) 필요한 컬럼만 추출 (Plant, Material, Unrestricted)
    KEEP_COLS = ["Plant", "Material", "Unrestricted"]
    if rows:
        header = rows[0]
        keep_idx = [i for i, h in enumerate(header) if h.strip() in KEEP_COLS]
        rows = [[r[i] for i in keep_idx] for r in rows]

    # 5) CSV로 저장
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for r in rows:
            w.writerow(r)

def file_ok(path, min_size):
    try:
        return os.path.exists(path) and os.path.getsize(path) >= min_size
    except:
        return False


# ---------- Export trigger strategies ----------
def try_export_via_alv_toolbar(session) -> bool:
    """
    ALV grid export is usually the most stable method.
    Typical grid IDs vary; we try several common ones.
    """
    grid_ids = [
        "wnd[0]/usr/cntlGRID1/shellcont/shell",
        "wnd[0]/usr/cntlCONTAINER/shellcont/shell",
        "wnd[0]/usr/cntlALV_CONTAINER/shellcont/shell",
        "wnd[0]/usr/cntlGRID1/shellcont/shellcont/shell",
    ]
    for gid in grid_ids:
        if not exists(session, gid):
            continue
        try:
            grid = session.findById(gid)

            # open export context
            opened = False
            for export_btn in ("&MB_EXPORT", "EXPORT", "&EXPORT"):
                try:
                    grid.pressToolbarContextButton(export_btn)
                    wait(0.4)
                    opened = True
                    break
                except:
                    continue
            if not opened:
                continue

            # choose spreadsheet/xxl
            for item in ("&XXL", "XXL", "&SPREADSHEET", "SPREADSHEET", "&PC", "PC"):
                try:
                    grid.selectContextMenuItem(item)
                    wait(0.8)
                    close_popups(session, 4)
                    return True
                except:
                    continue
        except:
            continue
    return False


def find_menu_path_by_text(session, texts):
    """
    Finds nested menu path by visible text, e.g. ["List","Export","Spreadsheet"].
    Returns the final menu item object or None.
    """
    try:
        mbar = session.findById("wnd[0]/mbar")
    except:
        return None

    level = []
    try:
        for i in range(mbar.Children.Count):
            level.append(mbar.Children(i))
    except:
        return None

    last_hit = None
    for t in texts:
        hit = None
        for it in level:
            try:
                txt = (getattr(it, "Text", "") or "").replace("&", "").strip().lower()
                if txt == t.lower():
                    hit = it
                    break
            except:
                continue
        if hit is None:
            return None
        last_hit = hit
        # next level
        nxt = []
        try:
            for i in range(hit.Children.Count):
                nxt.append(hit.Children(i))
        except:
            nxt = []
        level = nxt

    return last_hit


def try_export_via_menu(session) -> bool:
    # by text
    for path in (["List", "Export", "Spreadsheet"],
                 ["List", "Export", "Local File"],
                 ["List", "Export", "Spreadsheet..."]):
        item = find_menu_path_by_text(session, path)
        if item is not None:
            try:
                item.select()
                wait(0.8)
                close_popups(session, 6)
                return True
            except:
                pass

    # by common indexes
    fallbacks = [
        "wnd[0]/mbar/menu[0]/menu[3]/menu[1]",
        "wnd[0]/mbar/menu[0]/menu[11]/menu[1]",
        "wnd[0]/mbar/menu[0]/menu[4]/menu[1]",
        "wnd[0]/mbar/menu[0]/menu[3]/menu[2]",
    ]
    for wid in fallbacks:
        if exists(session, wid):
            try:
                session.findById(wid).select()
                wait(0.8)
                close_popups(session, 6)
                return True
            except:
                pass
    return False


def trigger_export(session):
    if try_export_via_alv_toolbar(session):
        return True
    if try_export_via_menu(session):
        return True
    return False


# ================= MAIN =================
def main():
    ensure_out_dir(OUT_CSV)

    if os.path.exists(OUT_CSV):
        try:
            os.remove(OUT_CSV)
        except:
            pass

    export_start_ts = time.time()

    SapGuiAuto = win32com.client.GetObject("SAPGUI")
    app = SapGuiAuto.GetScriptingEngine

    if app.Children.Count == 0:
        raise RuntimeError("No SAP connection found. Open SAP GUI and log in first.")

    conn = app.Children(0)
    if conn.Children.Count == 0:
        raise RuntimeError("No SAP session found.")

    session = conn.Children(0)

    try:
        session.findById("wnd[0]").maximize()
    except:
        pass

    start_mb52_fresh(session)
    set_plant(session)

    # Execute (F8)
    session.findById("wnd[0]/tbar[1]/btn[8]").press()
    wait(2.0)
    close_popups(session, 4)

    ok = trigger_export(session)
    if not ok:
        raise RuntimeError("Could not trigger export (menu/ALV export controls not found).")

    # wait for XLSX to appear
    xlsx_path = ""
    for _ in range(40):
        xlsx_path = newest_export_xlsx(export_start_ts)
        if xlsx_path and os.path.exists(xlsx_path):
            s1 = os.path.getsize(xlsx_path)
            wait(0.8)
            s2 = os.path.getsize(xlsx_path)
            if s2 >= s1 and s2 > 500:
                break
        wait(0.6)

    if not xlsx_path:
        raise RuntimeError(
            f"Could not find SAP auto-export XLSX in {SAP_EXPORT_DIR} "
            f"(pattern {SAP_EXPORT_GLOB}) after {datetime.fromtimestamp(export_start_ts)}"
        )

    print("Detected XLSX:", xlsx_path)

    xlsx_to_csv(xlsx_path, OUT_CSV)

    if not file_ok(OUT_CSV, MIN_CSV_SIZE):
        raise RuntimeError("CSV conversion failed or CSV is too small.")

    print("CSV saved:", OUT_CSV)

    if DELETE_XLSX_AFTER_CONVERT:
        try:
            os.remove(xlsx_path)
            print("Deleted XLSX:", xlsx_path)
        except Exception as e:
            print("[WARN] Could not delete XLSX:", e)

if __name__ == "__main__":
    main()
