import os
import sys
import time
import glob
import csv
import subprocess
import win32com.client
from datetime import datetime, date, timedelta
from openpyxl import load_workbook

"""
ZMMR40430 "AP Stock Status 2 (with Open Inquiry QTY)" auto-export.

Oct-21 user ask: SPRF stock cells should read from the RT (real-time)
stock feed instead of the DOT-aging roll-up.  The RT feed carries:
  • unrestricted stock
  • on order
  • ATP (unrestricted − on order)
  • at dock / on water / factory (incoming pipeline)
  • 3-month average sales
...per (Plant, Material) row.

Selection screen setup:
  • Plant: 42R0 to 42R4 (QLD / NSW / VIC / WA)
  • Sales Organization: 4200
  • Distribute Channel: d1
  • Valid On: today
  • "All" stock-status radio (Unrestricted + Block + Quality Inspection)
  • "No Zero Stock Lines" checkbox ticked
  • Layout: /ART_TH (saved layout; drives the keep-columns list)

Output chain:
  • SAP writes EXPORT_*.xlsx to C:\\temp
  • We copy it to rawdata/unlock/rt_stock.xlsx
  • Then convert to rawdata/unlock/rt_stock_YYMMDD.csv (dated so the
    loader can pick the newest one by default)

Downstream:
  • load_csv_to_mysql.py reads the newest rt_stock_*.csv and
    (re)loads the `stock_rt` MySQL table.
  • /api/orders/stock_rt_by_material returns per-plant ATP /
    Unrestricted / At dock / On water / Factory for one M-Code.
  • SPRF per-row stock cells render "ATP / Unrestricted"; double-
    click opens a popover with the pipeline breakdown.

Pre-req:
  • SAP GUI open AND logged in (or batch_sap_logon can launch it)
  • pywin32, openpyxl
"""

# ================= CONFIG =================
TCODE          = "ZMMR40430"
SALES_ORG      = "4200"
DISTRIBUTE_CH  = "d1"

# Plants to pull — the ZMMR40430 selection screen accepts a plant
# RANGE (LOW..HIGH), so one F8 covers every plant from 42R0 to 42R4.
PLANT_LOW      = "42R0"
PLANT_HIGH     = "42R4"

SAP_EXPORT_DIR  = r"C:\temp"
SAP_EXPORT_GLOB = "EXPORT_*.xlsx"

# Resolve relative to this script so the same code runs on every
# checkout regardless of the absolute path the repo was cloned to.
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
UNLOCK_DIR = os.path.join(BASE_DIR, "rawdata", "unlock")
OUT_XLSX   = os.path.join(UNLOCK_DIR, "rt_stock.xlsx")

MIN_CSV_SIZE = 200  # bytes

# SAP user-profile date format varies per operator (System → User
# Profile → Own Data → Defaults → Date format).  Order matters: the
# first accepted format wins.  Order kept in sync with sapcrawling.py
# / sapcrawling_sales1.py so the batch orchestrator's run against
# a single SAP session uses the same resolution path.
DATE_FORMATS = [
    "%d.%m.%Y",   # 21.10.2026
    "%Y.%m.%d",   # 2026.10.21  (SAP default 4 — this operator's profile)
    "%m/%d/%Y",   # 10/21/2026  (SAP default 2 — US)
    "%d/%m/%Y",   # 21/10/2026  (SAP default 6 — UK/AU)
    "%Y-%m-%d",   # 2026-10-21  (ISO)
]

# ALV Grid saved layout to auto-apply after F8 (Settings → Layout →
# Choose).  Use the TECHNICAL name from the first column of the layout
# dialog (starts with '/' and is uppercase).  /ART_TH is the layout
# the user ships for this report — it carries On Order, Stock, ATP Qt,
# At dock, On water, Factory, 3M Avrg.
LAYOUT_NAME = "/ART_TH"

# Field-ID candidates for the ZMMR40430 selection screen.  First
# existing ID wins; unmatched candidates skip quietly.
ZMMR_FIELDS = {
    "material_low":    ["wnd[0]/usr/ctxtS_MATNR-LOW",
                        "wnd[0]/usr/ctxtS_MATNR-LOW"],
    "material_high":   ["wnd[0]/usr/ctxtS_MATNR-HIGH"],
    "plant_low":       ["wnd[0]/usr/ctxtS_WERKS-LOW",
                        "wnd[0]/usr/ctxtS_PLANT-LOW",
                        "wnd[0]/usr/ctxtP_WERKS"],
    "plant_high":      ["wnd[0]/usr/ctxtS_WERKS-HIGH",
                        "wnd[0]/usr/ctxtS_PLANT-HIGH"],
    "sales_org":       ["wnd[0]/usr/ctxtP_VKORG",
                        "wnd[0]/usr/ctxtS_VKORG-LOW"],
    "distribute_ch":   ["wnd[0]/usr/ctxtP_VTWEG",
                        "wnd[0]/usr/ctxtS_VTWEG-LOW"],
    "valid_on":        ["wnd[0]/usr/ctxtP_DATE",
                        "wnd[0]/usr/ctxtP_VALID",
                        "wnd[0]/usr/ctxtP_VDATE",
                        "wnd[0]/usr/ctxtS_DATUM-LOW"],
    # "All" stock-status radio — ticks every bucket (Unrestricted +
    # Block + Quality Inspection).  The user's screenshot shows "All"
    # as the live default.
    "radio_all":       ["wnd[0]/usr/radP_ALL",
                        "wnd[0]/usr/radRA_ALL"],
    # "No Zero Stock Lines" checkbox — kept TICKED (skip zero rows).
    "no_zero":         ["wnd[0]/usr/chkP_NOZERO",
                        "wnd[0]/usr/chkP_NZERO"],
}

# Yellow columns the layout /ART_TH exposes — we keep ALL of them
# through to the CSV so load_csv_to_mysql can build the stock_rt
# table from the full set.  Lower-case + stripped header match.
KEEP_COLUMNS = {
    "plant":        "plant",
    "material":     "material",
    "description":  "description",
    "pattern":      "pattern",
    "load index":   "load_index",
    "hq group":     "hq_group",
    "on order":     "on_order",
    "stock":        "unrestricted",
    "atp qt":       "atp_qt",
    "atp":          "atp_qt",   # fallback header
    "list price":   "list_price",
    "at dock":      "at_dock",
    "on water":     "on_water",
    "factory":      "factory",
    "3m avrg.":     "avg_3m",
    "3m avrg":      "avg_3m",
    "3 month avg":  "avg_3m",
}
OUTPUT_HEADER = ["plant", "material", "description", "pattern",
                 "load_index", "hq_group", "on_order", "unrestricted",
                 "atp_qt", "list_price", "at_dock", "on_water",
                 "factory", "avg_3m"]


# ================= DATE HELPERS =================
def today_date() -> date:
    return datetime.now().date()


# ================= HELPERS =================
def wait(sec=0.3):
    time.sleep(sec)

def ensure_dir(d):
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)

def exists(session, wid):
    try:
        session.findById(wid)
        return True
    except:
        return False


# ──────────────────────────────────────────────────────────────────────
# Excel killer — stops Office from locking the files we're about to write
# ──────────────────────────────────────────────────────────────────────
def _kill_excel(reason=""):
    for exe in ("EXCEL.EXE", "excel.exe"):
        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", exe, "/T"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=10, creationflags=0x08000000,
            )
        except Exception:
            pass
    for exe in ("OSPPREARM.EXE", "msoia.exe", "OFFICECLICKTORUN.EXE",
                "SETUP.EXE"):
        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", exe],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=5, creationflags=0x08000000,
            )
        except Exception:
            pass
    if reason:
        try: print(f"  [kill-excel] {reason}")
        except Exception: pass


def close_popups(session, max_steps=6):
    for _ in range(max_steps):
        if not exists(session, "wnd[1]"):
            return
        for wid in ("wnd[1]/tbar[0]/btn[0]",
                    "wnd[1]/usr/btnSPOP-OPTION1"):
            try:
                session.findById(wid).press(); wait(0.25); break
            except:
                pass
        else:
            try:
                session.findById("wnd[1]").sendVKey(0); wait(0.25)
            except:
                return


def start_tcode(session, tcode):
    session.findById("wnd[0]/tbar[0]/okcd").Text = f"/n{tcode}"
    session.findById("wnd[0]").sendVKey(0)
    wait(1.5)
    close_popups(session, 4)


def sbar_text(session) -> str:
    try:
        return (session.findById("wnd[0]/sbar").Text or "").strip()
    except:
        return ""

def sbar_is_error(session) -> bool:
    try:
        return (session.findById("wnd[0]/sbar").MessageType or "").upper() in ("E", "W", "A")
    except:
        return False

def looks_like_date_format_error(msg: str) -> bool:
    m = (msg or "").lower()
    return any(k in m for k in ("format", "date", "invalid", "enter", "gueltig", "valid"))


def _try_set_text(session, id_candidates, value):
    for wid in id_candidates:
        if exists(session, wid):
            try:
                session.findById(wid).Text = value
                return True
            except: pass
    return False

def _try_check(session, id_candidates, state=True):
    for wid in id_candidates:
        if exists(session, wid):
            try:
                session.findById(wid).Selected = bool(state)
                return True
            except: pass
    return False

def _try_select_radio(session, id_candidates):
    for wid in id_candidates:
        if exists(session, wid):
            try:
                session.findById(wid).select()
                return True
            except: pass
    return False


def _select_radio_by_text(session, wanted_text):
    """Walk every control under wnd[0]/usr and .select() the first
    GuiRadioButton whose displayed Text matches `wanted_text`
    (case-insensitive, whitespace-trimmed).  Robust to per-SAP-build
    ID differences — ZMMR40430's 'All' radio has no stable radP_ALL
    / radRA_ALL ID across SAP builds, so an ID-only match misses
    and the screen stays on the default 'Unrestricted Use' radio.
    This path only needs the label the operator sees on screen."""
    wanted = (wanted_text or "").strip().lower()
    if not wanted:
        return False
    try:
        root = session.findById("wnd[0]/usr")
    except Exception:
        return False

    def _walk(elem):
        try:
            if (elem.Type == "GuiRadioButton"
                    and (elem.Text or "").strip().lower() == wanted):
                try:
                    elem.select()
                    return True
                except Exception:
                    return False
        except Exception:
            pass
        try:
            n = elem.Children.Count
        except Exception:
            return False
        for i in range(n):
            try:
                if _walk(elem.Children(i)):
                    return True
            except Exception:
                pass
        return False

    return _walk(root)


def set_selection_screen(session, valid_on_str):
    """Fill the ZMMR40430 selection screen.  Plant range + Sales Org
    + Distribute Channel + Valid On + 'All' radio + 'No Zero' tick."""
    _try_set_text(session, ZMMR_FIELDS["plant_low"],    PLANT_LOW)
    _try_set_text(session, ZMMR_FIELDS["plant_high"],   PLANT_HIGH)
    _try_set_text(session, ZMMR_FIELDS["sales_org"],    SALES_ORG)
    _try_set_text(session, ZMMR_FIELDS["distribute_ch"], DISTRIBUTE_CH)
    _try_set_text(session, ZMMR_FIELDS["valid_on"],     valid_on_str)
    # 'All' stock-status radio — try the ID candidates first (fast,
    # exact), fall back to a text-based scan when the IDs don't match
    # this SAP build.  Reports success / fallback to the console so
    # the log shows which path landed on the right radio.
    if _try_select_radio(session, ZMMR_FIELDS["radio_all"]):
        pass
    elif _select_radio_by_text(session, "All"):
        print("  [radio] 'All' selected via text-scan fallback (no stable ID)")
    else:
        print("  [WARN] Could not find the 'All' radio button — "
              "selection screen will fall back to its default (likely "
              "'Unrestricted Use').  Export may miss Block / Quality "
              "Inspection stock.")
    _try_check(session, ZMMR_FIELDS["no_zero"], True)
    wait(0.3)


# ================= Export + layout =================
def newest_export_xlsx(after_ts: float) -> str:
    """Return the newest .xlsx under SAP_EXPORT_DIR with mtime >=
    after_ts.  Oct-22: widened from the strict EXPORT_*.xlsx pattern
    to ANY *.xlsx — some SAP builds / layouts default the save-as
    filename to the TCODE ("ZMMR40430.xlsx") or to the ALV title
    rather than "EXPORT_<timestamp>.xlsx", and the strict glob missed
    those files entirely.  Still bounded by after_ts so a stale file
    from a previous run can't match."""
    patterns = (SAP_EXPORT_GLOB, "*.xlsx", "*.XLSX")
    seen = set()
    candidates = []
    for pat in patterns:
        for p in glob.glob(os.path.join(SAP_EXPORT_DIR, pat)):
            if p in seen:
                continue
            seen.add(p)
            try:
                mtime = os.path.getmtime(p)
                if mtime >= after_ts:
                    candidates.append((mtime, p))
            except: pass
    if not candidates:
        return ""
    candidates.sort(reverse=True)
    return candidates[0][1]


def _list_recent_xlsx(after_ts: float, limit: int = 20):
    """Diagnostic — list every .xlsx under SAP_EXPORT_DIR with mtime
    anywhere near after_ts (±5 minute window), so a file-not-found
    error can show what SAP actually wrote (or didn't)."""
    rows = []
    for p in glob.glob(os.path.join(SAP_EXPORT_DIR, "*")):
        try:
            mtime = os.path.getmtime(p)
            if abs(mtime - after_ts) <= 300:
                rows.append((mtime, p, os.path.getsize(p)))
        except: pass
    rows.sort(reverse=True)
    return rows[:limit]


def try_set_layout_on_selection(session, layout_name):
    """Set a saved layout name on the selection screen via the
    user-variant / layout settings panel.  No-op if the layout box
    isn't on screen — the apply_layout_via_dialog path below will
    retry from the ALV grid afterwards."""
    if not layout_name:
        return
    for wid in ("wnd[0]/usr/ctxtP_LAYOUT",
                "wnd[0]/usr/ctxtP_VARIANT"):
        if exists(session, wid):
            try:
                session.findById(wid).Text = layout_name
                return
            except: pass


def apply_layout_via_dialog(session, layout_name):
    """After F8, open Settings → Layout → Choose and pick layout_name.
    Returns True on success, False if the dialog path failed; the
    caller just logs a warning and uses the SAP default layout."""
    if not layout_name:
        return True
    try:
        # Grid context: pressToolbarContextButton often exposes the
        # Layout menu as "&LAYOUT" or similar; fallback to menu path.
        grid_ids = [
            "wnd[0]/usr/cntlG_CONTAINER/shellcont/shell",
            "wnd[0]/usr/cntlGRID1/shellcont/shell",
            "wnd[0]/usr/cntlCONTAINER/shellcont/shell",
            "wnd[0]/usr/cntlALV_CONTAINER/shellcont/shell",
        ]
        grid = None
        for gid in grid_ids:
            if exists(session, gid):
                try:
                    grid = session.findById(gid); break
                except: continue
        if grid is not None:
            try:
                grid.pressToolbarContextButton("&MB_VARIANT")
                wait(0.3)
                grid.selectContextMenuItem("&LOAD")
                wait(0.6)
            except: pass
        # Settings → Layout → Choose fallback via menu path.
        if not exists(session, "wnd[1]"):
            try:
                session.findById("wnd[0]/mbar").select()
            except: pass
        # On the Choose Layout popup, type the layout name into the
        # filter field and double-click the matching row.
        if exists(session, "wnd[1]"):
            for wid in ("wnd[1]/usr/ctxtLTDX-VARIANT",
                        "wnd[1]/usr/txtLTDX-VARIANT"):
                if exists(session, wid):
                    try:
                        session.findById(wid).Text = layout_name
                        session.findById("wnd[1]").sendVKey(0)
                        wait(0.4)
                        break
                    except: pass
            try:
                session.findById("wnd[1]/tbar[0]/btn[0]").press()
                wait(0.4); close_popups(session, 3)
                return True
            except: pass
        return False
    except Exception:
        return False


def try_export_via_alv_toolbar(session) -> bool:
    """ALV grid toolbar's Export → XXL / Spreadsheet context menu.
    This is the method that works on ZSDM64300; ZMMR40430 uses the
    same ALV layer so the toolbar context should match."""
    grid_ids = [
        "wnd[0]/usr/cntlG_CONTAINER/shellcont/shell",
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
            opened = False
            for export_btn in ("&MB_EXPORT", "EXPORT", "&EXPORT"):
                try:
                    grid.pressToolbarContextButton(export_btn)
                    wait(0.4); opened = True; break
                except: continue
            if not opened:
                continue
            for item in ("&XXL", "XXL", "&SPREADSHEET", "SPREADSHEET", "&PC", "PC"):
                try:
                    grid.selectContextMenuItem(item)
                    wait(0.8); close_popups(session, 4)
                    return True
                except: continue
        except: continue
    return False


def try_export_via_system_menu(session) -> bool:
    """SAP's universal 'List → Export → Local File' path.  Same hard-
    coded menu lookup as sapcrawling_batch — proven on ZSDM64300 and
    typically identical on ZMMR40430."""
    for path in (
        "wnd[0]/mbar/menu[0]/menu[5]/menu[2]/menu[2]",
        "wnd[0]/mbar/menu[0]/menu[3]/menu[2]/menu[2]",
        "wnd[0]/mbar/menu[0]/menu[1]/menu[2]/menu[2]",
    ):
        if not exists(session, path):
            continue
        try:
            session.findById(path).select()
            wait(0.6)
        except:
            continue
        # Format popup — pick Spreadsheet (radio index 1).
        for rid in (
            "wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]",
            "wnd[1]/usr/radRB_SPRE",
            "wnd[1]/usr/radR_XLS",
        ):
            if exists(session, rid):
                try: session.findById(rid).select()
                except: pass
                break
        try:
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            wait(0.6)
        except: pass
        if exists(session, "wnd[1]"):
            try:
                if exists(session, "wnd[1]/usr/ctxtDY_PATH"):
                    session.findById("wnd[1]/usr/ctxtDY_PATH").Text = SAP_EXPORT_DIR + "\\"
                if exists(session, "wnd[1]/usr/ctxtDY_FILENAME"):
                    session.findById("wnd[1]/usr/ctxtDY_FILENAME").Text = "EXPORT_RTSTOCK.xlsx"
            except: pass
            for sid in ("wnd[1]/tbar[0]/btn[11]",
                        "wnd[1]/tbar[0]/btn[0]"):
                try:
                    session.findById(sid).press(); wait(0.8); break
                except: pass
            close_popups(session, 4)
        return True
    return False


def try_export_via_menu(session) -> bool:
    """Fallback: List → Export → Spreadsheet (menu text lookup)."""
    try:
        mbar = session.findById("wnd[0]/mbar")
    except:
        return False
    target_path = ["List", "Export", "Spreadsheet"]
    level = [mbar.Children(i) for i in range(mbar.Children.Count)]
    for wanted in target_path:
        found = None
        for m in level:
            try:
                if m.Text.strip().lower() == wanted.lower():
                    found = m; break
            except: pass
        if not found: return False
        try:
            level = [found.Children(i) for i in range(found.Children.Count)]
        except:
            try:
                found.select(); wait(0.6); close_popups(session, 4)
                return True
            except:
                return False
    try:
        level[0].select(); wait(0.6); close_popups(session, 4)
        return True
    except:
        return False


def try_export_via_okcd(session) -> bool:
    """SAP's %pc list-save backdoor."""
    for cmd in ("%pc", "%%pc"):
        try:
            session.findById("wnd[0]/tbar[0]/okcd").Text = cmd
            session.findById("wnd[0]").sendVKey(0)
            wait(0.8)
            if exists(session, "wnd[1]"):
                for rid in ("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]",
                            "wnd[1]/usr/radRB_SPRE",
                            "wnd[1]/usr/radR_XLS",
                            "wnd[1]/usr/radSPRE",
                            "wnd[1]/usr/radR_SPRE"):
                    if exists(session, rid):
                        try: session.findById(rid).select(); break
                        except: pass
                try:
                    session.findById("wnd[1]/tbar[0]/btn[0]").press(); wait(0.6)
                except: pass
                close_popups(session, 4)
                return True
        except: pass
    return False


def try_export_via_toolbar_button(session) -> bool:
    """Some Z-transactions expose 'Local File' as a top-toolbar button
    or via Ctrl+Shift+F9."""
    for vk in (45,):   # Ctrl+Shift+F9 → standard "Local File" list menu
        try:
            session.findById("wnd[0]").sendVKey(vk)
            wait(0.6)
            if exists(session, "wnd[1]"):
                close_popups(session, 4)
                return True
        except: pass
    return False


def dump_screen(session):
    """Print the full SAP window tree.  Called ONCE on the first
    export failure so the operator can share the output back; the
    output names every toolbar button / menu path that ZMMR40430's
    ALV grid is exposing on this box, so we can pin the right export
    method for this layout."""
    print("  ==== ZMMR40430 SCREEN DUMP ====")
    def _walk(elem, depth=0):
        try:    eid = elem.Id
        except: eid = "?"
        try:    etype = elem.Type
        except: etype = "?"
        try:    etext = (elem.Text or "").strip()[:40]
        except: etext = ""
        print(f"    {'  '*depth}[{etype}] {eid}  {etext!r}")
        try:
            for i in range(elem.Children.Count):
                _walk(elem.Children(i), depth + 1)
        except: pass
    try:
        _walk(session.findById("wnd[0]"))
    except Exception as e:
        print(f"    dump failed: {e}")
    print("  ==== END DUMP ====")


# Dump the screen ONCE per script run so a repeated failure doesn't
# flood the log with the same 300-line listing.
_DEBUG_DUMPED = False
DEBUG_DUMP_ON_EXPORT_FAIL = True


def trigger_export(session) -> bool:
    """Try every known SAP export path in rough success-order from the
    ZSDM64300 experience.  First one that returns True wins; falls
    through to a one-shot screen dump so we can see what controls the
    ALV grid exposes on ZMMR40430's layout.

    Each attempt is logged so a 'file not in C:\\temp' failure can be
    traced back to which method claimed success — in particular the
    ALV-toolbar &XXL / SPREADSHEET path opens Excel INLINE on some
    SAP builds instead of saving to disk, which is a false-positive
    'export triggered' with no file."""
    global _DEBUG_DUMPED
    for name, fn in (("system_menu",     try_export_via_system_menu),
                     ("alv_toolbar",     try_export_via_alv_toolbar),
                     ("menu_text",       try_export_via_menu),
                     ("okcd_%pc",        try_export_via_okcd),
                     ("toolbar_button",  try_export_via_toolbar_button)):
        try:
            if fn(session):
                print(f"  [export] method '{name}' claimed success")
                return True
        except Exception as e:
            print(f"  [export] method '{name}' raised: {e}")
    if DEBUG_DUMP_ON_EXPORT_FAIL and not _DEBUG_DUMPED:
        _DEBUG_DUMPED = True
        dump_screen(session)
    return False


# ================= XLSX → CSV =================
def _norm_header(h):
    return (str(h or "")).strip().lower().rstrip(".")


def extract_rows(xlsx_path):
    """Open the XLSX, locate the header row (first row with 'Plant' +
    'Material' + 'Stock' columns), and pull every data row's keep-
    columns into a dict keyed by the normalised OUTPUT_HEADER names."""
    wb = load_workbook(xlsx_path, data_only=True, read_only=True)
    ws = wb.active

    header_row = None
    col_map    = {}
    for r_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        normalised = [_norm_header(c) for c in row]
        if "plant" in normalised and "material" in normalised:
            header_row = r_idx
            for c_idx, h in enumerate(normalised):
                tgt = KEEP_COLUMNS.get(h)
                if tgt:
                    col_map[tgt] = c_idx
            break
    if header_row is None:
        wb.close()
        raise RuntimeError(f"Header row with Plant + Material not found in {xlsx_path}")

    rows = []
    for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
        if row is None:
            continue
        if all((c is None or str(c).strip() == "") for c in row):
            continue
        out = []
        for key in OUTPUT_HEADER:
            idx = col_map.get(key)
            val = row[idx] if (idx is not None and idx < len(row)) else ""
            if val is None:
                val = ""
            # Strip SAP's thousands separators / spurious whitespace
            # on numeric columns so MySQL CAST reads them cleanly.
            if key in ("on_order", "unrestricted", "atp_qt",
                       "list_price", "at_dock", "on_water", "factory",
                       "avg_3m"):
                s = str(val).replace(",", "").replace(" ", "").strip()
                val = s or "0"
            else:
                val = str(val).strip()
            out.append(val)
        rows.append(out)
    wb.close()
    return rows


def xlsx_to_csv(xlsx_path, csv_path):
    rows = extract_rows(xlsx_path)
    ensure_dir(os.path.dirname(csv_path))
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(OUTPUT_HEADER)
        for r in rows:
            w.writerow(r)
    return len(rows)


# ================= MAIN =================
def _dated_csv_path(d: date) -> str:
    return os.path.join(UNLOCK_DIR, f"rt_stock_{d.strftime('%Y%m%d')}.csv")


def main():
    ensure_dir(UNLOCK_DIR)
    valid_dt = today_date()
    print(f"ZMMR40430 RT stock crawl · plants={PLANT_LOW}..{PLANT_HIGH} · valid={valid_dt.isoformat()}")

    _kill_excel("pre-flight: clear Excel locks before SAP export")

    sap = win32com.client.GetObject("SAPGUI").GetScriptingEngine
    conn = sap.Children(0)
    session = conn.Children(0)
    try:
        session.findById("wnd[0]").maximize()
    except: pass

    # Loop over date formats until SAP accepts one.  Each attempt
    # restarts the TCODE so the selection screen is clean before
    # re-entering plant / sales-org / channel / valid-on.
    export_start_ts = None
    accepted_fmt    = None
    for i, fmt in enumerate(DATE_FORMATS, 1):
        valid_on_str = valid_dt.strftime(fmt)
        print(f"  [Attempt {i}/{len(DATE_FORMATS)}] date format = {fmt} -> {valid_on_str}")

        start_tcode(session, TCODE)
        set_selection_screen(session, valid_on_str)
        try_set_layout_on_selection(session, LAYOUT_NAME)

        export_start_ts = time.time()
        session.findById("wnd[0]/tbar[1]/btn[8]").press()   # F8
        wait(2.0); close_popups(session, 4)

        msg = sbar_text(session)
        if sbar_is_error(session) and looks_like_date_format_error(msg):
            print(f"    SAP rejected date format: {msg}")
            continue

        accepted_fmt = fmt
        if msg:
            print(f"    format {fmt} accepted (status: {msg})")
        else:
            print(f"    format {fmt} accepted")
        break

    if not accepted_fmt:
        raise RuntimeError(
            "None of the candidate date formats were accepted by SAP."
        )

    if apply_layout_via_dialog(session, LAYOUT_NAME):
        wait(0.6)
    else:
        print(f"  [WARN] Could not auto-apply layout '{LAYOUT_NAME}' — using SAP default")

    if not trigger_export(session):
        raise RuntimeError("Could not trigger export (no path worked).")

    xlsx_path = ""
    for _ in range(50):
        xlsx_path = newest_export_xlsx(export_start_ts)
        if xlsx_path and os.path.exists(xlsx_path):
            s1 = os.path.getsize(xlsx_path); wait(0.8)
            s2 = os.path.getsize(xlsx_path)
            if s2 >= s1 and s2 > 500:
                break
        wait(0.6)
    if not xlsx_path:
        # Oct-22 diagnostic: dump every recently-modified file in
        # SAP_EXPORT_DIR so a "file not found" failure shows what SAP
        # actually wrote (if anything).  Often reveals one of:
        #   • SAP wrote the file but with an unexpected name (e.g.
        #     "ZMMR40430.xlsx" instead of "EXPORT_<ts>.xlsx") — our
        #     newest_export_xlsx now matches *.xlsx too but a legacy
        #     pattern mismatch would still show here.
        #   • SAP opened Excel INLINE (XXL path) and no file ever
        #     hit disk — the folder listing stays empty.
        #   • A dialog is still open waiting for user input — then
        #     this listing shows the previous run's file, not today's.
        print(f"\n  ==== C:\\temp listing (± 5 min window) ====")
        recent = _list_recent_xlsx(export_start_ts)
        if not recent:
            print(f"    (nothing modified in the last 5 min)")
        for mtime, p, size in recent:
            try:
                ts_str = datetime.fromtimestamp(mtime).isoformat()
            except Exception:
                ts_str = "?"
            print(f"    {ts_str}  {size:>10,}  {os.path.basename(p)}")
        print(f"  ==== end listing ====\n")
        raise RuntimeError(f"SAP export XLSX not found in {SAP_EXPORT_DIR}")

    _kill_excel("before copy of rt_stock XLSX")
    import shutil
    shutil.copy2(xlsx_path, OUT_XLSX)
    print(f"  rt_stock XLSX -> {OUT_XLSX}")
    _kill_excel("after copy of rt_stock XLSX")

    # Convert to a dated CSV so the loader can pick the newest file
    # by default without needing a specific filename.  Also drops a
    # convenience alias rt_stock.csv for one-off manual testing.
    _kill_excel("pre-convert: release lock before openpyxl load")
    dated_csv = _dated_csv_path(valid_dt)
    n = xlsx_to_csv(OUT_XLSX, dated_csv)
    try:
        shutil.copy2(dated_csv, os.path.join(UNLOCK_DIR, "rt_stock.csv"))
    except Exception: pass
    _kill_excel("post-convert: close every Excel window on the box")

    size = os.path.getsize(dated_csv)
    if size < MIN_CSV_SIZE:
        raise RuntimeError(f"rt_stock CSV too small ({size} bytes).")
    print(f"\nDone.  {n:,} rows written to {dated_csv}.")
    print(f"Run load_csv_to_mysql.py next to push into MySQL stock_rt table.")


if __name__ == "__main__":
    main()
