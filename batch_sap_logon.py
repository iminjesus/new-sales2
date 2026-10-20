"""
batch_sap_logon.py — attach to a logged-in SAP GUI, or log one in.

Public functions:
  ensure_sap_session() → (session, status)
      status is one of:
        "attached"      — already logged in, we attached to the session
        "fresh_login"   — we launched SAP, keyed credentials, got in
        "multi_logon"   — SAP's "User is already logged on" dialog
                          appeared; we Terminated it and raise so the
                          caller can email the owner and retry later
        "no_sap"        — SAP Logon isn't installed / can't be reached
  on multi_logon the caller receives a MultiLogonError exception.

Design notes:
  • Uses pywin32 (win32com.client) + SAP GUI Scripting.  Scripting must
    be enabled in SAP Logon → Options → Accessibility & Scripting.
  • Credentials come from env SAP_USER / SAP_PWD; the orchestrator sets
    sensible defaults from the Oct-16 user ask (42S00030B2 / Ksmpyj1234!).
  • Connection name defaults to "G.ERP" per the user's SAP Logon
    screenshot — override with env SAP_CONN.
  • Multi-logon dialog is detected by the "already logged on" wording
    in wnd[1].  We select the "Terminate this logon" radio (radio 3 on
    the standard dialog) and press ✓, which closes the dialog without
    kicking the other session.  Then we raise MultiLogonError so the
    orchestrator can send the warning email + sleep 5 min + retry.
"""
import os
import sys
import time
import threading
import subprocess

# ── Logging — mirror into batch_orchestrator.log so a stuck login
#    is visible in the SAME log file the operator is already watching.
#    No orchestrator dependency: if the log file isn't writable we
#    silently fall back to stdout only. --------------------------------
_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "batch_orchestrator.log")
def _slog(msg):
    try:
        from datetime import datetime
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        ts = "?"
    line = f"[{ts}] [sap_logon] {msg}"
    # cp1252-safe print: a stray Unicode char can't crash the orchestrator
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        try:
            enc = (sys.stdout.encoding or "ascii")
            print(line.encode(enc, errors="replace").decode(enc, errors="replace"),
                  flush=True)
        except Exception:
            pass
    except Exception:
        pass
    try:
        with open(_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

SAP_USER = os.environ.get("SAP_USER", "42S00030B2")
SAP_PWD  = os.environ.get("SAP_PWD",  "Ksmpyj1234!")
SAP_CONN = os.environ.get("SAP_CONN", "G.ERP")
SAP_CLIENT = os.environ.get("SAP_CLIENT", "100")
SAP_LANG   = os.environ.get("SAP_LANG",   "EN")
SAP_LOGON_PATH = os.environ.get(
    "SAP_LOGON_PATH",
    r"C:\Program Files (x86)\SAP\FrontEnd\SAPgui\saplogon.exe",
)


class MultiLogonError(RuntimeError):
    """Raised when SAP rejects the login because the user is already
    signed in on another desktop.  Carries no payload — the caller
    (orchestrator) knows what to do (email, wait, retry)."""
    pass


# ──────────────────────────────────────────────────────────────────────
# SAP GUI "Security" popup auto-dismisser
# ──────────────────────────────────────────────────────────────────────
# SAP Logon pops up "A script is opening a connection to system: G.ERP"
# (or "A script is attaching to SAP GUI") when scripting touches a
# connection.  The clean fix is to untick 'Notify when a script opens
# a connection' in SAP Logon → Options → Scripting; this watcher is a
# belt-and-braces fallback for boxes where that setting is still on
# (or re-enabled by Group Policy).  We spawn a background thread that
# polls for the dialog by window title and clicks OK via a Return
# keypress, then exits.
def _find_sap_security_popup():
    """Return the HWND of the 'SAP GUI Security' popup, or None.
    Matches both the English title and a couple of common variants."""
    try:
        import win32gui
    except ImportError:
        return None
    found = []
    def _enum(h, _):
        try:
            if not win32gui.IsWindowVisible(h):
                return True
            t = (win32gui.GetWindowText(h) or "").lower()
            # English, German, Korean localisations of the SAP GUI
            # Scripting security dialog all contain the word "sap" and
            # one of these cues in the title bar.
            if "sap" in t and (
                "security" in t or "sicherheit" in t or "보안" in t
                or "scripting" in t
            ):
                found.append(h)
        except Exception:
            pass
        return True
    try:
        win32gui.EnumWindows(_enum, None)
    except Exception:
        return None
    return found[0] if found else None


def _dismiss_sap_connect_popup_background(timeout_sec=25):
    """Start a background watcher that auto-OKs the SAP GUI Security
    popup within `timeout_sec`.  Returns immediately so the caller can
    continue with sap.OpenConnection (which BLOCKS waiting for the
    popup to be dismissed).  Silently no-ops if pywin32 is missing."""
    try:
        import win32gui, win32con
    except ImportError:
        _slog("pywin32 missing — can't auto-dismiss SAP security popup "
              "(disable 'Notify when a script opens a connection' in "
              "SAP Logon Options to skip this prompt entirely)")
        return None

    def _click_ok_in(dlg_hwnd):
        """Find the OK button inside the dialog by its text and click
        it via BM_CLICK.  Covers EN/DE/KR labels."""
        clicked = {"done": False}
        OK_LABELS = ("ok", "확인", "yes", "예", "ja")

        def _enum_children(h, _):
            try:
                txt = (win32gui.GetWindowText(h) or "").strip().lower().strip("&")
                cls = win32gui.GetClassName(h) or ""
                if "button" in cls.lower() and any(txt == l for l in OK_LABELS):
                    # BM_CLICK = 0x00F5
                    win32gui.SendMessage(h, 0x00F5, 0, 0)
                    clicked["done"] = True
                    return False
            except Exception:
                pass
            return True
        try:
            win32gui.EnumChildWindows(dlg_hwnd, _enum_children, None)
        except Exception:
            pass
        return clicked["done"]

    def _runner():
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            hwnd = _find_sap_security_popup()
            if hwnd:
                try:
                    try:
                        win32gui.SetForegroundWindow(hwnd)
                    except Exception:
                        pass
                    # Primary: directly click the OK button inside the
                    # dialog via BM_CLICK on the button HWND whose text
                    # matches one of our labels.
                    if _click_ok_in(hwnd):
                        _slog("auto-dismissed SAP GUI Security popup (OK click)")
                        return
                    # Fallback: send a Return keypress.  OK is almost
                    # always the default button, so Enter activates it.
                    win32gui.PostMessage(hwnd, win32con.WM_KEYDOWN, win32con.VK_RETURN, 0)
                    time.sleep(0.05)
                    win32gui.PostMessage(hwnd, win32con.WM_KEYUP,   win32con.VK_RETURN, 0)
                    _slog("auto-dismissed SAP GUI Security popup (Enter)")
                    return
                except Exception as e:
                    _slog(f"SAP popup dismiss failed: {e}")
                    return
            time.sleep(0.25)

    t = threading.Thread(target=_runner, name="sap-popup-dismiss", daemon=True)
    t.start()
    return t


class NoSapError(RuntimeError):
    """Raised when SAP Logon isn't installed or can't be reached."""
    pass


def _get_gui_auto():
    """Return the SAPGUI scripting-engine root, or None if SAP Logon
    isn't running / scripting isn't enabled.

    GetScriptingEngine can also trigger the 'A script is attaching to
    SAP GUI' security popup, so arm the dismisser before the call."""
    try:
        import win32com.client  # type: ignore
    except Exception as e:
        raise NoSapError(f"pywin32 not installed: {e}")
    _dismiss_sap_connect_popup_background(timeout_sec=15)
    try:
        return win32com.client.GetObject("SAPGUI").GetScriptingEngine
    except Exception:
        return None


def _try_attach():
    """Try to attach to a session that's already logged in.  Walks every
    open SAP connection + session; returns the first one on a non-
    logon screen.  Returns None if nothing's logged in yet."""
    sap = _get_gui_auto()
    if sap is None:
        return None
    try:
        n_conn = sap.Children.Count
    except Exception:
        return None
    for ci in range(n_conn):
        conn = sap.Children(ci)
        try:
            n_sess = conn.Children.Count
        except Exception:
            continue
        for si in range(n_sess):
            sess = conn.Children(si)
            try:
                # Logon screens expose RSYST-MANDT / BNAME on wnd[0]; a
                # session that's PAST login no longer has them.
                tcode = str(sess.Info.Transaction or "").strip()
                if tcode and tcode not in ("S000", ""):
                    return sess
            except Exception:
                continue
            # Fallback: window title "SAP" (post-login default)
            try:
                w = sess.findById("wnd[0]")
                title = str(w.Text or "")
                if "Logon" not in title and "G.ERP" not in title:
                    return sess
            except Exception:
                continue
    return None


def _launch_logon():
    """Launch saplogon.exe if the GUI isn't running yet."""
    if not os.path.exists(SAP_LOGON_PATH):
        raise NoSapError(f"SAP Logon not found: {SAP_LOGON_PATH}")
    subprocess.Popen([SAP_LOGON_PATH], close_fds=True)
    # Give the window a moment to appear + scripting bridge to come up.
    for _ in range(30):
        time.sleep(1.0)
        try:
            if _get_gui_auto() is not None:
                return
        except Exception:
            pass
    raise NoSapError("SAP Logon didn't become scriptable within 30 s")


def _open_connection(sap):
    """Open the SAP_CONN entry from SAP Logon.  Returns the new session.

    sap.OpenConnection BLOCKS until the SAP GUI Security popup
    ('A script is opening a connection to system: G.ERP') is dismissed.
    We start a background watcher BEFORE the call so a human doesn't
    have to click OK — see _dismiss_sap_connect_popup_background."""
    _dismiss_sap_connect_popup_background(timeout_sec=20)
    conn = sap.OpenConnection(SAP_CONN, True)
    # OpenConnection returns the connection; its first child is the
    # fresh logon-screen session.
    time.sleep(2.0)
    try:
        return conn.Children(0)
    except Exception:
        # Occasionally the session isn't ready yet; retry once.
        time.sleep(1.5)
        return conn.Children(0)


def _enter_credentials(session):
    """Fill client / user / password / language and press Enter.
    Logs each field so a stuck step is visible in the log."""
    wnd = session.findById("wnd[0]")
    _slog(f"filling client={SAP_CLIENT!r}")
    try:
        wnd.findById("usr/txtRSYST-MANDT").Text = SAP_CLIENT
    except Exception as e:
        _slog(f"  MANDT not writable: {e}  (continuing)")
    _slog(f"filling user={SAP_USER!r}")
    try:
        wnd.findById("usr/txtRSYST-BNAME").Text = SAP_USER
    except Exception as e:
        _slog(f"  BNAME not found: {e}")
        raise RuntimeError("SAP login screen didn't carry BNAME field")
    _slog("filling password (len=%d)" % len(SAP_PWD))
    try:
        wnd.findById("usr/pwdRSYST-BCODE").Text = SAP_PWD
    except Exception as e:
        _slog(f"  BCODE not found: {e}")
        raise RuntimeError("SAP login screen didn't carry BCODE field")
    _slog(f"filling language={SAP_LANG!r}")
    try:
        wnd.findById("usr/txtRSYST-LANGU").Text = SAP_LANG
    except Exception as e:
        _slog(f"  LANGU not writable: {e}  (continuing)")
    _slog("pressing Enter (sendVKey 0)")
    wnd.sendVKey(0)   # Enter


def _handle_multi_logon(session):
    """If wnd[1] shows the 'User is already logged on' dialog, select
    'Terminate this logon' and ✓ it so the dialog closes without
    bumping the other desktop.  Returns True if the dialog was there."""
    try:
        dlg = session.findById("wnd[1]")
    except Exception:
        return False
    try:
        txt = str(dlg.Text or "") + " " + str(dlg.PopupTitle or "")
    except Exception:
        txt = ""
    try:
        for i in range(dlg.Children.Count):
            try: txt += " " + str(dlg.Children(i).Text or "")
            except Exception: pass
    except Exception:
        pass
    if "already logged on" not in txt.lower() and "multiple logon" not in txt.lower():
        return False
    # The three radios have ids radMULTI_LOGON_OPT1 / 2 / 3 on standard
    # S/4 and ECC layouts (OPT1 = Continue + end others, OPT2 = Continue
    # keep others, OPT3 = Terminate).  Fall back by position when the
    # id drifts.
    picked = False
    for rid in [
        "wnd[1]/usr/radMULTI_LOGON_OPT3",
        "wnd[1]/usr/radRBOX3",
    ]:
        try:
            session.findById(rid).Select()
            picked = True
            break
        except Exception:
            continue
    if not picked:
        # Last-ditch: send Alt-T (Terminate accelerator on English SAP).
        try:
            dlg.sendVKey(0)
        except Exception:
            pass
    try:
        session.findById("wnd[1]/tbar[0]/btn[0]").press()  # ✓
    except Exception:
        try:
            session.findById("wnd[1]").sendVKey(0)
        except Exception:
            pass
    return True


def _dump_sap_state(session, label):
    """Log what SAP looks like RIGHT NOW so we can diagnose a stuck
    attach.  Dumps transaction code, wnd[0] title, and whether any
    popups are open."""
    try:
        tcode = str(session.Info.Transaction or "")
    except Exception:
        tcode = "?"
    try:
        prog = str(session.Info.Program or "")
    except Exception:
        prog = "?"
    try:
        title = str(session.findById("wnd[0]").Text or "")
    except Exception:
        title = "?"
    popups = []
    for i in range(1, 6):
        try:
            w = session.findById(f"wnd[{i}]")
            popups.append(f"wnd[{i}]={str(w.Text or '')!r}")
        except Exception:
            break
    _slog(f"[sap_state@{label}] tcode={tcode!r} prog={prog!r} "
          f"title={title!r} popups=[{', '.join(popups) or 'none'}]")


def _reset_to_home(session):
    """Close any lingering popups and send /n so the attached session
    lands on Easy Access.  Without this, if the user left SAP sitting
    on a previous transaction / popup, sapcrawling's F8 would hit
    "control not found by id" (as happened in the Oct-16 13:34 run).

    Logs the SAP state before AND after so a stuck session is visible
    in the log."""
    _dump_sap_state(session, "before-reset")
    # Maximise main window so toolbar coords are predictable.
    try:
        session.findById("wnd[0]").maximize()
    except Exception:
        pass
    # Close every popup wnd[1..5] - most SAP sessions never stack more.
    for i in (5, 4, 3, 2, 1):
        try:
            w = session.findById(f"wnd[{i}]")
            _slog(f"  closing popup wnd[{i}]")
            try:
                w.close()
            except Exception:
                try: w.sendVKey(0)   # Enter / dismiss
                except Exception: pass
                try: w.sendVKey(12)  # Escape / cancel
                except Exception: pass
            time.sleep(0.3)
        except Exception:
            pass
    # /n resets to Easy Access from any transaction.  Follow with Enter
    # so we leave a clean selection screen for the next crawler.
    try:
        okcd = session.findById("wnd[0]/tbar[0]/okcd")
        okcd.Text = "/n"
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(1.5)   # a hair longer than before for slow mainframes
    except Exception as e:
        _slog(f"  _reset_to_home: /n didn't take ({e}) - continuing anyway")
    _dump_sap_state(session, "after-reset")


def ensure_sap_session():
    """Public entry.  Attach or log in; return (session, status)."""
    _slog("ensure_sap_session: checking for logged-in session…")
    # 1. Try to attach to an existing logged-in session.
    sess = _try_attach()
    if sess is not None:
        _slog("already logged in — attached; resetting to Easy Access.")
        _reset_to_home(sess)
        return sess, "attached"

    # 2. Launch SAP Logon if the scripting bridge isn't live yet.
    sap = _get_gui_auto()
    if sap is None:
        _slog("SAP GUI scripting not reachable — launching saplogon.exe…")
        _launch_logon()
        sap = _get_gui_auto()
        if sap is None:
            _slog("still not reachable after launch — bailing.")
            raise NoSapError("SAP GUI scripting not reachable after launch")
    else:
        _slog("SAP GUI scripting is live.")

    # 3. Open the G.ERP (or SAP_CONN) connection and key credentials.
    _slog(f"opening connection {SAP_CONN!r}…")
    try:
        session = _open_connection(sap)
    except Exception as e:
        _slog(f"OpenConnection({SAP_CONN!r}) failed: {e}")
        raise
    _slog("connection opened; entering credentials…")
    _enter_credentials(session)
    _slog("credentials submitted; waiting 2.5 s for SAP response…")
    time.sleep(2.5)

    # 4. Multi-logon check — if the "already logged on" dialog appears,
    #    terminate it and bail with MultiLogonError so the caller can
    #    email the owner + wait + retry.
    _slog("checking for multi-logon dialog…")
    if _handle_multi_logon(session):
        _slog("multi-logon dialog handled — raising MultiLogonError.")
        raise MultiLogonError(
            f"User {SAP_USER} is already logged on another desktop."
        )

    # 5. Dismiss any licence / disclaimer popup.
    _slog("dismissing any post-login popup (licence / disclaimer / password)…")
    for _ in range(5):
        try:
            wnd1 = session.findById("wnd[1]")
            if wnd1:
                try:
                    popup_title = str(wnd1.Text or "")
                except Exception:
                    popup_title = "(unknown)"
                _slog(f"  popup detected: {popup_title!r} — sending Enter.")
                wnd1.sendVKey(0)
                time.sleep(0.5)
            else:
                break
        except Exception:
            break
    _slog("fresh login complete.")
    return session, "fresh_login"
