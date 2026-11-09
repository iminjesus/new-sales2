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
    sensible defaults — Oct-16: 42S00030B2 / Ksmpyj1234! ; Nov-01 user
    rotated the password to Ksmpyj1234!! (added one trailing '!').  If
    SAP demands a change at login, _handle_password_change tries the
    SAP_NEW_PWD_CANDIDATES chain in order (Ksmpyj1234!!! then
    Ksmpyj1234!) — three BAD login attempts lock the SAP account so
    the chain is never applied on login, only inside the change popup.
  • Connection name defaults to "G.ERP" per the user's SAP Logon
    screenshot — override with env SAP_CONN.
  • Multi-logon dialog is detected by the "already logged on" wording
    in wnd[1].  Oct-23 user ask: the batch now has PRIORITY — we pick
    "Continue with this logon and end any other logons" (OPT1 radio)
    so this seat wins and the OTHER desktop's session is terminated.
    No MultiLogonError is raised; the orchestrator continues as if
    this were a normal fresh_login.
  • If an ATTACHED session is detected to have been killed by another
    logon (SAP's "Back-end session closed by system" popup), we
    dismiss the popup and fall through to a fresh login, which then
    hits the multi-logon dialog and steals the seat back.
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
# Nov-01: rotated from "Ksmpyj1234!" -> "Ksmpyj1234!!" (added one '!').
# If the account is rotated again, update this default AND the first
# entry of SAP_NEW_PWD_CANDIDATES below.
_SAP_PWD_DEFAULT = "Ksmpyj1234!!"
# Nov-01 user ask (part 2): persist a successful password change to a
# local sidecar file so the NEXT batch run picks up the new password
# automatically — no manual file edit required.  Sidecar lives next
# to this .py (same directory used by the log file).  It is git-
# ignored (see .gitignore entry ".sap_pwd_current").  Resolution
# priority on each run:
#   1. env var SAP_PWD           (explicit operator override)
#   2. sidecar file               (last known good from auto-rotation)
#   3. _SAP_PWD_DEFAULT constant  (the version baked into this file)
_SAP_PWD_SIDECAR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    ".sap_pwd_current",
)
def _load_persisted_pwd():
    """Read the sidecar file if present and return the stored
    password string, or None if the file does not exist / is empty /
    is unreadable.  Logs are deferred — _slog is not yet defined at
    module-load time on first import, so we only print() errors."""
    try:
        if not os.path.isfile(_SAP_PWD_SIDECAR):
            return None
        with open(_SAP_PWD_SIDECAR, "r", encoding="utf-8") as f:
            pwd = f.read().strip()
        return pwd or None
    except Exception as e:
        print(f"[sap_pwd] WARNING: could not read sidecar "
              f"{_SAP_PWD_SIDECAR}: {e}", flush=True)
        return None
def _save_persisted_pwd(pwd):
    """Atomically write the new password to the sidecar file.  Uses
    tmp+rename so a mid-write crash cannot leave a half-written file.
    Called only from _handle_password_change on a successful rotation."""
    try:
        tmp = _SAP_PWD_SIDECAR + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(pwd)
        os.replace(tmp, _SAP_PWD_SIDECAR)
        try:
            # Tighten permissions on POSIX so the sidecar is readable
            # only by the owner.  No-op on Windows (chmod bits are
            # mostly ignored there) but harmless.
            os.chmod(_SAP_PWD_SIDECAR, 0o600)
        except Exception:
            pass
        return True
    except Exception as e:
        print(f"[sap_pwd] WARNING: could not write sidecar "
              f"{_SAP_PWD_SIDECAR}: {e}", flush=True)
        return False
SAP_PWD = (
    os.environ.get("SAP_PWD")
    or _load_persisted_pwd()
    or _SAP_PWD_DEFAULT
)
SAP_CONN = os.environ.get("SAP_CONN", "G.ERP")
# Nov-01 user ask: when SAP's "please change your password" dialog
# appears at login, rotate THROUGH a known universe of 3 passwords
# that only differ in the trailing '!' count.  The currently-active
# password is excluded from the proposal (SAP would reject it as
# "same as current"), and the remaining two are tried in ring order
# starting from "the next one after current".  Example:
#   current = "Ksmpyj1234!!"   -> try ["Ksmpyj1234!!!", "Ksmpyj1234!"]
#   current = "Ksmpyj1234!!!"  -> try ["Ksmpyj1234!",  "Ksmpyj1234!!"]
#   current = "Ksmpyj1234!"    -> try ["Ksmpyj1234!!", "Ksmpyj1234!!!"]
# Submitting a candidate inside the already-open change dialog does
# NOT count against SAP's "3 wrong logins -> account locked"
# counter, so cycling through 2 candidates is safe.  LOGIN itself is
# still a one-shot (no retry) elsewhere in this module.
SAP_PWD_UNIVERSE = [
    "Ksmpyj1234!",
    "Ksmpyj1234!!",
    "Ksmpyj1234!!!",
]
def _sap_pwd_candidates(current):
    """Return the UNIVERSE minus the currently-active password, in
    ring order starting from (current index + 1).  If `current` is
    not in the universe (e.g. a surprise manual rotation), return the
    whole universe untouched so every known variant still gets tried."""
    try:
        i = SAP_PWD_UNIVERSE.index(current)
    except ValueError:
        return list(SAP_PWD_UNIVERSE)
    n = len(SAP_PWD_UNIVERSE)
    return [SAP_PWD_UNIVERSE[(i + k) % n] for k in range(1, n)]
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
    """Return the HWND of SAP's script-security popup, or None.

    Older SAP GUI builds title the popup 'SAP GUI Security', newer /
    localised ones title it plainly 'SAP Logon' or 'SAP GUI Scripting',
    so a title-only match misses them.  Match by BODY TEXT instead —
    the dialog always contains one of these marker phrases in a static
    control inside the dialog ('A script is opening a connection to
    system: G.ERP', 'A script is attaching to SAP GUI', etc.)."""
    try:
        import win32gui
    except ImportError:
        return None

    # Markers inside the dialog body across EN / DE / KR.  Lower-cased
    # match on the concatenated text of every child static control.
    MARKERS = (
        "script is opening",      # EN: "A script is opening a connection to system: G.ERP"
        "script is attaching",    # EN: "A script is attaching to SAP GUI"
        "connection to system",   # EN fragment (catches variants)
        "attaching to sap",       # EN fragment
        "script öffnet",          # DE: "Ein Skript öffnet eine Verbindung..."
        "skript öffnet",          # DE alt spelling
        "script wird",            # DE fragment
        "스크립트",                # KR: "스크립트가 연결을 열고 있습니다..."
    )

    found = []

    def _enum(h, _):
        try:
            if not win32gui.IsWindowVisible(h):
                return True
            title = (win32gui.GetWindowText(h) or "").lower()
            # Fast-path: title must at least mention "sap" — otherwise
            # we'd scan every window on the desktop for the marker.
            if "sap" not in title:
                return True
            texts = []

            def _collect(child, _):
                try:
                    texts.append((win32gui.GetWindowText(child) or "").lower())
                except Exception:
                    pass
                return True

            try:
                win32gui.EnumChildWindows(h, _collect, None)
            except Exception:
                pass
            # Also include the dialog's own title so an old-style
            # 'SAP GUI Security' popup matches even if its body text
            # is empty.
            texts.append(title)
            full = " ".join(texts)
            if any(m in full for m in MARKERS) or "sap gui security" in full:
                found.append(h)
        except Exception:
            pass
        return True

    try:
        win32gui.EnumWindows(_enum, None)
    except Exception:
        return None
    return found[0] if found else None


def _find_sap_system_message_popup():
    """Find the native Win32 "SAP GUI for Windows" message box that
    appears when another desktop's logon kills this seat ("HKP: SAP
    System Message / Back-end session closed by system") or on any
    comparable blocking SAP system message.  This dialog is NOT a
    SAP Scripting wnd[1] — it's a plain Win32 MessageBox drawn by
    SAP GUI's front-end client, so _check_backend_closed() (which
    walks session.findById) can't see it.

    Oct-28 rework after a reporter saw an operator PC stuck at this
    popup for the overnight 8:00 run: broadened the match so ANY
    top-level window whose title OR body contains "sap" AND an OK
    button finder below hits — not just the specific "Back-end
    session closed" phrase.  Also returns ALL matches so the
    dismisser can clear a stack of them in one pass instead of
    one-popup-per-call.
    """
    try:
        import win32gui
    except ImportError:
        return []
    BODY_MARKERS = (
        "back-end session closed",
        "back end session closed",
        "backend session closed",
        "session closed by system",
        "closed by system",
        "sap system message",
        "system message",
        "connection closed",
        "connection to the server",
    )
    TITLE_MARKERS = (
        "sap gui for windows",
        "sap system message",
        "sap",
    )
    found = []
    def _enum(h, _):
        try:
            if not win32gui.IsWindowVisible(h):
                return True
            title = (win32gui.GetWindowText(h) or "").lower()
            texts = []
            def _collect(child, _c):
                try:
                    texts.append((win32gui.GetWindowText(child) or "").lower())
                except Exception:
                    pass
                return True
            try:
                win32gui.EnumChildWindows(h, _collect, None)
            except Exception:
                pass
            texts.append(title)
            full = " ".join(texts)
            # Match if (a) title explicitly carries a known SAP
            # dialog marker, OR (b) body text contains one of the
            # known session-closed / system-message phrases AND
            # the title has "sap" somewhere.
            title_hit = any(m in title for m in TITLE_MARKERS)
            body_hit  = any(m in full  for m in BODY_MARKERS)
            if (title_hit and body_hit) or ("sap system message" in full):
                found.append(h)
        except Exception:
            pass
        return True
    try:
        win32gui.EnumWindows(_enum, None)
    except Exception:
        return []
    return found


def _dismiss_sap_system_message_popup():
    """Click OK on EVERY "Back-end session closed" / SAP System
    Message Win32 message box currently on screen.  Returns True
    when at least one popup was found and dismissed, False otherwise.
    Non-blocking — bails fast when no such dialog exists.

    Oct-28 rework: a stack of popups (seen when SAP has fired more
    than one system message while the operator was away) is now
    cleared in one call instead of leaving the deeper ones in place.
    Each popup gets four dismissal attempts in order so an edge-case
    client build that ignores BM_CLICK still gets dismissed:
      1. BM_CLICK on the OK button (by text + class)
      2. WM_COMMAND IDOK on the dialog
      3. VK_RETURN keydown/up on the dialog
      4. WM_CLOSE on the dialog
    Logs every step for troubleshooting."""
    try:
        import win32gui, win32con
    except ImportError:
        _slog("pywin32 missing — can't auto-dismiss SAP system-message popup")
        return False
    hwnds = _find_sap_system_message_popup()
    if not hwnds:
        return False
    dismissed_any = False
    for hwnd in hwnds:
        if not win32gui.IsWindow(hwnd) or not win32gui.IsWindowVisible(hwnd):
            continue
        _slog(f"[sap_system_msg] popup hwnd=0x{hwnd:x} "
              f"title={win32gui.GetWindowText(hwnd)!r} — dismissing")
        try:
            try: win32gui.SetForegroundWindow(hwnd)
            except Exception: pass
            # (1) BM_CLICK on the OK button.
            OK_LABELS = ("ok", "확인", "yes", "예", "ja", "da", "close", "닫기")
            clicked = {"done": False}
            def _enum_children(h, _c):
                try:
                    txt = (win32gui.GetWindowText(h) or "").strip().lower().strip("&")
                    cls = win32gui.GetClassName(h) or ""
                    if "button" in cls.lower() and any(txt == l for l in OK_LABELS):
                        win32gui.SendMessage(h, 0x00F5, 0, 0)   # BM_CLICK
                        clicked["done"] = True
                        return False
                except Exception:
                    pass
                return True
            try: win32gui.EnumChildWindows(hwnd, _enum_children, None)
            except Exception: pass
            if clicked["done"]:
                _slog(f"[sap_system_msg]   step 1 ok (BM_CLICK)")
            # (2) WM_COMMAND IDOK (1) on the dialog — covers message
            #     boxes that don't expose their OK button via
            #     EnumChildWindows (owner-drawn / custom).
            time.sleep(0.2)
            if win32gui.IsWindow(hwnd) and win32gui.IsWindowVisible(hwnd):
                try:
                    win32gui.PostMessage(hwnd, win32con.WM_COMMAND, 1, 0)
                    _slog(f"[sap_system_msg]   step 2 ok (WM_COMMAND IDOK)")
                except Exception:
                    pass
            # (3) VK_RETURN keydown/up on the dialog — default-button
            #     fallback.
            time.sleep(0.2)
            if win32gui.IsWindow(hwnd) and win32gui.IsWindowVisible(hwnd):
                try:
                    win32gui.PostMessage(hwnd, win32con.WM_KEYDOWN, win32con.VK_RETURN, 0)
                    time.sleep(0.05)
                    win32gui.PostMessage(hwnd, win32con.WM_KEYUP, win32con.VK_RETURN, 0)
                    _slog(f"[sap_system_msg]   step 3 ok (VK_RETURN)")
                except Exception:
                    pass
            # (4) WM_CLOSE — belt-and-braces.
            time.sleep(0.2)
            if win32gui.IsWindow(hwnd) and win32gui.IsWindowVisible(hwnd):
                try:
                    win32gui.PostMessage(hwnd, win32con.WM_CLOSE, 0, 0)
                    _slog(f"[sap_system_msg]   step 4 ok (WM_CLOSE)")
                except Exception:
                    pass
            dismissed_any = True
        except Exception as e:
            _slog(f"[sap_system_msg] dismiss failed on hwnd=0x{hwnd:x}: {e}")
    return dismissed_any


_SAP_POPUP_WATCHER_STOP = None
def _start_sap_popup_watcher():
    """Oct-28 user ask: a one-shot dismiss at the start of ensure_sap_
    session isn't enough — a stale "Back-end session closed" popup can
    appear a few seconds LATER (SAP's own front-end client re-raises
    it after the connection re-negotiates), and the orchestrator ends
    up blocked at the modal.  Fire a daemon thread that polls for the
    popup every 400 ms and dismisses it the moment it appears.  The
    thread lives until _stop_sap_popup_watcher() is called at the end
    of ensure_sap_session.  Returns a stop handle."""
    import threading
    stop_flag = threading.Event()
    def _runner():
        while not stop_flag.is_set():
            try:
                if _dismiss_sap_system_message_popup():
                    _slog("[sap_popup_watcher] auto-dismissed a system "
                          "message popup during logon attempt")
            except Exception as _e:
                _slog(f"[sap_popup_watcher] poll error: {_e}")
            # 400 ms cadence — fast enough to catch an eye-blink popup
            # before it blocks the scripting bridge, light enough that
            # it never shows up on CPU graphs.
            stop_flag.wait(0.4)
    t = threading.Thread(target=_runner, name="sap-popup-watcher",
                         daemon=True)
    t.start()
    return stop_flag
def _stop_sap_popup_watcher(stop_flag):
    try:
        if stop_flag is not None:
            stop_flag.set()
    except Exception:
        pass


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
    _dismiss_sap_connect_popup_background(timeout_sec=60)
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
    _dismiss_sap_connect_popup_background(timeout_sec=60)
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
    """If any popup window shows the 'User is already logged on' /
    'License Information for Multiple Logon' dialog, pick
    "Continue with this logon and end any other logons" (OPT1) and ✓
    it so THIS seat wins — the OTHER desktop's session is terminated.

    Oct-23 user ask: "이쪽이 우선이 되서 다른 login을 죽이고라도
    들어가도록" — the batch has priority; a late operator logon from
    another desktop must NOT kick the orchestrator off.  Flipped from
    OPT3 (give up here) to OPT1 (end others, keep this).

    Oct-28 user ask: screenshot shows title "License Information for
    Multiple Logon" and the default radio is OPT3 (Terminate).  Make
    the detection scan wnd[1..3], add "license information" as a
    title marker, and add more OPT1 radio-ID fallbacks so we never
    accidentally take the SAP default (which would kill THIS seat).

    Returns True when the dialog was present and handled."""
    # Oct-28: iterate every popup window — SAP occasionally puts the
    # multi-logon dialog on wnd[2] when wnd[1] is already used for a
    # licence/disclaimer panel.
    # Nov-09: widened the scan range to wnd[1..5] because a reported
    # morning failure pattern (download never kicks off until manual
    # stop+start) is consistent with a multi-logon dialog landing on
    # a window index the handler didn't check (licence panel + GDPR
    # consent + news-of-the-day can push the real dialog to wnd[4]).
    for wi in (1, 2, 3, 4, 5):
        try:
            dlg = session.findById(f"wnd[{wi}]")
        except Exception:
            continue
        try:
            txt = str(dlg.Text or "") + " " + str(getattr(dlg, "PopupTitle", "") or "")
        except Exception:
            txt = ""
        try:
            for i in range(dlg.Children.Count):
                try: txt += " " + str(dlg.Children(i).Text or "")
                except Exception: pass
        except Exception:
            pass
        _tl = txt.lower()
        # Oct-28: added "license information" / "licence information" —
        # the dialog's own title bar carries that phrase even though the
        # body says "already logged on".  Keeping the older markers so
        # SAP variants that only say "multiple logon" still trip the branch.
        if ("already logged on" not in _tl
                and "multiple logon" not in _tl
                and "license information" not in _tl
                and "licence information" not in _tl):
            continue
        _slog(f"[multi_logon] dialog present on wnd[{wi}] — selecting "
              f"OPT1 (end OTHER logons, keep this seat)")
        # radMULTI_LOGON_OPT1 = Continue with this logon and end any other
        # logons in this system.  radRBOX1 is the positional fallback that
        # some SAP releases use.  Previously we picked OPT3 (terminate
        # this logon) which gave up the seat — now we take it.
        picked = False
        for rid in [
            f"wnd[{wi}]/usr/radMULTI_LOGON_OPT1",
            f"wnd[{wi}]/usr/radRBOX1",
            # Oct-28 additional fallbacks seen in older/newer SAP
            # releases:
            f"wnd[{wi}]/usr/radMULTI_LOGON_OPT_1",
            f"wnd[{wi}]/usr/radRB_LOGON_OPT1",
            f"wnd[{wi}]/usr/radMULTI_LOGON1",
        ]:
            try:
                session.findById(rid).Select()
                picked = True
                _slog(f"[multi_logon] selected radio {rid}")
                break
            except Exception:
                continue
        if not picked:
            # Oct-28 last-ditch: walk the dialog's children for the
            # first radio-button-like control and select it (visual
            # order is OPT1 → OPT2 → OPT3, so the first radio is
            # always OPT1).
            try:
                def _first_radio(ctrl):
                    try:
                        if str(getattr(ctrl, "Type", "")).lower() == "guiradiobutton":
                            return ctrl
                        for ci in range(ctrl.Children.Count):
                            r = _first_radio(ctrl.Children(ci))
                            if r is not None:
                                return r
                    except Exception:
                        pass
                    return None
                r = _first_radio(dlg)
                if r is not None:
                    r.Select()
                    picked = True
                    _slog(f"[multi_logon] selected first radio control (walk) "
                          f"{getattr(r, 'Id', '?')}")
            except Exception as _e:
                _slog(f"[multi_logon] walk-for-radio failed: {_e!r}")
        if not picked:
            _slog("[multi_logon] OPT1 radio not found — SAP default is "
                  "OPT3 (Terminate), which would kill this seat.  "
                  "Pressing ✓ anyway so the flow does not hang; the "
                  "subsequent attach will discover the failure and retry.")
        try:
            session.findById(f"wnd[{wi}]/tbar[0]/btn[0]").press()  # ✓
        except Exception:
            try:
                session.findById(f"wnd[{wi}]").sendVKey(0)
            except Exception:
                pass
        return True
    return False


def _handle_password_change(session):
    """Nov-01 user ask: when SAP shows the "please choose a new
    password" dialog at login (password expired / policy change / forced
    rotation), feed it the candidates from SAP_NEW_PWD_CANDIDATES in
    order until one is accepted.  The change-password popup requires
    the NEW password to be typed TWICE (new + confirm).

    Login itself is NEVER retried from here -- three wrong LOGIN
    attempts lock the SAP account.  This handler only drives the
    change-password dialog, which does not consume failed-login
    attempts; SAP just redraws the dialog on each policy rejection
    (e.g. "password used recently") and we try the next candidate.

    On success:
      - module global SAP_PWD is updated so the rest of this session
        (and any immediate re-login this process does) uses the new one;
      - a loud WARN line is written to the log asking the operator to
        update the SAP_PWD default in batch_sap_logon.py BEFORE the
        next scheduled run so file + account stay in sync.

    Returns True when the dialog was present and successfully handled
    (new password accepted), False when no change dialog was found.
    Raises RuntimeError if the dialog is present but every candidate
    is rejected -- the orchestrator should stop and alert in that case.
    """
    global SAP_PWD
    # Scan wnd[1..3] for a password-change popup.  Detect by title /
    # body keywords because SAP's text varies by release / language
    # (English "SAP Password Change" or "Change Password", German
    # "Kennwort aendern", etc.).
    target_wi = None
    for wi in (1, 2, 3):
        try:
            dlg = session.findById(f"wnd[{wi}]")
        except Exception:
            continue
        try:
            txt = str(dlg.Text or "") + " " + str(getattr(dlg, "PopupTitle", "") or "")
        except Exception:
            txt = ""
        try:
            for i in range(dlg.Children.Count):
                try: txt += " " + str(dlg.Children(i).Text or "")
                except Exception: pass
        except Exception:
            pass
        _tl = txt.lower()
        # Keyword set covers English + German + Korean fallbacks.
        # "neues kennwort" = German "new password"; the Korean set
        # covers both the SAP-translated "비밀번호" and the loanword
        # "패스워드" plus the native "암호", with change / expired /
        # new / reset verbs ("변경"/"만료"/"신규"/"새"/"재설정").
        # The user's SAP is normally English (SAP_LANG=EN) but the
        # change dialog sometimes falls back to the user profile's
        # language setting, so keeping all three is cheap insurance.
        has_pwd_word    = (
            ("password" in _tl)
            or ("kennwort" in _tl)
            or ("비밀번호" in txt)
            or ("패스워드" in txt)
            or ("암호" in txt)
        )
        has_change_word = (
            ("change" in _tl) or ("new" in _tl)
            or ("expired" in _tl) or ("aendern" in _tl)
            or ("neues" in _tl)
            or ("변경" in txt) or ("만료" in txt) or ("신규" in txt)
            or ("새" in txt)   or ("재설정" in txt) or ("지난" in txt)
        )
        if has_pwd_word and has_change_word:
            target_wi = wi
            break
    if target_wi is None:
        return False  # no change dialog present
    _slog(f"[pwd_change] change-password dialog detected on wnd[{target_wi}]")

    # SAP's standard field IDs for the change-password dialog.  Try
    # them in order; if none match, fall back to a tree walk that
    # picks the first two GuiPasswordField controls under /usr.
    STD_NEW_IDS = [
        f"wnd[{target_wi}]/usr/pwdRSYST-NCODE",
        f"wnd[{target_wi}]/usr/pwdV01-BCODE",
        f"wnd[{target_wi}]/usr/pwdRSYST-NEWC",
    ]
    STD_CONF_IDS = [
        f"wnd[{target_wi}]/usr/pwdRSYST-NCOD2",
        f"wnd[{target_wi}]/usr/pwdV02-BCODE",
        f"wnd[{target_wi}]/usr/pwdRSYST-NEWC2",
    ]

    def _find_field(id_list):
        for fid in id_list:
            try:
                return session.findById(fid)
            except Exception:
                continue
        return None

    def _find_pwd_fields_walk():
        """Fallback: walk the dialog's children and return the first
        two GuiPasswordField controls under /usr."""
        found = []
        try:
            usr = session.findById(f"wnd[{target_wi}]/usr")
        except Exception:
            return (None, None)
        def _walk(ctrl):
            try:
                if str(getattr(ctrl, "Type", "")).lower() == "guipasswordfield":
                    found.append(ctrl)
                    if len(found) >= 2:
                        return
                for ci in range(ctrl.Children.Count):
                    _walk(ctrl.Children(ci))
                    if len(found) >= 2:
                        return
            except Exception:
                pass
        _walk(usr)
        if len(found) >= 2:
            return (found[0], found[1])
        return (None, None)

    # Nov-01: dynamic ring rotation — compute candidates from the
    # currently-active SAP_PWD so we never propose the one SAP would
    # reject as "same as current".  See _sap_pwd_candidates docstring.
    _candidates = _sap_pwd_candidates(SAP_PWD)
    _slog(f"[pwd_change] ring rotation candidates "
          f"(current pwd len={len(SAP_PWD)}): "
          f"{len(_candidates)} to try")
    for cand_i, cand in enumerate(_candidates, 1):
        _slog(f"[pwd_change] trying candidate {cand_i}/{len(_candidates)} "
              f"(len={len(cand)})")
        new_f  = _find_field(STD_NEW_IDS)
        conf_f = _find_field(STD_CONF_IDS)
        if new_f is None or conf_f is None:
            _slog("[pwd_change] standard field IDs not found, walking tree…")
            new_f, conf_f = _find_pwd_fields_walk()
        if new_f is None or conf_f is None:
            _slog("[pwd_change] ERROR: could not locate NEW / CONFIRM "
                  "password fields on the change dialog")
            raise RuntimeError("SAP password-change dialog present but "
                               "NEW/CONFIRM password fields could not be located")
        try:
            new_f.Text  = cand
            conf_f.Text = cand
        except Exception as e:
            _slog(f"[pwd_change] filling fields failed: {e}")
            raise
        # Press the ✓ (green tick) button to submit.  Fall back to Enter
        # if the toolbar button is not addressable.
        try:
            session.findById(f"wnd[{target_wi}]/tbar[0]/btn[0]").press()
        except Exception:
            try:
                session.findById(f"wnd[{target_wi}]").sendVKey(0)
            except Exception as e:
                _slog(f"[pwd_change] submit failed: {e}")
                raise
        # Give SAP a beat to react (accept or redraw with an error).
        time.sleep(1.2)
        # Did the dialog go away?  If yes, the candidate was accepted.
        still_present = False
        try:
            session.findById(f"wnd[{target_wi}]")
            # Window still exists — check if it's STILL a password dialog
            # (SAP sometimes keeps the window and redraws with an error
            # message like "password matches an earlier one").
            try:
                dlg2 = session.findById(f"wnd[{target_wi}]")
                txt2 = str(dlg2.Text or "")
                try:
                    for i in range(dlg2.Children.Count):
                        try: txt2 += " " + str(dlg2.Children(i).Text or "")
                        except Exception: pass
                except Exception:
                    pass
                if ("password" in txt2.lower()) or ("kennwort" in txt2.lower()):
                    still_present = True
            except Exception:
                pass
        except Exception:
            still_present = False  # window gone -> accepted
        if not still_present:
            _slog(f"[pwd_change] *** SUCCESS *** candidate {cand_i} accepted")
            # Update in-process global so any immediate re-login uses
            # the new password, THEN persist to the sidecar so the
            # next batch run picks it up automatically (no manual
            # file edit).  Sidecar write is best-effort — if it fails
            # we still log the loud banner so an operator can rescue
            # the situation manually.
            SAP_PWD = cand
            saved = _save_persisted_pwd(cand)
            _slog("[pwd_change] " + "*" * 60)
            if saved:
                _slog(f"[pwd_change] *** SAP password rotated and PERSISTED to ***")
                _slog(f"[pwd_change] *** {_SAP_PWD_SIDECAR}")
                _slog(f"[pwd_change] *** Next batch run will read it automatically. ***")
                _slog(f"[pwd_change] *** (Optional: update _SAP_PWD_DEFAULT in     ***")
                _slog(f"[pwd_change] ***  batch_sap_logon.py line ~71 if you want  ***")
                _slog(f"[pwd_change] ***  the git-tracked baseline in sync too.)   ***")
            else:
                _slog(f"[pwd_change] *** OPERATOR ACTION REQUIRED ***")
                _slog(f"[pwd_change] *** SAP password was rotated to a NEW value   ***")
                _slog(f"[pwd_change] *** but the sidecar write FAILED — the next   ***")
                _slog(f"[pwd_change] *** scheduled run will try the OLD password   ***")
                _slog(f"[pwd_change] *** and risk a lockout.  Update SAP_PWD env   ***")
                _slog(f"[pwd_change] *** var OR _SAP_PWD_DEFAULT in                ***")
                _slog(f"[pwd_change] *** batch_sap_logon.py BEFORE the next run.   ***")
            _slog("[pwd_change] " + "*" * 60)
            return True
        _slog(f"[pwd_change] candidate {cand_i} REJECTED by SAP (dialog "
              f"redrawn) — trying next…")
    # Every candidate rejected — bail out so the orchestrator can alert.
    _slog("[pwd_change] all candidates rejected; stopping")
    raise RuntimeError(
        f"SAP demanded a password change but every candidate in "
        f"SAP_PWD_UNIVERSE ({len(_candidates)} tried) was rejected.  "
        f"The ring may have cycled past SAP's password-history window — "
        f"rotate the password manually in SAP GUI, then update "
        f"_SAP_PWD_DEFAULT in batch_sap_logon.py (and SAP_PWD_UNIVERSE "
        f"if the rotation scheme itself changed)."
    )


def _check_backend_closed(session):
    """Oct-23 user ask: detect the "HKP: SAP System Message / Back-end
    session closed by system" popup that appears when another desktop's
    logon kicks THIS session off the server.  When found: dismiss the
    popup and return True so the caller tears down this COM session
    and re-logs in (which will trigger _handle_multi_logon to steal
    the seat back).  Returns False when no such popup is present."""
    for wi in (1, 2, 3):
        try:
            w = session.findById(f"wnd[{wi}]")
        except Exception:
            continue
        try:
            txt = str(w.Text or "") + " " + str(getattr(w, "PopupTitle", "") or "")
        except Exception:
            txt = ""
        try:
            for ci in range(w.Children.Count):
                try: txt += " " + str(w.Children(ci).Text or "")
                except Exception: pass
        except Exception:
            pass
        _tl = txt.lower()
        if "back-end session" in _tl or "closed by system" in _tl \
                or "session closed" in _tl:
            _slog(f"[backend_closed] popup detected on wnd[{wi}]: {txt[:200]!r}")
            try:
                w.sendVKey(0)   # Enter / OK
            except Exception:
                try: w.close()
                except Exception: pass
            return True
    return False


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

    # Oct-23 user ask: when SAP first logs in it lands on the S000
    # splash with a "Start SAP Easy Access" button that nobody clicked
    # — tcode='S000', program='SAPMSYST', window title='SAP'.  TCODE
    # entry still works from here, but the ALV grid rendered later
    # (ZMMR40430 etc.) sometimes fails to expose its export menu when
    # the session never left the splash.  Click the button (or press
    # Enter) when the window title is still bare "SAP", so every
    # subsequent crawler starts from the real Easy Access screen.
    try:
        _title = str(session.findById("wnd[0]").Text or "").strip()
        _tcode = str(session.Info.Transaction or "").strip().upper()
        _prog  = str(session.Info.Program     or "").strip().upper()
        _on_splash = (
            _tcode in ("S000", "")
            and _prog == "SAPMSYST"
            and _title.upper() == "SAP"     # Easy Access title would be "SAP Easy Access"
        )
        if _on_splash:
            _slog("  _reset_to_home: on S000 splash — dismissing to Easy Access")
            _pressed = False
            # Path A: find the "Start SAP Easy Access" button by text and
            # press it.  SAP's user screen puts the button directly
            # under wnd[0]/usr; the id can be ":SAPMSYST:0040" or a
            # positioned name, so walk children by text instead of
            # hard-coding an id.  pywin32 wraps the SAP GUI
            # GuiComponentCollection as a COM object whose index method
            # is .Item(i) (NOT .ElementAt — that's .NET syntax); also
            # try plain iteration as a belt-and-braces fallback since
            # some pywin32 builds expose __iter__ via Count/Item.
            try:
                _usr = session.findById("wnd[0]/usr")
                _kids = _usr.Children
                try:
                    _count = int(_kids.Count)
                except Exception:
                    _count = 0
                _slog(f"  _reset_to_home: splash usr has {_count} children")
                for _i in range(_count):
                    try:
                        try:
                            _ch = _kids.Item(_i)      # SAP GUI Scripting way
                        except Exception:
                            _ch = _kids.ElementAt(_i) # .NET-style fallback
                        _txt = str(getattr(_ch, "Text", "") or "")
                        _typ = str(getattr(_ch, "Type", "") or "")
                        _slog(f"    child[{_i}] type={_typ!r} text={_txt!r}")
                        if "easy access" in _txt.lower():
                            try:
                                _ch.press()
                            except Exception:
                                _ch.setFocus(); session.findById("wnd[0]").sendVKey(0)
                            _pressed = True
                            _slog(f"  _reset_to_home: pressed button '{_txt}'")
                            break
                    except Exception as _ce:
                        _slog(f"    child[{_i}] inspect failed: {_ce}")
                        continue
            except Exception as _e:
                _slog(f"  _reset_to_home: splash child walk failed ({_e})")
            # Path B: id-guess.  The button on this release is
            # ":SAPMSYST:0040" more often than not; try a handful of
            # the most common variants.
            if not _pressed:
                for _bid in (
                    "wnd[0]/usr/btnBUTTON_SMS",
                    "wnd[0]/usr/btnWELCOMEBUTTON",
                    "wnd[0]/usr/btnBUTTON",
                ):
                    try:
                        session.findById(_bid).press()
                        _pressed = True
                        _slog(f"  _reset_to_home: pressed splash button by id {_bid}")
                        break
                    except Exception:
                        pass
            # Path C: Enter on the main window — the default button on
            # S000 IS 'Start SAP Easy Access' so Enter equals a click.
            if not _pressed:
                try:
                    session.findById("wnd[0]").sendVKey(0)
                    _slog("  _reset_to_home: splash dismissed via Enter")
                    _pressed = True
                except Exception as _e:
                    _slog(f"  _reset_to_home: splash Enter failed ({_e})")
            time.sleep(1.5)
            # Path D (last resort — user's own suggestion): type the
            # harmless 'SMEN' TCODE in the OK-code field.  SMEN is
            # SAP's canonical Easy Access entry — this works even when
            # none of the button presses did, because the OK-code field
            # at top is live on S000.  Doesn't do anything if we're
            # already past the splash.
            if not _pressed:
                try:
                    okcd = session.findById("wnd[0]/tbar[0]/okcd")
                    okcd.Text = "/nSMEN"
                    session.findById("wnd[0]").sendVKey(0)
                    time.sleep(1.5)
                    _slog("  _reset_to_home: splash bypassed via /nSMEN okcd")
                except Exception as _e:
                    _slog(f"  _reset_to_home: /nSMEN fallback failed ({_e})")
    except Exception as _e:
        _slog(f"  _reset_to_home: splash check failed ({_e}) - continuing anyway")

    _dump_sap_state(session, "after-reset")


def _is_session_alive(sess) -> bool:
    """Return True when a COM session object still responds — a session
    returned from _try_attach may hold a stale pointer to a connection
    that SAP has since disconnected, and calling it later yields
    'object invoked has disconnected from its clients'.  Catch that
    here so ensure_sap_session can retry with a fresh login."""
    if sess is None:
        return False
    try:
        _ = sess.findById("wnd[0]").Text   # cheap round-trip
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────
# Nov-01 user ask: SAP session keep-alive.
#
# SAP's server-side idle timeout drops a session when no activity is
# seen for N minutes (typically 10-30).  During off-hours (nights,
# weekends, AU holidays) the orchestrator's pipeline is dormant, so a
# session the operator left logged in in the morning will quietly die
# by midnight and the first scheduled run next morning fails.
#
# The keep-alive thread, started by batch_orchestrator.py, pings the
# SAP session every KEEPALIVE_INTERVAL_SEC seconds (default 10 min,
# override via env SAP_KEEPALIVE_SEC).  Each tick:
#   1. try-acquires sap_lock non-blocking  (if a pipeline step holds
#      the lock, skip this tick — the step's own SAP activity resets
#      the idle timer anyway).
#   2. attach to a live session; if attach fails, call
#      ensure_sap_session() which handles launch + auto-login +
#      auto-rotate-on-password-change — so Windows updates / reboots
#      / SAP server restarts recover without operator intervention.
#   3. send a benign "/N" command (reset to home transaction) which
#      SAP's idle timer counts as activity.
#
# sap_lock is an RLock so a single thread can re-enter it (ensure_sap
# -> _enter_credentials -> …) safely.  Pipeline code acquires the
# lock in batch_orchestrator.run_pipeline (and release on exit) so a
# keep-alive tick that lands mid-pipeline is skipped cleanly rather
# than racing on the SAP COM object.
# ──────────────────────────────────────────────────────────────────────
sap_lock = threading.RLock()

KEEPALIVE_INTERVAL_SEC = int(os.environ.get("SAP_KEEPALIVE_SEC", "600"))
_keepalive_stop   = threading.Event()
_keepalive_thread = None


def _keepalive_tick():
    """One keep-alive tick.  Non-blocking on sap_lock; silent no-op
    when a pipeline step is active."""
    if not sap_lock.acquire(blocking=False):
        _slog("[keepalive] skip tick — pipeline in use")
        return
    try:
        session = _try_attach()
        if session is not None:
            try:
                # /N is the softest SAP no-op: resets the current
                # window to the SESSION_MANAGER easy-access screen
                # from any transaction (and does nothing when
                # already there).  Counts as activity server-side
                # and so resets the idle timer.
                session.sendCommand("/N")
                _slog("[keepalive] /N sent — session kept alive")
                return
            except Exception as e:
                _slog(f"[keepalive] ping failed: {e} — session likely dead; "
                      f"will try to re-login")
                session = None
        # No live session.  Try a full attach-or-login dance.  This
        # also handles the Windows-reboot / SAP-server-restart case:
        # ensure_sap_session() launches saplogon.exe if needed,
        # OpenConnection on G.ERP, enters credentials, and runs
        # _handle_password_change if SAP demands a rotation.
        _slog("[keepalive] no live session — calling ensure_sap_session()")
        try:
            ensure_sap_session()
            _slog("[keepalive] session restored")
        except Exception as e:
            _slog(f"[keepalive] restore failed: {e} — will retry next tick")
    finally:
        sap_lock.release()


def _keepalive_loop():
    _slog(f"[keepalive] thread started "
          f"(interval={KEEPALIVE_INTERVAL_SEC}s)")
    while not _keepalive_stop.wait(KEEPALIVE_INTERVAL_SEC):
        try:
            _keepalive_tick()
        except Exception as e:
            # NEVER let the loop die — swallow and log every error so
            # a transient COM glitch at 3am doesn't leave the session
            # unmaintained until morning.
            _slog(f"[keepalive] tick raised {e!r} — continuing")
    _slog("[keepalive] thread exiting")


def start_keepalive_thread():
    """Idempotent: safe to call multiple times from the orchestrator
    (e.g. if the orchestrator re-initialises after an error)."""
    global _keepalive_thread
    if _keepalive_thread is not None and _keepalive_thread.is_alive():
        return
    _keepalive_stop.clear()
    _keepalive_thread = threading.Thread(
        target=_keepalive_loop,
        daemon=True,
        name="sap_keepalive",
    )
    _keepalive_thread.start()


def stop_keepalive_thread():
    """Signal the keep-alive thread to exit on its next wake-up."""
    _keepalive_stop.set()


def ensure_sap_session():
    """Public entry.  Attach or log in; return (session, status).

    Oct-22 user ask: fully-automatic logon when batch runs AND SAP
    isn't signed in — but DO NOT touch any SAP window the operator
    already has open (their manual work must survive).  Policy:

      • Something logged-in and alive → attach and reuse.
      • SAP Logon running but no live connection → OpenConnection
        + enter credentials (new connection, no restart of saplogon).
      • SAP Logon not running at all → launch saplogon.exe +
        OpenConnection + enter credentials.

    Up to 3 attempts, each validating the returned session before
    returning.  A dead/stale COM pointer that only reveals itself
    as 'disconnected' on first use is caught here and the next
    attempt opens a fresh connection — WITHOUT killing saplogon.

    Oct-28 user ask: a background popup watcher runs for the whole
    duration of this call so a stale "Back-end session closed"
    dialog that appears MID-logon (not just before attach) gets
    auto-dismissed without the batch getting stuck."""
    last_err = None
    # Start the popup watcher BEFORE the first attach attempt and
    # keep it running through every retry.  Guarantees a stale
    # Win32 modal doesn't block scripting at any point during logon.
    _popup_watcher = None
    try:
        _popup_watcher = _start_sap_popup_watcher()
    except Exception as _e:
        _slog(f"popup watcher failed to start: {_e!r} — continuing without it")
    try:
        for attempt in range(1, 4):
            try:
                # Belt-and-braces one-shot dismiss on top of the
                # continuous watcher — catches any popup that was
                # already up when ensure_sap_session was called,
                # even if the watcher thread hasn't ticked yet.
                try:
                    if _dismiss_sap_system_message_popup():
                        _slog(f"attempt {attempt}/3: dismissed stale "
                              f"'Back-end session closed' popup; proceeding "
                              f"with attach/login")
                        time.sleep(0.8)
                except Exception as _e:
                    _slog(f"attempt {attempt}/3: popup-dismiss helper failed: {_e!r}")
                sess, status = _ensure_sap_session_once(attempt)
                if _is_session_alive(sess):
                    _slog(f"attempt {attempt}/3: session validated as alive "
                          f"(status={status})")
                    return sess, status
                _slog(f"attempt {attempt}/3: returned session is DEAD "
                      f"(COM disconnected) — retrying with a fresh "
                      f"OpenConnection (saplogon.exe NOT touched).")
                time.sleep(1.5)
            except MultiLogonError:
                raise  # caller handles the email + wait + retry
            except Exception as e:
                last_err = e
                _slog(f"attempt {attempt}/3 raised: {e!r} — "
                      f"retrying (saplogon.exe NOT touched).")
                time.sleep(2.0)
        raise NoSapError(
            f"ensure_sap_session failed after 3 attempts.  Last error: {last_err!r}"
        )
    finally:
        _stop_sap_popup_watcher(_popup_watcher)


def _ensure_sap_session_once(attempt):
    """One attempt at attach-or-login.  Factored out of ensure_sap_session
    so the retry wrapper can taskkill saplogon between attempts without
    duplicating the attach/login logic."""
    _slog(f"ensure_sap_session attempt {attempt}/3: checking for "
          f"logged-in session…")
    # 1. Try to attach to an existing logged-in session.
    sess = _try_attach()
    if sess is not None and _is_session_alive(sess):
        # Oct-23 user ask: before trusting an attached session, check
        # for the "Back-end session closed by system" popup that
        # appears when another desktop's logon just killed this one.
        # If present, treat this seat as dead and fall through to a
        # fresh login — _handle_multi_logon will then steal the seat
        # back (OPT1).
        if _check_backend_closed(sess):
            _slog("attached session was killed by another logon — "
                  "popup dismissed, falling through to fresh login "
                  "so we can re-claim the seat (OPT1).")
            sess = None
        else:
            # Oct-28 user ask: the "License Information for Multiple
            # Logon" dialog may already be sitting on the attached
            # session (e.g. an earlier logon from another desktop
            # triggered it and the operator never dismissed it).
            # Click OPT1 immediately so this seat stays live.
            try:
                if _handle_multi_logon(sess):
                    _slog("attached session had a pending Multiple-Logon "
                          "dialog — OPT1 selected, other logon(s) terminated.")
                    time.sleep(1.5)
            except Exception as _e:
                _slog(f"[attach] multi_logon check raised: {_e!r} — continuing")
            _slog("already logged in — attached; resetting to Easy Access.")
            _reset_to_home(sess)
            return sess, "attached"
    if sess is not None:
        _slog("_try_attach returned a session but it's already DEAD "
              "— falling through to fresh login.")

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
        try:
            n_conn = sap.Children.Count
        except Exception:
            n_conn = -1
        _slog(f"SAP GUI scripting is live (Children.Count={n_conn}).")

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
    #    pick OPT1 so THIS seat wins and the other logon is terminated.
    #    Oct-23 user ask: no longer raises MultiLogonError — we take
    #    the seat and continue.  The orchestrator's old wait-and-retry
    #    branch becomes a no-op in this flow (kept for backwards
    #    compat in case a future SAP release blocks OPT1).
    _slog("checking for multi-logon dialog…")
    if _handle_multi_logon(session):
        _slog("multi-logon dialog handled — THIS seat kept, other "
              "logon(s) terminated.  Proceeding as fresh_login.")
        time.sleep(1.5)  # let SAP settle after the ✓

    # 4b. Password-change dialog check — if SAP demands a rotation at
    # login, feed it the SAP_NEW_PWD_CANDIDATES chain BEFORE the
    # generic Enter-dismisser loop below (which would blindly submit
    # the dialog with empty NEW/CONFIRM fields and trigger an error).
    _slog("checking for password-change dialog…")
    try:
        if _handle_password_change(session):
            _slog("password-change dialog handled — new password set; "
                  "operator must update batch_sap_logon.py default.")
            time.sleep(1.5)
    except RuntimeError as _pce:
        _slog(f"password-change handling FAILED: {_pce}")
        raise

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
