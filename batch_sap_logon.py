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
import time
import subprocess

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


class NoSapError(RuntimeError):
    """Raised when SAP Logon isn't installed or can't be reached."""
    pass


def _get_gui_auto():
    """Return the SAPGUI scripting-engine root, or None if SAP Logon
    isn't running / scripting isn't enabled."""
    try:
        import win32com.client  # type: ignore
    except Exception as e:
        raise NoSapError(f"pywin32 not installed: {e}")
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
    """Open the SAP_CONN entry from SAP Logon.  Returns the new session."""
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
    """Fill client / user / password / language and press Enter."""
    wnd = session.findById("wnd[0]")
    try:
        wnd.findById("usr/txtRSYST-MANDT").Text = SAP_CLIENT
    except Exception:
        pass
    try:
        wnd.findById("usr/txtRSYST-BNAME").Text = SAP_USER
    except Exception:
        raise RuntimeError("SAP login screen didn't carry BNAME field")
    try:
        wnd.findById("usr/pwdRSYST-BCODE").Text = SAP_PWD
    except Exception:
        raise RuntimeError("SAP login screen didn't carry BCODE field")
    try:
        wnd.findById("usr/txtRSYST-LANGU").Text = SAP_LANG
    except Exception:
        pass
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


def ensure_sap_session():
    """Public entry.  Attach or log in; return (session, status)."""
    # 1. Try to attach to an existing logged-in session.
    sess = _try_attach()
    if sess is not None:
        return sess, "attached"

    # 2. Launch SAP Logon if the scripting bridge isn't live yet.
    sap = _get_gui_auto()
    if sap is None:
        _launch_logon()
        sap = _get_gui_auto()
        if sap is None:
            raise NoSapError("SAP GUI scripting not reachable after launch")

    # 3. Open the G.ERP (or SAP_CONN) connection and key credentials.
    session = _open_connection(sap)
    _enter_credentials(session)
    time.sleep(2.5)

    # 4. Multi-logon check — if the "already logged on" dialog appears,
    #    terminate it and bail with MultiLogonError so the caller can
    #    email the owner + wait + retry.
    if _handle_multi_logon(session):
        raise MultiLogonError(
            f"User {SAP_USER} is already logged on another desktop."
        )

    # 5. Dismiss any licence / disclaimer popup.
    for _ in range(5):
        try:
            wnd1 = session.findById("wnd[1]")
            if wnd1:
                wnd1.sendVKey(0)
                time.sleep(0.5)
            else:
                break
        except Exception:
            break

    return session, "fresh_login"
