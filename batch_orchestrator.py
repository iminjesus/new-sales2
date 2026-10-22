"""
batch_orchestrator.py — hourly SAP crawl + MySQL load, Sydney business hours.

Oct-16 user spec:
  • Run every hour on the hour, 08:00 - 18:00 Sydney time
  • Skip Saturdays, Sundays, and Australia-wide federal holidays
  • Each run:
      1. Attach to a logged-in SAP session (via batch_sap_logon).
         If not logged in: open SAP Logon, key credentials, submit.
         If SAP says "User already logged on another desktop":
           - Terminate the attempted logon (don't kick the other seat)
           - Email brian.park@hankooktyre.com.au ONCE per hour (first
             attempt of the hour that fails)
           - Sleep 5 min, retry.  Silent retries thereafter.
      2. sapcrawling_batch.main()        — ZSDM64300 → dot_stock.csv (+ DOT)
      3. sapcrawling_sales1_batch.main() — ZSDR24030 → sales_thismonth.csv
      4. load_csv_to_mysql.main()  — load both CSVs into MySQL
      5. Stamp the batch_status table with the last-successful times.

Status table (batch_status):
  key TEXT PK, value TEXT
Current keys written:
  stock_updated_at   — ISO Sydney time of the last successful stock load
  sales_updated_at   — ISO Sydney time of the last successful sales load
  last_error         — last failure's text (cleared on next success)
  last_error_at      — ISO Sydney time of last failure
  last_multi_logon_at — hour-of-last-multi-logon-email so we only mail once/hour

How to run (Windows, from the project root):
  Foreground (visible, logs to console):
      python batch_orchestrator.py
  Background (survives VS Code terminal closing):
      start /B pythonw.exe batch_orchestrator.py > batch_orchestrator.log 2>&1
    OR double-click start_batch.bat (same thing, writes a PID file too).

How to stop:
  • Foreground: Ctrl-C
  • Background: double-click stop_batch.bat
    (uses the PID stashed in batch_orchestrator.pid; falls back to killing
    every pythonw.exe running batch_orchestrator.py)

The script is a plain while-loop + time.sleep — no OS cron needed, so
starting / stopping is self-contained.  If the box reboots the batch
dies with it; add a Windows Task Scheduler entry pointing at
start_batch.bat with trigger "At startup" to make it survive reboots.
"""
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, date

# ── UTF-8 everywhere ─────────────────────────────────────────────────
# The Windows console pythonw.exe hands us is cp1252 by default, which
# blows up on the first Unicode char any imported sub-script tries to
# print (sapcrawling_batch.py uses '──' box drawing,
# sapcrawling_sales1_batch.py uses '→', etc.).  Reconfigure sys.stdout
# / sys.stderr to utf-8 with
# errors='replace' so a stray Unicode char becomes '?' on the console
# INSTEAD of raising UnicodeEncodeError and tearing down the batch.
# Must run before any import that might print at import time.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ── Config ────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PID_FILE = os.path.join(BASE_DIR, "batch_orchestrator.pid")
LOG_FILE = os.path.join(BASE_DIR, "batch_orchestrator.log")

# Business-hours window (Sydney local time, inclusive)
RUN_HOURS = list(range(8, 19))   # 8, 9, 10, …, 18

# Multi-logon retry cadence
MULTI_LOGON_RETRY_MIN = 5
MULTI_LOGON_MAX_RETRIES_PER_HOUR = 11   # 11 × 5 min = ~55 min, bail into next slot

# Oct-23 user ask (watchdog + heartbeat + task scheduler):
#   • Per-step timeout — each crawl step is given a hard wall-clock
#     ceiling.  When exceeded, the orchestrator logs the hang and
#     os._exit()'s with code 2 so the Windows watchdog task restarts
#     the whole process from a clean SAP attach.  Prevents the "SAP
#     popup / Office activation wizard / Excel save-as dialog froze
#     the whole box overnight" failure mode.
#   • Heartbeat — a background thread writes
#     batch_status.heartbeat_at = NOW() every HEARTBEAT_SEC seconds.
#     The dashboard reads it and warns when it's > 10 min stale.
STEP_TIMEOUT_MIN = {
    "sap_attach":  5,
    "dot_stock":  20,
    "stock_rt":   15,
    "sales":      15,
    "mysql_load": 10,
}
HEARTBEAT_SEC = 60   # write batch_status.heartbeat_at every N seconds
# Exit codes — matched by the Windows watchdog .bat / Task Scheduler.
EXIT_STEP_TIMEOUT    = 2
EXIT_UNCAUGHT        = 3

# Email recipient for the "SAP in use elsewhere" warning
MULTI_LOGON_NOTIFY_TO = ["brian.park@hankooktyre.com.au"]

# Hard-coded Australia-wide federal holidays.  Edit when the year rolls
# over; state-only days (Queen's Birthday, Labour Day etc.) are
# deliberately EXCLUDED per the user spec "호주 전체가 공휴일인 경우만".
AU_NATIONAL_HOLIDAYS = {
    # 2026
    date(2026, 1,  1): "New Year's Day",
    date(2026, 1, 26): "Australia Day",
    date(2026, 4,  3): "Good Friday",
    date(2026, 4,  6): "Easter Monday",
    date(2026, 4, 25): "Anzac Day",
    date(2026,12, 25): "Christmas Day",
    date(2026,12, 28): "Boxing Day (observed)",  # 26 Dec = Sat → obs Mon
    # 2027
    date(2027, 1,  1): "New Year's Day",
    date(2027, 1, 26): "Australia Day",
    date(2027, 3, 26): "Good Friday",
    date(2027, 3, 29): "Easter Monday",
    date(2027, 4, 26): "Anzac Day (observed)",   # 25 Apr = Sun → obs Mon
    date(2027,12, 27): "Christmas Day (observed)",  # 25 Dec = Sat → obs Mon
    date(2027,12, 28): "Boxing Day (observed)",
}

# ── Sydney timezone (stdlib zoneinfo, Python 3.9+) ────────────────────
try:
    from zoneinfo import ZoneInfo
    SYD_TZ = ZoneInfo("Australia/Sydney")
except Exception:
    SYD_TZ = None   # fallback below uses local time

def _now_syd():
    if SYD_TZ is not None:
        return datetime.now(SYD_TZ)
    return datetime.now()


# ── Logging ──────────────────────────────────────────────────────────
def log(msg):
    ts = _now_syd().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    # Windows console on the operator's box defaults to cp1252 which
    # can't encode Unicode arrows / checkmarks.  Wrap print() in a
    # try/except that falls back to ASCII-replaced output so a stray
    # non-cp1252 character can NEVER crash the orchestrator (as it
    # did on Oct-16 — the first run attached to SAP, then the
    # "→ sapcrawling_batch.main" log line's arrow blew up the process).
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
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ── Schedule gates ───────────────────────────────────────────────────
def _is_business_day(d: date) -> bool:
    if d.weekday() >= 5:        # 5 = Sat, 6 = Sun
        return False
    if d in AU_NATIONAL_HOLIDAYS:
        log(f"Holiday skip: {d} ({AU_NATIONAL_HOLIDAYS[d]})")
        return False
    return True


def _next_run_at(now):
    """Return the next datetime (Sydney TZ) at which a batch should fire."""
    d = now.date()
    h = now.hour
    # Candidate hours TODAY that haven't passed yet
    for cand_h in RUN_HOURS:
        if cand_h > h or (cand_h == h and now.minute == 0 and now.second < 5):
            cand = now.replace(hour=cand_h, minute=0, second=0, microsecond=0)
            if _is_business_day(cand.date()):
                return cand
    # Nothing left today — jump to the first slot on the next business day
    nd = d + timedelta(days=1)
    while not _is_business_day(nd):
        nd += timedelta(days=1)
    first_h = RUN_HOURS[0]
    return (now.replace(hour=first_h, minute=0, second=0, microsecond=0)
            + timedelta(days=(nd - d).days))


# ── MySQL status table ──────────────────────────────────────────────
def _db_conn():
    """Open MySQL using the same env vars load_csv_to_mysql uses.

    Oct-23 hardening: parse .env manually when python-dotenv isn't on
    this interpreter's path (the operator PC has two Pythons — the
    batch's venv has dotenv, their user-shell Python 3.13 doesn't).
    Accepts DB_PASSWORD or DB_PASS and DB_NAME or DB_DATABASE so a
    one-off .env with different casing still works."""
    import mysql.connector
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(BASE_DIR, ".env"))
    except Exception:
        try:
            with open(os.path.join(BASE_DIR, ".env")) as _f:
                for _line in _f:
                    _s = _line.strip()
                    if not _s or _s.startswith("#") or "=" not in _s:
                        continue
                    _k, _v = _s.split("=", 1)
                    os.environ.setdefault(
                        _k.strip(), _v.strip().strip('"').strip("'"))
        except Exception:
            pass
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "100.127.139.79"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD") or os.getenv("DB_PASS", ""),
        database=os.getenv("DB_NAME") or os.getenv("DB_DATABASE", "my_new_database"),
    )


def _ensure_status_table(cur):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS batch_status ("
        "  k VARCHAR(64) PRIMARY KEY,"
        "  v TEXT NULL,"
        "  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP "
        "    ON UPDATE CURRENT_TIMESTAMP"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )


def status_set(key, value):
    try:
        conn = _db_conn()
        cur  = conn.cursor()
        _ensure_status_table(cur)
        cur.execute(
            "INSERT INTO batch_status (k, v) VALUES (%s, %s) "
            "ON DUPLICATE KEY UPDATE v = VALUES(v)",
            (key, str(value) if value is not None else None),
        )
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        log(f"[status] failed to persist {key}={value!r}: {e}")


def status_get(key):
    try:
        conn = _db_conn()
        cur  = conn.cursor()
        _ensure_status_table(cur)
        cur.execute("SELECT v FROM batch_status WHERE k = %s", (key,))
        row = cur.fetchone()
        cur.close(); conn.close()
        return row[0] if row else None
    except Exception:
        return None


# ── Email ────────────────────────────────────────────────────────────
def send_multi_logon_email(hour_key):
    """Fire the warning email about SAP being in use elsewhere.  Only
    sends when the hour hasn't already been notified (hour_key =
    "YYYY-MM-DD HH").  Reuses app.py's _send_mail_async so SMTP /
    Graph credentials stay in one place."""
    last = status_get("last_multi_logon_at") or ""
    if last == hour_key:
        return False
    try:
        sys.path.insert(0, BASE_DIR)
        from app import _send_mail_async
    except Exception as e:
        log(f"[mail] can't import _send_mail_async from app.py: {e}")
        return False
    subject = f"[Hankook Dashboard] SAP crawl paused — your session is on another desktop"
    body = (
        "<p>The hourly SAP crawler just tried to log in as "
        "<b>42S00030B2</b> and SAP replied that this user is already "
        "signed on another desktop.</p>"
        "<p>I've terminated the attempted logon here (your other desktop "
        "is untouched) and I'll retry every 5 minutes.  If you want this "
        "run to go through, sign out of the other desktop and the next "
        "retry will catch it automatically.</p>"
        f"<p>Attempted at: <b>{_now_syd().strftime('%Y-%m-%d %H:%M:%S')}</b> (Sydney)</p>"
        "<p>You'll only get this once per hour — silent retries follow.</p>"
    )
    _send_mail_async(MULTI_LOGON_NOTIFY_TO, [], subject, body)
    status_set("last_multi_logon_at", hour_key)
    log(f"[mail] multi-logon warning sent to {MULTI_LOGON_NOTIFY_TO}")
    return True


# ── Heartbeat (Oct-23) ───────────────────────────────────────────────
# A background daemon thread stamps batch_status.heartbeat_at every
# HEARTBEAT_SEC seconds.  The dashboard polls this value and warns the
# operator when it's > 10 minutes stale — fastest way to notice the
# box went to sleep or the process hung.
_HEARTBEAT_STARTED = False
def _start_heartbeat_thread():
    global _HEARTBEAT_STARTED
    if _HEARTBEAT_STARTED:
        return
    _HEARTBEAT_STARTED = True
    import threading
    def _beat():
        while True:
            try:
                status_set("heartbeat_at", _now_syd().strftime("%Y-%m-%d %H:%M:%S"))
            except Exception as _e:
                # Never let the heartbeat loop crash the whole thread —
                # a MySQL blip is survivable.
                pass
            time.sleep(HEARTBEAT_SEC)
    t = threading.Thread(target=_beat, name="batch-heartbeat", daemon=True)
    t.start()
    log(f"heartbeat thread started (every {HEARTBEAT_SEC}s → batch_status.heartbeat_at).")


# ── Pipeline step wrappers ──────────────────────────────────────────
def _run_step(label, fn, *, timeout_min=None):
    """Run one of the pipeline steps and log the outcome.  Returns
    True on success, False on failure (orchestrator keeps going).
    ASCII-only markers (-> / OK / FAIL) so cp1252 consoles don't
    throw UnicodeEncodeError on the log line.

    Oct-23 user ask: when timeout_min is set, the step runs in a
    daemon thread and the main thread joins with that wall-clock
    ceiling.  A step that doesn't return in time (SAP popup / Office
    activation wizard / modal Excel dialog) force-exits the whole
    orchestrator with EXIT_STEP_TIMEOUT so the Windows watchdog task
    restarts it from a clean SAP attach.  The hung thread is left
    behind as a daemon — Python's exit skips it since daemon=True."""
    import threading
    log(f"-> {label}")
    _result = {"ok": False, "err": None, "done": False}
    def _runner():
        try:
            fn()
            _result["ok"] = True
        except Exception as e:
            _result["err"] = (e, traceback.format_exc())
        finally:
            _result["done"] = True
    if timeout_min:
        t = threading.Thread(target=_runner, name=f"step-{label[:30]}", daemon=True)
        t.start()
        t.join(timeout=timeout_min * 60.0)
        if not _result["done"]:
            # Hard hang — write status, log the stuck step, bail out so
            # the Windows watchdog takes over and relaunches from
            # scratch.  Any cleaner "soft recovery" is risky because
            # the hung thread is probably sitting inside a COM call
            # that can't be interrupted from Python.
            msg = (f"TIMEOUT {label} after {timeout_min} min — "
                   f"orchestrator force-exiting so Windows watchdog "
                   f"can relaunch with a fresh SAP attach.")
            log(msg)
            try:
                status_set("last_error", f"{label}: TIMEOUT after {timeout_min} min")
                status_set("last_error_at", _now_syd().strftime("%Y-%m-%d %H:%M:%S"))
            except Exception:
                pass
            try:
                os.remove(PID_FILE)
            except Exception:
                pass
            os._exit(EXIT_STEP_TIMEOUT)
    else:
        _runner()
    if _result["ok"]:
        log(f"OK {label}")
        return True
    e, tb = _result["err"] if _result["err"] else (RuntimeError("unknown"), "")
    log(f"FAIL {label}: {e}\n{tb}")
    status_set("last_error", f"{label}: {e}")
    status_set("last_error_at", _now_syd().strftime("%Y-%m-%d %H:%M:%S"))
    return False


def run_pipeline():
    """Attach to SAP + run stock crawl + sales crawl + MySQL load.
    Raises batch_sap_logon.MultiLogonError if the user is already on
    another desktop so the caller can wait + retry."""
    from batch_sap_logon import ensure_sap_session, MultiLogonError, NoSapError, _reset_to_home
    try:
        sess, status = ensure_sap_session()
    except MultiLogonError:
        raise
    except NoSapError as e:
        raise RuntimeError(f"SAP not reachable: {e}")
    log(f"SAP session {status}.")

    # Stock + DOT: sapcrawling_batch.main writes rawdata/unlock/dot_stock.csv
    # (plus per-plant xlsx) and raises on failure.  We let the first
    # exception bubble so the orchestrator records it.
    # Oct-20: use sapcrawling_batch.py (batch-only fork of sapcrawling.py)
    # so the stable manually-runnable sapcrawling.py stays untouched.
    #
    # Oct-21 user ask: the DOT snapshot (interface_date = yesterday) is
    # generated overnight at SAP side, so it doesn't change throughout
    # the business day — pulling it every hour is wasted SAP load.
    # Run it only ONCE per day: the 08:00 slot, OR an immediate-kickoff
    # run when nothing has recorded today's crawl yet.  The RT stock
    # feed (ZMMR40430, next step) still runs every hour since it IS
    # time-sensitive.
    today_syd = _now_syd().strftime("%Y-%m-%d")
    last_dot_date = (status_get("dot_stock_crawled_on") or "")[:10]
    current_hour  = _now_syd().hour
    dot_already_today = (last_dot_date == today_syd)
    _is_8am_slot      = (current_hour == 8)
    should_crawl_dot  = (not dot_already_today) and (_is_8am_slot or True)
    # The condition reads a bit funny — _is_8am_slot OR True evaluates
    # to True always.  Intent: run DOT whenever today's crawl hasn't
    # happened yet (covers immediate kickoff at any hour AND the 08:00
    # scheduled slot).  Subsequent hourly runs on the same day skip it.
    if should_crawl_dot:
        if _is_8am_slot:
            log("DOT stock: 08:00 slot — running today's daily snapshot.")
        else:
            log(f"DOT stock: no run recorded for {today_syd} yet — "
                f"running once now (hour={current_hour:02d}).")
        stock_ok = _run_step(
            "sapcrawling_batch.main - ZSDM64300 stock + DOT",
            lambda: __import__("sapcrawling_batch").main(),
            timeout_min=STEP_TIMEOUT_MIN["dot_stock"],
        )
        if stock_ok:
            status_set("dot_stock_crawled_on", today_syd)
    else:
        log(f"DOT stock: already crawled today ({last_dot_date}) — "
            f"skipping (hour={current_hour:02d}).  Next scheduled run "
            f"is 08:00 tomorrow.")
        stock_ok = False

    # If the stock crawl crashed partway it may have left SAP on a
    # mid-transaction screen (popup, selection screen, grid, …).  Reset
    # to Easy Access before the sales crawler so its own
    # start_transaction_fresh starts from a known-good state.
    try:
        _reset_to_home(sess)
    except Exception as e:
        log(f"[warn] _reset_to_home between crawls failed: {e}")

    # Oct-21: RT stock (ZMMR40430) — writes rawdata/unlock/rt_stock_<YYYYMMDD>.csv
    # which load_csv_to_mysql picks up as the newest and loads into the
    # stock_rt table.  Sits between the DOT-aging stock crawl above and
    # the sales crawl below; failures here are logged but do NOT abort
    # the pipeline (sales + loader must still run even if the RT feed
    # misses one hour).
    try:
        _reset_to_home(sess)
    except Exception as e:
        log(f"[warn] _reset_to_home before stock_rt failed: {e}")
    rt_ok = _run_step(
        "sapcrawling_stock_rt_batch.main - ZMMR40430 RT stock + ATP",
        lambda: __import__("sapcrawling_stock_rt_batch").main(),
        timeout_min=STEP_TIMEOUT_MIN["stock_rt"],
    )
    try:
        _reset_to_home(sess)
    except Exception as e:
        log(f"[warn] _reset_to_home after stock_rt failed: {e}")

    # Sales: sapcrawling_sales1_batch.main writes rawdata/unlock/sales_thismonth.csv.
    # Oct-20: use sapcrawling_sales1_batch.py (batch-only fork of
    # sapcrawling_sales1.py) so the stable manually-runnable
    # sapcrawling_sales1.py stays untouched.
    sales_ok = _run_step(
        "sapcrawling_sales1_batch.main - ZSDR24030 daily sales",
        lambda: __import__("sapcrawling_sales1_batch").main(),
        timeout_min=STEP_TIMEOUT_MIN["sales"],
    )

    # Loader needs AT LEAST one of the three CSVs to be fresh.  Run it
    # even if only one crawl succeeded — load_csv_to_mysql skips the
    # table whose CSV is missing (its own existence check).  DOT stock
    # being skipped (not failed) still counts as "nothing fresh" for
    # this slot, but RT + sales usually carry the load.
    if stock_ok or sales_ok or rt_ok:
        load_ok = _run_step(
            "load_csv_to_mysql.main - push CSVs into MySQL",
            lambda: __import__("load_csv_to_mysql").main(),
            timeout_min=STEP_TIMEOUT_MIN["mysql_load"],
        )
    else:
        load_ok = False
        log("All crawls skipped or failed - skipping MySQL load for this slot.")

    now_syd_str = _now_syd().strftime("%Y-%m-%d %H:%M:%S")
    if stock_ok and load_ok:
        status_set("stock_updated_at", now_syd_str)
    if sales_ok and load_ok:
        status_set("sales_updated_at", now_syd_str)
    if rt_ok and load_ok:
        status_set("stock_rt_updated_at", now_syd_str)
    # Clear last_error when THE RELEVANT crawls for this slot all
    # succeeded.  DOT stock may be legitimately skipped (not failed),
    # so don't require stock_ok on skip hours — just require that
    # sales + RT succeeded AND the loader ran.
    if sales_ok and rt_ok and load_ok and (stock_ok or dot_already_today):
        status_set("last_error", "")
        status_set("last_error_at", "")


def run_slot(scheduled_at):
    """One hour slot's attempt → retry loop for multi-logon."""
    hour_key = scheduled_at.strftime("%Y-%m-%d %H")
    log(f"=== Slot {hour_key} ===")
    for attempt in range(1, MULTI_LOGON_MAX_RETRIES_PER_HOUR + 1):
        try:
            run_pipeline()
            return True
        except Exception as e:
            # MultiLogon imported lazily to avoid pywin32 import at
            # module scope on dev boxes.
            from batch_sap_logon import MultiLogonError
            if isinstance(e, MultiLogonError):
                log(f"Attempt {attempt}: multi-logon.")
                send_multi_logon_email(hour_key)
                if attempt >= MULTI_LOGON_MAX_RETRIES_PER_HOUR:
                    log(f"Giving up this slot after {attempt} multi-logon retries.")
                    return False
                log(f"Sleeping {MULTI_LOGON_RETRY_MIN} min before retry.")
                time.sleep(MULTI_LOGON_RETRY_MIN * 60)
                continue
            tb = traceback.format_exc()
            log(f"Slot {hour_key} failed with non-multi-logon error:\n{tb}")
            status_set("last_error", str(e))
            status_set("last_error_at", _now_syd().strftime("%Y-%m-%d %H:%M:%S"))
            return False


# ── Main loop ────────────────────────────────────────────────────────
def _write_pid():
    try:
        with open(PID_FILE, "w") as f:
            f.write(str(os.getpid()))
    except Exception:
        pass


def main():
    _write_pid()
    log(f"batch_orchestrator PID={os.getpid()} starting.")
    log(f"Business hours: {RUN_HOURS[0]:02d}-{RUN_HOURS[-1]:02d} Sydney, Mon-Fri, "
        f"skipping {len(AU_NATIONAL_HOLIDAYS)} AU federal holidays.")
    # Oct-23 user ask: fire the heartbeat before anything else so the
    # dashboard sees the orchestrator alive even if the first kickoff
    # run takes a while to finish.
    _start_heartbeat_thread()

    # Oct-16 user ask: run the full pipeline IMMEDIATELY on startup
    # (via start_batch.bat or a manual launch), regardless of hour /
    # day / holiday - the operator who just clicked start_batch.bat
    # expects to see the dashboard refresh now, not at the top of the
    # next business hour.  After the kickoff run completes, fall into
    # the normal top-of-the-hour schedule.
    log("Immediate kickoff run (start_batch.bat behaviour)...")
    try:
        run_slot(_now_syd())
    except Exception as e:
        log(f"[kickoff fatal-but-recovering] {e}\n{traceback.format_exc()}")

    while True:
        now = _now_syd()
        nxt = _next_run_at(now)
        wait_s = max(1.0, (nxt - now).total_seconds())
        log(f"Next run at {nxt.strftime('%Y-%m-%d %H:%M')} "
            f"({int(wait_s // 60)} min {int(wait_s % 60)} s away).")
        # Sleep in 30 s chunks so Ctrl-C is responsive on Windows.
        slept = 0
        while slept < wait_s:
            chunk = min(30.0, wait_s - slept)
            time.sleep(chunk)
            slept += chunk
        try:
            run_slot(_now_syd())
        except Exception as e:
            log(f"[fatal-but-recovering] {e}\n{traceback.format_exc()}")
        # Loop around to compute the next slot.


def _install_excepthook():
    """pythonw.exe silently drops stderr, so an uncaught exception
    would vanish without a trace.  Pipe sys.excepthook into our log
    file so crashes land next to the regular status lines."""
    def _hook(exc_type, exc_value, exc_tb):
        import traceback as _tb
        tb = "".join(_tb.format_exception(exc_type, exc_value, exc_tb))
        log(f"[uncaught] {exc_type.__name__}: {exc_value}\n{tb}")
    sys.excepthook = _hook


if __name__ == "__main__":
    _install_excepthook()
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted - exiting.")
        try: os.remove(PID_FILE)
        except Exception: pass
