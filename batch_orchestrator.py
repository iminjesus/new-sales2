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
      2. sapcrawling.main()        — ZSDM64300 → dot_stock.csv (+ DOT)
      3. sapcrawling_sales1.main() — ZSDR24030 → sales_thismonth.csv
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

# ── Config ────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PID_FILE = os.path.join(BASE_DIR, "batch_orchestrator.pid")
LOG_FILE = os.path.join(BASE_DIR, "batch_orchestrator.log")

# Business-hours window (Sydney local time, inclusive)
RUN_HOURS = list(range(8, 19))   # 8, 9, 10, …, 18

# Multi-logon retry cadence
MULTI_LOGON_RETRY_MIN = 5
MULTI_LOGON_MAX_RETRIES_PER_HOUR = 11   # 11 × 5 min = ~55 min, bail into next slot

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
    print(line, flush=True)
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
    """Open MySQL using the same env vars load_csv_to_mysql uses."""
    import mysql.connector
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(BASE_DIR, ".env"))
    except Exception:
        pass
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "100.127.139.79"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "sales"),
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


# ── Pipeline step wrappers ──────────────────────────────────────────
def _run_step(label, fn):
    """Run one of the pipeline steps and log the outcome.  Returns
    True on success, False on failure (orchestrator keeps going)."""
    log(f"→ {label}")
    try:
        fn()
        log(f"✓ {label}")
        return True
    except Exception as e:
        tb = traceback.format_exc()
        log(f"✗ {label}: {e}\n{tb}")
        status_set("last_error", f"{label}: {e}")
        status_set("last_error_at", _now_syd().strftime("%Y-%m-%d %H:%M:%S"))
        return False


def run_pipeline():
    """Attach to SAP + run stock crawl + sales crawl + MySQL load.
    Raises batch_sap_logon.MultiLogonError if the user is already on
    another desktop so the caller can wait + retry."""
    from batch_sap_logon import ensure_sap_session, MultiLogonError, NoSapError
    try:
        _sess, status = ensure_sap_session()
    except MultiLogonError:
        raise
    except NoSapError as e:
        raise RuntimeError(f"SAP not reachable: {e}")
    log(f"SAP session {status}.")

    # Stock + DOT: sapcrawling.main writes rawdata/unlock/dot_stock.csv
    # (plus per-plant xlsx) and raises on failure.  We let the first
    # exception bubble so the orchestrator records it.
    stock_ok = _run_step(
        "sapcrawling.main — ZSDM64300 stock + DOT",
        lambda: __import__("sapcrawling").main(),
    )

    # Sales: sapcrawling_sales1.main writes rawdata/unlock/sales_thismonth.csv.
    sales_ok = _run_step(
        "sapcrawling_sales1.main — ZSDR24030 daily sales",
        lambda: __import__("sapcrawling_sales1").main(),
    )

    # Loader needs AT LEAST one of the two CSVs to be fresh.  Run it
    # even if only one crawl succeeded — load_csv_to_mysql skips the
    # table whose CSV is missing (its own existence check).
    if stock_ok or sales_ok:
        load_ok = _run_step(
            "load_csv_to_mysql.main — push CSVs into MySQL",
            lambda: __import__("load_csv_to_mysql").main(),
        )
    else:
        load_ok = False
        log("Both crawls failed — skipping MySQL load for this slot.")

    now_syd_str = _now_syd().strftime("%Y-%m-%d %H:%M:%S")
    if stock_ok and load_ok:
        status_set("stock_updated_at", now_syd_str)
    if sales_ok and load_ok:
        status_set("sales_updated_at", now_syd_str)
    if stock_ok and sales_ok and load_ok:
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
    log(f"Business hours: {RUN_HOURS[0]:02d}–{RUN_HOURS[-1]:02d} Sydney, Mon–Fri, "
        f"skipping {len(AU_NATIONAL_HOLIDAYS)} AU federal holidays.")

    # Oct-16 user ask: run the full pipeline IMMEDIATELY on startup
    # (via start_batch.bat or a manual launch), regardless of hour /
    # day / holiday — the operator who just clicked start_batch.bat
    # expects to see the dashboard refresh now, not at the top of the
    # next business hour.  After the kickoff run completes, fall into
    # the normal top-of-the-hour schedule.
    log("Immediate kickoff run (start_batch.bat behaviour)…")
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


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted — exiting.")
        try: os.remove(PID_FILE)
        except Exception: pass
