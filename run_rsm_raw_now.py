#!/usr/bin/env python3
"""
run_rsm_raw_now.py -- one-shot driver to test the ZSDR24030 /RSM_RAW
second-pass download without waiting for the hourly batch to cycle.

Uses the same ensure_sap_session() the orchestrator uses, so it will:
  * attach to an already-logged-in SAP session when one exists, OR
  * launch SAP Logon + log in automatically when nothing is running
    (same credentials path batch_orchestrator uses)

Then runs ONLY the /RSM_RAW export (no JAYDENSQL), writing
rawdata/unlock/sales_rsm_raw.csv.

Usage:
  python run_rsm_raw_now.py                # runs the current-month default
  python run_rsm_raw_now.py 2026-10-01 2026-10-31   # explicit window

Requires the sapcrawling_sales1_batch + batch_sap_logon modules to
be importable from the same directory.
"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta


def _parse_date(s):
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    raise SystemExit(f"[fatal] cannot parse date '{s}'. "
                     f"Accepted: YYYY-MM-DD, DD.MM.YYYY, DD/MM/YYYY")


def main():
    import sapcrawling_sales1_batch as sa
    from batch_sap_logon import ensure_sap_session, sap_lock

    # Date window: same logic sapcrawling_sales1_batch.main() uses, but
    # exposed to the CLI so you can pull a historical window to seed
    # so_do_billing_test.
    today = date.today()
    if len(sys.argv) >= 3:
        d_from = _parse_date(sys.argv[1])
        d_to   = _parse_date(sys.argv[2])
    else:
        if today <= sa.first_business_day(today):
            last_day_prev = sa.month_start(today) - timedelta(days=1)
            d_from = sa.month_start(last_day_prev)
            d_to   = today
        else:
            d_from = sa.month_start(today)
            d_to   = today

    print(f"[run_rsm_raw_now] window: {d_from} ~ {d_to}")
    print(f"[run_rsm_raw_now] output: {sa.OUT_CSV_RSM_RAW}")
    print(f"[run_rsm_raw_now] layout: {sa.LAYOUT_NAME_RSM_RAW}")

    sa.ensure_out_dir(sa.OUT_CSV_RSM_RAW)

    print("[run_rsm_raw_now] attaching / logging in to SAP...")
    with sap_lock:
        sess, status = ensure_sap_session()
        print(f"[run_rsm_raw_now] SAP session {status}.")
        try:
            sess.findById("wnd[0]").maximize()
        except Exception:
            pass

        # ensure_sap_session uses DD.MM.YYYY throughout batch_sap_logon,
        # which is the SAP-standard format and the first candidate the
        # main batch tries, so start with it; _export_second_layout
        # will fail loudly if the operator's profile needs a different
        # format (run the full sapcrawling_sales1_batch.main() once
        # first in that case so the primary pass can negotiate the fmt).
        sa._export_second_layout(
            sess,
            sa.LAYOUT_NAME_RSM_RAW,
            sa.OUT_CSV_RSM_RAW,
            d_from,
            d_to,
            "%d.%m.%Y",
        )

    print("[run_rsm_raw_now] done.")
    print(f"[run_rsm_raw_now] CSV ready at: {sa.OUT_CSV_RSM_RAW}")
    print(f"[run_rsm_raw_now] send its HEADER ROW back for the schema design.")


if __name__ == "__main__":
    main()
