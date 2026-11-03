# CLAUDE.md

Project-specific instructions for Claude Code. Read first every session.

## Project overview

**Hankook Tyre Australia — Sales Dashboard**
Flask + MySQL web app serving an internal sales/ops dashboard.

- Backend: `app.py` (single large Flask module; ~1 MB)
- Frontend: inline HTML + vanilla JS under `static/` (no build step)
- Database: MySQL, DB name `my_new_database`
- Main UI: `static/orders.html` (the **SPRF** screen — Sales Pricing & Reference)
- Price dashboard: `price_compare.py` + `static/*price*.html`
- Crawlers (Selenium/Chrome on Angular sites):
  - `tempeweborder_crawling.py` — base crawler
  - `tempeweborder_crawling_extend.py` — **actual production crawler**, run this one
  - `NewbeeWeborder_crawling.py` — Newbee clone of Tempe
  - `jax_crawling.py`, `bobjane_crawling.py` — competitor sites
- Shared crawler helpers: `tempeweborder_shared.py`
- Price lookup (multi-source match): `tempe_price_lookup.py`

## How to run

- Flask: `python app.py` (serves on configured port; must be fully restarted to pick up `.py` changes — hot reload is off)
- Crawlers write CSVs to the project root with timestamped names
  (`Tempeweborder_YYYYMMDD_HHMM.csv`, `NewbeeWeborder_YYYYMMDD_HHMM.csv`,
  `JAX_...csv`, `BobJane_...csv`)

## Credentials & secrets

**Never commit secrets.** Credentials live in:
- `.env` (git-ignored) — see `.env.example` for the schema
- Windows Credential Manager via `keyring` — the SAP password used by
  the standalone `sap_login.py` is stored there, set once with
  `python sap_login.py --set-password`
- OS env vars for the crawler site logins (`Tempe_username`, `Tempe_password`,
  equivalents for Newbee/JAX/BobJane)
- `.sap_pwd_current` (git-ignored) — the **batch pipeline's** SAP
  password sidecar.  `batch_sap_logon.py` writes to it automatically
  whenever its `_handle_password_change` handler accepts a new password
  during a forced rotation at login, so the next scheduled run picks
  up the new value without any manual file edit.  Resolution order on
  each run: env `SAP_PWD` → sidecar → `_SAP_PWD_DEFAULT` constant
  baked into `batch_sap_logon.py` (currently `Ksmpyj1234!!`).

### Password-rotation handler

`batch_sap_logon.py :: _handle_password_change(session)` fires between
the multi-logon check and the generic post-login popup-dismisser.
The handler detects the SAP change-password dialog by title/body
keyword match in English (`password`+`change/new/expired`), German
(`kennwort`/`aendern`/`neues`) **and Korean** (`비밀번호`/`패스워드`/
`암호` combined with `변경`/`만료`/`신규`/`재설정`).

When the dialog appears, the handler computes candidates dynamically
from `SAP_PWD_UNIVERSE` (3 known passwords differing only in trailing
`!` count: `Ksmpyj1234!`, `Ksmpyj1234!!`, `Ksmpyj1234!!!`) in ring
order starting from (current+1), excluding the currently-active
password.  Example: `current="Ksmpyj1234!!"` →
`candidates=["Ksmpyj1234!!!", "Ksmpyj1234!"]`.  The accepted
candidate is written to the sidecar + the module global for
immediate re-use.

**Login itself is never retried from here** — three wrong LOGIN
attempts lock the SAP account; the candidate chain only drives the
already-open change dialog, which does not consume the lockout
counter.

### SAP session keep-alive

`batch_sap_logon.py :: start_keepalive_thread()` is started by the
orchestrator's `main()` right after the heartbeat thread.  The
thread wakes every `KEEPALIVE_INTERVAL_SEC` (default 600 = 10 min,
override via env `SAP_KEEPALIVE_SEC`) and:

- try-acquires `sap_lock` non-blocking (if a pipeline step holds
  the lock the tick is skipped — the step's own SAP activity resets
  the idle timer anyway)
- attaches to the current SAP session via `_try_attach()`
- sends `/N` (reset to home transaction, SAP's softest no-op) which
  SAP counts as activity server-side
- if attach fails or ping fails, calls `ensure_sap_session()` to
  re-launch SAP Logon and re-login — this is the recovery path for
  Windows updates, PC reboots, SAP server restarts, and any other
  event that killed the session during off-hours

`sap_lock` is an `RLock` shared between `run_pipeline` (which wraps
its entire body in `with sap_lock:`) and the keep-alive thread, so
a tick that lands mid-pipeline is cleanly skipped instead of racing
on the SAP COM object.

Windows reboot / update recovery is handled end-to-end by the
existing `install_batch_scheduler.bat` entries (`SPRF-Batch-Startup`
at system boot + `SPRF-Batch-Watchdog` every 10 min) combined with
the keep-alive thread above.  No extra scheduler entry is needed.

### Sensitive data display rules (SPRF / orders.html)

- Full credit card details may only be shown to `accounts@hankooktyre.com.au`.
  Every other viewer sees a masked PAN.
- The resolver that decides which customer group a card belongs to **prefers
  `dc_basic_customer` over the historical fallback**, because customer groups
  change over time (e.g. account `739199` moved from AKD → AEU; the resolver
  must follow the current assignment, not the stale one).

## Organisation (used by approval chains, email routing, etc.)

- **MGMT approvers** (end-of-chain sign-off): Justin Harding, JunJong Cho, Kenny Kim
  - Nov-03: Justin Harding (justin.harding@hankooktyre.com.au) replaced
    Hayden Begbie in the approver_a slot after Hayden left; the
    historical rows that carry `approved_by_name = "Hayden"` are kept
    as-is for audit trail, and everything going forward writes
    "Justin" instead.
- **Price editors** (allowed to edit the price tables): Brian Park, Pamela Lau
- **CS team** (first-line customer service): Harry Jallis, HKCustomerService,
  Andy Niki, Hayden Lee
  - ⚠️ `Hayden Lee` (CS) is a genuinely separate employee from the
    former MGMT approver `Hayden Begbie` (who was the Sales head,
    left Hankook and was replaced by `Justin Harding` in Nov-03).
    They share the first name "Hayden" but are different people.
    Don't collapse them.

## Dashboard terminology

- **SPRF** = Sales Pricing & Reference — the main orders view (`static/orders.html`)
- **AEU** = Australia End User (customer group)
- **AKD** = Automotive Key Dealer (customer group)
- **OE** = Original Equipment (factory-fit vehicle size)
- **Dealer sell-in / sell-out** = wholesale price Hankook pays / dealer charges
- Price-source chips on the SPRF Remark cell:
  - `TempeConsumer` (red), `TempeDealer` (orange), `JAX` (green),
    `BobJane` (purple), `NewBDealer` (blue `#1D4ED8`)

## Crawler behaviour (Tempe + Newbee)

Both sites use the same Angular layout. The crawlers:
1. Open the login page, wait for manual login
2. Navigate to the Search page
3. For each query in the merged size list, type into `#simpleSearchText`,
   scrape the result grid, write a row per hit to the output CSV

The query list is built by `_merged_search_queries()`:
- Base 24 common sizes (hard-coded)
- Plus every size seen in **any** historical `Tempeweborder_*.csv` or
  `NewbeeWeborder_*.csv` in the project root (last 365 days by default)
- Deduplicated, >= 4 digits only (shorter is almost always a parse artifact)

`tempeweborder_crawling_extend.py` defaults to the **size-sweep**. The
legacy brand-sweep is behind `--brand-sweep` CLI flag (or `BRAND_SWEEP=1`
env var) if ever needed.

## Git / commit conventions

- Work on the feature branch named in the task description
  (`claude/<task-slug>`). Never push directly to `main`.
- One logical change per commit. Keep commit messages explanatory of
  the **why**, not the what.
- **DO NOT** put the model identifier (e.g. `claude-opus-4-7`,
  `claude-fable-5-1`) in commit messages, PR titles/bodies, or source
  code comments.
- End every commit message with the attribution lines the harness
  prints in a system-reminder at the start of the session (they rotate
  per session, so always use the current reminder — do not hard-code).

## File-layout quirks that matter

- `app.py` is one big file (~1 MB). Don't try to refactor it unprompted;
  find the right section with Grep and edit in place.
- `static/orders.html` is also huge (~500 KB) with the SPRF rendering
  logic inline. Chip renderers live under `_renderTempeRefOnRow`,
  `_buildColoredRemarkHTML`, filter state under `_REMARK_FILTER_DEFAULT`
  / `_REMARK_SOURCE_PATTERNS` / `data-rfilter` attributes.
- CSVs sitting next to the Python files are **inputs**, not junk. Don't
  delete historical `Tempeweborder_*.csv` / `NewbeeWeborder_*.csv` —
  the crawler re-reads them to build the merged size list.

## When adding a new price source

To wire a new competitor (follow the NewBDealer pattern):
1. Clone the Tempe crawler, swap only the login + search URL
2. In `tempe_price_lookup.py`, add a `_files_by_month("NewSource")` reader
   and surface it in the lookup return dict
3. In `static/orders.html`:
   - Add a colour branch in `_buildColoredRemarkHTML`
   - Add a pattern to `_REMARK_SOURCE_PATTERNS`
   - Add a default-true entry to `_REMARK_FILTER_DEFAULT`
   - Build a chip in `_renderTempeRefOnRow` + add to `_willReplace`
   - Add a `<input type="checkbox" data-rfilter="newsource">` to `#remarkFilter`
4. In `price_compare.py`, add short-key entries (e.g. `nso`/`nsi` for
   sell-out/sell-in) to the `SL`/`SC` dicts alongside existing
   `tw`/`twi` TempeDealer keys
