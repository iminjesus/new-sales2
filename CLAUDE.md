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
- Windows Credential Manager via `keyring` — the SAP password is stored
  there, set once with `python sap_login.py --set-password`
- OS env vars for the crawler site logins (`Tempe_username`, `Tempe_password`,
  equivalents for Newbee/JAX/BobJane)

SAP connection fields (user, client, connection name) are low-sensitivity
and can live in `.env`; the SAP password is **always** in keyring.

### Sensitive data display rules (SPRF / orders.html)

- Full credit card details may only be shown to `accounts@hankooktyre.com.au`.
  Every other viewer sees a masked PAN.
- The resolver that decides which customer group a card belongs to **prefers
  `dc_basic_customer` over the historical fallback**, because customer groups
  change over time (e.g. account `739199` moved from AKD → AEU; the resolver
  must follow the current assignment, not the stale one).

## Organisation (used by approval chains, email routing, etc.)

- **MGMT approvers** (end-of-chain sign-off): Hayden Begbie, JunJong Cho, Kenny Kim
- **Price editors** (allowed to edit the price tables): Brian Park, Pamela Lau
- **CS team** (first-line customer service): Harry Jallis, HKCustomerService,
  Andy Niki, Hayden Lee
  - ⚠️ `Hayden Lee` (CS) is a different person from `Hayden Begbie` (MGMT
    approver). Don't collapse them.

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
