#!/usr/bin/env python3
"""
vehicle_rego.py — Australian vehicle-registration data collector.

Pulls the BITRE Motor Vehicle Census (and related registration datasets)
from data.gov.au's CKAN API, downloads every CSV / ZIP-of-CSVs resource,
identifies the postcode × make × model tables, and writes normalized
CSVs ready for Power BI, tyre-market sizing, or dealer-territory
analysis.

Why this approach vs. hard-coding a URL:
  BITRE re-publishes annually (sometimes twice a year) and the resource
  URLs change every release.  Searching by name + downloading whatever
  resources the picked dataset carries keeps the script working across
  years without edits.

Usage:
  python3 vehicle_rego.py                       # search + download + process
  python3 vehicle_rego.py --list                # only list matching datasets
  python3 vehicle_rego.py --dataset-id <slug>   # pick a specific dataset
  python3 vehicle_rego.py --out out/rego        # output directory
  python3 vehicle_rego.py --keep-raw            # keep raw downloads too

Requirements:
  Python 3.8+, `requests`, `pandas`.  Install:
      pip install requests pandas openpyxl

Author: Claude Code
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
import time
import zipfile
from pathlib import Path
from typing import Iterable

import requests

try:
    import pandas as pd
except ImportError:
    sys.stderr.write(
        "\n[fatal] pandas is required. Install with: pip install pandas openpyxl\n"
    )
    sys.exit(1)


CKAN_BASE = "https://data.gov.au/data/api/3/action"
DEFAULT_QUERY = "motor vehicle census"
DEFAULT_ROWS  = 20
USER_AGENT    = "vehicle_rego.py/1.0 (+bitre-fetch)"

# Column-name synonyms so we recognise the same field across BITRE years.
POSTCODE_ALIASES = {"postcode", "post_code", "registered_postcode", "regd_postcode"}
MAKE_ALIASES     = {"make", "vehicle_make", "manufacturer"}
MODEL_ALIASES    = {"model", "vehicle_model"}
STATE_ALIASES    = {"state", "state_abb", "state_territory",
                    "state_of_registration", "state_of_regn"}
QTY_ALIASES      = {"count", "number", "vehicles", "qty", "quantity",
                    "registered_vehicles", "no_vehicles", "n"}
TYPE_ALIASES     = {"vehicle_type", "type", "vehicle_category"}
MOTIVE_ALIASES   = {"motive_power", "fuel", "fuel_type"}
YEAR_ALIASES     = {"year_of_manufacture", "manufacture_year", "manufacturing_year",
                    "year_manufactured", "yom", "vehicle_year", "model_year"}


# ── HTTP helpers ─────────────────────────────────────────────────────
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _get_json(sess: requests.Session, url: str, *, tries: int = 3) -> dict:
    """CKAN endpoint call with light retry so a single flaky request
    doesn't kill the whole run."""
    last_err = None
    for i in range(tries):
        try:
            r = sess.get(url, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"CKAN request failed after {tries} tries: {url} — {last_err}")


def _download(sess: requests.Session, url: str, dest: Path, *, tries: int = 3) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    last_err = None
    for i in range(tries):
        try:
            with sess.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 16):
                        if chunk:
                            f.write(chunk)
            return dest
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"download failed: {url} — {last_err}")


# ── CKAN search / resource pick ──────────────────────────────────────
def search_packages(sess: requests.Session, query: str, rows: int = DEFAULT_ROWS) -> list[dict]:
    """Return package summaries matching the free-text query.  Filters
    to items whose organisation or title hints at BITRE / vehicle
    registration so unrelated hits (bus / cycling / accident data with
    the word "vehicle") stay out."""
    url = f"{CKAN_BASE}/package_search?q={requests.utils.quote(query)}&rows={rows}"
    data = _get_json(sess, url)
    hits = data.get("result", {}).get("results", []) or []

    def _looks_relevant(pkg: dict) -> bool:
        title = (pkg.get("title") or "").lower()
        org   = (pkg.get("organization") or {}).get("title", "").lower()
        return (
            "vehicle" in title
            and ("census" in title or "registered" in title or "registration" in title)
        ) or "bitre" in org or "infrastructure" in org

    return [p for p in hits if _looks_relevant(p)]


def get_package(sess: requests.Session, pkg_id: str) -> dict:
    url = f"{CKAN_BASE}/package_show?id={requests.utils.quote(pkg_id)}"
    return _get_json(sess, url).get("result") or {}


# ── file classification + reading ────────────────────────────────────
def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (text or "").lower()).strip("_")


def _norm_cols(cols: Iterable[str]) -> dict[str, str]:
    """Map original column names → normalised keys we care about."""
    out = {}
    for c in cols:
        key = _slug(str(c))
        if key in POSTCODE_ALIASES:  out[c] = "postcode"
        elif key in MAKE_ALIASES:    out[c] = "make"
        elif key in MODEL_ALIASES:   out[c] = "model"
        elif key in STATE_ALIASES:   out[c] = "state"
        elif key in QTY_ALIASES:     out[c] = "qty"
        elif key in TYPE_ALIASES:    out[c] = "vehicle_type"
        elif key in MOTIVE_ALIASES:  out[c] = "motive_power"
        elif key in YEAR_ALIASES:    out[c] = "year_of_manufacture"
    return out


def _read_csv_or_excel(path: Path) -> pd.DataFrame | None:
    """Read a CSV / Excel file with a couple of encoding fallbacks —
    BITRE's older CSVs sometimes ship as latin-1."""
    suffix = path.suffix.lower()
    try:
        if suffix in (".xlsx", ".xls"):
            return pd.read_excel(path)
        for enc in ("utf-8", "utf-8-sig", "latin-1"):
            try:
                return pd.read_csv(path, encoding=enc, low_memory=False)
            except UnicodeDecodeError:
                continue
        return pd.read_csv(path, encoding_errors="replace", low_memory=False)
    except Exception as e:
        print(f"  [warn] couldn't read {path.name}: {e}")
        return None


def _classify(df: pd.DataFrame) -> tuple[str, dict[str, str]]:
    """Look at df's columns; return (kind, colmap).

    Kinds are ordered from richest to sparsest — the richer the
    dimension set, the more analytics you can do with it later.
    year_of_manufacture is a top-tier dimension because BITRE sometimes
    publishes it in a separate resource; when it appears together with
    make/model it unlocks fleet-age / renewal-cycle analysis.
    """
    colmap = _norm_cols(df.columns)
    keys = set(colmap.values())
    has_qty = "qty" in keys
    if not has_qty:
        return "other", colmap
    has_pc   = "postcode" in keys
    has_mk   = "make" in keys
    has_md   = "model" in keys
    has_yr   = "year_of_manufacture" in keys

    if has_pc and has_mk and has_md and has_yr: return "postcode_make_model_year", colmap
    if has_mk and has_md and has_yr:            return "make_model_year",          colmap
    if has_pc and has_mk and has_md:            return "postcode_make_model",      colmap
    if has_pc and has_mk:                       return "postcode_make",            colmap
    if has_mk and has_md:                       return "make_model",               colmap
    if has_pc:                                  return "postcode_only",            colmap
    if has_yr and has_qty:                      return "year_only",                colmap
    return "other", colmap


def _extract_zip(zip_path: Path, dest_dir: Path) -> list[Path]:
    """Return the list of member files extracted (only CSV / xlsx)."""
    out = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if name.endswith("/"): continue
            low = name.lower()
            if not low.endswith((".csv", ".xlsx", ".xls")): continue
            dst = dest_dir / Path(name).name
            with zf.open(name) as src, open(dst, "wb") as fout:
                fout.write(src.read())
            out.append(dst)
    return out


# ── main pipeline ────────────────────────────────────────────────────
_YEAR_RX = re.compile(r"(?<!\d)(20\d{2})(?!\d)")

def _detect_census_year(*strs: str) -> int | None:
    """Return the newest 4-digit 20YY year token seen in any of the
    passed strings (resource name, URL, filename), or None if there
    isn't one.  Used to filter out older-census resources on datasets
    that ship multiple years side by side."""
    years = []
    for s in strs:
        if not s: continue
        for m in _YEAR_RX.finditer(str(s)):
            years.append(int(m.group(1)))
    return max(years) if years else None


def process_package(
    sess: requests.Session,
    pkg: dict,
    out_dir: Path,
    keep_raw: bool,
    min_qty: int = 0,
    rollup: bool = False,
    keep_noise: bool = False,
    census_year: int | None = None,
    all_years: bool = False,
) -> dict[str, Path]:
    """Download every relevant resource in pkg, classify, and write
    normalised outputs.  Returns a dict of {kind: path_written}."""
    raw_dir = out_dir / "_raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # First pass over resources: detect each one's census year so we
    # can filter the pool to a single reporting cycle.  BITRE packages
    # sometimes carry 4-5 years' worth of files side by side; without
    # this filter every year gets pooled and the postcode totals blow
    # up to 2-3× the real vehicle count.
    all_resources = pkg.get("resources") or []
    resource_years: dict[int, list[dict]] = {}
    for res in all_resources:
        yr = _detect_census_year(res.get("name"), res.get("url"),
                                 res.get("description"))
        resource_years.setdefault(yr or 0, []).append(res)
    detected = sorted([y for y in resource_years if y], reverse=True)
    if detected:
        print(f"  census years detected in pkg: {detected}")

    if all_years or not detected:
        pick_years = set(resource_years.keys())
    elif census_year is not None:
        if census_year not in detected:
            print(f"  [warn] --census-year {census_year} not on this package "
                  f"(available: {detected}); nothing to download")
            return {}
        pick_years = {census_year, 0}
    else:
        pick_years = {detected[0], 0}
        print(f"  → keeping census year {detected[0]} only "
              f"(pass --all-years to pool every year, or --census-year YYYY to force)")
    pick_resources = [r for yr, rs in resource_years.items()
                      if yr in pick_years for r in rs]

    downloaded: list[Path] = []
    used_names: set[str] = set()
    for res in pick_resources:
        url  = res.get("url") or ""
        name = res.get("name") or Path(url).name or "resource"
        fmt  = (res.get("format") or "").lower()
        low  = url.lower()
        if not any(low.endswith(x) for x in (".csv", ".xlsx", ".xls", ".zip")) \
                and fmt not in ("csv", "xlsx", "xls", "zip"):
            continue

        # Filename strategy: the URL's own basename is almost always
        # unique (BITRE ships each resource with a distinct
        # rva-YYYY-mvs-*.csv name), so use that first.  Fall back to a
        # slug of the resource name only when the URL doesn't carry a
        # usable filename.  Truncating similar names to 64 chars used
        # to collapse three or four resources onto the same file and
        # Windows would then throw a "Permission denied" on the second
        # writer.  Belt-and-braces: add "_1", "_2" suffixes if a
        # collision still slips through.
        url_base = Path(url.split("?", 1)[0]).name  # strip any query string
        if url_base and "." in url_base:
            stem = Path(url_base).stem
            ext  = Path(url_base).suffix.lower()
        else:
            stem = _slug(name)[:80] or "resource"
            ext  = f".{fmt}" if fmt else ".csv"
        stem = _slug(stem)[:100] or "resource"
        candidate = f"{stem}{ext}"
        i = 1
        while candidate in used_names:
            candidate = f"{stem}_{i}{ext}"
            i += 1
        used_names.add(candidate)
        dest = raw_dir / candidate

        print(f"  → downloading: {name}")
        try:
            _download(sess, url, dest)
            downloaded.append(dest)
        except Exception as e:
            print(f"    [warn] {e}")

    if not downloaded:
        print("  [warn] no downloadable CSV / XLSX / ZIP resources found")
        return {}

    # unzip ZIPs into raw_dir so downstream processing sees plain files
    plain: list[Path] = []
    for p in downloaded:
        if p.suffix.lower() == ".zip":
            plain.extend(_extract_zip(p, raw_dir))
        else:
            plain.append(p)

    # classify each and collect the best-matching frames.
    # Each entry is (filename, renamed_df) so we can log and pick the
    # best file per bucket further down.
    buckets: dict[str, list[tuple[str, pd.DataFrame]]] = {
        "postcode_make_model_year": [],   # richest: every dimension
        "make_model_year":          [],   # year present but no postcode
        "postcode_make_model":      [],
        "postcode_make":            [],
        "make_model":               [],
        "postcode_only":            [],
        "year_only":                [],
    }
    for path in plain:
        df = _read_csv_or_excel(path)
        if df is None or df.empty: continue
        kind, colmap = _classify(df)
        if kind == "other": continue
        # rename cols to normalised names and drop everything else
        keep = [c for c in df.columns if c in colmap]
        renamed = df[keep].rename(columns=colmap).copy()
        # normalise types
        if "postcode" in renamed.columns:
            renamed["postcode"] = (
                renamed["postcode"].astype(str).str.extract(r"(\d{3,4})", expand=False)
            )
        if "qty" in renamed.columns:
            renamed["qty"] = pd.to_numeric(renamed["qty"], errors="coerce").fillna(0).astype(int)
        for c in ("make", "model", "state", "vehicle_type", "motive_power"):
            if c in renamed.columns:
                renamed[c] = renamed[c].astype(str).str.strip().str.upper()
        if "year_of_manufacture" in renamed.columns:
            renamed["year_of_manufacture"] = (
                pd.to_numeric(renamed["year_of_manufacture"], errors="coerce")
                  .astype("Int64")
            )

        # BITRE ships TOTAL summary rows inside the same CSV — e.g.
        # a row with motive_power='TOTAL' (or blank) that already sums
        # the fuel types below it, or vehicle_type='TOTAL' that sums
        # passenger + LCV + HCV + buses + motorcycles.  Leaving them
        # in doubles everything when we later aggregate.
        #
        # Trick: apply the summary drop per-column only when THAT
        # column has at least one non-summary value.  Otherwise a file
        # that's intentionally aggregated at (say) fuel-type level
        # would have every row wiped.
        SUMMARY_TOKENS = ("-", "—", "TOTAL", "ALL", "nan", "NAN", "")
        summary_mask = None
        summary_cols = ("state", "make", "model", "postcode",
                        "vehicle_type", "motive_power")
        drop_notes = []
        for c in summary_cols:
            if c not in renamed.columns:
                continue
            col_str = renamed[c].astype(str).str.strip()
            is_summary = col_str.isin(SUMMARY_TOKENS)
            # Only drop when this column carries real detail elsewhere.
            if not (~is_summary).any():
                continue
            summary_mask = is_summary if summary_mask is None else (summary_mask | is_summary)
            n_sum = int(is_summary.sum())
            if n_sum:
                drop_notes.append(f"{c}={n_sum:,}")
        if summary_mask is not None and summary_mask.any():
            before = len(renamed)
            renamed = renamed[~summary_mask].copy()
            print(f"      dropped {before - len(renamed):,} TOTAL / summary rows "
                  f"({', '.join(drop_notes)})")

        buckets[kind].append((path.name, renamed))
        print(f"    ✓ {path.name}  →  {kind}  ({len(renamed):,} rows)")

    # write one normalised CSV per non-empty bucket
    written: dict[str, Path] = {}
    aggregated: dict[str, pd.DataFrame] = {}
    # Dimension columns that make one file a strict superset of
    # another when a bucket receives multiple resources.
    CANON_DIMS = ("state", "postcode", "vehicle_type", "motive_power",
                  "year_of_manufacture", "make", "model")
    for kind, entries in buckets.items():
        if not entries: continue
        # Multiple resources land in the same bucket when BITRE
        # publishes the same universe cut several ways (e.g., three
        # postcode-only slices split by vehicle_type / motive_power /
        # year of manufacture).  Summing them multiplies the total by
        # the number of cuts — the 3x / 2x blow-up we were seeing.
        # Pick the ONE file with the most canonical dimensions (most
        # detailed breakdown); rollup will collapse it back to the
        # bucket's nominal grain with the correct total.
        if len(entries) > 1:
            def _score(entry):
                fname, df = entry
                return (
                    sum(1 for c in CANON_DIMS if c in df.columns),
                    len(df),
                )
            entries_sorted = sorted(entries, key=_score, reverse=True)
            picked_name, picked_df = entries_sorted[0]
            skipped = [n for n, _ in entries_sorted[1:]]
            print(f"  [dedupe] {kind}: {len(entries)} resources match this bucket. "
                  f"Keeping {picked_name}; skipping {len(skipped)} redundant "
                  f"cut(s): {', '.join(skipped)}")
            frames = [picked_df]
        else:
            frames = [entries[0][1]]
        merged = pd.concat(frames, ignore_index=True)
        # aggregate — duplicate rows across resource splits collapse to one
        group_cols = [c for c in (
            "state", "postcode", "vehicle_type",
            "motive_power", "year_of_manufacture", "make", "model",
        ) if c in merged.columns]
        if group_cols and "qty" in merged.columns:
            merged = merged.groupby(group_cols, dropna=False, as_index=False)["qty"].sum()

        # BITRE noise-floor drop — MUST run BEFORE rollup.  For
        # (postcode × make × model × fuel) slices at very small counts
        # (0-3 actual vehicles) BITRE publishes the count as either 0
        # or 3 to protect privacy; counts of 4+ are published as the
        # real integer.  We drop the two ambiguous buckets:
        #   qty == 0  → real value 0, 1, or 2 — no signal.
        #   qty == 3  → real value 1, 2, or 3 — signal too thin.
        # Everything from qty ≥ 4 is trusted as the true count.
        #
        # Order matters: dropping BEFORE the fuel/body rollup prevents
        # phantom values like 6 (= 3+3), 9 (= 3+3+3 or 3+6), and 7
        # (= 3+4) from being manufactured out of noise+signal sums.
        # After the drop, every remaining slice is ≥ 4 so the rolled-up
        # sum is trustworthy end-to-end.  --keep-noise skips the drop.
        if "qty" in merged.columns and not keep_noise:
            before = len(merged)
            merged = merged[~merged["qty"].isin([0, 3])]
            if before != len(merged):
                print(f"      {kind}: dropped {before - len(merged):,} confidentialised rows (qty ∈ 0,3)")

        # Rollup: collapse over vehicle_type + motive_power so each
        # (state, postcode, make, model[, year]) row shows the *sum*
        # across fuel types / body styles.  Runs AFTER the noise drop
        # so the sum only combines trusted (≥ 4) values.
        if rollup:
            drop_dims = [c for c in ("vehicle_type", "motive_power") if c in merged.columns]
            if drop_dims:
                keep_dims = [c for c in group_cols if c not in drop_dims]
                if keep_dims and "qty" in merged.columns:
                    merged = merged.groupby(keep_dims, dropna=False, as_index=False)["qty"].sum()
                    group_cols = keep_dims

        # Optional stricter floor on top of the RR3-noise drop.
        if min_qty > 0 and "qty" in merged.columns:
            before = len(merged)
            merged = merged[merged["qty"] >= min_qty]
            if before != len(merged):
                print(f"      {kind}: dropped {before - len(merged):,} rows below qty {min_qty}")

        if group_cols:
            merged.sort_values(group_cols, inplace=True, ignore_index=True)
        out_path = out_dir / f"vehicle_{kind}.csv"
        try:
            merged.to_csv(out_path, index=False)
        except PermissionError:
            # Common Windows failure mode — the CSV is open in Notepad
            # / Excel / another viewer, so pandas can't overwrite it.
            # Fall back to a timestamped filename so the run doesn't
            # abort, and tell the user how to fix it cleanly.
            from datetime import datetime as _dt
            stamp = _dt.now().strftime("%Y%m%d_%H%M%S")
            alt = out_path.with_name(f"vehicle_{kind}_{stamp}.csv")
            print(f"  [warn] {out_path.name} is locked (open in Notepad/Excel?). "
                  f"Writing to {alt.name} instead — close the other viewer "
                  f"and re-run to overwrite the canonical file.")
            merged.to_csv(alt, index=False)
            out_path = alt
        total_qty = int(merged["qty"].sum()) if "qty" in merged.columns else 0
        print(f"  wrote {out_path.name}  ({len(merged):,} rows · Σ qty {total_qty:,})")
        # State-coverage sanity report — a common failure mode is that
        # BITRE ships some releases as per-state ZIPs and the download
        # only picked up 1-2 states; the coverage line makes that
        # obvious at a glance.
        if "state" in merged.columns:
            state_counts = merged["state"].value_counts()
            state_line = ", ".join(f"{s}:{n:,}" for s, n in state_counts.items())
            print(f"      states covered ({len(state_counts)}): {state_line}")
        written[kind] = out_path
        aggregated[kind] = merged

    # Derived estimate: postcode × make × model × year.
    #
    # BITRE publishes postcode × make × model (no year) and separately
    # make × model × year (no postcode).  Combine them with a
    # proportional allocation — for each (make, model) pair the
    # year-mix from the second file is applied to the postcode-level
    # totals from the first.  Assumption: within a make/model, age
    # distribution is roughly uniform across postcodes.  That's not
    # perfect (richer areas tend to skew newer) but it's a defensible
    # starting point for tyre-replacement-cycle sizing.
    if "postcode_make_model" in aggregated and "make_model_year" in aggregated \
            and "postcode_make_model_year" not in written:
        pmm  = aggregated["postcode_make_model"]
        mmy  = aggregated["make_model_year"]

        # Step 1: pre-collapse mmy to unique (make, model, year).  It
        # arrived aggregated at (make, model, year, vehicle_type,
        # motive_power) so the same (make, model, year) appears several
        # times.  Without this collapse the (make, model) join on the
        # right side had ~300 rows per key and blew out to a 1.25 B-row
        # merge (~9 GB) before the OS ran out of RAM.
        mmy = (mmy.groupby(["make", "model", "year_of_manufacture"],
                           dropna=False, as_index=False)["qty"].sum())

        # Step 2: per-(make, model) year share, sums to 1 within each pair
        totals = (mmy.groupby(["make", "model"], as_index=False)["qty"]
                     .sum().rename(columns={"qty": "mm_total"}))
        share = mmy.merge(totals, on=["make", "model"], how="left")
        share = share[share["mm_total"] > 0].copy()
        share["year_share"] = share["qty"] / share["mm_total"]
        share = share[["make", "model", "year_of_manufacture", "year_share"]]
        share.sort_values(["make", "model"], inplace=True, ignore_index=True)

        # Step 3: stream the merge in chunks of pmm and append to the
        # output CSV.  Each chunk × share is small enough to hold in
        # RAM, and pmm is already deduplicated on
        # (state, postcode, vehicle_type, motive_power, make, model) so
        # no post-pass groupby is needed.
        est_path = out_dir / "vehicle_postcode_make_model_year_estimate.csv"
        CHUNK = 200_000
        header_written = False
        n_written = 0
        for start in range(0, len(pmm), CHUNK):
            chunk = pmm.iloc[start:start + CHUNK]
            part  = chunk.merge(share, on=["make", "model"], how="inner")
            part["qty"] = (part["qty"] * part["year_share"]).round().astype(int)
            part.drop(columns=["year_share"], inplace=True)
            part = part[part["qty"] > 0]
            part.to_csv(est_path, mode="w" if not header_written else "a",
                        index=False, header=not header_written)
            header_written = True
            n_written += len(part)
        print(f"  wrote {est_path.name}  ({n_written:,} rows)  "
              f"[derived: postcode_make_model × make_model_year year-share, streamed]")
        written["postcode_make_model_year_estimate"] = est_path

    if not keep_raw:
        for p in raw_dir.iterdir():
            try: p.unlink()
            except Exception: pass
        try: raw_dir.rmdir()
        except Exception: pass

    return written


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--query", default=DEFAULT_QUERY,
                    help=f"CKAN search query (default: '{DEFAULT_QUERY}')")
    ap.add_argument("--dataset-id", default=None,
                    help="Skip search — download this data.gov.au dataset id / slug directly")
    ap.add_argument("--list", action="store_true",
                    help="Only list matching datasets, don't download")
    ap.add_argument("--list-resources", action="store_true",
                    help="For --dataset-id, print every resource (name/format/url) "
                         "instead of downloading — helps you spot the "
                         "'manufacturing year' file before processing.")
    ap.add_argument("--out", default="out/rego", help="output directory")
    ap.add_argument("--keep-raw", action="store_true",
                    help="keep the raw downloads under out/_raw/")
    ap.add_argument("--min-qty", type=int, default=0,
                    help="Optional stricter floor on qty (default: %(default)s, "
                         "disabled).  Regardless of this flag, BITRE's RR3 "
                         "noise floor rows (qty ∈ {0, 3}) are always dropped "
                         "because they represent 0-4 actual vehicles and "
                         "carry too little signal to be worth keeping — "
                         "pass --keep-noise to override.")
    ap.add_argument("--keep-noise", action="store_true",
                    help="Skip the always-on drop of qty ∈ {0, 3} rows.")
    ap.add_argument("--census-year", type=int, default=None,
                    help="Only download / process resources for this census "
                         "year (default: newest year found in the package).  "
                         "BITRE packages sometimes carry several years' worth "
                         "of CSVs side by side; pooling all of them multiplies "
                         "the postcode totals by however many years are on "
                         "the package, so we pick one year by default.")
    ap.add_argument("--all-years", action="store_true",
                    help="Pool every year found on the package (the old "
                         "behaviour).  Only sensible for a longitudinal "
                         "study that keeps year_of_manufacture separate.")
    ap.add_argument("--no-rollup", dest="rollup", action="store_false",
                    help="Keep vehicle_type + motive_power dimensions "
                         "separate.  The default is to roll those up so "
                         "each (state, postcode, make, model[, year]) row "
                         "shows a single summed qty — coarser cells "
                         "cancel out most of the RR3 rounding error.")
    ap.set_defaults(rollup=True)
    args = ap.parse_args(argv)

    sess = _session()
    out  = Path(args.out).resolve()

    if args.dataset_id:
        pkg = get_package(sess, args.dataset_id)
        if not pkg:
            print(f"[error] dataset id '{args.dataset_id}' not found on data.gov.au")
            return 1
        print(f"Using dataset: {pkg.get('title')}  ({pkg.get('name')})")
        if args.list_resources:
            resources = pkg.get("resources") or []
            print(f"\n{len(resources)} resource(s):")
            for i, res in enumerate(resources, 1):
                nm  = res.get("name") or "?"
                fmt = (res.get("format") or "?").upper()
                url = res.get("url") or ""
                print(f"  [{i:>2}] {fmt:<6}  {nm}")
                print(f"       {url}")
            return 0
        process_package(sess, pkg, out, keep_raw=args.keep_raw,
                        min_qty=args.min_qty, rollup=args.rollup,
                        keep_noise=args.keep_noise,
                        census_year=args.census_year,
                        all_years=args.all_years)
        return 0

    hits = search_packages(sess, args.query)
    if not hits:
        print(f"[error] no datasets matched '{args.query}'.  Try browsing "
              f"https://data.gov.au/data/dataset?q={requests.utils.quote(args.query)}")
        return 1

    print(f"Found {len(hits)} matching dataset(s):")
    for i, pkg in enumerate(hits, 1):
        title = pkg.get("title") or pkg.get("name") or "?"
        org   = (pkg.get("organization") or {}).get("title") or "?"
        slug  = pkg.get("name") or "?"
        n_res = len(pkg.get("resources") or [])
        print(f"  [{i:>2}] {title}")
        print(f"       id={slug}  org={org}  resources={n_res}")

    if args.list:
        return 0

    # Default: process the FIRST match.  User can override with --dataset-id.
    pick = hits[0]
    print(f"\nProcessing first match — pass --dataset-id to pick another.")
    print(f"→ {pick.get('title')}\n")
    process_package(sess, pick, out, keep_raw=args.keep_raw,
                    min_qty=args.min_qty, rollup=args.rollup)
    return 0


if __name__ == "__main__":
    sys.exit(main())
