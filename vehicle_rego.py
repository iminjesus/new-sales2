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
STATE_ALIASES    = {"state", "state_territory", "state_of_registration", "state_of_regn"}
QTY_ALIASES      = {"count", "number", "vehicles", "qty", "quantity", "registered_vehicles", "n"}
TYPE_ALIASES     = {"vehicle_type", "type", "vehicle_category"}


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
        if key in POSTCODE_ALIASES: out[c] = "postcode"
        elif key in MAKE_ALIASES:   out[c] = "make"
        elif key in MODEL_ALIASES:  out[c] = "model"
        elif key in STATE_ALIASES:  out[c] = "state"
        elif key in QTY_ALIASES:    out[c] = "qty"
        elif key in TYPE_ALIASES:   out[c] = "vehicle_type"
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
    """Look at df's columns; return (kind, colmap) where kind is:
    - "postcode_make_model"  — the gold table (postcode × make × model)
    - "postcode_make"        — postcode × make totals
    - "make_model"           — make × model totals (no postcode)
    - "postcode_only"        — postcode totals
    - "other"                — didn't recognise
    """
    colmap = _norm_cols(df.columns)
    keys = set(colmap.values())
    has_qty = "qty" in keys
    if not has_qty:
        # Some releases pivot years into columns; try last numeric col.
        # Skip for now — user can extend the aliases list.
        return "other", colmap
    has_pc = "postcode" in keys
    has_mk = "make" in keys
    has_md = "model" in keys
    if has_pc and has_mk and has_md: return "postcode_make_model", colmap
    if has_pc and has_mk:            return "postcode_make",       colmap
    if has_mk and has_md:            return "make_model",          colmap
    if has_pc:                       return "postcode_only",       colmap
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
def process_package(
    sess: requests.Session,
    pkg: dict,
    out_dir: Path,
    keep_raw: bool,
) -> dict[str, Path]:
    """Download every relevant resource in pkg, classify, and write
    normalised outputs.  Returns a dict of {kind: path_written}."""
    raw_dir = out_dir / "_raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    downloaded: list[Path] = []
    for res in (pkg.get("resources") or []):
        url  = res.get("url") or ""
        name = res.get("name") or Path(url).name or "resource"
        fmt  = (res.get("format") or "").lower()
        low  = url.lower()
        if not any(low.endswith(x) for x in (".csv", ".xlsx", ".xls", ".zip")) \
                and fmt not in ("csv", "xlsx", "xls", "zip"):
            continue
        # infer a sane filename
        stem = _slug(name)[:64] or _slug(Path(url).name)[:64] or "resource"
        ext  = Path(url).suffix.lower() if "." in url.split("/")[-1] else f".{fmt}"
        dest = raw_dir / f"{stem}{ext}"
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

    # classify each and collect the best-matching frames
    buckets: dict[str, list[pd.DataFrame]] = {
        "postcode_make_model": [],
        "postcode_make":       [],
        "make_model":          [],
        "postcode_only":       [],
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
        for c in ("make", "model", "state", "vehicle_type"):
            if c in renamed.columns:
                renamed[c] = renamed[c].astype(str).str.strip().str.upper()
        buckets[kind].append(renamed)
        print(f"    ✓ {path.name}  →  {kind}  ({len(renamed):,} rows)")

    # write one normalised CSV per non-empty bucket
    written: dict[str, Path] = {}
    for kind, frames in buckets.items():
        if not frames: continue
        merged = pd.concat(frames, ignore_index=True)
        # aggregate — duplicate rows across resource splits collapse to one
        group_cols = [c for c in ("state", "postcode", "vehicle_type", "make", "model")
                      if c in merged.columns]
        if group_cols and "qty" in merged.columns:
            merged = merged.groupby(group_cols, dropna=False, as_index=False)["qty"].sum()
        merged.sort_values(group_cols, inplace=True, ignore_index=True)
        out_path = out_dir / f"vehicle_{kind}.csv"
        merged.to_csv(out_path, index=False)
        print(f"  wrote {out_path.name}  ({len(merged):,} rows)")
        written[kind] = out_path

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
    ap.add_argument("--out", default="out/rego", help="output directory")
    ap.add_argument("--keep-raw", action="store_true",
                    help="keep the raw downloads under out/_raw/")
    args = ap.parse_args(argv)

    sess = _session()
    out  = Path(args.out).resolve()

    if args.dataset_id:
        pkg = get_package(sess, args.dataset_id)
        if not pkg:
            print(f"[error] dataset id '{args.dataset_id}' not found on data.gov.au")
            return 1
        print(f"Using dataset: {pkg.get('title')}  ({pkg.get('name')})")
        process_package(sess, pkg, out, keep_raw=args.keep_raw)
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
    process_package(sess, pick, out, keep_raw=args.keep_raw)
    return 0


if __name__ == "__main__":
    sys.exit(main())
