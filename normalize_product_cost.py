"""Match Sheet 1 `Product` strings against Sheet 2 (SIZE + brand + DESCRIPTION)
and copy the COST (and PRICE) values into Sheet 1.

Usage:
    python normalize_product_cost.py INPUT.xlsx
    python normalize_product_cost.py INPUT.xlsx -o OUTPUT.xlsx
    python normalize_product_cost.py INPUT.xlsx --sheet1 "Form1" --sheet2 "Cost"
"""

import argparse
import re
from pathlib import Path

import pandas as pd


# Tokens that add no signal when matching (decorations, marketing tags, etc.)
NOISE_TOKENS = {
    "RUNFLAT", "RFT", "SILENT", "OWL", "BSW", "WBL", "MUD", "PLUS",
    "RADIAL",  # appears only in Sheet 2's DESCRIPTION
}


def _normalize_text(s) -> str:
    """Upper-case, strip decorations, and collapse whitespace."""
    if pd.isna(s):
        return ""
    s = str(s).upper()
    # Strip common decorations: (*), ***RUNFLAT***, (Silent), brackets, asterisks
    s = re.sub(r"\(\s*\*\s*\)", " ", s)
    s = re.sub(r"[\(\)\[\]\{\}]", " ", s)
    s = re.sub(r"\*+", " ", s)
    s = re.sub(r"[+]", " ", s)
    # Normalize size separators: "2356018" -> "235/60R18" not always reliable, leave alone
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _normalize_size(size: str) -> str:
    """Normalize a tyre-size token into a canonical form for matching."""
    s = _normalize_text(size)
    # Strip "LT" / "P" prefix; the matching is done over the whole token set,
    # so if Sheet 1 has "LT265/70R15" and Sheet 2 has "265/70R15", we still want
    # them to match. We keep one canonical form by dropping the prefix for the
    # matching key only.
    s = re.sub(r"^(LT|P)\s*(?=\d)", "", s)
    # Compact "235 / 75 R 15" -> "235/75R15"
    s = re.sub(r"\s*/\s*", "/", s)
    s = re.sub(r"\s*R\s*", "R", s)
    return s


def _tokenize(s: str) -> frozenset:
    """Return a frozenset of meaningful tokens for set-based matching."""
    s = _normalize_text(s)
    tokens = set(s.split())
    # Normalize size-like tokens (e.g. drop LT/P prefixes) so they line up.
    normalized = set()
    for t in tokens:
        if t in NOISE_TOKENS:
            continue
        if re.match(r"^(LT|P)?\d{2,3}/\d{2}R\d{2}", t):
            normalized.add(_normalize_size(t))
        else:
            normalized.add(t)
    return frozenset(normalized)


def _build_lookup(df2: pd.DataFrame, size_col: str, brand_col: str,
                  desc_col: str, value_cols) -> dict:
    """Build a lookup from token-set -> dict of value columns from Sheet 2."""
    lookup: dict = {}
    for _, row in df2.iterrows():
        size = row.get(size_col, "")
        brand = row.get(brand_col, "")
        desc = row.get(desc_col, "")
        combined = f"{brand} {size} {desc}"
        key = _tokenize(combined)
        if not key:
            continue
        values = {col: row.get(col) for col in value_cols}
        # If duplicates exist, keep the first occurrence.
        lookup.setdefault(key, values)
    return lookup


def _lookup(product: str, lookup: dict):
    """Match a Sheet 1 `product` string against the Sheet 2 lookup.

    Tries:
        1. Exact token-set equality.
        2. Sheet 2 key is a subset of the product tokens (Sheet 1 has extras
           like trim/section info).
    """
    key = _tokenize(product)
    if not key:
        return None
    if key in lookup:
        return lookup[key]
    # Fall back: pick the Sheet 2 entry with the largest subset-overlap.
    best = None
    best_size = 0
    for sheet2_key, value in lookup.items():
        if sheet2_key.issubset(key) and len(sheet2_key) > best_size:
            best = value
            best_size = len(sheet2_key)
    return best


def _find_column(df: pd.DataFrame, *candidates: str) -> str:
    """Return the first column whose normalized name contains any candidate."""
    norm = {col: re.sub(r"\s+", " ", str(col)).strip().lower() for col in df.columns}
    for cand in candidates:
        target = cand.lower()
        for col, n in norm.items():
            if target in n:
                return col
    raise KeyError(f"None of {candidates!r} found in columns {list(df.columns)!r}")


def process(input_path: Path, output_path: Path, sheet1, sheet2,
            product_col: str | None = None) -> None:
    df1 = pd.read_excel(input_path, sheet_name=sheet1)
    df2 = pd.read_excel(input_path, sheet_name=sheet2)

    # Resolve column names
    if product_col is None:
        product_col = _find_column(df1, "product")
    print(f"Sheet 1 product column: {product_col!r}")

    size_col = _find_column(df2, "size")
    brand_col = _find_column(df2, "brand")
    desc_col = _find_column(df2, "description", "desc")
    value_cols = [c for c in ("COST", "PRICE", "SKU") if c in df2.columns]
    print(f"Sheet 2 columns: SIZE={size_col!r}, brand={brand_col!r}, "
          f"DESCRIPTION={desc_col!r}, value cols={value_cols}")

    lookup = _build_lookup(df2, size_col, brand_col, desc_col, value_cols)

    matches = df1[product_col].apply(lambda p: _lookup(p, lookup))
    for col in value_cols:
        df1[f"matched_{col}"] = matches.apply(
            lambda v: v.get(col) if isinstance(v, dict) else None
        )

    matched_count = matches.apply(lambda v: isinstance(v, dict)).sum()
    print(f"Matched {matched_count}/{len(df1)} rows "
          f"({matched_count / max(len(df1), 1):.1%})")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df1.to_excel(writer, sheet_name="Sheet1_with_cost", index=False)
        df2.to_excel(writer, sheet_name="Sheet2", index=False)
    print(f"Wrote {output_path}")

    # Surface unmatched rows so they can be hand-fixed.
    unmatched = df1[matches.apply(lambda v: not isinstance(v, dict))]
    if not unmatched.empty:
        print(f"\n{len(unmatched)} unmatched rows (showing up to 20):")
        for p in unmatched[product_col].head(20):
            print(f"  - {p}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="Input Excel file (with both sheets)")
    parser.add_argument("-o", "--output",
                        help="Output Excel file (default: <input>_with_cost.xlsx)")
    parser.add_argument("--sheet1", default=0,
                        help="Sheet 1 name or 0-based index (default: 0)")
    parser.add_argument("--sheet2", default=1,
                        help="Sheet 2 name or 0-based index (default: 1)")
    parser.add_argument("--product-col", default=None,
                        help="Product column name in Sheet 1 "
                             "(default: auto-detect any column containing 'product')")
    args = parser.parse_args()

    input_path = Path(args.input)
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.with_name(f"{input_path.stem}_with_cost.xlsx")

    sheet1 = int(args.sheet1) if str(args.sheet1).isdigit() else args.sheet1
    sheet2 = int(args.sheet2) if str(args.sheet2).isdigit() else args.sheet2

    process(input_path, output_path, sheet1, sheet2, args.product_col)


if __name__ == "__main__":
    main()
