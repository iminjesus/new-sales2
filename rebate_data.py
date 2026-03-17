"""
rebate_data.py  –  CSV → MySQL import for rebate tables.

두 CSV 파일을 파싱하여 rebate_customer_map, rebate_structure 테이블에 삽입합니다.

Usage:
    python rebate_data.py

CSV files required (same directory):
    rebate_customer.csv   – Sold-to ↔ rebate code mapping
    rebate_category.csv   – Category tier definitions (2 rows per category)
"""

import csv
import os
import re
import mysql.connector

DB = dict(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASS", ""),
    database=os.getenv("DB_NAME", "my_new_database"),
    port=int(os.getenv("DB_PORT", "3306")),
)

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
RAW_DIR      = os.path.join(BASE_DIR, "rawdata", "unlock")
CUSTOMER_CSV = os.path.join(RAW_DIR, "rebate_structure.csv")
CATEGORY_CSV = os.path.join(RAW_DIR, "rebate_category.csv")


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def clean_number(raw: str) -> float | None:
    """'$5,000', ' 2000 ', '0.0%', '-', '$-' 등 → float, 파싱 불가 시 None"""
    s = raw.strip().replace(",", "").replace("$", "").replace("%", "").strip()
    if s in ("", "-", "$-", "$- ", "- "):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return None


def unit_from_name(name: str) -> str:
    """
    구조 코드에서 단위 추출.
    패턴: {REGION}_{PRODUCT}_{A|Q}_min_...
    세 번째 '_' 구분자 위치의 A 또는 Q.
    """
    parts = name.split("_")
    for part in parts:
        if part in ("A", "Q"):
            return part
    return "A"   # 기본값


# ──────────────────────────────────────────────────────────────
# 1. Parse customer mapping CSV
# ──────────────────────────────────────────────────────────────

def parse_customer_csv(path: str) -> list[tuple[str, str, str]]:
    """
    Returns list of (sold_to, territory, structure_name).
    territory: 'TTL' | 'HK' | 'LF'
    """
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for line in reader:
            if len(line) < 1 or not line[0].strip():
                continue
            sold_to = line[0].strip()
            ttl = line[1].strip() if len(line) > 1 else ""
            hk  = line[2].strip() if len(line) > 2 else ""
            lf  = line[3].strip() if len(line) > 3 else ""
            if ttl:
                rows.append((sold_to, "TTL", ttl))
            if hk:
                rows.append((sold_to, "HK", hk))
            if lf:
                rows.append((sold_to, "LF", lf))
    return rows


# ──────────────────────────────────────────────────────────────
# 2. Parse category tier CSV
# ──────────────────────────────────────────────────────────────

def parse_category_csv(path: str) -> list[tuple[str, str, int, float, float]]:
    """
    Returns list of (structure_name, unit, tier_order, threshold, rate).

    CSV 구조: 카테고리마다 2행
      행 1: category_code, t0, t1, ..., t14   (15개 임계값)
      행 2: (빈칸),         r0, r1, ..., r14   (15개 비율%)
    """
    tiers = []
    # 인코딩 자동 감지: utf-8-sig → utf-8 → cp1252 순으로 시도
    for enc in ("utf-8-sig", "utf-8", "cp1252", "cp949"):
        try:
            with open(path, encoding=enc) as _f:
                _f.read()
            break
        except UnicodeDecodeError:
            continue
    else:
        enc = "utf-8-sig"

    with open(path, newline="", encoding=enc) as f:
        reader = csv.reader(f)
        header = next(reader)  # skip header "Category,,,,..."
        print(f"  [category CSV] encoding={enc}, header cols={len(header)}")

        pending_name = None
        pending_thresholds = []
        row_count = 0

        for line in reader:
            row_count += 1
            if not line:
                continue

            first = line[0].strip()
            if row_count <= 4:
                print(f"  [row {row_count}] first={repr(first)}, cols={len(line)}")

            if first == "":
                # 비율 행 – 직전 카테고리의 rate 행
                if pending_name is None:
                    continue
                rates = []
                for cell in line[1:]:
                    v = clean_number(cell)
                    rates.append(v if v is not None else 0.0)

                unit = unit_from_name(pending_name)
                n = min(len(pending_thresholds), len(rates))
                for i in range(n):
                    tiers.append((pending_name, unit, i, pending_thresholds[i], rates[i]))

                pending_name = None
                pending_thresholds = []

            else:
                # 임계값 행 – 새 카테고리
                pending_name = first
                pending_thresholds = []
                for cell in line[1:]:
                    v = clean_number(cell)
                    pending_thresholds.append(v if v is not None else 0.0)

    return tiers


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    customer_rows = parse_customer_csv(CUSTOMER_CSV)
    tier_rows     = parse_category_csv(CATEGORY_CSV)

    print(f"Customer mapping rows : {len(customer_rows)}")
    print(f"Structure tier rows   : {len(tier_rows)}")

    conn = mysql.connector.connect(**DB)
    cur  = conn.cursor()

    # Create / update tables
    sql_path = os.path.join(BASE_DIR, "rebate_setup.sql")
    with open(sql_path, encoding="utf-8") as f:
        for stmt in f.read().split(";"):
            # 주석 줄 제거 후 실행 여부 판단
            lines = [l for l in stmt.splitlines() if not l.strip().startswith("--")]
            stmt = "\n".join(lines).strip()
            if stmt:
                cur.execute(stmt)
    conn.commit()

    # ── rebate_customer_map ──────────────────────────────────
    cur.execute("DELETE FROM rebate_customer_map")
    cur.executemany(
        "INSERT IGNORE INTO rebate_customer_map (sold_to, territory, structure_name) "
        "VALUES (%s, %s, %s)",
        customer_rows,
    )
    print(f"  → customer_map inserted: {cur.rowcount} rows")

    # ── rebate_structure ─────────────────────────────────────
    cur.execute("DELETE FROM rebate_structure")
    cur.executemany(
        "INSERT IGNORE INTO rebate_structure "
        "(structure_name, unit, tier_order, threshold, rate) "
        "VALUES (%s, %s, %s, %s, %s)",
        tier_rows,
    )
    print(f"  → rebate_structure inserted: {cur.rowcount} rows")

    conn.commit()
    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
