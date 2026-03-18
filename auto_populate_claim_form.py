"""
auto_populate_claim_form.py
============================
Customer master 데이터를 읽어 Claim Report Form Excel 파일에
Store Name, Sold-to, Ship-to 정보를 자동으로 입력하는 스크립트.

사용법:
    python auto_populate_claim_form.py --sold_to 731942
    python auto_populate_claim_form.py --sold_to 731942 100142 724363   (여러 고객)
    python auto_populate_claim_form.py --all   (전체 고객 파일 생성)
"""

import os
import sys
import argparse
import pandas as pd
import shutil
from openpyxl import load_workbook

# ─────────────────────────────────────────────
# 경로 설정 (본인 환경에 맞게 수정하세요)
# ─────────────────────────────────────────────
BASE_DIR = r"E:\01. work\2025\Data_Anal_Website"
RAWDATA_DIR = os.path.join(BASE_DIR, "rawdata")
TEMPLATE_PATH = os.path.join(RAWDATA_DIR, "Claim report form_v2(1).xlsx")
CUSTOMER_CSV = os.path.join(RAWDATA_DIR, "unlock", "customer.csv")
OUTPUT_DIR = os.path.join(RAWDATA_DIR, "output")

# 이 스크립트를 /home/user/new-sales2 에서 실행할 경우 경로 자동 전환
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if not os.path.exists(BASE_DIR):
    BASE_DIR   = _SCRIPT_DIR
    RAWDATA_DIR = os.path.join(BASE_DIR, "rawdata")
    TEMPLATE_PATH = os.path.join(RAWDATA_DIR, "Claim report form_v2(1).xlsx")
    CUSTOMER_CSV  = os.path.join(RAWDATA_DIR, "unlock", "customer.csv")
    OUTPUT_DIR    = os.path.join(RAWDATA_DIR, "output")

# ─────────────────────────────────────────────
# 셀 위치 설정 (템플릿의 실제 셀 주소로 맞추세요)
# 스크린샷 기준: Store Name 행5, Sold-to 행8, Ship-to 행11
# 값은 레이블 오른쪽/아래 셀에 기입됩니다.
# ─────────────────────────────────────────────
CELL_STORE_NAME = "D5"   # Store Name 값 입력 셀
CELL_SOLD_TO_ID = "D6"   # Sold-to 코드
CELL_SOLD_TO_NAME = "D8" # Sold-to 회사명
CELL_SHIP_TO_ID  = "D11" # Ship-to 코드
CELL_SHIP_TO_NAME = "D12"# Ship-to 회사명
CELL_ADDRESS     = "D13" # 주소


def load_customer_master(csv_path: str) -> pd.DataFrame:
    """customer.csv 를 읽어 sold_to 기준으로 unique 하게 반환."""
    df = pd.read_csv(csv_path, dtype={"sold_to": str, "ship_to": str}, encoding="latin1")
    # 컬럼명 공백 제거
    df.columns = df.columns.str.strip()
    # sold_to 기준 중복 제거 (첫 번째 ship_to 사용)
    df = df.drop_duplicates(subset=["sold_to"], keep="first")
    return df


def find_label_cell(ws, label: str):
    """워크시트에서 특정 텍스트가 포함된 셀 좌표를 반환."""
    for row in ws.iter_rows():
        for cell in row:
            if cell.value and label.lower() in str(cell.value).lower():
                return cell.row, cell.column
    return None, None


def detect_cells_from_template(ws):
    """
    템플릿에서 레이블 셀을 자동 탐지해 값을 넣을 셀 딕셔너리를 반환.
    탐지 실패 시 기본값(CELL_* 상수) 사용.
    """
    mapping = {}

    labels = {
        "store_name":  ["store name"],
        "sold_to":     ["sold-to", "sold to"],
        "ship_to":     ["ship-to", "ship to"],
    }

    for key, keywords in labels.items():
        for kw in keywords:
            r, c = find_label_cell(ws, kw)
            if r:
                # 값은 레이블 바로 오른쪽 열에 기입 (같은 행, +1 열)
                mapping[key] = (r, c + 1)
                break

    return mapping


def fill_form(template_path: str, customer_row: pd.Series, output_path: str):
    """
    템플릿을 복사 후 고객 정보를 채워 output_path 에 저장.
    """
    # 템플릿 복사
    shutil.copy2(template_path, output_path)

    wb = load_workbook(output_path)
    ws = wb.active  # 첫 번째 시트 사용

    # 레이블 자동 탐지
    mapping = detect_cells_from_template(ws)

    def write_cell(key, default_cell, value):
        if key in mapping:
            r, c = mapping[key]
            ws.cell(row=r, column=c, value=value)
        else:
            ws[default_cell] = value

    # ── 값 기입 ────────────────────────────────
    store_name = str(customer_row.get("sold_to_name", "")).strip()
    sold_to_id = str(customer_row.get("sold_to", "")).strip()
    ship_to_id = str(customer_row.get("ship_to", "")).strip()
    ship_to_name = str(customer_row.get("ship_to_name", "")).strip()
    address = str(customer_row.get("address", "")).strip()

    write_cell("store_name", CELL_STORE_NAME, store_name)
    write_cell("sold_to",    CELL_SOLD_TO_ID, sold_to_id)
    write_cell("ship_to",    CELL_SHIP_TO_ID, ship_to_id)

    # 추가 정보 (탐지 실패 시 하드코딩 셀에 기입)
    if "sold_to" in mapping:
        r, c = mapping["sold_to"]
        ws.cell(row=r + 1, column=c, value=store_name)  # sold_to 아래 줄에 회사명
    else:
        ws[CELL_SOLD_TO_NAME] = store_name

    if "ship_to" in mapping:
        r, c = mapping["ship_to"]
        ws.cell(row=r + 1, column=c, value=ship_to_name)
        ws.cell(row=r + 2, column=c, value=address)
    else:
        ws[CELL_SHIP_TO_NAME] = ship_to_name
        ws[CELL_ADDRESS] = address

    wb.save(output_path)
    print(f"  ✔ 저장 완료: {output_path}")


def create_sample_files(n: int = 3):
    """
    customer.csv 에서 n 개를 선택해 샘플 파일을 output 폴더에 생성.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = load_customer_master(CUSTOMER_CSV)

    if not os.path.exists(TEMPLATE_PATH):
        print(f"[ERROR] 템플릿 파일이 없습니다: {TEMPLATE_PATH}")
        sys.exit(1)

    samples = df.head(n)
    for _, row in samples.iterrows():
        sold_to = row["sold_to"]
        safe_name = str(row.get("sold_to_name", sold_to)).replace("/", "_").replace("\\", "_")[:40]
        filename = f"Claim_Form_{sold_to}_{safe_name}.xlsx"
        output_path = os.path.join(OUTPUT_DIR, filename)
        print(f"처리중: {sold_to} - {row.get('sold_to_name')}")
        fill_form(TEMPLATE_PATH, row, output_path)

    print(f"\n총 {len(samples)}개 파일 생성 완료 → {OUTPUT_DIR}")


def main():
    parser = argparse.ArgumentParser(
        description="Customer Master → Claim Report Form 자동 입력"
    )
    parser.add_argument(
        "--sold_to", nargs="+", metavar="SOLD_TO",
        help="입력할 sold_to 코드 (예: 731942 100142)"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="customer.csv 전체 고객 파일 생성"
    )
    parser.add_argument(
        "--sample", type=int, default=3, metavar="N",
        help="샘플 파일 생성 개수 (기본 3개)"
    )
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = load_customer_master(CUSTOMER_CSV)

    if not os.path.exists(TEMPLATE_PATH):
        print(f"[ERROR] 템플릿 파일이 없습니다: {TEMPLATE_PATH}")
        sys.exit(1)

    if args.all:
        targets = df
    elif args.sold_to:
        targets = df[df["sold_to"].isin([str(s) for s in args.sold_to])]
        if targets.empty:
            print(f"[ERROR] 해당 sold_to 를 customer.csv 에서 찾을 수 없습니다: {args.sold_to}")
            sys.exit(1)
    else:
        # 기본: 샘플 N개
        targets = df.head(args.sample)

    for _, row in targets.iterrows():
        sold_to = row["sold_to"]
        safe_name = str(row.get("sold_to_name", sold_to)).replace("/", "_").replace("\\", "_")[:40]
        filename = f"Claim_Form_{sold_to}_{safe_name}.xlsx"
        output_path = os.path.join(OUTPUT_DIR, filename)
        print(f"처리중: {sold_to} - {row.get('sold_to_name')}")
        fill_form(TEMPLATE_PATH, row, output_path)

    print(f"\n총 {len(targets)}개 파일 생성 완료 → {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
