"""
create_sample_claim_forms.py
============================
템플릿 파일 없이 Claim Report Form 양식을 openpyxl로 직접 생성합니다.
customer.csv에서 2~3개 고객을 선택해 샘플 파일을 만듭니다.

실행: python create_sample_claim_forms.py
"""

import os
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import (
    Font, Alignment, Border, Side, PatternFill, colors
)
from openpyxl.utils import get_column_letter
from openpyxl.drawing.image import Image as XLImage

# ─────────────────────────────────────────────
# 경로 설정
# ─────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
CUSTOMER_CSV = os.path.join(SCRIPT_DIR, "rawdata", "unlock", "customer.csv")
OUTPUT_DIR   = os.path.join(SCRIPT_DIR, "rawdata", "output")

# ─────────────────────────────────────────────
# 스타일 헬퍼
# ─────────────────────────────────────────────
def thin_border(top=False, bottom=False, left=False, right=False):
    thin = Side(style="thin")
    return Border(
        top=thin    if top    else Side(style=None),
        bottom=thin if bottom else Side(style=None),
        left=thin   if left   else Side(style=None),
        right=thin  if right  else Side(style=None),
    )

def box_border():
    thin = Side(style="thin")
    return Border(top=thin, bottom=thin, left=thin, right=thin)

def dashed_border():
    dash = Side(style="dashed")
    return Border(top=dash, bottom=dash, left=dash, right=dash)

def label_font(bold=False, size=9):
    return Font(name="Arial", size=size, bold=bold)

def title_font():
    return Font(name="Arial", size=16, bold=True)


def set_col_widths(ws):
    widths = {
        "A": 4, "B": 2, "C": 12, "D": 2, "E": 14, "F": 2,
        "G": 12, "H": 2, "I": 14, "J": 2, "K": 3,
        "L": 12, "M": 2, "N": 14, "O": 2, "P": 3,
    }
    for col, w in widths.items():
        ws.column_dimensions[col].width = w
    for r in range(1, 40):
        ws.row_dimensions[r].height = 15


def build_claim_form(wb: Workbook, customer: dict) -> None:
    ws = wb.active
    ws.title = "Claim Report"
    set_col_widths(ws)

    # ── 제목 ─────────────────────────────────
    ws.merge_cells("A1:P1")
    ws["A1"] = "Claim report form"
    ws["A1"].font = title_font()
    ws["A1"].alignment = Alignment(vertical="center")
    ws.row_dimensions[1].height = 28

    # ── 왼쪽 박스 (Store Name / Sold-to / Ship-to) ──
    left_box_rows = list(range(3, 16))  # rows 3~15
    # 테두리 그리기 (A3:J15)
    for r in range(3, 16):
        for c in range(1, 11):   # A~J
            cell = ws.cell(row=r, column=c)
            top    = (r == 3)
            bottom = (r == 15)
            left   = (c == 1)
            right  = (c == 10)
            cell.border = thin_border(top=top, bottom=bottom, left=left, right=right)

    # Store Name 레이블 + 값
    ws["A3"] = "Store Name :"
    ws["A3"].font = label_font(bold=False, size=9)
    ws["A3"].alignment = Alignment(vertical="top")
    ws.merge_cells("A3:J4")
    ws["A3"] = f"Store Name :   {customer['sold_to_name']}"
    ws["A3"].font = label_font(size=9)

    # 구분선 (점선)
    for c in range(1, 11):
        ws.cell(row=5, column=c).border = thin_border(bottom=True)

    # Sold-to 레이블 + 값
    ws.merge_cells("A6:J7")
    ws["A6"] = f"Sold-to :   {customer['sold_to']}  {customer['sold_to_name']}"
    ws["A6"].font = label_font(size=9)

    # 구분선
    for c in range(1, 11):
        ws.cell(row=8, column=c).border = thin_border(bottom=True)

    # Ship-to 레이블 + 값
    ws.merge_cells("A9:J10")
    ws["A9"] = (
        f"Ship-to :   {customer['ship_to']}  {customer['ship_to_name']}\n"
        f"            {customer['address']}"
    )
    ws["A9"].font = label_font(size=9)
    ws["A9"].alignment = Alignment(wrap_text=True, vertical="top")

    # ── 오른쪽 박스 (Manufacturer) ──────────
    ws.merge_cells("K2:P2")
    ws["K2"] = "Manufacturer"
    ws["K2"].font = label_font(size=8)

    # Hankook 로고 텍스트 (실제 로고 이미지 없을 경우)
    ws.merge_cells("K3:P6")
    ws["K3"] = "HANKOOK"
    ws["K3"].font = Font(name="Arial", size=20, bold=True, color="E85D04")
    ws["K3"].alignment = Alignment(horizontal="center", vertical="center")

    for r in range(2, 16):
        for c in range(11, 17):
            cell = ws.cell(row=r, column=c)
            top    = (r == 2)
            bottom = (r == 15)
            left   = (c == 11)
            right  = (c == 16)
            cell.border = thin_border(top=top, bottom=bottom, left=left, right=right)

    ws.merge_cells("K7:P8")
    ws["K7"] = "Manufacturer Handling\nClaim no."
    ws["K7"].font = label_font(size=8)
    ws["K7"].alignment = Alignment(wrap_text=True, vertical="top")

    ws.merge_cells("K9:P10")
    ws["K9"] = "Inspector Name :\nMobile Phone no. :"
    ws["K9"].font = label_font(size=8)
    ws["K9"].alignment = Alignment(wrap_text=True, vertical="top")

    ws.merge_cells("K11:P12")
    ws["K11"] = "Tyre Owner Name :\nMobile Phone No."
    ws["K11"].font = label_font(size=8)
    ws["K11"].alignment = Alignment(wrap_text=True, vertical="top")

    # ── Tyre Purchase Date ──────────────────
    ws["A17"] = "Tyre Purchase Date:"
    ws["A17"].font = label_font(bold=True, size=9)
    ws.merge_cells("D17:J17")
    ws["D17"].border = box_border()

    # ── Return Tires Information ────────────
    ws["A19"] = "Return Tires Information"
    ws["A19"].font = label_font(bold=True, size=9)

    headers = [
        ("Tire Size\nDescription", "A21:C22"),
        ("Pattern Name\n(or Product Name)", "D21:F22"),
        ("DOT Number", "G21:H22"),
        ("Serial Number\n(Truck & BUS Only)", "I21:K22"),
        ("Remain Skid\nDepth (mm)", "L21:M22"),
    ]
    header_fill = PatternFill("solid", fgColor="D9D9D9")
    for text, merge_range in headers:
        ws.merge_cells(merge_range)
        start_cell = merge_range.split(":")[0]
        cell = ws[start_cell]
        cell.value = text
        cell.font = label_font(bold=False, size=8)
        cell.fill = header_fill
        cell.alignment = Alignment(wrap_text=True, horizontal="center", vertical="center")
        cell.border = box_border()

    row_labels = ["ex", "1", "2", "3"]
    for i, lbl in enumerate(row_labels):
        base_row = 23 + i * 2
        ws.cell(row=base_row, column=1).value = lbl
        ws.cell(row=base_row, column=1).font = label_font(size=8)
        for col_range in ["A:C", "D:F", "G:H", "I:K", "L:M"]:
            s, e = col_range.split(":")
            sc = ord(s) - 64
            ec = ord(e) - 64
            ws.merge_cells(
                start_row=base_row, start_column=sc,
                end_row=base_row+1,  end_column=ec
            )
            ws.cell(row=base_row, column=sc).border = box_border()

    # ── Description ─────────────────────────
    desc_row = 23 + len(row_labels) * 2 + 1
    ws.cell(row=desc_row, column=1).value = "Description (Claim reason)"
    ws.cell(row=desc_row, column=1).font = label_font(bold=False, size=8)
    ws.merge_cells(
        start_row=desc_row, start_column=1,
        end_row=desc_row+3,  end_column=13
    )
    ws.cell(row=desc_row, column=1).border = box_border()
    ws.cell(row=desc_row, column=1).alignment = Alignment(vertical="top")


def create_sample_files(n: int = 3):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.read_csv(CUSTOMER_CSV, dtype={"sold_to": str, "ship_to": str}, encoding="latin1")
    df.columns = df.columns.str.strip()
    df = df.drop_duplicates(subset=["sold_to"], keep="first")

    samples = df.head(n)
    created = []

    for _, row in samples.iterrows():
        customer = {
            "sold_to":       str(row.get("sold_to", "")).strip(),
            "sold_to_name":  str(row.get("sold_to_name", "")).strip(),
            "ship_to":       str(row.get("ship_to", "")).strip(),
            "ship_to_name":  str(row.get("ship_to_name", "")).strip(),
            "address":       str(row.get("address", "")).strip(),
        }

        wb = Workbook()
        build_claim_form(wb, customer)

        safe_name = customer["sold_to_name"].replace("/", "_").replace("\\", "_")[:40]
        filename = f"Claim_Form_{customer['sold_to']}_{safe_name}.xlsx"
        output_path = os.path.join(OUTPUT_DIR, filename)
        wb.save(output_path)
        created.append(output_path)
        print(f"  ✔ {filename}")

    print(f"\n총 {len(created)}개 파일 생성 완료 → {OUTPUT_DIR}")
    return created


if __name__ == "__main__":
    print("샘플 Claim Report Form 파일 생성 중...")
    create_sample_files(n=3)
