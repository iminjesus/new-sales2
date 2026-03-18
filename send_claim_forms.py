"""
send_claim_forms.py
====================
customer_2603.csv 의 email 컬럼을 참조해
output 폴더의 Claim Form Excel 파일을 Outlook 으로 단체 발송합니다.

준비사항:
  1. customer_2603.csv 에 'email' 컬럼 추가 (ship_to 별 수신자 이메일)
  2. auto_populate_claim_form.py 로 Excel 파일 먼저 생성
  3. 이 스크립트 실행 (Windows, Outlook 설치 필요)

사용법:
  python send_claim_forms.py                  # 전체 발송 (드래프트 저장)
  python send_claim_forms.py --send           # 실제 발송
  python send_claim_forms.py --ship_to 724363 731942   # 특정 고객만
  python send_claim_forms.py --ship_to 724363 --send   # 특정 고객 실제 발송
"""

import os
import sys
import argparse
import glob
import pandas as pd

# ─────────────────────────────────────────────
# 경로 설정 (auto_populate_claim_form.py 와 동일)
# ─────────────────────────────────────────────
BASE_DIR    = r"E:\01. work\2025\Data_Anal_Website"
RAWDATA_DIR = os.path.join(BASE_DIR, "rawdata")
UNLOCK_DIR  = os.path.join(RAWDATA_DIR, "unlock")
OUTPUT_DIR  = os.path.join(RAWDATA_DIR, "output")

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if not os.path.exists(BASE_DIR):
    BASE_DIR    = _SCRIPT_DIR
    RAWDATA_DIR = os.path.join(BASE_DIR, "rawdata")
    UNLOCK_DIR  = os.path.join(RAWDATA_DIR, "unlock")
    OUTPUT_DIR  = os.path.join(RAWDATA_DIR, "output")

CUSTOMER_CSV = os.path.join(UNLOCK_DIR, "customer_2603.csv")
if not os.path.exists(CUSTOMER_CSV):
    CUSTOMER_CSV = os.path.join(_SCRIPT_DIR, "customer_2603.csv")

# ─────────────────────────────────────────────
# 이메일 설정  ← 필요에 따라 수정하세요
# ─────────────────────────────────────────────
EMAIL_SUBJECT = "Claim Report Form"
EMAIL_BODY    = """\
Dear {store_name},

Please find attached the Claim Report Form for your review.

Best regards,
"""

# ─────────────────────────────────────────────
# CSV 로드
# ─────────────────────────────────────────────
def load_customers() -> pd.DataFrame:
    for enc in ["utf-8", "latin1", "cp1252", "utf-16"]:
        try:
            df = pd.read_csv(CUSTOMER_CSV, encoding=enc)
            break
        except (UnicodeDecodeError, Exception):
            continue
    else:
        raise ValueError(f"파일을 읽을 수 없습니다: {CUSTOMER_CSV}")

    df.columns = df.columns.str.strip()

    # 컬럼 정규화
    col_map = {col.lower().replace(" ", "_").replace("-", "_"): col for col in df.columns}
    rename = {}
    for target in ["sold_to", "ship_to", "ship_to_name", "email"]:
        if target not in df.columns and target in col_map:
            rename[col_map[target]] = target
    if rename:
        df = df.rename(columns=rename)

    for col in ["sold_to", "ship_to"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df.drop_duplicates(subset=["ship_to"], keep="first")


# ─────────────────────────────────────────────
# output 폴더에서 해당 ship_to 의 Excel 파일 탐색
# ─────────────────────────────────────────────
def find_excel(ship_to: str) -> str | None:
    pattern = os.path.join(OUTPUT_DIR, f"Claim_Form_{ship_to}_*.xlsx")
    matches = glob.glob(pattern)
    return matches[0] if matches else None


# ─────────────────────────────────────────────
# Outlook 메일 생성 (draft 저장 or 실제 발송)
# ─────────────────────────────────────────────
def send_via_outlook(to_email: str, store_name: str, attachment_path: str, do_send: bool):
    try:
        import win32com.client
    except ImportError:
        print("[ERROR] pywin32 가 설치되어 있지 않습니다.")
        print("        pip install pywin32  을 실행한 후 다시 시도하세요.")
        sys.exit(1)

    outlook = win32com.client.Dispatch("Outlook.Application")
    mail = outlook.CreateItem(0)  # 0 = olMailItem

    mail.To      = to_email
    mail.Subject = EMAIL_SUBJECT
    mail.Body    = EMAIL_BODY.format(store_name=store_name)
    mail.Attachments.Add(os.path.abspath(attachment_path))

    if do_send:
        mail.Send()
        print(f"  [발송] {store_name} → {to_email}")
    else:
        mail.Save()  # 임시보관함(Drafts) 저장
        print(f"  [임시보관] {store_name} → {to_email}")


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Claim Form Outlook 단체 발송")
    parser.add_argument("--ship_to", nargs="+", help="특정 ship_to 코드만 발송")
    parser.add_argument("--send",    action="store_true",
                        help="실제 발송 (없으면 임시보관함에 저장만)")
    args = parser.parse_args()

    df = load_customers()

    # email 컬럼 확인
    if "email" not in df.columns:
        print("[ERROR] customer_2603.csv 에 'email' 컬럼이 없습니다.")
        print("        CSV 에 email 컬럼을 추가한 후 다시 실행하세요.")
        sys.exit(1)

    # 대상 필터링
    if args.ship_to:
        df = df[df["ship_to"].isin([str(s) for s in args.ship_to])]
        if df.empty:
            print(f"[ERROR] 해당 ship_to 를 찾을 수 없습니다: {args.ship_to}")
            sys.exit(1)

    mode = "실제 발송" if args.send else "임시보관함 저장 (테스트 모드)"
    print(f"모드: {mode}")
    print(f"대상: {len(df)}명\n")

    ok, skip = 0, 0
    for _, row in df.iterrows():
        ship_to    = str(row.get("ship_to", "")).strip()
        store_name = str(row.get("ship_to_name", ship_to)).strip()
        email      = str(row.get("email", "")).strip()

        if not email or email.lower() == "nan":
            print(f"  [건너뜀] {store_name} — 이메일 없음")
            skip += 1
            continue

        excel_path = find_excel(ship_to)
        if not excel_path:
            print(f"  [건너뜀] {store_name} — Excel 파일 없음 (ship_to={ship_to})")
            skip += 1
            continue

        send_via_outlook(email, store_name, excel_path, do_send=args.send)
        ok += 1

    print(f"\n완료: 성공 {ok}건 / 건너뜀 {skip}건")
    if not args.send:
        print("※ 실제 발송하려면 --send 옵션을 추가하세요.")


if __name__ == "__main__":
    main()
