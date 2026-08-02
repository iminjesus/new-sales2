"""
capture_demo_pdf.py
-------------------
Automate Chromium via Playwright to visit every /demo page on your
local Flask app, wait for charts / tables to render, screenshot each,
and stitch them into a single multi-page PDF.

Prerequisites (one-time):
    pip install playwright pillow
    playwright install chromium

Then, with the Flask app running on http://127.0.0.1:5000:
    python capture_demo_pdf.py
Output: HKAU_Dashboard_Live_Demo.pdf next to this script.

Configure BASE_URL / PAGES / WAIT_MS below if you want to include
extra pages or point at a different host.
"""
from playwright.sync_api import sync_playwright
from PIL import Image, ImageDraw, ImageFont
from pathlib import Path
import io
import sys

BASE_URL = "http://127.0.0.1:5000"

# (path, title, description, extra wait ms).  Description prints on
# the strip above the screenshot in the PDF so each page has a
# self-contained caption.  Highlights + Map get long waits because
# their charts are the slowest to render.
PAGES = [
    ("/demo",             "Sales Dashboard / graph view",
     "Region × Salesmen roll-up with This-Month / Q1-Q4 achievement %, "
     "monthly qty/$ KPIs, and drill-down into daily sales, promo membership, "
     "product-line mix and customer-level detail.",
     3500),
    ("/demo/map",         "Map View",
     "Geo-plotted shops coloured by state / performance.  Overlays for "
     "aged stock, fleet-demand postcodes, and salesmen visit density.",
     6000),
    ("/demo/stock",       "Stock",
     "SKU-level on-hand at every plant with DOT-age buckets (≤12M, 13-18M, "
     "19-24M, 25-36M, 37M+).  Drives aging-DC promotions and the aged-stock "
     "side panel on the Order form.",
     3000),
    ("/demo/rebate",      "Rebate Calculator",
     "Per ship-to rebate accrual against the codified rebate_structure "
     "tiers.  Next-tier gap in units, projected rebate at current pace, "
     "eligibility flag from customer master.",
     3500),
    ("/demo/price",       "Price",
     "Competitor price scraping (retail + wholesale portals) merged with "
     "our own list price so each SKU's market position is one glance away.",
     2500),
    ("/demo/meeting",     "Salesmen Visit Log",
     "Drag-and-drop scheduling calendar plus post-visit memo with pre/post "
     "sales-impact delta per visit.  Postcode / Region / City cards group "
     "multiple ship-tos into one drag-as-one bundle.",
     3500),
    ("/demo/claims",      "Claims",
     "Warranty claim intake + follow-up thread; per-shop history + status.",
     2500),
    ("/demo/order",       "Special Price Request Form",
     "Line-by-line pricing with server-authoritative Base DC (5-tier "
     "priority), 443 promo (multi-condition AND gate), aging DC override, "
     "compound formula, and management-approval workflow.",
     2500),
    ("/demo/orders_list", "Submitted Orders",
     "Audit-locked list of every submitted SPRF with SAP-entry flag "
     "(CS-only), parallel approver status, edit re-triggers approvals.",
     2500),
    ("/demo/highlights",  "Highlights",
     "Auto-generated strategic commentary: trajectory slope, mix-shift, "
     "decline analysis, watch list of actionable to-dos, plus the "
     "narrative card that summarises the month in three sentences.",
     12000),
]

VIEWPORT = {"width": 1600, "height": 1000}     # widescreen so nothing wraps
OUT_PDF  = Path(__file__).resolve().parent / "HKAU_Dashboard_Live_Demo.pdf"


def full_page_shot(page, extra_wait_ms=1500):
    """Wait for JS-driven content to settle, then capture the whole
    scrollable page (not just the visible viewport).

    Long calendar / list pages can overrun the default 30s screenshot
    budget on slower machines — we bump timeout to 90s, and if it
    STILL times out we fall back to a viewport-only capture so the
    PDF still gets one page for that route (better than aborting the
    entire run)."""
    # networkidle waits until there are no >2 network calls for 500ms
    # — good proxy for "charts have loaded their data".
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass
    page.wait_for_timeout(extra_wait_ms)
    # Scroll to the bottom once so lazy-loaded rows render before we
    # snapshot; then scroll back to top so the shot starts cleanly.
    try:
        page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(400)
        page.evaluate("() => window.scrollTo(0, 0)")
        page.wait_for_timeout(300)
    except Exception:
        pass
    try:
        return page.screenshot(full_page=True, type="png",
                               timeout=90_000, animations="disabled")
    except Exception as e:
        print(f"          full-page shot timed out ({e}); "
              f"falling back to viewport-only")
        try:
            return page.screenshot(full_page=False, type="png",
                                   timeout=30_000, animations="disabled")
        except Exception as e2:
            print(f"          viewport shot also failed: {e2}")
            return None


def _load_font(size, bold=False):
    """Try a few common Windows / Linux fonts; fall back to PIL's
    built-in default if none of them exist on the host."""
    candidates = [
        ("arialbd.ttf" if bold else "arial.ttf"),
        ("C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf"),
        ("C:/Windows/Fonts/segoeuib.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold
            else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold
            else "/System/Library/Fonts/Supplemental/Arial.ttf",
    ]
    for c in candidates:
        try: return ImageFont.truetype(c, size)
        except Exception: pass
    return ImageFont.load_default()


def _wrap(draw, text, font, max_width):
    """Word-wrap `text` to fit inside max_width when rendered with `font`."""
    words = text.split()
    lines, cur = [], ""
    for w in words:
        trial = (cur + " " + w).strip()
        bbox = draw.textbbox((0, 0), trial, font=font)
        if bbox[2] - bbox[0] <= max_width:
            cur = trial
        else:
            if cur: lines.append(cur)
            cur = w
    if cur: lines.append(cur)
    return lines


def add_caption(img, title, description):
    """Prepend a slate header strip with the page title (large, white)
    and description (smaller, light-grey) above the screenshot."""
    W = img.width
    title_font = _load_font(30, bold=True)
    desc_font  = _load_font(16, bold=False)
    demo_font  = _load_font(12, bold=True)
    dummy = Image.new("RGB", (W, 10))
    d = ImageDraw.Draw(dummy)
    desc_lines = _wrap(d, description, desc_font, W - 80)
    # Header height = title (~44) + each desc line (~24) + padding.
    header_h = 32 + 44 + max(24 * len(desc_lines), 24) + 20
    header = Image.new("RGB", (W, header_h), (30, 41, 59))     # slate-800
    d = ImageDraw.Draw(header)
    # DEMO badge in the top-right corner.
    d.rectangle([(W - 130, 12), (W - 20, 34)], fill=(249, 115, 22))
    d.text((W - 120, 15), "DEMO MODE", font=demo_font, fill=(255, 255, 255))
    # Title + description.
    d.text((40, 30), title, font=title_font, fill=(255, 255, 255))
    y = 30 + 44 + 8
    for ln in desc_lines:
        d.text((40, y), ln, font=desc_font, fill=(203, 213, 225))    # slate-300
        y += 24
    combined = Image.new("RGB", (W, header_h + img.height), (255, 255, 255))
    combined.paste(header, (0, 0))
    combined.paste(img, (0, header_h))
    return combined


def main():
    imgs = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport=VIEWPORT,
                                  device_scale_factor=1.5)   # crisper text
        page = ctx.new_page()

        for path, title, description, wait_ms in PAGES:
            url = BASE_URL + path
            print(f"[capture] {title:34s}  wait {wait_ms/1000:.1f}s  {url}")
            try:
                page.goto(url, wait_until="load", timeout=30_000)
            except Exception as e:
                print(f"          skip — {e}")
                continue
            png = full_page_shot(page, wait_ms)
            if png is None:
                print(f"          skip — no screenshot captured")
                continue
            img = Image.open(io.BytesIO(png)).convert("RGB")
            img = add_caption(img, title, description)
            imgs.append((title, img))

        browser.close()

    if not imgs:
        print("nothing captured — is the Flask app running on",
              BASE_URL, "?", file=sys.stderr)
        sys.exit(1)

    # PIL saves multi-page PDFs when you pass save_all=True + append_images.
    first_title, first = imgs[0]
    rest             = [img for (_t, img) in imgs[1:]]
    first.save(OUT_PDF, "PDF", save_all=True, append_images=rest,
               resolution=100.0)
    total_size = OUT_PDF.stat().st_size
    print(f"\n✓ wrote {OUT_PDF}")
    print(f"  {len(imgs)} pages · {total_size/1024:.0f} KB")


if __name__ == "__main__":
    main()
