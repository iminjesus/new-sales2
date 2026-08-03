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

# (path, title, description, wait_selector, min_wait_ms).
#   wait_selector — CSS selector to wait for before shooting; if the
#                   page doesn't show it in `min_wait_ms`, the script
#                   waits `min_wait_ms` anyway and captures whatever
#                   rendered so a slow page doesn't kill the run.
#   min_wait_ms   — floor wait after the selector fires (or full wait
#                   if the selector never appears).  Charts / maps
#                   need generous windows because Chart.js finishes
#                   the animation frame AFTER the DOM node exists.
PAGES = [
    ("/demo", "Sales Dashboard / graph view",
     "The main landing page — Region × Salesmen achievement grid plus every "
     "sales/target chart under it.  The screenshot below shows the filter "
     "row (Product / Region / Salesmen / Channel), the achievement roll-up "
     "table, the monthly Profit chart, daily sales bars, and the region-"
     "stacked daily volume view — all drivable from the top-of-page filters.",
     "table",        10000),

    ("/demo/map", "Map View",
     "Every customer plotted on an Australia map with pins coloured by "
     "salesman territory.  Overlays for aged stock, fleet-demand postcodes, "
     "and salesmen visit density can be toggled from the legend.",
     "canvas, .leaflet-container, #map", 10000),

    ("/demo/stock", "Stock",
     "SKU-level on-hand at every plant (NSW/QLD/VIC/WA) with DOT-age buckets "
     "(≤12M / 13-18M / 19-24M / 25-36M / 37M+).  The aged tiers here drive "
     "the Aging-DC ladder on the Order form and the aged-stock side panel.",
     "table tbody tr", 8000),

    ("/demo/rebate", "Rebate Calculator",
     "Per ship-to rebate accrual against the codified rebate_structure "
     "tiers.  Every row shows YTD qty / $, current tier %, next-tier "
     "% + units-needed-to-reach, and current rebate accrued.",
     "table tbody tr", 8000),

    ("/demo/price", "Tyre Price Comparison",
     "Competitor price scraping (Tempe / Bob Jane / JAX — retail + "
     "wholesale portals) merged with our own list price by size and "
     "month.  Left table = ours vs each competitor per month; right "
     "chart lets you flip between By Store and By Brand views.",
     "table, canvas, svg", 10000),

    ("/demo/fleet", "Fleet-demand Chart",
     "Postcode-level tyre demand estimated from vehicle registrations × "
     "base-OE fitment × ownership churn.  Bar chart is aggregate demand "
     "by rim / size; drives which SKUs to stock in each state's plant.",
     "canvas, svg, .chart", 10000),

    ("/demo/meeting", "Salesmen Visit Log",
     "Drag-and-drop calendar (planned visits per day / per salesman) with "
     "post-visit memos on the right.  Impact chips per entry show pre/post "
     "sales delta; Postcode / Region / City cards on the left let you drag "
     "a whole region's shops onto a date in one shot.",
     ".cal-cell, #calGrid, #recentSection", 8000),

    ("/demo/claims", "Claims",
     "Warranty claim intake + threaded follow-up.  Per-shop history, current "
     "status, and photo evidence all in one thread.",
     "table, .claim-card, .card", 6000),

    ("/demo/order", "Special Price Request Form",
     "Line-by-line pricing with server-authoritative Base DC (5-tier priority "
     "chain), 443 promo (multi-condition AND gate), Aging-DC override, "
     "compound pricing formula, and parallel management-approval workflow.  "
     "Screenshot shows the empty template before a customer is loaded.",
     ".hdr, #linesTable", 6000),

    ("/demo/orders_list", "Submitted Orders",
     "Audit-locked list of every submitted SPRF with the SAP-entry flag "
     "(CS-only toggle), parallel approver status, and Edit link (author-only) "
     "that re-triggers approvals on any content change.",
     "table, .list-table, #listBody", 6000),

    ("/demo/highlights", "Highlights",
     "Auto-generated strategic commentary: trajectory slope (linear "
     "regression on 6 months of sales), month-over-month mix shift, "
     "decline analysis per customer group, and a watch list of actionable "
     "to-dos.  The narrative card summarises the month in 3 sentences.",
     ".narrative, .decl-card, .tr-card, .wl-card, .mx-card, canvas",
     18000),
]

VIEWPORT = {"width": 1600, "height": 1000}     # widescreen so nothing wraps
OUT_PDF  = Path(__file__).resolve().parent / "HKAU_Dashboard_Live_Demo.pdf"


def full_page_shot(page, wait_selector=None, min_wait_ms=3000):
    """Wait for JS-driven content to actually appear, then capture the
    whole scrollable page (not just the visible viewport).

    Strategy:
      1. Wait for network to go quiet (charts probably fetched data).
      2. Try to wait for `wait_selector` to appear — proves the
         page's actual content is in the DOM.  If nothing shows up
         in min_wait_ms we still sleep min_wait_ms so slow renderers
         get their chance.
      3. Scroll through the page in chunks so lazy-loaded rows /
         charts below the fold get drawn before we snapshot.
      4. Screenshot with 120s budget; fall back to viewport-only if
         even that overruns."""
    # networkidle waits until there are no >2 network calls for 500ms
    # — good proxy for "charts have loaded their data".
    try:
        page.wait_for_load_state("networkidle", timeout=20_000)
    except Exception:
        pass
    saw_selector = False
    if wait_selector:
        try:
            page.wait_for_selector(wait_selector,
                                   timeout=min_wait_ms, state="visible")
            saw_selector = True
        except Exception:
            saw_selector = False
    # Floor wait so animation frames complete even after the DOM
    # node exists — Chart.js draws AFTER the canvas is in the tree.
    page.wait_for_timeout(min_wait_ms if not saw_selector else 2500)
    # Scroll through the page in chunks so lazy-loaded rows / charts
    # below the fold get drawn before we snapshot.
    try:
        for y in (0, 500, 1000, 1600, 2400, 3200, 999999):
            page.evaluate(f"() => window.scrollTo(0, {y})")
            page.wait_for_timeout(250)
        page.evaluate("() => window.scrollTo(0, 0)")
        page.wait_for_timeout(500)
    except Exception:
        pass
    try:
        return page.screenshot(full_page=True, type="png",
                               timeout=120_000, animations="disabled")
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


def add_caption(img, page_num, total_pages, title, description):
    """Prepend a big yellow-band caption above each screenshot with:
      - PAGE N of M badge (top-left corner, slate)
      - DEMO MODE badge (top-right corner, orange)
      - Huge title in the middle (Hankook yellow band)
      - Description body underneath (slate body, wraps)
      - Thin "screenshot below is proof of the above" hint bar

    Deliberately loud so the reader immediately knows what the
    following screenshot is meant to demonstrate.  Screenshot below
    should visually confirm what the description says."""
    W = img.width
    title_font = _load_font(56, bold=True)
    desc_font  = _load_font(22, bold=False)
    small_font = _load_font(14, bold=True)
    tiny_font  = _load_font(13, bold=False)

    # Measure the description so we can size the block.
    dummy = Image.new("RGB", (W, 10))
    d = ImageDraw.Draw(dummy)
    body_w = W - 120
    desc_lines = _wrap(d, description, desc_font, body_w)

    # Layout: 3 stacked bands
    # 1) yellow title band (fixed height)
    # 2) slate description band (grows with text)
    # 3) thin arrow-down hint bar
    title_band_h = 130
    desc_band_h  = 34 + 30 * len(desc_lines) + 28
    hint_band_h  = 44
    header_h = title_band_h + desc_band_h + hint_band_h

    header = Image.new("RGB", (W, header_h), (255, 255, 255))
    d = ImageDraw.Draw(header)

    # ─── 1) Title band — Hankook signature yellow ───
    d.rectangle([(0, 0), (W, title_band_h)], fill=(245, 197, 24))
    # Page N of M badge (top-left)
    d.rectangle([(24, 20), (150, 52)], fill=(15, 23, 42))
    d.text((36, 26), f"PAGE {page_num} of {total_pages}",
           font=small_font, fill=(255, 255, 255))
    # DEMO MODE badge (top-right)
    d.rectangle([(W - 174, 20), (W - 24, 52)], fill=(249, 115, 22))
    d.text((W - 162, 26), "DEMO MODE",
           font=small_font, fill=(255, 255, 255))
    # Big title, centered vertically
    d.text((36, 62), title, font=title_font, fill=(15, 23, 42))

    # ─── 2) Description band — slate ───
    y0 = title_band_h
    d.rectangle([(0, y0), (W, y0 + desc_band_h)], fill=(30, 41, 59))
    y = y0 + 18
    for ln in desc_lines:
        d.text((60, y), ln, font=desc_font, fill=(226, 232, 240))
        y += 30

    # ─── 3) Hint bar — "screenshot below is proof" ───
    y0 += desc_band_h
    d.rectangle([(0, y0), (W, y0 + hint_band_h)], fill=(254, 243, 199))  # amber-50
    d.text((36, y0 + 12),
           "↓  SCREENSHOT BELOW  ·  what the description above looks like in the live app",
           font=tiny_font, fill=(146, 64, 14))       # amber-800

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

        total = len(PAGES)
        for idx, (path, title, description, sel, wait_ms) in enumerate(PAGES, 1):
            url = BASE_URL + path
            print(f"[capture] {idx}/{total}  {title:34s}  "
                  f"wait {wait_ms/1000:.1f}s  {url}")
            try:
                resp = page.goto(url, wait_until="load", timeout=30_000)
            except Exception as e:
                print(f"          skip — {e}")
                continue
            # Skip pages that 404 / 5xx — no point taking a screenshot
            # of the error page.  A dead nav link (e.g. /price with
            # no route) surfaces here instead of silently producing
            # a blank slide in the PDF.
            if resp is not None and resp.status >= 400:
                print(f"          skip — server returned {resp.status}")
                continue
            png = full_page_shot(page, wait_selector=sel, min_wait_ms=wait_ms)
            if png is None:
                print(f"          skip — no screenshot captured")
                continue
            img = Image.open(io.BytesIO(png)).convert("RGB")
            img = add_caption(img, idx, total, title, description)
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
