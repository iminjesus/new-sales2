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
from PIL import Image
from pathlib import Path
import io
import sys

BASE_URL = "http://127.0.0.1:5000"

# (path,  human title,  extra wait ms for slow charts).
# The path lands under /demo/... so the demo cookie is set and
# every subsequent API response comes back anonymised.
PAGES = [
    ("/demo",             "Sales Dashboard / graph view",  2500),
    ("/demo/map",         "Map View",                       3500),
    ("/demo/stock",       "Stock",                          2000),
    ("/demo/rebate",      "Rebate Calculator",              2500),
    ("/demo/price",       "Price",                          1500),
    ("/demo/meeting",     "BDE Visit Log",                  2500),
    ("/demo/claims",      "Claims",                         1500),
    ("/demo/order",       "Special Price Request Form",     1500),
    ("/demo/orders_list", "Submitted Orders",               1500),
    ("/demo/highlights",  "Highlights",                     3000),
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


def main():
    imgs = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport=VIEWPORT,
                                  device_scale_factor=1.5)   # crisper text
        page = ctx.new_page()

        for path, title, wait_ms in PAGES:
            url = BASE_URL + path
            print(f"[capture] {title:38s}  {url}")
            try:
                page.goto(url, wait_until="load", timeout=20_000)
            except Exception as e:
                print(f"          skip — {e}")
                continue
            png = full_page_shot(page, wait_ms)
            if png is None:
                print(f"          skip — no screenshot captured")
                continue
            img = Image.open(io.BytesIO(png)).convert("RGB")
            # Stamp a small footer with the title so the PDF page
            # has a clear label at the top even after downscaling.
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
