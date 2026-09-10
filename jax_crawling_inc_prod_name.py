"""
jax_crawling_inc_prod_name.py
-----------------------------
Improved variant of jax_crawling.py that reliably captures the product
model name (e.g. "G Fit AS-01 LH42") even when the JAX product card
sandwiches promo badges ("BUY 4 & GET 20% OFF", "SEPTEMBER SPECIAL
DEALS") between the promo text and the brand name.  The original
crawler's name extractor picked the promo text as the description
because:
  - JUNK_RE didn't strip "BUY 4 & GET ... % OFF" or "OR BUY 4 FOR $X"
  - BRAND_NAMES_RE was anchored with ^, so a brand name in the middle
    of the pre-size text was left in
This variant fixes both, and adds an explicit "text between brand name
and size pattern" capture so the model name survives even when other
promo text pads the card.

Strategy: URL-based pagination, pagesize=45 (maximum per page).
Page URL template:
  https://www.jaxtyres.com.au/tyres/{brand}
    ?searchqrytype=Brand&isrunflat=False&searchtype=All
    &sorttype=PriceAsc&pagesize=45&pagenumber={N}

Output: jax_inc_prod_name_YYYYMMDD_HHMM.csv
Columns: SIZE | brand | DESCRIPTION | PRICE | DISC_PRICE | PROMO | RUN_FLAT

  PRICE      — regular per-tyre price
  DISC_PRICE — "OR BUY 4 FOR $X" per-tyre price (blank if no bulk deal)
  RUN_FLAT   — "Y" if the SKU came from JAX's isrunflat=True view,
               "N" if from isrunflat=False.  Each brand is crawled twice
               so both catalogues are captured (~95 runflat SKUs on
               Bridgestone were silently skipped by the original crawler).

Usage:
    python jax_crawling_inc_prod_name.py
"""
import re
import csv
import math
import time
from datetime import datetime
from selenium import webdriver

PAGE_URL = (
    "https://www.jaxtyres.com.au/tyres/{brand}"
    "?searchqrytype=Brand&isrunflat={runflat}&searchtype=All"
    "&sorttype=PriceAsc&pagesize=45&pagenumber={page}"
)
# We crawl each brand TWICE — once for non-runflat, once for runflat —
# so a brand's runflat catalogue (~95 SKUs on Bridgestone) is captured
# too.  Each row in the CSV carries a RUN_FLAT flag ("N" or "Y").
RUNFLAT_PASSES = [("False", "N"), ("True", "Y")]
OUTPUT_FILE = datetime.now().strftime("jax_inc_prod_name_%Y%m%d_%H%M.csv")
PAGE_SIZE   = 45

# Set HEADLESS = True to run without a visible Chrome window (safer if
# you want to keep using the PC while the crawler runs — no risk of
# accidentally clicking the browser).  Set False (default) if you want
# to watch the pages load or JAX starts blocking headless requests.
HEADLESS = False

# abbr → (display name, JAX URL slug)
# Verify slugs at: https://www.jaxtyres.com.au/tyres/<slug>
BRANDS = {
    "MC":  ("Michelin",     "michelin"),
    "BS":  ("Bridgestone",  "bridgestone"),
    "CT":  ("Continental",  "continental"),
    "GY":  ("Goodyear",     "goodyear"),
    "FK":  ("Falken",       "falken"),
    "HK":  ("Hankook",      "hankook"),
    "LF":  ("Laufenn",      "laufenn"),
    "DL":  ("Dunlop",       "dunlop"),
}


# ── Post-processing patterns ─────────────────────────────────────────────────
# Applied AFTER the JS extractor returns.  Splits any promo / attribute /
# vehicle-tag text out of the raw description and moves it to the PROMO
# column, then (if the description shrunk to a stub like "88V XL") rescues
# the marketing line by pattern-matching the full card text.  Same logic
# used by jax_crawling_hk_only.py — proven on Hankook, now every brand.
PROMO_PATTERNS = [
    # eGift Card family (with or without a "Buy N & Get $X" prefix)
    (r'BUY\s*\d+\s*[&+]?\s*GET\s+\$?\d+(?:\.\d+)?\s*(?:EGIFT|E[- ]?GIFT|Fuel\s+eGift)\s*CARD',   "eGift Card"),
    (r'\$\d+(?:\.\d+)?\s*(?:EGIFT|e-?Gift)\s*CARD',                                              "eGift Card"),
    # Standalone "BUY N & GET $X" (suffix got split off)
    (r'BUY\s*\d+\s*[&+]?\s*GET\s+\$\d+(?:\.\d+)?\b',                                             "Buy N Get $X"),
    # Bulk-deal banners
    (r'BUY\s*\d+\s*[&+]?\s*GET\s+\d+\s*%\s*OFF',        "Buy N Get N% Off"),
    (r'Buy\s*\d+\s*[&+]?\s*Get\s+\d+\s*%\s*Off',        "Buy N Get N% Off"),
    (r'BUY\s*\d+\s*[&+]?\s*GET\s+4TH\s+TYRE\s+FREE',    "Buy 4 Get 4th Free"),
    (r'Buy\s*\d+\s*[&+]?\s*Get\s+4th\s+Tyre\s+Free',    "Buy 4 Get 4th Free"),
    (r'OR\s+BUY\s+\d+\s+FOR\s+\$[\d.,]+',               "Or Buy N For $X"),
    # Cross-brand promo + monthly campaigns
    (r'Recommended\s+by\s+JAX',                         "Recommended by JAX"),
    (r'(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[A-Z]*\s+SPECIAL\s+(?:DEAL|OFFER)S?',   "Special Deal"),
    (r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+Special\s+(?:Deal|Offer)s?',   "Special Deal"),
    # Tyre-attribute badges (informational, not part of the model name)
    (r'Mud\s+and\s+Snow\s*\(?\s*M\+S\s*\)?',           "M+S"),
    (r'\bM\+S\b',                                      "M+S"),
    (r'3\s*Peak\s+Mountain\s+Snowflake\s*\(?\s*3PMSF\s*\)?', "3PMSF"),
    (r'Seal\s+Inside\s+Technology\s*\(?\s*s-i\s*\)?',  "Seal Inside"),
    (r'Noise\s+Cancell?ing\s+System\s*\(?\s*NCS\s*\)?',"NCS"),
    (r'ROAD\s+HAZARD(?:\s*\+\s*\d+\s*DAY\s+SATISFACTION)?', "Road Hazard"),
    # Vehicle-brand tags
    (r'\b(?:Toyota|Audi\s+Volkswagen|Volkswagen|Mercedes(?:[-\s]?Benz)?|Volvo|Hyundai|BMW|Honda|Nissan|Ford|Mazda|Subaru|Kia)\b',
     "Vehicle tag"),
    (r'\+\d+\s*(?=\s|$)',                              "+N badge"),
    (r'FREE\s+SHIPPING',                               "Free Shipping"),
    (r'\bFIT\s+AT\s+HOME\b',                           "Fit at Home"),
]

# Negative-lookahead guard reused inside every brand's line-name regex.
# The token after the line word must NOT look like a size (185/55R16),
# load rating (88V), lone model code (K425), price ($180), or promo verb
# — those signal we've walked past the marketing line into the spec /
# banner text.
_NEXT_TOKEN = (
    r'(?:\s+(?!'
    r'\d{2,3}[/A-Za-z]'                              # 185/55R16, 88V, 175R14
    r'|[A-Z]{1,3}\d{2,4}\b'                          # K425, RA43, IH01, T005
    r'|\$\d'                                         # $180 (price)  — bare "4"/"5" allowed (Primacy 4, Pilot Sport 5)
    r'|BUY|GET|OR|WAS|NOW|FREE|SPECIAL|RECOMMENDED'  # promo verbs
    r'|MUD|SNOW|SEAL|NOISE|ROAD|HAZARD'              # attribute tags
    r'|TOYOTA|AUDI|VOLKSWAGEN|MERCEDES|VOLVO|HYUNDAI'
    r'|BMW|HONDA|NISSAN|FORD|MAZDA|SUBARU|KIA'
    r')[A-Za-z0-9][\w\.\*\+\-]*)'  # first char can be digit too (Primacy 4, Pilot Sport 5, Turanza 6, Performance 2)
)

# Per-brand marketing-line patterns.  A miss returns "" so no rescue is
# forced when the JS extractor already got the name right.  Only used
# when the JS description shrank to a stub (empty / <15 chars / bare
# load rating).  Coverage: every model family JAX currently lists for
# the eight brands we crawl (Sep 2026).
BRAND_LINE_PATTERNS = {
    "Michelin": re.compile(
        r'\b(?:'
        r'Primacy'       + _NEXT_TOKEN + r'{0,3}'
        r'|Pilot'        + _NEXT_TOKEN + r'{0,4}'
        r'|CrossClimate' + _NEXT_TOKEN + r'{0,3}'
        r'|Latitude'     + _NEXT_TOKEN + r'{0,3}'
        r'|Energy'       + _NEXT_TOKEN + r'{0,3}'
        r'|Defender'     + _NEXT_TOKEN + r'{0,3}'
        r'|LTX'          + _NEXT_TOKEN + r'{0,3}'
        r'|Agilis'       + _NEXT_TOKEN + r'{0,3}'
        r'|X[- ]?Ice'    + _NEXT_TOKEN + r'{0,2}'
        r')\b', re.IGNORECASE),
    "Bridgestone": re.compile(
        r'\b(?:'
        r'Turanza'  + _NEXT_TOKEN + r'{0,3}'
        r'|Potenza' + _NEXT_TOKEN + r'{0,3}'
        r'|Ecopia'  + _NEXT_TOKEN + r'{0,3}'
        r'|Alenza'  + _NEXT_TOKEN + r'{0,3}'
        r'|Dueler'  + _NEXT_TOKEN + r'{0,3}'
        r'|Blizzak' + _NEXT_TOKEN + r'{0,3}'
        r'|Weather\s+Control' + _NEXT_TOKEN + r'{0,2}'
        r')\b', re.IGNORECASE),
    "Continental": re.compile(
        r'\b(?:'
        r'PremiumContact'   + _NEXT_TOKEN + r'{0,2}'
        r'|SportContact'    + _NEXT_TOKEN + r'{0,2}'
        r'|EcoContact'      + _NEXT_TOKEN + r'{0,2}'
        r'|UltraContact'    + _NEXT_TOKEN + r'{0,2}'
        r'|CrossContact'    + _NEXT_TOKEN + r'{0,3}'
        r'|VanContact'      + _NEXT_TOKEN + r'{0,3}'
        r'|WinterContact'   + _NEXT_TOKEN + r'{0,3}'
        r'|AllSeasonContact'+ _NEXT_TOKEN + r'{0,2}'
        r'|Conti(?:SportContact|PremiumContact|WinterContact|EcoContact|CrossContact|VikingContact)'
                            + _NEXT_TOKEN + r'{0,2}'
        r')\b', re.IGNORECASE),
    "Goodyear": re.compile(
        r'\b(?:'
        r'Efficient\s*Grip' + _NEXT_TOKEN + r'{0,3}'
        r'|EfficientGrip'   + _NEXT_TOKEN + r'{0,3}'
        r'|Eagle'           + _NEXT_TOKEN + r'{0,4}'
        r'|Wrangler'        + _NEXT_TOKEN + r'{0,3}'
        r'|Assurance'       + _NEXT_TOKEN + r'{0,3}'
        r'|UltraGrip'       + _NEXT_TOKEN + r'{0,3}'
        r'|Vector'          + _NEXT_TOKEN + r'{0,3}'
        r')\b', re.IGNORECASE),
    "Falken": re.compile(
        r'\b(?:'
        r'Sincera'     + _NEXT_TOKEN + r'{0,3}'
        r'|Azenis'     + _NEXT_TOKEN + r'{0,3}'
        r'|Ziex'       + _NEXT_TOKEN + r'{0,3}'
        r'|WildPeak'   + _NEXT_TOKEN + r'{0,3}'
        r'|EuroWinter' + _NEXT_TOKEN + r'{0,3}'
        r')\b', re.IGNORECASE),
    "Hankook": re.compile(
        r'\b(?:'
        r'Kinergy'  + _NEXT_TOKEN + r'{0,3}'
        r'|Dynapro' + _NEXT_TOKEN + r'{0,3}'
        r'|Ventus'  + _NEXT_TOKEN + r'{0,4}'
        r'|Winter\s+i[\*\.]?cept' + _NEXT_TOKEN + r'{0,2}'
        r'|iON'     + _NEXT_TOKEN + r'{1,4}'
        r'|Optimo'  + _NEXT_TOKEN + r'{0,3}'
        r'|Enfren'  + _NEXT_TOKEN + r'{0,2}'
        r'|Ecomate' + _NEXT_TOKEN + r'{0,2}'
        r'|Vantra'  + _NEXT_TOKEN + r'{0,2}'
        r'|Smart'   + _NEXT_TOKEN + r'{0,2}'
        r')\b', re.IGNORECASE),
    "Laufenn": re.compile(
        r'\b(?:'
        r'G\s*Fit'  + _NEXT_TOKEN + r'{0,3}'
        r'|X\s*Fit' + _NEXT_TOKEN + r'{0,3}'
        r'|S\s*Fit' + _NEXT_TOKEN + r'{0,3}'
        r'|I\s*Fit' + _NEXT_TOKEN + r'{0,3}'
        r')\b', re.IGNORECASE),
    "Dunlop": re.compile(
        r'\b(?:'
        r'Grandtrek'      + _NEXT_TOKEN + r'{0,3}'
        r'|SP\s+Sport'    + _NEXT_TOKEN + r'{0,3}'
        r'|Direzza'       + _NEXT_TOKEN + r'{0,3}'
        r'|Winter\s+Maxx' + _NEXT_TOKEN + r'{0,3}'
        r'|Enasave'       + _NEXT_TOKEN + r'{0,3}'
        r')\b', re.IGNORECASE),
}

# Marketing-line model code (K425, RA43, IH01, LA41, T005, XM2+, MK01…).
LINE_CODE_RE = re.compile(r'\b[A-Z]{1,3}\d{2,4}[A-Z\+]{0,2}\b')


def split_promo_from_desc(raw_desc):
    """Return (clean_desc, promo_extra_list) — pull known promo strings
    out of the raw description and hand them back as a list so the
    caller can join them into the PROMO column."""
    if not raw_desc:
        return "", []
    text = raw_desc
    found = []
    for pat, _label in PROMO_PATTERNS:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            found.append(m.group(0).strip())
        text = re.sub(pat, ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+', ' ', text).strip(" -–|,")
    seen = set(); dedup = []
    for f in found:
        k = re.sub(r'\s+', ' ', f).strip().upper()
        if k and k not in seen:
            seen.add(k); dedup.append(re.sub(r'\s+', ' ', f).strip())
    return text, dedup


def rescue_line_name(brand_name, raw_desc, full_card_text=""):
    """When the description shrank to a stub ("88V XL", "K425 88V XL"),
    scan the full card text for the brand's marketing line and return
    it (with the nearby model code appended if one exists).  Returns
    "" when no rescue is possible — the caller keeps the JS output."""
    pattern = BRAND_LINE_PATTERNS.get(brand_name)
    if pattern is None:
        return ""
    stub_ok = (not raw_desc) or len(raw_desc) < 15 or bool(re.match(
        r'^\s*(?:[A-Z]{1,3}\d{2,4}[A-Z\+]{0,2}\s+)?\d{2,3}[A-Z]{1,2}(?:\s+XL)?\s*$',
        raw_desc))
    if not stub_ok:
        return ""
    haystack = full_card_text or raw_desc
    line_m = pattern.search(haystack)
    if not line_m:
        return ""
    line_name = re.sub(r'\s+', ' ', line_m.group(0)).strip()
    code_m = LINE_CODE_RE.search(haystack)
    if code_m and code_m.group(0).upper() not in line_name.upper():
        return f"{line_name} {code_m.group(0)}".strip()
    return line_name


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1400,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    if HEADLESS:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=options)
    driver.set_script_timeout(60)
    return driver


# ── JS snippets ───────────────────────────────────────────────────────────────

_JS_TOTAL = r"""
var t = (document.body.innerText || document.body.textContent || '');
var m = t.match(/\d[\d,]*\s*[-\u2013]\s*\d[\d,]*\s+of\s+([\d,]+)\s+items/i);
if (m) return parseInt(m[1].replace(/,/g,''));
var m2 = t.match(/of\s+([\d,]+)\s+items/i);
if (m2) return parseInt(m2[1].replace(/,/g,''));
return -1;
"""

# Diagnostic: run when 0 products found to identify correct selectors.
_JS_DIAGNOSE = r"""
return (function() {
    var info = { url: location.href, title: document.title };

    /* Count "View Product Details" links (reliable card marker) */
    var detail_count = 0;
    var links = document.querySelectorAll('a');
    for (var i = 0; i < links.length; i++) {
        if (/view\s+product\s+details/i.test(links[i].textContent || '')) detail_count++;
    }
    info.detail_links = detail_count;

    /* Count price / size patterns in body text */
    var body = document.body.textContent || '';
    var pm = body.match(/\$\d{2,4}/g);
    info.price_count = pm ? pm.length : 0;
    var sm = body.match(/\d{3}\/\d{2}[A-Za-z]\d{2}/g);
    info.size_count  = sm ? sm.length : 0;

    /* Collect unique div class names (first 300 divs) */
    var divs = document.querySelectorAll('div');
    var seenCls = {};
    info.div_classes = [];
    for (var i = 0; i < Math.min(divs.length, 300); i++) {
        var cls = divs[i].className;
        if (cls && typeof cls === 'string' && !seenCls[cls] &&
                cls.length > 3 && cls.length < 120) {
            seenCls[cls] = 1;
            info.div_classes.push(cls);
            if (info.div_classes.length >= 30) break;
        }
    }

    /* Dump first card's raw textContent so we can see structure */
    info.first_card_text = '';
    for (var li = 0; li < links.length; li++) {
        if (/view\s+product\s+details/i.test(links[li].textContent || '')) {
            var el = links[li];
            for (var lvl = 0; lvl < 10; lvl++) {
                el = el.parentElement;
                if (!el || el === document.body) break;
                var t = el.textContent || '';
                if (/\d{3}\/\d{2}[A-Za-z]\d{2}/.test(t) && /\$\d{2,4}/.test(t)) {
                    info.first_card_text = t.replace(/\s+/g,' ').substring(0, 300);
                    break;
                }
            }
            if (info.first_card_text) break;
        }
    }
    return info;
}());
"""

# Extracts all product cards.  Strategy:
#   1. Walk up from each "View Product Details" link until we find an ancestor
#      containing both a tyre-size pattern and a price → that's the card.
#   2. Fallback to common CSS class selectors if strategy 1 yields < 3 results.
#
# Name extraction: the card text is structured as:
#   "MICHELIN Energy XM2+ 175/50R15 79H XL $145 ..."
#   → regex captures the text between brand name and size pattern.
_JS_EXTRACT = r"""
return (function() {
    var results = [];
    var seen = {};

    // Expanded JUNK_RE — now catches promo banners AND card-corner badges
    // that used to leak into the name field: bulk deals ("BUY 4 & GET 20%
    // OFF"), "OR BUY 4 FOR $X", monthly campaigns ("SEPTEMBER SPECIAL
    // DEALS"), "WAS $x / NOW", "RECOMMENDED BY JAX", "M+S" mud-snow
    // badge, "+1" recommendation badge, satisfaction blurbs, tyre-fitting
    // fine print.
    var JUNK_RE = new RegExp([
        '\\d+\\s*day\\s+satisfaction\\s+guarantee',
        'compare\\s+fuel\\s+saving', '\\bfuel\\s+saving\\b',
        '\\bcompare\\b', 'run\\s*flat', 'EV\\s+tyre',
        'select\\s+a\\s+store', 'add\\s+to\\s+booking', 'view\\s+product',
        '\\bqty\\b',
        // Bulk-deal banners (also match plain "BUY 4 & GET 20% OFF"):
        'BUY\\s*\\d+\\s*[&+]\\s*GET\\s+\\d+\\s*%\\s*OFF',
        'BUY\\s*\\d+\\s*[&+]\\s*GET\\s+\\d+(?:ST|ND|RD|TH)\\s+TYRE\\s+FREE',
        'BUY\\s*\\d+\\s*[&+]\\s*GET\\s+4TH\\s+TYRE\\s+FREE',
        'BUY\\s*\\d+\\s+FOR\\s+\\$[\\d,.]+',
        'OR\\s+BUY\\s+\\d+\\s+FOR\\s+\\$[\\d,.]+',
        // Monthly campaigns (any month name + special deals or offer):
        '(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[A-Z]*\\s+SPECIAL\\s+(?:DEAL|OFFER)S?',
        // Price flip banners:
        '\\bWAS\\s*\\$?[\\d,.]+', '\\bNOW\\s*\\$?[\\d,.]+',
        // Cross-brand promo:
        'RECOMMENDED\\s+BY\\s+JAX',
        // Card-corner badges (small icons):
        '\\bM\\+S\\b',                  // Mud + Snow
        '\\bM\\s*\\+\\s*S\\b',          // Space variants
        '\\+\\d+\\s*(?=\\s|$|[A-Z])',   // "+1", "+2" recommendation badges
        '\\bAll\\s+Weather\\b',
        // Extra noise:
        'FREE\\s+SHIPPING', '\\$\\d+\\s+OFF', 'FIT\\s+AT\\s+HOME',
        'Tyre\\s+Fitting', 'Wheel\\s+Balancing', 'Tubeless\\s+Valve',
        'Waste\\s+Tyre\\s+Management', 'Best\\s+Sellers', 'All\\s+Tyres',
    ].join('|'), 'gi');
    // Broad brand-name regex, NOT anchored — so it strips a brand name
    // that appears in the middle of the pre-size chunk (which happens
    // when a promo badge sits before the brand block).  Includes every
    // brand JAX carries as of Sep 2026.
    var BRAND_NAMES_RE = /\b(?:michelin|bridgestone|continental|goodyear|falken|hankook|laufenn|dunlop|kumho|yokohama|pirelli|bfgoodrich|double\s*coin|dynamo|general\s+tire|giti|mickey\s+thompson(?:\s+m\/t)?|radar(?:\s+tyres)?|rovelo|tracmax|transmate|venom|wanli|nexen|toyo|nitto|maxxis|cooper)\b\s*/gi;

    /* ── Shared card-extraction helper ─────────────────────────────────── */
    function extractCard(card) {
        var text = (card.textContent || '').replace(/\s+/g, ' ').trim();

        /* Size: extract from text */
        var size = '';
        var szm = text.match(/(\d{3}\/\d{2}[A-Za-z]\d{2})/);
        if (szm) {
            size = szm[1].toUpperCase();
        } else {
            var szm2 = text.match(/(\d{3}R\d{2})/i);
            if (szm2) size = szm2[1].toUpperCase();
        }

        /* Spec: size + load/speed rating + optional XL only — stop before words */
        var spec = size;
        if (size) {
            var escaped = size.replace('/', '\\/');
            var loadM = text.match(new RegExp(escaped + '\\s+(\\d{2,3}(?:\\/\\d{2,3})?[A-Za-z]{1,2}(?:\\s+XL)?)', 'i'));
            if (loadM) spec = size + ' ' + loadM[1].trim();
        }

        /* Name extraction.  Multi-strategy pipeline (first VALIDATED
           match wins).  A "promo pattern" check runs after each
           strategy's raw output — if the extracted string still looks
           like a bulk-deal banner ("BUY 4 & GET ... OFF", "OR BUY 4
           FOR $X", "20% OFF") we discard it and try the next strategy,
           because we know that text isn't a model name.
           Strategies:
             1) h2/h3/h4 inside the card
             2) DOM-based name element (class-name heuristics)
             3) img[alt] on the product image (JAX often carries the
                model name in the alt text)
             4) "<brand>\s+<candidate>\s+<size>" surgical capture
             5) Full pre-size text with JUNK + BRAND stripped
             6) Detail-page link text
        */
        var name = '';
        var sizePos = szm ? szm.index : (text.search(/\d{3}R\d{2}/i));

        // Aggressive promo-recogniser used to validate every candidate.
        // A hit means "this is promo text, not a product name" → reject.
        var PROMO_LOOKS_LIKE = /^\s*(?:BUY\s*\d|OR\s+BUY|(?:\d+\s*%|SPECIAL|SEPTEMBER|NEW|WAS\b|NOW\b|FREE\s+SHIPPING|GET\s+\d+\s*%|GIFT|EGIFT))/i;

        function _cleanCandidate(s) {
            return (s || '')
                // hard-strip common promo prefixes even if JUNK_RE
                // missed them due to unusual whitespace / punctuation
                .replace(/BUY\s*\d+\s*[&+]?\s*GET\s*\d+\s*%\s*OFF/gi, ' ')
                .replace(/BUY\s*\d+\s*[&+]?\s*GET\s*\d+(?:ST|ND|RD|TH)?\s*TYRE\s+FREE/gi, ' ')
                .replace(/OR\s+BUY\s+\d+\s+FOR\s+\$[\d.,]+/gi, ' ')
                .replace(/\b\d+\s*%\s*OFF\b/gi, ' ')
                .replace(JUNK_RE, ' ')
                .replace(BRAND_NAMES_RE, ' ')
                .replace(/\s+/g, ' ')
                .trim();
        }

        function _acceptName(v) {
            if (!v) return '';
            if (v.length < 2 || v.length > 90) return '';
            if (PROMO_LOOKS_LIKE.test(v)) return '';
            return v;
        }

        // 1) h2/h3/h4 inside the card
        var heads = card.querySelectorAll('h1,h2,h3,h4');
        for (var hi = 0; hi < heads.length; hi++) {
            var ht = (heads[hi].textContent || '').trim();
            if (!ht || ht.length > 90) continue;
            if (/\$|\d{3}\/\d{2}/.test(ht)) continue;
            var cleaned = _acceptName(_cleanCandidate(ht));
            if (cleaned) { name = cleaned; break; }
        }

        // 2) DOM elements with product-name-like class hints
        if (!name) {
            var nameCandidates = card.querySelectorAll(
                '[class*="product-name" i],[class*="ProductName" i],' +
                '[class*="product-title" i],[class*="ProductTitle" i],' +
                '[class*="tyre-name" i],[class*="tyre-title" i],' +
                '[class*="model" i]');
            for (var nci = 0; nci < nameCandidates.length; nci++) {
                var nct = (nameCandidates[nci].textContent || '').trim();
                if (!nct || nct.length > 90) continue;
                if (/\$|\d{3}\/\d{2}/.test(nct)) continue;
                var cleaned2 = _acceptName(_cleanCandidate(nct));
                if (cleaned2) { name = cleaned2; break; }
            }
        }

        // 3) img[alt] on product image (JAX often uses alt="X Fit HP LA41")
        if (!name) {
            var imgs = card.querySelectorAll('img[alt]');
            for (var ii = 0; ii < imgs.length; ii++) {
                var alt = (imgs[ii].getAttribute('alt') || '').trim();
                if (!alt || alt.length > 90) continue;
                if (/logo|badge|icon|promo|banner/i.test(alt)) continue;
                if (/\$|\d{3}\/\d{2}/.test(alt)) continue;
                var cleaned3 = _acceptName(_cleanCandidate(alt));
                if (cleaned3) { name = cleaned3; break; }
            }
        }

        // 4) Surgical "brand + candidate + size" capture
        if (!name) {
            var brandSpanRe = new RegExp(
                '\\b(?:michelin|bridgestone|continental|goodyear|falken|hankook|laufenn|dunlop|kumho|yokohama|pirelli|bfgoodrich|double\\s*coin|dynamo|general\\s+tire|giti|mickey\\s+thompson(?:\\s+m\\/t)?|radar(?:\\s+tyres)?|rovelo|tracmax|transmate|venom|wanli|nexen|toyo|nitto|maxxis|cooper)\\b\\s+' +
                '([^\\$\\n]{2,80}?)\\s+' +
                '\\d{3}[\\/R]',
                'i'
            );
            var bsm = text.match(brandSpanRe);
            if (bsm) {
                var candidate = _acceptName(_cleanCandidate(bsm[1]));
                if (candidate) name = candidate;
            }
        }

        // 5) Legacy fallback: everything before the size, stripped
        if (!name && sizePos > 0) {
            var pre = _acceptName(_cleanCandidate(text.substring(0, sizePos)));
            if (pre) name = pre;
        }

        // 6) Detail-page link text
        if (!name) {
            var links2 = card.querySelectorAll('a');
            for (var li2 = 0; li2 < links2.length; li2++) {
                var lt2 = (links2[li2].textContent || '').trim();
                if (!lt2 || lt2.length > 90) continue;
                if (/view|detail|store|booking|cart|add/i.test(lt2)) continue;
                var cleaned6 = _acceptName(_cleanCandidate(lt2));
                if (cleaned6) { name = cleaned6; break; }
            }
        }

        // 7) Last-ditch: any p/span with reasonable non-promo text
        if (!name) {
            var elems = card.querySelectorAll('p,span,div');
            for (var ei = 0; ei < elems.length; ei++) {
                var et = (elems[ei].textContent || '').trim();
                if (et.length < 3 || et.length > 60) continue;
                if (/\$|\d{3}\/|\bqty\b|booking|detail|view|store/i.test(et)) continue;
                if (/^\d/.test(et)) continue;                   // starts with digit → probably spec
                var cleaned7 = _acceptName(_cleanCandidate(et));
                if (cleaned7) { name = cleaned7; break; }
            }
        }

        /* Regular per-tyre price — skip any $X that lives inside a promo
           banner like "BUY 4 & GET $100 EGIFT CARD" or "OR BUY 4 FOR $X".
           Walk every $-match and pick the first one that ISN'T promo-tagged. */
        var price = '';
        var priceRe = /\$([\d,]+(?:\.\d+)?)/g;
        var pm;
        while ((pm = priceRe.exec(text)) !== null) {
            var i = pm.index;
            var ctxBefore = text.substring(Math.max(0, i - 30), i).toUpperCase();
            var ctxAround = text.substring(Math.max(0, i - 40), i + 60).toUpperCase();
            if (/GIFT\s*CARD|EGIFT/.test(ctxAround)) continue;   // egift card promo
            if (/OR\s+BUY|BUY\s+\d+\s*[&+]\s*GET/.test(ctxBefore)) continue;  // bulk promo
            price = pm[1].replace(/,/g, '');
            break;
        }

        /* Bulk / promo */
        var disc = '', promo = '';
        var b4m = text.match(/OR\s+BUY\s+4\s+FOR\s+\$([\d,]+(?:\.\d+)?)/i);
        if (b4m) {
            disc  = b4m[1].replace(/,/g, '');
            promo = 'OR BUY 4 FOR $' + b4m[1];
        }
        if (!promo && /BUY\s+4\s*[&+]\s*GET\s+4TH\s+TYRE\s+FREE/i.test(text)) {
            promo = 'Buy 4 Get 4th Free';
            if (price) disc = (Math.round(parseFloat(price) * 0.75 * 100) / 100).toFixed(2);
        }

        return { name: name, spec: spec, size: size,
                 price: price, disc_price: disc, promo: promo,
                 card_text: text };
    }

    function addCard(card) {
        var key = (card.textContent || '').replace(/\s+/g,' ').substring(0, 100);
        if (seen[key]) return;
        seen[key] = 1;
        var item = extractCard(card);
        if (item.name || item.price) results.push(item);
    }

    /* ── Strategy 1: anchor on "View Product Details" links ─────────────
       Walk up from each link until we hit an element containing both a
       tyre-size pattern AND a price — that ancestor is the product card. */
    var allLinks = document.querySelectorAll('a');
    var anchorLinks = [];
    for (var li = 0; li < allLinks.length; li++) {
        if (/view\s+product\s+details/i.test(allLinks[li].textContent || ''))
            anchorLinks.push(allLinks[li]);
    }

    for (var ai = 0; ai < anchorLinks.length; ai++) {
        var el = anchorLinks[ai];
        for (var level = 0; level < 10; level++) {
            el = el.parentElement;
            if (!el || el === document.body) break;
            var t = (el.textContent || '');
            if (/\d{3}\/\d{2}[A-Za-z]\d{2}/.test(t) && /\$\d{2,4}/.test(t)) {
                addCard(el);
                break;
            }
        }
    }
    if (results.length >= 3) return results;

    /* ── Strategy 2: CSS class selectors (fallback) ─────────────────── */
    var SELS = [
        'div.product-listing-item', 'div[class*="ProductItem"]',
        'div[class*="product-item"]', 'li[class*="product"]',
        'article[class*="product"]', 'div[class*="tyre-card"]',
        'div[class*="TyreCard"]',    'div[class*="listing-item"]',
    ];
    for (var si = 0; si < SELS.length; si++) {
        var cards = document.querySelectorAll(SELS[si]);
        if (cards && cards.length >= 3) {
            for (var ci = 0; ci < cards.length; ci++) addCard(cards[ci]);
            if (results.length >= 3) break;
        }
    }
    return results;
}());
"""


# ── Python helpers ────────────────────────────────────────────────────────────

def get_total(driver):
    try:
        n = driver.execute_script(_JS_TOTAL)
        return int(n) if n and n > 0 else -1
    except Exception:
        return -1


def diagnose(driver):
    """Print DOM debug info to help identify the correct card selector."""
    try:
        info = driver.execute_script(_JS_DIAGNOSE)
        if not info:
            print("  [DIAG] No info returned")
            return
        print(f"  [DIAG] URL:          {info.get('url','')}")
        print(f"  [DIAG] Title:        {info.get('title','')}")
        print(f"  [DIAG] detail_links: {info.get('detail_links',0)}")
        print(f"  [DIAG] price_count:  {info.get('price_count',0)}")
        print(f"  [DIAG] size_count:   {info.get('size_count',0)}")
        print(f"  [DIAG] div classes (first 30):")
        for cls in (info.get('div_classes') or []):
            print(f"           {cls}")
        if info.get('first_card_text'):
            print(f"  [DIAG] first card text: {info['first_card_text']}")
    except Exception as e:
        print(f"  [DIAG error] {e}")


def extract_page(driver, run_diag=False):
    try:
        raw = driver.execute_script(_JS_EXTRACT)
        if not raw and run_diag:
            print("  [0 products — running DOM diagnostic]")
            diagnose(driver)
        return raw or []
    except Exception as e:
        print(f"  [JS error] {e}")
        return []


def process_raw(raw_items, brand_name, run_flat_flag):
    rows = []
    for item in raw_items:
        name      = (item.get("name")       or "").strip()
        spec      = (item.get("spec")       or "").strip()
        size      = (item.get("size")       or "").strip().upper()
        price_s   = (item.get("price")      or "").strip()
        disc_s    = (item.get("disc_price") or "").strip()
        promo     = (item.get("promo")      or "").strip()
        card_txt  = (item.get("card_text")  or "").strip()

        # Raw description that the old crawler dumped into DESCRIPTION.
        desc_extra = re.sub(re.escape(size), "", spec, flags=re.IGNORECASE).strip(" -–") if size else spec
        raw_desc   = f"{name} {desc_extra}".strip()

        # Split promo strings out of the description; DESCRIPTION keeps
        # only the model line + load rating, PROMO gets the promo text.
        clean_desc, promo_extras = split_promo_from_desc(raw_desc)

        # If the split reduced the description to a bare load rating,
        # rescue the marketing line from the full card text.
        rescued = rescue_line_name(brand_name, clean_desc, card_txt)
        if rescued:
            tail_m = re.search(r'\d{2,3}[A-Za-z]{1,2}(?:\s+XL)?\s*$', clean_desc)
            tail   = tail_m.group(0).strip() if tail_m else ""
            clean_desc = f"{rescued} {tail}".strip() if tail else rescued

        clean_desc = re.sub(r'\s+', ' ', clean_desc).strip(" -–|,")

        # Consolidate the PROMO column: JS-side promo + Python-side extras.
        combined_parts = [p for p in [promo] + promo_extras if p]
        _seen = set(); combined = []
        for p in combined_parts:
            k = re.sub(r'\s+', ' ', p).strip().upper()
            if k and k not in _seen:
                _seen.add(k); combined.append(re.sub(r'\s+', ' ', p).strip())
        combined_promo = " | ".join(combined)

        price = f"{float(price_s):.2f}" if price_s else ""
        disc  = f"{float(disc_s):.2f}"  if disc_s  else ""

        if not size or not clean_desc:
            continue

        rows.append({
            "size": size, "brand": brand_name, "desc": clean_desc,
            "price": price, "disc": disc, "promo": combined_promo,
            "run_flat": run_flat_flag,
        })
    return rows


def write_rows(writer, rows):
    for r in rows:
        writer.writerow([r["size"], r["brand"], r["desc"],
                         r["price"], r["disc"], r["promo"], r["run_flat"]])
        rf = " RF" if r["run_flat"] == "Y" else "   "
        line = f"  {r['brand']:<14} | {r['size']:<12} |{rf}| {r['desc'][:35]:<35} | ${r['price']}"
        if r["disc"]:
            line += f"  → ${r['disc']}"
        print(line)


def scrape_brand(driver, abbr, brand_name, slug, writer, f_out):
    print(f"\n{'='*65}")
    print(f"  {abbr}  {brand_name}  (slug: {slug})")
    print(f"{'='*65}")

    brand_total = 0
    diag_done   = False   # only run DOM diagnostic once per brand

    # Loop the two runflat variants so a brand's runflat catalogue is
    # captured too (was silently skipped by the original crawler).
    for rf_param, rf_flag in RUNFLAT_PASSES:
        variant = "Run Flat" if rf_flag == "Y" else "Non-Run Flat"
        print(f"\n  --- {variant} pass -----------------------------------------")

        url1 = PAGE_URL.format(brand=slug, runflat=rf_param, page=1)
        driver.get(url1)
        time.sleep(4)

        total = get_total(driver)
        if total <= 0:
            print(f"  [{variant}] no items (total={total}) — skipping.")
            continue
        total_pages = math.ceil(total / PAGE_SIZE)
        print(f"  [{variant}] {total} items → {total_pages} page(s)")

        for page in range(1, total_pages + 1):
            if page > 1:
                driver.get(PAGE_URL.format(brand=slug, runflat=rf_param, page=page))
                time.sleep(3)

            raw  = extract_page(driver, run_diag=(not diag_done))
            diag_done = True
            rows = process_raw(raw, brand_name, rf_flag)
            write_rows(writer, rows)
            f_out.flush()
            brand_total += len(rows)
            print(f"  ── [{variant}] page {page}/{total_pages}: {len(rows)} products  (subtotal: {brand_total})")

    return brand_total


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    driver = init_driver()
    print(f"\nOutput: {OUTPUT_FILE}")
    print("JAX Tyres crawler — brand-by-brand, URL pagination (pagesize=45)\n")

    grand_total = 0

    try:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["SIZE", "brand", "DESCRIPTION",
                             "PRICE", "DISC_PRICE", "PROMO", "RUN_FLAT"])

            for abbr, (brand_name, slug) in BRANDS.items():
                try:
                    count = scrape_brand(driver, abbr, brand_name, slug, writer, f)
                    grand_total += count
                    print(f"\n  ✓ {brand_name}: {count} products saved")
                except Exception as e:
                    print(f"\n  ✗ {brand_name}: ERROR — {e}")

        print(f"\n{'='*65}")
        print(f"Done. {grand_total} total products → {OUTPUT_FILE}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
