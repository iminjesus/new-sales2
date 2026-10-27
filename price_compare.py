"""
price_compare.py — Tyre Price Comparison Dashboard
http://127.0.0.1:5000/price

Left  : summary table (latest month)
Right :
  [Size buttons]   ← global filter, applies to both views
  [By Store]  → brand sub-buttons → 3 store lines (Tempe/BJ/JAX) for selected brand + size
  [By Brand]  → store sub-buttons → multiple brand lines for selected store + size
  [chart area]
"""
import csv, glob, json, os, re, sys
from datetime import datetime
from flask import Flask, render_template_string

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_BASE = os.path.dirname(os.path.abspath(__file__))

from price_compare_jax import (
    load_csv,
    build_tempe_lookup, build_bj_lookup, build_jax_lookup, build_tw_lookup,
    best_tempe, best_bj, best_jax, best_tw, best_twi,
    BRANDS, SIZE_CATEGORY, BRAND_COLOURS, ROW_FILLS,
    COMPETITOR_PATTERNS,
)

app = Flask(__name__)


# ── helpers ───────────────────────────────────────────────────────────────────

_MONTH_ABBR = {
    'JAN':'01','JANUARY':'01','FEB':'02','FEBRUARY':'02','MAR':'03','MARCH':'03',
    'APR':'04','APRIL':'04','MAY':'05','JUN':'06','JUNE':'06',
    'JUL':'07','JULY':'07','AUG':'08','AUGUST':'08','SEP':'09','SEPTEMBER':'09',
    'OCT':'10','OCTOBER':'10','NOV':'11','NOVEMBER':'11','DEC':'12','DECEMBER':'12',
}

def _month_key(fp):
    name = os.path.basename(fp).upper()
    # Format 1: YYYYMMDD anywhere in name  (e.g. Tempe_20260401_1439.csv)
    m = re.search(r'(\d{4})(\d{2})\d{2}', name)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    # Format 2: MonthName_YYYY  (e.g. Tempe_Apr_2026.csv, BobJane_March_2026.csv)
    yr = re.search(r'(\d{4})', name)
    if yr:
        year = yr.group(1)
        # find longest matching month name
        best_len, best_num = 0, None
        for key, num in _MONTH_ABBR.items():
            if key in name and len(key) > best_len:
                best_len, best_num = len(key), num
        if best_num:
            return f"{year}{best_num}"
    return "000000"

def _month_label(yyyymm):
    """Short label like "'26.9" (year 2-digit, month 1-2 digit, no zero-pad).
    Compact so the x-axis doesn't need to wrap or shrink under many months."""
    try:
        d = datetime.strptime(str(yyyymm), "%Y%m")
        return f"'{d.year % 100:02d}.{d.month}"
    except Exception:
        return str(yyyymm)

def _latest_per_month(pattern):
    by_month = {}
    for f in sorted(glob.glob(os.path.join(_BASE, pattern))):
        by_month[_month_key(f)] = f
    return by_month

def _ci_glob(prefix):
    """Case-insensitive glob — matches <prefix>_*.csv regardless of
    how the prefix is cased on disk (Tempeweborder vs TempeWebOrder
    vs tempeweborder etc.).  Linux's glob is case-sensitive by
    default so a slightly different casing on a downloaded CSV would
    silently miss the dashboard; this helper walks _BASE once and
    filters names in lower-case so any variant is caught. """
    pl = prefix.lower()
    hits = []
    try:
        for name in os.listdir(_BASE):
            nl = name.lower()
            if nl.startswith(pl + "_") and nl.endswith(".csv"):
                hits.append(os.path.join(_BASE, name))
    except OSError:
        return {}
    by_month = {}
    for f in sorted(hits):
        by_month[_month_key(f)] = f
    return by_month


# ── data loading ──────────────────────────────────────────────────────────────

def load_all_months():
    # Case-insensitive filename matching so a downloaded CSV with
    # unexpected casing (TempeWebOrder_… vs Tempeweborder_… etc.)
    # still gets picked up — the Linux glob is case-sensitive by
    # default and that was eating Oct's Tempeweborder file.
    t_f  = _ci_glob("Tempe")
    bj_f = _ci_glob("BobJane")
    jx_f = _ci_glob("JAX")
    tw_f = _ci_glob("Tempeweborder")
    # Oct-12 diagnostic: Tempeweborder_20261007_1739.csv was on disk
    # but the dashboard still showed no '26.10 data.  Likely causes:
    # another Tempeweborder file with the same month-key sorted later
    # (and won the picker), OR the file wasn't in _BASE at all.  Log
    # the FULL list of candidate files per prefix so we can tell what
    # the picker actually saw.
    print(f"[price_compare] _BASE = {_BASE}")
    for _prefix, _dict in (("Tempe",         t_f),
                           ("BobJane",       bj_f),
                           ("JAX",           jx_f),
                           ("Tempeweborder", tw_f)):
        print(f"[price_compare] {_prefix:>14}: {len(_dict)} month(s) → "
              + ", ".join(f"{mk}={os.path.basename(_dict[mk])}"
                          for mk in sorted(_dict.keys())))
    all_months = sorted(set(list(t_f) + list(bj_f) + list(jx_f) + list(tw_f)))
    return {
        mk: {
            "t_rows":  load_csv(t_f[mk])  if mk in t_f  else [],
            "bj_rows": load_csv(bj_f[mk]) if mk in bj_f else [],
            "jx_rows": load_csv(jx_f[mk]) if mk in jx_f else [],
            "tw_rows": load_csv(tw_f[mk]) if mk in tw_f else [],
        }
        for mk in all_months
    }


def load_compare_base():
    fp = os.path.join(_BASE, 'compare_base.csv')
    if not os.path.exists(fp):
        return []
    rows = []
    with open(fp, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            rows.append(dict(r))
    n = len(rows)
    for i, row in enumerate(rows):
        row['line_span'] = 0
        row['pat_span']  = 0
        if i == 0 or rows[i-1]['line'] != row['line']:
            span = 1
            while i + span < n and rows[i+span]['line'] == row['line']:
                span += 1
            row['line_span'] = span
        if i == 0 or rows[i-1]['pattern'] != row['pattern']:
            span = 1
            while i + span < n and rows[i+span]['pattern'] == row['pattern']:
                span += 1
            row['pat_span'] = span
    return rows


def _pattern_key(desc, size, abbr):
    """Return the full marketing name from a product description by
    stripping the trailing load rating.  So "Kinergy Eco2 K425 94V"
    becomes "Kinergy Eco2 K425" — model line AND model code are kept
    so the legend can show them (the previous version returned only
    a COMPETITOR_PATTERNS keyword substring like "Kinergy Eco" which
    the user asked to be expanded).

    The load-rating regex matches the standard "<digits>[/<digits>]
    <1-2 letters> [XL]" spec that comes AT THE END of every JAX / BJ
    / Tempe description.  If the tail can't be identified we fall
    back to the first N meaningful words, which keeps oddball
    descriptions readable.

    Consistent SKU-to-pattern grouping (Kinergy Eco2 K425 94V and
    91V both collapse under "Kinergy Eco2 K425") works because the
    trailing tokens differ ONLY in the load rating."""
    if not desc:
        return ""
    s = desc.strip()
    # Strip trailing load rating: "91V" / "88Y XL" / "108T" / "94/92R"
    s = re.sub(r'\s+\d{2,3}(?:/\d{2,3})?[A-Za-z]{1,2}(?:\s+XL)?\s*$', '', s)
    # Oct-26: Tempe WebOrder descriptions put the load rating at the
    # START instead of the end — "84H K435 KINERGY ECO2",
    # "107/103S ALL-TERRAIN T/A KO3", "91V SK10 ROAD FIT".  Without
    # stripping it, every load variant of the same pattern (84H / 86H /
    # 91H / 94H …) becomes its own legend line, so the chart shows a
    # cloud of near-duplicate labels instead of one line per pattern.
    s = re.sub(r'^\s*\d{2,3}(?:/\d{2,3})?[A-Za-z]{1,2}\s+', '', s)
    # Strip any remaining trailing badge / spec suffix so "Primacy 5
    # XL" or "Grandtrek AT5 M+S" doesn't inflate the label.
    s = re.sub(r'\s+(?:XL|OWT|RWL|OBL|TL|TT|FR|MFS|DOT|M\+S)\s*$',
               '', s, flags=re.IGNORECASE)
    # Also drop those badges when they sit at the START of a Tempe
    # description that already had load stripped — e.g. "RWL ALL-
    # TERRAIN T/A KO3".
    s = re.sub(r'^\s*(?:XL|OWT|RWL|OBL|TL|TT|FR|MFS|DOT|M\+S)\s+',
               '', s, flags=re.IGNORECASE)
    s = s.strip(' -–|,')
    if s:
        return s
    # Fallback: first 3 meaningful words when the description doesn't
    # have a recognisable load-rating tail.
    words = []
    for tok in desc.split():
        t = re.sub(r'[().,+]', '', tok).strip()
        if not t or len(t) <= 1:
            continue
        if re.match(r'^\d', t):
            continue
        if t.upper() in ('XL','OWT','RWL','OBL','TL','TT','FR','MFS',
                         'DOT','BMW','AUDI','BENZ','DEMO','EV','RC'):
            continue
        words.append(t)
        if len(words) >= 3:
            break
    return ' '.join(words) if words else desc[:40]


def build_data(monthly):
    # Sentinel values that should be treated as "no price" — these creep in
    # from the source data and otherwise show up as nonsensical points on the
    # chart ($0 collapses the line, $999 floats far above everything).
    VOID_PRICES = {0, 999}
    def _norm(p):
        if p is None:
            return None
        try:
            if float(p) in VOID_PRICES:
                return None
        except (TypeError, ValueError):
            return None
        return p

    months       = sorted(monthly.keys())
    month_labels = [_month_label(m) for m in months]
    # ── Size universe ──────────────────────────────────────────────
    # Collect EVERY size that shows up in any raw row across every
    # month / store so the Size button bar can list the full catalogue
    # (not just the 32 curated ones in SIZE_CATEGORY).  The Oct-12
    # user ask #3: "모든 사이즈를 나열해 줄 것" (list all sizes).
    # Curated sizes stay first in SIZE_CATEGORY insertion order so
    # the "popular" grid at the top keeps its established layout;
    # extras discovered in the raw data are appended afterwards,
    # sorted so the extras aren't a random dump.
    _curated     = list(SIZE_CATEGORY.keys())
    _curated_set = set(_curated)
    _extra_sizes = set()
    for _mk, _md in monthly.items():
        for _bucket in ("t_rows", "bj_rows", "jx_rows", "tw_rows"):
            for _r in _md.get(_bucket, []):
                _sz = (_r.get("SIZE","") or "").strip()
                if _sz and _sz not in _curated_set:
                    _extra_sizes.add(_sz)
    sizes = _curated + sorted(_extra_sizes)
    abbrs = list(BRANDS.keys())

    store_avg = {"tempe": [], "bj": [], "jax": [], "tw": [], "twi": []}
    # size_data[size][abbr][store]     = [price_per_month, ...]
    # size_data[size][abbr]["<st>_d"]  = [desc_per_month,  ...]   (jax / bj / tempe)
    # size_data[size][abbr]["<st>_f"]  = [flag_per_month,  ...]   ("" / cat_fb / chg)
    # The desc + flag arrays let the front-end show a tooltip with the
    # actual product name at each point, and highlight points where
    # the product changed month-over-month or fell back to the
    # COMPETITOR_PATTERNS filter because Tempe had no reference row.
    def _mk_slots():
        # Oct-26 ask: TempeWOS Sell Out / Sell In also need per-point
        # descriptions AND per-pattern series dicts so the By-Brand
        # legend reads "KH Kumho — Kinergy Eco2 K425" (and extra dashed
        # lines for sibling patterns) instead of a bare "KH Kumho".
        # The CSV from tempeweborder_crawling_extend.py already carries
        # the full desc (e.g. "84H K435 KINERGY ECO2" or "107/103S
        # ALL-TERRAIN T/A KO3") — we just weren't exposing it before.
        return {"tempe": [], "bj": [], "jax": [], "tw": [], "twi": [],
                # Promo-price arrays — Bob Jane and JAX publish both a
                # regular PRICE and a promotional DISC_PRICE in their
                # CSVs; the toggle at the top of the Price Comparison
                # Chart switches which array the chart / tables read.
                # Fallback: when a month has no DISC_PRICE, we store
                # the normal PRICE so the promo line stays continuous
                # and the chart still reads "the effective customer
                # price for that month".  Tempe / TempeWOS have no
                # promo concept — the toggle is a no-op for them.
                "bj_promo":  [], "jax_promo": [],
                # Oct-27: per-month stock arrays for the stores whose
                # CSVs now carry a STOCK column (tempe retail, tw/twi
                # TempeWebOrder).  Value per month: None (unknown /
                # pre-STOCK CSV), 0 (listed-but-OOS), N (available).
                # Front-end turns 0 into a hollow point on the chart
                # and parentheses around the price in the left tables.
                # BJ / JAX stock info not yet scraped so no array yet.
                "tempe_stk": [], "tw_stk": [], "twi_stk": [],
                "tempe_d": [], "bj_d": [], "jax_d": [],
                "tw_d":    [], "twi_d":  [],
                "bj_f":    [], "jax_f":  [],
                # Per-pattern series: pattern_label → {prices, descs,
                # promos}.  Promos array parallel to prices; the
                # front-end switches between them based on pricingMode.
                # Populated month-by-month below.  Front-end expands
                # these into extra dashed lines on the "By Brand"
                # chart so competing series (Kinergy Eco 2 vs Kinergy
                # GT for HK/K135) show up side-by-side.
                "jax_series": {}, "bj_series": {},
                "tw_series":  {}, "twi_series": {}}
    size_data = {s: {a: _mk_slots() for a in abbrs} for s in sizes}

    latest       = months[-1] if months else None
    summary_rows = []

    # ── Per-month, per-store per-pattern series builder ──────────────
    # Given the raw candidate list for (size, abbr) at store, group
    # candidates by pattern_key, keep the cheapest per pattern, then
    # append this month's price into the running series arrays.  New
    # patterns discovered mid-series get back-filled with None for
    # prior months so all series arrays stay length-aligned.
    def _accumulate_series(store, cands, size, abbr, series_dict, mk_idx, price_idx,
                           promo_idx=None):
        # Oct-26 ask: track promo prices alongside normal so the Normal/
        # Promo toggle at the top of the chart can switch each series
        # between its regular and discounted price-per-month without a
        # second data pass.  promo_idx = tuple index of the DISC_PRICE
        # field in each candidate (2 for JAX / BJ, where candidates are
        # (desc, price, disc, promo_name, save_text, flag)).  Stores
        # without a promo concept (Tempe / TempeWOS) pass promo_idx=None
        # and we fall back to the normal price so the array stays well-
        # defined (toggle is a no-op for them).
        by_pat = {}
        for c in cands:
            cd = c[0]
            cp = c[price_idx] if len(c) > price_idx else None
            if cp is None:
                continue
            try:
                if float(cp) in VOID_PRICES:
                    continue
            except (TypeError, ValueError):
                continue
            # Pull the promo price when available; drop sentinels.
            cpr = None
            if promo_idx is not None and len(c) > promo_idx:
                _cpr = c[promo_idx]
                try:
                    if _cpr is not None and float(_cpr) not in VOID_PRICES:
                        cpr = _cpr
                except (TypeError, ValueError):
                    cpr = None
            pk = _pattern_key(cd, size, abbr)
            if not pk:
                continue
            if pk not in by_pat or cp < by_pat[pk][0]:
                by_pat[pk] = (cp, cd, cpr)
        # Extend every already-tracked pattern with this month's slot
        for pk, entry in series_dict.items():
            if pk in by_pat:
                _cp, _cd, _cpr = by_pat[pk]
                entry["prices"].append(_cp)
                entry["descs"].append(_cd)
                # Promo falls back to normal when the row carried no
                # DISC_PRICE this month — keeps the "effective customer
                # price" line continuous.
                entry["promos"].append(_cpr if _cpr is not None else _cp)
            else:
                entry["prices"].append(None)
                entry["descs"].append("")
                entry["promos"].append(None)
        # Register any new pattern discovered this month, back-filling
        # None for the (mk_idx) months that came before it.
        for pk, (cp, cd, cpr) in by_pat.items():
            if pk not in series_dict:
                series_dict[pk] = {
                    "prices":  [None] * mk_idx + [cp],
                    "descs":   [""]   * mk_idx + [cd],
                    "promos":  [None] * mk_idx + [cpr if cpr is not None else cp],
                }

    for mk_idx, mk in enumerate(months):
        md     = monthly[mk]
        t_lk   = build_tempe_lookup(md["t_rows"])
        bj_lk  = build_bj_lookup(md["bj_rows"])
        jx_lk  = build_jax_lookup(md["jx_rows"])
        tw_lk  = build_tw_lookup(md["tw_rows"])
        tp, bp, jp, twp, twip2 = [], [], [], [], []

        for size in sizes:
            for abbr in abbrs:
                # Oct-27: best_tempe / best_tw / best_twi now return
                # a 4-tuple ending in stock (int or None).  Capture it
                # into size_data[size][abbr]["<store>_stk"] so the
                # front-end can render OOS months with the hollow-dot
                # + parenthesised-price treatment.
                t_desc, t_cost, t_price, t_stock      = best_tempe(size, abbr, t_lk)
                bj_desc, bj_price, bj_disc, _, _, bj_flag = best_bj(size, abbr, bj_lk, t_desc)
                jx_desc, jx_price, jx_disc, _, _, jx_flag = best_jax(size, abbr, jx_lk, t_desc)
                _tw_desc, _tw_cost, tw_price, tw_stock      = best_tw(size, abbr, tw_lk)
                _twi_desc, _, twi_price, twi_stock          = best_twi(size, abbr, tw_lk)
                # Void the meaningless sentinels (0 / 999) so the chart and
                # tables treat them as missing instead of plotting them.
                t_price  = _norm(t_price)
                bj_price = _norm(bj_price)
                jx_price = _norm(jx_price)
                tw_price = _norm(tw_price)
                twi_price = _norm(twi_price)
                bj_disc  = _norm(bj_disc)
                jx_disc  = _norm(jx_disc)

                size_data[size][abbr]["tempe"].append(t_price)
                size_data[size][abbr]["bj"].append(bj_price)
                size_data[size][abbr]["jax"].append(jx_price)
                size_data[size][abbr]["tw"].append(tw_price)
                size_data[size][abbr]["twi"].append(twi_price)
                size_data[size][abbr]["tempe_stk"].append(t_stock)
                size_data[size][abbr]["tw_stk"].append(tw_stock)
                size_data[size][abbr]["twi_stk"].append(twi_stock)
                # Promo arrays: effective customer price per month.
                # Fallback = normal price when DISC_PRICE is absent, so
                # the Promo view's line stays unbroken.  Null-when-null
                # happens only when the whole row is missing (same gap
                # as the Normal view).
                size_data[size][abbr]["bj_promo"].append(
                    bj_disc if bj_disc is not None else bj_price)
                size_data[size][abbr]["jax_promo"].append(
                    jx_disc if jx_disc is not None else jx_price)

                # Descriptions (per month) — used by the front-end tooltip
                # AND (for tw/twi, Oct-26) by _guessPatternFromDescs() so
                # the Legend reads e.g. "KH Kumho — Centara 3 CT667"
                # instead of a bare "KH Kumho".
                size_data[size][abbr]["tempe_d"].append(t_desc    or "")
                size_data[size][abbr]["bj_d"].append(bj_desc      or "")
                size_data[size][abbr]["jax_d"].append(jx_desc     or "")
                size_data[size][abbr]["tw_d"].append(_tw_desc     or "")
                size_data[size][abbr]["twi_d"].append(_twi_desc   or "")

                # Flag: "cat_fb" when the row came from the
                # COMPETITOR_PATTERNS fallback (Tempe had no reference),
                # "match" for the normal word-walk path, "" otherwise.
                # A "chg" flag is layered on below when the picked
                # description differs from the previous month's.
                size_data[size][abbr]["bj_f"].append(bj_flag  or "")
                size_data[size][abbr]["jax_f"].append(jx_flag or "")

                # Per-pattern series: JAX + BJ candidates grouped by
                # pattern label (Kinergy Eco 2 vs Kinergy GT etc.).
                # price_idx=1 because build_bj_lookup / build_jax_lookup
                # both put price at tuple index [1].  promo_idx=2 so
                # the Normal/Promo toggle also works for sibling lines.
                _accumulate_series("jax", jx_lk.get((size, abbr), []),
                                   size, abbr,
                                   size_data[size][abbr]["jax_series"],
                                   mk_idx, price_idx=1, promo_idx=2)
                _accumulate_series("bj", bj_lk.get((size, abbr), []),
                                   size, abbr,
                                   size_data[size][abbr]["bj_series"],
                                   mk_idx, price_idx=1, promo_idx=2)
                # Oct-26 ask: same per-pattern breakdown for TempeWOS
                # Sell Out (tw) and Sell In (twi).  tw_lk tuples are
                # (desc, sell_in_price, sell_out_price), so Sell Out
                # reads index 2 and Sell In reads index 1.  Without
                # these the By-Brand chart's TempeWOS views only ever
                # drew ONE line per brand with no pattern label in the
                # legend — e.g. "KH Kumho" alone, instead of
                # "KH Kumho — Centara 3 CT667" plus dashed sibling
                # lines for the other patterns in the same size.
                _accumulate_series("tw", tw_lk.get((size, abbr), []),
                                   size, abbr,
                                   size_data[size][abbr]["tw_series"],
                                   mk_idx, price_idx=2)
                _accumulate_series("twi", tw_lk.get((size, abbr), []),
                                   size, abbr,
                                   size_data[size][abbr]["twi_series"],
                                   mk_idx, price_idx=1)

                if t_price:   tp.append(t_price)
                if bj_price:  bp.append(bj_price)
                if jx_price:  jp.append(jx_price)
                if tw_price:  twp.append(tw_price)
                if twi_price: twip2.append(twi_price)

                if mk == latest and (t_price or bj_price or jx_price):
                    cat = SIZE_CATEGORY.get(size, ("?", "?", "?"))
                    summary_rows.append({
                        "size":      size,
                        "brand":     f"{abbr} {BRANDS[abbr]}",
                        "abbr":      abbr,
                        "t_price":   t_price,
                        "bj_price":  bj_price,
                        "jax_price": jx_price,
                        "category":  f"{cat[0]}/{cat[1]}",
                    })

        store_avg["tempe"].append(round(sum(tp)/len(tp),   2) if tp  else None)
        store_avg["bj"].append(round(sum(bp)/len(bp),     2) if bp  else None)
        store_avg["jax"].append(round(sum(jp)/len(jp),    2) if jp  else None)
        store_avg["tw"].append(round(sum(twp)/len(twp),   2) if twp   else None)
        store_avg["twi"].append(round(sum(twip2)/len(twip2), 2) if twip2 else None)

    # ── Month-over-month "product changed" flag ──────────────────────
    # Compare each month's picked description with the previous month's
    # for the same (size, brand).  When the *first meaningful word* of
    # the description differs (e.g. "Primacy" → "Lotus"), tag the point
    # so the front-end can render a callout showing the change.  We
    # skip empty descriptions on either side — a blank slot is a miss,
    # not a product change.
    def _first_word(desc):
        if not desc:
            return ""
        for tok in desc.split():
            t = re.sub(r'[().,+]', '', tok).strip()
            if not t or len(t) <= 1:
                continue
            if re.match(r'^\d', t):        # skip pure-numeric leaders
                continue
            if t.upper() in ('XL','OWT','RWL','OBL','TL','TT','FR','MFS',
                             'DOT','BMW','AUDI','BENZ','DEMO','EV','RC'):
                continue
            return t.upper()
        return ""

    for size in sizes:
        for abbr in abbrs:
            d = size_data[size][abbr]
            for store_d, store_f in (("jax_d", "jax_f"), ("bj_d", "bj_f")):
                descs = d[store_d]; flags = d[store_f]
                prev_key = ""
                for i, desc in enumerate(descs):
                    key = _first_word(desc)
                    if key and prev_key and key != prev_key:
                        # Layer "chg" onto any existing flag so the
                        # front-end can distinguish plain fallback vs
                        # a mid-series product change.
                        flags[i] = (flags[i] + "+chg") if flags[i] else "chg"
                    if key:
                        prev_key = key

    # brand_size_data: only combos that have at least one real price
    brand_size_data = {}
    for abbr in abbrs:
        for size in sizes:
            d = size_data[size][abbr]
            if any(p is not None for p in d["tempe"] + d["bj"] + d["jax"] + d["tw"] + d["twi"]):
                brand_size_data.setdefault(abbr, {})[size] = d

    _sizes_with_data = sorted({
        size for size in sizes
        for abbr in abbrs
        if any(p is not None for p in
               size_data[size][abbr]["tempe"] +
               size_data[size][abbr]["bj"] +
               size_data[size][abbr]["jax"] +
               size_data[size][abbr]["tw"] +
               size_data[size][abbr]["twi"])
    }, key=lambda s: sizes.index(s))
    return {
        "months":          month_labels,
        "store_avg":       store_avg,
        "size_data":       size_data,
        "brand_size_data": brand_size_data,
        "summary_rows":    summary_rows,
        "sizes_with_data": _sizes_with_data,
        # The subset of sizes_with_data that lives in the curated
        # SIZE_CATEGORY list, so the front-end can render them with
        # a highlight pill style AND filter down to just these when
        # the user ticks the "Reserved" checkbox.
        "popular_sizes":   [s for s in _sizes_with_data if s in _curated_set],
    }


# ── HTML template ─────────────────────────────────────────────────────────────

_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Tyre Price Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<!-- chartjs-plugin-zoom gives us the box-select drag-to-zoom and
     ctx.resetZoom() needed for Oct-12 request #2: drag a vertical area
     on the chart to zoom into that Y-axis range; drag toward the
     upper-left to reset. -->
<script src="https://cdn.jsdelivr.net/npm/hammerjs@2.0.8/hammer.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2.0.1/dist/chartjs-plugin-zoom.min.js"></script>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: system-ui, sans-serif; background: #f0f2f5;
       height: 100vh; display: flex; flex-direction: column; overflow: hidden; }

.hdr { background: #1F4E79; color: #fff; padding: 11px 18px; flex-shrink: 0;
       display: flex; align-items: center; gap: 14px; }
.hdr h1 { font-size: 16px; }
.hdr span { font-size: 11px; opacity: .65; }
.hdr .nav { margin-left: auto; display: flex; gap: 6px; }
.hdr .nav a { font-size: 12px; padding: 4px 11px; border-radius: 4px; cursor: pointer;
              border: 1px solid rgba(255,255,255,0.5); color: #fff; text-decoration: none;
              background: transparent; }
.hdr .nav a:hover { background: rgba(255,255,255,0.15); }
.hdr .nav a.active { background: #fff; color: #1F4E79; font-weight: 600; }

.body { display: flex; flex: 1; overflow: hidden; }

/* ── LEFT panel — comparison tables ── */
.left { width: 40%; border-right: 1px solid #d0d5dd;
        display: flex; flex-direction: column; overflow: hidden; background: #fff; }
.left-title { background: #1F4E79; color: #fff; padding: 7px 14px;
              font-size: 11.5px; font-weight: 700; flex-shrink: 0; letter-spacing: .02em;
              display: flex; align-items: center; justify-content: space-between; }
.left-title .lt-text { flex: 1; }
.left-scroll { flex: 1; overflow-y: auto; }

/* Expand / collapse icon for the tables — clicking it swaps the LEFT
   panel to full-viewport overlay so the ‘26.8 / ‘26.9 columns that
   normally get clipped by the 40 % left-panel width become fully
   readable.  Clicking again returns to the split layout. */
.expand-btn { background: rgba(255,255,255,0.12); color: #fff;
              border: 1px solid rgba(255,255,255,0.45); border-radius: 3px;
              width: 22px; height: 22px; padding: 0; margin-left: 8px;
              font: 14px/1 system-ui, sans-serif; cursor: pointer;
              display: flex; align-items: center; justify-content: center; }
.expand-btn:hover { background: rgba(255,255,255,0.28); }
body.tables-expanded .left {
    position: fixed; top: 0; left: 0; right: 0; bottom: 0;
    width: 100vw; height: 100vh; z-index: 9998; border-right: none;
    box-shadow: 0 0 0 9999px rgba(0,0,0,0.35);
}
body.tables-expanded .left-title { background: #143756; }
body.tables-expanded .expand-btn { background: #C62828; border-color: #C62828; }
body.tables-expanded .ctable { font-size: 14px; }

/* Oct-23 user ask: mirror the expand icon on the RIGHT chart panel
   so a user can enlarge the chart to the whole viewport the same
   way the LEFT tables panel can.  Overlay styling identical to the
   left-expand branch (fixed viewport + dim backdrop).  While
   right-expanded, the LEFT panel is hidden instead of overlaid so
   the two can't simultaneously claim the viewport. */
body.chart-expanded .right {
    position: fixed; top: 0; left: 0; right: 0; bottom: 0;
    width: 100vw; height: 100vh; z-index: 9998;
    box-shadow: 0 0 0 9999px rgba(0,0,0,0.35);
    background: #fff;
}
body.chart-expanded .left { display: none; }
body.chart-expanded .expand-chart-btn { background: #C62828; border-color: #C62828; }
/* Oct-27 split-view: the right-half pane holds an iframe loading
   this same dashboard, so each side can carry its own independent
   filter / view state.  Half-width layout only kicks in when
   #chart-split-pane exists in the DOM (toggled by toggleChartSplit).
   Keeps the LEFT comparison tables single — the duplicate is only
   for the chart panel. */
.chart-split-pane {
    width: 50%; border-left: 1px solid #d0d5dd;
    display: flex; flex-direction: column; min-width: 0; background:#fff;
}
.chart-split-pane iframe {
    flex: 1; width: 100%; height: 100%; border: 0;
    background:#fff;
}
.chart-split-close {
    background: #C62828; color: #fff;
    border: 1px solid #a81d1d; border-radius: 3px;
    width: 22px; height: 22px; padding: 0; margin-left: 8px;
    font: 14px/1 system-ui, sans-serif; cursor: pointer;
    display: flex; align-items: center; justify-content: center;
}
.chart-split-close:hover { background: #a81d1d; }
body.chart-split .right { width: 50%; min-width: 0; }
body.chart-split .left  { width: 25%; }
body.chart-split #split-chart-btn { background: #C62828; border-color: #C62828; }
/* Oct-27 "hide tables" state — the LEFT panel collapses entirely so
   the two chart panes get the full viewport.  A fixed-position chip
   at the top-left (visible only when the state is on) lets the user
   bring the tables back. */
body.left-hidden .left { display: none; }
body.left-hidden .right { width: 50%; }
body.left-hidden .chart-split-pane { width: 50%; }
body:not(.chart-split).left-hidden .right { width: 100%; }
.show-tables-chip {
    position: fixed; top: 46px; left: 8px; z-index: 9997;
    background: #1F4E79; color: #fff;
    border: 1px solid #163a60; border-radius: 3px;
    padding: 3px 10px; font-size: 10.5px; font-weight: 700;
    letter-spacing: .03em; cursor: pointer;
    display: none;
}
body.left-hidden .show-tables-chip { display: inline-block; }
.show-tables-chip:hover { background: #163a60; }
/* The expand button sits in the title bar — same style tokens as
   .expand-btn so the two feel identical. */
.right-title {
    display: flex; align-items: center;
    background: #1F4E79; color: #fff;
    padding: 4px 10px; font-size: 11px; font-weight: 700;
    letter-spacing: .05em; text-transform: uppercase;
    border-bottom: 1px solid #163a60;
}
.right-title .rt-text { flex: 1; }
.expand-chart-btn { background: rgba(255,255,255,0.12); color: #fff;
                    border: 1px solid rgba(255,255,255,0.45); border-radius: 3px;
                    width: 22px; height: 22px; padding: 0; margin-left: 8px;
                    font: 14px/1 system-ui, sans-serif; cursor: pointer;
                    display: flex; align-items: center; justify-content: center; }
.expand-chart-btn:hover { background: rgba(255,255,255,0.28); }
#chart-table-wrap { padding: 4px 0 0; }
/* ── Base pattern table ── */
.base-section { border-top: 3px solid #1F4E79; margin-top: 14px; }
.base-title { background: #37474F; color: #fff; padding: 5px 10px;
              font-size: 10px; font-weight: 700; letter-spacing: .05em; text-transform: uppercase; }
.btable { width: 100%; border-collapse: collapse; font-size: 9.5px; }
.btable thead th { padding: 4px 5px; background: #1F4E79; color: #fff;
                   position: sticky; top: 0; z-index: 4; text-align: center;
                   white-space: nowrap; font-weight: 700; border: 1px solid #2a5f94; }
.btable thead th.bh-brand { background: #263238; }
.btable tbody td { padding: 3px 5px; border: 1px solid #e8e8e8;
                   white-space: nowrap; vertical-align: middle; }
.btable tbody td.td-line { background: #1F4E79; color: #fff; font-weight: 700;
                            text-align: center; font-size: 9px; }
.btable tbody td.td-pat  { background: #EEF2F7; font-weight: 700; font-size: 9px;
                            text-align: center; }
.btable tbody td.td-sub  { background: #F7F9FC; color: #555; font-size: 8.5px; }
.btable tbody td.td-inch { text-align: center; color: #555; }
.btable tbody td.td-size { font-family: monospace; color: #1F4E79; font-weight: 700; }
.btable tbody tr.new-line > td:not(.td-line) { border-top: 2px solid #1F4E79; }
.btable tbody tr.new-pat  > td:not(.td-line) { border-top: 1px dashed #aab; }

/* ── RIGHT panel ── */
.right { width: 60%; display: flex; flex-direction: column; overflow: hidden; }

/* Button panel */
.btn-panel { flex-shrink: 0; padding: 10px 14px 8px; background: #f5f7fa;
             border-bottom: 1px solid #dde1e7; }

.btn-row { display: flex; align-items: flex-start; gap: 8px; margin-bottom: 7px; }
.btn-row:last-child { margin-bottom: 0; }

.row-label { font-size: 10px; font-weight: 700; color: #1F4E79; letter-spacing: .03em;
             min-width: 70px; padding-top: 5px; flex-shrink: 0; text-transform: uppercase; }

.btns { display: flex; flex-wrap: wrap; gap: 4px; }

/* buttons */
.btn {
    font-size: 11px; padding: 4px 9px; border-radius: 4px; cursor: pointer;
    border: 1.5px solid #c5ccd8; background: #fff; color: #444;
    transition: background .12s, color .12s, border-color .12s;
    white-space: nowrap; line-height: 1.4;
}
.btn:hover  { background: #eef1f8; border-color: #1F4E79; color: #1F4E79; }
.btn.active { background: #1F4E79 !important; color: #fff !important;
              border-color: #1F4E79 !important; }

/* mode buttons are slightly bigger */
.btn.mode { font-size: 12px; padding: 5px 14px; font-weight: 600; }

/* divider between size row and mode rows */
.divider { border: none; border-top: 1px solid #dde1e7; margin: 6px 0; }

/* sub-buttons row animation */
#sub-row { display: none; }

/* ── Chart area ── */
.chart-panel { flex: 1; padding: 12px 14px; display: flex;
               flex-direction: column; min-height: 0; overflow: hidden; }
.chart-title { font-size: 12.5px; font-weight: 600; color: #223;
               margin-bottom: 8px; }
.no-data { color: #e53; font-size: 13px; padding-top: 20px; text-align: center; }
.chart-wrap { flex: 1; min-height: 220px; position: relative; }
.chart-wrap canvas { position: absolute; inset: 0; width:100%!important; height:100%!important; }

.ctable { width: 100%; border-collapse: collapse; font-size: 13.5px; }
.ctable thead th { padding: 7px 12px; position: sticky; top: 0;
                   z-index: 3; text-align: center; white-space: nowrap; font-weight: 700; }
.ctable thead th.th-month { background: #1F4E79; color: #fff; }
.ctable thead th:not(.th-month) { background: #f0f4f9; border-bottom: 2px solid #c5ccd8; }
.ctable tbody td { padding: 7px 12px; border-bottom: 1px solid #eee; white-space: nowrap; font-size: 13.5px; }
.ctable tbody tr:hover td { background: #f0f4fa; }
.ctable .r { text-align: right; font-family: monospace; }
/* Sibling series rows (Kinergy GT under HK Hankook — Kinergy Eco 2)
   get a thinner border and a slight indent so the primary brand rows
   stay visually dominant. */
.ctable tbody tr.extra td { border-bottom: 1px dashed #eee; padding-top: 4px; padding-bottom: 4px; }
.ctable tbody tr.extra + tr:not(.extra) td { border-top: 1px solid #d5dae2; }
.ctable .r.anom { color: #C62828; font-weight: 700; }
.ctable.idxtbl thead th.th-month { background: #37474F; }
.idx-title { font-size: 12px; font-weight: 700; color: #37474F;
             padding: 12px 12px 4px 12px; letter-spacing: .03em; text-transform: uppercase; }

/* ── Mobile / tablet ─────────────────────────────────────────────── */
@media (max-width: 900px) {
  body { height: auto; overflow: auto; padding-top: 46px; }
  .hdr { padding: 8px 12px; flex-wrap: wrap; gap: 6px; }
  .hdr h1, .hdr .title { font-size: 14px !important; }
  /* Pin the nav to the very top of the viewport on mobile so it's
     always one tap away from any of the dashboard pages. */
  .hdr .nav {
    position: fixed !important; top: 0; left: 0; right: 0; z-index: 1000;
    margin: 0 !important; background: #1F4E79; border-bottom: 1px solid #163C5C;
    padding: 6px 6px; display: flex; flex-wrap: nowrap; overflow-x: auto;
    -webkit-overflow-scrolling: touch; gap: 4px; width: auto;
  }
  .hdr .nav a { flex: 0 0 auto; font-size: 11px !important; padding: 5px 10px !important; text-align: center; white-space: nowrap; }
  /* Stack the two main panels vertically */
  .main, .body, .layout { flex-direction: column !important; height: auto !important; }
  .left, .right { width: 100% !important; border-right: none !important;
                  border-bottom: 1px solid #d0d5dd; }
  /* Tables: horizontal scroll wrapper + smaller font */
  .left, .right { overflow-x: auto; -webkit-overflow-scrolling: touch; }
  .btable, .ctable { font-size: 11px !important; min-width: 480px; }
  .btable th, .btable td, .ctable th, .ctable td {
    padding: 5px 6px !important; white-space: nowrap;
  }
  /* Chart container: shorter on mobile */
  .chart-wrap { height: 280px !important; }
  /* Filter / button rows wrap nicely */
  .filters, .btns, .toolbar { flex-wrap: wrap !important; gap: 5px !important; }
  .btns button, .filters select { font-size: 11px !important; padding: 4px 8px !important; }
}
@media (max-width: 480px) {
  .hdr h1, .hdr .title { font-size: 12px !important; }
  .btable, .ctable { min-width: 360px; }
}
</style>
</head>
<body>

<div class="hdr">
  <h1>Tyre Price Comparison Dashboard</h1>
  <span>Tempe &nbsp;|&nbsp; Bob Jane &nbsp;|&nbsp; JAX &nbsp;&middot;&nbsp;
        {{ month_count }} month{{ 's' if month_count != 1 else '' }} of data</span>
  <nav class="nav">
    <a href="/">Graph View</a>
    <a href="/map">Map View</a>
    <a href="/stock">Stock</a>
    <a href="/stock_balance">Stock Balance</a>
    <a href="/rebate">Rebate</a>
    <a href="/price" class="active">Price</a>
    <a href="/meeting">Meeting</a>
  </nav>
</div>

<!-- Oct-27: appears when the LEFT comparison tables are hidden.
     Click → restore the tables.  Hidden by default (CSS). -->
<button class="show-tables-chip" onclick="toggleTablesHidden()"
        title="Show the comparison tables again">&#x2630; Show tables</button>

<div class="body">

  <!-- ── LEFT: comparison tables ── -->
  <div class="left">
    <div class="left-title">
      <span class="lt-text">Comparison Base table for popular size</span>
      <!-- Oct-27 user ask: close button hides the LEFT panel entirely
           so the two chart panes (main + split duplicate) take the
           full viewport.  A small "Show tables" chip appears at the
           top-left corner when hidden so the user can bring the
           tables back. -->
      <button id="close-tables-btn" class="expand-btn"
              onclick="toggleTablesHidden()" title="Hide the comparison tables"
              aria-label="Hide tables"
              style="background:#C62828;border-color:#C62828">&#x2715;</button>
      <button id="expand-tables-btn" class="expand-btn"
              onclick="toggleTablesExpand()" title="Expand tables to full view"
              aria-label="Expand tables">&#x26F6;</button>
    </div>
    <div class="left-scroll">
      <div id="chart-table-wrap"></div>
      {% if base_rows %}
      <div class="base-section">
        <div class="base-title">Pattern Reference</div>
        <table class="btable">
          <thead>
            <tr>
              <th>Line</th><th>Pattern</th><th>Sub</th><th>In</th><th>Size</th>
              <th class="bh-brand" style="color:#FFB3B3">MC</th>
              <th class="bh-brand" style="color:#90CAF9">BS</th>
              <th class="bh-brand" style="color:#FFCC80">CT</th>
              <th class="bh-brand" style="color:#9FA8DA">GY</th>
              <th class="bh-brand" style="color:#80CBC4">KH</th>
              <th class="bh-brand" style="color:#F48FB1">FK</th>
              <th class="bh-brand" style="color:#FFCC80">HK</th>
              <th class="bh-brand" style="color:#A5D6A7">LF</th>
            </tr>
          </thead>
          <tbody>
          {% for r in base_rows %}
            <tr class="{{ 'new-line' if r.line_span > 0 else ('new-pat' if r.pat_span > 0 else '') }}">
              {% if r.line_span > 0 %}
                <td class="td-line" rowspan="{{ r.line_span }}">{{ r.line }}</td>
              {% endif %}
              {% if r.pat_span > 0 %}
                <td class="td-pat" rowspan="{{ r.pat_span }}">{{ r.pattern }}</td>
              {% endif %}
              <td class="td-sub">{{ r.subcat }}</td>
              <td class="td-inch">{{ r.inch }}</td>
              <td class="td-size">{{ r.size }}</td>
              <td>{{ r.mc }}</td><td>{{ r.bs }}</td><td>{{ r.ct }}</td><td>{{ r.gy }}</td>
              <td>{{ r.kh }}</td><td>{{ r.fk }}</td><td>{{ r.hk }}</td><td>{{ r.lf }}</td>
            </tr>
          {% endfor %}
          </tbody>
        </table>
      </div>
      {% endif %}
    </div>
  </div>

  <!-- ── RIGHT: controls + chart ── -->
  <div class="right">

    <!-- Oct-23 user ask: mirror the LEFT panel's expand icon on the
         RIGHT so a user can blow the chart up to the full viewport,
         then collapse back to the split view.  Same icon vocabulary
         (⛶ expand / ✕ collapse) as the left title bar. -->
    <div class="right-title">
      <span class="rt-text">Price comparison chart</span>
      <!-- Oct-27 user ask: side-by-side duplicate chart.  Clicking
           opens a second instance of this dashboard in an iframe to
           the right of the main panel, each half carrying its own
           independent filter / view state.  Closing either half
           returns to single-chart.  Done as an iframe (over refacto-
           ring the whole chart-panel into two cloned columns) so the
           duplicate's internal state is naturally isolated — no
           global clash between the two chart instances. -->
      <button id="split-chart-btn" class="expand-chart-btn"
              onclick="toggleChartSplit()" title="Open a second chart beside this one"
              aria-label="Split chart"
              style="font-size:11px;width:auto;padding:0 6px">&#x25A6;</button>
      <button id="expand-chart-btn" class="expand-chart-btn"
              onclick="toggleChartExpand()" title="Expand chart to full view"
              aria-label="Expand chart">&#x26F6;</button>
    </div>

    <div class="btn-panel">

      <!-- Row 1: Size filter (global)
           Oct-12 PM 2nd pass: the full-grid button bar swallowed the
           chart below.  Collapse to a native <select> dropdown so
           only one row of controls shows; options are grouped as
           "Reserved" (curated SIZE_CATEGORY sizes) then "Others"
           (every other size seen in the data) so popular sizes are
           always at the top of the open list.  "Reserved only"
           checkbox trims the dropdown to just the Reserved group. -->
      <div class="btn-row">
        <span class="row-label">Size</span>
        <select id="size-select"
                style="padding:4px 24px 4px 10px;border:1px solid #cbd5e1;border-radius:3px;background:#fff;font:inherit;font-size:12.5px;cursor:pointer;min-width:140px"></select>
        <label style="font-size:11px;color:#444;margin-left:10px;display:inline-flex;align-items:center;gap:4px;cursor:pointer;white-space:nowrap">
          <input type="checkbox" id="size-reserved-only" style="margin:0;cursor:pointer">
          Reserved only
        </label>
      </div>

      <hr class="divider">

      <!-- Row 2: View mode -->
      <div class="btn-row">
        <span class="row-label">View</span>
        <div class="btns">
          <button class="btn mode" id="btn-store" onclick="onModeStore(this)">By Channel</button>
          <button class="btn mode" id="btn-brand" onclick="onModeBrand(this)">By Brand</button>
        </div>
      </div>

      <!-- Row 3: Normal / Promo pricing toggle.  BJ and JAX publish
           both a regular PRICE and a DISC_PRICE in their CSVs; this
           bar switches which one the chart + tables read.  Tempe /
           TempeWOS have no promo concept so the toggle is a no-op
           for those stores (both buttons show the same line). -->
      <div class="btn-row">
        <span class="row-label">Price</span>
        <div class="btns">
          <button class="btn mode" id="btn-normal" onclick="onPricingNormal(this)">Normal</button>
          <button class="btn mode" id="btn-promo"  onclick="onPricingPromo(this)">Promo</button>
          <span id="pricing-hint"
                style="margin-left:10px;font-size:11px;color:#777;align-self:center">
            Promo = DISC_PRICE where available (BJ / JAX)
          </span>
        </div>
      </div>

      <!-- Row 4: Sub-buttons (brand list or store list) -->
      <div class="btn-row" id="sub-row">
        <span class="row-label" id="sub-label"></span>
        <div class="btns" id="sub-btns"></div>
      </div>

    </div>

    <!-- Chart -->
    <div class="chart-panel">
      <div class="chart-title" id="chart-title">Select a size and view mode above</div>
      <div class="chart-wrap" id="chart-wrap">
        <canvas id="main-chart"></canvas>
      </div>
    </div>

  </div>
</div>

<script>
const D       = {{ chart_json | safe }};
const BRANDS  = {{ brands_json | safe }};
const BCOLORS = {{ bcolors_json | safe }};
const SIZES   = {{ sizes_json | safe }};
const POPULAR = {{ popular_json | safe }};
const POPULAR_SET = new Set(POPULAR);
const months  = D.months;

const SC = { tempe:'#2196F3', bj:'#4CAF50', jax:'#FF9800', tw:'#9C27B0', twi:'#E040FB' };
const SL = { tempe:'Tempe',   bj:'Bob Jane', jax:'JAX',   tw:'TempeWOS Sell Out', twi:'TempeWOS Sell In' };
const SD = { tempe:[], bj:[5,5], jax:[2,3], tw:[8,3], twi:[3,3] };
const PALETTE = ['#C8102E','#E31837','#FFA500','#003087','#00539B',
                 '#E8001A','#FF6600','#E4002B','#003DA5','#555555'];

// Custom plugin: draw each brand's label at the Y-height of its LAST
// non-null data point (right-anchored, one label per line-end).  If two
// labels would overlap, nudge them apart with a minimum vertical gap so
// the text stays legible.  Replaces Chart.js's built-in legend for this
// dashboard.
const brandEndLabelsPlugin = {
    id: 'brandEndLabels',
    afterDatasetsDraw(chart) {
        const opts = chart.options.plugins && chart.options.plugins.brandEndLabels;
        if (!opts || opts.enabled === false) return;
        const yScale = chart.scales && chart.scales.y;
        const area   = chart.chartArea;
        if (!yScale || !area) return;
        const ctx = chart.ctx;

        // Gather anchor points: one per dataset that has any non-null value.
        const items = [];
        chart.data.datasets.forEach((ds, i) => {
            if (!ds || !Array.isArray(ds.data)) return;
            let lastIdx = -1;
            for (let k = ds.data.length - 1; k >= 0; k--) {
                if (ds.data[k] != null) { lastIdx = k; break; }
            }
            if (lastIdx < 0) return;
            const value = +ds.data[lastIdx];
            items.push({
                text:   ds.label || '',
                colour: ds.borderColor || ds.backgroundColor || '#555',
                yTarget: yScale.getPixelForValue(value),
            });
        });
        if (!items.length) return;

        // Sort top-to-bottom, then space overlapping labels apart.
        items.sort((a, b) => a.yTarget - b.yTarget);
        const MIN_GAP = 13;
        for (let i = 1; i < items.length; i++) {
            if (items[i].yTarget - items[i-1].yTarget < MIN_GAP) {
                items[i].yTarget = items[i-1].yTarget + MIN_GAP;
            }
        }
        // If the pushdown overflowed the chart bottom, walk back up.
        if (items[items.length-1].yTarget > area.bottom - 4) {
            items[items.length-1].yTarget = area.bottom - 4;
            for (let i = items.length - 2; i >= 0; i--) {
                if (items[i+1].yTarget - items[i].yTarget < MIN_GAP) {
                    items[i].yTarget = items[i+1].yTarget - MIN_GAP;
                }
            }
        }

        // Draw each label to the right of the chart area.
        ctx.save();
        ctx.font         = '10px system-ui, sans-serif';
        ctx.textAlign    = 'left';
        ctx.textBaseline = 'middle';
        const boxX = area.right + 6;
        const boxW = 9, boxH = 9;
        const textX = boxX + boxW + 4;

        items.forEach(it => {
            ctx.fillStyle = it.colour;
            ctx.fillRect(boxX, it.yTarget - boxH/2, boxW, boxH);
            ctx.fillStyle = '#111';
            ctx.fillText(it.text, textX, it.yTarget);
        });
        ctx.restore();
    }
};
if (window.Chart && window.Chart.register) {
    try { window.Chart.register(brandEndLabelsPlugin); } catch (_) {}
}

const baseOpts = {
    responsive:true, maintainAspectRatio:false,
    clip: false,
    // Reserve horizontal space on the right for the end-labels so the
    // last data point isn't overlapped by the label text.  Widened
    // from 120 → 240 after the legend labels started carrying the
    // pattern name ("MC Michelin — Primacy 5" needs ~40 chars).
    layout: { padding: { right: 240 } },
    plugins:{
        legend:  { display: false },   // replaced by brandEndLabels plugin
        brandEndLabels: { enabled: true },
        /* chartjs-plugin-zoom: drag a vertical rectangle on the chart
           to zoom into that Y-axis range (per Oct-12 user ask #2 —
           prices clustered close together).  Mode 'y' means the drag
           box selection narrows the Y scale only; the X (month) scale
           stays the full timeline.  The reset-via-drag-upper-left
           gesture is wired below in _attachZoomReset() because the
           plugin itself only exposes resetZoom() programmatically. */
        zoom: {
            zoom: {
                drag: {
                    enabled: true,
                    backgroundColor: 'rgba(100,149,237,0.15)',
                    borderColor: 'rgba(100,149,237,0.7)',
                    borderWidth: 1,
                    threshold: 10,
                },
                mode: 'y',
            },
        },
        tooltip: {
            /* Full descriptions + a callout line whenever the point was
               picked via the COMPETITOR_PATTERNS fallback (Tempe had no
               reference) or when the picked product changed vs the
               previous month.  Datasets carry these as `_descs` and
               `_flags` arrays parallel to `data`. */
            callbacks: {
                label(ctx) {
                    return ' $' + (ctx.raw ?? '—');
                },
                afterLabel(ctx) {
                    const ds = ctx.dataset;
                    const i  = ctx.dataIndex;
                    const parts = [];
                    if (ds._descs && ds._descs[i]) {
                        parts.push('— ' + ds._descs[i]);
                    }
                    const f = ds._flags && ds._flags[i] ? ds._flags[i] : '';
                    if (f.includes('chg')) {
                        parts.push('⚠ Product changed vs prev month');
                    }
                    if (f.includes('cat_fb')) {
                        parts.push('⚠ Tempe reference missing — pattern fallback');
                    }
                    return parts;
                }
            }
        }
    },
    scales:{
        x:{ offset:true, ticks:{ font:{size:10}, padding:8 } },
        y:{
            beginAtZero: false,
            ticks:{ font:{size:10}, callback: v => '$'+v },
            grid:{ color:'#f0f0f0' },
        }
    },
    elements:{
        point:{
            // Small dot painted in the line colour, no white ring.
            // Per-dataset pointRadius / pointBackgroundColor arrays
            // (built by _pointStyles) override these defaults per
            // point when there's an anomaly (chg / cat_fb).
            radius: 2,
            hoverRadius: 4,
            borderWidth: 0,
            hitRadius: 10,
        }
    }
};

/* Compute Y min/max from datasets before chart creation */
function _yRange(datasets) {
    const vals = datasets.flatMap(ds => ds.data).filter(v => v !== null && v !== undefined);
    if (!vals.length) return {};
    const lo = Math.min(...vals), hi = Math.max(...vals);
    const pad = Math.max((hi - lo) * 0.2, 20);   // 20% padding, min $20
    return { min: Math.floor(lo - pad), max: Math.ceil(hi + pad) };
}

/* Draw price labels above each point after chart renders */
const _labelPlugin = {
    id: 'pointLabels',
    afterDatasetsDraw(chart) {
        const ctx = chart.ctx;
        ctx.save();
        ctx.font = 'bold 10px system-ui, sans-serif';
        ctx.textAlign = 'center';
        chart.data.datasets.forEach((ds, di) => {
            const meta = chart.getDatasetMeta(di);
            meta.data.forEach((pt, i) => {
                const val = ds.data[i];
                if (val === null || val === undefined) return;
                const color = Array.isArray(ds.borderColor) ? ds.borderColor[i] : ds.borderColor;
                ctx.fillStyle = color || '#333';
                ctx.fillText('$' + val, pt.x, pt.y - 8);
            });
        });
        ctx.restore();
    }
};
Chart.register(_labelPlugin);


/* ── Table below chart ───────────────────────────────── */
function _renderTable() {
    const wrap = document.getElementById('chart-table-wrap');
    if (!wrap || !selectedMode || !selectedSubKey) return;
    if (selectedMode === 'store') _renderStoreTable(wrap);
    else                          _renderBrandTable(wrap);
}

function _renderStoreTable(wrap) {
    const abbr = selectedSubKey;
    const stores = [
        { key:'tempe', label:'Tempe',              c: SC.tempe },
        { key:'bj',    label:'Bob Jane',           c: SC.bj   },
        { key:'jax',   label:'JAX',                c: SC.jax  },
        { key:'tw',    label:'TempeWOS Sell Out',   c: SC.tw   },
        { key:'twi',   label:'TempeWOS Sell In',    c: SC.twi  },
    ];
    const storePrices = stores.map(function(s) {
        if (abbr === 'ALL') return _avgAllBrands(selectedSize, s.key);
        if (selectedSize === 'ALL') return _avgAllSizes(abbr, s.key);
        const bsd = (D.brand_size_data[abbr] || {})[selectedSize];
        return bsd ? (bsd[_priceKey(s.key)] || []) : [];
    });
    // Oct-27: parallel stocks array per store so a cell with stock=0
    // can render its price wrapped in parentheses.  Averaged views
    // ("ALL brands" / "ALL sizes") can't carry a single stock value
    // per cell so we leave those as nulls — the parenthesising only
    // fires for the drilled-down (brand, size) view where the row
    // actually represents one real product.
    const storeStocks = stores.map(function(s) {
        if (abbr === 'ALL' || selectedSize === 'ALL') return [];
        return _stocksFor(abbr, selectedSize, s.key);
    });

    // ── Price table ──
    let html = '<table class="ctable"><thead><tr><th class="th-month">Store</th>';
    months.forEach(function(m) { html += '<th class="th-month">' + m + '</th>'; });
    html += '</tr></thead><tbody>';
    stores.forEach(function(s, si) {
        const hasData = storePrices[si].some(function(p){ return p !== null && p !== undefined; });
        if (!hasData) return;
        html += '<tr><td style="color:' + s.c + ';font-weight:700">' + s.label + '</td>';
        const stks = storeStocks[si] || [];
        storePrices[si].forEach(function(p, i) {
            if (p === null || p === undefined) {
                html += '<td class="r">—</td>';
                return;
            }
            const isOos = (stks[i] === 0);
            // Parentheses signal "listed but out of stock" — grey the
            // cell too so the eye catches the OOS state before
            // reading the figure.
            const cell = isOos
                ? '<span style="color:#777">($' + p + ')</span>'
                : '$' + p;
            html += '<td class="r">' + cell + '</td>';
        });
        html += '</tr>';
    });
    html += '</tbody></table>';

    // ── JAX Index table (JAX = 100) ──
    const jaxIdx = stores.findIndex(function(s) { return s.key === 'jax'; });
    const jaxPrices = jaxIdx >= 0 ? storePrices[jaxIdx] : [];
    html += '<div class="idx-title">JAX Index &nbsp;(JAX = 100)</div>';
    html += '<table class="ctable idxtbl"><thead><tr><th class="th-month">Store</th>';
    months.forEach(function(m) { html += '<th class="th-month">' + m + '</th>'; });
    html += '</tr></thead><tbody>';
    stores.forEach(function(s, si) {
        const hasData = storePrices[si].some(function(p){ return p !== null && p !== undefined; });
        if (!hasData) return;
        html += '<tr><td style="color:' + s.c + ';font-weight:700">' + s.label + '</td>';
        storePrices[si].forEach(function(p, i) {
            const base = jaxPrices[i];
            let cell;
            if (base == null) {
                cell = '—';                          // JAX blank → all blank
            } else if (p == null) {
                cell = '—';                          // store blank → blank
            } else {
                const idx = Math.round(p / base * 100);
                const clr = idx < 98 ? '#388E3C' : (idx > 102 ? '#C62828' : '#37474F');
                cell = '<b style="color:' + clr + '">' + idx + '</b>';
            }
            html += '<td class="r">' + cell + '</td>';
        });
        html += '</tr>';
    });
    html += '</tbody></table>';
    wrap.innerHTML = html;
}

function _renderBrandTable(wrap) {
    const store = selectedSubKey;

    // Build one entry per (brand, pattern) — same primary + siblings
    // logic the chart uses, so the tables and the chart stay in sync.
    // `isExtra` marks sibling rows with a subtler style so the primary
    // brand entries remain easy to scan at a glance.
    const entries = [];
    Object.keys(BRANDS).forEach(function(abbr) {
        const primaryPrices = selectedSize === 'ALL'
            ? _avgAllSizes(abbr, store)
            : ((D.size_data[selectedSize] || {})[abbr] || {})[_priceKey(store)];
        if (!primaryPrices || !primaryPrices.some(p => p != null)) return;

        const descs   = _descsFor(abbr, selectedSize, store);
        const flags   = _flagsFor(abbr, selectedSize, store);
        const primaryPattern = _seriesPattern(abbr, selectedSize, store)
                            || _guessPatternFromDescs(descs);
        entries.push({
            abbr:      abbr,
            pattern:   primaryPattern,
            prices:    primaryPrices,
            flags:     flags,
            isExtra:   false,
        });

        // Sibling series get their own rows too — filtered by the same
        // price band used on the chart so segment-different products
        // stay out of the table just like they stay off the chart.
        if (selectedSize !== 'ALL') {
            const extras = _extraSeries(abbr, selectedSize, store,
                                        primaryPattern, primaryPrices);
            extras.forEach(function(series) {
                entries.push({
                    abbr:    abbr,
                    pattern: series.pattern,
                    prices:  series.prices,
                    flags:   series.flags || series.prices.map(() => ''),
                    isExtra: true,
                });
            });
        }
    });

    function _label(e) {
        const brand = e.abbr + ' ' + (BRANDS[e.abbr] || '');
        return e.pattern ? (brand + ' — ' + e.pattern) : brand;
    }
    function _cellClass(flag) {
        if (!flag) return 'r';
        return /chg|cat_fb/.test(flag) ? 'r anom' : 'r';
    }

    // ── Price table ──
    let html = '<table class="ctable"><thead><tr><th class="th-month">Brand</th>';
    months.forEach(function(m) { html += '<th class="th-month">' + m + '</th>'; });
    html += '</tr></thead><tbody>';
    entries.forEach(function(e) {
        const c = '#' + (BCOLORS[e.abbr] || '555555');
        const rowStyle = e.isExtra
            ? 'color:' + c + ';font-weight:500;padding-left:14px;font-size:.92em'
            : 'color:' + c + ';font-weight:700';
        html += '<tr' + (e.isExtra ? ' class="extra"' : '') + '>'
              + '<td style="' + rowStyle + '">' + _label(e) + '</td>';
        e.prices.forEach(function(p, i) {
            html += '<td class="' + _cellClass(e.flags[i]) + '">'
                  + (p !== null && p !== undefined ? '$' + p : '—') + '</td>';
        });
        html += '</tr>';
    });
    html += '</tbody></table>';

    // ── HK Index table (HK = 100) ──
    // Baseline: the primary Hankook row.  HK's own sibling patterns
    // (e.g. Kinergy GT) are indexed against Kinergy Eco 2 so users can
    // see how HK's alternative series compares against HK's canonical.
    const hkEntry = entries.find(function(e) { return e.abbr === 'HK' && !e.isExtra; });
    const hkPrices = hkEntry ? hkEntry.prices : [];
    html += '<div class="idx-title">HK Index &nbsp;(Hankook = 100)</div>';
    html += '<table class="ctable idxtbl"><thead><tr><th class="th-month">Brand</th>';
    months.forEach(function(m) { html += '<th class="th-month">' + m + '</th>'; });
    html += '</tr></thead><tbody>';
    entries.forEach(function(e) {
        const c = '#' + (BCOLORS[e.abbr] || '555555');
        const rowStyle = e.isExtra
            ? 'color:' + c + ';font-weight:500;padding-left:14px;font-size:.92em'
            : 'color:' + c + ';font-weight:700';
        html += '<tr' + (e.isExtra ? ' class="extra"' : '') + '>'
              + '<td style="' + rowStyle + '">' + _label(e) + '</td>';
        e.prices.forEach(function(p, i) {
            const base = hkPrices[i];
            let cell;
            if (base == null) {
                cell = '—';
            } else if (p == null) {
                cell = '—';
            } else {
                const idx = Math.round(p / base * 100);
                const clr = idx < 98 ? '#388E3C' : (idx > 102 ? '#C62828' : '#37474F');
                cell = '<b style="color:' + clr + '">' + idx + '</b>';
            }
            html += '<td class="r">' + cell + '</td>';
        });
        html += '</tr>';
    });
    html += '</tbody></table>';
    wrap.innerHTML = html;
}

/* ── state ────────────────────────────────────────────── */
let selectedSize    = null;
let selectedMode    = null;   // 'store' | 'brand'
let selectedSubKey  = null;   // abbr (store mode) | 'tempe'/'bj'/'jax' (brand mode)
let pricingMode     = 'normal';   // 'normal' | 'promo'  (toggle at top)
let _chart          = null;
let _activeSizeBtn  = null;
let _activeModeBtn  = null;
let _activeSubBtn   = null;
let _activePricingBtn = null;

/* Which backend key holds the price array for a store under the
   current pricingMode.  Only BJ and JAX have a separate promo array;
   Tempe / TempeWOS fall through so the toggle is a no-op for them. */
function _priceKey(store) {
    if (pricingMode === 'promo' && (store === 'bj' || store === 'jax')) {
        return store + '_promo';
    }
    return store;
}
/* Mirror helper for a per-pattern series dict entry — "prices" vs
   "promos".  Both arrays are populated by _accumulate_series and
   fall back to the normal price when the row carries no DISC_PRICE,
   so there's no missing-data gap when the toggle is on. */
function _seriesKey(store) {
    if (pricingMode === 'promo' && (store === 'bj' || store === 'jax')) {
        return 'promos';
    }
    return 'prices';
}

/* ── chart helpers ────────────────────────────────────── */
function _render(title, datasets) {
    document.getElementById('chart-title').textContent = title;
    if (_chart) { _chart.destroy(); _chart = null; }

    const yrSelf = _yRange(datasets);
    /* Oct-27 split-view: merge in the sibling pane's current Y range
       so both charts show the same Y window.  Both panes do the same
       merge so the result is deterministic — whichever pane renders
       last applies the union of both ranges.  Also broadcasts our
       own range before the merge so the sibling can match. */
    _publishYRange(yrSelf.min, yrSelf.max);
    const yr = _mergedYRange(yrSelf);
    const opts = JSON.parse(JSON.stringify(baseOpts));  // deep-clone so baseOpts is not mutated
    if (yr.min !== undefined) {
        opts.scales.y.min = yr.min;
        opts.scales.y.max = yr.max;
    }

    _chart = new Chart(document.getElementById('main-chart'), {
        type: 'line', data: { labels: months, datasets }, options: opts
    });
    _attachZoomReset(_chart);
    _syncZoomResetChart(_chart);
}

/* Zoom-reset gesture (Oct-12 user ask #2): mouse-down at any point
   then drag toward the upper-left ≥ 60 px → chart.resetZoom().
   chartjs-plugin-zoom owns the normal drag-to-zoom gesture — its
   pointerup handler runs ALONGSIDE ours, so an upper-left drag
   would still try to zoom to its (inverted) box.  Two tricks make
   this reliable:
     • capture:true on pointerdown so we record the start position
       even if the plugin tries to swallow the event; and
     • setTimeout(…, 0) around chart.resetZoom() so our reset runs
       AFTER the plugin's synchronous zoom this tick — the user
       sees the chart snap back to the unzoomed state. */
function _attachZoomReset(chart) {
    const canvas = chart.canvas;
    if (!canvas || canvas._zoomResetWired) return;
    canvas._zoomResetWired = true;
    canvas._zoomResetChart = chart;
    let _sx = 0, _sy = 0, _down = false;
    canvas.addEventListener('pointerdown', function(ev) {
        _down = true; _sx = ev.clientX; _sy = ev.clientY;
    }, {capture: true});
    canvas.addEventListener('pointerup', function(ev) {
        if (!_down) return;
        _down = false;
        const dx = ev.clientX - _sx;
        const dy = ev.clientY - _sy;
        // Upper-left means dx < 0 AND dy < 0; require a reasonable
        // minimum travel so stray clicks don't reset accidentally.
        if (dx < -60 && dy < -60) {
            // Defer so the plugin's own zoom runs first this tick,
            // then our resetZoom() wipes it on the next.  Use the
            // CURRENT chart stored on the canvas (chart objects get
            // replaced on every _render() but the canvas persists).
            setTimeout(function() {
                try {
                    const c = canvas._zoomResetChart;
                    if (c && c.resetZoom) c.resetZoom();
                } catch (_) {}
            }, 0);
        }
    }, {capture: true});
    canvas.addEventListener('pointercancel', function() { _down = false; }, {capture: true});
}

/* Keep the pointer-handler's chart reference in sync with the live
   chart (handler is attached once, chart is replaced on every
   _render()).  Called from _render() after new Chart() returns. */
function _syncZoomResetChart(chart) {
    if (chart && chart.canvas) chart.canvas._zoomResetChart = chart;
}

function _noData(msg) {
    document.getElementById('chart-title').textContent = msg;
    if (_chart) { _chart.destroy(); _chart = null; }
    const wrap = document.getElementById('chart-table-wrap');
    if (wrap) wrap.innerHTML = '';
}

/* ── Per-point description / flag lookup ──────────────────
   Each price array in size_data has parallel "<store>_d" (descriptions)
   and "<store>_f" (flags) arrays.  These helpers fetch them safely and
   return arrays whose length matches `months` (so the tooltip and
   point-styling callbacks can index into them without bounds checks).
   ---------------------------------------------------------------- */
function _descsFor(abbr, size, store) {
    if (!store) return [];
    // Averaged views ("ALL sizes" / "ALL brands") don't map to a single
    // real row, so we return blanks — the tooltip just shows the price.
    if (abbr === 'ALL' || size === 'ALL') return months.map(() => '');
    const bsd = (D.brand_size_data[abbr] || {})[size];
    if (!bsd) return months.map(() => '');
    // All five stores now expose <store>_d (tempe / bj / jax / tw / twi)
    // so the Legend's _guessPatternFromDescs fallback and the tooltip
    // both have real product descriptions to show.  Oct-26 fix: before
    // this, tw/twi returned a blank-month array here, which is why the
    // Legend on the TempeWOS views read just "KH Kumho" with no pattern.
    const key = store + '_d';
    return bsd[key] || months.map(() => '');
}
function _flagsFor(abbr, size, store) {
    if (!store) return [];
    if (abbr === 'ALL' || size === 'ALL') return months.map(() => '');
    const bsd = (D.brand_size_data[abbr] || {})[size];
    if (!bsd) return months.map(() => '');
    const key = store + '_f';
    return bsd[key] || months.map(() => '');
}
/* Oct-27: per-month stock arrays — only populated for stores whose
   CSV carries a STOCK column (tempe retail, tw / twi TempeWebOrder).
   Returns an all-null array otherwise so callers don't need to
   handle "no data" separately. */
function _stocksFor(abbr, size, store) {
    if (!store) return months.map(() => null);
    if (abbr === 'ALL' || size === 'ALL') return months.map(() => null);
    const bsd = (D.brand_size_data[abbr] || {})[size];
    if (!bsd) return months.map(() => null);
    const key = store + '_stk';
    return bsd[key] || months.map(() => null);
}
/* Per-point colour + radius arrays for Chart.js.  Normal points
   are small dots painted in the line's own colour (no white ring
   any more — the ring made the dots look larger than they were and
   almost lost their identity against the line).  Anomalous points
   (product changed vs previous month, or COMPETITOR_PATTERNS
   fallback fired) are slightly larger and painted red so they
   stand out without dominating the chart.

   Oct-27: when the row for that month carried stock = 0 (listed-
   but-OOS), render the point as a HOLLOW dot — line colour on the
   border, white fill.  Keeps the line connected and the price
   visible while signalling "price exists but item isn't actually
   buyable right now".  Takes precedence over the "normal" style
   but not the anomaly red (if a product also changed, the red
   anomaly dot still wins so the reader doesn't miss the pattern
   switch). */
function _pointStyles(flags, prices, lineColour, stocks) {
    const border = [], bg = [], radius = [];
    const _st = stocks || [];
    for (let i = 0; i < prices.length; i++) {
        const f = flags[i] || '';
        const anom = f.includes('chg') || f.includes('cat_fb');
        const oos  = (_st[i] === 0);
        if (anom) {
            border.push('#C62828');
            bg.push('#E53935');
            radius.push(3);
        } else if (oos) {
            // Hollow: line colour on border, white fill, same size as
            // normal so the dot stays legible on the line.
            border.push(lineColour);
            bg.push('#ffffff');
            radius.push(3);
        } else {
            border.push(lineColour);
            bg.push(lineColour);
            radius.push(2);
        }
    }
    return { border, bg, radius };
}

/* ── ALL sizes: average across every size ──────────────── */
function _avgAllSizes(abbr, store) {
    const nM = months.length;
    const sums = new Array(nM).fill(0), counts = new Array(nM).fill(0);
    const storeKey = _priceKey(store);  // switches to <store>_promo in Promo mode
    Object.values(D.size_data).forEach(function(sizeAbbrs) {
        const prices = ((sizeAbbrs[abbr] || {})[storeKey]) || [];
        prices.forEach(function(p, i) {
            if (p !== null && p !== undefined) { sums[i] += p; counts[i]++; }
        });
    });
    return sums.map(function(s, i) { return counts[i] > 0 ? Math.round(s / counts[i]) : null; });
}

/* average across every brand for a store */
function _avgAllBrands(size, store) {
    // Note: D.store_avg is Normal-only — we fall back to it only for
    // the "ALL sizes" aggregate.  Promo mode on an "ALL sizes" view
    // would need its own aggregate; keeping Normal here avoids a
    // misleading number.  Users can still see the per-size Promo
    // numbers by picking a specific size.
    if (size === 'ALL') return D.store_avg[store] || [];
    const nM = months.length;
    const sums = new Array(nM).fill(0), counts = new Array(nM).fill(0);
    const storeKey = _priceKey(store);
    Object.keys(BRANDS).forEach(function(abbr) {
        const prices = ((D.size_data[size] || {})[abbr] || {})[storeKey] || [];
        prices.forEach(function(p, i) {
            if (p !== null && p !== undefined) { sums[i] += p; counts[i]++; }
        });
    });
    return sums.map(function(s, i) { return counts[i] > 0 ? Math.round(s / counts[i]) : null; });
}

function _tryRender() {
    if (!selectedSize || !selectedMode || !selectedSubKey) return;
    if (selectedMode === 'store') _renderStore();
    else                          _renderBrand();
}

/* ── By Store: selected brand → 3 store lines ───────── */
function _renderStore() {
    const abbr = selectedSubKey;
    const sizeLabel  = selectedSize === 'ALL' ? 'All Sizes (avg)' : selectedSize;
    const brandLabel = abbr === 'ALL' ? 'All Brands (avg)' : (BRANDS[abbr] || abbr);
    const ds = ['tempe','bj','jax','tw','twi'].reduce((arr, s) => {
        let prices;
        if (abbr === 'ALL') {
            prices = _avgAllBrands(selectedSize, s);
        } else if (selectedSize === 'ALL') {
            prices = _avgAllSizes(abbr, s);
        } else {
            const bsd = (D.brand_size_data[abbr] || {})[selectedSize];
            prices = bsd ? bsd[_priceKey(s)] : null;
        }
        if (prices && !prices.every(p => p === null)) {
            const descs = _descsFor(abbr, selectedSize, s);
            const flags = _flagsFor(abbr, selectedSize, s);
            const stks  = _stocksFor(abbr, selectedSize, s);
            const st    = _pointStyles(flags, prices, SC[s], stks);
            arr.push({ label:SL[s], data:prices, borderColor:SC[s],
                       backgroundColor:SC[s]+'33', borderDash:SD[s],
                       spanGaps:true, tension:.3, fill:false,
                       pointRadius: st.radius,
                       pointHoverRadius: st.radius.map(r => r + 2),
                       pointBackgroundColor: st.bg,
                       pointBorderColor: st.border,
                       // Oct-27: width 1.5 so hollow-dot OOS points
                       // actually render as a ring instead of
                       // disappearing (0-width hid the border).
                       pointBorderWidth: 1.5,
                       _descs: descs, _flags: flags, _stocks: stks });
        }
        return arr;
    }, []);
    if (!ds.length) { _noData(brandLabel + ' — ' + sizeLabel + ': no data'); return; }
    _render((BRANDS[abbr]||abbr) + ' \u2014 ' + sizeLabel
            + ' \u2014 Tempe vs Bob Jane vs JAX', ds);
    _renderTable();
}

/* ── By Brand: selected store → brand lines ─────────── */
function _renderBrand() {
    const store = selectedSubKey;
    const sizeLabel = selectedSize === 'ALL' ? 'All Sizes (avg)' : selectedSize;
    const ds = [];
    let ci = 0;
    Object.keys(BRANDS).forEach(function(abbr) {
        const prices = selectedSize === 'ALL'
            ? _avgAllSizes(abbr, store)
            : ((D.size_data[selectedSize] || {})[abbr] || {})[_priceKey(store)];
        if (!prices || prices.every(p => p === null)) return;
        const c = '#' + (BCOLORS[abbr] || PALETTE[ci%PALETTE.length].slice(1));
        const solid = (abbr === 'HK' || abbr === 'LF');
        ci++;
        const descs = _descsFor(abbr, selectedSize, store);
        const flags = _flagsFor(abbr, selectedSize, store);
        const stks  = _stocksFor(abbr, selectedSize, store);
        const st    = _pointStyles(flags, prices, c, stks);
        // Point size is small + uniform (2px normal, 3px anomaly) so
        // the dot doesn't dominate over the line.  The old code varied
        // radius per brand which produced inconsistent-sized markers
        // even for non-anomalous points.
        const finalR = st.radius;
        // Derive the primary line's pattern label so the legend reads
        // "HK Hankook — Kinergy Eco 2" instead of just "HK Hankook".
        const primaryPattern = _seriesPattern(abbr, selectedSize, store)
                            || _guessPatternFromDescs(descs);
        const primaryLabel = abbr + ' ' + (BRANDS[abbr]||'')
                           + (primaryPattern ? ' — ' + primaryPattern : '');
        ds.push({ label: primaryLabel, data: prices,
                  borderColor:c, backgroundColor:c+'33',
                  borderDash: solid ? [] : [6,4],
                  borderWidth: solid ? 3 : 1.5,
                  pointRadius: finalR,
                  pointHoverRadius: finalR.map(r => r + 2),
                  pointBackgroundColor: st.bg,
                  pointBorderColor: st.border,
                  pointBorderWidth: 1.5,
                  spanGaps:true, tension:.3, fill:false,
                  _descs: descs, _flags: flags, _stocks: stks });

        // Extra dashed lines for sibling patterns (Kinergy Eco 2 vs
        // Kinergy GT etc.) filtered to prices in a reasonable band
        // around the primary line — patterns whose typical price is
        // way above the primary (e.g. Michelin Lotus Pilot Sport Cup
        // 2 vs Primacy) stay hidden because the user asked for
        // extras only when prices don't differ too much.
        if (selectedSize !== 'ALL') {
            const extras = _extraSeries(abbr, selectedSize, store,
                                        primaryPattern, prices);
            extras.forEach(function(series, si) {
                const label = abbr + ' ' + (BRANDS[abbr]||'')
                            + ' — ' + series.pattern;
                // Dash pattern rotates so 3+ siblings stay distinct.
                const dashPatterns = [[2,3], [10,4], [4,2,1,2], [6,6,2,6]];
                const dash = dashPatterns[si % dashPatterns.length];
                const sflags = series.flags || series.descs.map(() => '');
                const sstocks = series.stocks || series.prices.map(() => null);
                const sst    = _pointStyles(sflags, series.prices, c, sstocks);
                ds.push({ label: label, data: series.prices,
                          borderColor: c, backgroundColor: c+'22',
                          borderDash: dash, borderWidth: 1.2,
                          pointRadius: sst.radius,
                          pointHoverRadius: sst.radius.map(r => r + 2),
                          pointBackgroundColor: sst.bg,
                          pointBorderColor: sst.border,
                          pointBorderWidth: 1.5,
                          spanGaps:true, tension:.3, fill:false,
                          _descs: series.descs, _flags: sflags });
            });
        }
    });
    if (!ds.length) { _noData(SL[store] + ' — ' + sizeLabel + ': no data'); return; }
    _render(SL[store] + ' \u2014 ' + sizeLabel + ' \u2014 Brand Comparison', ds);
    _renderTable();
}

/* \u2500\u2500 Series helpers (per-pattern lines on By-Brand chart) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
   Each (size, brand, store) can carry a `<store>_series` dict:
     { pattern_label: { prices:[...], descs:[...] } }
   The primary line uses whichever pattern's latest-month price is
   lowest (matches best_jax / best_bj), and extra dashed lines are
   added for sibling patterns whose median price is within a
   reasonable band around the primary \u2014 so competing series in the
   same segment show up side-by-side but genuinely off-segment
   products (Michelin Lotus Pilot Sport Cup 2 vs Primacy) stay
   hidden. */
function _seriesDict(abbr, size, store) {
    if (!size || size === 'ALL' || abbr === 'ALL') return null;
    const bsd = (D.brand_size_data[abbr] || {})[size];
    if (!bsd) return null;
    return bsd[store + '_series'] || null;
}
function _seriesPattern(abbr, size, store) {
    const sd = _seriesDict(abbr, size, store);
    if (!sd) return '';
    // Pick the cheapest-latest pattern under whichever price array the
    // current toggle wants.  Keeps the legend's primary label in sync
    // with the primary line's actual data points.
    const key = _seriesKey(store);
    let bestPat = '', bestPrice = Infinity;
    Object.keys(sd).forEach(function(pk) {
        const prices = sd[pk][key] || sd[pk].prices || [];
        for (let i = prices.length - 1; i >= 0; i--) {
            const p = prices[i];
            if (p !== null && p !== undefined) {
                if (p < bestPrice) { bestPat = pk; bestPrice = p; }
                break;
            }
        }
    });
    return bestPat;
}
function _guessPatternFromDescs(descs) {
    if (!descs || !descs.length) return '';
    for (let i = descs.length - 1; i >= 0; i--) {
        const d = descs[i];
        if (!d) continue;
        const toks = d.split(/\s+/).filter(function(t) {
            if (!t || t.length <= 1) return false;
            if (/^\d/.test(t)) return false;
            if (/^(XL|OWT|RWL|OBL|TL|TT|FR|MFS|DOT|BMW|AUDI|BENZ|DEMO|EV|RC)$/i.test(t)) return false;
            return true;
        }).slice(0, 2);
        if (toks.length) return toks.join(' ');
    }
    return '';
}
function _extraSeries(abbr, size, store, primaryPattern, primaryPrices) {
    const sd = _seriesDict(abbr, size, store);
    if (!sd) return [];
    const PRICE_BAND_HI = 1.6;   /* <= 60 % above primary median */
    const PRICE_BAND_LO = 0.55;  /* >= 45 % below primary median */
    const primaryMed = _median(primaryPrices);
    if (primaryMed == null) return [];
    // When the toggle is on Promo, read promos out of each series
    // entry — otherwise read prices.  Both arrays are populated for
    // BJ / JAX; non-promo stores only have prices and we fall back.
    const key = _seriesKey(store);
    const extras = [];
    Object.keys(sd).forEach(function(pk) {
        if (pk === primaryPattern) return;
        const series = sd[pk];
        const prices = series[key] || series.prices || [];
        if (prices.every(p => p == null)) return;
        const med = _median(prices);
        if (med == null) return;
        if (med > primaryMed * PRICE_BAND_HI) return;
        if (med < primaryMed * PRICE_BAND_LO) return;
        extras.push({
            pattern: pk,
            prices:  prices,
            descs:   series.descs || prices.map(() => ''),
            flags:   series.flags || prices.map(() => ''),
            median:  med,
        });
    });
    extras.sort((a, b) => a.median - b.median);
    /* Cap at 4 extras per brand so a busy size doesn't drown in dashes. */
    return extras.slice(0, 4);
}
function _median(arr) {
    const vals = (arr || []).filter(p => p != null).sort((a, b) => a - b);
    if (!vals.length) return null;
    const mid = Math.floor(vals.length / 2);
    return vals.length % 2 ? vals[mid] : (vals[mid-1] + vals[mid]) / 2;
}

/* ── Expand / collapse the LEFT tables ─────────────────
   Overlays the entire viewport with the tables panel so the
   right-most month columns (‘26.8 / ‘26.9) that get clipped at
   40 % left-panel width become fully readable.  Icon swaps to an
   ✕ while expanded so the click target's meaning is obvious. */
/* Oct-27 "hide tables" toggle.  The LEFT panel collapses entirely
   so the chart panes get the full viewport; a "Show tables" chip
   (CSS-pinned top-left) brings it back on next click.  Also fires
   Chart.js resize nudge so Chart.js re-measures the (now wider)
   canvas without waiting for a window resize event. */
function toggleTablesHidden() {
    const body = document.body;
    const on = body.classList.toggle('left-hidden');
    if (on && body.classList.contains('tables-expanded')) {
        body.classList.remove('tables-expanded');
    }
    requestAnimationFrame(() => {
        try { if (_chart && typeof _chart.resize === 'function') _chart.resize(); }
        catch (_e) {}
    });
}

function toggleTablesExpand() {
    const on = document.body.classList.toggle('tables-expanded');
    const btn = document.getElementById('expand-tables-btn');
    if (btn) {
        btn.innerHTML = on ? '✕' : '⛶';   // ✕ or ⛶
        btn.title     = on ? 'Return to split view' : 'Expand tables to full view';
    }
    /* When collapsing back, scroll the left panel back to the top so
       the user isn't stranded mid-table. */
    if (!on) {
        const sc = document.querySelector('.left-scroll');
        if (sc) sc.scrollTop = 0;
    }
}
/* Escape key exits the expanded view — muscle memory from most
   overlay UIs. */
document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape' && document.body.classList.contains('tables-expanded')) {
        toggleTablesExpand();
    }
    if (e.key === 'Escape' && document.body.classList.contains('chart-expanded')) {
        toggleChartExpand();
    }
});

/* Oct-23 user ask: expand / collapse the RIGHT chart panel.  Same
   overlay mechanic as toggleTablesExpand, plus a mutual-exclusion:
   if the LEFT tables are currently expanded, collapse THAT first so
   the two overlays never fight for the viewport.  After the layout
   shifts we also trigger a Plotly relayout so the SVG re-fits the
   larger area without a browser resize. */
/* ── Chart split-view ─────────────────────────────────────
   Open / close a sibling .chart-split-pane that holds an iframe of
   this same page.  The main chart narrows to 50 %; the LEFT tables
   panel narrows to 25 % to free room; the duplicate takes the
   remaining 50 %.  Each half carries its own state because they
   live in separate browsing contexts, so a filter change on one
   doesn't ripple to the other.  Closing either half removes the
   iframe and restores the 40 / 60 default. */
function toggleChartSplit() {
    const body = document.body;
    const btn  = document.getElementById('split-chart-btn');
    const existing = document.getElementById('chart-split-pane');
    if (existing) {
        existing.remove();
        body.classList.remove('chart-split');
        if (btn) btn.innerHTML = '▦';
        return;
    }
    // Append the pane as a sibling of .right so both share the
    // .body flex container.  URL has ?split=1 so the duplicate can
    // hide its OWN split button (no infinite nesting).
    const bodyEl = document.querySelector('.body');
    if (!bodyEl) return;
    const pane = document.createElement('div');
    pane.id = 'chart-split-pane';
    pane.className = 'chart-split-pane';
    pane.innerHTML =
        '<div class="right-title">'
        + '<span class="rt-text">Second chart (independent filters)</span>'
        + '<button class="chart-split-close" onclick="toggleChartSplit()"'
        + ' title="Close second chart" aria-label="Close">✕</button>'
        + '</div>'
        + '<iframe src="' + location.pathname + '?split=1"'
        + ' title="Second chart"></iframe>';
    bodyEl.appendChild(pane);
    body.classList.add('chart-split');
    if (btn) btn.innerHTML = '✕';
}

/* The iframe loads this same page with ?split=1 appended.  Strip
   every piece of chrome the duplicate doesn't need so its chart
   area matches the main pane's physical height:
     • Top .hdr (title + nav) — same links the parent already shows.
     • .left comparison tables — single-table only.
     • The duplicate's own split + expand buttons — no recursive
       nesting; one duplicate is enough.
     • PRICE COMPARISON CHART label inside .right-title — already
       labelled by the parent's "SECOND CHART" title.
   Also compress the chart-panel filter rows so the chart canvas
   gets the full remaining height. */
(function _hideSplitInsideIframe(){
    try {
        const sp = new URLSearchParams(location.search);
        if (sp.get('split') === '1') {
            const style = document.createElement('style');
            style.textContent =
                '.hdr{display:none !important}'
                + '.left{display:none !important}'
                + '#split-chart-btn,#expand-chart-btn{display:none !important}'
                + '.right-title{display:none !important}'
                + '.right{width:100% !important}'
                + '.body{overflow:hidden}'
                + '.chart-panel{padding:8px 10px !important}'
                + '.btn-panel{padding:4px 8px 6px !important}';
            document.head.appendChild(style);
        }
    } catch (_e) {}
})();

/* Oct-27 Y-axis sync.  When the main window and the iframe both
   show the "same scale" the reader can compare prices across
   panes at a glance.  Each chart computes its own Y range off its
   own datasets (so independent filters still work) BUT also broad-
   casts the range to the sibling window via localStorage.  The
   sibling listens for storage events and re-renders its chart
   using max(selfRange, siblingRange) so both end up on the
   larger of the two — nothing ever gets clipped.  Same origin so
   localStorage.setItem is the simplest cross-window bus. */
let _siblingYRange = null;
const _SYNC_KEY = 'pc_y_range_sync';
function _publishYRange(min, max) {
    try {
        localStorage.setItem(_SYNC_KEY, JSON.stringify({
            min: min, max: max, t: Date.now(),
            src: (new URLSearchParams(location.search).get('split') === '1')
                 ? 'iframe' : 'main',
        }));
    } catch (_e) {}
}
function _mergedYRange(selfRange) {
    if (!_siblingYRange) return selfRange;
    const r = selfRange || {};
    return {
        min: Math.min(r.min != null ? r.min : _siblingYRange.min,
                       _siblingYRange.min),
        max: Math.max(r.max != null ? r.max : _siblingYRange.max,
                       _siblingYRange.max),
    };
}
window.addEventListener('storage', function(e) {
    if (e.key !== _SYNC_KEY || !e.newValue) return;
    try {
        const d = JSON.parse(e.newValue);
        if (!d || typeof d.min !== 'number' || typeof d.max !== 'number') return;
        _siblingYRange = { min: d.min, max: d.max };
        /* Re-render current chart with the merged range so the two
           panes line up visually. */
        try { if (typeof _tryRender === 'function') _tryRender(); } catch (_err) {}
    } catch (_err) {}
});

function toggleChartExpand() {
    if (document.body.classList.contains('tables-expanded')) {
        toggleTablesExpand();        // collapse LEFT first
    }
    const on = document.body.classList.toggle('chart-expanded');
    const btn = document.getElementById('expand-chart-btn');
    if (btn) {
        btn.innerHTML = on ? '✕' : '⛶';
        btn.title     = on ? 'Return to split view' : 'Expand chart to full view';
    }
    /* Nudge Plotly so the chart re-fits its container now that the
       available width + height just changed.  Deferred one frame so
       the CSS transition has finished before we measure. */
    requestAnimationFrame(() => {
        try {
            const el = document.getElementById('chart');
            if (el && typeof Plotly !== 'undefined') Plotly.Plots.resize(el);
        } catch (_e) {}
    });
}

/* ── Size buttons ─────────────────────────────────────── */
function _setSize(size, btn) {
    if (_activeSizeBtn) _activeSizeBtn.classList.remove('active');
    _activeSizeBtn = btn; btn.classList.add('active');
    selectedSize = size;
    _tryRender();
}

/* ── Mode: By Store ───────────────────────────────────── */
function onModeStore(btn) {
    if (_activeModeBtn) _activeModeBtn.classList.remove('active');
    _activeModeBtn = btn; btn.classList.add('active');
    selectedMode = 'store'; selectedSubKey = null;
    if (_activeSubBtn) { _activeSubBtn.classList.remove('active'); _activeSubBtn = null; }

    document.getElementById('sub-label').textContent = 'Brand';
    const subBtns = document.getElementById('sub-btns');
    subBtns.innerHTML = '';

    // ALL button first
    (function() {
        const b = document.createElement('button');
        b.className = 'btn';
        b.textContent = 'ALL';
        b.style.fontWeight = '700';
        b.onclick = function() {
            if (_activeSubBtn) _activeSubBtn.classList.remove('active');
            _activeSubBtn = b; b.classList.add('active');
            selectedSubKey = 'ALL'; _tryRender();
        };
        subBtns.appendChild(b);
    }());

    // one button per brand that has data in any size
    Object.keys(D.brand_size_data).forEach(function(abbr) {
        const c = '#' + (BCOLORS[abbr] || '555555');
        const b = document.createElement('button');
        b.className = 'btn';
        b.textContent = BRANDS[abbr] || abbr;
        b.style.borderColor = c; b.style.color = c;
        b.onclick = function() {
            if (_activeSubBtn) _activeSubBtn.classList.remove('active');
            _activeSubBtn = b; b.classList.add('active');
            selectedSubKey = abbr; _tryRender();
        };
        subBtns.appendChild(b);
    });
    document.getElementById('sub-row').style.display = 'flex';
    if (subBtns.firstChild) subBtns.firstChild.click();
}

/* ── Mode: By Brand ───────────────────────────────────── */
function onModeBrand(btn) {
    if (_activeModeBtn) _activeModeBtn.classList.remove('active');
    _activeModeBtn = btn; btn.classList.add('active');
    selectedMode = 'brand'; selectedSubKey = null;
    if (_activeSubBtn) { _activeSubBtn.classList.remove('active'); _activeSubBtn = null; }

    document.getElementById('sub-label').textContent = 'Store';
    const subBtns = document.getElementById('sub-btns');
    subBtns.innerHTML = '';

    [{key:'tempe', label:'Tempe',              c:'#2196F3'},
     {key:'bj',    label:'Bob Jane',          c:'#4CAF50'},
     {key:'jax',   label:'JAX',               c:'#FF9800'},
     {key:'tw',    label:'TempeWOS Sell Out',  c:'#9C27B0'},
     {key:'twi',   label:'TempeWOS Sell In',   c:'#E040FB'}].forEach(function(s) {
        const b = document.createElement('button');
        b.className = 'btn';
        b.textContent = s.label;
        b.style.borderColor = s.c; b.style.color = s.c;
        b.onclick = function() {
            if (_activeSubBtn) _activeSubBtn.classList.remove('active');
            _activeSubBtn = b; b.classList.add('active');
            selectedSubKey = s.key; _tryRender();
        };
        subBtns.appendChild(b);
    });
    document.getElementById('sub-row').style.display = 'flex';
    if (subBtns.firstChild) subBtns.firstChild.click();
}

/* ── Pricing: Normal / Promo toggle ───────────────────────
   JAX and BJ carry a DISC_PRICE separate from PRICE; this bar
   switches which one the chart + both side tables read.  State
   update + re-render only — no data refetch, so flipping is
   instant.  Tempe / TempeWOS have no promo concept so their
   lines look identical in both modes. */
function onPricingNormal(btn) {
    if (_activePricingBtn) _activePricingBtn.classList.remove('active');
    _activePricingBtn = btn; btn.classList.add('active');
    pricingMode = 'normal';
    _tryRender();
}
function onPricingPromo(btn) {
    if (_activePricingBtn) _activePricingBtn.classList.remove('active');
    _activePricingBtn = btn; btn.classList.add('active');
    pricingMode = 'promo';
    _tryRender();
}

/* ── Size dropdown ────────────────────────────────────── */
/* Oct-12 PM 2nd pass: replaced the full-grid button bar (hid the
   chart) with a native <select> dropdown.
     • Default: "ALL" + "Reserved" <optgroup> (curated SIZE_CATEGORY
       sizes) + "Others" <optgroup> (extras seen in raw data).
     • "Reserved only" checkbox: hides the Others group.
   The dropdown's change event drives _setSize() the same way the
   button clicks used to; the previous _setSize contract (size name
   + button element to highlight) is preserved by passing the
   <select> itself as the "active" element — _setSize only reads
   its textContent for the chart title, which the <select>'s
   .value gives us too. */
function _buildSizeDropdown() {
    const sel = document.getElementById('size-select');
    const reservedOnly = document.getElementById('size-reserved-only').checked;
    sel.innerHTML = '';

    // Always first: the global "ALL" aggregate.
    const allOpt = document.createElement('option');
    allOpt.value = 'ALL';
    allOpt.textContent = 'ALL';
    sel.appendChild(allOpt);

    // Reserved (curated) group — always shown.
    if (POPULAR.length) {
        const g1 = document.createElement('optgroup');
        g1.label = 'Reserved';
        POPULAR.forEach(function(size) {
            const o = document.createElement('option');
            o.value = size;
            o.textContent = size;
            g1.appendChild(o);
        });
        sel.appendChild(g1);
    }

    // Others group — hidden when Reserved-only is ticked.
    if (!reservedOnly) {
        const extras = SIZES.filter(function(s) { return !POPULAR_SET.has(s); });
        if (extras.length) {
            const g2 = document.createElement('optgroup');
            g2.label = 'Others';
            extras.forEach(function(size) {
                const o = document.createElement('option');
                o.value = size;
                o.textContent = size;
                g2.appendChild(o);
            });
            sel.appendChild(g2);
        }
    }

    // Default to ALL whenever the list rebuilds so a dangling
    // selection (e.g. an Others size that got filtered out) can't
    // leave the chart stuck on nothing.
    sel.value = 'ALL';
    _setSize('ALL', sel);
}

/* ── init ─────────────────────────────────────────────── */
(function init() {
    const sel = document.getElementById('size-select');
    sel.addEventListener('change', function() {
        _setSize(sel.value, sel);
    });
    document.getElementById('size-reserved-only')
            .addEventListener('change', _buildSizeDropdown);
    _buildSizeDropdown();
    // Normal is the default pricing mode on first load so the chart
    // shows regular retail prices until the user flips Promo on.  We
    // click the button rather than set the state directly so the
    // button's "active" visual stays in sync.
    document.getElementById('btn-normal').click();
    // auto-select By Brand mode
    document.getElementById('btn-brand').click();
}());
</script>
</body>
</html>
"""


# ── Flask route ───────────────────────────────────────────────────────────────

@app.route('/price')
def price_dashboard():
    monthly = load_all_months()
    if not monthly:
        return ("<h2 style='padding:40px;font-family:sans-serif;color:#c00'>"
                "No data files found. Need Tempe_*.csv, BobJane_*.csv, JAX_*.csv.</h2>")

    # Diagnostic: Oct-12 user flagged that Tempeweborder_20261007_1739.csv
    # (and newer month downloads) didn't show up on the dashboard.  Log
    # per-month row counts for every store bucket so if a month's file
    # exists on disk but goes through with zero rows, it's visible in
    # the server log without needing a debugger.
    print("[price_compare] load_all_months → month-key : t_rows / bj_rows / jx_rows / tw_rows")
    for _mk in sorted(monthly.keys()):
        _md = monthly[_mk]
        print(f"[price_compare]   {_mk}: "
              f"t={len(_md.get('t_rows',  []))} "
              f"bj={len(_md.get('bj_rows', []))} "
              f"jx={len(_md.get('jx_rows', []))} "
              f"tw={len(_md.get('tw_rows', []))}")

    data = build_data(monthly)
    chart_json = json.dumps({
        "months":          data["months"],
        "store_avg":       data["store_avg"],
        "size_data":       data["size_data"],
        "brand_size_data": data["brand_size_data"],
    }, default=str)

    base_rows = load_compare_base()
    return render_template_string(
        _HTML,
        month_count    = len(data["months"]),
        chart_json     = chart_json,
        brands_json    = json.dumps(BRANDS),
        bcolors_json   = json.dumps(BRAND_COLOURS),
        sizes_json     = json.dumps(data["sizes_with_data"]),
        popular_json   = json.dumps(data["popular_sizes"]),
        base_rows      = base_rows,
    )


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
