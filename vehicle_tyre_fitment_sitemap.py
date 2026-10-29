#!/usr/bin/env python3
"""
vehicle_tyre_fitment_sitemap.py - sitemap-based (make, model) catalog
builder for wheel-size.com.

Why this exists:
  The companion script vehicle_tyre_fitment.py's --discover-au mode
  walks each make's /size/<make>/ page one at a time, which is slow
  (one HTTP request per make - hundreds of them) and gets throttled
  by Cloudflare's bot-fight-mode well before it finishes.  The user
  pointed out that wheel-size.com itself publishes a sitemap that
  already lists EVERY vehicle URL in a single bulk XML file; we can
  skip the per-make drill-down entirely by parsing that sitemap.

Pipeline:
  1. Fetch https://www.wheel-size.com/sitemap.xml
  2. If it's a <sitemapindex>, recursively fetch each child <sitemap>
  3. For every <url><loc> that matches /size/<make>/<model>/ (two
     path segments after /size/, no further), extract (make, model)
  4. Validate + dedupe through the same _pair_looks_valid helpers
     from vehicle_tyre_fitment.py so garbage slugs don't poison the
     catalog
  5. Write the catalog to the same CSV path vehicle_tyre_fitment.py
     reads from (--discover-cache default:
     out/rego/wheel_size_au_catalog.csv), so the main crawler picks
     it up on its next --discover-au run without any extra flags

Usage:
    python vehicle_tyre_fitment_sitemap.py                      # default output
    python vehicle_tyre_fitment_sitemap.py --output cat.csv     # custom path
    python vehicle_tyre_fitment_sitemap.py --sitemap <url>      # custom root sitemap
    python vehicle_tyre_fitment_sitemap.py --limit 500          # smoke-test
    python vehicle_tyre_fitment_sitemap.py --market AU          # set market cookie

The output file is a plain CSV with header `make,model`, upper-cased,
drop-in compatible with vehicle_tyre_fitment.py's --discover-cache
loader.  Running this script replaces a slow per-make /size/<make>/
scan with ONE HTTP request to /sitemap.xml plus a handful of child
sitemap requests - typically done in under a minute even on a cold
cache.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import os
import re
import sys
import time
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

# Oct-29: cp949/cp1252 safe stdout for the Windows users running this.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# Reuse the same TLS-impersonating HTTP client the main crawler uses -
# wheel-size.com's CF challenge is already JA3-fingerprint-sensitive on
# ordinary /size/ pages; the sitemap endpoints have been observed to
# be more lenient but we match the main crawler's fingerprint anyway
# so a single session that falls back here from --discover-au carries
# the same cookies through.
try:
    from curl_cffi import requests as _http
    _HAS_CURL_CFFI = True
    _IMPERSONATE = "chrome124"
except ImportError:
    import requests as _http    # type: ignore
    _HAS_CURL_CFFI = False
    _IMPERSONATE = None

# Share the sanity-check helpers so a sitemap-sourced catalog gets the
# same shape validation --discover-au applies: reject news-article
# slugs, year-mashed model slugs, make-name-prefixed garbage.
try:
    from vehicle_tyre_fitment import (
        _looks_like_article_slug,
        _slug_has_model_with_year,
        _pair_looks_valid,
        _clean_model_name,
    )
except Exception as _e:
    # Soft-fail: let the script still run (just without the deep
    # validation).  Print why so the user knows what they're losing.
    print(f"[warn] couldn't import vehicle_tyre_fitment helpers "
          f"({_e}); running WITHOUT shape validation - the output "
          f"catalog may carry garbage slugs that --discover-au would "
          f"normally reject", file=sys.stderr)
    def _looks_like_article_slug(_s): return False
    def _slug_has_model_with_year(_s): return False
    def _clean_model_name(name, make=""): return str(name or "").strip()
    def _pair_looks_valid(mk, md): return bool(mk and md)


BASE_URL          = "https://www.wheel-size.com"
BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
REGO_DIR          = os.path.join(BASE_DIR, "out", "rego")
DEFAULT_OUTPUT    = os.path.join(REGO_DIR, "wheel_size_au_catalog.csv")
DEFAULT_SITEMAP   = f"{BASE_URL}/sitemap.xml"
# Cloudflare rejects obviously-bot UAs on the main /size/ pages, but
# the sitemap endpoints are usually open.  Use a real desktop UA so
# a borderline-protected sitemap child still answers.
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
SITEMAP_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"

# A sitemap child XML can arrive gzipped (as /sitemap-xxx.xml.gz) OR
# plain XML - we handle both.  gzip magic bytes come first; if we see
# them we unwrap before XML parsing.
_GZIP_MAGIC = b"\x1f\x8b"


def _mk_session(market: str) -> "_http.Session":
    """Build an HTTP session with the right market cookie + default
    headers so wheel-size.com's catalog listings are AU-market filtered
    (just like discover_au_catalog does in the main crawler)."""
    sess = _http.Session() if _HAS_CURL_CFFI else _http.Session()
    try:
        sess.cookies.set("market", market.upper(), domain="www.wheel-size.com")
        sess.cookies.set("market", market.upper(), domain=".wheel-size.com")
    except Exception as _e:
        print(f"[warn] couldn't set market cookie: {_e}", file=sys.stderr)
    sess.headers.update({
        "User-Agent": DEFAULT_UA,
        "Accept": "application/xml,text/xml,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
    })
    return sess


def _cf_warmup(sess) -> bool:
    """Oct-29: wheel-size.com's /sitemap.xml sits behind the same CF
    Bot Fight Mode as /size/ pages.  Fetching the public homepage
    FIRST lets CF stamp the session with __cf_bm / cf_clearance cookies
    on the clean-first-request path; subsequent sitemap requests then
    carry those cookies and usually sail through.  Also sends Accept:
    text/html so CF's content-type sniffer treats the warmup as a
    browser visit instead of a scraper probe.

    Returns True when the warmup succeeded (we got a non-blocked body),
    False when CF rejected the homepage too (which usually means the
    whole IP is blocklisted and no amount of retries will recover)."""
    url = f"{BASE_URL}/"
    print(f"[warmup] priming CF cookies via {url}", file=sys.stderr)
    for attempt in range(2):
        try:
            kw = {
                "timeout": 20,
                "headers": {
                    "User-Agent": DEFAULT_UA,
                    "Accept": ("text/html,application/xhtml+xml,"
                               "application/xml;q=0.9,*/*;q=0.8"),
                    "Accept-Language": "en-AU,en;q=0.9",
                    "Upgrade-Insecure-Requests": "1",
                },
            }
            if _HAS_CURL_CFFI:
                kw["impersonate"] = _IMPERSONATE
            r = sess.get(url, **kw)
            if r.status_code == 200 and len(r.content) > 1000:
                print(f"[warmup] OK ({r.status_code}, {len(r.content)} bytes)",
                      file=sys.stderr)
                return True
            print(f"[warmup] attempt {attempt + 1}: {r.status_code} "
                  f"({len(r.content) if r.content else 0} bytes)",
                  file=sys.stderr)
        except Exception as _e:
            print(f"[warmup] attempt {attempt + 1} error: {_e}",
                  file=sys.stderr)
        time.sleep(5 + attempt * 10)
    return False


def _wayback_latest(url: str, sess) -> str | None:
    """Oct-29: when the direct fetch is CF-blocked at the IP level,
    archive.org usually has a snapshot of the sitemap that was
    captured months ago.  Hitting the Wayback Machine doesn't touch
    wheel-size.com's servers, so CF has no visibility into it.
    Returns the Wayback snapshot URL (which Serves the raw bytes), or
    None when no snapshot exists."""
    api = ("https://archive.org/wayback/available?"
           + f"url={url}")
    try:
        kw = {"timeout": 20, "headers": {
            "User-Agent": DEFAULT_UA,
            "Accept": "application/json,*/*;q=0.5",
        }}
        if _HAS_CURL_CFFI:
            kw["impersonate"] = _IMPERSONATE
        r = sess.get(api, **kw)
        if r.status_code != 200:
            return None
        import json as _json
        data = _json.loads(r.text or "{}")
        snap = (data.get("archived_snapshots") or {}).get("closest") or {}
        if snap.get("available") and snap.get("url"):
            # The default Wayback replay URL serves a wrapper HTML; add
            # "if_" to the timestamp to get the raw original bytes.
            u = snap["url"]
            # https://web.archive.org/web/20240501000000/https://...
            # -> https://web.archive.org/web/20240501000000if_/https://...
            u = re.sub(r"(/web/\d{14})/", r"\1if_/", u)
            return u
    except Exception as _e:
        print(f"[wayback] lookup failed: {_e}", file=sys.stderr)
    return None


def _candidate_sitemap_urls(sess, root_hint: str) -> list[str]:
    """Figure out which sitemap URL(s) wheel-size.com actually serves.
    Tries, in order:
      1. The hint URL the caller passed (--sitemap).
      2. Every URL listed under "Sitemap:" in /robots.txt (which is
         usually NOT CF-protected and is the authoritative source).
      3. A short list of conventional fallbacks (/sitemap_index.xml,
         /sitemap/sitemap.xml, /sitemaps.xml).
    Returns the de-duped list in try-order so the caller can walk it
    until one answers 200."""
    out = [root_hint]
    # robots.txt first - the standard says this file lists every
    # sitemap URL the site exposes, and it almost never carries the
    # same Bot-Fight-Mode protection as content pages.
    try:
        kw = {"timeout": 15, "headers": {
            "User-Agent": DEFAULT_UA,
            "Accept": "text/plain,*/*;q=0.5",
        }}
        if _HAS_CURL_CFFI:
            kw["impersonate"] = _IMPERSONATE
        r = sess.get(f"{BASE_URL}/robots.txt", **kw)
        if r.status_code == 200:
            for line in (r.text or "").splitlines():
                s = line.strip()
                if s.lower().startswith("sitemap:"):
                    u = s.split(":", 1)[1].strip()
                    if u and u not in out:
                        out.append(u)
            print(f"[robots] /robots.txt returned {r.status_code}; "
                  f"found {sum(1 for x in out if x != root_hint)} "
                  f"sitemap URL(s)", file=sys.stderr)
        else:
            print(f"[robots] /robots.txt returned {r.status_code} - "
                  f"skipping", file=sys.stderr)
    except Exception as _e:
        print(f"[robots] fetch failed: {_e}", file=sys.stderr)
    # Conventional fallbacks for sites that forget to list sitemap in
    # robots.txt - cheap to try, we stop at the first 200.
    for p in ("/sitemap_index.xml", "/sitemap/sitemap.xml",
              "/sitemaps.xml", "/sitemap-index.xml"):
        u = f"{BASE_URL}{p}"
        if u not in out:
            out.append(u)
    return out


def _fetch_xml(session, url: str, delay: float = 1.5,
               max_retries: int = 3) -> bytes | None:
    """Download a sitemap URL with simple exponential backoff.  Returns
    the raw response bytes (gzip-unwrapped if needed) or None on
    failure.  Keeps the retry budget small - a sitemap miss is cheap
    to work around and bot-block retries aren't going to recover."""
    for attempt in range(max_retries):
        try:
            kwargs = {"timeout": 30}
            if _HAS_CURL_CFFI:
                kwargs["impersonate"] = _IMPERSONATE
            r = session.get(url, **kwargs)
            if r.status_code != 200:
                print(f"    [http] {r.status_code} <- {url}  "
                      f"(attempt {attempt + 1}/{max_retries})",
                      file=sys.stderr)
                time.sleep(min(60, delay * (2 ** attempt)))
                continue
            body = r.content
            # gzip-wrapped sitemaps come through with a .gz extension;
            # we also sniff the magic bytes in case the server emits
            # gzip without the extension (some CDNs do).
            if body[:2] == _GZIP_MAGIC:
                try:
                    body = gzip.decompress(body)
                except Exception as _e:
                    print(f"    [gzip] decompress failed: {_e}",
                          file=sys.stderr)
                    return None
            return body
        except Exception as _e:
            print(f"    [http] {type(_e).__name__}: {_e} <- {url}  "
                  f"(attempt {attempt + 1}/{max_retries})",
                  file=sys.stderr)
            time.sleep(min(60, delay * (2 ** attempt)))
    return None


def _parse_sitemap(body: bytes) -> tuple[list[str], list[str]]:
    """Return (child_sitemap_urls, page_urls).

    A sitemap XML is one of two shapes:
      1. <sitemapindex> - a list of other sitemap URLs.  Returned as
         the first tuple element so the caller can recurse into them.
      2. <urlset> - a flat list of page URLs.  Returned as the second
         tuple element for the (make, model) extractor to filter.

    Both ends are returned on every call so a hybrid sitemap (rare but
    legal) doesn't require the caller to know ahead of time."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError as _e:
        print(f"    [xml] parse error: {_e}", file=sys.stderr)
        return [], []
    children: list[str] = []
    pages: list[str] = []
    # The sitemap XML namespace is baked into every tag; strip the
    # namespace prefix with a wildcard match so our .find/.findall
    # calls don't need to spell it out everywhere.
    for sm in root.findall(f"{SITEMAP_NS}sitemap"):
        loc = sm.find(f"{SITEMAP_NS}loc")
        if loc is not None and loc.text:
            children.append(loc.text.strip())
    for u in root.findall(f"{SITEMAP_NS}url"):
        loc = u.find(f"{SITEMAP_NS}loc")
        if loc is not None and loc.text:
            pages.append(loc.text.strip())
    return children, pages


# Match /size/<make>/<model>/ exactly - two non-empty path segments
# after /size/, with an optional trailing slash and no further
# segments.  This is the shape of a model root page on wheel-size.com;
# /size/<make>/<model>/<year>/ pages (three segments) are skipped
# because the main crawler visits them itself once it has the catalog.
_MODEL_URL_RX = re.compile(
    r"^/size/([^/]+)/([^/]+)/?$",
    re.IGNORECASE,
)
# Known catalog paths we DON'T want to mine as model pages even
# though they match the two-segment shape: /size/all/, /size/popular/,
# category / listing / pseudo pages, etc.  Add here as spotted.
_SKIP_SLUGS: set[str] = {"all", "popular", "sitemap", "categories",
                         "wheels", "tires", "tyres"}


def _extract_pairs_from_urls(urls: list[str]) -> list[tuple[str, str]]:
    """Return [(MAKE_UP, MODEL_UP), ...] for every URL that matches the
    model-root shape.  Non-matches are silently skipped so a full
    sitemap of ~100k URLs collapses to a few thousand model entries
    without noisy per-URL logging."""
    pairs: list[tuple[str, str]] = []
    for u in urls:
        try:
            path = urlparse(u).path or ""
        except Exception:
            continue
        m = _MODEL_URL_RX.match(path)
        if not m:
            continue
        make_slug  = m.group(1).lower()
        model_slug = m.group(2).lower()
        if make_slug in _SKIP_SLUGS or model_slug in _SKIP_SLUGS:
            continue
        # Shape-check both halves through the main crawler's helpers
        # so an article-slug like /size/foo/bar-sep-21-2026/ doesn't
        # leak into the catalog.
        if _looks_like_article_slug(make_slug):
            continue
        if _looks_like_article_slug(model_slug):
            continue
        if _slug_has_model_with_year(model_slug):
            continue
        # Mirror discover_au_catalog's rule: a model slug that starts
        # with the make slug as a dash-separated prefix is a stale /
        # alias URL (/size/bmw/bmw-x5/ is the alias; /size/bmw/x5/ is
        # the real page).  Reject so the catalog doesn't ship pairs
        # that will 404 on the main crawler.
        if model_slug.startswith(f"{make_slug}-"):
            continue
        # Clean + validate the pair exactly the way --discover-au does
        # so the two catalogs are directly comparable.
        make_name  = make_slug.upper()
        model_name = _clean_model_name(model_slug, make=make_name).upper() \
                     or model_slug.upper()
        if not _pair_looks_valid(make_name, model_name):
            continue
        pairs.append((make_name, model_name))
    return pairs


def crawl_sitemap(root_sitemap: str, market: str, delay: float,
                   max_children: int = 0) -> list[tuple[str, str]]:
    """Walk root_sitemap -> every child sitemap -> every page URL,
    returning the deduped list of (MAKE_UP, MODEL_UP) pairs.
    max_children caps the number of child sitemaps fetched (0 =
    unlimited) for a quick smoke-test."""
    sess = _mk_session(market)
    # Oct-29: CF Bot Fight Mode gates the sitemap endpoint too.  Warm
    # the session with a homepage fetch so it picks up __cf_bm /
    # cf_clearance cookies BEFORE the first sitemap request lands.
    _cf_warmup(sess)
    # Oct-29: the --sitemap hint may be blocked even after warmup.
    # Try the hint first, then every URL listed in /robots.txt (the
    # authoritative source), then conventional fallbacks.  First one
    # that returns 200 and parses as valid XML wins.
    candidates = _candidate_sitemap_urls(sess, root_sitemap)
    print(f"[sitemap] candidate URLs to try (in order):",
          file=sys.stderr)
    for u in candidates:
        print(f"           - {u}", file=sys.stderr)
    body = None
    chosen = None
    for cand in candidates:
        print(f"[sitemap] GET {cand}", file=sys.stderr)
        body = _fetch_xml(sess, cand, delay=delay)
        if body is not None:
            # Also require it parses as valid sitemap XML (urlset or
            # sitemapindex) before accepting - a 200 that returns HTML
            # or a bare XML parse error isn't actually useful.
            try:
                _probe_c, _probe_p = _parse_sitemap(body)
                if _probe_c or _probe_p:
                    chosen = cand
                    break
                print(f"[sitemap]   returned 200 but no <sitemap>/<url> "
                      f"entries - trying next candidate", file=sys.stderr)
            except Exception as _e:
                print(f"[sitemap]   parse failed ({_e}) - trying next",
                      file=sys.stderr)
        body = None
    if body is None or chosen is None:
        print(f"[sitemap] every DIRECT candidate URL failed", file=sys.stderr)
        # Oct-29: when CF has fully blocked the user's outbound IP, try
        # the Wayback Machine - it serves a snapshot of wheel-size.com's
        # sitemap captured by archive.org without ever hitting the live
        # site, so CF has no visibility and no way to block it.
        print(f"[wayback] trying archive.org snapshots as fallback ...",
              file=sys.stderr)
        for cand in candidates:
            wu = _wayback_latest(cand, sess)
            if not wu:
                continue
            print(f"[wayback] GET {wu}", file=sys.stderr)
            wbody = _fetch_xml(sess, wu, delay=delay)
            if wbody is None:
                continue
            try:
                _wc, _wp = _parse_sitemap(wbody)
                if _wc or _wp:
                    body = wbody
                    chosen = wu
                    print(f"[wayback] using archived snapshot of {cand}",
                          file=sys.stderr)
                    break
            except Exception:
                continue
    if body is None or chosen is None:
        print(f"[sitemap] every candidate sitemap URL failed - aborting",
              file=sys.stderr)
        print(f"[hint] wheel-size.com's CF Bot Fight Mode is blocking "
              f"this IP and archive.org has no usable snapshot.  Options:\n"
              f"  1. Wait a few hours and retry - CF blocks usually "
              f"clear within a day.\n"
              f"  2. Switch outbound IP: VPN, mobile hotspot, or a "
              f"different machine.\n"
              f"  3. Save the sitemap manually from your browser (where "
              f"you CAN view wheel-size.com interactively), then re-run "
              f"this script with --local <saved_file.xml>.\n"
              f"  4. Install Docker + FlareSolverr for a one-time CF "
              f"challenge solve (headless Chromium).", file=sys.stderr)
        return []
    print(f"[sitemap] using {chosen}", file=sys.stderr)
    children, pages = _parse_sitemap(body)
    all_pairs: dict[tuple[str, str], None] = {}
    # Pages at the root level (rare for wheel-size.com but handle it).
    for pair in _extract_pairs_from_urls(pages):
        all_pairs[pair] = None
    print(f"[sitemap] root sitemap: {len(children)} child sitemap(s), "
          f"{len(pages)} direct page URL(s)", file=sys.stderr)
    if not children and not pages:
        print(f"[sitemap] no entries in root sitemap - is the URL "
              f"correct?", file=sys.stderr)
        return []
    if max_children and max_children > 0 and len(children) > max_children:
        print(f"[sitemap] limiting to first {max_children} child "
              f"sitemaps for smoke-test", file=sys.stderr)
        children = children[:max_children]
    for idx, child_url in enumerate(children, 1):
        print(f"[sitemap] {idx}/{len(children)}  {child_url}",
              file=sys.stderr)
        body = _fetch_xml(sess, child_url, delay=delay)
        if body is None:
            print(f"[sitemap]   skipped (fetch failed)", file=sys.stderr)
            continue
        # A child sitemap can itself be an index (nested two levels
        # deep is legal), so parse + handle both branches again.
        sub_children, sub_pages = _parse_sitemap(body)
        if sub_children:
            # One extra level only - don't go deeper to avoid runaway
            # recursion on a mis-configured sitemap.
            for grand_idx, grand in enumerate(sub_children, 1):
                print(f"[sitemap]     nested {grand_idx}/{len(sub_children)}  "
                      f"{grand}", file=sys.stderr)
                gbody = _fetch_xml(sess, grand, delay=delay)
                if gbody is None:
                    continue
                _, grand_pages = _parse_sitemap(gbody)
                before = len(all_pairs)
                for pair in _extract_pairs_from_urls(grand_pages):
                    all_pairs[pair] = None
                print(f"[sitemap]       +{len(all_pairs) - before} "
                      f"new pair(s), total={len(all_pairs)}",
                      file=sys.stderr)
                time.sleep(max(0.5, delay * 0.5))
            continue
        before = len(all_pairs)
        for pair in _extract_pairs_from_urls(sub_pages):
            all_pairs[pair] = None
        print(f"[sitemap]   +{len(all_pairs) - before} new pair(s), "
              f"total={len(all_pairs)}", file=sys.stderr)
        # Light pacing between child sitemaps so we don't trigger CF
        # even on the usually-lenient sitemap endpoints.
        time.sleep(max(0.5, delay * 0.5))
    print(f"[sitemap] TOTAL unique (make, model) pairs: {len(all_pairs)}",
          file=sys.stderr)
    return sorted(all_pairs.keys())


def main():
    ap = argparse.ArgumentParser(
        description="Build the wheel-size.com (make, model) catalog "
                    "from the site's sitemap XML, bypassing the per-"
                    "make page walk --discover-au does.")
    ap.add_argument("--output", default=DEFAULT_OUTPUT,
                    help=f"Where to write the catalog CSV "
                         f"(default: {DEFAULT_OUTPUT}).  Format matches "
                         f"vehicle_tyre_fitment.py's --discover-cache "
                         f"loader so the main crawler can read this "
                         f"file directly with --discover-au.")
    ap.add_argument("--sitemap", default=DEFAULT_SITEMAP,
                    help=f"Root sitemap URL to crawl "
                         f"(default: {DEFAULT_SITEMAP}).")
    ap.add_argument("--market", default="AU",
                    help="Market cookie value - sets wheel-size.com's "
                         "market filter during fetch.  Default AU.")
    ap.add_argument("--delay", type=float, default=1.5,
                    help="Seconds between HTTP requests (default 1.5). "
                         "Sitemap endpoints are usually lenient but "
                         "we pace anyway so a mis-configured sitemap "
                         "doesn't trip CF on a cold session.")
    ap.add_argument("--limit", type=int, default=0,
                    help="Limit child sitemaps processed (default 0 = "
                         "unlimited; use e.g. --limit 2 for a smoke "
                         "test).")
    ap.add_argument("--no-overwrite", action="store_true",
                    help="Refuse to overwrite --output if it already "
                         "exists.  Default behaviour overwrites in "
                         "place (same as the main crawler's cache).")
    ap.add_argument("--local", nargs="+", default=None, metavar="PATH",
                    help="Parse one or more LOCAL sitemap XML files "
                         "(no network fetch at all).  Use this when CF "
                         "has fully blocked your IP from wheel-size.com: "
                         "open the sitemap URL in your browser (where "
                         "you can clear the CF challenge interactively), "
                         "File > Save As > sitemap.xml, then pass that "
                         "file here.  Both plain XML AND .xml.gz are "
                         "supported.  Pass several files to merge their "
                         "contents.")
    args = ap.parse_args()

    if args.no_overwrite and os.path.exists(args.output):
        print(f"[fatal] {args.output} already exists and --no-overwrite "
              f"is set", file=sys.stderr)
        sys.exit(1)
    out_dir = os.path.dirname(os.path.abspath(args.output))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    t0 = time.time()
    if args.local:
        # Oct-29: local-file mode - zero network, user supplies the
        # sitemap(s) they already have on disk.  Walk every file,
        # recursively parse sitemapindex entries by converting their
        # <loc> URLs to filenames in the SAME directory (standard
        # wheel-size.com layout: sitemap.xml references sitemap-NNN
        # .xml siblings).  Collect pairs into one deduped set.
        all_pairs: dict[tuple[str, str], None] = {}
        import pathlib as _pl
        pending = [os.path.abspath(p) for p in args.local]
        seen_files: set[str] = set()
        while pending:
            fp = pending.pop(0)
            if fp in seen_files: continue
            seen_files.add(fp)
            if not os.path.exists(fp):
                print(f"[local] missing: {fp}", file=sys.stderr); continue
            try:
                with open(fp, "rb") as f:
                    body = f.read()
                if body[:2] == _GZIP_MAGIC:
                    body = gzip.decompress(body)
            except Exception as _e:
                print(f"[local] read failed for {fp}: {_e}", file=sys.stderr)
                continue
            print(f"[local] parsing {fp}  ({len(body)} bytes)",
                  file=sys.stderr)
            children, pages = _parse_sitemap(body)
            before = len(all_pairs)
            for pair in _extract_pairs_from_urls(pages):
                all_pairs[pair] = None
            print(f"[local]   +{len(all_pairs) - before} new pair(s) "
                  f"from {len(pages)} URL(s); "
                  f"{len(children)} nested sitemap(s)",
                  file=sys.stderr)
            # If this sitemap REFERENCES child sitemaps by URL, look
            # for them as sibling files in the same directory - that's
            # the layout wheel-size.com's full sitemap uses.
            parent_dir = _pl.Path(fp).parent
            for ch in children:
                try:
                    base = os.path.basename(urlparse(ch).path)
                except Exception:
                    continue
                if not base: continue
                sibling = str(parent_dir / base)
                if os.path.exists(sibling) and sibling not in seen_files:
                    pending.append(sibling)
                else:
                    print(f"[local]   skipping nested sitemap not on "
                          f"disk: {base}  (save it next to {os.path.basename(fp)} "
                          f"and re-run)", file=sys.stderr)
        pairs = sorted(all_pairs.keys())
        print(f"[local] TOTAL unique (make, model) pairs: {len(pairs)}",
              file=sys.stderr)
    else:
        pairs = crawl_sitemap(args.sitemap, args.market, args.delay,
                               max_children=args.limit)
    if not pairs:
        print(f"[fatal] 0 pairs collected - not writing the output "
              f"file.  Check the sitemap URL and that the HTTP fetches "
              f"above returned 200 OK.", file=sys.stderr)
        sys.exit(2)

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["make", "model"])
        for mk, md in pairs:
            w.writerow([mk, md])
    elapsed = time.time() - t0
    print(f"[done] wrote {len(pairs)} (make, model) pairs to "
          f"{args.output}  ({elapsed:.1f} s)")
    print(f"[hint] vehicle_tyre_fitment.py --discover-au will now pick "
          f"this catalog up from its --discover-cache default; add "
          f"--limit N to the main crawler for a size-capped first run.")


if __name__ == "__main__":
    main()
