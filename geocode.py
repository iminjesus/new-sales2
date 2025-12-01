import csv
import time
import os
import requests

INPUT_FILE = "addresses_1.csv"             # your exported CSV from Excel
OUTPUT_FILE = "addresses_1_geocoded.csv"   # final output
CACHE_FILE = "geocode_1_cache.csv"         # progress cache

# Column names (as they appear in your CSV header)
ADDRESS1_COL = "Address 1"
CITY_COL = "City"
REGION_COLS = ("Region", "Regio")        # try these, use whichever exists
POSTCODE_COLS = ("Postal Code", "Postal Cod")
COUNTRY_COLS = ("Country", "Countr")


def pick_col(row, candidates):
    """Return the first existing non-empty column from candidates."""
    if isinstance(candidates, str):
        candidates = (candidates,)
    for c in candidates:
        if c in row and row[c] is not None:
            val = row[c].strip()
            if val:
                return val
    return ""


def build_address(row):
    """Build a full postal address string from the CSV row."""
    street = pick_col(row, ADDRESS1_COL)
    city = pick_col(row, CITY_COL)
    region = pick_col(row, REGION_COLS)
    postcode = pick_col(row, POSTCODE_COLS)
    country_code = pick_col(row, COUNTRY_COLS).upper()

    # Map country codes to full country names
    country_map = {
        "AU": "Australia",
        "NZ": "New Zealand",
        "PG": "Papua New Guinea",
        "CO": "Colombia",
    }
    country = country_map.get(country_code, country_code or "")

    parts = []

    if street:
        parts.append(street)

    line2_parts = []
    if city:
        line2_parts.append(city)
    # e.g. "SA 5013"
    region_post = " ".join(p for p in [region, postcode] if p)
    if region_post:
        line2_parts.append(region_post)
    if line2_parts:
        parts.append(", ".join(line2_parts))

    if country:
        parts.append(country)

    return ", ".join(parts)


def geocode(address, session):
    """
    Geocode using Google Maps Geocoding API.
    Returns (lat, lon) as strings, or (None, None) if not found.
    """
    if not address:
        return None, None

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": "",  # <-- put your key here
    }

    resp = session.get(url, params=params, timeout=10)
    try:
        resp.raise_for_status()
    except Exception as e:
        print(f"HTTP error {resp.status_code} for '{address}': {e}")
        return None, None

    data = resp.json()
    status = data.get("status")
    if status != "OK" or not data.get("results"):
        print(f"No result ({status}) for '{address}'")
        return None, None

    loc = data["results"][0]["geometry"]["location"]
    lat = loc.get("lat")
    lng = loc.get("lng")
    return str(lat), str(lng)


def load_cache():
    """
    Load geocode_cache.csv into a dict keyed by line_index:
        cache[line_index] = (lat, lon)
    """
    cache = {}
    if not os.path.exists(CACHE_FILE):
        return cache

    with open(CACHE_FILE, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                line_index = int(row.get("line_index", "").strip())
            except (ValueError, TypeError):
                continue
            lat = row.get("lat") or None
            lon = row.get("lon") or None
            cache[line_index] = (lat, lon)
    return cache


def append_to_cache(line_index, built_address, lat, lon):
    """
    Append one result (by line index) to geocode_cache.csv.
    Creates the file with header if it does not exist yet.
    """
    file_exists = os.path.exists(CACHE_FILE)
    write_header = not file_exists or os.path.getsize(CACHE_FILE) == 0

    # IMPORTANT: mode="a" for append, and keep errors="replace" if you added it
    with open(CACHE_FILE, "a", newline="", encoding="utf-8", errors="replace") as f:
        fieldnames = ["line_index", "full_address", "lat", "lon"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if write_header:
            writer.writeheader()

        writer.writerow({
            "line_index": line_index,
            "full_address": built_address,
            "lat": lat,
            "lon": lon
        })

def write_output(rows, cache):
    """
    Write addresses_geocoded.csv using cache keyed by line index.
    Keeps all original columns and adds lat/lon at the end.
    """
    fieldnames = list(rows[0].keys())
    if "lat" not in fieldnames:
        fieldnames.append("lat")
    if "lon" not in fieldnames:
        fieldnames.append("lon")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        for i, r in enumerate(rows, start=1):
            lat, lon = cache.get(i, (None, None))
            r = dict(r)  # copy
            r["lat"] = lat
            r["lon"] = lon
            writer.writerow(r)


def main():
    # 1. Load input rows (DictReader so we can use column names)
    with open(INPUT_FILE, newline="", encoding="utf-8", errors="replace") as f_in:
        reader = csv.DictReader(f_in)
        rows = list(reader)

    if not rows:
        print("No rows found in input file.")
        return

    # 2. Load existing cache (if any), keyed by line index
    cache = load_cache()
    print(f"Loaded {len(cache)} cached rows from {CACHE_FILE}.")

    total_rows = len(rows)
    print(f"Total data rows in input (excluding header): {total_rows}")

    session = requests.Session()

    try:
        # 3. Process rows strictly in line order
        for i, row in enumerate(rows, start=1):
            if i in cache:
                # already processed in a previous run
                continue

            built_address = build_address(row)
            print(f"[{i}/{total_rows}] {built_address}")

            if not built_address:
                lat, lon = None, None
            else:
                try:
                    lat, lon = geocode(built_address, session)
                except Exception as e:
                    print(f"  Error geocoding line {i}: {e}")
                    lat, lon = None, None

            cache[i] = (lat, lon)
            append_to_cache(i, built_address, lat, lon)
            print(f"  -> lat={lat}, lon={lon}")

            # Be polite to Nominatim
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nInterrupted by user (Ctrl+C).")
        print("Progress so far is saved in geocode_cache.csv.")
    finally:
        # 4. Always regenerate output with whatever we have so far
        write_output(rows, cache)
        print(f"Done (partial or full). Wrote {OUTPUT_FILE} from cache.")


if __name__ == "__main__":
    main()
