#!/usr/bin/env python3
"""
Downloads the WRI water consumption factor (WCF) spreadsheet and maps cloud
regions from cloud_regions.json to a WCF value in l/kWh.

Source: WRI "Guidance for Calculating Water Use Embedded in Purchased Electricity"
  Appendix 1 — US eGRID subregions (WCF in gal/kWh, col C)
  Appendix 2 — Countries            (WCF in gal/kWh, col G)

Resolution order per cloud region:
  US regions  → reverse-geocode (lat/lon) → US state → eGRID subregion → WCF
  Other       → country name → WCF

Output columns: provider,region,wcf   (wcf in l/kWh)

Usage:
  ./fetch_wri_wcf.py [cloud_regions.json] [output.csv]

Requires: Python 3.8+, openpyxl  (pip install openpyxl)
"""

import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
import zipfile
from io import BytesIO

try:
    import openpyxl
except ImportError:
    sys.exit("error: openpyxl not installed — run: pip install openpyxl")

WRI_URL = (
    "https://files.wri.org/d8/s3fs-public/"
    "guidance-calculating-water-use-embedded-purchased-electricity-appendices-1-to-7.xlsx"
)
NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
NOMINATIM_UA = "wri-wcf-cloud-region-script/1.0 (contact: spruce@digitalpebble.com)"

GAL_TO_LITRE = 3.78541

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Trailing administrative suffixes returned by Nominatim (English) but absent in data
_ADMIN_SUFFIXES = (
    " prefecture", " province", " county", " region",
    " governorate", " district", " municipality",
    " metropolis", " autonomous region",
)


def _strip_admin_suffix(name: str) -> str:
    lower = name.lower()
    for suffix in _ADMIN_SUFFIXES:
        if lower.endswith(suffix):
            return name[: len(name) - len(suffix)].strip()
    return name


def _is_ascii(s: str) -> bool:
    try:
        s.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


# Map US state name (as returned by Nominatim) to eGRID subregion code.
# Virginia is split: lat > 38.5 → RFCE (Northern VA / Pepco), else SRVC (Dominion).
STATE_TO_EGRID: dict[str, str] = {
    "Alabama": "SRSO",
    "Alaska": "AKGD",
    "Arizona": "AZNM",
    "Arkansas": "SRMV",
    "California": "CAMX",
    "Colorado": "RMPA",
    "Connecticut": "NEWE",
    "Delaware": "RFCE",
    "District of Columbia": "RFCE",
    "Florida": "FRCC",
    "Georgia": "SRSO",
    "Hawaii": "HIOA",
    "Idaho": "NWPP",
    "Illinois": "RFCW",
    "Indiana": "RFCW",
    "Iowa": "MROW",
    "Kansas": "SPNO",
    "Kentucky": "SRMW",
    "Louisiana": "SRMV",
    "Maine": "NEWE",
    "Maryland": "RFCE",
    "Massachusetts": "NEWE",
    "Michigan": "RFCM",
    "Minnesota": "MROW",
    "Mississippi": "SRMV",
    "Missouri": "SRMW",
    "Montana": "NWPP",
    "Nebraska": "MROW",
    "Nevada": "AZNM",
    "New Hampshire": "NEWE",
    "New Jersey": "RFCE",
    "New Mexico": "AZNM",
    "New York": "NYUP",
    "North Carolina": "SRVC",
    "North Dakota": "MROW",
    "Ohio": "RFCW",
    "Oklahoma": "SPSO",
    "Oregon": "NWPP",
    "Pennsylvania": "RFCE",
    "Rhode Island": "NEWE",
    "South Carolina": "SRSO",
    "South Dakota": "MROW",
    "Tennessee": "SRTV",
    "Texas": "ERCT",
    "Utah": "RMPA",
    "Vermont": "NEWE",
    # Virginia split handled separately
    "Washington": "NWPP",
    "West Virginia": "RFCW",
    "Wisconsin": "MROE",
    "Wyoming": "RMPA",
}

# WRI uses "Great Britain"; cloud_regions.json uses "United Kingdom"
COUNTRY_ALIASES: dict[str, str] = {
    "United Kingdom": "Great Britain",
}


def download_xlsx(cache_dir: str) -> str:
    xlsx_path = os.path.join(cache_dir, "wri-wcf.xlsx")
    if os.path.exists(xlsx_path):
        print(f"Using cached {xlsx_path}", file=sys.stderr)
        return xlsx_path

    print(f"Downloading {WRI_URL} …", file=sys.stderr)
    req = urllib.request.Request(WRI_URL, headers={"User-Agent": NOMINATIM_UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read()

    with open(xlsx_path, "wb") as f:
        f.write(data)
    print(f"Saved to {xlsx_path}", file=sys.stderr)
    return xlsx_path


def load_wri(xlsx_path: str) -> tuple[dict[str, float], dict[str, float]]:
    """
    Returns:
      egrid_wcf:   eGRID code → WCF in l/kWh
      country_wcf: country name (lowercase) → WCF in l/kWh
    """
    print(f"Parsing {xlsx_path} …", file=sys.stderr)
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.worksheets[0]

    egrid_wcf: dict[str, float] = {}
    country_wcf: dict[str, float] = {}

    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i < 2:
            continue  # skip header rows
        # Appendix 1: col 0 = "XXXX (full name)", col 2 = WCF gal/kWh
        if row[0] and row[2] is not None:
            code = str(row[0]).split()[0]  # extract 4-letter code
            egrid_wcf[code] = float(row[2]) * GAL_TO_LITRE
        # Appendix 2: col 4 = country name, col 6 = WCF gal/kWh
        if row[4] and row[6] is not None:
            country_wcf[str(row[4]).lower().strip()] = float(row[6]) * GAL_TO_LITRE

    wb.close()
    print(
        f"  {len(egrid_wcf)} eGRID subregions, {len(country_wcf)} countries loaded",
        file=sys.stderr,
    )
    return egrid_wcf, country_wcf


def load_geocache(path: str) -> dict[tuple[str, str], str]:
    """Load TSV geocache: lat<TAB>lon<TAB>state_name. Skip stale non-ASCII entries."""
    cache: dict[tuple[str, str], str] = {}
    if not os.path.exists(path):
        return cache
    with open(path) as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 3:
                state = parts[2]
                if state and not _is_ascii(state):
                    continue
                cache[(parts[0], parts[1])] = state
    return cache


def save_geocache_entry(path: str, lat: str, lon: str, state: str) -> None:
    with open(path, "a") as f:
        f.write(f"{lat}\t{lon}\t{state}\n")


def reverse_geocode_state(lat: str, lon: str) -> str:
    url = f"{NOMINATIM_URL}?format=jsonv2&zoom=6&lat={lat}&lon={lon}"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": NOMINATIM_UA, "Accept-Language": "en"},
    )
    try:
        time.sleep(1.1)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except (urllib.error.URLError, json.JSONDecodeError) as exc:
        print(f"  geocode warning: {exc}", file=sys.stderr)
        return ""

    addr = data.get("address", {})
    for field in ("state", "province", "region", "county"):
        val = addr.get(field, "")
        if val:
            return _strip_admin_suffix(val)
    return ""


def egrid_for_us(state: str, lat: str) -> str | None:
    if state == "Virginia":
        return "RFCE" if float(lat) > 38.5 else "SRVC"
    return STATE_TO_EGRID.get(state)


def lookup_wcf(
    country: str,
    lat: str,
    lon: str,
    egrid_wcf: dict,
    country_wcf: dict,
    geocache: dict,
    geocache_path: str,
) -> float | None:
    if country == "United States":
        key = (lat, lon)
        if key in geocache:
            state = geocache[key]
        else:
            state = reverse_geocode_state(lat, lon)
            geocache[key] = state
            save_geocache_entry(geocache_path, lat, lon, state)

        egrid = egrid_for_us(state, lat) if state else None
        if egrid and egrid in egrid_wcf:
            return egrid_wcf[egrid]
        print(
            f"    no eGRID match for state={repr(state)}", file=sys.stderr
        )
        return None

    # Non-US: look up by country name (with alias translation)
    lookup_name = COUNTRY_ALIASES.get(country, country).lower()
    return country_wcf.get(lookup_name)


def main():
    cloud_regions_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        PROJECT_ROOT, "src", "main", "resources", "cloud_regions.json"
    )
    output_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
        PROJECT_ROOT, "src", "main", "resources", "wcf.csv"
    )
    geocache_path = os.environ.get(
        "GEOCACHE", os.path.join(os.path.dirname(output_path), "geocode_cache.tsv")
    )
    cache_dir = os.path.join(SCRIPT_DIR, ".aqueduct_cache")
    os.makedirs(cache_dir, exist_ok=True)

    if not os.path.exists(cloud_regions_path):
        sys.exit(f"error: {cloud_regions_path} not found")

    xlsx_path = download_xlsx(cache_dir)
    egrid_wcf, country_wcf = load_wri(xlsx_path)

    with open(cloud_regions_path) as f:
        cloud_regions = json.load(f)

    geocache = load_geocache(geocache_path)

    rows: list[tuple[str, str, str]] = []
    providers = ["aws", "gcp", "azure"]

    for provider in providers:
        regions = cloud_regions.get(provider, {}).get("cloud_regions", {})
        for region_code, info in regions.items():
            country = info.get("country", "")
            lat = str(info.get("latitude", ""))
            lon = str(info.get("longitude", ""))
            if not country or not lat or not lon:
                continue

            wcf = lookup_wcf(
                country, lat, lon, egrid_wcf, country_wcf, geocache, geocache_path
            )
            if wcf is not None:
                rows.append((provider, region_code, f"{wcf:.6f}"))
                print(
                    f"  {provider}:{region_code} ({country}) → {wcf:.4f} l/kWh",
                    file=sys.stderr,
                )
            else:
                print(
                    f"  {provider}:{region_code} ({country}) → no data",
                    file=sys.stderr,
                )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="") as f:
        f.write("# WRI water consumption factors (WCF) in l/kWh\n")
        f.write(f"# Source: {WRI_URL}\n")
        f.write("# US regions mapped via eGRID subregion; others by country\n")
        writer = csv.writer(f)
        writer.writerow(["provider", "region", "wcf"])
        writer.writerows(sorted(rows))

    print(f"Wrote {output_path} ({len(rows)} rows)", file=sys.stderr)


if __name__ == "__main__":
    main()
