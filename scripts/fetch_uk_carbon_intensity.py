#!/usr/bin/env python3
"""
Builds monthly carbon intensity figures for the cloud regions located in Great Britain,
from the regional data of the Carbon Intensity API (National Energy System Operator).

For every keyed region of cloud_regions.json whose country is the United Kingdom:
  - reverse-geocode the region's lat/lon to the nearest postcode with postcodes.io and keep
    its outward code (e.g. SW1A for London, CF10 for Cardiff);
  - retrieve the half-hourly intensities for that outward code with the carbonintensity-api
    CLI (https://github.com/jnioche/carbonintensity-api), which splits the period into the
    windows the API accepts;
  - average them per calendar month (UTC), for every complete month from FROM_MONTH on.

The API resolves an outward code to one of its 14 DNO regions; regions sharing one are
fetched once. The regional figures are forecasts, the API publishes no actual values per
region.

Output columns: provider,region,month,gCO2_per_kWh (month as yyyy-MM)

Usage:
  ./fetch_uk_carbon_intensity.py [cloud_regions.json] [output.csv]

Requires the carbonintensity-api executable on the PATH (cargo install carbonintensity-api),
or its location in the CARBONINTENSITY_API environment variable.
"""

import collections
import datetime
import json
import os
import shutil
import subprocess
import sys
import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Same first year as the Ember figures (see fetch_ember_co2_intensity.sh)
FROM_MONTH = datetime.date(2022, 1, 1)
COUNTRY = "United Kingdom"
POSTCODES_URL = "https://api.postcodes.io/postcodes?lat={lat}&lon={lon}&limit=1&radius=2000"
USER_AGENT = "spruce-uk-carbon-intensity-script/1.0"
# A month is skipped when fewer of its half-hours than this are returned
MIN_COVERAGE = 0.9


def outward_code(lat, lon):
    """Returns the outward code of the postcode closest to lat/lon."""
    req = urllib.request.Request(POSTCODES_URL.format(lat=lat, lon=lon),
                                 headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.load(resp).get("result") or []
    if not result:
        return None
    return result[0]["outcode"]


def fetch_half_hours(cli, outcode, start, end):
    """Returns {datetime: gCO2/kWh} for the half-hours between start and end (exclusive)."""
    out = subprocess.run([cli, "-s", start.isoformat(), "-e", end.isoformat(), outcode],
                         check=True, capture_output=True, text=True).stdout
    values = {}
    for line in out.splitlines():
        if not line.strip():
            continue
        when, value = line.split(",")
        when = datetime.datetime.fromisoformat(when.strip())
        # the CLI pads the windows it requests, values outside the period are dropped
        if start <= when.date() < end:
            values[when] = int(value)
    return values


def monthly_averages(values):
    """Returns {(year, month): average} for the months where enough half-hours are known."""
    by_month = collections.defaultdict(list)
    for when, value in values.items():
        by_month[(when.year, when.month)].append(value)
    averages = {}
    for (year, month), month_values in sorted(by_month.items()):
        next_month = datetime.date(year + month // 12, month % 12 + 1, 1)
        expected = (next_month - datetime.date(year, month, 1)).days * 48
        if len(month_values) < expected * MIN_COVERAGE:
            print(f"warning: {year}-{month:02d} has {len(month_values)} of {expected} "
                  f"half-hours, skipped", file=sys.stderr)
            continue
        averages[(year, month)] = sum(month_values) / len(month_values)
    return averages


def main():
    cloud_regions_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        PROJECT_ROOT, "src", "main", "resources", "cloud_regions.json")
    output_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
        PROJECT_ROOT, "src", "main", "resources", "carbonintensity-uk", "monthly_co2_intensity.csv")

    cli = os.environ.get("CARBONINTENSITY_API") or shutil.which("carbonintensity-api")
    if not cli:
        sys.exit("error: carbonintensity-api not found, install it with "
                 "'cargo install carbonintensity-api' or set CARBONINTENSITY_API")

    with open(cloud_regions_path) as f:
        cloud_regions = json.load(f)

    regions = []  # (provider, region, outcode)
    for provider in ("aws", "gcp", "azure"):
        for region, info in cloud_regions.get(provider, {}).get("cloud_regions", {}).items():
            if info.get("country") != COUNTRY:
                continue
            outcode = outward_code(info["latitude"], info["longitude"])
            if not outcode:
                sys.exit(f"error: no postcode near {provider} {region} "
                         f"({info['latitude']},{info['longitude']})")
            print(f"{provider} {region} ({info['metro_area']}) -> {outcode}", file=sys.stderr)
            regions.append((provider, region, outcode))

    # the last complete month ends where the current one starts
    end = datetime.date.today().replace(day=1)
    averages = {}
    for outcode in sorted({r[2] for r in regions}):
        print(f"Fetching {outcode} from {FROM_MONTH} to {end}...", file=sys.stderr)
        averages[outcode] = monthly_averages(fetch_half_hours(cli, outcode, FROM_MONTH, end))

    rows = 0
    tmp_path = output_path + ".tmp"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(tmp_path, "w") as out:
        out.write("# Carbon Intensity API, National Energy System Operator, "
                  "https://carbonintensity.org.uk\n")
        out.write("# Creative Commons Attribution Licence (CC-BY-4.0)\n")
        out.write("#provider,region,month,gCO2_per_kWh\n")
        for provider, region, outcode in regions:
            for (year, month), value in averages[outcode].items():
                out.write(f"{provider},{region},{year}-{month:02d},{value:.2f}\n")
                rows += 1
    os.replace(tmp_path, output_path)
    print(f"Wrote {output_path} ({rows} rows)", file=sys.stderr)


if __name__ == "__main__":
    main()
