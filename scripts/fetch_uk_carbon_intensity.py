#!/usr/bin/env python3
"""
Builds monthly lifecycle carbon intensity figures for the cloud regions located in Great
Britain, from the regional fuel mix published by the Carbon Intensity API (National Energy
System Operator) and Ember's lifecycle emission factors.

The API's own intensities only count the CO2 emitted at generation, whereas the figures used
for every other region come from Ember and cover the full lifecycle. The half-hourly fuel mix
of each region is therefore combined with Ember factors instead:

  intensity = sum over fuels of (share of the fuel x Ember lifecycle factor for the fuel)

  - Domestic fuels get Ember's factor for the UK and the year, derived from Ember's yearly
    data as power sector emissions / generation per fuel.
  - Imports get the Ember intensity of the countries whose interconnectors land in the API
    region, weighted by the capacity of the links (see INTERCONNECTORS).

For every keyed region of cloud_regions.json whose country is the United Kingdom:
  - reverse-geocode the region's lat/lon to the nearest postcode with postcodes.io and keep
    its outward code (e.g. SW1A for London, CF10 for Cardiff), resolved to an API region;
  - retrieve the half-hourly fuel mix for that outward code with the carbonintensity-api CLI
    (https://github.com/jnioche/carbonintensity-api), which splits the period into the
    windows the API accepts;
  - compute the lifecycle intensity of every half-hour and average it per calendar month
    (UTC), for every complete month from FROM_MONTH on.

The regional fuel mix comes from NESO's forecasts, the API publishes no actual values per
region. The result is per kWh of electricity in the mix, before transmission and
distribution losses, like the Ember figures.

Output columns: provider,region,month,gCO2_per_kWh (month as yyyy-MM)

Usage:
  ./fetch_uk_carbon_intensity.py [cloud_regions.json] [output.csv]

Requires the carbonintensity-api executable with the --mix option on the PATH
(cargo install carbonintensity-api), or its location in the CARBONINTENSITY_API environment
variable.
"""

import bisect
import collections
import csv
import datetime
import io
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
REGION_URL = "https://api.carbonintensity.org.uk/regional/postcode/{outcode}"
EMBER_URL = "https://files.ember-energy.org/public-downloads/yearly_full_release_long_format.csv"
USER_AGENT = "spruce-uk-carbon-intensity-script/2.0"
# A month is skipped when fewer of its half-hours than this are returned
MIN_COVERAGE = 0.9

# Fuels of the API mix -> Ember fuel whose UK lifecycle factor applies. "other" is 0 in the
# regional mix in practice; Ember's "Other Fossil" is the closest category.
FUELS = {
    "biomass": "Bioenergy",
    "coal": "Coal",
    "gas": "Gas",
    "nuclear": "Nuclear",
    "other": "Other Fossil",
    "hydro": "Hydro",
    "solar": "Solar",
    "wind": "Wind",
}
IMPORTS = "imports"

# GB interconnectors: (name, Ember country, capacity in MW, API region the link lands in).
# Ireland stands for the Single Electricity Market, Northern Ireland included (Moyle).
INTERCONNECTORS = [
    ("IFA", "France", 2000, 14),
    ("ElecLink", "France", 1000, 14),
    ("Nemo Link", "Belgium", 1000, 14),
    ("BritNed", "Netherlands", 1000, 14),
    ("IFA2", "France", 1000, 12),
    ("North Sea Link", "Norway", 1400, 4),
    ("Viking Link", "Denmark", 1400, 9),
    ("East-West", "Ireland", 500, 6),
    ("Greenlink", "Ireland", 500, 7),
    ("Moyle", "Ireland", 500, 2),
]
# Regions without a landing point that get the links of their neighbours
IMPORT_NEIGHBOURS = {
    13: [12, 14],  # London: South England and South East England
}


def http_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def outward_code(lat, lon):
    """Returns the outward code of the postcode closest to lat/lon."""
    result = http_json(POSTCODES_URL.format(lat=lat, lon=lon)).get("result") or []
    if not result:
        return None
    return result[0]["outcode"]


def api_region(outcode):
    """Returns (id, name) of the API region an outward code belongs to."""
    data = http_json(REGION_URL.format(outcode=outcode))["data"][0]
    return data["regionid"], data["shortname"]


def import_countries(region_id):
    """Returns {Ember country: weight} for the imports of an API region, weights summing to 1."""
    landing = IMPORT_NEIGHBOURS.get(region_id, [region_id])
    capacity = collections.Counter()
    for _, country, mw, region in INTERCONNECTORS:
        if region in landing:
            capacity[country] += mw
    total = sum(capacity.values())
    return {country: mw / total for country, mw in capacity.items()} if total else None


def load_ember(countries):
    """Returns (UK lifecycle factor per Ember fuel and year, intensity per country and year)."""
    print(f"Downloading {EMBER_URL}...", file=sys.stderr)
    req = urllib.request.Request(EMBER_URL, headers={"User-Agent": USER_AGENT})
    generation = {}
    emissions = {}
    intensities = collections.defaultdict(dict)
    with urllib.request.urlopen(req, timeout=300) as resp:
        for row in csv.DictReader(io.TextIOWrapper(resp, encoding="utf-8")):
            area, unit = row["Area"], row["Unit"]
            if not row["Value"]:
                continue
            year, value = int(row["Year"]), float(row["Value"])
            if area in countries and row["Variable"] == "CO2 intensity" and unit == "gCO2/kWh":
                intensities[area][year] = value
            elif area == COUNTRY and row["Subcategory"] == "Fuel":
                if unit == "TWh":
                    generation[(row["Variable"], year)] = value
                elif unit == "mtCO2":
                    emissions[(row["Variable"], year)] = value
    factors = collections.defaultdict(dict)
    for (fuel, year), twh in generation.items():
        if twh > 0 and (fuel, year) in emissions:
            # mtCO2 / TWh = kgCO2 / MWh = gCO2 / kWh
            factors[fuel][year] = emissions[(fuel, year)] / twh * 1000
    for fuel in FUELS.values():
        if not factors.get(fuel):
            sys.exit(f"error: no Ember factor for {fuel} in {COUNTRY}")
    for country in countries:
        if not intensities.get(country):
            sys.exit(f"error: no Ember intensity for {country}")
    return factors, intensities


def for_year(by_year, year):
    """Value for the year, else the closest earlier year, else the first year (as in SPRUCE)."""
    years = sorted(by_year)
    i = bisect.bisect_right(years, year)
    return by_year[years[i - 1] if i else years[0]]


def fetch_mix(cli, outcode, start, end):
    """Returns {datetime: {fuel: share}} for the half-hours between start and end (exclusive)."""
    out = subprocess.run([cli, "-s", start.isoformat(), "-e", end.isoformat(), "--mix", outcode],
                         check=True, capture_output=True, text=True).stdout
    reader = csv.reader(io.StringIO(out))
    header = [h.strip().lower() for h in next(reader)]
    fuels = list(FUELS) + [IMPORTS]
    missing = [f for f in fuels if f not in header]
    if missing:
        sys.exit(f"error: the CLI output has no column for {', '.join(missing)}: {header}")
    mixes = {}
    for row in reader:
        if not row:
            continue
        when = datetime.datetime.fromisoformat(row[0].strip())
        # the CLI pads the windows it requests, values outside the period are dropped
        if not start <= when.date() < end:
            continue
        values = dict(zip(header, row))
        shares = {f: float(values[f] or 0) for f in fuels}
        total = sum(shares.values())
        if total > 0:
            mixes[when] = {f: s / total for f, s in shares.items()}
    return mixes


def lifecycle_intensity(mix, year, factors, imports):
    """gCO2e/kWh of a half-hour's mix, imports valued at the weighted partner intensities."""
    value = sum(share * for_year(factors[FUELS[fuel]], year)
                for fuel, share in mix.items() if fuel != IMPORTS)
    return value + mix[IMPORTS] * sum(weight * for_year(by_year, year)
                                      for by_year, weight in imports)


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
    import_mix = {}  # outcode -> {country: weight}
    for provider in ("aws", "gcp", "azure"):
        for region, info in cloud_regions.get(provider, {}).get("cloud_regions", {}).items():
            if info.get("country") != COUNTRY:
                continue
            outcode = outward_code(info["latitude"], info["longitude"])
            if not outcode:
                sys.exit(f"error: no postcode near {provider} {region} "
                         f"({info['latitude']},{info['longitude']})")
            region_id, region_name = api_region(outcode)
            countries = import_countries(region_id)
            if not countries:
                sys.exit(f"error: no interconnector for API region {region_id} ({region_name}), "
                         f"add one to INTERCONNECTORS or IMPORT_NEIGHBOURS")
            weights = ", ".join(f"{c} {w:.0%}" for c, w in sorted(countries.items()))
            print(f"{provider} {region} ({info['metro_area']}) -> {outcode}, "
                  f"region {region_id} {region_name}, imports from {weights}", file=sys.stderr)
            regions.append((provider, region, outcode))
            import_mix[outcode] = countries

    factors, intensities = load_ember({c for mix in import_mix.values() for c in mix})

    # the last complete month ends where the current one starts
    end = datetime.date.today().replace(day=1)
    averages = {}
    for outcode in sorted(import_mix):
        print(f"Fetching {outcode} from {FROM_MONTH} to {end}...", file=sys.stderr)
        imports = [(intensities[c], w) for c, w in import_mix[outcode].items()]
        values = {when: lifecycle_intensity(mix, when.year, factors, imports)
                  for when, mix in fetch_mix(cli, outcode, FROM_MONTH, end).items()}
        averages[outcode] = monthly_averages(values)

    rows = 0
    tmp_path = output_path + ".tmp"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(tmp_path, "w") as out:
        out.write("# Fuel mix: Carbon Intensity API, National Energy System Operator, "
                  "https://carbonintensity.org.uk, CC-BY-4.0\n")
        out.write("# Lifecycle factors: Ember, https://ember-energy.org/creative-commons/, "
                  "CC-BY-4.0\n")
        out.write("#provider,region,month,gCO2_per_kWh\n")
        for provider, region, outcode in regions:
            for (year, month), value in averages[outcode].items():
                out.write(f"{provider},{region},{year}-{month:02d},{value:.2f}\n")
                rows += 1
    os.replace(tmp_path, output_path)
    print(f"Wrote {output_path} ({rows} rows)", file=sys.stderr)


if __name__ == "__main__":
    main()
