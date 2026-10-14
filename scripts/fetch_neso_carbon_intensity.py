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
    region (see INTERCONNECTORS), weighted by what each link imported in that half-hour
    according to Elexon. When all the links of the region were exporting, the imports of the
    month are used instead, and failing that the capacity of the links.

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
  ./fetch_neso_carbon_intensity.py [cloud_regions.json] [output.csv]

Requires the carbonintensity-api executable, version 0.4.0 or later for the --mix option, on
the PATH (cargo install carbonintensity-api), or its location in the CARBONINTENSITY_API
environment variable.
"""

import bisect
import collections
import concurrent.futures
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
# Half-hourly generation per fuel type, interconnector flows included (imports positive)
ELEXON_URL = ("https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELHH?settlementDateFrom={start}"
              "&settlementDateTo={end}{fuel_types}&format=json")
# Days per request accepted by the Elexon API
ELEXON_MAX_DAYS = 7
USER_AGENT = "spruce-neso-carbon-intensity-script/2.0"
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

# GB interconnectors: (Elexon fuel type, name, Ember country, capacity in MW, API region the
# link lands in). Ireland stands for the Single Electricity Market, Northern Ireland included
# (Moyle).
INTERCONNECTORS = [
    ("INTFR", "IFA", "France", 2000, 14),
    ("INTELEC", "ElecLink", "France", 1000, 14),
    ("INTNEM", "Nemo Link", "Belgium", 1000, 14),
    ("INTNED", "BritNed", "Netherlands", 1000, 14),
    ("INTIFA2", "IFA2", "France", 1000, 12),
    ("INTNSL", "North Sea Link", "Norway", 1400, 4),
    ("INTVKL", "Viking Link", "Denmark", 1400, 9),
    ("INTEW", "East-West", "Ireland", 500, 6),
    ("INTGRNL", "Greenlink", "Ireland", 500, 7),
    ("INTIRL", "Moyle", "Ireland", 500, 2),
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


def region_links(region_id):
    """Returns the interconnectors whose imports reach an API region."""
    landing = IMPORT_NEIGHBOURS.get(region_id, [region_id])
    return [link for link in INTERCONNECTORS if link[4] in landing]


def fetch_flows(start, end):
    """Returns {datetime: {Elexon fuel type: MW}} for the interconnectors, imports positive."""
    fuel_types = "".join(f"&fuelType={link[0]}" for link in INTERCONNECTORS)
    # settlement days are UK local days, one more on each side covers the UTC period
    windows = []
    day = start - datetime.timedelta(days=1)
    while day <= end:
        last = min(day + datetime.timedelta(days=ELEXON_MAX_DAYS - 1), end)
        windows.append(ELEXON_URL.format(start=day, end=last, fuel_types=fuel_types))
        day = last + datetime.timedelta(days=1)
    flows = collections.defaultdict(dict)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        for data in pool.map(lambda url: http_json(url)["data"], windows):
            for row in data:
                when = datetime.datetime.fromisoformat(row["startTime"].replace("Z", "+00:00"))
                flows[when.replace(tzinfo=None)][row["fuelType"]] = row["generation"]
    return flows


class ImportWeights:
    """Weights of the countries in the imports of an API region, from the interconnector flows."""

    def __init__(self, links, flows):
        self.links = links
        self.flows = flows
        self.monthly = collections.defaultdict(collections.Counter)
        for when, by_link in flows.items():
            self.monthly[(when.year, when.month)].update(self._imports(by_link))
        self.capacity = collections.Counter()
        for _, _, country, mw, _ in links:
            self.capacity[country] += mw

    def _imports(self, by_link):
        imports = collections.Counter()
        for fuel_type, _, country, _, _ in self.links:
            imports[country] += max(by_link.get(fuel_type, 0), 0)
        return imports

    def at(self, when):
        """Returns {Ember country: weight} for the half-hour starting at when."""
        for imports in (self._imports(self.flows.get(when, {})),
                        self.monthly[(when.year, when.month)], self.capacity):
            total = sum(imports.values())
            if total > 0:
                return {country: mw / total for country, mw in imports.items() if mw > 0}
        return {}


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


def lifecycle_intensity(mix, year, factors, intensities, import_weights):
    """gCO2e/kWh of a half-hour's mix, imports valued at the weighted partner intensities."""
    value = sum(share * for_year(factors[FUELS[fuel]], year)
                for fuel, share in mix.items() if fuel != IMPORTS)
    return value + mix[IMPORTS] * sum(weight * for_year(intensities[country], year)
                                      for country, weight in import_weights.items())


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
        PROJECT_ROOT, "src", "main", "resources", "neso", "monthly_co2_intensity.csv")

    cli = os.environ.get("CARBONINTENSITY_API") or shutil.which("carbonintensity-api")
    if not cli:
        sys.exit("error: carbonintensity-api not found, install it with "
                 "'cargo install carbonintensity-api' or set CARBONINTENSITY_API")

    with open(cloud_regions_path) as f:
        cloud_regions = json.load(f)

    regions = []  # (provider, region, outcode)
    links = {}  # outcode -> interconnectors reaching its API region
    for provider in ("aws", "gcp", "azure"):
        for region, info in cloud_regions.get(provider, {}).get("cloud_regions", {}).items():
            if info.get("country") != COUNTRY:
                continue
            outcode = outward_code(info["latitude"], info["longitude"])
            if not outcode:
                sys.exit(f"error: no postcode near {provider} {region} "
                         f"({info['latitude']},{info['longitude']})")
            region_id, region_name = api_region(outcode)
            reaching = region_links(region_id)
            if not reaching:
                sys.exit(f"error: no interconnector for API region {region_id} ({region_name}), "
                         f"add one to INTERCONNECTORS or IMPORT_NEIGHBOURS")
            names = ", ".join(f"{link[1]} ({link[2]})" for link in reaching)
            print(f"{provider} {region} ({info['metro_area']}) -> {outcode}, "
                  f"region {region_id} {region_name}, imports via {names}", file=sys.stderr)
            regions.append((provider, region, outcode))
            links[outcode] = reaching

    factors, intensities = load_ember({link[2] for ls in links.values() for link in ls})

    # the last complete month ends where the current one starts
    end = datetime.date.today().replace(day=1)
    print(f"Fetching interconnector flows from {FROM_MONTH} to {end}...", file=sys.stderr)
    flows = fetch_flows(FROM_MONTH, end)
    averages = {}
    for outcode in sorted(links):
        print(f"Fetching {outcode} from {FROM_MONTH} to {end}...", file=sys.stderr)
        weights = ImportWeights(links[outcode], flows)
        values = {when: lifecycle_intensity(mix, when.year, factors, intensities, weights.at(when))
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
        out.write(f"# Interconnector flows: Contains BMRS data (c) Elexon Limited copyright and "
                  f"database right {datetime.date.today().year}, https://www.elexon.co.uk/bsc/data/"
                  f"balancing-mechanism-reporting-agent/copyright-licence-bmrs-data/\n")
        out.write("#provider,region,month,gCO2_per_kWh\n")
        for provider, region, outcode in regions:
            for (year, month), value in averages[outcode].items():
                out.write(f"{provider},{region},{year}-{month:02d},{value:.2f}\n")
                rows += 1
    os.replace(tmp_path, output_path)
    print(f"Wrote {output_path} ({rows} rows)", file=sys.stderr)


if __name__ == "__main__":
    main()
