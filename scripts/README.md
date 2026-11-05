# Scripts

Utility scripts that generate resource files consumed by the SPRUCE pipeline
from external data sources. Run these to refresh the bundled datasets when
upstream data changes.

## Cloud region carbon intensity

Scripts that build a per-cloud-region carbon intensity dataset by joining
[cloudinfrastructuremap.com](https://www.cloudinfrastructuremap.com) region
metadata with [Ember](https://ember-energy.org) power-sector CO₂ intensity
data.

## Pipeline

Run in this order from the project root:

```sh
scripts/fetch_cloud_regions.sh
scripts/fix_cloud_regions.sh src/main/resources/cloud_regions.json
scripts/fetch_ember_co2_intensity.sh src/main/resources/cloud_regions.json
```

End result: `src/main/resources/ember/ember_co2_intensity.csv`, columns `provider,region,year,gCO2_per_kWh`, one row per region and year from 2022 on.

## Scripts

### `fetch_cloud_regions.sh`

Downloads AWS, GCP, and Azure region lists from cloudinfrastructuremap.com
and merges them into a single `cloud_regions.json`:

```json
{
  "aws":   { "_source": "...", "_license": "...", "_unresolved": [...], "cloud_regions": { "us-east-1": {...}, ... } },
  "gcp":   { "_source": "...", "_license": "...", "_unresolved": [...], "cloud_regions": { ... } },
  "azure": { "_source": "...", "_license": "...", "_unresolved": [...], "cloud_regions": { ... } }
}
```

- Every provider object has the same four fields: `_source`, `_license`,
  `_unresolved`, and `cloud_regions`.
- Entries whose upstream `cloud_region` is `null` or empty can't be used as
  object keys, so they're parked under `_unresolved` per provider for the
  fix script to handle later.
- Scratch dir is deleted on success; on failure it's left behind for
  inspection.

Usage: `./fetch_cloud_regions.sh [output.json]` (default `src/main/resources/cloud_regions.json`).

### `fix_cloud_regions.sh`

Edits `cloud_regions.json` in place to correct known upstream errors:

- **AWS**: swap `us-west-1` (N. California) and `us-west-2` (Oregon); move
  Auckland out of `ap-east-2` into `ap-southeast-6`; promote Malaysia →
  `ap-southeast-5`, Mexico → `mx-central-1`, Taipei → `ap-east-2` from
  `_unresolved`; fix `us-east-1` coordinates (Washington DC →
  Ashburn/Loudoun County, VA); fix `cn-northwest-1` coordinates and
  `metro_area` (Ningxiang/Hunan → Yinchuan/Ningxia); fix `ca-central-1`
  coordinates (~50 km west of Montréal Island → Montréal); move `eu-west-2`
  from central London to Hayes, west of London, where most of the sites listed
  for it by third parties are (this places it in the South England region of
  the GB Carbon Intensity API).
- **GCP**: promote Mexico → `northamerica-south1`, Thailand →
  `asia-southeast3` from `_unresolved`.
- **Azure**: move `eastus` coordinates from Richmond to Ashburn/Sterling
  (Loudoun County, VA) and rename to `"East US (Ashburn)"`.

Remaining `_unresolved` entries are left alone — extend the script when new
region codes become available.

Usage: `./fix_cloud_regions.sh [cloud_regions.json]` (default `src/main/resources/cloud_regions.json`).
Run it on fresh `fetch_cloud_regions.sh` output: a second run on a fixed file breaks it.

### `fetch_ember_co2_intensity.sh`

Downloads three Ember CSVs and emits one CSV row per keyed cloud region and year:

- `generation/outputs/release_generation_yearly_global.csv` — per-country
  power-sector intensity.
- `generation/outputs/release_us_subnational_generation_yearly.csv` —
  per-US-state intensity.
- `india_yearly_full_release_long_format.csv` — per-Indian-state intensity.

Ember publishes these in two layouts and is moving its releases from the
second to the first; India is the last one still on the old layout. The
`SUBNATIONAL` entries carry the layout of their source:

- `wide` (global, US) — one row per area, year and electricity source. Keep
  the rows whose `Electricity source` is `Total generation`, i.e. the
  all-sources row, and read `Emissions intensity (gCO2e/kWh)`.
- `long` (India) — one row per variable. Keep the rows whose `Unit` is
  `gCO2/kWh` and read `Value`.

Note the unit: the wide releases publish CO2e, where the long ones published
CO2. The output column is still named `gCO2_per_kWh`.

Columns are addressed by header name, not position, since the two layouts
differ in column order and Ember has added columns between releases. A
missing column stops the script rather than emptying the output. Fields
holding a comma are quoted upstream (`Hydro, bioenergy and other
renewables`, `Washington, D.C.`), so the CSVs are parsed with the quoting
rather than split on every comma.

Filtering and reduction (all datasets):

- Keep every year from `FROM_YEAR` (2022) on, per ISO3 code / state code,
  dropping empty values.
- Country rows are restricted to countries that appear in
  `cloud_regions.json`, and to rows with a non-empty `ISO 3 code`, which
  drops Ember's aggregate regions (`ASEAN`, `Europe`, ...).

Joining to cloud regions:

- For every keyed region under `aws`/`gcp`/`azure.cloud_regions`, emit one
  `(provider, region_code, year, gCO2_per_kWh)` row per year.
- For regions whose country has a sub-national source (US, India),
  reverse-geocode the region's `latitude`/`longitude` via OpenStreetMap
  Nominatim (`zoom=5`, read `address["ISO3166-2-lvl4"]`) to get an ISO
  3166-2 subdivision code (e.g. `US-VA`, `IN-MH`) and use the
  state-level Ember value.
- If a region in one of those countries has no coordinates, or Nominatim
  places its coordinates in no subdivision, or Ember has no figure for that
  subdivision, the script stops without writing the csv and lists the
  regions with the reason: each of them would otherwise get the national
  figure.
- If a region's country has no Ember figure at all, the csv is still
  written, with a warning naming the country and its regions: those regions
  get no carbon intensity rather than a wrong one.
- Resolved subdivisions are cached in `scripts/.geocode_cache_iso` (`lat`,
  `lon`, ISO 3166-2 code); failed requests are not, so they are retried on
  the next run. Repeat runs are free; the 1 req/sec rate limit only matters
  on the first run or when new regions in supported countries appear.
  Delete the cache to force a refresh. The water scripts below keep a
  separate cache, since they store state names rather than ISO codes.
- `_unresolved` entries are skipped (no region code to emit).

Source and alias configuration lives in four arrays at the top of the script:

- `SUBNATIONAL` — one entry per source,
  `country_name|url|prefix|layout` (the prefix is prepended to Ember's bare
  state codes so keys match what Nominatim returns, e.g. Ember `VA` →
  `US-VA`). Add a new source by adding another line.
- `COUNTRY_ALIASES` — bridges countries Ember names differently from
  `cloud_regions.json`. Currently populated for Taiwan (Ember
  `Taiwan (China)`).
- `SUBNATIONAL_ALIASES` — bridges cases where Nominatim and Ember use
  different ISO 3166-2 codes for the same region. Currently populated
  for Telangana (`IN-TS` ↔ `IN-TG`) and Odisha (`IN-OD` ↔ `IN-OR`).

Ember retires a download by replacing it with a two-line CSV holding a
notice and the URL of its replacement, served as `200`, which `curl --fail`
does not catch. The script checks for that notice and reports it, so a
retired URL is named rather than silently yielding no rows.

Output is `src/main/resources/ember/ember_co2_intensity.csv` with three `#`-prefixed header lines
(licence URL, licence name, column names).

Usage: `./fetch_ember_co2_intensity.sh [cloud_regions.json] [output.csv]`.

Environment variables:

- `EMBER_GEOCACHE` — override the geocode cache path (default
  `scripts/.geocode_cache_iso`).

## UK monthly carbon intensity

### `fetch_neso_carbon_intensity.py`

Builds monthly lifecycle carbon intensity figures for the cloud regions located in Great
Britain from the regional fuel mix of the [Carbon Intensity API](https://carbonintensity.org.uk/)
(National Energy System Operator) and [Ember](https://ember-energy.org)'s lifecycle factors.

For every keyed region of `cloud_regions.json` whose country is `United Kingdom`:

- reverse-geocode the region's `latitude`/`longitude` to the nearest postcode with
  [postcodes.io](https://postcodes.io/) and keep its outward code (`SW1A` for London, `CF10`
  for Cardiff), resolved to an API region (London, South Wales);
- retrieve the half-hourly fuel mix for that outward code from January 2022 to the end of the
  last complete month with the
  [carbonintensity-api](https://github.com/jnioche/carbonintensity-api) CLI (`--mix` option),
  which splits the period into the windows the API accepts; regions sharing an outward code
  are fetched once;
- value each half-hour's mix with Ember's lifecycle factors: per fuel, the intensity Ember
  publishes for that fuel in the UK for the year, read from the same
  `release_generation_yearly_global.csv` as `fetch_ember_co2_intensity.sh`; for imports,
  Ember's intensity of the countries whose interconnectors land in the API region (table
  `INTERCONNECTORS` in the script, with `IMPORT_NEIGHBOURS` for regions without a landing point
  such as London), weighted by what each link imported in that half-hour according to the
  Elexon `FUELHH` dataset (exports count as zero). When all the links of the region were
  exporting, the imports of the month are used, and failing that the capacity of the links.
  The script stops if a region has no link;
- average the result per calendar month (UTC). A month with less than 90% of its half-hours
  is skipped with a warning.

Ember's release holds one row per area, year and electricity source, with the intensity of
that source in `Emissions intensity (gCO2e/kWh)`. A fuel's row gives its lifecycle factor,
and the `Total generation` row of a country gives the intensity of its grid. The script stops
if the download has been retired, if one of the columns it reads is missing, or if a fuel or
an interconnector country has no figure. The names Ember uses for the fuels are the values of
`FUELS` in the script.

Note the unit: this release publishes CO2e, where Ember's retired long format published CO2.
The output column is still named `gCO2_per_kWh`.

Years Ember has not published yet get the latest year available.

Output: `src/main/resources/neso/monthly_co2_intensity.csv`, columns
`provider,region,month,gCO2_per_kWh` (month as `yyyy-MM`), with four `#`-prefixed header lines
(sources, licences and column names).

Requires the CLI, version 0.4.0 or later (for `--mix`), installed with `cargo install
carbonintensity-api`, on the `PATH`, or its location in the `CARBONINTENSITY_API`
environment variable. Python standard library only.

Usage: `./fetch_neso_carbon_intensity.py [cloud_regions.json] [output.csv]`

## Cloud region water data

Scripts that build per-cloud-region water datasets from WRI sources.
Both scripts share the same geocode cache (`scripts/.geocode_cache`, lat/lon → state name)
and download cache (`scripts/.aqueduct_cache/`).

Run from the project root after `fetch_cloud_regions.sh` / `fix_cloud_regions.sh`:

```sh
python3 scripts/fetch_wri_wcf.py
python3 scripts/fetch_aqueduct_water_stress.py
```

Requires: Python 3.8+, `openpyxl` (`pip install openpyxl`).

### `fetch_wri_wcf.py`

Downloads the WRI spreadsheet *Guidance for Calculating Water Use Embedded in
Purchased Electricity* and produces a Water Consumption Factor (WCF, l/kWh)
for each cloud region.

**Source:** `https://files.wri.org/d8/s3fs-public/guidance-calculating-water-use-embedded-purchased-electricity-appendices-1-to-7.xlsx`

The spreadsheet's first sheet contains two side-by-side tables:
- **Appendix 1** — US eGRID subregion WCF values (gal/kWh).
- **Appendix 2** — Country-level WCF values (gal/kWh).

Values are converted to l/kWh (× 3.78541).

Resolution per cloud region:
1. **US regions** — reverse-geocode the region's lat/lon via Nominatim to get the
   US state, then map state → eGRID subregion code (e.g. Virginia → RFCE,
   Oregon → NWPP, Ohio → RFCW). Virginia is split: lat > 38.5° → RFCE
   (Northern Virginia / Pepco), otherwise SRVC (Dominion).
2. **Non-US regions** — match by country name. "United Kingdom" is aliased to
   the WRI entry "Great Britain".

Output: `src/main/resources/wcf.csv`, columns `provider,region,wcf`.

Usage: `./fetch_wri_wcf.py [cloud_regions.json] [output.csv]`

Environment variables:
- `GEOCACHE` — override the geocode cache path (default `scripts/.geocode_cache`).

### `fetch_aqueduct_water_stress.py`

Downloads the WRI Aqueduct 4.0 country/province baseline water stress rankings
and maps each cloud region to a stress category (0–4).

**Source:** `https://files.wri.org/aqueduct/aqueduct-4-0-country-rankings.zip`

The zip contains an xlsx with two relevant sheets:
- `province_baseline` — sub-national stress categories.
- `country_baseline` — national stress categories.

Only rows with `indicator_name = bws` (baseline water stress) and `weight = Tot`
are used. Category values: 0 = Low, 1 = Low-Medium, 2 = Medium-High, 3 = High,
4 = Extremely High.

Resolution per cloud region (first match wins):
1. **Province** — reverse-geocode the region's lat/lon via Nominatim (zoom=6,
   `Accept-Language: en`, `namedetails=1`). The subdivision is the first of
   `state`, `province`, `region`, `county` or `city` in the address (city
   covers city-level subdivisions such as Berlin, Beijing or Kuala Lumpur).
   Its English name is tried first, then its local, official and alternative
   names, since Aqueduct mostly uses local names (`Lombardia`, `Noord-Holland`,
   `Comunidad de Madrid`). Common administrative suffixes ("Prefecture",
   "Province", "County", etc.) are stripped before matching against
   `province_baseline`.
2. **Country** — matched by country name against `country_baseline`.

Geocoded results are cached in `scripts/.geocode_cache` as
`lat`, `lon`, English name, `|`-separated other names. Entries without the
fourth column (written by `fetch_wri_wcf.py`, which only needs the English
name, or by older runs) are re-fetched. Failed requests are not cached.

Output: `src/main/resources/water-stress.csv`, columns `provider,region,water_stress_cat`.

Usage: `./fetch_aqueduct_water_stress.py [cloud_regions.json] [output.csv]`

Environment variables:
- `GEOCACHE` — override the geocode cache path (default `scripts/.geocode_cache`).

## EC2 instance types

### `fetch_ec2_instance_types.sh`

Writes every EC2 instance type, current and discontinued, one per line, in the
format `com.digitalpebble.spruce.modules.boavizta.APITester` reads. It unions
the price list catalogue (`aws pricing get-attribute-values`, which keeps
retired families such as `cc2` or `g2`), the types each region offers today
(`aws ec2 describe-instance-type-offerings`) and the public price lists of
2015 to 2017, which hold the few types retired before that catalogue was
assembled. Needs credentials allowing `pricing:GetAttributeValues`,
`ec2:DescribeRegions` and `ec2:DescribeInstanceTypeOfferings`.

Options: `-a`, `--all-regions` to query every region AWS advertises rather
than those enabled on the account; `--current-only` for the types launchable
today; `--no-history` to skip the historical price lists;
`--history-years 2015,2016,2017` to change the years they are taken from.

Usage: `./fetch_ec2_instance_types.sh [options] [output-file]` (default
`instanceTypes` at the project root).

## Requirements

- `bash` 4+
- `curl`
- `jq`
- `awk` (BSD awk on macOS or GNU awk both work)
- Python 3.8+, `openpyxl` (`pip install openpyxl`)
- Network access to `files.ember-energy.org`, `cloudinfrastructuremap.com`,
  `files.wri.org`, `nominatim.openstreetmap.org`, `api.postcodes.io`,
  `api.carbonintensity.org.uk` and `data.elexon.co.uk`.

## Data sources and licensing

- Cloud region metadata: cloudinfrastructuremap.com, CC BY-SA 4.0.
- Power-sector CO₂ intensity: [Ember](https://ember-energy.org), released
  under Creative Commons — see
  [ember-energy.org/creative-commons/](https://ember-energy.org/creative-commons/)
  for the specific licence and attribution requirements.
- UK regional fuel mix: [Carbon Intensity API](https://carbonintensity.org.uk/),
  National Energy System Operator, Creative Commons Attribution 4.0 International.
- GB interconnector flows: [Elexon BMRS](https://bmrs.elexon.co.uk/), "Contains BMRS data
  © Elexon Limited copyright and database right", under the
  [BMRS open data licence](https://www.elexon.co.uk/bsc/data/balancing-mechanism-reporting-agent/copyright-licence-bmrs-data/).
- Water Consumption Factors: [WRI](https://www.wri.org/research/guidance-calculating-water-use-embedded-purchased-electricity),
  Creative Commons Attribution 4.0 International.
- Aqueduct 4.0 water stress: [WRI](https://www.wri.org/data/aqueduct-global-maps-40-data),
  Creative Commons Attribution 4.0 International.
- Reverse geocoding: OpenStreetMap Nominatim (please read and respect their
  [usage policy](https://operations.osmfoundation.org/policies/nominatim/)
  — the scripts send a descriptive `User-Agent` and sleep 1 s between
  uncached requests).

