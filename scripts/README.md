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

End result: `src/main/resources/ember/ember_co2_intensity.csv`, columns `provider,region,gCO2_per_kWh`.

## Scripts

### `fetch_cloud_regions.sh`

Downloads AWS, GCP, and Azure region lists from cloudinfrastructuremap.com
and merges them into a single `cloud_regions.json`:

```json
{
  "aws":   { "_source": "...", "_license": "...", "_unresolved": [...], "cloud_regions": { "us-east-1": {...}, ... } },
  "gcp":   { ... },
  "azure": { ... }
}
```

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
  `_unresolved`.
- **GCP**: promote Mexico → `northamerica-south1`, Thailand →
  `asia-southeast3` from `_unresolved`.
- **Azure**: move `eastus` coordinates from Richmond to Ashburn/Sterling
  (Loudoun County, VA).

Remaining `_unresolved` entries are left alone — extend the script when new
region codes become available.

Usage: `./fix_cloud_regions.sh [cloud_regions.json]` (no default — pass the path explicitly).

### `fetch_ember_co2_intensity.sh`

Downloads three Ember CSVs and emits one CSV row per keyed cloud region:

- `yearly_full_release_long_format.csv` — per-country power-sector intensity.
- `us_yearly_full_release_long_format.csv` — per-US-state intensity.
- `india_yearly_full_release_long_format.csv` — per-Indian-state intensity.

Filtering and reduction (all datasets):

- `Unit == "gCO2/kWh"`.
- Keep only the row with the highest `Year` per ISO3 code / state code.
- Country rows are further restricted to countries that appear in
  `cloud_regions.json` (one alias: Ember's "United States of America" ↔
  cloud_regions' "United States").

Joining to cloud regions:

- For every keyed region under `aws`/`gcp`/`azure.cloud_regions`, emit
  `(provider, region_code, gCO2_per_kWh)`.
- For regions whose country has a sub-national source (US, India),
  reverse-geocode the region's `latitude`/`longitude` via OpenStreetMap
  Nominatim (`zoom=5`, read `address["ISO3166-2-lvl4"]`) to get an ISO
  3166-2 subdivision code (e.g. `US-VA`, `IN-MH`) and use the
  state-level Ember value.
- If the subdivision can't be resolved (e.g. the coordinates point at DC,
  which isn't a state) or has no Ember entry, fall back to the
  country-level value.
- Nominatim results are cached in `geocode_cache.tsv` (`lat`, `lon`,
  ISO 3166-2 code). Repeat runs are free; the 1 req/sec rate limit only
  matters on the first run or when new regions in supported countries
  appear. Delete the cache to force a refresh.
- `_unresolved` entries are skipped (no region code to emit).

Sub-national configuration lives in two arrays at the top of the script:

- `SUBNATIONAL` — one entry per source, `country_name|url|prefix` (the
  prefix is prepended to Ember's bare state codes so keys match what
  Nominatim returns, e.g. Ember `VA` → `US-VA`). Add a new source by
  adding another line.
- `SUBNATIONAL_ALIASES` — bridges cases where Nominatim and Ember use
  different ISO 3166-2 codes for the same region. Currently populated
  for Telangana (`IN-TS` ↔ `IN-TG`) and Odisha (`IN-OD` ↔ `IN-OR`).

Output is `src/main/resources/ember/ember_co2_intensity.csv` with three `#`-prefixed header lines
(licence URL, licence name, column names).

Usage: `./fetch_ember_co2_intensity.sh [cloud_regions.json] [output.csv]`.

Environment variables:

- `EMBER_GEOCACHE` — override the geocode cache path (default
  `./geocode_cache.tsv`).

## Cloud region water data

Scripts that build per-cloud-region water datasets from WRI sources.
Both scripts share the same geocode cache (`src/main/resources/geocode_cache.tsv`)
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
- `GEOCACHE` — override the geocode cache path (default `<output_dir>/geocode_cache.tsv`).

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
   `Accept-Language: en`) to get the state/province name in English. Common
   administrative suffixes ("Prefecture", "Province", "County", etc.) are
   stripped before matching. Matched against `province_baseline`.
2. **Country** — matched by country name against `country_baseline`.

Geocoded results are cached in `geocode_cache.tsv`. Cached entries with
non-ASCII state names (fetched before the `Accept-Language: en` fix) are
discarded and re-fetched automatically.

Output: `src/main/resources/water-stress.csv`, columns `provider,region,water_stress_cat`.

Usage: `./fetch_aqueduct_water_stress.py [cloud_regions.json] [output.csv]`

Environment variables:
- `GEOCACHE` — override the geocode cache path (default `<output_dir>/geocode_cache.tsv`).

## Requirements

- `bash` 4+
- `curl`
- `jq`
- `awk` (BSD awk on macOS or GNU awk both work)
- Python 3.8+, `openpyxl` (`pip install openpyxl`)
- Network access to `files.ember-energy.org`, `cloudinfrastructuremap.com`,
  `files.wri.org`, and `nominatim.openstreetmap.org`.

## Data sources and licensing

- Cloud region metadata: cloudinfrastructuremap.com, CC BY-SA 4.0.
- Power-sector CO₂ intensity: [Ember](https://ember-energy.org), released
  under Creative Commons — see
  [ember-energy.org/creative-commons/](https://ember-energy.org/creative-commons/)
  for the specific licence and attribution requirements.
- Water Consumption Factors: [WRI](https://www.wri.org/research/guidance-calculating-water-use-embedded-purchased-electricity),
  Creative Commons Attribution 4.0 International.
- Aqueduct 4.0 water stress: [WRI](https://www.wri.org/data/aqueduct-global-maps-40-data),
  Creative Commons Attribution 4.0 International.
- Reverse geocoding: OpenStreetMap Nominatim (please read and respect their
  [usage policy](https://operations.osmfoundation.org/policies/nominatim/)
  — the scripts send a descriptive `User-Agent` and sleep 1 s between
  uncached requests).

