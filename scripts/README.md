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

## Requirements

- `bash` 4+
- `curl`
- `jq`
- `awk` (BSD awk on macOS or GNU awk both work)
- Network access to `files.ember-energy.org`, `cloudinfrastructuremap.com`,
  and `nominatim.openstreetmap.org`.

## Data sources and licensing

- Cloud region metadata: cloudinfrastructuremap.com, CC BY-SA 4.0.
- Power-sector CO₂ intensity: [Ember](https://ember-energy.org), released
  under Creative Commons — see
  [ember-energy.org/creative-commons/](https://ember-energy.org/creative-commons/)
  for the specific licence and attribution requirements.
- Reverse geocoding: OpenStreetMap Nominatim (please read and respect their
  [usage policy](https://operations.osmfoundation.org/policies/nominatim/)
  — the script sends a descriptive `User-Agent` and sleeps 1 s between
  uncached requests).

