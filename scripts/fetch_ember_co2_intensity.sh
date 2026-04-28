#!/usr/bin/env bash
#
# Downloads Ember's yearly full-release CSVs (country + per-country sub-
# national releases for US and India) and joins them against cloud_regions.json
# to produce a per-cloud-region carbon intensity table.
#
# Filters (all CSVs):
#   - Unit == "gCO2/kWh"
# Country CSV:
#   - ISO 3 code non-empty, Area in cloud_regions.json country set.
#   - One value per ISO3: the row with the highest Year.
# Sub-national CSVs (US, India):
#   - State code non-empty.
#   - One value per state code: the row with the highest Year.
# Join:
#   - For every keyed region under aws/gcp/azure.cloud_regions, emit
#     (provider, region_code, gCO2_per_kWh).
#   - For regions in a country with a sub-national source, reverse-geocode
#     the region's lat/lon via OpenStreetMap Nominatim to get the ISO 3166-2
#     subdivision code and use the sub-national value. If the subdivision
#     can't be resolved or has no Ember value, fall back to the country
#     value.
#   - Results are cached in ./geocode_cache.tsv (lat, lon, ISO_3166-2 code)
#     so repeat runs don't hit the API.
#   - _unresolved entries are skipped (no region code).
#   - Regions with no country/subdivision match are skipped.
#
# Output columns: provider,region,gCO2_per_kWh
#
# Usage:  ./fetch_ember_co2_intensity.sh [cloud_regions.json] [output.csv]
#
# Requires: bash, curl, jq, awk.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

URL="https://files.ember-energy.org/public-downloads/yearly_full_release_long_format.csv"
CLOUD_REGIONS="${1:-cloud_regions.json}"
OUTPUT="${2:-$PROJECT_ROOT/src/main/resources/ember/ember_co2_intensity.csv}"
GEO_CACHE="${EMBER_GEOCACHE:-./geocode_cache.tsv}"
NOMINATIM_UA="ember-cloud-region-script/1.0"

# Sub-national sources: "country_name|url|iso_3166-2_prefix". The prefix is
# prepended to Ember's bare state code so that keys match what Nominatim
# returns in address["ISO3166-2-lvl4"] (e.g. Ember "VA" -> "US-VA").
SUBNATIONAL=(
    "United States|https://files.ember-energy.org/public-downloads/us_yearly_full_release_long_format.csv|US-"
    "India|https://files.ember-energy.org/public-downloads/india_yearly_full_release_long_format.csv|IN-"
)

# Aliases for codes where Nominatim returns a different ISO 3166-2 code
# than Ember uses. Format: "ISO_code_from_nominatim|ISO_code_in_ember".
# The lookup gets a duplicate entry under the Nominatim code pointing to
# the same value.
SUBNATIONAL_ALIASES=(
    "IN-TS|IN-TG"   # Telangana: Nominatim IN-TS, Ember IN-TG
    "IN-OD|IN-OR"   # Odisha: Nominatim IN-OD, Ember IN-OR
)

for cmd in curl jq awk; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "error: required command '$cmd' not found on PATH" >&2
        exit 1
    fi
done

if [[ ! -f "$CLOUD_REGIONS" ]]; then
    echo "error: $CLOUD_REGIONS not found" >&2
    exit 1
fi

tmp_csv="$(mktemp)"
tmp_countries="$(mktemp)"
tmp_lookup="$(mktemp)"
tmp_sub_lookup="$(mktemp)"
tmp_sub_csv="$(mktemp)"
trap 'rm -f "$tmp_csv" "$tmp_countries" "$tmp_lookup" "$tmp_sub_lookup" "$tmp_sub_csv"' EXIT

echo "Downloading $URL..." >&2
curl --fail -sSL "$URL" -o "$tmp_csv"

# Unique list of country names referenced anywhere in cloud_regions.json
# (including _unresolved entries, since those are still real countries).
jq -r '
    [
        (.[] | .cloud_regions // {} | to_entries[].value.country),
        (.[] | ._unresolved // [] | .[].country)
    ]
    | map(select(. != null and . != ""))
    | unique
    | .[]
' "$CLOUD_REGIONS" > "$tmp_countries"

# Build country -> latest gCO2/kWh lookup (TSV: country<TAB>value).
# Canonicalise the one known name mismatch so the join works.
awk -F',' -v countries_file="$tmp_countries" '
    BEGIN {
        while ((getline line < countries_file) > 0) {
            if (line != "") ok[line] = 1
        }
        close(countries_file)
        if (ok["United States"]) ok["United States of America"] = 1
    }
    NR == 1 { next }
    $15 == "gCO2/kWh" && $2 != "" && ($1 in ok) {
        iso = $2
        year = $3 + 0
        if (year > best_year[iso]) {
            best_year[iso] = year
            best_country[iso] = $1
            best_value[iso] = $16
        }
    }
    END {
        for (iso in best_value) {
            name = best_country[iso]
            if (name == "United States of America") name = "United States"
            print name "\t" best_value[iso]
        }
    }
' "$tmp_csv" > "$tmp_lookup"

# Build combined sub-national lookup (TSV: ISO_3166-2_code<TAB>value) by
# pulling each configured source and prefixing bare state codes.
# Ember sub-national CSVs share a schema: col 4=State code, 6=Year, 10=Unit,
# 11=Value.
: > "$tmp_sub_lookup"
for entry in "${SUBNATIONAL[@]}"; do
    IFS='|' read -r sn_country sn_url sn_prefix <<< "$entry"
    echo "Downloading $sn_url..." >&2
    curl --fail -sSL "$sn_url" -o "$tmp_sub_csv"
    awk -F',' -v prefix="$sn_prefix" '
        NR == 1 { next }
        $10 == "gCO2/kWh" && $4 != "" {
            code = $4
            year = $6 + 0
            if (year > best_year[code]) {
                best_year[code] = year
                best_value[code] = $11
            }
        }
        END {
            for (code in best_value) print prefix code "\t" best_value[code]
        }
    ' "$tmp_sub_csv" >> "$tmp_sub_lookup"
done

# Add alias rows so Nominatim's subdivision codes resolve to Ember's values
# even when the two systems use different codes for the same region.
for alias in "${SUBNATIONAL_ALIASES[@]}"; do
    IFS='|' read -r nom_code ember_code <<< "$alias"
    val=$(awk -F'\t' -v k="$ember_code" '$1==k {print $2; exit}' "$tmp_sub_lookup")
    if [[ -n "$val" ]]; then
        printf '%s\t%s\n' "$nom_code" "$val" >> "$tmp_sub_lookup"
    fi
done

# Check whether a country has a sub-national source configured.
has_subnational() {
    local c="$1" entry sn_country
    for entry in "${SUBNATIONAL[@]}"; do
        IFS='|' read -r sn_country _ _ <<< "$entry"
        [[ "$sn_country" == "$c" ]] && return 0
    done
    return 1
}

# Reverse-geocode (lat, lon) -> ISO 3166-2 subdivision code via Nominatim,
# with caching. Returns empty if no subdivision can be resolved.
touch "$GEO_CACHE"
geo_to_subdivision() {
    local lat="$1" lon="$2"
    local hit
    hit=$(awk -F'\t' -v lat="$lat" -v lon="$lon" '
        $1 == lat && $2 == lon { print $3; found=1; exit }
        END { if (!found) exit 1 }
    ' "$GEO_CACHE") && { printf '%s' "$hit"; return; }

    sleep 1  # respect Nominatim 1 req/sec policy
    local resp code
    resp=$(curl --fail -sSL -A "$NOMINATIM_UA" \
        "https://nominatim.openstreetmap.org/reverse?format=jsonv2&zoom=5&lat=${lat}&lon=${lon}" \
        2>/dev/null || echo '{}')
    code=$(jq -r '.address["ISO3166-2-lvl4"] // ""' <<< "$resp")
    # Validate shape: "XX-..." where XX is a 2-letter country code.
    if [[ ! "$code" =~ ^[A-Z]{2}-[A-Z0-9]+$ ]]; then code=""; fi
    printf '%s\t%s\t%s\n' "$lat" "$lon" "$code" >> "$GEO_CACHE"
    printf '%s' "$code"
}

lookup_value() {
    awk -F'\t' -v key="$1" -v file="$2" '
        BEGIN {
            while ((getline line < file) > 0) {
                split(line, a, "\t")
                if (a[1] == key) { print a[2]; exit }
            }
        }
    '
}

mkdir -p "$(dirname "$OUTPUT")"

# Emit a row per keyed cloud region.
{
    echo "# https://ember-energy.org/creative-commons/"
    echo "# Creative Commons Attribution Licence (CC-BY-4.0)"
    echo "#provider,region,gCO2_per_kWh"
    while IFS=$'\t' read -r provider region country lat lon; do
        value=""
        if [[ -n "$lat" && -n "$lon" ]] && has_subnational "$country"; then
            code=$(geo_to_subdivision "$lat" "$lon")
            if [[ -n "$code" ]]; then
                value=$(lookup_value "$code" "$tmp_sub_lookup")
            fi
        fi
        if [[ -z "$value" ]]; then
            value=$(lookup_value "$country" "$tmp_lookup")
        fi
        if [[ -n "$value" ]]; then
            echo "$provider,$region,$value"
        fi
    done < <(jq -r '
        ["aws","gcp","azure"][] as $p
        | .[$p].cloud_regions // {}
        | to_entries[]
        | [$p, .key, .value.country, .value.latitude, .value.longitude] | @tsv
    ' "$CLOUD_REGIONS")
} > "$OUTPUT"

rows=$(($(wc -l < "$OUTPUT") - 1))
echo "Wrote $OUTPUT ($rows rows)" >&2
