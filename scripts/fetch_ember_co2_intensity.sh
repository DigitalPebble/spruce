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
#   - One value per ISO3 and year, from FROM_YEAR on.
# Sub-national CSVs (US, India):
#   - State code non-empty.
#   - One value per state code and year, from FROM_YEAR on.
# Join:
#   - For every keyed region under aws/gcp/azure.cloud_regions, emit one
#     (provider, region_code, year, gCO2_per_kWh) row per year.
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
# Output columns: provider,region,year,gCO2_per_kWh
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
# First year of figures to ship. Usage before it gets the figures of this year (see
# AverageCarbonIntensity); the PUE / WUE file starts the same year.
FROM_YEAR=2022
GEO_CACHE="${EMBER_GEOCACHE:-$SCRIPT_DIR/.geocode_cache}"
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
tmp_alias="$(mktemp)"
tmp_out="$(mktemp)"
trap 'rm -f "$tmp_csv" "$tmp_countries" "$tmp_lookup" "$tmp_sub_lookup" "$tmp_sub_csv" "$tmp_alias" "$tmp_out"' EXIT

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

# Build country -> gCO2/kWh by year lookup (TSV: country<TAB>year<TAB>value),
# from FROM_YEAR on. Canonicalise the one known name mismatch so the join works.
awk -F',' -v countries_file="$tmp_countries" -v from="$FROM_YEAR" '
    BEGIN {
        while ((getline line < countries_file) > 0) {
            if (line != "") ok[line] = 1
        }
        close(countries_file)
        if (ok["United States"]) ok["United States of America"] = 1
    }
    NR == 1 { next }
    $15 == "gCO2/kWh" && $2 != "" && ($1 in ok) && $3 + 0 >= from {
        name = $1
        if (name == "United States of America") name = "United States"
        print name "\t" $3 "\t" $16
    }
' "$tmp_csv" > "$tmp_lookup"

# Build combined sub-national lookup (TSV: ISO_3166-2_code<TAB>year<TAB>value)
# by pulling each configured source and prefixing bare state codes.
# Ember sub-national CSVs share a schema: State code, Year, Unit and Value are
# the 4th, 6th, 10th and 11th of 13 columns. A state name with a comma
# ("Washington, D.C.") is quoted and shifts the columns after it, so the fields
# are counted from the end of the line: the last three columns are Value,
# YoY absolute change and YoY % change.
: > "$tmp_sub_lookup"
for entry in "${SUBNATIONAL[@]}"; do
    IFS='|' read -r sn_country sn_url sn_prefix <<< "$entry"
    echo "Downloading $sn_url..." >&2
    curl --fail -sSL "$sn_url" -o "$tmp_sub_csv"
    awk -F',' -v prefix="$sn_prefix" -v from="$FROM_YEAR" '
        NR == 1 { next }
        $(NF-3) == "gCO2/kWh" && $(NF-9) != "" && $(NF-7) + 0 >= from {
            print prefix $(NF-9) "\t" $(NF-7) "\t" $(NF-2)
        }
    ' "$tmp_sub_csv" >> "$tmp_sub_lookup"
done

# Add alias rows so Nominatim's subdivision codes resolve to Ember's values
# even when the two systems use different codes for the same region.
for alias in "${SUBNATIONAL_ALIASES[@]}"; do
    IFS='|' read -r nom_code ember_code <<< "$alias"
    awk -F'\t' -v k="$ember_code" -v n="$nom_code" -v OFS='\t' '$1 == k {print n, $2, $3}' \
        "$tmp_sub_lookup" >> "$tmp_alias"
done
cat "$tmp_alias" >> "$tmp_sub_lookup"

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
# with caching. Prints the code, or nothing when Nominatim places the point in
# no subdivision; returns 1 when the request fails. Only resolved codes are
# cached, so a failed request is retried on the next run.
touch "$GEO_CACHE"
geo_to_subdivision() {
    local lat="$1" lon="$2"
    local hit
    hit=$(awk -F'\t' -v lat="$lat" -v lon="$lon" '
        $1 == lat && $2 == lon && $3 != "" { print $3; found=1; exit }
        END { if (!found) exit 1 }
    ' "$GEO_CACHE") && { printf '%s' "$hit"; return; }

    sleep 1  # respect Nominatim 1 req/sec policy
    local resp code
    resp=$(curl --fail -sSL -A "$NOMINATIM_UA" \
        "https://nominatim.openstreetmap.org/reverse?format=jsonv2&zoom=5&lat=${lat}&lon=${lon}" \
        2>/dev/null) || return 1
    code=$(jq -r '.address["ISO3166-2-lvl4"] // ""' <<< "$resp")
    # Validate shape: "XX-..." where XX is a 2-letter country code.
    if [[ ! "$code" =~ ^[A-Z]{2}-[A-Z0-9]+$ ]]; then code=""; fi
    if [[ -n "$code" ]]; then
        printf '%s\t%s\t%s\n' "$lat" "$lon" "$code" >> "$GEO_CACHE"
    fi
    printf '%s' "$code"
}

# Print "year<TAB>value" lines for a key, in year order.
lookup_years() {
    awk -F'\t' -v key="$1" '$1 == key { print $2 "\t" $3 }' "$2" | sort -n
}

mkdir -p "$(dirname "$OUTPUT")"

# Regions of a country with state figures that would get the national figure,
# each with the reason. The csv is written only when there are none.
problems=()

# Emit a row per keyed cloud region.
{
    echo "# https://ember-energy.org/creative-commons/"
    echo "# Creative Commons Attribution Licence (CC-BY-4.0)"
    echo "#provider,region,year,gCO2_per_kWh"
    while IFS=$'\t' read -r provider region country lat lon; do
        years=""
        if has_subnational "$country"; then
            if [[ -z "$lat" || -z "$lon" ]]; then
                problems+=("$provider $region: no coordinates in $CLOUD_REGIONS")
            elif ! code=$(geo_to_subdivision "$lat" "$lon"); then
                problems+=("$provider $region: Nominatim request failed for $lat,$lon")
            elif [[ -z "$code" ]]; then
                problems+=("$provider $region: no subdivision at $lat,$lon")
            else
                years=$(lookup_years "$code" "$tmp_sub_lookup")
                if [[ -z "$years" ]]; then
                    problems+=("$provider $region: no Ember figure for $code")
                fi
            fi
        fi
        if [[ -z "$years" ]]; then
            years=$(lookup_years "$country" "$tmp_lookup")
        fi
        if [[ -n "$years" ]]; then
            awk -F'\t' -v p="$provider" -v r="$region" '{ print p "," r "," $1 "," $2 }' <<< "$years"
        fi
    done < <(jq -r '
        ["aws","gcp","azure"][] as $p
        | .[$p].cloud_regions // {}
        | to_entries[]
        | [$p, .key, .value.country, .value.latitude, .value.longitude] | @tsv
    ' "$CLOUD_REGIONS")
} > "$tmp_out"

if (( ${#problems[@]} )); then
    echo "error: these regions would get the national figure instead of their state's:" >&2
    printf '  %s\n' "${problems[@]}" >&2
    echo "$OUTPUT left unchanged" >&2
    exit 1
fi
mv "$tmp_out" "$OUTPUT"

rows=$(($(wc -l < "$OUTPUT") - 1))
echo "Wrote $OUTPUT ($rows rows)" >&2
