#!/usr/bin/env bash
#
# Downloads Ember's yearly electricity release (global + per-country sub-
# national releases for US and India) and joins them against cloud_regions.json
# to produce a per-cloud-region carbon intensity table.
#
# Filters:
#   Global and US csvs ("wide" layout, one row per area, year and electricity
#   source):
#     - Electricity source == "Total generation", i.e. the all-sources row.
#     - Emissions intensity (gCO2e/kWh) non-empty.
#   India csv ("long" layout, one row per variable):
#     - Unit == "gCO2/kWh".
#   Global csv:
#     - ISO 3 code non-empty, which drops Ember's aggregate regions (ASEAN,
#       Europe, ...), and Area in cloud_regions.json's country set, after
#       COUNTRY_ALIASES.
#   Sub-national csvs (US, India):
#     - State code non-empty.
#   All:
#     - One value per key and year, from FROM_YEAR on.
# Join:
#   - For every keyed region under aws/gcp/azure.cloud_regions, emit one
#     (provider, region_code, year, gCO2_per_kWh) row per year.
#   - For regions in a country with a sub-national source, reverse-geocode
#     the region's lat/lon via OpenStreetMap Nominatim to get the ISO 3166-2
#     subdivision code and use the sub-national value. If the subdivision
#     can't be resolved or has no Ember value, fall back to the country
#     value.
#   - Results are cached in scripts/.geocode_cache_iso (lat, lon, ISO_3166-2
#     code), kept apart from the state-name cache of the water scripts
#     so repeat runs don't hit the API.
#   - _unresolved entries are skipped (no region code).
#   - Regions with no country/subdivision match are skipped, with a warning
#     naming the country: those regions get no carbon intensity at all.
#
# Output columns: provider,region,year,gCO2_per_kWh
#
# Usage:  ./fetch_ember_co2_intensity.sh [cloud_regions.json] [output.csv]
#
# Requires: bash, curl, jq, awk.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

URL="https://files.ember-energy.org/public-downloads/generation/outputs/release_generation_yearly_global.csv"
CLOUD_REGIONS="${1:-cloud_regions.json}"
OUTPUT="${2:-$PROJECT_ROOT/src/main/resources/ember/ember_co2_intensity.csv}"
# First year of figures to ship. Usage before it gets the figures of this year (see
# AverageCarbonIntensity); the PUE / WUE file starts the same year.
FROM_YEAR=2022
GEO_CACHE="${EMBER_GEOCACHE:-$SCRIPT_DIR/.geocode_cache_iso}"
NOMINATIM_UA="ember-cloud-region-script/1.0"

# Column names and filter values of Ember's two layouts. The wide releases put
# the intensity in its own column and spread the fuels across rows; the long
# release puts every variable in one Value column, picked out by its unit.
WIDE_SOURCE_COL="Electricity source"
WIDE_TOTAL_SOURCE="Total generation"
WIDE_INTENSITY_COL="Emissions intensity (gCO2e/kWh)"
LONG_UNIT_COL="Unit"
LONG_INTENSITY_UNIT="gCO2/kWh"
LONG_VALUE_COL="Value"

# Sub-national sources: "country_name|url|iso_3166-2_prefix|layout". The prefix
# is prepended to Ember's bare state code so that keys match what Nominatim
# returns in address["ISO3166-2-lvl4"] (e.g. Ember "VA" -> "US-VA"). The layout
# is "wide" or "long"; Ember is moving its releases from long to wide and India
# is the last one still on long.
SUBNATIONAL=(
    "United States|https://files.ember-energy.org/public-downloads/generation/outputs/release_us_subnational_generation_yearly.csv|US-|wide"
    "India|https://files.ember-energy.org/public-downloads/india_yearly_full_release_long_format.csv|IN-|long"
)

# Countries Ember names differently from cloud_regions.json.
# Format: "name_in_ember|name_in_cloud_regions".
COUNTRY_ALIASES=(
    "Taiwan (China)|Taiwan"
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
tmp_country_alias="$(mktemp)"
tmp_lookup="$(mktemp)"
tmp_sub_lookup="$(mktemp)"
tmp_sub_csv="$(mktemp)"
tmp_alias="$(mktemp)"
tmp_out="$(mktemp)"
trap 'rm -f "$tmp_csv" "$tmp_countries" "$tmp_country_alias" "$tmp_lookup" "$tmp_sub_lookup" "$tmp_sub_csv" "$tmp_alias" "$tmp_out"' EXIT

# awk helpers, prepended to each awk program below.
#
# Ember quotes any field holding a comma ("Hydro, bioenergy and other
# renewables", "Washington, D.C."), which shifts every field after it under a
# plain -F','. These parse the quoting, and address columns by header name
# rather than position, since Ember's releases differ in column order and gain
# columns from one release to the next.
CSV_AWK=$(cat <<'AWK'
# Split a csv line into f[1..n], honouring quoted fields; returns n.
# A line with no quote at all takes the cheap path.
function csv_split(line, f,   n, i, c, cur, inq) {
    if (index(line, "\"") == 0) return split(line, f, ",")
    delete f
    n = 1; cur = ""; inq = 0
    for (i = 1; i <= length(line); i++) {
        c = substr(line, i, 1)
        if (inq) {
            if (c != "\"") cur = cur c
            else if (substr(line, i + 1, 1) == "\"") { cur = cur "\""; i++ }
            else inq = 0
        }
        else if (c == "\"") inq = 1
        else if (c == ",") { f[n++] = cur; cur = "" }
        else cur = cur c
    }
    f[n] = cur
    return n
}

# Record the header's column positions as col[name] -> index.
function csv_header(line, col,   f, n, i) {
    n = csv_split(line, f)
    for (i = 1; i <= n; i++) col[f[i]] = i
}

# Field "name" of the current row. Stops the script when the column is missing,
# so that a rename upstream is reported instead of emptying the output.
function csv_get(f, col, name) {
    if (!(name in col)) {
        print "error: no column \"" name "\" in " FILENAME > "/dev/stderr"
        exit 1
    }
    return f[col[name]]
}
AWK
)

# Ember replaces a retired download with a two-line csv holding a notice and
# the url of its replacement, served as 200, so curl --fail does not catch it.
check_not_retired() {
    local file="$1" url="$2"
    if [[ "$(head -1 "$file")" == notice,* ]]; then
        echo "error: Ember has retired $url" >&2
        echo "  $(sed -n '2p' "$file")" >&2
        exit 1
    fi
}

echo "Downloading $URL..." >&2
curl --fail -sSL "$URL" -o "$tmp_csv"
check_not_retired "$tmp_csv" "$URL"

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

for alias in "${COUNTRY_ALIASES[@]}"; do
    IFS='|' read -r ember_name regions_name <<< "$alias"
    printf '%s\t%s\n' "$ember_name" "$regions_name" >> "$tmp_country_alias"
done

# Build country -> gCO2/kWh by year lookup (TSV: country<TAB>year<TAB>value),
# from FROM_YEAR on.
awk -v countries_file="$tmp_countries" -v aliases_file="$tmp_country_alias" \
    -v from="$FROM_YEAR" -v source_col="$WIDE_SOURCE_COL" \
    -v total_source="$WIDE_TOTAL_SOURCE" -v intensity_col="$WIDE_INTENSITY_COL" \
    "$CSV_AWK"'
BEGIN {
    while ((getline line < countries_file) > 0) {
        if (line != "") ok[line] = 1
    }
    close(countries_file)
    while ((getline line < aliases_file) > 0) {
        if (split(line, a, "\t") == 2) rename[a[1]] = a[2]
    }
    close(aliases_file)
}
NR == 1 { csv_header($0, col); next }
{
    csv_split($0, f)
    if (csv_get(f, col, "ISO 3 code") == "") next
    if (csv_get(f, col, source_col) != total_source) next
    year = csv_get(f, col, "Year") + 0
    if (year < from) next
    value = csv_get(f, col, intensity_col)
    if (value == "") next
    name = csv_get(f, col, "Area")
    if (name in rename) name = rename[name]
    if (!(name in ok)) next
    print name "\t" year "\t" value
}
' "$tmp_csv" > "$tmp_lookup"

# Build combined sub-national lookup (TSV: ISO_3166-2_code<TAB>year<TAB>value)
# by pulling each configured source and prefixing bare state codes.
: > "$tmp_sub_lookup"
for entry in "${SUBNATIONAL[@]}"; do
    IFS='|' read -r sn_country sn_url sn_prefix sn_layout <<< "$entry"
    case "$sn_layout" in
        wide)
            sn_filter_col="$WIDE_SOURCE_COL"
            sn_filter_value="$WIDE_TOTAL_SOURCE"
            sn_value_col="$WIDE_INTENSITY_COL"
            ;;
        long)
            sn_filter_col="$LONG_UNIT_COL"
            sn_filter_value="$LONG_INTENSITY_UNIT"
            sn_value_col="$LONG_VALUE_COL"
            ;;
        *)
            echo "error: unknown layout '$sn_layout' for $sn_country" >&2
            exit 1
            ;;
    esac
    echo "Downloading $sn_url..." >&2
    curl --fail -sSL "$sn_url" -o "$tmp_sub_csv"
    check_not_retired "$tmp_sub_csv" "$sn_url"
    awk -v prefix="$sn_prefix" -v from="$FROM_YEAR" -v filter_col="$sn_filter_col" \
        -v filter_value="$sn_filter_value" -v value_col="$sn_value_col" \
        "$CSV_AWK"'
NR == 1 { csv_header($0, col); next }
{
    csv_split($0, f)
    code = csv_get(f, col, "State code")
    if (code == "") next
    if (csv_get(f, col, filter_col) != filter_value) next
    year = csv_get(f, col, "Year") + 0
    if (year < from) next
    value = csv_get(f, col, value_col)
    if (value == "") next
    print prefix code "\t" year "\t" value
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
        IFS='|' read -r sn_country _ <<< "$entry"
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
# Regions whose country has no Ember figure at all, by country. Those get no
# carbon intensity, which is worth saying out loud but not worth stopping for.
declare -A missing_countries=()

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
        else
            missing_countries["$country"]+=" $provider/$region"
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
if (( ${#missing_countries[@]} )); then
    echo "warning: no Ember figure for these countries, their regions get none:" >&2
    for country in "${!missing_countries[@]}"; do
        printf '  %s:%s\n' "$country" "${missing_countries[$country]}" >&2
    done
fi
mv "$tmp_out" "$OUTPUT"

rows=$(($(wc -l < "$OUTPUT") - 1))
echo "Wrote $OUTPUT ($rows rows)" >&2
