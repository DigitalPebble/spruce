#!/usr/bin/env bash
#
# Retrieves the list of EC2 instance types, current and discontinued, and
# writes one instance type per line.
#
# No single API returns every instance type that ever existed, so the script
# unions three sources:
#   1. pricing get-attribute-values --attribute-name instanceType
#                                 -> every type in the current price list
#                                    catalogue, including retired families
#                                    (cc2, cr1, hs1, g2, p2 ...) and types not
#                                    offered in the account's regions.
#   2. ec2 describe-instance-type-offerings --location-type region
#                                 -> the types offered in each region today.
#                                    Adds the handful missing from the price
#                                    list catalogue.
#   3. the historical price lists of 2015 to 2017
#                                 -> types retired before the current
#                                    catalogue was assembled (cc1.4xlarge,
#                                    cg1.4xlarge, hi1.4xlarge). Public files,
#                                    no credentials needed. Later years add
#                                    nothing, as everything they list is still
#                                    in the catalogue of source 1.
#
# Sources 1 and 2 need AWS credentials. The Price List API is only served from
# a few regions, so it is queried in us-east-1, falling back to ap-south-1.
#
# By default source 2 only queries the regions enabled on the account: calling
# DescribeInstanceTypeOfferings in a region the account has not opted into
# fails with AuthFailure. --all-regions queries every region AWS advertises
# and skips the ones that error, which is only useful if the account has
# opted into the extra regions.
#
# Output: one instance type per line, sorted by family then size, preceded by
# a few '#' comment lines. That is the format expected by
# com.digitalpebble.spruce.modules.boavizta.APITester, which reads the file
# and ignores '#' lines.
#
# Usage:  ./fetch_ec2_instance_types.sh [options] [output-file]
#
#   -a, --all-regions     query every region AWS advertises, not just the ones
#                         enabled on the account
#       --current-only    only types launchable today (source 2 alone)
#       --no-history      skip the historical price lists (source 3)
#       --history-years Y comma separated years to take the historical price
#                         lists from (default 2015,2016,2017)
#
# Requires: bash 4+, awk, curl, python3, and an AWS CLI v2 with working
# credentials (AWS_PROFILE / AWS_REGION and friends are honoured as usual).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ALL_REGIONS=0
CURRENT_ONLY=0
HISTORY=1
HISTORY_YEARS="2015,2016,2017"
HISTORY_REGIONS="us-east-1,eu-west-1"
PRICING_REGIONS=(us-east-1 ap-south-1)
OUTPUT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -a|--all-regions) ALL_REGIONS=1; shift ;;
        --current-only) CURRENT_ONLY=1; HISTORY=0; shift ;;
        --no-history) HISTORY=0; shift ;;
        --history-years)
            if [[ $# -lt 2 ]]; then
                echo "error: --history-years needs a value" >&2
                exit 1
            fi
            HISTORY_YEARS="$2"; shift 2
            ;;
        -h|--help)
            sed -n '2,48p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        -*)
            echo "error: unknown option '$1'" >&2
            exit 1
            ;;
        *)
            if [[ -n "$OUTPUT" ]]; then
                echo "error: unexpected argument '$1'" >&2
                exit 1
            fi
            OUTPUT="$1"; shift
            ;;
    esac
done

OUTPUT="${OUTPUT:-$PROJECT_ROOT/instanceTypes}"

required=(aws awk)
if [[ $HISTORY -eq 1 ]]; then
    required+=(python3)
fi
for cmd in "${required[@]}"; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "error: required command '$cmd' not found on PATH" >&2
        exit 1
    fi
done

# describe-regions is itself region-scoped; fall back to us-east-1 when the
# environment has no region configured.
HOME_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
pricing_types="$tmp_dir/pricing"
offered_types="$tmp_dir/offered"
history_types="$tmp_dir/history"
: > "$pricing_types"
: > "$offered_types"
: > "$history_types"

sources=()

# ---------------------------------------------------------------- source 1
if [[ $CURRENT_ONLY -eq 0 ]]; then
    echo "Querying the Price List API for instance types..." >&2
    pricing_ok=0
    for region in "${PRICING_REGIONS[@]}"; do
        if aws pricing get-attribute-values \
            --service-code AmazonEC2 \
            --attribute-name instanceType \
            --region "$region" \
            --query 'AttributeValues[].Value' \
            --output text 2>/dev/null | tr '\t' '\n' > "$pricing_types"; then
            pricing_ok=1
            break
        fi
        echo "  warning: the Price List API failed in $region" >&2
    done
    if [[ $pricing_ok -eq 0 ]]; then
        echo "error: could not query the Price List API - check the AWS credentials" >&2
        echo "       (needs pricing:GetAttributeValues), or pass --current-only" >&2
        exit 1
    fi
    # the catalogue also holds bare family names ('c5'), which are not types
    grep '\.' "$pricing_types" | sort -u > "$pricing_types.clean"
    mv "$pricing_types.clean" "$pricing_types"
    sources+=("price list catalogue: $(grep -c . "$pricing_types") type(s)")
fi

# ---------------------------------------------------------------- source 2
if [[ $ALL_REGIONS -eq 1 ]]; then
    regions_args=(--all-regions)
else
    # Without --all-regions the API only returns regions enabled on the account.
    regions_args=()
fi

echo "Listing regions..." >&2
mapfile -t REGIONS < <(aws ec2 describe-regions "${regions_args[@]}" \
    --region "$HOME_REGION" \
    --query 'Regions[].RegionName' --output text | tr '\t' '\n' | sort)

if [[ ${#REGIONS[@]} -eq 0 ]]; then
    echo "error: no regions returned by describe-regions" >&2
    exit 1
fi

echo "Querying instance type offerings in ${#REGIONS[@]} region(s)..." >&2

queried=0
skipped=()
for region in "${REGIONS[@]}"; do
    if ! aws ec2 describe-instance-type-offerings \
        --location-type region \
        --region "$region" \
        --query 'InstanceTypeOfferings[].InstanceType' \
        --output text 2>/dev/null | tr '\t' '\n' >> "$offered_types"; then
        # Typically AuthFailure on a region the account has not opted into.
        echo "  warning: skipping $region (query failed)" >&2
        skipped+=("$region")
        continue
    fi
    queried=$((queried + 1))
done

if [[ $queried -eq 0 ]]; then
    echo "error: every region failed - check the AWS credentials" >&2
    exit 1
fi

sort -u -o "$offered_types" "$offered_types"
sources+=("offerings of $queried region(s): $(grep -c . "$offered_types") type(s)")

# ---------------------------------------------------------------- source 3
if [[ $HISTORY -eq 1 ]]; then
    echo "Reading the historical price lists of $HISTORY_YEARS..." >&2
    python3 -I - "$HISTORY_YEARS" "$HISTORY_REGIONS" <<'PY' | sort -u > "$history_types"
import csv, io, json, sys, urllib.request

BASE = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2"
years = {y.strip() for y in sys.argv[1].split(",") if y.strip()}
regions = [r.strip() for r in sys.argv[2].split(",") if r.strip()]


def get(url):
    request = urllib.request.Request(url, headers={"User-Agent": "spruce-instance-types"})
    return urllib.request.urlopen(request, timeout=120)


with get(f"{BASE}/index.json") as response:
    versions = sorted(json.load(response)["versions"])

# the first published version of each requested year is enough: a type retired
# mid-year is still listed by the version before it went away
picked, seen = [], set()
for version in versions:
    year = version[:4]
    if year in years and year not in seen:
        seen.add(year)
        picked.append(version)

for year in sorted(years - seen):
    print(f"  warning: no price list version for {year}", file=sys.stderr)

for version in picked:
    for region in regions:
        url = f"{BASE}/{version}/{region}/index.csv"
        found = set()
        try:
            with get(url) as response:
                reader = csv.reader(io.TextIOWrapper(response, encoding="utf-8", newline=""))
                index = None
                for row in reader:
                    if index is None:
                        # a few metadata lines precede the header row
                        if "Instance Type" in row:
                            index = row.index("Instance Type")
                        continue
                    if len(row) > index:
                        instance_type = row[index].strip()
                        if "." in instance_type:
                            found.add(instance_type)
        except Exception as error:
            print(f"  warning: skipping {version} {region} ({error})", file=sys.stderr)
            continue
        print(f"  {version} {region}: {len(found)} type(s)", file=sys.stderr)
        for instance_type in sorted(found):
            print(instance_type)
PY
    sources+=("historical price lists ($HISTORY_YEARS): $(grep -c . "$history_types") type(s)")
fi

mkdir -p "$(dirname "$OUTPUT")"

# Sort by family, then by size. A plain lexicographic sort would interleave
# the sizes (2xlarge before large), so rank them explicitly: nano..large, then
# Nxlarge as 5+N, with the bare-metal sizes last within their family.
sort_natural() {
    awk -F'.' '
        NF < 2 || $1 == "" { next }
        {
            size = $2
            rank = 500
            if (size == "nano")        rank = 1
            else if (size == "micro")  rank = 2
            else if (size == "small")  rank = 3
            else if (size == "medium") rank = 4
            else if (size == "large")  rank = 5
            else if (size == "xlarge") rank = 6
            else if (size ~ /^[0-9]+xlarge$/) { n = size; sub(/xlarge$/, "", n); rank = 5 + n }
            else if (size == "metal")  rank = 1000
            else if (size ~ /^metal-[0-9]+xl$/) { n = size; sub(/^metal-/, "", n); sub(/xl$/, "", n); rank = 1000 + n }
            printf "%s\t%04d\t%s\n", $1, rank, $0
        }
    ' | sort -t$'\t' -k1,1 -k2,2n -k3,3 -u | cut -f3-
}

sorted="$(cat "$pricing_types" "$offered_types" "$history_types" \
    | sort -u | sed '/^$/d' | sort_natural)"
count="$(printf '%s\n' "$sorted" | grep -c . || true)"

{
    echo "# EC2 instance types, generated by scripts/fetch_ec2_instance_types.sh"
    echo "# generated on $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if [[ $CURRENT_ONLY -eq 1 ]]; then
        echo "# types launchable today only (--current-only)"
    else
        echo "# current and discontinued types"
    fi
    for source in "${sources[@]}"; do
        echo "# union of $source"
    done
    echo "# regions queried: ${REGIONS[*]}"
    if [[ ${#skipped[@]} -gt 0 ]]; then
        echo "# skipped ${#skipped[@]} region(s): ${skipped[*]}"
    fi
    echo "# $count instance types"
    printf '%s\n' "$sorted"
} > "$OUTPUT"

echo "Wrote $OUTPUT ($count instance types)" >&2
