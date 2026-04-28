#!/usr/bin/env bash
#
# Fetches cloud region data from cloudinfrastructuremap.com for AWS, GCP, and
# Azure, then merges the three into a single JSON file keyed by provider.
# Within each provider, regions live under a "cloud_regions" object keyed by
# region code, kept separate from the _source and _license metadata fields.
#
# Output shape:
#   {
#     "aws": {
#       "_source": "...",
#       "_license": "...",
#       "cloud_regions": {
#         "us-east-1": {...},
#         "us-west-2": {...},
#         ...
#       }
#     },
#     "gcp": { ... },
#     "azure": { ... }
#   }
#
# Requires: bash 4+, curl, jq.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

OUTPUT_FILE="${1:-$PROJECT_ROOT/src/main/resources/cloud_regions.json}"
LICENSE_TEXT="Creative Commons Attribution-ShareAlike 4.0 International -- CC BY-SA 4.0"

# Provider key -> source URL. Parallel arrays to guarantee iteration order.
PROVIDERS=(aws gcp azure)
URLS=(
    "https://www.cloudinfrastructuremap.com/api/cloud-service-provider/amazon-web-services.js"
    "https://www.cloudinfrastructuremap.com/api/cloud-service-provider/google-cloud.js"
    "https://www.cloudinfrastructuremap.com/api/cloud-service-provider/microsoft-azure.js"
)

# Scratch dir in the current working directory so it's easy to inspect on
# failure. On success we remove it at the end; on failure the trap leaves it
# behind for debugging.
TMP_DIR="$(mktemp -d ./tmp.XXXXXX)"
echo "temp dir: $TMP_DIR" >&2

# Check prerequisites up front so we fail fast with a clear message.
for cmd in curl jq; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "error: required command '$cmd' not found on PATH" >&2
        exit 1
    fi
done

combined_args=()
combined_filter='{'

for i in "${!PROVIDERS[@]}"; do
    provider="${PROVIDERS[$i]}"
    url="${URLS[$i]}"
    raw_file="$TMP_DIR/${provider}.json"
    processed_file="$TMP_DIR/${provider}.processed.json"

    echo "Fetching ${provider} from ${url}..." >&2

    if ! curl --fail -sSL "$url" -o "$raw_file"; then
        echo "error: failed to download $url" >&2
        exit 1
    fi

    if ! jq empty "$raw_file" >/dev/null 2>&1; then
        echo "error: response from $url is not valid JSON" >&2
        exit 1
    fi

    # Transform the provider payload:
    #   1. Metadata fields (_source, _license) at the top.
    #   2. cloud_regions as its own nested object keyed by region code.
    #   3. Entries with null/empty cloud_region can't be used as keys, so park
    #      them under _unresolved to be fixed up later.
    #   4. Remove the now-redundant cloud_region field from keyed entries.
    jq \
        --arg source "$url" \
        --arg license "$LICENSE_TEXT" \
        '
        . as $in
        | ($in.cloud_regions | map(select(.cloud_region != null and .cloud_region != ""))) as $keyed
        | ($in.cloud_regions | map(select(.cloud_region == null or .cloud_region == ""))) as $unresolved
        | {
            _source: $source,
            _license: $license,
            _unresolved: $unresolved,
            cloud_regions: (
                $keyed
                | map({ (.cloud_region): (. | del(.cloud_region)) })
                | add
                // {}
            )
        }
        ' "$raw_file" > "$processed_file"

    # Stage this provider into the combined-output jq invocation.
    combined_args+=(--slurpfile "$provider" "$processed_file")
    if [[ $i -gt 0 ]]; then
        combined_filter+=','
    fi
    # --slurpfile wraps the file in a single-element array, so we dereference [0].
    combined_filter+="${provider}: \$${provider}[0]"
done

combined_filter+='}'

jq -n "${combined_args[@]}" "$combined_filter" > "$OUTPUT_FILE"

rm -rf "$TMP_DIR"

echo "Wrote $OUTPUT_FILE" >&2