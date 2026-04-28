#!/usr/bin/env bash
#
# Post-processes cloud_regions.json to fix known upstream issues:
#   - Fills in cloud_region codes for entries that arrive as null.
#   - Corrects misassigned region codes (e.g. AWS ap-east-2 was pointing at
#     Auckland; it belongs to Taipei, and Auckland is ap-southeast-6).
#   - Swaps AWS us-west-1 and us-west-2, which are swapped upstream.
#   - Fixes Azure East US coordinates (upstream points both East US and
#     East US 2 at Richmond; East US is actually Ashburn/Sterling).
#   - Fixes AWS us-east-1 coordinates (upstream points to Washington DC;
#     the region is actually in Ashburn/Loudoun County, VA).
#   - Fixes AWS cn-northwest-1 (upstream labels it "Ningxiang" in Hunan;
#     the region is in Ningxia, near Yinchuan).
#   - Fixes AWS ca-central-1 coordinates (upstream points ~50 km west of
#     Montréal Island; the region is in Montréal).
#
# Input and output default to cloud_regions.json (edited in place via a temp
# file). Pass a path to override.
#
# Requires: jq.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

FILE="${1:-$PROJECT_ROOT/src/main/resources/cloud_regions.json}"

if ! command -v jq >/dev/null 2>&1; then
    echo "error: jq not found on PATH" >&2
    exit 1
fi

if [[ ! -f "$FILE" ]]; then
    echo "error: $FILE not found" >&2
    exit 1
fi

tmp="$(mktemp "${FILE}.fixed.XXXXXX")"
trap 'rm -f "$tmp"' EXIT

jq '
    # Helper: pull an entry out of a provider _unresolved list by matching
    # on the "name" field and return [remaining_unresolved, matched_entry].
    def extract(provider; match_name):
        (.[provider]._unresolved | map(select(.name == match_name)) | .[0]) as $hit
        | .[provider]._unresolved |= map(select(.name != match_name))
        | .[provider].cloud_regions[match_name] = $hit   # stash under name temporarily
        ;

    # --- AWS fixes -------------------------------------------------------
    # Move Auckland out of ap-east-2 (it belongs at ap-southeast-6).
    .aws.cloud_regions["ap-southeast-6"] = (.aws.cloud_regions["ap-east-2"])
    | del(.aws.cloud_regions["ap-east-2"])

    # Promote AWS unresolved entries into their correct region codes.
    | (.aws._unresolved | map(select(.name == "Malaysia")) | .[0]) as $my
    | (.aws._unresolved | map(select(.name == "AWS Mexico central")) | .[0]) as $mx
    | (.aws._unresolved | map(select(.name == "Asia Pacific (Taipei)")) | .[0]) as $tw
    | .aws.cloud_regions["ap-southeast-5"] = ($my | del(.cloud_region))
    | .aws.cloud_regions["mx-central-1"]   = ($mx | del(.cloud_region))
    | .aws.cloud_regions["ap-east-2"]      = ($tw | del(.cloud_region))
    | .aws._unresolved |= map(select(
            .name != "Malaysia"
        and .name != "AWS Mexico central"
        and .name != "Asia Pacific (Taipei)"
      ))

    # Swap us-west-1 and us-west-2 (upstream has them reversed).
    | (.aws.cloud_regions["us-west-1"]) as $uw1
    | (.aws.cloud_regions["us-west-2"]) as $uw2
    | .aws.cloud_regions["us-west-1"] = $uw2
    | .aws.cloud_regions["us-west-2"] = $uw1

    # us-east-1 is Ashburn/Loudoun County, VA, not Washington DC.
    | .aws.cloud_regions["us-east-1"].latitude   = "39.043757"
    | .aws.cloud_regions["us-east-1"].longitude  = "-77.487442"
    | .aws.cloud_regions["us-east-1"].metro_area = "Ashburn"

    # cn-northwest-1 is in Ningxia (Yinchuan), not Ningxiang in Hunan.
    | .aws.cloud_regions["cn-northwest-1"].latitude   = "38.487193"
    | .aws.cloud_regions["cn-northwest-1"].longitude  = "106.230909"
    | .aws.cloud_regions["cn-northwest-1"].metro_area = "Yinchuan"

    # ca-central-1 is Montréal, not Saint-Lazare ~50 km west of the island.
    | .aws.cloud_regions["ca-central-1"].latitude   = "45.501690"
    | .aws.cloud_regions["ca-central-1"].longitude  = "-73.567253"

    # --- GCP fixes -------------------------------------------------------
    | (.gcp._unresolved | map(select(.name == "Mexico")) | .[0]) as $gmx
    | (.gcp._unresolved | map(select(.name == "Thailand")) | .[0]) as $gth
    | .gcp.cloud_regions["northamerica-south1"] = ($gmx | del(.cloud_region))
    | .gcp.cloud_regions["asia-southeast3"]     = ($gth | del(.cloud_region))
    | .gcp._unresolved |= map(select(.name != "Mexico" and .name != "Thailand"))

    # --- Azure fixes -----------------------------------------------------
    # East US is Ashburn/Sterling (Loudoun County, VA), not Richmond.
    | .azure.cloud_regions.eastus.latitude   = "39.043757"
    | .azure.cloud_regions.eastus.longitude  = "-77.487442"
    | .azure.cloud_regions.eastus.metro_area = "Ashburn"
    | .azure.cloud_regions.eastus.name       = "East US (Ashburn)"
' "$FILE" > "$tmp"

mv "$tmp" "$FILE"
trap - EXIT

echo "Applied fixes to $FILE" >&2
