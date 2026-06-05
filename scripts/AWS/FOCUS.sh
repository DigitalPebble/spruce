#!/usr/bin/env bash
set -euo pipefail

pip install ijson

BASE_URL="https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws"
 
OFFER_CODES=(
  "AmazonEC2"
  "AmazonS3"
)
 
for offer_code in "${OFFER_CODES[@]}"; do
  url="${BASE_URL}/${offer_code}/current/index.json"
  json_file="${offer_code}.json"
  ndjson_file="${offer_code}.ndjson"
 
  if [ -f "$json_file" ]; then
    echo "Skipping download, $json_file already exists"
  else
    echo "Downloading $offer_code..."
    curl -fSL --progress-bar "$url" -o "$json_file"
  fi

  if [ -f "$ndjson_file" ]; then
    echo "Skipping conversion, $ndjson_file already exists"
    continue
  fi
  
  echo "Converting to NDJSON..."
  python3 - "$json_file" "$ndjson_file" << 'PYEOF'
import ijson, json, sys
 
in_file, out_file = sys.argv[1], sys.argv[2]
 
with open(in_file, 'rb') as f, open(out_file, 'w') as out:
    count = 0
    for key, value in ijson.kvitems(f, 'products'):
        out.write(json.dumps(value) + '\n')
        count += 1
        if count % 10000 == 0:
            print(f"  {count} records...", flush=True)
    print(f"Done: {count} records")
PYEOF
 
  echo "Written to $ndjson_file"
done
 
