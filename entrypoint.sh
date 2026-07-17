#!/bin/bash
set -e

case "$1" in
  report)
    shift
    exec /opt/spruce/venv/bin/python /opt/spruce/reporting/report.py "$@"
    ;;
  dashboard)
    shift
    exec /opt/spruce/venv/bin/streamlit run /opt/spruce/reporting/dashboard.py \
      --server.address=0.0.0.0 --server.headless=true \
      --browser.gatherUsageStats=false -- "$@"
    ;;
  *)
    exec /opt/spark/bin/spark-submit --driver-memory 4g --master "local[*]" \
      /usr/local/lib/spruce.jar "$@"
    ;;
esac
