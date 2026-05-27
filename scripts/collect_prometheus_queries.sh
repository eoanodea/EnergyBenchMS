#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 PROM_URL START_EPOCH END_EPOCH OUTDIR

Downloads a set of Prometheus query results useful to validate kepler/kube labels
and saves them as JSON files into OUTDIR.

Example:
  $0 http://localhost:9090 1672531200 1672534800 ./prom_queries
EOF
}

if [ "$#" -lt 4 ]; then
  usage
  exit 1
fi

PROM=$1
START=$2
END=$3
OUTDIR=$4

mkdir -p "$OUTDIR"

curl_series() {
  local match="$1" out="$2"
  curl -sG "${PROM}/api/v1/series" \
    --data-urlencode "match[]=${match}" \
    --data-urlencode "start=${START}" \
    --data-urlencode "end=${END}" \
    -o "${OUTDIR}/${out}"
}

curl_query_range() {
  local q="$1" out="$2"
  curl -sG "${PROM}/api/v1/query_range" \
    --data-urlencode "query=${q}" \
    --data-urlencode "start=${START}" \
    --data-urlencode "end=${END}" \
    --data-urlencode "step=60" \
    -o "${OUTDIR}/${out}"
}

curl_query() {
  local q="$1" out="$2"
  curl -sG "${PROM}/api/v1/query" \
    --data-urlencode "query=${q}" \
    -o "${OUTDIR}/${out}"
}

echo "Writing Prometheus query outputs to ${OUTDIR}"

curl_series 'kepler_container_cpu_joules_total' kepler_series_all.json
curl_series 'kepler_container_cpu_joules_total{pod_id!=""}' kepler_series_with_pod_id.json
curl_series 'kepler_container_cpu_joules_total{runtime="docker"}' kepler_series_runtime_docker.json
curl_query_range 'sum by (namespace,pod,container_name,pod_id,container_id) (increase(kepler_container_cpu_joules_total[5m]))' kepler_ns_pod_container.json
curl_query_range 'sum by (container_name) (increase(kepler_container_cpu_joules_total[5m]))' kepler_by_container_name.json
curl_query 'count by (container_name,container_id,pod,pod_id,namespace,runtime) (kepler_container_cpu_joules_total)' kepler_counts_by_labels.json

curl_series 'kube_pod_info' kube_pod_info.json
curl_series 'kube_pod_container_info' kube_pod_container_info.json

curl_query_range 'sum by (container_name,container_id,pod,namespace) (increase(container_cpu_usage_seconds_total[5m]))' cpu_by_id_all.json

echo "Done. Files written to ${OUTDIR}"

exit 0
