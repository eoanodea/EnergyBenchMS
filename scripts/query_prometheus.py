#!/usr/bin/env python3
"""Extract Prometheus metrics for a completed experiment run."""

import argparse
import json
from datetime import datetime
from pathlib import Path

import requests


ENERGY_QUERY = (
    'sum by (container_name) ('
    'rate(kepler_container_cpu_joules_total{container_name!=""}[1m])'
    ')'
)

CPU_QUERY = (
    'sum by (container_name) ('
    'rate(container_cpu_usage_seconds_total[1m])'
    ')'
)


def load_metadata(run_dir):
    """Load experiment metadata from the run directory."""
    metadata_path = Path(run_dir) / "metadata.json"
    print("Loading metadata")
    with metadata_path.open("r", encoding="utf-8") as metadata_file:
        return json.load(metadata_file)


def extract_timestamp(metadata, key):
    """Read a timestamp from metadata, supporting nested or flat layouts."""
    if key in metadata:
        return metadata[key]

    timestamps = metadata.get("timestamps", {})
    if key in timestamps:
        return timestamps[key]

    raise KeyError(f"Missing '{key}' in metadata")


def to_unix_seconds(timestamp_value):
    """Convert an ISO timestamp or datetime object to UNIX seconds."""
    if isinstance(timestamp_value, (int, float)):
        return float(timestamp_value)

    if isinstance(timestamp_value, str):
        parsed_value = datetime.fromisoformat(timestamp_value)
        return parsed_value.timestamp()

    raise TypeError(f"Unsupported timestamp value: {timestamp_value!r}")


def query_prometheus(prom_url, query, start, end, step="5s"):
    """Run a Prometheus range query and return the parsed JSON response."""
    response = requests.get(
        f"{prom_url.rstrip('/')}/api/v1/query_range",
        params={
            "query": query,
            "start": start,
            "end": end,
            "step": step,
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def save_results(run_dir, filename, payload):
    """Save a Prometheus response into the run directory."""
    output_path = Path(run_dir) / filename
    with output_path.open("w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="Query Prometheus for metrics from a completed experiment run"
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to a completed run directory (for example, runs/20260413_173526)",
    )
    parser.add_argument(
        "--prom-url",
        required=True,
        help="Base URL of Prometheus (for example, http://192.168.0.100:9090)",
    )

    args = parser.parse_args()
    run_dir = Path(args.run_dir)

    metadata = load_metadata(run_dir)
    workload_start = to_unix_seconds(extract_timestamp(metadata, "workload_start"))
    workload_end = to_unix_seconds(extract_timestamp(metadata, "workload_end"))

    print("Querying energy")
    energy_results = query_prometheus(
        args.prom_url,
        ENERGY_QUERY,
        workload_start,
        workload_end,
        step="5s",
    )

    print("Querying CPU")
    cpu_results = query_prometheus(
        args.prom_url,
        CPU_QUERY,
        workload_start,
        workload_end,
        step="5s",
    )

    print("Saving results")
    save_results(run_dir, "energy.json", energy_results)
    save_results(run_dir, "cpu.json", cpu_results)


if __name__ == "__main__":
    main()