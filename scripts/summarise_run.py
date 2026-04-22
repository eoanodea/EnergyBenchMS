#!/usr/bin/env python3
"""Summarise CPU and energy metrics for a completed experiment run."""

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path


def load_json(path):
    with Path(path).open("r", encoding="utf-8") as infile:
        return json.load(infile)


def to_unix_seconds(timestamp_value):
    if isinstance(timestamp_value, (int, float)):
        return float(timestamp_value)
    if isinstance(timestamp_value, str):
        return datetime.fromisoformat(timestamp_value).timestamp()
    raise TypeError(f"Unsupported timestamp value: {timestamp_value!r}")


def parse_locust_timestamp(raw_value):
    if raw_value in (None, ""):
        return None
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        pass

    try:
        return datetime.fromisoformat(str(raw_value)).timestamp()
    except ValueError:
        return None


def safe_float(raw_value):
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return None


def parse_prometheus_by_label(prometheus_payload, label_name, min_timestamp=None):
    """Parse Prometheus query_range response into label value -> list of floats."""
    series = {}
    results = prometheus_payload.get("data", {}).get("result", [])

    for result in results:
        metric = result.get("metric", {})
        label_value = metric.get(label_name, "")
        if not label_value:
            continue

        values = result.get("values", [])
        for point in values:
            if not isinstance(point, list) or len(point) < 2:
                continue
            try:
                timestamp = float(point[0])
            except (TypeError, ValueError):
                continue
            if min_timestamp is not None and timestamp < min_timestamp:
                continue
            raw_value = point[1]
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            series.setdefault(label_value, []).append(value)

    return series


def parse_prometheus_single_series(prometheus_payload, min_timestamp=None):
    """Parse a Prometheus response containing a single unlabeled series."""
    results = prometheus_payload.get("data", {}).get("result", [])
    if not results:
        return []

    values = results[0].get("values", [])
    series = []
    for point in values:
        if not isinstance(point, list) or len(point) < 2:
            continue
        try:
            timestamp = float(point[0])
        except (TypeError, ValueError):
            continue
        if min_timestamp is not None and timestamp < min_timestamp:
            continue
        raw_value = point[1]
        try:
            series.append(float(raw_value))
        except (TypeError, ValueError):
            continue
    return series


def compute_stats(series_by_container):
    """Compute mean and max for each container in parsed series data."""
    stats = {}
    for container_name, values in series_by_container.items():
        if not values:
            continue
        stats[container_name] = {
            "mean": sum(values) / len(values),
            "max": max(values),
        }
    return stats


def parse_locust_workload_metrics(run_dir, min_timestamp=None):
    """Summarise workload metrics from Locust history over the effective window."""
    history_path = Path(run_dir) / "locust_stats_history.csv"
    if not history_path.exists():
        return {}

    requests_per_second = []
    p95_values = []
    sum_req_rate = 0.0
    sum_fail_rate = 0.0

    with history_path.open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            name = (row.get("Name") or "").strip().lower()
            row_type = (row.get("Type") or "").strip().lower()
            if name != "aggregated" and row_type != "aggregated":
                continue

            row_timestamp = parse_locust_timestamp(row.get("Timestamp"))
            if min_timestamp is not None and row_timestamp is not None and row_timestamp < min_timestamp:
                continue

            req_rate = safe_float(row.get("Requests/s"))
            fail_rate = safe_float(row.get("Failures/s"))
            p95 = safe_float(row.get("95%"))

            if req_rate is not None:
                requests_per_second.append(req_rate)
                sum_req_rate += req_rate
            if fail_rate is not None:
                sum_fail_rate += fail_rate
            if p95 is not None:
                p95_values.append(p95)

    if not requests_per_second:
        return {}

    throughput_mean_rps = sum(requests_per_second) / len(requests_per_second)
    error_rate = 0.0
    if sum_req_rate > 0:
        error_rate = max(0.0, min(1.0, sum_fail_rate / sum_req_rate))

    workload_summary = {
        "throughput_mean_rps": throughput_mean_rps,
        "error_rate": error_rate,
    }

    if p95_values:
        workload_summary["p95_latency"] = sum(p95_values) / len(p95_values)

    return workload_summary


def build_summary(energy_stats, cpu_k8s_stats, cpu_total_stats, workload_summary):
    """Build the final output structure with separate metric groupings."""
    summary = {
        "energy_by_container_name": {
            container_name: {
                "mean": stats["mean"],
                "max": stats["max"],
            }
            for container_name, stats in sorted(energy_stats.items())
        },
        "cpu_k8s_by_id": {
            container_id: {
                "mean": stats["mean"],
                "max": stats["max"],
            }
            for container_id, stats in sorted(cpu_k8s_stats.items())
        },
        "cpu_total": cpu_total_stats,
    }
    if workload_summary:
        summary["workload"] = workload_summary
    return summary


def load_energy_source_info(run_dir):
    """Load selected energy source metadata when available."""
    query_info_path = Path(run_dir) / "query_info.json"
    if not query_info_path.exists():
        return {}

    try:
        query_info = load_json(query_info_path)
    except (OSError, json.JSONDecodeError):
        return {}

    selected = query_info.get("selected_energy_source")
    requested = query_info.get("requested_energy_source")
    result = {}
    if selected:
        result["selected_energy_source"] = selected
    if requested:
        result["requested_energy_source"] = requested
    return result


def save_summary_json(run_dir, summary):
    output_path = Path(run_dir) / "summary.json"
    with output_path.open("w", encoding="utf-8") as outfile:
        json.dump(summary, outfile, indent=2)


def save_summary_csv(run_dir, summary):
    output_path = Path(run_dir) / "summary.csv"
    fieldnames = [
        "group",
        "label",
        "cpu_mean",
        "cpu_max",
        "energy_mean",
        "energy_max",
    ]

    with output_path.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for container_name in sorted(summary.get("energy_by_container_name", {})):
            energy_entry = summary["energy_by_container_name"][container_name]
            row = {
                "group": "energy_by_container_name",
                "label": container_name,
                "cpu_mean": "",
                "cpu_max": "",
                "energy_mean": energy_entry["mean"],
                "energy_max": energy_entry["max"],
            }
            writer.writerow(row)

        for container_id in sorted(summary.get("cpu_k8s_by_id", {})):
            cpu_entry = summary["cpu_k8s_by_id"][container_id]
            row = {
                "group": "cpu_k8s_by_id",
                "label": container_id,
                "cpu_mean": cpu_entry["mean"],
                "cpu_max": cpu_entry["max"],
                "energy_mean": "",
                "energy_max": "",
            }
            writer.writerow(row)

        cpu_total = summary.get("cpu_total", {})
        if cpu_total:
            row = {
                "group": "cpu_total",
                "label": "total",
                "cpu_mean": cpu_total.get("mean", ""),
                "cpu_max": cpu_total.get("max", ""),
                "energy_mean": "",
                "energy_max": "",
            }
            writer.writerow(row)


def get_cpu_payload_path(run_dir):
    """Prefer cpu_k8s_by_id output, fallback to cpu_by_container or legacy cpu.json."""
    preferred = Path(run_dir) / "cpu_k8s_by_id.json"
    if preferred.exists():
        return preferred

    fallback = Path(run_dir) / "cpu_by_container.json"
    if fallback.exists():
        return fallback

    return Path(run_dir) / "cpu.json"


def get_cpu_total_payload_path(run_dir):
    """Load the separate total CPU output when available."""
    preferred = Path(run_dir) / "cpu_total.json"
    if preferred.exists():
        return preferred
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Summarise CPU and energy metrics for a completed run"
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help=(
            "Path to run directory containing energy.json, cpu_k8s_by_id.json, "
            "and cpu_total.json (with fallbacks for older runs)"
        ),
    )
    parser.add_argument(
        "--ramp-exclusion-seconds",
        type=int,
        help="Optional override for ramp exclusion seconds",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    cpu_path = get_cpu_payload_path(run_dir)
    cpu_total_path = get_cpu_total_payload_path(run_dir)
    energy_path = run_dir / "energy.json"
    metadata_path = run_dir / "metadata.json"

    print("loading data")
    cpu_payload = load_json(cpu_path)
    cpu_total_payload = load_json(cpu_total_path) if cpu_total_path else None
    energy_payload = load_json(energy_path)
    metadata = load_json(metadata_path) if metadata_path.exists() else {}

    ramp_exclusion_seconds = args.ramp_exclusion_seconds
    if ramp_exclusion_seconds is None:
        ramp_exclusion_seconds = metadata.get("ramp_exclusion_seconds", 0)

    timestamps = metadata.get("timestamps", {}) if isinstance(metadata, dict) else {}
    effective_start = timestamps.get("workload_effective_start")
    workload_start = timestamps.get("workload_start")
    min_timestamp = None
    if effective_start:
        min_timestamp = to_unix_seconds(effective_start)
    elif workload_start and ramp_exclusion_seconds:
        min_timestamp = to_unix_seconds(workload_start) + ramp_exclusion_seconds

    print("processing CPU")
    cpu_series = parse_prometheus_by_label(cpu_payload, "id", min_timestamp=min_timestamp)
    cpu_k8s_stats = compute_stats(cpu_series)

    cpu_total_stats = {}
    if cpu_total_payload:
        cpu_total_values = parse_prometheus_single_series(
            cpu_total_payload,
            min_timestamp=min_timestamp,
        )
        if cpu_total_values:
            cpu_total_stats = {
                "mean": sum(cpu_total_values) / len(cpu_total_values),
                "max": max(cpu_total_values),
            }

    print("processing energy")
    energy_series = parse_prometheus_by_label(
        energy_payload,
        "container_name",
        min_timestamp=min_timestamp,
    )
    energy_stats = compute_stats(energy_series)

    print("processing workload")
    workload_summary = parse_locust_workload_metrics(run_dir, min_timestamp=min_timestamp)

    summary = build_summary(
        energy_stats,
        cpu_k8s_stats,
        cpu_total_stats,
        workload_summary,
    )
    energy_source_info = load_energy_source_info(run_dir)
    if energy_source_info:
        summary["energy_source"] = energy_source_info

    print("saving outputs")
    save_summary_json(run_dir, summary)
    save_summary_csv(run_dir, summary)


if __name__ == "__main__":
    main()