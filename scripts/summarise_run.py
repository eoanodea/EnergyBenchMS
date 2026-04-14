#!/usr/bin/env python3
"""Summarise CPU and energy metrics for a completed experiment run."""

import argparse
import csv
import json
from pathlib import Path


def load_json(path):
    with Path(path).open("r", encoding="utf-8") as infile:
        return json.load(infile)


def parse_prometheus_by_label(prometheus_payload, label_name):
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
            raw_value = point[1]
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            series.setdefault(label_value, []).append(value)

    return series


def parse_prometheus_single_series(prometheus_payload):
    """Parse a Prometheus response containing a single unlabeled series."""
    results = prometheus_payload.get("data", {}).get("result", [])
    if not results:
        return []

    values = results[0].get("values", [])
    series = []
    for point in values:
        if not isinstance(point, list) or len(point) < 2:
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


def build_summary(energy_stats, cpu_k8s_stats, cpu_total_stats):
    """Build the final output structure with separate metric groupings."""
    return {
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
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    cpu_path = get_cpu_payload_path(run_dir)
    cpu_total_path = get_cpu_total_payload_path(run_dir)
    energy_path = run_dir / "energy.json"

    print("loading data")
    cpu_payload = load_json(cpu_path)
    cpu_total_payload = load_json(cpu_total_path) if cpu_total_path else None
    energy_payload = load_json(energy_path)

    print("processing CPU")
    cpu_series = parse_prometheus_by_label(cpu_payload, "id")
    cpu_k8s_stats = compute_stats(cpu_series)

    cpu_total_stats = {}
    if cpu_total_payload:
        cpu_total_values = parse_prometheus_single_series(cpu_total_payload)
        if cpu_total_values:
            cpu_total_stats = {
                "mean": sum(cpu_total_values) / len(cpu_total_values),
                "max": max(cpu_total_values),
            }

    print("processing energy")
    energy_series = parse_prometheus_by_label(energy_payload, "container_name")
    energy_stats = compute_stats(energy_series)

    summary = build_summary(energy_stats, cpu_k8s_stats, cpu_total_stats)

    print("saving outputs")
    save_summary_json(run_dir, summary)
    save_summary_csv(run_dir, summary)


if __name__ == "__main__":
    main()