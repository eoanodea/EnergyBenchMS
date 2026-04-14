#!/usr/bin/env python3
"""Summarise CPU and energy metrics for a completed experiment run."""

import argparse
import csv
import json
from pathlib import Path


def load_json(path):
    with Path(path).open("r", encoding="utf-8") as infile:
        return json.load(infile)


def parse_prometheus_by_container(prometheus_payload):
    """Parse Prometheus query_range response into container -> list of float values."""
    series = {}
    results = prometheus_payload.get("data", {}).get("result", [])

    for result in results:
        metric = result.get("metric", {})
        container_name = metric.get("container_name", "")
        if not container_name:
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
            series.setdefault(container_name, []).append(value)

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


def build_summary(cpu_stats, energy_stats):
    """Merge CPU and energy stats into the final output structure."""
    containers = sorted(set(cpu_stats.keys()) | set(energy_stats.keys()))
    summary = {}

    for container_name in containers:
        cpu_entry = cpu_stats.get(container_name, {})
        energy_entry = energy_stats.get(container_name, {})
        summary[container_name] = {
            "cpu_mean": cpu_entry.get("mean"),
            "cpu_max": cpu_entry.get("max"),
            "energy_mean": energy_entry.get("mean"),
            "energy_max": energy_entry.get("max"),
        }

    return summary


def save_summary_json(run_dir, summary):
    output_path = Path(run_dir) / "summary.json"
    with output_path.open("w", encoding="utf-8") as outfile:
        json.dump(summary, outfile, indent=2)


def save_summary_csv(run_dir, summary):
    output_path = Path(run_dir) / "summary.csv"
    fieldnames = [
        "container_name",
        "cpu_mean",
        "cpu_max",
        "energy_mean",
        "energy_max",
    ]

    with output_path.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for container_name in sorted(summary.keys()):
            row = {"container_name": container_name}
            row.update(summary[container_name])
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(
        description="Summarise CPU and energy metrics for a completed run"
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to run directory containing metadata.json, cpu.json and energy.json",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    cpu_path = run_dir / "cpu.json"
    energy_path = run_dir / "energy.json"

    print("loading data")
    cpu_payload = load_json(cpu_path)
    energy_payload = load_json(energy_path)

    print("processing CPU")
    cpu_series = parse_prometheus_by_container(cpu_payload)
    cpu_stats = compute_stats(cpu_series)

    print("processing energy")
    energy_series = parse_prometheus_by_container(energy_payload)
    energy_stats = compute_stats(energy_series)

    summary = build_summary(cpu_stats, energy_stats)

    print("saving outputs")
    save_summary_json(run_dir, summary)
    save_summary_csv(run_dir, summary)


if __name__ == "__main__":
    main()