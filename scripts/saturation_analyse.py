#!/usr/bin/env python3
"""Analyse saturation calibration data and emit simple threshold-based decisions."""

import argparse
import csv
import json
from pathlib import Path


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_calibration_rows(calibration_csv_path):
    """Load and sort calibration rows by user level."""
    rows = []
    with Path(calibration_csv_path).open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            level = safe_float(row.get("user_level"))
            if level is None:
                continue
            rows.append(
                {
                    "user_level": int(level),
                    "throughput_mean": safe_float(row.get("throughput_mean")),
                    "cpu_mean": safe_float(row.get("cpu_mean")),
                    "cpu_max": safe_float(row.get("cpu_max")),
                    "energy_total": safe_float(row.get("energy_total")),
                    "energy_per_request": safe_float(row.get("energy_per_request")),
                    "p95_latency": safe_float(row.get("p95_latency")),
                    "error_rate": safe_float(row.get("error_rate")),
                }
            )

    rows.sort(key=lambda item: item["user_level"])
    return rows


def find_max_throughput(rows):
    """Find the level with maximum throughput."""
    best = None
    for row in rows:
        throughput = row.get("throughput_mean")
        if throughput is None:
            continue
        if best is None or throughput > best["throughput_mean"]:
            best = {
                "user_level": row["user_level"],
                "throughput_mean": throughput,
            }
    return best


def find_throughput_plateau(rows, threshold):
    """Find first adjacent-level plateau where marginal gain drops below threshold."""
    for index in range(len(rows) - 1):
        current = rows[index]
        nxt = rows[index + 1]

        current_tp = current.get("throughput_mean")
        next_tp = nxt.get("throughput_mean")
        if current_tp is None or next_tp is None:
            continue
        if current_tp <= 0:
            continue

        marginal_gain = (next_tp - current_tp) / current_tp
        if marginal_gain < threshold:
            return {
                "plateau_user_level": nxt["user_level"],
                "previous_user_level": current["user_level"],
                "marginal_gain": marginal_gain,
                "threshold": threshold,
            }

    return None


def find_degradation(rows, latency_jump_threshold, error_rate_threshold):
    """Find first adjacent-level performance degradation signal."""
    for index in range(len(rows) - 1):
        current = rows[index]
        nxt = rows[index + 1]

        next_error_rate = nxt.get("error_rate")
        if next_error_rate is not None and next_error_rate >= error_rate_threshold:
            return {
                "degradation_user_level": nxt["user_level"],
                "reason": "error_rate_threshold",
                "error_rate": next_error_rate,
                "error_rate_threshold": error_rate_threshold,
            }

        current_p95 = current.get("p95_latency")
        next_p95 = nxt.get("p95_latency")
        if current_p95 is None or next_p95 is None:
            continue
        if current_p95 <= 0:
            continue

        latency_jump = (next_p95 - current_p95) / current_p95
        if latency_jump >= latency_jump_threshold:
            return {
                "degradation_user_level": nxt["user_level"],
                "reason": "latency_jump_threshold",
                "latency_jump": latency_jump,
                "latency_jump_threshold": latency_jump_threshold,
                "previous_p95_latency": current_p95,
                "current_p95_latency": next_p95,
            }

    return None


def find_cpu_threshold(rows, cpu_threshold):
    """Find first level where mean CPU crosses configured threshold."""
    for row in rows:
        cpu_mean = row.get("cpu_mean")
        if cpu_mean is None:
            continue
        if cpu_mean >= cpu_threshold:
            return {
                "cpu_threshold_user_level": row["user_level"],
                "cpu_mean": cpu_mean,
                "cpu_threshold": cpu_threshold,
            }

    return None


def build_summary(rows, plateau, degradation, cpu_threshold_hit, max_throughput, config):
    """Build saturation summary payload."""
    return {
        "config": config,
        "row_count": len(rows),
        "levels": [row["user_level"] for row in rows],
        "signals": {
            "throughput_plateau": plateau,
            "degradation": degradation,
            "cpu_threshold": cpu_threshold_hit,
        },
        "max_throughput": max_throughput,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Analyse calibration_summary.csv and emit saturation_summary.json"
    )
    parser.add_argument(
        "--calibration-csv",
        required=True,
        help="Path to calibration_summary.csv",
    )
    parser.add_argument(
        "--output",
        default="saturation_summary.json",
        help="Path to output saturation summary JSON",
    )
    parser.add_argument(
        "--plateau-threshold",
        type=float,
        default=0.05,
        help="Throughput plateau threshold as fraction (default: 0.05 for 5%%)",
    )
    parser.add_argument(
        "--latency-jump-threshold",
        type=float,
        default=0.30,
        help="Latency jump threshold as fraction (default: 0.30 for 30%%)",
    )
    parser.add_argument(
        "--error-rate-threshold",
        type=float,
        default=0.01,
        help="Error rate threshold as fraction (default: 0.01 for 1%%)",
    )
    parser.add_argument(
        "--cpu-threshold",
        type=float,
        default=0.90,
        help="CPU mean threshold as fraction (default: 0.90 for 90%%)",
    )
    args = parser.parse_args()

    if args.plateau_threshold < 0:
        raise SystemExit("--plateau-threshold must be at least 0")
    if args.latency_jump_threshold < 0:
        raise SystemExit("--latency-jump-threshold must be at least 0")
    if args.error_rate_threshold < 0:
        raise SystemExit("--error-rate-threshold must be at least 0")
    if args.cpu_threshold < 0:
        raise SystemExit("--cpu-threshold must be at least 0")

    rows = load_calibration_rows(args.calibration_csv)
    if len(rows) < 2:
        raise SystemExit("Need at least two calibration rows for saturation analysis")

    plateau = find_throughput_plateau(rows, args.plateau_threshold)
    degradation = find_degradation(
        rows,
        args.latency_jump_threshold,
        args.error_rate_threshold,
    )
    cpu_threshold_hit = find_cpu_threshold(rows, args.cpu_threshold)
    max_throughput = find_max_throughput(rows)

    summary = build_summary(
        rows,
        plateau,
        degradation,
        cpu_threshold_hit,
        max_throughput,
        {
            "plateau_threshold": args.plateau_threshold,
            "latency_jump_threshold": args.latency_jump_threshold,
            "error_rate_threshold": args.error_rate_threshold,
            "cpu_threshold": args.cpu_threshold,
        },
    )

    output_path = Path(args.output)
    with output_path.open("w", encoding="utf-8") as outfile:
        json.dump(summary, outfile, indent=2)

    print(f"Saturation summary written to {output_path}")


if __name__ == "__main__":
    main()
