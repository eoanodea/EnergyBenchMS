#!/usr/bin/env python3
"""Extract experiment result aggregates from runs_comparison.html dashboard files."""

import argparse
import csv
import json
from pathlib import Path

SKIP_SUBSTRINGS = ["mediaservice", "onlineboutique-0.10"]

AGGREGATE_FIELDS = [
    "workload_level",
    "run_count",
    "energy_per_request_mean",
    "energy_per_request_std",
    "throughput_mean",
    "throughput_std",
    "p95_latency_mean",
    "p95_latency_std",
    "cpu_mean_mean",
    "cpu_mean_std",
    "energy_total_mean",
    "energy_total_std",
    "meter_power_mean",
    "meter_power_std",
    "meter_corrected_energy_wh_mean",
    "meter_corrected_energy_wh_std",
    "display_energy_total_mean",
    "display_energy_total_std",
    "display_energy_per_request_mean",
    "display_energy_per_request_std",
]

AGGREGATE_CSV_FIELDS = (
    ["source_folder", "app_name", "machine", "experiment_name"]
    + AGGREGATE_FIELDS
    + ["error_runs", "runs_with_flags", "total_runs"]
)

SERVICE_CSV_FIELDS = [
    "source_folder",
    "app_name",
    "machine",
    "experiment_name",
    "workload_level",
    "service",
    "mean_joules",
    "std_joules",
    "cv",
]


def infer_machine(run_dir_name: str) -> str:
    if run_dir_name.startswith("eoan_"):
        return "workstation"
    if run_dir_name.startswith("julian_"):
        return "ec2"
    return "unknown"


def extract_data_from_html(html_path: Path) -> dict:
    text = html_path.read_text(encoding="utf-8")
    marker = "const data = "
    idx = text.find(marker)
    if idx == -1:
        raise ValueError("marker 'const data = ' not found")
    start = idx + len(marker)
    end_of_line = text.find("\n", start)
    if end_of_line == -1:
        end_of_line = len(text)
    json_str = text[start:end_of_line].rstrip().rstrip(";").rstrip()
    return json.loads(json_str)


def find_html_files(base_dir: Path) -> list[Path]:
    files = []
    for app_dir in sorted(base_dir.iterdir()):
        if not app_dir.is_dir():
            continue
        if any(s in str(app_dir) for s in SKIP_SUBSTRINGS):
            continue
        for run_dir in sorted(app_dir.iterdir()):
            if not run_dir.is_dir():
                continue
            html = run_dir / "runs_comparison.html"
            if html.exists():
                files.append(html)
    return files


def top5_m2_services(attribution_phase1: dict, workload_level: str) -> list[dict]:
    workloads = attribution_phase1.get("workloads", {})
    workload_data = workloads.get(workload_level, {})
    models = workload_data.get("models", {})
    m2 = models.get("M2", {})
    services = m2.get("services", {})
    ranked = sorted(
        services.items(),
        key=lambda kv: kv[1].get("mean") or 0,
        reverse=True,
    )
    result = []
    for service_name, stats in ranked[:5]:
        result.append(
            {
                "service": service_name,
                "mean_joules": stats.get("mean"),
                "std_joules": stats.get("std"),
                "cv": stats.get("cv"),
            }
        )
    return result


def main():
    default_base = Path(__file__).parent.parent / "final-runs" / "experiment-runs-with-attribution"
    default_out = Path(__file__).parent.parent / "final-runs"

    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", type=Path, default=default_base,
                        help="Path to experiment-runs-with-attribution/ directory")
    parser.add_argument("--output-dir", type=Path, default=default_out,
                        help="Directory to write the two output CSVs")
    args = parser.parse_args()

    html_files = find_html_files(args.base_dir)
    print(f"Found {len(html_files)} HTML file(s) to process.\n")

    aggregate_rows = []
    service_rows = []

    for html_path in html_files:
        source_folder = html_path.parent.name
        app_name = html_path.parent.parent.name  # e.g. onlineboutique-0.4.2
        machine = infer_machine(source_folder)
        print(f"Processing: {html_path.relative_to(args.base_dir.parent)}")

        try:
            data = extract_data_from_html(html_path)
        except Exception as exc:
            print(f"  ERROR parsing JSON: {exc}")
            continue

        try:
            config = data.get("experiment_config", {})
            experiment_name = config.get("experiment_name", "unknown")
            level_aggregates = data.get("level_aggregates", [])
            quality_counts = data.get("quality_counts", {})
            attribution_phase1 = data.get("attribution_phase1", {})

            error_runs = quality_counts.get("error_runs")
            runs_with_flags = quality_counts.get("runs_with_flags")
            total_runs = quality_counts.get("total_runs")

            agg_count = 0
            svc_count = 0

            for level_agg in level_aggregates:
                level = level_agg.get("workload_level", "unknown")
                row = {
                    "source_folder": source_folder,
                    "app_name": app_name,
                    "machine": machine,
                    "experiment_name": experiment_name,
                    "error_runs": error_runs,
                    "runs_with_flags": runs_with_flags,
                    "total_runs": total_runs,
                }
                for field in AGGREGATE_FIELDS:
                    row[field] = level_agg.get(field)
                aggregate_rows.append(row)
                agg_count += 1

                for svc in top5_m2_services(attribution_phase1, level):
                    service_rows.append(
                        {
                            "source_folder": source_folder,
                            "app_name": app_name,
                            "machine": machine,
                            "experiment_name": experiment_name,
                            "workload_level": level,
                            **svc,
                        }
                    )
                    svc_count += 1

            print(f"  OK — {agg_count} level aggregate row(s), {svc_count} M2 service row(s)")

        except Exception as exc:
            print(f"  ERROR processing data: {exc}")
            continue

    # Write CSVs
    out_dir = args.output_dir
    agg_csv = out_dir / "results_aggregates.csv"
    svc_csv = out_dir / "results_m2_top_services.csv"

    with agg_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=AGGREGATE_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(aggregate_rows)

    with svc_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SERVICE_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(service_rows)

    print(f"\nWrote {len(aggregate_rows)} rows to {agg_csv.name}")
    print(f"Wrote {len(service_rows)} rows to {svc_csv.name}")


if __name__ == "__main__":
    main()
