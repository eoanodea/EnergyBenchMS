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


def safe_mean(values):
    cleaned = [value for value in values if isinstance(value, (int, float))]
    if not cleaned:
        return None
    return sum(cleaned) / len(cleaned)


def parse_effective_duration_seconds(metadata):
    timestamps = metadata.get("timestamps", {}) if isinstance(metadata, dict) else {}
    start = timestamps.get("workload_effective_start") or timestamps.get("workload_start")
    end = timestamps.get("workload_end")
    if not start or not end:
        return None

    try:
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
    except ValueError:
        return None

    return max(0.0, (end_dt - start_dt).total_seconds())


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


def compute_numeric_stats(values):
    cleaned = [value for value in values if isinstance(value, (int, float))]
    if not cleaned:
        return None
    return {
        "mean": sum(cleaned) / len(cleaned),
        "min": min(cleaned),
        "max": max(cleaned),
    }


def load_power_meter_samples(run_dir):
    samples_path = Path(run_dir) / "physical_power_meter.csv"
    if not samples_path.exists():
        metadata_path = Path(run_dir) / "metadata.json"
        if metadata_path.exists():
            metadata = load_json(metadata_path)
            meter = metadata.get("power_meter", {}) if isinstance(metadata, dict) else {}
            meter_samples = meter.get("samples_csv")
            if meter_samples:
                samples_path = Path(meter_samples)
    if not samples_path.exists():
        return []

    with samples_path.open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        return list(reader)


def parse_power_meter_samples(run_dir):
    samples = load_power_meter_samples(run_dir)
    if not samples:
        return {}

    numeric_fields = [
        "apower",
        "voltage",
        "freq",
        "current",
        "temperature_c",
        "temperature_f",
        "aenergy_total",
        "ret_aenergy_total",
    ]
    field_values = {field: [] for field in numeric_fields}
    successful_samples = []
    errors = 0

    for row in samples:
        error_text = (row.get("error") or "").strip()
        if error_text:
            errors += 1
            continue

        parsed_row = {}
        for field in numeric_fields:
            value = safe_float(row.get(field))
            field_values[field].append(value)
            parsed_row[field] = value
        successful_samples.append(parsed_row)

    if not successful_samples:
        return {
            "sample_count": len(samples),
            "successful_sample_count": 0,
            "error_count": errors,
        }

    meter_summary = {
        "sample_count": len(samples),
        "successful_sample_count": len(successful_samples),
        "error_count": errors,
        "metrics": {},
    }

    for field, values in field_values.items():
        stats = compute_numeric_stats(values)
        if stats is not None:
            meter_summary["metrics"][field] = stats

    first_energy = successful_samples[0].get("aenergy_total")
    last_energy = successful_samples[-1].get("aenergy_total")
    if isinstance(first_energy, (int, float)) and isinstance(last_energy, (int, float)):
        meter_summary["metrics"]["aenergy_total"]["start"] = first_energy
        meter_summary["metrics"]["aenergy_total"]["end"] = last_energy
        meter_summary["metrics"]["aenergy_total"]["delta"] = last_energy - first_energy

    return meter_summary


def parse_meter_timestamp(sample_row):
    timestamp = safe_float(sample_row.get("timestamp_unix"))
    if isinstance(timestamp, (int, float)):
        return float(timestamp)

    raw_timestamp = sample_row.get("timestamp_iso")
    if raw_timestamp in (None, ""):
        return None

    try:
        return datetime.fromisoformat(str(raw_timestamp)).timestamp()
    except ValueError:
        return None


def resolve_meter_window(metadata):
    timestamps = metadata.get("timestamps", {}) if isinstance(metadata, dict) else {}

    workload_start = timestamps.get("workload_start")
    workload_effective_start = timestamps.get("workload_effective_start") or workload_start
    workload_end = timestamps.get("workload_end")

    return {
        "workload_start": to_unix_seconds(workload_start) if workload_start else None,
        "workload_effective_start": to_unix_seconds(workload_effective_start) if workload_effective_start else None,
        "workload_end": to_unix_seconds(workload_end) if workload_end else None,
    }


def classify_meter_sample(sample_row, metadata, meter_window):
    phase = (sample_row.get("phase") or "").strip().lower()
    if phase in {"baseline", "workload"}:
        return phase

    sample_timestamp = parse_meter_timestamp(sample_row)
    if sample_timestamp is None:
        return "unknown"

    baseline_seconds = None
    power_meter_metadata = metadata.get("power_meter", {}) if isinstance(metadata, dict) else {}
    if isinstance(power_meter_metadata, dict):
        baseline_seconds = safe_float(power_meter_metadata.get("baseline_seconds"))
    if baseline_seconds is None and isinstance(metadata, dict):
        baseline_seconds = safe_float(metadata.get("baseline_seconds"))

    if baseline_seconds is not None:
        elapsed_seconds = safe_float(sample_row.get("elapsed_seconds"))
        if elapsed_seconds is not None:
            return "baseline" if elapsed_seconds < baseline_seconds else "workload"

    workload_start = meter_window.get("workload_start")
    workload_effective_start = meter_window.get("workload_effective_start")
    workload_end = meter_window.get("workload_end")

    if workload_start is not None and sample_timestamp < workload_start:
        return "baseline"

    if (
        workload_effective_start is not None
        and workload_end is not None
        and workload_effective_start <= sample_timestamp <= workload_end
    ):
        return "workload"

    return "unknown"


def load_physical_meter_samples(run_dir, metadata):
    raw_rows = load_power_meter_samples(run_dir)
    meter_window = resolve_meter_window(metadata)
    parsed_rows = []

    for row in raw_rows:
        parsed_rows.append(
            {
                "timestamp": parse_meter_timestamp(row),
                "phase": classify_meter_sample(row, metadata, meter_window),
                "apower": safe_float(row.get("apower")),
                "aenergy_total": safe_float(row.get("aenergy_total")),
                "error": (row.get("error") or "").strip(),
            }
        )

    parsed_rows.sort(key=lambda item: item["timestamp"] if item["timestamp"] is not None else float("inf"))
    return parsed_rows


def convert_energy_to_joules(value, unit):
    if not isinstance(value, (int, float)):
        return None

    normalized_unit = str(unit or "").strip().lower()
    if normalized_unit == "wh":
        return float(value) * 3600.0
    if normalized_unit == "kwh":
        return float(value) * 3_600_000.0
    if normalized_unit == "j":
        return float(value)

    return None


def integrate_net_energy_joules(samples, baseline_power_watts, start_timestamp=None, end_timestamp=None):
    usable_samples = []
    for sample in samples:
        timestamp = sample.get("timestamp")
        power = sample.get("apower")
        if not isinstance(timestamp, (int, float)) or not isinstance(power, (int, float)):
            continue
        if start_timestamp is not None and timestamp < start_timestamp:
            continue
        if end_timestamp is not None and timestamp > end_timestamp:
            continue
        usable_samples.append(sample)

    if len(usable_samples) < 2 or not isinstance(baseline_power_watts, (int, float)):
        return None

    energy_joules = 0.0
    for previous_sample, current_sample in zip(usable_samples, usable_samples[1:]):
        delta_seconds = current_sample["timestamp"] - previous_sample["timestamp"]
        if delta_seconds <= 0:
            continue
        previous_net_power = previous_sample["apower"] - baseline_power_watts
        current_net_power = current_sample["apower"] - baseline_power_watts
        energy_joules += ((previous_net_power + current_net_power) / 2.0) * delta_seconds

    return energy_joules


def compute_baseline_drift_ratio(baseline_samples):
    powers = [sample.get("apower") for sample in baseline_samples if isinstance(sample.get("apower"), (int, float))]
    if len(powers) < 4:
        return None

    midpoint = len(powers) // 2
    first_half = powers[:midpoint]
    second_half = powers[midpoint:]
    if not first_half or not second_half:
        return None

    first_mean = safe_mean(first_half)
    second_mean = safe_mean(second_half)
    baseline_mean = safe_mean(powers)
    if not isinstance(first_mean, (int, float)) or not isinstance(second_mean, (int, float)):
        return None
    if not isinstance(baseline_mean, (int, float)) or baseline_mean <= 0:
        return None

    return abs(second_mean - first_mean) / baseline_mean


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


def build_physical_power_meter_summary(run_dir, metadata, workload_summary):
    meter_samples = load_physical_meter_samples(run_dir, metadata)
    if not meter_samples:
        return None

    meter_window = resolve_meter_window(metadata)
    workload_start = meter_window.get("workload_start")
    workload_effective_start = meter_window.get("workload_effective_start")
    workload_end = meter_window.get("workload_end")

    baseline_samples = [sample for sample in meter_samples if sample.get("phase") == "baseline"]
    workload_samples = [sample for sample in meter_samples if sample.get("phase") == "workload"]

    # Backward compatibility for older runs that do not have explicit phase tags.
    if not baseline_samples and workload_start is not None:
        baseline_samples = [
            sample
            for sample in meter_samples
            if isinstance(sample.get("timestamp"), (int, float)) and sample["timestamp"] < workload_start
        ]

    if not workload_samples and workload_effective_start is not None and workload_end is not None:
        workload_samples = [
            sample
            for sample in meter_samples
            if isinstance(sample.get("timestamp"), (int, float))
            and workload_effective_start <= sample["timestamp"] <= workload_end
        ]

    power_meter_metadata = metadata.get("power_meter", {}) if isinstance(metadata, dict) else {}
    meter_energy_unit = None
    if isinstance(power_meter_metadata, dict):
        meter_energy_unit = power_meter_metadata.get("energy_unit")
    if not meter_energy_unit:
        meter_energy_unit = "Wh"

    unit_unknown = meter_energy_unit not in {"Wh", "kWh", "J"}

    raw_energy_delta_wh = None
    raw_energy_delta_joules = None
    energy_samples = [sample for sample in meter_samples if isinstance(sample.get("aenergy_total"), (int, float))]
    if len(energy_samples) >= 2:
        raw_energy_delta_wh = energy_samples[-1]["aenergy_total"] - energy_samples[0]["aenergy_total"]
        raw_energy_delta_joules = convert_energy_to_joules(raw_energy_delta_wh, meter_energy_unit)

    effective_energy_delta_wh = None
    effective_energy_delta_joules = None
    effective_energy_samples = [sample for sample in workload_samples if isinstance(sample.get("aenergy_total"), (int, float))]
    if len(effective_energy_samples) >= 2:
        effective_energy_delta_wh = effective_energy_samples[-1]["aenergy_total"] - effective_energy_samples[0]["aenergy_total"]
        effective_energy_delta_joules = convert_energy_to_joules(effective_energy_delta_wh, meter_energy_unit)

    baseline_power_watts = safe_mean([sample.get("apower") for sample in baseline_samples])
    baseline_drift_ratio = compute_baseline_drift_ratio(baseline_samples)

    baseline_corrected_workload_energy_joules = integrate_net_energy_joules(
        workload_samples,
        baseline_power_watts,
        start_timestamp=workload_effective_start,
        end_timestamp=workload_end,
    )

    effective_duration = parse_effective_duration_seconds(metadata)
    throughput_mean_rps = workload_summary.get("throughput_mean_rps") if isinstance(workload_summary, dict) else None
    successful_requests = None
    if isinstance(throughput_mean_rps, (int, float)) and isinstance(effective_duration, (int, float)):
        successful_requests = throughput_mean_rps * effective_duration

    meter_energy_per_request_joules = None
    if isinstance(baseline_corrected_workload_energy_joules, (int, float)) and isinstance(successful_requests, (int, float)) and successful_requests > 0:
        meter_energy_per_request_joules = baseline_corrected_workload_energy_joules / successful_requests

    meter_quality_flags = []
    if not baseline_samples:
        meter_quality_flags.append("missing_baseline")
    if unit_unknown:
        meter_quality_flags.append("meter_unit_unknown")
    if len(meter_samples) < 3:
        meter_quality_flags.append("too_few_meter_samples")
    if isinstance(baseline_drift_ratio, (int, float)) and baseline_drift_ratio > 0.15:
        meter_quality_flags.append("baseline_drift_too_high")
    if isinstance(baseline_corrected_workload_energy_joules, (int, float)) and baseline_corrected_workload_energy_joules < 0:
        meter_quality_flags.append("negative_baseline_corrected_energy")

    meter_summary = {
        "sample_count": len(meter_samples),
        "baseline_sample_count": len(baseline_samples),
        "workload_sample_count": len(workload_samples),
        "error_count": sum(1 for sample in meter_samples if sample.get("error")),
        "meter_energy_unit": meter_energy_unit,
        "raw_energy_delta_wh": raw_energy_delta_wh,
        "raw_energy_delta_joules": raw_energy_delta_joules,
        "effective_energy_delta_wh": effective_energy_delta_wh,
        "effective_energy_delta_joules": effective_energy_delta_joules,
        "baseline_power_watts": baseline_power_watts,
        "baseline_drift_ratio": baseline_drift_ratio,
        "baseline_corrected_workload_energy_wh": (
            baseline_corrected_workload_energy_joules / 3600.0
            if isinstance(baseline_corrected_workload_energy_joules, (int, float))
            else None
        ),
        "baseline_corrected_workload_energy_joules": baseline_corrected_workload_energy_joules,
        "meter_energy_per_request_joules": meter_energy_per_request_joules,
        "quality_flags": meter_quality_flags,
        "metrics": {
            "apower": {
                "mean": safe_mean([sample.get("apower") for sample in meter_samples]),
                "min": min([sample.get("apower") for sample in meter_samples if isinstance(sample.get("apower"), (int, float))], default=None),
                "max": max([sample.get("apower") for sample in meter_samples if isinstance(sample.get("apower"), (int, float))], default=None),
            },
            "aenergy_total": {
                "mean": safe_mean([sample.get("aenergy_total") for sample in meter_samples]),
                "min": min([sample.get("aenergy_total") for sample in meter_samples if isinstance(sample.get("aenergy_total"), (int, float))], default=None),
                "max": max([sample.get("aenergy_total") for sample in meter_samples if isinstance(sample.get("aenergy_total"), (int, float))], default=None),
            },
        },
    }

    if isinstance(power_meter_metadata, dict):
        meter_summary["source_url"] = power_meter_metadata.get("url")
        meter_summary["interval_seconds"] = power_meter_metadata.get("interval_seconds")
        meter_summary["request_timeout_seconds"] = power_meter_metadata.get("request_timeout_seconds")
        meter_summary["baseline_seconds"] = power_meter_metadata.get("baseline_seconds")
        meter_summary["sampler_exit_code"] = power_meter_metadata.get("exit_code")
        meter_summary["sampler_pid"] = power_meter_metadata.get("pid")

    return meter_summary


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
        "meter_mean",
        "meter_min",
        "meter_max",
        "meter_start",
        "meter_end",
        "meter_delta",
        "sample_count",
        "error_count",
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
                "meter_mean": "",
                "meter_min": "",
                "meter_max": "",
                "meter_start": "",
                "meter_end": "",
                "meter_delta": "",
                "sample_count": "",
                "error_count": "",
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
                "meter_mean": "",
                "meter_min": "",
                "meter_max": "",
                "meter_start": "",
                "meter_end": "",
                "meter_delta": "",
                "sample_count": "",
                "error_count": "",
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
                "meter_mean": "",
                "meter_min": "",
                "meter_max": "",
                "meter_start": "",
                "meter_end": "",
                "meter_delta": "",
                "sample_count": "",
                "error_count": "",
            }
            writer.writerow(row)

        meter = summary.get("physical_power_meter", {})
        metrics = meter.get("metrics", {}) if isinstance(meter, dict) else {}
        status_row = {
            "group": "physical_power_meter",
            "label": "status",
            "cpu_mean": "",
            "cpu_max": "",
            "energy_mean": "",
            "energy_max": "",
            "meter_mean": "",
            "meter_min": "",
            "meter_max": "",
            "meter_start": "",
            "meter_end": "",
            "meter_delta": "",
            "sample_count": meter.get("sample_count", ""),
            "error_count": meter.get("error_count", ""),
        }
        writer.writerow(status_row)
        for label in ["apower", "voltage", "freq", "current", "temperature_c", "temperature_f"]:
            stats = metrics.get(label)
            if not isinstance(stats, dict):
                continue
            row = {
                "group": "physical_power_meter",
                "label": label,
                "cpu_mean": "",
                "cpu_max": "",
                "energy_mean": "",
                "energy_max": "",
                "meter_mean": stats.get("mean", ""),
                "meter_min": stats.get("min", ""),
                "meter_max": stats.get("max", ""),
                "meter_start": "",
                "meter_end": "",
                "meter_delta": "",
                "sample_count": meter.get("sample_count", ""),
                "error_count": meter.get("error_count", ""),
            }
            writer.writerow(row)

        energy_stats = metrics.get("aenergy_total")
        if isinstance(energy_stats, dict):
            row = {
                "group": "physical_power_meter_energy",
                "label": "aenergy_total",
                "cpu_mean": "",
                "cpu_max": "",
                "energy_mean": "",
                "energy_max": "",
                "meter_mean": energy_stats.get("mean", ""),
                "meter_min": energy_stats.get("min", ""),
                "meter_max": energy_stats.get("max", ""),
                "meter_start": energy_stats.get("start", ""),
                "meter_end": energy_stats.get("end", ""),
                "meter_delta": energy_stats.get("delta", ""),
                "sample_count": meter.get("sample_count", ""),
                "error_count": meter.get("error_count", ""),
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

    print("processing physical power meter")
    physical_power_meter_summary = build_physical_power_meter_summary(run_dir, metadata, workload_summary)

    summary = build_summary(
        energy_stats,
        cpu_k8s_stats,
        cpu_total_stats,
        workload_summary,
    )
    if physical_power_meter_summary:
        summary["physical_power_meter"] = physical_power_meter_summary
    energy_source_info = load_energy_source_info(run_dir)
    if energy_source_info:
        summary["energy_source"] = energy_source_info

    print("saving outputs")
    save_summary_json(run_dir, summary)
    save_summary_csv(run_dir, summary)


if __name__ == "__main__":
    main()