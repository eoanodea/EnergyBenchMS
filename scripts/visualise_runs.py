#!/usr/bin/env python3
"""Generate an HTML dashboard focused on workload-level run comparison."""

import argparse
import csv
import json
import statistics
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


# Configure which containers belong to the system under test.
SUT_CONTAINERS = [
    "nginx",
]

# Exclude infrastructure services from application-focused energy analysis.
EXCLUDED_CONTAINERS = {
    "kepler",
    "coredns",
    "metrics-server",
    "traefik",
    "local-path-provisioner",
}

DEFAULT_LEVEL_ORDER = ["low", "medium", "high"]


def load_json(path):
    path = Path(path)
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as infile:
        return json.load(infile)


def load_csv_rows(path):
    path = Path(path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as infile:
        return list(csv.DictReader(infile))


def parse_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_duration_seconds(metadata):
    if not metadata:
        return None

    timestamps = metadata.get("timestamps", {})
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


def safe_stdev(values):
    cleaned = [v for v in values if isinstance(v, (int, float))]
    if len(cleaned) <= 1:
        return 0.0
    return statistics.pstdev(cleaned)


def safe_mean(values):
    cleaned = [v for v in values if isinstance(v, (int, float))]
    if not cleaned:
        return None
    return sum(cleaned) / len(cleaned)


def format_level_sort_key(level):
    lowered = (level or "unknown").lower()
    if lowered in DEFAULT_LEVEL_ORDER:
        return (0, DEFAULT_LEVEL_ORDER.index(lowered), lowered)
    return (1, 0, lowered)


def get_filtered_sut_energy_means(summary):
    energy = summary.get("energy_by_container_name", {}) if isinstance(summary, dict) else {}
    out = {}
    for container_name, stats in energy.items():
        if container_name in EXCLUDED_CONTAINERS:
            continue
        if container_name not in SUT_CONTAINERS:
            continue
        if not isinstance(stats, dict):
            continue
        value = stats.get("mean")
        if isinstance(value, (int, float)):
            out[container_name] = value
    return out


def parse_locust_stats(run_dir):
    stats_rows = load_csv_rows(Path(run_dir) / "locust_stats.csv")
    if not stats_rows:
        return {}

    aggregated = None
    for row in stats_rows:
        name = str(row.get("Name", "")).strip().lower()
        row_type = str(row.get("Type", "")).strip().lower()
        if name == "aggregated" or row_type == "aggregated":
            aggregated = row
            break

    if not aggregated:
        return {}

    total_requests = parse_float(aggregated.get("Request Count"))
    total_failures = parse_float(aggregated.get("Failure Count"))
    p95_latency = parse_float(aggregated.get("95%"))
    throughput_rps = parse_float(aggregated.get("Requests/s"))

    error_rate = None
    if isinstance(total_requests, (int, float)) and total_requests > 0 and isinstance(total_failures, (int, float)):
        error_rate = max(0.0, min(1.0, total_failures / total_requests))

    return {
        "total_requests": total_requests,
        "total_failures": total_failures,
        "p95_latency": p95_latency,
        "throughput_mean_rps": throughput_rps,
        "error_rate": error_rate,
    }


def infer_workload_level(run_name, metadata):
    if isinstance(metadata, dict):
        label = metadata.get("workload_label")
        if label:
            return str(label)

    parts = str(run_name).split("/")
    if parts:
        head = parts[0].strip()
        if head and not head.startswith("iteration_"):
            return head
    return "unknown"


def infer_users_duration(metadata):
    params = metadata.get("workload_parameters", {}) if isinstance(metadata, dict) else {}
    users = params.get("users")
    duration = params.get("duration")
    return users, duration


def load_plan_payload(runs_dir):
    runs_root = Path(runs_dir)
    for filename in ["workload_plan.json", "saturation_plan.json"]:
        plan_path = runs_root / filename
        payload = load_json(plan_path)
        if isinstance(payload, dict):
            payload["_plan_file"] = filename
            return payload
    return None


def collect_runs(runs_dir, specific_run=None):
    runs_root = Path(runs_dir)

    if specific_run:
        specific = Path(specific_run)
        candidate_dirs = set()
        if (specific / "summary.json").exists() or (specific / "metadata.json").exists():
            candidate_dirs.add(specific)
        else:
            for summary_path in specific.rglob("summary.json"):
                candidate_dirs.add(summary_path.parent)
            for metadata_path in specific.rglob("metadata.json"):
                candidate_dirs.add(metadata_path.parent)
    else:
        candidate_dirs = set()
        for summary_path in runs_root.rglob("summary.json"):
            candidate_dirs.add(summary_path.parent)
        for metadata_path in runs_root.rglob("metadata.json"):
            candidate_dirs.add(metadata_path.parent)

        plan = load_plan_payload(runs_root)
        if isinstance(plan, dict):
            for run_entry in plan.get("runs", []):
                run_dir = run_entry.get("run_dir")
                if not run_dir:
                    continue
                run_path = Path(run_dir)
                if not run_path.is_absolute():
                    run_path = runs_root / run_path
            if run_path.exists():
              candidate_dirs.add(run_path)

    runs = []
    for run_dir in sorted(candidate_dirs):
        summary = load_json(run_dir / "summary.json")
        metadata = load_json(run_dir / "metadata.json")
        locust_stats = parse_locust_stats(run_dir)

        try:
            run_name = str(run_dir.relative_to(runs_root))
        except ValueError:
            run_name = run_dir.name

        missing_files = []
        for filename in ["summary.json", "metadata.json", "locust_stats.csv"]:
            if not (run_dir / filename).exists():
                missing_files.append(filename)

        runs.append(
            {
                "run_name": run_name,
                "run_dir": str(run_dir),
                "summary": summary,
                "metadata": metadata,
                "locust_stats": locust_stats,
                "missing_files": missing_files,
            }
        )

    return runs


def mask_prom_url(prom_url):
    if not prom_url:
        return None
    try:
        parsed = urlsplit(str(prom_url))
    except ValueError:
        return "***"

    hostname = parsed.hostname
    if not hostname:
        return "***"

    masked_host = "***"
    if parsed.port:
        netloc = f"{masked_host}:{parsed.port}"
    else:
        netloc = masked_host

    return urlunsplit((parsed.scheme or "http", netloc, parsed.path, "", ""))


def infer_experiment_config(runs_dir, runs, plan):
    first_metadata = None
    first_summary = None
    for item in runs:
        if not first_metadata and isinstance(item.get("metadata"), dict):
            first_metadata = item["metadata"]
        if not first_summary and isinstance(item.get("summary"), dict):
            first_summary = item["summary"]

    app_name = None
    environment_name = None
    energy_source = None
    levels_used = []
    repetitions_per_level = None
    warmup_enabled = None
    cleanup_reset_enabled = None
    cooldown_seconds = None
    dwell_duration_seconds = None
    ramp_exclusion_seconds = None
    prom_url_masked = None

    if isinstance(plan, dict):
        app_name = Path(str(plan.get("app", ""))).name or None
        energy_source = plan.get("energy_source")
        prom_url_masked = mask_prom_url(plan.get("prom_url"))

        if plan.get("_plan_file") == "workload_plan.json":
            levels_used = [
                entry.get("label")
                for entry in plan.get("workload_levels", [])
                if isinstance(entry, dict) and entry.get("label")
            ]
            repetitions_per_level = plan.get("count")
            warmup_enabled = True
            cleanup_reset_enabled = True
        elif plan.get("_plan_file") == "saturation_plan.json":
            levels_used = [str(level) for level in plan.get("levels", [])]
            repetitions_per_level = 1
            warmup_enabled = True
            cleanup_reset_enabled = bool(plan.get("reset_between_levels"))
            cooldown_seconds = plan.get("cooldown_seconds")
            dwell_duration_seconds = plan.get("dwell_seconds")
            ramp_exclusion_seconds = plan.get("ramp_exclusion_seconds")

    if isinstance(first_metadata, dict):
        if not app_name:
            app_path = first_metadata.get("app_path")
            app_name = Path(str(app_path)).name if app_path else None

        deployment = first_metadata.get("deployment", {})
        if isinstance(deployment, dict):
            environment_name = deployment.get("namespace_override")

        if ramp_exclusion_seconds is None:
            ramp_exclusion_seconds = first_metadata.get("ramp_exclusion_seconds")

        workload_params = first_metadata.get("workload_parameters", {})
        if isinstance(workload_params, dict):
            saturation = workload_params.get("saturation", {})
            if isinstance(saturation, dict):
                if cooldown_seconds is None:
                    cooldown_seconds = saturation.get("cooldown_seconds")
                if dwell_duration_seconds is None:
                    dwell_duration_seconds = saturation.get("dwell_seconds")
                if cleanup_reset_enabled is None and "reset_between_levels" in saturation:
                    cleanup_reset_enabled = bool(saturation.get("reset_between_levels"))

            if dwell_duration_seconds is None:
                dwell_duration_seconds = workload_params.get("duration")

    if isinstance(first_summary, dict):
        summary_energy_source = first_summary.get("energy_source", {})
        if isinstance(summary_energy_source, dict) and not energy_source:
            energy_source = summary_energy_source.get("requested_energy_source") or summary_energy_source.get(
                "selected_energy_source"
            )

    if not levels_used:
        levels_used = sorted(
            {infer_workload_level(item["run_name"], item.get("metadata")) for item in runs},
            key=format_level_sort_key,
        )

    if repetitions_per_level is None:
        counts = {}
        for item in runs:
            level = infer_workload_level(item["run_name"], item.get("metadata"))
            counts[level] = counts.get(level, 0) + 1
        if counts:
            repetitions_per_level = max(counts.values())

    if warmup_enabled is None:
        warmup_enabled = True if isinstance(plan, dict) else None

    return {
        "app_name": app_name or "unknown",
        "environment_name": environment_name or "unknown",
        "energy_source": energy_source or "unknown",
        "levels_used": levels_used,
        "repetitions_per_level": repetitions_per_level,
        "warmup_enabled": warmup_enabled,
        "cleanup_reset_enabled": cleanup_reset_enabled,
        "cooldown_seconds": cooldown_seconds,
        "dwell_duration_seconds": dwell_duration_seconds,
        "ramp_exclusion_seconds": ramp_exclusion_seconds,
        "prom_url_masked": prom_url_masked,
        "experiment_name": Path(runs_dir).name,
    }


def build_run_row(item, sut_container):
    run_name = item["run_name"]
    summary = item.get("summary") if isinstance(item.get("summary"), dict) else {}
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    locust_stats = item.get("locust_stats", {}) if isinstance(item.get("locust_stats"), dict) else {}

    cpu_total = summary.get("cpu_total", {}) if isinstance(summary, dict) else {}
    cpu_mean = cpu_total.get("mean") if isinstance(cpu_total, dict) else None
    cpu_max = cpu_total.get("max") if isinstance(cpu_total, dict) else None

    workload_summary = summary.get("workload", {}) if isinstance(summary, dict) else {}
    throughput_mean = workload_summary.get("throughput_mean_rps")
    p95_latency = workload_summary.get("p95_latency")
    error_rate = workload_summary.get("error_rate")

    if throughput_mean is None:
        throughput_mean = locust_stats.get("throughput_mean_rps")
    if p95_latency is None:
        p95_latency = locust_stats.get("p95_latency")
    if error_rate is None:
        error_rate = locust_stats.get("error_rate")

    total_requests = locust_stats.get("total_requests")

    duration_seconds = parse_duration_seconds(metadata)
    users, configured_duration = infer_users_duration(metadata)
    workload_level = infer_workload_level(run_name, metadata)

    cooldown_seconds = None
    saturation = metadata.get("workload_parameters", {}).get("saturation", {})
    if isinstance(saturation, dict):
        cooldown_seconds = saturation.get("cooldown_seconds")

    ramp_exclusion_seconds = metadata.get("ramp_exclusion_seconds")

    sut_means = get_filtered_sut_energy_means(summary)
    sut_energy_mean = sut_means.get(sut_container)

    energy_total = None
    if isinstance(sut_energy_mean, (int, float)) and isinstance(duration_seconds, (int, float)):
        energy_total = sut_energy_mean * duration_seconds

    energy_per_request = None
    if isinstance(energy_total, (int, float)) and isinstance(total_requests, (int, float)) and total_requests > 0:
        energy_per_request = energy_total / total_requests

    flags = []
    missing_files = item.get("missing_files", [])
    if missing_files:
        flags.append(f"missing_files:{','.join(sorted(missing_files))}")

    missing_metrics = []
    required_metrics = {
        "throughput": throughput_mean,
        "p95_latency": p95_latency,
        "error_rate": error_rate,
        "cpu_mean": cpu_mean,
        "cpu_max": cpu_max,
        "energy_total": energy_total,
        "energy_per_request": energy_per_request,
    }
    for key, value in required_metrics.items():
        if value is None:
            missing_metrics.append(key)
    if missing_metrics:
        flags.append(f"missing_metrics:{','.join(missing_metrics)}")

    if isinstance(error_rate, (int, float)) and error_rate > 0:
        flags.append("error_rate>0")

    if users not in (None, 0) and isinstance(throughput_mean, (int, float)) and throughput_mean > 0:
        if cpu_mean is None or cpu_mean <= 0:
            flags.append("cpu_missing_or_zero")
        if energy_total is None or energy_total <= 0:
            flags.append("energy_missing_or_zero")

    return {
        "name": run_name,
        "workload_level": workload_level,
        "users": users,
        "duration_seconds": configured_duration,
        "effective_duration_seconds": duration_seconds,
        "cooldown_seconds": cooldown_seconds,
        "ramp_exclusion_seconds": ramp_exclusion_seconds,
        "throughput_mean": throughput_mean,
        "total_requests": total_requests,
        "p95_latency": p95_latency,
        "error_rate": error_rate,
        "cpu_mean": cpu_mean,
        "cpu_max": cpu_max,
        "energy_total": energy_total,
        "energy_per_request": energy_per_request,
        "flags": flags,
    }


def add_latency_outlier_flags(run_rows):
    latencies = [row["p95_latency"] for row in run_rows if isinstance(row.get("p95_latency"), (int, float))]
    if len(latencies) < 2:
        return

    mean_value = sum(latencies) / len(latencies)
    stdev_value = safe_stdev(latencies)
    threshold = mean_value + (2.0 * stdev_value)

    for row in run_rows:
        value = row.get("p95_latency")
        if not isinstance(value, (int, float)):
            continue
        if stdev_value > 0 and value > threshold:
            row["flags"].append("unusually_high_latency")


def compute_level_aggregates(run_rows):
    grouped = {}
    for row in run_rows:
        level = row.get("workload_level") or "unknown"
        grouped.setdefault(level, []).append(row)

    aggregates = []
    for level in sorted(grouped.keys(), key=format_level_sort_key):
        rows = grouped[level]
        aggregates.append(
            {
                "workload_level": level,
                "run_count": len(rows),
                "energy_per_request_mean": safe_mean([r.get("energy_per_request") for r in rows]),
                "energy_per_request_std": safe_stdev([r.get("energy_per_request") for r in rows]),
                "throughput_mean": safe_mean([r.get("throughput_mean") for r in rows]),
                "throughput_std": safe_stdev([r.get("throughput_mean") for r in rows]),
                "p95_latency_mean": safe_mean([r.get("p95_latency") for r in rows]),
                "p95_latency_std": safe_stdev([r.get("p95_latency") for r in rows]),
                "cpu_mean_mean": safe_mean([r.get("cpu_mean") for r in rows]),
                "cpu_mean_std": safe_stdev([r.get("cpu_mean") for r in rows]),
                "energy_total_mean": safe_mean([r.get("energy_total") for r in rows]),
                "energy_total_std": safe_stdev([r.get("energy_total") for r in rows]),
            }
        )

    return aggregates


def make_dashboard_data(runs_dir, runs, sut_container):
    run_rows = [build_run_row(item, sut_container) for item in runs]
    add_latency_outlier_flags(run_rows)

    cpu_values = [row["cpu_mean"] for row in run_rows]
    energy_per_req_values = [row["energy_per_request"] for row in run_rows]
    throughput_values = [row["throughput_mean"] for row in run_rows]

    plan = load_plan_payload(runs_dir)
    config = infer_experiment_config(runs_dir, runs, plan)
    level_aggregates = compute_level_aggregates(run_rows)

    quality_counts = {
        "total_runs": len(run_rows),
        "runs_with_flags": sum(1 for row in run_rows if row.get("flags")),
        "error_runs": sum(1 for row in run_rows if "error_rate>0" in row.get("flags", [])),
        "missing_files_runs": sum(
            1 for row in run_rows if any(str(flag).startswith("missing_files:") for flag in row.get("flags", []))
        ),
        "missing_metrics_runs": sum(
            1 for row in run_rows if any(str(flag).startswith("missing_metrics:") for flag in row.get("flags", []))
        ),
        "latency_outlier_runs": sum(1 for row in run_rows if "unusually_high_latency" in row.get("flags", [])),
    }

    return {
        "sut_container": sut_container,
        "sut_containers": SUT_CONTAINERS,
        "excluded_containers": sorted(EXCLUDED_CONTAINERS),
        "experiment_config": config,
        "quality_counts": quality_counts,
        "runs": run_rows,
        "level_aggregates": level_aggregates,
        "overall_consistency": {
            "cpu_mean_stdev": safe_stdev(cpu_values),
            "energy_per_request_stdev": safe_stdev(energy_per_req_values),
            "throughput_stdev": safe_stdev(throughput_values),
        },
    }


def build_html(data):
    payload = json.dumps(data)
    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Run Workload Comparison Dashboard</title>
  <script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>
  <style>
    :root {{
      --bg: #f6f2e7;
      --card: #fffdf8;
      --ink: #1f1b16;
      --muted: #6f665d;
      --line: #ddd2c3;
      --accent: #116466;
      --accent2: #b85c38;
      --accent3: #2f3e46;
      --warn: #8a2d3b;
    }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background: radial-gradient(circle at top left, #fff, var(--bg));
      color: var(--ink);
    }}
    .wrap {{
      max-width: 1400px;
      margin: 24px auto;
      padding: 0 16px 24px;
    }}
    h1 {{
      margin: 0 0 8px;
      letter-spacing: 0.4px;
    }}
    p {{
      margin: 0 0 18px;
      color: var(--muted);
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }}
    .controls, .card {{
      border: 1px solid var(--line);
      background: var(--card);
      border-radius: 12px;
      padding: 12px;
    }}
    .controls h2, .card h2 {{
      margin: 0 0 10px;
      font-size: 18px;
    }}
    .run-list {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 14px;
      margin-bottom: 10px;
    }}
    .run-item {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 14px;
    }}
    .actions {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 8px;
    }}
    .actions button {{
      border: 1px solid var(--line);
      background: #fff;
      border-radius: 8px;
      padding: 6px 10px;
      cursor: pointer;
      color: var(--ink);
    }}
    .small {{
      font-size: 13px;
      color: var(--muted);
      margin-top: 6px;
    }}
    .warn {{
      color: var(--warn);
      font-weight: 600;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
      position: sticky;
      top: 0;
      background: var(--card);
    }}
    .table-wrap {{
      overflow-x: auto;
      max-height: 420px;
      border: 1px solid var(--line);
      border-radius: 8px;
    }}
    .kv {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
    }}
    .kv-row {{
      display: grid;
      grid-template-columns: 240px 1fr;
      border-bottom: 1px solid var(--line);
      padding: 6px 0;
      gap: 10px;
    }}
    .kv-row:last-child {{
      border-bottom: 0;
    }}
    .key {{
      color: var(--muted);
      font-weight: 600;
    }}
    canvas {{
      width: 100% !important;
      max-height: 300px;
    }}
    .grid-2 {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }}
    .grid-3 {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }}
    @media (min-width: 980px) {{
      .grid-2 {{
        grid-template-columns: 1fr 1fr;
      }}
      .grid-3 {{
        grid-template-columns: 1fr 1fr 1fr;
      }}
      .kv {{
        grid-template-columns: 1fr 1fr;
        column-gap: 20px;
      }}
    }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <h1>Run Workload Comparison Dashboard</h1>
    <p>Workload-first interpretation of low/medium/high behavior with per-run drill-down.</p>

    <section class=\"card\" style=\"margin-bottom:14px;\">
      <h2>Experiment Config Summary</h2>
      <div class=\"kv\" id=\"configSummary\"></div>
    </section>

    <section class=\"controls\" style=\"margin-bottom:14px;\">
      <h2>Run Selection</h2>
      <div id=\"runSelector\" class=\"run-list\"></div>
      <div class=\"actions\">
        <button id=\"selectAllBtn\" type=\"button\">Select all</button>
        <button id=\"selectNoneBtn\" type=\"button\">Select none</button>
      </div>
      <div class=\"small\" id=\"qualityText\"></div>
      <div class=\"small\" id=\"consistencyText\"></div>
      <div class=\"small\" id=\"filtersText\"></div>
    </section>

    <section class=\"card\" style=\"margin-bottom:14px;\">
      <h2>Per-Level Aggregate Summary (Mean ± Std Dev)</h2>
      <div class=\"table-wrap\">
        <table>
          <thead>
            <tr>
              <th>Workload Level</th>
              <th>Run Count</th>
              <th>Energy / Request</th>
              <th>Throughput</th>
              <th>P95 Latency</th>
              <th>CPU Mean</th>
            </tr>
          </thead>
          <tbody id=\"levelTableBody\"></tbody>
        </table>
      </div>
    </section>

    <div class=\"grid-3\" style=\"margin-bottom:14px;\">
      <section class=\"card\">
        <h2>Energy per Request by Level</h2>
        <canvas id=\"chartEnergyPerRequest\"></canvas>
      </section>
      <section class=\"card\">
        <h2>Energy Total by Level</h2>
        <canvas id=\"chartEnergyTotal\"></canvas>
      </section>
      <section class=\"card\">
        <h2>Throughput by Level</h2>
        <canvas id=\"chartThroughput\"></canvas>
      </section>
      <section class=\"card\">
        <h2>P95 Latency by Level</h2>
        <canvas id=\"chartP95\"></canvas>
      </section>
      <section class=\"card\">
        <h2>CPU Mean by Level</h2>
        <canvas id=\"chartCpuMean\"></canvas>
      </section>
    </div>

    <section class=\"card\" style=\"margin-bottom:14px;\">
      <h2>Per-Run Summary</h2>
      <div class=\"table-wrap\">
        <table>
          <thead>
            <tr>
              <th>Run</th>
              <th>Workload Level</th>
              <th>Users</th>
              <th>Duration</th>
              <th>Cooldown</th>
              <th>Ramp Exclusion</th>
              <th>Throughput Mean</th>
              <th>Total Requests</th>
              <th>P95 Latency</th>
              <th>Error Rate</th>
              <th>CPU Mean</th>
              <th>CPU Max</th>
              <th>Energy Total</th>
              <th>Energy / Request</th>
              <th>Quality Flags</th>
            </tr>
          </thead>
          <tbody id=\"runTableBody\"></tbody>
        </table>
      </div>
    </section>

    <section class=\"card\" style=\"margin-bottom:14px;\">
      <h2>P95 Latency over Iterations (per Workload Level)</h2>
      <canvas id=\"latencyIterationChart\"></canvas>
    </section>

    <div class=\"grid-2\">
      <section class=\"card\">
        <h2>Per-Run CPU (Mean/Max)</h2>
        <canvas id=\"perRunCpuChart\"></canvas>
      </section>
      <section class=\"card\">
        <h2>Per-Run Energy (Total and per Request)</h2>
        <canvas id=\"perRunEnergyChart\"></canvas>
      </section>
    </div>
  </div>

  <script>
    const data = {payload};
    const runs = data.runs || [];
    const runNames = runs.map((r) => r.name);
    const selected = new Set(runNames);

    function formatMaybeNumber(value, digits = 6) {{
      if (typeof value !== 'number' || Number.isNaN(value)) return '-';
      return value.toFixed(digits);
    }}

    function formatMeanStd(mean, std, digits = 6) {{
      if (typeof mean !== 'number' || Number.isNaN(mean)) return '-';
      const shownStd = (typeof std === 'number' && !Number.isNaN(std)) ? std : 0;
      return `${{mean.toFixed(digits)}} ± ${{shownStd.toFixed(digits)}}`;
    }}

    function selectedRuns() {{
      return runs.filter((r) => selected.has(r.name));
    }}

    function levelSortKey(level) {{
      const normalized = String(level || 'unknown').toLowerCase();
      const rank = {{ low: 0, medium: 1, high: 2 }};
      if (Object.prototype.hasOwnProperty.call(rank, normalized)) return [0, rank[normalized], normalized];
      return [1, 0, normalized];
    }}

    function sortLevels(levels) {{
      return [...levels].sort((a, b) => {{
        const ka = levelSortKey(a);
        const kb = levelSortKey(b);
        if (ka[0] !== kb[0]) return ka[0] - kb[0];
        if (ka[1] !== kb[1]) return ka[1] - kb[1];
        return ka[2].localeCompare(kb[2]);
      }});
    }}

    function computeMean(values) {{
      const nums = values.filter((v) => typeof v === 'number' && !Number.isNaN(v));
      if (nums.length === 0) return null;
      return nums.reduce((a, b) => a + b, 0) / nums.length;
    }}

    function computeStdev(values) {{
      const nums = values.filter((v) => typeof v === 'number' && !Number.isNaN(v));
      if (nums.length <= 1) return 0;
      const mean = nums.reduce((a, b) => a + b, 0) / nums.length;
      const variance = nums.reduce((acc, v) => acc + ((v - mean) ** 2), 0) / nums.length;
      return Math.sqrt(variance);
    }}

    function computeLevelAggregates(rows) {{
      const grouped = new Map();
      rows.forEach((r) => {{
        const level = r.workload_level || 'unknown';
        if (!grouped.has(level)) grouped.set(level, []);
        grouped.get(level).push(r);
      }});

      const levels = sortLevels(Array.from(grouped.keys()));
      return levels.map((level) => {{
        const items = grouped.get(level) || [];
        const metricValues = (key) => items.map((r) => r[key]);
        return {{
          workload_level: level,
          run_count: items.length,
          energy_per_request_mean: computeMean(metricValues('energy_per_request')),
          energy_per_request_std: computeStdev(metricValues('energy_per_request')),
          throughput_mean: computeMean(metricValues('throughput_mean')),
          throughput_std: computeStdev(metricValues('throughput_mean')),
          p95_latency_mean: computeMean(metricValues('p95_latency')),
          p95_latency_std: computeStdev(metricValues('p95_latency')),
          cpu_mean_mean: computeMean(metricValues('cpu_mean')),
          cpu_mean_std: computeStdev(metricValues('cpu_mean')),
          energy_total_mean: computeMean(metricValues('energy_total')),
          energy_total_std: computeStdev(metricValues('energy_total')),
        }};
      }});
    }}

    function extractIterationIndex(runName) {{
      const text = String(runName || '');
      const match = text.match(/iteration_(\\d{{8}}_\\d{{6}}_\\d{{6}})/i);
      if (match && match[1]) return match[1];

      const trailingDigits = text.match(/(\\d+)$/);
      if (trailingDigits && trailingDigits[1]) return trailingDigits[1];

      return text;
    }}

    function buildLatencyIterationSeries(rows) {{
      const grouped = new Map();
      rows.forEach((r) => {{
        const level = r.workload_level || 'unknown';
        if (!grouped.has(level)) grouped.set(level, []);
        grouped.get(level).push(r);
      }});

      const levels = sortLevels(Array.from(grouped.keys()));
      let maxLen = 0;
      const orderedRowsByLevel = levels.map((level) => {{
        const ordered = [...(grouped.get(level) || [])].sort((a, b) => {{
          const ia = extractIterationIndex(a.name);
          const ib = extractIterationIndex(b.name);
          return String(ia).localeCompare(String(ib));
        }});
        if (ordered.length > maxLen) maxLen = ordered.length;
        return {{ level, rows: ordered }};
      }});

      const labels = Array.from({{ length: maxLen }}, (_, idx) => `iter-${{idx + 1}}`);
      const palette = ['#116466', '#b85c38', '#2f3e46', '#8a2d3b', '#556b2f', '#5a4e7a'];

      const datasets = orderedRowsByLevel.map((entry, idx) => {{
        const dataPoints = labels.map((_, i) => {{
          const row = entry.rows[i];
          if (!row || typeof row.p95_latency !== 'number' || Number.isNaN(row.p95_latency)) return null;
          return row.p95_latency;
        }});
        const color = palette[idx % palette.length];
        return {{
          label: `${{entry.level}} p95 latency`,
          data: dataPoints,
          borderColor: color,
          backgroundColor: color + '55',
          tension: 0.2,
          spanGaps: true,
        }};
      }});

      return {{ labels, datasets }};
    }}

    function renderConfigSummary() {{
      const cfg = data.experiment_config || {{}};
      const rows = [
        ['Experiment', cfg.experiment_name || '-'],
        ['App', cfg.app_name || '-'],
        ['Environment', cfg.environment_name || '-'],
        ['Energy source', cfg.energy_source || '-'],
        ['Levels used', Array.isArray(cfg.levels_used) ? cfg.levels_used.join(', ') : '-'],
        ['Repetitions per level', cfg.repetitions_per_level ?? '-'],
        ['Warmup enabled', cfg.warmup_enabled === null || cfg.warmup_enabled === undefined ? '-' : String(cfg.warmup_enabled)],
        ['Cleanup/reset enabled', cfg.cleanup_reset_enabled === null || cfg.cleanup_reset_enabled === undefined ? '-' : String(cfg.cleanup_reset_enabled)],
        ['Cooldown seconds', cfg.cooldown_seconds ?? '-'],
        ['Dwell duration seconds', cfg.dwell_duration_seconds ?? '-'],
        ['Ramp exclusion seconds', cfg.ramp_exclusion_seconds ?? '-'],
        ['Prometheus URL', cfg.prom_url_masked || '(hidden)'],
      ];

      const host = document.getElementById('configSummary');
      host.innerHTML = '';
      rows.forEach(([k, v]) => {{
        const row = document.createElement('div');
        row.className = 'kv-row';

        const key = document.createElement('div');
        key.className = 'key';
        key.textContent = k;

        const value = document.createElement('div');
        value.textContent = String(v);

        row.appendChild(key);
        row.appendChild(value);
        host.appendChild(row);
      }});
    }}

    function renderRunTable(rows) {{
      const body = document.getElementById('runTableBody');
      body.innerHTML = '';

      rows.forEach((r) => {{
        const tr = document.createElement('tr');
        const cells = [
          r.name,
          r.workload_level || 'unknown',
          r.users ?? '-',
          r.duration_seconds ?? '-',
          r.cooldown_seconds ?? '-',
          r.ramp_exclusion_seconds ?? '-',
          formatMaybeNumber(r.throughput_mean, 4),
          formatMaybeNumber(r.total_requests, 0),
          formatMaybeNumber(r.p95_latency, 4),
          formatMaybeNumber(r.error_rate, 6),
          formatMaybeNumber(r.cpu_mean, 6),
          formatMaybeNumber(r.cpu_max, 6),
          formatMaybeNumber(r.energy_total, 6),
          formatMaybeNumber(r.energy_per_request, 9),
          (r.flags || []).join(' | ') || '-',
        ];

        cells.forEach((value, idx) => {{
          const td = document.createElement('td');
          td.textContent = value;
          if (idx === 14 && value !== '-') td.className = 'warn';
          tr.appendChild(td);
        }});

        body.appendChild(tr);
      }});
    }}

    function renderLevelTable(levelRows) {{
      const body = document.getElementById('levelTableBody');
      body.innerHTML = '';

      levelRows.forEach((row) => {{
        const tr = document.createElement('tr');
        const cells = [
          row.workload_level,
          String(row.run_count),
          formatMeanStd(row.energy_per_request_mean, row.energy_per_request_std, 9),
          formatMeanStd(row.throughput_mean, row.throughput_std, 4),
          formatMeanStd(row.p95_latency_mean, row.p95_latency_std, 4),
          formatMeanStd(row.cpu_mean_mean, row.cpu_mean_std, 6),
        ];

        cells.forEach((value) => {{
          const td = document.createElement('td');
          td.textContent = value;
          tr.appendChild(td);
        }});

        body.appendChild(tr);
      }});
    }}

    const commonOptions = {{
      responsive: true,
      maintainAspectRatio: false,
      interaction: {{ mode: 'index', intersect: false }},
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: {{ y: {{ beginAtZero: true }} }}
    }};

    function buildBarChart(canvasId, label, color) {{
      return new Chart(document.getElementById(canvasId), {{
        type: 'bar',
        data: {{
          labels: [],
          datasets: [{{
            label,
            data: [],
            borderColor: color,
            backgroundColor: color + '55',
            borderWidth: 1,
          }}]
        }},
        options: commonOptions,
      }});
    }}

    const chartEnergyPerRequest = buildBarChart('chartEnergyPerRequest', 'energy_per_request mean', '#116466');
    const chartEnergyTotal = buildBarChart('chartEnergyTotal', 'energy_total mean', '#b85c38');
    const chartThroughput = buildBarChart('chartThroughput', 'throughput mean', '#2f3e46');
    const chartP95 = buildBarChart('chartP95', 'p95 latency mean', '#8a2d3b');
    const chartCpuMean = buildBarChart('chartCpuMean', 'cpu mean', '#556b2f');

    const latencyIterationChart = new Chart(document.getElementById('latencyIterationChart'), {{
      type: 'line',
      data: {{
        labels: [],
        datasets: [],
      }},
      options: commonOptions,
    }});

    const perRunCpuChart = new Chart(document.getElementById('perRunCpuChart'), {{
      type: 'line',
      data: {{
        labels: [],
        datasets: [
          {{
            label: 'cpu mean',
            data: [],
            borderColor: '#116466',
            backgroundColor: 'rgba(17,100,102,0.2)',
            tension: 0.2,
          }},
          {{
            label: 'cpu max',
            data: [],
            borderColor: '#b85c38',
            backgroundColor: 'rgba(184,92,56,0.2)',
            tension: 0.2,
          }}
        ]
      }},
      options: commonOptions,
    }});

    const perRunEnergyChart = new Chart(document.getElementById('perRunEnergyChart'), {{
      type: 'line',
      data: {{
        labels: [],
        datasets: [
          {{
            label: 'energy total',
            data: [],
            borderColor: '#2f3e46',
            backgroundColor: 'rgba(47,62,70,0.2)',
            tension: 0.2,
          }},
          {{
            label: 'energy per request',
            data: [],
            borderColor: '#8a2d3b',
            backgroundColor: 'rgba(138,45,59,0.2)',
            tension: 0.2,
          }}
        ]
      }},
      options: commonOptions,
    }});

    function updateLevelCharts(levelRows) {{
      const labels = levelRows.map((r) => r.workload_level);

      chartEnergyPerRequest.data.labels = labels;
      chartEnergyPerRequest.data.datasets[0].data = levelRows.map((r) => r.energy_per_request_mean);
      chartEnergyPerRequest.update();

      chartEnergyTotal.data.labels = labels;
      chartEnergyTotal.data.datasets[0].data = levelRows.map((r) => r.energy_total_mean);
      chartEnergyTotal.update();

      chartThroughput.data.labels = labels;
      chartThroughput.data.datasets[0].data = levelRows.map((r) => r.throughput_mean);
      chartThroughput.update();

      chartP95.data.labels = labels;
      chartP95.data.datasets[0].data = levelRows.map((r) => r.p95_latency_mean);
      chartP95.update();

      chartCpuMean.data.labels = labels;
      chartCpuMean.data.datasets[0].data = levelRows.map((r) => r.cpu_mean_mean);
      chartCpuMean.update();
    }}

    function updatePerRunCharts(rows) {{
      const labels = rows.map((r) => r.name);

      perRunCpuChart.data.labels = labels;
      perRunCpuChart.data.datasets[0].data = rows.map((r) => r.cpu_mean);
      perRunCpuChart.data.datasets[1].data = rows.map((r) => r.cpu_max);
      perRunCpuChart.update();

      perRunEnergyChart.data.labels = labels;
      perRunEnergyChart.data.datasets[0].data = rows.map((r) => r.energy_total);
      perRunEnergyChart.data.datasets[1].data = rows.map((r) => r.energy_per_request);
      perRunEnergyChart.update();

      const latencySeries = buildLatencyIterationSeries(rows);
      latencyIterationChart.data.labels = latencySeries.labels;
      latencyIterationChart.data.datasets = latencySeries.datasets;
      latencyIterationChart.update();
    }}

    function renderRunSelector() {{
      const host = document.getElementById('runSelector');
      host.innerHTML = '';

      runNames.forEach((name) => {{
        const label = document.createElement('label');
        label.className = 'run-item';

        const input = document.createElement('input');
        input.type = 'checkbox';
        input.checked = selected.has(name);
        input.addEventListener('change', () => {{
          if (input.checked) selected.add(name);
          else selected.delete(name);
          renderAll();
        }});

        label.appendChild(input);
        label.appendChild(document.createTextNode(name));
        host.appendChild(label);
      }});
    }}

    function renderMeta(rows) {{
      const quality = data.quality_counts || {{}};
      const selectedFlagged = rows.filter((r) => Array.isArray(r.flags) && r.flags.length > 0).length;
      document.getElementById('qualityText').textContent =
        `Quality flags: selected=${{selectedFlagged}}/${{rows.length}}, total=${{quality.runs_with_flags || 0}}/${{quality.total_runs || 0}}`;

      const cpuStdev = computeStdev(rows.map((r) => r.cpu_mean));
      const energyReqStdev = computeStdev(rows.map((r) => r.energy_per_request));
      const throughputStdev = computeStdev(rows.map((r) => r.throughput_mean));

      document.getElementById('consistencyText').textContent =
        `Std dev (selected): cpu_mean=${{formatMaybeNumber(cpuStdev)}}, energy_per_request=${{formatMaybeNumber(energyReqStdev)}}, throughput=${{formatMaybeNumber(throughputStdev)}}`;

      document.getElementById('filtersText').textContent =
        `SUT containers: ${{(data.sut_containers || []).join(', ')}} | Excluded infra: ${{(data.excluded_containers || []).join(', ')}}`;
    }}

    function renderAll() {{
      const rows = selectedRuns();
      const levelRows = computeLevelAggregates(rows);

      renderRunTable(rows);
      renderLevelTable(levelRows);
      updateLevelCharts(levelRows);
      updatePerRunCharts(rows);
      renderMeta(rows);
    }}

    document.getElementById('selectAllBtn').addEventListener('click', () => {{
      runNames.forEach((name) => selected.add(name));
      renderRunSelector();
      renderAll();
    }});

    document.getElementById('selectNoneBtn').addEventListener('click', () => {{
      selected.clear();
      renderRunSelector();
      renderAll();
    }});

    renderConfigSummary();
    renderRunSelector();
    renderAll();
  </script>
</body>
</html>
"""


def main():
    parser = argparse.ArgumentParser(description="Build an HTML run comparison dashboard")
    parser.add_argument(
        "--runs-dir",
        default="runs",
        help="Directory containing run folders (default: runs)",
    )
    parser.add_argument(
        "--run-dir",
        help="Optional single run directory to visualize",
    )
    parser.add_argument(
        "--output",
        default="runs_comparison.html",
        help="Output HTML path (default: runs_comparison.html)",
    )
    parser.add_argument(
        "--sut-container",
        default="nginx",
        help="Container name to highlight as application SUT (default: nginx)",
    )
    args = parser.parse_args()

    runs = collect_runs(args.runs_dir, args.run_dir)
    if not runs:
        raise SystemExit("No summary/metadata files found in the selected run directories")

    data = make_dashboard_data(args.runs_dir, runs, args.sut_container)
    html = build_html(data)

    output_path = Path(args.output)
    output_path.write_text(html, encoding="utf-8")
    print(f"Dashboard written to {output_path}")
    print(f"Runs included: {', '.join(row['name'] for row in data['runs'])}")


if __name__ == "__main__":
    main()
