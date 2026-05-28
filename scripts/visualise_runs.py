#!/usr/bin/env python3
"""Generate an HTML dashboard focused on workload-level run comparison."""



import argparse
import csv
import json
import math
import statistics
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


# Configure which containers belong to the system under test.
# Expanded to include common application services across different deployments.
SUT_CONTAINERS = [
  "nginx",
  "server",
  "frontend",
  "api",
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


def joules_to_wh(value):
  if not isinstance(value, (int, float)):
    return None
  return float(value) / 3600.0


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


def safe_min(values):
  cleaned = [v for v in values if isinstance(v, (int, float))]
  if not cleaned:
    return None
  return min(cleaned)


def safe_max(values):
  cleaned = [v for v in values if isinstance(v, (int, float))]
  if not cleaned:
    return None
  return max(cleaned)


def safe_median(values):
  cleaned = [v for v in values if isinstance(v, (int, float))]
  if not cleaned:
    return None
  return statistics.median(cleaned)


def safe_mad(values):
  cleaned = [v for v in values if isinstance(v, (int, float))]
  if not cleaned:
    return None
  median_value = statistics.median(cleaned)
  deviations = [abs(v - median_value) for v in cleaned]
  return statistics.median(deviations)


def format_level_sort_key(level):
  lowered = (level or "unknown").lower()
  if lowered in DEFAULT_LEVEL_ORDER:
    return (0, DEFAULT_LEVEL_ORDER.index(lowered), lowered)
  return (1, 0, lowered)


def get_filtered_sut_energy_means(summary, sut_container=None, sut_containers=None):
  """Extract SUT energy means, optionally filtering to a specific container.

  `sut_containers` may be provided (list/set) to override the default `SUT_CONTAINERS` allowlist.
  If the allowlist matches no containers, falls back to all non-excluded containers.
  """
  energy = summary.get("energy_by_container_name", {}) if isinstance(summary, dict) else {}
  out = {}
  allowed = set(sut_containers) if isinstance(sut_containers, (list, tuple, set)) else set(SUT_CONTAINERS)

  # If sut_containers was explicitly provided, use fallback strategy: try exact matches first, then all non-excluded.
  use_fallback = isinstance(sut_containers, (list, tuple, set))

  for container_name, stats in energy.items():
    if container_name in EXCLUDED_CONTAINERS:
      continue
    # If a specific container is requested, prioritize that one (only when using default allowlist).
    if sut_container and not use_fallback:
      if container_name != sut_container:
        continue
    else:
      if container_name not in allowed:
        continue
    if not isinstance(stats, dict):
      continue
    value = stats.get("mean")
    if isinstance(value, (int, float)):
      out[container_name] = value

  # Fallback: if allowlist returned no matches and we were using an explicit allowlist, include all non-excluded.
  if not out and use_fallback:
    for container_name, stats in energy.items():
      if container_name in EXCLUDED_CONTAINERS:
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


def infer_power_meter_config(metadata, plan):
    if isinstance(plan, dict):
        power_meter = plan.get("power_meter")
        if isinstance(power_meter, dict):
            return power_meter

    if isinstance(metadata, dict):
        power_meter = metadata.get("power_meter")
        if isinstance(power_meter, dict):
            return power_meter

    return None


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


def load_attribution_artifacts(run_dir):
    attribution_dir = Path(run_dir) / "attribution"
    attribution_json_path = attribution_dir / "attribution.json"
    service_csv_path = attribution_dir / "service_attribution.csv"

    if not attribution_json_path.exists() or not service_csv_path.exists():
      missing = []
      if not attribution_json_path.exists():
        missing.append("attribution.json")
      if not service_csv_path.exists():
        missing.append("service_attribution.csv")
      return {
        "present": False,
        "error": None,
        "missing": missing,
        "attribution": None,
        "service_rows": [],
      }

    try:
      attribution_payload = load_json(attribution_json_path)
      service_rows = load_csv_rows(service_csv_path)
      return {
        "present": True,
        "error": None,
        "missing": [],
        "attribution": attribution_payload if isinstance(attribution_payload, dict) else None,
        "service_rows": service_rows,
      }
    except (OSError, json.JSONDecodeError, csv.Error, ValueError) as exc:
      return {
        "present": False,
        "error": str(exc),
        "missing": [],
        "attribution": None,
        "service_rows": [],
      }


def normalize_service_row(row):
    normalized = dict(row)
    normalized["allocated_energy_joules"] = parse_float(row.get("allocated_energy_joules"))
    normalized["container_count"] = parse_float(row.get("container_count"))
    normalized["mapped_container_count"] = parse_float(row.get("mapped_container_count"))
    normalized["mapped"] = str(row.get("mapped", "")).strip().lower() == "true"
    return normalized


def extract_service_name_candidate(row):
  """Pick canonical-facing service identity from service attribution rows.

  Preference order keeps Phase 1 anchored to emitted service-level artifacts:
  service_name -> deployment_name -> service -> entity_name.
  """
  if not isinstance(row, dict):
    return None, None

  for source in ["service_name", "deployment_name", "service", "entity_name"]:
    value = row.get(source)
    if value is None:
      continue
    text = str(value).strip()
    if text:
      return text, source

  return None, None


def service_alias_key(name):
  if name is None:
    return None
  text = str(name).strip().lower()
  if not text:
    return None
  return "".join(ch for ch in text if ch.isalnum())


def service_name_score(name, source):
  source_priority = {
    "service_name": 0,
    "deployment_name": 1,
    "service": 2,
    "entity_name": 3,
  }
  raw = str(name or "")
  lowered = raw.lower()
  separator_penalty = sum(1 for ch in lowered if ch in " _./:")
  dash_penalty = lowered.count("-")
  return (
    source_priority.get(source, 99),
    separator_penalty + dash_penalty,
    len(lowered),
    lowered,
  )


def resolve_canonical_service_name(row, model_alias_registry):
  candidate, source = extract_service_name_candidate(row)
  alias = service_alias_key(candidate)
  if alias is None:
    return None

  score = service_name_score(candidate, source)
  existing = model_alias_registry.get(alias)
  if existing is None or score < existing["score"]:
    model_alias_registry[alias] = {"name": candidate, "score": score}

  return model_alias_registry[alias]["name"]


def compute_phase1_attribution_aggregates(run_items):
    by_workload = {}
    services_found_per_model = {"M1": set(), "M2": set()}
    sum_consistency = {}
    model_alias_registry = {"M1": {}, "M2": {}}

    for item in run_items:
      run_name = item.get("run_name")
      metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
      workload = infer_workload_level(run_name, metadata)
      workload_bucket = by_workload.setdefault(
        workload,
        {
          "iterations_total": 0,
          "attribution_present": 0,
          "attribution_missing": 0,
          "attribution_error": 0,
          "models": {
            "M1": {"valid_iterations": 0, "missing_or_failed_iterations": 0, "services": {}},
            "M2": {"valid_iterations": 0, "missing_or_failed_iterations": 0, "services": {}},
          },
          "m2_primary_services": set(),
        },
      )
      workload_bucket["iterations_total"] += 1

      attr = item.get("attribution") if isinstance(item.get("attribution"), dict) else None
      if not attr:
        workload_bucket["attribution_missing"] += 1
        for model in ["M1", "M2"]:
          workload_bucket["models"][model]["missing_or_failed_iterations"] += 1
        if item.get("attribution_error"):
          workload_bucket["attribution_error"] += 1
        continue

      workload_bucket["attribution_present"] += 1
      service_rows = [normalize_service_row(r) for r in attr.get("service_rows", [])]
      if not service_rows:
        service_rows = [normalize_service_row(r) for r in (attr.get("artifacts", {}).get("service_rows") or [])]
      if not service_rows:
        service_rows = [normalize_service_row(r) for r in item.get("attribution_service_rows", [])]

      report = attr.get("attribution_report") if isinstance(attr.get("attribution_report"), dict) else None
      model_coverage = {}
      models = {}
      if isinstance(report, dict) and isinstance(report.get("models"), dict):
        models = report.get("models")
      elif isinstance(attr.get("models"), dict):
        models = attr.get("models")

      for model in ["M1", "M2"]:
        model_payload = models.get(model) if isinstance(models, dict) else None
        coverage = model_payload.get("coverage") if isinstance(model_payload, dict) else None
        model_coverage[model] = coverage if isinstance(coverage, dict) else {}

      for model in ["M1", "M2"]:
        rows_for_model = [r for r in service_rows if str(r.get("model_variant")) == model]
        if not rows_for_model:
          workload_bucket["models"][model]["missing_or_failed_iterations"] += 1
          continue

        workload_bucket["models"][model]["valid_iterations"] += 1
        services_for_model = workload_bucket["models"][model]["services"]
        for row in rows_for_model:
          service_name = resolve_canonical_service_name(row, model_alias_registry[model])
          if not service_name:
            continue
          services_found_per_model[model].add(service_name)
          value = row.get("allocated_energy_joules")
          if not isinstance(value, (int, float)):
            continue
          services_for_model.setdefault(service_name, []).append(value)
          if model == "M2" and row.get("service_group") == "primary_application":
            workload_bucket["m2_primary_services"].add(service_name)

        service_sum = sum(
          value
          for value in [r.get("allocated_energy_joules") for r in rows_for_model]
          if isinstance(value, (int, float))
        )
        reference_sum = model_coverage.get(model, {}).get("total_attributed_energy_joules")
        delta = None
        consistent = None
        if isinstance(reference_sum, (int, float)):
          delta = service_sum - reference_sum
          consistent = math.isclose(service_sum, reference_sum, rel_tol=1e-9, abs_tol=1e-9)

        sum_consistency.setdefault(workload, {}).setdefault(model, []).append(
          {
            "run_name": run_name,
            "service_sum": service_sum,
            "reference_sum": reference_sum,
            "delta": delta,
            "consistent": consistent,
          }
        )

    # Build compact aggregate stats per workload/model/service
    aggregate = {
      "model_labels": {
        "M2": "primary_valid_service_attribution",
        "M1": "identity_limited_direct_kepler_diagnostic",
      },
      "workloads": {},
      "services_found_per_model": {
        "M1": sorted(services_found_per_model["M1"]),
        "M2": sorted(services_found_per_model["M2"]),
      },
    }

    for workload, payload in sorted(by_workload.items(), key=lambda item: format_level_sort_key(item[0])):
      aggregate_workload = {
        "iterations_total": payload["iterations_total"],
        "attribution_present": payload["attribution_present"],
        "attribution_missing": payload["attribution_missing"],
        "attribution_error": payload["attribution_error"],
        "m2_primary_application_services": sorted(payload["m2_primary_services"]),
        "models": {},
        "sum_consistency": {},
      }

      for model in ["M1", "M2"]:
        model_payload = payload["models"][model]
        model_services = {}
        for service_name, values in sorted(model_payload["services"].items()):
          mean_value = safe_mean(values)
          median_value = safe_median(values)
          stdev_value = safe_stdev(values)
          mad_value = safe_mad(values)
          cv_value = None
          robust_cv_value = None
          if isinstance(mean_value, (int, float)) and mean_value != 0:
            cv_value = stdev_value / mean_value
          if isinstance(median_value, (int, float)) and median_value != 0 and isinstance(mad_value, (int, float)):
            robust_cv_value = mad_value / median_value

          model_services[service_name] = {
            "count": len(values),
            "mean": mean_value,
            "median": median_value,
            "std": stdev_value,
            "min": safe_min(values),
            "max": safe_max(values),
            "cv": cv_value,
            "mad": mad_value,
            "robust_cv": robust_cv_value,
          }

        aggregate_workload["models"][model] = {
          "valid_iterations": model_payload["valid_iterations"],
          "missing_or_failed_iterations": model_payload["missing_or_failed_iterations"],
          "services": model_services,
        }

        checks = sum_consistency.get(workload, {}).get(model, [])
        aggregate_workload["sum_consistency"][model] = {
          "runs_checked": len(checks),
          "runs_with_reference": sum(1 for c in checks if isinstance(c.get("reference_sum"), (int, float))),
          "all_consistent": all(c.get("consistent") is True for c in checks if c.get("consistent") is not None),
          "max_abs_delta": safe_max([abs(c["delta"]) for c in checks if isinstance(c.get("delta"), (int, float))]),
          "checks": checks,
        }

      aggregate["workloads"][workload] = aggregate_workload

    return aggregate


def format_phase1_validation_summary(attribution_aggregate):
    lines = []
    lines.append("=== ATTRIBUTION PHASE1 VALIDATION SUMMARY ===")
    workloads = sorted(attribution_aggregate.get("workloads", {}).keys(), key=format_level_sort_key)
    lines.append(f"workloads_found: {', '.join(workloads) if workloads else '<none>'}")

    for workload in workloads:
      payload = attribution_aggregate["workloads"][workload]
      lines.append(f"- workload={workload}")
      lines.append(
        "  iterations="
        f"total:{payload.get('iterations_total', 0)} "
        f"attr_present:{payload.get('attribution_present', 0)} "
        f"attr_missing:{payload.get('attribution_missing', 0)} "
        f"attr_error:{payload.get('attribution_error', 0)}"
      )
      lines.append(
        "  m2_primary_application_services: "
        + (", ".join(payload.get("m2_primary_application_services", [])) or "<none>")
      )

      for model in ["M1", "M2"]:
        model_data = payload.get("models", {}).get(model, {})
        services = sorted((model_data.get("services") or {}).keys())
        lines.append(
          f"  {model}: valid_iterations={model_data.get('valid_iterations', 0)} "
          f"missing_or_failed={model_data.get('missing_or_failed_iterations', 0)} "
          f"services={len(services)}"
        )
        lines.append("    services_list: " + (", ".join(services) if services else "<none>"))

        consistency = payload.get("sum_consistency", {}).get(model, {})
        lines.append(
          "    sum_consistency: "
          f"runs_checked={consistency.get('runs_checked', 0)} "
          f"runs_with_reference={consistency.get('runs_with_reference', 0)} "
          f"all_consistent={consistency.get('all_consistent')} "
          f"max_abs_delta={consistency.get('max_abs_delta')}"
        )

    global_services = attribution_aggregate.get("services_found_per_model", {})
    lines.append("services_found_per_model:")
    lines.append("  M1: " + (", ".join(global_services.get("M1", [])) or "<none>"))
    lines.append("  M2: " + (", ".join(global_services.get("M2", [])) or "<none>"))
    return "\n".join(lines)


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
    power_meter_config = None

    if isinstance(plan, dict):
        app_name = Path(str(plan.get("app", ""))).name or None
        energy_source = plan.get("energy_source")
        prom_url_masked = mask_prom_url(plan.get("prom_url"))
        power_meter_config = infer_power_meter_config(first_metadata, plan)

        if plan.get("_plan_file") == "workload_plan.json":
            levels_used = [
                entry.get("label")
                for entry in plan.get("workload_levels", [])
                if isinstance(entry, dict) and entry.get("label")
            ]
            repetitions_per_level = plan.get("count")
            warmup_enabled = True
            cleanup_reset_enabled = True
            cooldown_seconds = plan.get("cooldown_seconds")
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

        if power_meter_config is None:
            power_meter_config = infer_power_meter_config(first_metadata, plan)

    if isinstance(first_summary, dict):
        summary_energy_source = first_summary.get("energy_source", {})
        if isinstance(summary_energy_source, dict) and not energy_source:
            energy_source = summary_energy_source.get("requested_energy_source") or summary_energy_source.get(
                "selected_energy_source"
            )

    if power_meter_config is None and isinstance(first_summary, dict):
        meter_summary = first_summary.get("physical_power_meter", {})
        if isinstance(meter_summary, dict):
            power_meter_config = {
                "enabled": True,
                "url": meter_summary.get("source_url"),
                "interval_seconds": meter_summary.get("interval_seconds"),
                "request_timeout_seconds": meter_summary.get("request_timeout_seconds"),
            }

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
        "power_meter": power_meter_config,
        "experiment_name": Path(runs_dir).name,
    }


def build_run_row(item, sut_container, sut_containers=None):
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

    meter_summary = summary.get("physical_power_meter", {}) if isinstance(summary, dict) else {}
    meter_metrics = meter_summary.get("metrics", {}) if isinstance(meter_summary, dict) else {}
    meter_power_stats = meter_metrics.get("apower", {}) if isinstance(meter_metrics, dict) else {}
    meter_energy_stats = meter_metrics.get("aenergy_total", {}) if isinstance(meter_metrics, dict) else {}
    meter_sample_count = meter_summary.get("sample_count") if isinstance(meter_summary, dict) else None
    meter_error_count = meter_summary.get("error_count") if isinstance(meter_summary, dict) else None
    meter_power_mean = meter_power_stats.get("mean") if isinstance(meter_power_stats, dict) else None
    meter_baseline_energy_joules = None
    meter_raw_energy_joules = meter_summary.get("raw_energy_delta_joules") if isinstance(meter_summary, dict) else None
    meter_corrected_energy_joules = meter_summary.get("baseline_corrected_workload_energy_joules") if isinstance(meter_summary, dict) else None
    meter_energy_per_request_joules = meter_summary.get("meter_energy_per_request_joules") if isinstance(meter_summary, dict) else None
    meter_quality_flags = meter_summary.get("quality_flags", []) if isinstance(meter_summary, dict) else []
    meter_raw_energy_wh = joules_to_wh(meter_raw_energy_joules)
    meter_baseline_energy_wh = None
    meter_corrected_energy_wh = joules_to_wh(meter_corrected_energy_joules)
    meter_energy_per_request_wh = joules_to_wh(meter_energy_per_request_joules)

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

    if isinstance(meter_power_mean, (int, float)) and isinstance(duration_seconds, (int, float)):
      meter_baseline_energy_joules = meter_power_mean * duration_seconds
      meter_baseline_energy_wh = joules_to_wh(meter_baseline_energy_joules)

    cooldown_seconds = None
    saturation = metadata.get("workload_parameters", {}).get("saturation", {})
    if isinstance(saturation, dict):
        cooldown_seconds = saturation.get("cooldown_seconds")

    ramp_exclusion_seconds = metadata.get("ramp_exclusion_seconds")

    sut_means = get_filtered_sut_energy_means(summary, sut_container, sut_containers)
    # Prefer the specific sut_container if available and non-zero, otherwise sum all filtered containers
    sut_energy_mean = sut_means.get(sut_container) if sut_means.get(sut_container) else (
        sum(sut_means.values()) / len(sut_means) if sut_means else None
    )

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

    meter_enabled = bool((metadata.get("power_meter") or {}).get("url"))
    if meter_enabled and not isinstance(meter_corrected_energy_joules, (int, float)):
      flags.append("power_meter_missing")
    for meter_flag in meter_quality_flags:
      if meter_flag:
        flags.append(str(meter_flag))

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
        "meter_sample_count": meter_sample_count,
        "meter_error_count": meter_error_count,
        "meter_power_mean": meter_power_mean,
        "meter_baseline_energy_joules": meter_baseline_energy_joules,
        "meter_baseline_energy_wh": meter_baseline_energy_wh,
        "meter_raw_energy_joules": meter_raw_energy_joules,
        "meter_raw_energy_wh": meter_raw_energy_wh,
        "meter_corrected_energy_joules": meter_corrected_energy_joules,
        "meter_corrected_energy_wh": meter_corrected_energy_wh,
        "meter_energy_per_request_joules": meter_energy_per_request_joules,
        "meter_energy_per_request_wh": meter_energy_per_request_wh,
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
          "meter_power_mean": safe_mean([r.get("meter_power_mean") for r in rows]),
          "meter_power_std": safe_stdev([r.get("meter_power_mean") for r in rows]),
          "meter_corrected_energy_wh_mean": safe_mean([r.get("meter_corrected_energy_wh") for r in rows]),
          "meter_corrected_energy_wh_std": safe_stdev([r.get("meter_corrected_energy_wh") for r in rows]),
          "display_energy_total_mean": safe_mean([
            r.get("meter_corrected_energy_wh") if isinstance(r.get("meter_corrected_energy_wh"), (int, float)) else r.get("energy_total")
            for r in rows
          ]),
          "display_energy_total_std": safe_stdev([
            r.get("meter_corrected_energy_wh") if isinstance(r.get("meter_corrected_energy_wh"), (int, float)) else r.get("energy_total")
            for r in rows
          ]),
          "display_energy_per_request_mean": safe_mean([
            r.get("meter_energy_per_request_wh") if isinstance(r.get("meter_energy_per_request_wh"), (int, float)) else r.get("energy_per_request")
            for r in rows
          ]),
          "display_energy_per_request_std": safe_stdev([
            r.get("meter_energy_per_request_wh") if isinstance(r.get("meter_energy_per_request_wh"), (int, float)) else r.get("energy_per_request")
            for r in rows
          ]),
            }
        )

    return aggregates


def make_dashboard_data(runs_dir, runs, sut_container):
    # Prefer metadata-provided SUT containers where available
    sut_containers_list = None
    for item in runs:
        md = item.get("metadata") if isinstance(item.get("metadata"), dict) else None
        if isinstance(md, dict):
            sc = md.get("sut_containers") or (md.get("deployment") or {}).get("sut_containers")
            if isinstance(sc, list) and sc:
                sut_containers_list = sc
                break
    if sut_containers_list is None:
        sut_containers_list = SUT_CONTAINERS

    run_rows = [build_run_row(item, sut_container, sut_containers_list) for item in runs]
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

    run_items_with_attr = []
    for item in runs:
        attr_payload = load_attribution_artifacts(item.get("run_dir"))
        run_items_with_attr.append(
            {
                **item,
                "attribution": attr_payload.get("attribution"),
                "attribution_service_rows": attr_payload.get("service_rows") or [],
                "attribution_present": bool(attr_payload.get("present")),
                "attribution_missing": attr_payload.get("missing") or [],
                "attribution_error": attr_payload.get("error"),
            }
        )

    attribution_phase1 = compute_phase1_attribution_aggregates(run_items_with_attr)

    return {
        "sut_container": sut_container,
        "sut_containers": sut_containers_list,
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
        "attribution_phase1": attribution_phase1,
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

    <section class=\"controls\" style=\"margin-bottom:14px;\">
      <h2>Trend Chart X-Axis</h2>
      <div class=\"actions\">
        <button id=\"axisIterationsBtn\" type=\"button\">Iterations</button>
        <button id=\"axisLevelsBtn\" type=\"button\">Actual levels</button>
      </div>
      <div class=\"small\">Switch the latency and energy trend charts between iteration order and workload-level labels.</div>
    </section>


    <section class="card" style="margin-bottom:14px;">
      <h2>Per-Level Aggregate Summary (Mean ± Std Dev)</h2>
      <div class="table-wrap">
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
          <tbody id="levelTableBody"></tbody>
        </table>
      </div>
    </section>

    <!-- Phase 2 Attribution Panels -->
    <section class="card" style="margin-bottom:14px;">
      <h2>Attribution Quality Summary by Workload</h2>
      <div id="attrQualitySummary"></div>
    </section>

    <section class="card" style="margin-bottom:14px;">
      <h2>Total Service Contribution by Workload</h2>
      <canvas id="m2PrimaryEnergyBar"></canvas>
      <div class="small" id="m2PrimaryEnergyWarn"></div>
    </section>

    <section class="card" style="margin-bottom:14px;">
      <h2>Top Service Energy Trends</h2>
      <canvas id="topServiceTrends"></canvas>
      <div class="small" id="topServiceTrendsWarn"></div>
    </section>

    <section class="card" style="margin-bottom:14px;">
      <h2>M1 vs M2 Diagnostic Comparison</h2>
      <canvas id="m1m2Compare"></canvas>
      <div class="small">M1 is identity-limited and not valid for service-level attribution. M2 is the valid SUT model.</div>
    </section>

    <section class="card" style="margin-bottom:14px;">
      <h2>Attribution Aggregate Table</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Workload</th>
              <th>Model</th>
              <th>Service</th>
              <th>Mean (J)</th>
              <th>Median (J)</th>
              <th>Std</th>
              <th>CV</th>
              <th>Robust CV</th>
              <th>Iterations</th>
              <th>Missing/Failed</th>
            </tr>
          </thead>
          <tbody id="attrAggTableBody"></tbody>
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
              <th>Meter Samples</th>
              <th>Meter Power Mean</th>
              <th>Meter Raw Energy</th>
              <th>Meter Corrected Energy</th>
              <th>Meter Energy / Request</th>
              <th>Meter Errors</th>
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

    <section class=\"card\" style=\"margin-bottom:14px;\">
      <h2>Energy Comparison over Iterations (Kepler vs Physical Meter)</h2>
      <canvas id=\"energyComparisonChart\"></canvas>
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

    function joulesToWh(value) {{
      if (typeof value !== 'number' || Number.isNaN(value)) return null;
      return value / 3600.0;
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
          energy_per_request_wh_mean: computeMean(metricValues('energy_per_request').map(joulesToWh)),
          energy_per_request_wh_std: computeStdev(metricValues('energy_per_request').map(joulesToWh)),
          meter_power_mean: computeMean(metricValues('meter_power_mean')),
          meter_power_std: computeStdev(metricValues('meter_power_mean')),
          meter_corrected_energy_wh_mean: computeMean(metricValues('meter_corrected_energy_wh')),
          meter_corrected_energy_wh_std: computeStdev(metricValues('meter_corrected_energy_wh')),
          display_energy_total_mean: computeMean([...items].map((r) => r.meter_corrected_energy_wh ?? r.energy_total)),
          display_energy_total_std: computeStdev([...items].map((r) => r.meter_corrected_energy_wh ?? r.energy_total)),
          display_energy_per_request_mean: computeMean([...items].map((r) => r.meter_energy_per_request_wh ?? r.energy_per_request)),
          display_energy_per_request_std: computeStdev([...items].map((r) => r.meter_energy_per_request_wh ?? r.energy_per_request)),
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

    function displayLevelLabel(row) {{
      const runName = String((row && row.name) || '');
      const levelMatch = runName.match(/level_(\\d+)/i);
      if (levelMatch && levelMatch[1]) {{
        return String(parseInt(levelMatch[1], 10));
      }}
      return String((row && row.workload_level) || 'unknown');
    }}

    function displayLevelSortKey(level) {{
      const text = String(level || 'unknown');
      if (/^\\d+$/.test(text)) return [0, Number(text), text];
      return levelSortKey(text);
    }}

    function sortDisplayLevels(levels) {{
      return [...levels].sort((a, b) => {{
        const ka = displayLevelSortKey(a);
        const kb = displayLevelSortKey(b);
        if (ka[0] !== kb[0]) return ka[0] - kb[0];
        if (ka[1] !== kb[1]) return ka[1] - kb[1];
        return ka[2].localeCompare(kb[2]);
      }});
    }}

    function groupRowsByDisplayLevel(rows) {{
      const grouped = new Map();
      rows.forEach((r) => {{
        const level = displayLevelLabel(r);
        if (!grouped.has(level)) grouped.set(level, []);
        grouped.get(level).push(r);
      }});
      return grouped;
    }}

    function buildLatencyIterationSeries(rows) {{
      const grouped = new Map();
      rows.forEach((r) => {{
        const level = displayLevelLabel(r);
        if (!grouped.has(level)) grouped.set(level, []);
        grouped.get(level).push(r);
      }});

      const levels = sortDisplayLevels(Array.from(grouped.keys()));
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

    function buildLatencyLevelSeries(rows) {{
      const grouped = groupRowsByDisplayLevel(rows);
      const levels = sortDisplayLevels(Array.from(grouped.keys()));
      const labels = levels;
      const dataPoints = levels.map((level) => {{
        const values = (grouped.get(level) || []).map((row) => row.p95_latency);
        return computeMean(values);
      }});

      return {{
        labels,
        datasets: [{{
          label: 'p95 latency',
          data: dataPoints,
          borderColor: '#8a2d3b',
          backgroundColor: '#8a2d3b55',
          tension: 0.2,
          spanGaps: true,
          borderWidth: 2,
        }}],
      }};
    }}

    function buildEnergyComparisonIterationSeries(rows) {{
      const grouped = new Map();
      rows.forEach((r) => {{
        const level = displayLevelLabel(r);
        if (!grouped.has(level)) grouped.set(level, []);
        grouped.get(level).push(r);
      }});

      const levels = sortDisplayLevels(Array.from(grouped.keys()));
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
      const datasets = [];

      orderedRowsByLevel.forEach((entry, levelIdx) => {{
        const keplerData = labels.map((_, i) => {{
          const row = entry.rows[i];
          if (!row || typeof row.energy_total !== 'number' || Number.isNaN(row.energy_total)) return null;
          return row.energy_total;
        }});
        const baselineData = labels.map((_, i) => {{
          const row = entry.rows[i];
          const value = row ? row.meter_baseline_energy_wh : null;
          if (typeof value !== 'number' || Number.isNaN(value)) return null;
          return value;
        }});
        const meterData = labels.map((_, i) => {{
          const row = entry.rows[i];
          const corrected = row ? row.meter_corrected_energy_wh : null;
          const legacy = row ? row.meter_energy_delta : null;
          const value = (typeof corrected === 'number' && !Number.isNaN(corrected)) ? corrected : legacy;
          if (typeof value !== 'number' || Number.isNaN(value)) return null;
          return value;
        }});

        const hasKeplerData = keplerData.some((v) => v !== null);
        const hasBaselineData = baselineData.some((v) => v !== null);
        const hasMeterData = meterData.some((v) => v !== null);

        if (hasKeplerData) {{
          datasets.push({{
            label: `${{entry.level}} - Kepler energy (Wh)`,
            data: keplerData,
            borderColor: '#116466',
            backgroundColor: '#11646655',
            tension: 0.2,
            spanGaps: true,
            borderWidth: 2,
          }});
        }}

        if (hasBaselineData) {{
          datasets.push({{
            label: `${{entry.level}} - Baseline energy (Wh)`,
            data: baselineData,
            borderColor: '#556b2f',
            backgroundColor: '#556b2f55',
            tension: 0.2,
            spanGaps: true,
            borderWidth: 2,
            borderDash: [2, 4],
          }});
        }}

        if (hasMeterData) {{
          datasets.push({{
            label: `${{entry.level}} - Meter energy (Wh)`,
            data: meterData,
            borderColor: '#b85c38',
            backgroundColor: '#b85c3855',
            tension: 0.2,
            spanGaps: true,
            borderWidth: 2,
            borderDash: [5, 5],
          }});
        }}
      }});

      return {{ labels, datasets }};
    }}

    function buildEnergyComparisonLevelSeries(rows) {{
      const grouped = groupRowsByDisplayLevel(rows);
      const levels = sortDisplayLevels(Array.from(grouped.keys()));
      const labels = levels;
      const keplerData = levels.map((level) => {{
        const values = (grouped.get(level) || []).map((row) => row.energy_total);
        return computeMean(values);
      }});
      const baselineData = levels.map((level) => {{
        const values = (grouped.get(level) || []).map((row) => row.meter_baseline_energy_wh);
        return computeMean(values);
      }});
      const meterData = levels.map((level) => {{
        const values = (grouped.get(level) || []).map((row) => row.meter_corrected_energy_wh);
        return computeMean(values);
      }});

      const datasets = [];
      if (keplerData.some((value) => value !== null)) {{
        datasets.push({{
          label: 'Kepler energy (Wh)',
          data: keplerData,
          borderColor: '#116466',
          backgroundColor: '#11646655',
          tension: 0.2,
          spanGaps: true,
          borderWidth: 2,
        }});
      }}
      if (baselineData.some((value) => value !== null)) {{
        datasets.push({{
          label: 'Baseline energy (Wh)',
          data: baselineData,
          borderColor: '#556b2f',
          backgroundColor: '#556b2f55',
          tension: 0.2,
          spanGaps: true,
          borderWidth: 2,
          borderDash: [2, 4],
        }});
      }}
      if (meterData.some((value) => value !== null)) {{
        datasets.push({{
          label: 'Meter energy (Wh)',
          data: meterData,
          borderColor: '#b85c38',
          backgroundColor: '#b85c3855',
          tension: 0.2,
          spanGaps: true,
          borderWidth: 2,
          borderDash: [5, 5],
        }});
      }}

      return {{ labels, datasets }};
    }}

    function renderConfigSummary() {{
      const cfg = data.experiment_config || {{}};
      const rows = [
        ['Experiment', cfg.experiment_name || '-'],
        ['App', cfg.app_name || '-'],
        ['Environment', cfg.environment_name || '-'],
        ['Energy source', cfg.energy_source || '-'],
        ['Physical meter', cfg.power_meter ? 'enabled' : 'disabled'],
        ['Meter URL', (cfg.power_meter && cfg.power_meter.url) ? cfg.power_meter.url : '-'],
        ['Meter interval seconds', (cfg.power_meter && cfg.power_meter.interval_seconds !== undefined) ? cfg.power_meter.interval_seconds : '-'],
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
          r.meter_sample_count ?? '-',
          formatMaybeNumber(r.meter_power_mean, 6),
          formatMaybeNumber(r.meter_raw_energy_wh, 6),
          formatMaybeNumber(r.meter_corrected_energy_wh, 6),
          formatMaybeNumber(r.meter_energy_per_request_wh, 9),
          r.meter_error_count ?? '-',
          (r.flags || []).join(' | ') || '-',
        ];

        cells.forEach((value, idx) => {{
          const td = document.createElement('td');
          td.textContent = value;
          if (idx === 19 && value !== '-') td.className = 'warn';
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
          formatMeanStd(row.display_energy_per_request_mean, row.display_energy_per_request_std, 9),
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

    const chartEnergyPerRequest = buildBarChart('chartEnergyPerRequest', 'Kepler energy / request (Wh)', '#116466');
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

    const energyComparisonChart = new Chart(document.getElementById('energyComparisonChart'), {{
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
            label: 'kepler energy total',
            data: [],
            borderColor: '#2f3e46',
            backgroundColor: 'rgba(47,62,70,0.2)',
            tension: 0.2,
          }},
          {{
            label: 'meter corrected energy total',
            data: [],
            borderColor: '#b85c38',
            backgroundColor: 'rgba(184,92,56,0.2)',
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

    const m2PrimaryEnergyBar = new Chart(document.getElementById('m2PrimaryEnergyBar'), {{
      type: 'bar',
      data: {{ labels: [], datasets: [] }},
      options: {{
        responsive: true,
        maintainAspectRatio: false,
        interaction: {{ mode: 'index', intersect: false }},
        plugins: {{
          legend: {{
            position: 'right',
            labels: {{ boxWidth: 10, boxHeight: 10 }}
          }}
        }},
        scales: {{
          x: {{ stacked: true }},
          y: {{ beginAtZero: true, stacked: true, title: {{ display: true, text: 'Allocated energy (J)' }} }},
        }},
      }},
    }});

    const topServiceTrends = new Chart(document.getElementById('topServiceTrends'), {{
      type: 'bar',
      data: {{ labels: [], datasets: [] }},
      options: {{
        responsive: true,
        maintainAspectRatio: false,
        interaction: {{ mode: 'index', intersect: false }},
        plugins: {{
          legend: {{
            position: 'bottom',
            labels: {{ boxWidth: 10, boxHeight: 10, usePointStyle: true }}
          }}
        }},
        scales: {{
          x: {{ stacked: false }},
          y: {{ beginAtZero: true, title: {{ display: true, text: 'Mean allocated energy (J)' }} }},
        }},
      }},
    }});

    const m1m2Compare = new Chart(document.getElementById('m1m2Compare'), {{
      type: 'bar',
      data: {{ labels: [], datasets: [] }},
      options: commonOptions,
    }});

    function updateLevelCharts(levelRows) {{
      const labels = levelRows.map((r) => r.workload_level);

      chartEnergyPerRequest.data.labels = labels;
      chartEnergyPerRequest.data.datasets[0].data = levelRows.map((r) => r.energy_per_request_wh_mean);
      chartEnergyPerRequest.data.datasets[0].label = 'Kepler energy / request (Wh)';
      if (chartEnergyPerRequest.data.datasets.length < 2) {{
        chartEnergyPerRequest.data.datasets.push({{
          label: 'Meter energy / request (Wh)',
          data: [],
          borderColor: '#b85c38',
          backgroundColor: '#b85c3855',
          borderWidth: 1,
        }});
      }}
      chartEnergyPerRequest.data.datasets[1].data = levelRows.map((r) => r.display_energy_per_request_mean);
      chartEnergyPerRequest.update();

      chartEnergyTotal.data.labels = labels;
      chartEnergyTotal.data.datasets[0].data = levelRows.map((r) => r.display_energy_total_mean);
      if (chartEnergyTotal.data.datasets.length < 2) {{
        chartEnergyTotal.data.datasets.push({{
          label: 'meter corrected energy mean',
          data: [],
          borderColor: '#b85c38',
          backgroundColor: '#b85c3855',
          borderWidth: 1,
        }});
      }}
      chartEnergyTotal.data.datasets[1].data = levelRows.map((r) => r.meter_corrected_energy_wh_mean);
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

    let chartAxisMode = 'iterations';

    function updateAxisModeButtons() {{
      const iterationsBtn = document.getElementById('axisIterationsBtn');
      const levelsBtn = document.getElementById('axisLevelsBtn');
      if (iterationsBtn) iterationsBtn.classList.toggle('active', chartAxisMode === 'iterations');
      if (levelsBtn) levelsBtn.classList.toggle('active', chartAxisMode === 'levels');
    }}

    function updatePerRunCharts(rows) {{
      const labels = rows.map((r) => r.name);

      perRunCpuChart.data.labels = labels;
      perRunCpuChart.data.datasets[0].data = rows.map((r) => r.cpu_mean);
      perRunCpuChart.data.datasets[1].data = rows.map((r) => r.cpu_max);
      perRunCpuChart.update();

      perRunEnergyChart.data.labels = labels;
      perRunEnergyChart.data.datasets[0].data = rows.map((r) => r.energy_total);
      perRunEnergyChart.data.datasets[1].data = rows.map((r) => r.meter_corrected_energy_wh);
      perRunEnergyChart.data.datasets[2].data = rows.map((r) => r.energy_per_request);
      perRunEnergyChart.update();

      const latencySeries = chartAxisMode === 'levels' ? buildLatencyLevelSeries(rows) : buildLatencyIterationSeries(rows);
      latencyIterationChart.data.labels = latencySeries.labels;
      latencyIterationChart.data.datasets = latencySeries.datasets;
      latencyIterationChart.update();

      const energySeries = chartAxisMode === 'levels' ? buildEnergyComparisonLevelSeries(rows) : buildEnergyComparisonIterationSeries(rows);
      energyComparisonChart.data.labels = energySeries.labels;
      energyComparisonChart.data.datasets = energySeries.datasets;
      energyComparisonChart.update();
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

    function getAttributionWorkloadsSorted() {{
      const workloads = (data.attribution_phase1 && data.attribution_phase1.workloads) || {{}};
      return sortLevels(Object.keys(workloads));
    }}

    function renderAttrQualitySummary() {{
      const host = document.getElementById('attrQualitySummary');
      const workloads = (data.attribution_phase1 && data.attribution_phase1.workloads) || {{}};
      const labels = getAttributionWorkloadsSorted();

      if (!labels.length) {{
        host.textContent = 'No attribution workload aggregates found.';
        return;
      }}

      const rows = labels.map((level) => {{
        const w = workloads[level] || {{}};
        const m2 = ((w.models || {{}}).M2) || {{}};
        const services = Object.keys(m2.services || {{}}).length;
        return `${{level}}: iters=${{w.iterations_total || 0}}, attr_present=${{w.attribution_present || 0}}, M2_valid=${{m2.valid_iterations || 0}}, M2_services=${{services}}`;
      }});
      host.textContent = rows.join(' | ');
    }}

    function serviceColorMap(serviceNames) {{
      const palette = ['#116466', '#b85c38', '#2f3e46', '#8a2d3b', '#556b2f', '#5a4e7a', '#e07a5f', '#3d405b', '#6b705c', '#bc6c25', '#7f5539', '#3a86ff'];
      const map = new Map();
      [...serviceNames].sort().forEach((service, idx) => {{
        map.set(service, palette[idx % palette.length]);
      }});
      map.set('Other', '#7c7c7c');
      return map;
    }}

    function shortServiceLabel(name) {{
      const text = String(name || '');
      if (text === 'recommendationservice') return 'recommendation';
      if (text === 'productcatalogservice') return 'productcatalog';
      if (text.endsWith('service')) return text.replace(/service$/i, '');
      return text;
    }}

    function collectM2ServiceSeries(labels) {{
      const workloads = (data.attribution_phase1 && data.attribution_phase1.workloads) || {{}};
      const seriesByService = new Map();

      labels.forEach((level) => {{
        const m2Services = ((((workloads[level] || {{}}).models || {{}}).M2 || {{}}).services) || {{}};
        Object.entries(m2Services).forEach(([service, stats]) => {{
          if (!seriesByService.has(service)) seriesByService.set(service, Array(labels.length).fill(null));
          const mean = stats && typeof stats.mean === 'number' ? stats.mean : null;
          seriesByService.get(service)[labels.indexOf(level)] = mean;
        }});
      }});

      return seriesByService;
    }}

    function hasValidM2Data(seriesByService) {{
      return Array.from(seriesByService.values()).some((values) => values.some((v) => typeof v === 'number' && !Number.isNaN(v)));
    }}

    function rankServicesByMean(seriesByService) {{
      return Array.from(seriesByService.entries()).map(([service, values]) => {{
        const nums = values.filter((v) => typeof v === 'number' && !Number.isNaN(v));
        const mean = nums.length ? nums.reduce((a, b) => a + b, 0) / nums.length : 0;
        return {{ service, values, mean }};
      }}).sort((a, b) => b.mean - a.mean);
    }}

    function collapseTinyServices(seriesByService) {{
      const ranked = rankServicesByMean(seriesByService);
      const totalMean = ranked.reduce((acc, row) => acc + (row.mean > 0 ? row.mean : 0), 0);
      const maxVisibleServices = 8;
      const minShareForStandalone = 0.03;

      const keep = new Set();
      ranked.forEach((row, idx) => {{
        const share = totalMean > 0 ? row.mean / totalMean : 0;
        if (idx < maxVisibleServices && (share >= minShareForStandalone || idx < 5)) keep.add(row.service);
      }});

      if (ranked.length <= keep.size) return {{ collapsed: seriesByService, collapsedCount: 0 }};

      const collapsed = new Map();
      let labelsLength = 0;
      ranked.forEach((row) => {{
        labelsLength = Math.max(labelsLength, row.values.length);
        if (keep.has(row.service)) collapsed.set(row.service, row.values);
      }});

      const otherValues = Array(labelsLength).fill(0);
      let collapsedCount = 0;
      ranked.forEach((row) => {{
        if (keep.has(row.service)) return;
        collapsedCount += 1;
        row.values.forEach((value, idx) => {{
          if (typeof value === 'number' && !Number.isNaN(value)) otherValues[idx] += value;
        }});
      }});

      if (otherValues.some((v) => Math.abs(v) > 1e-9)) collapsed.set('Other', otherValues);
      return {{ collapsed, collapsedCount }};
    }}

    function renderM2PrimaryEnergyBar() {{
      const labels = getAttributionWorkloadsSorted();
      const warnHost = document.getElementById('m2PrimaryEnergyWarn');
      const seriesByService = collectM2ServiceSeries(labels);

      if (!labels.length || !hasValidM2Data(seriesByService)) {{
        m2PrimaryEnergyBar.data.labels = labels;
        m2PrimaryEnergyBar.data.datasets = [];
        m2PrimaryEnergyBar.update();
        warnHost.textContent = 'No valid M2 attribution data available.';
        return;
      }}

      const {{ collapsed, collapsedCount }} = collapseTinyServices(seriesByService);
      const colorByService = serviceColorMap(collapsed.keys());
      const datasets = [];

      Array.from(collapsed.entries()).sort((a, b) => a[0].localeCompare(b[0])).forEach(([service, values], idx) => {{
        const hasNonZero = values.some((v) => typeof v === 'number' && Math.abs(v) > 1e-9);
        if (!hasNonZero) return;
        const color = colorByService.get(service);
        datasets.push({{
          label: shortServiceLabel(service),
          data: values,
          borderColor: color,
          backgroundColor: color + 'bb',
          borderWidth: 1,
          stack: 'm2-energy',
        }});
      }});

      m2PrimaryEnergyBar.data.labels = labels;
      m2PrimaryEnergyBar.data.datasets = datasets;
      m2PrimaryEnergyBar.update();

      if (!datasets.length) {{
        warnHost.textContent = 'No valid M2 attribution data available.';
      }} else if (collapsedCount > 0) {{
        warnHost.textContent = `Collapsed ${'{'}collapsedCount{'}'} low-contribution services into Other.`;
      }} else {{
        warnHost.textContent = '';
      }}
    }}

    function renderTopServiceTrends() {{
      const labels = getAttributionWorkloadsSorted();
      const warnHost = document.getElementById('topServiceTrendsWarn');
      const seriesByService = collectM2ServiceSeries(labels);

      if (!labels.length || !hasValidM2Data(seriesByService)) {{
        topServiceTrends.data.labels = labels;
        topServiceTrends.data.datasets = [];
        topServiceTrends.update();
        warnHost.textContent = 'No valid M2 attribution data available.';
        return;
      }}

      const ranked = rankServicesByMean(seriesByService).filter((row) => row.mean >= 0);

      const top = ranked.slice(0, 5);
      const colorByService = serviceColorMap(seriesByService.keys());
      const datasets = top.map((entry) => {{
        const color = colorByService.get(entry.service);
        return {{
          label: shortServiceLabel(entry.service),
          data: entry.values,
          borderColor: color,
          backgroundColor: color + '99',
          borderWidth: 1,
        }};
      }});

      topServiceTrends.data.labels = labels;
      topServiceTrends.data.datasets = datasets;
      topServiceTrends.update();

      warnHost.textContent = datasets.length ? '' : 'No valid M2 attribution data available.';
    }}

    function renderM1M2DiagnosticComparison() {{
      const workloads = (data.attribution_phase1 && data.attribution_phase1.workloads) || {{}};
      const labels = getAttributionWorkloadsSorted();

      const m1Counts = labels.map((level) => Object.keys((((((workloads[level] || {{}}).models || {{}}).M1 || {{}}).services) || {{}})).length);
      const m2Counts = labels.map((level) => Object.keys((((((workloads[level] || {{}}).models || {{}}).M2 || {{}}).services) || {{}})).length);

      m1m2Compare.data.labels = labels;
      m1m2Compare.data.datasets = [
        {{
          label: 'M1 service count',
          data: m1Counts,
          borderColor: '#8a2d3b',
          backgroundColor: '#8a2d3b99',
          borderWidth: 1,
        }},
        {{
          label: 'M2 service count',
          data: m2Counts,
          borderColor: '#116466',
          backgroundColor: '#11646699',
          borderWidth: 1,
        }},
      ];
      m1m2Compare.update();
    }}

    function renderAttributionAggregateTable() {{
      const body = document.getElementById('attrAggTableBody');
      const workloads = (data.attribution_phase1 && data.attribution_phase1.workloads) || {{}};
      body.innerHTML = '';

      getAttributionWorkloadsSorted().forEach((workload) => {{
        const payload = workloads[workload] || {{}};
        ['M1', 'M2'].forEach((model) => {{
          const modelPayload = ((payload.models || {{}})[model]) || {{}};
          const services = modelPayload.services || {{}};
          const serviceNames = Object.keys(services).sort();

          if (!serviceNames.length) {{
            const tr = document.createElement('tr');
            [
              workload,
              model,
              '-',
              '-',
              '-',
              '-',
              '-',
              '-',
              String(modelPayload.valid_iterations ?? 0),
              String(modelPayload.missing_or_failed_iterations ?? 0),
            ].forEach((value) => {{
              const td = document.createElement('td');
              td.textContent = value;
              tr.appendChild(td);
            }});
            body.appendChild(tr);
            return;
          }}

          serviceNames.forEach((service) => {{
            const stats = services[service] || {{}};
            const tr = document.createElement('tr');
            [
              workload,
              model,
              service,
              formatMaybeNumber(stats.mean, 6),
              formatMaybeNumber(stats.median, 6),
              formatMaybeNumber(stats.std, 6),
              formatMaybeNumber(stats.cv, 6),
              formatMaybeNumber(stats.robust_cv, 6),
              String(modelPayload.valid_iterations ?? 0),
              String(modelPayload.missing_or_failed_iterations ?? 0),
            ].forEach((value) => {{
              const td = document.createElement('td');
              td.textContent = value;
              tr.appendChild(td);
            }});
            body.appendChild(tr);
          }});
        }});
      }});
    }}

    function renderPhase2AttributionPanels() {{
      renderAttrQualitySummary();
      renderM2PrimaryEnergyBar();
      renderTopServiceTrends();
      renderM1M2DiagnosticComparison();
      renderAttributionAggregateTable();
    }}

    function renderAll() {{
      const rows = selectedRuns();
      const levelRows = computeLevelAggregates(rows);

      renderRunTable(rows);
      renderLevelTable(levelRows);
      updateLevelCharts(levelRows);
      updatePerRunCharts(rows);
      renderMeta(rows);
      renderPhase2AttributionPanels();
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

    document.getElementById('axisIterationsBtn').addEventListener('click', () => {{
      chartAxisMode = 'iterations';
      updateAxisModeButtons();
      renderAll();
    }});

    document.getElementById('axisLevelsBtn').addEventListener('click', () => {{
      chartAxisMode = 'levels';
      updateAxisModeButtons();
      renderAll();
    }});

    renderConfigSummary();
    renderRunSelector();
    updateAxisModeButtons();
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
    parser.add_argument(
      "--validate-attribution-contract",
      action="store_true",
      help="Print compact Phase 1 attribution aggregate validation summary",
    )
    args = parser.parse_args()

    runs = collect_runs(args.runs_dir, args.run_dir)
    if not runs:
        raise SystemExit("No summary/metadata files found in the selected run directories")

    data = make_dashboard_data(args.runs_dir, runs, args.sut_container)

    # Temporary forensic debug output to verify dashboard payload integrity.
    print("=== DASHBOARD PAYLOAD DEBUG ===")
    print(f"runs_count: {len(data.get('runs', []))}")
    print(f"dashboard_top_level_keys: {sorted(data.keys())}")
    workload_counts = {
      str(level): int((payload or {}).get("iterations_total", 0))
      for level, payload in (data.get("attribution_phase1", {}).get("workloads", {}) or {}).items()
    }
    print(f"per_workload_counts: {workload_counts}")
    attr_keys = sorted((data.get("attribution_phase1", {}) or {}).keys())
    print(f"attribution_aggregate_keys: {attr_keys}")

    html = build_html(data)

    output_path = Path(args.output)
    output_path.write_text(html, encoding="utf-8")

    if args.validate_attribution_contract:
      print(format_phase1_validation_summary(data.get("attribution_phase1", {})))

    print(f"Dashboard written to {output_path}")
    print(f"Runs included: {', '.join(row['name'] for row in data['runs'])}")


if __name__ == "__main__":
    main()
