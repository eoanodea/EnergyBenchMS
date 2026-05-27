#!/usr/bin/env python3
"""Post-process a completed run into service-level attribution artifacts.

The attribution layer intentionally consumes the raw run artifacts as the source
of truth. It does not modify the experiment runner, telemetry collection, or the
existing summarisation pipeline.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import yaml

from app_config import candidate_config_paths


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_SUBDIR = "attribution"
PRIMARY_GROUP = "primary_application"
AUXILIARY_GROUP = "auxiliary_application"
PLATFORM_GROUP = "platform_observability"
EXTERNAL_OBSERVABILITY_CATEGORY = "external_observability"
SYSTEM_GROUP = "kubernetes_system"
MEASUREMENT_GROUP = "measurement_infrastructure"
UNKNOWN_GROUP = "unknown"
SUT_SCOPE = "sut"

DEFAULT_IGNORE_PATTERNS = [
    r"(^|/)loadgenerator($|[-/])",
    r"(^|/)prometheus($|[-/])",
    r"(^|/)grafana($|[-/])",
    r"(^|/)jaeger($|[-/])",
    r"(^|/)zipkin($|[-/])",
]

BUILTIN_GROUP_SETS = {
    PRIMARY_GROUP: set(),
    AUXILIARY_GROUP: {"loadgenerator"},
    PLATFORM_GROUP: {"prometheus", "grafana", "jaeger", "zipkin", "metrics-server"},
    SYSTEM_GROUP: {"coredns", "traefik", "local-path-provisioner", "kube-proxy", "lb-tcp-80", "lb-tcp-443"},
    MEASUREMENT_GROUP: {"kepler", "cadvisor", "node-exporter"},
}

MODEL_M1 = "M1"
MODEL_M2 = "M2"
MODEL_M1_SOURCE = "kepler_container_cpu_joules_total"
MODEL_M2_SOURCE = "container_cpu_usage_seconds_total"


class AttributionError(RuntimeError):
    """Raised when the post-processing input is malformed or unusable."""


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as infile:
        return json.load(infile)


def load_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as infile:
        return list(csv.DictReader(infile))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as outfile:
        json.dump(payload, outfile, indent=2)


def write_csv(path: Path, rows: Sequence[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_mean(values: Iterable[Any]) -> Optional[float]:
    cleaned = [value for value in values if isinstance(value, (int, float))]
    if not cleaned:
        return None
    return sum(cleaned) / len(cleaned)


def safe_max(values: Iterable[Any]) -> Optional[float]:
    cleaned = [value for value in values if isinstance(value, (int, float))]
    if not cleaned:
        return None
    return max(cleaned)


def safe_min(values: Iterable[Any]) -> Optional[float]:
    cleaned = [value for value in values if isinstance(value, (int, float))]
    if not cleaned:
        return None
    return min(cleaned)


def parse_iso_timestamp(raw_value: Any) -> Optional[float]:
    if raw_value in (None, ""):
        return None
    if isinstance(raw_value, (int, float)):
        return float(raw_value)
    try:
        return datetime.fromisoformat(str(raw_value)).timestamp()
    except ValueError:
        return None


def normalize_entity_name(raw_name: Any) -> str:
    return str(raw_name or "").strip().lower()


def is_hash_like(value: str) -> bool:
    cleaned = value.replace("-", "").replace("_", "")
    return len(cleaned) >= 8 and bool(re.fullmatch(r"[0-9a-f]+", cleaned))


def extract_hex_candidates(text: str) -> List[str]:
    if not text:
        return []
    # find hex-like substrings of length >=8
    candidates = re.findall(r"[0-9a-fA-F]{8,}", str(text))
    return [c.lower() for c in candidates]


def reconcile_entities(
    energy_series: List[Dict[str, Any]],
    cpu_series: List[Dict[str, Any]],
    cpu_k8s_payload: Dict[str, Any],
    kube_pod_info: Dict[str, Any],
    kube_pod_container_info: Dict[str, Any],
    metadata: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Attempt to reconcile entity identities across sources.

    Adds reconciliation fields onto series rows: `canonical_entity_name`,
    `container_id`, `pod_name`, `deployment_name`, `reconciliation_provenance`,
    `reconciliation_confidence`.
    """
    # Build hex -> info mapping from kube payloads and cpu_k8s payload
    hex_map: Dict[str, Dict[str, Any]] = {}

    # From kube_pod_container_info (if present)
    try:
        containers = kube_pod_container_info.get("containers", []) if isinstance(kube_pod_container_info, dict) else []
        for c in containers:
            cid = c.get("container_id") or c.get("id")
            if cid:
                for h in extract_hex_candidates(str(cid)):
                    hex_map.setdefault(h, {}).update({
                        "container_name": normalize_entity_name(c.get("name") or c.get("container_name")),
                        "pod_name": normalize_entity_name(c.get("pod_name") or c.get("pod")),
                        "deployment": normalize_entity_name(c.get("deployment") or c.get("owner_reference")),
                        "provenance": "kube_pod_container_info",
                    })
    except Exception:
        pass

    # From cpu_k8s_by_id payload metrics
    try:
        for result in cpu_k8s_payload.get("data", {}).get("result", []) if isinstance(cpu_k8s_payload, dict) else []:
            metric = result.get("metric", {})
            # check common labels
            cid_labels = [metric.get(k) for k in ("container_id", "id", "container") if metric.get(k)]
            if not cid_labels:
                # sometimes id is encoded in the metric name or path
                for v in metric.values():
                    cid_labels.extend(extract_hex_candidates(str(v)))
            for cid in cid_labels:
                for h in extract_hex_candidates(str(cid)):
                    hex_map.setdefault(h, {}).update({
                        "container_name": normalize_entity_name(metric.get("container_name") or metric.get("container")),
                        "pod_name": normalize_entity_name(metric.get("pod") or metric.get("pod_name") or metric.get("kube_pod_name")),
                        "deployment": normalize_entity_name(metric.get("deployment") or metric.get("kube_deployment")),
                        "provenance": "cpu_k8s_by_id_metric",
                    })
    except Exception:
        pass

    # Heuristic mapping: scan human-labeled cpu_series for patterns containing hex
    for row in cpu_series:
        raw = row.get("raw_entity_name") or ""
        for h in extract_hex_candidates(str(raw)):
            hex_map.setdefault(h, {}).update({
                "container_name": normalize_entity_name(row.get("entity_name") or raw),
                "provenance": "cpu_by_container_heuristic",
            })

    # Also inspect metadata.sut_containers or deployment deployment->images
    try:
        sut = metadata.get("sut_containers", []) if isinstance(metadata, dict) else []
        for item in sut:
            name = normalize_entity_name(item)
            # try to detect hex suffixes in name
            for h in extract_hex_candidates(str(item)):
                hex_map.setdefault(h, {}).update({"container_name": name, "provenance": "metadata.sut_containers"})
    except Exception:
        pass

    # Build canonical name for each series row using heuristics
    def pick_canonical(row: Dict[str, Any]) -> Dict[str, Any]:
        raw = row.get("raw_entity_name") or row.get("entity_name") or ""
        candidates = extract_hex_candidates(str(raw))
        cid = candidates[0] if candidates else None
        info = {}
        provenance_parts: List[str] = []
        confidence = 0.0
        container_name = None
        pod_name = None
        deployment = None

        if cid and cid in hex_map:
            info = hex_map[cid]
            container_name = info.get("container_name")
            pod_name = info.get("pod_name")
            deployment = info.get("deployment")
            provenance_parts.append(info.get("provenance") or "hex_map")
            confidence = 0.95 if container_name else 0.6

        # If no hex mapping, prefer existing human name
        if not container_name:
            # if entity_name looks human (not hash) use it
            if not is_hash_like(str(row.get("entity_name") or "")):
                container_name = normalize_entity_name(row.get("entity_name"))
                provenance_parts.append("source_label")
                confidence = max(confidence, 0.9)

        # fallback: try to match by prefix to deployment names
        if not container_name:
            for d in (metadata.get("deployment", {}).get("deployments", []) if isinstance(metadata.get("deployment"), dict) else []):
                name = normalize_entity_name(d.get("name") if isinstance(d, dict) else d)
                if name and str(raw).lower().startswith(name + "-"):
                    container_name = name
                    provenance_parts.append("metadata_prefix_heuristic")
                    confidence = max(confidence, 0.75)
                    break

        # as last resort, preserve raw
        if not container_name:
            container_name = normalize_entity_name(raw)
            provenance_parts.append("raw")
            if is_hash_like(container_name):
                confidence = max(confidence, 0.2)
            else:
                confidence = max(confidence, 0.5)

        identity_type = "raw_label"
        resolution_status = "resolved"
        if cid and is_hash_like(container_name) and container_name == cid:
            identity_type = "short_container_id_candidate"
            resolution_status = "unresolved"
        elif cid:
            identity_type = "container_id"
            resolution_status = "reconciled"
        elif is_hash_like(container_name):
            identity_type = "short_container_id_candidate"
            resolution_status = "unresolved"

        row["canonical_entity_name"] = container_name
        row["container_id"] = cid
        row["pod_name"] = pod_name
        row["deployment_name"] = deployment
        row["reconciliation_provenance"] = ",".join([p for p in provenance_parts if p])
        row["reconciliation_confidence"] = float(confidence)
        row["identity_type"] = identity_type
        row["resolution_status"] = resolution_status
        # replace entity_name used by downstream classification
        row["entity_name"] = container_name
        return row

    energy_series = [pick_canonical(r) for r in energy_series]
    cpu_series = [pick_canonical(r) for r in cpu_series]
    return energy_series, cpu_series


def integrate_series(points: Sequence[Sequence[Any]]) -> Optional[float]:
    usable_points: List[Tuple[float, float]] = []
    for point in points:
        if not isinstance(point, list) or len(point) < 2:
            continue
        timestamp = safe_float(point[0])
        value = safe_float(point[1])
        if timestamp is None or value is None:
            continue
        usable_points.append((timestamp, value))

    if not usable_points:
        return None
    if len(usable_points) == 1:
        return usable_points[0][1]

    usable_points.sort(key=lambda item: item[0])
    area = 0.0
    for (left_ts, left_value), (right_ts, right_value) in zip(usable_points, usable_points[1:]):
        delta_seconds = right_ts - left_ts
        if delta_seconds <= 0:
            continue
        area += ((left_value + right_value) / 2.0) * delta_seconds
    return area


def summarize_series(points: Sequence[Sequence[Any]]) -> Dict[str, Optional[float]]:
    values = []
    for point in points:
        if not isinstance(point, list) or len(point) < 2:
            continue
        value = safe_float(point[1])
        if value is not None:
            values.append(value)
    return {
        "mean": safe_mean(values),
        "max": safe_max(values),
        "min": safe_min(values),
        "sample_count": float(len(values)) if values else 0.0,
    }


def load_attribution_config(app_name: str) -> Dict[str, Any]:
    for config_path in candidate_config_paths(app_name):
        if not config_path.exists():
            continue
        with config_path.open("r", encoding="utf-8") as infile:
            loaded = yaml.safe_load(infile) or {}
        if not isinstance(loaded, dict):
            continue
        attribution = loaded.get("attribution", {})
        return attribution if isinstance(attribution, dict) else {}
    return {}


def load_locust_workload(metadata: Dict[str, Any]) -> Dict[str, Any]:
    workload = metadata.get("workload_parameters", {}) if isinstance(metadata, dict) else {}
    timestamps = metadata.get("timestamps", {}) if isinstance(metadata, dict) else {}
    workload_label = metadata.get("workload_label") if isinstance(metadata, dict) else None
    workload_region = None
    if isinstance(metadata, dict):
        workload_region = metadata.get("workload_region")
    if not workload_region:
        workload_region = workload_label or "unknown"

    total_requests = None
    successful_requests = None
    throughput_mean_rps = None
    error_rate = None
    p95_latency = None

    return {
        "workload_level": workload_label or "unknown",
        "workload_region": workload_region,
        "users": workload.get("users"),
        "duration_seconds": parse_duration_seconds(timestamps),
        "total_requests": total_requests,
        "successful_requests": successful_requests,
        "throughput_mean_rps": throughput_mean_rps,
        "error_rate": error_rate,
        "p95_latency": p95_latency,
    }


def parse_duration_seconds(timestamps: Dict[str, Any]) -> Optional[float]:
    start = timestamps.get("workload_effective_start") or timestamps.get("workload_start")
    end = timestamps.get("workload_end")
    start_ts = parse_iso_timestamp(start)
    end_ts = parse_iso_timestamp(end)
    if start_ts is None or end_ts is None:
        return None
    return max(0.0, end_ts - start_ts)


def parse_locust_stats(run_dir: Path) -> Dict[str, Any]:
    rows = load_csv_rows(run_dir / "locust_stats.csv")
    aggregated = None
    for row in rows:
        name = str(row.get("Name", "")).strip().lower()
        row_type = str(row.get("Type", "")).strip().lower()
        if name == "aggregated" or row_type == "aggregated":
            aggregated = row
            break

    if not aggregated:
        return {}

    request_count = safe_float(aggregated.get("Request Count"))
    failure_count = safe_float(aggregated.get("Failure Count")) or 0.0
    throughput_mean_rps = safe_float(aggregated.get("Requests/s"))
    p95_latency = safe_float(aggregated.get("95%"))
    error_rate = None
    if request_count and request_count > 0 and failure_count is not None:
        error_rate = max(0.0, min(1.0, failure_count / request_count))

    return {
        "total_requests": request_count,
        "successful_requests": (request_count - failure_count) if request_count is not None else None,
        "throughput_mean_rps": throughput_mean_rps,
        "error_rate": error_rate,
        "p95_latency": p95_latency,
    }


def load_power_meter_context(run_dir: Path, metadata: Dict[str, Any]) -> Dict[str, Any]:
    samples = load_csv_rows(run_dir / "physical_power_meter.csv")
    if not samples:
        return {
            "available": False,
            "quality_flags": ["missing_power_meter_samples"],
        }

    numeric_apower = []
    numeric_energy = []
    errors = 0
    timestamps = []
    for row in samples:
        if str(row.get("error", "")).strip():
            errors += 1
            continue
        apower = safe_float(row.get("apower"))
        aenergy = safe_float(row.get("aenergy_total"))
        ts = safe_float(row.get("timestamp_unix"))
        if ts is None:
            ts = parse_iso_timestamp(row.get("timestamp_iso"))
        if apower is not None:
            numeric_apower.append(apower)
        if aenergy is not None:
            numeric_energy.append(aenergy)
        if ts is not None:
            timestamps.append(ts)

    quality_flags: List[str] = []
    if errors:
        quality_flags.append("power_meter_sample_errors")
    if len(numeric_apower) < 3:
        quality_flags.append("too_few_power_meter_samples")
    baseline_seconds = safe_float((metadata.get("power_meter") or {}).get("baseline_seconds"))
    if baseline_seconds is None:
        baseline_seconds = safe_float(metadata.get("baseline_seconds"))
    if baseline_seconds is None:
        quality_flags.append("missing_power_meter_baseline_seconds")

    return {
        "available": True,
        "sample_count": len(samples),
        "error_count": errors,
        "baseline_power_watts": safe_mean(numeric_apower),
        "apower_mean": safe_mean(numeric_apower),
        "apower_min": safe_min(numeric_apower),
        "apower_max": safe_max(numeric_apower),
        "aenergy_total_start": numeric_energy[0] if numeric_energy else None,
        "aenergy_total_end": numeric_energy[-1] if numeric_energy else None,
        "aenergy_total_delta": (numeric_energy[-1] - numeric_energy[0]) if len(numeric_energy) >= 2 else None,
        "quality_flags": quality_flags,
        "timestamps": timestamps,
    }


def build_service_catalog(metadata: Dict[str, Any], attribution_config: Dict[str, Any]) -> Dict[str, Any]:
    deployments = metadata.get("deployment", {}).get("deployments", []) if isinstance(metadata, dict) else []
    sut_containers = metadata.get("sut_containers", []) if isinstance(metadata, dict) else []
    deployment_names = {
        normalize_entity_name(item.get("name"))
        for item in deployments
        if isinstance(item, dict) and item.get("name")
    }
    primary_services = {
        normalize_entity_name(name) for name in sut_containers if normalize_entity_name(name)
    }
    if not primary_services:
        primary_services = deployment_names.copy()

    explicit_groups = attribution_config.get("service_groups", {}) if isinstance(attribution_config, dict) else {}
    group_index: Dict[str, str] = {}
    if isinstance(explicit_groups, dict):
        for group_name, members in explicit_groups.items():
            if not isinstance(members, list):
                continue
            for member in members:
                member_name = normalize_entity_name(member)
                if member_name:
                    group_index[member_name] = str(group_name)

    overrides = attribution_config.get("service_overrides", {}) if isinstance(attribution_config, dict) else {}
    ignore_patterns = list(DEFAULT_IGNORE_PATTERNS)
    ignore_patterns.extend(attribution_config.get("ignore_patterns", []) if isinstance(attribution_config, dict) else [])
    compiled_ignore_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in ignore_patterns if pattern]

    external_observability = {"prometheus", "grafana", "jaeger", "zipkin"}

    return {
        "deployment_names": deployment_names,
        "primary_services": primary_services,
        "group_index": group_index,
        "overrides": overrides,
        "ignore_patterns": compiled_ignore_patterns,
        "external_observability": external_observability,
        "scope": SUT_SCOPE,
    }


def infer_override_match(entity_name: str, overrides: Any) -> Optional[Dict[str, Any]]:
    if isinstance(overrides, dict):
        for key, value in overrides.items():
            key_name = normalize_entity_name(key)
            if key_name and key_name == entity_name:
                payload = value if isinstance(value, dict) else {"service_name": value}
                return {
                    "match_status": "override_exact",
                    "service_name": payload.get("service_name") or key_name,
                    "service_group": payload.get("service_group"),
                    "confidence": safe_float(payload.get("confidence")) or 1.0,
                    "provenance": f"attribution.service_overrides[{key}]",
                }
            if isinstance(value, dict):
                pattern = value.get("pattern")
                if pattern and re.search(str(pattern), entity_name, re.IGNORECASE):
                    return {
                        "match_status": "override_pattern",
                        "service_name": value.get("service_name") or key_name,
                        "service_group": value.get("service_group"),
                        "confidence": safe_float(value.get("confidence")) or 0.9,
                        "provenance": f"attribution.service_overrides[{key}]",
                    }
    elif isinstance(overrides, list):
        for index, value in enumerate(overrides):
            if not isinstance(value, dict):
                continue
            patterns = []
            if value.get("name"):
                patterns.append(re.escape(str(value["name"])) )
            if value.get("pattern"):
                patterns.append(str(value["pattern"]))
            for pattern in patterns:
                if re.search(pattern, entity_name, re.IGNORECASE):
                    return {
                        "match_status": "override_pattern",
                        "service_name": value.get("service_name") or entity_name,
                        "service_group": value.get("service_group"),
                        "confidence": safe_float(value.get("confidence")) or 0.9,
                        "provenance": f"attribution.service_overrides[{index}]",
                    }
    return None


def classify_entity(entity_name: Any, catalog: Dict[str, Any]) -> Dict[str, Any]:
    normalized = normalize_entity_name(entity_name)
    if not normalized:
        return {
            "entity_name": entity_name,
            "service_name": None,
            "service_group": UNKNOWN_GROUP,
            "match_status": "unknown",
            "confidence": 0.0,
            "provenance": "unmapped",
            "scope_category": UNKNOWN_GROUP,
            "in_scope": False,
            "ignored": False,
        }

    override = infer_override_match(normalized, catalog.get("overrides"))
    if override:
        service_group = override.get("service_group") or catalog.get("group_index", {}).get(normalized)
        if not service_group:
            service_group = PRIMARY_GROUP if normalized in catalog.get("primary_services", set()) else UNKNOWN_GROUP
        return {
            "entity_name": normalized,
            "service_name": normalize_entity_name(override.get("service_name") or normalized),
            "service_group": service_group,
            "match_status": override["match_status"],
            "confidence": override["confidence"],
            "provenance": override["provenance"],
            "scope_category": PRIMARY_GROUP if service_group in {PRIMARY_GROUP, AUXILIARY_GROUP} else service_group,
            "in_scope": service_group in {PRIMARY_GROUP, AUXILIARY_GROUP},
            "ignored": False,
        }

    for pattern in catalog.get("ignore_patterns", []):
        if pattern.search(normalized):
            group = catalog.get("group_index", {}).get(normalized)
            if not group:
                group = MEASUREMENT_GROUP if normalized in BUILTIN_GROUP_SETS[MEASUREMENT_GROUP] else UNKNOWN_GROUP
            return {
                "entity_name": normalized,
                "service_name": normalized,
                "service_group": group,
                "match_status": "ignored_pattern",
                "confidence": 0.6,
                "provenance": f"ignore_pattern:{pattern.pattern}",
                "scope_category": EXTERNAL_OBSERVABILITY_CATEGORY if normalized in catalog.get("external_observability", set()) else group,
                "in_scope": False,
                "ignored": True,
            }

    if normalized in catalog.get("group_index", {}):
        group = catalog["group_index"][normalized]
        return {
            "entity_name": normalized,
            "service_name": normalized,
            "service_group": group,
            "match_status": "configured_group",
            "confidence": 1.0,
            "provenance": f"attribution.service_groups.{group}",
            "scope_category": PRIMARY_GROUP if group in {PRIMARY_GROUP, AUXILIARY_GROUP} else group,
            "in_scope": group in {PRIMARY_GROUP, AUXILIARY_GROUP},
            "ignored": False,
        }

    if normalized in catalog.get("primary_services", set()) or normalized in catalog.get("deployment_names", set()):
        return {
            "entity_name": normalized,
            "service_name": normalized,
            "service_group": PRIMARY_GROUP,
            "match_status": "exact_primary",
            "confidence": 1.0,
            "provenance": "metadata.deployment.deployments|sut_containers",
            "scope_category": PRIMARY_GROUP,
            "in_scope": True,
            "ignored": False,
        }

    for candidate in catalog.get("primary_services", set()) | catalog.get("deployment_names", set()):
        if not candidate or candidate == normalized:
            continue
        if normalized.startswith(f"{candidate}-"):
            suffix = normalized[len(candidate) + 1 :]
            if suffix and (is_hash_like(suffix) or suffix in {"main", "server", "app"}):
                return {
                    "entity_name": normalized,
                    "service_name": candidate,
                    "service_group": PRIMARY_GROUP,
                    "match_status": "heuristic_primary",
                    "confidence": 0.75,
                    "provenance": f"heuristic_prefix_match:{candidate}",
                    "scope_category": PRIMARY_GROUP,
                    "in_scope": True,
                    "ignored": False,
                }

    if normalized in BUILTIN_GROUP_SETS[MEASUREMENT_GROUP]:
        return {
            "entity_name": normalized,
            "service_name": normalized,
            "service_group": MEASUREMENT_GROUP,
            "match_status": "builtin_group",
            "confidence": 0.95,
            "provenance": "builtin_measurement_service",
            "scope_category": MEASUREMENT_GROUP,
            "in_scope": False,
            "ignored": False,
        }
    if normalized in BUILTIN_GROUP_SETS[PLATFORM_GROUP]:
        return {
            "entity_name": normalized,
            "service_name": normalized,
            "service_group": PLATFORM_GROUP,
            "match_status": "builtin_group",
            "confidence": 0.95,
            "provenance": "builtin_platform_service",
            "scope_category": EXTERNAL_OBSERVABILITY_CATEGORY,
            "in_scope": False,
            "ignored": False,
        }
    if normalized in BUILTIN_GROUP_SETS[SYSTEM_GROUP]:
        return {
            "entity_name": normalized,
            "service_name": normalized,
            "service_group": SYSTEM_GROUP,
            "match_status": "builtin_group",
            "confidence": 0.95,
            "provenance": "builtin_system_service",
            "scope_category": SYSTEM_GROUP,
            "in_scope": False,
            "ignored": False,
        }
    if normalized in BUILTIN_GROUP_SETS[AUXILIARY_GROUP]:
        return {
            "entity_name": normalized,
            "service_name": normalized,
            "service_group": AUXILIARY_GROUP,
            "match_status": "builtin_group",
            "confidence": 0.9,
            "provenance": "builtin_auxiliary_service",
            "scope_category": PRIMARY_GROUP,
            "in_scope": True,
            "ignored": False,
        }

    return {
        "entity_name": normalized,
        "service_name": normalized,
        "service_group": UNKNOWN_GROUP,
        "match_status": "unknown",
        "confidence": 0.0,
        "provenance": "unmapped",
        "scope_category": UNKNOWN_GROUP,
        "in_scope": False,
        "ignored": False,
    }


def parse_prometheus_payload(payload: Dict[str, Any], label_name: str) -> List[Dict[str, Any]]:
    series = []
    for result in payload.get("data", {}).get("result", []):
        metric = result.get("metric", {})
        label_value = metric.get(label_name)
        if not label_value:
            continue
        points = result.get("values", [])
        series.append(
            {
                "entity_name": normalize_entity_name(label_value),
                "raw_entity_name": label_value,
                "metric": metric,
                "points": points,
                "stats": summarize_series(points),
                "integrated_value": integrate_series(points),
            }
        )
    return series


def build_container_rows(
    model_variant: str,
    series_rows: Sequence[Dict[str, Any]],
    catalog: Dict[str, Any],
    source_artifact: str,
    source_metric: str,
    total_reference_energy_joules: Optional[float],
    allocate_by_weight: bool = False,
    weight_field: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    classified_rows: List[Dict[str, Any]] = []
    total_weight = 0.0
    for row in series_rows:
        classification = classify_entity(row.get("entity_name"), catalog)
        weight_value = row.get(weight_field) if weight_field else row.get("integrated_value")
        weight = safe_float(weight_value)
        if weight is None:
            weight = 0.0
        if classification.get("in_scope"):
            total_weight += max(0.0, weight)
        classified_rows.append(
            {
                **row,
                **classification,
                "model_variant": model_variant,
                "source_artifact": source_artifact,
                "source_metric": source_metric,
                "weight_value": weight,
            }
        )

    output_rows: List[Dict[str, Any]] = []
    mapped_container_count = 0
    mapped_energy_joules = 0.0
    unmapped_container_count = 0
    unmapped_energy_joules = 0.0
    unknown_energy_joules = 0.0
    total_container_count = len(classified_rows)
    in_scope_rows = [row for row in classified_rows if row.get("in_scope")]
    out_of_scope_rows = [row for row in classified_rows if not row.get("in_scope")]

    if allocate_by_weight and total_reference_energy_joules is None:
        total_reference_energy_joules = 0.0

    for row in classified_rows:
        raw_metric_value = safe_float(row.get("integrated_value")) or 0.0
        raw_energy_joules = raw_metric_value if model_variant == MODEL_M1 else None
        allocated_energy_joules = raw_energy_joules if row.get("in_scope") else 0.0
        if allocate_by_weight and row.get("in_scope"):
            weight_value = max(0.0, safe_float(row.get("weight_value")) or 0.0)
            if total_weight > 0:
                allocated_energy_joules = (total_reference_energy_joules or 0.0) * (weight_value / total_weight)
            else:
                allocated_energy_joules = 0.0
        if allocated_energy_joules is None:
            allocated_energy_joules = 0.0

        mapped = row.get("in_scope") and row.get("service_group") != UNKNOWN_GROUP
        if mapped:
            mapped_container_count += 1
            mapped_energy_joules += allocated_energy_joules
        else:
            unmapped_container_count += 1
            unmapped_energy_joules += allocated_energy_joules
            unknown_energy_joules += allocated_energy_joules

        output_rows.append(
            {
                "model_variant": model_variant,
                "entity_name": row.get("entity_name"),
                "raw_entity_name": row.get("raw_entity_name"),
                "scope_category": row.get("scope_category"),
                "in_scope": row.get("in_scope"),
                "service_name": row.get("service_name"),
                "service_group": row.get("service_group"),
                "match_status": row.get("match_status"),
                "confidence": row.get("confidence"),
                "provenance": row.get("provenance"),
                "mapping_provenance": row.get("reconciliation_provenance"),
                "mapping_confidence": row.get("reconciliation_confidence"),
                "identity_type": row.get("identity_type"),
                "resolution_status": row.get("resolution_status"),
                "container_id": row.get("container_id"),
                "pod_name": row.get("pod_name"),
                "deployment_name": row.get("deployment_name"),
                "ignored": row.get("ignored"),
                "source_artifact": source_artifact,
                "source_metric": source_metric,
                "raw_energy_joules": raw_energy_joules,
                "raw_metric_value": raw_metric_value,
                "allocated_energy_joules": allocated_energy_joules,
                "weight_value": row.get("weight_value"),
                "series_mean": row.get("stats", {}).get("mean"),
                "series_max": row.get("stats", {}).get("max"),
                "series_min": row.get("stats", {}).get("min"),
                "sample_count": row.get("stats", {}).get("sample_count"),
                # inactive / zero-activity flags
                "inactive_entity": (row.get("stats", {}).get("sample_count") or 0) == 0,
                "zero_activity": ((row.get("stats", {}).get("mean") or 0.0) == 0.0 and (row.get("stats", {}).get("sample_count") or 0) > 0),
            }
        )

    total_attributed_energy_joules = sum(row["allocated_energy_joules"] for row in output_rows)
    scope_energy_joules = sum(safe_float(row.get("allocated_energy_joules")) or 0.0 for row in output_rows if row.get("in_scope"))
    out_of_scope_energy_joules = sum(safe_float(row.get("allocated_energy_joules")) or 0.0 for row in output_rows if not row.get("in_scope"))
    scope_mapped_container_count = sum(1 for row in output_rows if row.get("in_scope") and row.get("service_group") != UNKNOWN_GROUP)
    scope_unmapped_container_count = sum(1 for row in output_rows if row.get("in_scope") and row.get("service_group") == UNKNOWN_GROUP)
    scope_mapped_energy_joules = sum(
        safe_float(row.get("allocated_energy_joules")) or 0.0
        for row in output_rows
        if row.get("in_scope") and row.get("service_group") != UNKNOWN_GROUP
    )
    scope_unmapped_energy_joules = sum(
        safe_float(row.get("allocated_energy_joules")) or 0.0
        for row in output_rows
        if row.get("in_scope") and row.get("service_group") == UNKNOWN_GROUP
    )
    coverage = {
        "discovered_container_count": total_container_count,
        "discovered_energy_joules": total_attributed_energy_joules,
        "scope": SUT_SCOPE,
        "scope_container_count": len(in_scope_rows),
        "scope_energy_joules": scope_energy_joules,
        "out_of_scope_container_count": len(out_of_scope_rows),
        "out_of_scope_energy_joules": out_of_scope_energy_joules,
        "mapped_container_ratio": (scope_mapped_container_count / len(in_scope_rows)) if in_scope_rows else None,
        "mapped_energy_ratio": (scope_mapped_energy_joules / scope_energy_joules) if scope_energy_joules else None,
        "unmapped_container_count": scope_unmapped_container_count,
        "unmapped_energy_joules": scope_unmapped_energy_joules,
        "unknown_service_group_energy_joules": scope_unmapped_energy_joules,
        "total_container_count": len(in_scope_rows),
        "total_attributed_energy_joules": scope_energy_joules,
        "mapped_container_count": scope_mapped_container_count,
        "mapped_energy_joules": scope_mapped_energy_joules,
        "total_weight": total_weight,
    }
    return output_rows, coverage


def aggregate_service_rows(
    model_variant: str,
    container_rows: Sequence[Dict[str, Any]],
    workload_context: Dict[str, Any],
    total_reference_energy_joules: Optional[float],
) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in container_rows:
        grouped[(str(row.get("service_name")), str(row.get("service_group")))].append(row)

    service_rows: List[Dict[str, Any]] = []
    for (service_name, service_group), rows in sorted(grouped.items(), key=lambda item: (item[0][1], item[0][0])):
        allocated_energy = sum(safe_float(row.get("allocated_energy_joules")) or 0.0 for row in rows)
        raw_energy = sum(safe_float(row.get("raw_energy_joules")) or 0.0 for row in rows)
        container_count = len(rows)
        mapped_container_count = sum(1 for row in rows if row.get("service_group") != UNKNOWN_GROUP)
        service_rows.append(
            {
                "model_variant": model_variant,
                "service_name": service_name,
                "service_group": service_group,
                "container_count": container_count,
                "mapped_container_count": mapped_container_count,
                "raw_energy_joules": raw_energy,
                "allocated_energy_joules": allocated_energy,
                "mapped": service_group != UNKNOWN_GROUP,
                **workload_context,
                "joules_per_request": (allocated_energy / workload_context["total_requests"])
                if workload_context.get("total_requests")
                else None,
                "joules_per_successful_request": (allocated_energy / workload_context["successful_requests"])
                if workload_context.get("successful_requests")
                else None,
                "share_of_reference_energy": (allocated_energy / total_reference_energy_joules)
                if total_reference_energy_joules
                else None,
            }
        )
    return service_rows


def load_run_artifacts(run_dir: Path) -> Dict[str, Any]:
    metadata = load_json(run_dir / "metadata.json")
    if not isinstance(metadata, dict):
        raise AttributionError(f"Missing or invalid metadata.json in {run_dir}")

    energy_payload = load_json(run_dir / "energy.json", default={})
    if not isinstance(energy_payload, dict):
        energy_payload = {}

    cpu_by_container_payload = load_json(run_dir / "cpu_by_container.json", default={})
    if not isinstance(cpu_by_container_payload, dict):
        cpu_by_container_payload = {}

    cpu_k8s_by_id_payload = load_json(run_dir / "cpu_k8s_by_id.json", default={})
    if not isinstance(cpu_k8s_by_id_payload, dict):
        cpu_k8s_by_id_payload = {}

    kube_pod_info = load_json(run_dir / "kube_pod_info.json", default={})
    if not isinstance(kube_pod_info, dict):
        kube_pod_info = {}

    kube_pod_container_info = load_json(run_dir / "kube_pod_container_info.json", default={})
    if not isinstance(kube_pod_container_info, dict):
        kube_pod_container_info = {}

    query_info = load_json(run_dir / "query_info.json", default={})
    if not isinstance(query_info, dict):
        query_info = {}

    return {
        "metadata": metadata,
        "energy_payload": energy_payload,
        "cpu_by_container_payload": cpu_by_container_payload,
        "cpu_k8s_by_id_payload": cpu_k8s_by_id_payload,
        "kube_pod_info": kube_pod_info,
        "kube_pod_container_info": kube_pod_container_info,
        "query_info": query_info,
    }


def build_warning(code: str, message: str, severity: str = "warning", **context: Any) -> Dict[str, Any]:
    payload = {"code": code, "message": message, "severity": severity}
    if context:
        payload["context"] = context
    return payload


def build_model_artifact(
    run_dir: Path,
    model_variant: str,
    source_artifact: str,
    source_metric: str,
    series_rows: Sequence[Dict[str, Any]],
    catalog: Dict[str, Any],
    workload_context: Dict[str, Any],
    total_reference_energy_joules: Optional[float],
    allocate_by_weight: bool = False,
    weight_field: Optional[str] = None,
) -> Dict[str, Any]:
    container_rows, coverage = build_container_rows(
        model_variant=model_variant,
        series_rows=series_rows,
        catalog=catalog,
        source_artifact=source_artifact,
        source_metric=source_metric,
        total_reference_energy_joules=total_reference_energy_joules,
        allocate_by_weight=allocate_by_weight,
        weight_field=weight_field,
    )
    scoped_container_rows = [row for row in container_rows if row.get("in_scope")]
    service_rows = aggregate_service_rows(
        model_variant=model_variant,
        container_rows=scoped_container_rows,
        workload_context=workload_context,
        total_reference_energy_joules=total_reference_energy_joules,
    )

    warnings: List[Dict[str, Any]] = []
    if coverage["unmapped_container_count"]:
        warnings.append(
            build_warning(
                "unmapped_containers",
                f"{coverage['unmapped_container_count']} containers could not be mapped to a logical service",
                unmapped_container_count=coverage["unmapped_container_count"],
            )
        )
    if coverage["mapped_container_ratio"] is not None and coverage["mapped_container_ratio"] < 0.5:
        warnings.append(
            build_warning(
                "low_mapping_coverage",
                "Less than half of the attribution input containers were mapped",
                mapped_container_ratio=coverage["mapped_container_ratio"],
            )
        )
    if coverage.get("out_of_scope_container_count"):
        warnings.append(
            build_warning(
                "out_of_scope_entities_present",
                "Telemetry includes entities outside the selected SUT attribution scope",
                out_of_scope_container_count=coverage["out_of_scope_container_count"],
                out_of_scope_energy_joules=coverage["out_of_scope_energy_joules"],
            )
        )
    if total_reference_energy_joules is None:
        warnings.append(
            build_warning(
                "missing_reference_energy",
                "Could not determine a total reference energy quantity for attribution",
            )
        )

    return {
        "model_variant": model_variant,
        "source_artifact": source_artifact,
        "source_metric": source_metric,
        "container_rows": container_rows,
        "service_rows": service_rows,
        "coverage": coverage,
        "warnings": warnings,
    }


def summarize_unknown_energy(container_rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    scoped_unknown_rows = [row for row in container_rows if row.get("in_scope") and row.get("service_group") == UNKNOWN_GROUP]
    out_of_scope_rows = [row for row in container_rows if not row.get("in_scope")]
    unknown_energy = sum(safe_float(row.get("allocated_energy_joules")) or 0.0 for row in scoped_unknown_rows)
    scoped_energy = sum(safe_float(row.get("allocated_energy_joules")) or 0.0 for row in container_rows if row.get("in_scope"))
    out_of_scope_energy = sum(safe_float(row.get("allocated_energy_joules")) or 0.0 for row in out_of_scope_rows)
    high_energy_unknown_entities = [
        {
            "entity_name": row.get("entity_name"),
            "raw_entity_name": row.get("raw_entity_name"),
            "allocated_energy_joules": safe_float(row.get("allocated_energy_joules")) or 0.0,
            "mapping_provenance": row.get("mapping_provenance"),
            "mapping_confidence": row.get("mapping_confidence"),
            "resolution_status": row.get("resolution_status"),
        }
        for row in sorted(
            scoped_unknown_rows,
            key=lambda item: safe_float(item.get("allocated_energy_joules")) or 0.0,
            reverse=True,
        )
    ]
    out_of_scope_entities = [
        {
            "entity_name": row.get("entity_name"),
            "raw_entity_name": row.get("raw_entity_name"),
            "scope_category": row.get("scope_category"),
            "service_group": row.get("service_group"),
            "identity_type": row.get("identity_type"),
            "resolution_status": row.get("resolution_status"),
            "allocated_energy_joules": safe_float(row.get("allocated_energy_joules")) or 0.0,
        }
        for row in sorted(
            out_of_scope_rows,
            key=lambda item: safe_float(item.get("allocated_energy_joules")) or 0.0,
            reverse=True,
        )
    ]
    return {
        "m1_unknown_energy_joules": unknown_energy,
        "m1_unknown_energy_ratio": (unknown_energy / scoped_energy) if scoped_energy else None,
        "m1_high_energy_unknown_entities": high_energy_unknown_entities,
        "m1_unknown_container_count": len(scoped_unknown_rows),
        "m1_out_of_scope_energy_joules": out_of_scope_energy,
        "m1_out_of_scope_container_count": len(out_of_scope_rows),
        "m1_out_of_scope_entities": out_of_scope_entities,
    }


def summarize_m2_comparison(
    model_m1: Dict[str, Any],
    model_m2: Dict[str, Any],
    catalog: Dict[str, Any],
) -> Dict[str, Any]:
    m2_rows = model_m2.get("container_rows", []) if isinstance(model_m2, dict) else []
    m2_scope_rows = [row for row in m2_rows if row.get("in_scope")]
    m2_total = len(m2_scope_rows)
    m2_cpu_coverage_ratio = (len(m2_scope_rows) / len(catalog.get("primary_services", set()))) if catalog.get("primary_services") else None

    m1_scope_entities = {
        row.get("service_name")
        for row in model_m1.get("container_rows", [])
        if row.get("in_scope") and row.get("service_name")
    }
    m2_service_names = {row.get("service_name") for row in m2_scope_rows if row.get("service_name")}
    missing_entities = sorted(entity for entity in m1_scope_entities if entity and entity not in m2_service_names)

    # A strict comparison needs SUT runtime coverage; infra-only CPU series are not sufficient.
    valid_for_comparison = bool(m2_cpu_coverage_ratio and m2_cpu_coverage_ratio >= 0.5 and len(m2_scope_rows) >= len(catalog.get("primary_services", set())) * 0.5)

    return {
        "m2_cpu_coverage_ratio": m2_cpu_coverage_ratio,
        "m2_entity_count": m2_total,
        "m2_missing_energy_entities": missing_entities,
        "m2_valid_for_comparison": valid_for_comparison,
        "m2_runtime_entity_count": len(m2_scope_rows),
    }


def build_attribution_report(run_dir: Path, output_dir: Optional[Path] = None) -> Dict[str, Any]:
    artifacts = load_run_artifacts(run_dir)
    metadata = artifacts["metadata"]
    app_path = metadata.get("app_path", "")
    app_name = Path(str(app_path)).name if app_path else run_dir.parent.name
    attribution_config = load_attribution_config(app_name)
    catalog = build_service_catalog(metadata, attribution_config)

    timestamps = metadata.get("timestamps", {}) if isinstance(metadata, dict) else {}
    workload_context = load_locust_workload(metadata)
    locust_stats = parse_locust_stats(run_dir)
    workload_context.update(locust_stats)
    workload_context["duration_seconds"] = workload_context.get("duration_seconds") or parse_duration_seconds(timestamps)
    workload_context["successful_requests"] = workload_context.get("successful_requests")
    if workload_context.get("successful_requests") is None and workload_context.get("total_requests") is not None:
        failure_count = None
        if workload_context.get("total_requests") is not None and workload_context.get("error_rate") is not None:
            failure_count = workload_context["total_requests"] * workload_context["error_rate"]
        workload_context["successful_requests"] = (workload_context["total_requests"] - failure_count) if failure_count is not None else None

    power_meter_context = load_power_meter_context(run_dir, metadata)

    energy_payload = artifacts["energy_payload"]
    energy_series = parse_prometheus_payload(energy_payload, "container_name")
    if not energy_series:
        raise AttributionError(f"No container_name series found in {run_dir / 'energy.json'}")
    energy_total_reference = sum(safe_float(row.get("integrated_value")) or 0.0 for row in energy_series)

    cpu_series: List[Dict[str, Any]] = []
    cpu_source_artifact = None
    cpu_source_metric = None
    if artifacts["cpu_by_container_payload"]:
        cpu_series = parse_prometheus_payload(artifacts["cpu_by_container_payload"], "container_name")
        cpu_source_artifact = "cpu_by_container.json"
        cpu_source_metric = MODEL_M2_SOURCE
    elif artifacts["cpu_k8s_by_id_payload"]:
        cpu_series = parse_prometheus_payload(artifacts["cpu_k8s_by_id_payload"], "id")
        cpu_source_artifact = "cpu_k8s_by_id.json"
        cpu_source_metric = MODEL_M2_SOURCE

    # Reconcile identities across energy and CPU series using kube payloads when available
    energy_series, cpu_series = reconcile_entities(
        energy_series=energy_series,
        cpu_series=cpu_series,
        cpu_k8s_payload=artifacts.get("cpu_k8s_by_id_payload", {}),
        kube_pod_info=artifacts.get("kube_pod_info", {}),
        kube_pod_container_info=artifacts.get("kube_pod_container_info", {}),
        metadata=metadata,
    )

    model_m1 = build_model_artifact(
        run_dir=run_dir,
        model_variant=MODEL_M1,
        source_artifact="energy.json",
        source_metric=MODEL_M1_SOURCE,
        series_rows=energy_series,
        catalog=catalog,
        workload_context=workload_context,
        total_reference_energy_joules=energy_total_reference,
        allocate_by_weight=False,
    )

    model_m2 = None
    if cpu_series:
        model_m2 = build_model_artifact(
            run_dir=run_dir,
            model_variant=MODEL_M2,
            source_artifact=cpu_source_artifact or "cpu_k8s_by_id.json",
            source_metric=cpu_source_metric or MODEL_M2_SOURCE,
            series_rows=cpu_series,
            catalog=catalog,
            workload_context=workload_context,
            total_reference_energy_joules=energy_total_reference,
            allocate_by_weight=True,
            weight_field="integrated_value",
        )
    else:
        model_m2 = {
            "model_variant": MODEL_M2,
            "source_artifact": None,
            "source_metric": MODEL_M2_SOURCE,
            "container_rows": [],
            "service_rows": [],
            "coverage": {
                "mapped_container_ratio": None,
                "mapped_energy_ratio": None,
                "unmapped_container_count": 0,
                "unmapped_energy_joules": 0.0,
                "unknown_service_group_energy_joules": 0.0,
                "total_container_count": 0,
                "total_attributed_energy_joules": 0.0,
                "mapped_container_count": 0,
                "mapped_energy_joules": 0.0,
                "total_weight": 0.0,
            },
            "warnings": [
                build_warning(
                    "missing_cpu_proxy",
                    "No CPU proxy telemetry was available for M2 attribution",
                )
            ],
        }

    warnings: List[Dict[str, Any]] = []
    warnings.extend(model_m1["warnings"])
    warnings.extend(model_m2["warnings"])

    m1_unknown_summary = summarize_unknown_energy(model_m1["container_rows"])
    m2_comparison_summary = summarize_m2_comparison(model_m1, model_m2, catalog)

    if not m2_comparison_summary["m2_valid_for_comparison"]:
        warnings.append(
            build_warning(
                "m2_incomplete_comparison",
                "M2 CPU telemetry does not cover the same logical runtime population as M1",
                m2_cpu_coverage_ratio=m2_comparison_summary["m2_cpu_coverage_ratio"],
                m2_entity_count=m2_comparison_summary["m2_entity_count"],
                m2_missing_energy_entities=m2_comparison_summary["m2_missing_energy_entities"],
            )
        )

    meter_flags = power_meter_context.get("quality_flags", [])
    if meter_flags:
        warnings.append(
            build_warning(
                "power_meter_quality",
                "Physical power meter data carries quality warnings and should be used only as contextual validation",
                flags=meter_flags,
            )
        )

    if metadata.get("experiment_status") and metadata.get("experiment_status") != "success":
        warnings.append(
            build_warning(
                "experiment_status_non_success",
                f"Experiment status is {metadata.get('experiment_status')}",
                severity="info",
            )
        )

    attribution_dir = output_dir or (run_dir / DEFAULT_OUTPUT_SUBDIR)
    attribution_dir.mkdir(parents=True, exist_ok=True)

    container_csv_rows = model_m1["container_rows"] + model_m2["container_rows"]
    service_csv_rows = model_m1["service_rows"] + model_m2["service_rows"]

    container_fieldnames = [
        "model_variant",
        "entity_name",
        "raw_entity_name",
        "container_id",
        "pod_name",
        "deployment_name",
        "service_name",
        "service_group",
        "match_status",
        "confidence",
        "provenance",
        "mapping_provenance",
        "mapping_confidence",
        "identity_type",
        "resolution_status",
        "ignored",
        "source_artifact",
        "source_metric",
        "raw_energy_joules",
        "raw_metric_value",
        "allocated_energy_joules",
        "weight_value",
        "series_mean",
        "series_max",
        "series_min",
        "sample_count",
        "inactive_entity",
        "zero_activity",
    ]
    service_fieldnames = [
        "model_variant",
        "service_name",
        "service_group",
        "container_count",
        "mapped_container_count",
        "raw_energy_joules",
        "allocated_energy_joules",
        "mapped",
        "workload_level",
        "workload_region",
        "users",
        "duration_seconds",
        "total_requests",
        "successful_requests",
        "throughput_mean_rps",
        "error_rate",
        "p95_latency",
        "joules_per_request",
        "joules_per_successful_request",
        "share_of_reference_energy",
    ]

    write_csv(attribution_dir / "container_attribution.csv", container_csv_rows, container_fieldnames)
    write_csv(attribution_dir / "service_attribution.csv", service_csv_rows, service_fieldnames)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_dir": str(run_dir),
        "output_dir": str(attribution_dir),
        "primary_model_variant": MODEL_M1,
        "selected_attribution_scope": SUT_SCOPE,
        "available_model_variants": [MODEL_M1, MODEL_M2],
        "input_artifacts": {
            "metadata.json": "metadata.json",
            "energy.json": "energy.json",
            "cpu_by_container.json": "cpu_by_container.json" if (run_dir / "cpu_by_container.json").exists() else None,
            "cpu_k8s_by_id.json": "cpu_k8s_by_id.json" if (run_dir / "cpu_k8s_by_id.json").exists() else None,
            "query_info.json": "query_info.json" if (run_dir / "query_info.json").exists() else None,
            "locust_stats.csv": "locust_stats.csv" if (run_dir / "locust_stats.csv").exists() else None,
            "physical_power_meter.csv": "physical_power_meter.csv" if (run_dir / "physical_power_meter.csv").exists() else None,
        },
        "provenance": {
            "app_name": app_name,
            "deployment_names": sorted(catalog["deployment_names"]),
            "primary_services": sorted(catalog["primary_services"]),
            "service_groups": {
                group_name: sorted(members)
                for group_name, members in BUILTIN_GROUP_SETS.items()
            },
            "mapping_strategy": "metadata_deployment_names_plus_overrides",
            "reconciliation_notes": [
                "Kepler energy labels for unresolved entities are short hex container-like IDs present in energy.json and summary.json.",
                "Those IDs do not appear in cpu_by_container.json, cpu_k8s_by_id.json, or any kube pod/container artifact in this run.",
                "They are therefore retained as unresolved short_container_id_candidate identities with explicit provenance and confidence.",
            ],
        },
        "workload": workload_context,
        "physical_power_meter": power_meter_context,
        "models": {
            MODEL_M1: {
                **model_m1,
                "reference_energy_source": MODEL_M1_SOURCE,
                "allocation_basis": "direct_measured",
                "normalization_reference": energy_total_reference,
                "model_semantics": "direct_energy_joules represent measured container energy from Kepler",
                "attribution_scope": SUT_SCOPE,
            },
            MODEL_M2: {
                **(model_m2 if isinstance(model_m2, dict) else {}),
                "reference_energy_source": MODEL_M1_SOURCE,
                "allocation_basis": "cpu_proxy_weighted",
                "normalization_reference": energy_total_reference,
                "model_semantics": "proxy_allocated_energy_joules allocate total M1 energy by normalized CPU weights",
                "attribution_scope": SUT_SCOPE,
            },
        },
        "coverage": model_m1["coverage"],
        "m1_unknown_energy_joules": m1_unknown_summary["m1_unknown_energy_joules"],
        "m1_unknown_energy_ratio": m1_unknown_summary["m1_unknown_energy_ratio"],
        "m1_unknown_container_count": m1_unknown_summary["m1_unknown_container_count"],
        "m1_high_energy_unknown_entities": m1_unknown_summary["m1_high_energy_unknown_entities"],
        "m1_out_of_scope_energy_joules": m1_unknown_summary["m1_out_of_scope_energy_joules"],
        "m1_out_of_scope_container_count": m1_unknown_summary["m1_out_of_scope_container_count"],
        "m1_out_of_scope_entities": m1_unknown_summary["m1_out_of_scope_entities"],
        "m2_cpu_coverage_ratio": m2_comparison_summary["m2_cpu_coverage_ratio"],
        "m2_entity_count": m2_comparison_summary["m2_entity_count"],
        "m2_missing_energy_entities": m2_comparison_summary["m2_missing_energy_entities"],
        "m2_valid_for_comparison": m2_comparison_summary["m2_valid_for_comparison"],
        "totals": {
            "total_direct_energy_joules": model_m1.get("coverage", {}).get("scope_energy_joules"),
            "total_mapped_energy_joules": model_m1.get("coverage", {}).get("mapped_energy_joules"),
            "total_unmapped_energy_joules": model_m1.get("coverage", {}).get("unmapped_energy_joules"),
            "mapped_energy_ratio": None if not model_m1.get("coverage", {}).get("scope_energy_joules") else (model_m1.get("coverage", {}).get("mapped_energy_joules") or 0.0) / max(model_m1.get("coverage", {}).get("scope_energy_joules") or 0.0, 1e-12),
            "unmapped_energy_ratio": None if not model_m1.get("coverage", {}).get("scope_energy_joules") else (model_m1.get("coverage", {}).get("unmapped_energy_joules") or 0.0) / max(model_m1.get("coverage", {}).get("scope_energy_joules") or 0.0, 1e-12),
            "total_container_count": model_m1.get("coverage", {}).get("scope_container_count"),
            "mapped_container_count": model_m1.get("coverage", {}).get("mapped_container_count"),
            "unmapped_container_count": model_m1.get("coverage", {}).get("unmapped_container_count"),
            "discovered_container_count": model_m1.get("coverage", {}).get("discovered_container_count"),
            "discovered_energy_joules": model_m1.get("coverage", {}).get("discovered_energy_joules"),
            "out_of_scope_container_count": model_m1.get("coverage", {}).get("out_of_scope_container_count"),
            "out_of_scope_energy_joules": model_m1.get("coverage", {}).get("out_of_scope_energy_joules"),
            "m2_cpu_coverage_ratio": m2_comparison_summary["m2_cpu_coverage_ratio"],
            "m2_entity_count": m2_comparison_summary["m2_entity_count"],
            "m2_missing_energy_entities": m2_comparison_summary["m2_missing_energy_entities"],
            "m2_valid_for_comparison": m2_comparison_summary["m2_valid_for_comparison"],
        },
        "warnings": warnings,
    }

    write_json(attribution_dir / "attribution.json", report)
    return report


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Post-process a completed run into attribution artifacts",
    )
    parser.add_argument("--run-dir", required=True, help="Path to a completed run directory")
    parser.add_argument(
        "--output-dir",
        help="Optional output directory. Defaults to <run-dir>/attribution",
    )
    return parser


def main() -> int:
    args = build_argument_parser().parse_args()
    run_dir = Path(args.run_dir).resolve()
    output_dir = Path(args.output_dir).resolve() if args.output_dir else None

    try:
        build_attribution_report(run_dir, output_dir=output_dir)
    except AttributionError as exc:
        print(f"attribution failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
