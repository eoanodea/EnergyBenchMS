#!/usr/bin/env python3
"""Extract Prometheus metrics for a completed experiment run."""

import argparse
import json
from datetime import datetime
from pathlib import Path

import requests


DEFAULT_STEP = "5s"
RANGE_WINDOW = "1m"


def build_selector(*parts):
    return ", ".join(part for part in parts if part)


def build_not_empty_selector(labels):
    return build_selector(*(f'{label}!=""' for label in labels))


def build_sum_by_query(metric, labels, selector=None):
    label_clause = ", ".join(labels)
    selector_clause = f"{{{selector}}}" if selector else ""
    return f"sum by ({label_clause}) ({metric}{selector_clause})"


def build_rate_sum_by_query(metric, labels, selector=None, window=RANGE_WINDOW):
    label_clause = ", ".join(labels)
    selector_clause = f"{{{selector}}}" if selector else ""
    return f"sum by ({label_clause}) (rate({metric}{selector_clause}[{window}]))"


def make_candidate(name, promql, required_labels=None, postprocess=None):
    return {
        "name": name,
        "promql": promql,
        "required_labels": list(required_labels or []),
        "postprocess": postprocess,
    }


ENERGY_QUERY_JOULES = build_rate_sum_by_query(
    "kepler_container_cpu_joules_total",
    ["container_name"],
    selector='container_name!=""',
)

ENERGY_QUERY_BPF_CPU_TIME = build_rate_sum_by_query(
    "kepler_container_bpf_cpu_time_ms_total",
    ["container_name"],
    selector='container_name!=""',
)

ENERGY_QUERY_BPF_BLOCK_IRQ = build_rate_sum_by_query(
    "kepler_container_bpf_block_irq_total",
    ["container_name"],
    selector='container_name!=""',
)

CPU_TOTAL_QUERY = f"sum(rate(container_cpu_usage_seconds_total[{RANGE_WINDOW}]))"

CPU_BY_CONTAINER_QUERY = build_rate_sum_by_query(
    "container_cpu_usage_seconds_total",
    ["container_name"],
    selector='container_name!=""',
)

CPU_BY_NAME_QUERY = build_rate_sum_by_query(
    "container_cpu_usage_seconds_total",
    ["name"],
    selector='name!=""',
)

CPU_K8S_BY_ID_QUERY = build_rate_sum_by_query(
    "container_cpu_usage_seconds_total",
    ["id"],
    selector='id=~".*cri-containerd-.*scope"',
)


MEMORY_WORKING_SET_QUERY_K8S = build_sum_by_query(
    "container_memory_working_set_bytes",
    ["namespace", "pod", "container"],
    selector=build_not_empty_selector(["namespace", "pod", "container"]),
)

MEMORY_WORKING_SET_QUERY_CONTAINER_NAME = build_sum_by_query(
    "container_memory_working_set_bytes",
    ["namespace", "pod", "container_name"],
    selector=build_not_empty_selector(["namespace", "pod", "container_name"]),
)

MEMORY_WORKING_SET_QUERY_CONTAINER_NAME_ONLY = build_sum_by_query(
    "container_memory_working_set_bytes",
    ["container_name"],
    selector='container_name!=""',
)

MEMORY_WORKING_SET_QUERY_NAME = build_sum_by_query(
    "container_memory_working_set_bytes",
    ["name"],
    selector='name!=""',
)

MEMORY_WORKING_SET_QUERY_ID = build_sum_by_query(
    "container_memory_working_set_bytes",
    ["id"],
    selector='id!=""',
)

NETWORK_RX_QUERY_K8S = build_rate_sum_by_query(
    "container_network_receive_bytes_total",
    ["namespace", "pod", "container"],
    selector=build_not_empty_selector(["namespace", "pod", "container"]),
)

NETWORK_RX_QUERY_CONTAINER_NAME = build_rate_sum_by_query(
    "container_network_receive_bytes_total",
    ["namespace", "pod", "container_name"],
    selector=build_not_empty_selector(["namespace", "pod", "container_name"]),
)

NETWORK_RX_QUERY_CONTAINER_NAME_ONLY = build_rate_sum_by_query(
    "container_network_receive_bytes_total",
    ["container_name"],
    selector='container_name!=""',
)

NETWORK_RX_QUERY_NAME = build_rate_sum_by_query(
    "container_network_receive_bytes_total",
    ["name"],
    selector='name!=""',
)

NETWORK_RX_QUERY_ID = build_rate_sum_by_query(
    "container_network_receive_bytes_total",
    ["id"],
    selector='id!=""',
)

NETWORK_TX_QUERY_K8S = build_rate_sum_by_query(
    "container_network_transmit_bytes_total",
    ["namespace", "pod", "container"],
    selector=build_not_empty_selector(["namespace", "pod", "container"]),
)

NETWORK_TX_QUERY_CONTAINER_NAME = build_rate_sum_by_query(
    "container_network_transmit_bytes_total",
    ["namespace", "pod", "container_name"],
    selector=build_not_empty_selector(["namespace", "pod", "container_name"]),
)

NETWORK_TX_QUERY_CONTAINER_NAME_ONLY = build_rate_sum_by_query(
    "container_network_transmit_bytes_total",
    ["container_name"],
    selector='container_name!=""',
)

NETWORK_TX_QUERY_NAME = build_rate_sum_by_query(
    "container_network_transmit_bytes_total",
    ["name"],
    selector='name!=""',
)

NETWORK_TX_QUERY_ID = build_rate_sum_by_query(
    "container_network_transmit_bytes_total",
    ["id"],
    selector='id!=""',
)

FS_READS_QUERY_K8S = build_rate_sum_by_query(
    "container_fs_reads_bytes_total",
    ["namespace", "pod", "container"],
    selector=build_not_empty_selector(["namespace", "pod", "container"]),
)

FS_READS_QUERY_CONTAINER_NAME = build_rate_sum_by_query(
    "container_fs_reads_bytes_total",
    ["namespace", "pod", "container_name"],
    selector=build_not_empty_selector(["namespace", "pod", "container_name"]),
)

FS_READS_QUERY_CONTAINER_NAME_ONLY = build_rate_sum_by_query(
    "container_fs_reads_bytes_total",
    ["container_name"],
    selector='container_name!=""',
)

FS_READS_QUERY_NAME = build_rate_sum_by_query(
    "container_fs_reads_bytes_total",
    ["name"],
    selector='name!=""',
)

FS_READS_QUERY_ID = build_rate_sum_by_query(
    "container_fs_reads_bytes_total",
    ["id"],
    selector='id!=""',
)

FS_WRITES_QUERY_K8S = build_rate_sum_by_query(
    "container_fs_writes_bytes_total",
    ["namespace", "pod", "container"],
    selector=build_not_empty_selector(["namespace", "pod", "container"]),
)

FS_WRITES_QUERY_CONTAINER_NAME = build_rate_sum_by_query(
    "container_fs_writes_bytes_total",
    ["namespace", "pod", "container_name"],
    selector=build_not_empty_selector(["namespace", "pod", "container_name"]),
)

FS_WRITES_QUERY_CONTAINER_NAME_ONLY = build_rate_sum_by_query(
    "container_fs_writes_bytes_total",
    ["container_name"],
    selector='container_name!=""',
)

FS_WRITES_QUERY_NAME = build_rate_sum_by_query(
    "container_fs_writes_bytes_total",
    ["name"],
    selector='name!=""',
)

FS_WRITES_QUERY_ID = build_rate_sum_by_query(
    "container_fs_writes_bytes_total",
    ["id"],
    selector='id!=""',
)

QUERY_REGISTRY = {
    "cpu_total": {
        "output_filename": "cpu_total.json",
        "required_for": "summary",
        "candidates": [
            make_candidate("cpu_total", CPU_TOTAL_QUERY),
        ],
    },
    "cpu_by_container": {
        "output_filename": "cpu_by_container.json",
        "aliases": ["cpu.json"],
        "required_for": "attribution_v1",
        "candidates": [
            make_candidate(
                "container_name",
                CPU_BY_CONTAINER_QUERY,
                required_labels=["container_name"],
            ),
            make_candidate(
                "name",
                CPU_BY_NAME_QUERY,
                required_labels=["name"],
                postprocess=lambda payload: normalize_name_label_to_container_name(
                    payload
                ),
            ),
        ],
    },
    "cpu_by_name": {
        "output_filename": "cpu_by_name.json",
        "required_for": "diagnostic",
        "candidates": [
            make_candidate("name", CPU_BY_NAME_QUERY, required_labels=["name"]),
        ],
    },
    "cpu_k8s_by_id": {
        "output_filename": "cpu_k8s_by_id.json",
        "required_for": "attribution_v1",
        "candidates": [
            make_candidate("id", CPU_K8S_BY_ID_QUERY, required_labels=["id"]),
        ],
    },
    "memory_working_set": {
        "output_filename": "memory_working_set.json",
        "required_for": "attribution_v2",
        "candidates": [
            make_candidate(
                "namespace_pod_container",
                MEMORY_WORKING_SET_QUERY_K8S,
                required_labels=["namespace", "pod", "container"],
            ),
            make_candidate(
                "namespace_pod_container_name",
                MEMORY_WORKING_SET_QUERY_CONTAINER_NAME,
                required_labels=["namespace", "pod", "container_name"],
            ),
            make_candidate(
                "container_name",
                MEMORY_WORKING_SET_QUERY_CONTAINER_NAME_ONLY,
                required_labels=["container_name"],
            ),
            make_candidate(
                "name",
                MEMORY_WORKING_SET_QUERY_NAME,
                required_labels=["name"],
            ),
            make_candidate(
                "id",
                MEMORY_WORKING_SET_QUERY_ID,
                required_labels=["id"],
            ),
        ],
    },
    "network_rx": {
        "output_filename": "network_rx.json",
        "required_for": "attribution_v2",
        "candidates": [
            make_candidate(
                "namespace_pod_container",
                NETWORK_RX_QUERY_K8S,
                required_labels=["namespace", "pod", "container"],
            ),
            make_candidate(
                "namespace_pod_container_name",
                NETWORK_RX_QUERY_CONTAINER_NAME,
                required_labels=["namespace", "pod", "container_name"],
            ),
            make_candidate(
                "container_name",
                NETWORK_RX_QUERY_CONTAINER_NAME_ONLY,
                required_labels=["container_name"],
            ),
            make_candidate("name", NETWORK_RX_QUERY_NAME, required_labels=["name"]),
            make_candidate("id", NETWORK_RX_QUERY_ID, required_labels=["id"]),
        ],
    },
    "network_tx": {
        "output_filename": "network_tx.json",
        "required_for": "attribution_v2",
        "candidates": [
            make_candidate(
                "namespace_pod_container",
                NETWORK_TX_QUERY_K8S,
                required_labels=["namespace", "pod", "container"],
            ),
            make_candidate(
                "namespace_pod_container_name",
                NETWORK_TX_QUERY_CONTAINER_NAME,
                required_labels=["namespace", "pod", "container_name"],
            ),
            make_candidate(
                "container_name",
                NETWORK_TX_QUERY_CONTAINER_NAME_ONLY,
                required_labels=["container_name"],
            ),
            make_candidate("name", NETWORK_TX_QUERY_NAME, required_labels=["name"]),
            make_candidate("id", NETWORK_TX_QUERY_ID, required_labels=["id"]),
        ],
    },
    "fs_reads": {
        "output_filename": "fs_reads.json",
        "required_for": "attribution_v2",
        "candidates": [
            make_candidate(
                "namespace_pod_container",
                FS_READS_QUERY_K8S,
                required_labels=["namespace", "pod", "container"],
            ),
            make_candidate(
                "namespace_pod_container_name",
                FS_READS_QUERY_CONTAINER_NAME,
                required_labels=["namespace", "pod", "container_name"],
            ),
            make_candidate(
                "container_name",
                FS_READS_QUERY_CONTAINER_NAME_ONLY,
                required_labels=["container_name"],
            ),
            make_candidate("name", FS_READS_QUERY_NAME, required_labels=["name"]),
            make_candidate("id", FS_READS_QUERY_ID, required_labels=["id"]),
        ],
    },
    "fs_writes": {
        "output_filename": "fs_writes.json",
        "required_for": "attribution_v2",
        "candidates": [
            make_candidate(
                "namespace_pod_container",
                FS_WRITES_QUERY_K8S,
                required_labels=["namespace", "pod", "container"],
            ),
            make_candidate(
                "namespace_pod_container_name",
                FS_WRITES_QUERY_CONTAINER_NAME,
                required_labels=["namespace", "pod", "container_name"],
            ),
            make_candidate(
                "container_name",
                FS_WRITES_QUERY_CONTAINER_NAME_ONLY,
                required_labels=["container_name"],
            ),
            make_candidate("name", FS_WRITES_QUERY_NAME, required_labels=["name"]),
            make_candidate("id", FS_WRITES_QUERY_ID, required_labels=["id"]),
        ],
    },
    "kube_pod_info": {
        "output_filename": "kube_pod_info.json",
        "required_for": "diagnostic",
        "candidates": [
            make_candidate("kube_pod_info", "kube_pod_info"),
        ],
    },
    "kube_pod_container_info": {
        "output_filename": "kube_pod_container_info.json",
        "required_for": "diagnostic",
        "candidates": [
            make_candidate("kube_pod_container_info", "kube_pod_container_info"),
        ],
    },
}


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


def empty_prometheus_payload(error_message=None):
    payload = {"status": "success", "data": {"resultType": "matrix", "result": []}}
    if error_message:
        payload["status"] = "error"
        payload["error"] = error_message
    return payload


def query_prometheus(prom_url, query, start, end, step=DEFAULT_STEP):
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


def query_prometheus_safe(prom_url, query, start, end, step=DEFAULT_STEP):
    """Run a Prometheus query and return a payload plus any error message."""
    try:
        return query_prometheus(prom_url, query, start, end, step=step), None
    except (requests.RequestException, ValueError) as exc:
        message = str(exc)
        return empty_prometheus_payload(message), message


def save_results(run_dir, filename, payload):
    """Save a Prometheus response into the run directory."""
    output_path = Path(run_dir) / filename
    with output_path.open("w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, indent=2)


def result_series_count(payload):
    """Return the number of result series in a Prometheus payload."""
    return len(payload.get("data", {}).get("result", []))


def has_series(payload):
    """Return True when Prometheus payload includes at least one result series."""
    return result_series_count(payload) > 0


def has_nonzero_values(payload):
    """Return True when at least one sample value is non-zero."""
    results = payload.get("data", {}).get("result", [])
    for result in results:
        for point in result.get("values", []):
            if not isinstance(point, list) or len(point) < 2:
                continue
            try:
                if float(point[1]) != 0.0:
                    return True
            except (TypeError, ValueError):
                continue
    return False


def payload_has_required_labels(payload, required_labels):
    """Return True when at least one series has all required labels."""
    if not required_labels:
        return True

    required = set(required_labels)
    for result in payload.get("data", {}).get("result", []):
        metric = result.get("metric", {})
        if required.issubset(metric.keys()):
            return True
    return False


def normalize_name_label_to_container_name(payload):
    """Normalize fallback series labels from name -> container_name."""
    for result in payload.get("data", {}).get("result", []):
        metric = result.get("metric", {})
        if "container_name" not in metric and "name" in metric:
            metric["container_name"] = metric["name"]
    return payload


def run_query_candidates(prom_url, candidates, start, end, step=DEFAULT_STEP):
    """Run a best-effort query with label-aware fallbacks."""
    first_series_payload = None
    first_series_candidate = None
    first_candidate_result = None
    last_success_payload = None
    last_success_candidate = None
    last_payload = empty_prometheus_payload()
    last_candidate = None
    last_error = None

    for candidate in candidates:
        payload, error = query_prometheus_safe(
            prom_url,
            candidate["promql"],
            start,
            end,
            step=step,
        )
        last_payload = payload
        last_candidate = candidate

        if error:
            last_error = error
            continue

        last_success_payload = payload
        last_success_candidate = candidate

        if not has_series(payload):
            continue

        if first_series_payload is None:
            first_series_payload = payload
            first_series_candidate = candidate

        if payload_has_required_labels(payload, candidate.get("required_labels", [])):
            selected_payload = payload
            postprocess = candidate.get("postprocess")
            if postprocess is not None:
                selected_payload = postprocess(selected_payload)
            return selected_payload, candidate, None

        if first_candidate_result is None:
            first_candidate_result = (payload, candidate)

    if first_candidate_result is not None:
        selected_payload, selected_candidate = first_candidate_result
        postprocess = selected_candidate.get("postprocess")
        if postprocess is not None:
            selected_payload = postprocess(selected_payload)
        return selected_payload, selected_candidate, None

    if first_series_payload is not None:
        selected_payload = first_series_payload
        postprocess = first_series_candidate.get("postprocess")
        if postprocess is not None:
            selected_payload = postprocess(selected_payload)
        return selected_payload, first_series_candidate, None

    if last_success_payload is not None:
        selected_payload = last_success_payload
        postprocess = last_success_candidate.get("postprocess")
        if postprocess is not None:
            selected_payload = postprocess(selected_payload)
        return selected_payload, last_success_candidate, None

    if last_error is not None:
        return last_payload, last_candidate, last_error

    return last_payload, last_candidate, None


def query_energy_with_source(prom_url, start, end, source, step=DEFAULT_STEP):
    """Query energy-like series with source selection and auto fallback."""
    query_by_source = {
        "joules": ENERGY_QUERY_JOULES,
        "bpf_cpu_time": ENERGY_QUERY_BPF_CPU_TIME,
        "bpf_block_irq": ENERGY_QUERY_BPF_BLOCK_IRQ,
    }

    if source in query_by_source:
        query = query_by_source[source]
        payload, error = query_prometheus_safe(prom_url, query, start, end, step=step)
        return payload, source, query, error

    # auto mode: prefer joules for real energy, then fallback to BPF-based proxies.
    ordered_sources = ["joules", "bpf_cpu_time", "bpf_block_irq"]
    last_payload = empty_prometheus_payload()
    last_success_payload = None
    last_success_source = None
    last_success_query = None
    last_error = None
    last_selected_source = "auto_no_nonzero"
    last_selected_query = query_by_source[ordered_sources[-1]]

    for candidate in ordered_sources:
        query = query_by_source[candidate]
        payload, error = query_prometheus_safe(prom_url, query, start, end, step=step)
        last_payload = payload
        last_selected_source = candidate
        last_selected_query = query

        if error:
            last_error = error
            continue

        last_success_payload = payload
        last_success_source = candidate
        last_success_query = query

        if has_series(payload) and has_nonzero_values(payload):
            return payload, candidate, query, None

    if last_success_payload is not None:
        return last_success_payload, last_success_source, last_success_query, None

    if last_error is not None:
        return empty_prometheus_payload(last_error), "auto_error", last_selected_query, last_error

    return last_payload, "auto_no_nonzero", last_selected_query, last_error


def build_query_info_entry(query_name, spec, payload, start, end, step, error, promql):
    return {
        "query_name": query_name,
        "required_for": spec["required_for"],
        "promql": promql,
        "output_filename": spec["output_filename"],
        "start": start,
        "end": end,
        "step": step,
        "has_series": has_series(payload),
        "result_series_count": result_series_count(payload),
        "error": error,
    }


def print_query_summary(query_info):
    print("Query summary")
    for query_name, record in query_info["queries"].items():
        series_count = record.get("result_series_count", 0)
        error = record.get("error")
        if error:
            print(f"{query_name}: {series_count} series (error: {error})")
        else:
            print(f"{query_name}: {series_count} series")


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
    parser.add_argument(
        "--energy-source",
        choices=["auto", "joules", "bpf_cpu_time", "bpf_block_irq"],
        default="auto",
        help=(
            "Energy metric source. 'auto' tries joules first, then BPF counters "
            "as fallback (default: auto)."
        ),
    )
    parser.add_argument(
        "--persist-prom-queries",
        action="store_true",
        help="Persist extra Prometheus diagnostic queries to <run_dir>/prom_queries",
    )

    args = parser.parse_args()
    run_dir = Path(args.run_dir)

    metadata = load_metadata(run_dir)
    workload_start = to_unix_seconds(extract_timestamp(metadata, "workload_start"))
    workload_end = to_unix_seconds(extract_timestamp(metadata, "workload_end"))

    query_info = {
        "requested_energy_source": args.energy_source,
        "selected_energy_source": None,
        "query_window": {
            "workload_start": workload_start,
            "workload_end": workload_end,
        },
        "queries": {},
    }

    print(f"Querying energy using source mode: {args.energy_source}")
    energy_results, selected_energy_source, energy_promql, energy_error = query_energy_with_source(
        args.prom_url,
        workload_start,
        workload_end,
        args.energy_source,
        step=DEFAULT_STEP,
    )
    query_info["selected_energy_source"] = selected_energy_source
    query_info["queries"]["energy"] = {
        **build_query_info_entry(
            "energy",
            {"required_for": "summary", "output_filename": "energy.json"},
            energy_results,
            workload_start,
            workload_end,
            DEFAULT_STEP,
            energy_error,
            energy_promql,
        ),
        "requested_energy_source": args.energy_source,
        "selected_energy_source": selected_energy_source,
    }
    print(f"Selected energy source: {selected_energy_source}")

    saved_payloads = {"energy.json": energy_results}

    for query_name, spec in QUERY_REGISTRY.items():
        print(f"Querying {query_name}")
        payload, selected_candidate, error = run_query_candidates(
            args.prom_url,
            spec["candidates"],
            workload_start,
            workload_end,
            step=DEFAULT_STEP,
        )
        promql = selected_candidate["promql"] if selected_candidate else spec["candidates"][-1]["promql"]
        query_info["queries"][query_name] = build_query_info_entry(
            query_name,
            spec,
            payload,
            workload_start,
            workload_end,
            DEFAULT_STEP,
            error,
            promql,
        )
        saved_payloads[spec["output_filename"]] = payload

        for alias in spec.get("aliases", []):
            saved_payloads[alias] = payload

    print("Saving results")
    for filename, payload in saved_payloads.items():
        save_results(run_dir, filename, payload)

    save_results(run_dir, "query_info.json", query_info)
    print_query_summary(query_info)

    # Optional: persist additional Prometheus diagnostic queries used for mapping
    if getattr(args, "persist_prom_queries", False):
        prom_dir = Path(run_dir) / "prom_queries"
        prom_dir.mkdir(parents=True, exist_ok=True)

        def query_series(prom_url, match, start, end):
            try:
                response = requests.get(
                    f"{prom_url.rstrip('/')}/api/v1/series",
                    params={"match[]": match, "start": start, "end": end},
                    timeout=60,
                )
                response.raise_for_status()
                return response.json(), None
            except (requests.RequestException, ValueError) as exc:
                return empty_prometheus_payload(str(exc)), str(exc)

        extra_series_queries = {
            "kepler_series_all.json": "kepler_container_cpu_joules_total",
            "kepler_series_with_pod_id.json": 'kepler_container_cpu_joules_total{pod_id!=""}',
            "kepler_series_runtime_docker.json": 'kepler_container_cpu_joules_total{runtime="docker"}',
        }

        # range queries for grouped/aggregated kepler outputs
        extra_range_queries = {
            "kepler_by_container_name.json": ENERGY_QUERY_JOULES,
            "kepler_counts_by_labels.json": 'count by (container_id,container_name,runtime) (kepler_container_cpu_joules_total{container_name!=""})',
            "kepler_ns_pod_container.json": 'sum by (namespace,pod,container,container_id) (rate(kepler_container_cpu_joules_total[1m]))',
        }

        # run and save series queries
        for fname, match in extra_series_queries.items():
            payload, error = query_series(args.prom_url, match, workload_start, workload_end)
            save_results(prom_dir, fname, payload)

        # run and save range queries
        for fname, promql in extra_range_queries.items():
            payload, error = query_prometheus_safe(args.prom_url, promql, workload_start, workload_end, step=DEFAULT_STEP)
            save_results(prom_dir, fname, payload)
        print(f"Persisted extra Prometheus diagnostic queries to {prom_dir}")


if __name__ == "__main__":
    main()