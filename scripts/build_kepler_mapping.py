#!/usr/bin/env python3
"""Build Kepler -> pod/container mapping from saved Prometheus query outputs.

Usage: python scripts/build_kepler_mapping.py \
    --run-dir final-runs/experiment-runs/oteldemo/eoan_20260520_132436_360089_otel-demo/medium/iteration_20260520_141436_309416

This script reads:
- energyAnalyserPrometheusQueries/kepler_series_labels.json
- <run_dir>/cpu_k8s_by_id.json
- <run_dir>/cpu_by_container.json (optional)
- <run_dir>/metadata.json

And writes:
- <run_dir>/attribution/kepler_to_pod_mapping.json

Heuristics:
- match long container_id from kepler_series_labels to cpu_k8s_by_id id substrings (high confidence)
- if kepler entry has pod_id, record mapping to pod_id (high confidence)
- match container_name to known SUT container names from metadata (medium confidence)
"""

import argparse
import json
import os
from pathlib import Path
from collections import defaultdict


def load_json(path):
    if not os.path.exists(path):
        return None
    with open(path, 'r') as f:
        return json.load(f)


def ensure_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)


def build_mapping(run_dir):
    queries_dir = os.path.join('energyAnalyserPrometheusQueries')
    kepler_labels = load_json(os.path.join(queries_dir, 'kepler_series_labels.json'))
    if not kepler_labels:
        print('kepler_series_labels.json not found in', queries_dir)
        return None

    run_cpu_k8s = load_json(os.path.join(run_dir, 'cpu_k8s_by_id.json'))
    run_cpu_by_container = load_json(os.path.join(run_dir, 'cpu_by_container.json'))
    metadata = load_json(os.path.join(run_dir, 'metadata.json')) or {}

    # Build searchable sets
    cpu_ids = []
    if run_cpu_k8s and run_cpu_k8s.get('data') and run_cpu_k8s['data'].get('result'):
        for s in run_cpu_k8s['data']['result']:
            mid = s.get('metric', {}).get('id')
            if mid:
                cpu_ids.append(mid)

    sut_names = set()
    if metadata.get('sut_containers'):
        sut_names.update(metadata.get('sut_containers'))
    # also list deployments
    if metadata.get('deployment') and metadata['deployment'].get('deployments'):
        for d in metadata['deployment']['deployments']:
            sut_names.add(d.get('name'))

    results = []

    # kepler_labels: structure {"status":..., "data": [ {metric...}, ... ] }
    # kepler_series_labels.json may be from the /api/v1/series response (list of label dicts),
    # or from a query_range result (with 'data': { 'result': [ { 'metric': {...} }, ... ] }).
    data_section = kepler_labels.get('data')
    entries = []
    if isinstance(data_section, dict) and data_section.get('result'):
        entries = data_section.get('result')
    elif isinstance(data_section, list):
        # series API returns a list of label dicts
        entries = data_section
    else:
        entries = []

    for entry in entries:
        # if entry is a metric wrapper, it has 'metric'; otherwise entry itself is the labels dict
        metric = entry.get('metric', entry)
        container_id = metric.get('container_id')
        container_name = metric.get('container_name')
        pod_id = metric.get('pod_id')
        runtime = metric.get('runtime')

        rec = {
            'kepler_container_name': container_name or None,
            'container_id_long': container_id or None,
            'pod_id': pod_id or None,
            'runtime': runtime or None,
            'mapping': None,
            'mapping_confidence': 'none',
            'mapping_provenance': []
        }

        # 1) If pod_id present -> record mapping to pod_id (high confidence)
        if pod_id:
            rec['mapping'] = {'type': 'pod_id', 'value': pod_id}
            rec['mapping_confidence'] = 'high'
            rec['mapping_provenance'].append('kepler:pod_id')

        # 2) Try to match long container_id into cpu_k8s_by_id entries
        if not rec['mapping'] and container_id:
            for cid in cpu_ids:
                if container_id in cid:
                    rec['mapping'] = {'type': 'cpu_k8s_id', 'value': cid}
                    rec['mapping_confidence'] = 'high'
                    rec['mapping_provenance'].append('cpu_k8s_by_id:substring_match')
                    break

        # 3) If still not mapped, try to match container_name to known sut names
        if not rec['mapping'] and container_name:
            if container_name in sut_names:
                rec['mapping'] = {'type': 'sut_container', 'value': container_name}
                rec['mapping_confidence'] = 'medium'
                rec['mapping_provenance'].append('metadata:sut_containers')

        # 4) If still not mapped but container_name looks like a short hex and container_id_long exists,
        #    record unresolved docker entry with provenance
        if not rec['mapping']:
            if container_name and len(container_name) in (12, 11) and all(c in '0123456789abcdef' for c in container_name.lower()):
                rec['mapping_confidence'] = 'none'
                rec['mapping_provenance'].append('docker-short-id:no-pod-info')
            else:
                rec['mapping_confidence'] = 'low'
                rec['mapping_provenance'].append('no_match')

        results.append(rec)

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-dir', required=True)
    args = parser.parse_args()

    mapping = build_mapping(args.run_dir)
    if mapping is None:
        print('No mapping generated')
        return

    out_dir = os.path.join(args.run_dir, 'attribution')
    ensure_dir(out_dir)
    out_path = os.path.join(out_dir, 'kepler_to_pod_mapping.json')
    with open(out_path, 'w') as f:
        json.dump({'mapping': mapping}, f, indent=2)

    print('Wrote mapping to', out_path)


if __name__ == '__main__':
    main()
