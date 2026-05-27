#!/usr/bin/env python3
"""Preflight check for Prometheus Kepler/kube label coverage.

Usage: python3 scripts/preflight_prometheus_check.py --queries-dir prom_queries

Reads saved query outputs (from scripts/collect_prometheus_queries.sh) and prints
a short coverage report (counts and percentages) so you can validate before experiments.
"""
import argparse
import json
import os
from pathlib import Path


def load_json(path):
    if not os.path.exists(path):
        return None
    with open(path, 'r') as f:
        return json.load(f)


def summarize(queries_dir):
    kepler_all = load_json(os.path.join(queries_dir, 'kepler_series_all.json')) or []
    kepler_with_pod = load_json(os.path.join(queries_dir, 'kepler_series_with_pod_id.json')) or []
    kepler_runtime_docker = load_json(os.path.join(queries_dir, 'kepler_series_runtime_docker.json')) or []

    # input formats: series API returns a list of label dicts; query_range returns {data:{result:[{metric:...}]}}
    def entries_from(obj):
        if isinstance(obj, dict):
            data = obj.get('data')
            if isinstance(data, dict) and data.get('result'):
                return [r.get('metric', r) for r in data.get('result')]
            if isinstance(data, list):
                return data
            return []
        if isinstance(obj, list):
            return obj
        return []

    all_entries = entries_from(kepler_all)
    pod_entries = entries_from(kepler_with_pod)
    docker_entries = entries_from(kepler_runtime_docker)

    total = len(all_entries)
    with_pod = sum(1 for e in all_entries if e.get('pod_id'))
    with_container_id = sum(1 for e in all_entries if e.get('container_id'))
    runtime_docker = sum(1 for e in all_entries if e.get('runtime') == 'docker')
    docker_with_pod = sum(1 for e in all_entries if e.get('runtime') == 'docker' and e.get('pod_id'))

    print('Prometheus Kepler preflight report')
    print('Queries dir:', queries_dir)
    print('Total kepler series:', total)
    print(f' - series with pod_id: {with_pod} ({(with_pod/total*100) if total else 0:.1f}%)')
    print(f' - series with container_id: {with_container_id} ({(with_container_id/total*100) if total else 0:.1f}%)')
    print(f' - series runtime=docker: {runtime_docker} ({(runtime_docker/total*100) if total else 0:.1f}%)')
    print(f' - docker series with pod_id: {docker_with_pod} ({(docker_with_pod/runtime_docker*100) if runtime_docker else 0:.1f}%)')

    # list unresolved short-ids (docker series with container_name short hex)
    short_ids = []
    for e in all_entries:
        cname = e.get('container_name')
        r = e.get('runtime')
        cid = e.get('container_id')
        if r == 'docker' and cname and len(cname) in (11,12) and all(c in '0123456789abcdef' for c in cname.lower()):
            short_ids.append({'container_name': cname, 'container_id': cid, 'pod_id': e.get('pod_id')})

    if short_ids:
        print('\nDocker short container_name series detected (examples):')
        for s in short_ids[:10]:
            print(' -', s)
    else:
        print('\nNo docker short-id container_name series detected.')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--queries-dir', default='prom_queries')
    args = parser.parse_args()

    summarize(args.queries_dir)


if __name__ == '__main__':
    main()
