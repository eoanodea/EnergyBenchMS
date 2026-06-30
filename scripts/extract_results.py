#!/usr/bin/env python3
"""Extract experiment result aggregates from runs_comparison.html dashboard files.

Outputs:
  results_aggregates.csv       — one row per (html_file × workload_level)
  results_m2_top_services.csv  — one row per (html_file × workload_level × top-5 service)
    results_m1_top_services.csv  — one row per (html_file × workload_level × top-5 service)
    results_m1_vs_m2_energy.csv  — M1 total-attributed vs M2 Etotal, per experiment/workload
  diagnostics_cv.csv           — CV across repetitions per run × level × key metric
  diagnostics_epr_trend.csv    — energy-per-request trend across low/medium/high
  diagnostics_cross_release.csv — cross-release delta in total energy and top service share
  results_iterations_ws.csv    — one row per workstation iteration (raw per-run EPR + CPU)
"""

import argparse
import csv
import json
import re
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

CV_CSV_FIELDS = [
    "source_folder",
    "app_name",
    "machine",
    "workload_level",
    "n_reps",
    "energy_total_cv",
    "energy_per_request_cv",
    "display_energy_total_cv",
    "display_energy_per_request_cv",
    "throughput_cv",
    "p95_latency_cv",
    "m2_service_cv_mean",
    "m2_service_cv_max",
    "m2_service_cv_n",
]

EPR_TREND_CSV_FIELDS = [
    "source_folder",
    "app_name",
    "machine",
    "epr_low",
    "epr_medium",
    "epr_high",
    "ratio_medium_over_low",
    "ratio_high_over_low",
    "monotone_decreasing",
    "pct_drop_low_to_high",
]

CROSS_RELEASE_CSV_FIELDS = [
    "app_family",
    "machine",
    "workload_level",
    "app_name",
    "release_label",
    "release_order",
    "energy_total_mean",
    "energy_total_pct_vs_oldest",
    "top1_service",
    "top1_mean_joules",
    "top1_share_of_top5_pct",
    "top2_service",
    "top2_mean_joules",
    "top2_share_of_top5_pct",
]

ITERATION_CSV_FIELDS = [
    "source_folder",
    "app_name",
    "machine",
    "workload_level",
    "iteration_name",
    "energy_per_request_j",        # model (eBPF) EPR
    "meter_energy_per_request_j",  # hardware-meter EPR (None when no meter)
    "cpu_mean",                    # mean CPU utilisation over iteration (from runs array)
    "cpu_p80",                     # p80 of CPU time-series (from cpu_total.json)
    "error_rate",
]

M1_M2_ENERGY_CSV_FIELDS = [
    "source_folder",
    "app_name",
    "machine",
    "experiment_name",
    "workload_level",
    "m1_iterations",
    "m2_iterations",
    "m1_total_attributed_energy_joules",
    "m1_mean_attributed_energy_joules",
    "m2_etotal_joules",
    "m2_mean_etotal_joules",
    "m1_to_m2_ratio",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def infer_machine(run_dir_name: str) -> str:
    if run_dir_name.startswith("eoan_"):
        return "workstation"
    if run_dir_name.startswith("julian_"):
        return "ec2"
    return "unknown"


def infer_app_family(app_name: str) -> tuple[str, tuple | None]:
    """Return (family, version_tuple).

    version_tuple is None for unversioned names (treated as latest release).
    E.g. 'otel-demo-1.12.0' → ('otel-demo', (1, 12, 0))
         'otel-demo'        → ('otel-demo', None)
    """
    m = re.search(r"-(\d+(?:\.\d+)+)$", app_name)
    if m:
        family = app_name[: m.start()]
        version = tuple(int(x) for x in m.group(1).split("."))
        return family, version
    return app_name, None


def safe_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def safe_cv(mean, std):
    m, s = safe_float(mean), safe_float(std)
    if m is None or s is None or m == 0:
        return None
    return s / m


def _percentile80(values: list) -> float:
    """Linear-interpolation p80 of a list of floats."""
    arr = sorted(values)
    n = len(arr)
    idx = 0.80 * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    return arr[lo] + (idx - lo) * (arr[hi] - arr[lo])


def extract_iteration_name(run_name: str):
    """Return the 'iteration_...' folder name from a run name like 'medium/iteration_...'."""
    m = re.search(r'(iteration_[0-9_]+)', str(run_name))
    return m.group(1) if m else None


def load_cpu_p80(iteration_dir: Path):
    """Read cpu_total.json and return the p80 of the CPU utilisation time series, or None."""
    cpu_json = iteration_dir / 'cpu_total.json'
    if not cpu_json.exists():
        return None
    try:
        data = json.loads(cpu_json.read_text(encoding='utf-8'))
        series = ((data.get('data') or {}).get('result') or [])
        if not series:
            return None
        vals = []
        for v in (series[0].get('values') or []):
            try:
                f = float(v[1])
                if f == f:  # exclude NaN
                    vals.append(f)
            except (TypeError, ValueError, IndexError):
                pass
        return _percentile80(vals) if vals else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# HTML extraction
# ---------------------------------------------------------------------------

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


def top5_model_services(attribution_phase1: dict, workload_level: str, model: str) -> list[dict]:
    workloads = attribution_phase1.get("workloads", {})
    workload_data = workloads.get(workload_level, {})
    services = workload_data.get("models", {}).get(model, {}).get("services", {})
    ranked = sorted(
        services.items(),
        key=lambda kv: kv[1].get("mean") or 0,
        reverse=True,
    )
    return [
        {
            "service": name,
            "mean_joules": stats.get("mean"),
            "std_joules": stats.get("std"),
            "cv": stats.get("cv"),
        }
        for name, stats in ranked[:5]
    ]


def top5_m2_services(attribution_phase1: dict, workload_level: str) -> list[dict]:
    return top5_model_services(attribution_phase1, workload_level, "M2")


def top5_m1_services(attribution_phase1: dict, workload_level: str) -> list[dict]:
    return top5_model_services(attribution_phase1, workload_level, "M1")


def extract_m1_vs_m2_energy_rows(
    attribution_phase1: dict,
    source_folder: str,
    app_name: str,
    machine: str,
    experiment_name: str,
) -> list[dict]:
    """Extract per-workload and overall M1-vs-M2 energy summaries for one experiment.

    Values come from attribution_phase1.workloads.*.sum_consistency checks:
      - M1 total attributed energy from M1 reference_sum (or service_sum fallback)
      - M2 Etotal from M2 reference_sum (or service_sum fallback)
    """
    workloads = attribution_phase1.get("workloads", {})
    if not isinstance(workloads, dict):
        return []

    level_order = {"low": 0, "medium": 1, "high": 2}

    def model_values(workload_data: dict, model: str) -> list[float]:
        checks = (
            workload_data.get("sum_consistency", {})
            .get(model, {})
            .get("checks", [])
        )
        values = []
        for c in checks:
            ref = safe_float(c.get("reference_sum"))
            svc = safe_float(c.get("service_sum"))
            v = ref if ref is not None else svc
            if v is not None:
                values.append(v)
        return values

    rows = []
    all_m1_vals = []
    all_m2_vals = []

    for level, workload_data in sorted(
        workloads.items(), key=lambda kv: (level_order.get(kv[0], 99), kv[0])
    ):
        m1_vals = model_values(workload_data, "M1")
        m2_vals = model_values(workload_data, "M2")

        all_m1_vals.extend(m1_vals)
        all_m2_vals.extend(m2_vals)

        m1_total = sum(m1_vals) if m1_vals else None
        m2_total = sum(m2_vals) if m2_vals else None
        m1_mean = (m1_total / len(m1_vals)) if m1_vals else None
        m2_mean = (m2_total / len(m2_vals)) if m2_vals else None
        ratio = (m1_mean / m2_mean) if m1_mean is not None and m2_mean not in (None, 0) else None

        rows.append({
            "source_folder": source_folder,
            "app_name": app_name,
            "machine": machine,
            "experiment_name": experiment_name,
            "workload_level": level,
            "m1_iterations": len(m1_vals),
            "m2_iterations": len(m2_vals),
            "m1_total_attributed_energy_joules": m1_total,
            "m1_mean_attributed_energy_joules": m1_mean,
            "m2_etotal_joules": m2_total,
            "m2_mean_etotal_joules": m2_mean,
            "m1_to_m2_ratio": ratio,
        })

    # Overall row across all workload levels for this experiment.
    m1_total_all = sum(all_m1_vals) if all_m1_vals else None
    m2_total_all = sum(all_m2_vals) if all_m2_vals else None
    m1_mean_all = (m1_total_all / len(all_m1_vals)) if all_m1_vals else None
    m2_mean_all = (m2_total_all / len(all_m2_vals)) if all_m2_vals else None
    ratio_all = (
        m1_mean_all / m2_mean_all
        if m1_mean_all is not None and m2_mean_all not in (None, 0)
        else None
    )

    rows.append({
        "source_folder": source_folder,
        "app_name": app_name,
        "machine": machine,
        "experiment_name": experiment_name,
        "workload_level": "all",
        "m1_iterations": len(all_m1_vals),
        "m2_iterations": len(all_m2_vals),
        "m1_total_attributed_energy_joules": m1_total_all,
        "m1_mean_attributed_energy_joules": m1_mean_all,
        "m2_etotal_joules": m2_total_all,
        "m2_mean_etotal_joules": m2_mean_all,
        "m1_to_m2_ratio": ratio_all,
    })

    return rows


# ---------------------------------------------------------------------------
# Diagnostic 1: CV across repetitions
# ---------------------------------------------------------------------------

def compute_cv_diagnostics(aggregate_rows, service_rows):
    """One row per (source_folder × workload_level) summarising measurement stability."""
    svc_cvs: dict[tuple, list[float]] = {}
    for r in service_rows:
        key = (r["source_folder"], r["workload_level"])
        cv = safe_float(r.get("cv"))
        if cv is not None:
            svc_cvs.setdefault(key, []).append(cv)

    rows = []
    for r in aggregate_rows:
        key = (r["source_folder"], r["workload_level"])
        cvs = svc_cvs.get(key, [])
        rows.append({
            "source_folder": r["source_folder"],
            "app_name": r["app_name"],
            "machine": r["machine"],
            "workload_level": r["workload_level"],
            "n_reps": r.get("run_count"),
            "energy_total_cv": safe_cv(r.get("energy_total_mean"), r.get("energy_total_std")),
            "energy_per_request_cv": safe_cv(r.get("energy_per_request_mean"), r.get("energy_per_request_std")),
            "display_energy_total_cv": safe_cv(r.get("display_energy_total_mean"), r.get("display_energy_total_std")),
            "display_energy_per_request_cv": safe_cv(r.get("display_energy_per_request_mean"), r.get("display_energy_per_request_std")),
            "throughput_cv": safe_cv(r.get("throughput_mean"), r.get("throughput_std")),
            "p95_latency_cv": safe_cv(r.get("p95_latency_mean"), r.get("p95_latency_std")),
            "m2_service_cv_mean": sum(cvs) / len(cvs) if cvs else None,
            "m2_service_cv_max": max(cvs) if cvs else None,
            "m2_service_cv_n": len(cvs),
        })
    return rows


# ---------------------------------------------------------------------------
# Diagnostic 2: Energy-per-request trend across workload levels
# ---------------------------------------------------------------------------

def compute_epr_trend(aggregate_rows):
    """One row per source_folder — EPR at each level and monotonicity flag."""
    by_folder: dict[str, dict] = {}
    for r in aggregate_rows:
        folder = r["source_folder"]
        entry = by_folder.setdefault(folder, {"app_name": r["app_name"], "machine": r["machine"]})
        entry[r["workload_level"]] = safe_float(r.get("display_energy_per_request_mean"))

    rows = []
    for folder, data in sorted(by_folder.items()):
        epr_low = data.get("low")
        epr_med = data.get("medium")
        epr_high = data.get("high")

        ratio_med_low = (epr_med / epr_low) if epr_low and epr_med else None
        ratio_high_low = (epr_high / epr_low) if epr_low and epr_high else None

        monotone = None
        if epr_low is not None and epr_med is not None and epr_high is not None:
            monotone = epr_med <= epr_low and epr_high <= epr_med

        pct_drop = None
        if epr_low and epr_high:
            pct_drop = (epr_low - epr_high) / epr_low * 100

        rows.append({
            "source_folder": folder,
            "app_name": data["app_name"],
            "machine": data["machine"],
            "epr_low": epr_low,
            "epr_medium": epr_med,
            "epr_high": epr_high,
            "ratio_medium_over_low": ratio_med_low,
            "ratio_high_over_low": ratio_high_low,
            "monotone_decreasing": monotone,
            "pct_drop_low_to_high": pct_drop,
        })
    return rows


# ---------------------------------------------------------------------------
# Diagnostic 3: Cross-release delta in total energy and top-service share
# ---------------------------------------------------------------------------

def compute_cross_release_delta(aggregate_rows, service_rows):
    """One row per (app_family × machine × workload_level × release).

    Releases are sorted oldest-first (lowest version number), with the
    unversioned name treated as the latest release. The % delta is relative
    to the oldest release as baseline.
    """
    svc_by_key: dict[tuple, list] = {}
    for r in service_rows:
        key = (r["source_folder"], r["workload_level"])
        svc_by_key.setdefault(key, []).append(r)
    for key in svc_by_key:
        svc_by_key[key].sort(
            key=lambda x: safe_float(x.get("mean_joules")) or 0, reverse=True
        )

    grouped: dict[tuple, list] = {}
    for r in aggregate_rows:
        family, version = infer_app_family(r["app_name"])
        key = (family, r["machine"], r["workload_level"])
        grouped.setdefault(key, []).append({**r, "_family": family, "_version": version})

    def release_sort_key(r):
        v = r["_version"]
        # versioned releases sort by version tuple; None (latest/unversioned) sorts last
        return (0, v) if v is not None else (1, ())

    rows = []
    for (family, machine, workload_level), releases in sorted(grouped.items()):
        releases_sorted = sorted(releases, key=release_sort_key)

        baseline_energy = safe_float(releases_sorted[0].get("energy_total_mean")) if releases_sorted else None

        for idx, r in enumerate(releases_sorted):
            src = r["source_folder"]
            energy = safe_float(r.get("energy_total_mean"))

            pct_vs_oldest = None
            if baseline_energy and energy is not None and baseline_energy != 0:
                pct_vs_oldest = (energy - baseline_energy) / baseline_energy * 100

            svcs = svc_by_key.get((src, workload_level), [])
            top1 = svcs[0] if svcs else {}
            top2 = svcs[1] if len(svcs) > 1 else {}
            top5_sum = sum(safe_float(s.get("mean_joules")) or 0 for s in svcs) or None

            def share(svc):
                if not svc or not svc.get("mean_joules") or not top5_sum:
                    return None
                return (safe_float(svc["mean_joules"]) or 0) / top5_sum * 100

            v = r["_version"]
            release_label = ".".join(str(x) for x in v) if v is not None else "latest"

            rows.append({
                "app_family": family,
                "machine": machine,
                "workload_level": workload_level,
                "app_name": r["app_name"],
                "release_label": release_label,
                "release_order": idx,
                "energy_total_mean": energy,
                "energy_total_pct_vs_oldest": pct_vs_oldest,
                "top1_service": top1.get("service"),
                "top1_mean_joules": top1.get("mean_joules"),
                "top1_share_of_top5_pct": share(top1),
                "top2_service": top2.get("service"),
                "top2_mean_joules": top2.get("mean_joules"),
                "top2_share_of_top5_pct": share(top2),
            })

    return rows


# ---------------------------------------------------------------------------
# CSV writer helper
# ---------------------------------------------------------------------------

def write_csv(path: Path, fields: list[str], rows: list[dict]):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows):>4} rows to {path.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    default_base = Path(__file__).parent.parent / "final-runs" / "experiment-runs-with-attribution"
    default_out = Path(__file__).parent.parent / "final-runs"

    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", type=Path, default=default_base,
                        help="Path to experiment-runs-with-attribution/ directory")
    parser.add_argument("--output-dir", type=Path, default=default_out,
                        help="Directory to write output CSVs")
    args = parser.parse_args()

    html_files = find_html_files(args.base_dir)
    print(f"Found {len(html_files)} HTML file(s) to process.\n")

    aggregate_rows = []
    service_rows = []
    service_rows_m1 = []
    iteration_rows = []
    m1_m2_energy_rows = []

    for html_path in html_files:
        source_folder = html_path.parent.name
        app_name = html_path.parent.parent.name
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

            agg_count = svc_count = svc_count_m1 = 0

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
                    service_rows.append({
                        "source_folder": source_folder,
                        "app_name": app_name,
                        "machine": machine,
                        "experiment_name": experiment_name,
                        "workload_level": level,
                        **svc,
                    })
                    svc_count += 1

                for svc in top5_m1_services(attribution_phase1, level):
                    service_rows_m1.append({
                        "source_folder": source_folder,
                        "app_name": app_name,
                        "machine": machine,
                        "experiment_name": experiment_name,
                        "workload_level": level,
                        **svc,
                    })
                    svc_count_m1 += 1

            print(f"  OK — {agg_count} level aggregate row(s), {svc_count} M2 service row(s), {svc_count_m1} M1 service row(s)")

            m1_m2_energy_rows.extend(
                extract_m1_vs_m2_energy_rows(
                    attribution_phase1,
                    source_folder,
                    app_name,
                    machine,
                    experiment_name,
                )
            )

            # Per-iteration extraction (workstation only — EC2 lacks cpu_total.json)
            if machine == 'workstation':
                experiment_dir = html_path.parent
                it_count = 0
                for run in data.get('runs', []):
                    workload = str(run.get('workload_level', '')).lower()
                    if workload not in ('low', 'medium', 'high'):
                        continue
                    it_name = extract_iteration_name(str(run.get('name', '')))
                    if not it_name:
                        continue
                    iteration_rows.append({
                        'source_folder': source_folder,
                        'app_name': app_name,
                        'machine': machine,
                        'workload_level': workload,
                        'iteration_name': it_name,
                        'energy_per_request_j': safe_float(run.get('energy_per_request')),
                        'meter_energy_per_request_j': safe_float(run.get('meter_energy_per_request_joules')),
                        'cpu_mean': safe_float(run.get('cpu_mean')),
                        'cpu_p80': load_cpu_p80(experiment_dir / workload / it_name),
                        'error_rate': safe_float(run.get('error_rate')),
                    })
                    it_count += 1
                print(f"  WS iterations — {it_count} row(s)")

        except Exception as exc:
            print(f"  ERROR processing data: {exc}")
            continue

    print()
    out = args.output_dir
    write_csv(out / "results_aggregates.csv",      AGGREGATE_CSV_FIELDS,    aggregate_rows)
    write_csv(out / "results_m2_top_services.csv", SERVICE_CSV_FIELDS,      service_rows)
    write_csv(out / "results_m1_top_services.csv", SERVICE_CSV_FIELDS,      service_rows_m1)
    write_csv(out / "diagnostics_cv.csv",          CV_CSV_FIELDS,           compute_cv_diagnostics(aggregate_rows, service_rows))
    write_csv(out / "diagnostics_epr_trend.csv",   EPR_TREND_CSV_FIELDS,    compute_epr_trend(aggregate_rows))
    write_csv(out / "diagnostics_cross_release.csv", CROSS_RELEASE_CSV_FIELDS, compute_cross_release_delta(aggregate_rows, service_rows))
    write_csv(out / "results_iterations_ws.csv",   ITERATION_CSV_FIELDS,    iteration_rows)
    write_csv(out / "results_m1_vs_m2_energy.csv", M1_M2_ENERGY_CSV_FIELDS, m1_m2_energy_rows)


if __name__ == "__main__":
    main()
