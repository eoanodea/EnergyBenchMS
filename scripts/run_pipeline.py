#!/usr/bin/env python3
"""Run a batch of experiments, collect metrics, and build a comparison dashboard."""

import argparse
import csv
from datetime import datetime
import json
import re
import subprocess
import sys
from pathlib import Path

import yaml


RUN_DIR_PATTERN = re.compile(r"Results saved to:\s*(runs/\S+)")
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
CLEANUP_SCRIPT = SCRIPT_DIR / "cleanup_sut.py"
SATURATION_ANALYSE_SCRIPT = SCRIPT_DIR / "saturation_analyse.py"


def run_step(command, description):
    """Run a subprocess command and return its completed process."""
    print(f"{description}: {' '.join(command)}")
    completed = subprocess.run(command, capture_output=True, text=True, cwd=REPO_ROOT)

    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)

    if completed.returncode != 0:
        raise subprocess.CalledProcessError(
            completed.returncode,
            command,
            output=completed.stdout,
            stderr=completed.stderr,
        )

    return completed


def cleanup_sut(app, cooldown_seconds):
    """Delete the SUT manifests, wait for pods to terminate, then pause."""
    cleanup_cmd = [
        sys.executable,
        str(CLEANUP_SCRIPT),
        "--app",
        app,
        "--sleep-seconds",
        str(cooldown_seconds),
    ]
    run_step(cleanup_cmd, "Cleaning up SUT")


def read_sut_name(app_path):
    """Read the SUT name from the deployment manifest."""
    deployment_path = Path(app_path) / "deployment.yaml"
    with deployment_path.open("r", encoding="utf-8") as infile:
        deployment = yaml.safe_load(infile)

    name = deployment.get("metadata", {}).get("name")
    return name or Path(app_path).name


def create_batch_directory(runs_root, sut_name):
    """Create the top-level batch directory for a pipeline execution."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    batch_dir = Path(runs_root) / f"{timestamp}_{sut_name}"
    batch_dir.mkdir(parents=True, exist_ok=False)
    return batch_dir


def create_iteration_directory(batch_dir):
    """Create an iteration directory inside the current batch."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    iteration_dir = Path(batch_dir) / f"iteration_{timestamp}"
    iteration_dir.mkdir(parents=True, exist_ok=False)
    return iteration_dir


def create_saturation_iteration_directory(batch_dir, level):
    """Create a saturation iteration directory that includes the user level."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    iteration_dir = Path(batch_dir) / f"level_{int(level):03d}_iteration_{timestamp}"
    iteration_dir.mkdir(parents=True, exist_ok=False)
    return iteration_dir


def extract_run_dir(output_text):
    """Extract the run directory path from run_experiment output."""
    match = RUN_DIR_PATTERN.search(output_text)
    if not match:
        raise ValueError("Could not determine the created run directory from output")
    return Path(match.group(1))


def parse_levels(levels_text):
    """Parse comma-separated user levels from CLI input."""
    values = []
    for raw_part in str(levels_text).split(","):
        part = raw_part.strip()
        if not part:
            continue
        values.append(int(part))
    if not values:
        raise ValueError("At least one saturation level is required")
    if any(level <= 0 for level in values):
        raise ValueError("Saturation levels must be positive integers")
    return values


def load_workload_yaml(workload_path):
    """Load workload YAML from disk."""
    with Path(workload_path).open("r", encoding="utf-8") as infile:
        loaded = yaml.safe_load(infile)
    if not isinstance(loaded, dict):
        raise ValueError("Workload YAML must contain a top-level mapping")
    return loaded


def resolve_saturation_settings(args):
    """Resolve saturation settings from workload YAML and CLI overrides."""
    workload = load_workload_yaml(args.workload)
    saturation = workload.get("saturation", {})
    if not isinstance(saturation, dict):
        raise ValueError("'saturation' must be a mapping when provided")

    levels = args.sat_levels
    if levels is None:
        levels = saturation.get("levels")
    if not levels:
        levels = [20, 40, 60, 80, 100]

    if isinstance(levels, str):
        levels = parse_levels(levels)
    else:
        levels = [int(level) for level in levels]

    dwell_seconds = args.sat_dwell_seconds
    if dwell_seconds is None:
        dwell_seconds = saturation.get("dwell_seconds")
    if dwell_seconds is None:
        raise ValueError("Saturation mode requires dwell_seconds")
    dwell_seconds = int(dwell_seconds)

    spawn_rate = args.sat_spawn_rate
    if spawn_rate is None:
        spawn_rate = saturation.get("spawn_rate")
    if spawn_rate is None:
        spawn_rate = workload.get("spawn_rate")
    if spawn_rate is None:
        raise ValueError("Saturation mode requires spawn_rate")
    spawn_rate = float(spawn_rate)

    ramp_exclusion_seconds = args.sat_ramp_exclusion_seconds
    if ramp_exclusion_seconds is None:
        ramp_exclusion_seconds = saturation.get("ramp_exclusion_seconds", 20)
    ramp_exclusion_seconds = int(ramp_exclusion_seconds)

    cooldown_seconds = args.sat_cooldown_seconds
    if cooldown_seconds is None:
        cooldown_seconds = saturation.get("cooldown_seconds", args.cooldown_seconds)
    cooldown_seconds = int(cooldown_seconds)

    reset_between_levels = args.sat_reset_between_levels
    if reset_between_levels is None:
        reset_between_levels = saturation.get("reset_between_levels", True)

    if dwell_seconds <= 0:
        raise ValueError("dwell_seconds must be greater than 0")
    if spawn_rate <= 0:
        raise ValueError("spawn_rate must be greater than 0")
    if ramp_exclusion_seconds < 0:
        raise ValueError("ramp_exclusion_seconds must be at least 0")
    if cooldown_seconds < 0:
        raise ValueError("cooldown_seconds must be at least 0")
    if ramp_exclusion_seconds >= dwell_seconds:
        raise ValueError("ramp_exclusion_seconds must be less than dwell_seconds")

    return {
        "levels": levels,
        "dwell_seconds": dwell_seconds,
        "spawn_rate": spawn_rate,
        "ramp_exclusion_seconds": ramp_exclusion_seconds,
        "cooldown_seconds": cooldown_seconds,
        "reset_between_levels": bool(reset_between_levels),
    }


def write_json(path, payload):
    """Write JSON payload to disk."""
    with Path(path).open("w", encoding="utf-8") as outfile:
        json.dump(payload, outfile, indent=2)


def load_json(path):
    """Load JSON payload from disk."""
    with Path(path).open("r", encoding="utf-8") as infile:
        return json.load(infile)


def parse_effective_duration_seconds(metadata):
    """Compute effective duration from metadata timestamps and ramp exclusion."""
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

    duration = max(0.0, (end_dt - start_dt).total_seconds())
    if "workload_effective_start" not in timestamps:
        ramp_exclusion = metadata.get("ramp_exclusion_seconds", 0)
        try:
            duration = max(0.0, duration - float(ramp_exclusion))
        except (TypeError, ValueError):
            pass
    return duration


def sum_energy_mean(summary):
    """Sum mean energy across reported containers for this run."""
    total = 0.0
    energy_by_container = summary.get("energy_by_container_name", {})
    if not isinstance(energy_by_container, dict):
        return 0.0

    for stats in energy_by_container.values():
        if not isinstance(stats, dict):
            continue
        value = stats.get("mean")
        if isinstance(value, (int, float)):
            total += float(value)

    return total


def compute_calibration_row(user_level, run_dir):
    """Build one calibration CSV row from run outputs."""
    summary = load_json(Path(run_dir) / "summary.json")
    metadata = load_json(Path(run_dir) / "metadata.json")
    query_info_path = Path(run_dir) / "query_info.json"
    query_info = load_json(query_info_path) if query_info_path.exists() else {}

    workload = summary.get("workload", {}) if isinstance(summary, dict) else {}
    cpu_total = summary.get("cpu_total", {}) if isinstance(summary, dict) else {}

    throughput_mean_rps = workload.get("throughput_mean_rps")
    p95_latency = workload.get("p95_latency")
    error_rate = workload.get("error_rate")
    cpu_mean = cpu_total.get("mean") if isinstance(cpu_total, dict) else None
    cpu_max = cpu_total.get("max") if isinstance(cpu_total, dict) else None
    selected_energy_source = query_info.get("selected_energy_source", "")

    effective_duration = parse_effective_duration_seconds(metadata)
    energy_mean = sum_energy_mean(summary)
    energy_total = ""
    energy_per_request = ""

    if isinstance(effective_duration, (int, float)):
        energy_total = energy_mean * effective_duration

    successful_requests = ""
    if isinstance(throughput_mean_rps, (int, float)) and isinstance(effective_duration, (int, float)):
        successful_requests = throughput_mean_rps * effective_duration

    if isinstance(energy_total, (int, float)) and isinstance(successful_requests, (int, float)):
        if successful_requests > 0:
            energy_per_request = energy_total / successful_requests

    return {
        "user_level": user_level,
        "energy_source": selected_energy_source,
        "throughput_mean": throughput_mean_rps if throughput_mean_rps is not None else "",
        "cpu_mean": cpu_mean if cpu_mean is not None else "",
        "cpu_max": cpu_max if cpu_max is not None else "",
        "energy_total": energy_total,
        "energy_per_request": energy_per_request,
        "p95_latency": p95_latency if p95_latency is not None else "",
        "error_rate": error_rate if error_rate is not None else "",
    }


def write_calibration_summary(batch_dir, saturation_plan):
    """Generate calibration_summary.csv from saturation run outputs."""
    rows = []
    for run_info in saturation_plan.get("runs", []):
        user_level = run_info.get("user_level")
        run_dir = run_info.get("run_dir")
        if user_level is None or not run_dir:
            continue
        rows.append(compute_calibration_row(user_level, run_dir))

    output_path = Path(batch_dir) / "calibration_summary.csv"
    fieldnames = [
        "user_level",
        "energy_source",
        "throughput_mean",
        "cpu_mean",
        "cpu_max",
        "energy_total",
        "energy_per_request",
        "p95_latency",
        "error_rate",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in sorted(rows, key=lambda item: int(item["user_level"])):
            writer.writerow(row)


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Run the same experiment multiple times, query Prometheus for each run, "
            "summarise the outputs, and generate a comparison dashboard."
        )
    )
    parser.add_argument(
        "--count",
        "-n",
        type=int,
        default=3,
        help="Number of times to repeat the experiment (default: 3)",
    )
    parser.add_argument(
        "--app",
        required=True,
        help="Path to application directory (for example, apps/simple-web)",
    )
    parser.add_argument(
        "--workload",
        required=True,
        help="Path to workload YAML file (for example, workloads/simple-web.yaml)",
    )
    parser.add_argument(
        "--locustfile",
        default="locustfile.py",
        help="Path to Locust file (default: locustfile.py)",
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
            "Energy metric source passed to query_prometheus.py "
            "(default: auto)"
        ),
    )
    parser.add_argument(
        "--cooldown-seconds",
        type=int,
        default=0,
        help="Cooldown time in seconds to wait after warmup and between runs (default: 0)",
    )
    parser.add_argument(
        "--runs-dir",
        default="runs",
        help="Directory containing run folders (default: runs)",
    )
    parser.add_argument(
        "--output",
        default="runs_comparison.html",
        help="Output HTML path (default: runs_comparison.html)",
    )
    parser.add_argument(
        "--saturation-enabled",
        action="store_true",
        help="Enable stepwise saturation mode",
    )
    parser.add_argument(
        "--sat-levels",
        type=parse_levels,
        help="Comma-separated user levels for saturation mode (for example: 20,40,60)",
    )
    parser.add_argument(
        "--sat-dwell-seconds",
        type=int,
        help="Fixed dwell duration per saturation level (seconds)",
    )
    parser.add_argument(
        "--sat-spawn-rate",
        type=float,
        help="Spawn rate used in saturation mode",
    )
    parser.add_argument(
        "--sat-ramp-exclusion-seconds",
        type=int,
        help="Ramp exclusion seconds used for effective measurement windows",
    )
    parser.add_argument(
        "--sat-cooldown-seconds",
        type=int,
        help="Cooldown between saturation levels",
    )
    parser.add_argument(
        "--sat-plateau-threshold",
        type=float,
        default=0.05,
        help="Throughput plateau threshold as fraction (default: 0.05)",
    )
    parser.add_argument(
        "--sat-latency-jump-threshold",
        type=float,
        default=0.30,
        help="Latency jump threshold as fraction (default: 0.30)",
    )
    parser.add_argument(
        "--sat-error-rate-threshold",
        type=float,
        default=0.01,
        help="Error rate threshold as fraction (default: 0.01)",
    )
    parser.add_argument(
        "--sat-cpu-threshold",
        type=float,
        default=0.90,
        help="CPU mean threshold as fraction (default: 0.90)",
    )
    reset_group = parser.add_mutually_exclusive_group()
    reset_group.add_argument(
        "--sat-reset-between-levels",
        dest="sat_reset_between_levels",
        action="store_true",
        help="Cleanup and cooldown between each saturation level",
    )
    reset_group.add_argument(
        "--sat-no-reset-between-levels",
        dest="sat_reset_between_levels",
        action="store_false",
        help="Do not cleanup between saturation levels",
    )
    parser.set_defaults(sat_reset_between_levels=None)
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.count < 1:
        raise SystemExit("--count must be at least 1")
    if args.cooldown_seconds < 0:
        raise SystemExit("--cooldown-seconds must be at least 0")

    sut_name = read_sut_name(args.app)
    batch_dir = create_batch_directory(args.runs_dir, sut_name)
    created_runs = []
    output_path = batch_dir / Path(args.output).name

    print("=== Warmup ===")
    warmup_cmd = [
        sys.executable,
        str(SCRIPT_DIR / "run_experiment.py"),
        "--app",
        args.app,
        "--workload",
        args.workload,
        "--locustfile",
        args.locustfile,
        "--no-results",
    ]
    run_step(warmup_cmd, "Running warmup")

    if args.saturation_enabled:
        saturation = resolve_saturation_settings(args)
        cleanup_sut(
            args.app,
            saturation["cooldown_seconds"] if saturation["reset_between_levels"] else 0,
        )

        saturation_plan = {
            "mode": "stepwise",
            "created_at": datetime.now().isoformat(),
            "app": args.app,
            "workload": args.workload,
            "locustfile": args.locustfile,
            "prom_url": args.prom_url,
            "energy_source": args.energy_source,
            "levels": saturation["levels"],
            "dwell_seconds": saturation["dwell_seconds"],
            "spawn_rate": saturation["spawn_rate"],
            "ramp_exclusion_seconds": saturation["ramp_exclusion_seconds"],
            "reset_between_levels": saturation["reset_between_levels"],
            "cooldown_seconds": saturation["cooldown_seconds"],
            "runs": [],
        }
        saturation_plan_path = Path(batch_dir) / "saturation_plan.json"
        write_json(saturation_plan_path, saturation_plan)

        total_levels = len(saturation["levels"])
        for index, level in enumerate(saturation["levels"], start=1):
            print(f"=== Saturation level {index}/{total_levels}: users={level} ===")
            iteration_dir = create_saturation_iteration_directory(batch_dir, level)
            run_experiment_cmd = [
                sys.executable,
                str(SCRIPT_DIR / "run_experiment.py"),
                "--app",
                args.app,
                "--workload",
                args.workload,
                "--locustfile",
                args.locustfile,
                "--run-dir",
                str(iteration_dir),
                "--users",
                str(level),
                "--spawn-rate",
                str(saturation["spawn_rate"]),
                "--duration",
                str(saturation["dwell_seconds"]),
                "--ramp-exclusion-seconds",
                str(saturation["ramp_exclusion_seconds"]),
            ]
            completed = run_step(run_experiment_cmd, "Running saturation level")
            run_dir = extract_run_dir(completed.stderr or completed.stdout)
            created_runs.append(run_dir)

            query_cmd = [
                sys.executable,
                str(SCRIPT_DIR / "query_prometheus.py"),
                "--run-dir",
                str(run_dir),
                "--prom-url",
                args.prom_url,
                "--energy-source",
                args.energy_source,
            ]
            run_step(query_cmd, "Querying Prometheus")

            summarise_cmd = [
                sys.executable,
                str(SCRIPT_DIR / "summarise_run.py"),
                "--run-dir",
                str(run_dir),
                "--ramp-exclusion-seconds",
                str(saturation["ramp_exclusion_seconds"]),
            ]
            run_step(summarise_cmd, "Summarising run")

            saturation_plan["runs"].append(
                {
                    "user_level": level,
                    "run_dir": str(run_dir),
                }
            )
            write_json(saturation_plan_path, saturation_plan)

            if saturation["reset_between_levels"]:
                cleanup_sleep = (
                    saturation["cooldown_seconds"] if index < total_levels else 0
                )
                cleanup_sut(args.app, cleanup_sleep)

        if not saturation["reset_between_levels"]:
            cleanup_sut(args.app, 0)

        write_calibration_summary(batch_dir, saturation_plan)

        saturation_summary_path = Path(batch_dir) / "saturation_summary.json"
        analyse_cmd = [
            sys.executable,
            str(SATURATION_ANALYSE_SCRIPT),
            "--calibration-csv",
            str(Path(batch_dir) / "calibration_summary.csv"),
            "--output",
            str(saturation_summary_path),
            "--plateau-threshold",
            str(args.sat_plateau_threshold),
            "--latency-jump-threshold",
            str(args.sat_latency_jump_threshold),
            "--error-rate-threshold",
            str(args.sat_error_rate_threshold),
            "--cpu-threshold",
            str(args.sat_cpu_threshold),
        ]
        run_step(analyse_cmd, "Analysing saturation")
    else:
        cleanup_sut(args.app, args.cooldown_seconds)

        for index in range(1, args.count + 1):
            print(f"=== Experiment {index}/{args.count} ===")
            iteration_dir = create_iteration_directory(batch_dir)
            run_experiment_cmd = [
                sys.executable,
                str(SCRIPT_DIR / "run_experiment.py"),
                "--app",
                args.app,
                "--workload",
                args.workload,
                "--locustfile",
                args.locustfile,
                "--run-dir",
                str(iteration_dir),
            ]
            completed = run_step(run_experiment_cmd, "Running experiment")
            run_dir = extract_run_dir(completed.stderr or completed.stdout)
            created_runs.append(run_dir)

            query_cmd = [
                sys.executable,
                str(SCRIPT_DIR / "query_prometheus.py"),
                "--run-dir",
                str(run_dir),
                "--prom-url",
                args.prom_url,
                "--energy-source",
                args.energy_source,
            ]
            run_step(query_cmd, "Querying Prometheus")

            summarise_cmd = [
                sys.executable,
                str(SCRIPT_DIR / "summarise_run.py"),
                "--run-dir",
                str(run_dir),
            ]
            run_step(summarise_cmd, "Summarising run")

            cleanup_sleep = args.cooldown_seconds if index < args.count else 0
            cleanup_sut(args.app, cleanup_sleep)

    visualise_cmd = [
        sys.executable,
        str(SCRIPT_DIR / "visualise_runs.py"),
        "--runs-dir",
        str(batch_dir),
        "--output",
        str(output_path),
    ]
    run_step(visualise_cmd, "Generating comparison dashboard")

    print("Completed runs:")
    for run_dir in created_runs:
        print(f"- {run_dir}")
    print(f"Batch directory: {batch_dir}")
    print(f"Dashboard written to {output_path}")


if __name__ == "__main__":
    main()