#!/usr/bin/env python3
"""Run a batch of experiments, collect metrics, and build a comparison dashboard."""

import argparse
from datetime import datetime
import re
import subprocess
import sys
from pathlib import Path

import yaml


RUN_DIR_PATTERN = re.compile(r"Results saved to:\s*(runs/\S+)")
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
CLEANUP_SCRIPT = SCRIPT_DIR / "cleanup_sut.py"


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


def extract_run_dir(output_text):
    """Extract the run directory path from run_experiment output."""
    match = RUN_DIR_PATTERN.search(output_text)
    if not match:
        raise ValueError("Could not determine the created run directory from output")
    return Path(match.group(1))


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