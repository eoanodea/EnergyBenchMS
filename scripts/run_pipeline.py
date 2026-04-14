#!/usr/bin/env python3
"""Run a batch of experiments, collect metrics, and build a comparison dashboard."""

import argparse
import re
import subprocess
import sys
from pathlib import Path


RUN_DIR_PATTERN = re.compile(r"Results saved to:\s*(runs/\S+)")
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent


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

    created_runs = []

    for index in range(1, args.count + 1):
        print(f"=== Experiment {index}/{args.count} ===")
        run_experiment_cmd = [
            sys.executable,
            str(SCRIPT_DIR / "run_experiment.py"),
            "--app",
            args.app,
            "--workload",
            args.workload,
            "--locustfile",
            args.locustfile,
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
        ]
        run_step(query_cmd, "Querying Prometheus")

        summarise_cmd = [
            sys.executable,
            str(SCRIPT_DIR / "summarise_run.py"),
            "--run-dir",
            str(run_dir),
        ]
        run_step(summarise_cmd, "Summarising run")

    visualise_cmd = [
        sys.executable,
        str(SCRIPT_DIR / "visualise_runs.py"),
        "--runs-dir",
        args.runs_dir,
        "--output",
        args.output,
    ]
    run_step(visualise_cmd, "Generating comparison dashboard")

    print("Completed runs:")
    for run_dir in created_runs:
        print(f"- {run_dir}")
    print(f"Dashboard written to {args.output}")


if __name__ == "__main__":
    main()