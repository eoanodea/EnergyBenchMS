#!/usr/bin/env python3
"""
Experiment controller for energy analysis.

Deploys an application to Kubernetes, waits for readiness, then runs a Locust workload.
"""

import argparse
import csv
import json
import logging
import subprocess
import sys
import time
import yaml
from datetime import datetime, timedelta
from pathlib import Path

from app_config import (
    infer_sut_name,
    load_and_filter_manifests,
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

MANAGE_SUT_SCRIPT = Path(__file__).resolve().parent / "manage_sut.py"
POWER_METER_SAMPLER_SCRIPT = Path(__file__).resolve().parent / "sample_power_meter.py"
DEFAULT_MAX_ERROR_RATE = 0.01


def load_workload(workload_path):
    """Load workload configuration from YAML file."""
    logger.info(f"Loading workload from {workload_path}")
    with open(workload_path, 'r') as f:
        workload = yaml.safe_load(f)
    logger.info(f"Workload loaded: {workload}")
    return workload


def run_command(cmd, check=True):
    """Run a shell command and return the result."""
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=check)
    return result


def resolve_locustfile(locust_file, app_path):
    """Resolve locust file path with clear fallback rules."""
    raw = Path(locust_file)
    if raw.is_absolute():
        return raw

    cwd_candidate = Path.cwd() / raw
    if cwd_candidate.exists():
        return cwd_candidate

    app_candidate = Path(app_path) / raw
    if app_candidate.exists():
        return app_candidate

    return cwd_candidate


def append_deployment_overrides(command, args):
    """Append manifest and exclusion overrides to a subprocess command."""
    if args.manifest_path:
        command.extend(["--manifest-path", args.manifest_path])
    if args.namespace:
        command.extend(["--namespace", args.namespace])
    for pattern in args.exclude_resource_pattern:
        command.extend(["--exclude-resource-pattern", pattern])
    for kind in args.exclude_kind:
        command.extend(["--exclude-kind", kind])


def manage_sut_up(app, args):
    """Deploy the SUT manifests and wait for rollout readiness."""
    deploy_cmd = [
        sys.executable,
        str(MANAGE_SUT_SCRIPT),
        "up",
        "--app",
        app,
    ]
    append_deployment_overrides(deploy_cmd, args)
    run_command(deploy_cmd)


def describe_exclusions(manifests, filtered_manifests):
    """Log which resources are excluded after filtering."""
    excluded = len(manifests) - len(filtered_manifests)
    logger.info(
        "Manifest filtering selected %s resources and excluded %s resources",
        len(filtered_manifests),
        excluded,
    )


def wait_baseline(duration=20):
    """Wait for baseline period before starting workload."""
    logger.info(f"Waiting {duration} seconds for baseline period")
    time.sleep(duration)
    logger.info("Baseline period complete")


def run_locust(workload, locust_file_path, csv_prefix=None):
    """Run Locust with parameters from workload configuration."""
    logger.info("Starting Locust workload")
    locust_path = Path(locust_file_path)

    if not locust_path.exists():
        raise FileNotFoundError(
            f"Locust file not found: {locust_path}. "
            "Pass --locustfile with a valid .py file path."
        )
    
    # Extract parameters
    host = workload.get('target')
    users = workload.get('users')
    spawn_rate = workload.get('spawn_rate')
    duration = workload.get('duration')
    
    if not all([host, users, spawn_rate, duration]):
        raise ValueError(f"Missing required workload parameters. Workload: {workload}")
    
    cmd = [
        "locust",
        "-f", str(locust_path),
        "--host", host,
        "--users", str(users),
        "--spawn-rate", str(spawn_rate),
        "--run-time", f"{duration}s",
        "--headless"
    ]

    if csv_prefix:
        cmd.extend([
            "--csv", str(csv_prefix),
            "--csv-full-history",
        ])
    
    logger.info(f"Using locust file: {locust_path}")
    logger.info(f"Locust command: {' '.join(cmd)}")
    run_command(cmd)
    logger.info("Locust workload completed")


def _read_numeric_field(row, field_names):
    """Return the first numeric field value found in a CSV row."""
    for field_name in field_names:
        raw_value = row.get(field_name)
        if raw_value in (None, ""):
            continue
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            continue
    return None


def read_locust_error_rate(stats_csv_path):
    """Read aggregated Locust error rate from locust_stats.csv if available."""
    stats_path = Path(stats_csv_path)
    if not stats_path.exists():
        return None

    with stats_path.open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            row_type = (row.get("Type") or "").strip().lower()
            row_name = (row.get("Name") or "").strip().lower()
            if row_type != "aggregated" and row_name != "aggregated":
                continue

            request_count = _read_numeric_field(
                row,
                ["Request Count", "# requests", "# reqs", "Num Requests"],
            )
            failure_count = _read_numeric_field(
                row,
                ["Failure Count", "# failures", "# fails", "Num Failures"],
            )
            if request_count is None or request_count <= 0:
                return None
            if failure_count is None:
                failure_count = 0.0
            return max(0.0, min(1.0, failure_count / request_count))

    return None


def apply_workload_overrides(workload, users=None, spawn_rate=None, duration=None):
    """Apply optional CLI overrides to workload fields."""
    merged = dict(workload)
    if users is not None:
        merged["users"] = users
    if spawn_rate is not None:
        merged["spawn_rate"] = spawn_rate
    if duration is not None:
        merged["duration"] = duration
    return merged


def validate_workload(workload):
    """Validate required workload fields and ranges."""
    required = ["target", "users", "spawn_rate", "duration"]
    missing = [field for field in required if workload.get(field) in (None, "")]
    if missing:
        raise ValueError(f"Missing required workload parameters: {', '.join(missing)}")

    for numeric_field in ["users", "spawn_rate", "duration"]:
        try:
            value = float(workload[numeric_field])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid {numeric_field}: {workload[numeric_field]!r}") from exc
        if value <= 0:
            raise ValueError(f"{numeric_field} must be greater than 0")


def normalize_ramp_exclusion_seconds(cli_value, workload):
    """Resolve ramp exclusion seconds from CLI or workload with defaults."""
    if cli_value is not None:
        value = cli_value
    else:
        value = workload.get("ramp_exclusion_seconds", 0)

    try:
        ramp_exclusion = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid ramp exclusion seconds: {value!r}") from exc

    if ramp_exclusion < 0:
        raise ValueError("ramp exclusion seconds must be at least 0")

    return ramp_exclusion


def normalize_baseline_seconds(cli_value):
    """Resolve the baseline capture duration from CLI input."""
    try:
        baseline_seconds = int(cli_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid baseline seconds: {cli_value!r}") from exc

    if baseline_seconds < 0:
        raise ValueError("baseline seconds must be at least 0")

    return baseline_seconds


def normalize_max_error_rate(cli_value, workload):
    """Resolve maximum allowed error rate from CLI/workload/default values."""
    candidates = [
        cli_value,
        workload.get("max_error_rate"),
        workload.get("error_rate_threshold"),
        workload.get("allowed_error_rate"),
    ]
    selected = next((value for value in candidates if value is not None), DEFAULT_MAX_ERROR_RATE)

    try:
        max_error_rate = float(selected)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid max error rate: {selected!r}") from exc

    if max_error_rate < 0 or max_error_rate > 1:
        raise ValueError("max error rate must be between 0 and 1")

    return max_error_rate


def create_runs_directory():
    """Create timestamped runs directory and return its path."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    runs_dir = Path("runs") / timestamp
    runs_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created runs directory: {runs_dir}")
    return runs_dir


def prepare_run_directory(run_dir=None):
    """Create a run directory, using a caller-supplied path when provided."""
    if run_dir:
        run_path = Path(run_dir)
        run_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Using provided run directory: {run_path}")
        return run_path

    return create_runs_directory()


def capture_k8s_pod_snapshot(run_dir):
    """Capture a best-effort Kubernetes pod snapshot for attribution enrichment."""
    snapshot_path = Path(run_dir) / "k8s_pod_snapshot.json"
    cmd = ["kubectl", "get", "pods", "-A", "-o", "json"]
    try:
        logger.info("Capturing Kubernetes pod snapshot")
        completed = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        # Validate JSON before writing so attribution can rely on a consistent payload.
        payload = json.loads(completed.stdout)
        with snapshot_path.open("w", encoding="utf-8") as outfile:
            json.dump(payload, outfile, indent=2)
        return {
            "captured": True,
            "path": str(snapshot_path),
            "error": None,
        }
    except (subprocess.CalledProcessError, OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not capture Kubernetes pod snapshot: %s", exc)
        return {
            "captured": False,
            "path": str(snapshot_path),
            "error": str(exc),
        }


def save_metadata(
    runs_dir,
    app_path,
    workload_path,
    workload,
    timestamps,
    ramp_exclusion_seconds,
    baseline_seconds,
    locust_artifacts,
    power_meter=None,
    workload_label=None,
    experiment_status="success",
    locust_exit_code=None,
    locust_error=None,
    locust_observed_error_rate=None,
    locust_max_error_rate=None,
    locust_error_rate_threshold_exceeded=None,
):
    """Save experiment metadata to JSON file."""
    metadata = {
        "app_path": str(Path(app_path).absolute()),
        "workload_path": str(Path(workload_path).absolute()),
        "workload_parameters": workload,
        "workload_label": workload_label,
        "experiment_status": experiment_status,
        "locust_exit_code": locust_exit_code,
        "locust_error": locust_error,
        "locust_observed_error_rate": locust_observed_error_rate,
        "locust_max_error_rate": locust_max_error_rate,
        "locust_error_rate_threshold_exceeded": locust_error_rate_threshold_exceeded,
        "ramp_exclusion_seconds": ramp_exclusion_seconds,
        "baseline_seconds": baseline_seconds,
        "locust_artifacts": locust_artifacts,
        "timestamps": {
            "experiment_start": timestamps['experiment_start'],
            "workload_start": timestamps['workload_start'],
            "workload_effective_start": timestamps['workload_effective_start'],
            "workload_end": timestamps['workload_end']
        }
    }
    if power_meter is not None:
        metadata["power_meter"] = power_meter
    
    metadata_file = runs_dir / "metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    logger.info(f"Metadata saved to {metadata_file}")


def main():
    """Main experiment controller logic."""
    parser = argparse.ArgumentParser(
        description="Run an energy analysis experiment"
    )
    parser.add_argument(
        "--app",
        required=True,
        help="Path to application directory (e.g., apps/simple-web)"
    )
    parser.add_argument(
        "--workload",
        required=True,
        help="Path to workload YAML file (e.g., workloads/simple-web.yaml)"
    )
    parser.add_argument(
        "--locustfile",
        default="locustfile.py",
        help="Path to Locust file (default: locustfile.py)"
    )
    parser.add_argument(
        "--no-results",
        action="store_true",
        help="Run the experiment without creating a results directory or metadata"
    )
    parser.add_argument(
        "--run-dir",
        help="Optional output directory for the run results"
    )
    parser.add_argument(
        "--workload-label",
        help="Optional workload label/name for grouped runs"
    )
    parser.add_argument(
        "--users",
        type=int,
        help="Override users from workload YAML"
    )
    parser.add_argument(
        "--spawn-rate",
        type=float,
        help="Override spawn_rate from workload YAML"
    )
    parser.add_argument(
        "--duration",
        type=int,
        help="Override duration (seconds) from workload YAML"
    )
    parser.add_argument(
        "--ramp-exclusion-seconds",
        type=int,
        help="Seconds at workload start to exclude from downstream summaries"
    )
    parser.add_argument(
        "--manifest-path",
        help=(
            "Optional manifest source path relative to --app (file or directory). "
            "Defaults to pipeline_app.yaml manifest_path or app root."
        )
    )
    parser.add_argument(
        "--namespace",
        help="Optional namespace override for rollout checks"
    )
    parser.add_argument(
        "--exclude-resource-pattern",
        action="append",
        default=[],
        help=(
            "Regex pattern for resources to exclude from apply/delete. "
            "Can be repeated. Matches kind/name and namespace/kind/name identities."
        )
    )
    parser.add_argument(
        "--exclude-kind",
        action="append",
        default=[],
        help="Resource kind to exclude from apply/delete (can be repeated)"
    )
    parser.add_argument(
        "--baseline-seconds",
        type=int,
        default=20,
        help="Seconds to record baseline samples before the workload starts (default: 20)",
    )
    parser.add_argument(
        "--max-error-rate",
        type=float,
        help=(
            "Maximum allowed Locust request failure ratio (0-1). "
            "Defaults to workload max_error_rate/error_rate_threshold/allowed_error_rate, or 0.01."
        ),
    )
    parser.add_argument(
        "--power-meter-url",
        help="Optional physical power meter API URL for CSV sampling"
    )
    parser.add_argument(
        "--power-meter-interval-seconds",
        type=float,
        default=5.0,
        help="Interval between physical power meter samples in seconds (default: 5)"
    )
    parser.add_argument(
        "--power-meter-request-timeout-seconds",
        type=float,
        default=5.0,
        help="HTTP timeout for each physical power meter sample request (default: 5)"
    )
    
    args = parser.parse_args()
    logger.info("Invocation argv: %s", sys.argv)
    
    try:
        # Record experiment start
        timestamps = {
            'experiment_start': datetime.now().isoformat()
        }
        logger.info("=" * 60)
        logger.info("Starting energy analysis experiment")
        logger.info(f"App: {args.app}")
        logger.info(f"Workload: {args.workload}")
        logger.info(f"Locust file argument: {args.locustfile}")
        logger.info(f"Current working directory: {Path.cwd()}")
        logger.info("=" * 60)
        
        # Load workload configuration
        workload = load_workload(args.workload)
        workload = apply_workload_overrides(
            workload,
            users=args.users,
            spawn_rate=args.spawn_rate,
            duration=args.duration,
        )
        validate_workload(workload)
        baseline_seconds = normalize_baseline_seconds(args.baseline_seconds)
        max_error_rate = normalize_max_error_rate(args.max_error_rate, workload)
        ramp_exclusion_seconds = normalize_ramp_exclusion_seconds(
            args.ramp_exclusion_seconds,
            workload,
        )

        # Resolve locust file path from CLI input.
        resolved_locustfile = resolve_locustfile(args.locustfile, args.app)
        logger.info(f"Resolved locust file path: {resolved_locustfile}")
        
        (
            manifest_source,
            namespace_override,
            manifests,
            filtered_manifests,
            deployment_targets,
            exclusion_patterns,
            excluded_kinds,
        ) = load_and_filter_manifests(
            args.app,
            namespace_override=args.namespace,
            manifest_path_override=args.manifest_path,
            exclude_resource_patterns=args.exclude_resource_pattern,
            exclude_kinds=args.exclude_kind,
        )
        describe_exclusions(manifests, filtered_manifests)
        if not deployment_targets:
            logger.warning("No deployment resources found after filtering")
        else:
            logger.info(
                "Deployment targets: %s",
                ", ".join(
                    [
                        f"{item['namespace']}/{item['name']}"
                        if item.get("namespace")
                        else item["name"]
                        for item in deployment_targets
                    ]
                ),
            )

        # Deploy application through the shared lifecycle helper.
        manage_sut_up(args.app, args)
        
        runs_dir = None
        locust_csv_prefix = None
        locust_artifacts = {}
        power_meter_artifacts = None
        power_meter_process = None
        if not args.no_results:
            runs_dir = prepare_run_directory(args.run_dir)
            locust_csv_prefix = runs_dir / "locust"
            locust_artifacts = {
                "stats_csv": str(runs_dir / "locust_stats.csv"),
                "stats_history_csv": str(runs_dir / "locust_stats_history.csv"),
                "failures_csv": str(runs_dir / "locust_failures.csv"),
                "exceptions_csv": str(runs_dir / "locust_exceptions.csv"),
            }
            if args.power_meter_url:
                power_meter_artifacts = {
                    "enabled": True,
                    "url": args.power_meter_url,
                    "interval_seconds": args.power_meter_interval_seconds,
                    "request_timeout_seconds": args.power_meter_request_timeout_seconds,
                    "energy_unit": "Wh",
                    "samples_csv": str(runs_dir / "physical_power_meter.csv"),
                    "baseline_seconds": baseline_seconds,
                }
                power_meter_cmd = [
                    sys.executable,
                    str(POWER_METER_SAMPLER_SCRIPT),
                    "--url",
                    args.power_meter_url,
                    "--output",
                    power_meter_artifacts["samples_csv"],
                    "--interval-seconds",
                    str(args.power_meter_interval_seconds),
                    "--request-timeout-seconds",
                    str(args.power_meter_request_timeout_seconds),
                    "--baseline-seconds",
                    str(baseline_seconds),
                ]
                logger.info("Starting physical power meter sampler")
                power_meter_process = subprocess.Popen(
                    power_meter_cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                power_meter_artifacts["pid"] = power_meter_process.pid
        elif args.power_meter_url:
            logger.warning(
                "--power-meter-url was provided but no-results is enabled; skipping power meter sampling"
            )

        # Allow the baseline capture window to elapse before the workload starts.
        wait_baseline(baseline_seconds)

        # Record workload start
        workload_start_dt = datetime.now()
        timestamps['workload_start'] = workload_start_dt.isoformat()
        effective_start_dt = workload_start_dt + timedelta(seconds=ramp_exclusion_seconds)
        timestamps['workload_effective_start'] = effective_start_dt.isoformat()

        # Run Locust workload
        locust_error = None
        locust_exit_code = None
        locust_observed_error_rate = None
        locust_error_rate_threshold_exceeded = None
        experiment_failed = False
        try:
            run_locust(workload, resolved_locustfile, csv_prefix=locust_csv_prefix)
        except subprocess.CalledProcessError as exc:
            locust_error = str(exc)
            locust_exit_code = exc.returncode
            logger.error(f"Locust workload failed: {exc}", exc_info=True)

        if power_meter_process is not None:
            logger.info("Stopping physical power meter sampler")
            power_meter_process.terminate()
            try:
                power_meter_exit_code = power_meter_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.warning("Power meter sampler did not exit cleanly; killing it")
                power_meter_process.kill()
                power_meter_exit_code = power_meter_process.wait(timeout=10)
            if power_meter_artifacts is not None:
                power_meter_artifacts["exit_code"] = power_meter_exit_code
                samples_csv = Path(power_meter_artifacts["samples_csv"])
                if samples_csv.exists():
                    with samples_csv.open("r", encoding="utf-8") as infile:
                        power_meter_artifacts["sample_count"] = max(0, sum(1 for _ in infile) - 1)
            if power_meter_exit_code not in (0, None):
                logger.warning(
                    "Physical power meter sampler exited with code %s",
                    power_meter_exit_code,
                )
        
        # Record workload end
        timestamps['workload_end'] = datetime.now().isoformat()

        if not args.no_results and locust_artifacts.get("stats_csv"):
            locust_observed_error_rate = read_locust_error_rate(locust_artifacts["stats_csv"])
            if locust_observed_error_rate is not None:
                locust_error_rate_threshold_exceeded = locust_observed_error_rate > max_error_rate
                logger.info(
                    "Observed Locust error rate: %.6f (threshold: %.6f)",
                    locust_observed_error_rate,
                    max_error_rate,
                )
            else:
                logger.warning(
                    "Could not determine Locust error rate from %s",
                    locust_artifacts["stats_csv"],
                )

        if locust_error_rate_threshold_exceeded is True:
            experiment_failed = True
            logger.error(
                "Locust error rate exceeded threshold (observed=%.6f, threshold=%.6f)",
                locust_observed_error_rate,
                max_error_rate,
            )
        elif locust_error and args.no_results:
            experiment_failed = True
            logger.warning("Locust exited non-zero during no-results run")
        elif locust_error and not args.no_results and locust_observed_error_rate is None:
            experiment_failed = True
            logger.error(
                "Locust exited non-zero and error rate could not be evaluated; marking run as failed"
            )
        elif locust_error:
            logger.warning(
                "Locust exited non-zero but error rate is within threshold; preserving run artifacts"
            )

        if args.no_results:
            if experiment_failed:
                logger.info("=" * 60)
                logger.info("Warmup completed with errors, continuing pipeline")
                logger.info("No results directory created")
                logger.info("=" * 60)
            else:
                logger.info("=" * 60)
                logger.info("Warmup completed successfully")
                logger.info("No results directory created")
                logger.info("=" * 60)
        else:
            save_metadata(
                runs_dir,
                args.app,
                args.workload,
                workload,
                timestamps,
                ramp_exclusion_seconds,
                baseline_seconds,
                locust_artifacts,
                power_meter=power_meter_artifacts,
                workload_label=args.workload_label,
                experiment_status="failed" if experiment_failed else "success",
                locust_exit_code=locust_exit_code,
                locust_error=locust_error,
                locust_observed_error_rate=locust_observed_error_rate,
                locust_max_error_rate=max_error_rate,
                locust_error_rate_threshold_exceeded=locust_error_rate_threshold_exceeded,
            )

            metadata_file = runs_dir / "metadata.json"
            with metadata_file.open("r", encoding="utf-8") as infile:
                metadata = json.load(infile)

            # Store deployment names as the SUT container allowlist.
            # These names are what we expect to match Kepler's container_name labels.
            sut_containers = [
                item["name"]
                for item in deployment_targets
                if isinstance(item, dict) and item.get("name")
            ]
            if not sut_containers:
                sut_containers = [infer_sut_name(args.app, filtered_manifests)]

            metadata["sut_containers"] = sut_containers
            metadata["sut_container_source"] = "deployment_targets"

            metadata["deployment"] = {
                "manifest_source": str(manifest_source),
                "namespace_override": namespace_override,
                "excluded_kinds": sorted(excluded_kinds),
                "excluded_resource_patterns": [pattern.pattern for pattern in exclusion_patterns],
                "sut_name": infer_sut_name(args.app, filtered_manifests),
                "deployments": deployment_targets,
            }

            # Capture Kubernetes pod snapshot before workload execution
            metadata["k8s_pod_snapshot"] = capture_k8s_pod_snapshot(runs_dir)

            metadata["k8s_pod_snapshot"] = capture_k8s_pod_snapshot(runs_dir)
            with metadata_file.open("w", encoding="utf-8") as outfile:
                json.dump(metadata, outfile, indent=2)
            
            logger.info("=" * 60)
            if experiment_failed:
                logger.info("Experiment completed with errors")
            else:
                logger.info("Experiment completed successfully")
            logger.info(f"Results saved to: {runs_dir}")
            logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Experiment failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
