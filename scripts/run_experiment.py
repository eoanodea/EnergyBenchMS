#!/usr/bin/env python3
"""
Experiment controller for energy analysis.

Deploys an application to Kubernetes, waits for readiness, then runs a Locust workload.
"""

import argparse
import json
import logging
import subprocess
import sys
import time
import yaml
from datetime import datetime, timedelta
from pathlib import Path


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_workload(workload_path):
    """Load workload configuration from YAML file."""
    logger.info(f"Loading workload from {workload_path}")
    with open(workload_path, 'r') as f:
        workload = yaml.safe_load(f)
    logger.info(f"Workload loaded: {workload}")
    return workload


def get_deployment_name(app_path):
    """Extract deployment name from Kubernetes manifests in app directory."""
    deployment_file = Path(app_path) / "deployment.yaml"
    
    if not deployment_file.exists():
        raise FileNotFoundError(f"No deployment.yaml found in {app_path}")
    
    with open(deployment_file, 'r') as f:
        deployment = yaml.safe_load(f)
    
    name = deployment.get('metadata', {}).get('name')
    if not name:
        raise ValueError("Could not extract deployment name from deployment.yaml")
    
    logger.info(f"Found deployment name: {name}")
    return name


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


def deploy_app(app_path):
    """Deploy application using kubectl apply."""
    logger.info(f"Deploying application from {app_path}")
    app_path = Path(app_path)
    
    if not app_path.exists():
        raise FileNotFoundError(f"App path does not exist: {app_path}")
    
    cmd = ["kubectl", "apply", "-f", str(app_path)]
    run_command(cmd)
    logger.info("Application deployed")


def wait_for_deployment(deployment_name, timeout=300):
    """Wait for deployment to be ready."""
    logger.info(f"Waiting for deployment '{deployment_name}' to be ready")
    cmd = [
        "kubectl",
        "rollout",
        "status",
        f"deployment/{deployment_name}",
        f"--timeout={timeout}s"
    ]
    run_command(cmd)
    logger.info(f"Deployment '{deployment_name}' is ready")


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


def save_metadata(
    runs_dir,
    app_path,
    workload_path,
    workload,
    timestamps,
    ramp_exclusion_seconds,
    locust_artifacts,
    workload_label=None,
):
    """Save experiment metadata to JSON file."""
    metadata = {
        "app_path": str(Path(app_path).absolute()),
        "workload_path": str(Path(workload_path).absolute()),
        "workload_parameters": workload,
        "workload_label": workload_label,
        "ramp_exclusion_seconds": ramp_exclusion_seconds,
        "locust_artifacts": locust_artifacts,
        "timestamps": {
            "experiment_start": timestamps['experiment_start'],
            "workload_start": timestamps['workload_start'],
            "workload_effective_start": timestamps['workload_effective_start'],
            "workload_end": timestamps['workload_end']
        }
    }
    
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
    
    args = parser.parse_args()
    
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
        ramp_exclusion_seconds = normalize_ramp_exclusion_seconds(
            args.ramp_exclusion_seconds,
            workload,
        )

        # Resolve locust file path from CLI input.
        resolved_locustfile = resolve_locustfile(args.locustfile, args.app)
        logger.info(f"Resolved locust file path: {resolved_locustfile}")
        
        # Get deployment name from app manifests
        deployment_name = get_deployment_name(args.app)
        
        # Deploy application
        deploy_app(args.app)
        
        # Wait for deployment to be ready
        wait_for_deployment(deployment_name)
        
        # Wait baseline period
        wait_baseline(20)
        
        runs_dir = None
        locust_csv_prefix = None
        locust_artifacts = {}
        if not args.no_results:
            runs_dir = prepare_run_directory(args.run_dir)
            locust_csv_prefix = runs_dir / "locust"
            locust_artifacts = {
                "stats_csv": str(runs_dir / "locust_stats.csv"),
                "stats_history_csv": str(runs_dir / "locust_stats_history.csv"),
                "failures_csv": str(runs_dir / "locust_failures.csv"),
                "exceptions_csv": str(runs_dir / "locust_exceptions.csv"),
            }

        # Record workload start
        workload_start_dt = datetime.now()
        timestamps['workload_start'] = workload_start_dt.isoformat()
        effective_start_dt = workload_start_dt + timedelta(seconds=ramp_exclusion_seconds)
        timestamps['workload_effective_start'] = effective_start_dt.isoformat()
        
        # Run Locust workload
        run_locust(workload, resolved_locustfile, csv_prefix=locust_csv_prefix)
        
        # Record workload end
        timestamps['workload_end'] = datetime.now().isoformat()

        if args.no_results:
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
                locust_artifacts,
                workload_label=args.workload_label,
            )
            
            logger.info("=" * 60)
            logger.info("Experiment completed successfully")
            logger.info(f"Results saved to: {runs_dir}")
            logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Experiment failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
