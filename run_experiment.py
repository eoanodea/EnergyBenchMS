#!/usr/bin/env python3
"""
Experiment controller for energy analysis.

Deploys an application to Kubernetes, waits for readiness, then runs a Locust workload.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import yaml
from datetime import datetime
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


def run_locust(workload, locust_file="locustfile.py"):
    """Run Locust with parameters from workload configuration."""
    logger.info("Starting Locust workload")
    locust_path = Path(locust_file)

    if not locust_path.exists():
        raise FileNotFoundError(
            f"Locust file not found: {locust_file}. "
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
    
    logger.info(f"Using locust file: {locust_path}")
    logger.info(f"Locust command: {' '.join(cmd)}")
    run_command(cmd)
    logger.info("Locust workload completed")


def create_runs_directory():
    """Create timestamped runs directory and return its path."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    runs_dir = Path("runs") / timestamp
    runs_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created runs directory: {runs_dir}")
    return runs_dir


def save_metadata(runs_dir, app_path, workload_path, workload, timestamps):
    """Save experiment metadata to JSON file."""
    metadata = {
        "app_path": str(Path(app_path).absolute()),
        "workload_path": str(Path(workload_path).absolute()),
        "workload_parameters": workload,
        "timestamps": {
            "experiment_start": timestamps['experiment_start'],
            "workload_start": timestamps['workload_start'],
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
        logger.info("=" * 60)
        
        # Load workload configuration
        workload = load_workload(args.workload)
        
        # Get deployment name from app manifests
        deployment_name = get_deployment_name(args.app)
        
        # Deploy application
        deploy_app(args.app)
        
        # Wait for deployment to be ready
        wait_for_deployment(deployment_name)
        
        # Wait baseline period
        wait_baseline(20)
        
        # Record workload start
        timestamps['workload_start'] = datetime.now().isoformat()
        
        # Run Locust workload
        run_locust(workload, args.locustfile)
        
        # Record workload end
        timestamps['workload_end'] = datetime.now().isoformat()
        
        # Create runs directory and save metadata
        runs_dir = create_runs_directory()
        save_metadata(runs_dir, args.app, args.workload, workload, timestamps)
        
        logger.info("=" * 60)
        logger.info("Experiment completed successfully")
        logger.info(f"Results saved to: {runs_dir}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Experiment failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
