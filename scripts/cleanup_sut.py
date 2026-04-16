#!/usr/bin/env python3
"""Delete the SUT manifests and wait until application pods terminate."""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import yaml


def run_command(command):
    """Run a kubectl command and return its stdout."""
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    if completed.stdout:
        return completed.stdout.strip()
    return ""


def load_deployment_manifest(app_path):
    """Load the first deployment manifest from the app directory."""
    app_path = Path(app_path)
    manifest_paths = sorted(
        [path for path in app_path.iterdir() if path.suffix in {".yaml", ".yml"}]
    )

    for manifest_path in manifest_paths:
        with manifest_path.open("r", encoding="utf-8") as infile:
            manifest = yaml.safe_load(infile)
        if isinstance(manifest, dict) and manifest.get("kind") == "Deployment":
            return manifest

    raise FileNotFoundError(f"No Deployment manifest found in {app_path}")


def build_label_selector(manifest):
    """Build a kubectl label selector from the deployment spec."""
    selector = manifest.get("spec", {}).get("selector", {}).get("matchLabels", {})
    if not selector:
        raise ValueError("Deployment manifest does not define selector.matchLabels")

    return ",".join(f"{key}={value}" for key, value in sorted(selector.items()))


def get_namespace(manifest, explicit_namespace=None):
    """Resolve the namespace to target without crossing namespace boundaries."""
    if explicit_namespace:
        return explicit_namespace

    namespace = manifest.get("metadata", {}).get("namespace")
    return namespace or None


def build_namespace_args(namespace):
    """Return kubectl namespace args when a namespace is known."""
    if namespace:
        return ["-n", namespace]
    return []


def get_running_pods(selector, namespace):
    """Return the running application pods matching the deployment selector."""
    command = [
        "kubectl",
        "get",
        "pods",
        *build_namespace_args(namespace),
        "-l",
        selector,
        "-o",
        "json",
    ]
    payload = run_command(command)
    data = json.loads(payload) if payload else {"items": []}
    return [item["metadata"]["name"] for item in data.get("items", [])]


def delete_manifests(app_path, namespace):
    """Delete only the SUT manifests from the application directory."""
    command = ["kubectl", "delete", "-f", str(app_path), "--ignore-not-found"]
    if namespace:
        command.extend(["-n", namespace])
    run_command(command)


def wait_for_pod_termination(selector, namespace, timeout_seconds, poll_interval_seconds):
    """Wait until no pods remain for the SUT label selector."""
    deadline = time.time() + timeout_seconds
    while True:
        remaining = get_running_pods(selector, namespace)
        if not remaining:
            return

        if time.time() >= deadline:
            raise TimeoutError(
                f"Timed out waiting for pods to terminate: {', '.join(remaining)}"
            )

        print(f"Waiting for pods to terminate: {', '.join(remaining)}")
        time.sleep(poll_interval_seconds)


def main():
    parser = argparse.ArgumentParser(
        description="Delete the SUT manifests, wait for pods to terminate, then sleep"
    )
    parser.add_argument(
        "--app",
        required=True,
        help="Path to the application directory containing the SUT manifests",
    )
    parser.add_argument(
        "--namespace",
        help="Optional namespace override; otherwise uses the manifest namespace or current context",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=300,
        help="How long to wait for SUT pods to terminate (default: 300)",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=2,
        help="How often to poll for pod termination (default: 2)",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=int,
        default=0,
        help="How long to sleep after the SUT has terminated (default: 0)",
    )

    args = parser.parse_args()

    if args.timeout_seconds < 0:
        raise SystemExit("--timeout-seconds must be at least 0")
    if args.poll_interval_seconds <= 0:
        raise SystemExit("--poll-interval-seconds must be greater than 0")
    if args.sleep_seconds < 0:
        raise SystemExit("--sleep-seconds must be at least 0")

    app_path = Path(args.app)
    if not app_path.exists():
        raise FileNotFoundError(f"Application directory does not exist: {app_path}")

    deployment = load_deployment_manifest(app_path)
    selector = build_label_selector(deployment)
    namespace = get_namespace(deployment, args.namespace)

    existing_pods = get_running_pods(selector, namespace)
    if existing_pods:
        print(f"Found SUT pods to remove: {', '.join(existing_pods)}")
    else:
        print("No SUT pods are currently running")

    print(f"Deleting SUT manifests from {app_path}")
    delete_manifests(app_path, namespace)

    print("Waiting for SUT pods to terminate")
    wait_for_pod_termination(
        selector,
        namespace,
        args.timeout_seconds,
        args.poll_interval_seconds,
    )

    if args.sleep_seconds:
        print(f"Sleeping for {args.sleep_seconds} seconds after cleanup")
        time.sleep(args.sleep_seconds)

    print("Cleanup complete")


if __name__ == "__main__":
    main()