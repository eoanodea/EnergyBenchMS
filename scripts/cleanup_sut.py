#!/usr/bin/env python3
"""Delete the SUT manifests and wait until application pods terminate."""

import argparse
import subprocess
import time
from pathlib import Path

import yaml

from app_config import (
    extract_deployments,
    filter_manifest_documents,
    load_app_config,
    load_manifest_documents,
    resolve_excluded_kinds,
    resolve_exclusion_patterns,
    resolve_manifest_source,
    resolve_namespace,
)


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


def build_namespace_args(namespace):
    """Return kubectl namespace args when a namespace is known."""
    if namespace:
        return ["-n", namespace]
    return []


def deployment_exists(name, namespace):
    """Return true if a deployment currently exists."""
    command = [
        "kubectl",
        "get",
        "deployment",
        name,
        *build_namespace_args(namespace),
    ]
    completed = subprocess.run(command, capture_output=True, text=True)
    return completed.returncode == 0


def write_filtered_manifest_file(manifests):
    """Write selected manifests to a temporary file and return its path."""
    if not manifests:
        raise ValueError("No manifests left after filtering; nothing to clean up")

    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".yaml",
        delete=False,
    ) as outfile:
        yaml.safe_dump_all(manifests, outfile, explicit_start=True, sort_keys=False)
        return Path(outfile.name)


def delete_manifests(manifest_file):
    """Delete only the selected SUT manifests."""
    command = ["kubectl", "delete", "-f", str(manifest_file), "--ignore-not-found"]
    run_command(command)


def wait_for_deployment_termination(deployments, timeout_seconds, poll_interval_seconds):
    """Wait until target deployments no longer exist."""
    if not deployments:
        return

    deadline = time.time() + timeout_seconds
    while True:
        remaining = []
        for deployment in deployments:
            if deployment_exists(deployment["name"], deployment.get("namespace")):
                remaining.append(deployment)

        if not remaining:
            return

        if time.time() >= deadline:
            formatted = ", ".join(
                [
                    f"{item['namespace']}/{item['name']}"
                    if item.get("namespace")
                    else item["name"]
                    for item in remaining
                ]
            )
            raise TimeoutError(
                f"Timed out waiting for deployments to terminate: {formatted}"
            )

        formatted = ", ".join(
            [
                f"{item['namespace']}/{item['name']}"
                if item.get("namespace")
                else item["name"]
                for item in remaining
            ]
        )
        print(f"Waiting for deployments to terminate: {formatted}")
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
        "--manifest-path",
        help=(
            "Optional manifest source path relative to --app (file or directory). "
            "Defaults to pipeline_app.yaml manifest_path or app root."
        ),
    )
    parser.add_argument(
        "--exclude-resource-pattern",
        action="append",
        default=[],
        help=(
            "Regex pattern for resources to exclude from apply/delete. "
            "Can be repeated. Matches kind/name and namespace/kind/name identities."
        ),
    )
    parser.add_argument(
        "--exclude-kind",
        action="append",
        default=[],
        help="Resource kind to exclude from apply/delete (can be repeated)",
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

    app_config = load_app_config(app_path)
    manifest_source = resolve_manifest_source(
        app_path,
        app_config,
        manifest_path_override=args.manifest_path,
    )
    namespace_override = resolve_namespace(app_config, namespace_override=args.namespace)
    exclusion_patterns = resolve_exclusion_patterns(
        app_config,
        extra_patterns=args.exclude_resource_pattern,
    )
    excluded_kinds = resolve_excluded_kinds(
        app_config,
        extra_kinds=args.exclude_kind,
    )

    manifests = load_manifest_documents(manifest_source)
    if not manifests:
        raise ValueError(f"No manifest documents found in {manifest_source}")
    filtered_manifests = filter_manifest_documents(
        manifests,
        excluded_kinds,
        exclusion_patterns,
    )

    deployments = extract_deployments(
        filtered_manifests,
        default_namespace=namespace_override,
    )

    manifest_file = write_filtered_manifest_file(filtered_manifests)

    print(f"Deleting SUT manifests from {manifest_source}")
    delete_manifests(manifest_file)

    print("Waiting for SUT deployments to terminate")
    wait_for_deployment_termination(
        deployments,
        args.timeout_seconds,
        args.poll_interval_seconds,
    )

    if args.sleep_seconds:
        print(f"Sleeping for {args.sleep_seconds} seconds after cleanup")
        time.sleep(args.sleep_seconds)

    try:
        manifest_file.unlink(missing_ok=True)
    except OSError:
        pass

    print("Cleanup complete")


if __name__ == "__main__":
    main()