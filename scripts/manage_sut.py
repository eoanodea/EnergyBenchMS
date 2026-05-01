#!/usr/bin/env python3
"""Bring the SUT up or down using the same filtered manifests."""

import argparse
import subprocess
import time
from pathlib import Path

import yaml

from app_config import load_and_filter_manifests


def run_command(command):
    """Run a kubectl command and return its stdout."""
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        if completed.stdout:
            print(completed.stdout, end="")
        if completed.stderr:
            print(completed.stderr, end="", file=sys.stderr)
        completed.check_returncode()
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


def deploy_app(manifest_file, namespace=None):
    """Deploy application using kubectl apply with a manifest file."""
    command = ["kubectl", "apply", *build_namespace_args(namespace), "-f", str(manifest_file)]
    run_command(command)


def wait_for_deployments(deployments, timeout=300):
    """Wait for all deployments to be ready."""
    if not deployments:
        return

    for deployment in deployments:
        deployment_name = deployment["name"]
        namespace = deployment.get("namespace")
        command = [
            "kubectl",
            "rollout",
            "status",
            f"deployment/{deployment_name}",
        ]
        if namespace:
            command.extend(["-n", namespace])
        command.append(f"--timeout={timeout}s")
        run_command(command)


def delete_manifests(manifest_file, namespace=None):
    """Delete only the selected SUT manifests."""
    command = [
        "kubectl",
        "delete",
        *build_namespace_args(namespace),
        "-f",
        str(manifest_file),
        "--ignore-not-found",
    ]
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


def load_filtered_sut(
    app_path,
    namespace_override=None,
    manifest_path_override=None,
    exclude_resource_patterns=None,
    exclude_kinds=None,
):
    """Load and filter manifests for either deploy or cleanup."""
    (
        manifest_source,
        namespace,
        manifests,
        filtered_manifests,
        deployments,
        exclusion_patterns,
        excluded_kinds,
    ) = load_and_filter_manifests(
        app_path,
        namespace_override=namespace_override,
        manifest_path_override=manifest_path_override,
        exclude_resource_patterns=exclude_resource_patterns,
        exclude_kinds=exclude_kinds,
    )

    manifest_file = write_filtered_manifest_file(filtered_manifests)
    return manifest_source, namespace, deployments, manifest_file


def build_parser():
    """Build the lifecycle command parser."""
    parser = argparse.ArgumentParser(
        description="Deploy or remove the SUT using filtered manifests"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--app",
        required=True,
        help="Path to the application directory containing the SUT manifests",
    )
    common.add_argument(
        "--namespace",
        help="Optional namespace override; otherwise uses the manifest namespace or current context",
    )
    common.add_argument(
        "--manifest-path",
        help=(
            "Optional manifest source path relative to --app (file or directory). "
            "Defaults to pipeline_app.yaml manifest_path or app root."
        ),
    )
    common.add_argument(
        "--exclude-resource-pattern",
        action="append",
        default=[],
        help=(
            "Regex pattern for resources to exclude from apply/delete. "
            "Can be repeated. Matches kind/name and namespace/kind/name identities."
        ),
    )
    common.add_argument(
        "--exclude-kind",
        action="append",
        default=[],
        help="Resource kind to exclude from apply/delete (can be repeated)",
    )

    up = subparsers.add_parser("up", parents=[common], help="Deploy the SUT manifests")
    up.add_argument(
        "--timeout-seconds",
        type=int,
        default=300,
        help="How long to wait for the SUT deployments to become ready (default: 300)",
    )

    down = subparsers.add_parser("down", parents=[common], help="Delete the SUT manifests")
    down.add_argument(
        "--timeout-seconds",
        type=int,
        default=300,
        help="How long to wait for SUT pods to terminate (default: 300)",
    )
    down.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=2,
        help="How often to poll for pod termination (default: 2)",
    )
    down.add_argument(
        "--sleep-seconds",
        type=int,
        default=0,
        help="How long to sleep after the SUT has terminated (default: 0)",
    )

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.timeout_seconds < 0:
        raise SystemExit("--timeout-seconds must be at least 0")
    if getattr(args, "poll_interval_seconds", 1) <= 0:
        raise SystemExit("--poll-interval-seconds must be greater than 0")
    if getattr(args, "sleep_seconds", 0) < 0:
        raise SystemExit("--sleep-seconds must be at least 0")

    app_path = Path(args.app)
    if not app_path.exists():
        raise FileNotFoundError(f"Application directory does not exist: {app_path}")

    manifest_file = None
    try:
        manifest_source, namespace, deployments, manifest_file = load_filtered_sut(
            app_path,
            namespace_override=args.namespace,
            manifest_path_override=args.manifest_path,
            exclude_resource_patterns=args.exclude_resource_pattern,
            exclude_kinds=args.exclude_kind,
        )

        print(f"Using manifests from {manifest_source}")
        if namespace:
            print(f"Namespace: {namespace}")

        if args.command == "up":
            print("Deploying SUT manifests")
            deploy_app(manifest_file, namespace=namespace)
            print("Waiting for SUT deployments to become ready")
            wait_for_deployments(deployments, timeout=args.timeout_seconds)
            print("Deploy complete")
        else:
            print(f"Deleting SUT manifests from {manifest_source}")
            delete_manifests(manifest_file, namespace=namespace)
            print("Waiting for SUT deployments to terminate")
            wait_for_deployment_termination(
                deployments,
                args.timeout_seconds,
                args.poll_interval_seconds,
            )
            if args.sleep_seconds:
                print(f"Sleeping for {args.sleep_seconds} seconds after cleanup")
                time.sleep(args.sleep_seconds)
            print("Cleanup complete")
    finally:
        if manifest_file is not None:
            try:
                manifest_file.unlink(missing_ok=True)
            except OSError:
                pass


if __name__ == "__main__":
    main()