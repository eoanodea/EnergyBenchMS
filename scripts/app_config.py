#!/usr/bin/env python3
"""Utilities for app deployment configuration and manifest filtering."""

from pathlib import Path
import re

import yaml


APP_CONFIG_FILENAME = "pipeline_app.yaml"


def _as_list(value, field_name):
    """Normalize scalar/list config values into a string list."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        return [value]
    raise ValueError(f"'{field_name}' must be a string or list")


def load_app_config(app_path):
    """Load optional per-app pipeline configuration."""
    config_path = Path(app_path) / APP_CONFIG_FILENAME
    if not config_path.exists():
        return {}

    with config_path.open("r", encoding="utf-8") as infile:
        loaded = yaml.safe_load(infile) or {}

    if not isinstance(loaded, dict):
        raise ValueError(f"{APP_CONFIG_FILENAME} must contain a mapping")
    return loaded


def resolve_manifest_source(app_path, config, manifest_path_override=None):
    """Resolve manifest source path from app path, config, and CLI override."""
    manifest_rel = manifest_path_override or config.get("manifest_path") or "."
    manifest_source = (Path(app_path) / manifest_rel).resolve()
    if not manifest_source.exists():
        raise FileNotFoundError(f"Manifest source does not exist: {manifest_source}")
    return manifest_source


def resolve_namespace(config, namespace_override=None):
    """Resolve namespace from app config and CLI override."""
    if namespace_override:
        return namespace_override
    namespace = config.get("namespace")
    if namespace is None:
        return None
    return str(namespace)


def resolve_exclusion_patterns(config, extra_patterns=None):
    """Resolve exclusion regex patterns from config plus CLI additions."""
    configured = _as_list(config.get("exclude_resource_patterns"), "exclude_resource_patterns")
    extras = _as_list(extra_patterns, "exclude_resource_patterns")
    combined = configured + extras

    compiled = []
    for raw_pattern in combined:
        try:
            compiled.append(re.compile(raw_pattern, re.IGNORECASE))
        except re.error as exc:
            raise ValueError(f"Invalid exclusion regex '{raw_pattern}': {exc}") from exc
    return compiled


def resolve_excluded_kinds(config, extra_kinds=None):
    """Resolve excluded kinds from config plus CLI additions."""
    configured = _as_list(config.get("exclude_kinds"), "exclude_kinds")
    extras = _as_list(extra_kinds, "exclude_kinds")
    return {kind.strip().lower() for kind in configured + extras if str(kind).strip()}


def _list_manifest_files(manifest_source):
    """List YAML files for a manifest source path."""
    source = Path(manifest_source)
    if source.is_file():
        return [source]

    return sorted(
        [
            path
            for path in source.iterdir()
            if path.is_file() and path.suffix.lower() in {".yaml", ".yml"}
        ]
    )


def load_manifest_documents(manifest_source):
    """Load all YAML documents from a manifest source."""
    documents = []
    for file_path in _list_manifest_files(manifest_source):
        with file_path.open("r", encoding="utf-8") as infile:
            for doc in yaml.safe_load_all(infile):
                if isinstance(doc, dict):
                    documents.append(doc)
    return documents


def should_exclude_resource(manifest, excluded_kinds, exclusion_patterns):
    """Return true when manifest should be excluded from apply/delete operations."""
    kind = str(manifest.get("kind", "")).strip()
    name = str(manifest.get("metadata", {}).get("name", "")).strip()
    namespace = str(manifest.get("metadata", {}).get("namespace", "")).strip()
    identity = f"{kind}/{name}" if kind or name else ""
    identity_with_namespace = f"{namespace}/{identity}" if namespace else identity

    if kind.lower() in excluded_kinds:
        return True

    for pattern in exclusion_patterns:
        if pattern.search(identity_with_namespace) or (name and pattern.search(name)):
            return True

    return False


def filter_manifest_documents(manifests, excluded_kinds, exclusion_patterns):
    """Filter manifest documents according to exclusions."""
    return [
        manifest
        for manifest in manifests
        if not should_exclude_resource(manifest, excluded_kinds, exclusion_patterns)
    ]


def manifest_namespace(manifest, default_namespace=None):
    """Resolve namespace for an individual manifest."""
    namespace = manifest.get("metadata", {}).get("namespace")
    if namespace:
        return str(namespace)
    return default_namespace


def extract_deployments(manifests, default_namespace=None):
    """Extract deployment identifiers from manifest list."""
    deployments = []
    seen = set()

    for manifest in manifests:
        if str(manifest.get("kind", "")).strip().lower() != "deployment":
            continue

        name = manifest.get("metadata", {}).get("name")
        if not name:
            continue

        namespace = manifest_namespace(manifest, default_namespace)
        identity = (str(namespace or ""), str(name))
        if identity in seen:
            continue
        seen.add(identity)
        deployments.append(
            {
                "name": str(name),
                "namespace": namespace,
            }
        )

    return deployments


def infer_sut_name(app_path, manifests):
    """Infer SUT name from config or first deployment name."""
    for manifest in manifests:
        if str(manifest.get("kind", "")).strip().lower() != "deployment":
            continue
        name = manifest.get("metadata", {}).get("name")
        if name:
            return str(name)

    return Path(app_path).name
