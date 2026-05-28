#!/usr/bin/env python3
"""Run attribution postprocess across a workload-batch directory.

Expected layout:
<batch_dir>/
  high/
    iteration_x/
  medium/
    iteration_x/
  low/
    iteration_x/
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple


SCRIPT_DIR = Path(__file__).resolve().parent
ATTRIBUTION_POSTPROCESS_SCRIPT = SCRIPT_DIR / "attribution_postprocess.py"
LEVEL_ORDER = ["low", "medium", "high"]


def level_sort_key(level_name: str) -> Tuple[int, int | str]:
    lowered = level_name.lower()
    if lowered in LEVEL_ORDER:
        return (0, LEVEL_ORDER.index(lowered))
    return (1, lowered)


def is_iteration_run_dir(path: Path) -> bool:
    return path.is_dir() and (path / "summary.json").exists() and (path / "metadata.json").exists()


def discover_run_dirs(batch_dir: Path) -> List[Path]:
    runs: List[Path] = []

    level_dirs = [p for p in batch_dir.iterdir() if p.is_dir()]
    for level_dir in sorted(level_dirs, key=lambda p: level_sort_key(p.name)):
        iteration_dirs = [child for child in level_dir.iterdir() if is_iteration_run_dir(child)]
        for iteration_dir in sorted(iteration_dirs):
            runs.append(iteration_dir)

    if runs:
        return runs

    # Fallback: accept any nested run folder containing summary+metadata.
    found = {summary_path.parent for summary_path in batch_dir.rglob("summary.json")}
    return sorted(path for path in found if is_iteration_run_dir(path))


def run_attribution_for_dir(run_dir: Path) -> subprocess.CompletedProcess:
    command = [
        sys.executable,
        str(ATTRIBUTION_POSTPROCESS_SCRIPT),
        "--run-dir",
        str(run_dir),
    ]
    return subprocess.run(command, capture_output=True, text=True)


def tail_lines(text: str, count: int = 5) -> str:
    lines = [line for line in (text or "").splitlines() if line.strip()]
    if not lines:
        return ""
    return "\n".join(lines[-count:])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run attribution postprocess on every iteration directory under a batch root "
            "(for example: runs/<timestamp>_<sut>/high|medium|low/iteration_*)."
        )
    )
    parser.add_argument(
        "--batch-dir",
        required=True,
        help="Path to the batch directory containing high/medium/low folders",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on the first failed run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print discovered run directories without executing postprocess",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    batch_dir = Path(args.batch_dir).resolve()
    if not batch_dir.exists() or not batch_dir.is_dir():
        raise SystemExit(f"Batch directory not found or not a directory: {batch_dir}")

    if not ATTRIBUTION_POSTPROCESS_SCRIPT.exists():
        raise SystemExit(f"Missing attribution script: {ATTRIBUTION_POSTPROCESS_SCRIPT}")

    run_dirs = discover_run_dirs(batch_dir)
    if not run_dirs:
        raise SystemExit(f"No iteration run directories found under {batch_dir}")

    print(f"Batch directory: {batch_dir}")
    print(f"Discovered run directories: {len(run_dirs)}")

    if args.dry_run:
        for run_dir in run_dirs:
            print(f"- {run_dir}")
        print("DRY_RUN_DONE")
        return

    ok_count = 0
    fail_count = 0
    failures: List[Tuple[Path, str]] = []

    for run_dir in run_dirs:
        print(f"== Attribution: {run_dir}")
        completed = run_attribution_for_dir(run_dir)

        if completed.returncode == 0:
            ok_count += 1
            print("   status=OK")
            continue

        fail_count += 1
        failure_excerpt = tail_lines(completed.stderr or completed.stdout)
        failures.append((run_dir, failure_excerpt))
        print("   status=FAIL")
        if failure_excerpt:
            print(failure_excerpt)

        if args.fail_fast:
            break

    print(f"POSTPROCESS_DONE ok={ok_count} fail={fail_count}")
    if failures:
        print("Failed run directories:")
        for run_dir, excerpt in failures:
            print(f"- {run_dir}")
            if excerpt:
                print(excerpt)

    if fail_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
