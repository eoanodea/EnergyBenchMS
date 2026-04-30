#!/usr/bin/env python3
"""Poll a physical power meter and persist samples to CSV until stopped."""

import argparse
import csv
import json
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


CSV_FIELDNAMES = [
    "sample_index",
    "timestamp_iso",
    "timestamp_unix",
    "url",
    "source",
    "output",
    "apower",
    "voltage",
    "freq",
    "current",
    "aenergy_total",
    "ret_aenergy_total",
    "temperature_c",
    "temperature_f",
    "error",
]


def parse_payload(raw_bytes):
    payload = json.loads(raw_bytes.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Power meter response must be a JSON object")
    return payload


def read_sample(url, timeout_seconds):
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout_seconds) as response:
        return parse_payload(response.read())


def write_row(writer, handle, row):
    writer.writerow(row)
    handle.flush()


def main():
    parser = argparse.ArgumentParser(description="Collect power meter samples into a CSV file")
    parser.add_argument("--url", required=True, help="Power meter API URL")
    parser.add_argument("--output", required=True, help="Path to the CSV output file")
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=5.0,
        help="Polling interval in seconds (default: 5)",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=5.0,
        help="HTTP timeout per sample request in seconds (default: 5)",
    )
    args = parser.parse_args()

    if args.interval_seconds <= 0:
        raise SystemExit("--interval-seconds must be greater than 0")
    if args.request_timeout_seconds <= 0:
        raise SystemExit("--request-timeout-seconds must be greater than 0")

    stop_requested = False

    def handle_signal(signum, frame):
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sample_index = 0
    next_sample_at = time.monotonic()

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        handle.flush()

        while not stop_requested:
            now = datetime.now(timezone.utc)
            row = {
                "sample_index": sample_index,
                "timestamp_iso": now.isoformat(),
                "timestamp_unix": now.timestamp(),
                "url": args.url,
                "source": "",
                "output": "",
                "apower": "",
                "voltage": "",
                "freq": "",
                "current": "",
                "aenergy_total": "",
                "ret_aenergy_total": "",
                "temperature_c": "",
                "temperature_f": "",
                "error": "",
            }

            try:
                payload = read_sample(args.url, args.request_timeout_seconds)
                row.update(
                    {
                        "source": payload.get("source", ""),
                        "output": payload.get("output", ""),
                        "apower": payload.get("apower", ""),
                        "voltage": payload.get("voltage", ""),
                        "freq": payload.get("freq", ""),
                        "current": payload.get("current", ""),
                        "aenergy_total": payload.get("aenergy", {}).get("total", ""),
                        "ret_aenergy_total": payload.get("ret_aenergy", {}).get("total", ""),
                        "temperature_c": payload.get("temperature", {}).get("tC", ""),
                        "temperature_f": payload.get("temperature", {}).get("tF", ""),
                    }
                )
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError, OSError) as exc:
                row["error"] = str(exc)

            write_row(writer, handle, row)
            sample_index += 1

            next_sample_at += args.interval_seconds
            if stop_requested:
                break

            sleep_seconds = max(0.0, next_sample_at - time.monotonic())
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)


if __name__ == "__main__":
    sys.exit(main())