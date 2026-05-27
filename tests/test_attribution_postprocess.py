import csv
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import attribution_postprocess as attribution  # noqa: E402


SAMPLE_RUN_DIR = (
    REPO_ROOT
    / "final-runs"
    / "experiment-runs"
    / "onlineboutique"
    / "eoan_20260505_193937_401383_onlineboutique"
    / "medium"
    / "iteration_20260505_202750_581542"
)


class AttributionMappingTests(unittest.TestCase):
    def test_classify_entity_uses_primary_and_builtin_groups(self):
        metadata = {
            "sut_containers": ["currencyservice", "frontend"],
            "deployment": {
                "deployments": [
                    {"name": "currencyservice", "namespace": "onlineboutique"},
                    {"name": "frontend", "namespace": "onlineboutique"},
                ]
            },
        }
        config = {
            "service_groups": {"measurement_infrastructure": ["kepler"]},
            "service_overrides": {},
            "ignore_patterns": [],
        }
        catalog = attribution.build_service_catalog(metadata, config)

        primary = attribution.classify_entity("currencyservice", catalog)
        measurement = attribution.classify_entity("kepler", catalog)
        unknown = attribution.classify_entity("mystery-container", catalog)

        self.assertEqual(primary["service_group"], attribution.PRIMARY_GROUP)
        self.assertEqual(primary["match_status"], "exact_primary")
        self.assertEqual(measurement["service_group"], attribution.MEASUREMENT_GROUP)
        self.assertEqual(measurement["match_status"], "configured_group")
        self.assertEqual(unknown["service_group"], attribution.UNKNOWN_GROUP)
        self.assertEqual(unknown["match_status"], "unknown")

    def test_aggregate_service_rows_sums_energy(self):
        workload_context = {
            "workload_level": "medium",
            "workload_region": "medium",
            "users": 80,
            "duration_seconds": 120,
            "total_requests": 10,
            "successful_requests": 9,
            "throughput_mean_rps": 1.0,
            "error_rate": 0.1,
            "p95_latency": 12.0,
        }
        rows = [
            {
                "service_name": "frontend",
                "service_group": attribution.PRIMARY_GROUP,
                "raw_energy_joules": 4.0,
                "allocated_energy_joules": 4.0,
            },
            {
                "service_name": "frontend",
                "service_group": attribution.PRIMARY_GROUP,
                "raw_energy_joules": 6.0,
                "allocated_energy_joules": 6.0,
            },
            {
                "service_name": "mystery",
                "service_group": attribution.UNKNOWN_GROUP,
                "raw_energy_joules": 2.0,
                "allocated_energy_joules": 2.0,
            },
        ]

        service_rows = attribution.aggregate_service_rows(
            model_variant=attribution.MODEL_M1,
            container_rows=rows,
            workload_context=workload_context,
            total_reference_energy_joules=12.0,
        )

        frontend = next(row for row in service_rows if row["service_name"] == "frontend")
        mystery = next(row for row in service_rows if row["service_name"] == "mystery")

        self.assertEqual(frontend["allocated_energy_joules"], 10.0)
        self.assertAlmostEqual(frontend["joules_per_request"], 1.0)
        self.assertAlmostEqual(frontend["joules_per_successful_request"], 10.0 / 9.0)
        self.assertEqual(mystery["service_group"], attribution.UNKNOWN_GROUP)
        self.assertEqual(mystery["allocated_energy_joules"], 2.0)


class AttributionIntegrationTests(unittest.TestCase):
    def test_sample_run_emits_attribution_artifacts(self):
        self.assertTrue(SAMPLE_RUN_DIR.exists(), SAMPLE_RUN_DIR)
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "attribution"
            report = attribution.build_attribution_report(SAMPLE_RUN_DIR, output_dir=output_dir)

            json_path = output_dir / "attribution.json"
            container_csv = output_dir / "container_attribution.csv"
            service_csv = output_dir / "service_attribution.csv"

            self.assertTrue(json_path.exists())
            self.assertTrue(container_csv.exists())
            self.assertTrue(service_csv.exists())
            self.assertIn(attribution.MODEL_M1, report["models"])
            self.assertIn(attribution.MODEL_M2, report["models"])
            self.assertEqual(report["selected_attribution_scope"], "sut")
            self.assertGreaterEqual(report["models"][attribution.MODEL_M1]["coverage"]["total_container_count"], 0)
            self.assertGreater(report["m1_out_of_scope_container_count"], 0)
            self.assertEqual(report["m2_valid_for_comparison"], False)
            self.assertGreater(len(report["models"][attribution.MODEL_M1]["container_rows"]), 0)

            with container_csv.open("r", encoding="utf-8", newline="") as infile:
                rows = list(csv.DictReader(infile))
            self.assertTrue(any(row["model_variant"] == attribution.MODEL_M1 for row in rows))
            self.assertTrue(any(row["model_variant"] == attribution.MODEL_M2 for row in rows))

            with service_csv.open("r", encoding="utf-8", newline="") as infile:
                service_rows = list(csv.DictReader(infile))
            self.assertTrue(any(row["model_variant"] == attribution.MODEL_M1 for row in service_rows))
            self.assertTrue(any(row["model_variant"] == attribution.MODEL_M2 for row in service_rows))

            self.assertIn("warnings", report)
            self.assertIsInstance(report["warnings"], list)


if __name__ == "__main__":
    unittest.main()
