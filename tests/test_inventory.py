import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.inventory import (
    diff_inventory,
    load_ledger,
    compute_snapshot_artifacts,
)
from utils.config_utils import load_pipeline_config


class TestInventory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config_path = Path("configs/config.yaml")
        if cls.config_path.exists():
            cls.kp = load_pipeline_config(cls.config_path)
        else:
            cls.kp = {
                "s3": {
                    "bucket": "weather",
                    "endpoint_url": "https://projects.pawsey.org.au",
                    "region_name": "us-east-1",
                },
                "output": {
                    "ledger_path": "acacia_refs_staging/_state/inventory_ledger.json",
                    "staging_volume_path": "acacia_refs_staging",
                    "temp_path": "acacia_refs_temp",
                },
                "source_flows": [],
                "execution": {"max_workers": "auto"},
            }

    def test_diff_inventory(self):
        """Test the inventory diffing logic."""
        previous = {
            "old.nc": {"etag": "e1", "last_modified": "t1", "size": 100},
            "changed.nc": {"etag": "e2", "last_modified": "t2", "size": 200},
            "deleted.nc": {"etag": "e3", "last_modified": "t3", "size": 300},
        }
        current = {
            "old.nc": {"etag": "e1", "last_modified": "t1", "size": 100},
            "changed.nc": {"etag": "e2-new", "last_modified": "t2", "size": 200},
            "new.nc": {"etag": "e4", "last_modified": "t4", "size": 400},
        }
        diff = diff_inventory(previous, current)
        self.assertEqual(diff["new"], ["new.nc"])
        self.assertEqual(diff["changed"], ["changed.nc"])
        self.assertEqual(diff["deleted"], ["deleted.nc"])
        self.assertEqual(diff["unchanged"], ["old.nc"])

    def test_compute_snapshot_artifacts_builds_expected_diff_and_counts(self):
        """Pure transform: verify inventory diff and summary counts are correct."""
        previous_objects = {
            "stable.nc": {"etag": "e1", "last_modified": "t1", "size": 100, "flow_id": "f1"},
            "changed.nc": {"etag": "e2", "last_modified": "t2", "size": 200, "flow_id": "f1"},
            "deleted.nc": {"etag": "e3", "last_modified": "t3", "size": 300, "flow_id": "f2"},
        }
        current_objects = {
            "stable.nc": {"etag": "e1", "last_modified": "t1", "size": 100, "flow_id": "f1"},
            "changed.nc": {"etag": "e2-new", "last_modified": "t2", "size": 200, "flow_id": "f1"},
            "new.nc": {"etag": "e4", "last_modified": "t4", "size": 400, "flow_id": "f2"},
        }

        artifacts = compute_snapshot_artifacts(
            previous_objects=previous_objects,
            current_objects=current_objects,
            bucket="weather",
        )

        self.assertEqual(artifacts["diff"]["new"], ["new.nc"])
        self.assertEqual(artifacts["diff"]["changed"], ["changed.nc"])
        self.assertEqual(artifacts["diff"]["deleted"], ["deleted.nc"])
        self.assertEqual(artifacts["diff"]["unchanged"], ["stable.nc"])
        self.assertEqual(artifacts["summary"]["scanned"], 3)
        self.assertEqual(artifacts["summary"]["new"], 1)
        self.assertEqual(artifacts["summary"]["changed"], 1)
        self.assertEqual(artifacts["summary"]["deleted"], 1)
        self.assertEqual(artifacts["summary"]["unchanged"], 1)

    def test_compute_snapshot_artifacts_returns_next_ledger_with_contract_shape(self):
        """Pure transform: next_ledger keeps expected schema/bucket/objects contract."""
        previous_objects = {}
        current_objects = {
            "only.nc": {"etag": "e1", "last_modified": "t1", "size": 10, "flow_id": "f1"},
        }

        artifacts = compute_snapshot_artifacts(
            previous_objects=previous_objects,
            current_objects=current_objects,
            bucket="weather",
        )

        next_ledger = artifacts["next_ledger"]
        self.assertEqual(next_ledger["schema_version"], 1)
        self.assertEqual(next_ledger["bucket"], "weather")
        self.assertEqual(next_ledger["objects"], current_objects)
        self.assertIsInstance(next_ledger["updated_at"], str)

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.open")
    def test_load_ledger_missing(self, mock_open, mock_exists):
        """Test loading ledger when it doesn't exist."""
        mock_exists.return_value = False
        ledger = load_ledger("missing_ledger.json")
        self.assertEqual(ledger["objects"], {})
        self.assertEqual(ledger["schema_version"], 1)


if __name__ == "__main__":
    unittest.main()
