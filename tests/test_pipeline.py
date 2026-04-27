import pickle
import traceback
import unittest
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from pipeline.generate_parquet import (
    _build_registry,
    reference_relpath_for_key,
    build_reference_paths,
    prepare_temp_target,
    _keys_to_generate,
    _resolve_workers,
    generate_reference_for_object
)
from pipeline.inventory import (
    diff_inventory,
    _normalise_etag,
    _to_iso_utc,
    load_ledger,
    scan_inventory,
    compute_snapshot_artifacts,
)
from utils.config_utils import load_pipeline_config

# Try to import these for the specialized pickling test
try:
    from obspec_utils.registry import ObjectStoreRegistry
    from obstore.store import S3Store
except ImportError:
    ObjectStoreRegistry = None
    S3Store = None

class TestKerchunkPipeline(unittest.TestCase):
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
                    "region_name": "us-east-1"
                },
                "output": {
                    "ledger_path": "acacia_refs_staging/_state/inventory_ledger.json",
                    "staging_volume_path": "acacia_refs_staging",
                    "temp_path": "acacia_refs_temp"
                },
                "source_flows": [],
                "execution": {"max_workers": "auto"}
            }

    def test_object_store_registry_pickling(self):
        """
        Pickling: Serialising an object by converting it into byte stream to be sent over network/different processses.
        Test that ObjectStoreRegistry containing obstore objects handles pickling correctly.
        """
        if ObjectStoreRegistry is None or S3Store is None:
            self.skipTest("obspec_utils or obstore not installed")

        bucket = self.kp["s3"]["bucket"]
        test_kp = {
            "s3": {
                "bucket": bucket,
                "endpoint_url": self.kp["s3"]["endpoint_url"],
                "region_name": self.kp.get("s3", {}).get("region_name", "us-east-1")
            }
        }
        access_key = "test-access-key"
        secret_key = "test-secret-key"
        
        print(f"\n--- Starting ObjectStoreRegistry Pickling Test (Bucket: {bucket}) ---")
        try:
            registry = _build_registry(test_kp, access_key, secret_key)
            
            self.assertIsInstance(registry, ObjectStoreRegistry)
            # Check for .stores mapping which is the underlying container
            if hasattr(registry, "stores"):
                self.assertIn(f"s3://{bucket}", registry.stores)
                print("Registry stores validated.")
            
            # Test pickling with standard pickle
            print("Testing pickling with standard pickle...")
            pickled = pickle.dumps(registry)
            unpickled = pickle.loads(pickled)
            self.assertIsInstance(unpickled, ObjectStoreRegistry)
            print("Standard pickle successful.")
            
            # Test pickling with cloudpickle
            try:
                import cloudpickle
                print("Testing pickling with cloudpickle...")
                cpickled = cloudpickle.dumps(registry)
                cunpickled = cloudpickle.loads(cpickled)
                self.assertIsInstance(cunpickled, ObjectStoreRegistry)
                print("Cloudpickle successful.")
            except ImportError:
                print("Cloudpickle not available, skipping that part of the test.")
            
        except Exception as e:
            print(f"EXCEPTION during pickling test: {type(e).__name__}: {e}")
            traceback.print_exc()
            raise e
        print("--- ObjectStoreRegistry Pickling Test Passed ---\n")

    def test_reference_relpath_for_key(self):
        """Test mapping of source key to parquet reference path."""
        key = "ecmwf_op_clean/2024/02/06.nc"
        expected = f"refs/{key}.parquet"
        self.assertEqual(reference_relpath_for_key(key), expected)

    def test_build_reference_paths(self):
        """Path seam: stable mapping from source key to final/tmp parquet paths."""
        key = "ecmwf_op_clean/2024/02/06.nc"
        paths = build_reference_paths(
            key=key,
            staging_volume_path="acacia_refs_staging",
            temp_path="acacia_refs_temp",
        )

        self.assertEqual(
            paths.final_ref_path,
            Path("acacia_refs_staging") / "refs/ecmwf_op_clean/2024/02/06.nc.parquet",
        )
        self.assertEqual(
            paths.tmp_ref_path,
            Path("acacia_refs_temp") / "ecmwf_op_clean__2024__02__06.nc.tmp.parquet",
        )

    def test_remove_tmpfile_for_existing_file(self):
        """Path seam: pre-existing temp parquet file is removed before generation."""
        with tempfile.TemporaryDirectory() as td:
            tmp_ref_path = Path(td) / "tmp" / "a.tmp.parquet"
            tmp_ref_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_ref_path.write_text("stale", encoding="utf-8")

            self.assertTrue(tmp_ref_path.exists())
            prepare_temp_target(tmp_ref_path)
            self.assertFalse(tmp_ref_path.exists())

    def test_remove_tmpdir_for_existing_dir(self):
        """Path seam: pre-existing temp directory at target is removed safely."""
        with tempfile.TemporaryDirectory() as td:
            tmp_ref_path = Path(td) / "tmp" / "a.tmp.parquet"
            tmp_ref_path.mkdir(parents=True, exist_ok=True)
            (tmp_ref_path / "nested.txt").write_text("stale-dir", encoding="utf-8")

            self.assertTrue(tmp_ref_path.exists())
            self.assertTrue(tmp_ref_path.is_dir())
            prepare_temp_target(tmp_ref_path)
            self.assertFalse(tmp_ref_path.exists())

    def test_keys_to_generate(self):
        """Test extraction of keys that need processing."""
        diff = {
            "new": ["a.nc", "b.nc"],
            "changed": ["c.nc"],
            "deleted": ["d.nc"],
            "unchanged": ["e.nc"]
        }
        keys = _keys_to_generate(diff)
        self.assertEqual(keys, ["a.nc", "b.nc", "c.nc"])

    def test_resolve_workers(self):
        """Test worker count resolution logic."""
        self.assertEqual(_resolve_workers(4), 4)
        self.assertEqual(_resolve_workers("4"), 4)
        self.assertGreaterEqual(_resolve_workers(None), 1)
        self.assertGreaterEqual(_resolve_workers("auto"), 1)

    def test_diff_inventory(self):
        """Test the inventory diffing logic."""
        previous = {
            "old.nc": {"etag": "e1", "last_modified": "t1", "size": 100},
            "changed.nc": {"etag": "e2", "last_modified": "t2", "size": 200},
        }
        current = {
            "old.nc": {"etag": "e1", "last_modified": "t1", "size": 100},
            "changed.nc": {"etag": "e2-new", "last_modified": "t2", "size": 200},
            "new.nc": {"etag": "e4", "last_modified": "t4", "size": 400},
        }
        diff = diff_inventory(previous, current)
        self.assertEqual(diff["new"], ["new.nc"])
        self.assertEqual(diff["changed"], ["changed.nc"])

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

    @patch("pipeline.generate_parquet.xr.open_dataset")    
    @patch("pipeline.generate_parquet.vz.open_virtual_dataset")
    @patch("pipeline.generate_parquet.os.replace")
    @patch("pipeline.generate_parquet.Path.mkdir")
    @patch("pipeline.generate_parquet.s3fs.S3FileSystem")
    def test_generate_reference_success(self, mock_s3fs, mock_mkdir, mock_replace, mock_open_vz, mock_xr_open):
        """Test successful generation of a reference with mocks."""
        # Setup mock Virtual Dataset context manager
        mock_vds = MagicMock()
        mock_open_vz.return_value.__enter__.return_value = mock_vds
        
        mock_dataset = MagicMock()
        mock_dataset.variables = {}
        mock_xr_open.return_value = mock_dataset

        registry = MagicMock(spec=ObjectStoreRegistry) if ObjectStoreRegistry else MagicMock()

        access_key= "test-access-key"
        secret_key= "test-secret-key"

        
        result = generate_reference_for_object(
            key="test/data.nc",
            bucket="my-bucket",
            access_key=access_key,
            secret_key=secret_key,
            s3_config=self.kp["s3"],
            registry=registry,
            staging_volume_path="staging",
            temp_path="temp",
            current_objects={"test/data.nc": {"flow_id": "flow1"}},
            record_size=100,
            categorical_threshold=10
        )
        
        self.assertEqual(result["status"], "generated")
        self.assertEqual(result["key"], "test/data.nc")
        # Ensure to_kerchunk was called
        mock_vds.vz.to_kerchunk.assert_called_once()
        # Check that we tried to use HDFParser (first in list)
        from virtualizarr.parsers import HDFParser
        mock_open_vz.assert_called()
        self.assertIsInstance(mock_open_vz.call_args[1]["parser"], HDFParser)

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
