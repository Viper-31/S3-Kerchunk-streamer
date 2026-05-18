import pickle
import traceback
import unittest
import tempfile
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pipeline.generate_parquet import (
    _build_registry,
    reference_relpath_for_key,
    build_reference_paths,
    prepare_temp_target,
    select_parser,
    build_vds_to_reference,
    enrich_string_variables,
    commit_reference,
    validate_generation_inputs,
    _keys_to_generate,
    _resolve_workers,
    generate_reference_for_object,
)
from pipeline.contracts import ContractError
from utils.config_utils import load_pipeline_config


# Try to import these for the specialized pickling test
try:
    from obspec_utils.registry import ObjectStoreRegistry
    from obstore.store import S3Store
except ImportError:
    ObjectStoreRegistry = None
    S3Store = None


class TestGenerateParquet(unittest.TestCase):
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

    @patch("pipeline.generate_parquet.build_vds_to_reference")
    @patch("pipeline.generate_parquet.select_parser")
    @patch("pipeline.generate_parquet.commit_reference")
    def test_config_propagation_to_generation(self, mock_commit, mock_select_parser, mock_build_vds):
        """Test that execution configuration parameters map correctly from config.yaml to generation."""
        mock_select_parser.return_value = (MagicMock(), [])

        # Pull values directly from the loaded config.yaml setup in setUpClass
        config_record_size = self.kp["execution"]["parquet_record_size"]
        config_cat_threshold = self.kp["execution"]["categorical_threshold"]

        with tempfile.TemporaryDirectory() as td:
            generate_reference_for_object(
                source_key="test/data.nc",
                bucket=self.kp["s3"]["bucket"],
                access_key="ak",
                secret_key="sk",
                s3_config=self.kp["s3"],
                registry=MagicMock(),
                staging_volume_path=str(Path(td) / "staging"),
                temp_path=str(Path(td) / "temp"),
                current_objects={"test/data.nc": {"flow_id": "flow1"}},
                record_size=config_record_size,
                categorical_threshold=config_cat_threshold,
            )

        # Verify that generate_reference_for_object passes config values down
        mock_build_vds.assert_called_once()
        _, kwargs = mock_build_vds.call_args
        self.assertEqual(kwargs["record_size"], config_record_size)
        self.assertEqual(kwargs["categorical_threshold"], config_cat_threshold)

    @pytest.mark.integration
    def test_object_store_registry_pickling(self):
        """
        Pickling: Serialising an object by converting it into byte stream to be sent over network/different processses.
        Integration test: ObjectStoreRegistry containing obstore objects handles pickling correctly.
        """
        if ObjectStoreRegistry is None or S3Store is None:
            self.skipTest("obspec_utils or obstore not installed")

        bucket = self.kp["s3"]["bucket"]
        test_kp = {
            "s3": {
                "bucket": bucket,
                "endpoint_url": self.kp["s3"]["endpoint_url"],
                "region_name": self.kp.get("s3", {}).get("region_name", "us-east-1"),
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
        source_key = "ecmwf_op_clean/2024/02/06.nc"
        expected = f"refs/{source_key}.parq"
        self.assertEqual(reference_relpath_for_key(source_key), expected)

    def test_build_reference_paths(self):
        """Path unit test: stable mapping from source key to final/tmp parquet paths."""
        source_key = "ecmwf_op_clean/2024/02/06.nc"
        paths = build_reference_paths(
            source_key=source_key,
            staging_volume_path="acacia_refs_staging",
            temp_path="acacia_refs_temp",
        )

        self.assertEqual(
            paths.final_ref_path,
            Path("acacia_refs_staging") / "refs/ecmwf_op_clean/2024/02/06.nc.parq",
        )
        self.assertEqual(
            paths.tmp_ref_path,
            Path("acacia_refs_temp") / "ecmwf_op_clean__2024__02__06.nc.tmp.parq",
        )

    def test_remove_tmpfile_for_existing_file(self):
        """Path unit test: pre-existing temp parquet file is removed before generation."""
        with tempfile.TemporaryDirectory() as td:
            tmp_ref_path = Path(td) / "tmp" / "a.tmp.parq"
            tmp_ref_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_ref_path.write_text("stale", encoding="utf-8")

            self.assertTrue(tmp_ref_path.exists())
            prepare_temp_target(tmp_ref_path)
            self.assertFalse(tmp_ref_path.exists())

    def test_remove_tmpdir_for_existing_dir(self):
        """Path unit test: pre-existing temp directory at target is removed safely."""
        with tempfile.TemporaryDirectory() as td:
            tmp_ref_path = Path(td) / "tmp" / "a.tmp.parq"
            tmp_ref_path.mkdir(parents=True, exist_ok=True)
            (tmp_ref_path / "nested.txt").write_text("stale-dir", encoding="utf-8")

            self.assertTrue(tmp_ref_path.exists())
            self.assertTrue(tmp_ref_path.is_dir())
            prepare_temp_target(tmp_ref_path)
            self.assertFalse(tmp_ref_path.exists())

    @patch("pipeline.generate_parquet.HDFParser")
    @patch("pipeline.generate_parquet.xr.open_dataset")
    def test_string_var_select_parser(self, mock_xr_open, mock_hdf_parser):
        """Parser unit test: detect string-like variables and pass them to drop_variables."""

        class _DType:
            def __init__(self, kind: str):
                self.kind = kind

        class _Var:
            def __init__(self, kind: str):
                self.dtype = _DType(kind)

        mock_dataset = MagicMock()
        mock_dataset.variables = {
            "temperature": _Var("f"),
            "station_name": _Var("U"),
            "notes": _Var("O"),
        }
        mock_dataset.__getitem__.side_effect = lambda k: mock_dataset.variables.get(k)
        mock_xr_open.return_value = mock_dataset

        fs = MagicMock()
        fs.open.return_value.__enter__.return_value = MagicMock()

        parser, string_vars = select_parser(fs, "weather", "ecmwf_op_clean/2024/02/06.nc")

        self.assertEqual(string_vars, ["station_name", "notes"])
        mock_hdf_parser.assert_called_once_with(drop_variables=["station_name", "notes"])
        self.assertIs(parser, mock_hdf_parser.return_value)

    def test_commit_reference_replaces_existing_file(self):
        """Existing final parquet file is replaced by tmp output."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            tmp_ref_path = td_path / "work" / "obj.tmp.parq"
            final_ref_path = td_path / "refs" / "obj.parq"

            tmp_ref_path.parent.mkdir(parents=True, exist_ok=True)
            final_ref_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_ref_path.write_text("new-bytes", encoding="utf-8")
            final_ref_path.write_text("old-bytes", encoding="utf-8")

            commit_reference(tmp_ref_path, final_ref_path)

            self.assertFalse(tmp_ref_path.exists())
            self.assertTrue(final_ref_path.exists())
            self.assertEqual(final_ref_path.read_text(encoding="utf-8"), "new-bytes")

    def test_replace_existing_ref_directory_atomically(self):
        """Existing final directory is removed and replaced by tmp file."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            tmp_ref_path = td_path / "work" / "obj.tmp.parq"
            final_ref_path = td_path / "refs" / "obj.parq"

            tmp_ref_path.parent.mkdir(parents=True, exist_ok=True)
            final_ref_path.mkdir(parents=True, exist_ok=True)
            (final_ref_path / "stale.txt").write_text("stale", encoding="utf-8")
            tmp_ref_path.write_text("new-bytes", encoding="utf-8")

            commit_reference(tmp_ref_path, final_ref_path)

            self.assertFalse(tmp_ref_path.exists())
            self.assertTrue(final_ref_path.exists())
            self.assertTrue(final_ref_path.is_file())
            self.assertEqual(final_ref_path.read_text(encoding="utf-8"), "new-bytes")

    @patch("pipeline.generate_parquet.xr.open_dataset")
    def test_enrich_string_variables(self, mock_xr_open):
        """Test string enrichment: no string vars means no reopen or coord assignment."""
        vds = MagicMock()
        fs = MagicMock()

        out = enrich_string_variables(
            vds=vds,
            fs=fs,
            bucket="weather",
            source_key="k.nc",
            string_vars=[],
        )

        self.assertIs(out, vds)
        fs.open.assert_not_called()
        mock_xr_open.assert_not_called()

    @pytest.mark.integration
    @patch("pipeline.generate_parquet.enrich_string_variables")
    @patch("pipeline.generate_parquet.vz.open_virtual_dataset")
    def test_build_vds_to_parq_reference(
        self,
        mock_open_vz,
        mock_enrich,
    ):
        """Integration test: Combine open virtual dataset, enrich, and write parquet reference."""
        raw_vds = MagicMock()
        enriched_vds = MagicMock()
        mock_open_vz.return_value = raw_vds
        mock_enrich.return_value = enriched_vds

        tmp_ref_path = Path("/tmp/ref.tmp.parq")
        parser = MagicMock()
        registry = MagicMock()
        fs = MagicMock()

        record_size = self.kp["execution"]["parquet_record_size"]
        cat_thresh = self.kp["execution"]["categorical_threshold"]

        build_vds_to_reference(
            source_url="s3://weather/x.nc",
            registry=registry,
            parser=parser,
            fs=fs,
            bucket="weather",
            source_key="x.nc",
            string_vars=["station_name"],
            tmp_ref_path=tmp_ref_path,
            record_size=record_size,
            categorical_threshold=cat_thresh,
        )

        mock_open_vz.assert_called_once_with(
            url="s3://weather/x.nc",
            registry=registry,
            parser=parser,
            loadable_variables=[],
        )
        mock_enrich.assert_called_once_with(
            vds=raw_vds,
            fs=fs,
            bucket="weather",
            source_key="x.nc",
            string_vars=["station_name"],
        )
        enriched_vds.vz.to_kerchunk.assert_called_once_with(
            filepath=str(tmp_ref_path),
            format="parquet",
            record_size=record_size,
            categorical_threshold=cat_thresh,
        )

    @patch("pipeline.generate_parquet.build_vds_to_reference")
    @patch("pipeline.generate_parquet.select_parser")
    @patch("pipeline.generate_parquet.commit_reference")
    @patch("pipeline.generate_parquet.s3fs.S3FileSystem")
    def test_generate_reference_orchestrator(
        self,
        mock_s3fs,
        mock_commit,
        mock_select_parser,
        mock_build_vds,
    ):
        """Orchestrator unit test: generate_reference_for_object delegates to extracted units."""
        parser = MagicMock()
        mock_select_parser.return_value = (parser, ["station_name"])

        registry = MagicMock(spec=ObjectStoreRegistry) if ObjectStoreRegistry else MagicMock()

        record_size = self.kp["execution"]["parquet_record_size"]
        cat_thresh = self.kp["execution"]["categorical_threshold"]

        with tempfile.TemporaryDirectory() as td:
            result = generate_reference_for_object(
                source_key="test/data.nc",
                bucket="my-bucket",
                access_key="ak",
                secret_key="sk",
                s3_config={"endpoint_url": "https://projects.pawsey.org.au"},
                registry=registry,
                staging_volume_path=str(Path(td) / "staging"),
                temp_path=str(Path(td) / "temp"),
                current_objects={"test/data.nc": {"flow_id": "flow1"}},
                record_size=record_size,
                categorical_threshold=cat_thresh,
            )

        self.assertEqual(result["status"], "generated")
        self.assertEqual(result["source_key"], "test/data.nc")
        self.assertEqual(result["flow_id"], "flow1")
        mock_select_parser.assert_called_once()
        mock_build_vds.assert_called_once()
        mock_commit.assert_called_once()

    def test_keys_to_generate(self):
        """Test extraction of keys that need processing."""
        diff = {
            "new": ["a.nc", "b.nc"],
            "changed": ["c.nc"],
            "deleted": ["d.nc"],
            "unchanged": ["e.nc"],
        }
        source_keys = _keys_to_generate(diff)
        self.assertEqual(source_keys, ["a.nc", "b.nc", "c.nc"])

    def test_generation_input_rejects_missing_key(self):
        """Current_objects missing required key fails validation early."""
        current_objects = {
            "a.nc": {"etag": "e", "last_modified": "t", "size": 1},
        }
        inventory_diff = {
            "new": ["a.nc"],
            "changed": [],
            "deleted": [],
            "unchanged": [],
        }

        with self.assertRaises(ContractError):
            validate_generation_inputs(current_objects, inventory_diff)

    def test_resolve_workers(self):
        """Test worker count resolution logic."""
        self.assertEqual(_resolve_workers(4), 4)
        self.assertEqual(_resolve_workers("4"), 4)
        self.assertGreaterEqual(_resolve_workers(None), 1)
        self.assertGreaterEqual(_resolve_workers("auto"), 1)

    @patch("pipeline.generate_parquet.xr.open_dataset")
    @patch("pipeline.generate_parquet.vz.open_virtual_dataset")
    @patch("pipeline.generate_parquet.os.replace")
    @patch("pipeline.generate_parquet.Path.mkdir")
    @patch("pipeline.generate_parquet.s3fs.S3FileSystem")
    def test_generate_reference_success(self, mock_s3fs, mock_mkdir, mock_replace, mock_open_vz, mock_xr_open):
        """Test successful generation of a reference with mocks."""
        # Setup mock Virtual Dataset context manager
        mock_vds = MagicMock()
        mock_open_vz.return_value = mock_vds

        mock_dataset = MagicMock()
        mock_dataset.variables = {}
        mock_xr_open.return_value = mock_dataset

        registry = MagicMock(spec=ObjectStoreRegistry) if ObjectStoreRegistry else MagicMock()

        record_size = self.kp["execution"]["parquet_record_size"]
        cat_thresh = self.kp["execution"]["categorical_threshold"]

        with tempfile.TemporaryDirectory() as td:
            result = generate_reference_for_object(
                source_key="test/data.nc",
                bucket="my-bucket",
                access_key="test-access-key",
                secret_key="test-secret-key",
                s3_config=self.kp["s3"],
                registry=registry,
                staging_volume_path=str(Path(td) / "staging"),
                temp_path=str(Path(td) / "temp"),
                current_objects={"test/data.nc": {"flow_id": "flow1"}},
                record_size=record_size,
                categorical_threshold=cat_thresh,
            )

        self.assertEqual(result["status"], "generated")
        self.assertEqual(result["source_key"], "test/data.nc")
        # Ensure to_kerchunk was called
        mock_vds.vz.to_kerchunk.assert_called_once()
        # Check that we tried to use HDFParser (first in list)
        from virtualizarr.parsers import HDFParser

        mock_open_vz.assert_called()
        self.assertIsInstance(mock_open_vz.call_args[1]["parser"], HDFParser)


if __name__ == "__main__":
    unittest.main()
