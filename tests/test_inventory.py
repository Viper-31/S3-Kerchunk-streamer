import unittest
from pathlib import Path
from unittest.mock import patch
from datetime import datetime, UTC
from botocore.exceptions import ClientError
from unittest.mock import MagicMock

from pipeline.inventory import (
    build_storage_clients,
    _iter_prefix_glob_objects,
    _iter_prefix_regex_objects,
    _iter_exact_key_object,
    scan_inventory,
    diff_inventory,
    load_ledger,
    compute_snapshot_artifacts,
    build_inventory_snapshot_and_diff,
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

    @patch("pipeline.inventory.boto3.client")
    @patch("pipeline.inventory.s3fs.S3FileSystem")
    def test_build_storage_clients_wires_credentials_and_config(
        self, mock_s3fs, mock_boto_client
    ):
        kp = {
            "s3": {
                "endpoint_url": "https://example.endpoint",
                "region_name": "us-west-2",
            }
        }
        fs = MagicMock()
        client = MagicMock()
        mock_s3fs.return_value = fs
        mock_boto_client.return_value = client

        out_fs, out_client = build_storage_clients(kp, "ak", "sk")

        self.assertIs(out_fs, fs)
        self.assertIs(out_client, client)

        mock_s3fs.assert_called_once()
        s3fs_kwargs = mock_s3fs.call_args.kwargs
        self.assertEqual(s3fs_kwargs["key"], "ak")
        self.assertEqual(s3fs_kwargs["secret"], "sk")
        self.assertEqual(
            s3fs_kwargs["client_kwargs"]["endpoint_url"], "https://example.endpoint"
        )
        self.assertEqual(s3fs_kwargs["client_kwargs"]["region_name"], "us-west-2")
        self.assertEqual(s3fs_kwargs["config_kwargs"]["signature_version"], "s3v4")
        self.assertEqual(s3fs_kwargs["config_kwargs"]["s3"]["addressing_style"], "path")

        mock_boto_client.assert_called_once()
        boto_kwargs = mock_boto_client.call_args.kwargs
        self.assertEqual(boto_kwargs["aws_access_key_id"], "ak")
        self.assertEqual(boto_kwargs["aws_secret_access_key"], "sk")
        self.assertEqual(boto_kwargs["endpoint_url"], "https://example.endpoint")
        self.assertEqual(boto_kwargs["region_name"], "us-west-2")

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.open")
    def test_load_ledger_rejects_non_dict_payload(self, mock_open, mock_exists):
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value.read.return_value = ""
        with patch("json.load", return_value=["not-a-dict"]):
            with self.assertRaises(ValueError):
                load_ledger("ledger.json")

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.open")
    def test_load_ledger_rejects_schema_mismatch(self, mock_open, mock_exists):
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value.read.return_value = ""
        payload = {"schema_version": 999, "objects": {}}
        with patch("json.load", return_value=payload):
            with self.assertRaises(ValueError):
                load_ledger("ledger.json")

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.open")
    def test_load_ledger_rejects_objects_not_dict(self, mock_open, mock_exists):
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value.read.return_value = ""
        payload = {"schema_version": 1, "objects": ["bad"]}
        with patch("json.load", return_value=payload):
            with self.assertRaises(ValueError):
                load_ledger("ledger.json")

    def _mock_paginator(self, pages):
        paginator = MagicMock()
        paginator.paginate.return_value = pages
        s3_client = MagicMock()
        s3_client.get_paginator.return_value = paginator
        return s3_client

    def test_iter_prefix_glob_objects_filters_by_glob_and_nc(self):
        pages = [
            {
                "Contents": [
                    {
                        "Key": "p/keep1.nc",
                        "ETag": '"e1"',
                        "LastModified": datetime(2024, 1, 1, tzinfo=UTC),
                        "Size": 10,
                    },
                    {
                        "Key": "p/skip.txt",
                        "ETag": '"e2"',
                        "LastModified": datetime(2024, 1, 1, tzinfo=UTC),
                        "Size": 10,
                    },
                    {
                        "Key": "p/keep2.nc",
                        "ETag": '"e3"',
                        "LastModified": datetime(2024, 1, 1, tzinfo=UTC),
                        "Size": 20,
                    },
                ]
            }
        ]
        s3 = self._mock_paginator(pages)
        flow = {"id": "f1", "prefix": "p/", "key_glob": "p/keep*.nc"}
        out = list(_iter_prefix_glob_objects(s3, "bucket", flow, 100))
        keys = [o.key for o in out]
        self.assertEqual(keys, ["p/keep1.nc", "p/keep2.nc"])

    def test_iter_prefix_regex_objects_filters_by_regex_and_nc(self):
        pages = [
            {
                "Contents": [
                    {
                        "Key": "p/a.nc",
                        "ETag": '"e1"',
                        "LastModified": datetime(2024, 1, 1, tzinfo=UTC),
                        "Size": 10,
                    },
                    {
                        "Key": "p/b.txt",
                        "ETag": '"e2"',
                        "LastModified": datetime(2024, 1, 1, tzinfo=UTC),
                        "Size": 10,
                    },
                    {
                        "Key": "p/aa.nc",
                        "ETag": '"e3"',
                        "LastModified": datetime(2024, 1, 1, tzinfo=UTC),
                        "Size": 20,
                    },
                ]
            }
        ]
        s3 = self._mock_paginator(pages)
        flow = {"id": "f2", "prefix": "p/", "key_regex": r"^p/a\.nc$"}
        out = list(_iter_prefix_regex_objects(s3, "bucket", flow, 100))
        keys = [o.key for o in out]
        self.assertEqual(keys, ["p/a.nc"])

    def test_iter_exact_key_object_skips_non_nc(self):
        s3 = MagicMock()
        flow = {"id": "f3", "exact_key": "p/file.txt"}
        out = list(_iter_exact_key_object(s3, "bucket", flow))
        self.assertEqual(out, [])
        s3.head_object.assert_not_called()

    def test_iter_exact_key_object_handles_missing(self):
        s3 = MagicMock()
        err = ClientError({"Error": {"Code": "NoSuchKey"}}, "HeadObject")
        s3.head_object.side_effect = err
        flow = {"id": "f3", "exact_key": "p/missing.nc"}
        out = list(_iter_exact_key_object(s3, "bucket", flow))
        self.assertEqual(out, [])

    def test_iter_exact_key_object_yields_on_success(self):
        s3 = MagicMock()
        s3.head_object.return_value = {
            "ETag": '"etag"',
            "LastModified": datetime(2024, 1, 1, tzinfo=UTC),
            "ContentLength": 123,
        }
        flow = {"id": "f3", "exact_key": "p/exist.nc"}
        out = list(_iter_exact_key_object(s3, "bucket", flow))
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].key, "p/exist.nc")

    @patch("pipeline.inventory._iter_prefix_regex_objects")
    @patch("pipeline.inventory._iter_prefix_glob_objects")
    @patch("pipeline.inventory._iter_exact_key_object")
    def test_scan_inventory_supported_modes_and_disabled(
        self, mock_exact, mock_glob, mock_regex
    ):
        mock_glob.return_value = []
        mock_regex.return_value = []
        mock_exact.return_value = []

        kp = {
            "s3": {"bucket": "b"},
            "execution": {"list_page_size": 5},
            "source_flows": [
                {"id": "f1", "mode": "prefix_glob", "enabled": True},
                {"id": "f2", "mode": "prefix_regex", "enabled": True},
                {"id": "f3", "mode": "exact_key", "enabled": True},
                {"id": "f4", "mode": "prefix_glob", "enabled": False},
            ],
        }
        out = scan_inventory(kp, MagicMock())
        self.assertEqual(out, {})
        self.assertTrue(mock_glob.called)
        self.assertTrue(mock_regex.called)
        self.assertTrue(mock_exact.called)

    @patch("pipeline.inventory._iter_prefix_glob_objects")
    def test_scan_inventory_rejects_duplicate_keys(self, mock_glob):
        # simulate duplicate keys across flow iterators
        from pipeline.contracts import ObjectRecord

        mock_glob.return_value = [
            ObjectRecord(
                key="dup.nc", etag="e1", last_modified="t", size=1, flow_id="f1"
            ),
            ObjectRecord(
                key="dup.nc", etag="e2", last_modified="t", size=2, flow_id="f1"
            ),
        ]
        kp = {
            "s3": {"bucket": "b"},
            "source_flows": [{"id": "f1", "mode": "prefix_glob", "enabled": True}],
        }
        with self.assertRaises(ValueError):
            scan_inventory(kp, MagicMock())

    def test_scan_inventory_rejects_unknown_mode(self):
        kp = {
            "s3": {"bucket": "b"},
            "source_flows": [{"id": "f1", "mode": "bad_mode", "enabled": True}],
        }
        with self.assertRaises(ValueError):
            scan_inventory(kp, MagicMock())

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
            "stable.nc": {
                "etag": "e1",
                "last_modified": "t1",
                "size": 100,
                "flow_id": "f1",
            },
            "changed.nc": {
                "etag": "e2",
                "last_modified": "t2",
                "size": 200,
                "flow_id": "f1",
            },
            "deleted.nc": {
                "etag": "e3",
                "last_modified": "t3",
                "size": 300,
                "flow_id": "f2",
            },
        }
        current_objects = {
            "stable.nc": {
                "etag": "e1",
                "last_modified": "t1",
                "size": 100,
                "flow_id": "f1",
            },
            "changed.nc": {
                "etag": "e2-new",
                "last_modified": "t2",
                "size": 200,
                "flow_id": "f1",
            },
            "new.nc": {
                "etag": "e4",
                "last_modified": "t4",
                "size": 400,
                "flow_id": "f2",
            },
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
            "only.nc": {
                "etag": "e1",
                "last_modified": "t1",
                "size": 10,
                "flow_id": "f1",
            },
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

    @patch("pipeline.inventory.compute_snapshot_artifacts")
    @patch("pipeline.inventory.scan_inventory")
    @patch("pipeline.inventory.load_ledger")
    @patch("pipeline.inventory.build_storage_clients")
    def test_build_inventory_snapshot_and_diff_orchestrates(
        self, mock_build_clients, mock_load_ledger, mock_scan, mock_compute
    ):
        fs = MagicMock()
        s3 = MagicMock()
        mock_build_clients.return_value = (fs, s3)

        mock_load_ledger.return_value = {"objects": {"old.nc": {"etag": "e"}}}
        mock_scan.return_value = {"new.nc": {"etag": "e2"}}
        mock_compute.return_value = {
            "summary": {
                "scanned": 1,
                "new": 1,
                "changed": 0,
                "deleted": 0,
                "unchanged": 0,
            },
            "diff": {"new": ["new.nc"], "changed": [], "deleted": [], "unchanged": []},
            "next_ledger": {
                "schema_version": 1,
                "bucket": "b",
                "objects": {"new.nc": {"etag": "e2"}},
            },
        }

        kp = {"s3": {"bucket": "b"}, "output": {"ledger_path": "path.json"}}
        out = build_inventory_snapshot_and_diff(kp, "ak", "sk")

        self.assertIs(out["filesystem"], fs)
        self.assertEqual(out["summary"]["scanned"], 1)
        self.assertEqual(out["diff"]["new"], ["new.nc"])
        self.assertEqual(out["previous_ledger"]["objects"], {"old.nc": {"etag": "e"}})
        self.assertEqual(out["current_objects"], {"new.nc": {"etag": "e2"}})
        self.assertEqual(out["next_ledger"]["bucket"], "b")


if __name__ == "__main__":
    unittest.main()
