import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import pipeline.ecmwf_consolidate as ec


ECMWF_FLOW_ID = "ecmwf_weekly_nc"
DPIRD_FLOW_ID = "dpird_final_singleton"


def _object_record(flow_id: str) -> dict[str, object]:
    return {
        "etag": f"etag-{flow_id}",
        "last_modified": "2026-05-28T00:00:00+00:00",
        "size": 1,
        "flow_id": flow_id,
    }


def _reference_path(staging: Path, source_key: str) -> Path:
    return staging / "refs" / f"{source_key}.parq"


class TestECMWFConsolidate(unittest.TestCase):
    def _create_consolidated_output(self, staging: Path) -> Path:
        output = ec.ecmwf_consolidated_reference_path(staging)
        output.mkdir(parents=True, exist_ok=True)
        return output

    def _create_weekly_reference(self, staging: Path, source_key: str) -> Path:
        ref_path = _reference_path(staging, source_key)
        ref_path.mkdir(parents=True, exist_ok=True)
        return ref_path

    def test_consolidated_reference_path_contract(self):
        path = ec.ecmwf_consolidated_reference_path("acacia_refs_staging")

        self.assertEqual(
            path,
            Path("acacia_refs_staging")
            / "refs"
            / "ECMWF_consolidated"
            / "ecmwf_combined.nc.parq",
        )

    def test_expected_reference_paths_are_inventory_driven_and_sorted(self):
        current_objects = {
            "ECMWF/2024/02/13.nc": _object_record(ECMWF_FLOW_ID),
            "DPIRD/dpird_wa_stations.nc": _object_record(DPIRD_FLOW_ID),
            "ECMWF/2024/02/06.nc": _object_record(ECMWF_FLOW_ID),
        }

        with tempfile.TemporaryDirectory() as td:
            staging = Path(td) / "staging"
            self._create_weekly_reference(staging, "ECMWF/1999/01/01.nc")

            paths = ec.expected_ecmwf_reference_paths(
                current_objects=current_objects,
                staging_volume_path=staging,
                flow_id=ECMWF_FLOW_ID,
            )

        self.assertEqual(
            paths,
            [
                _reference_path(staging, "ECMWF/2024/02/06.nc"),
                _reference_path(staging, "ECMWF/2024/02/13.nc"),
            ],
        )

    def test_unusable_reference_keys_treats_missing_and_unreadable_refs_equally(self):
        current_objects = {
            "ECMWF/2024/02/06.nc": _object_record(ECMWF_FLOW_ID),
            "ECMWF/2024/02/13.nc": _object_record(ECMWF_FLOW_ID),
            "ECMWF/2024/02/20.nc": _object_record(ECMWF_FLOW_ID),
            "DPIRD/dpird_wa_stations.nc": _object_record(DPIRD_FLOW_ID),
        }

        with tempfile.TemporaryDirectory() as td:
            staging = Path(td) / "staging"
            good_ref = self._create_weekly_reference(staging, "ECMWF/2024/02/06.nc")
            bad_ref = self._create_weekly_reference(staging, "ECMWF/2024/02/13.nc")

            def probe(path: Path) -> None:
                if path == bad_ref:
                    raise RuntimeError("corrupt buffer")
                self.assertEqual(path, good_ref)

            unusable = ec.unusable_ecmwf_reference_keys(
                current_objects=current_objects,
                staging_volume_path=staging,
                flow_id=ECMWF_FLOW_ID,
                reference_probe=probe,
            )

        self.assertEqual(
            unusable,
            ["ECMWF/2024/02/13.nc", "ECMWF/2024/02/20.nc"],
        )

    def test_should_consolidate_when_ecmwf_new_or_changed_key_is_present(self):
        current_objects = {
            "ECMWF/2024/02/06.nc": _object_record(ECMWF_FLOW_ID),
            "DPIRD/dpird_wa_stations.nc": _object_record(DPIRD_FLOW_ID),
        }
        previous_objects = dict(current_objects)
        inventory_diff = {
            "new": ["ECMWF/2024/02/06.nc"],
            "changed": [],
            "deleted": [],
        }

        with tempfile.TemporaryDirectory() as td:
            staging = Path(td) / "staging"
            self._create_consolidated_output(staging)

            should_run = ec.should_consolidate_ecmwf(
                inventory_diff=inventory_diff,
                current_objects=current_objects,
                previous_objects=previous_objects,
                staging_volume_path=staging,
                flow_id=ECMWF_FLOW_ID,
            )

        self.assertTrue(should_run)

    def test_should_consolidate_when_deleted_key_was_ecmwf_in_previous_inventory(self):
        current_objects = {
            "DPIRD/dpird_wa_stations.nc": _object_record(DPIRD_FLOW_ID),
        }
        previous_objects = {
            "ECMWF/2024/02/06.nc": _object_record(ECMWF_FLOW_ID),
            "DPIRD/dpird_wa_stations.nc": _object_record(DPIRD_FLOW_ID),
        }
        inventory_diff = {
            "new": [],
            "changed": [],
            "deleted": ["ECMWF/2024/02/06.nc"],
        }

        with tempfile.TemporaryDirectory() as td:
            staging = Path(td) / "staging"
            self._create_consolidated_output(staging)

            should_run = ec.should_consolidate_ecmwf(
                inventory_diff=inventory_diff,
                current_objects=current_objects,
                previous_objects=previous_objects,
                staging_volume_path=staging,
                flow_id=ECMWF_FLOW_ID,
            )

        self.assertTrue(should_run)

    def test_should_consolidate_when_consolidated_output_is_missing(self):
        current_objects = {
            "ECMWF/2024/02/06.nc": _object_record(ECMWF_FLOW_ID),
        }

        with tempfile.TemporaryDirectory() as td:
            should_run = ec.should_consolidate_ecmwf(
                inventory_diff={"new": [], "changed": [], "deleted": []},
                current_objects=current_objects,
                previous_objects=current_objects,
                staging_volume_path=Path(td) / "staging",
                flow_id=ECMWF_FLOW_ID,
            )

        self.assertTrue(should_run)

    def test_should_not_consolidate_for_non_ecmwf_diff_when_output_exists(self):
        current_objects = {
            "ECMWF/2024/02/06.nc": _object_record(ECMWF_FLOW_ID),
            "DPIRD/dpird_wa_stations.nc": _object_record(DPIRD_FLOW_ID),
        }
        inventory_diff = {
            "new": ["DPIRD/dpird_wa_stations.nc"],
            "changed": [],
            "deleted": [],
        }

        with tempfile.TemporaryDirectory() as td:
            staging = Path(td) / "staging"
            self._create_consolidated_output(staging)

            should_run = ec.should_consolidate_ecmwf(
                inventory_diff=inventory_diff,
                current_objects=current_objects,
                previous_objects=current_objects,
                staging_volume_path=staging,
                flow_id=ECMWF_FLOW_ID,
            )

        self.assertFalse(should_run)

    @patch("pipeline.ecmwf_consolidate.KerchunkParquetParser")
    @patch("pipeline.ecmwf_consolidate.vz.open_virtual_mfdataset")
    def test_consolidate_returns_failed_when_virtualizarr_open_fails(
        self, mock_open_virtual_mfdataset, mock_parser_cls
    ):
        mock_open_virtual_mfdataset.side_effect = RuntimeError("corrupt buffer")

        current_objects = {
            "ECMWF/2024/02/06.nc": _object_record(ECMWF_FLOW_ID),
        }

        with tempfile.TemporaryDirectory() as td:
            staging = Path(td) / "staging"
            self._create_weekly_reference(staging, "ECMWF/2024/02/06.nc")

            result = ec.consolidate_ecmwf_references(
                current_objects=current_objects,
                staging_volume_path=staging,
                flow_id=ECMWF_FLOW_ID,
                record_size=123,
                categorical_threshold=7,
            )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["input_count"], 1)
        self.assertIn("RuntimeError: corrupt buffer", result["error"])
        mock_parser_cls.assert_called_once_with()

    @pytest.mark.integration
    @patch("pipeline.ecmwf_consolidate.KerchunkParquetParser")
    @patch("pipeline.ecmwf_consolidate.vz.open_virtual_mfdataset")
    def test_consolidate_opens_sorted_weekly_refs_and_writes_consolidated_output(
        self, mock_open_virtual_mfdataset, mock_parser_cls
    ):
        current_objects = {
            "ECMWF/2024/02/13.nc": _object_record(ECMWF_FLOW_ID),
            "ECMWF/2024/02/06.nc": _object_record(ECMWF_FLOW_ID),
        }

        with tempfile.TemporaryDirectory() as td:
            staging = Path(td) / "staging"
            ref_06 = self._create_weekly_reference(staging, "ECMWF/2024/02/06.nc")
            ref_13 = self._create_weekly_reference(staging, "ECMWF/2024/02/13.nc")
            expected_output = ec.ecmwf_consolidated_reference_path(staging)

            vds = MagicMock()
            mock_open_virtual_mfdataset.return_value = vds

            result = ec.consolidate_ecmwf_references(
                current_objects=current_objects,
                staging_volume_path=staging,
                flow_id=ECMWF_FLOW_ID,
                record_size=123,
                categorical_threshold=7,
            )

            open_args, open_kwargs = mock_open_virtual_mfdataset.call_args
            opened_paths = (
                open_kwargs.get("paths") or open_kwargs.get("urls") or open_args[0]
            )

        self.assertEqual(
            [str(path) for path in opened_paths],
            [str(ref_06), str(ref_13)],
        )
        self.assertEqual(open_kwargs["combine"], "nested")
        self.assertEqual(open_kwargs["concat_dim"], "time")
        self.assertIs(open_kwargs["parser"], mock_parser_cls.return_value)
        vds.vz.to_kerchunk.assert_called_once_with(
            filepath=str(expected_output),
            format="parquet",
            record_size=123,
            categorical_threshold=7,
        )
        self.assertEqual(
            result,
            {
                "status": "generated",
                "input_count": 2,
                "reference_path": str(expected_output),
                "input_paths": [str(ref_06), str(ref_13)],
            },
        )


if __name__ == "__main__":
    unittest.main()
