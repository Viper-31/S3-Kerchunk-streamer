import unittest
from pipeline.contracts import(
    parse_object_record,
    parse_inventory_diff,
    ContractError,
)

class TestContracts(unittest.TestCase):
    def test_object_record_requires_fields(self):
        raw = {"etag": "a", "last_modified": "t", "size": 1} #Test with missing flow_id, should throw Error  
        with self.assertRaises(ContractError):
            parse_object_record("k.nc", raw)
    def test_inventory_diff_requires_lists(self):
        bad = {"new": "k.nc", "changed": [], "deleted": [], "unchanged": []}
        with self.assertRaises(ContractError):
            parse_inventory_diff(bad)
    def test_object_record_accepts_valid_data(self):
        """Test that valid object records pass validation"""
        valid_raw= {"etag": "a", "last_modified": "t", "size": 1, "flow_id": "f1"}
        result= parse_object_record("k.nc", valid_raw)
        self.assertEqual(result.flow_id, "f1")
    def test_inventory_diff_accepts_valid_data(self):
        """Test that valid inventory diffs pass validation."""
        valid_diff = {"new": ["a.nc"], "changed": [], "deleted": [], "unchanged": ["b.nc"]}
        result = parse_inventory_diff(valid_diff)
        self.assertEqual(result.new, ["a.nc"])
        self.assertEqual(result.unchanged, ["b.nc"])