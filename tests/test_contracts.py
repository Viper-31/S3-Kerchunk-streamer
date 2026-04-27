import unittest
from pipeline.contracts import(
    parse_object_record,
    parse_inventory_diff,
    ContractError,
)

class TestContracts(unittest.TestCase):
    def test_object_record_requires_fields(self):
        raw = {"etag": "a", "last_modified": "t", "size": 1}  # missing flow_id
        with self.assertRaises(ContractError):
            parse_object_record("k.nc", raw)
    def test_inventory_diff_requires_lists(self):
        bad = {"new": "k.nc", "changed": [], "deleted": [], "unchanged": []}
        with self.assertRaises(ContractError):
            parse_inventory_diff(bad)