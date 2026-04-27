from dataclasses import dataclass
from typing import Any, Mapping

class ContractError(ValueError):
    pass

"""
Per S3 Object row model, used in incremental diffing. Diff check for new/updated/deleted objects
"""
@dataclass(frozen=True)
class ObjectRecord:
    key: str
    etag: str
    last_modified: str
    size: int
    flow_id: str

"""
New, changed, deleted, and unchanged keys between per object records.
"""
@dataclass(frozen=True)
class InventoryDiff:
    new: list[str]
    changed: list[str]
    deleted: list[str]
    unchanged: list[str]

def parse_object_record(key: str, raw: Mapping[str, Any]) -> ObjectRecord:
    required = ["etag", "last_modified", "size", "flow_id"]
    missing = [k for k in required if k not in raw]
    if missing:
        raise ContractError(f"{key}: missing fields {missing}")
    return ObjectRecord(
        key=key,
        etag=str(raw["etag"]),
        last_modified=str(raw["last_modified"]),
        size=int(raw["size"]),
        flow_id=str(raw["flow_id"]),
    )

def parse_inventory_diff(raw: Mapping[str, Any]) -> InventoryDiff:
    for k in ("new", "changed", "deleted", "unchanged"):
        if not isinstance(raw.get(k), list):
            raise ContractError(f"diff.{k} must be list[str]")
    return InventoryDiff(
        new=sorted(set(raw["new"])),
        changed=sorted(set(raw["changed"])),
        deleted=sorted(set(raw["deleted"])),
        unchanged=sorted(set(raw["unchanged"])),
    )