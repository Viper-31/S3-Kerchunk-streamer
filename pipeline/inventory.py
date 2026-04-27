from __future__ import annotations

import json
import re
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import boto3
import s3fs
from botocore.config import Config
from botocore.exceptions import ClientError

from pipeline.contracts import ObjectRecord

LEDGER_SCHEMA_VERSION = 1

"""Return current UTC timestamp in ISO format."""
def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()

def _normalise_etag(raw: str | None) -> str:
    if not raw:
        return ""
    return raw.strip ('"')

"""Normalize metadata timestamps into UTC ISO strings for stable comparisons."""
def _to_iso_utc(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    return str(value)


def _object_record_to_row(record: ObjectRecord) -> dict[str, Any]:
    return {
        "etag": record.etag,
        "last_modified": record.last_modified,
        "size": record.size,
        "flow_id": record.flow_id,
    }

"""Create both s3fs and boto3 clients from the validated config and secrets."""
def build_storage_clients(kp: dict[str, Any], access_key: str, secret_key: str):
    s3_cfg = kp["s3"]

    client_kwargs = {
        "endpoint_url": s3_cfg["endpoint_url"],
        "region_name": s3_cfg.get("region_name", "us-east-1"),
    }
    config_kwargs = {
        "signature_version": "s3v4",
        "s3": {"addressing_style": "path"},
    }

    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs=client_kwargs,
        config_kwargs=config_kwargs,
    )

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(**config_kwargs),
        **client_kwargs,
    )

    return fs, s3_client

"""Load ledger from disk, or initialize an empty structure if it is missing."""
def load_ledger(ledger_path: str) -> dict[str, Any]:
    p = Path(ledger_path)
    if not p.exists():
        return {
            "schema_version": LEDGER_SCHEMA_VERSION,
            "updated_at": None,
            "objects": {},
        }
    with p.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
        if not isinstance(payload, dict):
            raise ValueError("Ledger file must contain a JSON object at the root")
    
    if payload.get("schema_version") != LEDGER_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported ledger schema version= {payload.get('schema_version')}, "
            f"expected {LEDGER_SCHEMA_VERSION}"
        )
    
    objects = payload.get("objects", {})
    if not isinstance(objects, dict):
        raise ValueError("Ledger objects must be a dict keyed by a source key")
    
    return payload

"""
Yield NetCDF objects matching prefix + regex flow selectors.
"""
def _iter_prefix_regex_objects(
        s3_client: Any,
        bucket: str,
        flow: dict[str, Any],
        page_size: int,
):
    flow_id = flow["id"]
    prefix = flow["prefix"]
    pattern = re.compile(flow["key_regex"])

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        PaginationConfig={"PageSize": page_size},
    )

    # Only include NetCDF source objects that satisfy this flow selector.
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".nc"):
                continue
            if not pattern.match(key):
                continue

            yield ObjectRecord(
                key=key,
                etag=_normalise_etag(obj.get("ETag")),
                last_modified=_to_iso_utc(obj.get("LastModified")),
                size=int(obj.get("Size", 0)),
                flow_id=flow_id,
            )

"""Yield a singleton NetCDF object from an exact-key flow definition."""
def _iter_exact_key_object(s3_client: Any, bucket: str, flow: dict[str, Any]):
    key = flow["exact_key"]
    if not key.endswith(".nc"):
        return

    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        err = str(exc.response.get("Error", {}).get("Code", ""))
        if err in {"404", "NoSuchKey", "NotFound"}:
            return
        raise

    yield ObjectRecord(
        key=key,
        etag=_normalise_etag(head.get("ETag")),
        last_modified=_to_iso_utc(head.get("LastModified")),
        size=int(head.get("ContentLength", 0)),
        flow_id=flow["id"],
    )

"""Scan all enabled flows and return object metadata keyed by source key."""
def scan_inventory(kp: dict[str, Any], s3_client: Any) -> dict[str, dict[str, Any]]:
    bucket = kp["s3"]["bucket"]
    page_size = int(kp.get("execution", {}).get("list_page_size", 1000))
    objects_by_key: dict[str, dict[str, Any]] = {}

    for flow in kp["source_flows"]:
        if not flow.get("enabled", True):
            continue

        mode = flow["mode"]
        if mode == "prefix_regex":
            iterator = _iter_prefix_regex_objects(s3_client, bucket, flow, page_size)
        elif mode == "exact_key":
            iterator = _iter_exact_key_object(s3_client, bucket, flow)
        else:
            raise ValueError(f"Unsupported flow mode: {mode}")

        for obj in iterator:
            if obj.key in objects_by_key:
                raise ValueError(
                    f"Duplicate source key detected across flows: {obj.key}. "
                    "Flow selectors must be mutually exclusive."
                )
            objects_by_key[obj.key] = _object_record_to_row(obj)

    return objects_by_key

"""Return the diff fingerprint tuple used for change detection."""
def _fingerprint(record: dict[str, Any]) -> tuple[str, str, int]:
    return (
        str(record.get("etag", "")),
        str(record.get("last_modified", "")),
        int(record.get("size", 0)),
    )

"""
Compute new, changed, deleted, and unchanged keys between ledger snapshots.
"""
def diff_inventory(
    previous_objects: dict[str, dict[str, Any]],
    current_objects: dict[str, dict[str, Any]],
) -> dict[str, list[str]]:
    prev_keys = set(previous_objects.keys())
    curr_keys = set(current_objects.keys())

    new_keys = sorted(curr_keys - prev_keys)
    deleted_keys = sorted(prev_keys - curr_keys)

    changed_keys: list[str] = []
    unchanged_keys: list[str] = []

    for key in sorted(curr_keys & prev_keys):
        if _fingerprint(current_objects[key]) == _fingerprint(previous_objects[key]):
            unchanged_keys.append(key)
        else:
            changed_keys.append(key)

    return {
        "new": new_keys,
        "changed": changed_keys,
        "deleted": deleted_keys,
        "unchanged": unchanged_keys,
    }

def compute_snapshot_artifacts(
    *,
    previous_objects: dict[str, dict[str, Any]],
    current_objects: dict[str, dict[str, Any]],
    bucket: str,
) -> dict[str, Any]:
    """Build diff, next-ledger payload, and summary from object snapshots."""
    diff = diff_inventory(previous_objects, current_objects)

    next_ledger = {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "updated_at": _utc_now_iso(),
        "bucket": bucket,
        "objects": current_objects,
    }

    summary = {
        "scanned": len(current_objects),
        "new": len(diff["new"]),
        "changed": len(diff["changed"]),
        "deleted": len(diff["deleted"]),
        "unchanged": len(diff["unchanged"]),
    }

    return {
        "diff": diff,
        "next_ledger": next_ledger,
        "summary": summary,
    }

"""Build the current inventory snapshot and diff it against the previAnalyse my codebase again, ignore hidden files/folders. 
I believe I have carried out Step 0 and 1, check that they are implemented correctly in test_contracts.py and contracts.py respectively. 
Proceed with showing me how to implement Step 2.  modularising build_inventory_snapshot_and_diff and the associated unit tests. ous ledger."""
def build_inventory_snapshot_and_diff(
    kp: dict[str, Any],
    access_key: str,
    secret_key: str,
) -> dict[str, Any]:
    fs, s3_client = build_storage_clients(kp, access_key, secret_key)

    previous_ledger = load_ledger(kp["output"]["ledger_path"])
    previous_objects = previous_ledger.get("objects", {})

    current_objects = scan_inventory(kp, s3_client)
    snapshot_artifacts = compute_snapshot_artifacts(
        previous_objects=previous_objects,
        current_objects=current_objects,
        bucket=kp["s3"]["bucket"],
    )

    return {
        "filesystem": fs,
        "summary": snapshot_artifacts["summary"],
        "diff": snapshot_artifacts["diff"],
        "previous_ledger": previous_ledger,
        "current_objects": current_objects,
        "next_ledger": snapshot_artifacts["next_ledger"],
    }
