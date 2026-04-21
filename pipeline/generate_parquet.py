"""Write Parquet references. One Parquet per S3 object"""
from __future__ import annotations

import json, os, shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import virtualizarr as vz
from obstore.store import S3Store
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser, NetCDF3Parser

def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()

def _resolve_workers(raw: Any) -> int:
    if raw in (None, "auto"):
        return max(1, min(8, (os.cpu_count() or 4)))
    return max(1, int(raw))

def _keys_to_generate(diff: dict[str, list[str]]) -> list[str]:
    return sorted(set(diff.get("new",[]) + diff.get("changed", [])))

# Stable, deterministic one-to-one mapping from source object to parquet ref directory.
def reference_relpath_for_key(source_key: str)-> str:
    return f"refs/{source_key}.parquet"

"""
Write to Masterledger?
"""
def _write_json_atomic(path: str, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
    os.replace(tmp, target)

"""Do not save to MasterLedger if error occurs"""
def save_ledger_after_success(
    ledger_path: str,
    next_ledger: dict[str, Any],
    generation_summary: dict[str, Any],
) -> None:
    if generation_summary.get("failed", 0) > 0:
        raise RuntimeError(
            "Ref generation had failures; ledger update is blocked to prevent drift."
        )
    _write_json_atomic(ledger_path, next_ledger)

"""
Build individual registry entry for MasterLedger
"""
def _build_registry(kp: dict[str, Any], access_key: str, secret_key: str) -> ObjectStoreRegistry:
    s3_cfg = kp["s3"]
    bucket = s3_cfg["bucket"]

    store = S3Store(
        bucket=bucket,
        endpoint=s3_cfg["endpoint_url"],
        region=s3_cfg.get("region_name", "us-east-1"),
        access_key_id=access_key,
        secret_access_key=secret_key,
        virtual_hosted_style_request=False,
    )
    # Register bucket root so urls like s3://weather/path/file.nc resolve correctly.
    return ObjectStoreRegistry({f"s3://{bucket}": store})

"""
Input: Registry from MasterLedger
Output: Parquet file with metadata and chunking format of NetCDF

Kerchunk ref size is 0.002% - 0.02% if source bytes
Downstream visualisation/analytics can read Parquet references for subset queries.
"""
def generate_one_reference(
    *,
    key: str,
    bucket: str,
    registry: ObjectStoreRegistry,
    staging_volume_path: str,
    temp_path: str,
    current_objects: dict[str, dict[str, Any]],
    record_size: int,
    categorical_threshold: int,
) -> dict[str, Any]:
    source_url = f"s3://{bucket}/{key}"
    flow_id = current_objects.get(key, {}).get("flow_id", "unknown")

    rel_ref= reference_relpath_for_key(key)
    final_ref_path= Path(staging_volume_path) / rel_ref
    tmp_ref_path = Path(temp_path) / f"{key.replace('/', '__')}.parquet.tmp"

    final_ref_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_ref_path.parent.mkdir(parents=True, exist_ok=True)

    if tmp_ref_path.exists():
        if tmp_ref_path.is_dir():
            shutil.rmtree(tmp_ref_path, ignore_errors=True)
        else:
            tmp_ref_path.unlink(missing_ok=True)

    parser_used = None
    last_error = None

    # Building using virtualizarr
    for parser in (HDFParser(), NetCDF3Parser()):
        try:
            with vz.open_virtual_dataset(
                url= source_url,
                registry= registry,
                parser= parser,
                loadable_variables=[],
            ) as vds: #Kerchunking
                vds.vz.to_kerchunk(
                    filepath= tmp_ref_path,
                    format= "parquet",
                    record_size= record_size,
                    categorical_threshold= categorical_threshold,
                )
            parser_used= parser.__class__.__name__
            last_error= None
            break
        except Exception as exc:
            last_error= exc
    
    if last_error is not None:
        return {
            "key": key,
            "flow_id": flow_id,
            "status": "failed",
            "error": f"{type(last_error).__name__}: {last_error}",
        }
    #Remove files in _tmp if no errors and final_ref_path found
    if final_ref_path.exists():
        if final_ref_path.is_dir():
            shutil.rmtree(final_ref_path, ignore_errors=True)
        else:
            final_ref_path.unlink(missing_ok=True) 
    
    os.replace(tmp_ref_path, final_ref_path)
    return {
        "key": key,
        "flow_id": flow_id,
        "status": "generated",
        "parser": parser_used,
        "reference_path": str(final_ref_path),
    }

"""
S3 object removed -> remove corresponding reference
"""
def remove_deleted_references(
    *,
    staging_volume_path: str,
    deleted_keys: list[str],
) -> dict[str, int]:
    removed = 0
    missing = 0

    for key in deleted_keys:
        ref_path = Path(staging_volume_path) / reference_relpath_for_key(key)
        if ref_path.exists():
            if ref_path.is_dir():
                shutil.rmtree(ref_path, ignore_errors=True)
            else:
                ref_path.unlink(missing_ok=True)
            removed += 1
        else:
            missing += 1

    return {"removed": removed, "missing": missing}

"""
Multi-threaded execution to build references
"""
def parallelise_generate_references(
    *,
    kp: dict[str, Any],
    access_key: str,
    secret_key: str,
    ledger_diff: dict[str, list[str]],
    current_objects: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    bucket = kp["s3"]["bucket"]
    out_cfg = kp["output"]
    exec_cfg = kp.get("execution", {})

    staging_volume_path = out_cfg["staging_volume_path"]
    temp_path = out_cfg["temp_path"]

    max_workers = _resolve_workers(exec_cfg.get("max_workers", "auto"))
    record_size = int(exec_cfg.get("parquet_record_size", 100000))
    categorical_threshold = int(exec_cfg.get("parquet_categorical_threshold", 10))

    keys = _keys_to_generate(ledger_diff)
    deleted_keys = sorted(ledger_diff.get("deleted", []))

    if not keys:
        delete_summary = remove_deleted_references(
            staging_volume_path=staging_volume_path,
            deleted_keys=deleted_keys,
        )
        summary = {
            "scanned": len(current_objects),
            "changed_or_new": 0,
            "generated": 0,
            "skipped": 0,
            "failed": 0,
            "deleted_refs_removed": delete_summary["removed"],
            "deleted_refs_missing": delete_summary["missing"],
            "timestamp": _utc_now_iso(),
        }
        return {"summary": summary, "results": [], "failures": []}

    registry = _build_registry(kp, access_key, secret_key)

    results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(
                generate_one_reference,
                key=key,
                bucket=bucket,
                registry=registry,
                staging_volume_path=staging_volume_path,
                temp_path=temp_path,
                current_objects=current_objects,
                record_size=record_size,
                categorical_threshold=categorical_threshold,
            )
            for key in keys
        ]

        for fut in as_completed(futures):
            item = fut.result()
            results.append(item)
            if item["status"] == "failed":
                failures.append(item)

    delete_summary = remove_deleted_references(
        staging_volume_path=staging_volume_path,
        deleted_keys=deleted_keys,
    )

    summary = {
        "scanned": len(current_objects),
        "changed_or_new": len(keys),
        "generated": len(results) - len(failures),
        "skipped": 0,
        "failed": len(failures),
        "deleted_refs_removed": delete_summary["removed"],
        "deleted_refs_missing": delete_summary["missing"],
        "timestamp": _utc_now_iso(),
    }

    return {
        "summary": summary,
        "results": sorted(results, key=lambda x: x["key"]),
        "failures": sorted(failures, key=lambda x: x["key"]),
    }
