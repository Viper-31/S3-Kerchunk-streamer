"""Generate one Kerchunk parquet reference per source object."""
from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import virtualizarr as vz
import s3fs
import xarray as xr
from obstore.store import from_url
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser

import dask
from dask.distributed import Client

"""Return current UTC timestamp in ISO format."""
def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()

"""Resolve worker count from config, defaulting to a conservative CPU-based value."""
def _resolve_workers(raw: Any) -> int:
    if raw in (None, "auto"):
        return max(1, min(8, os.cpu_count()))
    return max(1, int(raw))

"""Return sorted unique keys that need reference generation."""
def _keys_to_generate(diff: dict[str, list[str]]) -> list[str]:
    return sorted(set(diff.get("new", []) + diff.get("changed", [])))


"""Map a source key to a stable parquet reference output path."""
def reference_relpath_for_key(source_key: str) -> str:
    return f"refs/{source_key}.parquet"

"""
Tmp and final staging paths to drop Kerchunk Parquet reference files.
Paths configured in config.yaml
"""
@dataclass(frozen=True)
class ReferencePaths:
    final_ref_path: Path
    tmp_ref_path: Path

def build_reference_paths(key: str, staging_volume_path: str, temp_path: str) -> ReferencePaths:
    return ReferencePaths(
        final_ref_path=Path(staging_volume_path) / reference_relpath_for_key(key),
        tmp_ref_path=Path(temp_path) / f"{key.replace('/', '__')}.tmp.parquet",
    )

def prepare_temp_target(tmp_ref_path: Path) -> None:
    tmp_ref_path.parent.mkdir(parents=True, exist_ok=True)
    if tmp_ref_path.exists():
        if tmp_ref_path.is_dir():
            shutil.rmtree(tmp_ref_path, ignore_errors=True)
        else:
            tmp_ref_path.unlink(missing_ok=True)

"""
Inspect dtypes and choose parser configuration. Fixes AttributeError: 'bytes' object has no attribute .item()
If variables is String/Object/U dtype, drop the string_variable in HDFParser.
"""
def select_parser(fs: s3fs.S3FileSystem, bucket: str, key: str) -> tuple[HDFParser, list[str]]:
    with fs.open(f"{bucket}/{key}", "rb") as fh:
        ds_real = xr.open_dataset(fh, engine="h5netcdf")
        string_vars = [
            var_name
            for var_name in ds_real.variables
            if ds_real[var_name].dtype.kind in ("S", "U", "O")
        ]

    parser = HDFParser(drop_variables=string_vars) if string_vars else HDFParser()
    return parser, string_vars

"""Ensures that on re-run of pipeline, old references are removed and new references dropped into final_ref_path"""
def commit_reference(tmp_ref_path: Path, final_ref_path: Path) -> None:
    if final_ref_path.exists():
        if final_ref_path.is_dir():
            shutil.rmtree(final_ref_path, ignore_errors=True)
        else:
            final_ref_path.unlink(missing_ok=True)

    os.replace(tmp_ref_path, final_ref_path)


"""Atomically write a JSON payload by using a temp file and replace."""
def _write_json_atomic(path: str, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
    os.replace(tmp, target)

def save_ledger_after_success(
    ledger_path: str,
    next_ledger: dict[str, Any],
    generation_summary: dict[str, Any],
) -> None:
    # Persist the next ledger only when reference generation has no failures.
    if generation_summary.get("failed", 0) > 0:
        raise RuntimeError(
            "Ref generation had failures; ledger update is blocked to prevent drift."
        )
    _write_json_atomic(ledger_path, next_ledger)


"""Build an ObjectStore registry for authenticated reads from the configured bucket."""
def _build_registry(kp: dict[str, Any], access_key: str, secret_key: str) -> ObjectStoreRegistry:
    s3_cfg = kp["s3"]
    bucket_name = s3_cfg["bucket"]
    bucket_url = f"s3://{bucket_name}"

    store = from_url(
        bucket_url,
        endpoint=s3_cfg["endpoint_url"],
        region=s3_cfg.get("region_name", "us-east-1"),
        access_key_id=access_key,
        secret_access_key=secret_key,
        virtual_hosted_style_request=False, 
    )

    return ObjectStoreRegistry({bucket_url: store})

"""Generate a parquet reference for one source object with atomic output replace."""
def generate_reference_for_object(
    *,
    key: str,
    bucket: str,
    access_key: str,
    secret_key: str,
    s3_config: dict[str, Any],
    registry: ObjectStoreRegistry,
    staging_volume_path: str,
    temp_path: str,
    current_objects: dict[str, dict[str, Any]],
    record_size: int,
    categorical_threshold: int,
) -> dict[str, Any]:
    source_url = f"s3://{bucket}/{key}"
    flow_id = current_objects.get(key, {}).get("flow_id", "unknown")

    ref_paths = build_reference_paths(key, staging_volume_path, temp_path)
    final_ref_path = ref_paths.final_ref_path
    tmp_ref_path = ref_paths.tmp_ref_path

    final_ref_path.parent.mkdir(parents=True, exist_ok=True)
    prepare_temp_target(tmp_ref_path)

    parser_used = None
    last_error = None

    try:
        # Initialize S3FS for metadata string inspection
        fs = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={"endpoint_url": s3_config["endpoint_url"]},
            config_kwargs={"signature_version": "s3v4", "s3": {"addressing_style": "path"}}
        )

        parser, string_vars = select_parser(fs, bucket, key)
        
        vds = vz.open_virtual_dataset(
            url=source_url,
            registry=registry,
            parser=parser,
            loadable_variables=[],
        )

        if string_vars:
            with fs.open(f"{bucket}/{key}", "rb") as f:
                ds_real = xr.open_dataset(f, engine="h5netcdf")
                for var in string_vars:
                    if var in ds_real:
                        vds = vds.assign_coords({var: ds_real[var].load()})

        # Export to Kerchunk Parquet
        vds.vz.to_kerchunk(
            filepath=str(tmp_ref_path),
            format="parquet",
            record_size=record_size,
            categorical_threshold=categorical_threshold,
        )
        
        parser_used = f"{parser.__class__.__name__} (strings_handled={len(string_vars)})"
        last_error = None

    except Exception as exc:
        last_error = exc

    if last_error is not None:
        return {
            "key": key,
            "flow_id": flow_id,
            "status": "failed",
            "error": f"{type(last_error).__name__}: {last_error}",
        }

    commit_reference(tmp_ref_path, final_ref_path)
    
    return {
        "key": key,
        "flow_id": flow_id,
        "status": "generated",
        "parser": parser_used,
        "reference_path": str(final_ref_path),
    }


"""Remove references for objects that disappeared from source inventory."""
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


"""Generate references concurrently and return summary, results, and failures."""
def concurrent_dask_ref_generation(
    *,
    kp: dict[str, Any],
    access_key: str,
    secret_key: str,
    inventory_diff: dict[str, list[str]],
    current_objects: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    bucket = kp["s3"]["bucket"]
    out_cfg = kp["output"]
    exec_cfg = kp.get("execution", {})

    staging_volume_path = out_cfg["staging_volume_path"]
    temp_path = out_cfg["temp_path"]

    record_size = int(exec_cfg.get("parquet_record_size", 100000))
    categorical_threshold = int(exec_cfg.get("parquet_categorical_threshold", 10))

    keys = _keys_to_generate(inventory_diff)
    deleted_keys = sorted(inventory_diff.get("deleted", []))

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

    raw_workers= exec_cfg.get("max_workers")
    workers_number= _resolve_workers(raw_workers)
    client= Client(n_workers=workers_number,threads_per_worker=1)
    tasks= []
    for key in keys:
        task= dask.delayed(generate_reference_for_object) (
            key=key,
            bucket=bucket,
            access_key=access_key,
            secret_key=secret_key,
            s3_config=kp["s3"],
            registry=registry,
            staging_volume_path=staging_volume_path,
            temp_path=temp_path,
            current_objects=current_objects,
            record_size=record_size,
            categorical_threshold=categorical_threshold,
        )
        tasks.append(task)

    results= dask.compute(*tasks)
    failures= [r for r in results if r["status"]=="failed"]        

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
