from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import virtualizarr as vz
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import KerchunkParquetParser

from pipeline.generate_parquet import (
    _build_registry,
    reference_relpath_for_key,
    regenerate_missing_flow_references,
)


ECMWF_FLOW_ID = "ecmwf_weekly_nc"
ECMWF_CONSOLIDATED_RELPATH = (
    Path("refs") / "ECMWF_consolidated" / "ecmwf_combined.nc.parq"
)


@dataclass(frozen=True)
class FlowInventory:
    current_objects: dict[str, dict[str, Any]]
    previous_objects: dict[str, dict[str, Any]] = field(default_factory=dict)
    flow_id: str = ECMWF_FLOW_ID


@dataclass(frozen=True)
class StagingConfig:
    staging_volume_path: Path

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "staging_volume_path",
            Path(self.staging_volume_path).expanduser().resolve()
        )


@dataclass(frozen=True)
class ParquetWriteConfig:
    record_size: int
    categorical_threshold: int


@dataclass(frozen=True)
class ConsolidationInputs:
    inventory_diff: dict[str, list[str]]
    inventory: FlowInventory
    staging: StagingConfig

#Mark for deletion
def create_ecmwf_consolidated_ref_path(staging_volume_path: str | Path) -> Path:
    return Path(staging_volume_path) / ECMWF_CONSOLIDATED_RELPATH


def _path_to_file_uri(path:Path) -> str:
    return path.resolve().as_uri()


def expected_ecmwf_reference_paths(
    *,
    inventory: FlowInventory,
    staging: StagingConfig,
) -> list[Path]:
    """Use inventory.json as the source of truth for weekly ECMWF inputs."""
    return [
        staging.staging_volume_path / reference_relpath_for_key(source_key)
        for source_key in source_key_sorting(
            inventory.current_objects,
            inventory.flow_id,
        )
    ]


def unusable_ecmwf_reference_keys(
    *,
    inventory: FlowInventory,
    staging: StagingConfig,
    reference_probe: Callable[[Path], None] | None = None,
    registry: ObjectStoreRegistry | None = None,
) -> list[str]:
    """Identify weekly ECMWF refs that must be rebuilt for a usable consolidation."""
    probe = reference_probe or (
        lambda path: _probe_dataset_using_parquet(path, registry=registry)
    )

    unusable_keys: list[str] = []
    for source_key in source_key_sorting(
        inventory.current_objects,
        inventory.flow_id,
    ):
        ref_path = (
            staging.staging_volume_path / reference_relpath_for_key(source_key)
        )
        if not ref_path.exists():
            print(f"[ecmwf probe] missing: {source_key} -> {ref_path}")
            unusable_keys.append(source_key)
            continue

        try:
            probe(ref_path)
        except Exception as exc:
            print(f"[ecmwf_probe] unreadable: {source_key}")
            print(f"{type(exc).__name__}: {exc}")
            unusable_keys.append(source_key)

    return unusable_keys


def consolidate_on_inventory_diff(*, inputs: ConsolidationInputs) -> bool:
    """Avoid unnecessary work while ensuring missing output gets rebuilt."""
    if not create_ecmwf_consolidated_ref_path(
        inputs.staging.staging_volume_path
    ).exists():
        return True

    for source_key in inputs.inventory_diff.get("new", []) + inputs.inventory_diff.get(
        "changed", []
    ):
        if _key_matches_flow(
            inputs.inventory.current_objects,
            source_key,
            inputs.inventory.flow_id,
        ):
            return True

    for source_key in inputs.inventory_diff.get("deleted", []):
        if _key_matches_flow(
            inputs.inventory.previous_objects,
            source_key,
            inputs.inventory.flow_id,
        ):
            return True

    return False


def regenerate_unusable_ecmwf_references(                                             
    *,                                                                                
    client: Any,                                                                      
    kp: dict[str, Any],
    access_key: str,
    secret_key: str,
    inventory: FlowInventory,
    staging: StagingConfig,
    reference_probe: Callable[[Path], None] | None = None,
    registry: ObjectStoreRegistry | None = None,
) -> dict[str, Any]:
    """Keep weekly refs uncorrupted by regenerating once if unusable by _probe_dataset_using_parquet."""
    registry = registry or _build_registry(kp, access_key, secret_key)

    unusable_keys = unusable_ecmwf_reference_keys(
        inventory=inventory,
        staging=staging,
        reference_probe=reference_probe,
        registry=registry,
    )
    if not unusable_keys:
        return {
            "unusable_keys": [],
            "regeneration": {"missing_keys": [], "results": [], "failures": []},
            "remaining_unusable_keys": [],
        }

    print(f"Regenerating unusable ECMWF refs for flow_id={inventory.flow_id}:")
    for source_key in unusable_keys:
        print(f" - {source_key}")

    _remove_reference_paths_for_keys(
        source_keys=unusable_keys,
        staging_volume_path=staging.staging_volume_path,
    )

    regeneration = regenerate_missing_flow_references(
        client=client,
        kp=kp,
        access_key=access_key,
        secret_key=secret_key,
        current_objects=inventory.current_objects,
        flow_id=inventory.flow_id,
    )

    failures = regeneration.get("failures", [])
    if failures:
        raise RuntimeError(
            "ECMWF reference regeneration failed after one retry: "
            f"{failures}"
        )

    remaining_unusable_keys = unusable_ecmwf_reference_keys(
        inventory=inventory,
        staging=staging,
        reference_probe=reference_probe,
        registry=registry,
    )
    if remaining_unusable_keys:
        raise RuntimeError(
            "ECMWF references are still missing or unreadable after one retry: "
            f"{remaining_unusable_keys}"
        )

    return {
        "unusable_keys": unusable_keys,
        "regeneration": regeneration,
        "remaining_unusable_keys": [],
    }


def consolidate_ecmwf_references(
    *,
    inventory: FlowInventory,
    staging: StagingConfig,
    write_config: ParquetWriteConfig,
    registry: ObjectStoreRegistry | None = None,
) -> dict[str, Any]:
    """Produce the consolidated ECMWF reference so consumers can open one dataset."""
    input_paths = expected_ecmwf_reference_paths(
        inventory=inventory,
        staging=staging,
    )
    input_uris = [_path_to_file_uri(path) for path in input_paths]
    output_path = create_ecmwf_consolidated_ref_path(staging.staging_volume_path)
    registry = registry or ObjectStoreRegistry()

    try:
        if not input_paths:
            raise ValueError(
                f"No current objects found for flow_id={inventory.flow_id}"
            )

        vds = vz.open_virtual_mfdataset(
            input_uris,
            registry=registry,
            parser=KerchunkParquetParser(),
            combine="nested",
            concat_dim="time",
            parallel="dask",
            loadable_variables=[],
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        _remove_path(output_path)
        vds.vz.to_kerchunk(
            filepath=str(output_path),
            format="parquet",
            record_size=write_config.record_size,
            categorical_threshold=write_config.categorical_threshold,
        )
    except Exception as exc:
        return {
            "status": "failed",
            "input_count": len(input_paths),
            "input_paths": [str(path) for path in input_paths],
            "reference_path": str(output_path),
            "error": f"{type(exc).__name__}: {exc}",
        }

    return {
        "status": "generated",
        "input_count": len(input_paths),
        "reference_path": str(output_path),
        "input_paths": [str(path) for path in input_paths],
    }


def run_ecmwf_consolidation(
    *,
    client: Any,
    kp: dict[str, Any],
    access_key: str,
    secret_key: str,
    inputs: ConsolidationInputs,
) -> dict[str, Any]:
    """Coordinate recovery + consolidation keeping interface is lean."""
    out_cfg = kp["output"]
    exec_cfg = kp.get("execution", {})
    output_path = create_ecmwf_consolidated_ref_path(
        inputs.staging.staging_volume_path
    )

    if not consolidate_on_inventory_diff(inputs=inputs):
        return {
            "status": "skipped",
            "reason": "no_ecmwf_changes",
            "reference_path": str(output_path),
        }

    registry = _build_registry(kp, access_key, secret_key)
    repair = regenerate_unusable_ecmwf_references(
        client=client,
        kp=kp,
        access_key=access_key,
        secret_key=secret_key,
        inventory=inputs.inventory,
        staging=inputs.staging,
        registry=registry,
    )

    result = consolidate_ecmwf_references(
        inventory=inputs.inventory,
        staging=inputs.staging,
        write_config=ParquetWriteConfig(
            record_size=exec_cfg["parquet_record_size"],
            categorical_threshold=exec_cfg["categorical_threshold"],
        ),
        registry=registry,
    )
    result["repair"] = repair
    return result


def source_key_sorting(
    objects: dict[str, dict[str, Any]],
    flow_id: str,
) -> list[str]:
    return sorted(
        source_key
        for source_key, row in objects.items()
        if row.get("flow_id") == flow_id
    )


def _key_matches_flow(
    objects: dict[str, dict[str, Any]],
    source_key: str,
    flow_id: str,
) -> bool:
    return objects.get(source_key, {}).get("flow_id") == flow_id


def _probe_dataset_using_parquet(
    ref_path: Path,
    *,
    registry: ObjectStoreRegistry | None = None,
) -> None:
    vz.open_virtual_dataset(
        _path_to_file_uri(ref_path),
        registry=registry or ObjectStoreRegistry(),
        parser=KerchunkParquetParser(),
        loadable_variables=[],
    )


def _remove_reference_paths_for_keys(
    *,
    source_keys: list[str],
    staging_volume_path: str | Path,
) -> None:
    for source_key in source_keys:
        _remove_path(Path(staging_volume_path) / reference_relpath_for_key(source_key))


def _remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    elif path.exists():
        path.unlink(missing_ok=True)


__all__ = [
    "ECMWF_FLOW_ID",
    "consolidate_ecmwf_references",
    "ConsolidationInputs",
    "FlowInventory",
    "ParquetWriteConfig",
    "StagingConfig",
    "consolidate_on_inventory_diff",
    "create_ecmwf_consolidated_ref_path",
    "expected_ecmwf_reference_paths",
    "regenerate_unusable_ecmwf_references",
    "run_ecmwf_consolidation",
    "source_key_sorting",
    "unusable_ecmwf_reference_keys",
]
