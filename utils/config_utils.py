from __future__ import annotations

import re
import os
import sys
from pathlib import Path
from importlib import import_module
from importlib.metadata import version, PackageNotFoundError
from pathlib import Path
from typing import Any

import yaml


REQUIRED_EXACT = {
    "fsspec": "2026.3.0",
    "s3fs": "2026.3.0",
    "kerchunk": "0.2.10",
    "virtualizarr": "2.6.0",
}

REQUIRED_MODULES = [
    "dask"
    "fsspec",
    "fastparquet",
    "s3fs",
    "xarray",
    "zarr",
    "kerchunk",
    "virtualizarr",
]


def check_runtime_readiness() -> dict[str, str]:
    errors = []
    report: dict[str, str] = {}

    if sys.version_info < (3, 12):
        errors.append("Python 3.12+ required for virtualizarr==2.6.0.")

    for pkg, expected in REQUIRED_EXACT.items():
        try:
            got = version(pkg)
            report[pkg] = got
            if got != expected:
                errors.append(f"{pkg} expected {expected}, got {got}")
        except PackageNotFoundError:
            errors.append(f"{pkg} missing")

    for mod in REQUIRED_MODULES:
        try:
            import_module(mod)
        except Exception as exc:
            errors.append(f"import {mod} failed: {type(exc).__name__}: {exc}")

    if report.get("fsspec") and report.get("s3fs") and report["fsspec"] != report["s3fs"]:
        errors.append(f"fsspec/s3fs mismatch: {report['fsspec']} vs {report['s3fs']}")

    if errors:
        raise RuntimeError("Runtime readiness FAILED:\n- " + "\n- ".join(errors))

    return report


def load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh)
    if not isinstance(payload, dict):
        raise ValueError("Top-level YAML must be a mapping.")
    return payload


def validate_pipeline_schema(kp: dict[str, Any]) -> None:
    required_top = ["s3", "source_flows", "output", "execution"]
    missing_top = [k for k in required_top if k not in kp]
    if missing_top:
        raise ValueError(f"Missing kerchunk_pipeline sections: {missing_top}")

    s3 = kp["s3"]
    for k in ["endpoint_url", "bucket", "secret_scope"]:
        if not s3.get(k):
            raise ValueError(f"Missing s3.{k}")

    flows = kp["source_flows"]
    if not isinstance(flows, list) or len(flows) == 0:
        raise ValueError("source_flows must be a non-empty list")

    for idx, flow in enumerate(flows):
        if not isinstance(flow, dict):
            raise ValueError(f"source_flows[{idx}] must be a mapping")
        flow_id = flow.get("id")
        if not flow_id:
            raise ValueError(f"source_flows[{idx}].id is required")

        mode = flow.get("mode")
        if mode not in {"prefix_regex", "exact_key"}:
            raise ValueError(f"source_flows[{idx}] has unsupported mode: {mode}")

        if mode == "prefix_regex":
            if not flow.get("prefix"):
                raise ValueError(f"source_flows[{idx}].prefix is required for prefix_regex")
            key_regex = flow.get("key_regex")
            if not key_regex:
                raise ValueError(f"source_flows[{idx}].key_regex is required for prefix_regex")
            re.compile(key_regex)

        if mode == "exact_key":
            if not flow.get("exact_key"):
                raise ValueError(f"source_flows[{idx}].exact_key is required for exact_key")

    out = kp["output"]
    for k in ["staging_volume_path", "ledger_path", "temp_path"]:
        if not out.get(k):
            raise ValueError(f"Missing output.{k}")

    exec_cfg = kp["execution"]
    if "list_page_size" in exec_cfg:
        if int(exec_cfg["list_page_size"]) <= 0:
            raise ValueError("execution.list_page_size must be > 0")

    raw_workers = exec_cfg.get("max_workers", "auto")
    if raw_workers not in (None, "auto"):
        if int(raw_workers) <= 0:
            raise ValueError("execution.max_workers must be > 0 when numeric")


def load_pipeline_config(config_path: str | Path) -> dict[str, Any]:
    cfg = load_yaml(config_path)
    kp = cfg.get("kerchunk_pipeline")
    if not isinstance(kp, dict):
        raise ValueError("Missing or invalid kerchunk_pipeline section")
    validate_pipeline_schema(kp)
    return kp

def find_env_file(filename= "s3_connect.txt",env_dir=".env"):
    """Search upwards from current file to find the .env/filename"""
    curr_path= Path(__file__).resolve().parent

    for parent in [curr_path, *curr_path.parents]:
        env_path = parent / env_dir / filename
        if env_path.exists():
            return env_path
    raise FileNotFoundError(f"Could not find {env_dir}/{filename} in any parent folder")

def resolve_secrets(kp:dict[str, Any]) -> tuple[str, str]:
    secret_path= find_env_file()

    secrets={}
    with open(secret_path,"r") as f:
        for line in f:
            if "=" in line:
                key, value= line.strip.split("=",1)
                secrets[key]= value
    
    access_key= secrets.get("ACCESS_KEY")
    secret_key= secrets.get("SECRET_KEY")

    if not access_key or not secret_key:
        raise ValueError(f"Secrets file at {secret_path} is missing required keys.")
        
    return access_key, secret_key
