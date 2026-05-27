from __future__ import annotations
from packaging.version import parse as parse_version

import re
import os
import sys
import tomllib
from pathlib import Path
from importlib import import_module
from importlib.metadata import version, PackageNotFoundError
from pathlib import Path
from typing import Any

import yaml


REQUIRED_MIN_VERSION = {
    "fsspec": "2026.3.0",
    "s3fs": "2026.3.0",
}

REQUIRED_MODULES = [
    "distributed",
    "fsspec",
    "fastparquet",
    "obstore",
    "xarray",
    "zarr",
    "kerchunk",
    "virtualizarr",
]


def check_runtime_readiness() -> dict[str, str]:
    errors = []
    report: dict[str, str] = {}

    if sys.version_info < (3, 12):
        errors.append("Python 3.12+ required for virtualizarr=")

    for pkg, min_ver in REQUIRED_MIN_VERSION.items():
        try:
            current_pkg = version(pkg)
            report[pkg] = current_pkg
            if parse_version(current_pkg) < parse_version(min_ver):
                errors.append(f"{pkg} expected >={min}, got {current_pkg}")
        except PackageNotFoundError:
            errors.append(f"{pkg} missing")

    for mod in REQUIRED_MODULES:
        try:
            import_module(mod)
        except Exception as exc:
            errors.append(f"import {mod} failed: {type(exc).__name__}: {exc}")

    if (
        report.get("fsspec")
        and report.get("s3fs")
        and report["fsspec"] != report["s3fs"]
    ):
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
    for k in ["endpoint_url", "bucket", "project_scope"]:
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
        if mode not in {"prefix_regex", "prefix_glob", "exact_key"}:
            raise ValueError(f"source_flows[{idx}] has unsupported mode: {mode}")

        if mode == "prefix_regex":
            if not flow.get("prefix"):
                raise ValueError(
                    f"source_flows[{idx}].prefix is required for prefix_regex"
                )
            key_regex = flow.get("key_regex")
            if not key_regex:
                raise ValueError(
                    f"source_flows[{idx}].key_regex is required for prefix_regex"
                )
            re.compile(key_regex)

        if mode == "prefix_glob":
            if not flow.get("prefix"):
                raise ValueError(
                    f"source_flows[{idx}].prefix is required for prefix_glob"
                )
            key_glob = flow.get("key_glob")
            if not key_glob:
                raise ValueError(
                    f"source_flows[{idx}].key_glob is required for prefix_glob"
                )

        if mode == "exact_key":
            if not flow.get("exact_key"):
                raise ValueError(
                    f"source_flows[{idx}].exact_key is required for exact_key"
                )

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


def find_env_file(filename="s3_connect.toml", env_dir=".env"):
    """Search upwards from current file to find the .env/filename"""
    curr_path = Path(__file__).resolve().parent

    for parent in [curr_path, *curr_path.parents]:
        env_path = parent / env_dir / filename
        if env_path.exists():
            return env_path
    raise FileNotFoundError(f"Could not find {env_dir}/{filename} in any parent folder")


def resolve_secrets(kp: dict[str, Any]) -> tuple[str, str]:
    # Check for GitHub environment variables
    access_key = os.environ.get("ACACIA_ACCESS_KEY")
    secret_key = os.environ.get("ACACIA_SECRET_KEY")

    if access_key and secret_key:
        return access_key, secret_key
    try:
        secret_path = find_env_file()
        project_scope = kp["s3"]["project_scope"]

        with open(secret_path, "rb") as f:
            secret_data = tomllib.load(f)
            access_key = str(secret_data[project_scope]["aws_access_key_id"]).strip()
            secret_key = str(secret_data[project_scope]["aws_secret_access_key"]).strip()

    except FileNotFoundError as exc:
        print(exc)
    
    except KeyError as exc:
        raise KeyError("See README.md to provide Acacia access and secret keys in repo env file")

    if not access_key or not secret_key:
        raise ValueError(f"Secrets file at {secret_path} is missing required keys.")

    return access_key, secret_key
