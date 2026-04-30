# Run with: python -m pytest -s tests/dry_run_kerchunk.py

import time
import pytest
import s3fs
from pathlib import Path
import shutil
from utils.config_utils import load_pipeline_config, resolve_secrets
from pipeline import generate_parquet as gp
import warnings

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification*",
    category=UserWarning
)

"""Time wrappers for enrich_string_variables()"""
# Range GET requests issued and timings
def timing_wrapper_sub_functions(monkeypatch):
    timings = {"select_parser": [], "enrich_string_variables": []}
    range_stats = {"get_object": 0, "get_object_with_range": 0}

    # Time wrappers
    orig_select = gp.select_parser
    def timed_select(*args, **kwargs):
        t0 = time.time()
        try:
            return orig_select(*args, **kwargs)
        finally:
            timings["select_parser"].append(time.time() - t0)

    orig_enrich = gp.enrich_string_variables
    def timed_enrich(*args, **kwargs):
        t0 = time.time()
        try:
            return orig_enrich(*args, **kwargs)
        finally:
            timings["enrich_string_variables"].append(time.time() - t0)

    monkeypatch.setattr(gp, "select_parser", timed_select)
    monkeypatch.setattr(gp, "enrich_string_variables", timed_enrich)

    # Range GET detection via S3FS internal call
    orig_call_s3 = s3fs.S3FileSystem._call_s3
    def wrapped_call_s3(self, method, *args, **kwargs):
        if method == "get_object":
            range_stats["get_object"] += 1
            headers = kwargs.get("headers") or {}
            if "Range" in headers:
                range_stats["get_object_with_range"] += 1
        return orig_call_s3(self, method, *args, **kwargs)

    monkeypatch.setattr(s3fs.S3FileSystem, "_call_s3", wrapped_call_s3)
    
    return timings, range_stats

def test_dry_run_performance(monkeypatch):
    """Benchmark Kerchunk generation for specific DPIRD and ECMWF keys."""
    timings, range_stats = timing_wrapper_sub_functions(monkeypatch)
    
    repo_root = Path(__file__).parent.parent
    kp = load_pipeline_config(repo_root / "configs/config.yaml")
    ACCESS_KEY, SECRET_KEY = resolve_secrets(kp)
    
    registry = gp._build_registry(kp, ACCESS_KEY, SECRET_KEY)
    
    test_keys = [
        "FINAL_DPIRD/DPIRD_final_stations.nc",
        "ecmwf_op_clean/2024/02/06.nc"
    ]
    
    # Target .tmp directory relative to project root
    tmp_dir = repo_root / ".tmp"
    work_dir = repo_root / ".tmp" / "work"
    
    # Prepare directories (existing files will be overwritten during generation)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "="*50)
    print("DRY RUN PERFORMANCE BENCHMARK")
    print("="*50)

    for key in test_keys:
        start_time = time.time()
        
        result = gp.generate_reference_for_object(
            key=key,
            bucket=kp["s3"]["bucket"],
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            s3_config=kp["s3"],
            registry=registry,
            staging_volume_path=str(tmp_dir),
            temp_path=str(work_dir),
            current_objects={key: {"flow_id": "dry-run-test"}},
            record_size=100000,
            categorical_threshold=10
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"Key: {key}")
        print(f"Duration: {duration:.2f}s")
        print(f"Status: {result['status']}")
        if result['status'] == 'failed':
            print(f"Error: {result.get('error')}")
        print("-" * 30)

        sel_avg = sum(timings["select_parser"]) / len(timings["select_parser"]) if timings["select_parser"] else 0
        enr_avg = sum(timings["enrich_string_variables"]) / len(timings["enrich_string_variables"]) if timings["enrich_string_variables"] else 0
        
        print(f"select_parser avg: {sel_avg:.4f}s")
        print(f"enrich_string_variables avg: {enr_avg:.4f}s")
        print("get_object calls:", range_stats["get_object"])
        print("get_object with Range:", range_stats["get_object_with_range"])
        
        # Reset stats for the next key
        timings["select_parser"].clear()
        timings["enrich_string_variables"].clear()
        range_stats["get_object"] = 0
        range_stats["get_object_with_range"] = 0

        assert result["status"] == "generated", f"Failed to generate reference for {key}"

    print("="*50)
