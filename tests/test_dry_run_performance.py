import time
import pytest
from pathlib import Path
import shutil
from utils.config_utils import load_pipeline_config, resolve_secrets
from pipeline.generate_parquet import generate_reference_for_object, _build_registry

def test_dry_run_performance():
    """Benchmark Kerchunk generation for specific DPIRD and ECMWF keys."""
    # Setup paths and config
    repo_root = Path(__file__).parent.parent
    kp = load_pipeline_config(repo_root / "configs/config.yaml")
    ACCESS_KEY, SECRET_KEY = resolve_secrets(kp)
    
    registry = _build_registry(kp, ACCESS_KEY, SECRET_KEY)
    
    test_keys = [
        "FINAL_DPIRD/DPIRD_final_stations.nc",
        "ecmwf_op_clean/2024/02/06.nc"
    ]
    
    # Target .tmp directory relative to project root
    tmp_dir = repo_root / ".tmp"
    work_dir = tmp_dir / "work"
    
    # Clean and prepare directories
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "="*50)
    print("DRY RUN PERFORMANCE BENCHMARK")
    print("="*50)

    for key in test_keys:
        start_time = time.time()
        
        result = generate_reference_for_object(
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

        assert result["status"] == "generated", f"Failed to generate reference for {key}"

    print("="*50)
