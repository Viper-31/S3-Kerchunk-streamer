# End-to-end dry run test with sample DPIRD and ECMWF datasets
import os
import time
import pytest
import xarray as xr
import warnings
import s3fs
from pathlib import Path

from pipeline.inventory import build_storage_clients
from utils.config_utils import load_pipeline_config, resolve_secrets
from pipeline import generate_parquet as gp

pytestmark = pytest.mark.e2e

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification*",
    category=UserWarning,
)


@pytest.fixture(scope="module")
def setup_tmp_env():
    """Fixture to load configs, secrets and setup output dirs once."""
    repo_root = Path(__file__).parent.parent
    kp = load_pipeline_config(repo_root / "configs/config.yaml")
    ACCESS_KEY, SECRET_KEY = resolve_secrets(kp)

    fs, _ = build_storage_clients(kp, ACCESS_KEY, SECRET_KEY)
    s3_opts = fs.storage_options.copy()
    s3_opts["asynchronous"] = False
    kerchunk_opts = {"remote_protocol": "s3", "remote_options": s3_opts}

    registry = gp._build_registry(kp, ACCESS_KEY, SECRET_KEY)

    tmp_dir = repo_root / ".tmp"
    work_dir = repo_root / ".tmp" / "work"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    return {
        "kp": kp,
        "access": ACCESS_KEY,
        "secret": SECRET_KEY,
        "registry": registry,
        "kerchunk_opts": kerchunk_opts,
        "tmp_dir": tmp_dir,
        "work_dir": work_dir,
    }


@pytest.fixture
def perf_tracker(monkeypatch):
    """Fixture to track timings and S3 GET requests during the test."""
    timings = {"select_parser": [], "enrich_string_variables": []}
    range_stats = {"get_object": 0, "get_object_with_range": 0}

    orig_parser_func = gp.select_parser

    def timed_parser(*args, **kwargs):
        t0 = time.time()
        try:
            return orig_parser_func(*args, **kwargs)
        finally:
            timings["select_parser"].append(time.time() - t0)

    orig_enrich_string_func = gp.enrich_string_variables

    def timed_enrich(*args, **kwargs):
        t0 = time.time()
        try:
            return orig_enrich_string_func(*args, **kwargs)
        finally:
            timings["enrich_string_variables"].append(time.time() - t0)

    orig_call_s3_func = s3fs.S3FileSystem._call_s3

    def wrapped_call_s3(self, method, *args, **kwargs):
        if method == "get_object":
            range_stats["get_object"] += 1
            if "Range" in (kwargs.get("headers") or {}):
                range_stats["get_object_with_range"] += 1
        return orig_call_s3_func(self, method, *args, **kwargs)

    monkeypatch.setattr(gp, "select_parser", timed_parser)
    monkeypatch.setattr(gp, "enrich_string_variables", timed_enrich)
    monkeypatch.setattr(s3fs.S3FileSystem, "_call_s3", wrapped_call_s3)

    yield timings, range_stats


def record_github_summary(source_key, duration, timings, range_stats):
    """Persists benchmark results to GitHub Actions UI"""
    parser_avg = (
        sum(timings["select_parser"]) / len(timings["select_parser"])
        if timings["select_parser"]
        else 0
    )
    enrich_avg = (
        sum(timings["enrich_string_variables"])
        / len(timings["enrich_string_variables"])
        if timings["enrich_string_variables"]
        else 0
    )

    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_file:
        return

    markdown_row = (
        f"| `{source_key}` | {duration:.2f}s | {parser_avg:.4f}s | {enrich_avg:.4f}s | "
        f"{range_stats['get_object']} | {range_stats['get_object_with_range']} |\n"
    )

    with open(summary_file, "a") as f:
        # Write headers if it's the first entry
        if f.tell() == 0:
            f.write("### Dry Run Performance Benchmarks\n\n")
            f.write(
                "| Source Key | Total Duration | Avg Select Parser | Avg Enrich String | GET calls | GET w/Range |\n"
            )
            f.write(
                "|------------|----------------|-------------------|-------------------|-----------|--------------|\n"
            )
        f.write(markdown_row)


@pytest.mark.parametrize(
    "source_key, dataset_type",
    [
        ("DPIRD/dpird_wa_stations.nc", "DPIRD"),
        ("ECMWF/2024/02/06.nc", "ECMWF"),
    ],
)
def test_dry_run_performance(source_key, dataset_type, setup_tmp_env, perf_tracker):
    """Benchmark Kerchunk generation and validate output integrity"""
    timings, range_stats = perf_tracker
    start_time = time.time()

    result = gp.generate_reference_for_object(
        source_key=source_key,
        bucket=setup_tmp_env["kp"]["s3"]["bucket"],
        access_key=setup_tmp_env["access"],
        secret_key=setup_tmp_env["secret"],
        s3_config=setup_tmp_env["kp"]["s3"],
        registry=setup_tmp_env["registry"],
        staging_volume_path=str(setup_tmp_env["tmp_dir"]),
        temp_path=str(setup_tmp_env["work_dir"]),
        current_objects={source_key: {"flow_id": "dry-run-test"}},
        record_size=100000,
        categorical_threshold=10,
    )

    duration = time.time() - start_time
    record_github_summary(source_key, duration, timings, range_stats)

    # Generation success
    assert result["status"] == "generated", (
        f"Failed to generate reference for {source_key}: {result.get('error')}"
    )

    # Assert parquet reference exists
    ref_parquet_path = setup_tmp_env["tmp_dir"] / "refs" / f"{source_key}.parq"
    assert ref_parquet_path.exists(), f"Parquet file missing at {ref_parquet_path}"

    # Assert xarray reads from .parq
    ds = xr.open_dataset(
        str(ref_parquet_path),
        engine="kerchunk",
        storage_options=setup_tmp_env["kerchunk_opts"],
    )

    if dataset_type == "DPIRD":
        assert "station" in ds.coords, "DPIRD missing 'station' coordinate"
        assert ds.station.size > 0, "DPIRD dataset is empty"
    elif dataset_type == "ECMWF":
        assert "t2m" in ds.data_vars, "ECMWF missing 't2m' variable"
        assert ds.t2m.shape == (14, 113, 111, 151), (
            f"Unexpected shape for t2m: {ds.t2m.shape}"
        )
        assert "valid_time" in ds.coords, "ECMWF missing 'valid_time' coordinate"
