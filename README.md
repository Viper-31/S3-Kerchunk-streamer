# S3 Kerchunk Streamer

A local Dask-powered batch pipeline that scans NetCDF objects hosted on Pawsey's Acacia (S3-compatible object storage) and generates [Kerchunk](https://fsspec.github.io/kerchunk/) Parquet references. These references allow downstream visualization and web applications to stream exact byte ranges (via [VirtualiZarr](https://virtualizarr.readthedocs.io/)) instead of downloading massive 2-5 GB source files in their entirety.

## Overview

This pipeline enables efficient, cloud-optimized access to large historical weather and climate datasets (such as ECMWF and DPIRD stations) without requiring full file downloads over network connections.

**Key Capabilities:**
- **Cloud-Optimized Access**: Converts traditional NetCDF S3 objects into virtual cloud-optimized datasets using Kerchunk and VirtualiZarr.
- **Incremental Processing**: Tracks `ETag`, `LastModified`, and `Size` in a local JSON inventory ledger to ensure only new or changed files are reprocessed.
- **Local Parallelism**: Leverages Dask for parallel reference generation, optimized for local multi-core environments.
- **Atomic Operations**: Implements atomic writes for both Parquet references and the inventory ledger to prevent state corruption.
- **Visualization Ready**: Consumer applications can open the metadata via `ReferenceFileSystem` and `xarray` to pull precise byte slices for interactive streaming.

## Project Structure

- Depedencies are managed via `uv` and pinned in `pyproject.toml` / `uv.lock`
- `configs/config.yaml`: Central configuration for S3 endpoints, source flow selectors (ECMWF, DPIRD), and output paths.
- `pipeline/inventory.py`: Logic for scanning S3, building inventory snapshots, and performing incremental diffing.
- `pipeline/generate_parquet.py`: Parallel generation of Kerchunk Parquet references using `VirtualiZarr` and `Dask`.
- `utils/config_utils.py`: Runtime readiness checks, configuration loading, and local secret resolution.

## Getting Started

### Prerequisites
- Python 3.12+
- Access to Pawsey Acacia (S3) credentials.

### Setup
1. **Install Dependencies**:
   ```bash
   uv sync
   ```

2. **Configure Secrets**:
   Create a file at `.env/s3_connect.toml` with Acacia credentials:
   ```toml
   [project]
   aws_access_key_id = "your_access_key"
   aws_secret_access_key = "your_secret_key"
   ```

3. **Verify Runtime**:
   The pipeline includes a readiness check to ensure all dependencies and configurations are valid before execution.

## Execution

The pipeline can be executed via executing the `main.ipynb` orchestration notebook for the full S3 inventory scan and Parquet reference generation

The pipeline follows a **Research -> Strategy -> Execution** flow:
1. **Scan**: Enumerate S3 objects based on the configured `source_flows`.
2. **Diff**: Compare against the local `inventory_ledger.json` to identify new or changed objects.
3. **Generate**: Parallel produce one Parquet reference file per changed object.
4. **Commit**: Update the ledger only after successful generation.

## Testing

`.github/workflows/` contains automated Github action jobs. 

```bash
uv run pytest -m "not e2e" # Runs unit + integration tests
uv run pytest -m "e2e" # Runs only e2e test
uv run pytest # Run all
```

> Note: e2e tests require valid Acacia S3 credentials.

## Downstream Usage

Once the pipeline generates the Parquet references, visualization tools or web APIs can consume them:
```python
import xarray as xr
import fsspec

# Define storage options for the remote S3 data
storage_options = {
    "key": ACCESS_KEY,
    "secret": SECRET_KEY,
    "client_kwargs": {"endpoint_url": "https://projects.pawsey.org.au"},
    "config_kwargs": {"signature_version": "s3v4", "s3": {"addressing_style": "path"}}
}

# Open the virtualized dataset using the generated Parquet reference
ds = xr.open_dataset(
    "path/to/reference.parq",
    engine="kerchunk",
    backend_kwargs={"storage_options": {"remote_options": storage_options}}
)
```