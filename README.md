# S3 Kerchunk Streamer

A local Dask-powered batch pipeline that scans NetCDF objects hosted on Pawsey's Acacia (S3-compatible object storage) and generates [Kerchunk](https://fsspec.github.io/kerchunk/) Parquet references. These references allow downstream visualization and web applications to stream exact byte ranges (via [VirtualiZarr](https://virtualizarr.readthedocs.io/)) instead of downloading massive 2-5 GB source files in their entirety.

## Overview

This pipeline enables efficient, cloud-optimized access to large historical weather and climate datasets (such as ECMWF and DPIRD stations) without requiring full file downloads over network connections.

**Key Capabilities:**
- **Cloud-Optimized Access**: Converts traditional NetCDF S3 objects into virtual cloud-optimized datasets using Kerchunk and VirtualiZarr.
- **Incremental Processing**: Tracks `ETag`, `LastModified`, and `Size` in a local JSON inventory ledger to ensure only new or changed files are reprocessed.
- **Local Parallelism**: Leverages Dask for concurrent reference generation, optimized for local multi-core environments.
- **Atomic Operations**: Implements atomic writes for both Parquet references and the inventory ledger to prevent state corruption.
- **Visualization Ready**: Consumer applications can open the metadata via `ReferenceFileSystem` and `xarray` to pull precise byte slices for interactive streaming.

## Project Structure

- `configs/config.yaml`: Central configuration for S3 endpoints, source flow selectors (ECMWF, DPIRD), and output paths.
- `pipeline/inventory.py`: Logic for scanning S3, building inventory snapshots, and performing incremental diffing.
- `pipeline/generate_parquet.py`: Concurrent generation of Kerchunk Parquet references using `VirtualiZarr` and `Dask`.
- `utils/config_utils.py`: Runtime readiness checks, configuration loading, and local secret resolution.
- `requirements.txt`: Python dependencies including `kerchunk`, `virtualizarr`, `s3fs`, `xarray`, and `dask`.

## Getting Started

### Prerequisites
- Python 3.12+
- Access to Pawsey Acacia (S3) credentials.

### Setup
1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Secrets**:
   Create a file at `.env/s3_connect.txt` with your Acacia credentials:
   ```text
   ACCESS_KEY=your_access_key
   SECRET_KEY=your_secret_key
   ```

3. **Verify Runtime**:
   The pipeline includes a readiness check to ensure all dependencies and configurations are valid before execution.

## Execution

The pipeline can be executed via the provided notebooks in the `connections/` directory:
- `01.1-inventory_generate_refs.ipynb`: The main entry point for running the full inventory scan and reference generation.

The pipeline follows a **Research -> Strategy -> Execution** flow:
1. **Scan**: Enumerate S3 objects based on the configured `source_flows`.
2. **Diff**: Compare against the local `inventory_ledger.json` to identify new or changed objects.
3. **Generate**: Concurrently produce one Parquet reference file per changed object.
4. **Commit**: Update the ledger only after successful generation.

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
    "path/to/reference.parquet",
    engine="kerchunk",
    backend_kwargs={"storage_options": {"remote_options": storage_options}}
)
```