# S3 Kerchunk Streamer

A scheduled Databricks batch pipeline that scans NetCDF objects hosted on Pawsey's Acacia (S3-compatible object storage), and generates [Kerchunk](https://fsspec.github.io/kerchunk/) Parquet references. These references allow downstream visualization and web applications to stream exact byte ranges (via [VirtualiZarr](https://virtualizarr.readthedocs.io/)) instead of downloading massive 2-5 GB source files in their entirety.

## Overview

This pipeline solves the problem of efficiently accessing large historical weather and climate datasets (like ECMWF and DPIRD stations) over the network. 

**Key Capabilities:**
- **Cloud-Optimized Access**: Converts traditional NetCDF S3 objects into cloud-optimized formats virtually using Kerchunk.
- **Incremental Processing**: Tracks `ETag` and `LastModified` timestamps in an inventory ledger to only process new or changed files, saving compute.
- **Databricks Integration**: Runs as a Databricks jobs, pushing the derived reference Parquet files directly into Databrick's Volumes. The pipeline automatically creates necessary output directories if they do not exist.
- **Visualization Ready**: Consumer apps can open the metadata via `ReferenceFileSystem` and `xarray` to pull precise byte slices for interactive streaming.

## Project Structure
- `configs/config.yaml`: Core pipeline configuration dictating the S3 endpoint,  source matchers (e.g., ECMWF and DPIRD target prefixes), and Parquet reference output paths.
- `connections/02.0-S3_init.ipynb`: Notebooks demonstrating the authentication and S3 listing patterns against Pawsey's Acacia.
- `requirements.txt`: Python dependencies including `kerchunk`, `virtualizarr`, `s3fs`, and `xarray`.

## Downstream Usage

Once the pipeline generates the Parquet references in the Databricks Volume (`/Volumes/workspace/weather/kerchunk/acacia_refs`), visualisation jobs or web APIs can consume them dynamically:

```python
import xarray as xr
import fsspec

# Open the virtualized dataset
mapper = fsspec.get_mapper("reference://", fo="path/to/reference.parquet")
ds = xr.open_zarr(mapper, consolidated=False)
```