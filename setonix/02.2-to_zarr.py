import os
from pathlib import Path

import xarray as xr
import numpy as np
from numcodecs import Blosc
from dask.distributed import Client, LocalCluster, as_completed

SCRATCH = Path(os.environ.get("MYSCRATCH", "/tmp"))
dpird_staged_path = SCRATCH / "vz_kerchunk" / "DPIRD" / "DPIRD_final_stations.nc"
ecmwf_dir = SCRATCH / "acacia_clean_data" / "ECMWF"
zarr_out_dir = SCRATCH / "zarr_objects"

# Encoding specs 
encoding_specs = {
    "dpird": {
        "chunks": {"station": 96, "time": 13156},
        "shards": {"station": 192, "time": 52624},
        "fill_value": np.float64(np.nan),
        "pattern": None,
    },
    "ecmwf": {
        "chunks": {"time": 6, "step": 26, "latitude": 111, "longitude": 151},
        "shards": {"time": 120, "step": 156, "latitude": 111, "longitude": 151},
        "fill_value": np.float32(np.nan),
        "pattern": "**/*.nc",
    },
}

compressor= Blosc(cname="zstd", clevel= 5, shuffle=-1)

"""Builds per variable Zarr V3 encoding. Uses the respective chunk/shard mapping for each ds"""
def build_var_encoding(ds: xr.Dataset, chunk_dict: dict, shard_dict: dict, fill_value):
    enc= {}
    for var in ds.data_vars:
        var_dims= ds[var].dims
        var_chunks = tuple(chunk_dict.get(dim, ds[var].sizes[dim]) for dim in var_dims)
        var_shards = tuple(shard_dict.get(dim, ds[var].sizes[dim]) for dim in var_dims)

        enc[var] = {
            "chunks": var_chunks,
            "shards": var_shards,
            "compressor": compressor,
            "fill_value": fill_value,
        }
    return enc

def dpird_to_zarr():
    if not dpird_staged_path.exists():
        raise FileNotFoundError(f"02.1-chunk_n_compress.sh should have ran to produce: {dpird_staged_path}")
    
    spec= encoding_specs["dpird"]
    out_path= zarr_out_dir / "dpird.zarr"

    with xr.open_dataset(dpird_staged_path, engine="h5netcdf") as ds:
        encoding = build_var_encoding(
            ds,
            chunk_dict=spec["chunks"],
            shard_dict=spec["shards"],
            fill_value=spec["fill_value"],
        )
        ds.to_zarr(out_path, zarr_format=3, encoding=encoding, mode="w")

def ecmwf_to_zarr():
    spec = encoding_specs["ecmwf"]
    pattern = spec["pattern"]
    ecmwf_files = sorted(ecmwf_dir.glob(pattern))

    if not ecmwf_files:
        raise FileNotFoundError(f"No ECMWF files found at {ecmwf_dir} with pattern {pattern}")

    out_path = zarr_out_dir / "ecmwf.zarr"

    with xr.open_mfdataset(
        ecmwf_files,
        concat_dim="time",
        combine="nested",
        parallel=True,
        engine="h5netcdf",
    ) as ds:
        # Rechunk after load to above specified (6, 26, 111, 151) chunking
        ds = ds.chunk(spec["chunks"])

        encoding = build_var_encoding(
            ds,
            chunk_dict=spec["chunks"],
            shard_dict=spec["shards"],
            fill_value=spec["fill_value"],
        )

        ds.to_zarr(out_path, zarr_format=3, encoding=encoding, mode="w")

def main():
    zarr_out_dir.mkdir(parents=True, exist_ok=True)

    print("Starting DPIRD -> Zarr")
    dpird_to_zarr()
    print("DPIRD complete")

    print("Starting ECMWF -> Zarr")
    ecmwf_to_zarr()
    print("ECMWF complete")

if __name__ == "__main__":
    main()