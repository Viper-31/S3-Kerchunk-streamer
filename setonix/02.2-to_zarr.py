import os
import argparse
from pathlib import Path
import warnings

import xarray as xr
import numpy as np
from zarr.codecs import Blosc

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
        "chunks": {"time": 6, "step": 113, "latitude": 37, "longitude": 151},
        "shards": {"time": 120, "step": 113, "latitude": 111, "longitude": 151},
        "fill_value": np.float32(np.nan),
        "pattern": "**/*.nc",
    },
}

warnings.filterwarnings(
    "ignore",
    message=".*Numcodecs codecs are not in the Zarr version 3 specification.*"
)

compressors= [Blosc(cname="zstd", clevel= 5, shuffle= 1)]

"""--dry-run flag gets the first 8 ecmwf files to build 1 shard"""
def parse_args():
    parser= argparse.ArgumentParser()
    parser.add_argument("--dry-run", action= "store_true", help= "Limit ECMWF to 10 .nc sample and write to seperate .zarr store")
    
    return parser.parse_args()

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
            "compressors": compressors,
            "fill_value": fill_value,
        }
    return enc

def dpird_to_zarr(dry_run: bool = False):
    if not dpird_staged_path.exists():
        raise FileNotFoundError(f"02.1-chunk_n_compress.sh should have ran to produce: {dpird_staged_path}")
    
    spec= encoding_specs["dpird"]
    out_path= zarr_out_dir / ("dpird_dryrun.zarr" if dry_run else "dpird.zarr")

    with xr.open_dataset(dpird_staged_path, engine="h5netcdf") as ds:
        encoding = build_var_encoding(
            ds,
            chunk_dict=spec["chunks"],
            shard_dict=spec["shards"],
            fill_value=spec["fill_value"],
        )
        ds.to_zarr(out_path, zarr_format=3, encoding=encoding, mode="w", consolidated=False)

def ecmwf_to_zarr(dry_run: bool = False):
    spec = encoding_specs["ecmwf"]
    pattern = spec["pattern"]
    ecmwf_files= sorted(ecmwf_dir.glob(pattern))

    if not ecmwf_files or len(ecmwf_files)<=1:
        raise FileNotFoundError(f"Not enough ECMWF files found at {ecmwf_dir} with pattern {pattern}")
    
    if dry_run:
        ecmwf_files=[ecmwf_files[9]]     

    out_path = zarr_out_dir / ("ecmwf_dryrun.zarr" if dry_run else "ecmwf.zarr")

    with xr.open_mfdataset(
        ecmwf_files,
        concat_dim="time",
        combine="nested",
        parallel=True,
        engine="h5netcdf",
    ) as ds:
        # On disk chunk must match (120, 113, 111, 151) shard boundaries
        ds = ds.chunk(spec["shards"])

        # chunk_dict in encoding will tell Zarr V3 to write specified chunks inside shards
        encoding = build_var_encoding(
            ds,
            chunk_dict=spec["chunks"],
            shard_dict=spec["shards"],
            fill_value=spec["fill_value"],
        )

        ds.to_zarr(out_path, zarr_format=3, encoding=encoding, mode="w", consolidated=False)

def main():
    args= parse_args()
    zarr_out_dir.mkdir(parents=True, exist_ok=True)

    print("Starting DPIRD -> Zarr")
    dpird_to_zarr(dry_run=args.dry_run)
    print("DPIRD complete")

    print("Starting ECMWF -> Zarr")
    ecmwf_to_zarr(dry_run=args.dry_run)
    print("ECMWF complete")

if __name__ == "__main__":
    main()