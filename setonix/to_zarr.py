import os
from pathlib import Path
import warnings
import dask
from dask.distributed import Client, LocalCluster
import xarray as xr
import numpy as np
import zarr
from numcodecs import zstd

SCRATCH = Path(os.environ.get("MYSCRATCH", "/tmp"))
data_in_dir = SCRATCH / "acacia_clean_data"
data_out_dir = SCRATCH / "vz_kerchunk"

"""
!!---FOLLOW SEQUENCE----!!
Gemini CLI has initial Zarr V3 draft.
Gemini notebook has refined with encoding string var drops
Opencode root cause analysis of why chunked ds cannot xr.open_dataset()
"""
config= {
    "dpird":{
        "pattern": "DPIRD_final_stations.nc",
        #dpird station: 192 time: 105248
    },
    "ecmwf":{
        "pattern": "ecmwf_op_clean/**/*.nc", 
        #ecmwf time: 14 step: 113 latitude: 111 longitude: 151
    }
}

"""
A shard is an "outer chunk" that contains multiple "inner chunks"
Chunks remain the units of reading. Shards are units of writing
"""