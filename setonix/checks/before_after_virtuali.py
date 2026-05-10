# Activate zarr_venv then use python -u scripts/checks/before_after_virtuali.py
#!/usr/bin/env python3
import os
from pathlib import Path
import numpy as np
import xarray as xr

MYSCRATCH = Path(os.environ.get("MYSCRATCH", "."))
original_path = MYSCRATCH / "acacia_clean_data/ECMWF/2024/02/06.nc"
after_chunk_compress_path = MYSCRATCH / "vz_kerchunk/ECMWF/2024/02/06.nc"

def chunk_info_mb(da):
   enc = da.encoding or {}
   # Prefer on-disk chunksizes from encoding
   chunks = enc.get("chunksizes")
   if chunks is None:
      # Fall back to dask chunks if present
      data = da.data
      if hasattr(data, "chunks") and data.chunks:
          chunks = tuple(c[0] for c in data.chunks)
      else:
          chunks = da.shape

   # Use on-disk dtype if present, else in-memory dtype
   dtype = enc.get("dtype", da.dtype)
   itemsize = np.dtype(dtype).itemsize
   n = 1
   for c in chunks:
      n *= int(c)
   mb = n * itemsize / (1024 ** 2)
   return chunks, mb, dtype

def inspect(path, label):
   print(f"\n=== {label} ===")
   print(f"Path: {path}")
   # decode_cf=False avoids converting packed data to floats
   with xr.open_dataset(path, engine="h5netcdf", decode_cf=False, mask_and_scale=False) as ds:
      da = ds["t2m"]
      print("t2m encoding:", da.encoding)
      chunks, mb, dtype = chunk_info_mb(da)
      print(f"t2m chunk shape: {chunks}")
      print(f"t2m chunk size (MB): {mb:.3f}")
      print(f"t2m dtype used for size calc: {dtype}")

inspect(original_path, "BEFORE")
inspect(after_chunk_compress_path, "AFTER")