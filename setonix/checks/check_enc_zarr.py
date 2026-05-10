import os
from pathlib import Path
import xarray as xr
import warnings

def main():
    warnings.filterwarnings(
    "ignore",
    message=".*Numcodecs codecs are not in the Zarr version 3 specification.*"
)
    
    # Set up paths relative to MYSCRATCH
    SCRATCH = Path(os.environ.get("MYSCRATCH", "/tmp"))
    
    src_file = SCRATCH / "acacia_clean_data" / "ECMWF" / "2024" / "02" / "06.nc"
    zarr_object = SCRATCH / "zarr_objects" / "ecmwf.zarr"
    
    # Get the date string for dynamic slicing (e.g., '2024-02-06')
    year = src_file.parent.parent.name
    month = src_file.parent.name
    day = src_file.stem
    date_str = f"{year}-{month}-{day}"
    
    # 1. Open Original NetCDF
    nc_ds = xr.open_dataset(src_file, engine="h5netcdf")
    
    # 2. Open Zarr store
    zarr_ds = xr.open_zarr(zarr_object, chunks="auto")
    
    # 3. Slice Zarr store to match the specific day 
    try:
        zarr_ds_sliced = zarr_ds.sel(time=date_str)
        print(f"Successfully sliced Zarr object for time={date_str}")
    except KeyError:
        print("Warning: 'time' dimension not found or date not in Zarr store. Printing root encoding instead.")
        zarr_ds_sliced = zarr_ds

    # 4. Compare Encodings for 't2m'
    print("\n")
    print(f"Original NetCDF ({date_str}.nc) 't2m' encoding:")
    print("=" * 60)
    for key, value in nc_ds.t2m.encoding.items():
        print(f"  {key}: {value}")
        
    print("\n")
    print("Converted Zarr 't2m' encoding:")
    print("=" * 60)
    for key, value in zarr_ds_sliced.t2m.encoding.items():
        print(f"  {key}: {value}")

if __name__ == "__main__":
    main()
