import os
from pathlib import Path
import warnings
import dask
from dask.distributed import Client, LocalCluster
import xarray as xr

SCRATCH = Path(os.environ.get("MYSCRATCH", "/tmp"))
data_in_dir = SCRATCH / "acacia_clean_data"
data_out_dir = SCRATCH / "vz_kerchunk"

# Encoding and Chunking Specifications
config = {
    "dpird": {
        "pattern": "DPIRD_final_stations.nc",
        "chunks": {"time": 3289},
        "complevel": 5
    },
    "ecmwf": {
        "pattern": "ecmwf_op_clean/**/*.nc",
        "chunks": {"time": 4, "step": 25},
        "complevel": 5
    }
}

warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification*",
    category=UserWarning
)

"""Builds xarray encoding dictionary for NetCDF4 zlib compression."""
def build_var_encoding(ds, chunk_dict, complevel=5):
    enc = {}
    for v in ds.data_vars:
        var_chunks = tuple(chunk_dict.get(dim, ds[v].sizes[dim]) for dim in ds[v].dims)
        
        enc[v] = {
            "zlib": True,
            "complevel": complevel,
            "shuffle": True,
            "chunksizes": var_chunks
        }
    return enc

"""Chunks and compresses a single NetCDF file."""
def process_file(in_path, dataset_type):
    spec = config[dataset_type]
    rel_path = in_path.relative_to(data_in_dir)
    out_path = data_out_dir / rel_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Processing {dataset_type}: {in_path} -> {out_path}")
    
    try:
        ds = xr.open_dataset(in_path, engine="h5netcdf", chunks=spec["chunks"])
        
        encoding = build_var_encoding(ds, spec["chunks"], complevel=spec["complevel"])
        
        write_job = ds.to_netcdf(
            path=out_path,
            engine="h5netcdf",
            format="NETCDF4",
            encoding=encoding,
            compute=False # Returns a Dask object
        )
        return write_job
    except Exception as e:
        print(f"Error preparing {in_path}: {e}")
        return None

def main():
    data_out_dir.mkdir(parents=True, exist_ok=True)

    # Initialize Dask LocalCluster for Setonix Node (128 cores)
    cluster = LocalCluster(
        n_workers=128, 
        threads_per_worker=1,
        memory_limit='auto'
    )
    client = Client(cluster)
    print(f"Dask Dashboard available at: {client.dashboard_link}")

    tasks = []
    
    dpird_files = list(data_in_dir.glob(config["dpird"]["pattern"]))
    for f in dpird_files:
        tasks.append(process_file(f, "dpird"))
        
    ecmwf_files = list(data_in_dir.glob(config["ecmwf"]["pattern"]))
    for f in ecmwf_files:
        tasks.append(process_file(f, "ecmwf"))

    tasks = [t for t in tasks if t is not None]

    if not tasks:
        print("No files found to process. Check $MYSCRATCH/acacia_clean_data")
        return

    print(f"Executing {len(tasks)} transformation tasks across {len(client.scheduler_info()['workers'])} workers...")
    
    #Execute in parallel
    dask.compute(*tasks)
    
    print("All transformations complete.")
    client.close()
    cluster.close()

if __name__ == "__main__":
    main()
