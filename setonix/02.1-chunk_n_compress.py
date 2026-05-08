import os
from pathlib import Path
import warnings
from dask.distributed import Client, LocalCluster, as_completed
import xarray as xr

SCRATCH = Path(os.environ.get("MYSCRATCH", "/tmp"))
data_in_dir = SCRATCH / "acacia_clean_data"
data_out_dir = SCRATCH / "vz_kerchunk"

# Encoding and Chunking Specifications
config = {
    "dpird": {
        "pattern": "DPIRD/DPIRD_final_stations.nc",
        "chunks": {'station':96,'time':13156},
        "complevel": 5
    },
    "ecmwf": {
        "pattern": "ECMWF/**/*.nc",
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
        var_dims= ds[v].dims
        var_chunks = tuple(chunk_dict.get(dim, ds[v].sizes[dim]) for dim in var_dims)
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

    try:
        with xr.open_dataset(in_path, engine="h5netcdf") as ds:
            ds = ds.chunk(spec["chunks"])
            encoding = build_var_encoding(ds, spec["chunks"], complevel=spec["complevel"])
        
            ds.to_netcdf(
                path=out_path,
                engine="h5netcdf",
                format="NETCDF4",
                encoding=encoding
            )
        return f"Completed: {in_path} -> {out_path}"
    except Exception as e:
        print(f"Error preparing {in_path}: {e}")
        return None

def main():
    data_out_dir.mkdir(parents=True, exist_ok=True)

    # Initialize Dask LocalCluster for Setonix Node (180GB/24 workers= 11.25GB per worker)
    cluster = LocalCluster(
        n_workers=24, 
        threads_per_worker=1, # n_workers x threads_per_worker = SBATCH cpus
        memory_limit="8GB",
        dashboard_address=":8787"
    )
    client = Client(cluster)
    print(f"Dask Dashboard available at: {client.dashboard_link}")

    all_files=[]
    all_ds_types=[]
    
    dpird_files = list(data_in_dir.glob(config["dpird"]["pattern"]))
    for f in dpird_files:
        all_files.append(f)
        all_ds_types.append("dpird")
        
    ecmwf_files = list(data_in_dir.glob(config["ecmwf"]["pattern"]))
    for f in ecmwf_files:
        all_files.append(f)
        all_ds_types.append("ecmwf")

    if not all_files:
        print("No files found to process. Check $MYSCRATCH/acacia_clean_data")
        client.close()
        cluster.close()
        return

    print(f"Submitting {len(all_files)} tasks across {len(client.scheduler_info()['workers'])} workers on cluster ...")
    
    futures= client.map(process_file,all_files,all_ds_types)
    
    for future in as_completed(futures):
        print(future.result())

    client.close()
    cluster.close()

if __name__ == "__main__":
    main()
