import pytest
import sys
import virtualizarr as vz
from virtualizarr.parsers import HDFParser
from pathlib import Path

repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root))
from utils.config_utils import load_pipeline_config, resolve_secrets
from pipeline import generate_json as gjson

pytestmark = pytest.mark.e2e


def _known_virtualizarr_Parser_failure(err: Exception) -> bool:
    """
    Error message checking that it contains either:
     - HDF5Parser AttributeError on string dtype: https://github.com/zarr-developers/VirtualiZarr/pull/988
     - corrupt buffer stream: https://github.com/NASA-IMPACT/veda-odd/issues/371
    """
    msg = str(err)
    return "'bytes' object has no attribute 'item'" in msg or "corrupt buffer" in msg


def test_vlen_string_export_limitation():
    """
    Architectural canary test:
    Kerchunk cannot currently map HDF5 variable-length string heaps over S3.
    If this test starts FAILING (i.e. it does NOT raise ValueError), it means
    upstream has fixed the limitation, and the `enrich_string_variables`
    workaround can finally be removed.
    """

    kp = load_pipeline_config("configs/config.yaml")
    access, secret = resolve_secrets(kp)
    registry = gjson._build_registry(kp, access, secret)
    url = "s3://webviz/DPIRD/DPIRD_final_stations.nc"
    parser = HDFParser()

    # Stage 1: Check AttributeError or corrupt buffer crash occurs on opening virtual dataset
    try:
        vds = vz.open_virtual_dataset(url, registry, parser=parser)
    except Exception as e:
        assert _known_virtualizarr_Parser_failure(e), (
            f"Expected known virtualizarr Parser error, but got :{e}"
        )
        return

    # If open_virtual_dataset() succeeds, proceed to verify codec
    has_vlen_codec = False
    for var_data in vds.variables.values():
        if "VLenUTF8Codec" in str(var_data.encoding.get("filters", "")):
            has_vlen_codec = True
            break

    assert has_vlen_codec, (
        "Expected VLenUTF8Codec to be present on 'station' or 'code' string variables"
    )

    # Stage 2: If codec is present, try generating the reference
    try:
        refs = vds.vz.to_kerchunk(format="dict")
    except Exception as e:
        assert _known_virtualizarr_Parser_failure(e), (
            f"Expected known virtualizarr Parser error, but got :{e}"
        )
        return

    # If both operations succeeded, the upstream limitation is resolved
    pytest.fail(
        "Kerchunk export SUCCEEDED! The HDF5 architectural heap limitation has been fixed.\n"
        "Check NASA-IMPACT/veda-odd#371 issue is closed/partially resolved"
    )
