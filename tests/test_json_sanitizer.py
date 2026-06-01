import json

from pipeline import json_sanitizer


def _write_reference(path, refs):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"version": 1, "refs": refs}), encoding="utf-8")


def test_sanitize_reference_file_replaces_bare_nan_fill_value(tmp_path):
    ref_path = tmp_path / "dpird_wa_stations.nc.json"
    _write_reference(
        ref_path,
        {
            "lat/.zattrs": '{"_FillValue":NaN,"_ARRAY_DIMENSIONS":["station"]}',
            "lon/.zattrs": '{"_FillValue":NaN,"_ARRAY_DIMENSIONS":["station"]}',
        },
    )

    sanitized_count = json_sanitizer.sanitize_reference_file(ref_path)

    payload = json.loads(ref_path.read_text(encoding="utf-8"))
    lat_attrs = json.loads(payload["refs"]["lat/.zattrs"])
    lon_attrs = json.loads(payload["refs"]["lon/.zattrs"])

    assert sanitized_count == 2
    assert lat_attrs == {
        "_FillValue": json_sanitizer.NAN_base64,
        "_ARRAY_DIMENSIONS": ["station"],
    }
    assert lon_attrs == {
        "_FillValue": json_sanitizer.NAN_base64,
        "_ARRAY_DIMENSIONS": ["station"],
    }


def test_sanitize_reference_file_preserves_zarray_nan_fill_value(tmp_path):
    ref_path = tmp_path / "ecmwf.nc.json"
    zarray = (
        '{"shape":[1],"chunks":[1],"dtype":"<f8",'
        '"fill_value":"NaN","order":"C","filters":null,'
        '"compressor":null,"zarr_format":2}'
    )
    _write_reference(
        ref_path,
        {
            "airTemperature/.zarray": zarray,
            "airTemperature/.zattrs": '{"_FillValue":"AAAAAAAA+H8="}',
        },
    )

    sanitized_count = json_sanitizer.sanitize_reference_file(ref_path)

    payload = json.loads(ref_path.read_text(encoding="utf-8"))
    assert sanitized_count == 0
    assert payload["refs"]["airTemperature/.zarray"] == zarray


def test_sanitize_reference_file_does_not_rewrite_clean_reference(tmp_path):
    ref_path = tmp_path / "clean.nc.json"
    refs = {
        "lat/.zattrs": '{"_FillValue":"AAAAAAAA+H8=","_ARRAY_DIMENSIONS":["station"]}',
    }
    _write_reference(ref_path, refs)
    original_text = ref_path.read_text(encoding="utf-8")

    sanitized_count = json_sanitizer.sanitize_reference_file(ref_path)

    assert sanitized_count == 0
    assert ref_path.read_text(encoding="utf-8") == original_text


def test_sanitize_generated_references_uses_reference_relpaths(tmp_path):
    staging_root = tmp_path / "staging"
    ref_path = staging_root / "refs" / "DPIRD" / "dpird_wa_stations.nc.json"
    _write_reference(
        ref_path,
        {
            "lat/.zattrs": '{"_FillValue":NaN,"_ARRAY_DIMENSIONS":["station"]}',
        },
    )

    summary = json_sanitizer.sanitize_generated_references(
        staging_volume_path=str(staging_root),
        source_keys=["DPIRD/dpird_wa_stations.nc", "missing.nc"],
    )

    payload = json.loads(ref_path.read_text(encoding="utf-8"))
    attrs = json.loads(payload["refs"]["lat/.zattrs"])

    assert summary == {"checked": 1, "sanitized_zattrs": 1}
    assert attrs["_FillValue"] == json_sanitizer.NAN_base64
