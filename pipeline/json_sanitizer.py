from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from pipeline.generate_json import reference_relpath_for_key

NAN_base64= "AAAAAAAA+H8=" # This base64 value decodes to np.float64(nan)


def _has_non_finite_FillValue(attrs: dict[str, Any]) -> bool:
    fill_value = attrs.get("_FillValue")
    return isinstance(fill_value, float) and not math.isfinite(fill_value)


def sanitize_reference_file(ref_path: Path) -> int:
    payload = json.loads(ref_path.read_text(encoding="utf-8"))
    refs = payload.get("refs", {})

    sanitized_zattrs = 0
   
    for ref_key, raw_value in refs.items():
        if not ref_key.endswith("/.zattrs") or not isinstance(raw_value, str):
            continue
        
        attrs = json.loads(raw_value)

        if _has_non_finite_FillValue(attrs):
            attrs["_FillValue"] = NAN_base64
            refs[ref_key] = json.dumps(
                attrs,
                separators = (",", ":"),
                allow_nan = False
            )
            sanitized_zattrs += 1

    if sanitized_zattrs:
        ref_path.write_text(
            json.dumps(payload, separators=(",", ":"), allow_nan=False),
            encoding = "utf-8"
        )      

    return sanitized_zattrs

def sanitize_generated_references(
    *,
    staging_volume_path: str,
    source_keys: list[str],
) -> dict[str, int]:
    staging_root = Path(staging_volume_path)
    summary = {"checked_refs": 0, "sanitized_zattrs_instances": 0}

    for source_key in sorted(source_keys):
        ref_path = staging_root / reference_relpath_for_key(source_key)

        if not ref_path.exists():
            continue

        summary["checked_refs"] += 1
        summary["sanitized_zattrs_instances"] += sanitize_reference_file(ref_path)

    return summary