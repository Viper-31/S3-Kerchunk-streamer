from __future__ import annotations

import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import boto3
import s3fs
from botocore.config import Config
from botocore.exceptions import ClientError

LEDGER_SCHEMA_VERSION= 1

"""
Define MasterLedger as JSON to store current object metadata (key, etag, ...).
Allows tracking of new object additions/modifications.
"""
@dataclass(frozen==True)
class InventoryObject:
    key: str
    etag: str
    last_modified: str
    size: int
    flow_id: str

    # Record to ledger as dict
    def to_ledger_record(self) -> dict[str,Any]:
        return asdict(self)
    
#Get ISO-format datetime adjusted to local timezone in
def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()

def _normalise_etag(raw: str | None) -> str:
    if not raw:
        return ""
    return raw.strip ('"')

# This seems redundant
def _to_iso_utc (value:Any) -> str: 
    #If value is datetime data-type
    if isinstance(value,datetime):
        if value.tzinfo is None:
            value= value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    return str(value)

"""Build Acacia S3 access keys & access bucket from config.yaml"""
def build_storage_clients(kp: dict[str, Any], access_key: str, secret_key: str):
    s3_cfg = kp["s3"]

    client_kwargs = {
        "endpoint_url": s3_cfg["endpoint_url"],
        "region_name": s3_cfg.get("region_name", "us-east-1"),
    }
    config_kwargs = {
        "signature_version": "s3v4",
        "s3": {"addressing_style": "path"},
    }

    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs=client_kwargs,
        config_kwargs=config_kwargs,
    )

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(**config_kwargs),
        **client_kwargs,
    )

    return fs, s3_client

"""Load MasterLedger returning its contents"""
def load_ledger(ledger_path: str) -> dict[str:Any]:
    p= Path(ledger_path)
    if not p.exists():
        return {
            "schema_version": LEDGER_SCHEMA_VERSION,
            "updated_at": None,
            "objects": {},
        }
    with p.open("r", encoding= "utf-8") as fh:
        payload= json.load(fh)
    
    if payload.get("schema_version") != LEDGER_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported ledger schema version= {payload.get('schema_version')}, "
            f"expected {LEDGER_SCHEMA_VERSION}"
        )
    
    objects= payload.get("objects",{})
    if not isinstance(objects,dict):
        raise ValueError("Ledger objects must be a dict keyed by a source key")
    
    return payload

"""
Iterate over objects to collect .nc files
"""
def _iter_prefix_regex_objects(
        s3_client: Any,
        bucket: str,
        flow: dict[str, Any],
        page_size: int,
):
    flow_id= flow["id"]
    prefix= flow["prefix"]
    pattern= re.compile(flow["key_regex"])

    paginator= s3_client.get_paginator("list_objects_v2")
    pages= paginator.paginate(
        Bucket= bucket,
        Prefix= prefix,
        PaginationConfig= {"PageSize": page_size},
    )

    #Only care about NetCDF file objects
    for page in pages:
        for obj in page.get("Contents",[]):
            key= obj["Key"]
            if not key.endswith(".nc"):
                continue
            if not pattern.match(key):
                continue

            yield InventoryObject(
                key=key,
                etag=_normalise_etag(obj.get("ETag")),
                last_modified=_to_iso_utc(obj.get("LastModified")),
                size=int(obj.get("Size", 0)),
                flow_id=flow_id,    
            )

"""
Catch 404, NoSuchKey, NotFound errors
"""
def _iter_exact_key_object(s3_client: Any, bucket: str, flow: dict[str, Any]):
    key = flow["exact_key"]
    if not key.endswith(".nc"):
        return

    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        err = str(exc.response.get("Error", {}).get("Code", ""))
        if err in {"404", "NoSuchKey", "NotFound"}:
            return
        raise

    yield InventoryObject(
        key=key,
        etag=_normalise_etag(head.get("ETag")),
        last_modified=_to_iso_utc(head.get("LastModified")),
        size=int(head.get("ContentLength", 0)),
        flow_id=flow["id"],
    )

