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

"""Define Inventory class to store current object metadata (key, etag, ...)"""
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