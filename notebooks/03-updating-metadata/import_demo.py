from fsspec import AbstractFileSystem
from pyiceberg.table import Table
import polars as pl
from typing import Any

def get_iceberg_manifest(fs: AbstractFileSystem, table: Table, index=-1) -> list[dict[str, Any]]:
    """Get the manifest at the `index` position from the manifest list. """
    manifest_list = get_iceberg_manifest_list(fs, table)
    with fs.open(manifest_list[index]["manifest_path"]) as m_f:
        return pl.read_avro(m_f).to_dicts()

def get_iceberg_manifest_list(fs: AbstractFileSystem, table: Table) -> list[dict[str, Any]]:
    """Fetch the manifest list for the current snapshot and convert to a list of dicts"""
    manifest_list = table.current_snapshot().manifest_list
    with fs.open(manifest_list) as f:
        return pl.read_avro(f).to_dicts()

def get_iceberg_metadata(fs: AbstractFileSystem, table: Table) -> dict[str, Any]:
    """Unzips the gzipped json and reads it into a dictionary"""
    with fs.open(table.metadata_location) as f, gzip.open(f) as g_f:
        return json.load(g_f)