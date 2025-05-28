import gzip
import json
from typing import Any

import polars as pl
from fsspec import AbstractFileSystem
from pyiceberg.table import Table
from pyiceberg.catalog.rest import RestCatalog
import sqlalchemy as sa
import s3fs

engine = sa.create_engine("trino://trino:@trino:8080/lakekeeper")
catalog = RestCatalog(
    "lakekeeper", uri="http://lakekeeper:8181/catalog", warehouse="lakehouse"
)
fs = s3fs.S3FileSystem(
    endpoint_url="http://minio:9000", key="minio", secret="minio1234"
)


def query(sql) -> pl.DataFrame:
    with engine.connect() as conn:
        return pl.read_database(sql, conn)


def get_iceberg_manifest(
    fs: AbstractFileSystem, table: Table
) -> list[list[dict[str, Any]]]:
    """Get the manifest at the `index` position from the manifest list."""
    manifest_list = get_iceberg_manifest_list(fs, table)
    manifest_lists = []
    for manifest_meta in manifest_list:
        with fs.open(manifest_meta["manifest_path"]) as m_f:
            manifest = pl.read_avro(m_f).to_dicts()
            manifest_lists.extend(manifest)
    return manifest_lists


def get_iceberg_manifest_list(
    fs: AbstractFileSystem, table: Table
) -> list[dict[str, Any]]:
    """Fetch the manifest list for the current snapshot and convert to a list of dicts"""
    manifest_list = table.current_snapshot().manifest_list
    with fs.open(manifest_list) as f:
        return pl.read_avro(f).to_dicts()


def get_iceberg_metadata(fs: AbstractFileSystem, table: Table) -> dict[str, Any]:
    """Unzips the gzipped JSON and reads it into a dictionary"""
    with fs.open(table.metadata_location) as f, gzip.open(f) as g_f:
        return json.load(g_f)


def get_iceberg_data_file(
    fs: AbstractFileSystem, table: Table, index=0
) -> pl.DataFrame:
    """Read the data file from the first `index` position in the data_file"""
    manifest = get_iceberg_manifest(fs, table, index)
    with fs.open(manifest[0][index]["data_file"]["file_path"]) as p_f:
        return pl.read_parquet(p_f)


def read_house_prices(filename: str) -> pl.DataFrame:
    """Read in a house prices CSV"""
    # Columns sourced from data dictionary
    house_prices_columns = [
        "transaction_id",
        "price",
        "date_of_transfer",
        "postcode",
        "property_type",
        "new_property",
        "duration",
        "paon",
        "saon",
        "street",
        "locality",
        "town",
        "district",
        "county",
        "ppd_category_type",
        "record_status",
    ]

    df = (
        pl.scan_csv(
            filename,
            has_header=False,
            new_columns=house_prices_columns,
        )
        .with_columns(pl.col("date_of_transfer").str.to_date("%Y-%m-%d %H:%M"))
        .collect()
    )
    return df
