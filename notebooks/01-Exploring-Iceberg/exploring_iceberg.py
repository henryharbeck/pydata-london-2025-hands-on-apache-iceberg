import marimo

__generated_with = "0.13.4"
app = marimo.App(
    width="medium",
    app_title="Exploring Iceberg",
    sql_output="polars",
)


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import pyiceberg
    from pyiceberg.catalog.rest import RestCatalog
    import polars as pl
    import pyarrow.csv as pc
    import pyarrow as pa
    return RestCatalog, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Hands-on with Apache Iceberg

    ## The Context
    - Came out of Netflix
    - Improved version of Hive
    - Separation of storage and compute
    - Big Providers
    - Iceberg is an implementation standard for the storage layer
    - Compute now just needs to speak Iceberg
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## The Catalog

    First we need a catalog - the catalog keeps track of the metadata. Depending on the catalog instance, it can do many more things, but today we will mainly focus on its role as the place to store the location of the metadata.
    """
    )
    return


@app.cell
def _(RestCatalog):
    catalog = RestCatalog("lakekeeper", uri="http://lakekeeper:8181/catalog", warehouse="lakehouse")
    return (catalog,)


@app.cell
def _(catalog):
    catalog.create_namespace_if_not_exists("house_prices")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Now we have a namespace that will contain all our tables - think of a namespace like a schema in a traditional database.

    ## Schema

    Next we need another type of schema - the data schema. In Iceberg, we can define the schema using Pyiceberg types, though many query engines also support creating Iceberg tables via SQL.
    """
    )
    return


@app.cell
def _():
    from pyiceberg.schema import Schema, NestedField, StringType, IntegerType, DateType

    house_prices_schema = Schema(
        NestedField(
            1,
            "transaction_id",
            StringType(),
            required=True,
            doc="A reference number which is generated automatically recording each published sale. The number is unique and will change each time a sale is recorded.",
        ),
        NestedField(
            2, "price", IntegerType(), required=True, doc="Sale price stated on the transfer deed."
        ),
        NestedField(
            3,
            "date_of_transfer",
            DateType(),
            required=True,
            doc="Date when the sale was completed, as stated on the transfer deed.",
        ),
        NestedField(
            4,
            "postcode",
            StringType(),
            required=True,
            doc="This is the postcode used at the time of the original transaction. Note that postcodes can be reallocated and these changes are not reflected in the Price Paid Dataset.",
        ),
        NestedField(
            5,
            "property_type",
            StringType(),
            required=True,
            doc="D = Detached, S = Semi-Detached, T = Terraced, F = Flats/Maisonettes, O = Other",
        ),
        NestedField(
            6,
            "new_property",
            StringType(),
            required=True,
            doc="Indicates the age of the property and applies to all price paid transactions, residential and non-residential. Y = a newly built property, N = an established residential building",
        ),
        NestedField(
            7,
            "duration",
            StringType(),
            required=True,
            doc="Relates to the tenure: F = Freehold, L= Leasehold etc. Note that HM Land Registry does not record leases of 7 years or less in the Price Paid Dataset.",
        ),
        NestedField(
            8,
            "paon",
            StringType(),
            doc="Primary Addressable Object Name. Typically the house number or name",
        ),
        NestedField(
            9,
            "saon",
            StringType(),
            doc="Secondary Addressable Object Name. Where a property has been divided into separate units (for example, flats), the PAON (above) will identify the building and a SAON will be specified that identifies the separate unit/flat.",
        ),
        NestedField(10, "street", StringType()),
        NestedField(11, "locality", StringType()),
        NestedField(12, "town", StringType()),
        NestedField(13, "district", StringType()),
        NestedField(14, "county", StringType()),
        NestedField(
            15,
            "ppd_category_type",
            StringType(),
            doc="Indicates the type of Price Paid transaction. A = Standard Price Paid entry, includes single residential property sold for value. B = Additional Price Paid entry including transfers under a power of sale/repossessions, buy-to-lets (where they can be identified by a Mortgage), transfers to non-private individuals and sales where the property type is classed as ‘Other’.",
        ),
        NestedField(16, "record_status", StringType(), doc="Indicates additions, changes and deletions to the records. A = Addition C = Change D = Delete"),
        identifier_field_ids=[1],
    )
    return (house_prices_schema,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## The Table

    With our schema in place, we're now ready to create the table. We need to specify the location where the table will be stored, though depending on the catalog, it can automatically assign a location.
    """
    )
    return


@app.cell
def _(catalog, house_prices_schema):
    house_prices_t = catalog.create_table_if_not_exists("house_prices.raw", schema=house_prices_schema, location="s3://warehouse/house_prices/raw")
    return (house_prices_t,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
    ## The Data
    Pyiceberg expects to receive a Pyarrow table, so we need to read in our CSV and convert it to Arrow. In this case, our data does not have headers, so we need to set those as well.
    """
    )
    return


@app.cell
def _(pl):
    def read_house_prices(filename: str) -> pl.DataFrame:
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


    house_prices_2024 = read_house_prices("data/pp-2024.csv")
    house_prices_2024
    return house_prices_2024, read_house_prices


@app.cell
def _():
    # catalog.drop_table("house_prices.raw", purge_requested=True)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Writing the Data
    Now that we have the data loaded, we're ready to write it out to our Iceberg table. We have 3 different strategies available to us:

    - append
    - overwrite
    - upsert

    Append and overwrite should hopefully make sense.
    Upsert is a recent addition to Pyiceberg. Given a key column, it will compare the keys to decide if data should be updated or inserted.

    For now, let's stick to `append`.

    /// admonition | Note on schemas
    PyIceberg is strict on the schema - by default, Polars is a bit looser, so we need to `cast` the exported polars arrow table into the same schema as we've defined - otherwise our write will be rejected.
    ///
    """
    )
    return


@app.cell
def _(house_prices_2024, house_prices_schema, house_prices_t):
    # Export to arrow and cast it
    house_prices_arrow = (
        house_prices_2024
        .to_arrow() # Export to an Arrow table
        .cast(house_prices_schema.as_arrow()) # Cast into the Iceberg schema
    )
    # Append data to the table
    house_prices_t.append(house_prices_arrow)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
    # Metadata - the secret of Iceberg

    Now that we've created a schema for our houseprices, let's take a look at the metadata that we've created. In Iceberg, all the metadata is stored in a combination of JSON and Avro, and all the metadata is stored in the S3 buckets directly, which is what makes it accessible from the various query engines. 

    Let's have a look at the different files we've created out of the box. First, we need something that can talk to S3 - in this case our Minio S3 - enter fsspec and s3fs
    """
    )
    return


@app.cell
def _():
    import s3fs
    fs = s3fs.S3FileSystem(endpoint_url="http://minio:9000", key="minio", secret="minio1234")
    fs.ls("/warehouse")
    return (fs,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Now that we have something that can read our S3 bucket in Minio, we need to know where our Iceberg Catalogue put our most recent table update. PyIceberg stores that information in the `metadata_location` of the table""")
    return


@app.cell
def _(house_prices_t):
    house_prices_t.metadata_location
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""That's a gzipped json file, a choice that our Iceberg Rest Catalog has chosen for us, so we need to do some extra work to read our metadata.""")
    return


@app.cell
def _():
    from fsspec import AbstractFileSystem
    from pyiceberg.table import Table
    from typing import Any
    import gzip
    import json

    def get_iceberg_metadata(fs: AbstractFileSystem, table: Table) -> dict[str, Any]:
        """Unzips the gzipped json and reads it into a dictionary"""
        with fs.open(table.metadata_location) as f, gzip.open(f) as g_f:
            return json.load(g_f)
    return AbstractFileSystem, Any, Table, get_iceberg_metadata


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Let's have a look at the contents of the current metadata.json to get a better understanding of how Iceberg does what it does""")
    return


@app.cell
def _(fs, get_iceberg_metadata, house_prices_t):
    get_iceberg_metadata(fs, house_prices_t)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""The next layer of the onion is the `manifest list` - we can find that by looking in the `snapshots` array for the current snapshot. Note that the manifest list is stored as Avro for faster scanning. Let's convert it to a dictionary for our simple human brains to understand.""")
    return


@app.cell
def _(AbstractFileSystem, Any, Table, fs, house_prices_t, pl):
    def get_iceberg_manifest_list(fs: AbstractFileSystem, table: Table) -> dict[str, Any]:
        """Fetch the manifest list for the current snapshot and convert to a list of dicts"""
        manifest_list = table.current_snapshot().manifest_list
        with fs.open(manifest_list) as f:
            return pl.read_avro(f).to_dicts()
    get_iceberg_manifest_list(fs, house_prices_t)
    return (get_iceberg_manifest_list,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""We can see that the manifest list unsurprisingly contains a list of manifests, alongside some metadata for that particular manifest. We've only written one file to our Iceberg table, so we have only one manifest currently. Let's dig one level deeper and open that manifest.""")
    return


@app.cell
def _(
    AbstractFileSystem,
    Table,
    fs,
    get_iceberg_manifest_list,
    house_prices_t,
    pl,
):
    def get_iceberg_manifest(fs: AbstractFileSystem, table: Table, index=-1):
        """Get the manifest at the `index` position from the manifest list. """
        manifest_list = get_iceberg_manifest_list(fs, table)
        with fs.open(manifest_list[index]["manifest_path"]) as m_f:
            return pl.read_avro(m_f).to_dicts()
    get_iceberg_manifest(fs, house_prices_t)
    return (get_iceberg_manifest,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    At the lowest level of metadata, we can see a reference to the actual data files that make up the physical data stored on disk. 

    /// admonition

    Note that Iceberg keeps track of the physical files of the table, unlike something like Hive, which uses a folder as a logical container for a table. 

    ///

    We can see that the Parquet file is pretty much as we expected, and we can read it directly as any other Parquet files - Iceberg doesn't specify anything about the physical data - it just stores metadata about the files to enable all the features of Iceberg
    """
    )
    return


@app.cell
def _(AbstractFileSystem, Table, fs, get_iceberg_manifest, house_prices_t, pl):
    def get_iceberg_data_file(fs: AbstractFileSystem, table: Table, index=-1) -> pl.DataFrame:
        """Read the data file from the `index` position in the data_file"""
        manifest = get_iceberg_manifest(fs, table, index)
        with fs.open(manifest[index]["data_file"]["file_path"]) as p_f:
            return pl.read_parquet(p_f)

    get_iceberg_data_file(fs, house_prices_t, 0)
    return


@app.cell(hide_code=True)
def _(fs, get_iceberg_manifest, house_prices_t, mo):
    mo.md(f"""In total, on disk, this comes to around {get_iceberg_manifest(fs, house_prices_t)[0]['data_file']['file_size_in_bytes'] / 1024 / 1024:0.2f} MB, which is pretty small, so we only have one data file in our manifest""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Let's add some more data. Using PyIceberg, we can easily append more data.""")
    return


@app.cell
def _(house_prices_schema, house_prices_t, read_house_prices):
    house_prices_2023 = read_house_prices("data/pp-2023.csv")
    house_prices_t.append(house_prices_2023.to_arrow().cast(house_prices_schema.as_arrow()))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Now that we have some more data, what do you see that has changed in the metadata?""")
    return


@app.cell
def _(fs, get_iceberg_metadata, house_prices_t):
    get_iceberg_metadata(fs, house_prices_t)
    return


@app.cell
def _(fs, get_iceberg_manifest_list, house_prices_t):
    get_iceberg_manifest_list(fs, house_prices_t)
    return


@app.cell
def _(fs, get_iceberg_manifest, house_prices_t):
    get_iceberg_manifest(fs, house_prices_t, index=0)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Concluding on Metadata

    All the metadata we've looked at here is stored in object storage. It's this metadata which powers all of Iceberg - if you can understand how this metadata is put together, you understand the inner workings of Iceberg.
    """
    )
    return


if __name__ == "__main__":
    app.run()
