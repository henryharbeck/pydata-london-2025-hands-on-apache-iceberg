import marimo

__generated_with = "0.13.4"
app = marimo.App(width="full", app_title="Updating Metadata")


@app.cell
def _():
    import marimo as mo
    from pyiceberg.catalog.rest import RestCatalog
    import polars as pl
    import datetime as dt
    import sqlalchemy as sa

    # The functions we defined in the previous notebook are defined in utils.py
    from utils import get_iceberg_manifest, get_iceberg_manifest_list, get_iceberg_metadata, read_house_prices
    from s3fs import S3FileSystem
    return (
        RestCatalog,
        S3FileSystem,
        dt,
        get_iceberg_manifest_list,
        get_iceberg_metadata,
        mo,
        pl,
        read_house_prices,
        sa,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Updating Metadata

    We've added data to our tables and inspected how Iceberg keeps track of the data in the metadata files

    Usually when Now that we've added some data, we've found out that we've made a mistake - we should have added a `_loaded_at` column to our data, so that we can differentiate downstream between the source timestamp and our loaded time
    """
    )
    return


@app.cell
def _(RestCatalog, S3FileSystem):
    # Get a reference to our catalog and table again
    catalog = RestCatalog("lakekeeper", uri="http://lakekeeper:8181/catalog", warehouse="lakehouse")
    house_prices = catalog.load_table('house_prices.raw')
    fs = S3FileSystem(endpoint_url="http://minio:9000", key="minio", secret="minio1234")
    return fs, house_prices


@app.cell
def _(dt, pl, read_house_prices):
    timestamp = dt.datetime.now(tz=dt.UTC)
    house_prices_2022 = read_house_prices("data/pp-2022.csv").with_columns(pl.lit(timestamp).alias("_loaded_at"))
    house_prices_2022
    return house_prices_2022, timestamp


@app.cell
def _(house_prices, house_prices_2022):
    try:
        house_prices.upsert(house_prices_2022.to_arrow())
    except ValueError as e:
        # Print out the error message instead of crashing
        print(e.args[0])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Pyiceberg is preventing us from doing something we shouldn't - Iceberg has a fixed schema, so we can't just add arbitrary columns to it. We need to update the schema to accomodate our new column. 

    Pyiceberg gives us the ability to do this within a transaction to live up to Iceberg's ACID guarantees.

    The new schema is added to the Iceberg metadata in the `schemas` array. Note that each of our snapshots reference the schema at the time the data was written. That way Iceberg can keep track of the schema evolution.
    """
    )
    return


@app.cell
def _(fs, get_iceberg_metadata, house_prices):
    from pyiceberg.types import TimestamptzType

    with house_prices.update_schema() as schema:
        # Avoid crashing for demo purposes
        if "_loaded_at" not in house_prices.schema().column_names:
            schema.add_column("_loaded_at", TimestamptzType(), doc="The date this row was loaded")
        else:
            print("_loaded_at already in schema")
        
    get_iceberg_metadata(fs, house_prices)
    return


@app.cell
def _(mo):
    mo.md(r"""Now we have our `_loaded_at` column as part of the table schema, Iceberg is happy for us to add our data with the new column""")
    return


@app.cell
def _(house_prices, house_prices_2022):
    house_prices.append(house_prices_2022.to_arrow().cast(house_prices.schema().as_arrow()))
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    What about the data we already added? How would we modify that data? Here we start running into some limitations of a foundational library like `pyiceberg` - we can do it, but wouldn't it be much easier to write an `UPDATE` in SQL and not have to worry about the details? 

    This is the power of Iceberg - we have the ability to switch query engines to suit our usecase - in this case, I want to use Trino to update the data back in time.

    Let's use the Marimo SQL editor to quickly verify how many nulls we currently have
    """
    )
    return


@app.cell
def _(sa):
    engine = sa.create_engine("trino://trino:@trino:8080/lakekeeper")
    return (engine,)


@app.cell
def _(engine, house_prices, mo, raw):
    _df = mo.sql(
        f"""
        SELECT
            count(*) as all_rows,
            NULL as null_rows
        FROM
            house_prices.raw
        UNION ALL
        SELECT
            NULL,
            count('*') as null_rows
        FROM
            house_prices.raw
        WHERE
            _loaded_at is null
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""Trino has a SQLAlchemy dialect built-in to the `trino` python package, so it's straightforward to run some SQL like we're used to""")
    return


@app.cell
def _(engine, sa, timestamp):

    with engine.connect() as conn:
        sql = f"UPDATE house_prices.raw SET _loaded_at = from_iso8601_timestamp('{timestamp.isoformat()}') WHERE _loaded_at is null"
        result = conn.execute(sa.text(sql))
        print(result.fetchone())
    return


@app.cell
def _(engine, house_prices, mo, raw):
    _df = mo.sql(
        f"""
        SELECT
            count(*) as all_rows,
            NULL as null_rows
        FROM
            house_prices.raw
        UNION ALL
        SELECT
            NULL,
            count('*') as null_rows
        FROM
            house_prices.raw
        WHERE
            _loaded_at is null
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""We should now have a new snapshot - let's have a peek""")
    return


@app.cell
def _(fs, get_iceberg_metadata, house_prices):
    get_iceberg_metadata(fs, house_prices)
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Deletes

    We have a new operation `overwrite` - Parquet is immutable, so we have to physically write out a new file and delete the old one. That is expensive, so Iceberg uses delete files to avoid having to up-front do the work of actually deleting data.

    In Iceberg V2, there are positional-deletes and equality-deletes. These are both represented by a new delete file, which is just a parquet file which specifies rows to mark as deleted, either by a filter like `transaction_id = '{045A1898-4ABF-9A24-E063-4804A8C048EA}'` or by position, `filea.parquet, 0`.

    \\\ admonition | Deprecation Warning
    Positional deletes will be replaced by deletion vectors in Iceberg V3
    \\\
    """
    )
    return


@app.cell
def _():
    return


@app.cell
def _(fs, get_iceberg_manifest_list, house_prices):
    get_iceberg_manifest_list(fs, house_prices)
    return


if __name__ == "__main__":
    app.run()
