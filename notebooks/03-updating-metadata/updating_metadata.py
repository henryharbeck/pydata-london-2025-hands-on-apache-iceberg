

import marimo

__generated_with = "0.13.3"
app = marimo.App(width="full")


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
def _():
    import marimo as mo
    from pyiceberg.catalog.rest import RestCatalog
    import polars as pl
    import datetime as dt
    import sqlalchemy as sa

    from import_demo import get_iceberg_manifest, get_iceberg_manifest_list, get_iceberg_metadata
    from s3fs import S3FileSystem
    return RestCatalog, dt, mo, pl, sa


@app.cell
def _(RestCatalog):
    # Get a reference to our catalog and table again
    catalog = RestCatalog("lakekeeper", uri="http://lakekeeper:8181/catalog", warehouse="lakehouse")
    house_prices = catalog.load_table('house_prices.raw')
    return (house_prices,)


@app.cell
def _(dt, pl):
    #
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
    house_prices_2024 = read_house_prices("data/pp-2024.csv").with_columns(pl.lit(dt.datetime.now(tz=dt.UTC)).alias("_loaded_at"))
    house_prices_2024
    return (house_prices_2024,)


@app.cell
def _(house_prices, house_prices_2024):
    try:
        house_prices.upsert(house_prices_2024.to_arrow())
    except ValueError as e:
        # Print out the error message instead of crashing
        print(e.args[0])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Pyiceberg is preventing us from doing something we shouldn't - Iceberg has a fixed schema, so we can't just add arbitrary columns to it. We need to update the schema to accomodate our new column""")
    return


@app.cell
def _(house_prices):
    from pyiceberg.types import TimestamptzType

    with house_prices.update_schema() as schema:
        # Make sure this is an idempotent operation
        if "_loaded_at" not in house_prices.schema().column_names:
            schema.add_column("_loaded_at", TimestamptzType(), doc="The date this row was loaded")
        else:
            print("_loaded_at already in schema")
    return


@app.cell
def _(house_prices, house_prices_2024):
    house_prices.upsert(house_prices_2024.to_arrow().cast(house_prices.schema().as_arrow()), join_cols=['transaction_id'])
    return


@app.cell
def _(sa):
    engine = sa.create_engine("trino://trino:@trino:8080/lakekeeper")
    return (engine,)


@app.cell
def _():
    return


@app.cell
def _(engine, house_prices, mo, raw):
    _df = mo.sql(
        f"""
        SELECT * FROM house_prices.raw
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()
