

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
        # Updating Metadata

        We've added data to our tables and inspected how Iceberg keeps track of the data in the metadata files

        Usually when Now that we've added some data, we've found out that we've made a mistake - we should have added a `_loaded_at` column to our data, so that we can catch late-arriving data
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    from pyiceberg.catalog.rest import RestCatalog
    import polars as pl
    import datetime as dt
    return RestCatalog, dt, mo, pl


@app.cell
def _(RestCatalog):
    catalog = RestCatalog("lakekeeper", uri="http://lakekeeper:8181/catalog", warehouse="lakehouse")
    house_prices = catalog.load_table('house_prices.raw')
    return (house_prices,)


@app.cell
def _(dt, pl):
    def read_house_prices(filename: str) -> pl.DataFrame:
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
                try_parse_dates=True,
                has_header=False,
                new_columns=house_prices_columns,
            )
            .with_columns(pl.col("date_of_transfer").cast(pl.Date()))
            .collect()
        )
        return df
    house_prices_2024 = read_house_prices("data/pp-2024.csv").with_columns(pl.lit(dt.datetime.now(tz=dt.UTC)).alias("_loaded_at")).drop("record_status")
    return (house_prices_2024,)


@app.cell
def _(house_prices, house_prices_2024):
    try:
        house_prices.upsert(house_prices_2024.to_arrow())
    except ValueError as e:
        # Print out the error message instead of crashing
        print(e.args[0])
    return


@app.cell
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
    house_prices.upsert(house_prices_2024.to_arrow().cast(house_prices.schema().as_arrow()))
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
