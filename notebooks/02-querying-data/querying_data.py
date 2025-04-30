

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # Querying the data

        The key selling point for Iceberg is that we have the option of using many different query engines to read from the same data storage. Let's run some simple queries using a few different query engines. Many of these engines are using pyiceberg as a jumping-off point, either to directly interface with it, or as a source for the current metadata.json
        """
    )
    return


@app.cell
def _():
    from pyiceberg.catalog.rest import RestCatalog

    catalog = RestCatalog("lakekeeper", uri="http://lakekeeper:8181/catalog", warehouse="lakehouse")
    return (catalog,)


@app.cell
def _(catalog):
    table = catalog.load_table("house_prices.raw")
    return (table,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Pyiceberg

        Let's see how Pyiceberg handles querying first. For each of these examples, we'll do something simple - like taking the mean monthly house price per month in 2024.
        """
    )
    return


@app.cell
def _(table):
    from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual, Or

    iceberg_results = table.scan(
        selected_fields=["price", "date_of_transfer"],
        row_filter="date_of_transfer >= '2024-01-01' and date_of_transfer <= '2024-12-31'",
    )
    return (iceberg_results,)


@app.cell
def _(iceberg_results, pl):
    iceberg_results.to_polars().group_by(
        pl.col("date_of_transfer").dt.month()
    ).agg(pl.col("price").mean()).sort(by="date_of_transfer")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Pyiceberg provides us with limited filtering and projection capabilities - it provides the building blocks for libraries that build on top of Pyiceberg. We used Polars to finish the job in this example, but polars can read Iceberg directly - no need for the extra step""")
    return


@app.cell
def _(pl, table):
    polars_df = pl.scan_iceberg(table).group_by(
        pl.col("date_of_transfer").dt.month()
    ).agg(pl.col("price").mean()).sort(by="date_of_transfer").collect()
    polars_df.style.fmt_number("price", decimals=2)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Duckdb is also an excellent choice for working with Iceberg, especially if you want to stick to SQL""")
    return


@app.cell
def _(table):
    import duckdb

    # Create a duckdb connection
    conn = duckdb.connect()
    # Load the Iceberg extension for DuckDB
    conn.install_extension('iceberg')
    conn.load_extension('iceberg')
    conn.install_extension("avro")
    conn.load_extension("avro")

    # To be able to read the Iceberg metadata, we need credentials for the bucket
    conn.sql("""
    CREATE OR REPLACE SECRET minio (
    TYPE S3,
    ENDPOINT 'minio:9000',
    KEY_ID 'minio',
    SECRET 'minio1234',
    USE_SSL false,
    URL_STYLE 'path'
    )
    """)

    # We can read the iceberg data using DuckDB
    conn.sql(f"""
    SELECT month(date_of_transfer) as transfer_month, mean(price) as mean_price
    FROM iceberg_scan('{table.metadata_location}')
    GROUP BY 1
    """).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Trino is another popular option, especially since AWS provides it as a serverless query engine through Athena. Trino is another SQL-based query engine, so the query looks pretty similar, just using Trino SQL dialect""")
    return


@app.cell
def _(pl):
    import sqlalchemy as sa

    engine = sa.create_engine("trino://trino:@trino:8080/lakekeeper")

    sql = """
    SELECT month(date_of_transfer) as transfer_month, avg(price) as mean_price 
    FROM house_prices.raw
    GROUP BY 1
    ORDER BY 1
    """

    with engine.connect() as c:
        df = pl.read_database(sa.text(sql), c)
    df
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # Exercise:

        Try running a query to calculate the average house price for your county. If you don't live in the UK - pick the funniest sounding one. (I quite like WOKINGHAM)
        """
    )
    return


@app.cell(hide_code=True)
def _():
    return


if __name__ == "__main__":
    app.run()
