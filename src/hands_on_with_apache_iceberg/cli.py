import pathlib

import httpx
import s3fs
import typer
from pyiceberg.exceptions import ForbiddenError, NoSuchNamespaceError
from rich.console import Console

from hands_on_with_apache_iceberg import bootstrap, download

app = typer.Typer(no_args_is_help=True)
console = Console()

@app.command("download")
def download_data(
    output_dir: pathlib.Path = typer.Option(
        "./data", help="Directory to save downloaded files"
    ),
) -> None:
    """
    Download data from a list of URLs using async HTTPX with a Rich progress bar.
    """

    base_url = (
        "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
    )
    # Example file: pp-2024.csv
    urls = [f"{base_url}/pp-{year}.csv" for year in range(1995, 2025)]
    # Example file: pp-2024.csv
    output_path = download.download_files(urls, output_dir)

    console.print(f"[green]All files downloaded successfully to {output_path}")


@app.command("bootstrap")
def bootstrap_demo() -> None:
    """Bootstrap the demo Iceberg warehouse"""
    fs = s3fs.S3FileSystem(endpoint_url="http://localhost:9000",
                           key="minio",
                           secret="minio1234",
                           use_ssl=False)

    with console.status("[bold blue]Creating bucket...") as status:
        bootstrap.create_bucket(fs, "warehouse")
        console.print("[bold green]✅ Bucket created successfully")

        status.update("[bold blue]Bootstrapping project...")
        with httpx.Client(base_url="http://localhost:8181") as client:
            bootstrap.bootstrap_project(client)
            console.print("[bold green]✅ Project bootstrapped successfully")

            status.update("[bold blue]Creating warehouse...")
            bootstrap.create_warehouse(client, "warehouse")
            console.print("[bold green]✅ Warehouse created successfully")

@app.command("clear")
def clear_data() -> None:
    """Delete all Iceberg data to start from scratch"""
    from pyiceberg.catalog.rest import RestCatalog
    catalog = RestCatalog("lakekeeper", uri="http://localhost:8181/catalog", warehouse="lakehouse")

    with console.status("[bold blue]Deleting data...") as status:
        try:
            catalog.drop_table("house_prices.raw", purge_requested=True)
            status.update("Table dropped!")
        except ForbiddenError:
            console.print("[bold red]Table not found!")
        try:
            catalog.drop_namespace("house_prices")
            status.update("Namespace dropped!")
        except NoSuchNamespaceError:
            console.print("[bold red]Namespace not found!")
        console.print("✅ Data cleared successfully!")