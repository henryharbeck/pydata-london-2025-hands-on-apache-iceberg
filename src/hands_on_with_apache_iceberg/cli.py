import pathlib

import typer

from hands_on_with_apache_iceberg import download

app = typer.Typer(no_args_is_help=True)


@app.command("download")
def download_data(
    output_dir: pathlib.Path = typer.Option("./data", help="Directory to save downloaded files"),
) -> None:
    """
    Download data from a list of URLs using async HTTPX with a Rich progress bar.
    """

    base_url = (
        "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
    )
    urls = [f"{base_url}/pp-{year}.csv" for year in range(1995, 2025)]
    # Example file: pp-2024.csv
    # Use the download module to handle the file downloads
    output_path = download.download_files(urls, output_dir)

    typer.echo(f"All files downloaded successfully to {output_path}")


@app.command("bootstrap")
def bootstrap_demo() -> None:
    pass
