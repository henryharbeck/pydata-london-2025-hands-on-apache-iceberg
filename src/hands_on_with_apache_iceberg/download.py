import pathlib

import httpx
import asyncio
from pathlib import Path
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
    TaskID,
)

import typer


async def download_all_files(urls: list[str], output_dir: Path) -> None:
    """
    Download multiple files asynchronously with a progress bar.
    """
    # Create a progress bar
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        # Create tasks for individual downloads
        download_tasks = []
        for url in urls:
            filename = url.split("/")[-1]
            task = progress.add_task(f"[cyan]Downloading {filename}", total=1.0)
            download_tasks.append((url, task, filename))

        # Create async tasks
        async with httpx.AsyncClient(follow_redirects=True) as client:
            tasks = [
                download_file(client, url, task, filename, output_dir, progress)
                for url, task, filename in download_tasks
            ]
            # Wait for all downloads to complete
            await asyncio.gather(*tasks)


async def download_file(
    client: httpx.AsyncClient,
    url: str,
    task_id: TaskID,
    filename: str,
    output_dir: pathlib.Path,
    progress: Progress,
) -> None:
    """
    Download a single file asynchronously and update the progress bar.
    """
    # Prepare the output file
    output_file = output_dir / filename

    # Check if the file already exists
    if output_file.exists():
        # Update the task description to indicate that the file was skipped
        progress.update(
            task_id,
            completed=1.0,
            description=f"[blue]Skipped: {filename} (already exists)",
        )
        return

    try:
        # Start streaming response
        async with client.stream("GET", url) as response:
            response.raise_for_status()

            # Get total size for progress tracking
            total_size = int(response.headers.get("Content-Length", 0))
            progress.update(task_id, total=total_size if total_size > 0 else 1.0)

            downloaded = 0

            # Write to file as chunks are received
            with open(output_file, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=8192):
                    f.write(chunk)
                    downloaded += len(chunk)
                    # Update progress
                    progress.update(task_id, completed=downloaded)

        # Ensure the task is marked as complete
        progress.update(
            task_id,
            completed=total_size if total_size > 0 else 1.0,
            description=f"[green]✓ Downloaded: {filename}",
        )

    except httpx.HTTPError as e:
        typer.echo(f"Error downloading {url}: {e}", err=True)
        # Mark the task as failed but completed
        progress.update(task_id, completed=1.0, description=f"[red]Failed: {filename}")


def download_files(urls: list[str], output_dir: pathlib.Path) -> pathlib.Path:
    """
    Download data from a list of URLs using async HTTPX with a Rich progress bar.
    """
    # Create the output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Run the async download function
    asyncio.run(download_all_files(urls, output_dir))

    return output_dir
