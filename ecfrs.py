from __future__ import annotations

import click
import datetime
import logging
from pathlib import Path
import subprocess

_LOGGER = logging.getLogger(__name__)

ecfr_api_host = "https://www.ecfr.gov"

class ScrapeContext:
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

    def agencies_json_path(self) -> Path:
        return self._work_dir / "ecfr-agencies.json"

    def title_xml_path(self, year: int, month: int, title: int) -> Path:
        return self._work_dir / "cfr-xml" / str(year) / str(month) / f"title-{title}.xml"

def agencies_json(ctx: ScrapeContext) -> None:
    path = ctx.agencies_json_path()
    if path.exists():
        _LOGGER.info("Agencies json already present, willn't download")
        return

    _LOGGER.info("Downloading agencies json...")
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".tmp")
    subprocess.check_call(["curl", "--fail", "--output", str(temp_path), f"{ecfr_api_host}/api/admin/v1/agencies.json"])
    temp_path.rename(path)

def title_xml(ctx: ScrapeContext, year: int, month: int, title: int) -> None:
    path = ctx.title_xml_path(year, month, title)
    if path.exists():
        _LOGGER.info(f"XML for {year}/{month}/title {title} already exists, willn't download")
        return

    _LOGGER.info(f"Downloading XML for {year}/{month}/title {title}...")
    as_of_date = datetime.date(year, month, 1)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".tmp")
    subprocess.check_call(["curl", "--fail", "--retry", "6", "--retry-connrefused", "--max-time", "600", "--output", str(temp_path), f"{ecfr_api_host}/api/versioner/v1/full/{as_of_date.isoformat()}/title-{title}.xml"])
    temp_path.rename(path)

@click.command()
@click.option("--work-dir", type=click.Path(path_type=Path, file_okay=False, exists=True), required=True, help="Where to put temp files. Use same work dir as for PDFs")
def scrape_ecfrs(work_dir: Path) -> None:
    ctx = ScrapeContext(work_dir)

    agencies_json(ctx)
    for title in range(1, 50+1):
        if title == 35:  # "reserved" title
            continue

        title_xml(ctx, 2025, 1, title)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scrape_ecfrs()
