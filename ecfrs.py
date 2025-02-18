from __future__ import annotations

from collections.abc import Iterator
import click
import datetime
import json
import logging
from pathlib import Path
import subprocess

from work_dir import WorkDir

_LOGGER = logging.getLogger(__name__)

ecfr_api_host = "https://www.ecfr.gov"

class ScrapeContext:
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir

def agencies_json(work_dir: WorkDir) -> None:
    path = work_dir.agencies_json_path()
    if path.exists():
        _LOGGER.info("Agencies json already present, willn't download")
        return

    _LOGGER.info("Downloading agencies json...")
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".tmp")
    subprocess.check_call(["curl", "--fail", "--output", str(temp_path), f"{ecfr_api_host}/api/admin/v1/agencies.json"])
    temp_path.rename(path)

def structure_json(work_dir: WorkDir, year: int, month: int, title: int) -> None:
    path = work_dir.title_structure_path(year, month, title)
    if path.exists():
        _LOGGER.info(f"Structure json already present for {year}/{month}/title {title}, willn't download")
        return

    _LOGGER.info(f"Downloading structure json for {year}/{month}/title {title}")
    as_of_date = datetime.date(year, month, 1)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".tmp")
    subprocess.check_call(["curl", "--fail", "--output", str(temp_path), f"{ecfr_api_host}/api/versioner/v1/structure/{as_of_date.isoformat()}/title-{title}.json"])
    temp_path.rename(path)

# recursively loop through all "children" until we find one of type "part"
def iter_structure(structure, wanted_type: str) -> Iterator:
    if structure["type"] == wanted_type:
        yield structure
    else:
        for child in structure.get("children", []):
            yield from iter_structure(child, wanted_type)

def title_xml(work_dir: WorkDir, year: int, month: int, title: int) -> None:
    with open(work_dir.title_structure_path(year, month, title), "r", encoding="utf8") as f:
        title_structure = json.load(f)

    # this is a bit of a hack: Some titles don't have chapters, and instead have parts directly under subtitles and shit...but it's rare enough so IDC
    for chapter_structure in iter_structure(title_structure, "chapter"):
        assert chapter_structure["type"] == "chapter"
        chapter = chapter_structure["identifier"]

        for part_structure in iter_structure(chapter_structure, "part"):
            assert part_structure["type"] == "part"
            try:
                part = int(part_structure["identifier"])
            except ValueError:
                _LOGGER.warning(f"Unable to parse part {part_structure['identifier']}, probably a reserved section, skipping")
                continue

            path = work_dir.part_xml_path(year, month, title=title, chapter=chapter, part=part)
            if path.exists():
                _LOGGER.info(f"XML for {year}/{month}/title {title}/part {part} already exists, willn't download")
                continue

            _LOGGER.info(f"Downloading XML for {year}/{month}/title {title}/part {part}...")
            as_of_date = datetime.date(year, month, 1)
            path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = path.with_suffix(".tmp")
            subprocess.check_call(["curl", "--fail", "--retry", "6", "--retry-connrefused", "--max-time", "600", "--output", str(temp_path), f"{ecfr_api_host}/api/versioner/v1/full/{as_of_date.isoformat()}/title-{title}.xml?part={part}"])
            temp_path.rename(path)


def title_descriptions_json(work_dir: WorkDir, year: int, month: int, title: int) -> None:
    path = work_dir.title_descriptions_json_path(year, month, title)
    if path.exists():
        _LOGGER.info(f"Skipping title descriptions for {year}/{month}/title {title}")
        return

    with open(work_dir.title_structure_path(year, month, title), "r", encoding="utf8") as f:
        title_structure = json.load(f)

    result = {
        "title": {str(title): title_structure["label_description"]},
        "part": {},
        "section": {}
    }

    for part_structure in iter_structure(title_structure, "part"):
        result["part"][part_structure["identifier"]] = part_structure["label_description"]

    for section_structure in iter_structure(title_structure, "section"):
        result["section"][section_structure["identifier"]] = section_structure["label_description"]

    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf8") as f:
        json.dump(result, f)
    temp_path.rename(path)
    

@click.command()
@click.option("--work-dir", "work_dir_path", type=click.Path(path_type=Path, file_okay=False, exists=True), required=True, help="Where to put temp files. Use same work dir as for PDFs")
def scrape_ecfrs(work_dir_path: Path) -> None:
    work_dir = WorkDir(work_dir_path)

    agencies_json(work_dir)
    for title in range(1, 50+1):
        if title == 35:  # "reserved" title
            continue

        structure_json(work_dir, 2025, 1, title)
        title_xml(work_dir, 2025, 1, title)
        title_descriptions_json(work_dir, 2025, 1, title)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scrape_ecfrs()
