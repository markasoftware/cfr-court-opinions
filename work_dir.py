from __future__ import annotations

from collections.abc import Iterator
import dataclasses as dc
from pathlib import Path

class WorkDir:
    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir
        work_dir.mkdir(parents=True, exist_ok=True)

    def package_list_path(self, year: int, month: int) -> Path:
        return self._work_dir / str(year) / str(month) / "packages.json"

    def package_list_paths_iter(self) -> Iterator[Path]:
        return self._work_dir.glob("*/*/packages.json")

    def cfr_references_path(self, year: int, month: int, package_id: str) -> Path:
        return self._work_dir / str(year) / str(month) / f"{package_id}-references.json"

    def cfr_reference_paths_iter(self) -> Iterator[Path]:
        return self._work_dir.glob("*/*/USCOURTS-*-references.json")

    def agencies_json_path(self) -> Path:
        return self._work_dir / "ecfr-agencies.json"

    def title_structure_path(self, year: int, month: int, title: int) -> Path:
        return self._work_dir / "cfr-structure" / str(year) / str(month) / f"title-{title}-structure.json"

    def part_xml_path(self, year: int, month: int, title: int, chapter: str, part: int) -> Path:
        return self._work_dir / "cfr-xml" / str(year) / str(month) / f"title-{title}" / f"chapter-{chapter}" / f"part-{part}.xml"

    def part_xml_paths_iter(self, year: int, month: int) -> Iterator[PartXmlDescriptor]:
        for title_path in (self._work_dir / "cfr-xml" / str(year) / str(month)).glob("title-*"):
            title = int(title_path.name.split('-')[1])
            for chapter_path in title_path.glob("chapter-*"):
                chapter = chapter_path.name.split('-')[1]
                for part_path in chapter_path.glob("part-*.xml"):
                    part = int(part_path.name.split('.')[0].split('-')[1])
                    yield PartXmlDescriptor(path=part_path, title=title, chapter=chapter, part=part)


@dc.dataclass(frozen=True, kw_only=True)
class PartXmlDescriptor:
    path: Path
    title: int
    chapter: str
    part: int
