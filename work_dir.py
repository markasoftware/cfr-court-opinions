from collections.abc import Iterator
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

    def title_xml_path(self, year: int, month: int, title: int) -> Path:
        return self._work_dir / "cfr-xml" / str(year) / str(month) / f"title-{title}.xml"
