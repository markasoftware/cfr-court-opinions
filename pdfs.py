from __future__ import annotations

import click
import dataclasses as dc
import datetime
import json
import logging
from pathlib import Path
import pdfminer.high_level
import urllib.parse
import requests
import subprocess
import tempfile
from tqdm import tqdm
import typing as ty
from zipfile import ZipFile

_LOGGER = logging.getLogger(__name__)

responseName = str

govinfo_api_host = "https://api.govinfo.gov"

class GovInfoApi:
    def __init__(self, api_key: str):
        self._api_key = api_key

    def url_add_auth(self, url: str) -> str:
        """Given abase url and some query params, add auth info and return a full, ready-to-get url"""
        parsed_url = urllib.parse.urlparse(url)
        if parsed_url.query:
            parsed_url = parsed_url._replace(query=parsed_url.query + f"&api_key={urllib.parse.quote(self._api_key, safe='')}")
        else:
            parsed_url = parsed_url._replace(query=f"api_key={urllib.parse.quote(self._api_key, safe='')}")
        return parsed_url.geturl()

    def single_request(self, url: str) -> ty.Any:
        full_url = self.url_add_auth(url)
        _LOGGER.info(f"Making request to {full_url}")
        res = requests.get(full_url)
        res.raise_for_status()
        return res.json()


    def paged_request(self, url: str) -> list:
        """
        Return a list of the full json results from each query.  Will use the returned `nextPage`
        json field to determine the next page to download, and will continue until there's no
        `nextPage` left!
        """
        result = []

        full_url = self.url_add_auth(url)
        response = self.single_request(full_url)
        result.append(response)
        pages_requested = 1
        while response["nextPage"]:
            _LOGGER.info(f"Found page {pages_requested+1}, about to download it")
            response = self.single_request(self.url_add_auth(response["nextPage"]))
            result.append(response)
            pages_requested += 1

        _LOGGER.info(f"Done with paged download; downloaded {pages_requested} many pages")
        return result
        

@dc.dataclass(frozen=True)
class Package:
    """A single govinfo pakage"""
    package_id: str
    title: str
    last_modified_str: str
    date_issued_str: str
    package_link: str

    @staticmethod
    def from_govinfo_json(json_obj: ty.Any) -> Package:
        if not isinstance(json_obj, dict):
            raise ValueError("Cannot construct a Package from json that's not a dict")
        return Package(package_id=json_obj["packageId"], title=json_obj["title"], last_modified_str=json_obj["lastModified"], date_issued_str=json_obj["dateIssued"], package_link=json_obj["packageLink"])

def packages_to_json(packages: list[Package]) -> list[dict]:
    """write our on-disk format"""
    return [dc.asdict(package) for package in packages]

def json_to_packages(json: ty.Any) -> list[Package]:
    """read our on-disk format"""
    if not isinstance(json, list):
        raise ValueError("Cannot convert non-list JSON to list of packages")
    return [Package(**j) for j in json]

def safe_write_json(json_obj: ty.Any, output: Path) -> None:
    tmp_out = output.with_suffix(".tmp")
    tmp_out.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp_out, "w", encoding="utf8") as f:
        json.dump(json_obj, f)
    tmp_out.rename(output)

def download_package_list(year: int, month: int, api: GovInfoApi) -> list[Package]:
    """Do a paged download of all packages in this date range."""
    start_date = datetime.date(year, month, 1)
    next_month = 1 if month == 12 else month + 1
    next_year = year+1 if month == 12 else year
    end_date = datetime.date(next_year, next_month, 1) - datetime.timedelta(days=1)

    url = f"{govinfo_api_host}/published/{start_date.isoformat()}/{end_date.isoformat()}?collection=USCOURTS&pageSize=1000&offsetMark={urllib.parse.quote('*')}"
    responses = api.paged_request(url)
    return [Package.from_govinfo_json(package_json) for response in responses for package_json in response["packages"]]

def package_list(year: int, month: int, ctx: ScrapeContext) -> list[Package]:
    """Get the package list, using on-disk cache if available"""
    path = ctx.package_list_path(year, month)
    if path.exists():
        with open(path, "r", encoding="utf8") as f:
            package_list_json = json.load(f)
            result = json_to_packages(package_list_json)
            _LOGGER.info(f"Package list path already exists; read {len(result)} many packages")
            return result

    # packages list file doesn't already exist; download it!
    packages = download_package_list(year=year, month=month, api=ctx.api)

    safe_write_json(packages_to_json(packages), path)
    _LOGGER.info(f"Wrote {len(packages)} many packages to disk successfully!")

    return packages

def scrape_pdf(year: int, month: int, package: Package, ctx: ScrapeContext) -> None:
    cfr_references_path = ctx.cfr_references_path(year, month, package.package_id)
    if cfr_references_path.exists():
        return

    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)

        zip_path = temp_dir / "zip.zip"
        unzip_dir = temp_dir / "unzipped"
        unzip_dir.mkdir()

        url = ctx.api.url_add_auth(f"{govinfo_api_host}/packages/{package.package_id}/zip")
        _LOGGER.info(f"Downloading zip: {url}")
        # I trust curl to download a large file to disk far more than anything in Python
        subprocess.check_call(["curl", "--fail", "--retry", "3", "--retry-connrefused", "--connect-timeout", "10", "--max-time", "120", "--output", str(zip_path), url])

        with ZipFile(str(zip_path), "r") as zf:
            for entry in zf.namelist():
                if entry.endswith(".pdf"):
                    _LOGGER.info(f"Scanning PDF {entry}")
                    with zf.open(entry, "r") as f:
                        text = pdfminer.high_level.extract_text(f)
                        if len(text) < 50:  # sanity check
                            raise RuntimeError(f"Text from PDF {entry} was too short: {text}")




@dc.dataclass
class CfrReference:
    """All the information needed to encode a connection between a PDF and a CFR"""
    package_id: str
    granule_id: str
    orig_text: str
    cfr_title: int
    cfr_part: int
    cfr_subpart: int

class ScrapeContext:
    def __init__(self, api_key: str, work_dir: Path) -> None:
        work_dir.mkdir(parents=True, exist_ok=True)

        self.api = GovInfoApi(api_key)
        self.work_dir = work_dir

    def package_list_path(self, year: int, month: int) -> Path:
        return self.work_dir / str(year) / str(month) / "packages.json"

    def cfr_references_path(self, year: int, month: int, package_id: str) -> Path:
        return self.work_dir / str(year) / str(month) / f"{package_id}-references.json"


@click.command()
@click.option("--api-key", required=True, help="GovInfo API key")
@click.option("--year", type=int, required=True)
@click.option("--month", type=int, required=True)
@click.option("--work-dir", type=click.Path(path_type=Path, file_okay=False), required=True)
def scrape_pdfs(api_key: str, year: int, month: int, work_dir: Path):
    ctx = ScrapeContext(api_key=api_key, work_dir=work_dir)

    _LOGGER.info(f"About to scrape PDFs for {year}/{month}")
    packages = package_list(year=year, month=month, ctx=ctx)
    for package in tqdm(packages):
        scrape_pdf(year=year, month=month, package=package, ctx=ctx)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scrape_pdfs()
