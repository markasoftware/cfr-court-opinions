import click
import dataclasses as dc
import json
from pathlib import Path
import logging
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert
from tqdm import tqdm

import tables
from work_dir import WorkDir

_LOGGER = logging.getLogger(__name__)

def insert_ecfr(work_dir: WorkDir, engine: Engine) -> None:
    ...

def insert_pdfs(work_dir: WorkDir, engine: Engine) -> None:
    # build a global map from package ID to case title human readable. I should have simply put this
    # in the references.json, but as of time of writing scraping is already mostly done and I don't
    # want to restart it.
    package_id_to_package_dict = {}
    for package_list_path in work_dir.package_list_paths_iter():
        with open(package_list_path, "r", encoding="utf8") as f:
            package_list_json = json.load(f)
        for package in package_list_json:
            package_id_to_package_dict[package["package_id"]] = package

    with Session(engine) as session:
        for cfr_reference_path in tqdm(work_dir.cfr_reference_paths_iter(), desc="pdfs"):
            with open(cfr_reference_path, "r", encoding="utf8") as f:
                references_json = json.load(f)

            for reference_json in references_json:
                package_dict = package_id_to_package_dict[reference_json["package_id"]]

                session.execute(insert(tables.CourtOpinionPdf).values(
                    package_id=reference_json["package_id"],
                    granule_id=reference_json["granule_id"],
                    case_title=package_dict["title"],
                    date_opinion_issued=package_dict["date_issued_str"],
                ).on_conflict_do_nothing())

                session.execute(insert(tables.CfrPdf).values(
                    granule_id=reference_json["granule_id"],
                    title=reference_json["cfr_title"],
                    part=reference_json["cfr_part"],
                    subpart=reference_json["cfr_subpart"],
                ).on_conflict_do_nothing())

        session.commit()
        

@click.command()
@click.option("--work-dir", "work_dir_path", type=click.Path(path_type=Path, file_okay=False, exists=True), help="The work dir you provided when scraping PDFs and the eCFR itself")
@click.option("--database", "database_path", type=click.Path(path_type=Path, dir_okay=False), help="Where to place the output sqlite file")
def make_database(work_dir_path: Path, database_path: Path) -> None:
    work_dir = WorkDir(work_dir_path)
    engine = create_engine(f"sqlite:///{database_path}")
    tables.Base.metadata.create_all(engine)  # create tables

    insert_ecfr(work_dir, engine)

    insert_pdfs(work_dir, engine)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    make_database()
