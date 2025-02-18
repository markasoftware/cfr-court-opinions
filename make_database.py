import click
from collections import defaultdict
import dataclasses as dc
import json
from pathlib import Path
import logging
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert
from tqdm import tqdm
import xml.etree.ElementTree as ET

import tables
from work_dir import WorkDir

_LOGGER = logging.getLogger(__name__)

def insert_agencies(work_dir: WorkDir, engine: Engine) -> None:
    with open(work_dir.agencies_json_path(), "r", encoding="utf8") as f:
        agencies_json = json.load(f)
    with Session(engine) as session:
        def insert_refs(agency):
            for cfr_ref in agency["cfr_references"]:
                if 'chapter' not in cfr_ref and 'subtitle' in cfr_ref:
                    # there are 6 cfr references that are to subtitle instead of chapter. Not going to deal with these.
                    continue
                # there are also a number of agencies that are specific to a subchapter or part
                # while also specifying a chapter. TODO try to deal with this, though as is we'll
                # just slightly overestimate the word count so NBD.
                session.execute(insert(tables.CfrAgency).values(
                    agency = agency["name"],
                    title = cfr_ref["title"],
                    chapter = cfr_ref["chapter"],
                ).on_conflict_do_nothing())
            for child in agency.get('children', []):
                insert_refs(child)

        for agency_json in agencies_json["agencies"]:
            insert_refs(agency_json)

        session.commit()

def insert_ecfr(work_dir: WorkDir, engine: Engine) -> None:
    with Session(engine) as session:
        for part_xml_desc in work_dir.part_xml_paths_iter(2025, 1):
            # determine how many words are in the XML
            title_descriptions_json_path = work_dir.title_descriptions_json_path(2025, 1, part_xml_desc.title)
            with open(title_descriptions_json_path, "r") as f:
                title_descriptions = json.load(f)

            tree = ET.parse(part_xml_desc.path)
            tree_root = tree.getroot()

            word_count_per_section = defaultdict(int)

            for div8 in tree_root.findall('.//DIV8'):
                split_full_section_name = div8.attrib['N'].split(".")
                if len(split_full_section_name) != 2:
                    # TODO investigate what's happening when there's a "range" of sections -- eg 457.104-457.109. Is it just reserved sections?
                    _LOGGER.warning(f"section name format: {div8.attrib['N']}")
                    continue
                section = int(''.join(c for c in split_full_section_name[1] if c.isdigit()))

                for text in div8.itertext():
                    word_count_per_section[section] += len(text.split())

            for section, word_count in word_count_per_section.items():
                _LOGGER.info(f"Found {word_count} many words in title {part_xml_desc.title}/part {part_xml_desc.part}/section {section}")
                session.add(tables.CfrSection(
                    title=part_xml_desc.title,
                    chapter=part_xml_desc.chapter,
                    part=part_xml_desc.part,
                    section=section,
                    num_words=word_count,
                    description=title_descriptions["section"].get(f"{part_xml_desc.part}.{section}", ''),
                ))

        for title in range(1, 50+1):
            if title == 35:
                continue
            title_descriptions_json_path = work_dir.title_descriptions_json_path(2025, 1, title)
            with open(title_descriptions_json_path, "r") as f:
                title_descriptions = json.load(f)

            session.add(tables.CfrTitle(
                title=title,
                description=title_descriptions["title"][str(title)],
            ))
            for part, description in title_descriptions["part"].items():
                session.add(tables.CfrPart(
                    title=title,
                    part=part,
                    description=description,
                ))

        session.commit()


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
                    section=reference_json["cfr_subpart"],  # what's referred to as "subpart" in the reference jsons is actually section
                ).on_conflict_do_nothing())

        session.commit()
        

@click.command()
@click.option("--work-dir", "work_dir_path", type=click.Path(path_type=Path, file_okay=False, exists=True), help="The work dir you provided when scraping PDFs and the eCFR itself")
@click.option("--database", "database_path", type=click.Path(path_type=Path, dir_okay=False), help="Where to place the output sqlite file")
def make_database(work_dir_path: Path, database_path: Path) -> None:
    database_path.unlink(missing_ok=True)

    work_dir = WorkDir(work_dir_path)
    engine = create_engine(f"sqlite:///{database_path}")
    tables.Base.metadata.create_all(engine)  # create tables

    insert_agencies(work_dir, engine)
    insert_ecfr(work_dir, engine)
    insert_pdfs(work_dir, engine)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    make_database()
