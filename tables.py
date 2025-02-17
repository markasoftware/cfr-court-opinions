from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class CfrSubpart(Base):
    """Using title/part/subpart as the primary key isn't exactly what our founding fathers wanted,
    but it'll do."""
    __tablename__ = "cfr_subpart"

    title: Mapped[int] = mapped_column(primary_key=True)
    chapter: Mapped[str]
    part: Mapped[int] = mapped_column(primary_key=True)
    subpart: Mapped[int] = mapped_column(primary_key=True)

    num_words: Mapped[int]

class CourtOpinionPdf(Base):
    __tablename__ = "court_opinion_pdf"

    package_id: Mapped[str]
    granule_id: Mapped[str] = mapped_column(primary_key=True)
    case_title: Mapped[str]  # technically this isn't normalized because there's a 1-1 correspondence between package IDs and case titles
    date_opinion_issued: Mapped[str]  # YYYY-MM-DD

    def link(self) -> str:
        # I've never seen any non-url-safe characters in the package or granule IDs; fingers crossed!
        return f"https://www.govinfo.gov/content/pkg/{self.package_id}/pdf/{self.granule_id}.pdf"
    
class CfrPdf(Base):
    """Join table between CFRs and PDFs. Records which CFRs are mentioned from which PDFs"""
    __tablename__ = "cfr_pdf"

    granule_id: Mapped[str] = mapped_column(ForeignKey(CourtOpinionPdf.granule_id), primary_key=True)
    title: Mapped[int] = mapped_column(ForeignKey(CfrSubpart.title), primary_key=True)
    part: Mapped[int] = mapped_column(ForeignKey(CfrSubpart.part), primary_key=True)
    subpart: Mapped[int] = mapped_column(ForeignKey(CfrSubpart.subpart), primary_key=True)
