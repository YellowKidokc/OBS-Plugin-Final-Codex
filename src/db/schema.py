"""
PostgreSQL Schema for Theophysics Definition Ingest Engine

This schema supports:
- Definition management with source attribution
- Excel data ingestion
- HTML table ingestion
- Obsidian/Markdown file ingestion
- Equation tracking across documents
- Usage drift detection
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, Boolean,
    ForeignKey, JSON, Enum, Float, Table, UniqueConstraint, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime
import enum

Base = declarative_base()


# === ENUMS ===

class SourceType(enum.Enum):
    """Source attribution - marks where data came from"""
    USER = "user"           # Human created/edited
    PYTHON = "python"       # Python script ingested
    WEB = "web"             # Fetched from internet
    PLUGIN = "plugin"       # Obsidian plugin or similar
    EXCEL = "excel"         # Imported from Excel
    HTML = "html"           # Parsed from HTML
    MARKDOWN = "markdown"   # Parsed from Markdown/Obsidian
    AI = "ai"               # AI-generated (copilot, etc.)
    UNKNOWN = "unknown"     # Source unknown


class ConfidenceLevel(enum.Enum):
    """How certain we are about this data"""
    VERIFIED = "verified"       # Human verified correct
    HIGH = "high"               # High confidence from reliable source
    MEDIUM = "medium"           # Medium confidence
    LOW = "low"                 # Low confidence - needs review
    UNVERIFIED = "unverified"   # Not yet checked


class DefinitionStatus(enum.Enum):
    """Status of a definition"""
    CANONICAL = "canonical"     # Official, locked definition
    DRAFT = "draft"             # Work in progress
    REVIEW = "review"           # Needs review
    DEPRECATED = "deprecated"   # No longer used
    CONFLICTED = "conflicted"   # Has conflicts detected


# === ASSOCIATION TABLES ===

definition_domains = Table(
    'definition_domains', Base.metadata,
    Column('definition_id', Integer, ForeignKey('definitions.id'), primary_key=True),
    Column('domain_id', Integer, ForeignKey('domains.id'), primary_key=True)
)

definition_equations = Table(
    'definition_equations', Base.metadata,
    Column('definition_id', Integer, ForeignKey('definitions.id'), primary_key=True),
    Column('equation_id', Integer, ForeignKey('equations.id'), primary_key=True)
)

definition_related = Table(
    'definition_related', Base.metadata,
    Column('definition_id', Integer, ForeignKey('definitions.id'), primary_key=True),
    Column('related_id', Integer, ForeignKey('definitions.id'), primary_key=True)
)


# === CORE TABLES ===

class IngestSession(Base):
    """Tracks each ingest operation for audit trail"""
    __tablename__ = 'ingest_sessions'

    id = Column(Integer, primary_key=True)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    source_type = Column(Enum(SourceType), nullable=False)
    source_path = Column(String(1024), nullable=True)  # File path or URL
    source_name = Column(String(255), nullable=True)   # Friendly name
    records_processed = Column(Integer, default=0)
    records_created = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    error_log = Column(Text, nullable=True)
    metadata = Column(JSON, nullable=True)  # Additional context

    # Relationships
    records = relationship("IngestRecord", back_populates="session")


class IngestRecord(Base):
    """Individual record from an ingest operation"""
    __tablename__ = 'ingest_records'

    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('ingest_sessions.id'), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Source tracking
    source_type = Column(Enum(SourceType), nullable=False)
    source_file = Column(String(1024), nullable=True)
    source_sheet = Column(String(255), nullable=True)  # For Excel
    source_row = Column(Integer, nullable=True)        # Row number
    source_cell = Column(String(50), nullable=True)    # Cell reference (e.g., "A1")
    source_url = Column(String(2048), nullable=True)   # For web sources

    # Data
    raw_content = Column(Text, nullable=True)          # Original content
    processed_content = Column(Text, nullable=True)    # Cleaned/processed
    content_hash = Column(String(64), nullable=True)   # For dedup

    # Status
    confidence = Column(Enum(ConfidenceLevel), default=ConfidenceLevel.UNVERIFIED)
    needs_review = Column(Boolean, default=True)
    review_notes = Column(Text, nullable=True)

    # Linking
    target_table = Column(String(100), nullable=True)  # Which table this goes to
    target_id = Column(Integer, nullable=True)         # ID in target table

    # Relationships
    session = relationship("IngestSession", back_populates="records")

    __table_args__ = (
        Index('idx_ingest_content_hash', 'content_hash'),
        Index('idx_ingest_source', 'source_type', 'source_file'),
    )


class Domain(Base):
    """Domains where definitions apply (physics, theology, etc.)"""
    __tablename__ = 'domains'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    description = Column(Text, nullable=True)
    parent_id = Column(Integer, ForeignKey('domains.id'), nullable=True)

    # Self-referential for hierarchy
    children = relationship("Domain", backref="parent", remote_side=[id])
    definitions = relationship("Definition", secondary=definition_domains, back_populates="domains")


class Definition(Base):
    """Core definition table - the heart of the system"""
    __tablename__ = 'definitions'

    id = Column(Integer, primary_key=True)

    # === Identity ===
    symbol = Column(String(50), nullable=True)         # e.g., "C", "χ", "Φ"
    name = Column(String(255), nullable=False)         # e.g., "Coherence"
    aliases = Column(JSON, nullable=True)              # ["informational coherence", "χ-coherence"]
    definition_id = Column(String(100), unique=True)   # e.g., "def-coherence"

    # === Core Definition ===
    canonical_definition = Column(Text, nullable=True) # One sentence canonical

    # === Ontological Category ===
    triad_position = Column(String(50), nullable=True)   # Necessity/Contingency/Relation
    domain_type = Column(String(50), nullable=True)      # Structure/Moral/Observer
    layer = Column(String(50), nullable=True)            # Numbers/Matter/Meaning/etc.

    # === Content Sections (matching the 10-section template) ===
    axioms = Column(JSON, nullable=True)               # List of axiom statements
    mathematical_primary = Column(Text, nullable=True) # Primary equations
    mathematical_dynamic = Column(Text, nullable=True) # Dynamical equations
    thresholds = Column(Text, nullable=True)           # Critical values
    domain_interpretations = Column(JSON, nullable=True)  # Per-domain meanings
    operationalization = Column(Text, nullable=True)   # How to measure
    failure_modes = Column(Text, nullable=True)        # What "broken" looks like
    integration_map = Column(JSON, nullable=True)      # Where it appears
    external_comparison = Column(Text, nullable=True)  # vs mainstream definitions
    notes = Column(Text, nullable=True)                # Additional notes

    # === Source Attribution ===
    source_type = Column(Enum(SourceType), default=SourceType.UNKNOWN)
    source_attribution = Column(String(255), nullable=True)  # "web", "python", "user", etc.
    source_url = Column(String(2048), nullable=True)
    source_file = Column(String(1024), nullable=True)

    # === Status & Tracking ===
    status = Column(Enum(DefinitionStatus), default=DefinitionStatus.DRAFT)
    confidence = Column(Enum(ConfidenceLevel), default=ConfidenceLevel.UNVERIFIED)
    last_reviewed = Column(DateTime, nullable=True)
    review_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String(100), nullable=True)

    # === Relationships ===
    domains = relationship("Domain", secondary=definition_domains, back_populates="definitions")
    equations = relationship("Equation", secondary=definition_equations, back_populates="definitions")
    related_terms = relationship(
        "Definition",
        secondary=definition_related,
        primaryjoin=id == definition_related.c.definition_id,
        secondaryjoin=id == definition_related.c.related_id
    )
    usages = relationship("DefinitionUsage", back_populates="definition")
    drift_logs = relationship("DriftLog", back_populates="definition")

    __table_args__ = (
        Index('idx_def_symbol', 'symbol'),
        Index('idx_def_name', 'name'),
        Index('idx_def_status', 'status'),
    )


class Equation(Base):
    """Tracks equations and where they appear"""
    __tablename__ = 'equations'

    id = Column(Integer, primary_key=True)
    equation_id = Column(String(100), unique=True)     # e.g., "eq-coherence-dynamic-01"
    latex = Column(Text, nullable=False)               # LaTeX representation
    plain_text = Column(Text, nullable=True)           # ASCII representation
    description = Column(Text, nullable=True)
    variables_used = Column(JSON, nullable=True)       # ["C", "G", "Γ"]

    # Source
    source_type = Column(Enum(SourceType), default=SourceType.UNKNOWN)
    source_file = Column(String(1024), nullable=True)
    source_paper = Column(String(255), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    definitions = relationship("Definition", secondary=definition_equations, back_populates="equations")

    __table_args__ = (
        Index('idx_eq_id', 'equation_id'),
    )


class DefinitionUsage(Base):
    """Tracks where definitions are used across documents"""
    __tablename__ = 'definition_usages'

    id = Column(Integer, primary_key=True)
    definition_id = Column(Integer, ForeignKey('definitions.id'), nullable=False)

    # Location
    file_path = Column(String(1024), nullable=False)
    line_number = Column(Integer, nullable=True)
    context = Column(Text, nullable=True)              # Surrounding text
    equation_id = Column(String(100), nullable=True)   # If used in an equation

    # Analysis
    usage_type = Column(String(50), nullable=True)     # "definition", "reference", "equation"
    deviation_detected = Column(Boolean, default=False)
    deviation_notes = Column(Text, nullable=True)

    detected_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    definition = relationship("Definition", back_populates="usages")


class DriftLog(Base):
    """Logs when a definition is used differently than canonical"""
    __tablename__ = 'drift_logs'

    id = Column(Integer, primary_key=True)
    definition_id = Column(Integer, ForeignKey('definitions.id'), nullable=False)

    detected_at = Column(DateTime, default=datetime.utcnow)
    file_path = Column(String(1024), nullable=False)
    context = Column(Text, nullable=True)
    deviation_summary = Column(Text, nullable=True)
    severity = Column(String(20), nullable=True)       # "minor", "major", "conflict"
    resolved = Column(Boolean, default=False)
    resolution_notes = Column(Text, nullable=True)

    # Source
    source_type = Column(Enum(SourceType), default=SourceType.PYTHON)

    # Relationships
    definition = relationship("Definition", back_populates="drift_logs")


class ExcelSheet(Base):
    """Tracks ingested Excel sheets"""
    __tablename__ = 'excel_sheets'

    id = Column(Integer, primary_key=True)
    file_path = Column(String(1024), nullable=False)
    file_name = Column(String(255), nullable=False)
    sheet_name = Column(String(255), nullable=False)

    # Metadata
    row_count = Column(Integer, nullable=True)
    column_count = Column(Integer, nullable=True)
    headers = Column(JSON, nullable=True)              # Column headers

    # Ingest info
    ingested_at = Column(DateTime, default=datetime.utcnow)
    session_id = Column(Integer, ForeignKey('ingest_sessions.id'), nullable=True)

    # Status
    fully_processed = Column(Boolean, default=False)
    error_count = Column(Integer, default=0)

    __table_args__ = (
        UniqueConstraint('file_path', 'sheet_name', name='uq_excel_sheet'),
        Index('idx_excel_file', 'file_path'),
    )


class HTMLTable(Base):
    """Tracks ingested HTML tables"""
    __tablename__ = 'html_tables'

    id = Column(Integer, primary_key=True)
    source_url = Column(String(2048), nullable=True)
    source_file = Column(String(1024), nullable=True)
    table_index = Column(Integer, default=0)           # Which table on the page

    # Content
    headers = Column(JSON, nullable=True)
    row_count = Column(Integer, nullable=True)
    column_count = Column(Integer, nullable=True)
    raw_html = Column(Text, nullable=True)

    # Ingest info
    ingested_at = Column(DateTime, default=datetime.utcnow)
    session_id = Column(Integer, ForeignKey('ingest_sessions.id'), nullable=True)

    __table_args__ = (
        Index('idx_html_source', 'source_url'),
    )


class ObsidianNote(Base):
    """Tracks ingested Obsidian/Markdown notes"""
    __tablename__ = 'obsidian_notes'

    id = Column(Integer, primary_key=True)
    file_path = Column(String(1024), unique=True, nullable=False)
    file_name = Column(String(255), nullable=False)

    # Frontmatter
    frontmatter = Column(JSON, nullable=True)
    note_type = Column(String(50), nullable=True)      # From frontmatter

    # Content
    title = Column(String(500), nullable=True)
    content_hash = Column(String(64), nullable=True)
    word_count = Column(Integer, nullable=True)

    # Links
    outgoing_links = Column(JSON, nullable=True)       # [[links]] in the note
    tags = Column(JSON, nullable=True)                 # #tags in the note

    # Ingest info
    ingested_at = Column(DateTime, default=datetime.utcnow)
    last_modified = Column(DateTime, nullable=True)
    session_id = Column(Integer, ForeignKey('ingest_sessions.id'), nullable=True)


# === DATABASE SETUP ===

def get_engine(connection_string: str):
    """Create database engine"""
    return create_engine(connection_string, echo=False)


def create_all_tables(engine):
    """Create all tables in the database"""
    Base.metadata.create_all(engine)


def get_session(engine):
    """Get a database session"""
    Session = sessionmaker(bind=engine)
    return Session()


# === CONVENIENCE FUNCTIONS ===

def init_database(connection_string: str = "postgresql://localhost/theophysics"):
    """Initialize the database with all tables"""
    engine = get_engine(connection_string)
    create_all_tables(engine)

    # Create default domains
    session = get_session(engine)
    default_domains = [
        "physics", "information-theory", "neuroscience",
        "psychology", "sociology", "economics", "theophysics", "theology"
    ]

    for domain_name in default_domains:
        existing = session.query(Domain).filter_by(name=domain_name).first()
        if not existing:
            session.add(Domain(name=domain_name))

    session.commit()
    session.close()

    return engine
