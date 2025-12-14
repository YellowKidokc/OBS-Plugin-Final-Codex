"""
Markdown/Obsidian Ingest Module for Theophysics Definition System

Uses established packages:
- python-frontmatter: YAML frontmatter parsing (1M+ weekly downloads)
- markdown: Standard markdown parsing (20M+ weekly downloads)
- mistune: Fast markdown parser alternative

Features:
- YAML frontmatter extraction
- Obsidian link parsing [[wikilinks]]
- Tag extraction (#tags)
- Section parsing (headers)
- Definition template detection
- Source attribution (marks all data as MARKDOWN source)
"""

import os
import re
import hashlib
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple, Set
from datetime import datetime
from dataclasses import dataclass, field

import frontmatter
import markdown
from markdown.extensions import tables, fenced_code

# Local imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from db.schema import (
    SourceType, ConfidenceLevel, DefinitionStatus, IngestSession,
    IngestRecord, ObsidianNote, Definition, get_session, get_engine
)


@dataclass
class ParsedNote:
    """Represents a parsed Obsidian/Markdown note"""
    file_path: str
    file_name: str
    title: Optional[str] = None
    frontmatter: Dict[str, Any] = field(default_factory=dict)
    content: str = ""
    sections: Dict[str, str] = field(default_factory=dict)
    outgoing_links: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    equations: List[str] = field(default_factory=list)
    word_count: int = 0
    content_hash: str = ""
    last_modified: Optional[datetime] = None


@dataclass
class ParsedDefinition:
    """Represents a definition extracted from a note"""
    note_path: str
    definition_id: Optional[str] = None
    symbol: Optional[str] = None
    name: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    canonical_definition: Optional[str] = None
    triad_position: Optional[str] = None
    domain_type: Optional[str] = None
    layer: Optional[str] = None
    axioms: List[str] = field(default_factory=list)
    mathematical_primary: Optional[str] = None
    mathematical_dynamic: Optional[str] = None
    thresholds: Optional[str] = None
    domain_interpretations: Dict[str, str] = field(default_factory=dict)
    operationalization: Optional[str] = None
    failure_modes: Optional[str] = None
    integration_map: Dict[str, Any] = field(default_factory=dict)
    external_comparison: Optional[str] = None
    notes: Optional[str] = None
    related_terms: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    status: str = "draft"


class MarkdownIngester:
    """
    Ingests Obsidian/Markdown notes into PostgreSQL with full source attribution.

    All ingested data is marked with:
    - source_type: MARKDOWN
    - source_file: path to the markdown file

    Usage:
        ingester = MarkdownIngester(db_connection_string)
        ingester.ingest_vault("/path/to/obsidian/vault")

        # Or single file
        note = ingester.parse_file("definition.md")
    """

    # Regex patterns
    WIKILINK_PATTERN = re.compile(r'\[\[([^\]|]+)(?:\|([^\]]+))?\]\]')
    TAG_PATTERN = re.compile(r'(?:^|\s)#([a-zA-Z][a-zA-Z0-9_/-]*)')
    EQUATION_BLOCK_PATTERN = re.compile(r'\$\$(.+?)\$\$', re.DOTALL)
    INLINE_EQUATION_PATTERN = re.compile(r'\$([^$\n]+)\$')
    HEADER_PATTERN = re.compile(r'^(#{1,6})\s+(.+)$', re.MULTILINE)

    # Definition template sections to extract
    DEFINITION_SECTIONS = [
        "Core Definition",
        "Ontological Category",
        "Logical Dependencies",
        "Crosslinks",
        "Mathematical Form",
        "Theological Mapping",
        "Application",
        "Axioms",
        "Mathematical Structure",
        "Domain Interpretations",
        "Operationalization",
        "Failure Modes",
        "Integration Map",
        "External Comparison",
        "Notes"
    ]

    def __init__(self, db_connection_string: Optional[str] = None):
        """
        Initialize the Markdown ingester.

        Args:
            db_connection_string: PostgreSQL connection string
        """
        self.db_connection_string = db_connection_string
        self.engine = None
        self.session = None
        self.current_ingest_session: Optional[IngestSession] = None

        # Markdown parser
        self.md = markdown.Markdown(extensions=[
            'tables',
            'fenced_code',
            'meta',
            'toc'
        ])

        if db_connection_string:
            self.engine = get_engine(db_connection_string)

    def _ensure_session(self):
        """Ensure database session exists"""
        if self.engine and not self.session:
            self.session = get_session(self.engine)

    def _create_ingest_session(self, source_path: str) -> IngestSession:
        """Create a new ingest session for tracking"""
        self._ensure_session()
        session = IngestSession(
            source_type=SourceType.MARKDOWN,
            source_path=source_path,
            source_name=Path(source_path).name,
            metadata={"ingester": "MarkdownIngester", "version": "1.0"}
        )
        if self.session:
            self.session.add(session)
            self.session.commit()
        return session

    def _hash_content(self, content: str) -> str:
        """Create SHA-256 hash of content for deduplication"""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    def _extract_wikilinks(self, content: str) -> List[str]:
        """Extract [[wikilinks]] from content"""
        links = []
        for match in self.WIKILINK_PATTERN.finditer(content):
            link_target = match.group(1).strip()
            links.append(link_target)
        return list(set(links))  # Unique links

    def _extract_tags(self, content: str) -> List[str]:
        """Extract #tags from content"""
        tags = []
        for match in self.TAG_PATTERN.finditer(content):
            tag = match.group(1).strip()
            if tag:
                tags.append(tag)
        return list(set(tags))

    def _extract_equations(self, content: str) -> List[str]:
        """Extract LaTeX equations from content"""
        equations = []

        # Block equations $$...$$
        for match in self.EQUATION_BLOCK_PATTERN.finditer(content):
            eq = match.group(1).strip()
            if eq:
                equations.append(eq)

        # Inline equations $...$
        for match in self.INLINE_EQUATION_PATTERN.finditer(content):
            eq = match.group(1).strip()
            if eq and len(eq) > 2:  # Avoid false positives like "$5"
                equations.append(eq)

        return equations

    def _extract_sections(self, content: str) -> Dict[str, str]:
        """Extract sections based on headers"""
        sections = {}
        lines = content.split('\n')

        current_header = None
        current_content = []

        for line in lines:
            header_match = self.HEADER_PATTERN.match(line)
            if header_match:
                # Save previous section
                if current_header:
                    sections[current_header] = '\n'.join(current_content).strip()

                current_header = header_match.group(2).strip()
                current_content = []
            elif current_header:
                current_content.append(line)

        # Save last section
        if current_header:
            sections[current_header] = '\n'.join(current_content).strip()

        return sections

    def _extract_title(self, content: str, frontmatter_data: Dict) -> Optional[str]:
        """Extract title from content or frontmatter"""
        # Try frontmatter first
        if 'title' in frontmatter_data:
            return frontmatter_data['title']

        if 'name' in frontmatter_data:
            return frontmatter_data['name']

        # Try first H1 header
        h1_match = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
        if h1_match:
            return h1_match.group(1).strip()

        return None

    def parse_file(self, file_path: str) -> ParsedNote:
        """
        Parse a markdown file and extract structured data.

        Args:
            file_path: Path to markdown file

        Returns:
            ParsedNote object
        """
        file_path = str(Path(file_path).resolve())

        with open(file_path, 'r', encoding='utf-8') as f:
            raw_content = f.read()

        # Parse frontmatter
        post = frontmatter.loads(raw_content)
        fm_data = dict(post.metadata)
        content = post.content

        # Get file stats
        stat = os.stat(file_path)
        last_modified = datetime.fromtimestamp(stat.st_mtime)

        # Extract components
        sections = self._extract_sections(content)
        wikilinks = self._extract_wikilinks(content)
        tags = self._extract_tags(content)
        equations = self._extract_equations(content)
        title = self._extract_title(content, fm_data)

        # Count words
        word_count = len(re.findall(r'\b\w+\b', content))

        return ParsedNote(
            file_path=file_path,
            file_name=Path(file_path).name,
            title=title,
            frontmatter=fm_data,
            content=content,
            sections=sections,
            outgoing_links=wikilinks,
            tags=tags,
            equations=equations,
            word_count=word_count,
            content_hash=self._hash_content(content),
            last_modified=last_modified
        )

    def parse_as_definition(self, file_path: str) -> Optional[ParsedDefinition]:
        """
        Parse a markdown file as a definition (using the 10-section template).

        Args:
            file_path: Path to markdown file

        Returns:
            ParsedDefinition object if it looks like a definition, None otherwise
        """
        note = self.parse_file(file_path)

        # Check if it looks like a definition
        fm = note.frontmatter
        is_definition = (
            fm.get('type') == 'definition' or
            'symbol' in fm or
            'canonical_definition' in note.sections or
            'Core Definition' in note.sections or
            '1. Core Definition' in note.sections
        )

        if not is_definition:
            return None

        # Extract from frontmatter
        definition_id = fm.get('id') or fm.get('definition_id')
        symbol = fm.get('symbol')
        name = fm.get('name') or note.title
        aliases = fm.get('aliases', [])
        if isinstance(aliases, str):
            aliases = [a.strip() for a in aliases.split(',')]

        # Extract domains
        domains = fm.get('domains', [])

        # Status
        status = fm.get('status', 'draft')

        # Extract from sections
        sections = note.sections

        # Helper to find section content
        def get_section(*possible_names):
            for name in possible_names:
                if name in sections:
                    return sections[name]
                # Try with number prefix
                for key in sections:
                    if name.lower() in key.lower():
                        return sections[key]
            return None

        # Build definition
        definition = ParsedDefinition(
            note_path=note.file_path,
            definition_id=definition_id,
            symbol=symbol,
            name=name,
            aliases=aliases,
            canonical_definition=get_section("Core Definition", "Canonical Definition", "Definition"),
            triad_position=fm.get('triad') or get_section("Ontological Category"),
            domain_type=fm.get('domain'),
            layer=fm.get('layer'),
            axioms=self._parse_axioms(get_section("Axioms") or ""),
            mathematical_primary=get_section("Mathematical Form", "Mathematical Structure", "Primary Forms"),
            mathematical_dynamic=get_section("Dynamical Equation"),
            thresholds=get_section("Threshold", "Thresholds", "Stability"),
            operationalization=get_section("Operationalization"),
            failure_modes=get_section("Failure Modes"),
            external_comparison=get_section("External Comparison", "External Reference"),
            notes=get_section("Notes", "Examples"),
            related_terms=note.outgoing_links,
            tags=note.tags,
            status=status
        )

        # Parse domain interpretations if present
        domain_section = get_section("Domain Interpretations")
        if domain_section:
            definition.domain_interpretations = self._parse_domain_interpretations(domain_section)

        # Parse integration map if present
        integration_section = get_section("Integration Map")
        if integration_section:
            definition.integration_map = {"raw": integration_section, "links": note.outgoing_links}

        return definition

    def _parse_axioms(self, content: str) -> List[str]:
        """Parse axiom statements from content"""
        axioms = []

        # Look for numbered items or bullet points
        lines = content.split('\n')
        current_axiom = []

        for line in lines:
            # Check for axiom pattern like "Axiom C1:" or "1." or "- "
            if re.match(r'^(\d+\.|Axiom\s+\w+:|-|\*)', line.strip()):
                if current_axiom:
                    axioms.append(' '.join(current_axiom).strip())
                current_axiom = [re.sub(r'^(\d+\.|Axiom\s+\w+:|-|\*)\s*', '', line.strip())]
            elif line.strip() and current_axiom:
                current_axiom.append(line.strip())

        if current_axiom:
            axioms.append(' '.join(current_axiom).strip())

        return [a for a in axioms if a]

    def _parse_domain_interpretations(self, content: str) -> Dict[str, str]:
        """Parse domain-specific interpretations"""
        interpretations = {}
        current_domain = None
        current_content = []

        for line in content.split('\n'):
            # Look for domain headers like "### Physics" or "#### 4.1 Physics"
            domain_match = re.match(r'^#{2,4}\s*\d*\.?\d*\s*(.+)$', line.strip())
            if domain_match:
                if current_domain:
                    interpretations[current_domain] = '\n'.join(current_content).strip()
                current_domain = domain_match.group(1).strip()
                current_content = []
            elif current_domain:
                current_content.append(line)

        if current_domain:
            interpretations[current_domain] = '\n'.join(current_content).strip()

        return interpretations

    def ingest_file(self, file_path: str, as_definition: bool = False) -> Dict[str, Any]:
        """
        Ingest a markdown file into the database.

        Args:
            file_path: Path to markdown file
            as_definition: Try to parse as a definition template

        Returns:
            Dictionary with ingest statistics
        """
        file_path = str(Path(file_path).resolve())

        stats = {
            "file": file_path,
            "success": False,
            "is_definition": False,
            "errors": []
        }

        try:
            note = self.parse_file(file_path)

            self._ensure_session()

            if self.session:
                # Store note record
                obsidian_note = ObsidianNote(
                    file_path=note.file_path,
                    file_name=note.file_name,
                    frontmatter=note.frontmatter,
                    note_type=note.frontmatter.get('type'),
                    title=note.title,
                    content_hash=note.content_hash,
                    word_count=note.word_count,
                    outgoing_links=note.outgoing_links,
                    tags=note.tags,
                    last_modified=note.last_modified,
                    session_id=self.current_ingest_session.id if self.current_ingest_session else None
                )
                self.session.add(obsidian_note)

                # Try to parse as definition
                if as_definition or note.frontmatter.get('type') == 'definition':
                    definition = self.parse_as_definition(file_path)
                    if definition:
                        stats["is_definition"] = True
                        self._store_definition(definition)

                self.session.commit()

            stats["success"] = True

        except Exception as e:
            stats["errors"].append(str(e))

        return stats

    def _store_definition(self, definition: ParsedDefinition):
        """Store a parsed definition in the database"""
        if not self.session:
            return

        # Map status
        status_map = {
            "canonical": DefinitionStatus.CANONICAL,
            "draft": DefinitionStatus.DRAFT,
            "review": DefinitionStatus.REVIEW,
            "deprecated": DefinitionStatus.DEPRECATED
        }

        db_definition = Definition(
            definition_id=definition.definition_id,
            symbol=definition.symbol,
            name=definition.name,
            aliases=definition.aliases if definition.aliases else None,
            canonical_definition=definition.canonical_definition,
            triad_position=definition.triad_position,
            domain_type=definition.domain_type,
            layer=definition.layer,
            axioms=definition.axioms if definition.axioms else None,
            mathematical_primary=definition.mathematical_primary,
            mathematical_dynamic=definition.mathematical_dynamic,
            thresholds=definition.thresholds,
            domain_interpretations=definition.domain_interpretations if definition.domain_interpretations else None,
            operationalization=definition.operationalization,
            failure_modes=definition.failure_modes,
            external_comparison=definition.external_comparison,
            notes=definition.notes,
            source_type=SourceType.MARKDOWN,
            source_file=definition.note_path,
            status=status_map.get(definition.status, DefinitionStatus.DRAFT),
            confidence=ConfidenceLevel.UNVERIFIED
        )

        self.session.add(db_definition)

    def ingest_vault(self, vault_path: str, recursive: bool = True,
                     parse_definitions: bool = True) -> Dict[str, Any]:
        """
        Ingest all markdown files from an Obsidian vault.

        Args:
            vault_path: Path to vault directory
            recursive: Search subdirectories
            parse_definitions: Try to parse notes as definitions

        Returns:
            Aggregated statistics
        """
        vault = Path(vault_path)
        pattern = "**/*.md" if recursive else "*.md"

        files = list(vault.glob(pattern))

        # Create ingest session
        self.current_ingest_session = self._create_ingest_session(str(vault))

        total_stats = {
            "vault": str(vault),
            "files_processed": 0,
            "definitions_found": 0,
            "errors": []
        }

        for file_path in files:
            try:
                stats = self.ingest_file(str(file_path), as_definition=parse_definitions)
                total_stats["files_processed"] += 1
                if stats.get("is_definition"):
                    total_stats["definitions_found"] += 1
                total_stats["errors"].extend(stats.get("errors", []))
            except Exception as e:
                total_stats["errors"].append(f"{file_path}: {str(e)}")

        # Update session
        if self.current_ingest_session and self.session:
            self.current_ingest_session.completed_at = datetime.utcnow()
            self.current_ingest_session.records_processed = total_stats["files_processed"]
            self.current_ingest_session.records_created = total_stats["definitions_found"]
            self.session.commit()

        return total_stats

    def get_note_preview(self, file_path: str) -> Dict[str, Any]:
        """
        Get a preview of a markdown note.

        Args:
            file_path: Path to markdown file

        Returns:
            Dictionary with parsed information
        """
        note = self.parse_file(file_path)

        return {
            "file": note.file_path,
            "title": note.title,
            "frontmatter": note.frontmatter,
            "sections": list(note.sections.keys()),
            "outgoing_links": note.outgoing_links,
            "tags": note.tags,
            "equations_found": len(note.equations),
            "word_count": note.word_count,
            "content_preview": note.content[:500] + "..." if len(note.content) > 500 else note.content,
            "source_attribution": {
                "source_type": "MARKDOWN",
                "source_file": file_path,
                "ingested_by": "python/MarkdownIngester"
            }
        }

    def close(self):
        """Close database session"""
        if self.session:
            self.session.close()
            self.session = None


# === CONVENIENCE FUNCTIONS ===

def parse_obsidian_note(file_path: str) -> Dict[str, Any]:
    """
    Quick parse of an Obsidian note (no database required).

    Args:
        file_path: Path to markdown file

    Returns:
        Dictionary with parsed note data
    """
    ingester = MarkdownIngester()
    note = ingester.parse_file(file_path)

    return {
        "file_path": note.file_path,
        "file_name": note.file_name,
        "title": note.title,
        "frontmatter": note.frontmatter,
        "sections": note.sections,
        "outgoing_links": note.outgoing_links,
        "tags": note.tags,
        "equations": note.equations,
        "word_count": note.word_count,
        "__source__": {
            "type": "MARKDOWN",
            "file": file_path,
            "ingested_by": "python/MarkdownIngester"
        }
    }


def vault_to_dict(vault_path: str) -> Dict[str, Dict]:
    """
    Convert entire vault to dictionary (no database required).

    Args:
        vault_path: Path to Obsidian vault

    Returns:
        Dictionary with file paths as keys, parsed notes as values
    """
    ingester = MarkdownIngester()
    vault = Path(vault_path)

    result = {}
    for md_file in vault.glob("**/*.md"):
        try:
            result[str(md_file)] = parse_obsidian_note(str(md_file))
        except Exception as e:
            result[str(md_file)] = {"error": str(e)}

    return result
