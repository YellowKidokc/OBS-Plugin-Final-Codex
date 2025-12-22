"""
Main Orchestrator for Theophysics Ingest Engine

This module provides a unified interface for ingesting data from multiple sources:
- Excel files (.xlsx, .xls)
- HTML files with tables
- Obsidian/Markdown notes

All data is tracked with source attribution:
- source_type: EXCEL, HTML, MARKDOWN, WEB, USER, PYTHON, AI
- source_file: Original file path
- ingested_by: Which module processed it

Usage:
    from orchestrator import IngestOrchestrator

    # Initialize with PostgreSQL
    engine = IngestOrchestrator("postgresql://localhost/theophysics")

    # Ingest different file types
    engine.ingest("data.xlsx")           # Auto-detects Excel
    engine.ingest("page.html")           # Auto-detects HTML
    engine.ingest("definition.md")       # Auto-detects Markdown
    engine.ingest_directory("/path/to/vault")  # Batch ingest

    # Or use specific ingesters
    engine.ingest_excel("data.xlsx")
    engine.ingest_html("page.html")
    engine.ingest_markdown("note.md")
"""

import os
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
from enum import Enum
import logging

from db.schema import (
    SourceType, init_database, get_engine, get_session,
    IngestSession, create_all_tables
)
from ingest.excel_ingest import ExcelIngester
from ingest.html_ingest import HTMLIngester
from ingest.markdown_ingest import MarkdownIngester


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileType(Enum):
    """Supported file types"""
    EXCEL = "excel"
    HTML = "html"
    MARKDOWN = "markdown"
    UNKNOWN = "unknown"


class IngestOrchestrator:
    """
    Unified interface for ingesting data from multiple sources into PostgreSQL.

    Features:
    - Auto-detection of file types
    - Source attribution on all ingested data
    - Batch processing with progress tracking
    - Session management for audit trails

    All ingested data is marked with its source:
    - EXCEL: Data from Excel files
    - HTML: Data from HTML tables
    - MARKDOWN: Data from Obsidian/Markdown notes
    - WEB: Data fetched from URLs
    - USER: Human-created content
    - PYTHON: Script-generated content
    - AI: AI-generated content
    """

    # File extension mappings
    EXTENSION_MAP = {
        '.xlsx': FileType.EXCEL,
        '.xls': FileType.EXCEL,
        '.xlsm': FileType.EXCEL,
        '.html': FileType.HTML,
        '.htm': FileType.HTML,
        '.md': FileType.MARKDOWN,
        '.markdown': FileType.MARKDOWN
    }

    def __init__(self, db_connection_string: Optional[str] = None,
                 auto_init_db: bool = True):
        """
        Initialize the Ingest Orchestrator.

        Args:
            db_connection_string: PostgreSQL connection string
                Example: "postgresql://user:pass@localhost:5432/theophysics"
            auto_init_db: Automatically create tables if they don't exist
        """
        self.db_connection_string = db_connection_string
        self.engine = None
        self.session = None

        # Initialize ingesters
        self.excel_ingester = ExcelIngester(db_connection_string)
        self.html_ingester = HTMLIngester(db_connection_string)
        self.markdown_ingester = MarkdownIngester(db_connection_string)

        # Initialize database
        if db_connection_string and auto_init_db:
            self._init_database()

    def _init_database(self):
        """Initialize database connection and create tables"""
        try:
            self.engine = get_engine(self.db_connection_string)
            create_all_tables(self.engine)
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.warning(f"Could not initialize database: {e}")

    def _detect_file_type(self, file_path: str) -> FileType:
        """Detect file type from extension"""
        ext = Path(file_path).suffix.lower()
        return self.EXTENSION_MAP.get(ext, FileType.UNKNOWN)

    def ingest(self, path: str, file_type: Optional[FileType] = None,
               **kwargs) -> Dict[str, Any]:
        """
        Ingest a file or directory, auto-detecting the type.

        Args:
            path: Path to file or directory
            file_type: Override auto-detection
            **kwargs: Additional arguments passed to specific ingester

        Returns:
            Dictionary with ingest statistics
        """
        path = str(Path(path).resolve())

        if os.path.isdir(path):
            return self.ingest_directory(path, **kwargs)

        # Detect file type
        detected_type = file_type or self._detect_file_type(path)

        if detected_type == FileType.EXCEL:
            return self.ingest_excel(path, **kwargs)
        elif detected_type == FileType.HTML:
            return self.ingest_html(path, **kwargs)
        elif detected_type == FileType.MARKDOWN:
            return self.ingest_markdown(path, **kwargs)
        else:
            return {
                "error": f"Unknown file type for: {path}",
                "detected_extension": Path(path).suffix,
                "supported_extensions": list(self.EXTENSION_MAP.keys())
            }

    def ingest_excel(self, file_path: str, **kwargs) -> Dict[str, Any]:
        """
        Ingest an Excel file.

        Args:
            file_path: Path to Excel file
            **kwargs: Additional arguments for ExcelIngester

        Returns:
            Ingest statistics
        """
        logger.info(f"Ingesting Excel: {file_path}")
        result = self.excel_ingester.ingest_file(file_path, **kwargs)
        result["source_attribution"] = {
            "source_type": "EXCEL",
            "ingested_by": "python/ExcelIngester"
        }
        return result

    def ingest_html(self, file_path: str, **kwargs) -> Dict[str, Any]:
        """
        Ingest HTML tables from a file.

        Args:
            file_path: Path to HTML file
            **kwargs: Additional arguments for HTMLIngester

        Returns:
            Ingest statistics
        """
        logger.info(f"Ingesting HTML: {file_path}")
        result = self.html_ingester.ingest_file(file_path)
        result["source_attribution"] = {
            "source_type": "HTML",
            "ingested_by": "python/HTMLIngester"
        }
        return result

    def ingest_markdown(self, file_path: str, as_definition: bool = True,
                        **kwargs) -> Dict[str, Any]:
        """
        Ingest a markdown/Obsidian note.

        Args:
            file_path: Path to markdown file
            as_definition: Try to parse as definition template
            **kwargs: Additional arguments for MarkdownIngester

        Returns:
            Ingest statistics
        """
        logger.info(f"Ingesting Markdown: {file_path}")
        result = self.markdown_ingester.ingest_file(file_path, as_definition=as_definition)
        result["source_attribution"] = {
            "source_type": "MARKDOWN",
            "ingested_by": "python/MarkdownIngester"
        }
        return result

    def ingest_directory(self, directory_path: str, recursive: bool = True,
                         file_types: Optional[List[FileType]] = None) -> Dict[str, Any]:
        """
        Ingest all supported files from a directory.

        Args:
            directory_path: Path to directory
            recursive: Search subdirectories
            file_types: Limit to specific file types

        Returns:
            Aggregated statistics
        """
        directory = Path(directory_path)
        logger.info(f"Ingesting directory: {directory}")

        if file_types is None:
            file_types = [FileType.EXCEL, FileType.HTML, FileType.MARKDOWN]

        total_stats = {
            "directory": str(directory),
            "excel": {"files": 0, "records": 0, "errors": []},
            "html": {"files": 0, "tables": 0, "errors": []},
            "markdown": {"files": 0, "definitions": 0, "errors": []},
            "total_files": 0,
            "total_errors": []
        }

        pattern = "**/*" if recursive else "*"

        # Ingest Excel files
        if FileType.EXCEL in file_types:
            for ext in ['.xlsx', '.xls']:
                for file_path in directory.glob(f"{pattern}{ext}"):
                    try:
                        result = self.ingest_excel(str(file_path))
                        total_stats["excel"]["files"] += 1
                        total_stats["excel"]["records"] += result.get("records_created", 0)
                        total_stats["total_files"] += 1
                    except Exception as e:
                        error = f"Excel {file_path}: {str(e)}"
                        total_stats["excel"]["errors"].append(error)
                        total_stats["total_errors"].append(error)

        # Ingest HTML files
        if FileType.HTML in file_types:
            for ext in ['.html', '.htm']:
                for file_path in directory.glob(f"{pattern}{ext}"):
                    try:
                        result = self.ingest_html(str(file_path))
                        total_stats["html"]["files"] += 1
                        total_stats["html"]["tables"] += result.get("tables_found", 0)
                        total_stats["total_files"] += 1
                    except Exception as e:
                        error = f"HTML {file_path}: {str(e)}"
                        total_stats["html"]["errors"].append(error)
                        total_stats["total_errors"].append(error)

        # Ingest Markdown files
        if FileType.MARKDOWN in file_types:
            for file_path in directory.glob(f"{pattern}.md"):
                try:
                    result = self.ingest_markdown(str(file_path))
                    total_stats["markdown"]["files"] += 1
                    if result.get("is_definition"):
                        total_stats["markdown"]["definitions"] += 1
                    total_stats["total_files"] += 1
                except Exception as e:
                    error = f"Markdown {file_path}: {str(e)}"
                    total_stats["markdown"]["errors"].append(error)
                    total_stats["total_errors"].append(error)

        return total_stats

    def ingest_vault(self, vault_path: str, parse_definitions: bool = True) -> Dict[str, Any]:
        """
        Ingest an Obsidian vault.

        Args:
            vault_path: Path to Obsidian vault
            parse_definitions: Try to parse notes as definitions

        Returns:
            Ingest statistics
        """
        logger.info(f"Ingesting Obsidian vault: {vault_path}")
        return self.markdown_ingester.ingest_vault(vault_path, parse_definitions=parse_definitions)

    # === Preview Methods ===

    def preview_excel(self, file_path: str, sheet_name: str = None,
                      n_rows: int = 10) -> Dict[str, Any]:
        """Preview an Excel file without ingesting"""
        return self.excel_ingester.get_sheet_preview(file_path, sheet_name, n_rows)

    def preview_html(self, file_path: str, table_index: int = 0,
                     n_rows: int = 10) -> Dict[str, Any]:
        """Preview HTML tables without ingesting"""
        return self.html_ingester.get_table_preview(file_path, table_index, n_rows)

    def preview_markdown(self, file_path: str) -> Dict[str, Any]:
        """Preview a markdown note without ingesting"""
        return self.markdown_ingester.get_note_preview(file_path)

    def preview(self, file_path: str, **kwargs) -> Dict[str, Any]:
        """Preview any supported file"""
        file_type = self._detect_file_type(file_path)

        if file_type == FileType.EXCEL:
            return self.preview_excel(file_path, **kwargs)
        elif file_type == FileType.HTML:
            return self.preview_html(file_path, **kwargs)
        elif file_type == FileType.MARKDOWN:
            return self.preview_markdown(file_path)
        else:
            return {"error": f"Cannot preview file type: {Path(file_path).suffix}"}

    # === Data Export Methods ===

    def to_dataframe(self, file_path: str, **kwargs):
        """
        Read file directly to pandas DataFrame (no database required).

        Args:
            file_path: Path to file

        Returns:
            pandas DataFrame or dict of DataFrames
        """
        import pandas as pd

        file_type = self._detect_file_type(file_path)

        if file_type == FileType.EXCEL:
            return self.excel_ingester.to_dataframe(file_path, **kwargs)
        elif file_type == FileType.HTML:
            return self.html_ingester.tables_to_dataframes(file_path)
        elif file_type == FileType.MARKDOWN:
            note = self.markdown_ingester.parse_file(file_path)
            return pd.DataFrame([{
                "file": note.file_path,
                "title": note.title,
                "frontmatter": str(note.frontmatter),
                "content": note.content[:1000],
                "tags": note.tags,
                "links": note.outgoing_links
            }])
        else:
            raise ValueError(f"Cannot convert file type: {Path(file_path).suffix}")

    def close(self):
        """Close all database connections"""
        self.excel_ingester.close()
        self.html_ingester.close()
        self.markdown_ingester.close()


# === CONVENIENCE FUNCTIONS ===

def quick_ingest(path: str, db_url: str = None) -> Dict[str, Any]:
    """
    Quick one-liner to ingest any supported file.

    Args:
        path: Path to file or directory
        db_url: PostgreSQL connection string (optional)

    Returns:
        Ingest statistics
    """
    engine = IngestOrchestrator(db_url)
    result = engine.ingest(path)
    engine.close()
    return result


def preview_file(file_path: str) -> Dict[str, Any]:
    """
    Quick preview of any supported file (no database required).

    Args:
        file_path: Path to file

    Returns:
        Preview data with source attribution
    """
    engine = IngestOrchestrator()
    return engine.preview(file_path)


# === CLI INTERFACE ===

def main():
    """Command-line interface for the ingest engine"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Theophysics Ingest Engine - Import data with source attribution"
    )
    parser.add_argument("path", help="File or directory to ingest")
    parser.add_argument("--db", "-d", help="PostgreSQL connection string")
    parser.add_argument("--preview", "-p", action="store_true",
                        help="Preview without ingesting")
    parser.add_argument("--type", "-t", choices=["excel", "html", "markdown"],
                        help="Force file type")
    parser.add_argument("--no-recursive", action="store_true",
                        help="Don't search subdirectories")

    args = parser.parse_args()

    # Initialize engine
    engine = IngestOrchestrator(args.db)

    if args.preview:
        result = engine.preview(args.path)
    else:
        file_type = None
        if args.type:
            file_type = FileType[args.type.upper()]

        result = engine.ingest(
            args.path,
            file_type=file_type,
            recursive=not args.no_recursive
        )

    # Print result
    import json
    print(json.dumps(result, indent=2, default=str))

    engine.close()


if __name__ == "__main__":
    main()
