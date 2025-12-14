# Ingest modules for Theophysics Definition System
from .excel_ingest import ExcelIngester
from .html_ingest import HTMLIngester
from .markdown_ingest import MarkdownIngester

__all__ = ['ExcelIngester', 'HTMLIngester', 'MarkdownIngester']
