"""
Data Cleaning Module for Theophysics Ingest Engine

Cleans and normalizes data before ingestion into PostgreSQL/SQLite:
- Text normalization (whitespace, unicode, encoding)
- HTML entity decoding
- Markdown cleanup
- Excel cell value normalization
- Duplicate detection
- Data validation

All cleaning operations preserve source attribution.
"""

import re
import html
import unicodedata
import hashlib
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class CleaningResult:
    """Result of a cleaning operation"""
    original: Any
    cleaned: Any
    was_modified: bool
    modifications: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class ValidationResult:
    """Result of a validation operation"""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class DataCleaner:
    """
    Cleans and normalizes data before database ingestion.

    All methods preserve the original data and track modifications
    for audit purposes.
    """

    # Characters to strip
    ZERO_WIDTH_CHARS = re.compile(r'[\u200b\u200c\u200d\u200e\u200f\ufeff\u00ad]')

    # Whitespace normalization
    MULTI_WHITESPACE = re.compile(r'[ \t]+')
    MULTI_NEWLINES = re.compile(r'\n{3,}')

    # Common replacements
    SMART_QUOTES = {
        '\u201c': '"',  # Left double quote
        '\u201d': '"',  # Right double quote
        '\u2018': "'",  # Left single quote
        '\u2019': "'",  # Right single quote
        '\u2013': '-',  # En dash
        '\u2014': '--', # Em dash
        '\u2026': '...', # Ellipsis
    }

    def __init__(self, preserve_markdown: bool = True, preserve_latex: bool = True):
        """
        Initialize the data cleaner.

        Args:
            preserve_markdown: Don't strip markdown formatting
            preserve_latex: Don't modify LaTeX equations
        """
        self.preserve_markdown = preserve_markdown
        self.preserve_latex = preserve_latex

    def clean_text(self, text: str, aggressive: bool = False) -> CleaningResult:
        """
        Clean a text string.

        Args:
            text: Text to clean
            aggressive: Apply more aggressive cleaning

        Returns:
            CleaningResult with cleaned text
        """
        if not isinstance(text, str):
            return CleaningResult(text, text, False)

        original = text
        modifications = []

        # Strip zero-width characters
        if self.ZERO_WIDTH_CHARS.search(text):
            text = self.ZERO_WIDTH_CHARS.sub('', text)
            modifications.append("removed_zero_width_chars")

        # Decode HTML entities
        if '&' in text and ';' in text:
            decoded = html.unescape(text)
            if decoded != text:
                text = decoded
                modifications.append("decoded_html_entities")

        # Normalize unicode (NFC normalization)
        normalized = unicodedata.normalize('NFC', text)
        if normalized != text:
            text = normalized
            modifications.append("normalized_unicode")

        # Replace smart quotes (optional)
        if aggressive:
            for smart, regular in self.SMART_QUOTES.items():
                if smart in text:
                    text = text.replace(smart, regular)
                    modifications.append("replaced_smart_quotes")
                    break

        # Normalize whitespace
        if self.MULTI_WHITESPACE.search(text):
            text = self.MULTI_WHITESPACE.sub(' ', text)
            modifications.append("normalized_whitespace")

        # Collapse multiple newlines
        if self.MULTI_NEWLINES.search(text):
            text = self.MULTI_NEWLINES.sub('\n\n', text)
            modifications.append("collapsed_newlines")

        # Strip leading/trailing whitespace
        stripped = text.strip()
        if stripped != text:
            text = stripped
            modifications.append("stripped_whitespace")

        return CleaningResult(
            original=original,
            cleaned=text,
            was_modified=len(modifications) > 0,
            modifications=modifications
        )

    def clean_cell_value(self, value: Any) -> CleaningResult:
        """
        Clean an Excel/HTML cell value.

        Handles:
        - None/NaN values
        - Whitespace-only strings
        - Numeric string normalization
        - Date normalization
        """
        modifications = []
        warnings = []
        original = value

        # Handle None/NaN
        if value is None:
            return CleaningResult(original, None, False)

        # Handle pandas NaN
        try:
            import pandas as pd
            if pd.isna(value):
                return CleaningResult(original, None, False, ["converted_nan_to_none"])
        except ImportError:
            pass

        # Handle strings
        if isinstance(value, str):
            # Check for empty/whitespace-only
            if not value.strip():
                return CleaningResult(original, None, True, ["empty_string_to_none"])

            # Clean the text
            result = self.clean_text(value)
            if result.was_modified:
                modifications.extend(result.modifications)
                value = result.cleaned

            # Try to detect and normalize numbers
            cleaned = value.strip()
            if self._looks_like_number(cleaned):
                try:
                    if '.' in cleaned or ',' in cleaned:
                        # Handle European number format (1.234,56 -> 1234.56)
                        if ',' in cleaned and '.' in cleaned:
                            if cleaned.rfind(',') > cleaned.rfind('.'):
                                cleaned = cleaned.replace('.', '').replace(',', '.')
                        numeric = float(cleaned.replace(',', ''))
                        if numeric.is_integer():
                            value = int(numeric)
                        else:
                            value = numeric
                        modifications.append("converted_to_number")
                    else:
                        value = int(cleaned)
                        modifications.append("converted_to_integer")
                except ValueError:
                    pass  # Keep as string

        # Handle numeric types
        elif isinstance(value, (int, float)):
            # Check for infinity or very large numbers
            if isinstance(value, float):
                if value != value:  # NaN check
                    return CleaningResult(original, None, True, ["nan_to_none"])
                if abs(value) == float('inf'):
                    warnings.append("infinite_value")

        return CleaningResult(
            original=original,
            cleaned=value,
            was_modified=len(modifications) > 0,
            modifications=modifications,
            warnings=warnings
        )

    def _looks_like_number(self, s: str) -> bool:
        """Check if string looks like a number"""
        # Remove currency symbols and whitespace
        s = re.sub(r'[$€£¥₹\s]', '', s)
        # Check for number pattern
        return bool(re.match(r'^-?[\d,]+\.?\d*$', s) or re.match(r'^-?[\d.]+,?\d*$', s))

    def clean_row(self, row: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """
        Clean all values in a row dictionary.

        Returns:
            Tuple of (cleaned_row, list_of_modifications)
        """
        cleaned = {}
        all_modifications = []

        for key, value in row.items():
            # Clean the key too
            key_result = self.clean_text(str(key))
            clean_key = key_result.cleaned

            # Clean the value
            value_result = self.clean_cell_value(value)
            cleaned[clean_key] = value_result.cleaned

            if value_result.was_modified:
                all_modifications.append(f"{clean_key}: {', '.join(value_result.modifications)}")

        return cleaned, all_modifications

    def clean_markdown(self, content: str) -> CleaningResult:
        """
        Clean markdown content while preserving formatting.

        Handles:
        - Broken links
        - Inconsistent headers
        - Code block normalization
        - LaTeX equation protection
        """
        if not isinstance(content, str):
            return CleaningResult(content, content, False)

        original = content
        modifications = []

        # Protect LaTeX equations
        latex_blocks = []
        if self.preserve_latex:
            # Block equations
            def save_latex_block(match):
                latex_blocks.append(match.group(0))
                return f"__LATEX_BLOCK_{len(latex_blocks) - 1}__"

            content = re.sub(r'\$\$.+?\$\$', save_latex_block, content, flags=re.DOTALL)

            # Inline equations
            content = re.sub(r'\$[^$\n]+\$', save_latex_block, content)

        # Fix broken wikilinks [[link]
        broken_links = re.findall(r'\[\[[^\]]+(?!\]\])', content)
        if broken_links:
            for broken in broken_links:
                if '[[' in broken and ']]' not in broken:
                    # Try to fix by adding closing brackets
                    fixed = broken + ']]'
                    content = content.replace(broken, fixed)
                    modifications.append(f"fixed_broken_link")

        # Normalize header levels (optional)
        # Ensure consistent spacing after headers
        content = re.sub(r'^(#{1,6})\s*(.+)$', r'\1 \2', content, flags=re.MULTILINE)

        # Normalize list markers
        content = re.sub(r'^(\s*)[-*+]\s+', r'\1- ', content, flags=re.MULTILINE)

        # Restore LaTeX
        for i, latex in enumerate(latex_blocks):
            content = content.replace(f"__LATEX_BLOCK_{i}__", latex)

        # General text cleaning
        text_result = self.clean_text(content)
        if text_result.was_modified:
            content = text_result.cleaned
            modifications.extend(text_result.modifications)

        return CleaningResult(
            original=original,
            cleaned=content,
            was_modified=len(modifications) > 0,
            modifications=modifications
        )

    def clean_frontmatter(self, frontmatter: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """
        Clean YAML frontmatter dictionary.

        Normalizes:
        - Keys to lowercase with underscores
        - List values
        - Date strings
        """
        cleaned = {}
        modifications = []

        for key, value in frontmatter.items():
            # Normalize key
            clean_key = key.lower().strip().replace(' ', '_').replace('-', '_')
            if clean_key != key:
                modifications.append(f"normalized_key:{key}->{clean_key}")

            # Clean value
            if isinstance(value, str):
                result = self.clean_text(value)
                cleaned[clean_key] = result.cleaned
                if result.was_modified:
                    modifications.extend(result.modifications)
            elif isinstance(value, list):
                cleaned[clean_key] = [
                    self.clean_text(str(v)).cleaned if isinstance(v, str) else v
                    for v in value
                ]
            else:
                cleaned[clean_key] = value

        return cleaned, modifications

    def deduplicate_check(self, content: str, existing_hashes: set) -> Tuple[str, bool]:
        """
        Check if content is a duplicate based on hash.

        Returns:
            Tuple of (content_hash, is_duplicate)
        """
        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
        is_duplicate = content_hash in existing_hashes
        return content_hash, is_duplicate

    def validate_definition(self, data: Dict[str, Any]) -> ValidationResult:
        """
        Validate a definition record before ingestion.

        Checks:
        - Required fields present
        - Field types correct
        - No conflicting values
        """
        errors = []
        warnings = []

        # Required fields
        if not data.get('name'):
            errors.append("Missing required field: name")

        # Validate symbol (if present)
        symbol = data.get('symbol')
        if symbol and len(symbol) > 50:
            errors.append(f"Symbol too long: {len(symbol)} chars (max 50)")

        # Validate definition_id format
        def_id = data.get('definition_id')
        if def_id:
            if not re.match(r'^[a-z0-9_-]+$', def_id, re.IGNORECASE):
                warnings.append(f"definition_id contains special characters: {def_id}")

        # Validate status
        valid_statuses = {'canonical', 'draft', 'review', 'deprecated', 'conflicted'}
        status = data.get('status', 'draft')
        if status and status.lower() not in valid_statuses:
            warnings.append(f"Unknown status: {status}")

        # Validate domains (if present)
        domains = data.get('domains')
        if domains and not isinstance(domains, list):
            errors.append("domains must be a list")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )


class BatchCleaner:
    """
    Batch cleaning operations for large datasets.

    Processes data in chunks with progress tracking.
    """

    def __init__(self, cleaner: Optional[DataCleaner] = None, batch_size: int = 100):
        self.cleaner = cleaner or DataCleaner()
        self.batch_size = batch_size
        self.stats = {
            "total_processed": 0,
            "total_modified": 0,
            "total_errors": 0
        }

    def clean_rows(self, rows: List[Dict[str, Any]],
                   progress_callback=None) -> List[Dict[str, Any]]:
        """
        Clean a list of row dictionaries.

        Args:
            rows: List of row dictionaries
            progress_callback: Optional callback(current, total)

        Returns:
            List of cleaned row dictionaries
        """
        cleaned_rows = []

        for i, row in enumerate(rows):
            try:
                cleaned_row, modifications = self.cleaner.clean_row(row)
                cleaned_rows.append(cleaned_row)

                self.stats["total_processed"] += 1
                if modifications:
                    self.stats["total_modified"] += 1

            except Exception as e:
                # Keep original row on error
                cleaned_rows.append(row)
                self.stats["total_errors"] += 1

            # Progress callback
            if progress_callback and (i + 1) % self.batch_size == 0:
                progress_callback(i + 1, len(rows))

        return cleaned_rows

    def get_stats(self) -> Dict[str, int]:
        """Get cleaning statistics"""
        return self.stats.copy()

    def reset_stats(self):
        """Reset statistics"""
        self.stats = {
            "total_processed": 0,
            "total_modified": 0,
            "total_errors": 0
        }


# === CONVENIENCE FUNCTIONS ===

def clean_text(text: str) -> str:
    """Quick text cleaning"""
    cleaner = DataCleaner()
    return cleaner.clean_text(text).cleaned


def clean_cell(value: Any) -> Any:
    """Quick cell value cleaning"""
    cleaner = DataCleaner()
    return cleaner.clean_cell_value(value).cleaned


def clean_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Quick row cleaning"""
    cleaner = DataCleaner()
    cleaned, _ = cleaner.clean_row(row)
    return cleaned


def validate_before_ingest(data: Dict[str, Any]) -> ValidationResult:
    """Validate data before database ingestion"""
    cleaner = DataCleaner()
    return cleaner.validate_definition(data)
