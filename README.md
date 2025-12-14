# Theophysics Ingest Engine

PostgreSQL ingest engine for Excel, HTML tables, and Obsidian/Markdown notes with **full source attribution**.

## Features

- **Excel Ingestion** (Priority) - `.xlsx`, `.xls` files with full cell-level tracking
- **HTML Table Extraction** - Accurate table parsing using pandas + BeautifulSoup
- **Obsidian/Markdown Parsing** - Frontmatter, wikilinks, tags, equations
- **Source Attribution** - All data is marked with its origin (EXCEL, HTML, MARKDOWN, WEB, USER, PYTHON, AI)
- **PostgreSQL Storage** - Scalable database backend for 700+ definitions
- **Batch Processing** - Efficient handling of large files with progress tracking

## Packages Used

All packages are well-established with millions of weekly downloads:

| Package | Purpose | Weekly Downloads |
|---------|---------|-----------------|
| `openpyxl` | Excel .xlsx files | 44M+ |
| `pandas` | Data manipulation | 300M+ |
| `beautifulsoup4` | HTML parsing | 85M+ |
| `lxml` | Fast XML/HTML parser | 40M+ |
| `python-frontmatter` | YAML frontmatter | 1M+ |
| `psycopg2-binary` | PostgreSQL adapter | 50M+ |
| `sqlalchemy` | ORM/Database toolkit | 25M+ |

## Installation

```bash
# Clone the repository
git clone https://github.com/YellowKidokc/OBS-Plugin-Final-Codex.git
cd OBS-Plugin-Final-Codex

# Install dependencies
pip install -r requirements.txt

# Or install as package
pip install -e .
```

## Quick Start

### Without Database (Preview/Convert Only)

```python
from src.ingest import ExcelIngester, HTMLIngester, MarkdownIngester

# Excel to dictionary
from src.ingest.excel_ingest import excel_to_dict
data = excel_to_dict("data.xlsx")
# Returns: {"Sheet1": [{"col1": "val1", "__source__": {...}}, ...]}

# HTML tables to list
from src.ingest.html_ingest import html_tables_to_dict
tables = html_tables_to_dict("page.html")
# Returns: [{"headers": [...], "rows": [...], ...}]

# Parse Obsidian note
from src.ingest.markdown_ingest import parse_obsidian_note
note = parse_obsidian_note("definition.md")
# Returns: {"frontmatter": {...}, "sections": {...}, "tags": [...], ...}
```

### With PostgreSQL

```python
from src.orchestrator import IngestOrchestrator

# Initialize with database
engine = IngestOrchestrator("postgresql://localhost/theophysics")

# Auto-detect and ingest any file type
engine.ingest("data.xlsx")           # Excel
engine.ingest("page.html")           # HTML
engine.ingest("definition.md")       # Markdown

# Batch ingest a directory
engine.ingest_directory("/path/to/files")

# Ingest entire Obsidian vault
engine.ingest_vault("/path/to/obsidian/vault")

# Preview without ingesting
preview = engine.preview("data.xlsx")
print(preview)

engine.close()
```

## Source Attribution

**Every piece of ingested data is marked with its source:**

```python
{
    "data": {"column1": "value1", ...},
    "__source__": {
        "type": "EXCEL",           # or HTML, MARKDOWN, WEB, USER, PYTHON, AI
        "file": "/path/to/file.xlsx",
        "sheet": "Sheet1",         # For Excel
        "row": 5,                  # Row number
        "ingested_by": "python/ExcelIngester"
    }
}
```

## Database Schema

The engine uses these main tables:

- **`definitions`** - Core definition storage with all 10 template sections
- **`equations`** - LaTeX equations with variable tracking
- **`definition_usages`** - Where definitions appear across documents
- **`drift_logs`** - Tracks when definitions are used differently
- **`ingest_sessions`** - Audit trail of all ingest operations
- **`ingest_records`** - Individual records with source attribution
- **`excel_sheets`** - Ingested Excel sheet metadata
- **`html_tables`** - Ingested HTML table metadata
- **`obsidian_notes`** - Ingested markdown note metadata

## Configuration

Copy `.env.example` to `.env` and configure:

```bash
# PostgreSQL
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=theophysics
PG_USER=postgres
PG_PASSWORD=your_password

# Obsidian Vault
OBSIDIAN_VAULT_PATH=/path/to/vault
DEFINITION_FOLDER=Glossary
```

## CLI Usage

```bash
# Ingest a file
python -m src.orchestrator data.xlsx --db postgresql://localhost/theophysics

# Preview without ingesting
python -m src.orchestrator data.xlsx --preview

# Ingest directory
python -m src.orchestrator /path/to/files --db postgresql://localhost/theophysics
```

## Definition Template Support

The Markdown ingester recognizes the 10-section definition template:

```markdown
---
type: definition
id: def-coherence
symbol: C
name: Coherence
---

# C - Coherence

## 1. Core Definition
> Coherence is...

## 2. Axioms
1. Axiom C1: ...

## 3. Mathematical Structure
$$C = 1 - \frac{S_{obs}}{S_{max}}$$

## 4. Domain Interpretations
### Physics
...

## 5. Operationalization
...

## 6. Failure Modes
...

## 7. Integration Map
...

## 8. External Comparison
...

## 9. Notes
...
```

## Project Structure

```
OBS-Plugin-Final-Codex/
├── src/
│   ├── db/
│   │   ├── __init__.py
│   │   └── schema.py          # PostgreSQL schema with SQLAlchemy
│   ├── ingest/
│   │   ├── __init__.py
│   │   ├── excel_ingest.py    # Excel ingestion (openpyxl + pandas)
│   │   ├── html_ingest.py     # HTML table extraction
│   │   └── markdown_ingest.py # Obsidian/Markdown parsing
│   ├── utils/
│   │   └── __init__.py
│   └── orchestrator.py        # Main unified interface
├── config/
│   ├── __init__.py
│   └── settings.py            # Configuration management
├── tests/
├── requirements.txt
├── setup.py
├── .env.example
└── README.md
```

## Performance

- **Batch processing**: Commits in batches of 100 rows (configurable)
- **Progress tracking**: tqdm progress bars for large files
- **Parallel capable**: Independent files can be processed in parallel
- **Memory efficient**: Streaming for large Excel files

## License

MIT License
