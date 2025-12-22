#!/usr/bin/env python3
"""
Launch the Theophysics Ingest Engine GUI

This is a standalone GUI application for:
- Ingesting Excel, HTML, Markdown files into PostgreSQL/SQLite
- Data cleaning before ingestion
- UUID management for all records
- Scheduled file sync

Usage:
    python run_gui.py
"""

import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from gui.main_window import main

if __name__ == "__main__":
    main()
