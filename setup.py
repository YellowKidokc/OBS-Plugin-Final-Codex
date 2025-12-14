"""
Setup script for Theophysics Ingest Engine
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_path = Path(__file__).parent / "README.md"
long_description = readme_path.read_text() if readme_path.exists() else ""

setup(
    name="theophysics-ingest",
    version="1.0.0",
    author="Theophysics Project",
    description="PostgreSQL ingest engine for Excel, HTML, and Obsidian/Markdown with source attribution",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/YellowKidokc/OBS-Plugin-Final-Codex",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "psycopg2-binary>=2.9.9",
        "sqlalchemy>=2.0.23",
        "openpyxl>=3.1.2",
        "pandas>=2.1.4",
        "xlrd>=2.0.1",
        "beautifulsoup4>=4.12.2",
        "lxml>=4.9.4",
        "html5lib>=1.1",
        "python-frontmatter>=1.0.1",
        "markdown>=3.5.1",
        "mistune>=3.0.2",
        "tqdm>=4.66.1",
        "python-dotenv>=1.0.0",
        "pyyaml>=6.0.1",
        "chardet>=5.2.0",
        "pydantic>=2.5.2",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
        ],
        "async": [
            "asyncpg>=0.29.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "theophysics-ingest=orchestrator:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Scientific/Engineering",
    ],
    keywords="postgresql, excel, html, markdown, obsidian, ingest, etl",
)
