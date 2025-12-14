"""
Configuration settings for Theophysics Ingest Engine

Uses environment variables or .env file for sensitive data.
"""

import os
from pathlib import Path
from typing import Optional
from pydantic import BaseSettings, Field


class DatabaseSettings(BaseSettings):
    """PostgreSQL database configuration"""

    host: str = Field(default="localhost", env="PG_HOST")
    port: int = Field(default=5432, env="PG_PORT")
    database: str = Field(default="theophysics", env="PG_DATABASE")
    user: str = Field(default="postgres", env="PG_USER")
    password: str = Field(default="", env="PG_PASSWORD")

    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string"""
        if self.password:
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class IngestSettings(BaseSettings):
    """Ingest engine configuration"""

    # Batch processing
    batch_size: int = Field(default=100, env="INGEST_BATCH_SIZE")

    # Excel settings
    excel_max_rows: int = Field(default=100000, env="EXCEL_MAX_ROWS")
    excel_detect_headers: bool = Field(default=True, env="EXCEL_DETECT_HEADERS")

    # HTML settings
    html_parser: str = Field(default="lxml", env="HTML_PARSER")
    html_encoding: str = Field(default="auto", env="HTML_ENCODING")

    # Markdown settings
    markdown_parse_definitions: bool = Field(default=True, env="MD_PARSE_DEFINITIONS")
    markdown_extract_equations: bool = Field(default=True, env="MD_EXTRACT_EQUATIONS")

    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class VaultSettings(BaseSettings):
    """Obsidian vault configuration"""

    vault_path: Optional[str] = Field(default=None, env="OBSIDIAN_VAULT_PATH")
    ignore_folders: list = Field(
        default=[".obsidian", ".git", ".trash", "node_modules"],
        env="VAULT_IGNORE_FOLDERS"
    )
    definition_folder: Optional[str] = Field(default="Glossary", env="DEFINITION_FOLDER")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instances
db_settings = DatabaseSettings()
ingest_settings = IngestSettings()
vault_settings = VaultSettings()


def get_db_url() -> str:
    """Get the database connection URL"""
    return db_settings.connection_string
