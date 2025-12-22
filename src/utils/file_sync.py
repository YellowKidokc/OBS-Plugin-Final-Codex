"""
File Sync Module for Theophysics Ingest Engine

Handles scheduled scanning and change detection:
- File hash tracking for change detection
- Incremental sync (only changed files)
- Scheduled background scanning
- Conflict detection

Usage:
    sync = FileSyncManager(db_connection_string, vault_path)
    sync.scan_for_changes()
    sync.sync_changed_files()
"""

import os
import hashlib
import threading
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Set, Callable
from dataclasses import dataclass, field
from enum import Enum

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.schema import (
    get_engine, get_session, FileSync, ObsidianNote,
    create_all_tables
)


class SyncStatus(Enum):
    """File sync status"""
    SYNCED = "synced"
    PENDING = "pending"
    MODIFIED = "modified"
    NEW = "new"
    DELETED = "deleted"
    ERROR = "error"
    CONFLICT = "conflict"


@dataclass
class FileChange:
    """Represents a detected file change"""
    file_path: str
    status: SyncStatus
    old_hash: Optional[str] = None
    new_hash: Optional[str] = None
    last_modified: Optional[datetime] = None
    error_message: Optional[str] = None


@dataclass
class SyncResult:
    """Result of a sync operation"""
    total_scanned: int = 0
    new_files: int = 0
    modified_files: int = 0
    deleted_files: int = 0
    synced_files: int = 0
    errors: List[str] = field(default_factory=list)
    changes: List[FileChange] = field(default_factory=list)


class FileSyncManager:
    """
    Manages file synchronization between filesystem and database.

    Features:
    - Hash-based change detection
    - Incremental sync
    - Background scanning
    - Change history tracking
    """

    def __init__(self, db_connection_string: str, vault_path: str,
                 extensions: List[str] = None):
        """
        Initialize the sync manager.

        Args:
            db_connection_string: Database connection URL
            vault_path: Path to monitor
            extensions: File extensions to track (default: ['.md'])
        """
        self.db_connection_string = db_connection_string
        self.vault_path = Path(vault_path)
        self.extensions = extensions or ['.md']

        self.engine = get_engine(db_connection_string)
        create_all_tables(self.engine)

        self._stop_event = threading.Event()
        self._sync_thread: Optional[threading.Thread] = None
        self._callbacks: List[Callable[[SyncResult], None]] = []

    def _get_session(self):
        """Get a new database session"""
        return get_session(self.engine)

    def _compute_hash(self, file_path: Path) -> str:
        """Compute SHA-256 hash of file contents"""
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _get_file_info(self, file_path: Path) -> Dict:
        """Get file metadata"""
        stat = file_path.stat()
        return {
            'size': stat.st_size,
            'mtime': datetime.fromtimestamp(stat.st_mtime),
            'hash': self._compute_hash(file_path)
        }

    def scan_vault(self) -> Set[Path]:
        """Scan vault for all tracked files"""
        files = set()
        for ext in self.extensions:
            files.update(self.vault_path.glob(f"**/*{ext}"))
        return files

    def get_tracked_files(self) -> Dict[str, FileSync]:
        """Get all tracked files from database"""
        session = self._get_session()
        try:
            records = session.query(FileSync).all()
            return {r.file_path: r for r in records}
        finally:
            session.close()

    def scan_for_changes(self) -> SyncResult:
        """
        Scan vault and detect changes since last sync.

        Returns:
            SyncResult with detected changes
        """
        result = SyncResult()

        # Get current files in vault
        current_files = self.scan_vault()
        result.total_scanned = len(current_files)

        # Get tracked files from database
        tracked_files = self.get_tracked_files()
        tracked_paths = set(tracked_files.keys())
        current_paths = {str(f) for f in current_files}

        session = self._get_session()

        try:
            # Check for new files
            for file_path in current_files:
                path_str = str(file_path)

                if path_str not in tracked_paths:
                    # New file
                    try:
                        file_info = self._get_file_info(file_path)
                        change = FileChange(
                            file_path=path_str,
                            status=SyncStatus.NEW,
                            new_hash=file_info['hash'],
                            last_modified=file_info['mtime']
                        )
                        result.changes.append(change)
                        result.new_files += 1
                    except Exception as e:
                        result.errors.append(f"Error reading {path_str}: {str(e)}")

                else:
                    # Existing file - check for modifications
                    tracked = tracked_files[path_str]
                    try:
                        file_info = self._get_file_info(file_path)

                        if file_info['hash'] != tracked.file_hash:
                            change = FileChange(
                                file_path=path_str,
                                status=SyncStatus.MODIFIED,
                                old_hash=tracked.file_hash,
                                new_hash=file_info['hash'],
                                last_modified=file_info['mtime']
                            )
                            result.changes.append(change)
                            result.modified_files += 1
                        else:
                            result.synced_files += 1

                    except Exception as e:
                        result.errors.append(f"Error checking {path_str}: {str(e)}")

            # Check for deleted files
            for path_str in tracked_paths:
                if path_str not in current_paths:
                    change = FileChange(
                        file_path=path_str,
                        status=SyncStatus.DELETED
                    )
                    result.changes.append(change)
                    result.deleted_files += 1

        finally:
            session.close()

        return result

    def sync_file(self, file_path: str) -> bool:
        """
        Sync a single file to database.

        Args:
            file_path: Path to file

        Returns:
            True if successful
        """
        path = Path(file_path)

        if not path.exists():
            return self._mark_deleted(file_path)

        session = self._get_session()

        try:
            file_info = self._get_file_info(path)

            # Check if tracked
            record = session.query(FileSync).filter_by(file_path=file_path).first()

            if record:
                # Update existing
                record.file_hash = file_info['hash']
                record.file_size = file_info['size']
                record.last_modified = file_info['mtime']
                record.last_synced = datetime.utcnow()
                record.sync_status = "synced"
                record.needs_resync = False
            else:
                # Create new
                record = FileSync(
                    file_path=file_path,
                    file_hash=file_info['hash'],
                    file_size=file_info['size'],
                    last_modified=file_info['mtime'],
                    last_synced=datetime.utcnow(),
                    sync_status="synced"
                )
                session.add(record)

            session.commit()
            return True

        except Exception as e:
            session.rollback()
            return False

        finally:
            session.close()

    def _mark_deleted(self, file_path: str) -> bool:
        """Mark a file as deleted in the database"""
        session = self._get_session()
        try:
            record = session.query(FileSync).filter_by(file_path=file_path).first()
            if record:
                record.sync_status = "deleted"
                record.last_synced = datetime.utcnow()
                session.commit()
            return True
        except:
            session.rollback()
            return False
        finally:
            session.close()

    def sync_all_changes(self, result: Optional[SyncResult] = None,
                         progress_callback: Optional[Callable[[int, int], None]] = None) -> int:
        """
        Sync all detected changes.

        Args:
            result: Previous scan result (or will scan fresh)
            progress_callback: Optional callback(current, total)

        Returns:
            Number of files synced
        """
        if result is None:
            result = self.scan_for_changes()

        synced = 0
        total = len(result.changes)

        for i, change in enumerate(result.changes):
            if change.status in (SyncStatus.NEW, SyncStatus.MODIFIED):
                if self.sync_file(change.file_path):
                    synced += 1
            elif change.status == SyncStatus.DELETED:
                if self._mark_deleted(change.file_path):
                    synced += 1

            if progress_callback:
                progress_callback(i + 1, total)

        return synced

    def initial_sync(self, progress_callback: Optional[Callable[[int, int], None]] = None) -> int:
        """
        Perform initial sync of all files in vault.

        Returns:
            Number of files synced
        """
        files = list(self.scan_vault())
        total = len(files)
        synced = 0

        for i, file_path in enumerate(files):
            if self.sync_file(str(file_path)):
                synced += 1

            if progress_callback:
                progress_callback(i + 1, total)

        return synced

    # === BACKGROUND SYNC ===

    def start_background_sync(self, interval_seconds: int = 300,
                              callback: Optional[Callable[[SyncResult], None]] = None):
        """
        Start background sync thread.

        Args:
            interval_seconds: Time between syncs
            callback: Optional callback when sync completes
        """
        if self._sync_thread and self._sync_thread.is_alive():
            return

        if callback:
            self._callbacks.append(callback)

        self._stop_event.clear()
        self._sync_thread = threading.Thread(
            target=self._background_sync_worker,
            args=(interval_seconds,),
            daemon=True
        )
        self._sync_thread.start()

    def stop_background_sync(self):
        """Stop background sync thread"""
        self._stop_event.set()
        if self._sync_thread:
            self._sync_thread.join(timeout=5)

    def _background_sync_worker(self, interval: int):
        """Background sync worker thread"""
        while not self._stop_event.is_set():
            try:
                result = self.scan_for_changes()

                if result.new_files > 0 or result.modified_files > 0 or result.deleted_files > 0:
                    self.sync_all_changes(result)

                # Notify callbacks
                for callback in self._callbacks:
                    try:
                        callback(result)
                    except:
                        pass

            except Exception as e:
                pass  # Log error in production

            # Wait for next interval
            self._stop_event.wait(interval)

    def add_callback(self, callback: Callable[[SyncResult], None]):
        """Add a sync completion callback"""
        self._callbacks.append(callback)

    def remove_callback(self, callback: Callable[[SyncResult], None]):
        """Remove a sync completion callback"""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    # === STATUS QUERIES ===

    def get_pending_files(self) -> List[FileSync]:
        """Get files pending sync"""
        session = self._get_session()
        try:
            return session.query(FileSync).filter(
                FileSync.needs_resync == True
            ).all()
        finally:
            session.close()

    def get_sync_stats(self) -> Dict[str, int]:
        """Get sync statistics"""
        session = self._get_session()
        try:
            total = session.query(FileSync).count()
            synced = session.query(FileSync).filter_by(sync_status="synced").count()
            pending = session.query(FileSync).filter_by(needs_resync=True).count()
            errors = session.query(FileSync).filter_by(sync_status="error").count()

            return {
                "total_tracked": total,
                "synced": synced,
                "pending": pending,
                "errors": errors
            }
        finally:
            session.close()


# === CONVENIENCE FUNCTIONS ===

def quick_sync(db_url: str, vault_path: str) -> SyncResult:
    """
    Quick one-shot sync of a vault.

    Args:
        db_url: Database connection string
        vault_path: Path to vault

    Returns:
        SyncResult
    """
    manager = FileSyncManager(db_url, vault_path)
    result = manager.scan_for_changes()
    manager.sync_all_changes(result)
    return result


def scan_vault(vault_path: str) -> List[str]:
    """
    Scan vault for markdown files (no database required).

    Args:
        vault_path: Path to vault

    Returns:
        List of file paths
    """
    vault = Path(vault_path)
    return [str(f) for f in vault.glob("**/*.md")]
