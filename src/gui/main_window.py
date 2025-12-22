"""
Tkinter GUI for Theophysics Ingest Engine

A standalone Python GUI for:
- Ingesting Excel, HTML, Markdown files
- Data cleaning before PostgreSQL/SQLite ingestion
- UUID management
- Scheduled file sync

Uses built-in tkinter (no extra dependencies).
"""

import os
import sys
import threading
import queue
from pathlib import Path
from datetime import datetime
from typing import Optional, Callable
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.schema import init_database, init_sqlite, get_session, get_engine, FileSync
from ingest.excel_ingest import ExcelIngester
from ingest.html_ingest import HTMLIngester
from ingest.markdown_ingest import MarkdownIngester
from utils.data_cleaner import DataCleaner, BatchCleaner


class IngestApp:
    """Main GUI Application for Theophysics Ingest Engine"""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Theophysics Ingest Engine")
        self.root.geometry("900x700")
        self.root.minsize(800, 600)

        # State
        self.db_engine = None
        self.db_session = None
        self.db_type = tk.StringVar(value="sqlite")
        self.db_path = tk.StringVar(value="theophysics.db")
        self.pg_host = tk.StringVar(value="localhost")
        self.pg_port = tk.StringVar(value="5432")
        self.pg_database = tk.StringVar(value="theophysics")
        self.pg_user = tk.StringVar(value="postgres")
        self.pg_password = tk.StringVar(value="")

        # Vault path for sync
        self.vault_path = tk.StringVar(value="")
        self.sync_interval = tk.IntVar(value=60)  # minutes
        self.sync_enabled = tk.BooleanVar(value=False)
        self.sync_job = None

        # Processing state
        self.is_processing = False
        self.message_queue = queue.Queue()
        self.cleaner = DataCleaner()

        # Build UI
        self._create_menu()
        self._create_notebook()
        self._create_status_bar()

        # Start message processor
        self._process_messages()

    def _create_menu(self):
        """Create menu bar"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Ingest File...", command=self._ingest_file_dialog)
        file_menu.add_command(label="Ingest Folder...", command=self._ingest_folder_dialog)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)

        # Database menu
        db_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Database", menu=db_menu)
        db_menu.add_command(label="Connect", command=self._connect_database)
        db_menu.add_command(label="Disconnect", command=self._disconnect_database)
        db_menu.add_separator()
        db_menu.add_command(label="View Stats", command=self._show_db_stats)

        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="About", command=self._show_about)

    def _create_notebook(self):
        """Create tabbed interface"""
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Tabs
        self._create_ingest_tab()
        self._create_database_tab()
        self._create_sync_tab()
        self._create_log_tab()

    def _create_ingest_tab(self):
        """Create the main ingest tab"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Ingest")

        # File selection frame
        file_frame = ttk.LabelFrame(tab, text="File Selection", padding=10)
        file_frame.pack(fill=tk.X, padx=10, pady=5)

        # File path entry
        ttk.Label(file_frame, text="File/Folder:").grid(row=0, column=0, sticky=tk.W)
        self.file_entry = ttk.Entry(file_frame, width=60)
        self.file_entry.grid(row=0, column=1, padx=5, sticky=tk.EW)

        btn_frame = ttk.Frame(file_frame)
        btn_frame.grid(row=0, column=2)
        ttk.Button(btn_frame, text="File", command=self._browse_file).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="Folder", command=self._browse_folder).pack(side=tk.LEFT, padx=2)

        file_frame.columnconfigure(1, weight=1)

        # Options frame
        options_frame = ttk.LabelFrame(tab, text="Options", padding=10)
        options_frame.pack(fill=tk.X, padx=10, pady=5)

        self.clean_data = tk.BooleanVar(value=True)
        self.parse_definitions = tk.BooleanVar(value=True)
        self.recursive = tk.BooleanVar(value=True)

        ttk.Checkbutton(options_frame, text="Clean data before ingest",
                       variable=self.clean_data).grid(row=0, column=0, sticky=tk.W)
        ttk.Checkbutton(options_frame, text="Parse as definitions (Markdown)",
                       variable=self.parse_definitions).grid(row=0, column=1, sticky=tk.W)
        ttk.Checkbutton(options_frame, text="Recursive folder scan",
                       variable=self.recursive).grid(row=0, column=2, sticky=tk.W)

        # File type filter
        ttk.Label(options_frame, text="File Types:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.file_types = {
            'excel': tk.BooleanVar(value=True),
            'html': tk.BooleanVar(value=True),
            'markdown': tk.BooleanVar(value=True)
        }
        ttk.Checkbutton(options_frame, text="Excel (.xlsx, .xls)",
                       variable=self.file_types['excel']).grid(row=1, column=1, sticky=tk.W)
        ttk.Checkbutton(options_frame, text="HTML (.html, .htm)",
                       variable=self.file_types['html']).grid(row=1, column=2, sticky=tk.W)
        ttk.Checkbutton(options_frame, text="Markdown (.md)",
                       variable=self.file_types['markdown']).grid(row=1, column=3, sticky=tk.W)

        # Action buttons
        action_frame = ttk.Frame(tab)
        action_frame.pack(fill=tk.X, padx=10, pady=10)

        self.ingest_btn = ttk.Button(action_frame, text="Start Ingest",
                                     command=self._start_ingest)
        self.ingest_btn.pack(side=tk.LEFT, padx=5)

        self.preview_btn = ttk.Button(action_frame, text="Preview",
                                      command=self._preview_file)
        self.preview_btn.pack(side=tk.LEFT, padx=5)

        self.stop_btn = ttk.Button(action_frame, text="Stop",
                                   command=self._stop_ingest, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT, padx=5)

        # Progress frame
        progress_frame = ttk.LabelFrame(tab, text="Progress", padding=10)
        progress_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.progress_bar = ttk.Progressbar(progress_frame, mode='determinate')
        self.progress_bar.pack(fill=tk.X, pady=5)

        self.progress_label = ttk.Label(progress_frame, text="Ready")
        self.progress_label.pack(anchor=tk.W)

        # Results tree
        columns = ('Type', 'File', 'Records', 'Status')
        self.results_tree = ttk.Treeview(progress_frame, columns=columns, show='headings', height=10)
        for col in columns:
            self.results_tree.heading(col, text=col)
            self.results_tree.column(col, width=100)
        self.results_tree.column('File', width=300)
        self.results_tree.pack(fill=tk.BOTH, expand=True, pady=5)

        # Scrollbar for results
        scrollbar = ttk.Scrollbar(progress_frame, orient=tk.VERTICAL, command=self.results_tree.yview)
        self.results_tree.configure(yscrollcommand=scrollbar.set)

    def _create_database_tab(self):
        """Create database configuration tab"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Database")

        # Database type selection
        type_frame = ttk.LabelFrame(tab, text="Database Type", padding=10)
        type_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Radiobutton(type_frame, text="SQLite (Local File)",
                       variable=self.db_type, value="sqlite",
                       command=self._update_db_fields).grid(row=0, column=0, sticky=tk.W)
        ttk.Radiobutton(type_frame, text="PostgreSQL (Server)",
                       variable=self.db_type, value="postgresql",
                       command=self._update_db_fields).grid(row=0, column=1, sticky=tk.W)

        # SQLite settings
        self.sqlite_frame = ttk.LabelFrame(tab, text="SQLite Settings", padding=10)
        self.sqlite_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(self.sqlite_frame, text="Database File:").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(self.sqlite_frame, textvariable=self.db_path, width=50).grid(row=0, column=1, padx=5)
        ttk.Button(self.sqlite_frame, text="Browse",
                  command=self._browse_db_file).grid(row=0, column=2)

        # PostgreSQL settings
        self.pg_frame = ttk.LabelFrame(tab, text="PostgreSQL Settings", padding=10)
        self.pg_frame.pack(fill=tk.X, padx=10, pady=5)

        labels = ["Host:", "Port:", "Database:", "User:", "Password:"]
        vars = [self.pg_host, self.pg_port, self.pg_database, self.pg_user, self.pg_password]

        for i, (label, var) in enumerate(zip(labels, vars)):
            ttk.Label(self.pg_frame, text=label).grid(row=i, column=0, sticky=tk.W, pady=2)
            if label == "Password:":
                ttk.Entry(self.pg_frame, textvariable=var, width=30, show="*").grid(row=i, column=1, padx=5, sticky=tk.W)
            else:
                ttk.Entry(self.pg_frame, textvariable=var, width=30).grid(row=i, column=1, padx=5, sticky=tk.W)

        # Connection status
        status_frame = ttk.LabelFrame(tab, text="Connection Status", padding=10)
        status_frame.pack(fill=tk.X, padx=10, pady=5)

        self.db_status_label = ttk.Label(status_frame, text="Not connected", foreground="red")
        self.db_status_label.pack(anchor=tk.W)

        btn_frame = ttk.Frame(status_frame)
        btn_frame.pack(anchor=tk.W, pady=5)

        ttk.Button(btn_frame, text="Connect", command=self._connect_database).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Test Connection", command=self._test_connection).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Initialize Tables", command=self._init_tables).pack(side=tk.LEFT, padx=5)

        self._update_db_fields()

    def _create_sync_tab(self):
        """Create scheduled sync tab"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Sync")

        # Vault selection
        vault_frame = ttk.LabelFrame(tab, text="Obsidian Vault", padding=10)
        vault_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(vault_frame, text="Vault Path:").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(vault_frame, textvariable=self.vault_path, width=50).grid(row=0, column=1, padx=5)
        ttk.Button(vault_frame, text="Browse",
                  command=self._browse_vault).grid(row=0, column=2)

        # Sync settings
        sync_frame = ttk.LabelFrame(tab, text="Scheduled Sync", padding=10)
        sync_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Checkbutton(sync_frame, text="Enable automatic sync",
                       variable=self.sync_enabled,
                       command=self._toggle_sync).grid(row=0, column=0, sticky=tk.W)

        ttk.Label(sync_frame, text="Sync interval (minutes):").grid(row=1, column=0, sticky=tk.W, pady=5)
        ttk.Spinbox(sync_frame, from_=1, to=1440, textvariable=self.sync_interval,
                   width=10).grid(row=1, column=1, sticky=tk.W)

        # Sync status
        self.sync_status_label = ttk.Label(sync_frame, text="Sync disabled")
        self.sync_status_label.grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=5)

        # Manual sync button
        btn_frame = ttk.Frame(sync_frame)
        btn_frame.grid(row=3, column=0, columnspan=2, sticky=tk.W, pady=5)

        ttk.Button(btn_frame, text="Sync Now", command=self._manual_sync).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Check Changes", command=self._check_changes).pack(side=tk.LEFT, padx=5)

        # Changed files list
        changes_frame = ttk.LabelFrame(tab, text="Detected Changes", padding=10)
        changes_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        columns = ('File', 'Status', 'Last Modified')
        self.changes_tree = ttk.Treeview(changes_frame, columns=columns, show='headings', height=10)
        for col in columns:
            self.changes_tree.heading(col, text=col)
        self.changes_tree.column('File', width=400)
        self.changes_tree.pack(fill=tk.BOTH, expand=True)

    def _create_log_tab(self):
        """Create log output tab"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Log")

        self.log_text = scrolledtext.ScrolledText(tab, wrap=tk.WORD, height=30)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        btn_frame = ttk.Frame(tab)
        btn_frame.pack(fill=tk.X, padx=10, pady=5)
        ttk.Button(btn_frame, text="Clear Log", command=self._clear_log).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Save Log", command=self._save_log).pack(side=tk.LEFT, padx=5)

    def _create_status_bar(self):
        """Create status bar at bottom"""
        self.status_bar = ttk.Label(self.root, text="Ready", relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    # === UI HELPERS ===

    def _update_db_fields(self):
        """Show/hide database fields based on type"""
        if self.db_type.get() == "sqlite":
            self.sqlite_frame.pack(fill=tk.X, padx=10, pady=5, after=self.notebook)
            self.pg_frame.pack_forget()
        else:
            self.pg_frame.pack(fill=tk.X, padx=10, pady=5, after=self.notebook)
            self.sqlite_frame.pack_forget()

    def _log(self, message: str):
        """Add message to log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.message_queue.put(('log', f"[{timestamp}] {message}"))

    def _update_status(self, message: str):
        """Update status bar"""
        self.message_queue.put(('status', message))

    def _update_progress(self, value: int, text: str = ""):
        """Update progress bar"""
        self.message_queue.put(('progress', (value, text)))

    def _process_messages(self):
        """Process queued UI updates (called from main thread)"""
        try:
            while True:
                msg_type, data = self.message_queue.get_nowait()

                if msg_type == 'log':
                    self.log_text.insert(tk.END, data + "\n")
                    self.log_text.see(tk.END)
                elif msg_type == 'status':
                    self.status_bar.config(text=data)
                elif msg_type == 'progress':
                    value, text = data
                    self.progress_bar['value'] = value
                    if text:
                        self.progress_label.config(text=text)
                elif msg_type == 'result':
                    self.results_tree.insert('', tk.END, values=data)
                elif msg_type == 'done':
                    self._ingest_complete()

        except queue.Empty:
            pass

        # Schedule next check
        self.root.after(100, self._process_messages)

    # === BROWSE DIALOGS ===

    def _browse_file(self):
        """Browse for file to ingest"""
        filetypes = [
            ("All supported", "*.xlsx *.xls *.html *.htm *.md"),
            ("Excel files", "*.xlsx *.xls"),
            ("HTML files", "*.html *.htm"),
            ("Markdown files", "*.md"),
            ("All files", "*.*")
        ]
        path = filedialog.askopenfilename(filetypes=filetypes)
        if path:
            self.file_entry.delete(0, tk.END)
            self.file_entry.insert(0, path)

    def _browse_folder(self):
        """Browse for folder to ingest"""
        path = filedialog.askdirectory()
        if path:
            self.file_entry.delete(0, tk.END)
            self.file_entry.insert(0, path)

    def _browse_db_file(self):
        """Browse for SQLite database file"""
        path = filedialog.asksaveasfilename(
            defaultextension=".db",
            filetypes=[("SQLite database", "*.db"), ("All files", "*.*")]
        )
        if path:
            self.db_path.set(path)

    def _browse_vault(self):
        """Browse for Obsidian vault folder"""
        path = filedialog.askdirectory()
        if path:
            self.vault_path.set(path)

    def _ingest_file_dialog(self):
        """Menu: Ingest File"""
        self._browse_file()
        if self.file_entry.get():
            self._start_ingest()

    def _ingest_folder_dialog(self):
        """Menu: Ingest Folder"""
        self._browse_folder()
        if self.file_entry.get():
            self._start_ingest()

    # === DATABASE OPERATIONS ===

    def _get_connection_string(self) -> str:
        """Build connection string from settings"""
        if self.db_type.get() == "sqlite":
            return f"sqlite:///{self.db_path.get()}"
        else:
            password = self.pg_password.get()
            if password:
                return f"postgresql://{self.pg_user.get()}:{password}@{self.pg_host.get()}:{self.pg_port.get()}/{self.pg_database.get()}"
            return f"postgresql://{self.pg_user.get()}@{self.pg_host.get()}:{self.pg_port.get()}/{self.pg_database.get()}"

    def _connect_database(self):
        """Connect to database"""
        try:
            conn_str = self._get_connection_string()
            self.db_engine = get_engine(conn_str)
            self.db_session = get_session(self.db_engine)

            self.db_status_label.config(text="Connected", foreground="green")
            self._log(f"Connected to database: {self.db_type.get()}")
            self._update_status("Database connected")

        except Exception as e:
            self.db_status_label.config(text=f"Error: {str(e)}", foreground="red")
            self._log(f"Database connection error: {str(e)}")
            messagebox.showerror("Connection Error", str(e))

    def _disconnect_database(self):
        """Disconnect from database"""
        if self.db_session:
            self.db_session.close()
            self.db_session = None
        self.db_engine = None
        self.db_status_label.config(text="Not connected", foreground="red")
        self._log("Disconnected from database")

    def _test_connection(self):
        """Test database connection"""
        try:
            conn_str = self._get_connection_string()
            engine = get_engine(conn_str)
            with engine.connect() as conn:
                conn.execute("SELECT 1")
            messagebox.showinfo("Success", "Connection successful!")
            self._log("Database connection test: OK")
        except Exception as e:
            messagebox.showerror("Connection Failed", str(e))
            self._log(f"Database connection test: FAILED - {str(e)}")

    def _init_tables(self):
        """Initialize database tables"""
        try:
            conn_str = self._get_connection_string()
            init_database(conn_str)
            messagebox.showinfo("Success", "Tables initialized successfully!")
            self._log("Database tables initialized")
        except Exception as e:
            messagebox.showerror("Error", str(e))
            self._log(f"Table initialization error: {str(e)}")

    def _show_db_stats(self):
        """Show database statistics"""
        if not self.db_session:
            messagebox.showwarning("Not Connected", "Please connect to database first")
            return

        try:
            from db.schema import Definition, ObsidianNote, ExcelSheet, HTMLTable

            stats = {
                "Definitions": self.db_session.query(Definition).count(),
                "Obsidian Notes": self.db_session.query(ObsidianNote).count(),
                "Excel Sheets": self.db_session.query(ExcelSheet).count(),
                "HTML Tables": self.db_session.query(HTMLTable).count(),
            }

            msg = "\n".join([f"{k}: {v}" for k, v in stats.items()])
            messagebox.showinfo("Database Statistics", msg)

        except Exception as e:
            messagebox.showerror("Error", str(e))

    # === INGEST OPERATIONS ===

    def _preview_file(self):
        """Preview file without ingesting"""
        path = self.file_entry.get()
        if not path:
            messagebox.showwarning("No File", "Please select a file first")
            return

        try:
            if path.endswith(('.xlsx', '.xls')):
                ingester = ExcelIngester()
                preview = ingester.get_sheet_preview(path)
            elif path.endswith(('.html', '.htm')):
                ingester = HTMLIngester()
                preview = ingester.get_table_preview(path)
            elif path.endswith('.md'):
                ingester = MarkdownIngester()
                preview = ingester.get_note_preview(path)
            else:
                messagebox.showwarning("Unknown Type", "Cannot preview this file type")
                return

            # Show preview in popup
            preview_window = tk.Toplevel(self.root)
            preview_window.title(f"Preview: {Path(path).name}")
            preview_window.geometry("600x400")

            text = scrolledtext.ScrolledText(preview_window, wrap=tk.WORD)
            text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

            import json
            text.insert(tk.END, json.dumps(preview, indent=2, default=str))

        except Exception as e:
            messagebox.showerror("Preview Error", str(e))

    def _start_ingest(self):
        """Start ingestion process"""
        path = self.file_entry.get()
        if not path:
            messagebox.showwarning("No File", "Please select a file or folder")
            return

        if not self.db_engine:
            if messagebox.askyesno("No Database",
                                   "No database connected. Connect now?"):
                self._connect_database()
                if not self.db_engine:
                    return
            else:
                return

        # Clear previous results
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)

        # Disable buttons during processing
        self.is_processing = True
        self.ingest_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)

        # Start in background thread
        thread = threading.Thread(target=self._ingest_worker, args=(path,))
        thread.daemon = True
        thread.start()

    def _ingest_worker(self, path: str):
        """Background worker for ingestion"""
        try:
            conn_str = self._get_connection_string()

            if os.path.isfile(path):
                self._ingest_single_file(path, conn_str)
            else:
                self._ingest_directory(path, conn_str)

        except Exception as e:
            self._log(f"Ingest error: {str(e)}")

        finally:
            self.message_queue.put(('done', None))

    def _ingest_single_file(self, path: str, conn_str: str):
        """Ingest a single file"""
        self._log(f"Ingesting: {path}")
        self._update_progress(0, f"Processing: {Path(path).name}")

        ext = Path(path).suffix.lower()

        try:
            if ext in ('.xlsx', '.xls') and self.file_types['excel'].get():
                ingester = ExcelIngester(conn_str)
                result = ingester.ingest_file(path)
                self.message_queue.put(('result', ('Excel', Path(path).name,
                                                    result.get('records_created', 0), 'OK')))
                ingester.close()

            elif ext in ('.html', '.htm') and self.file_types['html'].get():
                ingester = HTMLIngester(conn_str)
                result = ingester.ingest_file(path)
                self.message_queue.put(('result', ('HTML', Path(path).name,
                                                    result.get('records_created', 0), 'OK')))
                ingester.close()

            elif ext == '.md' and self.file_types['markdown'].get():
                ingester = MarkdownIngester(conn_str)
                result = ingester.ingest_file(path, as_definition=self.parse_definitions.get())
                status = 'Definition' if result.get('is_definition') else 'Note'
                self.message_queue.put(('result', ('Markdown', Path(path).name, 1, status)))
                ingester.close()

            self._update_progress(100, "Complete")
            self._log(f"Completed: {path}")

        except Exception as e:
            self._log(f"Error processing {path}: {str(e)}")
            self.message_queue.put(('result', (ext[1:].upper(), Path(path).name, 0, f'Error: {str(e)}')))

    def _ingest_directory(self, path: str, conn_str: str):
        """Ingest all files in directory"""
        self._log(f"Scanning directory: {path}")

        files = []
        pattern = "**/*" if self.recursive.get() else "*"

        for ext in ['.xlsx', '.xls', '.html', '.htm', '.md']:
            files.extend(Path(path).glob(f"{pattern}{ext}"))

        total = len(files)
        self._log(f"Found {total} files to process")

        for i, file_path in enumerate(files):
            if not self.is_processing:
                self._log("Ingest cancelled by user")
                break

            progress = int((i / total) * 100) if total > 0 else 0
            self._update_progress(progress, f"Processing {i + 1}/{total}: {file_path.name}")

            self._ingest_single_file(str(file_path), conn_str)

        self._update_progress(100, f"Completed: {total} files")

    def _stop_ingest(self):
        """Stop the current ingest operation"""
        self.is_processing = False
        self._log("Stopping ingest...")

    def _ingest_complete(self):
        """Called when ingest is complete"""
        self.is_processing = False
        self.ingest_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self._update_status("Ingest complete")

    # === SYNC OPERATIONS ===

    def _toggle_sync(self):
        """Toggle scheduled sync on/off"""
        if self.sync_enabled.get():
            self._start_scheduled_sync()
        else:
            self._stop_scheduled_sync()

    def _start_scheduled_sync(self):
        """Start scheduled sync"""
        if not self.vault_path.get():
            messagebox.showwarning("No Vault", "Please select an Obsidian vault first")
            self.sync_enabled.set(False)
            return

        interval_ms = self.sync_interval.get() * 60 * 1000
        self.sync_status_label.config(text=f"Sync enabled - every {self.sync_interval.get()} minutes")
        self._log(f"Started scheduled sync (interval: {self.sync_interval.get()} min)")

        self._schedule_next_sync()

    def _schedule_next_sync(self):
        """Schedule next sync"""
        if self.sync_enabled.get():
            interval_ms = self.sync_interval.get() * 60 * 1000
            self.sync_job = self.root.after(interval_ms, self._perform_scheduled_sync)

    def _stop_scheduled_sync(self):
        """Stop scheduled sync"""
        if self.sync_job:
            self.root.after_cancel(self.sync_job)
            self.sync_job = None
        self.sync_status_label.config(text="Sync disabled")
        self._log("Stopped scheduled sync")

    def _perform_scheduled_sync(self):
        """Perform a scheduled sync"""
        self._log("Running scheduled sync...")
        self._manual_sync()
        self._schedule_next_sync()

    def _manual_sync(self):
        """Manually trigger a sync"""
        vault = self.vault_path.get()
        if not vault:
            messagebox.showwarning("No Vault", "Please select a vault first")
            return

        if not self.db_engine:
            messagebox.showwarning("No Database", "Please connect to database first")
            return

        self._log(f"Syncing vault: {vault}")
        # TODO: Implement actual sync with change detection
        messagebox.showinfo("Sync", "Sync functionality coming soon!")

    def _check_changes(self):
        """Check for file changes"""
        vault = self.vault_path.get()
        if not vault:
            messagebox.showwarning("No Vault", "Please select a vault first")
            return

        # Clear previous
        for item in self.changes_tree.get_children():
            self.changes_tree.delete(item)

        # Scan for markdown files
        for md_file in Path(vault).glob("**/*.md"):
            stat = md_file.stat()
            modified = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M")
            self.changes_tree.insert('', tk.END, values=(str(md_file.name), "Modified", modified))

        self._log(f"Checked {vault} for changes")

    # === LOG OPERATIONS ===

    def _clear_log(self):
        """Clear the log text"""
        self.log_text.delete(1.0, tk.END)

    def _save_log(self):
        """Save log to file"""
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if path:
            with open(path, 'w') as f:
                f.write(self.log_text.get(1.0, tk.END))
            self._log(f"Log saved to: {path}")

    def _show_about(self):
        """Show about dialog"""
        messagebox.showinfo("About",
            "Theophysics Ingest Engine\n\n"
            "A standalone GUI for ingesting:\n"
            "- Excel files\n"
            "- HTML tables\n"
            "- Obsidian/Markdown notes\n\n"
            "With data cleaning, UUID management,\n"
            "and PostgreSQL/SQLite support.")


def main():
    """Run the GUI application"""
    root = tk.Tk()
    app = IngestApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
