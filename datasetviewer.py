#!/usr/bin/env python3
"""
JSONL Dataset Viewer v3
- Downloads dataset directly from HuggingFace
- Streams data directly to SQLite (no intermediate storage)
- Progressive loading - view content while downloading
- Improved description display with all summary types
"""

import sys
import json
import threading
import io
import time
import os
import hashlib
import sqlite3
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor

# --- Required Libraries ---
try:
    import dearpygui.dearpygui as dpg
    import requests
    from PIL import Image
    import numpy as np
    from huggingface_hub import HfApi, hf_hub_url, list_repo_files
except ImportError as e:
    print(f"Error: Required libraries not found: {e}")
    print("Please run: pip install dearpygui requests Pillow numpy huggingface_hub")
    sys.exit(1)


# ============================================================================
# Configuration
# ============================================================================
HUGGINGFACE_DATASET = "lodestones/e621-captions"  # The dataset to download
FILE_EXTENSION = ".jsonl"
TEST_FILE_LIMIT = 3  # Set to None for no limit (download all files)
DB_PATH = "dataset_v3.db"


# ============================================================================
# Database Handler - Stores Everything
# ============================================================================
class DatasetDatabase:
    """
    Single database that stores all post data.
    No more reading from JSONL files - everything is in SQLite.
    """

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.conn = None
        self.lock = threading.Lock()
        self._connect()
        self._create_tables()

    def _connect(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-64000")  # 64MB cache

    def _create_tables(self):
        with self.lock:
            cursor = self.conn.cursor()

            # Track which files have been processed
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT UNIQUE NOT NULL,
                    file_hash TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Main posts table - stores ALL data (no raw_json to save space)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id INTEGER NOT NULL,
                    md5 TEXT UNIQUE,
                    url TEXT,
                    tags TEXT,
                    tag_string TEXT,
                    rating TEXT,
                    file_ext TEXT,
                    source TEXT,
                    created_at TEXT,
                    score INTEGER,
                    fav_count INTEGER,
                    image_width INTEGER,
                    image_height INTEGER,
                    file_size INTEGER,
                    description TEXT,
                    regular_summary TEXT,
                    individual_parts TEXT,
                    midjourney_style_summary TEXT,
                    deviantart_commission_request TEXT,
                    brief_summary TEXT
                )
            """)

            # Indexes for fast searching
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_post_id ON posts(post_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_md5 ON posts(md5)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tag_string ON posts(tag_string)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_rating ON posts(rating)")

            self.conn.commit()

    def is_file_processed(self, filename):
        """Check if a file has already been processed."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM processed_files WHERE filename = ?", (filename,))
            return cursor.fetchone() is not None

    def mark_file_processed(self, filename, file_hash=None):
        """Mark a file as processed."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO processed_files (filename, file_hash) VALUES (?, ?)",
                (filename, file_hash)
            )
            self.conn.commit()

    def post_exists(self, md5):
        """Check if a post already exists by MD5."""
        if not md5:
            return False
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM posts WHERE md5 = ?", (md5,))
            return cursor.fetchone() is not None

    def insert_post(self, data):
        """Insert a single post into the database."""
        md5 = data.get('md5')
        if md5 and self.post_exists(md5):
            return False  # Skip duplicate

        with self.lock:
            cursor = self.conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO posts (
                        post_id, md5, url, tags, tag_string, rating, file_ext,
                        source, created_at, score, fav_count, image_width, image_height,
                        file_size, description, regular_summary, individual_parts,
                        midjourney_style_summary, deviantart_commission_request,
                        brief_summary
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    data.get('id'),
                    md5,
                    data.get('url') or data.get('file_url'),
                    data.get('tags'),
                    data.get('tag_string'),
                    data.get('rating'),
                    data.get('file_ext'),
                    data.get('source'),
                    data.get('created_at'),
                    data.get('score'),
                    data.get('fav_count'),
                    data.get('image_width'),
                    data.get('image_height'),
                    data.get('file_size'),
                    data.get('description'),
                    data.get('regular_summary'),
                    data.get('individual_parts'),
                    data.get('midjourney_style_summary'),
                    data.get('deviantart_commission_request'),
                    data.get('brief_summary')
                ))
                self.conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False  # Duplicate

    def insert_batch(self, posts):
        """Insert multiple posts efficiently."""
        inserted = 0
        with self.lock:
            cursor = self.conn.cursor()
            for data in posts:
                md5 = data.get('md5')
                try:
                    cursor.execute("""
                        INSERT INTO posts (
                            post_id, md5, url, tags, tag_string, rating, file_ext,
                            source, created_at, score, fav_count, image_width, image_height,
                            file_size, description, regular_summary, individual_parts,
                            midjourney_style_summary, deviantart_commission_request,
                            brief_summary
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        data.get('id'),
                        md5,
                        data.get('url') or data.get('file_url'),
                        data.get('tags'),
                        data.get('tag_string'),
                        data.get('rating'),
                        data.get('file_ext'),
                        data.get('source'),
                        data.get('created_at'),
                        data.get('score'),
                        data.get('fav_count'),
                        data.get('image_width'),
                        data.get('image_height'),
                        data.get('file_size'),
                        data.get('description'),
                        data.get('regular_summary'),
                        data.get('individual_parts'),
                        data.get('midjourney_style_summary'),
                        data.get('deviantart_commission_request'),
                        data.get('brief_summary')
                    ))
                    inserted += 1
                except sqlite3.IntegrityError:
                    pass  # Skip duplicates
            self.conn.commit()
        return inserted

    def get_total_count(self):
        """Get total number of posts in database."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM posts")
            return cursor.fetchone()[0]

    def search_posts(self, query=None, limit=50000):
        """
        Search posts using e621-style search syntax.
        Supports: tags, -tags, ~tags (OR), rating:X, score:>X, favcount:>X,
                  type:X, width:X, height:X, filesize:X, id:X, wildcards
        """
        sql = "SELECT id, post_id, tag_string FROM posts WHERE 1=1"
        params = []

        if query:
            terms = self._parse_search_query(query)

            or_tags = []  # For ~tag OR grouping

            for term in terms:
                term_lower = term.lower()

                # OR tags (~tag)
                if term.startswith('~'):
                    or_tags.append(term[1:])
                    continue

                # Exclusion (-tag or -meta:value)
                if term.startswith('-'):
                    neg_term = term[1:]
                    if ':' in neg_term:
                        key, value = neg_term.split(':', 1)
                        key = key.lower()
                        if key == 'rating':
                            sql += " AND (rating IS NULL OR rating != ?)"
                            params.append(value.lower()[:1])  # s, q, or e
                        elif key == 'type':
                            sql += " AND (file_ext IS NULL OR file_ext != ?)"
                            params.append(value.lower())
                        else:
                            # Treat as tag exclusion
                            sql += " AND tag_string NOT LIKE ?"
                            params.append(f"%{neg_term}%")
                    else:
                        sql += " AND tag_string NOT LIKE ?"
                        params.append(f"%{neg_term}%")
                    continue

                # Meta searches (key:value)
                if ':' in term:
                    key, value = term.split(':', 1)
                    key = key.lower()

                    # Rating search
                    if key == 'rating':
                        sql += " AND rating = ?"
                        params.append(value.lower()[:1])  # s, q, or e

                    # Score search
                    elif key == 'score':
                        sql, params = self._add_numeric_condition(sql, params, 'score', value)

                    # Favorites search
                    elif key in ('favcount', 'fav_count', 'favorites'):
                        sql, params = self._add_numeric_condition(sql, params, 'fav_count', value)

                    # File type search
                    elif key in ('type', 'filetype', 'file_type'):
                        sql += " AND file_ext = ?"
                        params.append(value.lower())

                    # Width search
                    elif key == 'width':
                        sql, params = self._add_numeric_condition(sql, params, 'image_width', value)

                    # Height search
                    elif key == 'height':
                        sql, params = self._add_numeric_condition(sql, params, 'image_height', value)

                    # File size search (supports kb, mb)
                    elif key in ('filesize', 'file_size', 'size'):
                        size_bytes = self._parse_filesize(value)
                        if size_bytes is not None:
                            sql, params = self._add_numeric_condition(sql, params, 'file_size', size_bytes)

                    # ID search
                    elif key == 'id':
                        sql, params = self._add_numeric_condition(sql, params, 'post_id', value)

                    else:
                        # Unknown meta, treat as tag
                        sql += " AND tag_string LIKE ?"
                        params.append(f"%{term}%")

                # Wildcard tag search
                elif '*' in term:
                    # Convert wildcards to SQL LIKE pattern
                    pattern = term.replace('*', '%')
                    sql += " AND tag_string LIKE ?"
                    params.append(f"%{pattern}%")

                # Regular tag search
                else:
                    sql += " AND tag_string LIKE ?"
                    params.append(f"%{term}%")

            # Handle OR tags
            if or_tags:
                or_conditions = []
                for tag in or_tags:
                    or_conditions.append("tag_string LIKE ?")
                    params.append(f"%{tag}%")
                sql += f" AND ({' OR '.join(or_conditions)})"

        sql += " ORDER BY post_id DESC LIMIT ?"
        params.append(limit)

        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            return cursor.fetchall()

    def _parse_search_query(self, query):
        """Parse search query into individual terms, respecting quotes."""
        terms = []
        current_term = ""
        in_quotes = False

        for char in query:
            if char == '"':
                in_quotes = not in_quotes
            elif char == ' ' and not in_quotes:
                if current_term:
                    terms.append(current_term)
                    current_term = ""
            else:
                current_term += char

        if current_term:
            terms.append(current_term)

        return terms

    def _add_numeric_condition(self, sql, params, column, value):
        """Add a numeric comparison condition to SQL."""
        # Handle operators: >, <, >=, <=, =
        if isinstance(value, (int, float)):
            sql += f" AND {column} = ?"
            params.append(value)
        elif value.startswith('>='):
            sql += f" AND {column} >= ?"
            params.append(int(value[2:]))
        elif value.startswith('<='):
            sql += f" AND {column} <= ?"
            params.append(int(value[2:]))
        elif value.startswith('>'):
            sql += f" AND {column} > ?"
            params.append(int(value[1:]))
        elif value.startswith('<'):
            sql += f" AND {column} < ?"
            params.append(int(value[1:]))
        else:
            # Exact match
            try:
                sql += f" AND {column} = ?"
                params.append(int(value))
            except ValueError:
                pass
        return sql, params

    def _parse_filesize(self, value):
        """Parse filesize string (e.g., '1mb', '500kb') to bytes."""
        value = value.lower().strip()
        multipliers = {
            'b': 1,
            'kb': 1024,
            'mb': 1024 * 1024,
            'gb': 1024 * 1024 * 1024
        }

        for suffix, mult in multipliers.items():
            if value.endswith(suffix):
                try:
                    num = value[:-len(suffix)]
                    # Handle operators
                    if num.startswith('>='):
                        return f">={int(float(num[2:]) * mult)}"
                    elif num.startswith('<='):
                        return f"<={int(float(num[2:]) * mult)}"
                    elif num.startswith('>'):
                        return f">{int(float(num[1:]) * mult)}"
                    elif num.startswith('<'):
                        return f"<{int(float(num[1:]) * mult)}"
                    else:
                        return int(float(num) * mult)
                except ValueError:
                    return None

        # No suffix, try parsing with operator
        try:
            if value.startswith('>='):
                return f">={int(value[2:])}"
            elif value.startswith('<='):
                return f"<={int(value[2:])}"
            elif value.startswith('>'):
                return f">{int(value[1:])}"
            elif value.startswith('<'):
                return f"<{int(value[1:])}"
            else:
                return int(value)
        except ValueError:
            return None

    def get_post_by_id(self, db_id):
        """Get full post data by database ID."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM posts WHERE id = ?", (db_id,))
            return cursor.fetchone()

    def close(self):
        if self.conn:
            self.conn.close()


# ============================================================================
# HuggingFace Downloader - Streams directly to DB
# ============================================================================
class DatasetDownloader:
    """
    Downloads dataset from HuggingFace and streams directly to database.
    Files are processed in memory - no disk storage of JSONL files.
    """

    def __init__(self, db, dataset_id=HUGGINGFACE_DATASET):
        self.db = db
        self.dataset_id = dataset_id
        self.api = HfApi()
        self.cancel_flag = threading.Event()
        self.file_limit = TEST_FILE_LIMIT  # Can be changed before download

        # Progress tracking
        self.total_files = 0
        self.processed_files = 0
        self.current_file = ""
        self.total_posts_added = 0
        self.is_downloading = False

    def get_jsonl_files(self):
        """Get list of JSONL files in the dataset."""
        try:
            all_files = list_repo_files(self.dataset_id, repo_type="dataset")
            jsonl_files = [f for f in all_files if f.endswith(FILE_EXTENSION)]
            return sorted(jsonl_files)
        except Exception as e:
            print(f"Error listing files: {e}")
            return []

    def download_and_process(self, progress_callback=None, file_done_callback=None):
        """
        Download all JSONL files and process them directly to DB.
        """
        self.is_downloading = True
        self.cancel_flag.clear()

        jsonl_files = self.get_jsonl_files()

        # Apply file limit if set
        if self.file_limit:
            jsonl_files = jsonl_files[:self.file_limit]

        self.total_files = len(jsonl_files)
        self.processed_files = 0

        # Filter out already processed files
        files_to_process = [f for f in jsonl_files if not self.db.is_file_processed(f)]

        if not files_to_process:
            self.is_downloading = False
            if progress_callback:
                progress_callback(1.0, "All files already processed!")
            return

        for i, filename in enumerate(files_to_process):
            if self.cancel_flag.is_set():
                break

            self.current_file = filename
            if progress_callback:
                progress_callback(
                    i / len(files_to_process),
                    f"Downloading: {os.path.basename(filename)}"
                )

            try:
                # Get download URL
                url = hf_hub_url(
                    self.dataset_id,
                    filename,
                    repo_type="dataset"
                )

                # Stream download and process
                posts_added = self._stream_process_file(url, filename, progress_callback)
                self.total_posts_added += posts_added

                # Mark as processed
                self.db.mark_file_processed(filename)
                self.processed_files += 1

                if file_done_callback:
                    file_done_callback(filename, posts_added)

            except Exception as e:
                print(f"Error processing {filename}: {e}")

        self.is_downloading = False
        if progress_callback:
            progress_callback(1.0, f"Done! Added {self.total_posts_added} posts.")

    def _stream_process_file(self, url, filename, progress_callback=None):
        """
        Stream download a file and process it line by line.
        Never saves the full file to disk.
        """
        posts_added = 0
        batch = []
        batch_size = 1000

        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            # Get total size for progress
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0

            # Process line by line
            buffer = ""
            for chunk in response.iter_content(chunk_size=8192, decode_unicode=True):
                if self.cancel_flag.is_set():
                    break

                if chunk:
                    buffer += chunk
                    downloaded += len(chunk.encode('utf-8'))

                    # Process complete lines
                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        if line.strip():
                            try:
                                data = json.loads(line)
                                batch.append(data)

                                if len(batch) >= batch_size:
                                    added = self.db.insert_batch(batch)
                                    posts_added += added
                                    batch = []
                            except json.JSONDecodeError:
                                pass

                    # Update progress
                    if progress_callback and total_size > 0:
                        file_progress = downloaded / total_size
                        overall = (self.processed_files + file_progress) / self.total_files
                        progress_callback(
                            overall,
                            f"Processing: {os.path.basename(filename)} ({file_progress*100:.1f}%)"
                        )

            # Process remaining buffer
            if buffer.strip():
                try:
                    data = json.loads(buffer)
                    batch.append(data)
                except json.JSONDecodeError:
                    pass

            # Insert remaining batch
            if batch:
                added = self.db.insert_batch(batch)
                posts_added += added

        except Exception as e:
            print(f"Stream error: {e}")

        return posts_added

    def cancel(self):
        """Cancel ongoing download."""
        self.cancel_flag.set()


# ============================================================================
# Main Application
# ============================================================================
class JsonViewerApp:
    def __init__(self):
        self.db = DatasetDatabase()
        self.downloader = DatasetDownloader(self.db)

        self.post_cache = []
        self.selectable_items = []
        self.selected_index = None
        self.image_queue = Queue()
        self.current_texture = None
        self.refresh_list_flag = threading.Event()

        dpg.create_context()
        self.setup_ui()

    def setup_ui(self):
        # Texture registry
        with dpg.texture_registry(show=False):
            dpg.add_static_texture(width=1, height=1, default_value=[0,0,0,0], tag="empty_texture")

        # Download confirmation dialog
        with dpg.window(
            label="Download Dataset",
            modal=True,
            show=False,
            tag="download_dialog",
            width=500,
            height=280,
            pos=[350, 200]
        ):
            dpg.add_text("Would you like to download the dataset from HuggingFace?")
            dpg.add_spacer(height=10)
            dpg.add_text(f"Dataset: {HUGGINGFACE_DATASET}", color=[150, 150, 255])
            dpg.add_spacer(height=10)

            dpg.add_checkbox(
                label="Test Mode (download only 3 files)",
                default_value=True,
                tag="test_mode_checkbox"
            )
            dpg.add_spacer(height=5)
            dpg.add_text("Uncheck for full dataset (~20GB)", color=[255, 200, 100])

            dpg.add_spacer(height=10)
            dpg.add_text("Make sure you have enough disk space for the SQLite database.")
            dpg.add_spacer(height=20)

            with dpg.group(horizontal=True):
                dpg.add_button(label="Download", width=120, callback=self.start_download)
                dpg.add_button(label="Skip", width=120, callback=lambda: dpg.hide_item("download_dialog"))

        # Main window
        with dpg.window(tag="primary_window"):
            # Top bar with controls
            with dpg.group(horizontal=True):
                dpg.add_button(label="Download Dataset", callback=lambda: dpg.show_item("download_dialog"))
                dpg.add_button(label="Refresh List", callback=self.refresh_list)
                self.status = dpg.add_text("Initializing...")

            # Progress bars
            with dpg.group():
                dpg.add_text("Download Progress:", tag="dl_label", show=False)
                self.dl_progress = dpg.add_progress_bar(show=False, width=-1, tag="dl_progress")

            # Search help dialog
            with dpg.window(
                label="Search Help",
                modal=True,
                show=False,
                tag="search_help_dialog",
                width=600,
                height=500,
                pos=[300, 100]
            ):
                dpg.add_text("Search Syntax", color=[150, 200, 255])
                dpg.add_separator()
                dpg.add_spacer(height=5)

                help_text = """Basic Tag Search:
  tag1 tag2       - Posts must have ALL tags (AND)
  ~tag1 ~tag2     - Posts must have ANY of these tags (OR)
  -tag1           - Exclude posts with this tag (NOT)

Rating Search:
  rating:s        - Safe posts only
  rating:q        - Questionable posts only
  rating:e        - Explicit posts only
  -rating:e       - Exclude explicit posts

Score Search:
  score:>100      - Score greater than 100
  score:<50       - Score less than 50
  score:>=10      - Score greater or equal to 10
  score:100       - Exact score of 100

Favorites Search:
  favcount:>500   - More than 500 favorites
  favcount:<100   - Less than 100 favorites

File Type Search:
  type:png        - PNG files only
  type:jpg        - JPG files only
  type:gif        - GIF files only
  type:webm       - WEBM videos only
  -type:webm      - Exclude videos

Size Search:
  width:>1920     - Width greater than 1920
  height:>1080    - Height greater than 1080
  filesize:>1mb   - File size greater than 1MB

ID Search:
  id:>100000      - Posts with ID greater than 100000
  id:12345        - Exact post ID

Wildcard Search:
  cat*            - Tags starting with "cat"
  *cat            - Tags ending with "cat"
  *cat*           - Tags containing "cat"

Examples:
  canine -fox rating:s score:>50
  ~wolf ~dog rating:e favcount:>100
  dragon type:png width:>1920"""

                dpg.add_input_text(
                    default_value=help_text,
                    multiline=True,
                    readonly=True,
                    width=-1,
                    height=-50,
                    tag="help_text_display"
                )
                dpg.add_spacer(height=10)
                dpg.add_button(label="Close", width=100, callback=lambda: dpg.hide_item("search_help_dialog"))

            dpg.add_spacer(height=5)

            # Search bar
            with dpg.group(horizontal=True):
                dpg.add_input_text(
                    tag="search_input",
                    width=500,
                    hint="Search (e.g., canine -fox rating:s score:>50)",
                    on_enter=True,
                    callback=self.perform_search
                )
                dpg.add_button(label="Search", callback=self.perform_search)
                dpg.add_button(label="Clear", callback=self.clear_search)
                dpg.add_button(label="?", width=30, callback=lambda: dpg.show_item("search_help_dialog"))

            dpg.add_spacer(height=10)

            # Main content area - use child window to fill remaining space
            with dpg.child_window(width=-1, height=-1, border=False, tag="main_content"):
                with dpg.group(horizontal=True):
                    # Left panel - post list (fixed width, full height)
                    with dpg.child_window(width=350, height=-1, tag="list_panel"):
                        dpg.add_text("Posts:", tag="list_header")
                        # Scrollable container for post items
                        with dpg.child_window(width=-1, height=-1, border=True, tag="post_list_container"):
                            pass  # Items added dynamically

                    # Right panel - details (fills remaining width and height)
                    with dpg.child_window(width=-1, height=-1, tag="detail_panel"):
                        # Image display - takes about 50% of height
                        with dpg.child_window(height=-300, border=False, tag="image_container"):
                            self.img_widget = dpg.add_image("empty_texture", width=1, height=1, show=False)

                        dpg.add_separator()

                        # Tabbed details view - fills remaining space
                        with dpg.child_window(width=-1, height=-1, border=False, tag="tabs_container"):
                            with dpg.tab_bar(tag="detail_tabs"):
                                with dpg.tab(label="Tags"):
                                    dpg.add_button(label="Copy Tags", callback=lambda: self.copy_to_clipboard("tags_display"))
                                    self.tags_text = dpg.add_input_text(
                                        multiline=True,
                                        width=-1,
                                        height=-1,
                                        readonly=True,
                                        tag="tags_display"
                                    )

                                with dpg.tab(label="Summary"):
                                    dpg.add_button(label="Copy Summary", callback=lambda: self.copy_to_clipboard("summary_display"))
                                    self.summary_text = dpg.add_input_text(
                                        multiline=True,
                                        width=-1,
                                        height=-1,
                                        readonly=True,
                                        tag="summary_display"
                                    )

                                with dpg.tab(label="Parts"):
                                    dpg.add_button(label="Copy Parts", callback=lambda: self.copy_to_clipboard("parts_display"))
                                    self.parts_text = dpg.add_input_text(
                                        multiline=True,
                                        width=-1,
                                        height=-1,
                                        readonly=True,
                                        tag="parts_display"
                                    )

                                with dpg.tab(label="Midjourney"):
                                    dpg.add_button(label="Copy Midjourney", callback=lambda: self.copy_to_clipboard("mj_display"))
                                    self.mj_text = dpg.add_input_text(
                                        multiline=True,
                                        width=-1,
                                        height=-1,
                                        readonly=True,
                                        tag="mj_display"
                                    )

                                with dpg.tab(label="DeviantArt"):
                                    dpg.add_button(label="Copy DeviantArt", callback=lambda: self.copy_to_clipboard("da_display"))
                                    self.da_text = dpg.add_input_text(
                                        multiline=True,
                                        width=-1,
                                        height=-1,
                                        readonly=True,
                                        tag="da_display"
                                    )

                                with dpg.tab(label="Brief"):
                                    dpg.add_button(label="Copy Brief", callback=lambda: self.copy_to_clipboard("brief_display"))
                                    self.brief_text = dpg.add_input_text(
                                        multiline=True,
                                        width=-1,
                                        height=-1,
                                        readonly=True,
                                        tag="brief_display"
                                    )

        # Viewport setup
        dpg.create_viewport(title="Dataset Viewer v3 - HuggingFace Streamer", width=1300, height=900)
        dpg.set_primary_window("primary_window", True)
        dpg.setup_dearpygui()

    def copy_to_clipboard(self, tag):
        """Copy text from a text widget to clipboard."""
        try:
            text = dpg.get_value(tag)
            if text:
                dpg.set_clipboard_text(text)
                dpg.set_value(self.status, "Copied to clipboard!")
        except Exception as e:
            print(f"Copy error: {e}")

    def start_download(self):
        """Start the download process in a background thread."""
        dpg.hide_item("download_dialog")
        dpg.configure_item("dl_label", show=True)
        dpg.configure_item("dl_progress", show=True)

        # Check if test mode is enabled
        test_mode = dpg.get_value("test_mode_checkbox")
        file_limit = 3 if test_mode else None
        self.downloader.file_limit = file_limit

        def download_thread():
            self.downloader.download_and_process(
                progress_callback=self.update_download_progress,
                file_done_callback=self.on_file_done
            )
            # Final refresh
            dpg.configure_item("dl_label", show=False)
            dpg.configure_item("dl_progress", show=False)
            self.refresh_list_flag.set()

        threading.Thread(target=download_thread, daemon=True).start()

    def update_download_progress(self, progress, message):
        """Update download progress bar (called from download thread)."""
        try:
            dpg.set_value("dl_progress", progress)
            dpg.set_value(self.status, message)
        except:
            pass

    def on_file_done(self, filename, posts_added):
        """Called when a file finishes processing."""
        self.refresh_list_flag.set()

    def refresh_list(self):
        """Refresh the post list from database."""
        query = dpg.get_value("search_input")
        if query:
            self.load_list(query)
        else:
            self.load_list()

    def load_list(self, query=None):
        """Load posts from database into list."""
        dpg.set_value(self.status, "Loading...")

        rows = self.db.search_posts(query)
        self.post_cache = [(r['id'], r['post_id']) for r in rows]
        self.selectable_items = []  # Track selectable widgets
        self.selected_index = None

        # Clear existing items
        dpg.delete_item("post_list_container", children_only=True)

        # Add new items as selectables (limit display for performance)
        display_limit = 2000
        for i, (db_id, post_id) in enumerate(self.post_cache[:display_limit]):
            sel = dpg.add_selectable(
                label=f"ID: {post_id}",
                callback=self.on_select_item,
                user_data=i,
                parent="post_list_container",
                span_columns=True
            )
            self.selectable_items.append(sel)

        if len(self.post_cache) > display_limit:
            dpg.add_text(
                "... (search to filter)",
                parent="post_list_container",
                color=[150, 150, 150]
            )

        total = self.db.get_total_count()
        dpg.set_value("list_header", f"Posts: {len(self.post_cache)} shown / {total} total")
        dpg.set_value(self.status, f"Loaded {len(self.post_cache)} posts")

    def on_select_item(self, sender, app_data, user_data):
        """Handle selection from the dynamic list."""
        try:
            idx = user_data

            # Deselect previous item
            if self.selected_index is not None and self.selected_index < len(self.selectable_items):
                dpg.set_value(self.selectable_items[self.selected_index], False)

            # Select new item
            self.selected_index = idx
            dpg.set_value(sender, True)

            if idx < len(self.post_cache):
                db_id = self.post_cache[idx][0]
                post = self.db.get_post_by_id(db_id)
                if post:
                    self.show_details(post)
        except Exception as e:
            print(f"Select error: {e}")

    def perform_search(self):
        """Execute tag search."""
        query = dpg.get_value("search_input")
        self.load_list(query)

    def clear_search(self):
        """Clear search and show all."""
        dpg.set_value("search_input", "")
        self.load_list()

    def wrap_text(self, text, width=80):
        """Wrap text to specified width for better readability."""
        if not text:
            return ""

        lines = []
        for paragraph in text.split('\n'):
            if len(paragraph) <= width:
                lines.append(paragraph)
            else:
                words = paragraph.split(' ')
                current_line = ""
                for word in words:
                    if len(current_line) + len(word) + 1 <= width:
                        current_line += (" " if current_line else "") + word
                    else:
                        if current_line:
                            lines.append(current_line)
                        current_line = word
                if current_line:
                    lines.append(current_line)

        return '\n'.join(lines)

    def show_details(self, post):
        """Display post details in the tabs."""
        # Tags tab - one per line for easy reading
        tags = post['tag_string'] or post['tags'] or ""
        formatted_tags = tags.replace(" ", "\n") if tags else "(No tags)"
        dpg.set_value("tags_display", formatted_tags)

        # Summary tab - wrapped text
        summary = post['regular_summary'] or "(No summary available)"
        dpg.set_value("summary_display", self.wrap_text(summary))

        # Parts tab - wrapped text
        parts = post['individual_parts'] or "(No parts description available)"
        dpg.set_value("parts_display", self.wrap_text(parts))

        # Midjourney tab - wrapped text
        mj = post['midjourney_style_summary'] or "(No Midjourney prompt available)"
        dpg.set_value("mj_display", self.wrap_text(mj))

        # DeviantArt tab - wrapped text
        da = post['deviantart_commission_request'] or "(No DeviantArt commission text available)"
        dpg.set_value("da_display", self.wrap_text(da))

        # Brief tab - wrapped text
        brief = post['brief_summary'] or "(No brief summary available)"
        dpg.set_value("brief_display", self.wrap_text(brief))

        # Load image
        url = post['url']
        if url:
            threading.Thread(target=self.download_image, args=(url,), daemon=True).start()
        else:
            dpg.configure_item(self.img_widget, show=False)

    def download_image(self, url):
        """Download image using e621 API guidelines."""
        try:
            # e621 API requires proper User-Agent header
            headers = {
                'User-Agent': 'DatasetViewerApp/1.0 (by user on e621)'
            }
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                self.image_queue.put(resp.content)
            elif resp.status_code == 403:
                print(f"Image access forbidden (403) - may need authentication")
            elif resp.status_code == 503:
                print(f"Rate limited (503) - slow down requests")
        except Exception as e:
            print(f"Image download error: {e}")

    def update_image(self):
        """Process image queue and update display (called from main loop)."""
        try:
            data = self.image_queue.get_nowait()

            img = Image.open(io.BytesIO(data)).convert("RGBA")
            img.thumbnail((600, 400))
            w, h = img.size
            d_arr = np.array(img, dtype=np.float32) / 255.0

            tag = f"tex_{time.time()}"
            with dpg.texture_registry():
                dpg.add_static_texture(w, h, d_arr, tag=tag)

            # Clean up old texture
            if self.current_texture and dpg.does_item_exist(self.current_texture):
                dpg.delete_item(self.current_texture)

            self.current_texture = tag
            dpg.configure_item(self.img_widget, texture_tag=tag, width=w, height=h, show=True)

        except Empty:
            pass
        except Exception as e:
            print(f"Image display error: {e}")

    def run(self):
        """Main application loop."""
        dpg.show_viewport()

        # Initial load from database
        self.load_list()

        # Check if we should offer download
        if self.db.get_total_count() == 0:
            dpg.show_item("download_dialog")

        while dpg.is_dearpygui_running():
            # Update image if available
            self.update_image()

            # Check if we need to refresh the list
            if self.refresh_list_flag.is_set():
                self.refresh_list_flag.clear()
                self.refresh_list()

            dpg.render_dearpygui_frame()

        # Cleanup
        self.downloader.cancel()
        self.db.close()
        dpg.destroy_context()


# ============================================================================
# Entry Point
# ============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("Dataset Viewer v3")
    print("=" * 60)
    print(f"Database: {DB_PATH}")
    print(f"Dataset: {HUGGINGFACE_DATASET}")
    if TEST_FILE_LIMIT:
        print(f"TEST MODE: Limited to {TEST_FILE_LIMIT} files")
    print("=" * 60)

    app = JsonViewerApp()
    app.run()
