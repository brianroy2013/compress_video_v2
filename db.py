"""
Database module for video compression tracking.

Local SQLite database on each machine. Multi-machine coordination
is handled separately via NAS claim files (see claim.py).
"""

import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / 'compress.db'


def get_connection(db_path=None):
    """Get a SQLite connection with WAL mode and foreign keys."""
    path = db_path or DB_PATH
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA foreign_keys=ON')
    return conn


def init_db(conn=None):
    """Create tables and indexes if they don't exist."""
    close = False
    if conn is None:
        conn = get_connection()
        close = True

    conn.executescript('''
        CREATE TABLE IF NOT EXISTS videos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            path            TEXT NOT NULL UNIQUE,
            filename        TEXT NOT NULL,
            size_bytes      INTEGER NOT NULL,
            codec           TEXT,
            width           INTEGER,
            height          INTEGER,
            bitrate         INTEGER,
            duration_sec    REAL,

            -- Strategy decision
            action          TEXT,
            target_cq       INTEGER,
            downscale_1080p INTEGER DEFAULT 0,

            -- Processing state
            status          TEXT NOT NULL DEFAULT 'pending',
            claimed_by      TEXT,
            claimed_at      TEXT,
            finished_at     TEXT,

            -- Results
            output_path     TEXT,
            output_size     INTEGER,
            savings_pct     REAL,
            backup_path     TEXT,
            error_message   TEXT,
            scanned_at      TEXT NOT NULL,
            updated_at      TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_status ON videos(status);
        CREATE INDEX IF NOT EXISTS idx_action ON videos(action);
        CREATE INDEX IF NOT EXISTS idx_path ON videos(path);

        CREATE TABLE IF NOT EXISTS processing_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id    INTEGER REFERENCES videos(id),
            machine     TEXT NOT NULL,
            event       TEXT NOT NULL,
            details     TEXT,
            timestamp   TEXT NOT NULL DEFAULT (datetime('now'))
        );
    ''')
    conn.commit()

    if close:
        conn.close()


def upsert_video(conn, *, path, filename, size_bytes, codec=None,
                 width=None, height=None, bitrate=None, duration_sec=None,
                 action=None, target_cq=None, downscale_1080p=0,
                 status='pending', scanned_at=None):
    """Insert or update a video record. Returns the row id."""
    now = scanned_at or datetime.now().isoformat()
    cur = conn.execute('''
        INSERT INTO videos (
            path, filename, size_bytes, codec, width, height, bitrate,
            duration_sec, action, target_cq, downscale_1080p, status,
            scanned_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            filename=excluded.filename,
            size_bytes=excluded.size_bytes,
            codec=excluded.codec,
            width=excluded.width,
            height=excluded.height,
            bitrate=excluded.bitrate,
            duration_sec=excluded.duration_sec,
            action=excluded.action,
            target_cq=excluded.target_cq,
            downscale_1080p=excluded.downscale_1080p,
            updated_at=excluded.updated_at
    ''', (path, filename, size_bytes, codec, width, height, bitrate,
          duration_sec, action, target_cq, downscale_1080p, status,
          now, now))
    conn.commit()
    return cur.lastrowid


def get_pending_encodes(conn, limit=None):
    """Get videos that need encoding, ordered by size descending."""
    sql = '''
        SELECT * FROM videos
        WHERE action = 'encode' AND status = 'pending'
        ORDER BY size_bytes DESC
    '''
    if limit:
        sql += f' LIMIT {int(limit)}'
    return conn.execute(sql).fetchall()


def get_video_by_path(conn, path):
    """Look up a video by its file path."""
    return conn.execute(
        'SELECT * FROM videos WHERE path = ?', (str(path),)
    ).fetchone()


def update_status(conn, video_id, status, **kwargs):
    """Update a video's status and optional fields."""
    sets = ['status = ?', 'updated_at = ?']
    vals = [status, datetime.now().isoformat()]

    for col in ('claimed_by', 'claimed_at', 'finished_at', 'output_path',
                'output_size', 'savings_pct', 'backup_path', 'error_message',
                'action', 'target_cq', 'downscale_1080p',
                'codec', 'width', 'height', 'bitrate', 'duration_sec',
                'size_bytes'):
        if col in kwargs:
            sets.append(f'{col} = ?')
            vals.append(kwargs[col])

    vals.append(video_id)
    conn.execute(
        f'UPDATE videos SET {", ".join(sets)} WHERE id = ?', vals
    )
    conn.commit()


def log_event(conn, video_id, machine, event, details=None):
    """Write an entry to the processing log."""
    conn.execute(
        'INSERT INTO processing_log (video_id, machine, event, details) VALUES (?, ?, ?, ?)',
        (video_id, machine, event, details)
    )
    conn.commit()


def get_status_summary(conn):
    """Return counts grouped by status and action."""
    rows = conn.execute('''
        SELECT status, action, COUNT(*) as cnt,
               SUM(size_bytes) as total_size,
               SUM(CASE WHEN output_size IS NOT NULL THEN output_size ELSE 0 END) as total_output,
               SUM(CASE WHEN savings_pct IS NOT NULL THEN savings_pct ELSE 0 END) as total_savings_pct
        FROM videos
        GROUP BY status, action
        ORDER BY status, action
    ''').fetchall()
    return rows


def get_needs_remediation(conn):
    """Get videos marked for remediation (badly compressed)."""
    return conn.execute(
        "SELECT * FROM videos WHERE status = 'needs_remediation' ORDER BY size_bytes DESC"
    ).fetchall()


def count_by_status(conn):
    """Simple status counts."""
    rows = conn.execute(
        'SELECT status, COUNT(*) as cnt FROM videos GROUP BY status'
    ).fetchall()
    return {row['status']: row['cnt'] for row in rows}


def count_by_action(conn):
    """Simple action counts."""
    rows = conn.execute(
        'SELECT action, COUNT(*) as cnt FROM videos GROUP BY action'
    ).fetchall()
    return {row['action']: row['cnt'] for row in rows}
