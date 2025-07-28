import sqlite3
import os
from datetime import datetime
from typing import Dict, Optional


def init_db(db_path: str) -> sqlite3.Connection:
    """
    Initialize SQLite database and create the posts table.
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        sqlite3.Connection: Database connection
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Connect to database (creates it if it doesn't exist)
    conn = sqlite3.connect(db_path)
    
    # Create the posts table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shortcode TEXT UNIQUE NOT NULL,
            url TEXT NOT NULL,
            description TEXT,
            original_owner TEXT,
            share_text TEXT,
            source TEXT, -- 'dm', 'saved', 'liked', 'profile'
            username TEXT,
            timestamp_ms INTEGER,
            downloaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'success', -- 'success', 'failed', 'skipped'
            error_message TEXT
        )
    ''')
    
    # Create index on shortcode for faster lookups
    conn.execute('''
        CREATE INDEX IF NOT EXISTS idx_posts_shortcode 
        ON posts(shortcode)
    ''')
    
    # Create index on source for filtering
    conn.execute('''
        CREATE INDEX IF NOT EXISTS idx_posts_source 
        ON posts(source)
    ''')
    
    conn.commit()
    return conn


def is_downloaded(conn: sqlite3.Connection, shortcode: str) -> bool:
    """
    Check if a post with the given shortcode has already been downloaded.
    
    Args:
        conn: Database connection
        shortcode: Instagram post shortcode
        
    Returns:
        bool: True if post is already recorded, False otherwise
    """
    cursor = conn.execute(
        'SELECT 1 FROM posts WHERE shortcode = ? LIMIT 1',
        (shortcode,)
    )
    return cursor.fetchone() is not None


def record_download(conn: sqlite3.Connection, post: Dict) -> bool:
    """
    Record a successful download in the database.
    
    Args:
        conn: Database connection
        post: Dictionary containing post information with keys:
              shortcode, url, description, original_owner, share_text,
              source, username, timestamp_ms, status (optional)
              
    Returns:
        bool: True if record was inserted, False if duplicate
    """
    try:
        # Use INSERT OR IGNORE to handle duplicates gracefully
        conn.execute('''
            INSERT OR IGNORE INTO posts 
            (shortcode, url, description, original_owner, share_text, 
             source, username, timestamp_ms, status, downloaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            post.get('shortcode'),
            post.get('url'),
            post.get('description'),
            post.get('original_owner'),
            post.get('share_text'),
            post.get('source'),
            post.get('username'),
            post.get('timestamp_ms'),
            post.get('status', 'success'),
            datetime.now().isoformat()
        ))
        
        conn.commit()
        
        # Check if the insert actually happened (not ignored due to duplicate)
        return conn.total_changes > 0
        
    except sqlite3.Error as e:
        print(f"Database error recording download: {e}")
        return False


def record_failure(conn: sqlite3.Connection, post: Dict, error: str) -> bool:
    """
    Record a failed download attempt in the database.
    
    Args:
        conn: Database connection
        post: Dictionary containing post information
        error: Error message describing the failure
        
    Returns:
        bool: True if record was inserted, False if duplicate
    """
    try:
        # Use INSERT OR IGNORE to handle duplicates gracefully
        conn.execute('''
            INSERT OR IGNORE INTO posts 
            (shortcode, url, description, original_owner, share_text, 
             source, username, timestamp_ms, status, error_message, downloaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            post.get('shortcode'),
            post.get('url'),
            post.get('description'),
            post.get('original_owner'),
            post.get('share_text'),
            post.get('source'),
            post.get('username'),
            post.get('timestamp_ms'),
            'failed',
            error,
            datetime.now().isoformat()
        ))
        
        conn.commit()
        
        # Check if the insert actually happened (not ignored due to duplicate)
        return conn.total_changes > 0
        
    except sqlite3.Error as e:
        print(f"Database error recording failure: {e}")
        return False


def get_download_stats(conn: sqlite3.Connection) -> Dict[str, int]:
    """
    Get download statistics from the database.
    
    Args:
        conn: Database connection
        
    Returns:
        Dict containing counts for each status and source
    """
    stats = {}
    
    # Count by status
    cursor = conn.execute('''
        SELECT status, COUNT(*) FROM posts GROUP BY status
    ''')
    for status, count in cursor.fetchall():
        stats[f'status_{status}'] = count
    
    # Count by source
    cursor = conn.execute('''
        SELECT source, COUNT(*) FROM posts GROUP BY source
    ''')
    for source, count in cursor.fetchall():
        stats[f'source_{source}'] = count
    
    # Total count
    cursor = conn.execute('SELECT COUNT(*) FROM posts')
    stats['total'] = cursor.fetchone()[0]
    
    return stats


def close_db(conn: sqlite3.Connection):
    """
    Safely close the database connection.
    
    Args:
        conn: Database connection to close
    """
    if conn:
        conn.close() 