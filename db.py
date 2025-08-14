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
            error_message TEXT,
            dm_thread TEXT
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
    
    # Create index on source and dm_thread for DM filtering
    conn.execute('''
        CREATE INDEX IF NOT EXISTS idx_posts_source_dmthread 
        ON posts(source, dm_thread)
    ''')
    
    conn.commit()
    return conn


def is_downloaded(conn: sqlite3.Connection, shortcode: str) -> bool:
    """
    Check if a post with the given shortcode has already been successfully downloaded.
    
    Args:
        conn: Database connection
        shortcode: Instagram post shortcode
        
    Returns:
        bool: True if post was successfully downloaded, False otherwise
    """
    cursor = conn.execute(
        'SELECT 1 FROM posts WHERE shortcode = ? AND status = "success" LIMIT 1',
        (shortcode,)
    )
    return cursor.fetchone() is not None


def get_post(conn: sqlite3.Connection, shortcode: str) -> Optional[Dict]:
    """
    Get a post record from the database by shortcode.
    
    Args:
        conn: Database connection
        shortcode: Instagram post shortcode
        
    Returns:
        Optional[Dict]: Post record as dictionary or None if not found
    """
    cursor = conn.execute('SELECT * FROM posts WHERE shortcode = ?', (shortcode,))
    row = cursor.fetchone()
    if row:
        colnames = [desc[0] for desc in cursor.description]
        return dict(zip(colnames, row))
    return None


def record_download(conn: sqlite3.Connection, post: Dict) -> str:
    """
    Record a successful download in the database.
    
    Args:
        conn: Database connection
        post: Dictionary containing post information with keys:
              shortcode, url, description, original_owner, share_text,
              source, username, timestamp_ms, status (optional)
              
    Returns:
        str: "inserted", "duplicate", or "error"
    """
    try:
        # Use INSERT OR IGNORE to handle duplicates gracefully
        conn.execute('''
            INSERT OR IGNORE INTO posts 
            (shortcode, url, description, original_owner, share_text, 
             source, username, timestamp_ms, status, downloaded_at, dm_thread)
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
            post.get('status', 'success'),
            datetime.now().isoformat(),
            post.get('dm_thread')
        ))
        
        conn.commit()
        
        # Check if the insert actually happened (not ignored due to duplicate)
        if conn.total_changes > 0:
            return "inserted"
        else:
            return "duplicate"
        
    except sqlite3.Error as e:
        print(f"Database error recording download: {e}")
        return "error"


def record_failure(conn: sqlite3.Connection, post: Dict, error: str) -> str:
    """
    Record a failed download attempt in the database.
    
    Args:
        conn: Database connection
        post: Dictionary containing post information
        error: Error message describing the failure
        
    Returns:
        str: "inserted", "duplicate", or "error"
    """
    try:
        # Use INSERT OR IGNORE to handle duplicates gracefully
        conn.execute('''
            INSERT OR IGNORE INTO posts 
            (shortcode, url, description, original_owner, share_text, 
             source, username, timestamp_ms, status, error_message, downloaded_at, dm_thread)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            datetime.now().isoformat(),
            post.get('dm_thread')
        ))
        
        conn.commit()
        
        # Check if the insert actually happened (not ignored due to duplicate)
        if conn.total_changes > 0:
            return "inserted"
        else:
            return "duplicate"
        
    except sqlite3.Error as e:
        print(f"Database error recording failure: {e}")
        return "error"


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


def get_recent_download_timestamps(conn: sqlite3.Connection, since_epoch_seconds: float) -> list:
    """
    Return a list of UNIX timestamps (seconds) for successful downloads
    with created_at >= since_epoch_seconds. If not available, return [].
    
    Args:
        conn: Database connection
        since_epoch_seconds: Minimum timestamp to include (UNIX seconds)
        
    Returns:
        list: List of UNIX timestamps (seconds) for successful downloads
    """
    try:
        # Convert to datetime for SQLite comparison
        since_datetime = datetime.fromtimestamp(since_epoch_seconds).isoformat()
        
        cursor = conn.execute('''
            SELECT downloaded_at FROM posts 
            WHERE status = 'success' AND downloaded_at >= ?
            ORDER BY downloaded_at
        ''', (since_datetime,))
        
        timestamps = []
        for row in cursor.fetchall():
            # Convert SQLite datetime to UNIX timestamp
            dt = datetime.fromisoformat(row[0])
            timestamps.append(dt.timestamp())
        
        return timestamps
        
    except Exception as e:
        print(f"Error fetching recent download timestamps: {e}")
        return [] 