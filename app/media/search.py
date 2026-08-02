from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

@dataclass
class MediaEntry:
    url: str
    root: str
    path: str
    filename: str
    size: str | None
    modified: str | None

    @property
    def display_text(self) -> str:
        # This is what the user sees in the TUI
        return f"{self.filename}  [{self.root}]  {self.path}"

def search_media(
    conn,
    pattern: str,
    limit: int = 50,
) -> List[Tuple[MediaEntry, float]]:
    """
    Search media using the FTS5 virtual table for instant full-text search.
    """
    pattern = pattern.strip()
    if not pattern:
        return []

    # Prepare FTS5 prefix match (e.g. 'breaking bad' -> '"breaking" "bad" *')
    # We strip out quotes from the user input to avoid syntax errors,
    # then wrap each word in quotes to handle special characters, and add a trailing *.
    clean_pattern = pattern.replace('"', '').replace("'", '')
    words = clean_pattern.split()
    if not words:
        return []
    
    fts_pattern = " ".join(f'"{word}"' for word in words) + "*"

    cur = conn.cursor()
    
    # We negate rank so higher score = better match for the UI display
    query = """
        SELECT m.url, m.root, m.path, m.filename, m.size, m.modified, -media_fts.rank as score
        FROM media_fts
        JOIN media m ON m.url = media_fts.url
        WHERE media_fts MATCH ?
        ORDER BY media_fts.rank
        LIMIT ?
    """
    
    try:
        cur.execute(query, (fts_pattern, limit))
        rows = cur.fetchall()
    except Exception:
        # Fallback to LIKE if FTS fails (e.g. malformed query)
        like_pattern = f"%{pattern}%"
        query_fallback = """
            SELECT url, root, path, filename, size, modified, 1.0 as score
            FROM media
            WHERE filename LIKE ? OR path LIKE ?
            LIMIT ?
        """
        cur.execute(query_fallback, (like_pattern, like_pattern, limit))
        rows = cur.fetchall()

    results = []
    for r in rows:
        entry = MediaEntry(
            url=r["url"],
            root=r["root"],
            path=r["path"],
            filename=r["filename"],
            size=r["size"],
            modified=r["modified"],
        )
        # Add 10 to score just to make it a nice positive float like the old rapidfuzz score (purely aesthetic)
        results.append((entry, float(r["score"]) + 10.0))
        
    return results
