import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "news.db"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id    TEXT NOT NULL,
                category     TEXT NOT NULL,
                title        TEXT NOT NULL,
                description  TEXT,
                url          TEXT,
                image        TEXT,
                source_name  TEXT,
                published_at TEXT,
                fetched_at   TEXT NOT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_source_fetched ON articles(source_id, fetched_at DESC)")
    print("  DB ready:", DB_PATH)

def save_article(source_id, category, article):
    now = datetime.now(timezone.utc).isoformat()
    title = article.get("title", "")
    if not title:
        return False
    with _conn() as c:
        # Skip if same title already stored for this source today
        existing = c.execute(
            "SELECT id FROM articles WHERE source_id=? AND title=? AND fetched_at >= date('now')",
            (source_id, title)
        ).fetchone()
        if existing:
            return False
        c.execute("""
            INSERT INTO articles (source_id, category, title, description, url, image, source_name, published_at, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            source_id, category, title,
            article.get("description") or article.get("content"),
            article.get("url"),
            article.get("image") or article.get("urlToImage"),
            article.get("source_name") or (article.get("source") or {}).get("name"),
            article.get("publishedAt") or article.get("published_at"),
            now
        ))
    return True

def get_latest_by_id(source_id):
    with _conn() as c:
        row = c.execute(
            """SELECT source_id, category, title, description, url, image,
                      source_name, published_at, fetched_at
               FROM articles WHERE source_id=?
               ORDER BY fetched_at DESC LIMIT 1""",
            (source_id,)
        ).fetchone()
        return dict(row) if row else None

def get_today(source_id):
    with _conn() as c:
        rows = c.execute(
            """SELECT source_id, category, title, description, url, image,
                      source_name, published_at, fetched_at
               FROM articles
               WHERE source_id=? AND fetched_at >= date('now')
               ORDER BY fetched_at DESC""",
            (source_id,)
        ).fetchall()
        return [dict(r) for r in rows]

def get_stats():
    with _conn() as c:
        total = c.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        today = c.execute("SELECT COUNT(*) FROM articles WHERE fetched_at >= date('now')").fetchone()[0]
        sources = c.execute("SELECT COUNT(DISTINCT source_id) FROM articles").fetchone()[0]
        return {"total": total, "today": today, "sources": sources}
