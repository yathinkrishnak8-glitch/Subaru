import os
import sqlite3
from contextlib import contextmanager

# Render Free Tier Fix: Detect if a remote Postgres URL exists, otherwise fallback to local SQLite
DATABASE_URL = os.environ.get("DATABASE_URL", "anime_activity.db")
IS_POSTGRES = DATABASE_URL.startswith("postgres://") or DATABASE_URL.startswith("postgresql://")

def init_db():
    """Initializes the database schema with error-resistant constraints."""
    if IS_POSTGRES:
        # If you link a free Supabase/Neon instance via environment variables
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS watch_history (
                user_id VARCHAR(64),
                anime_id VARCHAR(255),
                episode_num INT,
                sub_or_dub VARCHAR(10),
                progress_seconds FLOAT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, anime_id)
            );
        """)
    else:
        # Local testing fallback
        conn = sqlite3.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS watch_history (
                user_id TEXT,
                anime_id TEXT,
                episode_num INTEGER,
                sub_or_dub TEXT,
                progress_seconds REAL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, anime_id)
            );
        """)
    conn.commit()
    conn.close()

@contextmanager
def get_db_cursor():
    """Context manager to ensure safe connection closing and prevent locks/leaks."""
    if IS_POSTGRES:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()
    else:
        conn = sqlite3.connect(DATABASE_URL)
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

def save_progress(user_id: str, anime_id: str, episode_num: int, sub_or_dub: str, progress: float):
    """Saves or updates user watch progress using an upsert mechanism."""
    try:
        with get_db_cursor() as cursor:
            if IS_POSTGRES:
                cursor.execute("""
                    INSERT INTO watch_history (user_id, anime_id, episode_num, sub_or_dub, progress_seconds, updated_at)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (user_id, anime_id) 
                    DO UPDATE SET episode_num = EXCLUDED.episode_num, 
                                  sub_or_dub = EXCLUDED.sub_or_dub, 
                                  progress_seconds = EXCLUDED.progress_seconds,
                                  updated_at = CURRENT_TIMESTAMP;
                """, (user_id, anime_id, episode_num, sub_or_dub, progress))
            else:
                cursor.execute("""
                    INSERT INTO watch_history (user_id, anime_id, episode_num, sub_or_dub, progress_seconds, updated_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id, anime_id) 
                    DO UPDATE SET episode_num=excluded.episode_num, 
                                  sub_or_dub=excluded.sub_or_dub, 
                                  progress_seconds=excluded.progress_seconds,
                                  updated_at=CURRENT_TIMESTAMP;
                """, (user_id, anime_id, episode_num, sub_or_dub, progress))
    except Exception as e:
        print(f"[Database Error] Failed to save progress: {e}")

def get_progress(user_id: str, anime_id: str):
    """Retrieves saved playback data or returns default placeholders."""
    try:
        with get_db_cursor() as cursor:
            if IS_POSTGRES:
                cursor.execute("SELECT episode_num, sub_or_dub, progress_seconds FROM watch_history WHERE user_id = %s AND anime_id = %s", (user_id, anime_id))
            else:
                cursor.execute("SELECT episode_num, sub_or_dub, progress_seconds FROM watch_history WHERE user_id = ? AND anime_id = ?", (user_id, anime_id))
            row = cursor.fetchone()
            if row:
                return {"episode_num": row[0], "sub_or_dub": row[1], "progress_seconds": row[2]}
    except Exception as e:
        print(f"[Database Error] Failed to fetch progress: {e}")
    return {"episode_num": 1, "sub_or_dub": "sub", "progress_seconds": 0.0}
