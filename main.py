import os
import sqlite3
import httpx
import urllib.parse
import base64
import hashlib
import json
from contextlib import contextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from Crypto.Cipher import AES

# ==========================================
# 1. SELF-HEALING DATABASE CONFIGURATION
# ==========================================
DATABASE_URL = os.environ.get("DATABASE_URL", "subaru_activity.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

IS_POSTGRES = DATABASE_URL.startswith("postgresql://")

def init_db():
    global IS_POSTGRES
    if IS_POSTGRES:
        try:
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watch_history (
                    user_id VARCHAR(64),
                    anime_id VARCHAR(255),
                    episode_num INT,
                    progress_seconds FLOAT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, anime_id)
                );
            """)
            conn.commit()
            conn.close()
            print("[Database] PostgreSQL Initialized successfully.")
            return
        except Exception as e:
            print(f"[Database Warning] PostgreSQL initialization failed ({e}). Falling back to SQLite.")
            IS_POSTGRES = False

    # SQLite Fallback Layer
    conn = sqlite3.connect("subaru_activity.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS watch_history (
            user_id TEXT,
            anime_id TEXT,
            episode_num INTEGER,
            progress_seconds REAL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, anime_id)
        );
    """)
    conn.commit()
    conn.close()
    print("[Database] SQLite Initialized successfully.")

@contextmanager
def get_db_cursor():
    global IS_POSTGRES
    if IS_POSTGRES:
        try:
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
            return
        except Exception:
            IS_POSTGRES = False

    conn = sqlite3.connect("subaru_activity.db")
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

def save_progress(user_id: str, anime_id: str, episode_num: int, progress: float):
    try:
        with get_db_cursor() as cursor:
            if IS_POSTGRES:
                cursor.execute("""
                    INSERT INTO watch_history (user_id, anime_id, episode_num, progress_seconds, updated_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (user_id, anime_id) 
                    DO UPDATE SET episode_num = EXCLUDED.episode_num, 
                                  progress_seconds = EXCLUDED.progress_seconds,
                                  updated_at = CURRENT_TIMESTAMP;
                """, (str(user_id), str(anime_id), int(episode_num), float(progress)))
            else:
                cursor.execute("""
                    INSERT INTO watch_history (user_id, anime_id, episode_num, progress_seconds, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id, anime_id) 
                    DO UPDATE SET episode_num=excluded.episode_num, 
                                  progress_seconds=excluded.progress_seconds,
                                  updated_at=CURRENT_TIMESTAMP;
                """, (str(user_id), str(anime_id), int(episode_num), float(progress)))
    except Exception as e:
        print(f"[Database Error] Failed to save record: {e}")

def get_progress(user_id: str, anime_id: str):
    try:
        with get_db_cursor() as cursor:
            if IS_POSTGRES:
                cursor.execute("SELECT episode_num, progress_seconds FROM watch_history WHERE user_id = %s AND anime_id = %s", (str(user_id), str(anime_id)))
            else:
                cursor.execute("SELECT episode_num, progress_seconds FROM watch_history WHERE user_id = ? AND anime_id = ?", (str(user_id), str(anime_id)))
            row = cursor.fetchone()
            if row:
                return {"episode_num": row[0], "progress_seconds": row[1]}
    except Exception:
        pass
    return {"episode_num": 1, "progress_seconds": 0.0}

# ==========================================
# 2. DEFENSIVE PROVIDER ARCHITECTURE
# ==========================================
class BaseProvider:
    async def search(self, query: str, client: httpx.AsyncClient): pass
    async def info(self, anime_id: str, client: httpx.AsyncClient): pass
    async def stream(self, episode_id: str, client: httpx.AsyncClient): pass

class DirectExtractorProvider(BaseProvider):
    API_URL = "https://api-consumet.vercel.app/anime/zoro"
    
    async def search(self, query: str, client: httpx.AsyncClient):
        try:
            res = await client.get(f"{self.API_URL}/{urllib.parse.quote(query)}", timeout=8.0)
            if res.status_code == 200:
                data = res.json()
                results = []
                for item in data.get("results", []):
                    results.append({
                        "id": f"direct|{item.get('id', '')}",
                        "title": item.get('title', 'Unknown Title'),
                        "image": item.get('image', ''),
                        "type": item.get('type', 'TV')
                    })
                return results
        except Exception as e:
            print(f"[Provider Error] Direct Extractor Search Failed: {e}")
        return None

    async def info(self, anime_id: str, client: httpx.AsyncClient):
        try:
            clean_id = anime_id.replace("direct|", "")
            res = await client.get(f"{self.API_URL}/info?id={clean_id}", timeout=10.0)
            if res.status_code == 200:
                data = res.json()
                episodes = []
                for ep in data.get("episodes", []):
                    episodes.append({
                        "id": f"direct|{ep.get('id', '')}",
                        "number": ep.get('number', 1)
                    })
                return {"provider": "RAW DECRYPTION ENGINE", "episodes": episodes}
        except Exception as e:
            print(f"[Provider Error] Direct Extractor Info Failed: {e}")
        return None

    async def stream(self, episode_id: str, client: httpx.AsyncClient):
        try:
            clean_id = episode_id.replace("direct|", "")
            res = await client.get(f"{self.API_URL}/watch?episodeId={clean_id}", timeout=10.0)
            if res.status_code == 200:
                return res.json()
        except Exception as e:
            print(f"[Provider Error] Direct Extractor Stream Failed: {e}")
        return None

class AMVSTRProvider(BaseProvider):
    BASE_URL = "https://api.amvstr.me/api/v2"
    
    async def search(self, query: str, client: httpx.AsyncClient):
        try:
            res = await client.get(f"{self.BASE_URL}/search?q={urllib.parse.quote(query)}", timeout=8.0)
            if res.status_code == 200:
                data = res.json()
                results = []
                for item in data.get("results", []):
                    title_obj = item.get('title', {})
                    title = "Unknown"
                    if isinstance(title_obj, dict):
                        title = title_obj.get('english') or title_obj.get('romaji') or title_obj.get('native') or "Unknown"
                    
                    cover_obj = item.get('coverImage', {})
                    image = ""
                    if isinstance(cover_obj, dict):
                        image = cover_obj.get('large') or cover_obj.get('extraLarge') or ""

                    results.append({
                        "id": f"amvstr|{item.get('id', '')}",
                        "title": title,
                        "image": image,
                        "type": item.get('format', 'TV')
                    })
                return results
        except Exception as e:
            print(f"[Provider Error] AMVSTR Search Failed: {e}")
        return None

    async def info(self, anime_id: str, client: httpx.AsyncClient):
        try:
            clean_id = anime_id.replace("amvstr|", "")
            res = await client.get(f"{self.BASE_URL}/info/{clean_id}", timeout=10.0)
            if res.status_code == 200:
                data = res.json()
                episodes = []
                for ep in data.get("episodes", []):
                    episodes.append({
                        "id": f"amvstr|{ep.get('id', '')}",
                        "number": ep.get('number', 1)
                    })
                return {"provider": "AMVSTR SERVER", "episodes": episodes}
        except Exception as e:
            print(f"[Provider Error] AMVSTR Info Failed: {e}")
        return None

    async def stream(self, episode_id: str, client: httpx.AsyncClient):
        try:
            clean_id = episode_id.replace("amvstr|", "")
            res = await client.get(f"{self.BASE_URL}/stream/{clean_id}", timeout=10.0)
            if res.status_code == 200:
                data = res.json()
                url = data.get("stream", {}).get("multi", {}).get("main", {}).get("url")
                if url:
                    return {"sources": [{"url": url, "quality": "default"}]}
        except Exception as e:
            print(f"[Provider Error] AMVSTR Stream Failed: {e}")
        return None

class ProviderManager:
    def __init__(self):
        self.providers = [DirectExtractorProvider(), AMVSTRProvider()]

    async def search_all(self, query: str):
        async with httpx.AsyncClient(timeout=10.0) as client:
            for provider in self.providers:
                try:
                    results = await provider.search(query, client)
                    if results and len(results) > 0:
                        return {"results": results}
                except Exception:
                    continue
        return {"results": []}

    async def get_info(self, composite_id: str):
        async with httpx.AsyncClient(timeout=15.0) as client:
            for provider in self.providers:
                try:
                    if composite_id.startswith("direct|") and isinstance(provider, DirectExtractorProvider):
                        data = await provider.info(composite_id, client)
                        if data: return data
                    elif composite_id.startswith("amvstr|") and isinstance(provider, AMVSTRProvider):
                        data = await provider.info(composite_id, client)
                        if data: return data
                except Exception:
                    continue
        return {"episodes": [], "provider": "None"}

    async def get_stream(self, composite_id: str):
        async with httpx.AsyncClient(timeout=15.0) as client:
            for provider in self.providers:
                try:
                    if composite_id.startswith("direct|") and isinstance(provider, DirectExtractorProvider):
                        data = await provider.stream(composite_id, client)
                        if data: return data
                    elif composite_id.startswith("amvstr|") and isinstance(provider, AMVSTRProvider):
                        data = await provider.stream(composite_id, client)
                        if data: return data
                except Exception:
                    continue
        raise HTTPException(status_code=502, detail="All extraction nodes failed to parse streaming manifest links.")

extension_manager = ProviderManager()

# ==========================================
# 3. FASTAPI MOUNT PIECES
# ==========================================
app = FastAPI(title="Streaming Activity Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    init_db()

class ProgressPayload(BaseModel):
    user_id: str
    anime_id: str
    episode_num: int
    progress_seconds: float

@app.get("/")
async def serve_frontend():
    try:
        with open("index.html", "r", encoding="utf-8") as file:
            return HTMLResponse(content=file.read(), status_code=200)
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Error: index.html missing!</h1>", status_code=404)

@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "manager_active": True, "postgres_active": IS_POSTGRES}

@app.get("/api/search")
async def search_anime(q: str):
    if not q or q == "ping": return {"status": "active"}
    return await extension_manager.search_all(q)

@app.get("/api/anime/{anime_id:path}")
async def get_anime_details(anime_id: str):
    return await extension_manager.get_info(anime_id)

@app.get("/api/stream")
async def get_stream_urls(episode_id: str):
    return await extension_manager.get_stream(episode_id)

@app.post("/api/history/save")
async def save_user_history(data: ProgressPayload):
    save_progress(data.user_id, data.anime_id, data.episode_num, data.progress_seconds)
    return {"status": "saved"}

@app.get("/api/history/get")
async def get_user_history(user_id: str, anime_id: str):
    return get_progress(user_id, anime_id)

# ==========================================
# 4. WEBSOCKET SYNC LAYER
# ==========================================
class RoomManager:
    def __init__(self):
        self.active_rooms: dict[str, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, room_id: str):
        await websocket.accept()
        if room_id not in self.active_rooms: self.active_rooms[room_id] = []
        self.active_rooms[room_id].append(websocket)

    def disconnect(self, websocket: WebSocket, room_id: str):
        if room_id in self.active_rooms:
            if websocket in self.active_rooms[room_id]: self.active_rooms[room_id].remove(websocket)
            if not self.active_rooms[room_id]: del self.active_rooms[room_id]

    async def broadcast(self, message: dict, room_id: str, sender: WebSocket):
        if room_id in self.active_rooms:
            for connection in self.active_rooms[room_id]:
                if connection != sender:
                    try: await connection.send_json(message)
                    except Exception: pass 

manager = RoomManager()

@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    await manager.connect(websocket, room_id)
    try:
        while True:
            data = await websocket.receive_json()
            await manager.broadcast(data, room_id, sender=websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket, room_id)
    except Exception:
        manager.disconnect(websocket, room_id)
