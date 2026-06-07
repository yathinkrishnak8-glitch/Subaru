import os
import sqlite3
import httpx
import urllib.parse
from contextlib import contextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ==========================================
# 1. DATABASE CONFIGURATION
# ==========================================
DATABASE_URL = os.environ.get("DATABASE_URL", "subaru_activity.db")
IS_POSTGRES = DATABASE_URL.startswith("postgres://") or DATABASE_URL.startswith("postgresql://")

def init_db():
    if IS_POSTGRES:
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
    else:
        conn = sqlite3.connect(DATABASE_URL)
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

@contextmanager
def get_db_cursor():
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
                """, (user_id, anime_id, episode_num, progress))
            else:
                cursor.execute("""
                    INSERT INTO watch_history (user_id, anime_id, episode_num, progress_seconds, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id, anime_id) 
                    DO UPDATE SET episode_num=excluded.episode_num, 
                                  progress_seconds=excluded.progress_seconds,
                                  updated_at=CURRENT_TIMESTAMP;
                """, (user_id, anime_id, episode_num, progress))
    except Exception as e:
        print(f"[Database Error] Failed to save record: {e}")

def get_progress(user_id: str, anime_id: str):
    try:
        with get_db_cursor() as cursor:
            if IS_POSTGRES:
                cursor.execute("SELECT episode_num, progress_seconds FROM watch_history WHERE user_id = %s AND anime_id = %s", (user_id, anime_id))
            else:
                cursor.execute("SELECT episode_num, progress_seconds FROM watch_history WHERE user_id = ? AND anime_id = ?", (user_id, anime_id))
            row = cursor.fetchone()
            if row:
                return {"episode_num": row[0], "progress_seconds": row[1]}
    except Exception:
        pass
    return {"episode_num": 1, "progress_seconds": 0.0}

# ==========================================
# 2. PYTHON EXTENSION MANAGER CORE
# ==========================================
class BaseExtension:
    """Base class for Python-native streaming providers"""
    async def search(self, query: str, client: httpx.AsyncClient): pass
    async def info(self, anime_id: str, client: httpx.AsyncClient): pass
    async def stream(self, episode_id: str, client: httpx.AsyncClient): pass

class AMVSTRExtension(BaseExtension):
    """Primary Extension: Uses the AMVSTR API (High reliability, Cloudflare bypassed)"""
    BASE_URL = "https://api.amvstr.me/api/v2"
    
    async def search(self, query: str, client: httpx.AsyncClient):
        res = await client.get(f"{self.BASE_URL}/search?q={urllib.parse.quote(query)}")
        if res.status_code == 200:
            data = res.json()
            results = []
            for item in data.get("results", []):
                results.append({
                    "id": f"amvstr|{item.get('id')}", # Tag ID with provider for routing later
                    "title": item.get("title", {}).get("english") or item.get("title", {}).get("romaji") or "Unknown",
                    "image": item.get("coverImage", {}).get("extraLarge") or item.get("coverImage", {}).get("large"),
                    "type": item.get("format", "TV")
                })
            return results
        return None

    async def info(self, anime_id: str, client: httpx.AsyncClient):
        clean_id = anime_id.replace("amvstr|", "")
        res = await client.get(f"{self.BASE_URL}/info/{clean_id}")
        if res.status_code == 200:
            data = res.json()
            episodes = []
            for ep in data.get("episodes", []):
                episodes.append({
                    "id": f"amvstr|{ep.get('id')}",
                    "number": ep.get("number")
                })
            return {"provider": "AMVSTR", "episodes": episodes}
        return None

    async def stream(self, episode_id: str, client: httpx.AsyncClient):
        clean_id = episode_id.replace("amvstr|", "")
        res = await client.get(f"{self.BASE_URL}/stream/{clean_id}")
        if res.status_code == 200:
            data = res.json()
            # AMVSTR returns direct stream objects
            url = data.get("stream", {}).get("multi", {}).get("main", {}).get("url")
            if url:
                return {"sources": [{"url": url, "quality": "default"}]}
        return None

class ConsumetExtension(BaseExtension):
    """Fallback Extension: Uses community proxies for Zoro/Gogo"""
    BASE_URL = "https://api-consumet.vercel.app/anime/zoro"

    async def search(self, query: str, client: httpx.AsyncClient):
        res = await client.get(f"{self.BASE_URL}/{urllib.parse.quote(query)}")
        if res.status_code == 200:
            data = res.json()
            results = []
            for item in data.get("results", []):
                results.append({
                    "id": f"consumet|{item.get('id')}",
                    "title": item.get("title", "Unknown"),
                    "image": item.get("image", ""),
                    "type": item.get("type", "TV")
                })
            return results
        return None

    async def info(self, anime_id: str, client: httpx.AsyncClient):
        clean_id = anime_id.replace("consumet|", "")
        res = await client.get(f"{self.BASE_URL}/info?id={clean_id}")
        if res.status_code == 200:
            data = res.json()
            episodes = []
            for ep in data.get("episodes", []):
                episodes.append({
                    "id": f"consumet|{ep.get('id')}",
                    "number": ep.get("number")
                })
            return {"provider": "Consumet (Zoro)", "episodes": episodes}
        return None

    async def stream(self, episode_id: str, client: httpx.AsyncClient):
        clean_id = episode_id.replace("consumet|", "")
        res = await client.get(f"{self.BASE_URL}/watch?episodeId={clean_id}")
        if res.status_code == 200:
            return res.json()
        return None

class ExtensionManager:
    """Handles routing requests to active extensions"""
    def __init__(self):
        # The engine will attempt to load from these in order
        self.extensions = [AMVSTRExtension(), ConsumetExtension()]

    async def search_all(self, query: str):
        async with httpx.AsyncClient(timeout=10.0) as client:
            for ext in self.extensions:
                try:
                    results = await ext.search(query, client)
                    if results and len(results) > 0:
                        return {"results": results}
                except Exception:
                    continue
        return {"results": []}

    async def get_info(self, composite_id: str):
        async with httpx.AsyncClient(timeout=15.0) as client:
            for ext in self.extensions:
                # Route the info request to the specific extension that found it
                if composite_id.startswith("amvstr|") and isinstance(ext, AMVSTRExtension):
                    return await ext.info(composite_id, client)
                elif composite_id.startswith("consumet|") and isinstance(ext, ConsumetExtension):
                    return await ext.info(composite_id, client)
        return {"episodes": []}

    async def get_stream(self, composite_id: str):
        async with httpx.AsyncClient(timeout=15.0) as client:
            for ext in self.extensions:
                if composite_id.startswith("amvstr|") and isinstance(ext, AMVSTRExtension):
                    return await ext.stream(composite_id, client)
                elif composite_id.startswith("consumet|") and isinstance(ext, ConsumetExtension):
                    return await ext.stream(composite_id, client)
        raise HTTPException(status_code=502, detail="Failed to resolve stream link through extension manager.")

extension_manager = ExtensionManager()

# ==========================================
# 3. FASTAPI SERVER CORE
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
    return {"status": "healthy", "manager_active": True}

@app.get("/api/search")
async def search_anime(q: str):
    if not q or q == "ping":
        return {"status": "active"}
    return await extension_manager.search_all(q)

@app.get("/api/anime/{anime_id:path}") # :path allows the pipe | character to pass securely
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
# 4. WEBSOCKET PARTY SYNC WORKER
# ==========================================
class RoomManager:
    def __init__(self):
        self.active_rooms: dict[str, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, room_id: str):
        await websocket.accept()
        if room_id not in self.active_rooms:
            self.active_rooms[room_id] = []
        self.active_rooms[room_id].append(websocket)

    def disconnect(self, websocket: WebSocket, room_id: str):
        if room_id in self.active_rooms:
            if websocket in self.active_rooms[room_id]:
                self.active_rooms[room_id].remove(websocket)
            if not self.active_rooms[room_id]:
                del self.active_rooms[room_id]

    async def broadcast(self, message: dict, room_id: str, sender: WebSocket):
        if room_id in self.active_rooms:
            for connection in self.active_rooms[room_id]:
                if connection != sender:
                    try:
                        await connection.send_json(message)
                    except Exception:
                        pass 

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
