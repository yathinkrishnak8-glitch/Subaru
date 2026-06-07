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
# 2. FASTAPI WEB SERVER CORE
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

# ==========================================
# 3. ANILIST META SEARCH & PROVIDER FALLBACKS
# ==========================================
API_MIRRORS = [
    "https://api-consumet.vercel.app/meta/anilist",
    "https://consumet-api.onrender.com/meta/anilist",
    "https://api.consumet.org/meta/anilist"
]

# The engine will loop through these until it successfully extracts an episode list
PROVIDERS = ["zoro", "gogoanime", "animepahe", "enime"]

@app.get("/")
async def serve_frontend():
    try:
        with open("index.html", "r", encoding="utf-8") as file:
            return HTMLResponse(content=file.read(), status_code=200)
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Error: index.html missing!</h1>", status_code=404)

@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "database_engine": "postgres" if IS_POSTGRES else "sqlite"}

@app.get("/api/search")
async def search_anime(q: str):
    if not q or q == "ping":
        return {"status": "active"}
    
    async with httpx.AsyncClient(timeout=15.0) as client:
        for base_url in API_MIRRORS:
            try:
                # Fuzzy search using AniList
                safe_q = urllib.parse.quote(q)
                url = f"{base_url}/{safe_q}"
                response = await client.get(url)
                if response.status_code == 200:
                    data = response.json()
                    if "results" in data and len(data["results"]) > 0:
                        return data
            except Exception:
                continue
    return {"results": []}

@app.get("/api/anime/{anime_id}")
async def get_anime_details(anime_id: str):
    async with httpx.AsyncClient(timeout=20.0) as client:
        # Multi-Provider Fallback Loop: Ensures episodes will load
        for provider in PROVIDERS:
            for base_url in API_MIRRORS:
                try:
                    url = f"{base_url}/info/{anime_id}?provider={provider}"
                    response = await client.get(url)
                    if response.status_code == 200:
                        data = response.json()
                        if "episodes" in data and len(data["episodes"]) > 0:
                            # Attach the winning provider so the frontend knows which one to stream from
                            data["_active_provider"] = provider
                            return data
                except Exception:
                    continue
    return {"episodes": []}

@app.get("/api/stream")
async def get_stream_urls(episode_id: str, provider: str = ""):
    async with httpx.AsyncClient(timeout=15.0) as client:
        providers_to_try = [provider] if provider else PROVIDERS
        for prov in providers_to_try:
            for base_url in API_MIRRORS:
                try:
                    # Safe URL encoding prevents complex IDs (like Zoro's) from breaking the route
                    safe_ep_id = urllib.parse.quote(episode_id, safe='')
                    url = f"{base_url}/watch/{safe_ep_id}?provider={prov}"
                    response = await client.get(url)
                    if response.status_code == 200:
                        data = response.json()
                        if "sources" in data and len(data["sources"]) > 0:
                            return data
                except Exception:
                    continue
    raise HTTPException(status_code=502, detail="Failed to extract streaming links.")

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
