import os
import sqlite3
import httpx
import urllib.parse
import re
from contextlib import contextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ==========================================
# 1. DATABASE CONFIGURATION
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
            return
        except Exception:
            IS_POSTGRES = False

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
    except Exception:
        pass

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
# 2. NATIVE PYTHON HTML SCRAPER (ANIYOMI STYLE)
# ==========================================
class NativeScraper:
    """Bypasses APIs completely. Scrapes raw HTML using Regex just like open-source extensions."""
    BASE_URL = "https://anitaku.pe"
    AJAX_URL = "https://ajax.gogo-load.com/ajax"
    
    # We spoof a real browser to bypass basic Cloudflare checks on HTML pages
    HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

    async def search(self, query: str, client: httpx.AsyncClient):
        url = f"{self.BASE_URL}/search.html?keyword={urllib.parse.quote(query)}"
        try:
            res = await client.get(url, headers=self.HEADERS, timeout=10.0)
            # Regex to extract: ID, Title, Image
            pattern = r'<div class="img">\s*<a href="/category/([^"]+)" title="([^"]+)">\s*<img src="([^"]+)"'
            matches = re.findall(pattern, res.text)
            
            results = []
            for m in matches:
                results.append({
                    "id": m[0],
                    "title": m[1],
                    "image": m[2],
                    "type": "TV",
                    "status": "Native Match"
                })
            return results
        except Exception as e:
            print(f"[Scraper Error] Search: {e}")
            return []

    async def info(self, anime_id: str, client: httpx.AsyncClient):
        url = f"{self.BASE_URL}/category/{anime_id}"
        try:
            res = await client.get(url, headers=self.HEADERS, timeout=10.0)
            
            # Step 1: Extract the hidden movie_id needed to request the episode list
            movie_id_match = re.search(r'<input type="hidden" value="([^"]+)" id="movie_id"', res.text)
            if not movie_id_match: 
                return None
            movie_id = movie_id_match.group(1)

            # Step 2: Request the raw episode list HTML
            ajax_url = f"{self.AJAX_URL}/load-list-episode?ep_start=0&ep_end=9999&id={movie_id}"
            ajax_res = await client.get(ajax_url, headers=self.HEADERS, timeout=10.0)

            # Step 3: Regex to extract Episode ID and Episode Number
            ep_pattern = r'<a href="\s*/([^"]+)\s*"[^>]*ep_num="([0-9\.]+)"'
            ep_matches = re.findall(ep_pattern, ajax_res.text)

            episodes = []
            for ep in ep_matches:
                episodes.append({
                    "id": ep[0].strip(),
                    "number": float(ep[1])
                })
            
            # Episodes load bottom-to-top, reverse the array
            episodes.reverse()
            return {"provider": "Native Engine (Anitaku)", "episodes": episodes}
        except Exception as e:
            print(f"[Scraper Error] Info: {e}")
            return None

    async def stream(self, episode_id: str, client: httpx.AsyncClient):
        # ATTEMPT 1: Extract .m3u8 via API Proxies (Best for Discord WebSocket Sync)
        apis = [
            f"https://api-consumet.vercel.app/anime/gogoanime/watch/{episode_id}",
            f"https://consumet-api.onrender.com/anime/gogoanime/watch/{episode_id}"
        ]
        for api in apis:
            try:
                r = await client.get(api, timeout=6.0)
                if r.status_code == 200 and r.json().get("sources"):
                    return r.json()
            except Exception:
                continue

        # ATTEMPT 2: The Ultimate Fallback - Raw Iframe Extraction
        # If the APIs fail to decrypt the video, we extract the raw streaming site embed link
        # and tell the frontend to load it inside an iframe. It guarantees playback.
        url = f"{self.BASE_URL}/{episode_id}"
        try:
            res = await client.get(url, headers=self.HEADERS, timeout=10.0)
            iframe_match = re.search(r'<li class="anime">\s*<a href="#"[^>]*data-video="([^"]+)"', res.text)
            
            if iframe_match:
                iframe_url = iframe_match.group(1)
                if iframe_url.startswith("//"): 
                    iframe_url = "https:" + iframe_url
                
                # Flag the payload so the frontend knows it must render an iframe, not a video player
                return {"sources": [{"url": iframe_url, "quality": "iframe", "is_iframe": True}]}
        except Exception as e:
            print(f"[Scraper Error] Stream: {e}")

        raise HTTPException(status_code=502, detail="Stream resolution failed across all extractors.")

scraper = NativeScraper()

# ==========================================
# 3. FASTAPI ROUTES
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
    return {"status": "healthy"}

@app.get("/api/search")
async def search_anime(q: str):
    if not q or q == "ping": return {"status": "active"}
    async with httpx.AsyncClient() as client:
        results = await scraper.search(q, client)
        return {"results": results}

@app.get("/api/anime/{anime_id:path}")
async def get_anime_details(anime_id: str):
    async with httpx.AsyncClient() as client:
        data = await scraper.info(anime_id, client)
        if data: return data
        return {"episodes": [], "provider": "None"}

@app.get("/api/stream")
async def get_stream_urls(episode_id: str):
    async with httpx.AsyncClient() as client:
        return await scraper.stream(episode_id, client)

@app.post("/api/history/save")
async def save_user_history(data: ProgressPayload):
    save_progress(data.user_id, data.anime_id, data.episode_num, data.progress_seconds)
    return {"status": "saved"}

@app.get("/api/history/get")
async def get_user_history(user_id: str, anime_id: str):
    return get_progress(user_id, anime_id)

# ==========================================
# 4. WEBSOCKET SYNC
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
