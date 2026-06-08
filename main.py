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
# 2. ANILIST GRAPHQL NATIVE SEARCH
# ==========================================
async def search_anilist_graphql(query: str, client: httpx.AsyncClient):
    """Uses official AniList API with strict SEARCH_MATCH sorting."""
    url = "https://graphql.anilist.co"
    graphql_query = """
    query ($search: String) {
      Page(page: 1, perPage: 30) {
        media(search: $search, type: ANIME, sort: [SEARCH_MATCH, POPULARITY_DESC]) {
          id
          title { english romaji userPreferred native }
          coverImage { extraLarge large }
          format
          status
          averageScore
        }
      }
    }
    """
    try:
        res = await client.post(url, json={"query": graphql_query, "variables": {"search": query}}, timeout=10.0)
        if res.status_code == 200:
            data = res.json()
            results = []
            for item in data.get("data", {}).get("Page", {}).get("media", []):
                t = item.get("title", {})
                title = t.get("english") or t.get("romaji") or t.get("userPreferred") or t.get("native") or "Unknown"
                img = item.get("coverImage", {})
                image = img.get("extraLarge") or img.get("large") or ""
                
                results.append({
                    "id": f"anilist|{item['id']}",
                    "title": title,
                    "image": image,
                    "type": item.get("format", "TV"),
                    "status": item.get("status", "UNKNOWN"),
                    "rating": item.get("averageScore", "")
                })
            return results
    except Exception:
        pass
    return []

# ==========================================
# 3. MODERN META-API ROUTER
# ==========================================
class ModernMetaRouter:
    """Uses advanced Open-Source aggregation APIs (Anify/AMVSTR) instead of scraping HTML."""
    
    async def get_info(self, composite_id: str):
        clean_id = composite_id.replace("anilist|", "")
        async with httpx.AsyncClient(timeout=15.0) as client:
            
            # ATTEMPT 1: Anify API (The modern open-source gold standard)
            try:
                res = await client.get(f"https://api.anify.tv/episodes/{clean_id}")
                if res.status_code == 200:
                    data = res.json()
                    if isinstance(data, list) and len(data) > 0:
                        # Prioritize Zoro/Vidstream, then fallback
                        best_provider = next((p for p in data if p.get("providerId") in ["zoro", "gogoanime", "animepahe"]), data[0])
                        provider_id = best_provider.get("providerId")
                        eps = best_provider.get("episodes", [])
                        
                        episodes = [{"id": f"anify|{provider_id}|{ep['id']}|{ep.get('number', 1)}", "number": ep.get('number', 1)} for ep in eps]
                        return {"provider": f"Anify ({provider_id.upper()})", "episodes": episodes}
            except Exception:
                pass

            # ATTEMPT 2: AMVSTR (Highly reliable dedicated mapping API)
            try:
                res = await client.get(f"https://api.amvstr.me/api/v2/info/{clean_id}", timeout=10.0)
                if res.status_code == 200:
                    data = res.json()
                    eps = data.get("episodes", [])
                    if eps:
                        episodes = [{"id": f"amvstr|{ep['id']}", "number": ep.get('number', 1)} for ep in eps]
                        return {"provider": "AMVSTR NATIVE", "episodes": episodes}
            except Exception:
                pass

        return {"episodes": [], "provider": "None"}

    async def get_stream(self, composite_id: str, anilist_id: str = ""):
        parts = composite_id.split("|")
        engine = parts[0]
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            if engine == "anify":
                provider_id = parts[1]
                watch_id = urllib.parse.quote(parts[2], safe='')
                ep_num = parts[3]
                try:
                    url = f"https://api.anify.tv/sources?providerId={provider_id}&watchId={watch_id}&episodeNumber={ep_num}&id={anilist_id}&subType=sub"
                    res = await client.get(url)
                    if res.status_code == 200:
                        data = res.json()
                        if data.get("sources"):
                            return data
                except Exception:
                    pass
                    
            elif engine == "amvstr":
                ep_id = urllib.parse.quote(parts[1], safe='')
                try:
                    res = await client.get(f"https://api.amvstr.me/api/v2/stream/{ep_id}")
                    if res.status_code == 200:
                        url = res.json().get("stream", {}).get("multi", {}).get("main", {}).get("url")
                        if url:
                            return {"sources": [{"url": url, "quality": "default"}]}
                except Exception:
                    pass
                    
        raise HTTPException(status_code=502, detail="Stream resolution failed across all Meta-APIs.")

router = ModernMetaRouter()

# ==========================================
# 4. FASTAPI ROUTES
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
        results = await search_anilist_graphql(q, client)
        return {"results": results}

@app.get("/api/anime/{anime_id:path}")
async def get_anime_details(anime_id: str):
    return await router.get_info(anime_id)

@app.get("/api/stream")
async def get_stream_urls(episode_id: str, anime_id: str = ""):
    return await router.get_stream(episode_id, anime_id)

@app.post("/api/history/save")
async def save_user_history(data: ProgressPayload):
    save_progress(data.user_id, data.anime_id, data.episode_num, data.progress_seconds)
    return {"status": "saved"}

@app.get("/api/history/get")
async def get_user_history(user_id: str, anime_id: str):
    return get_progress(user_id, anime_id)

# ==========================================
# 5. WEBSOCKET SYNC
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
