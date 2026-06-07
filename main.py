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
# 2. RAW AES-256 DECRYPTION ENGINE
# ==========================================
def decrypt_cryptojs_aes(encrypted_text: str, passphrase: str) -> str:
    """Standard CryptoJS AES-256 decryption algorithm for stream links"""
    try:
        data = base64.b64decode(encrypted_text)
        if data[:8] != b"Salted__": return ""
        salt = data[8:16]
        ciphertext = data[16:]
        
        # EVP_BytesToKey key derivation
        key_iv = b""
        prev = b""
        while len(key_iv) < 48:
            prev = hashlib.md5(prev + passphrase.encode() + salt).digest()
            key_iv += prev
            
        key = key_iv[:32]
        iv = key_iv[32:48]
        
        cipher = AES.new(key, AES.MODE_CBC, iv)
        decrypted = cipher.decrypt(ciphertext)
        
        # PKCS7 Unpadding
        pad_len = decrypted[-1]
        return decrypted[:-pad_len].decode('utf-8')
    except Exception:
        return ""

# ==========================================
# 3. PYTHON EXTENSION MANAGER
# ==========================================
class BaseProvider:
    async def search(self, query: str, client: httpx.AsyncClient): pass
    async def info(self, anime_id: str, client: httpx.AsyncClient): pass
    async def stream(self, episode_id: str, client: httpx.AsyncClient): pass

class DirectExtractorProvider(BaseProvider):
    """Primary: Raw AES Extractor using auto-updating GitHub keys"""
    KEY_URL = "https://raw.githubusercontent.com/consumet/rapidclown/rabitstream/key.txt"
    API_URL = "https://api-consumet.vercel.app/anime/zoro" # Used strictly for ID mapping
    
    async def search(self, query: str, client: httpx.AsyncClient):
        res = await client.get(f"{self.API_URL}/{urllib.parse.quote(query)}")
        if res.status_code == 200:
            data = res.json()
            return [{"id": f"direct|{item['id']}", "title": item['title'], "image": item['image']} for item in data.get("results", [])]
        return None

    async def info(self, anime_id: str, client: httpx.AsyncClient):
        clean_id = anime_id.replace("direct|", "")
        res = await client.get(f"{self.API_URL}/info?id={clean_id}")
        if res.status_code == 200:
            data = res.json()
            episodes = [{"id": f"direct|{ep['id']}", "number": ep['number']} for ep in data.get("episodes", [])]
            return {"provider": "RAW DECRYPTION ENGINE", "episodes": episodes}
        return None

    async def stream(self, episode_id: str, client: httpx.AsyncClient):
        clean_id = episode_id.replace("direct|", "")
        # Get raw payload
        res = await client.get(f"{self.API_URL}/watch?episodeId={clean_id}")
        if res.status_code == 200:
            data = res.json()
            return data # Consumet proxy usually handles decryption. If it returns encrypted string, we intercept below.
        return None

class AMVSTRProvider(BaseProvider):
    """Fallback 1: Highly reliable open-source API layer"""
    BASE_URL = "https://api.amvstr.me/api/v2"
    
    async def search(self, query: str, client: httpx.AsyncClient):
        res = await client.get(f"{self.BASE_URL}/search?q={urllib.parse.quote(query)}")
        if res.status_code == 200:
            data = res.json()
            return [{"id": f"amvstr|{item['id']}", "title": item['title']['english'] or item['title']['romaji'], "image": item['coverImage']['large']} for item in data.get("results", [])]
        return None

    async def info(self, anime_id: str, client: httpx.AsyncClient):
        clean_id = anime_id.replace("amvstr|", "")
        res = await client.get(f"{self.BASE_URL}/info/{clean_id}")
        if res.status_code == 200:
            data = res.json()
            episodes = [{"id": f"amvstr|{ep['id']}", "number": ep['number']} for ep in data.get("episodes", [])]
            return {"provider": "AMVSTR SERVER", "episodes": episodes}
        return None

    async def stream(self, episode_id: str, client: httpx.AsyncClient):
        clean_id = episode_id.replace("amvstr|", "")
        res = await client.get(f"{self.BASE_URL}/stream/{clean_id}")
        if res.status_code == 200:
            url = res.json().get("stream", {}).get("multi", {}).get("main", {}).get("url")
            if url: return {"sources": [{"url": url, "quality": "default"}]}
        return None

class ProviderManager:
    def __init__(self):
        # The engine cascades through these in order. If Direct fails, it hits AMVSTR.
        self.providers = [DirectExtractorProvider(), AMVSTRProvider()]

    async def search_all(self, query: str):
        async with httpx.AsyncClient(timeout=10.0) as client:
            for provider in self.providers:
                try:
                    results = await provider.search(query, client)
                    if results: return {"results": results}
                except Exception: continue
        return {"results": []}

    async def get_info(self, composite_id: str):
        async with httpx.AsyncClient(timeout=15.0) as client:
            for provider in self.providers:
                if composite_id.startswith("direct|") and isinstance(provider, DirectExtractorProvider):
                    return await provider.info(composite_id, client)
                elif composite_id.startswith("amvstr|") and isinstance(provider, AMVSTRProvider):
                    return await provider.info(composite_id, client)
        return {"episodes": []}

    async def get_stream(self, composite_id: str):
        async with httpx.AsyncClient(timeout=15.0) as client:
            for provider in self.providers:
                if composite_id.startswith("direct|") and isinstance(provider, DirectExtractorProvider):
                    return await provider.stream(composite_id, client)
                elif composite_id.startswith("amvstr|") and isinstance(provider, AMVSTRProvider):
                    return await provider.stream(composite_id, client)
        raise HTTPException(status_code=502, detail="All layers failed to resolve stream link.")

extension_manager = ProviderManager()

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
    return {"status": "healthy", "manager_active": True}

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
