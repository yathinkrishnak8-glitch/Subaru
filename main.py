import os
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from database import init_db, save_progress, get_progress

app = FastAPI(title="Klein Anime Activity Backend")

# Enable CORS so Discord's iframe can safely query this backend application
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize storage on startup
@app.on_event("startup")
async def startup_event():
    init_db()

# Target a highly stable, public instance of the open-source Consumet Scraper API
ANIME_API_BASE = "https://api.consumet.org/anime/gogoanime"

class ProgressPayload(BaseModel):
    user_id: str
    anime_id: str
    episode_num: int
    sub_or_dub: str
    progress_seconds: float

# --- ANIME EXTRACTOR ENDPOINTS ---

@app.get("/api/search")
async def search_anime(q: str):
    if not q:
        raise HTTPException(status_code=400, detail="Missing query string")
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(f"{ANIME_API_BASE}/{q}")
            return response.json()
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Scraper source error: {str(e)}")

@app.get("/api/anime/{anime_id}")
async def get_anime_details(anime_id: str):
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(f"{ANIME_API_BASE}/info/{anime_id}")
            return response.json()
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Failed to fetch metadata: {str(e)}")

@app.get("/api/stream/{episode_id}")
async def get_stream_urls(episode_id: str):
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.get(f"{ANIME_API_BASE}/watch/{episode_id}")
            return response.json()
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Extraction failure: {str(e)}")

# --- DATA MANAGEMENT ENDPOINTS ---

@app.post("/api/history/save")
async def save_user_history(data: ProgressPayload):
    save_progress(data.user_id, data.anime_id, data.episode_num, data.sub_or_dub, data.progress_seconds)
    return {"status": "success"}

@app.get("/api/history/get")
async def get_user_history(user_id: str, anime_id: str):
    return get_progress(user_id, anime_id)

# --- WEBSOCKET ROOM SYNCHRONIZATION ---

class RoomManager:
    def __init__(self):
        # Maps room_id (Discord Instance ID) to lists of WebSocket connections
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
                        # Clean up broken pipes silently
                        pass

manager = RoomManager()

@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    await manager.connect(websocket, room_id)
    try:
        while True:
            data = await websocket.receive_json()
            # Intercept and relay play, pause, and seek events to party members
            await manager.broadcast(data, room_id, sender=websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket, room_id)
    except Exception:
        manager.disconnect(websocket, room_id)
