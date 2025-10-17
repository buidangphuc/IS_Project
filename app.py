from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from typing import List, Dict
from pathlib import Path
import numpy as np, faiss, redis, pandas as pd
import json, time, random, asyncio
from pydantic import BaseModel

from datascience.config.loader import load_all
from datascience.utils.logger import get_logger

log = get_logger("api")
app = FastAPI(title="Realtime Recommender (Lambda)")

# Mount static files for demo UI
app.mount("/static", StaticFiles(directory="static"), name="static")

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_text(json.dumps(message))
            except:
                pass

manager = ConnectionManager()

# Pydantic models
class ClickEvent(BaseModel):
    user_id: int
    genre: str

cfg, params, P = load_all()
MDIR = Path(P["TRAINER"])
ITEM_IDS = np.load(MDIR / "item_ids.npy")
ITEM_VECS = np.load(MDIR / "item_vecs.npy").astype("float32")
USER_IDS = np.load(MDIR / "user_ids.npy")
USER_VECS = np.load(MDIR / "user_vecs.npy").astype("float32")
faiss.normalize_L2(ITEM_VECS); faiss.normalize_L2(USER_VECS)
INDEX = faiss.read_index(str(MDIR / "faiss_ivf.index"))

MOVIES = pd.read_parquet(Path(P["LAKE"]) / "movies.parquet")[["movieId","title","genres_list"]]
MOVIE_TITLE = dict(zip(MOVIES["movieId"].astype(int), MOVIES["title"].astype(str)))

ALPHA = float(cfg["blend"]["realtime_weight"])  # 0.0 in batch-only mode
try:
    r = redis.Redis(host=cfg["redis"]["host"], port=cfg["redis"]["port"], decode_responses=True)
    r.ping()
    RT_AVAILABLE = True
except Exception:
    r = None
    RT_AVAILABLE = False

def user_vec(uid: int) -> np.ndarray:
    pos = np.searchsorted(USER_IDS, uid)
    if pos >= len(USER_IDS) or USER_IDS[pos] != uid:
        return USER_VECS.mean(axis=0, keepdims=True)
    return USER_VECS[pos:pos+1]

def ann_topk(vec: np.ndarray, k: int) -> Dict[int, float]:
    D, I = INDEX.search(vec.astype("float32"), k)
    ids = ITEM_IDS[I[0]]
    scores = D[0]
    return {int(i): float(s) for i, s in zip(ids, scores)}

def realtime_scores(user_id: int, k: int) -> Dict[int, float]:
    if not RT_AVAILABLE or ALPHA <= 0.0:
        return {}
    genre = r.get(f"user:{user_id}:recent_genre")
    if not genre:
        return {}
    items = r.zrevrange(f"trending:genre:{genre}", 0, k-1, withscores=True)
    return {int(i): float(s) for i, s in items}

def blend(batch: Dict[int,float], rt: Dict[int,float], alpha: float, k: int) -> List[int]:
    keys = set(batch) | set(rt)
    if not keys:
        return []
    def norm(d: Dict[int,float]) -> Dict[int,float]:
        if not d: return {}
        vals = list(d.values()); mn, mx = min(vals), max(vals)
        if mx == mn: return {kk: 1.0 for kk in d}
        return {kk: (vv-mn)/(mx-mn) for kk,vv in d.items()}
    b = norm(batch); rts = norm(rt)
    scores = {i: (1-alpha)*b.get(i,0.0) + alpha*rts.get(i,0.0) for i in keys}
    return [i for i,_ in sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:k]]

@app.get("/")
def demo_page():
    return FileResponse("static/demo.html")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/recommend")
def recommend(user_id: int, k: int = 20):
    uvec = user_vec(user_id)
    batch = batch_scores_from_topn(user_id, max(k*5, 100))
    if not batch:
        batch = ann_topk(uvec, max(k*5, 100))
    rt = realtime_scores(user_id, k*3)
    items = blend(batch, rt, ALPHA, k)
    return {
        "user_id": user_id,
        "items": items,
        "titles": [MOVIE_TITLE.get(i, str(i)) for i in items],
        "realtime_weight": ALPHA,
        "has_realtime": bool(rt),
    }


# Optional: load offline top-N per user if present
TOPN_MAP = {}
try:
    topn_df = pd.read_parquet(Path(P["TRAINER"]) / "offline_topn.parquet")
    TOPN_MAP = {int(r.userId): [int(x) for x in r.items] for r in topn_df.itertuples(index=False)}
    log.info("Loaded offline_topn.parquet with %d users", len(TOPN_MAP))
except Exception as e:
    log.info("offline_topn.parquet not found or failed to load: %s", e)

def batch_scores_from_topn(uid: int, k: int) -> dict[int,float]:
    items = TOPN_MAP.get(uid, [])[:k]
    if not items:
        return {}
    n = len(items)
    return {it: float(n - rank) / float(n) for rank, it in enumerate(items)}

@app.get("/recommend-offline")
def recommend_offline(user_id: int, k: int = 20):
    """Pure offline recommendations (no realtime blending)"""
    uvec = user_vec(user_id)
    batch = batch_scores_from_topn(user_id, max(k*5, 100))
    if not batch:
        batch = ann_topk(uvec, max(k*5, 100))
    
    items = [i for i,_ in sorted(batch.items(), key=lambda kv: kv[1], reverse=True)[:k]]
    return {
        "user_id": user_id,
        "items": items,
        "titles": [MOVIE_TITLE.get(i, str(i)) for i in items],
        "mode": "offline_only"
    }

@app.get("/realtime-only")
def recommend_realtime_only(user_id: int, k: int = 20):
    """Pure realtime recommendations"""
    rt = realtime_scores(user_id, k*3)
    items = [i for i,_ in sorted(rt.items(), key=lambda kv: kv[1], reverse=True)[:k]]
    return {
        "user_id": user_id,
        "items": items,
        "titles": [MOVIE_TITLE.get(i, str(i)) for i in items],
        "mode": "realtime_only",
        "has_data": bool(rt)
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial stats
        await websocket.send_text(json.dumps({
            "type": "stats",
            "realtime_weight": ALPHA,
            "total_events": 0
        }))
        
        # Keep connection alive and send periodic updates
        while True:
            await asyncio.sleep(5)  # Send stats every 5 seconds
            stats = {
                "type": "stats", 
                "timestamp": time.time(),
                "realtime_weight": ALPHA,
                "redis_available": RT_AVAILABLE
            }
            await websocket.send_text(json.dumps(stats))
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.post("/simulate-click")
async def simulate_click(event: ClickEvent):
    """Simulate a user click event for demo purposes"""
    # Get random movie from the selected genre
    genre_movies = MOVIES[MOVIES['genres_list'].apply(
        lambda x: event.genre in (x if isinstance(x, list) else [])
    )]
    
    if genre_movies.empty:
        # Fallback to any movie
        movie_id = random.choice(MOVIES['movieId'].tolist())
    else:
        movie_id = random.choice(genre_movies['movieId'].tolist())
    
    # Create event and send to WebSocket clients
    click_event = {
        "type": "event",
        "user_id": event.user_id,
        "item_id": int(movie_id),
        "genre": event.genre,
        "timestamp": int(time.time())
    }
    
    # Simulate writing to Redis (if available)
    if RT_AVAILABLE:
        try:
            ts = click_event["timestamp"]
            genre = event.genre
            mid = movie_id
            uid = event.user_id
            
            # Update recent genre for user
            r.set(f"user:{uid}:recent_genre", genre, ex=600)
            r.set(f"user:{uid}:last_ts", ts, ex=600)
            
            # Add to trending
            zkey = f"trending:genre:{genre}"
            r.zadd(zkey, {str(mid): ts})
            r.zremrangebyscore(zkey, 0, ts - 900)  # 15 min window
            r.zincrby(f"trending_cnt:genre:{genre}", 1.0, str(mid))
            r.expire(f"trending_cnt:genre:{genre}", 900)
        except Exception as e:
            log.warning("Failed to update Redis: %s", e)
    
    # Broadcast to all WebSocket connections
    await manager.broadcast(click_event)
    
    return {"success": True, "event": click_event}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
