import json
import logging
import random
import time
from typing import List, Optional
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Body
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import redis
from confluent_kafka import Producer
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FastAPIService")

# Configuration
REDIS_HOST = 'redis'
REDIS_PORT = 6379
KAFKA_BROKER = 'kafka:29092'
TOPIC = 'ratings'

# --- Global Connections ---
r: Optional[redis.Redis] = None
producer: Optional[Producer] = None
active_websockets: List[WebSocket] = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global r, producer
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        r.ping()
        logger.info("Connected to Redis.")
    except Exception as e:
        logger.warning(f"Redis connection failed: {e}")
        r = None

    try:
        conf = {'bootstrap.servers': KAFKA_BROKER}
        producer = Producer(conf)
        logger.info("Connected to Kafka.")
    except Exception as e:
        logger.warning(f"Kafka connection failed: {e}")
        producer = None

    yield
    
    # Shutdown
    if producer:
        producer.flush()

app = FastAPI(title="Movie Recommendation App", lifespan=lifespan)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def read_index():
    return FileResponse('static/demo.html')

@app.get("/health")
async def health_check():
    return {"status": "ok", "redis": r is not None, "kafka": producer is not None}

# --- Mock Data Helper ---
def get_movie_title(movie_id: int) -> str:
    # Deterministic mock title
    genres = ["Action", "Sci-Fi", "Drama", "Comedy", "Thriller"]
    genre = genres[movie_id % len(genres)]
    return f"Movie {movie_id} ({genre})"

# --- Endpoints ---

class ClickEvent(BaseModel):
    user_id: int
    genre: str
    count: int = 1
    movie_id: Optional[int] = None

@app.post("/simulate-click")
async def simulate_click(event: ClickEvent):
    # Loop to generate multiple events
    generated_events = []
    
    for _ in range(event.count):
        # Map genre to a random movie ID for simulation
        # Mock: 1-1000 IDs. 
        if event.movie_id is not None:
             movie_id = event.movie_id
        else:
             movie_id = random.randint(1, 1000)
        
        timestamp = int(time.time())
        
        kafka_event = {
            'userId': event.user_id,
            'movieId': movie_id,
            'rating': 5, # Click implies interest, treat as high rating
            'timestamp': timestamp,
            'genre': event.genre # Extra field for UI/Logs
        }
        
        # Send to Kafka
        if producer:
            producer.produce(TOPIC, value=json.dumps(kafka_event).encode('utf-8'))
            # Poll occasionally to prevent buffer filling up if count is high
            producer.poll(0)
            
        generated_events.append(kafka_event)
        
        # Broadcast to WebSockets (simulation feedback)
        ws_msg = {
            "type": "event",
            "user_id": event.user_id,
            "item_id": movie_id,
            "genre": event.genre,
            "timestamp": timestamp,
            "batch_index": len(generated_events),
            "total_batch": event.count
        }
        await broadcast_ws(ws_msg)
        
        # Small delay maybe? Not needed for mock speed.

    return {"status": "success", "count": event.count, "last_event": generated_events[-1] if generated_events else None}

@app.get("/recommend")
async def get_recommendations(user_id: int, k: int = 10, offline_only: bool = False):
    # 1. Fetch Offline (Redis: rec:personal:{userId})
    offline_items = []
    if r:
        try:
            # Format in Redis: "movieId:rating,movieId:rating"
            raw_recs = r.get(f"rec:personal:{user_id}")
            if raw_recs:
                # Parse string
                pairs = raw_recs.split(',')
                for p in pairs:
                    mid, rating = p.split(':')
                    offline_items.append(int(mid))
        except Exception as e:
            logger.error(f"Error reading offline recs: {e}")
    
    offline_items = offline_items[:k]
    
    if offline_only:
        return {
            "user_id": user_id,
            "items": offline_items,
            "titles": [get_movie_title(mid) for mid in offline_items],
            "realtime_weight": 0.0
        }
    
    # 2. Fetch Realtime (Redis: trending:global)
    realtime_items = []
    if r:
        try:
            # ZREVRANGE trending:global 0 k-1
            rt_data = r.zrevrange("trending:global", 0, k - 1)
            realtime_items = [int(mid) for mid in rt_data]
        except Exception as e:
            logger.error(f"Error reading realtime recs: {e}")
            
    # 3. Blend (Simple logic: Interleave or prioritize realtime if relevant)
    # For demo, let's just prepend realtime triggers to offline
    # In a real app, you'd filter offline items already seen or boost score
    
    # Simple blend: Top 2 Realtime + Top K-2 Offline
    num_rt = min(2, len(realtime_items))
    blended = realtime_items[:num_rt]
    
    for item in offline_items:
        if item not in blended:
            blended.append(item)
            
    blended = blended[:k]
    
    return {
        "user_id": user_id,
        "items": blended,
        "titles": [get_movie_title(mid) for mid in blended],
        "realtime_weight": 0.35 # Mock weight
    }

@app.get("/realtime-only")
async def get_realtime_only(user_id: int, k: int = 10):
    realtime_items = []
    if r:
        try:
            rt_data = r.zrevrange("trending:global", 0, k - 1)
            realtime_items = [int(mid) for mid in rt_data]
        except Exception as e:
            logger.error(f"Error reading realtime recs: {e}")
            
    return {
        "user_id": user_id,
        "items": realtime_items,
        "titles": [get_movie_title(mid) for mid in realtime_items]
    }

# --- WebSockets ---

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_websockets.append(websocket)
    try:
        while True:
            # Keep alive / listen for client messages if needed
            data = await websocket.receive_text()
            # Echo or process
    except WebSocketDisconnect:
        active_websockets.remove(websocket)

async def broadcast_ws(message: dict):
    for connection in active_websockets:
        try:
            await connection.send_json(message)
        except:
            # Stale connection
            pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
