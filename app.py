from fastapi import FastAPI
from typing import List, Dict
from pathlib import Path
import numpy as np, faiss, redis, pandas as pd

from datascience.config.loader import load_all
from datascience.utils.logger import get_logger

log = get_logger("api")
app = FastAPI(title="Realtime Recommender (Lambda)")

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
