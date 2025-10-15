from pathlib import Path
import numpy as np, pandas as pd, faiss
from datascience.config.loader import load_all
from datascience.utils.logger import get_logger

log = get_logger("offline_topn")

def run(k: int = 50):
    _, __, P = load_all()
    mdir = Path(P["TRAINER"])
    u_ids = np.load(mdir / "user_ids.npy")
    u_vecs = np.load(mdir / "user_vecs.npy").astype("float32")
    i_ids = np.load(mdir / "item_ids.npy")
    i_vecs = np.load(mdir / "item_vecs.npy").astype("float32")

    faiss.normalize_L2(u_vecs)
    faiss.normalize_L2(i_vecs)

    dim = i_vecs.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(i_vecs)

    B = 4096
    all_items, all_scores = [], []
    for s in range(0, len(u_vecs), B):
        e = min(s + B, len(u_vecs))
        D, I = index.search(u_vecs[s:e], k)
        all_items.extend(i_ids[I].tolist())
        all_scores.extend(D.tolist())

    df = pd.DataFrame({"userId": u_ids})
    df["items"] = all_items
    df["scores"] = all_scores

    out = mdir / "offline_topn.parquet"
    df.to_parquet(out, index=False)
    log.info("✓ Wrote offline top-%d per user → %s", k, out)

if __name__ == "__main__":
    run()
