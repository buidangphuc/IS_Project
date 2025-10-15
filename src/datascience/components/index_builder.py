from pathlib import Path
import numpy as np, faiss
from datascience.config.loader import load_all
from datascience.utils.logger import get_logger

log = get_logger("faiss")

def run():
    _, params, P = load_all()
    mdir = Path(P["TRAINER"])
    item_vecs = np.load(mdir / "item_vecs.npy").astype("float32")
    dim = item_vecs.shape[1]
    faiss.normalize_L2(item_vecs)

    quantizer = faiss.IndexFlatIP(dim)
    index = faiss.IndexIVFFlat(quantizer, dim, params["faiss"]["nlist"], faiss.METRIC_INNER_PRODUCT)
    index.train(item_vecs)
    index.add(item_vecs)
    faiss.write_index(index, str(mdir / "faiss_ivf.index"))
    log.info("Built FAISS index → %s", mdir / "faiss_ivf.index")

if __name__ == "__main__":
    run()
