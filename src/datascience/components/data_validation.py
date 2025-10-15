from pathlib import Path
import json
import pandas as pd
from datascience.config.loader import load_all
from datascience.utils.logger import get_logger

log = get_logger("validate")

def run():
    _, __, P = load_all()
    lake = Path(P["LAKE"]); out = Path(P["VAL"]); out.mkdir(parents=True, exist_ok=True)

    ratings = pd.read_parquet(lake / "ratings.parquet")
    movies = pd.read_parquet(lake / "movies.parquet")

    report = {
        "ratings_rows": int(len(ratings)),
        "movies_rows": int(len(movies)),
        "ratings_missing_cols": sorted(list({"userId","movieId","rating","timestamp"} - set(ratings.columns))),
        "movies_missing_cols": sorted(list({"movieId","title","genres","genres_list"} - set(movies.columns))),
        "ok": True,
    }
    report["ok"] = (len(report["ratings_missing_cols"]) == 0 and len(report["movies_missing_cols"]) == 0)

    (out / "validation.json").write_text(json.dumps(report, indent=2))
    log.info("Validation report: %s", out / "validation.json")
    if not report["ok"]:
        log.warning("Validation failed: %+r", report)

if __name__ == "__main__":
    run()
