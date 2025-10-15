from pathlib import Path
import pandas as pd
from datascience.config.loader import load_all
from datascience.utils.logger import get_logger

log = get_logger("ingest")

def run():
    cfg, _, P = load_all()
    raw = Path(P["RAW"]); lake = Path(P["LAKE"])
    lake.mkdir(parents=True, exist_ok=True)

    ratings = pd.read_csv(raw / "ratings.csv")  # userId,movieId,rating,timestamp
    movies = pd.read_csv(raw / "movies.csv")    # movieId,title,genres

    ratings.to_parquet(lake / "ratings.parquet", index=False)
    movies["genres_list"] = movies["genres"].str.split("|")
    movies.to_parquet(lake / "movies.parquet", index=False)
    log.info("Wrote Parquet to %s", lake)

if __name__ == "__main__":
    run()
