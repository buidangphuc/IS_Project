from pathlib import Path
import pandas as pd
from datascience.config.loader import load_all
from datascience.utils.logger import get_logger

log = get_logger("transform")

def run():
    _, __, P = load_all()
    lake = Path(P["LAKE"])

    ratings = pd.read_parquet(lake / "ratings.parquet")
    movies = pd.read_parquet(lake / "movies.parquet")[["movieId","genres_list"]]

    df = ratings.merge(movies, on="movieId", how="left")
    df["primary_genre"] = df["genres_list"].apply(lambda xs: xs[0] if isinstance(xs, list) and xs else "Unknown")
    pop = df.groupby(["primary_genre","movieId"]).size().reset_index(name="cnt")
    pop.to_parquet(lake / "popularity_by_genre.parquet", index=False)
    log.info("Wrote popularity_by_genre.parquet")

if __name__ == "__main__":
    run()
