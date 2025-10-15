from pathlib import Path
import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from datascience.config.loader import load_all
from datascience.utils.logger import get_logger

log = get_logger("trainer")

def export_factors(model, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    u = model.userFactors.toPandas().sort_values("id")
    i = model.itemFactors.toPandas().sort_values("id")
    np.save(out_dir / "user_ids.npy", u["id"].to_numpy(dtype=np.int64))
    np.save(out_dir / "user_vecs.npy", np.stack(u["features"].to_numpy()))
    np.save(out_dir / "item_ids.npy", i["id"].to_numpy(dtype=np.int64))
    np.save(out_dir / "item_vecs.npy", np.stack(i["features"].to_numpy()))

def run():
    cfg, params, P = load_all()
    spark = SparkSession.builder.appName("train-als").getOrCreate()
    ratings = spark.read.parquet(f"{P['LAKE']}/ratings.parquet").cache()

    als = ALS(
        userCol="userId", itemCol="movieId", ratingCol="rating",
        nonnegative=True, implicitPrefs=params["als"]["implicit"],
        coldStartStrategy="drop",
        rank=params["als"]["rank"], maxIter=params["als"]["max_iter"], regParam=params["als"]["reg_param"]
    )
    model = als.fit(ratings)
    rmse = RegressionEvaluator(metricName="rmse", labelCol="rating").evaluate(model.transform(ratings))
    log.info("ALS RMSE=%.4f", rmse)

    export_factors(model, Path(P["TRAINER"]))
    spark.stop()

if __name__ == "__main__":
    run()
