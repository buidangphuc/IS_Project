from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col
import sys
import argparse
import time

def main():
    parser = argparse.ArgumentParser(description="Train ALS Model")
    parser.add_argument("--data-path", type=str, default="s3a://datalake/data/raw/ratings.csv", help="Path to ratings CSV")
    parser.add_argument("--model-path", type=str, default="s3a://datalake/models/als_model", help="Path to save model")
    parser.add_argument("--benchmark-mode", action="store_true", help="Skip model saving for faster benchmarking")
    parser.add_argument("--iterations", type=int, default=10, help="Number of ALS iterations")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("MovieLensALS") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    print(f"🚀 Spark Session Created. Reading data from: {args.data_path}")

    # Paths
    ratings_path = args.data_path
    model_path = args.model_path

    try:
        # Load Data
        # Assuming CSV: userId,movieId,rating,timestamp
        df = spark.read.csv(ratings_path, header=True, inferSchema=True)
        df = df.select(
            col("userId").cast("integer"),
            col("movieId").cast("integer"),
            col("rating").cast("float")
        )
        
        record_count = df.count()
        print(f"Loaded {record_count} ratings")

        # Train ALS
        print(f"BENCHMARK_START: {time.time()}")
        als = ALS(
            maxIter=args.iterations, 
            regParam=0.1, 
            userCol="userId", 
            itemCol="movieId", 
            ratingCol="rating",
            coldStartStrategy="drop"
        )
        model = als.fit(df)
        print(f"BENCHMARK_END: {time.time()}")
        print("Model Trained")

        # Save Model (skip in benchmark mode for speed)
        if not args.benchmark_mode:
            model.write().overwrite().save(model_path)
            print(f"Model saved to {model_path}")
            
            # Save Factors for Serving (Parquet for heavy lifting, JSON for lightweight serving demo)
            # We append a suffix to avoid conflict if running multiple times (optional, but keep simple for now)
            base_output = model_path.replace("als_model", "")
            
            model.userFactors.write.mode("overwrite").json(f"{base_output}/user_factors_json")
            model.itemFactors.write.mode("overwrite").json(f"{base_output}/item_factors_json")

            # Save Top-K Recs (Batch View)
            recs = model.recommendForAllUsers(20) # Get top 20
            recs.write.mode("overwrite").json(f"{base_output}/user_recs_json")
        else:
            print("Benchmark mode: skipping model save")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
