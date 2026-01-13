from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, explode
import redis
import logging
import time
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_HOST = 'redis'
REDIS_PORT = 6379

def get_redis_connection():
    try:
        return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    except Exception as e:
        logger.warning(f"Failed to connect to Redis: {e}")
        return None

def run_batch_job():
    """Execute Spark ALS batch job."""
    
    # Initialize Spark Session
    # In Docker, master should be 'spark://spark-master:7077' usually, 
    # but 'local[*]' is safer if master is not reachable or for dev.
    # We will try to read from ENV or default to local for robustness.
    spark_master = os.environ.get('SPARK_MASTER', 'local[*]')
    
    spark = SparkSession.builder \
        .appName("MovieRecommendationALS") \
        .master(spark_master) \
        .getOrCreate()
        
    logger.info("Spark Session created.")

    # 1. Load Data
    # For MVP, generating synthetic dataframe if file not present
    data = [
        (1, 10, 4.0), (1, 20, 3.0), (1, 30, 5.0),
        (2, 10, 5.0), (2, 40, 1.0), (3, 20, 4.0),
        (3, 50, 5.0)
    ]
    # Replicate data to make it slightly bigger
    for i in range(4, 20):
        data.append((i, 10, 3.0))
        data.append((i, 20, 4.0))

    columns = ["userId", "movieId", "rating"]
    df = spark.createDataFrame(data, columns)
    
    logger.info(f"Training data loaded. Count: {df.count()}")

    # 2. Train ALS Model
    als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
    model = als.fit(df)
    
    logger.info("ALS Model trained.")

    # 3. Generate Recommendations
    user_recs = model.recommendForAllUsers(10)
    
    # 4. Save to Redis
    r = get_redis_connection()
    
    if r:
        logger.info("Saving recommendations to Redis...")
        # Collect to driver to loop and save (not efficient for Big Data, but fine for MVP demo)
        recs_list = user_recs.collect()
        
        for row in recs_list:
            user_id = row['userId']
            recs = row['recommendations'] # List of Rows (movieId, rating)
            
            # Format: MovieID:Rating
            rec_strings = [f"{rec['movieId']}:{rec['rating']:.2f}" for rec in recs]
            
            key = f"rec:personal:{user_id}"
            value = ",".join(rec_strings)
            
            try:
                r.set(key, value)
            except Exception as e:
                logger.error(f"Error saving to Redis: {e}")
        
        logger.info(f"Saved recommendations for {len(recs_list)} users.")
    else:
        logger.warning("Skipping Redis save (connection failed). Printing top 5 recs instead.")
        user_recs.show(5, False)

    spark.stop()

import sys

if __name__ == "__main__":
    if "--once" in sys.argv:
        logger.info("Single run mode detected.")
        start_time = time.time()
        run_batch_job()
        end_time = time.time()
        logger.info(f"Execution Time: {end_time - start_time:.2f} seconds")
    else:
        while True:
            logger.info("Starting Batch Run...")
            run_batch_job()
            logger.info("Batch Run Finished. Sleeping for 60s...")
            time.sleep(60)
