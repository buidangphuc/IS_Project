import json
import logging
import time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
import redis

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FlinkSpeedLayer")

REDIS_HOST = 'redis'
REDIS_PORT = 6379

class RedisSink(MapFunction):
    """Writes trending data to Redis."""
    def map(self, value):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
            # value is (movieId, count)
            # Store in a Sorted Set for easier leaderboard fetching
            # ZADD trending:global count movieId
            movie_id, count = value
            r.zincrby('trending:global', count, movie_id)
            # Also set a simple key for debug
            # r.set(f"trending:movie:{movie_id}", count)
            return value
        except Exception as e:
            print(f"Redis Sink Error: {e}")
            return value

def streaming_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Note: Jars must be added to env or placed in lib/
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")
    # For this implementation, we assume the environment is set up correctly
    # or we use a pure python fallback if the PyFlink bindings fail (impossible to mix easily).
    # We proceed with Standard PyFlink code logic.

    # 1. Source: Kafka
    deserialization_schema = SimpleStringSchema()
    kafka_consumer = FlinkKafkaConsumer(
        topics='ratings',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'kafka:29092', 'group.id': 'flink_group'}
    )
    
    # 2. Transform: Parse JSON
    ds = env.add_source(kafka_consumer)
    
    def parse_json(raw):
        try:
            data = json.loads(raw)
            return (data['movieId'], 1)
        except:
            return (0, 0)

    parsed_ds = ds.map(parse_json, output_type=Types.TUPLE([Types.INT(), Types.INT()]))
    
    # 3. Window Aggregation: Count per movieId in 30s Keyed Window
    # KeyBy movieId (index 0)
    windowed_stream = parsed_ds \
        .key_by(lambda x: x[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    # 4. Sink: Redis
    # We use a map function to side-effect to Redis (simplest without custom SinkFunction wrapper)
    windowed_stream.map(RedisSink())
    
    # Execute
    env.execute("MovieRecommendationSpeedLayer")

if __name__ == '__main__':
    streaming_job()
