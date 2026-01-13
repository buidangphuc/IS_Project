from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
import json
import os

# Note: This job runs inside Flink TaskManager.
# Ensure 'redis' pip package is installed on TaskManager.

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Add Kafka Connector JAR
    # jar_path = "file:///app/jars/flink-sql-connector-kafka-3.0.1-1.18.jar"
    # env.add_jars(jar_path)

    properties = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink-trending-group'
    }

    # 1. Source: Kafka
    kafka_consumer = FlinkKafkaConsumer(
        topics='clicks',
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    ds = env.add_source(kafka_consumer)

    # 2. Transform: JSON -> (Genre, 1)
    def parse_event(event_str):
        try:
            data = json.loads(event_str)
            return (data['genre'], 1)
        except:
            return ("Unknown", 1)

    ds = ds.map(parse_event, output_type=Types.TUPLE([Types.STRING(), Types.INT()]))

    # 3. Window & Aggregate
    # For simplicity in demo, just keyBy and sum continuous (tumbling needs time)
    ds = ds.key_by(lambda x: x[0]).sum(1)

    # 4. Sink: Redis (via custom map for demo simplicity)
    def write_to_redis(record):
        import redis
        try:
            # Use 'redis' service name defined in docker-compose
            r = redis.Redis(host='redis', port=6379, db=0)
            genre, count = record
            r.zadd('trending_genres', {genre: count})
            return f"Updated {genre}: {count}"
        except Exception as e:
            return f"Redis Error: {e}"

    ds.map(write_to_redis).print()

    env.execute("Flink Trending Genres")

if __name__ == "__main__":
    main()
