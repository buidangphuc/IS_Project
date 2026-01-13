import time
import json
import random
import logging
from confluent_kafka import Producer
import socket

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'kafka:29092'
TOPIC = 'ratings'

def get_producer():
    """Create a KafkaProducer."""
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': socket.gethostname()
    }
    # confluent-kafka Producer init is lazy/async, doesn't block on connection
    producer = Producer(conf)
    logger.info("Initialized Kafka Producer.")
    return producer

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logger.warning(f'Message delivery failed: {err}')
    # else:
    #     logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def generate_ratings():
    """Generates synthetic movie rating events."""
    producer = get_producer()
    
    # Simulate 100 users and 1000 movies
    user_ids = list(range(1, 101))
    movie_ids = list(range(1, 1001))
    
    logger.info(f"Starting rating generation to topic '{TOPIC}'...")
    
    try:
        while True:
            data = {
                'userId': random.choice(user_ids),
                'movieId': random.choice(movie_ids),
                'rating': random.randint(1, 5),
                'timestamp': int(time.time())
            }
            
            # Asynchronous produce
            producer.produce(
                TOPIC, 
                value=json.dumps(data).encode('utf-8'), 
                callback=delivery_report
            )
            
            # Serve delivery callback queue.
            # Since we are in a tight loop, we call poll(0) to trigger callbacks 
            # and handle network I/O without blocking.
            producer.poll(0)
            
            time.sleep(0.005) # ~200 events per second
    except KeyboardInterrupt:
        logger.info("Stopping generator.")
    finally:
        logger.info("Flushing producer...")
        producer.flush()

if __name__ == "__main__":
    # Give Kafka a moment to start up if we are starting simultaneously
    time.sleep(10) 
    generate_ratings()
