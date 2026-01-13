import redis
import os

class RedisClient:
    def __init__(self, host='localhost', port=6380):
        # Default to localhost:6380 (mapped) if running on host
        # If running in docker, env vars should override
        self.client = redis.Redis(host=host, port=port, db=0, decode_responses=True)

    def get_trending_genres(self):
        """
        Returns list of (genre, score) sorted by score desc
        """
        try:
            # Flink job writes to 'trending_genres' ZSET
            return self.client.zrevrange('trending_genres', 0, -1, withscores=True)
        except Exception as e:
            print(f"Redis Error: {e}")
            return []

    def get_trending_movies(self):
        # Placeholder if we extended Flink to do movie-level trending
        return []
