"""
Enhanced streaming pipeline that accumulates click events and triggers model updates
"""

import json
import time
import pandas as pd
from confluent_kafka import Consumer
import redis
from pathlib import Path
import sqlite3
from datetime import datetime, timedelta
from datascience.config.loader import load_all
from datascience.utils.logger import get_logger
from datascience.components.model_trainer import run as retrain_model

log = get_logger("enhanced_stream")

SESSION_GAP_SEC = 600      # 10'
TRENDING_WIN_SEC = 900     # 15'
DEDUP_TTL_SEC = 3600       # 1h
RETRAIN_THRESHOLD = 1000   # Retrain after N new events
RETRAIN_INTERVAL = 3600    # Min 1 hour between retrains

class StreamingModelUpdater:
    def __init__(self):
        self.cfg, _, self.P = load_all()
        self.movies = pd.read_parquet(Path(self.P["LAKE"]) / "movies.parquet")[["movieId","genres_list"]]
        self.genre_of = {
            int(r.movieId): (r.genres_list[0] if isinstance(r.genres_list, list) and r.genres_list else "Unknown")
            for r in self.movies.itertuples(index=False)
        }
        
        self.redis = redis.Redis(
            host=self.cfg["redis"]["host"], 
            port=self.cfg["redis"]["port"], 
            decode_responses=True
        )
        
        # SQLite for accumulating streaming events
        self.db_path = Path(self.P["LAKE"]) / "streaming_events.db"
        self.init_streaming_db()
        
        self.consumer = Consumer({
            "bootstrap.servers": self.cfg["kafka"]["bootstrap"],
            "group.id": "enhanced-recom",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 300000,
        })
        
        self.events_since_retrain = 0
        self.last_retrain_time = 0
        
    def init_streaming_db(self):
        """Initialize SQLite DB for streaming events"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS streaming_ratings (
                    user_id INTEGER,
                    item_id INTEGER, 
                    rating REAL,
                    timestamp INTEGER,
                    event_type TEXT,
                    genre TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, item_id, timestamp)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_item ON streaming_ratings(user_id, item_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timestamp ON streaming_ratings(timestamp)
            """)
    
    def save_streaming_event(self, event):
        """Save streaming event to SQLite"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO streaming_ratings 
                    (user_id, item_id, rating, timestamp, event_type, genre)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    event["user_id"], 
                    event["item_id"],
                    event["rating"],
                    event["ts"],
                    event.get("event_type", "click"),
                    event.get("genre", "Unknown")
                ))
            return True
        except Exception as e:
            log.error("Failed to save streaming event: %s", e)
            return False
    
    def should_retrain(self):
        """Check if we should retrain the model"""
        current_time = time.time()
        
        # Check threshold and time constraints
        threshold_met = self.events_since_retrain >= RETRAIN_THRESHOLD
        time_passed = (current_time - self.last_retrain_time) >= RETRAIN_INTERVAL
        
        return threshold_met and time_passed
    
    def merge_streaming_data(self):
        """Merge streaming events with original ratings data"""
        try:
            # Load original ratings
            original_ratings = pd.read_parquet(Path(self.P["LAKE"]) / "ratings.parquet")
            log.info("Loaded %d original ratings", len(original_ratings))
            
            # Load streaming events (recent ones)
            with sqlite3.connect(self.db_path) as conn:
                cutoff_time = time.time() - (7 * 24 * 3600)  # Last 7 days
                streaming_df = pd.read_sql_query("""
                    SELECT user_id as userId, item_id as movieId, rating, timestamp
                    FROM streaming_ratings 
                    WHERE timestamp > ?
                    ORDER BY timestamp DESC
                """, conn, params=(cutoff_time,))
            
            log.info("Loaded %d streaming events", len(streaming_df))
            
            if len(streaming_df) > 0:
                # Convert timestamp to match original format if needed
                streaming_df['timestamp'] = streaming_df['timestamp'].astype('int64')
                
                # Combine datasets
                combined = pd.concat([
                    original_ratings[['userId', 'movieId', 'rating', 'timestamp']], 
                    streaming_df[['userId', 'movieId', 'rating', 'timestamp']]
                ], ignore_index=True)
                
                # Remove duplicates (keep latest rating for each user-movie pair)
                combined = combined.sort_values('timestamp').drop_duplicates(
                    subset=['userId', 'movieId'], keep='last'
                )
                
                # Save merged data
                merged_path = Path(self.P["LAKE"]) / "ratings_with_streaming.parquet"
                combined.to_parquet(merged_path, index=False)
                log.info("Saved %d merged ratings to %s", len(combined), merged_path)
                
                # Temporarily replace original for retraining
                original_path = Path(self.P["LAKE"]) / "ratings.parquet"
                backup_path = Path(self.P["LAKE"]) / "ratings_backup.parquet"
                
                # Backup original
                original_ratings.to_parquet(backup_path, index=False)
                # Replace with merged
                combined.to_parquet(original_path, index=False)
                
                return True
            
            return False
            
        except Exception as e:
            log.error("Failed to merge streaming data: %s", e)
            return False
    
    def restore_original_ratings(self):
        """Restore original ratings after retraining"""
        try:
            backup_path = Path(self.P["LAKE"]) / "ratings_backup.parquet"
            original_path = Path(self.P["LAKE"]) / "ratings.parquet"
            
            if backup_path.exists():
                backup_df = pd.read_parquet(backup_path)
                backup_df.to_parquet(original_path, index=False)
                backup_path.unlink()  # Remove backup
                log.info("Restored original ratings")
        except Exception as e:
            log.error("Failed to restore original ratings: %s", e)
    
    def trigger_retrain(self):
        """Trigger model retraining with streaming data"""
        log.info("🚀 Starting model retrain with streaming data...")
        
        try:
            # Merge streaming data with original ratings
            if not self.merge_streaming_data():
                log.warning("No new streaming data to merge, skipping retrain")
                return False
            
            # Retrain the model
            log.info("🔄 Retraining ALS model...")
            retrain_model()
            
            # Restore original ratings
            self.restore_original_ratings()
            
            # Reset counters
            self.events_since_retrain = 0
            self.last_retrain_time = time.time()
            
            log.info("✅ Model retrain completed successfully!")
            return True
            
        except Exception as e:
            log.error("❌ Model retrain failed: %s", e)
            self.restore_original_ratings()  # Ensure cleanup
            return False

    def process_event(self, msg):
        """Process a single streaming event"""
        # Deduplication
        tpo = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
        if not self.redis.setnx(f"dedup:{tpo}", "1"):
            self.consumer.commit(msg)
            return
        
        self.redis.expire(f"dedup:{tpo}", DEDUP_TTL_SEC)
        
        # Parse event
        evt = json.loads(msg.value())
        uid = int(evt.get("user_id"))
        mid = int(evt.get("item_id"))
        ts = int(evt.get("ts", time.time()))
        genre = self.genre_of.get(mid, "Unknown")
        evt["genre"] = genre
        
        # Save to streaming database
        if self.save_streaming_event(evt):
            self.events_since_retrain += 1
            
            # Log progress
            if self.events_since_retrain % 100 == 0:
                log.info("📊 Processed %d events since last retrain", 
                        self.events_since_retrain)
        
        # Original Redis processing (session tracking, trending)
        self.process_redis_signals(uid, mid, ts, genre)
        
        # Check if we should retrain
        if self.should_retrain():
            self.trigger_retrain()
        
        self.consumer.commit(msg)
    
    def process_redis_signals(self, uid, mid, ts, genre):
        """Original Redis processing for session and trending"""
        # Session tracking
        last_ts_key = f"user:{uid}:last_ts"
        last_ts = self.redis.get(last_ts_key)
        if last_ts is None or (ts - int(last_ts)) > SESSION_GAP_SEC:
            self.redis.set(f"user:{uid}:recent_genre", genre, ex=SESSION_GAP_SEC)
        self.redis.set(last_ts_key, ts, ex=SESSION_GAP_SEC)

        # Trending tracking
        zkey = f"trending:genre:{genre}"
        self.redis.zadd(zkey, {str(mid): ts})
        self.redis.zremrangebyscore(zkey, 0, ts - TRENDING_WIN_SEC)
        self.redis.zincrby(f"trending_cnt:genre:{genre}", 1.0, str(mid))
        self.redis.expire(f"trending_cnt:genre:{genre}", TRENDING_WIN_SEC)

    def run(self):
        """Main streaming loop"""
        topic = self.cfg["kafka"]["topic_ratings"]
        self.consumer.subscribe([topic])
        
        log.info("🎬 Enhanced streaming pipeline with model updates running...")
        log.info("   Retrain threshold: %d events", RETRAIN_THRESHOLD)
        log.info("   Retrain interval: %d seconds", RETRAIN_INTERVAL)
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log.warning("Kafka error: %s", msg.error())
                    continue
                
                self.process_event(msg)
                
        except KeyboardInterrupt:
            log.info("Shutting down enhanced streaming pipeline...")
        finally:
            self.consumer.close()

def main():
    updater = StreamingModelUpdater()
    updater.run()

if __name__ == "__main__":
    main()