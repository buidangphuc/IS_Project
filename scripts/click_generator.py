#!/usr/bin/env python3
"""
Enhanced Kafka producer that simulates realistic user click patterns
for demo purposes with immediate effect on recommendations.
"""

import json
import time
import random
import pandas as pd
from typing import Dict, List
from confluent_kafka import Producer
from pathlib import Path

from datascience.config.loader import load_all
from datascience.utils.logger import get_logger

log = get_logger("click_generator")

class ClickGenerator:
    def __init__(self):
        self.cfg, _, self.P = load_all()
        self.movies = pd.read_parquet(Path(self.P["LAKE"]) / "movies.parquet")
        
        # Create genre-to-movies mapping
        self.genre_movies: Dict[str, List[int]] = {}
        for _, row in self.movies.iterrows():
            if isinstance(row.genres_list, list):
                for genre in row.genres_list:
                    if genre not in self.genre_movies:
                        self.genre_movies[genre] = []
                    self.genre_movies[genre].append(int(row.movieId))
        
        # Kafka producer
        self.producer = Producer({
            "bootstrap.servers": self.cfg["kafka"]["bootstrap"],
            "acks": "1",
            "retries": 3
        })
        self.topic = self.cfg["kafka"]["topic_ratings"]
        
        log.info("Click generator initialized with %d genres", len(self.genre_movies))
        
    def generate_click_event(self, user_id: int, genre: str = None) -> Dict:
        """Generate a realistic click event"""
        if genre and genre in self.genre_movies:
            movie_id = random.choice(self.genre_movies[genre])
        else:
            # Random genre if not specified
            genre = random.choice(list(self.genre_movies.keys()))
            movie_id = random.choice(self.genre_movies[genre])
        
        # Simulate rating (biased towards positive for clicks)
        rating = random.choices([3.0, 4.0, 5.0], weights=[0.3, 0.4, 0.3])[0]
        
        event = {
            "user_id": user_id,
            "item_id": movie_id,
            "rating": rating,
            "ts": int(time.time()),
            "genre": genre,  # Extra field for demo
            "event_type": "click"
        }
        
        return event
    
    def send_event(self, event: Dict) -> bool:
        """Send event to Kafka"""
        try:
            self.producer.produce(
                self.topic,
                key=str(event["user_id"]).encode(),
                value=json.dumps(event).encode("utf-8"),
                callback=self._delivery_callback
            )
            self.producer.poll(0)
            return True
        except Exception as e:
            log.error("Failed to send event: %s", e)
            return False
    
    def _delivery_callback(self, err, msg):
        """Kafka delivery callback"""
        if err:
            log.error("Event delivery failed: %s", err)
        else:
            log.debug("Event delivered to %s [%d]", msg.topic(), msg.partition())
    
    def simulate_user_session(self, user_id: int, genre_preference: str, num_clicks: int = 50):
        """Simulate a focused user session with genre preference"""
        log.info("Starting session for user %d with %s preference (%d clicks)", 
                user_id, genre_preference, num_clicks)
        
        events_sent = 0
        for i in range(num_clicks):
            # 80% chance of clicking preferred genre, 20% random for stronger signal
            if random.random() < 0.8:
                genre = genre_preference
            else:
                genre = random.choice(list(self.genre_movies.keys()))
            
            event = self.generate_click_event(user_id, genre)
            
            if self.send_event(event):
                events_sent += 1
                if events_sent % 10 == 0:
                    log.info("📱 User %d: %d/%d clicks sent (%s focus)", 
                            user_id, events_sent, num_clicks, genre_preference)
            
            # Shorter delay for faster generation
            time.sleep(random.uniform(0.1, 0.5))
        
        self.producer.flush()
        log.info("✅ Session complete: %d/%d events sent for user %d", 
                events_sent, num_clicks, user_id)
        return events_sent

    def continuous_stream(self, users: List[int], duration_sec: int = 300):
        """Generate continuous stream of events for multiple users"""
        log.info("Starting high-volume stream for %d users, %ds duration", 
                len(users), duration_sec)
        
        start_time = time.time()
        events_sent = 0
        
        while time.time() - start_time < duration_sec:
            user_id = random.choice(users)
            genre = random.choice(list(self.genre_movies.keys()))
            
            event = self.generate_click_event(user_id, genre)
            
            if self.send_event(event):
                events_sent += 1
                if events_sent % 50 == 0:  # Report every 50 events
                    elapsed = time.time() - start_time
                    rate = events_sent / elapsed
                    log.info("📊 %d events sent (%.1f/sec) - %ds remaining", 
                            events_sent, rate, int(duration_sec - elapsed))
            
            # Much shorter delay for high volume
            time.sleep(random.uniform(0.01, 0.1))  # 10-100ms between events
        
        self.producer.flush()
        log.info("🚀 High-volume stream complete: %d events sent in %ds (%.1f events/sec)", 
                events_sent, duration_sec, events_sent/duration_sec)
        return events_sent

    def demo_burst(self, user_id: int, genre_preference: str, burst_size: int = 1200):
        """Generate massive burst of clicks for immediate recommendation changes"""
        log.info("🎆 Starting DEMO BURST for user %d: %d clicks on %s", 
                user_id, burst_size, genre_preference)
        
        events_sent = 0
        batch_size = 50
        
        for batch in range(0, burst_size, batch_size):
            # Send batch of events rapidly
            for i in range(min(batch_size, burst_size - batch)):
                # 90% preferred genre for strong signal
                if random.random() < 0.9:
                    genre = genre_preference
                else:
                    # Pick a contrasting genre occasionally
                    contrasting_genres = [g for g in self.genre_movies.keys() 
                                        if g != genre_preference]
                    genre = random.choice(contrasting_genres)
                
                event = self.generate_click_event(user_id, genre)
                
                if self.send_event(event):
                    events_sent += 1
            
            # Brief pause between batches to avoid overwhelming
            time.sleep(0.1)
            
            # Progress update
            if (batch + batch_size) % 100 == 0:
                log.info("💥 BURST Progress: %d/%d events sent", 
                        events_sent, burst_size)
        
        self.producer.flush()
        log.info("🎯 DEMO BURST COMPLETE: %d events sent for user %d (%s preference)", 
                events_sent, user_id, genre_preference)
        return events_sent

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate click events for demo")
    parser.add_argument("--mode", choices=["session", "stream", "single", "demo-burst"], 
                       default="single", help="Generation mode")
    parser.add_argument("--user-id", type=int, default=1, 
                       help="User ID for single/session mode")
    parser.add_argument("--genre", default="Action", 
                       help="Genre preference for session mode")
    parser.add_argument("--clicks", type=int, default=100, 
                       help="Number of clicks in session mode")
    parser.add_argument("--duration", type=int, default=120, 
                       help="Duration in seconds for stream mode")
    parser.add_argument("--users", type=int, nargs="+", default=[1, 2, 3, 4, 5],
                       help="User IDs for stream mode")
    parser.add_argument("--burst-size", type=int, default=1200,
                       help="Number of events in demo-burst mode")
    
    args = parser.parse_args()
    
    generator = ClickGenerator()
    
    if args.mode == "single":
        event = generator.generate_click_event(args.user_id, args.genre)
        if generator.send_event(event):
            print(f"✅ Sent single click event: User {args.user_id} → {args.genre}")
        generator.producer.flush()
        
    elif args.mode == "session":
        generator.simulate_user_session(args.user_id, args.genre, args.clicks)
        
    elif args.mode == "stream":
        generator.continuous_stream(args.users, args.duration)
        
    elif args.mode == "demo-burst":
        generator.demo_burst(args.user_id, args.genre, args.burst_size)
    
    print("🎬 Click generation complete!")

if __name__ == "__main__":
    main()