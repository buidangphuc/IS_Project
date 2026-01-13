from .storage_client import StorageClient
from .redis_client import RedisClient

class HybridRecommender:
    def __init__(self):
        print("Initializing Recommender...")
        # Assume generic storage client (defaults to MinIO)
        self.storage = StorageClient(endpoint_url="http://minio:9000") 
        self.redis = RedisClient()
        
        # Cache Batch Recommendations
        self.batch_recs = {}
        self.reload_batch()

    def reload_batch(self):
        print("Reloading Batch Model from Storage...")
        self.batch_recs = self.storage.get_recommendations()
        print(f"Loaded {len(self.batch_recs)} user recommendations.")

    def get_recommendations(self, user_id, k=10):
        # 1. Get Batch Recs (Historical)
        # Structure: [{"movieId": 1, "rating": 4.5}, ...]
        batch = self.batch_recs.get(int(user_id), [])
        
        # 2. Get Speed Recs (Trending)
        # Redis Returns: [("Action", 50.0), ("Comedy", 30.0)]
        trending_genres = self.redis.get_trending_genres()
        top_trending_genres = [g[0] for g in trending_genres[:3]] # Top 3 genres

        # 3. Hybrid Logic (Ensemble)
        # - Boost score of Batch Rec if it matches trending genre
        # - Inject top movies from trending genres (Mock logic since we don't have movie metadata loaded here yet)
        
        final_recs = []
        for rec in batch:
            movie_id = rec['movieId']
            score = rec['rating']
            
            # Simple metadata mock: Odd IDs are Action, Even are Comedy
            genre = "Action" if movie_id % 2 != 0 else "Comedy"
            
            if genre in top_trending_genres:
                score *= 1.2 # 20% Boost for trending
                
            final_recs.append({
                "movie_id": movie_id,
                "score": score,
                "source": "batch+boost" if genre in top_trending_genres else "batch"
            })

        # Sort by boosted score
        final_recs.sort(key=lambda x: x['score'], reverse=True)
        return final_recs[:k]
