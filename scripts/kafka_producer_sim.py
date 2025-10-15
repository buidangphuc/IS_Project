import json, time
import pandas as pd
from datascience.config.loader import load_all
from confluent_kafka import Producer

def main():
    cfg, _, P = load_all()
    df = pd.read_parquet(f"{P['LAKE']}/ratings.parquet")
    p = Producer({"bootstrap.servers": cfg["kafka"]["bootstrap"]})

    for _, row in df.sample(frac=1.0, random_state=7).iterrows():
        evt = {
            "user_id": int(row["userId"]),
            "item_id": int(row["movieId"]),
            "rating": float(row["rating"]),
            "ts": int(row["timestamp"]),
        }
        p.produce(cfg["kafka"]["topic_ratings"], json.dumps(evt).encode("utf-8"))
        p.poll(0)
        time.sleep(0.02)
    p.flush()
    print("✓ Produced all rating events")

if __name__ == "__main__":
    main()
