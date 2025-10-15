
import json, time
import pandas as pd
from confluent_kafka import Consumer
import redis
from pathlib import Path
from datascience.config.loader import load_all
from datascience.utils.logger import get_logger

log = get_logger("stream")

SESSION_GAP_SEC = 600      # 10'
TRENDING_WIN_SEC = 900     # 15'
DEDUP_TTL_SEC   = 3600     # 1h giữ offset đã xử lý

def main():
    cfg, _, P = load_all()
    movies = pd.read_parquet(Path(P["LAKE"]) / "movies.parquet")[["movieId","genres_list"]]
    genre_of = {int(r.movieId): (r.genres_list[0] if isinstance(r.genres_list, list) and r.genres_list else "Unknown")
                for r in movies.itertuples(index=False)}

    r = redis.Redis(host=cfg["redis"]["host"], port=cfg["redis"]["port"], decode_responses=True)

    c = Consumer({
        "bootstrap.servers": cfg["kafka"]["bootstrap"],
        "group.id": "recom-speed",
        "enable.auto.commit": False,            # commit sau khi ghi (at-least-once + idempotent sinks)
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 300000,
    })
    topic = cfg["kafka"]["topic_ratings"]
    c.subscribe([topic])

    log.info("Speed layer (Python) running with dedup/session/trending…")
    while True:
        msg = c.poll(1.0)
        if msg is None: 
            continue
        if msg.error():
            log.warning("Kafka error: %s", msg.error())
            continue

        # ---------- DEDUP theo (topic, partition, offset)
        tpo = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
        if r.setnx(f"dedup:{tpo}", "1"):          # lần đầu thấy offset này
            r.expire(f"dedup:{tpo}", DEDUP_TTL_SEC)
        else:
            # đã xử lý -> skip, commit để không đọc lại
            c.commit(msg)
            continue

        evt = json.loads(msg.value())
        uid = int(evt.get("user_id"))
        mid = int(evt.get("item_id"))
        ts  = int(evt.get("ts", time.time()))

        genre = genre_of.get(mid, "Unknown")

        # ---------- SESSION 10' (inactivity gap)
        last_ts_key = f"user:{uid}:last_ts"
        last_ts = r.get(last_ts_key)
        if last_ts is None or (ts - int(last_ts)) > SESSION_GAP_SEC:
            # bắt đầu session mới -> đặt recent_genre
            r.set(f"user:{uid}:recent_genre", genre, ex=SESSION_GAP_SEC)
        r.set(last_ts_key, ts, ex=SESSION_GAP_SEC)

        # ---------- TRENDING WINDOW 15' bằng ZSET theo timestamp
        zkey = f"trending:genre:{genre}"
        r.zadd(zkey, {str(mid): ts})
        # cắt bỏ dữ liệu cũ hơn cửa sổ
        r.zremrangebyscore(zkey, 0, ts - TRENDING_WIN_SEC)
        # (tuỳ chọn) đếm số lần xuất hiện trong cửa sổ (hỗ trợ blend)
        r.zincrby(f"trending_cnt:genre:{genre}", 1.0, str(mid))
        r.expire(f"trending_cnt:genre:{genre}", TRENDING_WIN_SEC)

        # ---------- Commit offset SAU khi ghi Redis xong
        c.commit(msg)

if __name__ == "__main__":
    main()
