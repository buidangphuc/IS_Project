# Realtime Recommender (Lambda-friendly) — Kafka KRaft + Redis + FastAPI

Big Data / Distributed demo stack featuring:
- **Kafka KRaft 3 brokers** (replication, leader election)
- **Redis** (speed cache: session & trending)
- **FastAPI** (serving layer)
- **Producer/Consumer** (Python)
- **Kafka UI** (observe topics, partitions, consumer groups)

> **Offline training**

---

## 0) Prepare Data

Place MovieLens CSV files in the correct location:
```
artifacts/data_ingestion/movielens_raw/ratings.csv
artifacts/data_ingestion/movielens_raw/movies.csv
```

---

## 1) Bring Up Infrastructure

```bash
make kraft-up
make status        # check service status
# Kafka UI: http://localhost:8080
```

(Optional) Create default topics (RF=3, 12 partitions):
```bash
make topics
```

## 2) Train Offline (ALS/FAISS) Inside Container

```bash
make offline
```

Quick check:
```bash
make show-artifacts
```

After training completes, reload app to load new model:
```bash
make restart-app
```

## 3) Call API

```bash
make api-test USER_ID=1 K=10
# or:
# curl "http://localhost:8000/recommend?user_id=1&k=10"
```

## 4) Demo "Distributed" Behavior

### Scale-out Consumer Group
```bash
make scale-consumer N=3
```

### Chaos / HA (Broker Failure)
```bash
make kill-broker X=2
make start-broker X=2
```

## 5) Logs & Monitoring

```bash
make logs-app
make logs-consumer
make logs-producer
# Kafka UI: http://localhost:8080
```

## 6) Teardown

```bash
make kraft-down
```

### Available Make Targets

```bash
make kraft-up          # Up Kafka KRaft + Redis + App + Producer/Consumer + Kafka UI
make status            # Show containers
make topics            # Create default topics (RF=3, partitions=12)
make offline           # Train offline (inside stream-producer container)
make offline-local     # Train offline on host (requires Java 17 set up)
make restart-app       # Restart FastAPI app (reload model)
make api-test USER_ID=1 K=10  # Call API
make scale-consumer N=3       # Scale out consumer group
make kill-broker X=2 | start-broker X=2
make logs-app | logs-consumer | logs-producer
make kraft-down        # Down & remove volumes
make show-artifacts    # List model artifacts after training
```
