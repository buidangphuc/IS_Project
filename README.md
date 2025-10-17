# 🎬 Realtime Movie Recommender (Lambda Architecture)

**Big Data vibes with Real-time Demo!** 🚀

Recommendation system with Lambda Architecture featuring:
- **🎯 Interactive Demo UI** - Real-time visualization
- **⚡ WebSocket Streaming** - Live updates every second  
- **🔥 Click Generator** - Simulate user behavior
- **📊 3-Panel Dashboard** - Offline vs Realtime vs Blended
- **🏗️ Kafka KRaft 3 brokers** (replication, leader election)
- **💾 Redis** (speed layer: session & trending)
- **🌐 FastAPI** (serving layer with WebSocket)
- **📡 Producer/Consumer** (Python streaming)
- **🎛️ Kafka UI** (observe topics, partitions, consumer groups)

## 🚀 Quick Demo Start

```bash
# Start full demo stack (Docker-based)
make demo
```

Or step by step:

```bash
# 1. Start infrastructure 
make kraft-up

# 2. Train model (if not exists)
make offline  

# 3. Open browser: http://localhost:8000/
```

## 🎮 Demo Features

### 1. **Real-time Dashboard**
- **Offline Panel**: ALS Matrix Factorization recommendations
- **Realtime Panel**: Kafka + Redis trending by genre
- **Blended Panel**: Lambda architecture final output

### 2. **WebSocket Live Updates** 
- Streaming data updates every second
- Real-time event log with timestamps
- Live statistics (events processed, weights, etc.)

### 3. **Interactive Click Generator**
- **Single Click**: Generate one event for specific genre
- **User Session**: Simulate focused user behavior  
- **Auto Mode**: Continuous event stream
- **Manual Controls**: Pick user ID, genre preferences

## 🎯 Demo Usage

```bash
# Generate single click event (via Docker)
make demo-click USER_ID=1 GENRE=Action

# Simulate user session (multiple clicks with genre preference)  
make demo-session USER_ID=1 GENRE=Comedy CLICKS=10

# Continuous event stream (multiple users)
make demo-stream DURATION=60

# Test all API endpoints
make demo-api-test

# Check demo status
make demo-status

# Follow demo logs
make demo-logs
```

### 🌐 Demo URLs
- **Main Demo**: http://localhost:8000/
- **API Health**: http://localhost:8000/health  
- **WebSocket**: ws://localhost:8000/ws
- **Kafka UI**: http://localhost:8080 (if running full stack)

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
