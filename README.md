# 🎬 Real-time Movie Recommender (Lambda Architecture)

**Big Data vibes with Real-time Demo!** 🚀

A comprehensive recommendation system showcasing Lambda Architecture with:
- **🎯 Interactive Demo UI** - Real-time visualization
- **⚡ WebSocket Streaming** - Live updates every second
- **🏗️ Kafka KRaft** - 3-broker cluster with replication
- **💾 Redis** - Speed layer for session & trending data
- **🌐 FastAPI** - Serving layer with WebSocket support
- **🧩 Click Generator** - Simulation of realistic user traffic

## 🏗️ System Architecture

```mermaid
graph TD
    %% Data Sources
    User((User/Generator)) -->|Ratings| Kafka{Kafka Broker}
    
    %% Speed Layer
    Kafka -->|Stream| Flink[Flink Cluster\n(Speed Layer)]
    Flink -->|Trending Stats| Redis[(Redis\nCache)]
    
    %% Batch Layer
    Kafka -->|Store| HDFS[(HDFS\nData Lake)]
    HDFS -->|Batch Read| Spark[Spark Cluster\n(Batch Layer)]
    Spark -->|Personalized Recs| Redis
    
    %% Serving Layer
    Redis -->|Fetch Data| Streamlit[Streamlit Dashboard\n(Serving Layer)]
    Streamlit -->|View| User
```

---

## 🚀 Quick Start (One Command)

Start the entire stack, including infrastructure, data download, usage training, and the demo UI:

```bash
make demo
```

**That's it!** The script will:
1.  Check for prerequisites (Docker, Docker Compose).
2.  **Automatically download** the MovieLens Small dataset.
3.  Start the big data infrastructure (Kafka, Redis, etc.).
4.  Train the offline model (ALS) if needed.
5.  Launch the web application.

### 🌐 Access Points
- **Demo UI**: http://localhost:8000/
- **Kafka UI**: http://localhost:8080/ (Cluster monitoring)
- **API Docs**: http://localhost:8000/docs

---

## 🎮 Demo Features & Controls

Once the demo is running, you can use these commands in a **new terminal** to simulate traffic:

| Action | Command | Description |
| :--- | :--- | :--- |
| **Simulate Click** | `make demo-click USER_ID=1 GENRE=Action` | Send a single event |
| **Simulate Session** | `make demo-session USER_ID=1 GENRE=Comedy` | Simulate a user browsing a specific genre |
| **Stream Traffic** | `make demo-stream` | Continuous random traffic from multiple users |
| **Check Logs** | `make demo-logs` | Tail the application logs |
| **Stop Demo** | `make kraft-down` | Stop and remove all containers |

### Dashboard Panels
1.  **Offline Panel**: Recommendations based on historical data (batch processing).
2.  **Realtime Panel**: Trending items based on live stream data (speed layer).
3.  **Blended Panel**: The final recommendation combining both layers.

---

## 🛠️ Advanced Usage

For granular control, you can run individual steps:

### 1. Infrastructure Only
```bash
make kraft-up
```

### 2. Manual Model Training
```bash
make offline
```

### 3. Scale Consumer Group
Demonstrate consumer group rebalancing by adding more consumers:
```bash
make scale-consumer N=3
```

### 4. Chaos Engineering
Simulate broker failure to test fault tolerance:
```bash
make kill-broker X=2
# Wait a moment, then restore:
make start-broker X=2
```

---

## 📂 Project Structure

- **`infra/`**: Docker Compose files for Kafka and Big Data stack.
- **`src/`**: Source code for common modules.
- **`batch_layer/`**: Spark jobs for offline training.
- **`speed_layer/`**: Flink/Python streaming jobs.
- **`serving_layer/`**: FastAPI application.
- **`artifacts/`**: Data and model storage (auto-generated).

## 📝 Prerequisites
- Docker & Docker Compose
- Make (optional, but recommended)
- curl & unzip (for data download script)

docker compose -f infra/docker-compose.yml up -d --build
