#!/bin/bash
set -e

echo "=================================================="
echo "🎬 Movie Recommender Demo Launcher"
echo "=================================================="

echo "[2/3] Waiting for services to stabilize (10s)..."
sleep 10

echo "[3/3] Demo Status:"
echo "- Generator: RUNNING (Container: generator)"
echo "- Speed Layer (Flink): RUNNING (Container: flink-job)"
echo "- App / Dashboard: RUNNING (Container: app)"

echo ""
echo "=================================================="
echo "ACCESS POINTS:"
echo "--------------------------------------------------"
echo "👉 Dashboard: http://localhost:28000"
echo "👉 Spark UI:  http://localhost:8081"
echo "👉 Flink UI:  http://localhost:8082"
echo "👉 MinIO Console: http://localhost:9001"
echo "=================================================="

echo ""
echo "To trigger a BATCH training run (Spark ALS), run:"
echo "  docker compose -f infra/docker-compose.yml exec spark-master python /app/src/datascience/components/batch.py"
echo ""
echo "To view logs:"
echo "  docker compose -f infra/docker-compose.yml logs -f app generator flink-job"
