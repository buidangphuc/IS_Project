#!/bin/bash

# Docker-based demo startup script
echo "🎬 Movie Recommender Demo - Docker Stack"
echo "========================================"

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

echo "🔍 Checking prerequisites..."

# Check Docker
if ! command_exists docker; then
    echo "❌ Docker is required"
    exit 1
fi
echo "✅ Docker found"

# Check Docker Compose
if ! command_exists "docker compose" && ! command_exists "docker-compose"; then
    echo "❌ Docker Compose is required"
    exit 1
fi
echo "✅ Docker Compose found"

# Navigate to project directory
cd "$(dirname "$0")/.." || exit 1

echo ""
echo "🚀 Starting Docker demo stack..."
echo "================================"

# Start the main stack
echo "📦 Starting Kafka + Redis + FastAPI..."
docker compose -f infra/docker-compose.kraft.yml up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 10

# Check service status
echo "📊 Service status:"
docker compose -f infra/docker-compose.kraft.yml ps

# Wait for topics to be created
echo "📝 Creating Kafka topics..."
sleep 5
docker compose -f infra/docker-compose.kraft.yml exec -T broker-1 kafka-topics.sh \
  --create --topic events.ratings.v1 --bootstrap-server localhost:9092 --partitions 12 --replication-factor 3 \
  --if-not-exists || echo "Topic already exists"

# Check if model artifacts exist, if not run offline training
if [ ! -f "artifacts/model_trainer/faiss_ivf.index" ]; then
    echo "🎯 Model artifacts not found, running offline training..."
    echo "This may take a few minutes..."
    make offline
    echo "✅ Offline training completed"
    
    # Restart app to load new model
    echo "🔄 Restarting app to load trained model..."
    docker compose -f infra/docker-compose.kraft.yml restart app
    sleep 5
fi

# Start demo producer (optional background events)
echo "🎪 Starting demo event generator..."
docker compose -f infra/docker-compose.kraft.yml --profile demo up -d demo-producer

echo ""
echo "🎯 Demo Stack Ready!"
echo "==================="
echo "🌐 Demo UI: http://localhost:8000/"
echo "🛠️  API Health: http://localhost:8000/health"
echo "📊 Kafka UI: http://localhost:8080/"
echo "� Redis: localhost:6379"
echo ""
echo "💡 Demo Commands (run in new terminal):"
echo "   # Generate single click"
echo "   make demo-click USER_ID=1 GENRE=Action"
echo ""
echo "   # Generate session"  
echo "   make demo-session USER_ID=1 GENRE=Comedy CLICKS=10"
echo ""
echo "   # Test APIs"
echo "   make demo-api-test"
echo ""
echo "🔧 Docker Commands:"
echo "   # View logs"
echo "   docker compose -f infra/docker-compose.kraft.yml logs app"
echo "   docker compose -f infra/docker-compose.kraft.yml logs stream-consumer"
echo ""
echo "   # Stop demo"
echo "   docker compose -f infra/docker-compose.kraft.yml down"
echo ""
echo "Press Ctrl+C to stop monitoring (containers will keep running)..."

# Monitor logs (optional)
trap 'echo ""; echo "ℹ️  Demo containers are still running. Use \"make kraft-down\" to stop."; exit' INT

# Follow app logs
echo "📱 Following app logs (Ctrl+C to exit monitoring):"
docker compose -f infra/docker-compose.kraft.yml logs -f app