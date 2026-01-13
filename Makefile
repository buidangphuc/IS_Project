.PHONY: help cluster-up cluster-down cluster-status cluster-logs ingest-hdfs benchmark-als start-speed run-recom-api clean

DOCKER_COMPOSE_FILE ?= infra/docker-compose.yml
DC := docker compose -f $(DOCKER_COMPOSE_FILE)

help:
	@echo "🎬 Movie Recommender Big Data Migration - Scalability Benchmark"
	@echo "============================================================"
	@echo "🚀 CLUSTER COMMANDS:"
	@echo "  make cluster-up [N=3]    # Start cluster with N Spark workers (default 3)"
	@echo "  make cluster-down        # Stop and remove all containers"
	@echo "  make cluster-status      # Check status of cluster nodes"
	@echo "  make cluster-logs        # View all cluster logs"
	@echo "  make clean               # Full cleanup (containers + volumes)"
	@echo ""
	@echo "🛠️  WORKFLOW COMMANDS:"
	@echo "  make ingest-data         # 1. Ingest MovieLens data to object storage (MinIO)"
	@echo "  make benchmark-als       # 2. Run Single ALS Training Benchmark"
	@echo "  make benchmark-workers   # 2b. Run ALS Worker Scaling Benchmark (1-3 workers)"
	@echo "  make start-speed         # 3. Start Flink Real-time Speed Layer"
	@echo "  make run-recom-api       # 4. Launch FastAPI Serving Layer"
	@echo ""
	@echo "🌐 DASHBOARDS:"
	@echo "  Spark Master: http://localhost:8081"
	@echo "  Spark Master: http://localhost:8081"
	@echo "  MinIO Console: http://localhost:9001"
	@echo "  Flink UI: http://localhost:8082"
	@echo "  Web App: http://localhost:28000"

# ==============================================================================
# 🏗️ INFRASTRUCTURE
# ==============================================================================

cluster-up:
	@N=$(or $(N),3); \
	echo "🚀 Starting cluster with $$N Spark workers..."; \
	$(DC) up -d --build --remove-orphans --scale spark-worker=$$N
	@echo "✅ Cluster is launching! Access dashboards to monitor status."

cluster-down:
	$(DC) down

cluster-status:
	$(DC) ps
	@echo "\n📊 === MinIO Status ==="
	@$(DC) exec minio curl -s http://localhost:9000/minio/health/live && echo "MinIO is OK" || echo "MinIO is down"

cluster-logs:
	$(DC) logs -f

clean:
	$(DC) down -v
	rm -rf data/processed/* models/als/*

# ==============================================================================
# 🛠️ DATA & JOBS
# ==============================================================================

ingest-data:
	@echo "📥 Ingesting MovieLens data to MinIO..."
	@echo "TODO: Implement MinIO upload script using boto3 or mc"

benchmark-als:
	@echo "🎯 Running ALS Training Benchmark on Spark Cluster..."
	$(DC) exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		--driver-memory 2G \
		--executor-memory 2G \
		--total-executor-cores 6 \
		/app/src/batch_layer/src/jobs/train_als.py
	@echo "✅ Benchmark complete. Check logs for wall-clock time."

benchmark-workers:
	@echo "🎯 Running ALS Worker Scaling Benchmark..."
	@export JAVA_HOME=/opt/homebrew/opt/openjdk@17 && \
	export PATH="$$JAVA_HOME/bin:$$PATH" && \
	python scripts/benchmark_als_workers.py --workers 1 2 3 --output-dir artifacts/benchmarks

start-speed:
	@echo "⚡ Starting Flink Speed Layer..."
	$(DC) exec flink-jobmanager ./bin/flink run -py /app/src/speed_layer/preference_aggregator.py

run-recom-api:
	@echo "🌐 Starting Serving Layer (FastAPI)..."
	cd src && python -m serving_layer.app
