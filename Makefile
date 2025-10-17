.PHONY: help kraft-up kraft-down status topics offline offline-local restart-app serve seed \
        scale-consumer kill-broker start-broker logs-consumer logs-producer logs-app api-test show-artifacts \
        demo demo-install demo-click demo-session demo-stream demo-stop

DOCKER_COMPOSE_FILE ?= infra/docker-compose.kraft.yml
DC := docker compose -f $(DOCKER_COMPOSE_FILE)

help:
	@echo "🎬 Movie Recommender Demo - Available Commands:"
	@echo "=============================================="
	@echo ""
	@echo "🚀 DEMO COMMANDS:"
	@echo "  make demo              # Start full Docker demo stack"
	@echo "  make demo-quick        # Quick demo start (if stack running)"
	@echo "  make demo-click USER_ID=1 GENRE=Action    # Generate single click"
	@echo "  make demo-session USER_ID=1 GENRE=Comedy  # Simulate user session"
	@echo "  make demo-stream DURATION=60              # Continuous event stream"
	@echo "  make demo-api-test     # Test all API endpoints"
	@echo "  make demo-status       # Check demo stack status"
	@echo "  make demo-logs         # Follow app logs"
	@echo ""
	@echo "🏗️ INFRASTRUCTURE:"
	@echo "  make kraft-up          # Up Kafka KRaft + Redis + App + Producer/Consumer + Kafka UI"
	@echo "  make kraft-down        # Down & remove volumes"
	@echo "  make status            # Show containers"
	@echo "  make topics            # Create default topics (RF=3, partitions=12)"
	@echo ""
	@echo "🎯 TRAINING & TESTING:"
	@echo "  make offline           # Train offline (inside stream-producer container)"
	@echo "  make offline-local     # Train offline on host (requires Java 17 set up)"
	@echo "  make restart-app       # Restart FastAPI app (reload model)"
	@echo "  make api-test USER_ID=1 K=10  # Call API"
	@echo "  make show-artifacts    # List model artifacts after training"
	@echo ""
	@echo "🔧 OPERATIONS:"
	@echo "  make scale-consumer N=3       # Scale out consumer group"
	@echo "  make kill-broker X=2 | start-broker X=2"
	@echo "  make logs-app | logs-consumer | logs-producer"

# Bring up 3-broker KRaft cluster + app + producer/consumer + redis + schema-registry + kafka-ui
kraft-up:
	$(DC) up -d --build

kraft-down:
	$(DC) down -v

status:
	$(DC) ps

# Create topics with RF=3, partitions=12 (uses your helper script)
topics:
	chmod +x scripts/create_topics.sh
	./scripts/create_topics.sh

# ===== Offline training (RECOMMENDED): run inside stream-producer container =====
offline: offline-in-docker

offline-in-docker:
	$(DC) exec stream-producer bash -lc '\
		echo "JAVA:" && java -version && \
		export PYSPARK_PYTHON=$$(which python) && \
		export PYSPARK_DRIVER_PYTHON=$$(which python) && \
		export PYSPARK_SUBMIT_ARGS="--conf spark.master=local[*] --conf spark.driver.memory=2g pyspark-shell" && \
		export JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70" && \
		PYTHONPATH=src APP_CONFIG=config/config.docker.kraft.yaml python -m datascience.pipeline.offline \
	'

# ===== Offline training on HOST (requires Java 17 correctly configured) =====
offline-local:
	JAVA_HOME=$$CONDA_PREFIX PATH=$$CONDA_PREFIX/bin:$$PATH \
	PYSPARK_PYTHON=$$(which python) PYSPARK_DRIVER_PYTHON=$$(which python) \
	PYTHONPATH=src APP_CONFIG=config/config.docker.kraft.yaml \
	python -m datascience.pipeline.offline

# Run API locally (optional if you prefer the container 'app')
serve:
	PYTHONPATH=src APP_CONFIG=config/config.yaml uvicorn app:app --reload --host 0.0.0.0 --port 8000

# Seed stream by running the producer (container already auto-runs after parquet exists)
seed:
	$(DC) run --rm stream-producer

# Scale out consumers (show rebalance)
scale-consumer:
	@if [ -z "$(N)" ]; then echo "Usage: make scale-consumer N=3"; exit 1; fi
	$(DC) up -d --scale stream-consumer=$(N)

# Kill a broker to demo HA (X=1|2|3)
kill-broker:
	@if [ -z "$(X)" ]; then echo "Usage: make kill-broker X=2"; exit 1; fi
	docker stop broker-$(X)

# Restart a broker (X=1|2|3)
start-broker:
	@if [ -z "$(X)" ]; then echo "Usage: make start-broker X=2"; exit 1; fi
	docker start broker-$(X)

logs-consumer:
	$(DC) logs -f stream-consumer

logs-producer:
	$(DC) logs -f stream-producer

logs-app:
	$(DC) logs -f app

restart-app:
	$(DC) restart app

api-test:
	@if [ -z "$(USER_ID)" ]; then echo "Usage: make api-test USER_ID=1 K=10"; exit 1; fi
	@if [ -z "$(K)" ]; then echo "Usage: make api-test USER_ID=1 K=10"; exit 1; fi
	curl -s "http://localhost:8000/recommend?user_id=$(USER_ID)&k=$(K)"

show-artifacts:
	$(DC) exec stream-producer bash -lc 'ls -lh artifacts/model_trainer || true && ls -lh artifacts/data_transformation/lake || true'

# ===== DEMO COMMANDS =====
demo:
	@echo "🎬 Starting Docker-based Real-time Movie Recommender Demo"
	@echo "🚀 This will start the full Docker stack with demo features"
	@echo ""
	./scripts/quick_demo.sh

demo-quick:
	@echo "� Quick demo start (assumes stack is running)"
	@echo "🌐 Demo UI: http://localhost:8000/"
	$(DC) up -d --profile demo

demo-click:
	@echo "📱 Generating single click event via Docker..."
	@if [ -z "$(USER_ID)" ]; then echo "Usage: make demo-click USER_ID=1 GENRE=Action"; exit 1; fi
	@if [ -z "$(GENRE)" ]; then echo "Usage: make demo-click USER_ID=1 GENRE=Action"; exit 1; fi
	$(DC) exec stream-producer python scripts/click_generator.py --mode single --user-id $(USER_ID) --genre $(GENRE)

demo-session:
	@echo "🎯 Simulating user session via Docker..."
	@if [ -z "$(USER_ID)" ]; then echo "Usage: make demo-session USER_ID=1 GENRE=Comedy CLICKS=10"; exit 1; fi
	@if [ -z "$(GENRE)" ]; then echo "Usage: make demo-session USER_ID=1 GENRE=Comedy CLICKS=10"; exit 1; fi
	$(DC) exec stream-producer python scripts/click_generator.py --mode session --user-id $(USER_ID) --genre $(GENRE) --clicks $(if $(CLICKS),$(CLICKS),5)

demo-stream:
	@echo "🌊 Starting continuous event stream via Docker..."
	$(DC) exec stream-producer python scripts/click_generator.py --mode stream --duration $(if $(DURATION),$(DURATION),60) --users 1 2 3 4 5

demo-api-test:
	@echo "🧪 Testing API endpoints..."
	@echo "Health check:"
	curl -s http://localhost:8000/health | python -m json.tool || echo "API not ready"
	@echo "\nOffline recommendations:"
	curl -s "http://localhost:8000/recommend-offline?user_id=1&k=5" | python -m json.tool || echo "API not ready"
	@echo "\nRealtime recommendations:"
	curl -s "http://localhost:8000/realtime-only?user_id=1&k=5" | python -m json.tool || echo "API not ready" 
	@echo "\nBlended recommendations:"
	curl -s "http://localhost:8000/recommend?user_id=1&k=5" | python -m json.tool || echo "API not ready"

demo-logs:
	@echo "📱 Following demo logs..."
	$(DC) logs -f app

demo-status:
	@echo "📊 Demo stack status:"
	$(DC) ps
	@echo ""
	@echo "🌐 Demo URLs:"
	@echo "  - Demo UI: http://localhost:8000/"
	@echo "  - Kafka UI: http://localhost:8080/"
	@echo "  - API Health: http://localhost:8000/health"