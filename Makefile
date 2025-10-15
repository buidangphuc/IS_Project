.PHONY: help kraft-up kraft-down status topics offline offline-local restart-app serve seed \
        scale-consumer kill-broker start-broker logs-consumer logs-producer logs-app api-test show-artifacts

DOCKER_COMPOSE_FILE ?= infra/docker-compose.kraft.yml
DC := docker compose -f $(DOCKER_COMPOSE_FILE)

help:
	@echo "Targets:"
	@echo "  make kraft-up          # Up Kafka KRaft + Redis + App + Producer/Consumer + Kafka UI"
	@echo "  make status            # Show containers"
	@echo "  make topics            # Create default topics (RF=3, partitions=12)"
	@echo "  make offline           # Train offline (inside stream-producer container)"
	@echo "  make offline-local     # Train offline on host (requires Java 17 set up)"
	@echo "  make restart-app       # Restart FastAPI app (reload model)"
	@echo "  make api-test USER_ID=1 K=10  # Call API"
	@echo "  make scale-consumer N=3       # Scale out consumer group"
	@echo "  make kill-broker X=2 | start-broker X=2"
	@echo "  make logs-app | logs-consumer | logs-producer"
	@echo "  make kraft-down        # Down & remove volumes"
	@echo "  make show-artifacts    # List model artifacts after training"

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