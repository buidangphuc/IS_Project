#!/usr/bin/env bash
set -euo pipefail
# Create topics with RF=3, partitions=12 on KRaft cluster
BROKER=${BROKER:-broker-1:9092}
PARTS=${PARTS:-12}
RF=${RF:-3}

topics=( "events.ratings.v1" "events.clicks.v1" "events.impressions.v1" )
for t in "${topics[@]}"; do
  echo "Creating topic $t ..."
  docker compose -f infra/docker-compose.kraft.yml exec -T broker-1 \
    kafka-topics.sh --create --if-not-exists \
      --topic "$t" \
      --bootstrap-server "$BROKER" \
      --replication-factor "$RF" \
      --partitions "$PARTS" || true
done
echo "Done."
