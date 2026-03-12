#!/usr/bin/env bash
set -euo pipefail

BROKER="${BROKER:-kafka:9092}"
TOPIC="${TOPIC:-test-topic}"
GROUP="${GROUP:-reproducer}"
DOCKER="${DOCKER:-docker}"

$DOCKER compose exec kafka \
  kafka-consumer-groups \
    --bootstrap-server "$BROKER" \
    --group "$GROUP" \
    --describe
