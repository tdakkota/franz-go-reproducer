#!/usr/bin/env bash
set -euo pipefail

BROKER="${BROKER:-kafka:9092}"
TOPIC="${TOPIC:-test-topic}"
GROUP="${GROUP:-reproducer}"

nerdctl compose exec kafka \
  kafka-consumer-groups \
    --bootstrap-server "$BROKER" \
    --group "$GROUP" \
    --describe
