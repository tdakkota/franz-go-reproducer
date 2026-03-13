FROM golang:1.26-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /app/producer ./cmd/producer \
 && go build -o /app/consumer ./cmd/consumer \
 && go build -o /app/sink ./cmd/sink

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/producer /app/producer
COPY --from=builder /app/consumer /app/consumer
COPY --from=builder /app/sink /app/sink
