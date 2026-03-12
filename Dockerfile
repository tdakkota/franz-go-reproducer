FROM golang:1.26-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /app/producer ./cmd/producer \
 && go build -o /app/consumer ./cmd/consumer \
 && go build -o /app/sink ./cmd/sink

FROM alpine:3.21

COPY --from=builder /app/producer /app/producer
COPY --from=builder /app/consumer /app/consumer
COPY --from=builder /app/sink /app/sink
