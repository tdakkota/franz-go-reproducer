FROM golang:1.26-alpine AS builder

ARG LIBRARY

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /app/producer ./cmd/${LIBRARY}/producer \
 && go build -o /app/consumer ./cmd/${LIBRARY}/consumer

FROM alpine:3.21

COPY --from=builder /app/producer /app/producer
COPY --from=builder /app/consumer /app/consumer
