# franz-go-reproducer

A minimal reproducer for high memory usage in [sarama](https://github.com/IBM/sarama) when a consumer catches up after accumulating lag.

## What it does

- **producer** — publishes 5 MiB records to `test-topic` every 500 ms using zstd compression.
- **consumer** — consumes records with `sarama`, POSTs each record payload to the sink over HTTP, then commits.
- **sink** — a trivial HTTP server that drains the request body, sleeps 250 ms to emulate downstream processing latency, and returns 204.

The consumer is limited to 1 GiB of memory via cgroups (`deploy.resources.limits.memory`). When it catches up after a lag, memory spikes to that limit.

## Prerequisites

- Docker Compose (or [nerdctl](https://github.com/containerd/nerdctl) + containerd)

## Reproduction steps

### 1. Start the stack

```sh
docker compose up -d --build
```

Wait until all services are healthy. You can follow logs with:

```sh
docker compose logs -f consumer
```

The consumer logs a `consumed` line every 10 seconds. Wait until it is actively consuming records.

### 2. Stop the consumer to let lag accumulate

```sh
docker compose stop consumer
```

### 3. Monitor lag

With the consumer stopped, the producer keeps publishing. Use `lag.sh` to watch the lag grow:

```sh
bash lag.sh
```

Wait until the `LAG` column reaches roughly **200** records (~1 GiB of uncompressed payload).

### 4. Restart the consumer

```sh
docker compose start consumer
```

### 5. Observe memory usage

Watch the consumer's memory spike as it catches up:

```sh
docker stats $(docker compose ps -q consumer)
```

The consumer will reach its **1 GiB cgroup memory limit**. Without the bug, memory usage should stay well below that.

## Configuration

Key tunables in `docker-compose.yml`:

| Service  | Flag                       | Default  | Description                          |
|----------|----------------------------|----------|--------------------------------------|
| producer | `-payload-size`            | 5 MiB    | Size of each record value            |
| producer | `-rate`                    | 500 ms   | Interval between produce calls       |
| consumer | `-concurrency`             | 2        | Max concurrent HTTP pushes per partition     |
| consumer | `-fetch-max-bytes`         | 50 MiB   | `Consumer.Fetch.Max` passed to sarama        |
| consumer | `-fetch-default-bytes`     | 10 MiB   | `Consumer.Fetch.Default` passed to sarama    |
| sink     | `-sleep`                   | 250 ms   | Simulated downstream processing time |
