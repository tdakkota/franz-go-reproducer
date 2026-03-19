package testbed_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/dustin/go-humanize"
	tc "github.com/testcontainers/testcontainers-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// producerMemoryLimit is a fixed cap for the producer container.
// The producer is not the OOM subject; only the consumer needs a tunable limit.
const producerMemoryLimit = 256 * 1024 * 1024 // 256 MiB

// config holds all tunable parameters for the testbed.
type config struct {
	payloadSize            string
	produceRate            string
	fetchMaxBytes          string
	fetchMaxPartitionBytes string
	concurrency            int
	consumerSleep          string
	lagTarget              int64
	consumerMemoryLimit    int64
	reproTimeout           time.Duration
	expectOOM              bool
}

func loadConfig() (config, error) {
	c := config{
		payloadSize:            "5MiB",
		produceRate:            "500ms",
		fetchMaxBytes:          "50MiB",
		fetchMaxPartitionBytes: "10MiB",
		concurrency:            2,
		consumerSleep:          "250ms",
		lagTarget:              200,
		consumerMemoryLimit:    256 * 1024 * 1024, // 256 MiB
		reproTimeout:           5 * time.Minute,
		expectOOM:              true,
	}

	if v := os.Getenv("PAYLOAD_SIZE"); v != "" {
		c.payloadSize = v
	}
	if v := os.Getenv("PRODUCE_RATE"); v != "" {
		c.produceRate = v
	}
	if v := os.Getenv("FETCH_MAX_BYTES"); v != "" {
		c.fetchMaxBytes = v
	}
	if v := os.Getenv("FETCH_MAX_PARTITION_BYTES"); v != "" {
		c.fetchMaxPartitionBytes = v
	}
	if v := os.Getenv("CONCURRENCY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("CONCURRENCY: %w", err)
		}
		c.concurrency = n
	}
	if v := os.Getenv("CONSUMER_SLEEP"); v != "" {
		c.consumerSleep = v
	}
	if v := os.Getenv("LAG_TARGET"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return c, fmt.Errorf("LAG_TARGET: %w", err)
		}
		c.lagTarget = n
	}
	if v := os.Getenv("MEMORY_LIMIT"); v != "" {
		b, err := humanize.ParseBytes(v)
		if err != nil {
			return c, fmt.Errorf("MEMORY_LIMIT: %w", err)
		}
		c.consumerMemoryLimit = int64(b)
	}
	if v := os.Getenv("REPRO_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("REPRO_TIMEOUT: %w", err)
		}
		c.reproTimeout = d
	}
	if v := os.Getenv("EXPECT_OOM"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return c, fmt.Errorf("EXPECT_OOM: %w", err)
		}
		c.expectOOM = b
	}
	return c, nil
}

func ptr[T any](v T) *T { return &v }

var libraries = []struct {
	name       string
	dockerfile string
	buildArgs  map[string]*string
}{
	{"franz-go", "Dockerfile", map[string]*string{"LIBRARY": ptr("franz-go")}},
	{"sarama", "Dockerfile", map[string]*string{"LIBRARY": ptr("sarama")}},
	{"segmentio", "Dockerfile", map[string]*string{"LIBRARY": ptr("segmentio")}},
	{"confluent", "Dockerfile.confluent", nil},
}

func TestOOMReproducer(t *testing.T) {
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	for _, lib := range libraries {
		t.Run(lib.name, func(t *testing.T) {
			runOOMTest(t, cfg, lib.dockerfile, lib.buildArgs)
		})
	}
}

func runOOMTest(t *testing.T, cfg config, dockerfile string, buildArgs map[string]*string) {
	t.Helper()

	// t.Context() is cancelled when the test ends, automatically unblocking any
	// in-flight operations (lag polling, etc.) on early failure.
	ctx := t.Context()

	// Phase 0 — create shared network and start Kafka (KRaft, no Zookeeper).
	//
	// Networking design:
	//   - All three containers (kafka, producer, consumer) share one user-defined
	//     Docker network so they can reach each other by alias.
	//   - The kafka module's startup script sets KAFKA_ADVERTISED_LISTENERS to
	//     "PLAINTEXT://<host-mapped-addr>,BROKER://<hostname>:9092".
	//     By pinning the Kafka container's hostname to "kafka" we make the BROKER
	//     advertised address "kafka:9092", which producer/consumer (on the same
	//     network) can resolve via Docker's embedded DNS.
	//   - Producer and consumer connect to "kafka:9092" (the BROKER listener).
	//     Kafka's BROKER listener is PLAINTEXT so regular clients work fine.
	//   - The test process itself uses the host-mapped port (from kafkaCtr.Brokers)
	//     for kadm lag polling.
	//
	// Cleanup order: t.Cleanup is LIFO, so the network cleanup is registered first
	// and therefore runs last — after all containers have been terminated.
	t.Log("Phase 0: creating network and starting Kafka")

	nw, err := network.New(ctx)
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	// Registered first → executes last (LIFO): all containers are gone by this point.
	t.Cleanup(func() {
		if err := nw.Remove(context.Background()); err != nil {
			t.Logf("remove network: %v", err)
		}
	})

	kafkaCtr, err := kafkamodule.Run(ctx, "confluentinc/confluent-local:7.7.3",
		// Attach to the shared network with alias "kafka".
		// configureControllerQuorumVoters (inside the module) will pick up the
		// alias and set KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9094.
		network.WithNetwork([]string{"kafka"}, nw),
		// Pin the container hostname so the BROKER advertised listener becomes
		// "kafka:9092" — resolvable by producer/consumer on the same network.
		tc.WithConfigModifier(func(cfg *container.Config) {
			cfg.Hostname = "kafka"
		}),
		// Raise broker-side message size limits to match the producer payload.
		tc.WithEnv(map[string]string{
			"KAFKA_MESSAGE_MAX_BYTES":       "104857675",
			"KAFKA_REPLICA_FETCH_MAX_BYTES": "104857675",
		}),
	)
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() {
		if err := kafkaCtr.Terminate(context.Background()); err != nil {
			t.Logf("terminate kafka: %v", err)
		}
	})

	// brokerAddr is the host-mapped address used by the test process for kadm.
	brokers, err := kafkaCtr.Brokers(ctx)
	if err != nil {
		t.Fatalf("get brokers: %v", err)
	}
	brokerAddr := brokers[0]
	t.Logf("Kafka broker (host-mapped): %s", brokerAddr)

	// Extract just the port for use in container broker arguments.
	_, brokerPort, err := net.SplitHostPort(brokerAddr)
	if err != nil {
		t.Fatalf("parse broker addr %q: %v", brokerAddr, err)
	}
	_ = brokerPort // containers use "kafka:9092" directly; kept for clarity

	// Create the topic.
	kgoClient, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	if err != nil {
		t.Fatalf("create kgo client: %v", err)
	}
	adm := kadm.NewClient(kgoClient)
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, "test-topic"); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	kgoClient.Close()

	// fromDockerfile is shared; KeepImage=true so the image is built once and
	// reused across producer and consumer container starts/restarts.
	fromDockerfile := tc.FromDockerfile{
		Context:    ".",
		Dockerfile: dockerfile,
		BuildArgs:  buildArgs,
		KeepImage:  true,
	}

	// Phase 1 — start producer.
	t.Log("Phase 1: starting producer")
	producerCtr, err := tc.Run(ctx, "",
		tc.WithDockerfile(fromDockerfile),
		network.WithNetwork([]string{}, nw),
		tc.CustomizeRequestOption(func(req *tc.GenericContainerRequest) error {
			req.Cmd = []string{
				"/app/producer",
				"-brokers=kafka:9092",
				"-topic=test-topic",
				"-payload-size=" + cfg.payloadSize,
				"-rate=" + cfg.produceRate,
				"-pprof-addr=",
			}
			req.WaitingFor = wait.ForLog("producer started")
			return nil
		}),
		tc.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.Resources.Memory = producerMemoryLimit
		}),
	)
	if err != nil {
		t.Fatalf("start producer: %v", err)
	}
	t.Cleanup(func() {
		if err := producerCtr.Terminate(context.Background()); err != nil {
			t.Logf("terminate producer: %v", err)
		}
	})
	t.Log("Producer started")

	// Phase 2 — start consumer and wait until it actually processes a record.
	//
	// We wait for "sent request" rather than "consumed": "sent request" is logged
	// immediately after each record is processed (one log per record), while
	// "consumed" only appears in the 10-second stats ticker.  Waiting for
	// "sent request" guarantees the consumer has connected to Kafka AND received
	// at least one record — a much more reliable connectivity check.
	t.Log("Phase 2: starting consumer")
	consumerCtr, err := tc.Run(ctx, "",
		tc.WithDockerfile(fromDockerfile),
		network.WithNetwork([]string{}, nw),
		tc.CustomizeRequestOption(func(req *tc.GenericContainerRequest) error {
			req.Cmd = []string{
				"/app/consumer",
				"-brokers=kafka:9092",
				"-topic=test-topic",
				"-group=reproducer",
				"-concurrency=" + strconv.Itoa(cfg.concurrency),
				"-sleep=" + cfg.consumerSleep,
				"-fetch-max-bytes=" + cfg.fetchMaxBytes,
				"-fetch-max-partition-bytes=" + cfg.fetchMaxPartitionBytes,
				"-pprof-addr=",
			}
			// "sent request" is logged once per processed record — ensures the
			// consumer has actually connected and consumed before we proceed.
			req.WaitingFor = wait.ForLog("sent request")
			return nil
		}),
		tc.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.Resources.Memory = cfg.consumerMemoryLimit
		}),
	)
	if err != nil {
		t.Fatalf("start consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := consumerCtr.Terminate(context.Background()); err != nil {
			t.Logf("terminate consumer: %v", err)
		}
	})

	consumerID := consumerCtr.GetContainerID()
	t.Logf("Consumer started and confirmed consuming (id=%s)", consumerID)

	// Subscribe to OOM events NOW — before we stop the consumer — so the stream
	// is established and there is no race window around the restart in Phase 4.
	dockerCli, err := tc.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatalf("create docker client: %v", err)
	}
	defer dockerCli.Close()

	oomCtx, oomCancel := context.WithCancel(ctx)
	defer oomCancel()

	oomFilter := filters.NewArgs(
		filters.Arg("type", "container"),
		filters.Arg("action", string(events.ActionOOM)),
		filters.Arg("container", consumerID),
	)
	eventsCh, errsCh := dockerCli.Events(oomCtx, events.ListOptions{Filters: oomFilter})

	oomCh := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-oomCtx.Done():
				return
			case err := <-errsCh:
				if err != nil && oomCtx.Err() == nil {
					t.Logf("docker events error: %v", err)
				}
				return
			case ev := <-eventsCh:
				if ev.Action == events.ActionOOM {
					t.Logf("OOM event received for container %s", ev.Actor.ID)
					select {
					case oomCh <- struct{}{}:
					default:
					}
				}
			}
		}
	}()

	// Phase 3 — stop consumer, wait for lag to build up.
	t.Log("Phase 3: stopping consumer")
	stopTimeout := 10 * time.Second
	if err := consumerCtr.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop consumer: %v", err)
	}
	t.Logf("Consumer stopped; waiting for lag >= %d", cfg.lagTarget)

	lagClient, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	if err != nil {
		t.Fatalf("create lag kgo client: %v", err)
	}
	lagAdm := kadm.NewClient(lagClient)
	defer lagClient.Close()

	if err := waitForLag(ctx, lagAdm, "reproducer", cfg.lagTarget, t); err != nil {
		t.Fatalf("wait for lag: %v", err)
	}
	t.Logf("Lag target reached (>= %d)", cfg.lagTarget)

	// Phase 4 — restart consumer (OOM event stream already established above).
	t.Log("Phase 4: restarting consumer")
	if err := consumerCtr.Start(ctx); err != nil {
		t.Fatalf("restart consumer: %v", err)
	}
	t.Log("Consumer restarted; waiting for OOM or timeout")

	// Phase 5 — detect OOM or timeout.
	select {
	case <-oomCh:
		oomCancel()
		t.Log("OOM reproduced successfully")
	case <-time.After(cfg.reproTimeout):
		oomCancel()
		if cfg.expectOOM {
			t.Fatalf("no OOM detected within %s (EXPECT_OOM=true)", cfg.reproTimeout)
		} else {
			t.Logf("no OOM within %s (EXPECT_OOM=false, test passes)", cfg.reproTimeout)
		}
	}
}

// waitForLag polls kadm every 2s until total group lag >= target.
func waitForLag(ctx context.Context, adm *kadm.Client, group string, target int64, t *testing.T) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			lags, err := adm.Lag(ctx, group)
			if err != nil {
				t.Logf("lag poll error: %v", err)
				continue
			}
			gl, ok := lags[group]
			if !ok {
				continue
			}
			total := gl.Lag.Total()
			t.Logf("current lag: %d / %d", total, target)
			if total >= target {
				return nil
			}
		}
	}
}
