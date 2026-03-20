package testbed_test

import (
	"context"
	"fmt"
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
	payloadFile            string // path to a pre-generated payload file; overrides payloadSize
	payloadSize            string
	produceRate            string
	fetchMaxBytes          string
	fetchMaxPartitionBytes string
	concurrency            int
	consumerSleep          string
	backlogTarget          int64
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
		backlogTarget:          200,
		consumerMemoryLimit:    256 * 1024 * 1024, // 256 MiB
		reproTimeout:           time.Minute,
		expectOOM:              true,
	}

	if v := os.Getenv("PAYLOAD_FILE"); v != "" {
		c.payloadFile = v
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
	if v := os.Getenv("BACKLOG_TARGET"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return c, fmt.Errorf("BACKLOG_TARGET: %w", err)
		}
		c.backlogTarget = n
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

var libraries = []struct {
	name       string
	dockerfile string
	buildArgs  map[string]*string
}{
	{"franz-go", "Dockerfile", map[string]*string{"LIBRARY": new("franz-go")}},
	{"sarama", "Dockerfile", map[string]*string{"LIBRARY": new("sarama")}},
	{"segmentio", "Dockerfile", map[string]*string{"LIBRARY": new("segmentio")}},
	{"confluent", "Dockerfile.confluent", nil},
}

func TestOOMReproducer(t *testing.T) {
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	for _, lib := range libraries {
		t.Run(lib.name, func(t *testing.T) {
			runOOMTest(t, cfg, lib.name, lib.dockerfile, lib.buildArgs)
		})
	}
}

func runOOMTest(t *testing.T, cfg config, name, dockerfile string, buildArgs map[string]*string) {
	t.Helper()

	// t.Context() is cancelled when the test ends, automatically unblocking any
	// in-flight operations (lag polling, etc.) on early failure.
	ctx := t.Context()

	// withName returns an option that sets the Docker container name to
	// "<library>-<role>" (e.g. "franz-go-kafka", "sarama-consumer").
	// Consistent names make docker ps / docker logs much easier to read.
	withName := func(role string) tc.CustomizeRequestOption {
		return tc.CustomizeRequestOption(func(req *tc.GenericContainerRequest) error {
			req.Name = name + "-" + role
			return nil
		})
	}

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

	kafkaAlias := name + "-kafka"
	kafkaCtr, err := kafkamodule.Run(ctx, "confluentinc/confluent-local:7.7.3",
		withName("kafka"),
		// Attach to the shared network with alias "<library>-kafka".
		// configureControllerQuorumVoters (inside the module) will pick up the
		// alias and set KAFKA_CONTROLLER_QUORUM_VOTERS=1@<alias>:9094.
		network.WithNetwork([]string{kafkaAlias}, nw),
		// Pin the container hostname so the BROKER advertised listener becomes
		// "<alias>:9092" — resolvable by producer/consumer on the same network.
		tc.WithConfigModifier(func(cfg *container.Config) {
			cfg.Hostname = kafkaAlias
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
		Repo:       "testbed",
		Tag:        name,
		KeepImage:  true,
	}

	// Phase 1 — start producer.
	t.Log("Phase 1: starting producer")

	// Build producer command: use payload file if provided, otherwise generated size.
	producerCmd := []string{
		"/app/producer",
		"-brokers=" + kafkaAlias + ":9092",
		"-topic=test-topic",
		"-rate=" + cfg.produceRate,
		"-pprof-addr=",
	}
	var producerFileOpt tc.ContainerCustomizer
	if cfg.payloadFile != "" {
		producerCmd = append(producerCmd, "-payload-file=/payload")
		producerFileOpt = tc.WithFiles(tc.ContainerFile{
			HostFilePath:      cfg.payloadFile,
			ContainerFilePath: "/payload",
			FileMode:          0o444,
		})
	} else {
		producerCmd = append(producerCmd, "-payload-size="+cfg.payloadSize)
		producerFileOpt = tc.CustomizeRequestOption(func(_ *tc.GenericContainerRequest) error { return nil })
	}

	producerCtr, err := tc.Run(ctx, "",
		tc.WithDockerfile(fromDockerfile),
		withName("producer"),
		network.WithNetwork([]string{}, nw),
		producerFileOpt,
		tc.CustomizeRequestOption(func(req *tc.GenericContainerRequest) error {
			req.Cmd = producerCmd
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
		withName("consumer"),
		network.WithNetwork([]string{}, nw),
		tc.CustomizeRequestOption(func(req *tc.GenericContainerRequest) error {
			req.Cmd = []string{
				"/app/consumer",
				"-brokers=" + kafkaAlias + ":9092",
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
	t.Logf("Consumer stopped; waiting for producer backlog >= %d", cfg.backlogTarget)

	lagClient, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	if err != nil {
		t.Fatalf("create lag kgo client: %v", err)
	}
	lagAdm := kadm.NewClient(lagClient)
	defer lagClient.Close()

	if err := waitForProducerBacklog(ctx, lagAdm, "test-topic", cfg.backlogTarget, t); err != nil {
		t.Fatalf("wait for backlog: %v", err)
	}
	t.Logf("Producer backlog reached (>= %d)", cfg.backlogTarget)

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
		t.Logf("OOM reproduced successfully (%s)", name)
	case <-time.After(cfg.reproTimeout):
		oomCancel()
		if cfg.expectOOM {
			t.Fatalf("no OOM detected within %s (EXPECT_OOM=true)", cfg.reproTimeout)
		} else {
			t.Logf("no OOM within %s (EXPECT_OOM=false, test passes)", cfg.reproTimeout)
		}
	}
}

// waitForProducerBacklog polls the topic end offset every 2s until it has
// grown by at least target messages since the first successful sample.
//
// Using end-offset growth instead of consumer group lag avoids false failures
// caused by libraries that never commit offsets (sarama with AutoCommit=false)
// or by the consumer group transitioning to Empty state after session timeout,
// both of which make kadm.Lag report 0 even when the topic has a large backlog.
func waitForProducerBacklog(ctx context.Context, adm *kadm.Client, topic string, target int64, t *testing.T) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var baseline int64 = -1
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			listed, err := adm.ListEndOffsets(ctx, topic)
			if err != nil {
				t.Logf("end offset poll error: %v", err)
				continue
			}
			var total int64
			listed.Each(func(lo kadm.ListedOffset) {
				if lo.Err == nil {
					total += lo.Offset
				}
			})
			if baseline < 0 {
				baseline = total
			}
			growth := total - baseline
			t.Logf("end offset growth: %d / %d", growth, target)
			if growth >= target {
				return nil
			}
		}
	}
}
