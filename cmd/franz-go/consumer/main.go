package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/tdakkota/franz-go-reproducer/internal/apputil"
	consumer "github.com/tdakkota/franz-go-reproducer/internal/consumer"
	"github.com/tdakkota/franz-go-reproducer/internal/flagutil"
)

func consume(ctx context.Context, cl *kgo.Client, concurrency int, sleep time.Duration, sinkURL string, httpClient *http.Client, logger *zap.Logger, m *consumer.Metrics) error {
	for {
		select {
		case <-ctx.Done():
			logger.Info("consumer stopped",
				zap.Int64("consumed_records", m.Consumed()),
				zap.String("consumed_bytes", humanize.IBytes(uint64(m.ConsumedBytes()))),
			)
			return nil
		default:
		}

		fetches := cl.PollRecords(ctx, concurrency)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				logger.Error("fetch error", zap.Error(e.Err))
			}
		}

		if fetches.Empty() {
			continue
		}

		records := fetches.Records()

		var batchBytes int64
		for _, rec := range records {
			batchBytes += int64(len(rec.Value))
		}

		grp, grpCtx := errgroup.WithContext(ctx)
		grp.SetLimit(concurrency)

		for _, rec := range records {
			grp.Go(func() error {
				defer func() {
					logger.Info("sent request", zap.Int64("offset", rec.Offset))
				}()
				return consumer.ProcessRecord(grpCtx, rec.Value, sleep, sinkURL, httpClient, m)
			})
		}

		if err := grp.Wait(); err != nil {
			logger.Error("process error", zap.Error(err))
			continue
		}

		// Commit with the original ctx, not the errgroup ctx,
		// to avoid cancellation races on errgroup completion.
		if err := cl.CommitRecords(ctx, records...); err != nil {
			logger.Error("commit error", zap.Error(err))
		}

		m.Add(int64(len(records)), batchBytes)
	}
}

type readHook struct {
	logger *zap.Logger
}

var _ kgo.HookFetchBatchRead = (*readHook)(nil)

func (h *readHook) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, partition int32, metrics kgo.FetchBatchMetrics) {
	h.logger.Info("fetch batch read",
		zap.Int32("broker_id", meta.NodeID),
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int("num_records", metrics.NumRecords),
		zap.Int("compressed_bytes", metrics.CompressedBytes),
		zap.Int("uncompressed_bytes", metrics.UncompressedBytes),
		zap.Uint8("compression_type", metrics.CompressionType),
	)
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Comma-separated Kafka broker addresses")
	topic := flag.String("topic", "test-topic", "Kafka topic")
	group := flag.String("group", "reproducer", "Consumer group ID")
	concurrency := flag.Int("concurrency", 10, "Max concurrent record processors / PollRecords max")
	sleep := flag.Duration("sleep", 500*time.Millisecond, "Sleep per record when -sink-url is not set")
	sinkURL := flag.String("sink-url", "", "HTTP sink URL to POST each record to (e.g. http://sink:8080/)")
	pushTimeout := flag.Duration("push-timeout", 10*time.Second, "HTTP push timeout per record")
	pprofAddr := flag.String("pprof-addr", ":6060", "pprof HTTP listen address (empty to disable)")
	maxConcurrentFetches := flag.Int("max-concurrent-fetches", 1, "MaxConcurrentFetches (0 = default)")
	logLevelStr := flag.String("log-level", "info", "Log level (debug, info, warn, error)")

	fetchMaxBytes := flagutil.BytesFlag(50 << 20)          // 50 MiB
	fetchMaxPartitionBytes := flagutil.BytesFlag(10 << 20) // 10 MiB
	flag.Var(&fetchMaxBytes, "fetch-max-bytes", "FetchMaxBytes, human-readable (e.g. 50MiB)")
	flag.Var(&fetchMaxPartitionBytes, "fetch-max-partition-bytes", "FetchMaxPartitionBytes, human-readable (e.g. 10MiB)")

	flag.Parse()

	logger, atomicLevel, err := apputil.BuildLogger(*logLevelStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	apputil.StartPPROF(*pprofAddr, logger)

	hostname, err := os.Hostname()
	if err != nil {
		logger.Fatal("get hostname", zap.Error(err))
	}

	// Mirror the reference: the Kafka client receives the errgroup context,
	// tying its lifecycle to the consumer goroutine group.
	grp, grpCtx := errgroup.WithContext(ctx)

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.InstanceID(hostname + "-" + *topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(int32(fetchMaxBytes)),
		kgo.FetchMaxPartitionBytes(int32(fetchMaxPartitionBytes)),
		kgo.WithLogger(kzap.New(logger.Named("kafka"), kzap.AtomicLevel(atomicLevel))),
		kgo.WithHooks(&readHook{logger: logger}),
	}
	if *maxConcurrentFetches > 0 {
		opts = append(opts, kgo.MaxConcurrentFetches(*maxConcurrentFetches))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		logger.Fatal("create kafka client", zap.Error(err))
	}

	httpClient := &http.Client{Timeout: *pushTimeout}

	logger.Info("consumer started",
		zap.String("topic", *topic),
		zap.String("group", *group),
		zap.Int("concurrency", *concurrency),
		zap.Duration("sleep", *sleep),
		zap.String("sink_url", *sinkURL),
		zap.String("fetch_max_bytes", fetchMaxBytes.String()),
		zap.String("fetch_max_partition_bytes", fetchMaxPartitionBytes.String()),
	)

	m := new(consumer.Metrics)
	m.StartTicker(ctx, logger)

	grp.Go(func() error {
		defer cl.Close()
		return consume(grpCtx, cl, *concurrency, *sleep, *sinkURL, httpClient, logger, m)
	})

	if err := grp.Wait(); err != nil {
		logger.Fatal("consumer error", zap.Error(err))
	}
}
