package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

// bytesFlag is a flag.Value that accepts human-readable byte sizes (e.g. "50MiB", "10MB").
type bytesFlag uint64

func (b *bytesFlag) String() string { return humanize.IBytes(uint64(*b)) }
func (b *bytesFlag) Set(s string) error {
	v, err := humanize.ParseBytes(s)
	if err != nil {
		return fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	*b = bytesFlag(v)
	return nil
}

func consume(ctx context.Context, cl *kgo.Client, concurrency int, sleep time.Duration, sinkURL string, httpClient *http.Client, logger *zap.Logger) error {
	var consumed, consumedBytes atomic.Int64
	var pushCount, totalPushNs, lastPushNs atomic.Int64

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				fields := []zap.Field{
					zap.Int64("records", consumed.Load()),
					zap.String("bytes", humanize.IBytes(uint64(consumedBytes.Load()))),
				}
				if n := pushCount.Load(); n > 0 {
					avg := time.Duration(totalPushNs.Load() / n)
					last := time.Duration(lastPushNs.Load())
					fields = append(fields,
						zap.Int64("push_requests", n),
						zap.Duration("push_avg_latency", avg),
						zap.Duration("push_last_latency", last),
					)
				}
				logger.Info("consumed", fields...)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			logger.Info("consumer stopped",
				zap.Int64("consumed_records", consumed.Load()),
				zap.String("consumed_bytes", humanize.IBytes(uint64(consumedBytes.Load()))),
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
				if sinkURL == "" {
					select {
					case <-grpCtx.Done():
						return grpCtx.Err()
					case <-time.After(sleep):
						return nil
					}
				}

				req, err := http.NewRequestWithContext(grpCtx, http.MethodPost, sinkURL, bytes.NewReader(rec.Value))
				if err != nil {
					return err
				}

				start := time.Now()
				resp, err := httpClient.Do(req)
				if err != nil {
					return err
				}
				defer func() {
					_ = resp.Body.Close()
				}()
				_, _ = io.Copy(io.Discard, resp.Body)

				dur := time.Since(start)
				pushCount.Add(1)
				totalPushNs.Add(dur.Nanoseconds())
				lastPushNs.Store(dur.Nanoseconds())

				if resp.StatusCode < 200 || resp.StatusCode >= 300 {
					return fmt.Errorf("sink returned %d", resp.StatusCode)
				}
				return nil
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

		consumed.Add(int64(len(records)))
		consumedBytes.Add(batchBytes)
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

	fetchMaxBytes := bytesFlag(50 << 20)          // 50 MiB
	fetchMaxPartitionBytes := bytesFlag(10 << 20) // 10 MiB
	flag.Var(&fetchMaxBytes, "fetch-max-bytes", "FetchMaxBytes, human-readable (e.g. 50MiB)")
	flag.Var(&fetchMaxPartitionBytes, "fetch-max-partition-bytes", "FetchMaxPartitionBytes, human-readable (e.g. 10MiB)")

	flag.Parse()

	var logLevel zapcore.Level
	if err := logLevel.UnmarshalText([]byte(*logLevelStr)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level %q: %v\n", *logLevelStr, err)
		os.Exit(1)
	}
	atomicLevel := zap.NewAtomicLevelAt(logLevel)

	cfg := zap.NewDevelopmentConfig()
	cfg.Level = atomicLevel
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	defer logger.Sync() //nolint:errcheck

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if *pprofAddr != "" {
		go func() {
			logger.Info("pprof listening", zap.String("addr", *pprofAddr))
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				logger.Error("pprof server error", zap.Error(err))
			}
		}()
	}

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

	grp.Go(func() error {
		defer cl.Close()
		return consume(grpCtx, cl, *concurrency, *sleep, *sinkURL, httpClient, logger)
	})

	if err := grp.Wait(); err != nil {
		logger.Fatal("consumer error", zap.Error(err))
	}
}
