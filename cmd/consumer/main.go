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
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/dustin/go-humanize"
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

// collectBatch polls up to maxRecords messages, returning as soon as the batch
// is full or there are no more messages immediately available.
func collectBatch(ctx context.Context, c *kafka.Consumer, maxRecords int, logger *zap.Logger) ([]*kafka.Message, error) {
	var batch []*kafka.Message
	for len(batch) < maxRecords {
		select {
		case <-ctx.Done():
			return batch, nil
		default:
			if c.IsClosed() {
				return nil, fmt.Errorf("client is closed")
			}
		}

		ev := c.Poll(100) // 100 ms
		if ev == nil {
			if len(batch) > 0 {
				// Return partial batch rather than waiting for it to fill.
				return batch, nil
			}
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			if e.TopicPartition.Error != nil {
				logger.Warn("partition error", zap.Error(e.TopicPartition.Error))
				continue
			}
			batch = append(batch, e)
		case kafka.Error:
			if e.IsFatal() {
				return batch, fmt.Errorf("fatal kafka error: %w", e)
			}
			logger.Warn("kafka error", zap.Error(e))
		default:
			// AssignedPartitions, RevokedPartitions, OffsetsCommitted, etc.
			// These are handled automatically by librdkafka when using nil rebalance callback.
			logger.Info("kafka event", zap.String("type", fmt.Sprintf("%T", ev)))
		}
	}
	return batch, nil
}

// seekBack rewinds each partition to the earliest offset seen in the batch,
// causing the records to be re-delivered on the next poll.
func seekBack(c *kafka.Consumer, batch []*kafka.Message, logger *zap.Logger) {
	min := make(map[int32]kafka.TopicPartition)
	for _, msg := range batch {
		cur, ok := min[msg.TopicPartition.Partition]
		if !ok || msg.TopicPartition.Offset < cur.Offset {
			min[msg.TopicPartition.Partition] = msg.TopicPartition
		}
	}

	partitions := make([]kafka.TopicPartition, 0, len(min))
	for _, tp := range min {
		partitions = append(partitions, tp)
	}

	result, err := c.SeekPartitions(partitions)
	if err != nil {
		logger.Error("seek failed", zap.Error(err))
		return
	}
	for _, tp := range result {
		if tp.Error != nil {
			logger.Error("seek partition failed",
				zap.String("topic", *tp.Topic),
				zap.Int32("partition", tp.Partition),
				zap.Int64("offset", int64(tp.Offset)),
				zap.Error(tp.Error),
			)
		}
	}
}

// commitBatch commits the highest offset per partition in the batch (offset+1
// is what librdkafka stores, as CommitMessage follows the Kafka convention).
func commitBatch(c *kafka.Consumer, batch []*kafka.Message, logger *zap.Logger) {
	max := make(map[int32]*kafka.Message)
	for _, msg := range batch {
		cur, ok := max[msg.TopicPartition.Partition]
		if !ok || msg.TopicPartition.Offset > cur.TopicPartition.Offset {
			max[msg.TopicPartition.Partition] = msg
		}
	}
	for _, msg := range max {
		if _, err := c.CommitMessage(msg); err != nil {
			logger.Error("commit failed",
				zap.String("topic", *msg.TopicPartition.Topic),
				zap.Int32("partition", msg.TopicPartition.Partition),
				zap.Error(err),
			)
		}
	}
}

func consume(ctx context.Context, c *kafka.Consumer, maxPollRecords int, sleep time.Duration, sinkURL string, httpClient *http.Client, logger *zap.Logger) error {
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

		batch, err := collectBatch(ctx, c, maxPollRecords, logger)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			continue
		}

		var batchBytes int64
		for _, msg := range batch {
			batchBytes += int64(len(msg.Value))
		}

		grp, grpCtx := errgroup.WithContext(ctx)
		grp.SetLimit(maxPollRecords)

		for _, msg := range batch {
			grp.Go(func() error {
				defer func() {
					logger.Info("processed record",
						zap.String("topic", *msg.TopicPartition.Topic),
						zap.Int32("partition", msg.TopicPartition.Partition),
						zap.Int64("offset", int64(msg.TopicPartition.Offset)),
					)
				}()

				if sinkURL == "" {
					select {
					case <-grpCtx.Done():
						return grpCtx.Err()
					case <-time.After(sleep):
						return nil
					}
				}

				req, err := http.NewRequestWithContext(grpCtx, http.MethodPost, sinkURL, bytes.NewReader(msg.Value))
				if err != nil {
					return err
				}

				start := time.Now()
				resp, err := httpClient.Do(req)
				if err != nil {
					return err
				}
				defer func() { _ = resp.Body.Close() }()
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
			seekBack(c, batch, logger)
			continue
		}

		commitBatch(c, batch, logger)
		consumed.Add(int64(len(batch)))
		consumedBytes.Add(batchBytes)
	}
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Comma-separated Kafka broker addresses")
	topic := flag.String("topic", "test-topic", "Kafka topic")
	group := flag.String("group", "reproducer", "Consumer group ID")
	maxPollRecords := flag.Int("max-poll-records", 10, "Max records per poll batch / max concurrent processors")
	sleep := flag.Duration("sleep", 500*time.Millisecond, "Sleep per record when -sink-url is not set")
	sinkURL := flag.String("sink-url", "", "HTTP sink URL to POST each record to (e.g. http://sink:8080/)")
	pushTimeout := flag.Duration("push-timeout", 10*time.Second, "HTTP push timeout per record")
	pprofAddr := flag.String("pprof-addr", ":6060", "pprof HTTP listen address (empty to disable)")
	logLevelStr := flag.String("log-level", "info", "Log level (debug, info, warn, error)")

	fetchMaxBytes := bytesFlag(50 << 20)          // 50 MiB
	fetchMaxPartitionBytes := bytesFlag(10 << 20) // 10 MiB
	flag.Var(&fetchMaxBytes, "fetch-max-bytes", "fetch.max.bytes, human-readable (e.g. 50MiB)")
	flag.Var(&fetchMaxPartitionBytes, "fetch-max-partition-bytes", "max.partition.fetch.bytes, human-readable (e.g. 10MiB)")

	flag.Parse()

	var logLevel zapcore.Level
	if err := logLevel.UnmarshalText([]byte(*logLevelStr)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level %q: %v\n", *logLevelStr, err)
		os.Exit(1)
	}

	cfg := zap.NewDevelopmentConfig()
	cfg.Level = zap.NewAtomicLevelAt(logLevel)
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

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":         *brokers,
		"group.id":                  *group,
		"group.instance.id":         hostname,
		"auto.offset.reset":         "earliest",
		"enable.auto.commit":        false,
		"fetch.max.bytes":           int(fetchMaxBytes),
		"max.partition.fetch.bytes": int(fetchMaxPartitionBytes),
	})
	if err != nil {
		logger.Fatal("create consumer", zap.Error(err))
	}
	defer func() { _ = c.Close() }()

	if err := c.Subscribe(*topic, nil); err != nil {
		logger.Fatal("subscribe", zap.Error(err))
	}

	logger.Info("consumer started",
		zap.String("topic", *topic),
		zap.String("group", *group),
		zap.String("instance_id", hostname),
		zap.Int("max_poll_records", *maxPollRecords),
		zap.Duration("sleep", *sleep),
		zap.String("sink_url", *sinkURL),
		zap.String("fetch_max_bytes", fetchMaxBytes.String()),
		zap.String("fetch_max_partition_bytes", fetchMaxPartitionBytes.String()),
	)

	httpClient := &http.Client{Timeout: *pushTimeout}

	if err := consume(ctx, c, *maxPollRecords, *sleep, *sinkURL, httpClient, logger); err != nil {
		logger.Fatal("consumer error", zap.Error(err))
	}
}
