//go:build confluent

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/tdakkota/franz-go-reproducer/internal/apputil"
	"github.com/tdakkota/franz-go-reproducer/internal/consumer"
	"github.com/tdakkota/franz-go-reproducer/internal/flagutil"
)

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

func consume(ctx context.Context, c *kafka.Consumer, concurrency int, sleep time.Duration, sinkURL string, httpClient *http.Client, logger *zap.Logger, m *consumer.Metrics) error {
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

		batch, err := collectBatch(ctx, c, concurrency, logger)
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
		grp.SetLimit(concurrency)

		for _, msg := range batch {
			grp.Go(func() error {
				defer func() {
					logger.Info("sent request",
						zap.String("topic", *msg.TopicPartition.Topic),
						zap.Int32("partition", msg.TopicPartition.Partition),
						zap.Int64("offset", int64(msg.TopicPartition.Offset)),
					)
				}()
				return consumer.ProcessRecord(grpCtx, msg.Value, sleep, sinkURL, httpClient, m)
			})
		}

		if err := grp.Wait(); err != nil {
			logger.Error("process error", zap.Error(err))
			seekBack(c, batch, logger)
			continue
		}

		commitBatch(c, batch, logger)
		m.Add(int64(len(batch)), batchBytes)
	}
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Comma-separated Kafka broker addresses")
	topic := flag.String("topic", "test-topic", "Kafka topic")
	group := flag.String("group", "reproducer", "Consumer group ID")
	concurrency := flag.Int("concurrency", 10, "Max records per poll batch / max concurrent processors")
	sleep := flag.Duration("sleep", 500*time.Millisecond, "Sleep per record when -sink-url is not set")
	sinkURL := flag.String("sink-url", "", "HTTP sink URL to POST each record to (e.g. http://sink:8080/)")
	pushTimeout := flag.Duration("push-timeout", 10*time.Second, "HTTP push timeout per record")
	pprofAddr := flag.String("pprof-addr", ":6060", "pprof HTTP listen address (empty to disable)")
	logLevelStr := flag.String("log-level", "info", "Log level (debug, info, warn, error)")

	fetchMaxBytes := flagutil.BytesFlag(50 << 20)          // 50 MiB
	fetchMaxPartitionBytes := flagutil.BytesFlag(10 << 20) // 10 MiB
	flag.Var(&fetchMaxBytes, "fetch-max-bytes", "fetch.max.bytes, human-readable (e.g. 50MiB)")
	flag.Var(&fetchMaxPartitionBytes, "fetch-max-partition-bytes", "max.partition.fetch.bytes, human-readable (e.g. 10MiB)")

	flag.Parse()

	logger, _, err := apputil.BuildLogger(*logLevelStr)
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
		zap.Int("concurrency", *concurrency),
		zap.Duration("sleep", *sleep),
		zap.String("sink_url", *sinkURL),
		zap.String("fetch_max_bytes", fetchMaxBytes.String()),
		zap.String("fetch_max_partition_bytes", fetchMaxPartitionBytes.String()),
	)

	httpClient := &http.Client{Timeout: *pushTimeout}

	m := new(consumer.Metrics)
	m.StartTicker(ctx, logger)

	if err := consume(ctx, c, *concurrency, *sleep, *sinkURL, httpClient, logger, m); err != nil {
		logger.Fatal("consumer error", zap.Error(err))
	}
}
