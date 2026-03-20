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
	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/tdakkota/franz-go-reproducer/internal/apputil"
	"github.com/tdakkota/franz-go-reproducer/internal/consumer"
	"github.com/tdakkota/franz-go-reproducer/internal/flagutil"
)

// fetchBatch fetches up to maxMessages from the reader.
// The first message is fetched with a blocking call; subsequent messages are
// fetched with a short timeout to drain any already-buffered records.
func fetchBatch(ctx context.Context, reader *kafka.Reader, maxMessages int) ([]kafka.Message, error) {
	msg, err := reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}
	msgs := []kafka.Message{msg}

	for len(msgs) < maxMessages {
		tCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		m, err := reader.FetchMessage(tCtx)
		cancel()
		if err != nil {
			break
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func consume(ctx context.Context, reader *kafka.Reader, concurrency int, sleep time.Duration, sinkURL string, httpClient *http.Client, logger *zap.Logger, m *consumer.Metrics) error {
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				stats := reader.Stats()
				logger.Info("reader stats",
					zap.Int64("fetches", stats.Fetches),
					zap.Int64("messages", stats.Messages),
					zap.Int64("bytes", stats.Bytes),
					zap.Int64("lag", stats.Lag),
					zap.Int64("offset", stats.Offset),
				)
			}
		}
	}()

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

		messages, err := fetchBatch(ctx, reader, concurrency)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.Error("fetch error", zap.Error(err))
			continue
		}

		var batchBytes int64
		for _, msg := range messages {
			batchBytes += int64(len(msg.Value))
		}

		grp, grpCtx := errgroup.WithContext(ctx)
		grp.SetLimit(concurrency)

		for _, msg := range messages {
			grp.Go(func() error {
				defer func() {
					logger.Info("sent request", zap.Int64("offset", msg.Offset))
				}()
				return consumer.ProcessRecord(grpCtx, msg.Value, sleep, sinkURL, httpClient, m)
			})
		}

		if err := grp.Wait(); err != nil {
			logger.Error("process error", zap.Error(err))
			continue
		}

		// Commit with the original ctx, not the errgroup ctx,
		// to avoid cancellation races on errgroup completion.
		if err := reader.CommitMessages(ctx, messages...); err != nil {
			logger.Error("commit error", zap.Error(err))
		}

		m.Add(int64(len(messages)), batchBytes)
	}
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Comma-separated Kafka broker addresses")
	topic := flag.String("topic", "test-topic", "Kafka topic")
	group := flag.String("group", "reproducer", "Consumer group ID")
	concurrency := flag.Int("concurrency", 10, "Max concurrent record processors / fetch batch size")
	sleep := flag.Duration("sleep", 500*time.Millisecond, "Sleep per record when -sink-url is not set")
	sinkURL := flag.String("sink-url", "", "HTTP sink URL to POST each record to (e.g. http://sink:8080/)")
	pushTimeout := flag.Duration("push-timeout", 10*time.Second, "HTTP push timeout per record")
	pprofAddr := flag.String("pprof-addr", ":6060", "pprof HTTP listen address (empty to disable)")
	logLevelStr := flag.String("log-level", "info", "Log level (debug, info, warn, error)")

	fetchMaxBytes := flagutil.BytesFlag(50 << 20)          // 50 MiB
	fetchMaxPartitionBytes := flagutil.BytesFlag(10 << 20) // 10 MiB (registered for flag compatibility; kafka-go has no per-partition cap)
	flag.Var(&fetchMaxBytes, "fetch-max-bytes", "MaxBytes per fetch request, human-readable (e.g. 50MiB)")
	flag.Var(&fetchMaxPartitionBytes, "fetch-max-partition-bytes", "Registered for flag compatibility; kafka-go has no per-partition fetch cap")

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

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(*brokers, ","),
		GroupID:        *group,
		Topic:          *topic,
		MinBytes:       1,
		MaxBytes:       int(fetchMaxBytes),
		CommitInterval: 0, // disable auto-commit; we commit manually after processing
	})
	defer reader.Close()

	httpClient := &http.Client{Timeout: *pushTimeout}

	logger.Info("consumer started",
		zap.String("topic", *topic),
		zap.String("group", *group),
		zap.Int("concurrency", *concurrency),
		zap.Duration("sleep", *sleep),
		zap.String("sink_url", *sinkURL),
		zap.String("fetch_max_bytes", fetchMaxBytes.String()),
	)

	m := new(consumer.Metrics)
	m.StartTicker(ctx, logger)

	if err := consume(ctx, reader, *concurrency, *sleep, *sinkURL, httpClient, logger, m); err != nil {
		logger.Fatal("consumer error", zap.Error(err))
	}
}
