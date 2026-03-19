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
	"github.com/segmentio/kafka-go"
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

func consume(ctx context.Context, reader *kafka.Reader, concurrency int, sleep time.Duration, sinkURL string, httpClient *http.Client, logger *zap.Logger) error {
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
				zap.Int64("consumed_records", consumed.Load()),
				zap.String("consumed_bytes", humanize.IBytes(uint64(consumedBytes.Load()))),
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
		if err := reader.CommitMessages(ctx, messages...); err != nil {
			logger.Error("commit error", zap.Error(err))
		}

		consumed.Add(int64(len(messages)))
		consumedBytes.Add(batchBytes)
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

	fetchMaxBytes := bytesFlag(50 << 20) // 50 MiB
	flag.Var(&fetchMaxBytes, "fetch-max-bytes", "MaxBytes per fetch request, human-readable (e.g. 50MiB)")

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

	if err := consume(ctx, reader, *concurrency, *sleep, *sinkURL, httpClient, logger); err != nil {
		logger.Fatal("consumer error", zap.Error(err))
	}
}
