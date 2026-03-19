package main

import (
	"bytes"
	"context"
	"errors"
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

	"github.com/IBM/sarama"
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

type consumerHandler struct {
	concurrency int
	sleep       time.Duration
	sinkURL     string
	httpClient  *http.Client
	logger      *zap.Logger

	consumed      atomic.Int64
	consumedBytes atomic.Int64
	pushCount     atomic.Int64
	totalPushNs   atomic.Int64
	lastPushNs    atomic.Int64
}

func (h *consumerHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	grp, grpCtx := errgroup.WithContext(session.Context())
	grp.SetLimit(h.concurrency)

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return grp.Wait()
			}
			grp.Go(func() error {
				defer func() {
					h.logger.Info("sent request", zap.Int64("offset", msg.Offset))
				}()
				if err := h.processRecord(grpCtx, msg.Value); err != nil {
					return err
				}
				session.MarkMessage(msg, "")
				h.consumed.Add(1)
				h.consumedBytes.Add(int64(len(msg.Value)))
				return nil
			})
		case <-grpCtx.Done():
			_ = grp.Wait()
			return grpCtx.Err()
		}
	}
}

func (h *consumerHandler) processRecord(ctx context.Context, value []byte) error {
	if h.sinkURL == "" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(h.sleep):
			return nil
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.sinkURL, bytes.NewReader(value))
	if err != nil {
		return err
	}

	start := time.Now()
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	_, _ = io.Copy(io.Discard, resp.Body)

	dur := time.Since(start)
	h.pushCount.Add(1)
	h.totalPushNs.Add(dur.Nanoseconds())
	h.lastPushNs.Store(dur.Nanoseconds())

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("sink returned %d", resp.StatusCode)
	}
	return nil
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Comma-separated Kafka broker addresses")
	topic := flag.String("topic", "test-topic", "Kafka topic")
	group := flag.String("group", "reproducer", "Consumer group ID")
	concurrency := flag.Int("concurrency", 10, "Max concurrent record processors")
	sleep := flag.Duration("sleep", 500*time.Millisecond, "Sleep per record when -sink-url is not set")
	sinkURL := flag.String("sink-url", "", "HTTP sink URL to POST each record to (e.g. http://sink:8080/)")
	pushTimeout := flag.Duration("push-timeout", 10*time.Second, "HTTP push timeout per record")
	pprofAddr := flag.String("pprof-addr", ":6060", "pprof HTTP listen address (empty to disable)")
	channelBufferSize := flag.Int("channel-buffer-size", 10, "Sarama ChannelBufferSize (messages buffered per partition)")
	logLevelStr := flag.String("log-level", "info", "Log level (debug, info, warn, error)")

	fetchMaxBytes := bytesFlag(50 << 20)          // 50 MiB
	fetchMaxPartitionBytes := bytesFlag(10 << 20) // 10 MiB
	flag.Var(&fetchMaxBytes, "fetch-max-bytes", "Consumer.Fetch.Max, human-readable (e.g. 50MiB)")
	flag.Var(&fetchMaxPartitionBytes, "fetch-max-partition-bytes", "Consumer.Fetch.Default, human-readable (e.g. 10MiB)")

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

	saramaLogger, _ := zap.NewStdLogAt(logger.Named("sarama"), zapcore.DebugLevel)
	sarama.Logger = saramaLogger

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.Consumer.Fetch.Max = int32(fetchMaxBytes)
	saramaConfig.Consumer.Fetch.Default = int32(fetchMaxPartitionBytes)
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = true
	saramaConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.ChannelBufferSize = *channelBufferSize

	consumerGroup, err := sarama.NewConsumerGroup(strings.Split(*brokers, ","), *group, saramaConfig)
	if err != nil {
		logger.Fatal("create consumer group", zap.Error(err))
	}
	defer func() {
		_ = consumerGroup.Close()
	}()

	handler := &consumerHandler{
		concurrency: *concurrency,
		sleep:       *sleep,
		sinkURL:     *sinkURL,
		httpClient:  &http.Client{Timeout: *pushTimeout},
		logger:      logger,
	}

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				fields := []zap.Field{
					zap.Int64("records", handler.consumed.Load()),
					zap.String("bytes", humanize.IBytes(uint64(handler.consumedBytes.Load()))),
				}
				if n := handler.pushCount.Load(); n > 0 {
					avg := time.Duration(handler.totalPushNs.Load() / n)
					last := time.Duration(handler.lastPushNs.Load())
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

	logger.Info("consumer started",
		zap.String("topic", *topic),
		zap.String("group", *group),
		zap.Int("concurrency", *concurrency),
		zap.Duration("sleep", *sleep),
		zap.String("sink_url", *sinkURL),
		zap.String("fetch_max_bytes", fetchMaxBytes.String()),
		zap.String("fetch_max_partition_bytes", fetchMaxPartitionBytes.String()),
	)

	topics := []string{*topic}
	for {
		if err := consumerGroup.Consume(ctx, topics, handler); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				break
			}
			logger.Error("consume error", zap.Error(err))
		}
		if ctx.Err() != nil {
			break
		}
	}

	logger.Info("consumer stopped",
		zap.Int64("consumed_records", handler.consumed.Load()),
		zap.String("consumed_bytes", humanize.IBytes(uint64(handler.consumedBytes.Load()))),
	)
}
