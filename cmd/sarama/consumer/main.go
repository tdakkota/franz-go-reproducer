package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	"github.com/tdakkota/franz-go-reproducer/internal/apputil"
	"github.com/tdakkota/franz-go-reproducer/internal/consumer"
	"github.com/tdakkota/franz-go-reproducer/internal/flagutil"
)

type consumerHandler struct {
	concurrency int
	sleep       time.Duration
	sinkURL     string
	httpClient  *http.Client
	logger      *zap.Logger
	m           *consumer.Metrics
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
				if err := consumer.ProcessRecord(grpCtx, msg.Value, h.sleep, h.sinkURL, h.httpClient, h.m); err != nil {
					return err
				}
				session.MarkMessage(msg, "")
				h.m.Add(1, int64(len(msg.Value)))
				return nil
			})
		case <-grpCtx.Done():
			_ = grp.Wait()
			return grpCtx.Err()
		}
	}
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

	fetchMaxBytes := flagutil.BytesFlag(50 << 20)          // 50 MiB
	fetchMaxPartitionBytes := flagutil.BytesFlag(10 << 20) // 10 MiB
	flag.Var(&fetchMaxBytes, "fetch-max-bytes", "Consumer.Fetch.Max, human-readable (e.g. 50MiB)")
	flag.Var(&fetchMaxPartitionBytes, "fetch-max-partition-bytes", "Consumer.Fetch.Default, human-readable (e.g. 10MiB)")

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
	defer func() { _ = consumerGroup.Close() }()

	handler := &consumerHandler{
		concurrency: *concurrency,
		sleep:       *sleep,
		sinkURL:     *sinkURL,
		httpClient:  &http.Client{Timeout: *pushTimeout},
		logger:      logger,
		m:           new(consumer.Metrics),
	}

	handler.m.StartTicker(ctx, logger)

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
		zap.Int64("consumed_records", handler.m.Consumed()),
		zap.String("consumed_bytes", humanize.IBytes(uint64(handler.m.ConsumedBytes()))),
	)
}
