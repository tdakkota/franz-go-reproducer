//go:build confluent

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.uber.org/zap"

	"github.com/tdakkota/franz-go-reproducer/internal/apputil"
	"github.com/tdakkota/franz-go-reproducer/internal/flagutil"
	"github.com/tdakkota/franz-go-reproducer/internal/payload"
	"github.com/tdakkota/franz-go-reproducer/internal/producer"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Comma-separated Kafka broker addresses")
	topic := flag.String("topic", "test-topic", "Kafka topic")
	rate := flag.Duration("rate", 500*time.Millisecond, "Interval between produces")
	pprofAddr := flag.String("pprof-addr", ":6060", "pprof HTTP listen address (empty to disable)")

	payloadFile := flag.String("payload-file", "", "Path to a file whose contents are used as the record payload (overrides -payload-size)")
	payloadSize := flagutil.BytesFlag(5 << 20)    // 5 MiB
	batchMaxBytes := flagutil.BytesFlag(10 << 20) // 10 MiB — must exceed payload + framing overhead
	flag.Var(&payloadSize, "payload-size", "Payload size, human-readable (e.g. 1MiB, 512KB); ignored when -payload-file is set")
	flag.Var(&batchMaxBytes, "batch-max-bytes", "ProducerBatchMaxBytes, human-readable (e.g. 10MiB)")
	logLevelStr := flag.String("log-level", "info", "Log level (debug, info, warn, error)")

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

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.ProducerBatchMaxBytes(int32(batchMaxBytes)),
		kgo.WithLogger(kzap.New(logger.Named("kafka"), kzap.AtomicLevel(atomicLevel))),
	)
	if err != nil {
		logger.Fatal("create kafka client", zap.Error(err))
	}
	defer cl.Close()

	data, err := payload.Load(*payloadFile, uint64(payloadSize))
	if err != nil {
		logger.Fatal("load payload", zap.String("path", *payloadFile), zap.Error(err))
	}

	logger.Info("producer started",
		zap.String("topic", *topic),
		zap.String("payload_size", payloadSize.String()),
		zap.String("batch_max_bytes", batchMaxBytes.String()),
		zap.Duration("rate", *rate),
	)

	producer.Run(ctx, *rate, data, logger, func(ctx context.Context, p []byte) error {
		rec := &kgo.Record{Topic: *topic, Value: p}
		return cl.ProduceSync(ctx, rec).FirstErr()
	})
}
