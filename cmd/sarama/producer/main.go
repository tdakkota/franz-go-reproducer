package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

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
	payloadSize := flagutil.BytesFlag(5 << 20)      // 5 MiB
	maxMessageBytes := flagutil.BytesFlag(10 << 20) // 10 MiB — must exceed payload + framing overhead
	flag.Var(&payloadSize, "payload-size", "Payload size, human-readable (e.g. 1MiB, 512KB); ignored when -payload-file is set")
	flag.Var(&maxMessageBytes, "max-message-bytes", "Producer.MaxMessageBytes, human-readable (e.g. 10MiB)")
	logLevelStr := flag.String("log-level", "info", "Log level (debug, info, warn, error)")

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
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Compression = sarama.CompressionZSTD
	saramaConfig.Producer.MaxMessageBytes = int(maxMessageBytes)
	saramaConfig.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(strings.Split(*brokers, ","), saramaConfig)
	if err != nil {
		logger.Fatal("create producer", zap.Error(err))
	}
	defer func() { _ = p.Close() }()

	data, err := payload.Load(*payloadFile, uint64(payloadSize))
	if err != nil {
		logger.Fatal("load payload", zap.String("path", *payloadFile), zap.Error(err))
	}

	logger.Info("producer started",
		zap.String("topic", *topic),
		zap.String("payload_size", payloadSize.String()),
		zap.String("max_message_bytes", maxMessageBytes.String()),
		zap.Duration("rate", *rate),
	)

	producer.Run(ctx, *rate, data, logger, func(_ context.Context, data []byte) error {
		msg := &sarama.ProducerMessage{
			Topic: *topic,
			Value: sarama.ByteEncoder(data),
		}
		_, _, err := p.SendMessage(msg)
		return err
	})
}
