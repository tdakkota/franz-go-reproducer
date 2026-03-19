package main

import (
	"context"
	"flag"
	"fmt"
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
)

// bytesFlag is a flag.Value that accepts human-readable byte sizes (e.g. "1MiB", "50MB").
type bytesFlag uint64

func (b *bytesFlag) String() string        { return humanize.IBytes(uint64(*b)) }
func (b *bytesFlag) Set(s string) error {
	v, err := humanize.ParseBytes(s)
	if err != nil {
		return fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	*b = bytesFlag(v)
	return nil
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Comma-separated Kafka broker addresses")
	topic := flag.String("topic", "test-topic", "Kafka topic")
	rate := flag.Duration("rate", 500*time.Millisecond, "Interval between produces")
	pprofAddr := flag.String("pprof-addr", ":6060", "pprof HTTP listen address (empty to disable)")

	payloadFile := flag.String("payload-file", "", "Path to a file whose contents are used as the record payload (overrides -payload-size)")
	payloadSize := bytesFlag(5 << 20)     // 5 MiB
	batchMaxBytes := bytesFlag(10 << 20) // 10 MiB — must exceed payload + framing overhead
	flag.Var(&payloadSize, "payload-size", "Payload size, human-readable (e.g. 1MiB, 512KB); ignored when -payload-file is set")
	flag.Var(&batchMaxBytes, "batch-max-bytes", "ProducerBatchMaxBytes, human-readable (e.g. 10MiB)")
	logLevelStr := flag.String("log-level", "info", "Log level (debug, info, warn, error)")

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

	var payload []byte
	if *payloadFile != "" {
		var err error
		payload, err = os.ReadFile(*payloadFile)
		if err != nil {
			logger.Fatal("read payload file", zap.String("path", *payloadFile), zap.Error(err))
		}
	} else {
		payload = make([]byte, payloadSize)
		for i := range payload {
			payload[i] = byte(i % 256)
		}
	}

	ticker := time.NewTicker(*rate)
	defer ticker.Stop()

	var produced, producedBytes atomic.Int64
	logger.Info("producer started",
		zap.String("topic", *topic),
		zap.String("payload_size", payloadSize.String()),
		zap.String("batch_max_bytes", batchMaxBytes.String()),
		zap.Duration("rate", *rate),
	)

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				logger.Info("produced",
					zap.Int64("records", produced.Load()),
					zap.String("bytes", humanize.IBytes(uint64(producedBytes.Load()))),
				)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			logger.Info("producer stopped",
				zap.Int64("produced_records", produced.Load()),
				zap.String("produced_bytes", humanize.IBytes(uint64(producedBytes.Load()))),
			)
			return
		case <-ticker.C:
			rec := &kgo.Record{
				Topic: *topic,
				Value: payload,
			}
			if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
				logger.Error("produce error", zap.Error(err))
				continue
			}
			produced.Add(1)
			producedBytes.Add(int64(len(payload)))
		}
	}
}
