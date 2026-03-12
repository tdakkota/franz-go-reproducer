package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	sleep := flag.Duration("sleep", 500*time.Millisecond, "Sleep per request to emulate processing latency")
	pprofAddr := flag.String("pprof-addr", ":6060", "pprof HTTP listen address (empty to disable)")
	logLevelStr := flag.String("log-level", "info", "Log level (debug, info, warn, error)")
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

	if *pprofAddr != "" {
		go func() {
			logger.Info("pprof listening", zap.String("addr", *pprofAddr))
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				logger.Error("pprof server error", zap.Error(err))
			}
		}()
	}

	var requests, receivedBytes atomic.Int64

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for range t.C {
			logger.Info("sink stats",
				zap.Int64("requests", requests.Load()),
				zap.String("received_bytes", humanize.IBytes(uint64(receivedBytes.Load()))),
			)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			http.Error(w, "read body error", http.StatusInternalServerError)
			return
		}
		if *sleep > 0 {
			time.Sleep(*sleep)
		}
		requests.Add(1)
		receivedBytes.Add(n)
		w.WriteHeader(http.StatusNoContent)
	})

	logger.Info("sink listening", zap.String("addr", *addr), zap.Duration("sleep", *sleep))
	if err := http.ListenAndServe(*addr, mux); err != nil {
		logger.Fatal("sink server error", zap.Error(err))
	}
}
