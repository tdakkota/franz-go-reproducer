package producer

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

// Metrics tracks produce counters.
type Metrics struct {
	produced      atomic.Int64
	producedBytes atomic.Int64
}

// Run drives the ticker loop: on each tick calls produceFn(ctx, payload).
// Logs progress every 10 s. Returns when ctx is cancelled.
func Run(ctx context.Context, rate time.Duration, payload []byte, logger *zap.Logger, produceFn func(ctx context.Context, payload []byte) error) {
	ticker := time.NewTicker(rate)
	defer ticker.Stop()

	var m Metrics

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				logger.Info("produced",
					zap.Int64("records", m.produced.Load()),
					zap.String("bytes", humanize.IBytes(uint64(m.producedBytes.Load()))),
				)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			logger.Info("producer stopped",
				zap.Int64("produced_records", m.produced.Load()),
				zap.String("produced_bytes", humanize.IBytes(uint64(m.producedBytes.Load()))),
			)
			return
		case <-ticker.C:
			if err := produceFn(ctx, payload); err != nil {
				logger.Error("produce error", zap.Error(err))
				continue
			}
			m.produced.Add(1)
			m.producedBytes.Add(int64(len(payload)))
		}
	}
}
