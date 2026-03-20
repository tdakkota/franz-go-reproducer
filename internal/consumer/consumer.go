package consumer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

// Metrics tracks consume and HTTP push counters.
type Metrics struct {
	consumed      atomic.Int64
	consumedBytes atomic.Int64
	pushCount     atomic.Int64
	totalPushNs   atomic.Int64
	lastPushNs    atomic.Int64
}

// StartTicker logs a summary every 10 s until ctx is cancelled.
func (m *Metrics) StartTicker(ctx context.Context, logger *zap.Logger) {
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				fields := []zap.Field{
					zap.Int64("records", m.consumed.Load()),
					zap.String("bytes", humanize.IBytes(uint64(m.consumedBytes.Load()))),
				}
				if n := m.pushCount.Load(); n > 0 {
					avg := time.Duration(m.totalPushNs.Load() / n)
					last := time.Duration(m.lastPushNs.Load())
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
}

// Add records n records and b bytes as consumed.
func (m *Metrics) Add(n, b int64) {
	m.consumed.Add(n)
	m.consumedBytes.Add(b)
}

// Consumed returns the number of consumed records.
func (m *Metrics) Consumed() int64 { return m.consumed.Load() }

// ConsumedBytes returns the number of consumed bytes.
func (m *Metrics) ConsumedBytes() int64 { return m.consumedBytes.Load() }

// ProcessRecord executes the HTTP sink request (or sleep) for one record value.
// It updates push counters on m. sinkURL="" → sleep instead of HTTP.
func ProcessRecord(ctx context.Context, value []byte, sleep time.Duration, sinkURL string, client *http.Client, m *Metrics) error {
	if sinkURL == "" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
			return nil
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sinkURL, bytes.NewReader(value))
	if err != nil {
		return err
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)

	dur := time.Since(start)
	m.pushCount.Add(1)
	m.totalPushNs.Add(dur.Nanoseconds())
	m.lastPushNs.Store(dur.Nanoseconds())

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("sink returned %d", resp.StatusCode)
	}
	return nil
}
