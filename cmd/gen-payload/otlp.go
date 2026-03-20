package main

import (
	"fmt"
	"time"

	collectorv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

// generateOTLPMetrics returns a marshaled ExportMetricsServiceRequest whose
// size is as close to targetBytes as possible without exceeding it.
// It builds a single Gauge metric and binary-searches for the number of
// NumberDataPoints needed to reach the target.
func generateOTLPMetrics(targetBytes int) ([]byte, error) {
	now := uint64(time.Now().UnixNano())
	start := now - uint64(time.Minute)

	buildReq := func(n int) *collectorv1.ExportMetricsServiceRequest {
		pts := make([]*metricsv1.NumberDataPoint, n)
		for i := range pts {
			pts[i] = &metricsv1.NumberDataPoint{
				Attributes: []*commonv1.KeyValue{
					{Key: "host", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "worker-01"}}},
					{Key: "service", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "kafka-consumer"}}},
					{Key: "topic", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "test-topic"}}},
					{Key: "partition", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: int64(i % 16)}}},
				},
				StartTimeUnixNano: start,
				TimeUnixNano:      now,
				Value:             &metricsv1.NumberDataPoint_AsDouble{AsDouble: float64(i) * 1.5},
			}
		}
		return &collectorv1.ExportMetricsServiceRequest{
			ResourceMetrics: []*metricsv1.ResourceMetrics{
				{
					Resource: &resourcev1.Resource{
						Attributes: []*commonv1.KeyValue{
							{Key: "service.name", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "kafka-reproducer"}}},
							{Key: "service.version", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "0.1.0"}}},
						},
					},
					ScopeMetrics: []*metricsv1.ScopeMetrics{
						{
							Scope: &commonv1.InstrumentationScope{
								Name:    "kafka-reproducer/metrics",
								Version: "0.1.0",
							},
							Metrics: []*metricsv1.Metric{
								{
									Name:        "kafka.consumer.records_consumed_total",
									Description: "Total number of records consumed from Kafka",
									Unit:        "1",
									Data: &metricsv1.Metric_Gauge{
										Gauge: &metricsv1.Gauge{DataPoints: pts},
									},
								},
							},
						},
					},
				},
			},
		}
	}

	marshalN := func(n int) ([]byte, error) {
		return proto.Marshal(buildReq(n))
	}

	// Probe with 1 point to get per-point cost estimate.
	b1, err := marshalN(1)
	if err != nil {
		return nil, fmt.Errorf("probe marshal: %w", err)
	}
	if len(b1) >= targetBytes {
		return b1, nil
	}
	b2, err := marshalN(2)
	if err != nil {
		return nil, fmt.Errorf("probe marshal: %w", err)
	}
	perPoint := len(b2) - len(b1)
	if perPoint <= 0 {
		perPoint = 1
	}
	// Initial estimate.
	lo := (targetBytes-len(b1))/perPoint + 1
	hi := lo * 2

	// Expand hi until it overshoots the target.
	for {
		b, err := marshalN(hi)
		if err != nil {
			return nil, err
		}
		if len(b) >= targetBytes {
			break
		}
		hi *= 2
	}

	// Binary search: largest n where len(marshal(n)) <= targetBytes.
	var best []byte
	for lo <= hi {
		mid := (lo + hi) / 2
		b, err := marshalN(mid)
		if err != nil {
			return nil, err
		}
		if len(b) <= targetBytes {
			best = b
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if best == nil {
		return b1, nil
	}
	return best, nil
}
