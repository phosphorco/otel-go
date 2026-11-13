// PHOSPHOR PATCH-FOR-THE-PATCH (2026-11-13): downstream follow-up to
// Dagger 1.0.0-beta.23 telemetry lifecycle changes (f0a3d728), on otel-go
// 132b1dd82eb2. During an upstream conflict, preserve the caller deadline,
// independent remote metric forwarding, immutable bounded snapshots, and
// command output/verdict separation. See PHOSPHOR_PATCHES.md before dropping
// or replacing this follow-up; its failure probes are the acceptance check.

package otel

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

type heldMetricExporter struct {
	releaseOnce sync.Once
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	result      chan *metricdata.ResourceMetrics
	contexts    chan context.Context
}

func (e *heldMetricExporter) Temporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}
func (e *heldMetricExporter) Aggregation(sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.AggregationDefault{}
}
func (e *heldMetricExporter) Export(ctx context.Context, data *metricdata.ResourceMetrics) error {
	if e.contexts != nil {
		e.contexts <- ctx
	}
	e.once.Do(func() { close(e.entered) })
	<-e.release // Deliberately ignore cancellation.
	if e.result != nil {
		e.result <- data
	}
	return nil
}
func (*heldMetricExporter) ForceFlush(context.Context) error { return nil }
func (*heldMetricExporter) Shutdown(context.Context) error   { return nil }
func sampleMetric() *metricdata.ResourceMetrics {
	return &metricdata.ResourceMetrics{Resource: resource.Empty(), ScopeMetrics: []metricdata.ScopeMetrics{{Metrics: []metricdata.Metrics{
		{Name: "gauge", Data: metricdata.Gauge[int64]{DataPoints: []metricdata.DataPoint[int64]{{Value: 11}}}},
		{Name: "histogram", Data: metricdata.Histogram[float64]{Temporality: metricdata.CumulativeTemporality, DataPoints: []metricdata.HistogramDataPoint[float64]{{Count: 3, Bounds: []float64{1}, BucketCounts: []uint64{1, 2}}}}},
	}}}}
}
func heldQueue(t *testing.T) (*queuedMetricExporter, *heldMetricExporter) {
	t.Helper()
	e := &heldMetricExporter{entered: make(chan struct{}), release: make(chan struct{})}
	q := NewQueuedMetricExporter(e).(*queuedMetricExporter)
	t.Cleanup(func() {
		e.releaseOnce.Do(func() { close(e.release) })
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = q.Shutdown(ctx)
	})
	return q, e
}
func TestQueuedMetricExporterFreezesSnapshot(t *testing.T) {
	q, e := heldQueue(t)
	e.result = make(chan *metricdata.ResourceMetrics, 1)
	data := sampleMetric()
	if err := q.Export(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	<-e.entered
	data.ScopeMetrics[0].Metrics[0].Data.(metricdata.Gauge[int64]).DataPoints[0].Value = 99
	data.ScopeMetrics[0].Metrics[1].Data.(metricdata.Histogram[float64]).DataPoints[0].BucketCounts[0] = 99
	q.mu.Lock()
	pending := q.pendingBytes
	q.mu.Unlock()
	if pending == 0 {
		t.Fatal("in-flight bytes were released before export finished")
	}
	// A ForceFlush must not report success while this export remains blocked.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := q.ForceFlush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("flush: %v", err)
	}
	// Complete inside the test to inspect the values, retaining a safe cleanup.
	e.releaseOnce.Do(func() { close(e.release) })

	got := <-e.result
	if got.ScopeMetrics[0].Metrics[0].Data.(metricdata.Gauge[int64]).DataPoints[0].Value != 11 {
		t.Fatal("gauge snapshot was mutated")
	}
	if got.ScopeMetrics[0].Metrics[1].Data.(metricdata.Histogram[float64]).DataPoints[0].BucketCounts[0] != 1 {
		t.Fatal("histogram snapshot was mutated")
	}
}
func TestQueuedMetricExporterBoundsBatchesAndBytes(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(map[bool]string{false: "batches", true: "bytes"}[large], func(t *testing.T) {
			q, e := heldQueue(t)
			data := sampleMetric()
			if large {
				g := data.ScopeMetrics[0].Metrics[0].Data.(metricdata.Gauge[int64])
				g.DataPoints[0].Attributes = attribute.NewSet(attribute.String("large", strings.Repeat("x", 2<<20)))
				data.ScopeMetrics[0].Metrics[0].Data = g
			}
			if err := q.Export(context.Background(), data); err != nil {
				t.Fatal(err)
			}
			<-e.entered
			accepted := 1
			for i := 0; i < 100; i++ {
				err := q.Export(context.Background(), data)
				if errors.Is(err, errMetricQueueFull) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				accepted++
			}
			if accepted > metricQueueBatches+1 {
				t.Fatalf("unbounded batches: %d", accepted)
			}
			if large && accepted >= 4 {
				t.Fatalf("unbounded bytes: %d large batches", accepted)
			}
			q.mu.Lock()
			pending := q.pendingBytes
			q.mu.Unlock()
			if pending > metricQueueBytes {
				t.Fatalf("retained %d bytes", pending)
			}
		})
	}
}
func TestQueuedMetricExporterShutdownDoesNotWaitForSink(t *testing.T) {
	q, e := heldQueue(t)
	if err := q.Export(context.Background(), sampleMetric()); err != nil {
		t.Fatal(err)
	}
	<-e.entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := q.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown: %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("shutdown waited for remote sink")
	}
	if err := q.Export(context.Background(), sampleMetric()); !errors.Is(err, errMetricQueueClosed) {
		t.Fatalf("closed queue: %v", err)
	}
}

func TestMetricSnapshotPreservesAllAggregations(t *testing.T) {
	metrics := []metricdata.Metrics{
		{Name: "gauge-int", Data: metricdata.Gauge[int64]{DataPoints: []metricdata.DataPoint[int64]{{Value: 1, Exemplars: []metricdata.Exemplar[int64]{{SpanID: []byte{1}, TraceID: []byte{2}, FilteredAttributes: []attribute.KeyValue{attribute.String("a", "b")}}}}}}},
		{Name: "gauge-float", Data: metricdata.Gauge[float64]{DataPoints: []metricdata.DataPoint[float64]{{Value: 1.5}}}},
		{Name: "sum-int", Data: metricdata.Sum[int64]{IsMonotonic: true, Temporality: metricdata.CumulativeTemporality, DataPoints: []metricdata.DataPoint[int64]{{Value: 2}}}},
		{Name: "sum-float", Data: metricdata.Sum[float64]{Temporality: metricdata.DeltaTemporality, DataPoints: []metricdata.DataPoint[float64]{{Value: 2.5}}}},
		{Name: "hist-int", Data: metricdata.Histogram[int64]{Temporality: metricdata.CumulativeTemporality, DataPoints: []metricdata.HistogramDataPoint[int64]{{Count: 1, Bounds: []float64{1}, BucketCounts: []uint64{0, 1}}}}},
		{Name: "hist-float", Data: metricdata.Histogram[float64]{Temporality: metricdata.CumulativeTemporality, DataPoints: []metricdata.HistogramDataPoint[float64]{{Count: 1, Bounds: []float64{1}, BucketCounts: []uint64{0, 1}}}}},
		{Name: "exp-int", Data: metricdata.ExponentialHistogram[int64]{Temporality: metricdata.CumulativeTemporality, DataPoints: []metricdata.ExponentialHistogramDataPoint[int64]{{Count: 1, PositiveBucket: metricdata.ExponentialBucket{Counts: []uint64{1}}, NegativeBucket: metricdata.ExponentialBucket{Counts: []uint64{0}}}}}},
		{Name: "exp-float", Data: metricdata.ExponentialHistogram[float64]{Temporality: metricdata.CumulativeTemporality, DataPoints: []metricdata.ExponentialHistogramDataPoint[float64]{{Count: 1, PositiveBucket: metricdata.ExponentialBucket{Counts: []uint64{1}}, NegativeBucket: metricdata.ExponentialBucket{Counts: []uint64{0}}}}}},
		{Name: "summary", Data: metricdata.Summary{DataPoints: []metricdata.SummaryDataPoint{{Count: 1, Sum: 3, QuantileValues: []metricdata.QuantileValue{{Quantile: 0.5, Value: 3}}}}}},
	}
	data := &metricdata.ResourceMetrics{Resource: resource.Empty(), ScopeMetrics: []metricdata.ScopeMetrics{{Metrics: metrics}}}
	before, err := ResourceMetricsToPB(data)
	if err != nil {
		t.Fatal(err)
	}
	before = proto.Clone(before).(*metricspb.ResourceMetrics)
	copied, err := snapshotMetrics(data)
	if err != nil {
		t.Fatal(err)
	}
	metrics[0].Data.(metricdata.Gauge[int64]).DataPoints[0].Exemplars[0].SpanID[0] = 9
	metrics[0].Data.(metricdata.Gauge[int64]).DataPoints[0].Exemplars[0].TraceID[0] = 9
	metrics[0].Data.(metricdata.Gauge[int64]).DataPoints[0].Exemplars[0].FilteredAttributes[0] = attribute.String("changed", "value")
	metrics[2].Data.(metricdata.Sum[int64]).DataPoints[0].Value = 99
	metrics[6].Data.(metricdata.ExponentialHistogram[int64]).DataPoints[0].PositiveBucket.Counts[0] = 99
	metrics[7].Data.(metricdata.ExponentialHistogram[float64]).DataPoints[0].NegativeBucket.Counts[0] = 99
	metrics[8].Data.(metricdata.Summary).DataPoints[0].QuantileValues[0].Value = 99
	after, err := ResourceMetricsToPB(copied)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(before, after) {
		t.Fatal("snapshot changed metric values, temporality, exemplars or histogram buckets")
	}
}

func TestQueuedMetricExporterPreservesContextValues(t *testing.T) {
	type key struct{}
	q, e := heldQueue(t)
	e.contexts = make(chan context.Context, 1)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "export metadata"))
	if err := q.Export(ctx, sampleMetric()); err != nil {
		t.Fatal(err)
	}
	cancel()
	got := <-e.contexts
	if got.Value(key{}) != "export metadata" || got.Err() != nil {
		t.Fatal("queued export lost metadata or inherited collection cancellation")
	}
}
