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
	"fmt"
	"sync"

	otelapi "go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"google.golang.org/protobuf/proto"
)

const metricQueueBytes = 8 << 20
const metricQueueBatches = 32

var errMetricQueueFull = errors.New("remote metric export queue full")
var errMetricQueueClosed = errors.New("remote metric export queue closed")

type metricRequest struct {
	data    *metricdata.ResourceMetrics
	bytes   int
	barrier chan error
	ctx     context.Context
}

type queuedMetricExporter struct {
	next         sdkmetric.Exporter
	queue        chan metricRequest
	mu           sync.Mutex
	pendingBytes int
	closed       bool
	cancel       context.CancelFunc
	done         chan struct{}
	lastErr      error
}

// NewQueuedMetricExporter isolates a remote exporter from telemetry stream
// readers and application metric readers. Accepted snapshots are immutable;
// retained OTLP payload size, including the in-flight request, is bounded to 8 MiB and
// at most 32 queued batches. A full queue returns a visible error, never an
// assertion of delivery. The underlying exporter retains its retry policy.
func NewQueuedMetricExporter(next sdkmetric.Exporter) sdkmetric.Exporter {
	ctx, cancel := context.WithCancel(context.Background())
	q := &queuedMetricExporter{next: next, queue: make(chan metricRequest, metricQueueBatches), cancel: cancel, done: make(chan struct{})}
	go q.run(ctx)
	return q
}
func (q *queuedMetricExporter) Temporality(k sdkmetric.InstrumentKind) metricdata.Temporality {
	return q.next.Temporality(k)
}
func (q *queuedMetricExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return q.next.Aggregation(k)
}

func (q *queuedMetricExporter) Export(ctx context.Context, data *metricdata.ResourceMetrics) error {
	if data == nil || len(data.ScopeMetrics) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	pb, err := ResourceMetricsToPB(data)
	if err != nil {
		return err
	}
	if proto.Size(pb) > metricQueueBytes {
		return errMetricQueueFull
	}
	copied, err := snapshotMetrics(data)
	if err != nil {
		return err
	}
	return q.enqueue(metricRequest{data: copied, bytes: proto.Size(pb), ctx: context.WithoutCancel(ctx)})
}
func (q *queuedMetricExporter) enqueue(item metricRequest) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return errMetricQueueClosed
	}
	if item.bytes > metricQueueBytes-q.pendingBytes {
		return errMetricQueueFull
	}
	select {
	case q.queue <- item:
		q.pendingBytes += item.bytes
		return nil
	default:
		return errMetricQueueFull
	}
}
func (q *queuedMetricExporter) ForceFlush(ctx context.Context) error {
	barrier := make(chan error, 1)
	if err := q.enqueue(metricRequest{barrier: barrier, ctx: ctx}); err != nil {
		return err
	}
	select {
	case err := <-barrier:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (q *queuedMetricExporter) Shutdown(ctx context.Context) error {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		close(q.queue)
	}
	q.mu.Unlock()
	stop := context.AfterFunc(ctx, q.cancel)
	defer stop()
	select {
	case <-q.done:
		q.cancel()
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.lastErr
	case <-ctx.Done():
		q.cancel()
		return ctx.Err()
	}
}
func (q *queuedMetricExporter) run(ctx context.Context) {
	defer close(q.done)
	for item := range q.queue {
		var err error
		if item.barrier != nil {
			q.mu.Lock()
			err = q.lastErr
			q.mu.Unlock()
			flushCtx, cancel := context.WithCancel(item.ctx)
			stop := context.AfterFunc(ctx, cancel)
			err = errors.Join(err, q.next.ForceFlush(flushCtx))
			stop()
			cancel()
			item.barrier <- err
			continue
		}
		if err = ctx.Err(); err == nil {
			exportCtx, cancel := context.WithCancel(item.ctx)
			stop := context.AfterFunc(ctx, cancel)
			err = q.next.Export(exportCtx, item.data)
			stop()
			cancel()
		}
		q.mu.Lock()
		q.pendingBytes -= item.bytes
		if err != nil {
			q.lastErr = err
		}
		q.mu.Unlock()
		if err != nil {
			otelapi.Handle(fmt.Errorf("remote metric export: %w", err))
		}
	}
	if err := q.next.Shutdown(ctx); err != nil {
		q.mu.Lock()
		q.lastErr = err
		q.mu.Unlock()
	}
}
