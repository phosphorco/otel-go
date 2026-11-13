// PHOSPHOR PATCH-FOR-THE-PATCH (2026-11-13): downstream follow-up to
// Dagger 1.0.0-beta.23 telemetry lifecycle changes (f0a3d728), on otel-go
// 132b1dd82eb2. During an upstream conflict, preserve the caller deadline,
// independent remote metric forwarding, immutable bounded snapshots, and
// command output/verdict separation. See PHOSPHOR_PATCHES.md before dropping
// or replacing this follow-up; its failure probes are the acceptance check.

package otel

import (
	"fmt"
	"slices"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Resource and attribute sets are immutable SDK values. Mutable aggregation,
// bucket and exemplar slices must be detached before a reader reuses them.
func snapshotMetrics(data *metricdata.ResourceMetrics) (*metricdata.ResourceMetrics, error) {
	out := &metricdata.ResourceMetrics{Resource: data.Resource, ScopeMetrics: slices.Clone(data.ScopeMetrics)}
	for i := range out.ScopeMetrics {
		scope := &out.ScopeMetrics[i]
		scope.Metrics = slices.Clone(scope.Metrics)
		for j := range scope.Metrics {
			m := &scope.Metrics[j]
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				d.DataPoints = clonePoints(d.DataPoints)
				m.Data = d
			case metricdata.Gauge[float64]:
				d.DataPoints = clonePoints(d.DataPoints)
				m.Data = d
			case metricdata.Sum[int64]:
				d.DataPoints = clonePoints(d.DataPoints)
				m.Data = d
			case metricdata.Sum[float64]:
				d.DataPoints = clonePoints(d.DataPoints)
				m.Data = d
			case metricdata.Histogram[int64]:
				d.DataPoints = cloneHistogram(d.DataPoints)
				m.Data = d
			case metricdata.Histogram[float64]:
				d.DataPoints = cloneHistogram(d.DataPoints)
				m.Data = d
			case metricdata.ExponentialHistogram[int64]:
				d.DataPoints = cloneExponential(d.DataPoints)
				m.Data = d
			case metricdata.ExponentialHistogram[float64]:
				d.DataPoints = cloneExponential(d.DataPoints)
				m.Data = d
			case metricdata.Summary:
				d.DataPoints = slices.Clone(d.DataPoints)
				for k := range d.DataPoints {
					d.DataPoints[k].QuantileValues = slices.Clone(d.DataPoints[k].QuantileValues)
				}
				m.Data = d
			default:
				return nil, fmt.Errorf("unsupported queued aggregation %T", d)
			}
		}
	}
	return out, nil
}
func cloneExemplars[N int64 | float64](data []metricdata.Exemplar[N]) []metricdata.Exemplar[N] {
	out := slices.Clone(data)
	for i := range out {
		out[i].FilteredAttributes = slices.Clone(out[i].FilteredAttributes)
		out[i].SpanID = slices.Clone(out[i].SpanID)
		out[i].TraceID = slices.Clone(out[i].TraceID)
	}
	return out
}
func clonePoints[N int64 | float64](data []metricdata.DataPoint[N]) []metricdata.DataPoint[N] {
	out := slices.Clone(data)
	for i := range out {
		out[i].Exemplars = cloneExemplars(out[i].Exemplars)
	}
	return out
}
func cloneHistogram[N int64 | float64](data []metricdata.HistogramDataPoint[N]) []metricdata.HistogramDataPoint[N] {
	out := slices.Clone(data)
	for i := range out {
		out[i].Bounds = slices.Clone(out[i].Bounds)
		out[i].BucketCounts = slices.Clone(out[i].BucketCounts)
		out[i].Exemplars = cloneExemplars(out[i].Exemplars)
	}
	return out
}
func cloneExponential[N int64 | float64](data []metricdata.ExponentialHistogramDataPoint[N]) []metricdata.ExponentialHistogramDataPoint[N] {
	out := slices.Clone(data)
	for i := range out {
		out[i].PositiveBucket.Counts = slices.Clone(out[i].PositiveBucket.Counts)
		out[i].NegativeBucket.Counts = slices.Clone(out[i].NegativeBucket.Counts)
		out[i].Exemplars = cloneExemplars(out[i].Exemplars)
	}
	return out
}
