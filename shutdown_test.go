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
	"sync"
	"testing"
	"time"
)

func TestShutdownWithinConcurrent(t *testing.T) {
	allStarted := make(chan struct{})
	var started sync.WaitGroup
	started.Add(3)
	go func() { started.Wait(); close(allStarted) }()
	providers := make([]namedShutdown, 3)
	for i, name := range []string{"traces", "logs", "metrics"} {
		providers[i] = namedShutdown{name, func(ctx context.Context) error {
			started.Done()
			select {
			case <-allStarted:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := shutdownWithin(ctx, providers); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownWithinDoesNotWaitForUncooperativeSDK(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := shutdownWithin(ctx, []namedShutdown{{"blocked", func(context.Context) error { <-release; return nil }}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("shutdown waited %s", elapsed)
	}
}

func TestShutdownWithinReportsFailureWithoutWaiting(t *testing.T) {
	failed := errors.New("ingest unavailable")
	err := shutdownWithin(context.Background(), []namedShutdown{{"traces", func(context.Context) error { return failed }}})
	if !errors.Is(err, failed) || err.Error() != "traces: ingest unavailable" {
		t.Fatalf("unexpected error: %v", err)
	}
}
